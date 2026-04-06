"""
LegendService – business logic for the Legend Archive.

Operations
----------
archive_member        Archive a member (idempotent).
restore_member        Restore an archived member.
enable_simulation     Enable Persona Simulation for an archived member.
disable_simulation    Disable Persona Simulation.
get_legend_member     Fetch a single legend record by member_id.
list_legend_members   List legend records with optional filters.

Transaction model
-----------------
If ``db_session`` is None, the service creates its own ``SessionLocal()``,
commits on success, and rolls back + closes on error.
Pass an explicit session when the caller controls the transaction.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import List, Optional, Tuple

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from src.db.models import Member
from src.db.session import SessionLocal
from src.legend import legend_repository as repo
from src.legend.archive_policy import (
    STATUS_ARCHIVED,
    STATUS_RESTORED,
    ArchiveNotEligibleError,
    InvalidStatusTransitionError,
    assert_can_restore,
    assert_can_toggle_simulation,
    assert_eligible_for_archive,
)
from src.legend.legend_schemas import (
    ArchiveResult,
    LegendMemberSchema,
    RestoreResult,
    SimulationToggleResult,
)


class LegendService:
    """
    Orchestrates Legend Archive operations.

    Usage::

        service = LegendService()
        result = service.archive_member(member_id=some_uuid)
    """

    def __init__(self, db_session: Optional[Session] = None):
        self._session: Optional[Session] = db_session
        self._owns_session = db_session is None

    def _get_session(self) -> Session:
        if self._session is None:
            self._session = SessionLocal()
        return self._session

    def _close_session(self, commit: bool = True) -> None:
        if not self._owns_session or self._session is None:
            return
        try:
            if commit:
                self._session.commit()
            else:
                self._session.rollback()
        finally:
            self._session.close()
            self._session = None

    def commit(self) -> None:
        """Commit the current session (used by API when session is caller-owned)."""
        self._get_session().commit()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def archive_member(
        self,
        member_id: uuid.UUID,
        archived_reason: Optional[str] = None,
        archived_by: Optional[str] = None,
        force: bool = False,
    ) -> ArchiveResult:
        """
        Archive a member into the Legend Archive.

        Idempotency
        -----------
        If the member is already archived (``archive_status == "archived"``),
        returns the existing record without modification and sets
        ``was_already_archived=True``.

        Re-archive after restore
        ------------------------
        If the member was previously restored (``archive_status == "restored"``),
        the existing row is updated in place: archive fields are refreshed,
        the profile snapshot anchor is updated to the latest available snapshot,
        and ``simulation_enabled`` is reset to False.

        Profile snapshot
        ----------------
        The service looks up the most recent ``ProfileSnapshot`` for the member.
        If none exists, ``source_profile_snapshot_id`` is stored as None.
        This is expected for members who were never profiled; the archive
        record is still created.

        Args:
            member_id:       UUID of the member to archive.
            archived_reason: Optional human-readable reason.
            archived_by:     Optional actor identifier (username, system name).
            force:           Bypass eligibility check (archive any status).

        Returns:
            ArchiveResult

        Raises:
            ValueError:              member_id not found in DB.
            ArchiveNotEligibleError: member status not eligible (and force=False).
        """
        session = self._get_session()
        try:
            member = self._load_member_or_raise(session, member_id)
            assert_eligible_for_archive(member.status, force=force)

            existing = repo.get_by_member_id(session, member_id)
            now = datetime.now(tz=timezone.utc)
            snapshot_id = repo.get_latest_profile_snapshot_id(session, member_id)

            if existing is not None and existing.archive_status == STATUS_ARCHIVED:
                # Idempotent: already archived, return without changes
                self._close_session(commit=False)
                return ArchiveResult(
                    legend_member_id=existing.id,
                    member_id=member_id,
                    archive_status=STATUS_ARCHIVED,
                    was_already_archived=True,
                    profile_snapshot_id=existing.source_profile_snapshot_id,
                )

            if existing is not None:
                # Re-archive after restore: update in place
                lm = repo.update_archive(
                    session, existing,
                    archived_at=now,
                    archived_reason=archived_reason,
                    archived_by=archived_by,
                    source_profile_snapshot_id=snapshot_id,
                    member=member,
                )
            else:
                # First-time archive: create new row
                try:
                    # Use a SAVEPOINT so caller-owned sessions are not left in
                    # failed transaction state if this insert races on UNIQUE(member_id).
                    with session.begin_nested():
                        lm = repo.create_legend_member(
                            session, member,
                            archived_at=now,
                            archived_reason=archived_reason,
                            archived_by=archived_by,
                            source_profile_snapshot_id=snapshot_id,
                        )
                except IntegrityError:
                    # Concurrency fallback: another request inserted the row first.
                    existing_after_conflict = repo.get_by_member_id(session, member_id)
                    if (
                        existing_after_conflict is not None
                        and existing_after_conflict.archive_status == STATUS_ARCHIVED
                    ):
                        self._close_session(commit=False)
                        return ArchiveResult(
                            legend_member_id=existing_after_conflict.id,
                            member_id=member_id,
                            archive_status=STATUS_ARCHIVED,
                            was_already_archived=True,
                            profile_snapshot_id=existing_after_conflict.source_profile_snapshot_id,
                        )
                    raise

            self._close_session(commit=True)
            return ArchiveResult(
                legend_member_id=lm.id,
                member_id=member_id,
                archive_status=STATUS_ARCHIVED,
                was_already_archived=False,
                profile_snapshot_id=snapshot_id,
            )
        except Exception:
            self._close_session(commit=False)
            raise

    def restore_member(self, member_id: uuid.UUID) -> RestoreResult:
        """
        Restore an archived member.

        Sets ``archive_status = "restored"`` and ``simulation_enabled = False``.
        The original ``members`` row and the ``legend_members`` row are both
        preserved; nothing is deleted.

        Args:
            member_id: UUID of the member to restore.

        Returns:
            RestoreResult

        Raises:
            ValueError:                    member_id not found in legend_members.
            InvalidStatusTransitionError:  member is not currently archived.
        """
        session = self._get_session()
        try:
            lm = self._load_legend_or_raise(session, member_id)
            assert_can_restore(lm.archive_status)

            lm.simulation_enabled = False
            repo.set_archive_status(session, lm, STATUS_RESTORED)

            self._close_session(commit=True)
            return RestoreResult(
                legend_member_id=lm.id,
                member_id=member_id,
                archive_status=STATUS_RESTORED,
            )
        except Exception:
            self._close_session(commit=False)
            raise

    def enable_simulation(self, member_id: uuid.UUID) -> SimulationToggleResult:
        """
        Enable Persona Simulation for an archived legend member.

        Only allowed when ``archive_status == "archived"``.

        Args:
            member_id: UUID of the member.

        Returns:
            SimulationToggleResult

        Raises:
            ValueError:                    member_id not found in legend_members.
            InvalidStatusTransitionError:  member is not currently archived.
        """
        return self._toggle_simulation(member_id, enabled=True)

    def disable_simulation(self, member_id: uuid.UUID) -> SimulationToggleResult:
        """
        Disable Persona Simulation for a legend member.

        Allowed regardless of archive_status (disabling is always safe).

        Args:
            member_id: UUID of the member.

        Returns:
            SimulationToggleResult

        Raises:
            ValueError: member_id not found in legend_members.
        """
        return self._toggle_simulation(member_id, enabled=False)

    def get_legend_member(self, member_id: uuid.UUID) -> Optional[LegendMemberSchema]:
        """
        Return the LegendMemberSchema for *member_id*, or None if not archived.
        """
        session = self._get_session()
        try:
            lm = repo.get_by_member_id(session, member_id)
            result = LegendMemberSchema.model_validate(lm) if lm else None
            self._close_session(commit=False)
            return result
        except Exception:
            self._close_session(commit=False)
            raise

    def list_legend_members(
        self,
        group_id: Optional[uuid.UUID] = None,
        archive_status: Optional[str] = None,
        simulation_enabled: Optional[bool] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> Tuple[List[LegendMemberSchema], int]:
        """
        Return (schemas, total_count) with optional filters.

        Args:
            group_id:           Filter by group.
            archive_status:     Filter by "archived" or "restored".
            simulation_enabled: Filter by simulation gate.
            limit:              Page size (max 500).
            offset:             Page offset.

        Returns:
            (list of LegendMemberSchema, total count before pagination)
        """
        session = self._get_session()
        try:
            rows, total = repo.list_legend_members(
                session,
                group_id=group_id,
                archive_status=archive_status,
                simulation_enabled=simulation_enabled,
                limit=limit,
                offset=offset,
            )
            schemas = [LegendMemberSchema.model_validate(r) for r in rows]
            self._close_session(commit=False)
            return schemas, total
        except Exception:
            self._close_session(commit=False)
            raise

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _load_member_or_raise(session: Session, member_id: uuid.UUID) -> Member:
        member = session.get(Member, member_id)
        if member is None:
            raise ValueError(f"Member {member_id} not found in the database.")
        return member

    @staticmethod
    def _load_legend_or_raise(session: Session, member_id: uuid.UUID):
        lm = repo.get_by_member_id(session, member_id)
        if lm is None:
            raise ValueError(
                f"No legend record found for member {member_id}. "
                f"Archive the member first."
            )
        return lm

    def _toggle_simulation(
        self,
        member_id: uuid.UUID,
        enabled: bool,
    ) -> SimulationToggleResult:
        session = self._get_session()
        try:
            lm = self._load_legend_or_raise(session, member_id)
            if enabled:
                assert_can_toggle_simulation(lm.archive_status)
            repo.set_simulation_enabled(session, lm, enabled)
            self._close_session(commit=True)
            return SimulationToggleResult(
                legend_member_id=lm.id,
                member_id=member_id,
                simulation_enabled=enabled,
            )
        except Exception:
            self._close_session(commit=False)
            raise