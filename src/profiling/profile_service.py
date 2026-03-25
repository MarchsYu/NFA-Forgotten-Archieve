"""
ProfileService – DB orchestration layer for Persona Profile generation.

Responsibilities
----------------
- Load members and their messages / topic assignments from the DB.
- Resolve reply targets for interaction analysis.
- Call ProfileBuilder for each member.
- Write ProfileSnapshot rows to the DB.
- Handle idempotency (skip existing) and rerun (delete + re-insert).

Idempotency
-----------
The profile_snapshots table has a unique constraint on
(member_id, profile_version, window_start, window_end).

Incremental mode (rerun=False, default):
    Skip members who already have a snapshot for this version + window.

Rerun mode (rerun=True):
    Delete existing snapshots for this version + window, then re-generate.

Transaction isolation – per-member savepoints
---------------------------------------------
Each member's profile write is wrapped in a SAVEPOINT (nested transaction).
If one member fails, only that savepoint is rolled back; all previously
committed savepoints in the same run remain intact.  The outer transaction
is committed at the end of the loop.

This guarantees that profiles_written always matches the actual number of
rows in profile_snapshots after the run completes.

classifier_version vs profile_version
--------------------------------------
- profile_version  : version of the profiling algorithm (e.g. "profile_v1").
                     Stored in profile_snapshots.profile_version.
- classifier_version: version of the topic classifier whose MessageTopic rows
                     are consumed (e.g. "rule_v1").  Stored in
                     profile_snapshots.stats["classifier_version"].
                     Must be specified explicitly; defaults to CLASSIFIER_VERSION
                     from the classification module.

Run modes
---------
Mode A – group scope:
    service.run(profile_version=..., window_start=..., window_end=...,
                group_id=<uuid>)
    Profiles every member in the group.

Mode B – single member:
    service.run(profile_version=..., window_start=..., window_end=...,
                member_id=<uuid>, group_id=<uuid>)
    Profiles exactly one member.  member must belong to group_id.

Mode C – all groups (no filter):
    service.run(profile_version=..., window_start=..., window_end=...)
    Profiles every member across all groups.
"""

from __future__ import annotations

import uuid
import warnings
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional, Tuple

from sqlalchemy import select, delete
from sqlalchemy.orm import Session

from src.classification.topic_classifier import CLASSIFIER_VERSION
from src.db.models import Member, Message, MessageTopic, ProfileSnapshot, Topic
from src.db.session import SessionLocal
from src.profiling.profile_builder import ProfileBuilder, ProfileData, PROFILE_VERSION


@dataclass
class ProfilingResult:
    """Summary of a profile generation run."""
    profile_version: str
    classifier_version: str
    window_start: datetime
    window_end: datetime
    members_attempted: int
    profiles_written: int
    profiles_skipped: int
    profiles_failed: int
    missing_topic_count: int = 0          # topic_id→key lookups that failed
    failed_member_ids: List[str] = field(default_factory=list)


class ProfileService:
    """
    Orchestrates Persona Profile generation for stored members.

    Usage::

        service = ProfileService()
        result = service.run(
            profile_version="profile_v1",
            classifier_version="rule_v1",
            window_start=datetime(2026, 1, 1, tzinfo=timezone.utc),
            window_end=datetime(2026, 12, 31, 23, 59, 59, tzinfo=timezone.utc),
            group_id=some_uuid,
        )
    """

    def __init__(
        self,
        db_session: Optional[Session] = None,
    ):
        self._session: Optional[Session] = db_session
        self._owns_session = db_session is None
        self._builder = ProfileBuilder()

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

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def run(
        self,
        profile_version: str = PROFILE_VERSION,
        classifier_version: str = CLASSIFIER_VERSION,
        window_start: Optional[datetime] = None,
        window_end: Optional[datetime] = None,
        group_id: Optional[uuid.UUID] = None,
        member_id: Optional[uuid.UUID] = None,
        rerun: bool = False,
    ) -> ProfilingResult:
        """
        Generate Persona Profiles and write them to profile_snapshots.

        Args:
            profile_version:    Version tag for this run (e.g. "profile_v1").
            classifier_version: Which MessageTopic classifier version to read
                                (e.g. "rule_v1").  Must be explicit; no
                                auto-detection.
            window_start:       Start of the analysis window (UTC, inclusive).
            window_end:         End of the analysis window (UTC, inclusive).
            group_id:           Restrict to one group (optional).
            member_id:          Restrict to one member (requires group_id).
                                The member must belong to group_id.
            rerun:              If True, delete existing snapshots for this
                                version + window before re-generating.

        Returns:
            ProfilingResult with counts.
        """
        if window_start is None or window_end is None:
            raise ValueError("window_start and window_end are required.")
        if member_id is not None and group_id is None:
            raise ValueError("group_id is required when member_id is specified.")

        session = self._get_session()
        try:
            # Build topic_key lookup: topic_id → topic_key
            topic_key_map = self._load_topic_key_map(session)

            # Resolve members to process
            members = self._load_members(session, group_id, member_id)

            # Validate member/group consistency when both are specified
            if member_id is not None and group_id is not None:
                self._assert_member_in_group(members, member_id, group_id)

            if rerun:
                self._delete_existing(session, profile_version, window_start, window_end,
                                      group_id, member_id)
                session.flush()

            # Snapshot of already-existing member_ids (after potential delete)
            existing_keys = self._load_existing_keys(
                session, profile_version, window_start, window_end
            )

            result = ProfilingResult(
                profile_version=profile_version,
                classifier_version=classifier_version,
                window_start=window_start,
                window_end=window_end,
                members_attempted=len(members),
                profiles_written=0,
                profiles_skipped=0,
                profiles_failed=0,
            )

            for member in members:
                if member.id in existing_keys:
                    result.profiles_skipped += 1
                    continue

                # ── Per-member savepoint ──────────────────────────────────
                # If this member fails, only its savepoint is rolled back.
                # All previously released savepoints (successful members)
                # remain in the outer transaction and will be committed.
                try:
                    with session.begin_nested():
                        profile, missing = self._build_profile(
                            session, member, topic_key_map,
                            profile_version, classifier_version,
                            window_start, window_end,
                        )
                        self._write_snapshot(session, profile)
                    # Savepoint released (committed to outer transaction)
                    result.profiles_written += 1
                    result.missing_topic_count += missing
                    if missing:
                        warnings.warn(
                            f"ProfileService: {missing} topic_id(s) had no matching "
                            f"topic_key for member {member.id} ({member.display_name}). "
                            f"Those topic assignments were excluded from the profile. "
                            f"Run scripts/init_topics.py to seed missing topics."
                        )
                except Exception as exc:
                    # Savepoint was automatically rolled back by begin_nested()
                    warnings.warn(
                        f"ProfileService: failed to build profile for member "
                        f"{member.id} ({member.display_name}): {exc}"
                    )
                    result.profiles_failed += 1
                    result.failed_member_ids.append(str(member.id))

            self._close_session(commit=True)
            return result

        except Exception:
            self._close_session(commit=False)
            raise

    # ------------------------------------------------------------------
    # Internal helpers – validation
    # ------------------------------------------------------------------

    @staticmethod
    def _assert_member_in_group(
        members: List[Member],
        member_id: uuid.UUID,
        group_id: uuid.UUID,
    ) -> None:
        """
        Raise ValueError if the loaded member does not belong to group_id.

        Called only when both member_id and group_id are specified.
        """
        if not members:
            raise ValueError(
                f"member_id {member_id} not found in the database."
            )
        member = members[0]
        if member.group_id != group_id:
            raise ValueError(
                f"member_id {member_id} belongs to group {member.group_id}, "
                f"not to the specified group_id {group_id}. "
                f"Refusing to write profile_snapshot to avoid data corruption."
            )

    # ------------------------------------------------------------------
    # Internal helpers – data loading
    # ------------------------------------------------------------------

    @staticmethod
    def _load_topic_key_map(session: Session) -> Dict[int, str]:
        """Return {topic_id: topic_key} for all active topics."""
        rows = session.execute(
            select(Topic.id, Topic.topic_key).where(Topic.is_active == True)
        ).all()
        return {row.id: row.topic_key for row in rows}

    @staticmethod
    def _load_members(
        session: Session,
        group_id: Optional[uuid.UUID],
        member_id: Optional[uuid.UUID],
    ) -> List[Member]:
        stmt = select(Member)
        if member_id is not None:
            stmt = stmt.where(Member.id == member_id)
        elif group_id is not None:
            stmt = stmt.where(Member.group_id == group_id)
        return list(session.execute(stmt).scalars().all())

    @staticmethod
    def _load_existing_keys(
        session: Session,
        profile_version: str,
        window_start: datetime,
        window_end: datetime,
    ) -> set:
        """Return set of member_ids that already have a snapshot for this version+window."""
        rows = session.execute(
            select(ProfileSnapshot.member_id).where(
                ProfileSnapshot.profile_version == profile_version,
                ProfileSnapshot.window_start == window_start,
                ProfileSnapshot.window_end == window_end,
            )
        ).scalars().all()
        return set(rows)

    @staticmethod
    def _delete_existing(
        session: Session,
        profile_version: str,
        window_start: datetime,
        window_end: datetime,
        group_id: Optional[uuid.UUID],
        member_id: Optional[uuid.UUID],
    ) -> None:
        stmt = delete(ProfileSnapshot).where(
            ProfileSnapshot.profile_version == profile_version,
            ProfileSnapshot.window_start == window_start,
            ProfileSnapshot.window_end == window_end,
        )
        if member_id is not None:
            stmt = stmt.where(ProfileSnapshot.member_id == member_id)
        elif group_id is not None:
            stmt = stmt.where(ProfileSnapshot.group_id == group_id)
        session.execute(stmt)

    # ------------------------------------------------------------------
    # Internal helpers – profile building
    # ------------------------------------------------------------------

    def _build_profile(
        self,
        session: Session,
        member: Member,
        topic_key_map: Dict[int, str],
        profile_version: str,
        classifier_version: str,
        window_start: datetime,
        window_end: datetime,
    ) -> Tuple[ProfileData, int]:
        """
        Load member data and delegate to ProfileBuilder.

        Returns:
            (ProfileData, missing_topic_count)
        """
        messages = self._load_messages(session, member.id, window_start, window_end)
        topic_rows, missing = self._load_topic_rows(
            session, member.id, topic_key_map, classifier_version,
            window_start, window_end,
        )
        reply_targets = self._resolve_reply_targets(session, messages)

        profile = self._builder.build(
            member_id=member.id,
            group_id=member.group_id,
            messages=messages,
            topic_rows=topic_rows,
            reply_targets=reply_targets,
            profile_version=profile_version,
            classifier_version=classifier_version,
            window_start=window_start,
            window_end=window_end,
        )
        return profile, missing

    @staticmethod
    def _load_messages(
        session: Session,
        member_id: uuid.UUID,
        window_start: datetime,
        window_end: datetime,
    ) -> List[Message]:
        stmt = (
            select(Message)
            .where(
                Message.member_id == member_id,
                Message.sent_at >= window_start,
                Message.sent_at <= window_end,
            )
            .order_by(Message.sent_at)
        )
        return list(session.execute(stmt).scalars().all())

    @staticmethod
    def _load_topic_rows(
        session: Session,
        member_id: uuid.UUID,
        topic_key_map: Dict[int, str],
        classifier_version: str,
        window_start: datetime,
        window_end: datetime,
    ) -> Tuple[List[_TopicRow], int]:
        """
        Return lightweight topic-assignment rows for this member's messages,
        filtered to the specified classifier_version.

        Returns:
            (rows, missing_count)
            rows:          list of _TopicRow with .topic_key and .is_primary
            missing_count: number of topic_id values with no matching topic_key
                           (indicates unseeded topics – caller should warn)
        """
        # Subquery: message IDs for this member in the window
        msg_ids_sq = (
            select(Message.id)
            .where(
                Message.member_id == member_id,
                Message.sent_at >= window_start,
                Message.sent_at <= window_end,
            )
            .scalar_subquery()
        )

        rows = session.execute(
            select(MessageTopic.topic_id, MessageTopic.is_primary)
            .where(
                MessageTopic.message_id.in_(msg_ids_sq),
                # ── classifier_version filter ──────────────────────────
                # Only consume topic assignments from the specified version.
                # Without this, profiles built from different classifier runs
                # would be mixed, making results non-reproducible.
                MessageTopic.classifier_version == classifier_version,
            )
        ).all()

        result: List[_TopicRow] = []
        missing_count = 0
        for row in rows:
            topic_key = topic_key_map.get(row.topic_id)
            if topic_key is None:
                missing_count += 1
                continue
            result.append(_TopicRow(topic_key=topic_key, is_primary=row.is_primary))
        return result, missing_count

    @staticmethod
    def _resolve_reply_targets(
        session: Session,
        messages: List[Message],
    ) -> List[Dict]:
        """
        For each message with reply_to_message_id, look up the sender.

        Returns a list of {"member_id": uuid, "display_name": str} dicts,
        one entry per outgoing reply (duplicates allowed – counter handles them).

        Falls back to an empty list if no reply data is available.
        """
        reply_ids = [
            m.reply_to_message_id
            for m in messages
            if m.reply_to_message_id is not None
        ]
        if not reply_ids:
            return []

        replied_msgs = session.execute(
            select(Message.id, Message.member_id)
            .where(Message.id.in_(reply_ids))
        ).all()

        if not replied_msgs:
            return []

        sender_ids = list({row.member_id for row in replied_msgs})
        members = session.execute(
            select(Member.id, Member.display_name)
            .where(Member.id.in_(sender_ids))
        ).all()
        name_map = {row.id: row.display_name for row in members}

        replied_sender: Dict[int, uuid.UUID] = {
            row.id: row.member_id for row in replied_msgs
        }

        targets = []
        for m in messages:
            if m.reply_to_message_id is None:
                continue
            sender_id = replied_sender.get(m.reply_to_message_id)
            if sender_id is None:
                continue
            targets.append({
                "member_id": sender_id,
                "display_name": name_map.get(sender_id, str(sender_id)),
            })
        return targets

    # ------------------------------------------------------------------
    # Internal helpers – DB write
    # ------------------------------------------------------------------

    @staticmethod
    def _write_snapshot(session: Session, profile: ProfileData) -> None:
        snapshot = ProfileSnapshot(
            id=uuid.uuid4(),
            group_id=profile.group_id,
            member_id=profile.member_id,
            profile_version=profile.profile_version,
            snapshot_at=profile.snapshot_at,
            window_start=profile.window_start,
            window_end=profile.window_end,
            source_message_count=profile.source_message_count,
            persona_summary=profile.persona_summary,
            traits=profile.traits,
            stats=profile.stats,
        )
        session.add(snapshot)


# ---------------------------------------------------------------------------
# Internal lightweight data container (no ORM dependency)
# ---------------------------------------------------------------------------

class _TopicRow:
    """Minimal stand-in for a MessageTopic row used inside ProfileService."""
    __slots__ = ("topic_key", "is_primary")

    def __init__(self, topic_key: str, is_primary: bool):
        self.topic_key = topic_key
        self.is_primary = is_primary
