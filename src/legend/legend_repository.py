"""
Legend repository – all DB reads and writes for legend_members.

No business logic lives here; only query construction and persistence.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import List, Optional, Tuple

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from src.db.models import LegendMember, Member, ProfileSnapshot


# ---------------------------------------------------------------------------
# Reads
# ---------------------------------------------------------------------------

def get_by_member_id(
    session: Session,
    member_id: uuid.UUID,
) -> Optional[LegendMember]:
    """Return the LegendMember row for *member_id*, or None."""
    return session.execute(
        select(LegendMember).where(LegendMember.member_id == member_id)
    ).scalar_one_or_none()


def get_by_id(
    session: Session,
    legend_member_id: uuid.UUID,
) -> Optional[LegendMember]:
    """Return the LegendMember row by its own PK, or None."""
    return session.get(LegendMember, legend_member_id)


def list_legend_members(
    session: Session,
    group_id: Optional[uuid.UUID] = None,
    archive_status: Optional[str] = None,
    simulation_enabled: Optional[bool] = None,
    limit: int = 100,
    offset: int = 0,
) -> Tuple[List[LegendMember], int]:
    """
    Return (rows, total_count) with optional filters.

    Ordered by ``archived_at DESC, id DESC`` for stable pagination.
    """
    limit = min(max(1, limit), 500)
    offset = max(0, offset)

    base = select(LegendMember)
    if group_id is not None:
        base = base.where(LegendMember.group_id == group_id)
    if archive_status is not None:
        base = base.where(LegendMember.archive_status == archive_status)
    if simulation_enabled is not None:
        base = base.where(LegendMember.simulation_enabled == simulation_enabled)

    total = session.execute(
        select(func.count()).select_from(base.subquery())
    ).scalar_one()

    rows = list(
        session.execute(
            base.order_by(LegendMember.archived_at.desc())
            .order_by(LegendMember.id.desc())
            .limit(limit)
            .offset(offset)
        ).scalars().all()
    )
    return rows, total


def get_latest_profile_snapshot_id(
    session: Session,
    member_id: uuid.UUID,
) -> Optional[uuid.UUID]:
    """
    Return the id of the most recent ProfileSnapshot for *member_id*, or None.

    Used during archiving to anchor the legend record to the best available
    profile.  If no snapshot exists, the caller stores None and records this
    in the archive reason or logs.
    """
    row = session.execute(
        select(ProfileSnapshot.id)
        .where(ProfileSnapshot.member_id == member_id)
        .order_by(ProfileSnapshot.snapshot_at.desc())
        .limit(1)
    ).scalar_one_or_none()
    return row


# ---------------------------------------------------------------------------
# Writes
# ---------------------------------------------------------------------------

def create_legend_member(
    session: Session,
    member: Member,
    archived_at: datetime,
    archived_reason: Optional[str],
    archived_by: Optional[str],
    source_profile_snapshot_id: Optional[uuid.UUID],
) -> LegendMember:
    """Insert a new LegendMember row and flush (does not commit)."""
    now = datetime.now(tz=timezone.utc)
    lm = LegendMember(
        id=uuid.uuid4(),
        member_id=member.id,
        group_id=member.group_id,
        archive_status="archived",
        archived_at=archived_at,
        archived_reason=archived_reason,
        archived_by=archived_by,
        source_profile_snapshot_id=source_profile_snapshot_id,
        member_display_name_snapshot=member.display_name,
        member_external_id_snapshot=getattr(member, "external_member_id", None),
        member_status_snapshot=member.status,
        simulation_enabled=False,
        created_at=now,
        updated_at=now,
    )
    session.add(lm)
    session.flush()
    return lm


def update_archive(
    session: Session,
    lm: LegendMember,
    archived_at: datetime,
    archived_reason: Optional[str],
    archived_by: Optional[str],
    source_profile_snapshot_id: Optional[uuid.UUID],
    member: Member,
) -> LegendMember:
    """
    Re-archive a previously restored LegendMember (update in place).

    Refreshes all archive fields and re-snapshots member identity.
    Does not commit.
    """
    lm.archive_status = "archived"
    lm.archived_at = archived_at
    lm.archived_reason = archived_reason
    lm.archived_by = archived_by
    lm.source_profile_snapshot_id = source_profile_snapshot_id
    lm.member_display_name_snapshot = member.display_name
    lm.member_external_id_snapshot = getattr(member, "external_member_id", None)
    lm.member_status_snapshot = member.status
    lm.simulation_enabled = False
    session.flush()
    return lm


def set_archive_status(
    session: Session,
    lm: LegendMember,
    status: str,
) -> LegendMember:
    """Update archive_status and flush."""
    lm.archive_status = status
    session.flush()
    return lm


def set_simulation_enabled(
    session: Session,
    lm: LegendMember,
    enabled: bool,
) -> LegendMember:
    """Update simulation_enabled and flush."""
    lm.simulation_enabled = enabled
    session.flush()
    return lm
