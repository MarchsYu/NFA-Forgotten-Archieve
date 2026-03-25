"""
Read-only repository for the API layer.

All functions accept a SQLAlchemy Session and return ORM objects or
plain Python values.  No business logic lives here — only DB queries.

Design principles
-----------------
- Every function is a plain function (no class needed at this scale).
- No writes, no side-effects.
- Pagination uses (limit, offset) throughout for simplicity and
  predictability.  Keyset pagination can be added later if needed.
- Aggregates (member_count, message_count) are computed with scalar
  sub-queries so they don't require loading full relationship lists.
"""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import List, Optional, Tuple

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from src.db.models import Group, Member, Message, ProfileSnapshot


# ---------------------------------------------------------------------------
# Groups
# ---------------------------------------------------------------------------

def get_groups(session: Session) -> List[Tuple[Group, int, int]]:
    """
    Return all groups with member_count and message_count aggregates.

    Returns a list of (Group, member_count, message_count) tuples.
    """
    member_count_sq = (
        select(func.count(Member.id))
        .where(Member.group_id == Group.id)
        .correlate(Group)
        .scalar_subquery()
    )
    message_count_sq = (
        select(func.count(Message.id))
        .where(Message.group_id == Group.id)
        .correlate(Group)
        .scalar_subquery()
    )
    rows = session.execute(
        select(Group, member_count_sq.label("mc"), message_count_sq.label("msgc"))
        .order_by(Group.name)
    ).all()
    return [(row.Group, row.mc, row.msgc) for row in rows]


def get_group_by_id(session: Session, group_id: uuid.UUID) -> Optional[Group]:
    return session.get(Group, group_id)


# ---------------------------------------------------------------------------
# Members
# ---------------------------------------------------------------------------

def get_members_by_group(
    session: Session,
    group_id: uuid.UUID,
) -> List[Tuple[Member, Optional[datetime]]]:
    """
    Return all members in a group with their latest profile snapshot_at.

    Returns a list of (Member, latest_snapshot_at | None) tuples.
    """
    latest_snap_sq = (
        select(func.max(ProfileSnapshot.snapshot_at))
        .where(ProfileSnapshot.member_id == Member.id)
        .correlate(Member)
        .scalar_subquery()
    )
    rows = session.execute(
        select(Member, latest_snap_sq.label("latest_snap"))
        .where(Member.group_id == group_id)
        .order_by(Member.display_name)
    ).all()
    return [(row.Member, row.latest_snap) for row in rows]


def get_member_by_id(
    session: Session,
    member_id: uuid.UUID,
) -> Optional[Tuple[Member, Optional[datetime]]]:
    """
    Return (Member, latest_snapshot_at) or None if not found.
    """
    latest_snap_sq = (
        select(func.max(ProfileSnapshot.snapshot_at))
        .where(ProfileSnapshot.member_id == member_id)
        .scalar_subquery()
    )
    row = session.execute(
        select(Member, latest_snap_sq.label("latest_snap"))
        .where(Member.id == member_id)
    ).first()
    if row is None:
        return None
    return (row.Member, row.latest_snap)


# ---------------------------------------------------------------------------
# Messages
# ---------------------------------------------------------------------------

_MSG_MAX_LIMIT = 200
_MSG_DEFAULT_LIMIT = 50


def get_messages_by_member(
    session: Session,
    member_id: uuid.UUID,
    limit: int = _MSG_DEFAULT_LIMIT,
    offset: int = 0,
    sent_at_gte: Optional[datetime] = None,
    sent_at_lte: Optional[datetime] = None,
) -> Tuple[List[Message], int]:
    """
    Return (messages, total_count) for a member, ordered by sent_at DESC.

    Pagination
    ----------
    limit  : capped at MSG_MAX_LIMIT (200).  Default 50.
    offset : zero-based row offset.

    Filtering
    ---------
    sent_at_gte / sent_at_lte : optional UTC datetime bounds (inclusive).

    Ordering
    --------
    sent_at DESC, id DESC (stable tie-break for messages at the same second).
    """
    limit = min(max(1, limit), _MSG_MAX_LIMIT)
    offset = max(0, offset)

    base = select(Message).where(Message.member_id == member_id)
    if sent_at_gte is not None:
        base = base.where(Message.sent_at >= sent_at_gte)
    if sent_at_lte is not None:
        base = base.where(Message.sent_at <= sent_at_lte)

    total: int = session.execute(
        select(func.count()).select_from(base.subquery())
    ).scalar_one()

    messages = session.execute(
        base.order_by(Message.sent_at.desc(), Message.id.desc())
        .limit(limit)
        .offset(offset)
    ).scalars().all()

    return list(messages), total


# ---------------------------------------------------------------------------
# Profiles
# ---------------------------------------------------------------------------

_PROFILE_MAX_LIMIT = 100
_PROFILE_DEFAULT_LIMIT = 20


def get_profiles_by_member(
    session: Session,
    member_id: uuid.UUID,
    limit: int = _PROFILE_DEFAULT_LIMIT,
    offset: int = 0,
    profile_version: Optional[str] = None,
) -> Tuple[List[ProfileSnapshot], int]:
    """
    Return (snapshots, total_count) for a member, ordered by snapshot_at DESC.

    Pagination
    ----------
    limit  : capped at PROFILE_MAX_LIMIT (100).  Default 20.
    offset : zero-based row offset.

    Filtering
    ---------
    profile_version : optional exact-match filter.
    """
    limit = min(max(1, limit), _PROFILE_MAX_LIMIT)
    offset = max(0, offset)

    base = select(ProfileSnapshot).where(ProfileSnapshot.member_id == member_id)
    if profile_version is not None:
        base = base.where(ProfileSnapshot.profile_version == profile_version)

    total: int = session.execute(
        select(func.count()).select_from(base.subquery())
    ).scalar_one()

    snapshots = session.execute(
        base.order_by(ProfileSnapshot.snapshot_at.desc())
        .limit(limit)
        .offset(offset)
    ).scalars().all()

    return list(snapshots), total


def get_latest_profile(
    session: Session,
    member_id: uuid.UUID,
) -> Optional[ProfileSnapshot]:
    """Return the most recent ProfileSnapshot for a member, or None."""
    return session.execute(
        select(ProfileSnapshot)
        .where(ProfileSnapshot.member_id == member_id)
        .order_by(ProfileSnapshot.snapshot_at.desc())
        .limit(1)
    ).scalars().first()
