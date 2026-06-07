"""
Simulation repository – read-only DB queries for Persona Simulation.

No writes, no side-effects.  All functions accept a SQLAlchemy Session
and return ORM objects or plain Python values.

Queries
-------
get_legend_member_by_id        – load a LegendMember by its own PK
get_profile_snapshot_by_id     – load a ProfileSnapshot by its PK (exact anchor)
sample_member_messages         – deterministic message sample for a member + window
"""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import List, Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from src.db.models import LegendMember, Message, ProfileSnapshot


def get_legend_member_by_id(
    session: Session,
    legend_member_id: uuid.UUID,
) -> Optional[LegendMember]:
    """Return the LegendMember row by its own PK, or None."""
    return session.get(LegendMember, legend_member_id)


def get_profile_snapshot_by_id(
    session: Session,
    profile_snapshot_id: uuid.UUID,
) -> Optional[ProfileSnapshot]:
    """
    Return the ProfileSnapshot row by its own PK, or None.

    This is the ONLY correct way to load a profile for simulation.
    It must be called with legend_member.source_profile_snapshot_id so that
    the profile anchored at archive time is used – not the member's latest
    profile, which may reflect a different time window or algorithm version.
    """
    return session.get(ProfileSnapshot, profile_snapshot_id)


def sample_member_messages(
    session: Session,
    member_id: uuid.UUID,
    window_start: datetime,
    window_end: datetime,
    limit: int = 10,
) -> List[Message]:
    """
    Return up to *limit* messages for *member_id* within [window_start, window_end].

    Ordering: sent_at DESC, id DESC (most-recent first, stable tie-break).
    Limit:    clamped to [1, 20].
    Returns:  empty list (not an error) when no messages exist in the window.
    """
    limit = min(max(1, limit), 20)

    stmt = (
        select(Message)
        .where(
            Message.member_id == member_id,
            Message.sent_at >= window_start,
            Message.sent_at <= window_end,
        )
        .order_by(Message.sent_at.desc(), Message.id.desc())
        .limit(limit)
    )
    return list(session.execute(stmt).scalars().all())
