import uuid
from datetime import datetime
from typing import Annotated, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from src.api import deps
from src.api import repository as repo
from src.api.schemas.common import PagedResponse
from src.api.schemas.member import MemberSchema
from src.api.schemas.message import MessageSchema
from src.api.schemas.profile import ProfileSnapshotSchema

router = APIRouter(prefix="/members", tags=["members"])

# ── Pagination defaults / caps (also documented in repository.py) ──────────
_MSG_DEFAULT_LIMIT = 50
_MSG_MAX_LIMIT = 200
_PROFILE_DEFAULT_LIMIT = 20
_PROFILE_MAX_LIMIT = 100


def _member_or_404(session: Session, member_id: uuid.UUID) -> MemberSchema:
    """Fetch member + latest_snapshot_at, raise 404 if absent."""
    row = repo.get_member_by_id(session, member_id)
    if row is None:
        raise HTTPException(status_code=404, detail=f"Member {member_id} not found")
    member, latest_snap = row
    schema = MemberSchema.model_validate(member)
    schema.latest_profile_snapshot_at = latest_snap
    return schema


# ---------------------------------------------------------------------------
# GET /members/{member_id}
# ---------------------------------------------------------------------------

@router.get(
    "/{member_id}",
    response_model=MemberSchema,
    summary="Get member by ID",
)
def get_member(
    member_id: uuid.UUID,
    session: Session = Depends(deps.get_db),
) -> MemberSchema:
    """Return a single member with their latest profile snapshot timestamp."""
    return _member_or_404(session, member_id)


# ---------------------------------------------------------------------------
# GET /members/{member_id}/messages
# ---------------------------------------------------------------------------

@router.get(
    "/{member_id}/messages",
    response_model=PagedResponse[MessageSchema],
    summary="List messages for a member",
)
def list_member_messages(
    member_id: uuid.UUID,
    limit: Annotated[int, Query(ge=1, le=_MSG_MAX_LIMIT, description="Page size (max 200)")] = _MSG_DEFAULT_LIMIT,
    offset: Annotated[int, Query(ge=0, description="Zero-based row offset")] = 0,
    sent_at_gte: Annotated[Optional[datetime], Query(description="Filter: sent_at >= (ISO-8601 UTC)")] = None,
    sent_at_lte: Annotated[Optional[datetime], Query(description="Filter: sent_at <= (ISO-8601 UTC)")] = None,
    session: Session = Depends(deps.get_db),
) -> PagedResponse[MessageSchema]:
    """
    Return a paginated list of messages for the specified member.

    **Ordering**: sent_at DESC, id DESC (newest first; id breaks ties).

    **Pagination**: limit (default 50, max 200) + offset.

    **Filtering**: optional sent_at_gte / sent_at_lte for time-range queries.
    """
    # Verify member exists
    row = repo.get_member_by_id(session, member_id)
    if row is None:
        raise HTTPException(status_code=404, detail=f"Member {member_id} not found")

    messages, total = repo.get_messages_by_member(
        session, member_id,
        limit=limit, offset=offset,
        sent_at_gte=sent_at_gte, sent_at_lte=sent_at_lte,
    )
    return PagedResponse(
        items=[MessageSchema.model_validate(m) for m in messages],
        total=total,
        limit=limit,
        offset=offset,
    )


# ---------------------------------------------------------------------------
# GET /members/{member_id}/profile/latest
# ---------------------------------------------------------------------------

@router.get(
    "/{member_id}/profile/latest",
    response_model=ProfileSnapshotSchema,
    summary="Get latest Persona Profile snapshot for a member",
)
def get_latest_profile(
    member_id: uuid.UUID,
    session: Session = Depends(deps.get_db),
) -> ProfileSnapshotSchema:
    """
    Return the most recent ProfileSnapshot for the member
    (ordered by snapshot_at DESC).

    Returns 404 if the member does not exist or has no profile snapshots.
    """
    row = repo.get_member_by_id(session, member_id)
    if row is None:
        raise HTTPException(status_code=404, detail=f"Member {member_id} not found")

    snapshot = repo.get_latest_profile(session, member_id)
    if snapshot is None:
        raise HTTPException(
            status_code=404,
            detail=f"No profile snapshots found for member {member_id}",
        )
    return ProfileSnapshotSchema.model_validate(snapshot)


# ---------------------------------------------------------------------------
# GET /members/{member_id}/profiles
# ---------------------------------------------------------------------------

@router.get(
    "/{member_id}/profiles",
    response_model=PagedResponse[ProfileSnapshotSchema],
    summary="List Persona Profile snapshots for a member",
)
def list_member_profiles(
    member_id: uuid.UUID,
    limit: Annotated[int, Query(ge=1, le=_PROFILE_MAX_LIMIT, description="Page size (max 100)")] = _PROFILE_DEFAULT_LIMIT,
    offset: Annotated[int, Query(ge=0, description="Zero-based row offset")] = 0,
    profile_version: Annotated[Optional[str], Query(description="Filter by profile_version (e.g. profile_v1)")] = None,
    session: Session = Depends(deps.get_db),
) -> PagedResponse[ProfileSnapshotSchema]:
    """
    Return a paginated list of ProfileSnapshot rows for the member.

    **Ordering**: snapshot_at DESC (newest first).

    **Pagination**: limit (default 20, max 100) + offset.

    **Filtering**: optional profile_version for exact-match filter.
    """
    row = repo.get_member_by_id(session, member_id)
    if row is None:
        raise HTTPException(status_code=404, detail=f"Member {member_id} not found")

    snapshots, total = repo.get_profiles_by_member(
        session, member_id,
        limit=limit, offset=offset,
        profile_version=profile_version,
    )
    return PagedResponse(
        items=[ProfileSnapshotSchema.model_validate(s) for s in snapshots],
        total=total,
        limit=limit,
        offset=offset,
    )
