import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from src.api import deps
from src.api import repository as repo
from src.api.schemas.common import PagedResponse
from src.api.schemas.group import GroupSchema
from src.api.schemas.member import MemberSchema

router = APIRouter(prefix="/groups", tags=["groups"])

# ── Pagination defaults / caps ──────────────────────────────────────────────
_GROUP_DEFAULT_LIMIT = 100
_GROUP_MAX_LIMIT = 500
_MEMBER_DEFAULT_LIMIT = 100
_MEMBER_MAX_LIMIT = 500


@router.get("", response_model=PagedResponse[GroupSchema], summary="List all groups")
def list_groups(
    limit: Annotated[int, Query(ge=1, le=_GROUP_MAX_LIMIT, description="Page size (max 500)")] = _GROUP_DEFAULT_LIMIT,
    offset: Annotated[int, Query(ge=0, description="Zero-based row offset")] = 0,
    session: Session = Depends(deps.get_db),
) -> PagedResponse[GroupSchema]:
    """
    Return all groups with member_count and message_count aggregates.

    **Pagination**: limit (default 100, max 500) + offset.
    """
    rows, total = repo.get_groups(session, limit=limit, offset=offset)
    result = []
    for group, member_count, message_count in rows:
        schema = GroupSchema.model_validate(group)
        schema.member_count = member_count
        schema.message_count = message_count
        result.append(schema)
    return PagedResponse(items=result, total=total, limit=limit, offset=offset)


@router.get("/{group_id}", response_model=GroupSchema, summary="Get group by ID")
def get_group(
    group_id: uuid.UUID,
    session: Session = Depends(deps.get_db),
) -> GroupSchema:
    """Return a single group by UUID."""
    group = repo.get_group_by_id(session, group_id)
    if group is None:
        raise HTTPException(status_code=404, detail=f"Group {group_id} not found")
    return GroupSchema.model_validate(group)


@router.get(
    "/{group_id}/members",
    response_model=PagedResponse[MemberSchema],
    summary="List members in a group",
)
def list_group_members(
    group_id: uuid.UUID,
    limit: Annotated[int, Query(ge=1, le=_MEMBER_MAX_LIMIT, description="Page size (max 500)")] = _MEMBER_DEFAULT_LIMIT,
    offset: Annotated[int, Query(ge=0, description="Zero-based row offset")] = 0,
    session: Session = Depends(deps.get_db),
) -> PagedResponse[MemberSchema]:
    """
    Return members belonging to the specified group,
    each annotated with their latest profile snapshot timestamp.

    **Pagination**: limit (default 100, max 500) + offset.
    """
    group = repo.get_group_by_id(session, group_id)
    if group is None:
        raise HTTPException(status_code=404, detail=f"Group {group_id} not found")

    rows, total = repo.get_members_by_group(session, group_id, limit=limit, offset=offset)
    result = []
    for member, latest_snap in rows:
        schema = MemberSchema.model_validate(member)
        schema.latest_profile_snapshot_at = latest_snap
        result.append(schema)
    return PagedResponse(items=result, total=total, limit=limit, offset=offset)
