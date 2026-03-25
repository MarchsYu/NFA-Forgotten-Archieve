import uuid
from typing import List

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from src.api import deps
from src.api import repository as repo
from src.api.schemas.group import GroupSchema
from src.api.schemas.member import MemberSchema

router = APIRouter(prefix="/groups", tags=["groups"])


@router.get("", response_model=List[GroupSchema], summary="List all groups")
def list_groups(session: Session = Depends(deps.get_db)) -> List[GroupSchema]:
    """
    Return all groups with member_count and message_count aggregates.
    """
    rows = repo.get_groups(session)
    result = []
    for group, member_count, message_count in rows:
        schema = GroupSchema.model_validate(group)
        schema.member_count = member_count
        schema.message_count = message_count
        result.append(schema)
    return result


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
    response_model=List[MemberSchema],
    summary="List members in a group",
)
def list_group_members(
    group_id: uuid.UUID,
    session: Session = Depends(deps.get_db),
) -> List[MemberSchema]:
    """
    Return all members belonging to the specified group,
    each annotated with their latest profile snapshot timestamp.
    """
    group = repo.get_group_by_id(session, group_id)
    if group is None:
        raise HTTPException(status_code=404, detail=f"Group {group_id} not found")

    rows = repo.get_members_by_group(session, group_id)
    result = []
    for member, latest_snap in rows:
        schema = MemberSchema.model_validate(member)
        schema.latest_profile_snapshot_at = latest_snap
        result.append(schema)
    return result
