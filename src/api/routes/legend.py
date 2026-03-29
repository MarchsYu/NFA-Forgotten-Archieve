"""
Legend Archive API routes.

Endpoints
---------
GET  /legend/members                              List legend members (paged)
GET  /legend/members/{member_id}                  Get single legend member
POST /legend/members/{member_id}/archive          Archive a member
POST /legend/members/{member_id}/restore          Restore an archived member
POST /legend/members/{member_id}/enable-simulation
POST /legend/members/{member_id}/disable-simulation
"""

from __future__ import annotations

import uuid
from typing import Annotated, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from src.api import deps
from src.legend.archive_policy import ArchiveNotEligibleError, InvalidStatusTransitionError
from src.legend.legend_schemas import (
    ArchiveRequest,
    LegendMemberListSchema,
    LegendMemberSchema,
)
from src.legend.legend_service import LegendService

router = APIRouter(prefix="/legend", tags=["legend"])

_DEFAULT_LIMIT = 50
_MAX_LIMIT = 500


def _get_service(session: Session = Depends(deps.get_db)) -> LegendService:
    return LegendService(db_session=session)


def _commit(service: LegendService) -> None:
    """Commit the service's session (used after write operations in API routes)."""
    if service._session is not None:
        service._session.commit()


def _legend_or_404(service: LegendService, member_id: uuid.UUID) -> LegendMemberSchema:
    schema = service.get_legend_member(member_id)
    if schema is None:
        raise HTTPException(status_code=404, detail=f"Legend member {member_id} not found")
    return schema


# ---------------------------------------------------------------------------
# GET /legend/members
# ---------------------------------------------------------------------------

@router.get(
    "/members",
    response_model=LegendMemberListSchema,
    summary="List legend members",
)
def list_legend_members(
    group_id: Optional[uuid.UUID] = Query(default=None, description="Filter by group UUID"),
    archive_status: Optional[str] = Query(default=None, description="'archived' or 'restored'"),
    simulation_enabled: Optional[bool] = Query(default=None, description="Filter by simulation gate"),
    limit: Annotated[int, Query(ge=1, le=_MAX_LIMIT)] = _DEFAULT_LIMIT,
    offset: Annotated[int, Query(ge=0)] = 0,
    service: LegendService = Depends(_get_service),
) -> LegendMemberListSchema:
    schemas, total = service.list_legend_members(
        group_id=group_id,
        archive_status=archive_status,
        simulation_enabled=simulation_enabled,
        limit=limit,
        offset=offset,
    )
    return LegendMemberListSchema(items=schemas, total=total, limit=limit, offset=offset)


# ---------------------------------------------------------------------------
# GET /legend/members/{member_id}
# ---------------------------------------------------------------------------

@router.get(
    "/members/{member_id}",
    response_model=LegendMemberSchema,
    summary="Get a legend member by member_id",
)
def get_legend_member(
    member_id: uuid.UUID,
    service: LegendService = Depends(_get_service),
) -> LegendMemberSchema:
    return _legend_or_404(service, member_id)


# ---------------------------------------------------------------------------
# POST /legend/members/{member_id}/archive
# ---------------------------------------------------------------------------

@router.post(
    "/members/{member_id}/archive",
    response_model=LegendMemberSchema,
    summary="Archive a member",
    status_code=200,
)
def archive_member(
    member_id: uuid.UUID,
    body: ArchiveRequest = ArchiveRequest(),
    service: LegendService = Depends(_get_service),
) -> LegendMemberSchema:
    try:
        service.archive_member(
            member_id=member_id,
            archived_reason=body.archived_reason,
            archived_by=body.archived_by,
            force=body.force,
        )
    except ArchiveNotEligibleError as exc:
        raise HTTPException(status_code=422, detail=str(exc))
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    _commit(service)
    return _legend_or_404(service, member_id)


# ---------------------------------------------------------------------------
# POST /legend/members/{member_id}/restore
# ---------------------------------------------------------------------------

@router.post(
    "/members/{member_id}/restore",
    response_model=LegendMemberSchema,
    summary="Restore an archived member",
    status_code=200,
)
def restore_member(
    member_id: uuid.UUID,
    service: LegendService = Depends(_get_service),
) -> LegendMemberSchema:
    try:
        service.restore_member(member_id)
    except InvalidStatusTransitionError as exc:
        raise HTTPException(status_code=422, detail=str(exc))
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    _commit(service)
    return _legend_or_404(service, member_id)


# ---------------------------------------------------------------------------
# POST /legend/members/{member_id}/enable-simulation
# ---------------------------------------------------------------------------

@router.post(
    "/members/{member_id}/enable-simulation",
    response_model=LegendMemberSchema,
    summary="Enable Persona Simulation for a legend member",
    status_code=200,
)
def enable_simulation(
    member_id: uuid.UUID,
    service: LegendService = Depends(_get_service),
) -> LegendMemberSchema:
    try:
        service.enable_simulation(member_id)
    except InvalidStatusTransitionError as exc:
        raise HTTPException(status_code=422, detail=str(exc))
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    _commit(service)
    return _legend_or_404(service, member_id)


# ---------------------------------------------------------------------------
# POST /legend/members/{member_id}/disable-simulation
# ---------------------------------------------------------------------------

@router.post(
    "/members/{member_id}/disable-simulation",
    response_model=LegendMemberSchema,
    summary="Disable Persona Simulation for a legend member",
    status_code=200,
)
def disable_simulation(
    member_id: uuid.UUID,
    service: LegendService = Depends(_get_service),
) -> LegendMemberSchema:
    try:
        service.disable_simulation(member_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    _commit(service)
    return _legend_or_404(service, member_id)
