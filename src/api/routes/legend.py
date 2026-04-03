from typing import Literal

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from src.api.deps import get_db
from src.api.schemas.common import PagedResponse
from src.api.schemas.legend import (
    LegendMemberSchema,
    ArchiveMemberRequest,
    ArchiveMemberResponse,
)
from src.legend.legend_service import LegendService
from src.legend import legend_repository

router = APIRouter(prefix="/legend", tags=["legend"])


@router.post("/archive", response_model=ArchiveMemberResponse)
def archive_member(
    request: ArchiveMemberRequest,
    db: Session = Depends(get_db),
):
    service = LegendService(db)
    result = service.archive_member(request.member_id)
    db.commit()
    
    return ArchiveMemberResponse(
        member_id=result.member_id,
        was_already_archived=result.was_already_archived,
        legend_member=LegendMemberSchema.model_validate(result.legend_member),
    )


@router.get("/members", response_model=PagedResponse[LegendMemberSchema])
def list_legend_members(
    archive_status: Literal["archived", "restored"] | None = Query(None),
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db),
):
    members, total = legend_repository.list_legend_members(
        db,
        archive_status=archive_status,
        limit=limit,
        offset=offset,
    )
    
    return PagedResponse(
        items=[LegendMemberSchema.model_validate(m) for m in members],
        total=total,
        limit=limit,
        offset=offset,
    )
