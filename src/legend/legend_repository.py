from typing import Optional
from uuid import UUID

from sqlalchemy import select, func
from sqlalchemy.orm import Session

from src.db.models.legend_member import LegendMember


def get_legend_member_by_member_id(session: Session, member_id: UUID) -> Optional[LegendMember]:
    stmt = select(LegendMember).where(LegendMember.member_id == member_id)
    return session.execute(stmt).scalar_one_or_none()


def create_legend_member(session: Session, legend_member: LegendMember) -> LegendMember:
    session.add(legend_member)
    session.flush()
    return legend_member


def list_legend_members(
    session: Session,
    archive_status: Optional[str] = None,
    limit: int = 100,
    offset: int = 0,
) -> tuple[list[LegendMember], int]:
    stmt = select(LegendMember)
    
    if archive_status:
        stmt = stmt.where(LegendMember.archive_status == archive_status)
    
    count_stmt = select(func.count()).select_from(stmt.subquery())
    total = session.execute(count_stmt).scalar_one()
    
    stmt = stmt.order_by(LegendMember.archived_at.desc(), LegendMember.id.desc())
    stmt = stmt.limit(limit).offset(offset)
    
    results = session.execute(stmt).scalars().all()
    return list(results), total
