from dataclasses import dataclass
from datetime import datetime, timezone
from uuid import UUID

from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from src.db.models.legend_member import LegendMember
from src.legend import legend_repository


@dataclass
class ArchiveResult:
    member_id: UUID
    was_already_archived: bool
    legend_member: LegendMember


@dataclass
class RestoreResult:
    member_id: UUID
    legend_member: LegendMember


class LegendService:
    def __init__(self, session: Session):
        self.session = session

    def archive_member(self, member_id: UUID) -> ArchiveResult:
        existing = legend_repository.get_legend_member_by_member_id(self.session, member_id)
        
        if existing:
            if existing.archive_status == "archived":
                return ArchiveResult(
                    member_id=member_id,
                    was_already_archived=True,
                    legend_member=existing,
                )
            else:
                existing.archive_status = "archived"
                existing.archived_at = datetime.now(timezone.utc)
                existing.restored_at = None
                self.session.flush()
                return ArchiveResult(
                    member_id=member_id,
                    was_already_archived=False,
                    legend_member=existing,
                )
        
        legend_member = LegendMember(
            member_id=member_id,
            archive_status="archived",
            simulation_enabled=False,
            archived_at=datetime.now(timezone.utc),
        )
        
        try:
            legend_repository.create_legend_member(self.session, legend_member)
            return ArchiveResult(
                member_id=member_id,
                was_already_archived=False,
                legend_member=legend_member,
            )
        except IntegrityError:
            self.session.rollback()
            existing = legend_repository.get_legend_member_by_member_id(self.session, member_id)
            if existing and existing.archive_status == "archived":
                return ArchiveResult(
                    member_id=member_id,
                    was_already_archived=True,
                    legend_member=existing,
                )
            raise
