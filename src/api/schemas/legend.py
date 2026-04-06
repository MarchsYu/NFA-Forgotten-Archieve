from datetime import datetime
from uuid import UUID

from pydantic import BaseModel


class LegendMemberSchema(BaseModel):
    id: UUID
    member_id: UUID
    archive_status: str
    simulation_enabled: bool
    archived_at: datetime
    restored_at: datetime | None

    class Config:
        from_attributes = True


class ArchiveMemberRequest(BaseModel):
    member_id: UUID


class ArchiveMemberResponse(BaseModel):
    member_id: UUID
    was_already_archived: bool
    legend_member: LegendMemberSchema
