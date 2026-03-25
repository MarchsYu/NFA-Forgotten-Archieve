from __future__ import annotations

import uuid
from datetime import datetime
from typing import Optional

from pydantic import BaseModel, ConfigDict


class MemberSchema(BaseModel):
    """Serialised representation of a Member row."""
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    group_id: uuid.UUID
    external_member_id: str
    display_name: str
    nickname: Optional[str] = None
    status: str
    joined_at: Optional[datetime] = None
    left_at: Optional[datetime] = None
    created_at: datetime

    # Optional: timestamp of the most recent profile snapshot
    latest_profile_snapshot_at: Optional[datetime] = None
