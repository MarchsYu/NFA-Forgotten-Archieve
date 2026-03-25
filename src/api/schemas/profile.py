from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any, Dict, Optional

from pydantic import BaseModel, ConfigDict


class ProfileSnapshotSchema(BaseModel):
    """Serialised representation of a ProfileSnapshot row."""
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    member_id: uuid.UUID
    group_id: uuid.UUID
    profile_version: str
    snapshot_at: datetime
    window_start: datetime
    window_end: datetime
    source_message_count: int
    persona_summary: Optional[str] = None
    traits: Optional[Dict[str, Any]] = None
    stats: Optional[Dict[str, Any]] = None
