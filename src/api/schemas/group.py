from __future__ import annotations

import uuid
from datetime import datetime
from typing import Optional

from pydantic import BaseModel, ConfigDict


class GroupSchema(BaseModel):
    """Serialised representation of a Group row."""
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    platform: str
    external_group_id: str
    name: str
    created_at: datetime

    # Optional aggregates (populated by the repository when cheap to compute)
    member_count: Optional[int] = None
    message_count: Optional[int] = None
