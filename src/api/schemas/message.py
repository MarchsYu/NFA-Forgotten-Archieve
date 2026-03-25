from __future__ import annotations

from datetime import datetime
from typing import Optional

from pydantic import BaseModel, ConfigDict


class MessageSchema(BaseModel):
    """Serialised representation of a Message row."""
    model_config = ConfigDict(from_attributes=True)

    id: int
    sent_at: datetime
    content: str
    normalized_content: Optional[str] = None
    content_type: str
    external_message_id: Optional[str] = None
    reply_to_message_id: Optional[int] = None
