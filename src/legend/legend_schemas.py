"""
Legend Archive schemas – internal DTOs and API/CLI response shapes.

These are plain dataclasses / Pydantic models with no DB dependency,
so they can be imported freely without triggering engine initialisation.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional

from pydantic import BaseModel, ConfigDict


# ---------------------------------------------------------------------------
# Internal result objects (dataclasses – no Pydantic overhead)
# ---------------------------------------------------------------------------

@dataclass
class ArchiveResult:
    """Outcome of an archive_member() call."""
    legend_member_id: uuid.UUID
    member_id: uuid.UUID
    archive_status: str          # "archived"
    was_already_archived: bool   # True → idempotent no-op
    profile_snapshot_id: Optional[uuid.UUID] = None


@dataclass
class RestoreResult:
    """Outcome of a restore_member() call."""
    legend_member_id: uuid.UUID
    member_id: uuid.UUID
    archive_status: str          # "restored"


@dataclass
class SimulationToggleResult:
    """Outcome of enable/disable_simulation() calls."""
    legend_member_id: uuid.UUID
    member_id: uuid.UUID
    simulation_enabled: bool


# ---------------------------------------------------------------------------
# Pydantic schemas – API responses and CLI display
# ---------------------------------------------------------------------------

class LegendMemberSchema(BaseModel):
    """Serialised representation of a LegendMember row."""
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    member_id: uuid.UUID
    group_id: uuid.UUID
    archive_status: str
    archived_at: datetime
    archived_reason: Optional[str] = None
    archived_by: Optional[str] = None
    source_profile_snapshot_id: Optional[uuid.UUID] = None
    member_display_name_snapshot: str
    member_external_id_snapshot: Optional[str] = None
    member_status_snapshot: Optional[str] = None
    simulation_enabled: bool
    created_at: datetime
    updated_at: datetime


class ArchiveRequest(BaseModel):
    """Request body for POST /legend/members/{member_id}/archive."""
    archived_reason: Optional[str] = None
    archived_by: Optional[str] = None
    force: bool = False


class LegendMemberListSchema(BaseModel):
    """Paged list of legend members."""
    items: List[LegendMemberSchema]
    total: int
    limit: int
    offset: int
