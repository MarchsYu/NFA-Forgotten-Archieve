"""
Simulation schemas – internal DTOs and result shapes.

Plain dataclasses and Pydantic models with no DB dependency.
Import freely without triggering engine initialisation.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field

SIMULATION_VERSION = "sim_v1"


# ---------------------------------------------------------------------------
# Pydantic – request (external input, validated at CLI / API boundary)
# ---------------------------------------------------------------------------

class SimulationRequest(BaseModel):
    """Input from the caller: who to simulate and what to say to them."""
    legend_member_id: uuid.UUID
    current_message: str = Field(..., min_length=1)
    conversation_context: Optional[str] = None   # optional prior turns as plain text
    history_limit: int = Field(default=10, ge=1, le=20)


# ---------------------------------------------------------------------------
# Dataclasses – internal pipeline objects (no Pydantic overhead)
# ---------------------------------------------------------------------------

@dataclass
class HistoricalMessage:
    """A single sampled message from the member's history."""
    sent_at: datetime
    content: str
    normalized_content: Optional[str] = None


@dataclass
class SimulationInput:
    """
    Fully assembled input ready for prompt building.

    Produced by SimulationService after loading all necessary data from DB.
    Passed to prompt_builder.build_prompt() – no further DB access required.
    """
    legend_member_id: uuid.UUID
    member_id: uuid.UUID
    display_name: str
    profile_snapshot_id: uuid.UUID
    persona_summary: Optional[str]
    traits: Dict[str, Any]
    stats: Dict[str, Any]
    historical_messages: List[HistoricalMessage]
    current_message: str
    conversation_context: Optional[str]
    # Filled in by prompt_builder:
    prompt_messages: List[Dict[str, str]] = field(default_factory=list)


@dataclass
class SimulationResult:
    """Outcome of a single simulation round."""
    legend_member_id: uuid.UUID
    profile_snapshot_id: uuid.UUID
    prompt_messages: List[Dict[str, str]]
    response_text: str
    simulation_version: str = SIMULATION_VERSION
