"""
Unified intermediate message structure used by all parsers.

All parsers produce a list of ParsedMessage objects. The ingest service
consumes this list and writes to the database. Nothing outside the ingest
module should depend on this structure directly.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, Optional


@dataclass
class ParsedMessage:
    """
    Canonical in-memory representation of a single chat message after parsing.

    Fields map directly to the messages / groups / members DB schema.
    Parsers must populate every non-optional field; optional fields may be
    left as None when the source format does not provide them.
    """

    # ── Group identity ────────────────────────────────────────────────────────
    group_name: str
    platform: str                          # e.g. "wechat", "telegram", "generic"
    external_group_id: Optional[str]       # platform-assigned group ID; None if absent

    # ── Member identity ───────────────────────────────────────────────────────
    user_name: str                         # display name used in the source file
    external_member_id: Optional[str]      # platform-assigned user ID; None if absent

    # ── Message body ──────────────────────────────────────────────────────────
    content: str
    normalized_content: str               # trimmed + whitespace-normalised content
    timestamp: datetime                   # timezone-aware preferred; naive accepted
    content_type: str = "text"            # "text" | "image" | "file" | "sticker" …

    # ── Linkage ───────────────────────────────────────────────────────────────
    external_message_id: Optional[str] = None
    reply_to_external_message_id: Optional[str] = None

    # ── Provenance ────────────────────────────────────────────────────────────
    source_file: Optional[str] = None
    raw_payload: Dict[str, Any] = field(default_factory=dict)


def normalize_text(text: str) -> str:
    """Minimal content normalisation: strip edges, collapse internal whitespace."""
    import re
    return re.sub(r"\s+", " ", text).strip()
