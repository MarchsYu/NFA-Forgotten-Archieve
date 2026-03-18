"""
JSON chat log parser.

Expected format (minimal viable example):

    [
        {
            "group_name": "Family Chat",
            "platform": "wechat",
            "external_group_id": "wx_12345",
            "user_name": "Alice",
            "external_member_id": "wx_u_001",
            "content": "Hello everyone!",
            "timestamp": "2024-01-15T14:30:00+08:00",
            "content_type": "text",
            "external_message_id": "wx_msg_001",
            "reply_to_external_message_id": null
        },
        ...
    ]

Field mapping rules:
- Required: group_name, user_name, content, timestamp
- Optional: all other fields default to None / "generic"
- Missing timestamp → parser raises ValueError
- Invalid JSON → parser raises JSONDecodeError with context
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from src.ingest.models import ParsedMessage, normalize_text


class JSONParser:
    """Parse JSON array of message objects into ParsedMessage list."""

    SUPPORTED_EXTENSIONS = {".json"}

    def __init__(self, platform: Optional[str] = None):
        """
        Args:
            platform: Default platform name if not present in message object.
        """
        self.default_platform = platform or "generic"

    def parse(self, file_path: Path) -> List[ParsedMessage]:
        """
        Parse JSON file at *file_path*.

        Raises:
            FileNotFoundError: file does not exist.
            json.JSONDecodeError: invalid JSON syntax.
            ValueError: missing required fields or malformed timestamp.
        """
        if not file_path.exists():
            raise FileNotFoundError(f"JSON file not found: {file_path}")

        raw_text = file_path.read_text(encoding="utf-8")
        try:
            data = json.loads(raw_text)
        except json.JSONDecodeError as exc:
            raise json.JSONDecodeError(
                f"Invalid JSON in {file_path}: {exc.msg}",
                exc.doc,
                exc.pos,
            ) from exc

        if not isinstance(data, list):
            raise ValueError(
                f"JSON root must be an array of messages, got {type(data).__name__}"
            )

        return [self._convert(obj, file_path.name) for obj in data]

    def _convert(self, obj: Dict[str, Any], source_file: str) -> ParsedMessage:
        """Convert a single JSON object to ParsedMessage."""
        # ── Required fields ─────────────────────────────────────────────────────
        group_name = obj.get("group_name") or obj.get("group")
        if not group_name:
            raise ValueError(f"Missing required field 'group_name' in object: {obj}")

        user_name = obj.get("user_name") or obj.get("sender") or obj.get("from")
        if not user_name:
            raise ValueError(f"Missing required field 'user_name' in object: {obj}")

        content = obj.get("content") or obj.get("message") or obj.get("text")
        if content is None:
            raise ValueError(f"Missing required field 'content' in object: {obj}")

        ts_raw = obj.get("timestamp") or obj.get("time") or obj.get("date")
        if not ts_raw:
            raise ValueError(f"Missing required field 'timestamp' in object: {obj}")
        timestamp = self._parse_timestamp(ts_raw)

        # ── Optional fields ─────────────────────────────────────────────────────
        platform = obj.get("platform") or self.default_platform
        external_group_id = obj.get("external_group_id") or obj.get("group_id")
        external_member_id = obj.get("external_member_id") or obj.get("user_id")
        content_type = obj.get("content_type") or "text"
        external_msg_id = obj.get("external_message_id") or obj.get("message_id")
        reply_to_id = obj.get("reply_to_external_message_id") or obj.get("reply_to_id")

        return ParsedMessage(
            group_name=str(group_name),
            platform=str(platform),
            external_group_id=str(external_group_id) if external_group_id else None,
            user_name=str(user_name),
            external_member_id=str(external_member_id) if external_member_id else None,
            content=str(content),
            normalized_content=normalize_text(str(content)),
            timestamp=timestamp,
            content_type=str(content_type),
            external_message_id=str(external_msg_id) if external_msg_id else None,
            reply_to_external_message_id=str(reply_to_id) if reply_to_id else None,
            source_file=source_file,
            raw_payload=obj,
        )

    def _parse_timestamp(self, value: Any) -> datetime:
        """Parse ISO 8601 or Unix timestamp into UTC-aware datetime.

        Rule: all naive datetimes are treated as UTC.
        """
        if isinstance(value, (int, float)):
            # Explicit UTC for numeric epoch timestamps
            return datetime.fromtimestamp(value, tz=timezone.utc)

        if isinstance(value, str):
            try:
                dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt
            except ValueError:
                pass

            formats = [
                "%Y-%m-%d %H:%M:%S",
                "%Y-%m-%d %H:%M",
                "%Y/%m/%d %H:%M:%S",
                "%Y/%m/%d %H:%M",
                "%d/%m/%Y %H:%M:%S",
                "%d/%m/%Y %H:%M",
            ]
            for fmt in formats:
                try:
                    dt = datetime.strptime(value, fmt)
                    return dt.replace(tzinfo=timezone.utc)
                except ValueError:
                    continue

        raise ValueError(f"Unable to parse timestamp: {value!r}")
