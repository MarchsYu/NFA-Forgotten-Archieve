"""
TXT chat log parser.

Expected format — one message per line:

    [2024-01-15 14:30:00] Alice: Hello everyone!
    [2024-01-15 14:31:05] Bob: Hi Alice!

Rules:
- Line format: [TIMESTAMP] SENDER: CONTENT
- Timestamp must be parseable (ISO-like or common formats)
- Lines that do not match the pattern are skipped with a warning
- group_name and platform are supplied at parse time (not in the file)
- external_message_id is not available in TXT format → None
- external_member_id is not available in TXT format → None
"""

from __future__ import annotations

import re
import warnings
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional

from src.ingest.models import ParsedMessage, normalize_text

# Matches: [2024-01-15 14:30:00] Alice: Hello world
_LINE_RE = re.compile(
    r"^\[(?P<ts>[^\]]+)\]\s+(?P<sender>[^:]+?):\s*(?P<content>.+)$"
)


class TXTParser:
    """Parse line-based TXT chat logs into ParsedMessage list."""

    SUPPORTED_EXTENSIONS = {".txt"}

    def __init__(
        self,
        group_name: str,
        platform: str = "generic",
        external_group_id: Optional[str] = None,
    ):
        """
        Args:
            group_name: Name of the group this file belongs to.
            platform: Platform identifier (e.g. "wechat", "telegram").
            external_group_id: Optional platform-assigned group ID.
        """
        self.group_name = group_name
        self.platform = platform
        self.external_group_id = external_group_id

    def parse(self, file_path: Path) -> List[ParsedMessage]:
        """
        Parse TXT file at *file_path*.

        Raises:
            FileNotFoundError: file does not exist.
        """
        if not file_path.exists():
            raise FileNotFoundError(f"TXT file not found: {file_path}")

        messages: List[ParsedMessage] = []
        skipped = 0

        for lineno, line in enumerate(
            file_path.read_text(encoding="utf-8").splitlines(), start=1
        ):
            line = line.strip()
            if not line:
                continue

            match = _LINE_RE.match(line)
            if not match:
                warnings.warn(
                    f"{file_path.name}:{lineno}: skipping unrecognised line: {line!r}"
                )
                skipped += 1
                continue

            ts_str = match.group("ts").strip()
            sender = match.group("sender").strip()
            content = match.group("content").strip()

            try:
                timestamp = self._parse_timestamp(ts_str)
            except ValueError as exc:
                warnings.warn(
                    f"{file_path.name}:{lineno}: skipping line with bad timestamp: {exc}"
                )
                skipped += 1
                continue

            messages.append(
                ParsedMessage(
                    group_name=self.group_name,
                    platform=self.platform,
                    external_group_id=self.external_group_id,
                    user_name=sender,
                    external_member_id=None,
                    content=content,
                    normalized_content=normalize_text(content),
                    timestamp=timestamp,
                    content_type="text",
                    external_message_id=None,
                    reply_to_external_message_id=None,
                    source_file=file_path.name,
                    raw_payload={"raw_line": line, "lineno": lineno},
                )
            )

        if skipped:
            warnings.warn(
                f"{file_path.name}: {skipped} line(s) skipped due to parse errors."
            )

        return messages

    def _parse_timestamp(self, value: str) -> datetime:
        """Parse timestamp string; all naive results are treated as UTC."""
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
        # Try ISO 8601 last
        try:
            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except ValueError:
            pass
        raise ValueError(f"Unable to parse timestamp: {value!r}")
