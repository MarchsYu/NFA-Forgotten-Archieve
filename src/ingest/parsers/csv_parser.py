"""
CSV chat log parser.

Expected format — CSV with header row:

    group_name,platform,external_group_id,user_name,external_member_id,content,timestamp,content_type,external_message_id,reply_to_external_message_id

Minimal required columns: group_name, user_name, content, timestamp
All other columns are optional; missing columns default to None / "text".

Rules:
- First row must be a header
- Delimiter: comma (default); can be overridden via constructor
- Encoding: UTF-8
- Rows with missing required fields are skipped with a warning
"""

from __future__ import annotations

import csv
import warnings
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

from src.ingest.models import ParsedMessage, normalize_text

_REQUIRED = {"group_name", "user_name", "content", "timestamp"}


class CSVParser:
    """Parse CSV chat logs into ParsedMessage list."""

    SUPPORTED_EXTENSIONS = {".csv"}

    def __init__(
        self,
        platform: str = "generic",
        delimiter: str = ",",
    ):
        """
        Args:
            platform: Default platform when the 'platform' column is absent.
            delimiter: CSV field delimiter (default: comma).
        """
        self.default_platform = platform
        self.delimiter = delimiter

    def parse(self, file_path: Path) -> List[ParsedMessage]:
        """
        Parse CSV file at *file_path*.

        Raises:
            FileNotFoundError: file does not exist.
            ValueError: header row is missing required columns.
        """
        if not file_path.exists():
            raise FileNotFoundError(f"CSV file not found: {file_path}")

        messages: List[ParsedMessage] = []
        skipped = 0

        with file_path.open(encoding="utf-8", newline="") as fh:
            reader = csv.DictReader(fh, delimiter=self.delimiter)

            # Validate header
            if reader.fieldnames is None:
                raise ValueError(f"CSV file is empty: {file_path}")
            header = {f.strip().lower() for f in reader.fieldnames}
            missing = _REQUIRED - header
            if missing:
                raise ValueError(
                    f"CSV {file_path.name} is missing required columns: {missing}"
                )

            for rownum, row in enumerate(reader, start=2):
                # Normalise keys to lowercase/stripped
                row = {k.strip().lower(): (v.strip() if v else "") for k, v in row.items()}

                # Check required values are non-empty
                empty = [f for f in _REQUIRED if not row.get(f)]
                if empty:
                    warnings.warn(
                        f"{file_path.name}:row {rownum}: skipping row with empty "
                        f"required field(s): {empty}"
                    )
                    skipped += 1
                    continue

                try:
                    timestamp = self._parse_timestamp(row["timestamp"])
                except ValueError as exc:
                    warnings.warn(
                        f"{file_path.name}:row {rownum}: skipping row with bad "
                        f"timestamp: {exc}"
                    )
                    skipped += 1
                    continue

                content = row["content"]
                platform = row.get("platform") or self.default_platform
                external_group_id = row.get("external_group_id") or None
                external_member_id = row.get("external_member_id") or None
                content_type = row.get("content_type") or "text"
                external_msg_id = row.get("external_message_id") or None
                reply_to_id = row.get("reply_to_external_message_id") or None

                messages.append(
                    ParsedMessage(
                        group_name=row["group_name"],
                        platform=platform,
                        external_group_id=external_group_id,
                        user_name=row["user_name"],
                        external_member_id=external_member_id,
                        content=content,
                        normalized_content=normalize_text(content),
                        timestamp=timestamp,
                        content_type=content_type,
                        external_message_id=external_msg_id,
                        reply_to_external_message_id=reply_to_id,
                        source_file=file_path.name,
                        raw_payload=dict(row),
                    )
                )

        if skipped:
            warnings.warn(f"{file_path.name}: {skipped} row(s) skipped.")

        return messages

    def _parse_timestamp(self, value: str) -> datetime:
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
                return datetime.strptime(value, fmt)
            except ValueError:
                continue
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            pass
        raise ValueError(f"Unable to parse timestamp: {value!r}")
