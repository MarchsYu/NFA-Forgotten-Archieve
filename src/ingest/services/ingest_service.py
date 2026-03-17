"""
Ingest service – orchestrates file parsing and database persistence.

Responsibilities:
1. Select parser based on file extension
2. Parse file into ParsedMessage list
3. Get-or-create Group and Member records
4. Upsert messages with idempotency guarantees

Idempotency strategy:
- If external_message_id is present → use (group_id, external_message_id) unique constraint
  to avoid duplicates. The DB partial unique index rejects duplicates at INSERT time.
- If external_message_id is absent → we allow duplicates (no reliable key available).
  A warning is emitted for each such message so the operator knows.

This is the minimal viable strategy for MVP. Future improvements could include:
- Weak deduplication via (group_id, member_id, sent_at, content_hash)
- Content-hash based upsert for messages without platform IDs
"""

from __future__ import annotations

import uuid
import warnings
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from sqlalchemy import select, insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from src.db.models import Group, Member, Message
from src.db.session import SessionLocal
from src.ingest.models import ParsedMessage
from src.ingest.parsers.json_parser import JSONParser
from src.ingest.parsers.txt_parser import TXTParser
from src.ingest.parsers.csv_parser import CSVParser


@dataclass
class IngestResult:
    """Summary of an ingest operation."""

    group_id: uuid.UUID
    messages_inserted: int
    messages_skipped_duplicate: int
    messages_without_external_id: int
    members_created: int
    members_reused: int


class IngestService:
    """
    High-level ingest orchestrator.

    Usage:
        service = IngestService()
        result = service.ingest_file(Path("chat.json"))
    """

    # Mapping of extension -> parser class (not instance)
    _PARSERS = {
        ".json": JSONParser,
        ".txt": TXTParser,
        ".csv": CSVParser,
    }

    def __init__(self, db_session: Optional[Session] = None):
        """
        Args:
            db_session: SQLAlchemy session. If None, a new SessionLocal() is created
                        and committed/closed automatically per operation.
        """
        self._session: Optional[Session] = db_session
        self._owns_session = db_session is None

    def _get_session(self) -> Session:
        if self._session is None:
            self._session = SessionLocal()
        return self._session

    def _close_session(self, commit: bool = True) -> None:
        if not self._owns_session or self._session is None:
            return
        try:
            if commit:
                self._session.commit()
            else:
                self._session.rollback()
        finally:
            self._session.close()
            self._session = None

    def ingest_file(
        self,
        file_path: Path,
        platform_hint: Optional[str] = None,
        group_name_hint: Optional[str] = None,
    ) -> IngestResult:
        """
        Ingest a single chat log file.

        Args:
            file_path: Path to the file (json, txt, csv).
            platform_hint: Optional platform override (e.g. "wechat").
            group_name_hint: Optional group name override (required for TXT parser).

        Returns:
            IngestResult summarising what was written.

        Raises:
            ValueError: unsupported file extension or missing required hint.
        """
        ext = file_path.suffix.lower()
        if ext not in self._PARSERS:
            raise ValueError(
                f"Unsupported file extension: {ext}. "
                f"Supported: {list(self._PARSERS.keys())}"
            )

        # Instantiate parser with appropriate arguments
        parser = self._create_parser(ext, platform_hint, group_name_hint)
        parsed_messages: List[ParsedMessage] = parser.parse(file_path)

        if not parsed_messages:
            warnings.warn(f"No messages parsed from {file_path}")
            # Return empty result with dummy UUID
            return IngestResult(
                group_id=uuid.UUID(int=0),
                messages_inserted=0,
                messages_skipped_duplicate=0,
                messages_without_external_id=0,
                members_created=0,
                members_reused=0,
            )

        session = self._get_session()
        try:
            result = self._persist_messages(session, parsed_messages)
            self._close_session(commit=True)
            return result
        except Exception:
            self._close_session(commit=False)
            raise

    def _create_parser(
        self,
        ext: str,
        platform_hint: Optional[str],
        group_name_hint: Optional[str],
    ):
        """Factory method to create appropriate parser instance."""
        if ext == ".json":
            return JSONParser(platform=platform_hint)
        if ext == ".csv":
            return CSVParser(platform=platform_hint or "generic")
        if ext == ".txt":
            if not group_name_hint:
                raise ValueError(
                    "TXT parser requires group_name_hint (TXT files lack metadata)"
                )
            return TXTParser(
                group_name=group_name_hint,
                platform=platform_hint or "generic",
            )
        raise ValueError(f"Unexpected extension: {ext}")

    def _persist_messages(
        self,
        session: Session,
        messages: List[ParsedMessage],
    ) -> IngestResult:
        """
        Persist parsed messages to database with idempotency.

        Strategy:
        1. Determine group (all messages assumed same group in file)
        2. Get-or-create Group
        3. For each message: get-or-create Member, then insert Message
        4. Handle IntegrityError for duplicate external_message_id
        """
        if not messages:
            raise ValueError("Empty message list")

        # Assume all messages belong to same group (first message defines it)
        first = messages[0]
        group = self._get_or_create_group(
            session,
            platform=first.platform,
            external_group_id=first.external_group_id,
            name=first.group_name,
        )

        # Track members to avoid redundant DB calls
        member_cache: Dict[Tuple[uuid.UUID, Optional[str]], Member] = {}

        inserted = 0
        skipped_duplicate = 0
        without_external_id = 0
        members_created = 0
        members_reused = 0

        for pm in messages:
            # Get or create member
            cache_key = (group.id, pm.external_member_id or pm.user_name)
            if cache_key in member_cache:
                member = member_cache[cache_key]
                members_reused += 1
            else:
                member, was_created = self._get_or_create_member(
                    session,
                    group_id=group.id,
                    external_member_id=pm.external_member_id or pm.user_name,
                    display_name=pm.user_name,
                )
                member_cache[cache_key] = member
                if was_created:
                    members_created += 1
                else:
                    members_reused += 1

            # Track messages without external ID
            if not pm.external_message_id:
                without_external_id += 1
                warnings.warn(
                    f"Message without external_message_id from {pm.user_name} "
                    f"at {pm.timestamp} – duplicates possible"
                )

            # Attempt insert; rely on DB unique constraint for idempotency.
            # Use a savepoint (begin_nested) so that only this INSERT is rolled
            # back on duplicate, not the entire transaction.
            try:
                with session.begin_nested():
                    self._insert_message(session, group.id, member.id, pm)
                inserted += 1
            except IntegrityError:
                # Duplicate external_message_id detected by DB unique constraint.
                # Savepoint was rolled back; outer transaction remains intact.
                skipped_duplicate += 1
                continue

        return IngestResult(
            group_id=group.id,
            messages_inserted=inserted,
            messages_skipped_duplicate=skipped_duplicate,
            messages_without_external_id=without_external_id,
            members_created=members_created,
            members_reused=members_reused,
        )

    def _get_or_create_group(
        self,
        session: Session,
        platform: str,
        external_group_id: Optional[str],
        name: str,
    ) -> Group:
        """Get existing group or create new one."""
        # Use external_group_id if available, otherwise use name as identifier
        lookup_key = external_group_id or name

        stmt = select(Group).where(
            Group.platform == platform,
            Group.external_group_id == lookup_key,
        )
        existing = session.execute(stmt).scalar_one_or_none()
        if existing:
            return existing

        group = Group(
            platform=platform,
            external_group_id=lookup_key,
            name=name,
        )
        session.add(group)
        session.flush()  # Generate UUID
        return group

    def _get_or_create_member(
        self,
        session: Session,
        group_id: uuid.UUID,
        external_member_id: str,
        display_name: str,
    ) -> Tuple[Member, bool]:
        """
        Get existing member or create new one.

        Returns:
            (Member, was_created)
        """
        stmt = select(Member).where(
            Member.group_id == group_id,
            Member.external_member_id == external_member_id,
        )
        existing = session.execute(stmt).scalar_one_or_none()
        if existing:
            return existing, False

        member = Member(
            group_id=group_id,
            external_member_id=external_member_id,
            display_name=display_name,
            status="active",
        )
        session.add(member)
        session.flush()
        return member, True

    def _insert_message(
        self,
        session: Session,
        group_id: uuid.UUID,
        member_id: uuid.UUID,
        pm: ParsedMessage,
    ) -> None:
        """Insert a single message row."""
        # Ensure timezone-aware datetime
        sent_at = pm.timestamp
        if sent_at.tzinfo is None:
            sent_at = sent_at.replace(tzinfo=timezone.utc)

        message = Message(
            group_id=group_id,
            member_id=member_id,
            external_message_id=pm.external_message_id,
            sent_at=sent_at,
            content=pm.content,
            normalized_content=pm.normalized_content,
            content_type=pm.content_type,
            source_file=pm.source_file,
            raw_payload=pm.raw_payload,
        )
        session.add(message)
        # Flush to catch unique constraint violations early
        session.flush()
