"""
Ingest service – orchestrates file parsing and database persistence.

Responsibilities:
1. Select parser based on file extension
2. Parse file into ParsedMessage list
3. Get-or-create Group and Member records
4. Upsert messages with idempotency guarantees

Idempotency strategy
--------------------
Case A – external_message_id present:
    Rely on the DB partial unique index (group_id, external_message_id)
    WHERE external_message_id IS NOT NULL. IntegrityError → skip.

Case B – external_message_id absent (e.g. TXT imports):
    Weak dedup via service-layer SELECT before INSERT.
    Match condition: (group_id, member_id, sent_at, normalized_content, source_file).
    Guarantees: re-importing the *same file* will not double-insert messages.
    Limitation: cannot detect duplicates across different source files.

Group / Member fallback keys
-----------------------------
When external_group_id / external_member_id are absent, synthetic fallback keys
are constructed with the prefix "__fb__" to avoid collisions with real platform IDs:

    group   → "__fb__:{source_file}:{group_name}"
    member  → "__fb__:{source_file}:{user_name}"

This scopes identity to the source file, preventing "same name = same entity"
false merges across files. Cross-file identity resolution is deferred to Phase 2.

Timestamp rule
--------------
All datetimes stored in the DB are UTC-aware. Parsers guarantee tz-aware output;
_insert_message applies a final UTC fallback for any residual naive values.

reply_to backfill (Phase 2 note)
---------------------------------
reply_to_external_message_id is preserved in raw_payload["reply_to_external_message_id"].
The reply_to_message_id FK column exists in the schema but is not populated here.
Phase 2 should run a post-import pass: for each message where raw_payload contains
reply_to_external_message_id, resolve it to the internal message.id and update the FK.
"""

from __future__ import annotations

import uuid
import warnings
from dataclasses import dataclass
from datetime import timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from sqlalchemy import select
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
        2. Get-or-create Group using fallback key when external_group_id is absent
        3. For each message: get-or-create Member (with fallback key), then insert Message
        4. Case A (has external_message_id): rely on DB unique constraint
        5. Case B (no external_message_id): SELECT-before-INSERT weak dedup
        """
        if not messages:
            raise ValueError("Empty message list")

        first = messages[0]
        group = self._get_or_create_group(
            session,
            platform=first.platform,
            external_group_id=first.external_group_id,
            name=first.group_name,
            source_file=first.source_file,
        )

        # Cache key: (group_id, resolved_external_member_id)
        member_cache: Dict[Tuple[uuid.UUID, str], Member] = {}

        inserted = 0
        skipped_duplicate = 0
        without_external_id = 0
        members_created = 0
        members_reused = 0

        for pm in messages:
            resolved_member_id = self._resolve_member_id(pm)
            cache_key = (group.id, resolved_member_id)

            if cache_key in member_cache:
                member = member_cache[cache_key]
                members_reused += 1
            else:
                member, was_created = self._get_or_create_member(
                    session,
                    group_id=group.id,
                    external_member_id=resolved_member_id,
                    display_name=pm.user_name,
                )
                member_cache[cache_key] = member
                if was_created:
                    members_created += 1
                else:
                    members_reused += 1

            if not pm.external_message_id:
                without_external_id += 1
                # Case B: weak dedup via SELECT before INSERT
                if self._message_exists(session, group.id, member.id, pm):
                    skipped_duplicate += 1
                    continue
                # No duplicate found – proceed with insert (no IntegrityError expected)
                self._insert_message(session, group.id, member.id, pm)
                inserted += 1
            else:
                # Case A: rely on DB partial unique index; catch IntegrityError
                try:
                    with session.begin_nested():
                        self._insert_message(session, group.id, member.id, pm)
                    inserted += 1
                except IntegrityError:
                    skipped_duplicate += 1

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
        source_file: Optional[str],
    ) -> Group:
        """Get existing group or create new one.

        When external_group_id is absent, a synthetic fallback key is used:
            "__fb__:{source_file}:{group_name}"
        This prevents same-platform same-name groups from different files
        being incorrectly merged. The "__fb__" prefix distinguishes synthetic
        keys from real platform IDs.
        """
        if external_group_id:
            lookup_key = external_group_id
        else:
            # Scope to source file to avoid cross-file false merges
            lookup_key = f"__fb__:{source_file or ''}:{name}"

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
        session.flush()
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

    @staticmethod
    def _resolve_member_id(pm: ParsedMessage) -> str:
        """Return the external_member_id to use for DB lookup/creation.

        When external_member_id is present, use it directly (real platform ID).
        When absent, construct a synthetic fallback key scoped to the source file:
            "__fb__:{source_file}:{user_name}"

        This prevents same-display-name users from different files being merged.
        Limitation: the same real person appearing in multiple files will get
        separate Member rows. Cross-file identity resolution is a Phase 2 concern.
        """
        if pm.external_member_id:
            return pm.external_member_id
        return f"__fb__:{pm.source_file or ''}:{pm.user_name}"

    @staticmethod
    def _message_exists(
        session: Session,
        group_id: uuid.UUID,
        member_id: uuid.UUID,
        pm: ParsedMessage,
    ) -> bool:
        """Check if a semantically identical message already exists (weak dedup).

        Used only when external_message_id is absent. Matches on:
            (group_id, member_id, sent_at, normalized_content, source_file)

        This guarantees that re-importing the same source file does not produce
        duplicate rows. It does NOT detect duplicates across different source files.
        """
        sent_at = pm.timestamp
        if sent_at.tzinfo is None:
            sent_at = sent_at.replace(tzinfo=timezone.utc)

        stmt = select(Message.id).where(
            Message.group_id == group_id,
            Message.member_id == member_id,
            Message.sent_at == sent_at,
            Message.normalized_content == pm.normalized_content,
            Message.source_file == pm.source_file,
        ).limit(1)
        return session.execute(stmt).scalar_one_or_none() is not None

    def _insert_message(
        self,
        session: Session,
        group_id: uuid.UUID,
        member_id: uuid.UUID,
        pm: ParsedMessage,
    ) -> None:
        """Insert a single message row.

        reply_to_message_id is intentionally left NULL here.
        reply_to_external_message_id is preserved in raw_payload so that a
        Phase 2 backfill pass can resolve it to the internal FK.
        """
        sent_at = pm.timestamp
        if sent_at.tzinfo is None:
            # Final safety net: parsers should already produce tz-aware datetimes
            sent_at = sent_at.replace(tzinfo=timezone.utc)

        # Ensure reply_to_external_message_id survives in raw_payload
        raw = dict(pm.raw_payload) if pm.raw_payload else {}
        if pm.reply_to_external_message_id and "reply_to_external_message_id" not in raw:
            raw["reply_to_external_message_id"] = pm.reply_to_external_message_id

        message = Message(
            group_id=group_id,
            member_id=member_id,
            external_message_id=pm.external_message_id,
            sent_at=sent_at,
            content=pm.content,
            normalized_content=pm.normalized_content,
            content_type=pm.content_type,
            source_file=pm.source_file,
            raw_payload=raw,
        )
        session.add(message)
        session.flush()
