"""
Classification service – reads messages from DB, classifies them, writes results.

Two run modes
-------------
Mode A (default): classify only messages not yet classified by *classifier_version*.
    service.run(classifier_version="rule_v1")

Mode B (rerun): delete existing results for *classifier_version* then re-classify all.
    service.run(classifier_version="rule_v1", rerun=True)

Idempotency
-----------
message_topics PK is (message_id, topic_id, classifier_version).
In Mode A, already-classified messages are skipped via a NOT EXISTS subquery.
In Mode B, existing rows for the version are deleted before re-inserting.

Batch processing
----------------
Messages are fetched in configurable batches (default 500) to avoid loading
the entire table into memory.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional

from sqlalchemy import select, delete, exists
from sqlalchemy.orm import Session

from src.db.models import Message, Topic, MessageTopic
from src.db.session import SessionLocal
from src.classification.topic_classifier import TopicClassifier, CLASSIFIER_VERSION


@dataclass
class ClassificationResult:
    """Summary of a classification run."""
    classifier_version: str
    messages_processed: int
    messages_skipped: int
    topic_assignments_written: int
    messages_unmatched: int


class ClassificationService:
    """
    Orchestrates topic classification for stored messages.

    Usage:
        service = ClassificationService()
        result = service.run(classifier_version="rule_v1")
    """

    def __init__(
        self,
        db_session: Optional[Session] = None,
        batch_size: int = 500,
    ):
        self._session: Optional[Session] = db_session
        self._owns_session = db_session is None
        self._batch_size = batch_size
        self._classifier = TopicClassifier()

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

    def run(
        self,
        classifier_version: str = CLASSIFIER_VERSION,
        rerun: bool = False,
        group_id=None,
    ) -> ClassificationResult:
        """
        Run topic classification.

        Args:
            classifier_version: Version string written to every MessageTopic row.
            rerun: If True, delete existing results for this version and re-classify all.
            group_id: Optional UUID to restrict classification to one group.

        Returns:
            ClassificationResult with counts.
        """
        session = self._get_session()
        try:
            # Build topic_key → topic_id lookup (topics must be seeded first)
            topic_id_map = self._load_topic_id_map(session)
            if not topic_id_map:
                raise RuntimeError(
                    "No topics found in DB. Run scripts/init_topics.py first."
                )

            if rerun:
                self._delete_existing(session, classifier_version, group_id)
                session.flush()

            result = self._classify_messages(
                session, classifier_version, topic_id_map, group_id, rerun
            )
            self._close_session(commit=True)
            return result
        except Exception:
            self._close_session(commit=False)
            raise

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _load_topic_id_map(session: Session) -> dict[str, int]:
        """Return {topic_key: topic.id} for all active topics."""
        rows = session.execute(
            select(Topic.topic_key, Topic.id).where(Topic.is_active == True)
        ).all()
        return {row.topic_key: row.id for row in rows}

    @staticmethod
    def _delete_existing(
        session: Session,
        classifier_version: str,
        group_id,
    ) -> None:
        """Delete all MessageTopic rows for this classifier_version (optionally scoped to group)."""
        if group_id is not None:
            # Subquery: message IDs belonging to the group
            msg_ids = select(Message.id).where(Message.group_id == group_id).scalar_subquery()
            stmt = delete(MessageTopic).where(
                MessageTopic.classifier_version == classifier_version,
                MessageTopic.message_id.in_(msg_ids),
            )
        else:
            stmt = delete(MessageTopic).where(
                MessageTopic.classifier_version == classifier_version
            )
        session.execute(stmt)

    def _classify_messages(
        self,
        session: Session,
        classifier_version: str,
        topic_id_map: dict[str, int],
        group_id,
        rerun: bool,
    ) -> ClassificationResult:
        """Fetch messages in batches and classify each one."""
        processed = 0
        skipped = 0
        written = 0
        unmatched = 0
        offset = 0

        while True:
            batch = self._fetch_batch(
                session, classifier_version, group_id, rerun, offset
            )
            if not batch:
                break

            for message in batch:
                text = message.normalized_content or message.content or ""
                matches = self._classifier.classify(text)

                if not matches:
                    unmatched += 1
                    processed += 1
                    continue

                for match in matches:
                    topic_id = topic_id_map.get(match.topic_key)
                    if topic_id is None:
                        continue  # topic not seeded – skip silently

                    mt = MessageTopic(
                        message_id=message.id,
                        topic_id=topic_id,
                        classifier_version=classifier_version,
                        confidence=match.confidence,
                        is_primary=match.is_primary,
                        evidence=match.evidence,
                    )
                    session.add(mt)
                    written += 1

                processed += 1

            session.flush()
            offset += len(batch)

            # If batch was smaller than batch_size, we've reached the end
            if len(batch) < self._batch_size:
                break

        return ClassificationResult(
            classifier_version=classifier_version,
            messages_processed=processed,
            messages_skipped=skipped,
            topic_assignments_written=written,
            messages_unmatched=unmatched,
        )

    def _fetch_batch(
        self,
        session: Session,
        classifier_version: str,
        group_id,
        rerun: bool,
        offset: int,
    ) -> List[Message]:
        """Fetch a batch of messages to classify."""
        stmt = select(Message)

        if group_id is not None:
            stmt = stmt.where(Message.group_id == group_id)

        if not rerun:
            # Mode A: skip messages already classified by this version
            already_classified = (
                select(MessageTopic.message_id)
                .where(MessageTopic.classifier_version == classifier_version)
                .correlate(Message)
                .scalar_subquery()
            )
            stmt = stmt.where(
                ~exists(
                    select(MessageTopic.message_id).where(
                        MessageTopic.message_id == Message.id,
                        MessageTopic.classifier_version == classifier_version,
                    )
                )
            )

        stmt = stmt.order_by(Message.id).offset(offset).limit(self._batch_size)
        return list(session.execute(stmt).scalars().all())
