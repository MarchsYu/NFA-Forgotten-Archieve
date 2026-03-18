"""
Classification service – reads messages from DB, classifies them, writes results.

Two run modes
-------------
Mode A (default / incremental):
    Classify only messages not yet classified by *classifier_version*.
    service.run(classifier_version="rule_v1")

Mode B (rerun):
    Delete existing results for *classifier_version*, then re-classify all.
    service.run(classifier_version="rule_v1", rerun=True)

Idempotency
-----------
message_topics PK is (message_id, topic_id, classifier_version).
In Mode A, already-classified messages are excluded from each fetch query.
In Mode B, existing rows for the version are deleted before re-inserting.

Batch processing – keyset pagination
--------------------------------------
Both modes use keyset pagination (Message.id > last_seen_id) rather than
OFFSET-based pagination.

Why OFFSET is wrong for Mode A:
  The incremental query filters out already-classified messages via NOT EXISTS.
  After each batch is processed and flushed, those messages become classified,
  so the result set shrinks. Advancing OFFSET by batch_size on a shrinking
  result set skips over unprocessed messages ("漏数" bug).

Keyset pagination is safe because:
  - We always query id > last_seen_id, so we never revisit processed messages.
  - The NOT EXISTS filter only removes rows we've already handled (id <= last_seen_id).
  - New unclassified rows with id > last_seen_id are always picked up correctly.
"""

from __future__ import annotations

import warnings
from dataclasses import dataclass, field
from typing import List, Optional, Set

from sqlalchemy import select, delete, exists, func
from sqlalchemy.orm import Session

from src.db.models import Message, Topic, MessageTopic
from src.db.session import SessionLocal
from src.classification.topic_classifier import TopicClassifier, CLASSIFIER_VERSION


@dataclass
class ClassificationResult:
    """Summary of a classification run."""
    classifier_version: str
    messages_processed: int
    messages_skipped_already_classified: int
    topic_assignments_written: int
    messages_unmatched: int
    missing_topic_assignments: int          # assignments dropped because topic not in DB
    missing_topic_keys: List[str] = field(default_factory=list)


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
            topic_id_map = self._load_topic_id_map(session)
            if not topic_id_map:
                raise RuntimeError(
                    "No topics found in DB. Run scripts/init_topics.py first."
                )

            # Count already-classified messages before any changes
            already_classified = self._count_already_classified(
                session, classifier_version, group_id
            )

            if rerun:
                self._delete_existing(session, classifier_version, group_id)
                session.flush()
                already_classified = 0  # we just deleted them

            result = self._classify_messages(
                session, classifier_version, topic_id_map, group_id,
                already_classified_before_run=already_classified,
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
    def _load_topic_id_map(session: Session) -> dict:
        """Return {topic_key: topic.id} for all active topics."""
        rows = session.execute(
            select(Topic.topic_key, Topic.id).where(Topic.is_active == True)
        ).all()
        return {row.topic_key: row.id for row in rows}

    @staticmethod
    def _count_already_classified(
        session: Session,
        classifier_version: str,
        group_id,
    ) -> int:
        """Count distinct messages already classified by this version."""
        stmt = (
            select(func.count(func.distinct(MessageTopic.message_id)))
            .where(MessageTopic.classifier_version == classifier_version)
        )
        if group_id is not None:
            msg_ids = select(Message.id).where(Message.group_id == group_id).scalar_subquery()
            stmt = stmt.where(MessageTopic.message_id.in_(msg_ids))
        return session.execute(stmt).scalar_one() or 0

    @staticmethod
    def _delete_existing(
        session: Session,
        classifier_version: str,
        group_id,
    ) -> None:
        """Delete all MessageTopic rows for this classifier_version (optionally scoped to group)."""
        if group_id is not None:
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
        topic_id_map: dict,
        group_id,
        already_classified_before_run: int,
    ) -> ClassificationResult:
        """
        Fetch messages in batches using keyset pagination and classify each one.

        Keyset pagination (id > last_seen_id) is used instead of OFFSET to avoid
        the "漏数" bug in incremental mode where the result set shrinks as we write.
        """
        processed = 0
        written = 0
        unmatched = 0
        missing_assignments = 0
        missing_keys: Set[str] = set()
        last_seen_id: int = 0

        while True:
            batch = self._fetch_batch(
                session, classifier_version, group_id, last_seen_id
            )
            if not batch:
                break

            for message in batch:
                text = message.normalized_content or message.content or ""
                matches = self._classifier.classify(text)

                if not matches:
                    unmatched += 1
                    processed += 1
                    last_seen_id = message.id
                    continue

                for match in matches:
                    topic_id = topic_id_map.get(match.topic_key)
                    if topic_id is None:
                        missing_assignments += 1
                        missing_keys.add(match.topic_key)
                        continue

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
                last_seen_id = message.id

            session.flush()

        if missing_keys:
            warnings.warn(
                f"Classification: {missing_assignments} assignment(s) dropped because "
                f"these topic_keys are not in the DB: {sorted(missing_keys)}. "
                f"Run scripts/init_topics.py to seed missing topics."
            )

        return ClassificationResult(
            classifier_version=classifier_version,
            messages_processed=processed,
            messages_skipped_already_classified=already_classified_before_run,
            topic_assignments_written=written,
            messages_unmatched=unmatched,
            missing_topic_assignments=missing_assignments,
            missing_topic_keys=sorted(missing_keys),
        )

    def _fetch_batch(
        self,
        session: Session,
        classifier_version: str,
        group_id,
        last_seen_id: int,
    ) -> List[Message]:
        """
        Fetch the next batch of unclassified messages using keyset pagination.

        Always filters: Message.id > last_seen_id (keyset cursor)
        Always filters: NOT EXISTS matching MessageTopic for this classifier_version
          (incremental – skip already-classified messages regardless of rerun flag,
           since in rerun mode we deleted them all upfront anyway)
        """
        stmt = (
            select(Message)
            .where(
                Message.id > last_seen_id,
                ~exists(
                    select(MessageTopic.message_id).where(
                        MessageTopic.message_id == Message.id,
                        MessageTopic.classifier_version == classifier_version,
                    )
                ),
            )
            .order_by(Message.id)
            .limit(self._batch_size)
        )

        if group_id is not None:
            stmt = stmt.where(Message.group_id == group_id)

        return list(session.execute(stmt).scalars().all())
