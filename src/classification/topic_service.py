"""
Topic initialisation service – single canonical implementation.

Both ``scripts/init_topics.py`` and ``src/processing/pipeline.py`` call
``init_topics()`` from this module.  There is no other implementation.

Behaviour
---------
- Inserts each topic from ``topic_rules.TOPICS`` if it does not already exist.
- Uses ``topic_key`` as the unique identifier (upsert-safe: skips existing rows).
- Safe to re-run; existing topics are not modified.
- Returns a ``TopicInitResult`` so callers can log or surface counts.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import List

from sqlalchemy import select
from sqlalchemy.orm import Session

from src.db.models import Topic
from src.db.session import SessionLocal
from src.classification.topic_rules import TOPICS


@dataclass
class TopicInitResult:
    """Summary of a topic initialisation run."""
    inserted: int
    skipped: int
    topic_keys: List[str] = field(default_factory=list)


def init_topics(session: Session | None = None) -> TopicInitResult:
    """
    Seed the ``topics`` table from ``topic_rules.TOPICS``.

    Idempotent: rows whose ``topic_key`` already exists are skipped.

    Args:
        session: Optional SQLAlchemy session.  If *None*, a new
                 ``SessionLocal()`` is created, committed, and closed
                 automatically.  Pass an explicit session when you want
                 the caller to control the transaction (e.g. inside a
                 pipeline that manages its own session).

    Returns:
        ``TopicInitResult`` with inserted / skipped counts.

    Raises:
        Exception: any DB error; the session is rolled back before re-raising.
    """
    owns_session = session is None
    if owns_session:
        session = SessionLocal()

    try:
        inserted = 0
        skipped = 0

        for td in TOPICS:
            existing = session.execute(
                select(Topic).where(Topic.topic_key == td.topic_key)
            ).scalar_one_or_none()

            if existing:
                skipped += 1
                continue

            session.add(Topic(
                topic_key=td.topic_key,
                name=td.name,
                description=td.description,
                is_active=True,
            ))
            inserted += 1

        if owns_session:
            session.commit()

        return TopicInitResult(
            inserted=inserted,
            skipped=skipped,
            topic_keys=[td.topic_key for td in TOPICS],
        )

    except Exception:
        if owns_session:
            session.rollback()
        raise

    finally:
        if owns_session:
            session.close()
