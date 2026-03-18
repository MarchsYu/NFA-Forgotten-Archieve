#!/usr/bin/env python3
"""
Seed the topics table with the base taxonomy for NFA Forgotten Archive.

Usage:
    python scripts/init_topics.py

Behaviour:
- Inserts each topic from topic_rules.TOPICS if it does not already exist.
- Uses topic_key as the unique identifier (upsert-safe: skips existing rows).
- Safe to re-run; existing topics are not modified.
"""

import sys
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from sqlalchemy import select
from src.db.models import Topic
from src.db.session import SessionLocal
from src.classification.topic_rules import TOPICS


def init_topics() -> None:
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

        session.commit()
        print(f"✅ Topics initialized: {inserted} inserted, {skipped} already existed.")
        for td in TOPICS:
            print(f"   [{td.topic_key}] {td.name}")
    except Exception as exc:
        session.rollback()
        print(f"❌ Error: {exc}", file=sys.stderr)
        sys.exit(1)
    finally:
        session.close()


if __name__ == "__main__":
    init_topics()
