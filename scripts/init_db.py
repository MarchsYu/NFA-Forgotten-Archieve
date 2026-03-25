#!/usr/bin/env python3
"""
Database initialization script for NFA Forgotten Archive.

This script creates all tables defined in the SQLAlchemy models.
Run this script to initialize a fresh database.

Usage:
    python scripts/init_db.py

Environment Variables:
    DATABASE_URL: PostgreSQL connection string
                  (default: postgresql://postgres:postgres@localhost:5432/nfa_archive)

NOTE — MVP approach (Issue 8):
    create_all() is intentionally used here for the MVP phase. It is safe for
    fresh environments but does NOT handle incremental schema changes.

    Recommended migration path to Alembic:
      1. pip install alembic
      2. alembic init alembic
      3. Point alembic/env.py at Base.metadata and DATABASE_URL
      4. alembic revision --autogenerate -m "initial schema"
      5. alembic upgrade head

    Highest-priority tables to migrate first (most likely to evolve):
      - messages       (partial unique index, index ops — not fully expressed by create_all)
      - topics         (Identity column — verify DDL output matches expectations)
      - members        (is_active column removal needs explicit DROP COLUMN migration)
"""

import sys
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.db.base import Base
from src.db.session import get_engine
from src.db.models import (
    Group,
    Member,
    Message,
    Topic,
    MessageTopic,
    ProfileSnapshot,
)


def init_db():
    """Create all tables in the database."""
    engine = get_engine()
    print("Initializing database...")
    print(f"Database URL: {engine.url}")
    
    try:
        Base.metadata.create_all(bind=engine)
        print("\n✅ Database initialized successfully!")
        print("\nCreated tables:")
        for table_name in Base.metadata.tables.keys():
            print(f"  - {table_name}")
    except Exception as e:
        print(f"\n❌ Error initializing database: {e}")
        sys.exit(1)


if __name__ == "__main__":
    init_db()
