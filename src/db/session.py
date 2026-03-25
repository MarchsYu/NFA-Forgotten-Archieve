"""
Database session management for NFA Forgotten Archive.

Engine and session factory are initialised lazily on first use, so that
importing this module (or any module that imports from it) does NOT trigger
a PostgreSQL connection attempt.  This keeps pure-logic unit tests runnable
without a live database or the psycopg2 driver installed.

Usage
-----
    # Create a session (triggers engine init on first call):
    from src.db.session import SessionLocal
    session = SessionLocal()

    # Dependency-injection style (FastAPI / scripts):
    from src.db.session import get_session
    for session in get_session():
        ...

    # Direct engine access (e.g. init_db.py):
    from src.db.session import get_engine
    engine = get_engine()
"""

from __future__ import annotations

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session

from src.config import DATABASE_URL

# ---------------------------------------------------------------------------
# Lazy singletons – created on first call, not at import time
# ---------------------------------------------------------------------------

_engine = None
_session_factory = None


def _init() -> None:
    """Initialise engine and session factory (idempotent)."""
    global _engine, _session_factory
    if _engine is None:
        _engine = create_engine(
            DATABASE_URL,
            pool_pre_ping=True,
            future=True,
        )
        _session_factory = sessionmaker(
            bind=_engine,
            autocommit=False,
            autoflush=False,
            future=True,
        )


def get_engine():
    """Return the shared SQLAlchemy engine, creating it on first call."""
    _init()
    return _engine


class _LazySessionLocal:
    """
    Drop-in replacement for a sessionmaker instance.

    Calling ``SessionLocal()`` defers engine creation until the first
    actual session is needed, so importing this module never touches
    the database driver.
    """

    def __call__(self) -> Session:
        _init()
        return _session_factory()


# Public name used throughout the codebase
SessionLocal = _LazySessionLocal()


def get_session():
    """Yield a database session and ensure it is closed after use."""
    session = SessionLocal()
    try:
        yield session
    finally:
        session.close()

