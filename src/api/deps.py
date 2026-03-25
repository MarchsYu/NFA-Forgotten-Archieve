"""FastAPI dependency: yield a DB session per request."""
from __future__ import annotations

from typing import Generator

from sqlalchemy.orm import Session

from src.db.session import SessionLocal


def get_db() -> Generator[Session, None, None]:
    """
    Yield a SQLAlchemy session for the duration of a single request.

    The session is closed (and any uncommitted transaction rolled back)
    after the response is sent, regardless of success or failure.
    """
    session: Session = SessionLocal()
    try:
        yield session
    finally:
        session.close()
