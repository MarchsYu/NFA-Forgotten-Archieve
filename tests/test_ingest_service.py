"""
Tests for IngestService.

Uses an in-memory SQLite database to avoid requiring a live PostgreSQL instance.

Note: PostgreSQL-specific features (partial unique index, Identity columns) are
not exercised here. The idempotency test validates the service-level duplicate
detection logic, not the DB constraint.

Run with: pytest tests/test_ingest_service.py -v
"""

import json
from pathlib import Path

import pytest
from sqlalchemy import create_engine, event, text
from sqlalchemy.orm import sessionmaker

from src.db.base import Base
from src.db.models import Group, Member, Message

FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture()
def sqlite_session():
    """Provide a fresh in-memory SQLite session for each test."""
    from sqlalchemy import JSON, Integer, BigInteger
    from sqlalchemy.dialects.postgresql import JSONB

    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
    )

    # SQLite patches for PostgreSQL-specific types
    for table in Base.metadata.tables.values():
        for col in table.columns:
            # Patch JSONB -> JSON
            if isinstance(col.type, JSONB):
                col.type = JSON()
            # Patch BigInteger autoincrement -> Integer for SQLite
            # SQLite only supports autoincrement on INTEGER PRIMARY KEY
            if isinstance(col.type, BigInteger) and col.primary_key and col.autoincrement:
                col.type = Integer()

    Base.metadata.create_all(engine)

    Session = sessionmaker(bind=engine, autocommit=False, autoflush=False)
    session = Session()
    yield session
    session.close()
    engine.dispose()


class TestIngestServiceParsers:
    def test_ingest_json_file(self, sqlite_session):
        from src.ingest.services.ingest_service import IngestService

        service = IngestService(db_session=sqlite_session)
        result = service.ingest_file(FIXTURES / "sample_chat.json")

        assert result.messages_inserted == 4
        assert result.messages_skipped_duplicate == 0
        assert result.members_created == 3  # Alice, Bob, Carol

        # Verify DB state
        groups = sqlite_session.query(Group).all()
        assert len(groups) == 1
        assert groups[0].name == "NFA Test Group"

        members = sqlite_session.query(Member).all()
        assert len(members) == 3

        messages = sqlite_session.query(Message).all()
        assert len(messages) == 4

    def test_ingest_txt_file(self, sqlite_session):
        from src.ingest.services.ingest_service import IngestService

        service = IngestService(db_session=sqlite_session)
        result = service.ingest_file(
            FIXTURES / "sample_chat.txt",
            group_name_hint="NFA Test Group",
            platform_hint="wechat",
        )

        assert result.messages_inserted == 5
        assert result.messages_without_external_id == 5  # TXT has no IDs

    def test_ingest_csv_file(self, sqlite_session):
        from src.ingest.services.ingest_service import IngestService

        service = IngestService(db_session=sqlite_session)
        result = service.ingest_file(FIXTURES / "sample_chat.csv")

        assert result.messages_inserted == 4
        assert result.members_created == 3


class TestIngestIdempotency:
    def test_duplicate_json_import_skips_existing(self, sqlite_session):
        """
        Importing the same JSON file twice should not duplicate messages
        that have external_message_id.

        On SQLite the unique constraint is enforced at the application level
        (flush raises IntegrityError which the service catches).
        """
        from src.ingest.services.ingest_service import IngestService

        service = IngestService(db_session=sqlite_session)

        # First import
        result1 = service.ingest_file(FIXTURES / "sample_chat.json")
        assert result1.messages_inserted == 4

        # Re-create service with same session for second import
        service2 = IngestService(db_session=sqlite_session)
        result2 = service2.ingest_file(FIXTURES / "sample_chat.json")

        # All 4 should be skipped as duplicates
        assert result2.messages_inserted == 0
        assert result2.messages_skipped_duplicate == 4

        # DB should still have exactly 4 messages
        total = sqlite_session.query(Message).count()
        assert total == 4

    def test_txt_without_external_id_allows_duplicates(self, sqlite_session):
        """
        TXT messages have no external_message_id, so re-import creates duplicates.
        This is the documented behaviour for MVP.
        """
        from src.ingest.services.ingest_service import IngestService
        import warnings

        service = IngestService(db_session=sqlite_session)
        with warnings.catch_warnings(record=True):
            service.ingest_file(
                FIXTURES / "sample_chat.txt",
                group_name_hint="NFA Test Group",
            )
            service2 = IngestService(db_session=sqlite_session)
            service2.ingest_file(
                FIXTURES / "sample_chat.txt",
                group_name_hint="NFA Test Group",
            )

        total = sqlite_session.query(Message).count()
        assert total == 10  # 5 + 5 duplicates allowed


class TestIngestServiceEdgeCases:
    def test_unsupported_extension_raises(self, sqlite_session):
        from src.ingest.services.ingest_service import IngestService

        service = IngestService(db_session=sqlite_session)
        with pytest.raises(ValueError, match="Unsupported file extension"):
            service.ingest_file(Path("chat.xml"))

    def test_txt_without_group_name_raises(self, sqlite_session):
        from src.ingest.services.ingest_service import IngestService

        service = IngestService(db_session=sqlite_session)
        with pytest.raises(ValueError, match="group_name_hint"):
            service.ingest_file(FIXTURES / "sample_chat.txt")
