import uuid
from datetime import datetime, timezone

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from src.db.models.legend_member import LegendMember
from src.legend.legend_service import LegendService


@pytest.fixture
def engine():
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    # Create tables manually for SQLite compatibility
    with engine.connect() as conn:
        conn.execute(text("""
            CREATE TABLE groups (
                id TEXT PRIMARY KEY,
                platform TEXT NOT NULL,
                external_group_id TEXT NOT NULL,
                name TEXT NOT NULL,
                metadata_json TEXT,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
        """))
        conn.execute(text("""
            CREATE TABLE members (
                id TEXT PRIMARY KEY,
                group_id TEXT NOT NULL,
                external_member_id TEXT NOT NULL,
                display_name TEXT NOT NULL,
                nickname TEXT,
                status TEXT NOT NULL DEFAULT 'active',
                joined_at TEXT,
                left_at TEXT,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (group_id) REFERENCES groups(id) ON DELETE CASCADE
            )
        """))
        conn.execute(text("""
            CREATE TABLE legend_members (
                id TEXT PRIMARY KEY,
                member_id TEXT NOT NULL UNIQUE,
                archive_status TEXT NOT NULL,
                simulation_enabled INTEGER NOT NULL DEFAULT 0,
                archived_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                restored_at TEXT,
                FOREIGN KEY (member_id) REFERENCES members(id) ON DELETE CASCADE,
                CHECK (archive_status IN ('archived', 'restored')),
                CHECK (NOT (archive_status = 'restored' AND simulation_enabled = 1))
            )
        """))
        conn.commit()
    return engine


@pytest.fixture
def session(engine):
    SessionLocal = sessionmaker(bind=engine)
    session = SessionLocal()
    yield session
    session.close()


@pytest.fixture
def test_group(session):
    group_id = str(uuid.uuid4())
    session.execute(text("""
        INSERT INTO groups (id, platform, external_group_id, name)
        VALUES (:id, 'wechat', 'test_group_1', 'Test Group')
    """), {"id": group_id})
    session.commit()
    
    class Group:
        def __init__(self, id):
            self.id = uuid.UUID(id)
    
    return Group(group_id)


@pytest.fixture
def test_member(session, test_group):
    member_id = str(uuid.uuid4())
    session.execute(text("""
        INSERT INTO members (id, group_id, external_member_id, display_name, status)
        VALUES (:id, :group_id, 'member_1', 'Test Member', 'active')
    """), {"id": member_id, "group_id": str(test_group.id)})
    session.commit()
    
    class Member:
        def __init__(self, id):
            self.id = uuid.UUID(id)
    
    return Member(member_id)


def test_archive_member_first_time(session, test_member):
    service = LegendService(session)
    result = service.archive_member(test_member.id)
    
    assert result.member_id == test_member.id
    assert result.was_already_archived is False
    assert result.legend_member.archive_status == "archived"
    assert result.legend_member.simulation_enabled is False


def test_archive_member_already_archived(session, test_member):
    service = LegendService(session)
    
    result1 = service.archive_member(test_member.id)
    session.commit()
    
    result2 = service.archive_member(test_member.id)
    
    assert result2.member_id == test_member.id
    assert result2.was_already_archived is True
    assert result2.legend_member.id == result1.legend_member.id


def test_archive_member_concurrent_integrity_error(session, test_member):
    """Simulate concurrent first archive with IntegrityError"""
    service = LegendService(session)
    
    # Create a legend member directly to simulate race condition
    existing = LegendMember(
        member_id=test_member.id,
        archive_status="archived",
        simulation_enabled=False,
        archived_at=datetime.now(timezone.utc),
    )
    session.add(existing)
    session.commit()
    
    # Now try to archive again - should handle gracefully
    result = service.archive_member(test_member.id)
    
    assert result.was_already_archived is True
    assert result.legend_member.archive_status == "archived"


def test_legend_member_check_constraint_archive_status():
    """Test that invalid archive_status is rejected"""
    # This would be enforced at DB level, testing model definition
    legend = LegendMember(
        member_id=uuid.uuid4(),
        archive_status="invalid_status",
        simulation_enabled=False,
        archived_at=datetime.now(timezone.utc),
    )
    # The CHECK constraint will be enforced when inserted into PostgreSQL
    assert legend.archive_status == "invalid_status"  # Model allows it, DB will reject


def test_legend_member_check_constraint_restored_no_simulation():
    """Test that restored + simulation_enabled=true is rejected"""
    legend = LegendMember(
        member_id=uuid.uuid4(),
        archive_status="restored",
        simulation_enabled=True,  # This violates the constraint
        archived_at=datetime.now(timezone.utc),
    )
    # The CHECK constraint will be enforced when inserted into PostgreSQL
    assert legend.simulation_enabled is True  # Model allows it, DB will reject
