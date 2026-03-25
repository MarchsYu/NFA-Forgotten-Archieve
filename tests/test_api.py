"""
Tests for the read-only API layer (Task 6).

All tests use FastAPI's TestClient with an in-memory SQLite database.
No PostgreSQL driver or live DB is required.

Test strategy
-------------
- A SQLite in-memory engine is created once per module.
- Tables are created with raw SQLite-compatible DDL (the production ORM
  models use PostgreSQL-specific JSONB/UUID types that SQLite cannot
  render via create_all; raw DDL sidesteps this without touching the
  production models).
- Seed data is inserted via the ORM (queries work fine on SQLite).
- The app's get_db dependency is overridden to use the test session.
- Tests verify HTTP status codes, response shapes, and pagination.
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from typing import Generator

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from src.api.app import app
from src.api.deps import get_db
from src.db.models import Group, Member, Message, ProfileSnapshot

# ---------------------------------------------------------------------------
# In-memory SQLite engine (module-scoped)
#
# StaticPool: all sessions share the same underlying connection, so DDL
# created in the fixture is visible to every test session.  Without it,
# each SessionLocal() call would get a fresh empty in-memory database.
# ---------------------------------------------------------------------------

SQLITE_URL = "sqlite:///:memory:"

_engine = create_engine(
    SQLITE_URL,
    connect_args={"check_same_thread": False},
    poolclass=StaticPool,
)
_TestingSessionLocal = sessionmaker(bind=_engine, autocommit=False, autoflush=False)


def _override_get_db() -> Generator[Session, None, None]:
    session = _TestingSessionLocal()
    try:
        yield session
    finally:
        session.close()


app.dependency_overrides[get_db] = _override_get_db

# ---------------------------------------------------------------------------
# Seed data constants
# ---------------------------------------------------------------------------

_GROUP_ID = uuid.uuid4()
_MEMBER_A_ID = uuid.uuid4()
_MEMBER_B_ID = uuid.uuid4()
_SNAPSHOT_ID = uuid.uuid4()
_NOW = datetime(2026, 6, 15, 12, 0, 0, tzinfo=timezone.utc)

# ---------------------------------------------------------------------------
# SQLite-compatible DDL
# (Production models use JSONB/UUID which SQLite cannot render.
#  We create equivalent tables with TEXT columns; the ORM maps fine.)
# ---------------------------------------------------------------------------

_DDL = [
    """
    CREATE TABLE IF NOT EXISTS groups (
        id TEXT PRIMARY KEY,
        platform TEXT NOT NULL,
        external_group_id TEXT NOT NULL,
        name TEXT NOT NULL,
        metadata_json TEXT,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS members (
        id TEXT PRIMARY KEY,
        group_id TEXT NOT NULL REFERENCES groups(id),
        external_member_id TEXT NOT NULL,
        display_name TEXT NOT NULL,
        nickname TEXT,
        status TEXT NOT NULL DEFAULT 'active',
        joined_at TEXT,
        left_at TEXT,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS messages (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        group_id TEXT NOT NULL REFERENCES groups(id),
        member_id TEXT NOT NULL REFERENCES members(id),
        external_message_id TEXT,
        sent_at TEXT NOT NULL,
        content TEXT NOT NULL,
        normalized_content TEXT,
        content_type TEXT NOT NULL DEFAULT 'text',
        reply_to_message_id INTEGER,
        source_file TEXT,
        raw_payload TEXT,
        created_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS topics (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        key TEXT NOT NULL UNIQUE,
        label TEXT NOT NULL,
        description TEXT,
        is_primary_eligible INTEGER NOT NULL DEFAULT 1,
        created_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS message_topics (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        message_id INTEGER NOT NULL REFERENCES messages(id),
        topic_id INTEGER NOT NULL REFERENCES topics(id),
        classifier_version TEXT NOT NULL,
        confidence REAL NOT NULL DEFAULT 0.0,
        is_primary INTEGER NOT NULL DEFAULT 0,
        evidence TEXT,
        created_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS profile_snapshots (
        id TEXT PRIMARY KEY,
        group_id TEXT NOT NULL REFERENCES groups(id),
        member_id TEXT NOT NULL REFERENCES members(id),
        profile_version TEXT NOT NULL,
        snapshot_at TEXT NOT NULL,
        window_start TEXT NOT NULL,
        window_end TEXT NOT NULL,
        source_message_count INTEGER NOT NULL DEFAULT 0,
        persona_summary TEXT,
        traits TEXT,
        stats TEXT,
        created_at TEXT NOT NULL
    )
    """,
]


@pytest.fixture(scope="module", autouse=True)
def setup_db():
    """Create tables with SQLite DDL and seed test data once per module."""
    with _engine.connect() as conn:
        for ddl in _DDL:
            conn.execute(text(ddl))
        conn.commit()

    session = _TestingSessionLocal()
    try:
        group = Group(
            id=_GROUP_ID,
            platform="telegram",
            external_group_id="tg-001",
            name="Test Group",
            created_at=_NOW,
            updated_at=_NOW,
        )
        session.add(group)
        session.flush()

        member_a = Member(
            id=_MEMBER_A_ID,
            group_id=_GROUP_ID,
            external_member_id="ext-a",
            display_name="Alice",
            status="active",
            created_at=_NOW,
            updated_at=_NOW,
        )
        member_b = Member(
            id=_MEMBER_B_ID,
            group_id=_GROUP_ID,
            external_member_id="ext-b",
            display_name="Bob",
            status="active",
            created_at=_NOW,
            updated_at=_NOW,
        )
        session.add_all([member_a, member_b])
        session.flush()

        # 3 messages for Alice (hours 10, 11, 12), 1 for Bob
        for i in range(3):
            session.add(Message(
                group_id=_GROUP_ID,
                member_id=_MEMBER_A_ID,
                sent_at=datetime(2026, 6, 15, i + 10, 0, 0, tzinfo=timezone.utc),
                content=f"Alice message {i}",
                content_type="text",
                created_at=_NOW,
            ))
        session.add(Message(
            group_id=_GROUP_ID,
            member_id=_MEMBER_B_ID,
            sent_at=_NOW,
            content="Bob message",
            content_type="text",
            created_at=_NOW,
        ))
        session.flush()

        # One profile snapshot for Alice; traits/stats stored as JSON text
        snap = ProfileSnapshot(
            id=_SNAPSHOT_ID,
            group_id=_GROUP_ID,
            member_id=_MEMBER_A_ID,
            profile_version="profile_v1",
            snapshot_at=_NOW,
            window_start=datetime(2026, 1, 1, tzinfo=timezone.utc),
            window_end=datetime(2026, 12, 31, 23, 59, 59, tzinfo=timezone.utc),
            source_message_count=3,
            persona_summary="Test summary",
            traits={"dominant_topics": ["casual_chat"]},
            stats={"message_count": 3, "classifier_version": "rule_v1"},
            created_at=_NOW,
        )
        session.add(snap)
        session.commit()
    finally:
        session.close()

    yield


@pytest.fixture(scope="module")
def client() -> TestClient:
    return TestClient(app)


# ---------------------------------------------------------------------------
# Health
# ---------------------------------------------------------------------------

class TestHealth:
    def test_health_ok(self, client: TestClient):
        r = client.get("/api/v1/health")
        assert r.status_code == 200
        body = r.json()
        assert body["status"] == "ok"
        assert "service" in body


# ---------------------------------------------------------------------------
# Groups
# ---------------------------------------------------------------------------

class TestGroups:
    def test_list_groups_returns_200(self, client: TestClient):
        r = client.get("/api/v1/groups")
        assert r.status_code == 200
        groups = r.json()
        assert isinstance(groups, list)
        assert len(groups) >= 1

    def test_list_groups_has_aggregates(self, client: TestClient):
        r = client.get("/api/v1/groups")
        group = next(g for g in r.json() if g["id"] == str(_GROUP_ID))
        assert group["member_count"] == 2
        assert group["message_count"] == 4

    def test_get_group_by_id(self, client: TestClient):
        r = client.get(f"/api/v1/groups/{_GROUP_ID}")
        assert r.status_code == 200
        assert r.json()["id"] == str(_GROUP_ID)
        assert r.json()["name"] == "Test Group"

    def test_get_group_not_found(self, client: TestClient):
        r = client.get(f"/api/v1/groups/{uuid.uuid4()}")
        assert r.status_code == 404

    def test_list_group_members(self, client: TestClient):
        r = client.get(f"/api/v1/groups/{_GROUP_ID}/members")
        assert r.status_code == 200
        members = r.json()
        assert len(members) == 2
        names = {m["display_name"] for m in members}
        assert names == {"Alice", "Bob"}

    def test_list_group_members_group_not_found(self, client: TestClient):
        r = client.get(f"/api/v1/groups/{uuid.uuid4()}/members")
        assert r.status_code == 404

    def test_alice_has_latest_snapshot_at(self, client: TestClient):
        r = client.get(f"/api/v1/groups/{_GROUP_ID}/members")
        alice = next(m for m in r.json() if m["display_name"] == "Alice")
        assert alice["latest_profile_snapshot_at"] is not None

    def test_bob_has_no_snapshot(self, client: TestClient):
        r = client.get(f"/api/v1/groups/{_GROUP_ID}/members")
        bob = next(m for m in r.json() if m["display_name"] == "Bob")
        assert bob["latest_profile_snapshot_at"] is None


# ---------------------------------------------------------------------------
# Members
# ---------------------------------------------------------------------------

class TestMembers:
    def test_get_member_by_id(self, client: TestClient):
        r = client.get(f"/api/v1/members/{_MEMBER_A_ID}")
        assert r.status_code == 200
        body = r.json()
        assert body["id"] == str(_MEMBER_A_ID)
        assert body["display_name"] == "Alice"

    def test_get_member_not_found(self, client: TestClient):
        r = client.get(f"/api/v1/members/{uuid.uuid4()}")
        assert r.status_code == 404

    def test_member_has_latest_snapshot_at(self, client: TestClient):
        r = client.get(f"/api/v1/members/{_MEMBER_A_ID}")
        assert r.json()["latest_profile_snapshot_at"] is not None

    def test_member_no_snapshot_returns_null(self, client: TestClient):
        r = client.get(f"/api/v1/members/{_MEMBER_B_ID}")
        assert r.status_code == 200
        assert r.json()["latest_profile_snapshot_at"] is None


# ---------------------------------------------------------------------------
# Messages
# ---------------------------------------------------------------------------

class TestMessages:
    def test_list_messages_returns_paged_response(self, client: TestClient):
        r = client.get(f"/api/v1/members/{_MEMBER_A_ID}/messages")
        assert r.status_code == 200
        body = r.json()
        assert "items" in body
        assert "total" in body
        assert "limit" in body
        assert "offset" in body

    def test_list_messages_total_correct(self, client: TestClient):
        r = client.get(f"/api/v1/members/{_MEMBER_A_ID}/messages")
        assert r.json()["total"] == 3

    def test_list_messages_ordered_newest_first(self, client: TestClient):
        r = client.get(f"/api/v1/members/{_MEMBER_A_ID}/messages")
        items = r.json()["items"]
        sent_ats = [i["sent_at"] for i in items]
        assert sent_ats == sorted(sent_ats, reverse=True)

    def test_list_messages_pagination_limit(self, client: TestClient):
        r = client.get(f"/api/v1/members/{_MEMBER_A_ID}/messages?limit=2")
        body = r.json()
        assert len(body["items"]) == 2
        assert body["total"] == 3
        assert body["limit"] == 2

    def test_list_messages_pagination_offset(self, client: TestClient):
        r = client.get(f"/api/v1/members/{_MEMBER_A_ID}/messages?limit=2&offset=2")
        body = r.json()
        assert len(body["items"]) == 1
        assert body["offset"] == 2

    def test_list_messages_member_not_found(self, client: TestClient):
        r = client.get(f"/api/v1/members/{uuid.uuid4()}/messages")
        assert r.status_code == 404

    def test_list_messages_invalid_limit(self, client: TestClient):
        r = client.get(f"/api/v1/members/{_MEMBER_A_ID}/messages?limit=0")
        assert r.status_code == 422

    def test_list_messages_limit_too_large(self, client: TestClient):
        r = client.get(f"/api/v1/members/{_MEMBER_A_ID}/messages?limit=9999")
        assert r.status_code == 422

    def test_list_messages_time_filter(self, client: TestClient):
        # Only messages at hour >= 11 (2 messages: hour 11 and 12)
        r = client.get(
            f"/api/v1/members/{_MEMBER_A_ID}/messages"
            "?sent_at_gte=2026-06-15T11:00:00Z"
        )
        assert r.json()["total"] == 2

    def test_message_schema_fields(self, client: TestClient):
        r = client.get(f"/api/v1/members/{_MEMBER_A_ID}/messages?limit=1")
        item = r.json()["items"][0]
        assert "id" in item
        assert "sent_at" in item
        assert "content" in item
        assert "content_type" in item


# ---------------------------------------------------------------------------
# Profiles
# ---------------------------------------------------------------------------

class TestProfiles:
    def test_get_latest_profile(self, client: TestClient):
        r = client.get(f"/api/v1/members/{_MEMBER_A_ID}/profile/latest")
        assert r.status_code == 200
        body = r.json()
        assert body["id"] == str(_SNAPSHOT_ID)
        assert body["profile_version"] == "profile_v1"
        assert body["persona_summary"] == "Test summary"
        assert body["traits"]["dominant_topics"] == ["casual_chat"]
        assert body["stats"]["classifier_version"] == "rule_v1"

    def test_get_latest_profile_no_snapshot(self, client: TestClient):
        r = client.get(f"/api/v1/members/{_MEMBER_B_ID}/profile/latest")
        assert r.status_code == 404

    def test_get_latest_profile_member_not_found(self, client: TestClient):
        r = client.get(f"/api/v1/members/{uuid.uuid4()}/profile/latest")
        assert r.status_code == 404

    def test_list_profiles_paged(self, client: TestClient):
        r = client.get(f"/api/v1/members/{_MEMBER_A_ID}/profiles")
        assert r.status_code == 200
        body = r.json()
        assert body["total"] == 1
        assert len(body["items"]) == 1

    def test_list_profiles_version_filter_match(self, client: TestClient):
        r = client.get(
            f"/api/v1/members/{_MEMBER_A_ID}/profiles?profile_version=profile_v1"
        )
        assert r.json()["total"] == 1

    def test_list_profiles_version_filter_no_match(self, client: TestClient):
        r = client.get(
            f"/api/v1/members/{_MEMBER_A_ID}/profiles?profile_version=profile_v99"
        )
        assert r.json()["total"] == 0

    def test_list_profiles_member_not_found(self, client: TestClient):
        r = client.get(f"/api/v1/members/{uuid.uuid4()}/profiles")
        assert r.status_code == 404

    def test_profile_schema_has_required_fields(self, client: TestClient):
        r = client.get(f"/api/v1/members/{_MEMBER_A_ID}/profile/latest")
        body = r.json()
        for field in ("profile_version", "snapshot_at", "window_start",
                      "window_end", "persona_summary", "traits", "stats"):
            assert field in body, f"Missing field: {field}"
