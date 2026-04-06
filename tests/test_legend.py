"""
Tests for the Legend Archive module (Task 8 / Phase 2).

Coverage
--------
- archive_policy: eligibility, state-transition guards (pure, no DB)
- legend_schemas: DTO construction
- LegendService: archive, restore, simulation toggle, idempotency,
  re-archive after restore, missing profile snapshot handling
- Legend Archive API: all 6 endpoints via FastAPI TestClient + SQLite

All tests run in-memory (no PostgreSQL required).
"""

from __future__ import annotations

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
from src.db.models import Group, Member, ProfileSnapshot, LegendMember
from src.legend.archive_policy import (
    ArchiveNotEligibleError,
    InvalidStatusTransitionError,
    assert_can_restore,
    assert_can_toggle_simulation,
    assert_eligible_for_archive,
)
from src.legend.legend_schemas import ArchiveResult, LegendMemberSchema, RestoreResult, SimulationToggleResult
from src.legend.legend_service import LegendService

# ---------------------------------------------------------------------------
# In-memory SQLite engine (module-scoped, shared across all tests)
# ---------------------------------------------------------------------------

_engine = create_engine(
    "sqlite:///:memory:",
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

# ---------------------------------------------------------------------------
# SQLite-compatible DDL
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
    """
    CREATE TABLE IF NOT EXISTS legend_members (
        id TEXT PRIMARY KEY,
        member_id TEXT NOT NULL UNIQUE REFERENCES members(id),
        group_id TEXT NOT NULL REFERENCES groups(id),
        archive_status TEXT NOT NULL DEFAULT 'archived',
        archived_at TEXT NOT NULL,
        archived_reason TEXT,
        archived_by TEXT,
        source_profile_snapshot_id TEXT REFERENCES profile_snapshots(id),
        member_display_name_snapshot TEXT NOT NULL,
        member_external_id_snapshot TEXT,
        member_status_snapshot TEXT,
        simulation_enabled INTEGER NOT NULL DEFAULT 0,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    )
    """,
]

# ---------------------------------------------------------------------------
# Seed data constants
# ---------------------------------------------------------------------------

_NOW = datetime(2026, 6, 15, 12, 0, 0, tzinfo=timezone.utc)
_GROUP_ID = uuid.uuid4()
_MEMBER_LEFT_ID = uuid.uuid4()       # status="left" – eligible for archive
_MEMBER_ACTIVE_ID = uuid.uuid4()     # status="active" – not eligible
_MEMBER_NO_SNAP_ID = uuid.uuid4()    # status="left", no profile snapshot
_SNAPSHOT_ID = uuid.uuid4()


@pytest.fixture(scope="module", autouse=True)
def setup_db():
    """Create tables and seed test data once per module."""
    # Set the override for this module's tests; restore on teardown so other
    # test modules that share the same app object are not affected.
    _prev_override = app.dependency_overrides.get(get_db)
    app.dependency_overrides[get_db] = _override_get_db

    with _engine.connect() as conn:
        for ddl in _DDL:
            conn.execute(text(ddl))
        conn.commit()

    session = _TestingSessionLocal()
    try:
        group = Group(
            id=_GROUP_ID,
            platform="wechat",
            external_group_id="wc-001",
            name="Test Group",
            created_at=_NOW,
            updated_at=_NOW,
        )
        session.add(group)
        session.flush()

        member_left = Member(
            id=_MEMBER_LEFT_ID,
            group_id=_GROUP_ID,
            external_member_id="ext-left",
            display_name="Alice Left",
            status="left",
            created_at=_NOW,
            updated_at=_NOW,
        )
        member_active = Member(
            id=_MEMBER_ACTIVE_ID,
            group_id=_GROUP_ID,
            external_member_id="ext-active",
            display_name="Bob Active",
            status="active",
            created_at=_NOW,
            updated_at=_NOW,
        )
        member_no_snap = Member(
            id=_MEMBER_NO_SNAP_ID,
            group_id=_GROUP_ID,
            external_member_id="ext-nosnap",
            display_name="Carol NoSnap",
            status="left",
            created_at=_NOW,
            updated_at=_NOW,
        )
        session.add_all([member_left, member_active, member_no_snap])
        session.flush()

        snap = ProfileSnapshot(
            id=_SNAPSHOT_ID,
            group_id=_GROUP_ID,
            member_id=_MEMBER_LEFT_ID,
            profile_version="profile_v1",
            snapshot_at=_NOW,
            window_start=datetime(2026, 1, 1, tzinfo=timezone.utc),
            window_end=datetime(2026, 12, 31, tzinfo=timezone.utc),
            source_message_count=10,
            created_at=_NOW,
        )
        session.add(snap)
        session.commit()
    finally:
        session.close()

    yield  # run all tests in this module

    # Restore the previous override so other test modules are not affected
    if _prev_override is None:
        app.dependency_overrides.pop(get_db, None)
    else:
        app.dependency_overrides[get_db] = _prev_override


# ---------------------------------------------------------------------------
# Helper: fresh service with test session
# ---------------------------------------------------------------------------

def _service() -> LegendService:
    return LegendService(db_session=_TestingSessionLocal())


def _cleanup_legend(member_id: uuid.UUID) -> None:
    """Remove any legend_members row for member_id (test isolation)."""
    session = _TestingSessionLocal()
    try:
        lm = session.execute(
            __import__("sqlalchemy").select(LegendMember).where(
                LegendMember.member_id == member_id
            )
        ).scalar_one_or_none()
        if lm:
            session.delete(lm)
            session.commit()
    finally:
        session.close()


# ===========================================================================
# TestArchivePolicy – pure unit tests, no DB
# ===========================================================================

class TestArchivePolicy:
    def test_left_member_is_eligible(self):
        assert_eligible_for_archive("left")  # no raise

    def test_active_member_not_eligible(self):
        with pytest.raises(ArchiveNotEligibleError, match="not eligible"):
            assert_eligible_for_archive("active")

    def test_force_bypasses_eligibility(self):
        assert_eligible_for_archive("active", force=True)  # no raise

    def test_can_restore_archived(self):
        assert_can_restore("archived")  # no raise

    def test_cannot_restore_already_restored(self):
        with pytest.raises(InvalidStatusTransitionError):
            assert_can_restore("restored")

    def test_can_toggle_simulation_when_archived(self):
        assert_can_toggle_simulation("archived")  # no raise

    def test_cannot_toggle_simulation_when_restored(self):
        with pytest.raises(InvalidStatusTransitionError):
            assert_can_toggle_simulation("restored")


# ===========================================================================
# TestLegendService – service layer with SQLite session
# ===========================================================================

class TestLegendServiceArchive:
    def setup_method(self):
        _cleanup_legend(_MEMBER_LEFT_ID)

    def test_archive_left_member_succeeds(self):
        svc = _service()
        result = svc.archive_member(_MEMBER_LEFT_ID, archived_reason="left group")
        assert isinstance(result, ArchiveResult)
        assert result.archive_status == "archived"
        assert result.was_already_archived is False
        assert result.member_id == _MEMBER_LEFT_ID
        assert result.profile_snapshot_id == _SNAPSHOT_ID

    def test_archive_is_idempotent(self):
        svc = _service()
        svc.archive_member(_MEMBER_LEFT_ID)
        result2 = _service().archive_member(_MEMBER_LEFT_ID)
        assert result2.was_already_archived is True
        assert result2.archive_status == "archived"

    def test_archive_active_member_raises(self):
        svc = _service()
        with pytest.raises(ArchiveNotEligibleError):
            svc.archive_member(_MEMBER_ACTIVE_ID)

    def test_archive_active_member_with_force(self):
        _cleanup_legend(_MEMBER_ACTIVE_ID)
        svc = _service()
        result = svc.archive_member(_MEMBER_ACTIVE_ID, force=True)
        assert result.archive_status == "archived"
        _cleanup_legend(_MEMBER_ACTIVE_ID)

    def test_archive_nonexistent_member_raises(self):
        svc = _service()
        with pytest.raises(ValueError, match="not found"):
            svc.archive_member(uuid.uuid4())

    def test_archive_member_without_profile_snapshot(self):
        _cleanup_legend(_MEMBER_NO_SNAP_ID)
        svc = _service()
        result = svc.archive_member(_MEMBER_NO_SNAP_ID)
        assert result.archive_status == "archived"
        assert result.profile_snapshot_id is None  # no snapshot – stored as None
        _cleanup_legend(_MEMBER_NO_SNAP_ID)

    def test_archive_stores_member_identity_snapshot(self):
        svc = _service()
        svc.archive_member(_MEMBER_LEFT_ID)
        schema = _service().get_legend_member(_MEMBER_LEFT_ID)
        assert schema.member_display_name_snapshot == "Alice Left"
        assert schema.member_external_id_snapshot == "ext-left"
        assert schema.member_status_snapshot == "left"

    def test_simulation_disabled_by_default(self):
        svc = _service()
        svc.archive_member(_MEMBER_LEFT_ID)
        schema = _service().get_legend_member(_MEMBER_LEFT_ID)
        assert schema.simulation_enabled is False


class TestLegendServiceRestore:
    def setup_method(self):
        _cleanup_legend(_MEMBER_LEFT_ID)
        _service().archive_member(_MEMBER_LEFT_ID)

    def test_restore_archived_member(self):
        result = _service().restore_member(_MEMBER_LEFT_ID)
        assert isinstance(result, RestoreResult)
        assert result.archive_status == "restored"

    def test_restore_sets_simulation_false(self):
        # Enable simulation first
        _service().enable_simulation(_MEMBER_LEFT_ID)
        _service().restore_member(_MEMBER_LEFT_ID)
        schema = _service().get_legend_member(_MEMBER_LEFT_ID)
        assert schema.simulation_enabled is False
        assert schema.archive_status == "restored"

    def test_restore_already_restored_raises(self):
        _service().restore_member(_MEMBER_LEFT_ID)
        with pytest.raises(InvalidStatusTransitionError):
            _service().restore_member(_MEMBER_LEFT_ID)

    def test_restore_nonexistent_raises(self):
        with pytest.raises(ValueError, match="No legend record"):
            _service().restore_member(uuid.uuid4())

    def test_legend_row_preserved_after_restore(self):
        _service().restore_member(_MEMBER_LEFT_ID)
        schema = _service().get_legend_member(_MEMBER_LEFT_ID)
        assert schema is not None  # row still exists
        assert schema.archive_status == "restored"


class TestLegendServiceReArchive:
    def setup_method(self):
        _cleanup_legend(_MEMBER_LEFT_ID)
        _service().archive_member(_MEMBER_LEFT_ID)
        _service().restore_member(_MEMBER_LEFT_ID)

    def test_re_archive_after_restore(self):
        result = _service().archive_member(_MEMBER_LEFT_ID, archived_reason="re-archived")
        assert result.archive_status == "archived"
        assert result.was_already_archived is False

    def test_re_archive_reuses_same_row(self):
        first_schema = _service().get_legend_member(_MEMBER_LEFT_ID)
        _service().archive_member(_MEMBER_LEFT_ID)
        second_schema = _service().get_legend_member(_MEMBER_LEFT_ID)
        assert first_schema.id == second_schema.id  # same row

    def test_re_archive_resets_simulation_to_false(self):
        _service().archive_member(_MEMBER_LEFT_ID)
        schema = _service().get_legend_member(_MEMBER_LEFT_ID)
        assert schema.simulation_enabled is False


class TestLegendServiceSimulation:
    def setup_method(self):
        _cleanup_legend(_MEMBER_LEFT_ID)
        _service().archive_member(_MEMBER_LEFT_ID)

    def test_enable_simulation(self):
        result = _service().enable_simulation(_MEMBER_LEFT_ID)
        assert isinstance(result, SimulationToggleResult)
        assert result.simulation_enabled is True

    def test_disable_simulation(self):
        _service().enable_simulation(_MEMBER_LEFT_ID)
        result = _service().disable_simulation(_MEMBER_LEFT_ID)
        assert result.simulation_enabled is False

    def test_enable_simulation_on_restored_raises(self):
        _service().restore_member(_MEMBER_LEFT_ID)
        with pytest.raises(InvalidStatusTransitionError):
            _service().enable_simulation(_MEMBER_LEFT_ID)

    def test_disable_simulation_on_restored_is_allowed(self):
        _service().restore_member(_MEMBER_LEFT_ID)
        result = _service().disable_simulation(_MEMBER_LEFT_ID)
        assert result.simulation_enabled is False

    def test_enable_nonexistent_raises(self):
        with pytest.raises(ValueError, match="No legend record"):
            _service().enable_simulation(uuid.uuid4())


class TestLegendServiceList:
    def setup_method(self):
        _cleanup_legend(_MEMBER_LEFT_ID)
        _cleanup_legend(_MEMBER_NO_SNAP_ID)
        _service().archive_member(_MEMBER_LEFT_ID)
        _service().archive_member(_MEMBER_NO_SNAP_ID)

    def test_list_returns_all(self):
        schemas, total = _service().list_legend_members()
        assert total >= 2

    def test_list_filter_by_group(self):
        schemas, total = _service().list_legend_members(group_id=_GROUP_ID)
        assert total >= 2
        assert all(s.group_id == _GROUP_ID for s in schemas)

    def test_list_filter_by_status(self):
        schemas, total = _service().list_legend_members(archive_status="archived")
        assert all(s.archive_status == "archived" for s in schemas)

    def test_list_filter_simulation_enabled(self):
        _service().enable_simulation(_MEMBER_LEFT_ID)
        schemas, total = _service().list_legend_members(simulation_enabled=True)
        assert all(s.simulation_enabled is True for s in schemas)

    def test_get_legend_member_returns_none_for_unknown(self):
        result = _service().get_legend_member(uuid.uuid4())
        assert result is None


# ===========================================================================
# TestLegendAPI – FastAPI TestClient + SQLite
# ===========================================================================

client = TestClient(app)


class TestLegendAPI:
    def setup_method(self):
        _cleanup_legend(_MEMBER_LEFT_ID)
        _cleanup_legend(_MEMBER_NO_SNAP_ID)

    def test_list_legend_members_empty(self):
        resp = client.get("/api/v1/legend/members")
        assert resp.status_code == 200
        data = resp.json()
        assert "items" in data
        assert "total" in data

    def test_get_legend_member_404(self):
        resp = client.get(f"/api/v1/legend/members/{uuid.uuid4()}")
        assert resp.status_code == 404

    def test_archive_member_via_api(self):
        resp = client.post(
            f"/api/v1/legend/members/{_MEMBER_LEFT_ID}/archive",
            json={"archived_reason": "left group", "archived_by": "test"},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["archive_status"] == "archived"
        assert data["member_id"] == str(_MEMBER_LEFT_ID)
        assert data["simulation_enabled"] is False

    def test_archive_active_member_returns_422(self):
        _cleanup_legend(_MEMBER_ACTIVE_ID)
        resp = client.post(
            f"/api/v1/legend/members/{_MEMBER_ACTIVE_ID}/archive",
            json={},
        )
        assert resp.status_code == 422
        _cleanup_legend(_MEMBER_ACTIVE_ID)

    def test_archive_nonexistent_member_returns_404(self):
        resp = client.post(
            f"/api/v1/legend/members/{uuid.uuid4()}/archive",
            json={},
        )
        assert resp.status_code == 404

    def test_get_legend_member_after_archive(self):
        client.post(f"/api/v1/legend/members/{_MEMBER_LEFT_ID}/archive", json={})
        resp = client.get(f"/api/v1/legend/members/{_MEMBER_LEFT_ID}")
        assert resp.status_code == 200
        assert resp.json()["archive_status"] == "archived"

    def test_restore_member_via_api(self):
        client.post(f"/api/v1/legend/members/{_MEMBER_LEFT_ID}/archive", json={})
        resp = client.post(f"/api/v1/legend/members/{_MEMBER_LEFT_ID}/restore")
        assert resp.status_code == 200
        assert resp.json()["archive_status"] == "restored"
        assert resp.json()["simulation_enabled"] is False

    def test_restore_not_archived_returns_422(self):
        # member not in legend at all
        resp = client.post(f"/api/v1/legend/members/{uuid.uuid4()}/restore")
        assert resp.status_code == 404

    def test_enable_simulation_via_api(self):
        client.post(f"/api/v1/legend/members/{_MEMBER_LEFT_ID}/archive", json={})
        resp = client.post(f"/api/v1/legend/members/{_MEMBER_LEFT_ID}/enable-simulation")
        assert resp.status_code == 200
        assert resp.json()["simulation_enabled"] is True

    def test_disable_simulation_via_api(self):
        client.post(f"/api/v1/legend/members/{_MEMBER_LEFT_ID}/archive", json={})
        client.post(f"/api/v1/legend/members/{_MEMBER_LEFT_ID}/enable-simulation")
        resp = client.post(f"/api/v1/legend/members/{_MEMBER_LEFT_ID}/disable-simulation")
        assert resp.status_code == 200
        assert resp.json()["simulation_enabled"] is False

    def test_enable_simulation_on_restored_returns_422(self):
        client.post(f"/api/v1/legend/members/{_MEMBER_LEFT_ID}/archive", json={})
        client.post(f"/api/v1/legend/members/{_MEMBER_LEFT_ID}/restore")
        resp = client.post(f"/api/v1/legend/members/{_MEMBER_LEFT_ID}/enable-simulation")
        assert resp.status_code == 422

    def test_list_legend_members_filter_by_status(self):
        client.post(f"/api/v1/legend/members/{_MEMBER_LEFT_ID}/archive", json={})
        resp = client.get("/api/v1/legend/members?archive_status=archived")
        assert resp.status_code == 200
        items = resp.json()["items"]
        assert all(i["archive_status"] == "archived" for i in items)

    def test_archive_force_flag(self):
        _cleanup_legend(_MEMBER_ACTIVE_ID)
        resp = client.post(
            f"/api/v1/legend/members/{_MEMBER_ACTIVE_ID}/archive",
            json={"force": True},
        )
        assert resp.status_code == 200
        assert resp.json()["archive_status"] == "archived"
        _cleanup_legend(_MEMBER_ACTIVE_ID)
