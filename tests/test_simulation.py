# -*- coding: utf-8 -*-
"""
Tests for the Persona Simulation module (P2T2-S1).

Session strategy
----------------
All tests share a single module-scoped SQLAlchemy session that is injected
directly into services and repository calls.  This avoids cross-session
identity-map / visibility issues that arise with StaticPool + multiple
session objects in the same in-memory database.

Seed data is written via ORM model objects (not raw SQL) so that SQLAlchemy
type coercion (UUID ↔ TEXT, JSONB ↔ TEXT, etc.) is handled consistently.

Schema uses SQLite-compatible DDL aligned with current ORM models.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone
from typing import Generator

import pytest
from sqlalchemy import create_engine, select, text
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from src.db.models import Group, LegendMember, Member, Message, ProfileSnapshot
from src.simulation.llm_client import EchoLLMClient, LLMClient
from src.simulation.prompt_builder import (
    _NO_FABRICATION_CONSTRAINT,
    _SIMULATION_DISCLAIMER,
    build_prompt,
)
from src.simulation.simulation_policy import SimulationNotAllowedError, assert_can_simulate
from src.simulation.simulation_schemas import (
    HistoricalMessage,
    SimulationRequest,
    SimulationResult,
    SIMULATION_VERSION,
)
from src.simulation.simulation_service import SimulationService

# ---------------------------------------------------------------------------
# In-memory SQLite engine – single StaticPool connection shared by all tests
# ---------------------------------------------------------------------------

_engine = create_engine(
    "sqlite:///:memory:",
    connect_args={"check_same_thread": False},
    poolclass=StaticPool,
)
_SessionFactory = sessionmaker(bind=_engine, autocommit=False, autoflush=False)

# ---------------------------------------------------------------------------
# SQLite-compatible DDL (column names and types aligned with current ORM)
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
# Seed IDs
# ---------------------------------------------------------------------------

_NOW        = datetime(2026, 6, 15, 12, 0, 0, tzinfo=timezone.utc)
_WIN_START  = datetime(2026, 1, 1, tzinfo=timezone.utc)
_WIN_END    = datetime(2026, 12, 31, 23, 59, 59, tzinfo=timezone.utc)

_GROUP_ID          = uuid.uuid4()

# Each legend_member requires its own member row (UNIQUE on member_id)
_MEM_FULL_ID       = uuid.uuid4()
_MEM_DISABLED_ID   = uuid.uuid4()
_MEM_RESTORED_ID   = uuid.uuid4()
_MEM_NOSNAP_ID     = uuid.uuid4()

# Two snapshots for _MEM_FULL_ID – anchor vs latest
_SNAP_ANCHOR_ID    = uuid.uuid4()   # referenced by LM_FULL (must be used)
_SNAP_LATEST_ID    = uuid.uuid4()   # newer – must NOT be chosen by simulation

_LM_FULL_ID        = uuid.uuid4()   # archived + enabled + has anchor  → allowed
_LM_DISABLED_ID    = uuid.uuid4()   # archived + disabled               → denied
_LM_RESTORED_ID    = uuid.uuid4()   # restored                          → denied
_LM_NOSNAP_ID      = uuid.uuid4()   # archived + enabled + no snapshot  → denied

# ---------------------------------------------------------------------------
# Module-scoped shared session
# ---------------------------------------------------------------------------

# We expose a single session object for the entire module so that every
# test that needs DB access can reuse it.  Services under test receive this
# same session via db_session= injection, avoiding cross-session visibility
# issues inherent to StaticPool + multiple session objects.
_shared_session: Session | None = None


@pytest.fixture(scope="module", autouse=True)
def setup_db():
    """Create schema, seed data, and expose a shared session for the module."""
    global _shared_session

    # Create tables
    with _engine.connect() as conn:
        for ddl in _DDL:
            conn.execute(text(ddl))
        conn.commit()

    _shared_session = _SessionFactory()

    try:
        _seed(_shared_session)
        _shared_session.commit()
    except Exception:
        _shared_session.rollback()
        _shared_session.close()
        raise

    yield

    _shared_session.close()
    _shared_session = None


def _sess() -> Session:
    """Return the module-scoped shared session (always the same object)."""
    assert _shared_session is not None, "setup_db fixture has not run"
    return _shared_session


def _svc(lm_client=None) -> SimulationService:
    """Build a SimulationService injected with the shared session."""
    return SimulationService(
        db_session=_sess(),
        llm_client=lm_client or EchoLLMClient(),
    )


def _req(legend_member_id: uuid.UUID, msg: str = "hello") -> SimulationRequest:
    return SimulationRequest(
        legend_member_id=legend_member_id,
        current_message=msg,
    )


# ---------------------------------------------------------------------------
# Seed helper (ORM objects – type coercion handled by SQLAlchemy)
# ---------------------------------------------------------------------------

def _seed(s: Session) -> None:
    group = Group(
        id=_GROUP_ID,
        platform="wechat",
        external_group_id="wc-sim-001",
        name="Sim Test Group",
        created_at=_NOW,
        updated_at=_NOW,
    )
    s.add(group)
    s.flush()

    # One member per legend_member (UNIQUE member_id constraint)
    members = {
        _MEM_FULL_ID:     ("SimUserFull",     "ext-full"),
        _MEM_DISABLED_ID: ("SimUserDisabled", "ext-disabled"),
        _MEM_RESTORED_ID: ("SimUserRestored", "ext-restored"),
        _MEM_NOSNAP_ID:   ("SimUserNoSnap",   "ext-nosnap"),
    }
    for mid, (dn, ext) in members.items():
        s.add(Member(
            id=mid, group_id=_GROUP_ID,
            external_member_id=ext, display_name=dn,
            status="left", created_at=_NOW, updated_at=_NOW,
        ))
    s.flush()

    # Anchor snapshot for _MEM_FULL_ID (earlier snapshot_at)
    s.add(ProfileSnapshot(
        id=_SNAP_ANCHOR_ID, group_id=_GROUP_ID, member_id=_MEM_FULL_ID,
        profile_version="profile_v1",
        snapshot_at=datetime(2026, 6, 1, tzinfo=timezone.utc),
        window_start=_WIN_START, window_end=_WIN_END,
        source_message_count=5,
        persona_summary="anchor-snapshot-summary",
        traits={"dominant_topics": ["gaming"], "verbosity_level": "terse",
                "style_hints": [], "activity_pattern": "night"},
        stats={"message_count": 5, "classifier_version": "rule_v1"},
        created_at=_NOW,
    ))
    # Latest snapshot for _MEM_FULL_ID (newer snapshot_at – must NOT be used)
    s.add(ProfileSnapshot(
        id=_SNAP_LATEST_ID, group_id=_GROUP_ID, member_id=_MEM_FULL_ID,
        profile_version="profile_v1",
        snapshot_at=datetime(2026, 6, 15, tzinfo=timezone.utc),
        window_start=_WIN_START, window_end=_WIN_END,
        source_message_count=20,
        persona_summary="latest-snapshot-must-not-be-used",
        traits={"dominant_topics": ["technical"], "verbosity_level": "verbose",
                "style_hints": [], "activity_pattern": "morning"},
        stats={"message_count": 20, "classifier_version": "rule_v1"},
        created_at=_NOW,
    ))
    s.flush()

    # Sample messages for _MEM_FULL_ID within the window
    for i in range(3):
        s.add(Message(
            group_id=_GROUP_ID, member_id=_MEM_FULL_ID,
            sent_at=datetime(2026, i + 1, 1, 10, 0, 0, tzinfo=timezone.utc),
            content=f"historical-msg-{i+1}",
            normalized_content=f"historical-msg-{i+1}",
            content_type="text",
            created_at=_NOW,
        ))
    s.flush()

    # Legend members
    def _lm(lm_id, member_id, display_name, archive_status, sim_enabled, snap_id):
        s.add(LegendMember(
            id=lm_id, member_id=member_id, group_id=_GROUP_ID,
            archive_status=archive_status,
            archived_at=_NOW,
            source_profile_snapshot_id=snap_id,
            member_display_name_snapshot=display_name,
            member_external_id_snapshot="ext-sim-001",
            member_status_snapshot="left",
            simulation_enabled=sim_enabled,
            created_at=_NOW, updated_at=_NOW,
        ))

    _lm(_LM_FULL_ID,     _MEM_FULL_ID,     "SimUserFull",     "archived", True,  _SNAP_ANCHOR_ID)
    _lm(_LM_DISABLED_ID, _MEM_DISABLED_ID, "SimUserDisabled", "archived", False, _SNAP_ANCHOR_ID)
    _lm(_LM_RESTORED_ID, _MEM_RESTORED_ID, "SimUserRestored", "restored", False, _SNAP_ANCHOR_ID)
    _lm(_LM_NOSNAP_ID,   _MEM_NOSNAP_ID,   "SimUserNoSnap",   "archived", True,  None)
    s.flush()


# ===========================================================================
# 1. simulation_policy – pure unit tests, no DB
# ===========================================================================

class TestSimulationPolicy:
    def test_all_conditions_met_does_not_raise(self):
        assert_can_simulate("archived", True, uuid.uuid4())

    def test_restored_status_raises(self):
        with pytest.raises(SimulationNotAllowedError, match="archived"):
            assert_can_simulate("restored", True, uuid.uuid4())

    def test_simulation_disabled_raises(self):
        with pytest.raises(SimulationNotAllowedError, match="not enabled"):
            assert_can_simulate("archived", False, uuid.uuid4())

    def test_no_snapshot_raises(self):
        with pytest.raises(SimulationNotAllowedError, match="profile snapshot"):
            assert_can_simulate("archived", True, None)

    def test_unknown_status_raises(self):
        with pytest.raises(SimulationNotAllowedError):
            assert_can_simulate("bad_status", True, uuid.uuid4())


# ===========================================================================
# 2. simulation_repository – using the shared session
# ===========================================================================

class TestSimulationRepository:
    def test_get_legend_member_by_id_returns_row(self):
        from src.simulation import simulation_repository as repo
        lm = repo.get_legend_member_by_id(_sess(), _LM_FULL_ID)
        assert lm is not None
        assert lm.id == _LM_FULL_ID

    def test_get_legend_member_by_id_unknown_returns_none(self):
        from src.simulation import simulation_repository as repo
        assert repo.get_legend_member_by_id(_sess(), uuid.uuid4()) is None

    def test_get_profile_snapshot_by_id_returns_anchor(self):
        from src.simulation import simulation_repository as repo
        snap = repo.get_profile_snapshot_by_id(_sess(), _SNAP_ANCHOR_ID)
        assert snap is not None
        assert snap.id == _SNAP_ANCHOR_ID
        assert snap.persona_summary == "anchor-snapshot-summary"

    def test_get_profile_snapshot_by_id_does_not_return_latest(self):
        """Anchor and latest are different rows; simulation must use anchor."""
        from src.simulation import simulation_repository as repo
        anchor = repo.get_profile_snapshot_by_id(_sess(), _SNAP_ANCHOR_ID)
        latest = repo.get_profile_snapshot_by_id(_sess(), _SNAP_LATEST_ID)
        assert anchor is not None and latest is not None
        assert anchor.id != latest.id
        assert anchor.persona_summary == "anchor-snapshot-summary"
        assert latest.persona_summary == "latest-snapshot-must-not-be-used"

    def test_sample_member_messages_returns_list(self):
        from src.simulation import simulation_repository as repo
        msgs = repo.sample_member_messages(
            _sess(), _MEM_FULL_ID, _WIN_START, _WIN_END, limit=10
        )
        assert isinstance(msgs, list)
        assert len(msgs) == 3

    def test_sample_member_messages_respects_limit(self):
        from src.simulation import simulation_repository as repo
        msgs = repo.sample_member_messages(
            _sess(), _MEM_FULL_ID, _WIN_START, _WIN_END, limit=2
        )
        assert len(msgs) <= 2

    def test_sample_member_messages_empty_window_returns_empty(self):
        from src.simulation import simulation_repository as repo
        future = datetime(2099, 1, 1, tzinfo=timezone.utc)
        msgs = repo.sample_member_messages(
            _sess(), _MEM_FULL_ID,
            future, future + timedelta(days=1), limit=10
        )
        assert msgs == []

    def test_sample_member_messages_limit_clamped(self):
        from src.simulation import simulation_repository as repo
        msgs = repo.sample_member_messages(
            _sess(), _MEM_FULL_ID, _WIN_START, _WIN_END, limit=99
        )
        assert isinstance(msgs, list)
        assert len(msgs) <= 20


# ===========================================================================
# 3. prompt_builder – pure unit tests, no DB
# ===========================================================================

class TestPromptBuilder:
    def _hist(self, n=2):
        return [
            HistoricalMessage(
                sent_at=datetime(2026, 1, i + 1, tzinfo=timezone.utc),
                content=f"msg-{i+1}",
                normalized_content=f"msg-{i+1}",
            )
            for i in range(n)
        ]

    def _build(self, **kw):
        defaults = dict(
            display_name="Alice",
            persona_summary=None,
            traits={},
            stats={},
            historical_messages=[],
            current_message="test",
        )
        defaults.update(kw)
        return build_prompt(**defaults)

    def _system(self, msgs):
        return next(m["content"] for m in msgs if m["role"] == "system")

    def _user(self, msgs):
        return next(m["content"] for m in msgs if m["role"] == "user")

    def test_returns_list_of_dicts_with_roles(self):
        msgs = self._build()
        roles = {m["role"] for m in msgs}
        assert "system" in roles
        assert "user" in roles

    def test_system_contains_simulation_disclaimer(self):
        sys_txt = self._system(self._build())
        # Verify the disclaimer constant itself is present in the system prompt
        assert _SIMULATION_DISCLAIMER in sys_txt

    def test_system_contains_no_fabrication_constraint(self):
        sys_txt = self._system(self._build())
        assert _NO_FABRICATION_CONSTRAINT in sys_txt

    def test_system_contains_uncertainty_constraint(self):
        from src.simulation.prompt_builder import _NO_FABRICATION_CONSTRAINT
        sys_txt = self._system(self._build())
        # The no-fabrication constraint includes the uncertainty instruction
        assert _NO_FABRICATION_CONSTRAINT in sys_txt

    def test_disclaimer_constants_are_non_empty(self):
        assert len(_SIMULATION_DISCLAIMER) > 0
        assert len(_NO_FABRICATION_CONSTRAINT) > 0

    def test_persona_summary_in_system(self):
        sys_txt = self._system(self._build(persona_summary="unique-summary-XYZ"))
        assert "unique-summary-XYZ" in sys_txt

    def test_historical_messages_in_system(self):
        sys_txt = self._system(self._build(historical_messages=self._hist(2)))
        assert "msg-1" in sys_txt
        assert "msg-2" in sys_txt

    def test_empty_historical_messages_no_crash(self):
        msgs = self._build(historical_messages=[])
        assert len(msgs) == 2

    def test_current_message_in_user(self):
        user_txt = self._user(self._build(current_message="unique-question-ABC"))
        assert "unique-question-ABC" in user_txt

    def test_conversation_context_in_user(self):
        user_txt = self._user(self._build(conversation_context="context-DEF"))
        assert "context-DEF" in user_txt

    def test_traits_reflected_in_system(self):
        traits = {"dominant_topics": ["gaming-unique-topic"],
                  "verbosity_level": "terse", "style_hints": [], "activity_pattern": "night"}
        sys_txt = self._system(self._build(traits=traits))
        assert "gaming-unique-topic" in sys_txt


# ===========================================================================
# 4. EchoLLMClient
# ===========================================================================

class TestEchoLLMClient:
    def test_implements_llm_client_interface(self):
        assert isinstance(EchoLLMClient(), LLMClient)

    def test_returns_string(self):
        result = EchoLLMClient().generate([
            {"role": "system", "content": "sys"},
            {"role": "user",   "content": "hello"},
        ])
        assert isinstance(result, str)

    def test_response_contains_echo_marker(self):
        result = EchoLLMClient().generate([{"role": "user", "content": "hi"}])
        assert "[Echo Simulation]" in result

    def test_response_includes_user_message(self):
        result = EchoLLMClient().generate([{"role": "user", "content": "unique-msg-XYZ"}])
        assert "unique-msg-XYZ" in result


# ===========================================================================
# 5. SimulationService – end-to-end, shared session injected
# ===========================================================================

class TestSimulationService:
    # -- Policy rejections --------------------------------------------------

    def test_disabled_raises(self):
        with pytest.raises(SimulationNotAllowedError, match="not enabled"):
            _svc().generate_once(_req(_LM_DISABLED_ID))

    def test_restored_raises(self):
        with pytest.raises(SimulationNotAllowedError, match="archived"):
            _svc().generate_once(_req(_LM_RESTORED_ID))

    def test_no_snapshot_raises(self):
        with pytest.raises(SimulationNotAllowedError, match="profile snapshot"):
            _svc().generate_once(_req(_LM_NOSNAP_ID))

    def test_unknown_legend_member_raises_value_error(self):
        with pytest.raises(ValueError, match="not found"):
            _svc().generate_once(_req(uuid.uuid4()))

    # -- Happy path ---------------------------------------------------------

    def test_returns_simulation_result(self):
        result = _svc().generate_once(_req(_LM_FULL_ID))
        assert isinstance(result, SimulationResult)

    def test_legend_member_id_matches(self):
        result = _svc().generate_once(_req(_LM_FULL_ID))
        assert result.legend_member_id == _LM_FULL_ID

    def test_uses_anchor_snapshot_not_latest(self):
        result = _svc().generate_once(_req(_LM_FULL_ID))
        assert result.profile_snapshot_id == _SNAP_ANCHOR_ID
        assert result.profile_snapshot_id != _SNAP_LATEST_ID

    def test_has_system_and_user_prompt_messages(self):
        result = _svc().generate_once(_req(_LM_FULL_ID))
        roles = {m["role"] for m in result.prompt_messages}
        assert "system" in roles and "user" in roles

    def test_response_is_non_empty_string(self):
        result = _svc().generate_once(_req(_LM_FULL_ID))
        assert isinstance(result.response_text, str)
        assert len(result.response_text) > 0

    def test_simulation_version(self):
        result = _svc().generate_once(_req(_LM_FULL_ID))
        assert result.simulation_version == SIMULATION_VERSION

    def test_prompt_contains_simulation_boundary(self):
        result = _svc().generate_once(_req(_LM_FULL_ID))
        system = next(m["content"] for m in result.prompt_messages if m["role"] == "system")
        assert _SIMULATION_DISCLAIMER in system

    def test_prompt_contains_no_fabrication_constraint(self):
        result = _svc().generate_once(_req(_LM_FULL_ID))
        system = next(m["content"] for m in result.prompt_messages if m["role"] == "system")
        assert _NO_FABRICATION_CONSTRAINT in system

    def test_echo_client_response_marker(self):
        result = _svc().generate_once(_req(_LM_FULL_ID))
        assert "[Echo Simulation]" in result.response_text

    # -- Read-only guarantee -----------------------------------------------

    def _count(self, table: str) -> int:
        return _sess().execute(
            text(f"SELECT COUNT(*) FROM {table}")
        ).scalar_one()

    def test_does_not_write_legend_members(self):
        before = self._count("legend_members")
        _svc().generate_once(_req(_LM_FULL_ID))
        assert self._count("legend_members") == before

    def test_does_not_write_messages(self):
        before = self._count("messages")
        _svc().generate_once(_req(_LM_FULL_ID))
        assert self._count("messages") == before

    def test_does_not_write_profile_snapshots(self):
        before = self._count("profile_snapshots")
        _svc().generate_once(_req(_LM_FULL_ID))
        assert self._count("profile_snapshots") == before


# ===========================================================================
# 6. Import isolation – pure modules must not trigger DB init
# ===========================================================================

class TestImportIsolation:
    def test_policy_importable_without_db(self):
        import importlib
        mod = importlib.import_module("src.simulation.simulation_policy")
        assert hasattr(mod, "assert_can_simulate")

    def test_prompt_builder_importable_without_db(self):
        import importlib
        mod = importlib.import_module("src.simulation.prompt_builder")
        assert hasattr(mod, "build_prompt")

    def test_llm_client_importable_without_db(self):
        import importlib
        mod = importlib.import_module("src.simulation.llm_client")
        assert hasattr(mod, "EchoLLMClient")

    def test_schemas_importable_without_db(self):
        import importlib
        mod = importlib.import_module("src.simulation.simulation_schemas")
        assert hasattr(mod, "SimulationRequest")
        assert hasattr(mod, "SimulationResult")

    def test_package_init_no_db_side_effects(self):
        import src.simulation as pkg  # noqa: F401
        assert True  # reaching here means no engine was initialised at import


# ===========================================================================
# 7. Provider factory (build_llm_client)
# ===========================================================================

class TestProviderFactory:
    def test_default_returns_echo_client(self):
        from src.simulation.providers import build_llm_client
        from src.simulation.llm_client import EchoLLMClient
        client = build_llm_client()
        assert isinstance(client, EchoLLMClient)

    def test_explicit_echo_returns_echo_client(self):
        from src.simulation.providers import build_llm_client
        from src.simulation.llm_client import EchoLLMClient
        client = build_llm_client(provider="echo")
        assert isinstance(client, EchoLLMClient)

    def test_env_echo_returns_echo_client(self, monkeypatch):
        from src.simulation.providers import build_llm_client
        from src.simulation.llm_client import EchoLLMClient
        monkeypatch.setenv("SIM_LLM_PROVIDER", "echo")
        client = build_llm_client()
        assert isinstance(client, EchoLLMClient)

    def test_unknown_provider_raises(self):
        from src.simulation.providers import build_llm_client
        with pytest.raises(ValueError, match="Unknown SIM_LLM_PROVIDER"):
            build_llm_client(provider="nonexistent_provider")

    def test_openai_compat_missing_api_key_raises(self):
        from src.simulation.providers import build_llm_client
        with pytest.raises(ValueError, match="API key"):
            build_llm_client(
                provider="openai_compatible",
                api_key="",          # explicitly empty
                model="gpt-4o-mini",
            )

    def test_openai_compat_missing_model_raises(self):
        from src.simulation.providers import build_llm_client
        with pytest.raises(ValueError, match="model name"):
            build_llm_client(
                provider="openai_compatible",
                api_key="sk-test",
                model="",            # explicitly empty
            )

    def test_openai_compat_missing_api_key_via_env_raises(self, monkeypatch):
        from src.simulation.providers import build_llm_client
        monkeypatch.setenv("SIM_LLM_PROVIDER", "openai_compatible")
        monkeypatch.delenv("SIM_LLM_API_KEY", raising=False)
        monkeypatch.setenv("SIM_LLM_MODEL", "gpt-4o-mini")
        with pytest.raises(ValueError, match="API key"):
            build_llm_client()

    def test_openai_compat_constructs_with_valid_config(self):
        from src.simulation.providers import build_llm_client, OpenAICompatibleLLMClient
        client = build_llm_client(
            provider="openai_compatible",
            api_key="sk-test-key",
            model="gpt-4o-mini",
            base_url="https://api.openai.com/v1",
        )
        assert isinstance(client, OpenAICompatibleLLMClient)

    def test_env_vars_configure_openai_compat(self, monkeypatch):
        from src.simulation.providers import build_llm_client, OpenAICompatibleLLMClient
        monkeypatch.setenv("SIM_LLM_PROVIDER", "openai_compatible")
        monkeypatch.setenv("SIM_LLM_API_KEY", "sk-env-key")
        monkeypatch.setenv("SIM_LLM_MODEL", "env-model")
        monkeypatch.setenv("SIM_LLM_BASE_URL", "https://custom.example.com/v1")
        client = build_llm_client()
        assert isinstance(client, OpenAICompatibleLLMClient)

    def test_explicit_args_override_env(self, monkeypatch):
        from src.simulation.providers import build_llm_client, OpenAICompatibleLLMClient
        monkeypatch.setenv("SIM_LLM_PROVIDER", "echo")  # env says echo
        client = build_llm_client(
            provider="openai_compatible",               # arg overrides
            api_key="sk-override",
            model="override-model",
        )
        assert isinstance(client, OpenAICompatibleLLMClient)

    def test_providers_importable_without_db(self):
        import importlib
        mod = importlib.import_module("src.simulation.providers")
        assert hasattr(mod, "build_llm_client")
        assert hasattr(mod, "OpenAICompatibleLLMClient")


# ===========================================================================
# 8. OpenAICompatibleLLMClient – mocked httpx
# ===========================================================================

class TestOpenAICompatibleLLMClient:
    def _client(self):
        from src.simulation.providers import OpenAICompatibleLLMClient
        return OpenAICompatibleLLMClient(
            api_key="sk-test",
            model="gpt-4o-mini",
            base_url="https://api.example.com/v1",
            timeout=10,
        )

    def _messages(self):
        return [
            {"role": "system", "content": "system text"},
            {"role": "user",   "content": "user question"},
        ]

    def test_generate_returns_string_on_success(self, monkeypatch):
        import httpx
        from unittest.mock import MagicMock
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {
            "choices": [{"message": {"content": "mock reply"}}]
        }
        monkeypatch.setattr(httpx, "post", lambda *a, **kw: mock_resp)
        result = self._client().generate(self._messages())
        assert result == "mock reply"

    def test_http_error_raises_runtime_error(self, monkeypatch):
        import httpx
        from unittest.mock import MagicMock
        mock_resp = MagicMock()
        mock_resp.status_code = 401
        mock_resp.text = "Unauthorized"
        monkeypatch.setattr(httpx, "post", lambda *a, **kw: mock_resp)
        with pytest.raises(RuntimeError, match="HTTP 401"):
            self._client().generate(self._messages())

    def test_timeout_raises_runtime_error(self, monkeypatch):
        import httpx
        monkeypatch.setattr(
            httpx, "post",
            lambda *a, **kw: (_ for _ in ()).throw(httpx.TimeoutException("t/o")),
        )
        with pytest.raises(RuntimeError, match="timed out"):
            self._client().generate(self._messages())

    def test_request_error_raises_runtime_error(self, monkeypatch):
        import httpx
        monkeypatch.setattr(
            httpx, "post",
            lambda *a, **kw: (_ for _ in ()).throw(
                httpx.RequestError("conn refused", request=None)
            ),
        )
        with pytest.raises(RuntimeError, match="request failed"):
            self._client().generate(self._messages())

    def test_malformed_response_raises_runtime_error(self, monkeypatch):
        import httpx
        from unittest.mock import MagicMock
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"unexpected": "shape"}
        mock_resp.text = '{"unexpected":"shape"}'
        monkeypatch.setattr(httpx, "post", lambda *a, **kw: mock_resp)
        with pytest.raises(RuntimeError, match="Unexpected LLM response"):
            self._client().generate(self._messages())

    def test_api_key_not_in_exception_message(self, monkeypatch):
        """API key must never appear in raised exceptions."""
        import httpx
        from unittest.mock import MagicMock
        mock_resp = MagicMock()
        mock_resp.status_code = 403
        mock_resp.text = "Forbidden"
        monkeypatch.setattr(httpx, "post", lambda *a, **kw: mock_resp)
        try:
            self._client().generate(self._messages())
        except RuntimeError as exc:
            assert "sk-test" not in str(exc)

    def test_content_null_raises_clear_runtime_error(self, monkeypatch):
        """content=null in response (e.g. tool_calls) must raise a clear RuntimeError."""
        import httpx
        from unittest.mock import MagicMock
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {
            "choices": [{"message": {"content": None}}]
        }
        monkeypatch.setattr(httpx, "post", lambda *a, **kw: mock_resp)
        with pytest.raises(RuntimeError, match="content=null"):
            self._client().generate(self._messages())


class TestProviderFactoryTimeout:
    def test_invalid_timeout_env_raises_clear_error(self, monkeypatch):
        """Non-integer SIM_LLM_TIMEOUT_SECONDS must raise ValueError with context."""
        from src.simulation.providers import build_llm_client
        monkeypatch.setenv("SIM_LLM_TIMEOUT_SECONDS", "not-a-number")
        with pytest.raises(ValueError, match="SIM_LLM_TIMEOUT_SECONDS"):
            build_llm_client(
                provider="openai_compatible",
                api_key="sk-test",
                model="gpt-4o-mini",
            )

    def test_valid_timeout_env_accepted(self, monkeypatch):
        """Integer SIM_LLM_TIMEOUT_SECONDS should be accepted without error."""
        from src.simulation.providers import build_llm_client, OpenAICompatibleLLMClient
        monkeypatch.setenv("SIM_LLM_TIMEOUT_SECONDS", "30")
        client = build_llm_client(
            provider="openai_compatible",
            api_key="sk-test",
            model="gpt-4o-mini",
        )
        assert isinstance(client, OpenAICompatibleLLMClient)


# ===========================================================================
# 9. stats summary in prompt
# ===========================================================================

class TestStatsSummaryInPrompt:
    def _build_with_stats(self, stats):
        return build_prompt(
            display_name="Alice",
            persona_summary=None,
            traits={},
            stats=stats,
            historical_messages=[],
            current_message="test",
        )

    def _system(self, msgs):
        return next(m["content"] for m in msgs if m["role"] == "system")

    def test_message_count_appears_in_system(self):
        sys_txt = self._system(self._build_with_stats({"message_count": 42}))
        assert "42" in sys_txt

    def test_top_keywords_appear_in_system(self):
        stats = {"top_keywords": [{"word": "unique-kw-XYZ", "count": 5}]}
        sys_txt = self._system(self._build_with_stats(stats))
        assert "unique-kw-XYZ" in sys_txt

    def test_topic_distribution_appears_in_system(self):
        stats = {"topic_distribution": {"gaming-unique": 10, "casual_chat": 3}}
        sys_txt = self._system(self._build_with_stats(stats))
        assert "gaming-unique" in sys_txt

    def test_active_hours_appear_in_system(self):
        stats = {"active_hours": {"22": 15, "23": 10}}
        sys_txt = self._system(self._build_with_stats(stats))
        assert "22" in sys_txt

    def test_empty_stats_no_crash(self):
        msgs = self._build_with_stats({})
        assert len(msgs) == 2

    def test_partial_stats_no_crash(self):
        # Only some fields present – should render what's there, skip the rest
        msgs = self._build_with_stats({"message_count": 5})
        sys_txt = self._system(msgs)
        assert "5" in sys_txt

    def test_stats_block_absent_when_all_empty(self):
        # Empty dict → no stats block rendered
        sys_txt = self._system(self._build_with_stats({}))
        assert "统计摘要" not in sys_txt

    def test_stats_block_present_when_has_data(self):
        sys_txt = self._system(self._build_with_stats({"message_count": 1}))
        assert "统计摘要" in sys_txt


# ===========================================================================
# 10. member_id consistency guard in SimulationService
# ===========================================================================

class TestMemberIdConsistencyGuard:
    """
    Verify that SimulationService rejects simulation when
    profile_snapshot.member_id != legend_member.member_id.
    """

    def test_mismatched_snapshot_member_id_raises(self):
        """
        Seed a legend_member whose source_profile_snapshot_id points to a
        snapshot that belongs to a *different* member, then confirm the
        service raises ValueError before generating any prompt.
        """
        from src.simulation.providers import build_llm_client

        s = _sess()

        # Create a second member to own the mismatched snapshot
        other_member_id = uuid.uuid4()
        s.add(Member(
            id=other_member_id, group_id=_GROUP_ID,
            external_member_id="ext-other-mismatch",
            display_name="OtherMember",
            status="left", created_at=_NOW, updated_at=_NOW,
        ))
        s.flush()

        # Snapshot that belongs to other_member
        mismatch_snap_id = uuid.uuid4()
        s.add(ProfileSnapshot(
            id=mismatch_snap_id, group_id=_GROUP_ID,
            member_id=other_member_id,          # ← wrong member
            profile_version="profile_v1",
            snapshot_at=_NOW,
            window_start=_WIN_START, window_end=_WIN_END,
            source_message_count=1,
            persona_summary="mismatch-snap",
            traits={}, stats={},
            created_at=_NOW,
        ))
        s.flush()

        # A fifth member for this legend_member
        mismatch_member_id = uuid.uuid4()
        s.add(Member(
            id=mismatch_member_id, group_id=_GROUP_ID,
            external_member_id="ext-mismatch-lm",
            display_name="MismatchLM",
            status="left", created_at=_NOW, updated_at=_NOW,
        ))
        s.flush()

        mismatch_lm_id = uuid.uuid4()
        s.add(LegendMember(
            id=mismatch_lm_id,
            member_id=mismatch_member_id,       # ← different from snapshot's owner
            group_id=_GROUP_ID,
            archive_status="archived",
            archived_at=_NOW,
            source_profile_snapshot_id=mismatch_snap_id,
            member_display_name_snapshot="MismatchLM",
            member_external_id_snapshot="ext-mismatch-lm",
            member_status_snapshot="left",
            simulation_enabled=True,
            created_at=_NOW, updated_at=_NOW,
        ))
        s.flush()
        s.commit()

        svc = SimulationService(
            db_session=_sess(),
            llm_client=EchoLLMClient(),
        )
        with pytest.raises(ValueError, match="integrity error"):
            svc.generate_once(SimulationRequest(
                legend_member_id=mismatch_lm_id,
                current_message="test",
            ))
