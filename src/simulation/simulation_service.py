"""
SimulationService – orchestrates a single stateless simulation round.

Transaction model
-----------------
If db_session is None, the service creates its own SessionLocal() and
closes it after each operation (read-only: no commit needed).
Pass an explicit session when the caller controls the transaction.

generate_once() pipeline
------------------------
1. Load LegendMember by legend_member_id → 404-style ValueError if absent
2. policy guard (assert_can_simulate)     → SimulationNotAllowedError
3. Load ProfileSnapshot by source_profile_snapshot_id (exact anchor, NOT latest)
4. Sample member messages within snapshot window
5. Build prompt_messages                  → pure, no DB
6. Call llm_client.generate()            → no DB, no writes
7. Return SimulationResult

All steps are read-only.  The service never writes to any table.
"""

from __future__ import annotations

import uuid
from typing import Optional

from sqlalchemy.orm import Session

from src.db.session import SessionLocal
from src.simulation import simulation_repository as repo
from src.simulation.llm_client import EchoLLMClient, LLMClient
from src.simulation.prompt_builder import build_prompt
from src.simulation.simulation_policy import SimulationNotAllowedError, assert_can_simulate
from src.simulation.simulation_schemas import (
    HistoricalMessage,
    SimulationInput,
    SimulationRequest,
    SimulationResult,
    SIMULATION_VERSION,
)


class SimulationService:
    """
    Orchestrates Persona Simulation for a single stateless round.

    Usage::

        service = SimulationService()
        result = service.generate_once(SimulationRequest(
            legend_member_id=some_uuid,
            current_message="你最近在玩什么游戏？",
        ))
    """

    def __init__(
        self,
        db_session: Optional[Session] = None,
        llm_client: Optional[LLMClient] = None,
    ):
        self._session: Optional[Session] = db_session
        self._owns_session = db_session is None
        self._llm_client: LLMClient = llm_client or EchoLLMClient()

    def _get_session(self) -> Session:
        if self._session is None:
            self._session = SessionLocal()
        return self._session

    def _close_session(self) -> None:
        """Close session if owned by this service (read-only: no commit needed)."""
        if not self._owns_session or self._session is None:
            return
        try:
            self._session.close()
        finally:
            self._session = None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def generate_once(self, request: SimulationRequest) -> SimulationResult:
        """
        Run a single stateless simulation round.

        Args:
            request: SimulationRequest with legend_member_id and current_message.

        Returns:
            SimulationResult

        Raises:
            ValueError:               legend_member_id not found.
            SimulationNotAllowedError: policy guard failed.
        """
        session = self._get_session()
        try:
            # Step 1: load LegendMember
            lm = repo.get_legend_member_by_id(session, request.legend_member_id)
            if lm is None:
                raise ValueError(
                    f"Legend member {request.legend_member_id} not found. "
                    f"Archive the member first."
                )

            # Step 2: policy guard
            assert_can_simulate(
                archive_status=lm.archive_status,
                simulation_enabled=lm.simulation_enabled,
                source_profile_snapshot_id=lm.source_profile_snapshot_id,
            )

            # Step 3: load profile snapshot (exact anchor – never latest)
            snapshot = repo.get_profile_snapshot_by_id(
                session, lm.source_profile_snapshot_id
            )
            if snapshot is None:
                raise ValueError(
                    f"Profile snapshot {lm.source_profile_snapshot_id} not found. "
                    f"The anchored snapshot may have been deleted."
                )

            # Step 3a: member_id consistency guard
            # The snapshot must belong to the same member as the legend record.
            # A mismatch indicates data corruption; abort rather than simulate
            # with a mismatched persona.
            if snapshot.member_id != lm.member_id:
                raise ValueError(
                    f"Data integrity error: profile snapshot "
                    f"{lm.source_profile_snapshot_id} belongs to member "
                    f"{snapshot.member_id}, but legend member "
                    f"{request.legend_member_id} references member "
                    f"{lm.member_id}. Refusing to simulate."
                )

            # Step 4: sample messages within the snapshot window
            raw_messages = repo.sample_member_messages(
                session,
                member_id=lm.member_id,
                window_start=snapshot.window_start,
                window_end=snapshot.window_end,
                limit=request.history_limit,
            )
            historical = [
                HistoricalMessage(
                    sent_at=m.sent_at,
                    content=m.content,
                    normalized_content=m.normalized_content,
                )
                for m in raw_messages
            ]

            # Step 5: build prompt
            prompt_messages = build_prompt(
                display_name=lm.member_display_name_snapshot,
                persona_summary=snapshot.persona_summary,
                traits=snapshot.traits or {},
                stats=snapshot.stats or {},
                historical_messages=historical,
                current_message=request.current_message,
                conversation_context=request.conversation_context,
            )

            # Step 6: call LLM client (EchoLLMClient by default – no network)
            response_text = self._llm_client.generate(prompt_messages)

            # Step 7: return result
            return SimulationResult(
                legend_member_id=request.legend_member_id,
                profile_snapshot_id=lm.source_profile_snapshot_id,
                prompt_messages=prompt_messages,
                response_text=response_text,
                simulation_version=SIMULATION_VERSION,
            )

        finally:
            self._close_session()
