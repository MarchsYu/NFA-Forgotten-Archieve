"""
Stage-1 pipeline orchestrator for NFA Forgotten Archive.

Stages (in order)
-----------------
1. ingest        – parse a chat log file and write messages to the DB
2. topics_init   – seed the topics table (idempotent)
3. classification – classify messages by topic
4. profiling     – build Persona Profile snapshots

Each stage can be skipped individually via ``PipelineParams.skip_stages``.
Any stage can be re-run from scratch via ``PipelineParams.rerun``.

Entry point
-----------
    from src.processing.pipeline import PipelineParams, run_stage1_pipeline

    params = PipelineParams(
        chat_file=Path("export.json"),
        window_start=datetime(2026, 1, 1, tzinfo=timezone.utc),
        window_end=datetime(2026, 12, 31, 23, 59, 59, tzinfo=timezone.utc),
    )
    result = run_stage1_pipeline(params)

run_id
------
Generated as ``YYYYMMDDTHHMMSS_<6-char hex>`` (UTC timestamp + random suffix).
The random suffix prevents collisions when two runs start within the same second.

Error handling
--------------
Each stage result carries an ``error_summary`` (short human-readable string).
Full tracebacks are emitted via ``logging`` at ERROR level and are NOT stored
in the result objects, keeping them clean for API / task-system consumers.

Topic init – single canonical implementation
--------------------------------------------
``_run_topics_init()`` delegates entirely to
``src.classification.topic_service.init_topics()``.
``scripts/init_topics.py`` calls the same function.
There is no second implementation of topic seeding logic anywhere.
"""

from __future__ import annotations

import logging
import traceback
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional

from src.classification.classification_service import ClassificationService, ClassificationResult
from src.classification.topic_classifier import CLASSIFIER_VERSION
from src.classification.topic_service import init_topics, TopicInitResult
from src.ingest.services.ingest_service import IngestService
from src.profiling.profile_builder import PROFILE_VERSION
from src.profiling.profile_service import ProfileService, ProfilingResult

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Stage name constants
# ---------------------------------------------------------------------------
STAGE_INGEST = "ingest"
STAGE_TOPICS_INIT = "topics_init"
STAGE_CLASSIFICATION = "classification"
STAGE_PROFILING = "profiling"

ALL_STAGES = [STAGE_INGEST, STAGE_TOPICS_INIT, STAGE_CLASSIFICATION, STAGE_PROFILING]


# ---------------------------------------------------------------------------
# Parameter object
# ---------------------------------------------------------------------------

@dataclass
class PipelineParams:
    """
    Parameters for a Stage-1 pipeline run.

    Required for profiling
    ----------------------
    ``window_start`` and ``window_end`` are required whenever the profiling
    stage runs (i.e. ``STAGE_PROFILING`` is not in ``skip_stages``).
    They must be fixed, explicit values for idempotency.

    member_id / group_id
    --------------------
    ``member_id`` requires ``group_id``.  Passing ``member_id`` without
    ``group_id`` raises ``ValueError`` at validation time.

    skip_stages / rerun
    -------------------
    ``skip_stages`` lists stage names to bypass entirely.
    ``rerun=True`` deletes existing results for classification and profiling
    before re-running them.  Combining ``rerun=True`` with a stage in
    ``skip_stages`` is allowed (the skip takes precedence for that stage).
    """

    # ── ingest ──────────────────────────────────────────────────────────────
    chat_file: Optional[Path] = None
    platform_hint: Optional[str] = None
    group_name_hint: Optional[str] = None

    # ── scope ───────────────────────────────────────────────────────────────
    group_id: Optional[uuid.UUID] = None
    member_id: Optional[uuid.UUID] = None

    # ── versions ────────────────────────────────────────────────────────────
    classifier_version: str = CLASSIFIER_VERSION
    profile_version: str = PROFILE_VERSION

    # ── profiling window (required when profiling stage runs) ───────────────
    window_start: Optional[datetime] = None
    window_end: Optional[datetime] = None

    # ── control ─────────────────────────────────────────────────────────────
    skip_stages: List[str] = field(default_factory=list)
    rerun: bool = False


# ---------------------------------------------------------------------------
# Per-stage result wrappers
# ---------------------------------------------------------------------------

@dataclass
class StageOutcome:
    """Outcome of a single pipeline stage."""
    stage: str
    skipped: bool = False
    success: bool = False
    error_summary: Optional[str] = None   # short message; no traceback


@dataclass
class PipelineResult:
    """Aggregated result of a full Stage-1 pipeline run."""
    run_id: str
    params: PipelineParams
    stages: List[StageOutcome] = field(default_factory=list)

    # Per-stage typed results (None if stage was skipped or failed)
    ingest_result: Optional[object] = None          # IngestResult
    topics_init_result: Optional[TopicInitResult] = None
    classification_result: Optional[ClassificationResult] = None
    profiling_result: Optional[ProfilingResult] = None

    @property
    def success(self) -> bool:
        return all(s.success or s.skipped for s in self.stages)

    @property
    def failed_stages(self) -> List[str]:
        return [s.stage for s in self.stages if not s.success and not s.skipped]


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

def _validate_params(params: PipelineParams) -> None:
    """
    Raise ``ValueError`` with a clear message for any invalid parameter
    combination.  Called at the very start of ``run_stage1_pipeline()``.
    """
    # member_id requires group_id
    if params.member_id is not None and params.group_id is None:
        raise ValueError(
            "member_id requires group_id. "
            "Pass --group-id (or set PipelineParams.group_id) alongside --member-id."
        )

    # profiling window required when profiling stage will run
    profiling_will_run = STAGE_PROFILING not in params.skip_stages
    if profiling_will_run:
        if params.window_start is None or params.window_end is None:
            raise ValueError(
                "window_start and window_end are required for the profiling stage. "
                "Pass --window-start and --window-end, or add 'profiling' to skip_stages."
            )
        if params.window_start >= params.window_end:
            raise ValueError(
                f"window_start ({params.window_start.isoformat()}) must be "
                f"before window_end ({params.window_end.isoformat()})."
            )

    # ingest stage requires chat_file
    ingest_will_run = STAGE_INGEST not in params.skip_stages
    if ingest_will_run and params.chat_file is None:
        raise ValueError(
            "chat_file is required for the ingest stage. "
            "Pass --chat-file, or add 'ingest' to skip_stages."
        )

    # unknown stage names in skip_stages
    unknown = set(params.skip_stages) - set(ALL_STAGES)
    if unknown:
        raise ValueError(
            f"Unknown stage name(s) in skip_stages: {sorted(unknown)}. "
            f"Valid stages: {ALL_STAGES}"
        )


# ---------------------------------------------------------------------------
# run_id generation
# ---------------------------------------------------------------------------

def _make_run_id() -> str:
    """
    Generate a run identifier: ``YYYYMMDDTHHMMSS_<6-char hex>``.

    The UTC timestamp provides human-readable ordering; the 6-char random
    hex suffix prevents collisions when two runs start within the same second.
    """
    ts = datetime.now(tz=timezone.utc).strftime("%Y%m%dT%H%M%S")
    suffix = uuid.uuid4().hex[:6]
    return f"{ts}_{suffix}"


# ---------------------------------------------------------------------------
# Stage runners
# ---------------------------------------------------------------------------

def _run_ingest(params: PipelineParams) -> tuple[StageOutcome, object]:
    """Run the ingest stage.  Returns (outcome, IngestResult | None)."""
    try:
        service = IngestService()
        result = service.ingest_file(
            file_path=params.chat_file,
            platform_hint=params.platform_hint,
            group_name_hint=params.group_name_hint,
        )
        logger.info(
            "[%s] ingest complete: %d inserted, %d skipped",
            STAGE_INGEST, result.messages_inserted, result.messages_skipped_duplicate,
        )
        return StageOutcome(stage=STAGE_INGEST, success=True), result
    except Exception as exc:
        summary = f"{type(exc).__name__}: {exc}"
        logger.error("[%s] failed\n%s", STAGE_INGEST, traceback.format_exc())
        return StageOutcome(stage=STAGE_INGEST, success=False, error_summary=summary), None


def _run_topics_init(params: PipelineParams) -> tuple[StageOutcome, Optional[TopicInitResult]]:
    """
    Run the topics_init stage.

    Delegates entirely to ``src.classification.topic_service.init_topics()``.
    This is the single canonical implementation; no topic seeding logic lives
    here.
    """
    try:
        result = init_topics()
        logger.info(
            "[%s] complete: %d inserted, %d skipped",
            STAGE_TOPICS_INIT, result.inserted, result.skipped,
        )
        return StageOutcome(stage=STAGE_TOPICS_INIT, success=True), result
    except Exception as exc:
        summary = f"{type(exc).__name__}: {exc}"
        logger.error("[%s] failed\n%s", STAGE_TOPICS_INIT, traceback.format_exc())
        return StageOutcome(stage=STAGE_TOPICS_INIT, success=False, error_summary=summary), None


def _run_classification(params: PipelineParams) -> tuple[StageOutcome, Optional[ClassificationResult]]:
    """Run the classification stage."""
    try:
        service = ClassificationService()
        result = service.run(
            classifier_version=params.classifier_version,
            rerun=params.rerun,
            group_id=params.group_id,
        )
        logger.info(
            "[%s] complete: %d processed, %d written",
            STAGE_CLASSIFICATION, result.messages_processed, result.topic_assignments_written,
        )
        return StageOutcome(stage=STAGE_CLASSIFICATION, success=True), result
    except Exception as exc:
        summary = f"{type(exc).__name__}: {exc}"
        logger.error("[%s] failed\n%s", STAGE_CLASSIFICATION, traceback.format_exc())
        return StageOutcome(stage=STAGE_CLASSIFICATION, success=False, error_summary=summary), None


def _run_profiling(params: PipelineParams) -> tuple[StageOutcome, Optional[ProfilingResult]]:
    """Run the profiling stage."""
    try:
        service = ProfileService()
        result = service.run(
            profile_version=params.profile_version,
            classifier_version=params.classifier_version,
            window_start=params.window_start,
            window_end=params.window_end,
            group_id=params.group_id,
            member_id=params.member_id,
            rerun=params.rerun,
        )
        logger.info(
            "[%s] complete: %d written, %d skipped, %d failed",
            STAGE_PROFILING, result.profiles_written, result.profiles_skipped, result.profiles_failed,
        )
        return StageOutcome(stage=STAGE_PROFILING, success=True), result
    except Exception as exc:
        summary = f"{type(exc).__name__}: {exc}"
        logger.error("[%s] failed\n%s", STAGE_PROFILING, traceback.format_exc())
        return StageOutcome(stage=STAGE_PROFILING, success=False, error_summary=summary), None


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def run_stage1_pipeline(params: PipelineParams) -> PipelineResult:
    """
    Execute the Stage-1 pipeline: ingest → topics_init → classification → profiling.

    Validation is performed upfront; any invalid parameter combination raises
    ``ValueError`` before any stage runs.

    Each stage is executed in order.  If a stage fails, subsequent stages that
    depend on it are still attempted (the pipeline is not aborted), but the
    overall ``PipelineResult.success`` will be ``False``.

    Args:
        params: ``PipelineParams`` controlling which stages run and how.

    Returns:
        ``PipelineResult`` with per-stage outcomes and typed result objects.

    Raises:
        ValueError: invalid parameter combination (raised before any DB work).
    """
    _validate_params(params)

    run_id = _make_run_id()
    logger.info("Pipeline run %s started", run_id)

    result = PipelineResult(run_id=run_id, params=params)

    # ── Stage 1: ingest ─────────────────────────────────────────────────────
    if STAGE_INGEST in params.skip_stages:
        result.stages.append(StageOutcome(stage=STAGE_INGEST, skipped=True, success=True))
        logger.info("[%s] skipped", STAGE_INGEST)
    else:
        outcome, ingest_result = _run_ingest(params)
        result.stages.append(outcome)
        result.ingest_result = ingest_result

    # ── Stage 2: topics_init ─────────────────────────────────────────────────
    if STAGE_TOPICS_INIT in params.skip_stages:
        result.stages.append(StageOutcome(stage=STAGE_TOPICS_INIT, skipped=True, success=True))
        logger.info("[%s] skipped", STAGE_TOPICS_INIT)
    else:
        outcome, topics_result = _run_topics_init(params)
        result.stages.append(outcome)
        result.topics_init_result = topics_result

    # ── Stage 3: classification ──────────────────────────────────────────────
    if STAGE_CLASSIFICATION in params.skip_stages:
        result.stages.append(StageOutcome(stage=STAGE_CLASSIFICATION, skipped=True, success=True))
        logger.info("[%s] skipped", STAGE_CLASSIFICATION)
    else:
        outcome, cls_result = _run_classification(params)
        result.stages.append(outcome)
        result.classification_result = cls_result

    # ── Stage 4: profiling ───────────────────────────────────────────────────
    if STAGE_PROFILING in params.skip_stages:
        result.stages.append(StageOutcome(stage=STAGE_PROFILING, skipped=True, success=True))
        logger.info("[%s] skipped", STAGE_PROFILING)
    else:
        outcome, prof_result = _run_profiling(params)
        result.stages.append(outcome)
        result.profiling_result = prof_result

    status = "SUCCESS" if result.success else f"FAILED stages: {result.failed_stages}"
    logger.info("Pipeline run %s finished – %s", run_id, status)
    return result
