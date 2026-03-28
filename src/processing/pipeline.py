"""
Stage 1 pipeline orchestrator.

Responsibilities
----------------
- Sequence the four stages: ingest → topics_init → classification → profiling.
- Pass parameters to each underlying service; never re-implement their logic.
- Return a PipelineResult with per-stage outcomes and an overall status.

Error-handling strategy
-----------------------
Default (continue_on_error=False):
    The first stage failure aborts all subsequent stages.  Remaining stages
    are recorded as "skipped" with an explanatory error_message.

continue_on_error=True:
    All stages are attempted regardless of earlier failures.
    overall_status is set to "partial" if any stage failed.

Topics initialisation
---------------------
topics_init always runs before classification (unless classification is
skipped).  It is idempotent: existing topics are left untouched.  This
removes the need for users to manually run init_topics.py before the
pipeline.  If topics_init fails, classification is aborted (it would fail
anyway without topics in the DB).
"""

from __future__ import annotations

import traceback
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from sqlalchemy import select

from src.classification.classification_service import ClassificationService
from src.classification.topic_rules import TOPICS
from src.db.models import Topic
from src.db.session import SessionLocal
from src.ingest.services.ingest_service import IngestService
from src.processing.pipeline_result import (
    ClassificationStageResult,
    IngestStageResult,
    OverallStatus,
    PipelineResult,
    ProfilingStageResult,
    TopicsInitStageResult,
)
from src.processing.pipeline_types import PipelineParams
from src.profiling.profile_service import ProfileService


def _make_run_id() -> str:
    """Generate a short timestamp-based run ID for log correlation."""
    return datetime.now(tz=timezone.utc).strftime("%Y%m%dT%H%M%S")


# ── Stage implementations ─────────────────────────────────────────────────────

def _run_ingest(params: PipelineParams) -> IngestStageResult:
    """Execute the ingest stage."""
    if params.input_file is None:
        return IngestStageResult(
            status="failed",
            error_message="input_file is required when skip_ingest=False.",
        )

    file_path = Path(params.input_file)
    if not file_path.exists():
        return IngestStageResult(
            status="failed",
            error_message=f"File not found: {file_path}",
        )

    try:
        service = IngestService()
        result = service.ingest_file(
            file_path,
            platform_hint=params.platform,
            group_name_hint=params.group_name,
        )
        return IngestStageResult(
            status="success",
            messages_inserted=result.messages_inserted,
            messages_skipped_duplicate=result.messages_skipped_duplicate,
            members_created=result.members_created,
        )
    except Exception as exc:
        return IngestStageResult(
            status="failed",
            error_message=f"{type(exc).__name__}: {exc}\n{traceback.format_exc()}",
        )


def _run_topics_init() -> TopicsInitStageResult:
    """
    Ensure all topics from topic_rules.TOPICS exist in the DB.

    Idempotent: existing topics are skipped, not overwritten.
    """
    session = None
    try:
        session = SessionLocal()
        inserted = 0
        skipped = 0
        for td in TOPICS:
            existing = session.execute(
                select(Topic).where(Topic.topic_key == td.topic_key)
            ).scalar_one_or_none()
            if existing:
                skipped += 1
                continue
            session.add(Topic(
                topic_key=td.topic_key,
                name=td.name,
                description=td.description,
                is_active=True,
            ))
            inserted += 1
        session.commit()
        return TopicsInitStageResult(
            status="success",
            topics_inserted=inserted,
            topics_already_existed=skipped,
        )
    except Exception as exc:
        if session is not None:
            session.rollback()
        return TopicsInitStageResult(
            status="failed",
            error_message=f"{type(exc).__name__}: {exc}\n{traceback.format_exc()}",
        )
    finally:
        if session is not None:
            session.close()


def _run_classification(params: PipelineParams) -> ClassificationStageResult:
    """Execute the classification stage."""
    try:
        service = ClassificationService()
        result = service.run(
            classifier_version=params.classifier_version,
            rerun=params.rerun_classification,
            group_id=params.group_id,
        )
        return ClassificationStageResult(
            status="success",
            messages_processed=result.messages_processed,
            messages_skipped_already_classified=result.messages_skipped_already_classified,
            messages_unmatched=result.messages_unmatched,
            topic_assignments_written=result.topic_assignments_written,
            missing_topic_assignments=result.missing_topic_assignments,
        )
    except Exception as exc:
        return ClassificationStageResult(
            status="failed",
            error_message=f"{type(exc).__name__}: {exc}\n{traceback.format_exc()}",
        )


def _run_profiling(params: PipelineParams) -> ProfilingStageResult:
    """Execute the profiling stage."""
    if params.window_start is None or params.window_end is None:
        return ProfilingStageResult(
            status="failed",
            error_message=(
                "window_start and window_end are required for profiling. "
                "Use fixed ISO-8601 UTC values (e.g. 2000-01-01T00:00:00Z / "
                "2099-12-31T23:59:59Z for all-time)."
            ),
        )
    if params.window_start >= params.window_end:
        return ProfilingStageResult(
            status="failed",
            error_message="window_start must be before window_end.",
        )

    try:
        service = ProfileService()
        result = service.run(
            profile_version=params.profile_version,
            classifier_version=params.classifier_version,
            window_start=params.window_start,
            window_end=params.window_end,
            group_id=params.group_id,
            member_id=params.member_id,
            rerun=params.rerun_profiling,
        )
        return ProfilingStageResult(
            status="success",
            members_attempted=result.members_attempted,
            profiles_written=result.profiles_written,
            profiles_skipped=result.profiles_skipped,
            profiles_failed=result.profiles_failed,
            missing_topic_count=result.missing_topic_count,
            failed_member_ids=result.failed_member_ids,
        )
    except Exception as exc:
        return ProfilingStageResult(
            status="failed",
            error_message=f"{type(exc).__name__}: {exc}\n{traceback.format_exc()}",
        )


def _skipped_ingest(reason: str) -> IngestStageResult:
    return IngestStageResult(status="skipped", error_message=reason)


def _skipped_topics_init(reason: str) -> TopicsInitStageResult:
    return TopicsInitStageResult(status="skipped", error_message=reason)


def _skipped_classification(reason: str) -> ClassificationStageResult:
    return ClassificationStageResult(status="skipped", error_message=reason)


def _skipped_profiling(reason: str) -> ProfilingStageResult:
    return ProfilingStageResult(status="skipped", error_message=reason)


# ── Main entry point ──────────────────────────────────────────────────────────

def run_stage1_pipeline(params: PipelineParams) -> PipelineResult:
    """
    Execute the Stage 1 processing pipeline.

    Stages (in order):
        1. ingest          – import chat log file into DB
        2. topics_init     – ensure topic taxonomy is seeded (idempotent)
        3. classification  – assign topics to messages
        4. profiling       – generate Persona Profile snapshots

    Args:
        params: PipelineParams controlling which stages run and how.

    Returns:
        PipelineResult with per-stage outcomes and overall_status.
    """
    run_id = _make_run_id()
    abort_reason: Optional[str] = None  # set when a stage fails and we must abort

    # ── dry_run shortcut ──────────────────────────────────────────────────────
    if params.dry_run:
        stages = []
        if not params.skip_ingest:
            stages.append(f"  ingest          → {params.input_file} (platform={params.platform})")
        else:
            stages.append("  ingest          → SKIPPED")
        if not params.skip_classification:
            stages.append(
                f"  topics_init     → seed topics (idempotent)\n"
                f"  classification  → version={params.classifier_version}"
                f", rerun={params.rerun_classification}"
                f", group_id={params.group_id}"
            )
        else:
            stages.append("  topics_init     → SKIPPED (classification skipped)\n"
                          "  classification  → SKIPPED")
        if not params.skip_profiling:
            stages.append(
                f"  profiling       → version={params.profile_version}"
                f", window={params.window_start} → {params.window_end}"
                f", rerun={params.rerun_profiling}"
            )
        else:
            stages.append("  profiling       → SKIPPED")

        print(f"[dry_run] Pipeline run_id={run_id}")
        print("[dry_run] Stages that would execute:")
        for s in stages:
            print(s)

        return PipelineResult(
            run_id=run_id,
            overall_status="dry_run",
            ingest=IngestStageResult(status="skipped", error_message="dry_run"),
            topics_init=TopicsInitStageResult(status="skipped", error_message="dry_run"),
            classification=ClassificationStageResult(status="skipped", error_message="dry_run"),
            profiling=ProfilingStageResult(status="skipped", error_message="dry_run"),
        )

    # ── Stage 1: ingest ───────────────────────────────────────────────────────
    if params.skip_ingest:
        ingest_result = _skipped_ingest("skip_ingest=True")
    elif abort_reason:
        ingest_result = _skipped_ingest(f"aborted: {abort_reason}")
    else:
        ingest_result = _run_ingest(params)
        if ingest_result.status == "failed" and not params.continue_on_error:
            abort_reason = f"ingest failed: {ingest_result.error_message}"

    # ── Stage 2: topics_init ──────────────────────────────────────────────────
    # Always run before classification unless classification itself is skipped.
    if params.skip_classification:
        topics_init_result = _skipped_topics_init("classification skipped")
    elif abort_reason:
        topics_init_result = _skipped_topics_init(f"aborted: {abort_reason}")
    else:
        topics_init_result = _run_topics_init()
        if topics_init_result.status == "failed" and not params.continue_on_error:
            abort_reason = f"topics_init failed: {topics_init_result.error_message}"

    # ── Stage 3: classification ───────────────────────────────────────────────
    if params.skip_classification:
        classification_result = _skipped_classification("skip_classification=True")
    elif abort_reason:
        classification_result = _skipped_classification(f"aborted: {abort_reason}")
    else:
        classification_result = _run_classification(params)
        if classification_result.status == "failed" and not params.continue_on_error:
            abort_reason = f"classification failed: {classification_result.error_message}"

    # ── Stage 4: profiling ────────────────────────────────────────────────────
    if params.skip_profiling:
        profiling_result = _skipped_profiling("skip_profiling=True")
    elif abort_reason:
        profiling_result = _skipped_profiling(f"aborted: {abort_reason}")
    else:
        profiling_result = _run_profiling(params)

    # ── Determine overall status ──────────────────────────────────────────────
    results = [ingest_result, topics_init_result, classification_result, profiling_result]
    failed_count = sum(1 for r in results if r.status == "failed")

    if failed_count == 0:
        overall: OverallStatus = "success"
    elif params.continue_on_error and failed_count > 0:
        overall = "partial"
    else:
        overall = "failed"

    return PipelineResult(
        run_id=run_id,
        overall_status=overall,
        ingest=ingest_result,
        topics_init=topics_init_result,
        classification=classification_result,
        profiling=profiling_result,
    )
