"""
Result types for Stage 1 pipeline stages and the overall pipeline run.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Literal, Optional


# ── Per-stage result ──────────────────────────────────────────────────────────

StageStatus = Literal["success", "skipped", "failed"]


@dataclass
class IngestStageResult:
    status: StageStatus
    messages_inserted: int = 0
    messages_skipped_duplicate: int = 0
    members_created: int = 0
    error_message: Optional[str] = None


@dataclass
class TopicsInitStageResult:
    status: StageStatus
    topics_inserted: int = 0
    topics_already_existed: int = 0
    error_message: Optional[str] = None


@dataclass
class ClassificationStageResult:
    status: StageStatus
    messages_processed: int = 0
    messages_skipped_already_classified: int = 0
    messages_unmatched: int = 0
    topic_assignments_written: int = 0
    missing_topic_assignments: int = 0
    error_message: Optional[str] = None


@dataclass
class ProfilingStageResult:
    status: StageStatus
    members_attempted: int = 0
    profiles_written: int = 0
    profiles_skipped: int = 0
    profiles_failed: int = 0
    missing_topic_count: int = 0
    failed_member_ids: List[str] = field(default_factory=list)
    error_message: Optional[str] = None


# ── Overall pipeline result ───────────────────────────────────────────────────

OverallStatus = Literal["success", "partial", "failed", "dry_run"]


@dataclass
class PipelineResult:
    """
    Aggregated result of a Stage 1 pipeline run.

    overall_status:
        "success"  – all non-skipped stages completed without error.
        "partial"  – at least one stage failed but continue_on_error=True
                     allowed subsequent stages to run.
        "failed"   – at least one stage failed and the pipeline was aborted.
        "dry_run"  – dry_run=True; no stages were executed.
    """

    run_id: str                              # e.g. "20260115T143022"
    overall_status: OverallStatus

    ingest: IngestStageResult
    topics_init: TopicsInitStageResult
    classification: ClassificationStageResult
    profiling: ProfilingStageResult

    @property
    def any_failed(self) -> bool:
        return any(
            s.status == "failed"
            for s in (self.ingest, self.topics_init, self.classification, self.profiling)
        )
