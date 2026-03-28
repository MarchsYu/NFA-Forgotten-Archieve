"""
Pipeline input parameter types for Stage 1 orchestration.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Optional

from src.classification.topic_classifier import CLASSIFIER_VERSION
from src.profiling.profile_builder import PROFILE_VERSION


@dataclass
class PipelineParams:
    """
    All parameters needed to run the Stage 1 pipeline.

    Stage control
    -------------
    skip_ingest / skip_classification / skip_profiling:
        Set True to bypass a stage entirely.  Useful when you've already run
        one stage and only want to re-run a later one.

    rerun_classification / rerun_profiling:
        Delete existing results for the given version and re-process from scratch.
        Ignored when the corresponding stage is skipped.

    Scope filters
    -------------
    group_id / member_id:
        Restrict classification and profiling to a single group or member.
        member_id requires group_id.
        Has no effect on the ingest stage (ingest always uses input_file).

    Profiling window
    ----------------
    window_start / window_end are REQUIRED when profiling is not skipped.
    They must be fixed, explicit values to guarantee idempotency.
    Use 2000-01-01T00:00:00Z / 2099-12-31T23:59:59Z for "all-time".

    Optional enhancements
    ---------------------
    dry_run:
        Print which stages would run without executing anything.
    continue_on_error:
        If False (default), abort remaining stages on first failure.
        If True, continue and report all stage outcomes in the summary.
    """

    # ── Ingest ────────────────────────────────────────────────────────────────
    input_file: Optional[Path] = None
    platform: Optional[str] = None
    group_name: Optional[str] = None       # required by TXT parser

    # ── Classification ────────────────────────────────────────────────────────
    classifier_version: str = field(default_factory=lambda: CLASSIFIER_VERSION)
    rerun_classification: bool = False

    # ── Profiling ─────────────────────────────────────────────────────────────
    profile_version: str = field(default_factory=lambda: PROFILE_VERSION)
    window_start: Optional[datetime] = None
    window_end: Optional[datetime] = None
    rerun_profiling: bool = False

    # ── Scope filters (classification + profiling) ────────────────────────────
    group_id: Optional[uuid.UUID] = None
    member_id: Optional[uuid.UUID] = None

    # ── Stage control ─────────────────────────────────────────────────────────
    skip_ingest: bool = False
    skip_classification: bool = False
    skip_profiling: bool = False

    # ── Optional enhancements ─────────────────────────────────────────────────
    dry_run: bool = False
    continue_on_error: bool = False
