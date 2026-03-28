"""
Stage 1 processing orchestration layer.

Public API
----------
    from src.processing import PipelineParams, PipelineResult, run_stage1_pipeline

    result = run_stage1_pipeline(PipelineParams(
        input_file=Path("data/raw/chat.json"),
        platform="qq",
        window_start=datetime(2026, 1, 1, tzinfo=timezone.utc),
        window_end=datetime(2026, 12, 31, 23, 59, 59, tzinfo=timezone.utc),
    ))
"""

from src.processing.pipeline import run_stage1_pipeline
from src.processing.pipeline_result import (
    ClassificationStageResult,
    IngestStageResult,
    OverallStatus,
    PipelineResult,
    ProfilingStageResult,
    StageStatus,
    TopicsInitStageResult,
)
from src.processing.pipeline_types import PipelineParams

__all__ = [
    "run_stage1_pipeline",
    "PipelineParams",
    "PipelineResult",
    "IngestStageResult",
    "TopicsInitStageResult",
    "ClassificationStageResult",
    "ProfilingStageResult",
    "StageStatus",
    "OverallStatus",
]
