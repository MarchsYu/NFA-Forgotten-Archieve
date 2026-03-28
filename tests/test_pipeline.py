"""
Unit tests for the Stage 1 processing pipeline.

All tests are pure in-memory – no DB connection required.
The pipeline's DB-touching stages (topics_init, classification, profiling)
are exercised via their error paths (no DB available) or by mocking the
underlying services.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from src.processing.pipeline import run_stage1_pipeline
from src.processing.pipeline_result import (
    ClassificationStageResult,
    IngestStageResult,
    PipelineResult,
    ProfilingStageResult,
    TopicsInitStageResult,
)
from src.processing.pipeline_types import PipelineParams


# ── Fixtures ──────────────────────────────────────────────────────────────────

WINDOW_START = datetime(2026, 1, 1, tzinfo=timezone.utc)
WINDOW_END = datetime(2026, 12, 31, 23, 59, 59, tzinfo=timezone.utc)


def _all_skip_params(**kwargs) -> PipelineParams:
    """Return params with all stages skipped (no DB needed)."""
    return PipelineParams(
        skip_ingest=True,
        skip_classification=True,
        skip_profiling=True,
        window_start=WINDOW_START,
        window_end=WINDOW_END,
        **kwargs,
    )


# ── PipelineParams defaults ───────────────────────────────────────────────────

class TestPipelineParamsDefaults:
    def test_classifier_version_default(self):
        from src.classification.topic_classifier import CLASSIFIER_VERSION
        p = PipelineParams()
        assert p.classifier_version == CLASSIFIER_VERSION

    def test_profile_version_default(self):
        from src.profiling.profile_builder import PROFILE_VERSION
        p = PipelineParams()
        assert p.profile_version == PROFILE_VERSION

    def test_all_skip_flags_default_false(self):
        p = PipelineParams()
        assert p.skip_ingest is False
        assert p.skip_classification is False
        assert p.skip_profiling is False

    def test_rerun_flags_default_false(self):
        p = PipelineParams()
        assert p.rerun_classification is False
        assert p.rerun_profiling is False

    def test_dry_run_default_false(self):
        p = PipelineParams()
        assert p.dry_run is False

    def test_continue_on_error_default_false(self):
        p = PipelineParams()
        assert p.continue_on_error is False


# ── All-skip: no DB needed ────────────────────────────────────────────────────

class TestAllSkip:
    def test_returns_pipeline_result(self):
        result = run_stage1_pipeline(_all_skip_params())
        assert isinstance(result, PipelineResult)

    def test_overall_status_success_when_all_skipped(self):
        result = run_stage1_pipeline(_all_skip_params())
        assert result.overall_status == "success"

    def test_all_stages_skipped(self):
        result = run_stage1_pipeline(_all_skip_params())
        assert result.ingest.status == "skipped"
        assert result.topics_init.status == "skipped"
        assert result.classification.status == "skipped"
        assert result.profiling.status == "skipped"

    def test_run_id_is_non_empty_string(self):
        result = run_stage1_pipeline(_all_skip_params())
        assert isinstance(result.run_id, str)
        assert len(result.run_id) > 0

    def test_any_failed_false_when_all_skipped(self):
        result = run_stage1_pipeline(_all_skip_params())
        assert result.any_failed is False


# ── Dry-run mode ──────────────────────────────────────────────────────────────

class TestDryRun:
    def test_dry_run_returns_dry_run_status(self, capsys):
        params = PipelineParams(
            input_file=Path("data/raw/chat.json"),
            platform="qq",
            window_start=WINDOW_START,
            window_end=WINDOW_END,
            dry_run=True,
        )
        result = run_stage1_pipeline(params)
        assert result.overall_status == "dry_run"

    def test_dry_run_all_stages_skipped(self, capsys):
        params = PipelineParams(
            input_file=Path("data/raw/chat.json"),
            platform="qq",
            window_start=WINDOW_START,
            window_end=WINDOW_END,
            dry_run=True,
        )
        result = run_stage1_pipeline(params)
        for stage in (result.ingest, result.topics_init, result.classification, result.profiling):
            assert stage.status == "skipped"

    def test_dry_run_prints_stage_info(self, capsys):
        params = PipelineParams(
            input_file=Path("data/raw/chat.json"),
            platform="qq",
            window_start=WINDOW_START,
            window_end=WINDOW_END,
            dry_run=True,
        )
        run_stage1_pipeline(params)
        captured = capsys.readouterr()
        assert "dry_run" in captured.out
        assert "ingest" in captured.out
        assert "classification" in captured.out
        assert "profiling" in captured.out


# ── Ingest stage ──────────────────────────────────────────────────────────────

class TestIngestStage:
    def test_ingest_fails_when_file_missing(self):
        params = PipelineParams(
            input_file=Path("/nonexistent/chat.json"),
            platform="qq",
            skip_classification=True,
            skip_profiling=True,
            window_start=WINDOW_START,
            window_end=WINDOW_END,
        )
        result = run_stage1_pipeline(params)
        assert result.ingest.status == "failed"
        assert "not found" in result.ingest.error_message.lower()

    def test_ingest_fails_when_no_input_file(self):
        params = PipelineParams(
            input_file=None,
            skip_classification=True,
            skip_profiling=True,
            window_start=WINDOW_START,
            window_end=WINDOW_END,
        )
        result = run_stage1_pipeline(params)
        assert result.ingest.status == "failed"
        assert "input_file" in result.ingest.error_message

    def test_ingest_failure_aborts_subsequent_stages_by_default(self):
        params = PipelineParams(
            input_file=Path("/nonexistent/chat.json"),
            platform="qq",
            window_start=WINDOW_START,
            window_end=WINDOW_END,
        )
        result = run_stage1_pipeline(params)
        assert result.ingest.status == "failed"
        assert result.topics_init.status == "skipped"
        assert result.classification.status == "skipped"
        assert result.profiling.status == "skipped"
        assert result.overall_status == "failed"

    def test_ingest_failure_continues_when_continue_on_error(self):
        params = PipelineParams(
            input_file=Path("/nonexistent/chat.json"),
            platform="qq",
            window_start=WINDOW_START,
            window_end=WINDOW_END,
            continue_on_error=True,
        )
        result = run_stage1_pipeline(params)
        assert result.ingest.status == "failed"
        # topics_init and classification will also fail (no DB), but they run
        assert result.topics_init.status in ("failed", "skipped")
        assert result.overall_status in ("partial", "failed")

    def test_ingest_skipped_when_skip_ingest_true(self):
        params = _all_skip_params()
        result = run_stage1_pipeline(params)
        assert result.ingest.status == "skipped"
        assert "skip_ingest=True" in result.ingest.error_message

    def test_ingest_success_with_mocked_service(self, tmp_path):
        """Verify ingest stage calls IngestService and maps result correctly."""
        fake_file = tmp_path / "chat.json"
        fake_file.write_text("{}")

        mock_ingest_result = MagicMock()
        mock_ingest_result.messages_inserted = 42
        mock_ingest_result.messages_skipped_duplicate = 3
        mock_ingest_result.members_created = 5

        with patch(
            "src.processing.pipeline.IngestService"
        ) as MockService:
            MockService.return_value.ingest_file.return_value = mock_ingest_result
            params = PipelineParams(
                input_file=fake_file,
                platform="qq",
                skip_classification=True,
                skip_profiling=True,
                window_start=WINDOW_START,
                window_end=WINDOW_END,
            )
            result = run_stage1_pipeline(params)

        assert result.ingest.status == "success"
        assert result.ingest.messages_inserted == 42
        assert result.ingest.messages_skipped_duplicate == 3
        assert result.ingest.members_created == 5


# ── Topics init stage ─────────────────────────────────────────────────────────

class TestTopicsInitStage:
    def test_topics_init_skipped_when_classification_skipped(self):
        params = _all_skip_params()
        result = run_stage1_pipeline(params)
        assert result.topics_init.status == "skipped"
        assert "classification skipped" in result.topics_init.error_message

    def test_topics_init_success_with_mocked_session(self):
        """topics_init runs before classification; mock the DB session."""
        with (
            patch("src.processing.pipeline.SessionLocal") as MockSession,
            patch("src.processing.pipeline.ClassificationService") as MockCls,
        ):
            mock_session = MagicMock()
            MockSession.return_value = mock_session
            mock_session.execute.return_value.scalar_one_or_none.return_value = None

            mock_cls_result = MagicMock()
            mock_cls_result.messages_processed = 10
            mock_cls_result.messages_skipped_already_classified = 0
            mock_cls_result.messages_unmatched = 1
            mock_cls_result.topic_assignments_written = 9
            mock_cls_result.missing_topic_assignments = 0
            MockCls.return_value.run.return_value = mock_cls_result

            params = PipelineParams(
                skip_ingest=True,
                skip_profiling=True,
                window_start=WINDOW_START,
                window_end=WINDOW_END,
            )
            result = run_stage1_pipeline(params)

        assert result.topics_init.status == "success"
        assert result.topics_init.topics_inserted >= 0
        assert result.topics_init.topics_already_existed >= 0


# ── Classification stage ──────────────────────────────────────────────────────

class TestClassificationStage:
    def test_classification_skipped_when_flag_set(self):
        params = _all_skip_params()
        result = run_stage1_pipeline(params)
        assert result.classification.status == "skipped"

    def test_classification_success_with_mocked_service(self):
        with (
            patch("src.processing.pipeline.SessionLocal") as MockSession,
            patch("src.processing.pipeline.ClassificationService") as MockCls,
        ):
            mock_session = MagicMock()
            MockSession.return_value = mock_session
            mock_session.execute.return_value.scalar_one_or_none.return_value = None

            mock_result = MagicMock()
            mock_result.messages_processed = 100
            mock_result.messages_skipped_already_classified = 20
            mock_result.messages_unmatched = 5
            mock_result.topic_assignments_written = 95
            mock_result.missing_topic_assignments = 0
            MockCls.return_value.run.return_value = mock_result

            params = PipelineParams(
                skip_ingest=True,
                skip_profiling=True,
                window_start=WINDOW_START,
                window_end=WINDOW_END,
            )
            result = run_stage1_pipeline(params)

        assert result.classification.status == "success"
        assert result.classification.messages_processed == 100
        assert result.classification.topic_assignments_written == 95

    def test_classification_passes_group_id_to_service(self):
        gid = uuid.uuid4()
        with (
            patch("src.processing.pipeline.SessionLocal") as MockSession,
            patch("src.processing.pipeline.ClassificationService") as MockCls,
        ):
            mock_session = MagicMock()
            MockSession.return_value = mock_session
            mock_session.execute.return_value.scalar_one_or_none.return_value = None

            mock_result = MagicMock()
            mock_result.messages_processed = 0
            mock_result.messages_skipped_already_classified = 0
            mock_result.messages_unmatched = 0
            mock_result.topic_assignments_written = 0
            mock_result.missing_topic_assignments = 0
            MockCls.return_value.run.return_value = mock_result

            params = PipelineParams(
                skip_ingest=True,
                skip_profiling=True,
                group_id=gid,
                window_start=WINDOW_START,
                window_end=WINDOW_END,
            )
            run_stage1_pipeline(params)

        MockCls.return_value.run.assert_called_once()
        call_kwargs = MockCls.return_value.run.call_args.kwargs
        assert call_kwargs.get("group_id") == gid

    def test_classification_passes_rerun_flag(self):
        with (
            patch("src.processing.pipeline.SessionLocal") as MockSession,
            patch("src.processing.pipeline.ClassificationService") as MockCls,
        ):
            mock_session = MagicMock()
            MockSession.return_value = mock_session
            mock_session.execute.return_value.scalar_one_or_none.return_value = None

            mock_result = MagicMock()
            mock_result.messages_processed = 0
            mock_result.messages_skipped_already_classified = 0
            mock_result.messages_unmatched = 0
            mock_result.topic_assignments_written = 0
            mock_result.missing_topic_assignments = 0
            MockCls.return_value.run.return_value = mock_result

            params = PipelineParams(
                skip_ingest=True,
                skip_profiling=True,
                rerun_classification=True,
                window_start=WINDOW_START,
                window_end=WINDOW_END,
            )
            run_stage1_pipeline(params)

        call_kwargs = MockCls.return_value.run.call_args.kwargs
        assert call_kwargs.get("rerun") is True


# ── Profiling stage ───────────────────────────────────────────────────────────

class TestProfilingStage:
    def test_profiling_skipped_when_flag_set(self):
        params = _all_skip_params()
        result = run_stage1_pipeline(params)
        assert result.profiling.status == "skipped"

    def test_profiling_fails_when_window_missing(self):
        params = PipelineParams(
            skip_ingest=True,
            skip_classification=True,
            window_start=None,
            window_end=None,
        )
        result = run_stage1_pipeline(params)
        assert result.profiling.status == "failed"
        assert "window_start" in result.profiling.error_message

    def test_profiling_fails_when_window_inverted(self):
        params = PipelineParams(
            skip_ingest=True,
            skip_classification=True,
            window_start=WINDOW_END,
            window_end=WINDOW_START,
        )
        result = run_stage1_pipeline(params)
        assert result.profiling.status == "failed"
        assert "before" in result.profiling.error_message

    def test_profiling_success_with_mocked_service(self):
        with patch("src.processing.pipeline.ProfileService") as MockProf:
            mock_result = MagicMock()
            mock_result.members_attempted = 10
            mock_result.profiles_written = 8
            mock_result.profiles_skipped = 2
            mock_result.profiles_failed = 0
            mock_result.missing_topic_count = 0
            mock_result.failed_member_ids = []
            MockProf.return_value.run.return_value = mock_result

            params = PipelineParams(
                skip_ingest=True,
                skip_classification=True,
                window_start=WINDOW_START,
                window_end=WINDOW_END,
            )
            result = run_stage1_pipeline(params)

        assert result.profiling.status == "success"
        assert result.profiling.members_attempted == 10
        assert result.profiling.profiles_written == 8
        assert result.profiling.profiles_skipped == 2

    def test_profiling_passes_window_to_service(self):
        with patch("src.processing.pipeline.ProfileService") as MockProf:
            mock_result = MagicMock()
            mock_result.members_attempted = 0
            mock_result.profiles_written = 0
            mock_result.profiles_skipped = 0
            mock_result.profiles_failed = 0
            mock_result.missing_topic_count = 0
            mock_result.failed_member_ids = []
            MockProf.return_value.run.return_value = mock_result

            params = PipelineParams(
                skip_ingest=True,
                skip_classification=True,
                window_start=WINDOW_START,
                window_end=WINDOW_END,
            )
            run_stage1_pipeline(params)

        call_kwargs = MockProf.return_value.run.call_args.kwargs
        assert call_kwargs["window_start"] == WINDOW_START
        assert call_kwargs["window_end"] == WINDOW_END


# ── Overall status logic ──────────────────────────────────────────────────────

class TestOverallStatus:
    def test_success_when_no_failures(self):
        result = run_stage1_pipeline(_all_skip_params())
        assert result.overall_status == "success"

    def test_failed_when_stage_fails_and_no_continue(self):
        params = PipelineParams(
            input_file=Path("/nonexistent/chat.json"),
            platform="qq",
            window_start=WINDOW_START,
            window_end=WINDOW_END,
            continue_on_error=False,
        )
        result = run_stage1_pipeline(params)
        assert result.overall_status == "failed"

    def test_partial_when_stage_fails_with_continue_on_error(self):
        params = PipelineParams(
            input_file=Path("/nonexistent/chat.json"),
            platform="qq",
            window_start=WINDOW_START,
            window_end=WINDOW_END,
            continue_on_error=True,
        )
        result = run_stage1_pipeline(params)
        assert result.overall_status == "partial"

    def test_any_failed_true_when_stage_failed(self):
        params = PipelineParams(
            input_file=Path("/nonexistent/chat.json"),
            platform="qq",
            window_start=WINDOW_START,
            window_end=WINDOW_END,
        )
        result = run_stage1_pipeline(params)
        assert result.any_failed is True

    def test_any_failed_false_when_all_success_or_skipped(self):
        result = run_stage1_pipeline(_all_skip_params())
        assert result.any_failed is False


# ── Full pipeline with all mocks ──────────────────────────────────────────────

class TestFullPipelineMocked:
    """End-to-end test with all DB services mocked."""

    def test_full_pipeline_success(self, tmp_path):
        fake_file = tmp_path / "chat.json"
        fake_file.write_text("{}")

        mock_ingest_result = MagicMock()
        mock_ingest_result.messages_inserted = 50
        mock_ingest_result.messages_skipped_duplicate = 0
        mock_ingest_result.members_created = 3

        mock_cls_result = MagicMock()
        mock_cls_result.messages_processed = 50
        mock_cls_result.messages_skipped_already_classified = 0
        mock_cls_result.messages_unmatched = 2
        mock_cls_result.topic_assignments_written = 48
        mock_cls_result.missing_topic_assignments = 0

        mock_prof_result = MagicMock()
        mock_prof_result.members_attempted = 3
        mock_prof_result.profiles_written = 3
        mock_prof_result.profiles_skipped = 0
        mock_prof_result.profiles_failed = 0
        mock_prof_result.missing_topic_count = 0
        mock_prof_result.failed_member_ids = []

        with (
            patch("src.processing.pipeline.IngestService") as MockIngest,
            patch("src.processing.pipeline.SessionLocal") as MockSession,
            patch("src.processing.pipeline.ClassificationService") as MockCls,
            patch("src.processing.pipeline.ProfileService") as MockProf,
        ):
            MockIngest.return_value.ingest_file.return_value = mock_ingest_result
            mock_session = MagicMock()
            MockSession.return_value = mock_session
            mock_session.execute.return_value.scalar_one_or_none.return_value = None
            MockCls.return_value.run.return_value = mock_cls_result
            MockProf.return_value.run.return_value = mock_prof_result

            params = PipelineParams(
                input_file=fake_file,
                platform="qq",
                window_start=WINDOW_START,
                window_end=WINDOW_END,
            )
            result = run_stage1_pipeline(params)

        assert result.overall_status == "success"
        assert result.ingest.status == "success"
        assert result.topics_init.status == "success"
        assert result.classification.status == "success"
        assert result.profiling.status == "success"
        assert result.ingest.messages_inserted == 50
        assert result.classification.topic_assignments_written == 48
        assert result.profiling.profiles_written == 3
