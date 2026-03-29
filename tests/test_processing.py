"""
Tests for the Stage-1 processing pipeline.

All tests run entirely in-memory (no DB required).
DB-touching code is patched at the service layer.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from src.processing.pipeline import (
    ALL_STAGES,
    STAGE_CLASSIFICATION,
    STAGE_INGEST,
    STAGE_PROFILING,
    STAGE_TOPICS_INIT,
    PipelineParams,
    PipelineResult,
    StageOutcome,
    _make_run_id,
    _validate_params,
    run_stage1_pipeline,
)
from src.classification.topic_service import TopicInitResult


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_WINDOW_START = datetime(2026, 1, 1, tzinfo=timezone.utc)
_WINDOW_END = datetime(2026, 12, 31, 23, 59, 59, tzinfo=timezone.utc)
_CHAT_FILE = Path("dummy.json")


def _full_skip_params(**overrides) -> PipelineParams:
    """Params that skip all stages – useful for testing validation in isolation."""
    base = dict(
        skip_stages=list(ALL_STAGES),
        window_start=_WINDOW_START,
        window_end=_WINDOW_END,
        chat_file=_CHAT_FILE,
    )
    base.update(overrides)
    return PipelineParams(**base)


# ---------------------------------------------------------------------------
# TestRunId
# ---------------------------------------------------------------------------

class TestRunId:
    def test_format_matches_pattern(self):
        run_id = _make_run_id()
        # e.g. "20260101T120000_a1b2c3"
        parts = run_id.split("_")
        assert len(parts) == 2
        ts_part, suffix = parts
        assert len(ts_part) == 15          # YYYYMMDDTHHmmss
        assert ts_part[8] == "T"
        assert len(suffix) == 6
        assert suffix.isalnum()

    def test_two_calls_produce_different_ids(self):
        ids = {_make_run_id() for _ in range(20)}
        # With a 6-char hex suffix the probability of collision is negligible
        assert len(ids) == 20


# ---------------------------------------------------------------------------
# TestValidateParams
# ---------------------------------------------------------------------------

class TestValidateParams:
    def test_member_id_without_group_id_raises(self):
        params = PipelineParams(
            member_id=uuid.uuid4(),
            group_id=None,
            skip_stages=[STAGE_INGEST, STAGE_TOPICS_INIT, STAGE_CLASSIFICATION, STAGE_PROFILING],
        )
        with pytest.raises(ValueError, match="group_id"):
            _validate_params(params)

    def test_member_id_with_group_id_ok(self):
        params = PipelineParams(
            member_id=uuid.uuid4(),
            group_id=uuid.uuid4(),
            skip_stages=list(ALL_STAGES),
            window_start=_WINDOW_START,
            window_end=_WINDOW_END,
        )
        _validate_params(params)  # should not raise

    def test_profiling_requires_window_start(self):
        params = PipelineParams(
            skip_stages=[STAGE_INGEST, STAGE_TOPICS_INIT, STAGE_CLASSIFICATION],
            chat_file=_CHAT_FILE,
            window_start=None,
            window_end=_WINDOW_END,
        )
        with pytest.raises(ValueError, match="window_start"):
            _validate_params(params)

    def test_profiling_requires_window_end(self):
        params = PipelineParams(
            skip_stages=[STAGE_INGEST, STAGE_TOPICS_INIT, STAGE_CLASSIFICATION],
            chat_file=_CHAT_FILE,
            window_start=_WINDOW_START,
            window_end=None,
        )
        with pytest.raises(ValueError, match="window_end"):
            _validate_params(params)

    def test_window_start_must_be_before_window_end(self):
        params = PipelineParams(
            skip_stages=[STAGE_INGEST, STAGE_TOPICS_INIT, STAGE_CLASSIFICATION],
            chat_file=_CHAT_FILE,
            window_start=_WINDOW_END,
            window_end=_WINDOW_START,
        )
        with pytest.raises(ValueError, match="before"):
            _validate_params(params)

    def test_window_equal_raises(self):
        params = PipelineParams(
            skip_stages=[STAGE_INGEST, STAGE_TOPICS_INIT, STAGE_CLASSIFICATION],
            chat_file=_CHAT_FILE,
            window_start=_WINDOW_START,
            window_end=_WINDOW_START,
        )
        with pytest.raises(ValueError, match="before"):
            _validate_params(params)

    def test_profiling_skipped_no_window_required(self):
        params = PipelineParams(
            skip_stages=list(ALL_STAGES),   # profiling also skipped
            window_start=None,
            window_end=None,
        )
        _validate_params(params)  # should not raise

    def test_ingest_requires_chat_file(self):
        params = PipelineParams(
            skip_stages=[STAGE_TOPICS_INIT, STAGE_CLASSIFICATION, STAGE_PROFILING],
            chat_file=None,
        )
        with pytest.raises(ValueError, match="chat_file"):
            _validate_params(params)

    def test_ingest_skipped_no_chat_file_required(self):
        params = PipelineParams(
            skip_stages=list(ALL_STAGES),
            chat_file=None,
        )
        _validate_params(params)  # should not raise

    def test_unknown_stage_name_raises(self):
        params = PipelineParams(
            skip_stages=["nonexistent_stage"],
            chat_file=_CHAT_FILE,
            window_start=_WINDOW_START,
            window_end=_WINDOW_END,
        )
        with pytest.raises(ValueError, match="Unknown stage"):
            _validate_params(params)


# ---------------------------------------------------------------------------
# TestPipelineResult
# ---------------------------------------------------------------------------

class TestPipelineResult:
    def test_success_true_when_all_stages_ok(self):
        result = PipelineResult(run_id="x", params=PipelineParams())
        result.stages = [
            StageOutcome(stage=STAGE_INGEST, success=True),
            StageOutcome(stage=STAGE_TOPICS_INIT, success=True),
            StageOutcome(stage=STAGE_CLASSIFICATION, success=True),
            StageOutcome(stage=STAGE_PROFILING, success=True),
        ]
        assert result.success is True

    def test_success_true_when_stages_skipped(self):
        result = PipelineResult(run_id="x", params=PipelineParams())
        result.stages = [
            StageOutcome(stage=STAGE_INGEST, skipped=True, success=True),
            StageOutcome(stage=STAGE_TOPICS_INIT, skipped=True, success=True),
        ]
        assert result.success is True

    def test_success_false_when_any_stage_failed(self):
        result = PipelineResult(run_id="x", params=PipelineParams())
        result.stages = [
            StageOutcome(stage=STAGE_INGEST, success=True),
            StageOutcome(stage=STAGE_CLASSIFICATION, success=False, error_summary="boom"),
        ]
        assert result.success is False

    def test_failed_stages_lists_failures(self):
        result = PipelineResult(run_id="x", params=PipelineParams())
        result.stages = [
            StageOutcome(stage=STAGE_INGEST, success=True),
            StageOutcome(stage=STAGE_CLASSIFICATION, success=False, error_summary="err"),
            StageOutcome(stage=STAGE_PROFILING, success=False, error_summary="err2"),
        ]
        assert result.failed_stages == [STAGE_CLASSIFICATION, STAGE_PROFILING]

    def test_error_summary_stored_not_traceback(self):
        outcome = StageOutcome(
            stage=STAGE_CLASSIFICATION,
            success=False,
            error_summary="RuntimeError: no topics in DB",
        )
        # error_summary is a short string, not a multi-line traceback
        assert "\n" not in outcome.error_summary
        assert "RuntimeError" in outcome.error_summary


# ---------------------------------------------------------------------------
# TestTopicInitDelegation
# ---------------------------------------------------------------------------

class TestTopicInitDelegation:
    """
    Verify that _run_topics_init() delegates to topic_service.init_topics()
    and does NOT contain its own topic seeding logic.
    """

    def test_delegates_to_topic_service(self):
        fake_result = TopicInitResult(inserted=3, skipped=5, topic_keys=["a", "b", "c"])
        with patch(
            "src.processing.pipeline.init_topics",
            return_value=fake_result,
        ) as mock_init:
            from src.processing.pipeline import _run_topics_init
            outcome, result = _run_topics_init(PipelineParams())

        mock_init.assert_called_once_with()
        assert outcome.success is True
        assert result is fake_result
        assert result.inserted == 3
        assert result.skipped == 5

    def test_failure_captured_as_error_summary(self):
        with patch(
            "src.processing.pipeline.init_topics",
            side_effect=RuntimeError("DB connection refused"),
        ):
            from src.processing.pipeline import _run_topics_init
            outcome, result = _run_topics_init(PipelineParams())

        assert outcome.success is False
        assert result is None
        assert "RuntimeError" in outcome.error_summary
        assert "DB connection refused" in outcome.error_summary
        # No traceback in error_summary
        assert "Traceback" not in outcome.error_summary


# ---------------------------------------------------------------------------
# TestRunStage1Pipeline – integration (all services mocked)
# ---------------------------------------------------------------------------

class TestRunStage1Pipeline:
    """
    End-to-end pipeline tests with all DB-touching services mocked.
    """

    def _make_mock_ingest_result(self):
        r = MagicMock()
        r.messages_inserted = 10
        r.messages_skipped_duplicate = 2
        r.members_created = 1
        r.members_reused = 0
        return r

    def _make_mock_cls_result(self):
        from src.classification.classification_service import ClassificationResult
        return ClassificationResult(
            classifier_version="rule_v1",
            messages_processed=10,
            messages_skipped_already_classified=0,
            topic_assignments_written=8,
            messages_unmatched=2,
            missing_topic_assignments=0,
        )

    def _make_mock_prof_result(self):
        from src.profiling.profile_service import ProfilingResult
        return ProfilingResult(
            profile_version="profile_v1",
            classifier_version="rule_v1",
            window_start=_WINDOW_START,
            window_end=_WINDOW_END,
            members_attempted=3,
            profiles_written=3,
            profiles_skipped=0,
            profiles_failed=0,
        )

    def test_all_stages_run_and_succeed(self):
        fake_ingest = self._make_mock_ingest_result()
        fake_topics = TopicInitResult(inserted=8, skipped=0, topic_keys=[])
        fake_cls = self._make_mock_cls_result()
        fake_prof = self._make_mock_prof_result()

        with (
            patch("src.processing.pipeline.IngestService") as MockIngest,
            patch("src.processing.pipeline.init_topics", return_value=fake_topics),
            patch("src.processing.pipeline.ClassificationService") as MockCls,
            patch("src.processing.pipeline.ProfileService") as MockProf,
        ):
            MockIngest.return_value.ingest_file.return_value = fake_ingest
            MockCls.return_value.run.return_value = fake_cls
            MockProf.return_value.run.return_value = fake_prof

            params = PipelineParams(
                chat_file=_CHAT_FILE,
                window_start=_WINDOW_START,
                window_end=_WINDOW_END,
            )
            result = run_stage1_pipeline(params)

        assert result.success is True
        assert len(result.stages) == 4
        assert all(s.success for s in result.stages)
        assert result.ingest_result is fake_ingest
        assert result.topics_init_result is fake_topics
        assert result.classification_result is fake_cls
        assert result.profiling_result is fake_prof

    def test_skip_ingest_stage(self):
        fake_topics = TopicInitResult(inserted=0, skipped=8, topic_keys=[])
        fake_cls = self._make_mock_cls_result()
        fake_prof = self._make_mock_prof_result()

        with (
            patch("src.processing.pipeline.init_topics", return_value=fake_topics),
            patch("src.processing.pipeline.ClassificationService") as MockCls,
            patch("src.processing.pipeline.ProfileService") as MockProf,
        ):
            MockCls.return_value.run.return_value = fake_cls
            MockProf.return_value.run.return_value = fake_prof

            params = PipelineParams(
                skip_stages=[STAGE_INGEST],
                window_start=_WINDOW_START,
                window_end=_WINDOW_END,
            )
            result = run_stage1_pipeline(params)

        assert result.success is True
        ingest_stage = next(s for s in result.stages if s.stage == STAGE_INGEST)
        assert ingest_stage.skipped is True
        assert result.ingest_result is None

    def test_skip_all_stages(self):
        params = PipelineParams(skip_stages=list(ALL_STAGES))
        result = run_stage1_pipeline(params)

        assert result.success is True
        assert all(s.skipped for s in result.stages)

    def test_run_id_is_set(self):
        params = PipelineParams(skip_stages=list(ALL_STAGES))
        result = run_stage1_pipeline(params)
        assert result.run_id
        assert "_" in result.run_id

    def test_classification_failure_does_not_abort_profiling(self):
        """Pipeline continues even if classification fails."""
        fake_topics = TopicInitResult(inserted=0, skipped=8, topic_keys=[])
        fake_prof = self._make_mock_prof_result()

        with (
            patch("src.processing.pipeline.init_topics", return_value=fake_topics),
            patch("src.processing.pipeline.ClassificationService") as MockCls,
            patch("src.processing.pipeline.ProfileService") as MockProf,
        ):
            MockCls.return_value.run.side_effect = RuntimeError("DB error")
            MockProf.return_value.run.return_value = fake_prof

            params = PipelineParams(
                skip_stages=[STAGE_INGEST],
                window_start=_WINDOW_START,
                window_end=_WINDOW_END,
            )
            result = run_stage1_pipeline(params)

        assert result.success is False
        cls_stage = next(s for s in result.stages if s.stage == STAGE_CLASSIFICATION)
        prof_stage = next(s for s in result.stages if s.stage == STAGE_PROFILING)
        assert cls_stage.success is False
        assert "RuntimeError" in cls_stage.error_summary
        # Profiling still ran
        assert prof_stage.success is True

    def test_invalid_params_raises_before_any_stage(self):
        params = PipelineParams(
            member_id=uuid.uuid4(),
            group_id=None,
            skip_stages=list(ALL_STAGES),
        )
        with pytest.raises(ValueError, match="group_id"):
            run_stage1_pipeline(params)

    def test_rerun_passed_to_classification_service(self):
        fake_topics = TopicInitResult(inserted=0, skipped=8, topic_keys=[])
        fake_cls = self._make_mock_cls_result()
        fake_prof = self._make_mock_prof_result()

        with (
            patch("src.processing.pipeline.init_topics", return_value=fake_topics),
            patch("src.processing.pipeline.ClassificationService") as MockCls,
            patch("src.processing.pipeline.ProfileService") as MockProf,
        ):
            MockCls.return_value.run.return_value = fake_cls
            MockProf.return_value.run.return_value = fake_prof

            params = PipelineParams(
                skip_stages=[STAGE_INGEST],
                window_start=_WINDOW_START,
                window_end=_WINDOW_END,
                rerun=True,
            )
            run_stage1_pipeline(params)

        call_kwargs = MockCls.return_value.run.call_args.kwargs
        assert call_kwargs.get("rerun") is True


# ---------------------------------------------------------------------------
# TestTopicServiceCanonical
# ---------------------------------------------------------------------------

class TestTopicServiceCanonical:
    """
    Verify that topic_service.init_topics() is the single implementation.
    The script and the pipeline both import from the same module.
    """

    def test_topic_service_importable_without_db(self):
        """Pure import should not trigger DB engine creation."""
        # If this raises, the module has a top-level DB side-effect
        from src.classification.topic_service import init_topics, TopicInitResult  # noqa: F401

    def test_topic_init_result_fields(self):
        r = TopicInitResult(inserted=3, skipped=5, topic_keys=["a", "b"])
        assert r.inserted == 3
        assert r.skipped == 5
        assert r.topic_keys == ["a", "b"]

    def test_init_topics_script_imports_from_topic_service(self):
        """
        Verify scripts/init_topics.py imports from topic_service, not a local copy.
        """
        import importlib.util, sys
        from pathlib import Path

        script_path = Path(__file__).parent.parent / "scripts" / "init_topics.py"
        source = script_path.read_text(encoding="utf-8")
        assert "from src.classification.topic_service import" in source
        # Must NOT contain its own Topic seeding logic
        assert "session.add(Topic(" not in source
        assert "select(Topic)" not in source
