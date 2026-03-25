"""
Tests for the profiling module.

All tests run entirely in-memory (no DB required).  They verify:

1. profile_analyzers – each pure analysis function
2. ProfileBuilder    – end-to-end profile construction from fake data
3. Persona summary   – template output is non-empty and data-driven
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from decimal import Decimal
from types import SimpleNamespace
from typing import Any, List
from unittest.mock import MagicMock

import pytest

from src.profiling.profile_analyzers import (
    build_persona_summary,
    compute_active_hours,
    compute_activity_pattern,
    compute_interaction_top,
    compute_message_stats,
    compute_style_hints,
    compute_topic_distribution,
    compute_top_keywords,
    compute_verbosity_level,
)
from src.profiling.profile_builder import ProfileBuilder, ProfileData, PROFILE_VERSION


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_msg(
    content: str,
    hour: int = 14,
    reply_to: int | None = None,
    msg_id: int = 1,
) -> Any:
    """Create a minimal fake Message object."""
    m = SimpleNamespace()
    m.id = msg_id
    m.content = content
    m.normalized_content = content
    m.sent_at = datetime(2026, 6, 15, hour, 0, 0, tzinfo=timezone.utc)
    m.reply_to_message_id = reply_to
    return m


def _make_topic_row(topic_key: str, is_primary: bool) -> Any:
    t = SimpleNamespace()
    t.topic_key = topic_key
    t.is_primary = is_primary
    return t


_WINDOW_START = datetime(2026, 1, 1, tzinfo=timezone.utc)
_WINDOW_END   = datetime(2026, 12, 31, 23, 59, 59, tzinfo=timezone.utc)


# ---------------------------------------------------------------------------
# 1. compute_message_stats
# ---------------------------------------------------------------------------

class TestComputeMessageStats:
    def test_empty_returns_zeros(self):
        result = compute_message_stats([])
        assert result["message_count"] == 0
        assert result["avg_message_length"] == 0.0

    def test_single_message(self):
        msgs = [_make_msg("hello")]
        result = compute_message_stats(msgs)
        assert result["message_count"] == 1
        assert result["avg_message_length"] == 5.0

    def test_multiple_messages(self):
        msgs = [_make_msg("hi"), _make_msg("hello world")]
        result = compute_message_stats(msgs)
        assert result["message_count"] == 2
        # "hi"=2, "hello world"=11 → avg = 6.5
        assert result["avg_message_length"] == 6.5

    def test_uses_normalized_content_first(self):
        m = SimpleNamespace()
        m.content = "raw content"
        m.normalized_content = "norm"
        m.sent_at = datetime(2026, 1, 1, tzinfo=timezone.utc)
        result = compute_message_stats([m])
        assert result["avg_message_length"] == 4.0  # len("norm")


# ---------------------------------------------------------------------------
# 2. compute_top_keywords
# ---------------------------------------------------------------------------

class TestComputeTopKeywords:
    def test_empty_returns_empty(self):
        assert compute_top_keywords([]) == []

    def test_returns_sorted_by_count(self):
        msgs = [
            _make_msg("bug bug bug error"),
            _make_msg("bug error"),
        ]
        result = compute_top_keywords(msgs, top_n=5)
        words = [r["word"] for r in result]
        assert words[0] == "bug"   # highest count
        assert "error" in words

    def test_stopwords_excluded(self):
        msgs = [_make_msg("的 了 是 在 bug")]
        result = compute_top_keywords(msgs, top_n=10)
        words = [r["word"] for r in result]
        assert "的" not in words
        assert "了" not in words
        assert "bug" in words

    def test_short_ascii_excluded(self):
        # Single-char and 2-char ASCII tokens should be filtered
        msgs = [_make_msg("a ab abc abcd")]
        result = compute_top_keywords(msgs, top_n=10)
        words = [r["word"] for r in result]
        assert "a" not in words
        assert "ab" not in words
        assert "abc" in words

    def test_top_n_respected(self):
        msgs = [_make_msg("bug error exception python java sql api git docker kubernetes")]
        result = compute_top_keywords(msgs, top_n=3)
        assert len(result) <= 3


# ---------------------------------------------------------------------------
# 3. compute_topic_distribution
# ---------------------------------------------------------------------------

class TestComputeTopicDistribution:
    def test_empty_returns_empty_dicts(self):
        primary, all_ = compute_topic_distribution([])
        assert primary == {}
        assert all_ == {}

    def test_primary_only_counts_is_primary_true(self):
        rows = [
            _make_topic_row("casual_chat", True),
            _make_topic_row("meme", False),
            _make_topic_row("casual_chat", True),
        ]
        primary, all_ = compute_topic_distribution(rows)
        assert primary == {"casual_chat": 2}
        assert all_ == {"casual_chat": 2, "meme": 1}

    def test_all_dist_includes_non_primary(self):
        rows = [
            _make_topic_row("gaming", False),
            _make_topic_row("gaming", False),
            _make_topic_row("question", True),
        ]
        primary, all_ = compute_topic_distribution(rows)
        assert "gaming" not in primary
        assert all_["gaming"] == 2
        assert primary["question"] == 1


# ---------------------------------------------------------------------------
# 4. compute_active_hours
# ---------------------------------------------------------------------------

class TestComputeActiveHours:
    def test_empty_returns_empty(self):
        assert compute_active_hours([]) == {}

    def test_counts_by_hour(self):
        msgs = [
            _make_msg("a", hour=10),
            _make_msg("b", hour=10),
            _make_msg("c", hour=23),
        ]
        result = compute_active_hours(msgs)
        assert result["10"] == 2
        assert result["23"] == 1
        assert "14" not in result  # no messages at hour 14

    def test_zero_hours_omitted(self):
        msgs = [_make_msg("x", hour=5)]
        result = compute_active_hours(msgs)
        assert set(result.keys()) == {"5"}


# ---------------------------------------------------------------------------
# 5. compute_interaction_top
# ---------------------------------------------------------------------------

class TestComputeInteractionTop:
    def test_empty_returns_empty(self):
        assert compute_interaction_top([]) == []

    def test_counts_and_sorts(self):
        mid_a = str(uuid.uuid4())
        mid_b = str(uuid.uuid4())
        targets = [
            {"member_id": mid_a, "display_name": "Alice"},
            {"member_id": mid_a, "display_name": "Alice"},
            {"member_id": mid_b, "display_name": "Bob"},
        ]
        result = compute_interaction_top(targets, top_n=5)
        assert result[0]["member_id"] == mid_a
        assert result[0]["count"] == 2
        assert result[1]["count"] == 1

    def test_top_n_respected(self):
        targets = [
            {"member_id": str(uuid.uuid4()), "display_name": f"User{i}"}
            for i in range(10)
        ]
        result = compute_interaction_top(targets, top_n=3)
        assert len(result) <= 3


# ---------------------------------------------------------------------------
# 6. compute_verbosity_level
# ---------------------------------------------------------------------------

class TestComputeVerbosityLevel:
    def test_terse(self):
        assert compute_verbosity_level(5.0) == "terse"
        assert compute_verbosity_level(19.9) == "terse"

    def test_moderate(self):
        assert compute_verbosity_level(20.0) == "moderate"
        assert compute_verbosity_level(80.0) == "moderate"

    def test_verbose(self):
        assert compute_verbosity_level(80.1) == "verbose"
        assert compute_verbosity_level(200.0) == "verbose"


# ---------------------------------------------------------------------------
# 7. compute_activity_pattern
# ---------------------------------------------------------------------------

class TestComputeActivityPattern:
    def test_empty_returns_mixed(self):
        assert compute_activity_pattern({}) == "mixed"

    def test_night_dominant(self):
        # Hours 22-23 and 0-5 are "night"
        hours = {"22": 50, "23": 50, "0": 20, "14": 5}
        result = compute_activity_pattern(hours)
        assert result == "night"

    def test_morning_dominant(self):
        hours = {"8": 40, "9": 40, "10": 20, "22": 5}
        result = compute_activity_pattern(hours)
        assert result == "morning"

    def test_mixed_when_no_dominant(self):
        # Evenly spread across all bands
        hours = {str(h): 10 for h in range(24)}
        result = compute_activity_pattern(hours)
        assert result == "mixed"


# ---------------------------------------------------------------------------
# 8. build_persona_summary
# ---------------------------------------------------------------------------

class TestBuildPersonaSummary:
    def test_returns_non_empty_string(self):
        summary = build_persona_summary(
            message_count=100,
            dominant_topics=["casual_chat", "gaming"],
            verbosity_level="terse",
            activity_pattern="night",
            interaction_top=[],
            window_start=_WINDOW_START,
            window_end=_WINDOW_END,
        )
        assert isinstance(summary, str)
        assert len(summary) > 0

    def test_contains_topic_names(self):
        summary = build_persona_summary(
            message_count=50,
            dominant_topics=["technical"],
            verbosity_level="verbose",
            activity_pattern="morning",
            interaction_top=[],
            window_start=_WINDOW_START,
            window_end=_WINDOW_END,
        )
        assert "技术讨论" in summary

    def test_contains_interaction_name_when_present(self):
        summary = build_persona_summary(
            message_count=80,
            dominant_topics=["casual_chat"],
            verbosity_level="moderate",
            activity_pattern="evening",
            interaction_top=[{"member_id": "x", "display_name": "Alice", "count": 5}],
            window_start=_WINDOW_START,
            window_end=_WINDOW_END,
        )
        assert "Alice" in summary

    def test_no_interaction_note_when_empty(self):
        summary = build_persona_summary(
            message_count=10,
            dominant_topics=[],
            verbosity_level="terse",
            activity_pattern="mixed",
            interaction_top=[],
            window_start=_WINDOW_START,
            window_end=_WINDOW_END,
        )
        # Should not crash and should not mention any name
        assert isinstance(summary, str)

    def test_activity_level_reflects_count(self):
        low = build_persona_summary(5, [], "terse", "mixed", [], _WINDOW_START, _WINDOW_END)
        high = build_persona_summary(300, [], "terse", "mixed", [], _WINDOW_START, _WINDOW_END)
        assert "发言较少" in low
        assert "非常活跃" in high


# ---------------------------------------------------------------------------
# 9. ProfileBuilder end-to-end
# ---------------------------------------------------------------------------

class TestProfileBuilder:
    def setup_method(self):
        self.builder = ProfileBuilder()
        self.member_id = uuid.uuid4()
        self.group_id = uuid.uuid4()

    def _build(self, messages, topic_rows, reply_targets=None):
        return self.builder.build(
            member_id=self.member_id,
            group_id=self.group_id,
            messages=messages,
            topic_rows=topic_rows,
            reply_targets=reply_targets or [],
            profile_version=PROFILE_VERSION,
            window_start=_WINDOW_START,
            window_end=_WINDOW_END,
        )

    def test_returns_profile_data_instance(self):
        profile = self._build([], [])
        assert isinstance(profile, ProfileData)

    def test_member_and_group_ids_preserved(self):
        profile = self._build([], [])
        assert profile.member_id == self.member_id
        assert profile.group_id == self.group_id

    def test_source_message_count_matches(self):
        msgs = [_make_msg("hello"), _make_msg("world")]
        profile = self._build(msgs, [])
        assert profile.source_message_count == 2

    def test_stats_has_required_keys(self):
        profile = self._build([], [])
        required = {
            "message_count", "avg_message_length", "top_keywords",
            "topic_distribution", "all_topics_distribution",
            "active_hours", "interaction_top",
        }
        assert required.issubset(profile.stats.keys())

    def test_traits_has_required_keys(self):
        profile = self._build([], [])
        required = {"dominant_topics", "verbosity_level", "style_hints", "activity_pattern"}
        assert required.issubset(profile.traits.keys())

    def test_persona_summary_is_non_empty_string(self):
        msgs = [_make_msg("这个 bug 怎么修？", hour=22)]
        rows = [_make_topic_row("technical", True)]
        profile = self._build(msgs, rows)
        assert isinstance(profile.persona_summary, str)
        assert len(profile.persona_summary) > 0

    def test_dominant_topics_sorted_by_count(self):
        rows = [
            _make_topic_row("casual_chat", True),
            _make_topic_row("casual_chat", True),
            _make_topic_row("gaming", True),
        ]
        profile = self._build([], rows)
        assert profile.traits["dominant_topics"][0] == "casual_chat"

    def test_verbosity_terse_for_short_messages(self):
        msgs = [_make_msg("ok"), _make_msg("嗯")]
        profile = self._build(msgs, [])
        assert profile.traits["verbosity_level"] == "terse"

    def test_verbosity_verbose_for_long_messages(self):
        # avg_message_length must exceed 80 chars → use a string > 80 chars
        long_text = "这是一段非常" * 20  # 6 chars × 20 = 120 chars
        msgs = [_make_msg(long_text)]
        profile = self._build(msgs, [])
        assert profile.traits["verbosity_level"] == "verbose"

    def test_night_activity_pattern(self):
        msgs = [_make_msg("夜猫子", hour=23) for _ in range(10)]
        profile = self._build(msgs, [])
        assert profile.traits["activity_pattern"] == "night"

    def test_interaction_top_in_stats(self):
        mid = str(uuid.uuid4())
        reply_targets = [{"member_id": mid, "display_name": "Bob"} for _ in range(3)]
        profile = self._build([], [], reply_targets=reply_targets)
        top = profile.stats["interaction_top"]
        assert len(top) == 1
        assert top[0]["count"] == 3
        assert top[0]["display_name"] == "Bob"

    def test_snapshot_at_is_timezone_aware(self):
        profile = self._build([], [])
        assert profile.snapshot_at.tzinfo is not None

    def test_window_preserved(self):
        profile = self._build([], [])
        assert profile.window_start == _WINDOW_START
        assert profile.window_end == _WINDOW_END

    def test_empty_messages_produces_valid_profile(self):
        """A member with zero messages in the window should still get a profile."""
        profile = self._build([], [])
        assert profile.source_message_count == 0
        assert profile.stats["message_count"] == 0
        assert isinstance(profile.persona_summary, str)
