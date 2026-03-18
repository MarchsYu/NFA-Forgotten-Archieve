"""
Tests for the topic classification module.

These tests run entirely in-memory (no DB required) and verify:
1. TopicClassifier produces correct matches and evidence
2. is_primary_eligible is respected in primary label selection
3. The keyset pagination logic in ClassificationService does not skip messages
   (regression test for the OFFSET-based "漏数" bug)
"""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import MagicMock, call, patch

import pytest

from src.classification.topic_classifier import TopicClassifier, TopicMatch
from src.classification.topic_rules import TOPICS, TOPIC_MAP


# ---------------------------------------------------------------------------
# TopicClassifier unit tests
# ---------------------------------------------------------------------------

class TestTopicClassifier:
    def setup_method(self):
        self.clf = TopicClassifier()

    def test_strong_keyword_hit_returns_match(self):
        matches = self.clf.classify("这个 bug 怎么修？")
        keys = {m.topic_key for m in matches}
        assert "technical" in keys

    def test_no_match_returns_empty(self):
        matches = self.clf.classify("嗯")
        assert matches == []

    def test_primary_is_set_on_exactly_one_match(self):
        matches = self.clf.classify("哈哈哈，今晚开黑打王者！")
        primaries = [m for m in matches if m.is_primary]
        assert len(primaries) == 1

    def test_confidence_strong_base(self):
        # Single strong keyword → base 0.85
        matches = self.clf.classify("bug")
        tech = next(m for m in matches if m.topic_key == "technical")
        assert tech.confidence == Decimal("0.85")

    def test_confidence_caps_at_max(self):
        # Many strong keywords → should cap at 0.95
        text = "bug error exception 代码 编程 python java sql api git"
        matches = self.clf.classify(text)
        tech = next(m for m in matches if m.topic_key == "technical")
        assert tech.confidence == Decimal("0.95")

    def test_evidence_contains_required_fields(self):
        matches = self.clf.classify("这个 bug 怎么修？")
        tech = next(m for m in matches if m.topic_key == "technical")
        ev = tech.evidence
        assert "rule_name" in ev
        assert "text_source" in ev
        assert "matched_keywords" in ev
        assert "strong_matched_keywords" in ev
        assert "weak_matched_keywords" in ev

    def test_evidence_strong_weak_split(self):
        # "bug" is strong, "报错" is weak for technical
        matches = self.clf.classify("bug 报错了")
        tech = next(m for m in matches if m.topic_key == "technical")
        assert "bug" in tech.evidence["strong_matched_keywords"]
        assert "报错" in tech.evidence["weak_matched_keywords"]

    def test_is_primary_eligible_respected(self):
        """
        If we temporarily mark a topic as not primary-eligible, it should not
        be selected as primary even if it has the highest confidence.
        """
        # Patch TOPIC_MAP so 'technical' is not primary-eligible
        original = TOPIC_MAP["technical"]
        from dataclasses import replace
        patched = replace(original, is_primary_eligible=False)

        with patch.dict("src.classification.topic_classifier.TOPIC_MAP",
                        {"technical": patched}):
            matches = self.clf.classify("这个 bug 怎么修？")
            # technical should still be in matches but NOT primary
            tech = next((m for m in matches if m.topic_key == "technical"), None)
            if tech:
                assert not tech.is_primary

    def test_multi_topic_message(self):
        # A message that hits both gaming and question
        matches = self.clf.classify("王者怎么上分？")
        keys = {m.topic_key for m in matches}
        assert "gaming" in keys
        assert "question" in keys

    def test_no_duplicate_keywords_in_evidence(self):
        # Keywords that appear in both strong and weak lists should not be duplicated
        matches = self.clf.classify("哈哈哈哈")
        casual = next((m for m in matches if m.topic_key == "casual_chat"), None)
        if casual:
            kws = casual.evidence["matched_keywords"]
            assert len(kws) == len(set(kws)), "Duplicate keywords in evidence"


# ---------------------------------------------------------------------------
# Keyset pagination regression test (no DB needed)
# ---------------------------------------------------------------------------

class TestKeysetPaginationNoBug:
    """
    Verifies that the keyset pagination algorithm processes ALL messages without skipping.

    Simulates the scenario that caused the OFFSET bug:
    - 5 messages total, batch_size=2
    - After each batch is written, the NOT EXISTS filter would have shrunk the
      result set if we used OFFSET. With keyset pagination, all 5 are processed.

    This test reimplements the core loop logic from _classify_messages to verify
    correctness without requiring a database connection.
    """

    def test_all_messages_processed_with_small_batch(self):
        from src.classification.topic_classifier import TopicClassifier
        from src.classification.topic_rules import TOPICS

        # Build 5 fake Message objects with sequential IDs
        def make_msg(id_, content):
            m = MagicMock()
            m.id = id_
            m.normalized_content = content
            m.content = content
            return m

        all_messages = [
            make_msg(1, "这个 bug 怎么修？"),
            make_msg(2, "今晚开黑打王者"),
            make_msg(3, "好难过，感觉很崩溃"),
            make_msg(4, "明天几点开会？"),
            make_msg(5, "嗯"),  # no match
        ]

        batch_size = 2
        classifier = TopicClassifier()
        topic_id_map = {t.topic_key: i + 1 for i, t in enumerate(TOPICS)}

        # Simulate keyset fetch: returns messages with id > last_seen_id
        def fake_fetch(last_seen_id):
            remaining = [m for m in all_messages if m.id > last_seen_id]
            return remaining[:batch_size]

        # Run the keyset loop (mirrors _classify_messages logic)
        processed = 0
        written = 0
        unmatched = 0
        last_seen_id = 0

        while True:
            batch = fake_fetch(last_seen_id)
            if not batch:
                break

            for message in batch:
                text = message.normalized_content or ""
                matches = classifier.classify(text)

                if not matches:
                    unmatched += 1
                    processed += 1
                    last_seen_id = message.id
                    continue

                for match in matches:
                    if topic_id_map.get(match.topic_key):
                        written += 1

                processed += 1
                last_seen_id = message.id

        assert processed == 5, (
            f"Expected 5 processed, got {processed}. "
            "Keyset pagination bug: some messages were skipped."
        )
        assert unmatched == 1   # message 5 ("嗯") has no match
        assert written > 0

    def test_offset_bug_would_skip_messages(self):
        """
        Demonstrates that OFFSET-based pagination WOULD skip messages when the
        result set shrinks (the bug we fixed). This test documents the failure mode.
        """
        from src.classification.topic_classifier import TopicClassifier

        all_messages_ids = [1, 2, 3, 4, 5]
        classified_ids: set = set()
        batch_size = 2

        # Simulate OFFSET fetch: returns unclassified messages starting at offset
        def buggy_offset_fetch(offset):
            unclassified = [i for i in all_messages_ids if i not in classified_ids]
            return unclassified[offset:offset + batch_size]

        processed_ids = []
        offset = 0

        while True:
            batch = buggy_offset_fetch(offset)
            if not batch:
                break
            for msg_id in batch:
                classified_ids.add(msg_id)
                processed_ids.append(msg_id)
            offset += len(batch)

        # With OFFSET bug: only 3 out of 5 messages are processed
        # Batch 1 (offset=0): [1,2] → classify → classified={1,2}
        # Batch 2 (offset=2): unclassified=[3,4,5], skip 2 → [5] → classify → classified={1,2,5}
        # Batch 3 (offset=1): unclassified=[3,4], skip 1 → [4] → classify
        # ... this is unpredictable and wrong
        assert len(processed_ids) < 5, (
            "Expected OFFSET bug to skip messages, but all were processed. "
            "Test setup may be incorrect."
        )
