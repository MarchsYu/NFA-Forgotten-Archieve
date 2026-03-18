"""
Rule-based topic classifier (version: rule_v1).

Applies keyword rules from topic_rules.py to a single message's text.
Returns a list of TopicMatch objects, one per matched topic.

Confidence model
----------------
- First strong keyword hit  → base 0.85
- First weak keyword hit    → base 0.55
- Each additional keyword   → +0.05 (capped at 0.95)
- If both strong and weak hit, strong base is used

Primary label selection
-----------------------
The caller (ClassificationService) marks the highest-confidence match
as is_primary=True. Ties are broken by topic order in TOPICS list.

Evidence format
---------------
{
    "rule_name": "keyword_match_v1",
    "text_source": "normalized_content",
    "matched_keywords": ["keyword1", "keyword2"],
    "strong_hits": 1,
    "weak_hits": 1
}
"""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import List, Optional

from src.classification.topic_rules import (
    TOPICS,
    STRONG_BASE,
    WEAK_BASE,
    EXTRA_KW_BONUS,
    MAX_CONFIDENCE,
)

CLASSIFIER_VERSION = "rule_v1"


@dataclass
class TopicMatch:
    """Result of matching a single topic against a message."""
    topic_key: str
    confidence: Decimal
    is_primary: bool = False        # set by caller after all topics are scored
    evidence: dict = field(default_factory=dict)


class TopicClassifier:
    """
    Stateless rule-based classifier.

    Usage:
        classifier = TopicClassifier()
        matches = classifier.classify("你好，今天怎么样？")
    """

    def classify(self, text: str, text_source: str = "normalized_content") -> List[TopicMatch]:
        """
        Classify *text* against all topic rules.

        Args:
            text: The message text to classify (should be normalized_content).
            text_source: Label for the evidence field indicating which field was used.

        Returns:
            List of TopicMatch for every topic that had at least one keyword hit.
            Empty list if no topic matched.
        """
        lowered = text.lower()
        matches: List[TopicMatch] = []

        for topic in TOPICS:
            strong_hits = [kw for kw in topic.strong_kws if kw.lower() in lowered]
            weak_hits = [kw for kw in topic.weak_kws if kw.lower() in lowered]

            if not strong_hits and not weak_hits:
                continue

            confidence = self._compute_confidence(strong_hits, weak_hits)
            # Deduplicate across strong/weak lists (a keyword may appear in both)
            seen: set = set()
            all_hits = []
            for kw in strong_hits + weak_hits:
                if kw not in seen:
                    seen.add(kw)
                    all_hits.append(kw)

            evidence = {
                "rule_name": "keyword_match_v1",
                "text_source": text_source,
                "matched_keywords": all_hits,
                "strong_hits": len(strong_hits),
                "weak_hits": len(weak_hits),
            }

            matches.append(TopicMatch(
                topic_key=topic.topic_key,
                confidence=confidence,
                evidence=evidence,
            ))

        # Mark the highest-confidence match as primary
        if matches:
            primary = max(matches, key=lambda m: m.confidence)
            primary.is_primary = True

        return matches

    @staticmethod
    def _compute_confidence(strong_hits: list, weak_hits: list) -> Decimal:
        """Compute confidence score from keyword hit counts."""
        if strong_hits:
            base = STRONG_BASE
            extra = (len(strong_hits) - 1 + len(weak_hits)) * EXTRA_KW_BONUS
        else:
            base = WEAK_BASE
            extra = (len(weak_hits) - 1) * EXTRA_KW_BONUS

        raw = min(base + extra, MAX_CONFIDENCE)
        # Round to 4 decimal places to match Numeric(5,4) column
        return Decimal(str(round(raw, 4)))
