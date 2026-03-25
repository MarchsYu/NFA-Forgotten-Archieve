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
1. Only topics with is_primary_eligible=True are candidates for is_primary.
2. Among eligible candidates, the highest-confidence match wins.
3. Ties are broken by topic order in TOPICS list (first defined wins).
4. Fallback: if no eligible candidate exists, the highest-confidence match
   overall is marked is_primary (should not happen with current taxonomy).

Conflict handling
-----------------
Topics can overlap (e.g. casual_chat and meme both match "哈哈哈"). The
is_primary flag resolves the "main" topic; all matched topics are still
written to message_topics so the full signal is preserved.

Evidence format
---------------
{
    "rule_name": "keyword_match_v1",
    "text_source": "normalized_content",
    "matched_keywords": ["kw1", "kw2"],       # deduplicated union
    "strong_matched_keywords": ["kw1"],        # strong hits only
    "weak_matched_keywords": ["kw2"],          # weak hits only
    "matched_excerpt": "…surrounding text…",  # snippet around first keyword hit
}
"""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import List

from src.classification.topic_rules import (
    TOPICS,
    TOPIC_MAP,
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
    is_primary: bool = False
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
                "strong_matched_keywords": strong_hits,
                "weak_matched_keywords": weak_hits,
                "matched_excerpt": self._extract_excerpt(text, all_hits),
            }

            matches.append(TopicMatch(
                topic_key=topic.topic_key,
                confidence=confidence,
                evidence=evidence,
            ))

        # Primary label selection:
        # 1. Prefer is_primary_eligible topics
        # 2. Highest confidence wins; ties broken by TOPICS list order (already preserved)
        if matches:
            eligible = [m for m in matches if TOPIC_MAP[m.topic_key].is_primary_eligible]
            primary_pool = eligible if eligible else matches  # fallback: all matches
            primary = max(primary_pool, key=lambda m: m.confidence)
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
        return Decimal(str(round(raw, 4)))

    @staticmethod
    def _extract_excerpt(text: str, keywords: list, window: int = 30) -> str:
        """
        Return a short snippet of *text* centred on the first matched keyword.

        Searches case-insensitively. Pads with "…" when the match is not at
        the start or end of the text. Returns "" if no keyword is found.
        """
        if not keywords or not text:
            return ""
        lowered = text.lower()
        for kw in keywords:
            pos = lowered.find(kw.lower())
            if pos >= 0:
                start = max(0, pos - window)
                end = min(len(text), pos + len(kw) + window)
                excerpt = text[start:end]
                if start > 0:
                    excerpt = "…" + excerpt
                if end < len(text):
                    excerpt = excerpt + "…"
                return excerpt
        return ""
