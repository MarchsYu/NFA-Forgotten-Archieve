"""
ProfileBuilder – pure computation layer for Persona Profile generation.

Responsibilities
----------------
- Receive pre-loaded messages and topic rows (no DB access here).
- Delegate to profile_analyzers for each statistical dimension.
- Assemble traits, stats, and persona_summary into a ProfileData object.
- Return ProfileData; the caller (ProfileService) handles DB writes.

ProfileData fields
------------------
Mirrors the profile_snapshots table exactly so ProfileService can write it
without any field mapping.

traits (JSONB)
    dominant_topics     : list[str]   – top topic_keys by primary count
    verbosity_level     : str         – "terse" | "moderate" | "verbose"
    style_hints         : list[str]   – e.g. ["emoji_user", "question_asker"]
    activity_pattern    : str         – "morning" | "afternoon" | "evening"
                                        | "night" | "mixed"

stats (JSONB)
    message_count           : int
    avg_message_length      : float
    top_keywords            : list[{word, count}]
    topic_distribution      : dict[str, int]   – primary topics only
    all_topics_distribution : dict[str, int]   – all matched topics
    active_hours            : dict[str, int]   – {"0": n, ..., "23": n}
    interaction_top         : list[{member_id, display_name, count}]
    classifier_version      : str              – which classifier version's
                                                 topic assignments were used
                                                 (for reproducibility)
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

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

from src.classification.topic_classifier import CLASSIFIER_VERSION

PROFILE_VERSION = "profile_v1"


@dataclass
class ProfileData:
    """
    Fully computed persona profile for one member in one time window.

    Designed to map 1-to-1 onto the profile_snapshots table row.
    """
    member_id: uuid.UUID
    group_id: uuid.UUID
    profile_version: str
    snapshot_at: datetime
    window_start: datetime
    window_end: datetime
    source_message_count: int
    persona_summary: str
    traits: Dict[str, Any] = field(default_factory=dict)
    stats: Dict[str, Any] = field(default_factory=dict)


class ProfileBuilder:
    """
    Stateless builder: given raw data, produce a ProfileData.

    Usage::

        builder = ProfileBuilder()
        profile = builder.build(
            member_id=...,
            group_id=...,
            messages=[...],          # Message ORM objects
            topic_rows=[...],        # (topic_key, is_primary) named tuples / objects
            reply_targets=[...],     # [{"member_id": ..., "display_name": ...}]
            profile_version="profile_v1",
            window_start=...,
            window_end=...,
        )
    """

    def build(
        self,
        member_id: uuid.UUID,
        group_id: uuid.UUID,
        messages: List[Any],
        topic_rows: List[Any],
        reply_targets: List[Dict[str, Any]],
        profile_version: str = PROFILE_VERSION,
        classifier_version: str = CLASSIFIER_VERSION,
        window_start: Optional[datetime] = None,
        window_end: Optional[datetime] = None,
    ) -> ProfileData:
        """
        Compute and return a ProfileData for the given member.

        Args:
            member_id:          UUID of the member being profiled.
            group_id:           UUID of the group.
            messages:           All Message ORM objects for this member in the window.
            topic_rows:         Objects with .topic_key (str) and .is_primary (bool).
                                One row per (message, topic) assignment.
                                Must already be filtered to the desired classifier_version
                                by the caller (ProfileService._load_topic_rows).
            reply_targets:      Pre-resolved reply targets from ProfileService.
                                Each entry: {"member_id": str, "display_name": str}.
            profile_version:    Version tag written to the snapshot row.
            classifier_version: The topic classifier version whose assignments were
                                used to build topic_rows.  Stored in stats for
                                reproducibility – does NOT filter here (filtering
                                is the service's responsibility).
            window_start:       Start of the analysis window (UTC).
            window_end:         End of the analysis window (UTC).

        Returns:
            ProfileData ready to be persisted by ProfileService.
        """
        now = datetime.now(tz=timezone.utc)
        window_start = window_start or now
        window_end = window_end or now

        # --- Raw statistics ---
        msg_stats = compute_message_stats(messages)
        top_kws = compute_top_keywords(messages)
        primary_dist, all_dist = compute_topic_distribution(topic_rows)
        active_hours = compute_active_hours(messages)
        interaction_top = compute_interaction_top(reply_targets)

        avg_length: float = msg_stats["avg_message_length"]

        # --- Derived traits ---
        verbosity = compute_verbosity_level(avg_length)
        activity_pattern = compute_activity_pattern(active_hours)
        style_hints = compute_style_hints(messages, primary_dist, avg_length)

        dominant_topics = sorted(
            primary_dist, key=lambda k: primary_dist[k], reverse=True
        )[:5]

        # --- Summary ---
        summary = build_persona_summary(
            message_count=msg_stats["message_count"],
            dominant_topics=dominant_topics,
            verbosity_level=verbosity,
            activity_pattern=activity_pattern,
            interaction_top=interaction_top,
            window_start=window_start,
            window_end=window_end,
        )

        traits: Dict[str, Any] = {
            "dominant_topics": dominant_topics,
            "verbosity_level": verbosity,
            "style_hints": style_hints,
            "activity_pattern": activity_pattern,
        }

        stats: Dict[str, Any] = {
            "message_count": msg_stats["message_count"],
            "avg_message_length": avg_length,
            "top_keywords": top_kws,
            "topic_distribution": primary_dist,
            "all_topics_distribution": all_dist,
            "active_hours": active_hours,
            "interaction_top": [
                {**e, "member_id": str(e["member_id"])}
                for e in interaction_top
            ],
            # Traceability: record which classifier version's topics were consumed.
            # This makes every profile_snapshot independently reproducible.
            "classifier_version": classifier_version,
        }

        return ProfileData(
            member_id=member_id,
            group_id=group_id,
            profile_version=profile_version,
            snapshot_at=now,
            window_start=window_start,
            window_end=window_end,
            source_message_count=msg_stats["message_count"],
            persona_summary=summary,
            traits=traits,
            stats=stats,
        )
