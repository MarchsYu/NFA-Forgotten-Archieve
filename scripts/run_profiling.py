#!/usr/bin/env python3
"""
Generate Persona Profile snapshots for group members.

This is the primary entry point for Persona Profile generation (Task 5).
Window parameters are optional: omitting them covers all messages in the DB.

Usage
-----
    python scripts/run_profiling.py [options]

Options
-------
    --profile-version     VERSION   Profile algorithm version (default: profile_v1)
    --classifier-version  VERSION   Topic classifier version to read from
                                    message_topics (default: rule_v1)
    --group-id            UUID      Restrict to one group (optional)
    --member-id           UUID      Restrict to one member (requires --group-id)
    --window-start        DATETIME  Start of analysis window, ISO-8601 UTC
                                    (default: 2000-01-01T00:00:00Z – all time)
    --window-end          DATETIME  End of analysis window, ISO-8601 UTC
                                    (default: current UTC time)
    --rerun                         Delete existing snapshots for this
                                    version+window and re-generate from scratch

Versioning semantics
--------------------
  --profile-version    : identifies the profiling algorithm.
                         Stored in profile_snapshots.profile_version.
                         Use a new value (e.g. profile_v2) when the analysis
                         logic changes and you want to keep old snapshots.

  --classifier-version : identifies which MessageTopic rows to consume.
                         Stored in profile_snapshots.stats["classifier_version"].
                         Must match the version used in run_topic_classification.py.

Idempotency
-----------
  Default (incremental): members who already have a snapshot for the given
  profile_version + window are skipped.

  --rerun: existing snapshots for this version + window are deleted first,
  then all members are re-profiled.  Per-member failures do NOT roll back
  other members' snapshots (savepoint isolation).

Examples
--------
    # Profile all members across all groups (full-time window)
    python scripts/run_profiling.py --profile-version profile_v1

    # Profile all members in one group for a specific year
    python scripts/run_profiling.py \\
        --group-id 11111111-1111-1111-1111-111111111111 \\
        --profile-version profile_v1 \\
        --window-start 2026-01-01T00:00:00Z \\
        --window-end   2026-12-31T23:59:59Z

    # Re-run for a single member
    python scripts/run_profiling.py \\
        --group-id  11111111-1111-1111-1111-111111111111 \\
        --member-id 22222222-2222-2222-2222-222222222222 \\
        --profile-version profile_v1 \\
        --rerun
"""

import argparse
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.classification.topic_classifier import CLASSIFIER_VERSION
from src.profiling.profile_builder import PROFILE_VERSION
from src.profiling.profile_service import ProfileService

# Default "all-time" window start – earlier than any realistic chat export
_ALL_TIME_START = datetime(2000, 1, 1, tzinfo=timezone.utc)


def _parse_datetime(value: str) -> datetime:
    """Parse ISO-8601 datetime string, ensure timezone-aware (UTC)."""
    value = value.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            f"Invalid datetime '{value}'. "
            f"Expected ISO-8601 format, e.g. 2026-01-01T00:00:00Z"
        ) from exc
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate Persona Profile snapshots (NFA Forgotten Archive).",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--profile-version",
        default=PROFILE_VERSION,
        dest="profile_version",
        metavar="VERSION",
        help=f"Profile algorithm version (default: {PROFILE_VERSION})",
    )
    parser.add_argument(
        "--classifier-version",
        default=CLASSIFIER_VERSION,
        dest="classifier_version",
        metavar="VERSION",
        help=(
            f"Topic classifier version to read from message_topics "
            f"(default: {CLASSIFIER_VERSION})"
        ),
    )
    parser.add_argument(
        "--group-id",
        default=None,
        dest="group_id",
        metavar="UUID",
        help="Restrict to one group",
    )
    parser.add_argument(
        "--member-id",
        default=None,
        dest="member_id",
        metavar="UUID",
        help="Restrict to one member (requires --group-id)",
    )
    parser.add_argument(
        "--window-start",
        default=None,
        dest="window_start",
        type=_parse_datetime,
        metavar="DATETIME",
        help=(
            "Start of analysis window, ISO-8601 UTC "
            f"(default: {_ALL_TIME_START.isoformat()} – all time)"
        ),
    )
    parser.add_argument(
        "--window-end",
        default=None,
        dest="window_end",
        type=_parse_datetime,
        metavar="DATETIME",
        help="End of analysis window, ISO-8601 UTC (default: current UTC time)",
    )
    parser.add_argument(
        "--rerun",
        action="store_true",
        help="Delete existing snapshots for this version+window and re-generate",
    )
    args = parser.parse_args()

    # ── Validate UUIDs ────────────────────────────────────────────────
    group_id: uuid.UUID | None = None
    member_id: uuid.UUID | None = None

    if args.group_id:
        try:
            group_id = uuid.UUID(args.group_id)
        except ValueError:
            print(f"❌ Invalid --group-id UUID: {args.group_id}", file=sys.stderr)
            sys.exit(1)

    if args.member_id:
        try:
            member_id = uuid.UUID(args.member_id)
        except ValueError:
            print(f"❌ Invalid --member-id UUID: {args.member_id}", file=sys.stderr)
            sys.exit(1)
        if group_id is None:
            print("❌ --member-id requires --group-id", file=sys.stderr)
            sys.exit(1)

    # ── Resolve window defaults ───────────────────────────────────────
    window_start: datetime = args.window_start or _ALL_TIME_START
    window_end: datetime = args.window_end or datetime.now(tz=timezone.utc)

    if window_start >= window_end:
        print("❌ --window-start must be before --window-end", file=sys.stderr)
        sys.exit(1)

    # ── Print run parameters ──────────────────────────────────────────
    mode = "rerun (replace existing)" if args.rerun else "incremental (skip existing)"
    scope = (
        f"member {member_id}" if member_id
        else f"group {group_id}" if group_id
        else "all groups"
    )
    window_label = (
        "all time"
        if args.window_start is None and args.window_end is None
        else f"{window_start.isoformat()} → {window_end.isoformat()}"
    )

    print(f"👤 Profile version     : {args.profile_version}")
    print(f"   Classifier version  : {args.classifier_version}  (topic assignments source)")
    print(f"   Mode                : {mode}")
    print(f"   Scope               : {scope}")
    print(f"   Window              : {window_label}")
    print()

    # ── Run ───────────────────────────────────────────────────────────
    service = ProfileService()
    try:
        result = service.run(
            profile_version=args.profile_version,
            classifier_version=args.classifier_version,
            window_start=window_start,
            window_end=window_end,
            group_id=group_id,
            member_id=member_id,
            rerun=args.rerun,
        )
    except ValueError as exc:
        print(f"❌ {exc}", file=sys.stderr)
        sys.exit(1)
    except Exception as exc:
        print(f"❌ Unexpected error: {exc}", file=sys.stderr)
        raise

    # ── Print summary ─────────────────────────────────────────────────
    status = "✅" if result.profiles_failed == 0 else "⚠️ "
    print(f"{status} Persona Profile generation complete")
    print(f"   Members attempted      : {result.members_attempted}")
    print(f"   Profiles written       : {result.profiles_written}")
    print(f"   Profiles skipped       : {result.profiles_skipped}"
          f"  (already exist for this version+window)")
    print(f"   Profiles failed        : {result.profiles_failed}")

    if result.missing_topic_count:
        print(f"   ⚠️  Missing topic mappings : {result.missing_topic_count}"
              f"  (topic_id not in DB)")
        print(f"      → Run: python scripts/init_topics.py")

    if result.failed_member_ids:
        print(f"   Failed member IDs      : {result.failed_member_ids}")
        print(f"   → Check warnings above for per-member error details.")
        print(f"   → Other members' profiles were NOT rolled back (savepoint isolation).")

    if result.profiles_written == 0 and result.members_attempted > 0:
        if result.profiles_skipped == result.members_attempted:
            print()
            print("ℹ️  All members already have profiles for this version+window.")
            print("   Use --rerun to regenerate them.")

    if result.profiles_written > 0:
        print()
        print("ℹ️  Profile structure written to profile_snapshots:")
        print("   traits : dominant_topics, verbosity_level, style_hints, activity_pattern")
        print("   stats  : message_count, avg_message_length, top_keywords,")
        print("            topic_distribution, all_topics_distribution,")
        print("            active_hours, interaction_top, classifier_version")
        print("   persona_summary : rule-template Chinese text summary")


if __name__ == "__main__":
    main()
