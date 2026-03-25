#!/usr/bin/env python3
"""
Generate Persona Profile snapshots for group members.

This is the primary entry point for Persona Profile generation (Task 5).

IMPORTANT: window parameters are REQUIRED for idempotency.
Every profile_snapshot is keyed by
(member_id, profile_version, window_start, window_end).
Using a non-fixed window_end (e.g. "now") would create a new key on
every run, bypassing the "skip existing" logic and accumulating
duplicate snapshots.  Always pass explicit, fixed window boundaries.

Usage
-----
    python scripts/run_profiling.py [options]

Options
-------
    --profile-version     VERSION   Profile algorithm version (default: profile_v1)
    --classifier-version  VERSION   Topic classifier version (default: rule_v1)
    --window-start        DATETIME  Start of analysis window, ISO-8601 UTC  [REQUIRED]
    --window-end          DATETIME  End of analysis window, ISO-8601 UTC    [REQUIRED]
    --group-id            UUID      Restrict to one group (optional)
    --member-id           UUID      Restrict to one member (requires --group-id)
    --rerun                         Delete existing snapshots and re-generate

Common window conventions
-------------------------
  Full-year:  --window-start 2026-01-01T00:00:00Z --window-end 2026-12-31T23:59:59Z
  All-time:   --window-start 2000-01-01T00:00:00Z --window-end 2099-12-31T23:59:59Z

Examples
--------
    python scripts/run_profiling.py \
        --group-id 11111111-1111-1111-1111-111111111111 \
        --profile-version profile_v1 \
        --window-start 2026-01-01T00:00:00Z \
        --window-end   2026-12-31T23:59:59Z
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
        help=f"Topic classifier version (default: {CLASSIFIER_VERSION})",
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
        required=True,
        dest="window_start",
        type=_parse_datetime,
        metavar="DATETIME",
        help=(
            "[REQUIRED] Start of analysis window, ISO-8601 UTC. "
            "Must be a fixed value for idempotent runs. "
            "Use 2000-01-01T00:00:00Z for all-time."
        ),
    )
    parser.add_argument(
        "--window-end",
        required=True,
        dest="window_end",
        type=_parse_datetime,
        metavar="DATETIME",
        help=(
            "[REQUIRED] End of analysis window, ISO-8601 UTC. "
            "Must be a fixed value for idempotent runs. "
            "Use 2099-12-31T23:59:59Z for all-time."
        ),
    )
    parser.add_argument(
        "--rerun",
        action="store_true",
        help="Delete existing snapshots for this version+window and re-generate",
    )
    args = parser.parse_args()

    # Validate UUIDs
    group_id = None
    member_id = None

    if args.group_id:
        try:
            group_id = uuid.UUID(args.group_id)
        except ValueError:
            print(f"Invalid --group-id UUID: {args.group_id}", file=sys.stderr)
            sys.exit(1)

    if args.member_id:
        try:
            member_id = uuid.UUID(args.member_id)
        except ValueError:
            print(f"Invalid --member-id UUID: {args.member_id}", file=sys.stderr)
            sys.exit(1)
        if group_id is None:
            print("--member-id requires --group-id", file=sys.stderr)
            sys.exit(1)

    if args.window_start >= args.window_end:
        print("--window-start must be before --window-end", file=sys.stderr)
        sys.exit(1)

    # Print run parameters
    mode = "rerun (replace existing)" if args.rerun else "incremental (skip existing)"
    scope = (
        f"member {member_id}" if member_id
        else f"group {group_id}" if group_id
        else "all groups"
    )
    print(f"Profile version     : {args.profile_version}")
    print(f"Classifier version  : {args.classifier_version}  (topic assignments source)")
    print(f"Mode                : {mode}")
    print(f"Scope               : {scope}")
    print(f"Window              : {args.window_start.isoformat()} -> {args.window_end.isoformat()}")
    print()

    # Run
    service = ProfileService()
    try:
        result = service.run(
            profile_version=args.profile_version,
            classifier_version=args.classifier_version,
            window_start=args.window_start,
            window_end=args.window_end,
            group_id=group_id,
            member_id=member_id,
            rerun=args.rerun,
        )
    except ValueError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        sys.exit(1)
    except Exception as exc:
        print(f"Unexpected error: {exc}", file=sys.stderr)
        raise

    # Print summary
    ok = result.profiles_failed == 0
    print(f"Persona Profile generation {'complete' if ok else 'complete (with failures)'}")
    print(f"  Members attempted      : {result.members_attempted}")
    print(f"  Profiles written       : {result.profiles_written}")
    print(f"  Profiles skipped       : {result.profiles_skipped}"
          f"  (already exist for this version+window)")
    print(f"  Profiles failed        : {result.profiles_failed}")

    if result.missing_topic_count:
        print(f"  WARNING missing topic mappings : {result.missing_topic_count}"
              f"  (topic_id not in DB)")
        print("    -> Run: python scripts/init_topics.py")

    if result.failed_member_ids:
        print(f"  Failed member IDs      : {result.failed_member_ids}")
        print("  -> Check warnings above for per-member error details.")
        print("  -> Other members profiles were NOT rolled back (savepoint isolation).")

    if result.profiles_written == 0 and result.members_attempted > 0:
        if result.profiles_skipped == result.members_attempted:
            print()
            print("All members already have profiles for this version+window.")
            print("Use --rerun to regenerate them.")

    if result.profiles_written > 0:
        print()
        print("Profile structure written to profile_snapshots:")
        print("  traits : dominant_topics, verbosity_level, style_hints, activity_pattern")
        print("  stats  : message_count, avg_message_length, top_keywords,")
        print("           topic_distribution, all_topics_distribution,")
        print("           active_hours (UTC hour-of-day 0-23), interaction_top,")
        print("           classifier_version")
        print("  persona_summary : rule-template Chinese text summary")


if __name__ == "__main__":
    main()
