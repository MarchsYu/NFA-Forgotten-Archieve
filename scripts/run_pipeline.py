#!/usr/bin/env python3
"""
Stage-1 pipeline CLI for NFA Forgotten Archive.

Runs the full ingest → topics_init → classification → profiling pipeline,
or any subset of stages via --skip.

Usage
-----
    # Full pipeline (ingest + classify + profile)
    python scripts/run_pipeline.py \\
        --chat-file exports/chat.json \\
        --window-start 2026-01-01T00:00:00Z \\
        --window-end   2026-12-31T23:59:59Z

    # Skip ingest (data already in DB)
    python scripts/run_pipeline.py \\
        --skip ingest \\
        --window-start 2000-01-01T00:00:00Z \\
        --window-end   2099-12-31T23:59:59Z

    # Re-run classification + profiling from scratch
    python scripts/run_pipeline.py \\
        --skip ingest topics_init \\
        --rerun \\
        --window-start 2026-01-01T00:00:00Z \\
        --window-end   2026-12-31T23:59:59Z

    # Restrict to one group
    python scripts/run_pipeline.py \\
        --skip ingest \\
        --group-id 11111111-1111-1111-1111-111111111111 \\
        --window-start 2026-01-01T00:00:00Z \\
        --window-end   2026-12-31T23:59:59Z

Stages
------
    ingest          Parse chat file and write messages to DB
    topics_init     Seed topics table (idempotent)
    classification  Classify messages by topic
    profiling       Build Persona Profile snapshots
"""

import argparse
import logging
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.processing.pipeline import (
    ALL_STAGES,
    PipelineParams,
    run_stage1_pipeline,
)


def _parse_datetime(value: str) -> datetime:
    value = value.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            f"Invalid datetime '{value}'. Expected ISO-8601, e.g. 2026-01-01T00:00:00Z"
        ) from exc
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def _parse_uuid(value: str) -> uuid.UUID:
    try:
        return uuid.UUID(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"Invalid UUID: {value}") from exc


def main() -> None:
    parser = argparse.ArgumentParser(
        description="NFA Forgotten Archive – Stage-1 pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )

    # ── ingest ───────────────────────────────────────────────────────────────
    parser.add_argument(
        "--chat-file",
        dest="chat_file",
        type=Path,
        metavar="PATH",
        help="Chat log file to ingest (.json / .txt / .csv). Required unless ingest is skipped.",
    )
    parser.add_argument(
        "--platform-hint",
        dest="platform_hint",
        metavar="PLATFORM",
        help="Optional platform override for the parser (e.g. 'wechat').",
    )
    parser.add_argument(
        "--group-name-hint",
        dest="group_name_hint",
        metavar="NAME",
        help="Optional group name override (required for TXT parser).",
    )

    # ── scope ────────────────────────────────────────────────────────────────
    parser.add_argument(
        "--group-id",
        dest="group_id",
        type=_parse_uuid,
        metavar="UUID",
        help="Restrict classification and profiling to one group.",
    )
    parser.add_argument(
        "--member-id",
        dest="member_id",
        type=_parse_uuid,
        metavar="UUID",
        help="Restrict profiling to one member (requires --group-id).",
    )

    # ── versions ─────────────────────────────────────────────────────────────
    parser.add_argument(
        "--classifier-version",
        dest="classifier_version",
        default=None,
        metavar="VERSION",
        help="Topic classifier version (default: rule_v1).",
    )
    parser.add_argument(
        "--profile-version",
        dest="profile_version",
        default=None,
        metavar="VERSION",
        help="Profile algorithm version (default: profile_v1).",
    )

    # ── profiling window ─────────────────────────────────────────────────────
    parser.add_argument(
        "--window-start",
        dest="window_start",
        type=_parse_datetime,
        metavar="DATETIME",
        help=(
            "Start of analysis window, ISO-8601 UTC. "
            "Required unless profiling is skipped. "
            "Use 2000-01-01T00:00:00Z for all-time."
        ),
    )
    parser.add_argument(
        "--window-end",
        dest="window_end",
        type=_parse_datetime,
        metavar="DATETIME",
        help=(
            "End of analysis window, ISO-8601 UTC. "
            "Required unless profiling is skipped. "
            "Use 2099-12-31T23:59:59Z for all-time."
        ),
    )

    # ── control ──────────────────────────────────────────────────────────────
    parser.add_argument(
        "--skip",
        dest="skip_stages",
        nargs="+",
        default=[],
        metavar="STAGE",
        choices=ALL_STAGES,
        help=f"Stage(s) to skip. Choices: {ALL_STAGES}",
    )
    parser.add_argument(
        "--rerun",
        action="store_true",
        help="Delete existing classification/profiling results and re-run from scratch.",
    )
    parser.add_argument(
        "--log-level",
        dest="log_level",
        default="WARNING",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging verbosity (default: WARNING).",
    )

    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)s %(name)s – %(message)s",
    )

    # Build params (validation happens inside run_stage1_pipeline)
    kwargs = dict(
        chat_file=args.chat_file,
        platform_hint=args.platform_hint,
        group_name_hint=args.group_name_hint,
        group_id=args.group_id,
        member_id=args.member_id,
        window_start=args.window_start,
        window_end=args.window_end,
        skip_stages=args.skip_stages,
        rerun=args.rerun,
    )
    if args.classifier_version:
        kwargs["classifier_version"] = args.classifier_version
    if args.profile_version:
        kwargs["profile_version"] = args.profile_version

    params = PipelineParams(**kwargs)

    # ── Run ──────────────────────────────────────────────────────────────────
    try:
        result = run_stage1_pipeline(params)
    except ValueError as exc:
        print(f"❌ Parameter error: {exc}", file=sys.stderr)
        sys.exit(1)

    # ── Summary ──────────────────────────────────────────────────────────────
    print(f"\n{'='*60}")
    print(f"Pipeline run : {result.run_id}")
    print(f"Overall      : {'✅ SUCCESS' if result.success else '❌ FAILED'}")
    print(f"{'='*60}")

    for stage in result.stages:
        if stage.skipped:
            status = "⏭  skipped"
        elif stage.success:
            status = "✅ ok"
        else:
            status = f"❌ FAILED – {stage.error_summary}"
        print(f"  {stage.stage:<20} {status}")

    # Per-stage detail
    if result.ingest_result:
        r = result.ingest_result
        print(f"\n[ingest]")
        print(f"  Messages inserted : {r.messages_inserted}")
        print(f"  Messages skipped  : {r.messages_skipped_duplicate}")
        print(f"  Members created   : {r.members_created}")

    if result.topics_init_result:
        r = result.topics_init_result
        print(f"\n[topics_init]")
        print(f"  Inserted : {r.inserted}")
        print(f"  Skipped  : {r.skipped}")

    if result.classification_result:
        r = result.classification_result
        print(f"\n[classification]  version={r.classifier_version}")
        print(f"  Processed  : {r.messages_processed}")
        print(f"  Skipped    : {r.messages_skipped_already_classified}")
        print(f"  Unmatched  : {r.messages_unmatched}")
        print(f"  Written    : {r.topic_assignments_written}")
        if r.missing_topic_assignments:
            print(f"  ⚠️  Dropped (topic not in DB) : {r.missing_topic_assignments}")
            print(f"     Missing keys: {r.missing_topic_keys}")
            print(f"     → Run: python scripts/init_topics.py")

    if result.profiling_result:
        r = result.profiling_result
        print(f"\n[profiling]  version={r.profile_version}")
        print(f"  Attempted : {r.members_attempted}")
        print(f"  Written   : {r.profiles_written}")
        print(f"  Skipped   : {r.profiles_skipped}")
        print(f"  Failed    : {r.profiles_failed}")
        if r.missing_topic_count:
            print(f"  ⚠️  Missing topic mappings: {r.missing_topic_count}")
        if r.failed_member_ids:
            print(f"  Failed member IDs: {r.failed_member_ids}")

    print()
    if not result.success:
        sys.exit(1)


if __name__ == "__main__":
    main()
