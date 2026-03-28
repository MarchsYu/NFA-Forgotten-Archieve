#!/usr/bin/env python3
"""
Stage 1 pipeline entry point – NFA Forgotten Archive.

Runs the full first-phase processing chain in order:
    1. ingest          – import chat log into DB
    2. topics_init     – seed topic taxonomy (idempotent)
    3. classification  – assign topics to messages
    4. profiling       – generate Persona Profile snapshots

Usage
-----
    python scripts/run_stage1_pipeline.py \\
        --input-file data/raw/sample_chat.json \\
        --platform qq \\
        --classifier-version rule_v1 \\
        --profile-version profile_v1 \\
        --window-start 2026-01-01T00:00:00Z \\
        --window-end   2026-03-01T00:00:00Z

Skip / rerun individual stages
-------------------------------
    --skip-ingest            Do not import a file (use data already in DB)
    --skip-classification    Skip topic classification
    --skip-profiling         Skip Persona Profile generation
    --rerun-classification   Delete existing classification results and redo
    --rerun-profiling        Delete existing profile snapshots and redo

Scope filters (classification + profiling only)
-----------------------------------------------
    --group-id UUID          Restrict to one group
    --member-id UUID         Restrict to one member (requires --group-id)

Optional
--------
    --group-name NAME        Group name hint (required for .txt files)
    --dry-run                Print what would run without executing
    --continue-on-error      Continue to next stage even if a stage fails

Examples
--------
    # Full run
    python scripts/run_stage1_pipeline.py \\
        --input-file data/raw/chat.json --platform wechat \\
        --window-start 2026-01-01T00:00:00Z --window-end 2026-12-31T23:59:59Z

    # Skip ingest (data already imported), re-run classification + profiling
    python scripts/run_stage1_pipeline.py \\
        --skip-ingest --rerun-classification --rerun-profiling \\
        --window-start 2026-01-01T00:00:00Z --window-end 2026-12-31T23:59:59Z

    # Dry run – see what would happen
    python scripts/run_stage1_pipeline.py \\
        --input-file data/raw/chat.json --platform qq \\
        --window-start 2026-01-01T00:00:00Z --window-end 2026-12-31T23:59:59Z \\
        --dry-run
"""

import argparse
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.processing import PipelineParams, run_stage1_pipeline


# ── Helpers ───────────────────────────────────────────────────────────────────

def _parse_datetime(value: str) -> datetime:
    """Parse ISO-8601 datetime string; ensure timezone-aware (UTC)."""
    value = value.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            f"Invalid datetime '{value}'. "
            "Expected ISO-8601 format, e.g. 2026-01-01T00:00:00Z"
        ) from exc
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def _parse_uuid(value: str, flag: str) -> uuid.UUID:
    try:
        return uuid.UUID(value)
    except ValueError:
        raise argparse.ArgumentTypeError(f"Invalid UUID for {flag}: {value!r}")


def _status_icon(status: str) -> str:
    return {"success": "✅", "skipped": "⏭ ", "failed": "❌", "dry_run": "🔍"}.get(status, "?")


def _print_summary(result) -> None:
    """Print a human-readable pipeline summary."""
    print()
    print("=" * 60)
    print(f"  Pipeline run_id : {result.run_id}")
    print(f"  Overall status  : {result.overall_status.upper()}")
    print("=" * 60)

    # ── ingest ────────────────────────────────────────────────────────────────
    r = result.ingest
    icon = _status_icon(r.status)
    print(f"\n{icon} Stage 1 – Ingest  [{r.status}]")
    if r.status == "success":
        print(f"     Messages inserted        : {r.messages_inserted}")
        print(f"     Messages skipped (dup)   : {r.messages_skipped_duplicate}")
        print(f"     Members created          : {r.members_created}")
    elif r.status == "failed":
        _print_error(r.error_message)
    elif r.status == "skipped":
        print(f"     Reason: {r.error_message}")

    # ── topics_init ───────────────────────────────────────────────────────────
    r = result.topics_init
    icon = _status_icon(r.status)
    print(f"\n{icon} Stage 2 – Topics Init  [{r.status}]")
    if r.status == "success":
        print(f"     Topics inserted          : {r.topics_inserted}")
        print(f"     Topics already existed   : {r.topics_already_existed}")
    elif r.status == "failed":
        _print_error(r.error_message)
    elif r.status == "skipped":
        print(f"     Reason: {r.error_message}")

    # ── classification ────────────────────────────────────────────────────────
    r = result.classification
    icon = _status_icon(r.status)
    print(f"\n{icon} Stage 3 – Classification  [{r.status}]")
    if r.status == "success":
        print(f"     Messages processed       : {r.messages_processed}")
        print(f"     Messages skipped (done)  : {r.messages_skipped_already_classified}")
        print(f"     Messages unmatched       : {r.messages_unmatched}")
        print(f"     Topic assignments written: {r.topic_assignments_written}")
        if r.missing_topic_assignments:
            print(f"     ⚠️  Assignments dropped   : {r.missing_topic_assignments}"
                  " (topic not in DB → run init_topics.py)")
    elif r.status == "failed":
        _print_error(r.error_message)
    elif r.status == "skipped":
        print(f"     Reason: {r.error_message}")

    # ── profiling ─────────────────────────────────────────────────────────────
    r = result.profiling
    icon = _status_icon(r.status)
    print(f"\n{icon} Stage 4 – Profiling  [{r.status}]")
    if r.status == "success":
        print(f"     Members attempted        : {r.members_attempted}")
        print(f"     Profiles written         : {r.profiles_written}")
        print(f"     Profiles skipped (exist) : {r.profiles_skipped}")
        print(f"     Profiles failed          : {r.profiles_failed}")
        if r.missing_topic_count:
            print(f"     ⚠️  Missing topic mappings: {r.missing_topic_count}"
                  " → run init_topics.py")
        if r.failed_member_ids:
            print(f"     Failed member IDs        : {r.failed_member_ids}")
    elif r.status == "failed":
        _print_error(r.error_message)
    elif r.status == "skipped":
        print(f"     Reason: {r.error_message}")

    print()


def _print_error(msg: str | None) -> None:
    if not msg:
        return
    # Show only the first line of the traceback in the summary; full trace
    # is already printed to stderr by the stage runner.
    first_line = msg.splitlines()[0] if msg else ""
    print(f"     Error: {first_line}")


# ── CLI ───────────────────────────────────────────────────────────────────────

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="NFA Forgotten Archive – Stage 1 pipeline.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )

    # Ingest
    ingest_group = parser.add_argument_group("Ingest")
    ingest_group.add_argument(
        "--input-file", dest="input_file", type=Path, default=None,
        metavar="PATH",
        help="Chat log file to import (.json / .csv / .txt).",
    )
    ingest_group.add_argument(
        "--platform", default=None, metavar="PLATFORM",
        help="Platform hint (e.g. qq, wechat, discord, generic).",
    )
    ingest_group.add_argument(
        "--group-name", dest="group_name", default=None, metavar="NAME",
        help="Group name override (required for .txt files).",
    )

    # Classification
    cls_group = parser.add_argument_group("Classification")
    cls_group.add_argument(
        "--classifier-version", dest="classifier_version",
        default=None, metavar="VERSION",
        help="Classifier version tag (default: rule_v1).",
    )

    # Profiling
    prof_group = parser.add_argument_group("Profiling")
    prof_group.add_argument(
        "--profile-version", dest="profile_version",
        default=None, metavar="VERSION",
        help="Profile algorithm version (default: profile_v1).",
    )
    prof_group.add_argument(
        "--window-start", dest="window_start",
        type=_parse_datetime, default=None, metavar="DATETIME",
        help="[REQUIRED for profiling] Start of analysis window, ISO-8601 UTC.",
    )
    prof_group.add_argument(
        "--window-end", dest="window_end",
        type=_parse_datetime, default=None, metavar="DATETIME",
        help="[REQUIRED for profiling] End of analysis window, ISO-8601 UTC.",
    )

    # Scope filters
    scope_group = parser.add_argument_group("Scope filters")
    scope_group.add_argument(
        "--group-id", dest="group_id", default=None, metavar="UUID",
        help="Restrict classification + profiling to one group.",
    )
    scope_group.add_argument(
        "--member-id", dest="member_id", default=None, metavar="UUID",
        help="Restrict profiling to one member (requires --group-id).",
    )

    # Stage control
    ctrl_group = parser.add_argument_group("Stage control")
    ctrl_group.add_argument(
        "--skip-ingest", dest="skip_ingest", action="store_true",
        help="Skip the ingest stage.",
    )
    ctrl_group.add_argument(
        "--skip-classification", dest="skip_classification", action="store_true",
        help="Skip the classification stage.",
    )
    ctrl_group.add_argument(
        "--skip-profiling", dest="skip_profiling", action="store_true",
        help="Skip the profiling stage.",
    )
    ctrl_group.add_argument(
        "--rerun-classification", dest="rerun_classification", action="store_true",
        help="Delete existing classification results and re-classify.",
    )
    ctrl_group.add_argument(
        "--rerun-profiling", dest="rerun_profiling", action="store_true",
        help="Delete existing profile snapshots and re-generate.",
    )

    # Optional enhancements
    opt_group = parser.add_argument_group("Optional")
    opt_group.add_argument(
        "--dry-run", dest="dry_run", action="store_true",
        help="Print what would run without executing any stage.",
    )
    opt_group.add_argument(
        "--continue-on-error", dest="continue_on_error", action="store_true",
        help="Continue to next stage even if a stage fails.",
    )

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    # ── Validate UUIDs ────────────────────────────────────────────────────────
    group_id: uuid.UUID | None = None
    member_id: uuid.UUID | None = None

    if args.group_id:
        group_id = _parse_uuid(args.group_id, "--group-id")
    if args.member_id:
        member_id = _parse_uuid(args.member_id, "--member-id")
        if group_id is None:
            parser.error("--member-id requires --group-id")

    # ── Validate ingest args ──────────────────────────────────────────────────
    if not args.skip_ingest and args.input_file is None and not args.dry_run:
        parser.error(
            "--input-file is required unless --skip-ingest or --dry-run is set."
        )

    # ── Validate profiling window ─────────────────────────────────────────────
    if not args.skip_profiling and not args.dry_run:
        if args.window_start is None or args.window_end is None:
            parser.error(
                "--window-start and --window-end are required for profiling. "
                "Use --skip-profiling to omit the profiling stage."
            )

    # ── Build params ──────────────────────────────────────────────────────────
    params_kwargs = dict(
        input_file=args.input_file,
        platform=args.platform,
        group_name=args.group_name,
        window_start=args.window_start,
        window_end=args.window_end,
        group_id=group_id,
        member_id=member_id,
        skip_ingest=args.skip_ingest,
        skip_classification=args.skip_classification,
        skip_profiling=args.skip_profiling,
        rerun_classification=args.rerun_classification,
        rerun_profiling=args.rerun_profiling,
        dry_run=args.dry_run,
        continue_on_error=args.continue_on_error,
    )
    if args.classifier_version:
        params_kwargs["classifier_version"] = args.classifier_version
    if args.profile_version:
        params_kwargs["profile_version"] = args.profile_version

    params = PipelineParams(**params_kwargs)

    # ── Print run header ──────────────────────────────────────────────────────
    print("🚀 NFA Forgotten Archive – Stage 1 Pipeline")
    if not args.dry_run:
        print(f"   Classifier version : {params.classifier_version}")
        print(f"   Profile version    : {params.profile_version}")
        if params.window_start:
            print(f"   Window             : {params.window_start.isoformat()} → "
                  f"{params.window_end.isoformat()}")
        if params.group_id:
            print(f"   Group filter       : {params.group_id}")
        if params.member_id:
            print(f"   Member filter      : {params.member_id}")
        if params.continue_on_error:
            print("   continue-on-error  : enabled")
    print()

    # ── Run ───────────────────────────────────────────────────────────────────
    result = run_stage1_pipeline(params)

    # ── Print summary ─────────────────────────────────────────────────────────
    _print_summary(result)

    # ── Exit code ─────────────────────────────────────────────────────────────
    if result.overall_status in ("success", "dry_run"):
        sys.exit(0)
    elif result.overall_status == "partial":
        sys.exit(2)   # partial success – some stages failed
    else:
        sys.exit(1)   # full failure


if __name__ == "__main__":
    main()
