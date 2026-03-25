#!/usr/bin/env python3
"""
Generate Persona Profiles for group members and write them to profile_snapshots.

Usage
-----
    python scripts/run_profile_generation.py [options]

Options
-------
    --profile-version VERSION   Profile version tag (default: profile_v1)
    --window-start DATETIME     Start of analysis window, ISO-8601 UTC
    --window-end   DATETIME     End of analysis window, ISO-8601 UTC
    --group-id     UUID         Restrict to one group (optional)
    --member-id    UUID         Restrict to one member (requires --group-id)
    --rerun                     Delete existing snapshots for this version+window
                                and re-generate from scratch

Examples
--------
    # Profile all members in a group for 2026
    python scripts/run_profile_generation.py \\
        --group-id 11111111-1111-1111-1111-111111111111 \\
        --profile-version profile_v1 \\
        --window-start 2026-01-01T00:00:00Z \\
        --window-end   2026-12-31T23:59:59Z

    # Re-run (replace existing) for a single member
    python scripts/run_profile_generation.py \\
        --group-id  11111111-1111-1111-1111-111111111111 \\
        --member-id 22222222-2222-2222-2222-222222222222 \\
        --profile-version profile_v1 \\
        --window-start 2026-01-01T00:00:00Z \\
        --window-end   2026-12-31T23:59:59Z \\
        --rerun
"""

import argparse
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.profiling.profile_builder import PROFILE_VERSION
from src.profiling.profile_service import ProfileService


def _parse_datetime(value: str) -> datetime:
    """Parse an ISO-8601 datetime string and ensure it is timezone-aware (UTC)."""
    # Accept both "2026-01-01T00:00:00Z" and "2026-01-01T00:00:00+00:00"
    value = value.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            f"Invalid datetime '{value}'. Expected ISO-8601 format, e.g. 2026-01-01T00:00:00Z"
        ) from exc
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate Persona Profiles for NFA Forgotten Archive members.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--profile-version",
        default=PROFILE_VERSION,
        dest="profile_version",
        help=f"Profile version tag (default: {PROFILE_VERSION})",
    )
    parser.add_argument(
        "--window-start",
        required=True,
        dest="window_start",
        type=_parse_datetime,
        metavar="DATETIME",
        help="Start of analysis window (ISO-8601 UTC, e.g. 2026-01-01T00:00:00Z)",
    )
    parser.add_argument(
        "--window-end",
        required=True,
        dest="window_end",
        type=_parse_datetime,
        metavar="DATETIME",
        help="End of analysis window (ISO-8601 UTC, e.g. 2026-12-31T23:59:59Z)",
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
        "--rerun",
        action="store_true",
        help="Delete existing snapshots for this version+window and re-generate",
    )
    args = parser.parse_args()

    # Validate UUIDs
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

    if args.window_start >= args.window_end:
        print("❌ --window-start must be before --window-end", file=sys.stderr)
        sys.exit(1)

    # Print run parameters
    mode = "rerun (replace existing)" if args.rerun else "incremental (skip existing)"
    scope = (
        f"member {member_id}" if member_id
        else f"group {group_id}" if group_id
        else "all groups"
    )
    print(f"👤 Profile version : {args.profile_version}")
    print(f"   Mode            : {mode}")
    print(f"   Scope           : {scope}")
    print(f"   Window          : {args.window_start.isoformat()} → {args.window_end.isoformat()}")
    print()

    service = ProfileService()
    try:
        result = service.run(
            profile_version=args.profile_version,
            window_start=args.window_start,
            window_end=args.window_end,
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

    # Print summary
    status = "✅" if result.profiles_failed == 0 else "⚠️ "
    print(f"{status} Profile generation complete")
    print(f"   Members attempted   : {result.members_attempted}")
    print(f"   Profiles written    : {result.profiles_written}")
    print(f"   Profiles skipped    : {result.profiles_skipped}  (already exist for this version+window)")
    print(f"   Profiles failed     : {result.profiles_failed}")
    if result.failed_member_ids:
        print(f"   Failed member IDs   : {result.failed_member_ids}")
        print(f"   → Check warnings above for per-member error details.")

    if result.profiles_written == 0 and result.members_attempted > 0:
        if result.profiles_skipped == result.members_attempted:
            print()
            print("ℹ️  All members already have profiles for this version+window.")
            print("   Use --rerun to regenerate them.")


if __name__ == "__main__":
    main()
