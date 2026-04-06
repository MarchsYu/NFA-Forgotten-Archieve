#!/usr/bin/env python3
"""
Archive a member into the Legend Archive.

Usage
-----
    python scripts/archive_member.py <member_id> [options]

Options
-------
    --reason TEXT      Human-readable reason for archiving
    --by TEXT          Actor identifier (username, system name, etc.)
    --force            Bypass eligibility check (archive any member status)

Examples
--------
    # Archive a member who has left the group
    python scripts/archive_member.py 11111111-1111-1111-1111-111111111111

    # Archive with reason and actor
    python scripts/archive_member.py 11111111-1111-1111-1111-111111111111 \\
        --reason "Left group on 2026-06-01" --by "admin"

    # Force-archive a member regardless of status
    python scripts/archive_member.py 11111111-1111-1111-1111-111111111111 --force
"""

import argparse
import sys
import uuid
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.legend.archive_policy import ArchiveNotEligibleError
from src.legend.legend_service import LegendService


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Archive a member into the NFA Legend Archive.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("member_id", help="UUID of the member to archive")
    parser.add_argument("--reason", dest="reason", default=None, metavar="TEXT",
                        help="Human-readable reason for archiving")
    parser.add_argument("--by", dest="by", default=None, metavar="TEXT",
                        help="Actor identifier (username, system name, etc.)")
    parser.add_argument("--force", action="store_true",
                        help="Bypass eligibility check (archive any member status)")
    args = parser.parse_args()

    try:
        member_id = uuid.UUID(args.member_id)
    except ValueError:
        print(f"Invalid member_id UUID: {args.member_id}", file=sys.stderr)
        sys.exit(1)

    service = LegendService()
    try:
        result = service.archive_member(
            member_id=member_id,
            archived_reason=args.reason,
            archived_by=args.by,
            force=args.force,
        )
    except ValueError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        sys.exit(1)
    except ArchiveNotEligibleError as exc:
        print(f"Not eligible: {exc}", file=sys.stderr)
        sys.exit(1)

    if result.was_already_archived:
        print(f"Already archived (no changes made).")
    else:
        print(f"Archived successfully.")

    print(f"  Legend member ID : {result.legend_member_id}")
    print(f"  Member ID        : {result.member_id}")
    print(f"  Status           : {result.archive_status}")
    if result.profile_snapshot_id:
        print(f"  Profile snapshot : {result.profile_snapshot_id}")
    else:
        print(f"  Profile snapshot : (none – member was never profiled)")


if __name__ == "__main__":
    main()
