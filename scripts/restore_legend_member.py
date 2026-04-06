#!/usr/bin/env python3
"""
Restore an archived Legend Archive member.

Usage
-----
    python scripts/restore_legend_member.py <member_id>

Notes
-----
- Sets archive_status to "restored" and simulation_enabled to False.
- The legend_members row and the members row are both preserved.
- The member can be re-archived later if needed.

Example
-------
    python scripts/restore_legend_member.py 11111111-1111-1111-1111-111111111111
"""

import argparse
import sys
import uuid
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.legend.archive_policy import InvalidStatusTransitionError
from src.legend.legend_service import LegendService


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Restore an archived member from the NFA Legend Archive.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("member_id", help="UUID of the legend member to restore")
    args = parser.parse_args()

    try:
        member_id = uuid.UUID(args.member_id)
    except ValueError:
        print(f"Invalid member_id UUID: {args.member_id}", file=sys.stderr)
        sys.exit(1)

    service = LegendService()
    try:
        result = service.restore_member(member_id)
    except ValueError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        sys.exit(1)
    except InvalidStatusTransitionError as exc:
        print(f"Not allowed: {exc}", file=sys.stderr)
        sys.exit(1)

    print(f"Restored successfully.")
    print(f"  Legend member ID : {result.legend_member_id}")
    print(f"  Member ID        : {result.member_id}")
    print(f"  Status           : {result.archive_status}")
    print(f"  Simulation       : disabled (automatically)")


if __name__ == "__main__":
    main()
