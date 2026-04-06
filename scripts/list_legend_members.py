#!/usr/bin/env python3
"""
List members in the Legend Archive.

Usage
-----
    python scripts/list_legend_members.py [options]

Options
-------
    --group-id UUID          Filter by group
    --status {archived,restored}
                             Filter by archive status
    --simulation-enabled     Show only simulation-enabled members
    --simulation-disabled    Show only simulation-disabled members
    --limit N                Max rows to show (default: 50)
    --offset N               Pagination offset (default: 0)

Examples
--------
    # List all legend members
    python scripts/list_legend_members.py

    # List archived members in a specific group
    python scripts/list_legend_members.py \\
        --group-id 11111111-1111-1111-1111-111111111111 --status archived

    # List simulation-enabled members
    python scripts/list_legend_members.py --simulation-enabled
"""

import argparse
import sys
import uuid
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.legend.legend_service import LegendService


def main() -> None:
    parser = argparse.ArgumentParser(
        description="List members in the NFA Legend Archive.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--group-id", dest="group_id", default=None, metavar="UUID",
                        help="Filter by group UUID")
    parser.add_argument("--status", dest="status", default=None,
                        choices=["archived", "restored"],
                        help="Filter by archive status")

    sim_group = parser.add_mutually_exclusive_group()
    sim_group.add_argument("--simulation-enabled", dest="sim_enabled",
                           action="store_true", default=None,
                           help="Show only simulation-enabled members")
    sim_group.add_argument("--simulation-disabled", dest="sim_disabled",
                           action="store_true", default=None,
                           help="Show only simulation-disabled members")

    parser.add_argument("--limit", type=int, default=50, metavar="N",
                        help="Max rows to show (default: 50)")
    parser.add_argument("--offset", type=int, default=0, metavar="N",
                        help="Pagination offset (default: 0)")
    args = parser.parse_args()

    group_id = None
    if args.group_id:
        try:
            group_id = uuid.UUID(args.group_id)
        except ValueError:
            print(f"Invalid --group-id UUID: {args.group_id}", file=sys.stderr)
            sys.exit(1)

    simulation_enabled = None
    if args.sim_enabled:
        simulation_enabled = True
    elif args.sim_disabled:
        simulation_enabled = False

    service = LegendService()
    try:
        members, total = service.list_legend_members(
            group_id=group_id,
            archive_status=args.status,
            simulation_enabled=simulation_enabled,
            limit=args.limit,
            offset=args.offset,
        )
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        sys.exit(1)

    print(f"Legend Archive  ({len(members)} of {total} total)")
    print(f"{'─' * 80}")

    if not members:
        print("  (no records match the filter)")
        return

    for m in members:
        sim_flag = "[SIM ON]" if m.simulation_enabled else "[sim off]"
        print(
            f"  {m.member_display_name_snapshot:<30} "
            f"{str(m.member_id):<36} "
            f"{m.archive_status:<10} "
            f"{sim_flag}"
        )
        print(f"    archived_at : {m.archived_at.isoformat()}")
        if m.archived_reason:
            print(f"    reason      : {m.archived_reason}")
        if m.source_profile_snapshot_id:
            print(f"    profile     : {m.source_profile_snapshot_id}")
        else:
            print(f"    profile     : (none)")

    if total > args.offset + len(members):
        remaining = total - args.offset - len(members)
        print(f"\n  ... {remaining} more. Use --offset {args.offset + args.limit} to see next page.")


if __name__ == "__main__":
    main()
