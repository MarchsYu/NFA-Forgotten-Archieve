#!/usr/bin/env python3
"""
Toggle Persona Simulation on/off for a Legend Archive member.

Usage
-----
    python scripts/toggle_legend_simulation.py <member_id> {enable,disable}

Examples
--------
    # Enable simulation for a legend member
    python scripts/toggle_legend_simulation.py \\
        11111111-1111-1111-1111-111111111111 enable

    # Disable simulation
    python scripts/toggle_legend_simulation.py \\
        11111111-1111-1111-1111-111111111111 disable
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
        description="Toggle Persona Simulation for a Legend Archive member.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("member_id", help="UUID of the legend member")
    parser.add_argument("action", choices=["enable", "disable"],
                        help="'enable' or 'disable' simulation")
    args = parser.parse_args()

    try:
        member_id = uuid.UUID(args.member_id)
    except ValueError:
        print(f"Invalid member_id UUID: {args.member_id}", file=sys.stderr)
        sys.exit(1)

    service = LegendService()
    try:
        if args.action == "enable":
            result = service.enable_simulation(member_id)
        else:
            result = service.disable_simulation(member_id)
    except ValueError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        sys.exit(1)
    except InvalidStatusTransitionError as exc:
        print(f"Not allowed: {exc}", file=sys.stderr)
        sys.exit(1)

    state = "ENABLED" if result.simulation_enabled else "DISABLED"
    print(f"Simulation {state} for member {result.member_id}")
    print(f"  Legend member ID : {result.legend_member_id}")


if __name__ == "__main__":
    main()
