# -*- coding: utf-8 -*-
#!/usr/bin/env python3
"""
Run a single Persona Simulation round via the Legend Archive.

Usage
-----
    # Echo mode (default, no credentials required):
    python scripts/run_simulation.py \\
        --legend-member-id <UUID> \\
        --message "你最近在玩什么游戏？"

    # Real LLM via environment variables:
    SIM_LLM_PROVIDER=openai_compatible \\
    SIM_LLM_API_KEY=sk-... \\
    SIM_LLM_MODEL=gpt-4o-mini \\
    python scripts/run_simulation.py \\
        --legend-member-id <UUID> \\
        --message "推荐个游戏"

    # Override provider / model on the command line:
    python scripts/run_simulation.py \\
        --legend-member-id <UUID> \\
        --message "推荐个游戏" \\
        --provider openai_compatible \\
        --model gpt-4o-mini \\
        --base-url https://openrouter.ai/api/v1

Options
-------
    --legend-member-id UUID   UUID of the legend member to simulate (required)
    --message TEXT            The message to present to the simulated persona (required)
    --provider STR            echo | openai_compatible  (default: echo or SIM_LLM_PROVIDER)
    --model STR               Model name (overrides SIM_LLM_MODEL)
    --base-url STR            API base URL (overrides SIM_LLM_BASE_URL)
    --history-limit INT       Max historical messages to sample (1-20, default 10)
    --context TEXT            Optional prior conversation context (plain text)
"""

import argparse
import sys
import uuid
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.simulation.providers import build_llm_client
from src.simulation.simulation_policy import SimulationNotAllowedError
from src.simulation.simulation_schemas import SimulationRequest
from src.simulation.simulation_service import SimulationService


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run a single Persona Simulation round.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--legend-member-id",
        required=True,
        metavar="UUID",
        help="UUID of the legend member to simulate",
    )
    parser.add_argument(
        "--message",
        required=True,
        metavar="TEXT",
        help="The message to present to the simulated persona",
    )
    parser.add_argument(
        "--provider",
        default=None,
        metavar="STR",
        help="LLM provider: echo | openai_compatible (overrides SIM_LLM_PROVIDER)",
    )
    parser.add_argument(
        "--model",
        default=None,
        metavar="STR",
        help="Model name, e.g. gpt-4o-mini (overrides SIM_LLM_MODEL)",
    )
    parser.add_argument(
        "--base-url",
        default=None,
        metavar="URL",
        help="API base URL (overrides SIM_LLM_BASE_URL)",
    )
    parser.add_argument(
        "--history-limit",
        type=int,
        default=10,
        metavar="INT",
        help="Max historical messages to sample (1-20, default 10)",
    )
    parser.add_argument(
        "--context",
        default=None,
        metavar="TEXT",
        help="Optional prior conversation context (plain text)",
    )
    args = parser.parse_args()

    try:
        legend_member_id = uuid.UUID(args.legend_member_id)
    except ValueError:
        print(f"Invalid legend-member-id UUID: {args.legend_member_id}", file=sys.stderr)
        sys.exit(1)

    # Build LLM client – fails fast with a clear message if config is missing
    try:
        llm_client = build_llm_client(
            provider=args.provider,
            model=args.model,
            base_url=args.base_url,
        )
    except ValueError as exc:
        print(f"Provider configuration error: {exc}", file=sys.stderr)
        sys.exit(1)

    provider_label = type(llm_client).__name__
    print(f"Provider : {provider_label}")

    request = SimulationRequest(
        legend_member_id=legend_member_id,
        current_message=args.message,
        conversation_context=args.context,
        history_limit=args.history_limit,
    )

    service = SimulationService(llm_client=llm_client)
    try:
        result = service.generate_once(request)
    except SimulationNotAllowedError as exc:
        print(f"Simulation not allowed: {exc}", file=sys.stderr)
        sys.exit(1)
    except ValueError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        sys.exit(1)
    except RuntimeError as exc:
        print(f"LLM error: {exc}", file=sys.stderr)
        sys.exit(1)

    # Output
    print(f"Legend Member ID   : {result.legend_member_id}")
    print(f"Profile Snapshot ID: {result.profile_snapshot_id}")
    print(f"Simulation Version : {result.simulation_version}")
    print()
    print("── Prompt Messages ─────────────────────────────────────────")
    for msg in result.prompt_messages:
        role = msg.get("role", "?").upper()
        content = msg.get("content", "")
        print(f"[{role}]\n{content}\n")
    print("── Response ─────────────────────────────────────────────────")
    print(result.response_text)


if __name__ == "__main__":
    main()
