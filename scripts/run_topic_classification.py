#!/usr/bin/env python3
"""
Run topic classification on stored messages.

Usage:
    python scripts/run_topic_classification.py [options]

Options:
    --classifier-version VERSION   Classifier version tag (default: rule_v1)
    --rerun                        Delete existing results for this version and re-classify all
    --group-id UUID                Restrict classification to a single group

Examples:
    # Classify all unclassified messages with rule_v1
    python scripts/run_topic_classification.py

    # Re-run rule_v1 from scratch (replaces existing results)
    python scripts/run_topic_classification.py --rerun

    # Classify only a specific group
    python scripts/run_topic_classification.py --group-id <uuid>

    # Use a different classifier version
    python scripts/run_topic_classification.py --classifier-version rule_v2
"""

import argparse
import sys
import uuid
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.classification.classification_service import ClassificationService
from src.classification.topic_classifier import CLASSIFIER_VERSION


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run topic classification on NFA Forgotten Archive messages."
    )
    parser.add_argument(
        "--classifier-version",
        default=CLASSIFIER_VERSION,
        dest="classifier_version",
        help=f"Classifier version tag (default: {CLASSIFIER_VERSION})",
    )
    parser.add_argument(
        "--rerun",
        action="store_true",
        help="Delete existing results for this version and re-classify all messages",
    )
    parser.add_argument(
        "--group-id",
        default=None,
        dest="group_id",
        help="Restrict classification to a single group UUID",
    )
    args = parser.parse_args()

    group_id = None
    if args.group_id:
        try:
            group_id = uuid.UUID(args.group_id)
        except ValueError:
            print(f"❌ Invalid group-id UUID: {args.group_id}", file=sys.stderr)
            sys.exit(1)

    mode = "rerun (replace existing)" if args.rerun else "incremental (skip already classified)"
    print(f"🏷  Classifier version : {args.classifier_version}")
    print(f"   Mode              : {mode}")
    if group_id:
        print(f"   Group filter      : {group_id}")
    print()

    service = ClassificationService()
    try:
        result = service.run(
            classifier_version=args.classifier_version,
            rerun=args.rerun,
            group_id=group_id,
        )
    except RuntimeError as exc:
        print(f"❌ {exc}", file=sys.stderr)
        sys.exit(1)
    except Exception as exc:
        print(f"❌ Unexpected error: {exc}", file=sys.stderr)
        raise

    print("✅ Classification complete")
    print(f"   Messages processed        : {result.messages_processed}")
    print(f"   Messages skipped          : {result.messages_skipped}")
    print(f"   Messages unmatched        : {result.messages_unmatched}")
    print(f"   Topic assignments written : {result.topic_assignments_written}")
    if result.messages_processed > 0:
        match_rate = (result.messages_processed - result.messages_unmatched) / result.messages_processed
        print(f"   Match rate                : {match_rate:.1%}")


if __name__ == "__main__":
    main()
