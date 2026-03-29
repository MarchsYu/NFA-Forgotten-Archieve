#!/usr/bin/env python3
"""
Seed the topics table with the base taxonomy for NFA Forgotten Archive.

Usage:
    python scripts/init_topics.py

Behaviour:
- Inserts each topic from topic_rules.TOPICS if it does not already exist.
- Uses topic_key as the unique identifier (upsert-safe: skips existing rows).
- Safe to re-run; existing topics are not modified.

Implementation note:
    The actual logic lives in src/classification/topic_service.init_topics().
    This script is a thin CLI wrapper around that canonical function.
"""

import sys
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.classification.topic_service import init_topics as _init_topics


def main() -> None:
    try:
        result = _init_topics()
        print(f"✅ Topics initialized: {result.inserted} inserted, {result.skipped} already existed.")
        for key in result.topic_keys:
            print(f"   [{key}]")
    except Exception as exc:
        print(f"❌ Error: {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
