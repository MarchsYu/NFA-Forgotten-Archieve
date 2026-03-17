#!/usr/bin/env python3
"""
Import a chat log file into the NFA Forgotten Archive database.

Usage:
    python scripts/import_chat.py <file_path> [options]

Options:
    --platform PLATFORM     Platform name (e.g. wechat, telegram, generic)
    --group-name NAME       Group name override (required for .txt files)

Examples:
    python scripts/import_chat.py data/raw/sample_chat.json
    python scripts/import_chat.py data/raw/sample_chat.txt --group-name "Family Chat" --platform wechat
    python scripts/import_chat.py data/raw/sample_chat.csv --platform telegram
"""

import argparse
import sys
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.ingest.services.ingest_service import IngestService


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Import a chat log file into NFA Forgotten Archive."
    )
    parser.add_argument("file_path", type=Path, help="Path to the chat log file")
    parser.add_argument(
        "--platform",
        default=None,
        help="Platform name override (e.g. wechat, telegram, generic)",
    )
    parser.add_argument(
        "--group-name",
        default=None,
        dest="group_name",
        help="Group name override (required for .txt files)",
    )
    args = parser.parse_args()

    file_path: Path = args.file_path
    if not file_path.exists():
        print(f"❌ File not found: {file_path}", file=sys.stderr)
        sys.exit(1)

    print(f"📂 Importing: {file_path}")
    print(f"   Platform : {args.platform or '(from file)'}")
    print(f"   Group    : {args.group_name or '(from file)'}")
    print()

    service = IngestService()
    try:
        result = service.ingest_file(
            file_path,
            platform_hint=args.platform,
            group_name_hint=args.group_name,
        )
    except ValueError as exc:
        print(f"❌ Import failed: {exc}", file=sys.stderr)
        sys.exit(1)
    except Exception as exc:
        print(f"❌ Unexpected error: {exc}", file=sys.stderr)
        raise

    print("✅ Import complete")
    print(f"   Group ID                  : {result.group_id}")
    print(f"   Messages inserted         : {result.messages_inserted}")
    print(f"   Messages skipped (dup)    : {result.messages_skipped_duplicate}")
    print(f"   Messages without ext ID   : {result.messages_without_external_id}")
    print(f"   Members created           : {result.members_created}")
    print(f"   Members reused            : {result.members_reused}")


if __name__ == "__main__":
    main()
