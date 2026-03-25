#!/usr/bin/env python3
"""
Launch the NFA Forgotten Archive read-only API server.

Usage
-----
    python scripts/run_api.py [--host HOST] [--port PORT] [--reload]

Options
-------
    --host    Bind address (default: 0.0.0.0)
    --port    Port number  (default: 8000)
    --reload  Enable auto-reload on code changes (development only)

Examples
--------
    # Development (auto-reload)
    python scripts/run_api.py --reload

    # Production-style
    python scripts/run_api.py --host 127.0.0.1 --port 8080

    # Or directly via uvicorn:
    uvicorn src.api.app:app --host 0.0.0.0 --port 8000

Interactive docs
----------------
    http://localhost:8000/docs      (Swagger UI)
    http://localhost:8000/redoc     (ReDoc)
"""

import argparse
import sys
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run the NFA Forgotten Archive API server.",
    )
    parser.add_argument("--host", default="0.0.0.0", help="Bind address (default: 0.0.0.0)")
    parser.add_argument("--port", type=int, default=8000, help="Port (default: 8000)")
    parser.add_argument("--reload", action="store_true", help="Enable auto-reload (dev mode)")
    args = parser.parse_args()

    import uvicorn
    print(f"Starting NFA Forgotten Archive API on http://{args.host}:{args.port}")
    print(f"  Swagger UI : http://localhost:{args.port}/docs")
    print(f"  ReDoc      : http://localhost:{args.port}/redoc")
    print(f"  Health     : http://localhost:{args.port}/api/v1/health")
    print()
    uvicorn.run(
        "src.api.app:app",
        host=args.host,
        port=args.port,
        reload=args.reload,
    )


if __name__ == "__main__":
    main()
