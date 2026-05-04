"""CLI entry point for storage operations.

Usage:
  python -m gas_calibrator.v2.storage.cli --index <output_dir> [--db gas_calibrator_index.db]
"""

from __future__ import annotations

import argparse
import sys

from .indexer import index_run


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Gas Calibrator v2 storage CLI")
    subparsers = parser.add_subparsers(dest="command", required=True)

    index_parser = subparsers.add_parser("index", help="Index a completed run output directory")
    index_parser.add_argument("output_dir", help="Path to the run output directory")
    index_parser.add_argument(
        "--db", default="gas_calibrator_index.db", help="SQLite database path (default: gas_calibrator_index.db)"
    )

    args = parser.parse_args(argv)
    if args.command == "index":
        result = index_run(args.output_dir, db_path=args.db)
        if result.get("ok"):
            print(f"Indexed: {result}")
        else:
            print(f"ERROR: {result.get('error', 'unknown')}", file=sys.stderr)
            return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
