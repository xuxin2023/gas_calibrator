"""Verify an existing V1.5 evidence bundle against on-disk artifacts."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Iterable, Optional

from ..storage.v1_5_evidence.bundle import verify_evidence_bundle_integrity


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Verify V1.5 evidence bundle artifact hashes.")
    parser.add_argument("--evidence-bundle-json", required=True, help="Path to evidence_bundle.json.")
    parser.add_argument("--output-json", default="", help="Optional path for the integrity result JSON.")
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    bundle_path = Path(args.evidence_bundle_json)
    try:
        bundle = json.loads(bundle_path.read_text(encoding="utf-8"))
        result = verify_evidence_bundle_integrity(bundle)
        rendered = json.dumps(result, ensure_ascii=False, indent=2, default=str)
        if args.output_json:
            output_path = Path(args.output_json)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_text(rendered, encoding="utf-8")
        print(rendered)
        return 0 if result.get("status") == "pass" else 1
    except Exception as exc:
        print(f"V1.5 evidence bundle verification failed: {exc}", file=sys.stderr, flush=True)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
