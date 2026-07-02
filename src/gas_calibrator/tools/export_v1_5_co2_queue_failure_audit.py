"""Export an offline V1.5 CO2 queue failure audit.

This tool never opens COM ports. It reads an existing queue_manifest.csv and
the referenced point logs, then writes a CSV/JSON/Markdown explanation package.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Iterable, Optional

from ..validation.v1_5_co2_queue_failure_audit import audit_and_write


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export V1.5 CO2 queue failure audit from existing logs.")
    parser.add_argument("--manifest", required=True, help="Path to queue_manifest.csv.")
    parser.add_argument("--output-dir", required=True, help="Directory for audit outputs.")
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    audit = audit_and_write(Path(args.manifest), Path(args.output_dir))
    print(json.dumps(audit.get("outputs", {}), ensure_ascii=False, indent=2), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
