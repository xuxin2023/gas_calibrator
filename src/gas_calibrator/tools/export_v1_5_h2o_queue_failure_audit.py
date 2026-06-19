"""Export a V1.5 H2O queue failure audit from a completed queue manifest."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Iterable, Optional

from ..validation.v1_5_h2o_queue_failure_audit import audit_and_write


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Export V1.5 H2O queue failure audit artifacts without opening hardware."
    )
    parser.add_argument("--manifest", required=True, help="Path to queue_manifest.csv.")
    parser.add_argument("--output-dir", required=True, help="Directory for audit artifacts.")
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    audit = audit_and_write(Path(args.manifest), Path(args.output_dir))
    print(
        json.dumps(
            {
                "status": "ok",
                "manifest": str(Path(args.manifest).resolve()),
                "output_dir": str(Path(args.output_dir).resolve()),
                "total_points": audit.get("total_points"),
                "status_counts": audit.get("status_counts"),
                "failure_category_counts": audit.get("failure_category_counts"),
                "outputs": audit.get("outputs"),
            },
            ensure_ascii=False,
            indent=2,
            default=str,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
