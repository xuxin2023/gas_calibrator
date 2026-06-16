"""Export the V1.5 formal calibration read-only operation console."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Iterable, Optional

from ..v1_5.ui.operation_console import write_operation_console


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Write a read-only V1.5 operation console without touching devices."
    )
    parser.add_argument("--output-dir", required=True, help="Operation-console output directory.")
    parser.add_argument("--workbench-json", default=None, help="Optional v1_5_formal_workbench.json input.")
    parser.add_argument(
        "--run-evidence-status-json",
        default=None,
        help="Optional v1_5_run_evidence_status.json input.",
    )
    parser.add_argument(
        "--calibration-capability-json",
        default=None,
        help="Optional v1_5_calibration_capability.json input.",
    )
    parser.add_argument(
        "--archive-index-json",
        default=None,
        help="Optional v1_5_formal_archive_closure_index.json input.",
    )
    parser.add_argument("--role", choices=("operator", "engineer", "reviewer", "admin"), default="operator")
    return parser.parse_args(list(argv) if argv is not None else None)


def _load_json(path: str | None, *, label: str) -> dict:
    if not path:
        return {}
    source = Path(path)
    if not source.exists():
        raise FileNotFoundError(f"{label} JSON not found: {source}")
    payload = json.loads(source.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"{label} JSON must contain a JSON object")
    return payload


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    try:
        outputs = write_operation_console(
            output_dir=args.output_dir,
            workbench_model=_load_json(args.workbench_json, label="workbench"),
            run_evidence_status=_load_json(args.run_evidence_status_json, label="run-evidence-status"),
            calibration_capability=_load_json(args.calibration_capability_json, label="calibration-capability"),
            archive_index=_load_json(args.archive_index_json, label="archive-index"),
            role=args.role,
        )
    except Exception as exc:
        print(f"V1.5 operation console export failed: {exc}", file=sys.stderr, flush=True)
        return 1
    print(json.dumps({key: str(Path(value).resolve()) for key, value in outputs.items()}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
