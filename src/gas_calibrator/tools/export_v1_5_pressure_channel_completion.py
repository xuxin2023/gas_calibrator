"""Export V1.5 pressure-channel completion package from existing evidence."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Dict, Iterable, Optional

from ..validation.pressure_channel_completion import write_pressure_channel_completion_report


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Consolidate V1.5 SENCO9 write, post-write pressure verification, and COM22 traceability evidence."
    )
    parser.add_argument("--senco9-write-summary", required=True)
    parser.add_argument("--post-write-fit-summary", required=True)
    parser.add_argument("--pressure-reference-json", required=True)
    parser.add_argument("--pressure-reference-traceability", default=None)
    parser.add_argument("--old-getco-json", default=None)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument(
        "--device-id",
        action="append",
        default=[],
        help="Limit the completion package to these analyzer device IDs; other observed devices are listed as excluded.",
    )
    parser.add_argument(
        "--known-limitation",
        action="append",
        default=[],
        help=(
            "Known limitation row as id|reason|impact. Use for pressure points or devices intentionally kept diagnostic, "
            "for example 500hpa_low_pressure_micro_leak|excluded|500 hPa not used for formal fit."
        ),
    )
    parser.add_argument("--max-abs-offset-kpa", type=float, default=0.05)
    parser.add_argument("--max-residual-hpa", type=float, default=0.5)
    parser.add_argument(
        "--acceptance-policy-note",
        default="",
        help=(
            "Human-readable rationale for the selected pressure-channel completion thresholds. "
            "Stored in CSV/XLSX/Markdown outputs for audit."
        ),
    )
    parser.add_argument("--today", default=None)
    return parser.parse_args(list(argv) if argv is not None else None)


def _parse_known_limitation(value: str) -> Dict[str, object]:
    parts = [part.strip() for part in str(value or "").split("|")]
    return {
        "limitation_id": parts[0] if len(parts) > 0 else "",
        "reason": parts[1] if len(parts) > 1 else "",
        "impact": parts[2] if len(parts) > 2 else "",
        "status": parts[3] if len(parts) > 3 else "engineering_diagnostic",
        "blocks_selected_device_completion": False,
    }


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    try:
        outputs = write_pressure_channel_completion_report(
            output_dir=args.output_dir,
            senco9_write_summary_path=args.senco9_write_summary,
            post_write_fit_summary_path=args.post_write_fit_summary,
            pressure_reference_path=args.pressure_reference_json,
            pressure_reference_traceability_path=args.pressure_reference_traceability,
            old_getco_snapshot_path=args.old_getco_json,
            selected_device_ids=args.device_id,
            known_limitations=[_parse_known_limitation(item) for item in args.known_limitation],
            max_abs_offset_kpa=float(args.max_abs_offset_kpa),
            max_residual_hpa=float(args.max_residual_hpa),
            acceptance_policy_note=args.acceptance_policy_note,
            today=args.today,
        )
    except Exception as exc:
        print(f"V1.5 pressure-channel completion export failed: {exc}", file=sys.stderr, flush=True)
        return 1
    print(json.dumps({key: str(Path(value).resolve()) for key, value in outputs.items()}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
