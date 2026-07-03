"""Export read-only QC gap audit for V1.5 historical replay evidence."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Iterable, Optional

from ..validation.v1_5_historical_replay_qc_gap_audit import (
    build_v1_5_historical_replay_qc_gap_audit,
    write_v1_5_historical_replay_qc_gap_audit,
)


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Audit missing-QC points from a V1.5 historical replay evidence JSON and identify "
            "same-run reject-only evidence, retry candidates, or cross-run references. Offline/no-write only."
        )
    )
    parser.add_argument("--replay-evidence-path", required=True, help="Path to v1_5_historical_replay_evidence.json.")
    parser.add_argument("--output-dir", required=True, help="Output directory for QC gap audit artifacts.")
    parser.add_argument(
        "--search-root",
        action="append",
        default=[],
        help="Historical evidence root to search for same-point/cross-run quality candidates. Repeat as needed.",
    )
    parser.add_argument("--fail-on-blocker", action="store_true", help="Return exit code 2 when blockers exist.")
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    try:
        outputs = write_v1_5_historical_replay_qc_gap_audit(
            replay_evidence_path=args.replay_evidence_path,
            search_roots=args.search_root,
            output_dir=args.output_dir,
        )
        model = build_v1_5_historical_replay_qc_gap_audit(
            replay_evidence_path=args.replay_evidence_path,
            search_roots=args.search_root,
        )
    except Exception as exc:
        print(f"V1.5 historical replay QC gap audit export failed: {exc}", file=sys.stderr, flush=True)
        return 1

    payload = {
        "status": model["manifest"]["status"],
        "blocker_count": model["manifest"]["blocker_count"],
        "review_required_count": model["manifest"]["review_required_count"],
        "missing_qc_point_count": model["manifest"]["missing_qc_point_count"],
        "direct_bindable_point_count": model["manifest"]["direct_bindable_point_count"],
        "unresolved_point_count": model["manifest"]["unresolved_point_count"],
        "outputs": {key: str(Path(value).resolve()) for key, value in outputs.items()},
        "physical_boundaries": {
            key: model["manifest"][key]
            for key in (
                "opens_com_ports",
                "connects_postgresql",
                "controls_water_or_gas_routes",
                "writes_coefficients",
                "formal_release_allowed",
                "database_import_allowed",
                "not_real_acceptance_evidence",
            )
        },
    }
    print(json.dumps(payload, ensure_ascii=False, indent=2), flush=True)
    if args.fail_on_blocker and model["manifest"]["blocker_count"]:
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
