"""Export a no-write V1.5 historical component-QC generator preflight."""

from __future__ import annotations

import argparse
import json
from typing import Iterable

from ..validation.v1_5_historical_component_qc_generator_preflight import (
    build_v1_5_historical_component_qc_generator_preflight,
    write_v1_5_historical_component_qc_generator_preflight,
)


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument("--p2-design-json-path", required=True)
    parser.add_argument("--p2-artifact-inventory-csv-path", required=True)
    parser.add_argument("--contract-json-path", required=True)
    parser.add_argument("--reference-evaluation-json-path", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--fail-on-blocker", action="store_true")
    args = parser.parse_args(list(argv) if argv is not None else None)
    try:
        model = build_v1_5_historical_component_qc_generator_preflight(
            p2_design_json_path=args.p2_design_json_path,
            p2_artifact_inventory_csv_path=args.p2_artifact_inventory_csv_path,
            contract_json_path=args.contract_json_path,
            reference_evaluation_json_path=args.reference_evaluation_json_path,
        )
        outputs = write_v1_5_historical_component_qc_generator_preflight(
            model, args.output_dir
        )
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        print(json.dumps({"overall_status": "blocked", "error": str(exc)}, ensure_ascii=False))
        return 2
    print(
        json.dumps(
            {
                "overall_status": model["overall_status"],
                "candidate_preflight_ready_count": model["candidate_preflight_ready_count"],
                "candidate_blocked_count": model["candidate_blocked_count"],
                "outputs": {key: str(value) for key, value in outputs.items()},
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 2 if args.fail_on_blocker and (
        model["global_blocker_codes"] or model["candidate_blocked_count"]
    ) else 0


if __name__ == "__main__":
    raise SystemExit(main())
