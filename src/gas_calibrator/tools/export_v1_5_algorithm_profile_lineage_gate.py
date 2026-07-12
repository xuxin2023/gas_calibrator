"""Export the offline V1.5 algorithm-profile lineage gate."""

from __future__ import annotations

import argparse
import json
from typing import Iterable

from ..validation.v1_5_algorithm_profile_lineage_gate import (
    build_v1_5_algorithm_profile_lineage_gate,
    write_v1_5_algorithm_profile_lineage_gate,
)


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument("--bootstrap-json", required=True)
    parser.add_argument("--queue-inputs-json", required=True)
    parser.add_argument("--co2-queue-summary-json", required=True)
    parser.add_argument("--h2o-queue-summary-json", required=True)
    parser.add_argument("--co2-queue-manifest-csv", required=True)
    parser.add_argument("--h2o-queue-manifest-csv", required=True)
    parser.add_argument("--co2-r0-model-json", default="")
    parser.add_argument("--h2o-r0-model-json", default="")
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--fail-on-blocker", action="store_true")
    args = parser.parse_args(list(argv) if argv is not None else None)
    model = build_v1_5_algorithm_profile_lineage_gate(
        bootstrap_json=args.bootstrap_json,
        queue_inputs_json=args.queue_inputs_json,
        co2_queue_summary_json=args.co2_queue_summary_json,
        h2o_queue_summary_json=args.h2o_queue_summary_json,
        co2_queue_manifest_csv=args.co2_queue_manifest_csv,
        h2o_queue_manifest_csv=args.h2o_queue_manifest_csv,
        co2_r0_model_json=args.co2_r0_model_json or None,
        h2o_r0_model_json=args.h2o_r0_model_json or None,
    )
    outputs = write_v1_5_algorithm_profile_lineage_gate(model, args.output_dir)
    print(json.dumps({"overall_status": model["overall_status"], "outputs": {key: str(value) for key, value in outputs.items()}}, ensure_ascii=False, indent=2))
    return 2 if args.fail_on_blocker and model["blocker_count"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
