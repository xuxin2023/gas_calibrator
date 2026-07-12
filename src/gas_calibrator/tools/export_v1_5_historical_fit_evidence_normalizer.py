"""Export normalized historical V1.5 fit-point evidence without hardware access."""

from __future__ import annotations

import argparse
import json
from typing import Iterable

from ..validation.v1_5_historical_fit_evidence_normalizer import (
    build_v1_5_historical_fit_evidence_normalizer,
    write_v1_5_historical_fit_evidence_normalizer,
)


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument("--algorithm-profile-lineage-json", required=True)
    parser.add_argument("--historical-replay-evidence-json", required=True)
    parser.add_argument("--route-baseline-attestation-json", default="")
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--fail-on-structural-blocker", action="store_true")
    args = parser.parse_args(list(argv) if argv is not None else None)
    model = build_v1_5_historical_fit_evidence_normalizer(
        algorithm_profile_lineage_json=args.algorithm_profile_lineage_json,
        historical_replay_evidence_json=args.historical_replay_evidence_json,
        route_baseline_attestation_json=args.route_baseline_attestation_json or None,
    )
    outputs = write_v1_5_historical_fit_evidence_normalizer(model, args.output_dir)
    print(
        json.dumps(
            {
                "overall_status": model["overall_status"],
                "structural_blocker_count": model["structural_blocker_count"],
                "fit_review_gap_count": model["fit_review_gap_count"],
                "outputs": {key: str(value) for key, value in outputs.items()},
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 2 if args.fail_on_structural_blocker and model["structural_blocker_count"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
