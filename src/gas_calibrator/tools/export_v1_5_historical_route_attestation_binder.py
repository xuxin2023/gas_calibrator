"""Export an offline, fail-closed V1.5 historical route attestation."""

from __future__ import annotations

import argparse
import json
from typing import Iterable

from ..validation.v1_5_historical_route_attestation_binder import (
    build_v1_5_historical_route_attestation_binder,
    write_v1_5_historical_route_attestation_binder,
)


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument("--historical-replay-evidence-json", required=True)
    parser.add_argument("--algorithm-profile-path", required=True)
    parser.add_argument("--mature-route-contract-json", required=True)
    parser.add_argument("--automation-control-contract-json", required=True)
    parser.add_argument("--reviewer", required=True)
    parser.add_argument("--reviewed-at", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--fail-on-blocker", action="store_true")
    args = parser.parse_args(list(argv) if argv is not None else None)
    model = build_v1_5_historical_route_attestation_binder(
        historical_replay_evidence_json=args.historical_replay_evidence_json,
        algorithm_profile_path=args.algorithm_profile_path,
        mature_route_contract_json=args.mature_route_contract_json,
        automation_control_contract_json=args.automation_control_contract_json,
        reviewer=args.reviewer,
        reviewed_at=args.reviewed_at,
    )
    outputs = write_v1_5_historical_route_attestation_binder(model, args.output_dir)
    print(
        json.dumps(
            {
                "overall_status": model["overall_status"],
                "reviewed_family_count": model["reviewed_family_count"],
                "blocker_count": model["blocker_count"],
                "outputs": {key: str(value) for key, value in outputs.items()},
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 2 if args.fail_on_blocker and model["blocker_count"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
