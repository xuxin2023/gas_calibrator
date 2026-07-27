"""Export the offline V1.5 new-algorithm 47/14 mature-queue handoff contract."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Iterable

from ..validation.v1_5_new_algorithm_mature_queue_live_handoff import (
    build_v1_5_new_algorithm_mature_queue_live_handoff,
    write_v1_5_new_algorithm_mature_queue_live_handoff,
)


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument("--repo-root", required=True)
    parser.add_argument("--profile-path", required=True)
    parser.add_argument("--mature-route-contract-json", required=True)
    parser.add_argument("--reference-source-catalog-json", required=True)
    parser.add_argument("--output-dir", required=True)
    args = parser.parse_args(list(argv) if argv is not None else None)
    model = build_v1_5_new_algorithm_mature_queue_live_handoff(
        repo_root=args.repo_root,
        profile_path=args.profile_path,
        mature_route_contract_json=args.mature_route_contract_json,
        reference_source_catalog_json=args.reference_source_catalog_json,
    )
    outputs = write_v1_5_new_algorithm_mature_queue_live_handoff(
        model,
        args.output_dir,
        repo_root=args.repo_root,
    )
    print(
        json.dumps(
            {
                "overall_status": model.get("overall_status"),
                "offline_handoff_contract_ready": model.get(
                    "offline_handoff_contract_ready"
                ),
                "production_live_gap_closed": model.get(
                    "production_live_gap_closed"
                ),
                "live_queue_execution_allowed": model.get(
                    "live_queue_execution_allowed"
                ),
                "outputs": {
                    key: str(Path(value).resolve()) for key, value in outputs.items()
                },
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0 if model.get("offline_handoff_contract_ready") else 2


if __name__ == "__main__":
    raise SystemExit(main())
