"""Export offline V1.5 historical fitting profile parity evidence."""

from __future__ import annotations

import argparse
import json
from typing import Iterable

from ..validation.v1_5_historical_fit_profile_parity import (
    build_v1_5_historical_fit_profile_parity,
    write_v1_5_historical_fit_profile_parity,
)


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument("--algorithm-profile-lineage-json", required=True)
    parser.add_argument("--fit-points-csv", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--fail-on-blocker", action="store_true")
    args = parser.parse_args(list(argv) if argv is not None else None)
    model = build_v1_5_historical_fit_profile_parity(
        algorithm_profile_lineage_json=args.algorithm_profile_lineage_json,
        fit_points_csv=args.fit_points_csv,
    )
    outputs = write_v1_5_historical_fit_profile_parity(model, args.output_dir)
    print(
        json.dumps(
            {
                "overall_status": model["overall_status"],
                "outputs": {key: str(value) for key, value in outputs.items()},
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 2 if args.fail_on_blocker and model["blocker_count"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
