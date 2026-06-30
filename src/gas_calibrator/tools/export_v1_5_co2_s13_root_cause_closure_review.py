"""Export V1.5 CO2 S1/S3 root-cause closure review artifacts."""

from __future__ import annotations

import argparse
from pathlib import Path

from gas_calibrator.validation.co2_s13_root_cause_closure_review import (
    write_co2_s13_root_cause_closure_review,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-state-dir", required=True)
    parser.add_argument("--ratio-mapping-dir", required=True)
    parser.add_argument("--target-state-bridge-dir", required=True)
    parser.add_argument("--bridge-correction-dir", required=True)
    parser.add_argument("--repair-fit-dir", required=True)
    parser.add_argument("--error-root-cause-dir", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--acceptance-percent", type=float, default=1.0)
    args = parser.parse_args(argv)
    paths = write_co2_s13_root_cause_closure_review(
        source_state_dir=Path(args.source_state_dir),
        ratio_mapping_dir=Path(args.ratio_mapping_dir),
        target_state_bridge_dir=Path(args.target_state_bridge_dir),
        bridge_correction_dir=Path(args.bridge_correction_dir),
        repair_fit_dir=Path(args.repair_fit_dir),
        error_root_cause_dir=Path(args.error_root_cause_dir),
        output_dir=Path(args.output_dir),
        acceptance_percent=float(args.acceptance_percent),
    )
    print(f"CO2 S1/S3 root-cause closure review saved: {paths['markdown']}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
