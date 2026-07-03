"""Export V1.5 CO2 S1/S3 blocker closure action review artifacts."""

from __future__ import annotations

import argparse
from pathlib import Path

from gas_calibrator.validation.co2_s13_blocker_closure_action_review import (
    write_co2_s13_blocker_closure_action_review,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root-cause-closure-dir", required=True)
    parser.add_argument("--output-dir", required=True)
    args = parser.parse_args(argv)
    paths = write_co2_s13_blocker_closure_action_review(
        root_cause_closure_dir=Path(args.root_cause_closure_dir),
        output_dir=Path(args.output_dir),
    )
    print(f"CO2 S1/S3 blocker closure action review saved: {paths['markdown']}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
