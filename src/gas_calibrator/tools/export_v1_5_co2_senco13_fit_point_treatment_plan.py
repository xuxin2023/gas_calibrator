"""CLI wrapper for the offline V1.5 CO2 SENCO1/SENCO3 fit-point plan."""

from __future__ import annotations

import argparse
import sys
from typing import Iterable, Optional

from ..validation.co2_senco13_fit_point_treatment_plan import (
    TreatmentPlanInputs,
    write_co2_senco13_fit_point_treatment_plan,
)


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export offline V1.5 CO2 S1/S3 fit-point treatment plan.")
    parser.add_argument("--common-mode-audit-csv", required=True)
    parser.add_argument("--output-dir", required=True)
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    outputs = write_co2_senco13_fit_point_treatment_plan(
        inputs=TreatmentPlanInputs(common_mode_audit_csv=args.common_mode_audit_csv),
        output_dir=args.output_dir,
    )
    print(f"CO2 S1/S3 fit-point treatment plan saved: {outputs['markdown']}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
