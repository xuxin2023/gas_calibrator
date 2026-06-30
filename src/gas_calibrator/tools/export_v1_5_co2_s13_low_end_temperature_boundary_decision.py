"""Export V1.5 CO2 S1/S3 low-end temperature-boundary decision artifacts."""

from __future__ import annotations

import argparse
import sys
from typing import Iterable, Optional

from ..validation.co2_s13_low_end_temperature_boundary_decision import (
    write_co2_s13_low_end_temperature_boundary_decision,
)


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ratio-mapping-device-summary-csv", required=True)
    parser.add_argument("--model-capacity-boundary-csv", required=True)
    parser.add_argument("--segment-diagnostic-csv", required=True)
    parser.add_argument("--output-dir", required=True)
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    outputs = write_co2_s13_low_end_temperature_boundary_decision(
        ratio_mapping_device_summary_csv=args.ratio_mapping_device_summary_csv,
        model_capacity_boundary_csv=args.model_capacity_boundary_csv,
        segment_diagnostic_csv=args.segment_diagnostic_csv,
        output_dir=args.output_dir,
    )
    print(f"CO2 S1/S3 low-end temperature-boundary decision saved: {outputs['markdown']}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
