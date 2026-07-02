"""Export a no-COM V1.5 H2O special diagnostic queue."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Iterable, Optional

from ..validation.h2o_special_diagnostic_queue import (
    H2OSpecialDiagnosticQueueInputs,
    write_h2o_special_diagnostic_queue,
)


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export a V1.5 H2O no-write special diagnostic queue.")
    parser.add_argument("--device-decision-csv", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--target-device-id", default="084")
    parser.add_argument("--temp-c", type=float, default=20.0)
    parser.add_argument("--hgen-temp-c", type=float, default=20.0)
    parser.add_argument("--low-rh-pct", type=float, default=30.0)
    parser.add_argument("--high-rh-pct", type=float, default=70.0)
    parser.add_argument("--purge-s", type=float, default=360.0)
    parser.add_argument("--sample-count", type=int, default=30)
    parser.add_argument("--sample-interval-s", type=float, default=1.0)
    parser.add_argument("--analyzer-acquisition", default="active_stream_1hz")
    parser.add_argument("--reference-pressure-hpa", type=float, default=1013.25)
    parser.add_argument("--config-placeholder", default="<V1_5_RUNTIME_CONFIG_JSON>")
    parser.add_argument("--run-output-placeholder", default="<H2O_084_SPECIAL_DIAGNOSTIC_OUTPUT_DIR>")
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    inputs = H2OSpecialDiagnosticQueueInputs(
        device_decision_csv=Path(args.device_decision_csv),
        target_device_id=str(args.target_device_id),
        temp_c=float(args.temp_c),
        hgen_temp_c=float(args.hgen_temp_c),
        low_rh_pct=float(args.low_rh_pct),
        high_rh_pct=float(args.high_rh_pct),
        purge_s=float(args.purge_s),
        sample_count=int(args.sample_count),
        sample_interval_s=float(args.sample_interval_s),
        analyzer_acquisition=str(args.analyzer_acquisition),
        reference_pressure_hpa=float(args.reference_pressure_hpa),
        config_placeholder=str(args.config_placeholder),
        run_output_placeholder=str(args.run_output_placeholder),
    )
    outputs = write_h2o_special_diagnostic_queue(inputs=inputs, output_dir=args.output_dir)
    print(f"H2O special diagnostic queue saved: {outputs['runbook']}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
