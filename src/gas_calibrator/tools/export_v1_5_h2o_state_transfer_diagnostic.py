"""CLI wrapper for the offline V1.5 H2O state-transfer diagnostic."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Iterable, Optional

from ..validation.h2o_state_transfer_diagnostic import (
    H2OStateTransferDiagnosticInputs,
    write_h2o_state_transfer_diagnostic_report,
)


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export a no-COM H2O state-transfer diagnostic report.")
    parser.add_argument("--candidate-device-policy-csv", required=True)
    parser.add_argument("--state-transfer-csv", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--target-device-id", action="append", default=[])
    parser.add_argument("--raw-excess-limit-mmol", type=float, default=0.1)
    parser.add_argument("--post-s6-relative-limit-pct", type=float, default=2.0)
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    inputs = H2OStateTransferDiagnosticInputs(
        candidate_device_policy_csv=Path(args.candidate_device_policy_csv),
        state_transfer_csv=Path(args.state_transfer_csv),
        target_device_ids=tuple(args.target_device_id) if args.target_device_id else ("084",),
        raw_excess_limit_mmol=float(args.raw_excess_limit_mmol),
        post_s6_relative_limit_pct=float(args.post_s6_relative_limit_pct),
    )
    outputs = write_h2o_state_transfer_diagnostic_report(inputs=inputs, output_dir=args.output_dir)
    print(f"H2O state-transfer diagnostic saved: {outputs['markdown']}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
