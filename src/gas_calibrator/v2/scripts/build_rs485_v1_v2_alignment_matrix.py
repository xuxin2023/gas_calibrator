from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
from typing import Optional


def _bootstrap_src_path_for_direct_script() -> None:
    src_root = Path(__file__).resolve().parents[3]
    src_root_text = str(src_root)
    if src_root_text not in sys.path:
        sys.path.insert(0, src_root_text)


_bootstrap_src_path_for_direct_script()

from gas_calibrator.v2.core.run001_rs485_alignment import (  # noqa: E402
    write_rs485_v1_v2_alignment_matrix,
)


def create_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Build A2.15 offline RS485 V1/historical/V2 command alignment matrix."
    )
    parser.add_argument("--current-a2-14-dir", required=True, help="A2.14 RS485 diagnostic output directory")
    parser.add_argument("--v1-config", required=True, help="V1 production or historical runtime config JSON")
    parser.add_argument("--output", required=True, help="Output rs485_v1_v2_alignment_matrix.json path")
    parser.add_argument("--historical-pace-identity", default="", help="Historical PACE identity probe JSON")
    parser.add_argument("--historical-pace-readback", default="", help="Historical PACE readback probe JSON")
    parser.add_argument("--historical-pressure-gauge-io", default="", help="Historical P3 success io_log CSV")
    return parser


def main(argv: Optional[list[str]] = None) -> int:
    args = create_argument_parser().parse_args(argv)
    payload = write_rs485_v1_v2_alignment_matrix(
        output_path=args.output,
        current_a2_14_dir=args.current_a2_14_dir,
        v1_config_path=args.v1_config,
        historical_pace_identity_path=args.historical_pace_identity or None,
        historical_pace_readback_path=args.historical_pace_readback or None,
        historical_pressure_gauge_io_path=args.historical_pressure_gauge_io or None,
    )
    print(json.dumps(payload, ensure_ascii=False, indent=2), flush=True)
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main(sys.argv[1:]))
