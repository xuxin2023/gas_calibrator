"""Export V1.5 offline CO2/H2O cross-effect review artifacts."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Iterable, Optional

from ..validation.co2_h2o_cross_effect_review import write_co2_h2o_cross_effect_review


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Export no-COM V1.5 CO2/H2O water-vapor cross-effect review."
    )
    parser.add_argument(
        "--input-csv",
        action="append",
        required=True,
        help="Input calibration CSV artifact. May be repeated.",
    )
    parser.add_argument(
        "--source-label",
        action="append",
        default=[],
        help="Optional label for each input CSV. May be repeated in the same order.",
    )
    parser.add_argument("--output-dir", required=True)
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    try:
        outputs = write_co2_h2o_cross_effect_review(
            csv_paths=[Path(item) for item in args.input_csv],
            source_labels=[str(item) for item in args.source_label],
            output_dir=Path(args.output_dir),
        )
    except Exception as exc:
        print(f"V1.5 CO2/H2O cross-effect review failed: {exc}", file=sys.stderr, flush=True)
        return 2
    print(json.dumps({key: str(value) for key, value in outputs.items()}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
