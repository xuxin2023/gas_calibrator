"""Export V1.5 canonical open-flow ambient point plans."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Iterable, Optional

from ..validation.open_flow_canonical_points import (
    DEFAULT_CO2_FIT_PPM,
    DEFAULT_CO2_VERIFICATION_PPM,
    write_open_flow_canonical_point_plan,
)


def _parse_int_list(raw: str | None, default: tuple[int, ...]) -> list[int]:
    if raw is None or not str(raw).strip():
        return list(default)
    out: list[int] = []
    for token in str(raw).replace(";", ",").split(","):
        token = token.strip()
        if not token:
            continue
        out.append(int(round(float(token))))
    return sorted(dict.fromkeys(out))


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Convert the legacy V1/V1.5 point table into formal V1.5 open-flow "
            "current-atmosphere CO2/H2O point queues. Offline/no-write only."
        )
    )
    parser.add_argument("--source-points-xlsx", required=True, help="Legacy/normalized V1 point-table workbook.")
    parser.add_argument("--output-dir", required=True, help="Destination directory for workbook/CSV artifacts.")
    parser.add_argument(
        "--co2-fit-ppm",
        default=",".join(str(value) for value in DEFAULT_CO2_FIT_PPM),
        help="CO2 nominal ppm values allowed for fit. Default includes zero gas.",
    )
    parser.add_argument(
        "--co2-verification-ppm",
        default=",".join(str(value) for value in DEFAULT_CO2_VERIFICATION_PPM),
        help="CO2 nominal ppm values reserved for independent verification.",
    )
    parser.add_argument("--purge-s", type=float, default=360.0)
    parser.add_argument("--sample-count", type=int, default=10)
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    try:
        outputs = write_open_flow_canonical_point_plan(
            source_points_xlsx=args.source_points_xlsx,
            output_dir=args.output_dir,
            co2_fit_ppm=_parse_int_list(args.co2_fit_ppm, DEFAULT_CO2_FIT_PPM),
            co2_verification_ppm=_parse_int_list(args.co2_verification_ppm, DEFAULT_CO2_VERIFICATION_PPM),
            purge_s=float(args.purge_s),
            sample_count=int(args.sample_count),
        )
    except Exception as exc:
        print(f"Export V1.5 open-flow canonical points failed: {exc}", file=sys.stderr, flush=True)
        return 1
    print(json.dumps({key: str(value) for key, value in outputs.items()}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
