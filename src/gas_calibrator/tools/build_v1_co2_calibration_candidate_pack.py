"""Build a V1 CO2 grouped calibration candidate pack from completed outputs.

This tool stays outside the frozen V1 UI and default live path. It only reads
completed point exports and organizes the existing hardened evidence into a
calibration-oriented candidate pack.
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Sequence

from ..workflow.co2_calibration_candidate_pack import (
    build_co2_calibration_candidate_pack,
    read_csv_rows,
    write_pack_artifacts,
)


def _latest_points_csv(run_dir: Path) -> Path:
    direct_points = sorted(
        [
            path
            for path in run_dir.glob("points_*.csv")
            if path.is_file() and not path.name.startswith("points_readable_")
        ],
        key=lambda item: item.stat().st_mtime,
    )
    if direct_points:
        return direct_points[-1]
    readable_points = sorted(
        [path for path in run_dir.glob("points_readable_*.csv") if path.is_file()],
        key=lambda item: item.stat().st_mtime,
    )
    if readable_points:
        return readable_points[-1]
    raise FileNotFoundError(f"未找到 points 导出: {run_dir}")


def build_pack_from_points_csv(points_csv: Path, output_dir: Path) -> dict[str, Path]:
    rows = read_csv_rows(points_csv)
    pack = build_co2_calibration_candidate_pack(rows)
    return write_pack_artifacts(output_dir, pack)


def build_pack_from_run_dir(run_dir: Path, output_dir: Path | None = None) -> dict[str, Path]:
    points_csv = _latest_points_csv(run_dir)
    target_dir = output_dir or (run_dir / "co2_calibration_candidate_pack")
    return build_pack_from_points_csv(points_csv, target_dir)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Build a V1 CO2 grouped calibration candidate pack from completed point exports."
    )
    parser.add_argument("--run-dir", type=Path, help="Completed V1 run directory.")
    parser.add_argument("--points-csv", type=Path, help="Explicit points/points_readable csv path.")
    parser.add_argument("--output-dir", type=Path, required=True, help="Directory for candidate pack artifacts.")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(list(argv) if argv is not None else None)
    if bool(args.run_dir) == bool(args.points_csv):
        parser.error("必须且只能提供 --run-dir 或 --points-csv 其中一个")
    if args.run_dir:
        build_pack_from_run_dir(args.run_dir, args.output_dir)
    else:
        build_pack_from_points_csv(args.points_csv, args.output_dir)
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
