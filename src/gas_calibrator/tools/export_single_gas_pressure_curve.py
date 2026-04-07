"""Export point means and simple pressure-relationship plots from a room-temp diagnostic run."""

from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path
from typing import Iterable, Mapping, Optional

from ..validation.single_gas_pressure_curve import (
    build_pressure_curve_point_means,
    generate_pressure_curve_plots,
    summarize_pressure_curve_relationships,
    write_pressure_curve_point_means_csv,
    write_pressure_curve_summary_json,
)


def _load_csv_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        raise FileNotFoundError(path)
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export single-gas pressure curve means and plots.")
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--variant", default=None)
    parser.add_argument("--gas-ppm", type=int, default=None)
    parser.add_argument("--repeat-index", type=int, default=1)
    parser.add_argument("--ambient-tail-window-s", type=float, default=60.0)
    parser.add_argument("--output-dir", default=None)
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = parse_args(argv)
    run_dir = Path(args.run_dir).resolve()
    raw_rows = _load_csv_rows(run_dir / "raw_timeseries.csv")
    point_rows = build_pressure_curve_point_means(
        raw_rows,
        process_variant=args.variant,
        gas_ppm=args.gas_ppm,
        repeat_index=args.repeat_index,
        ambient_tail_window_s=float(args.ambient_tail_window_s),
    )
    if not point_rows:
        print("No pressure-curve rows found for the requested filters.", flush=True)
        return 1

    output_dir = Path(args.output_dir).resolve() if args.output_dir else (run_dir / "single_gas_pressure_curve").resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    means_csv = write_pressure_curve_point_means_csv(output_dir / "pressure_curve_point_means.csv", point_rows)
    summary = summarize_pressure_curve_relationships(point_rows)
    summary_json = write_pressure_curve_summary_json(output_dir / "pressure_curve_summary.json", summary)
    plots = generate_pressure_curve_plots(output_dir, point_rows=point_rows)

    print(f"point_means -> {means_csv}", flush=True)
    print(f"summary -> {summary_json}", flush=True)
    for key, path in plots.items():
        print(f"{key} -> {path}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
