"""Build a V1 CO2 weighted-fit stability audit from exported artifacts."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Sequence

from ..workflow.co2_calibration_candidate_pack import read_csv_rows
from ..workflow.co2_fit_stability_audit import (
    build_co2_fit_stability_audit,
    extract_candidate_rows_from_weighted_fit_payload,
    write_co2_fit_stability_artifacts,
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


def _load_rows(
    *,
    weighted_fit_summary_json: Path | None,
    candidate_pack_json: Path | None,
    points_csv: Path | None,
    run_dir: Path | None,
) -> list[dict]:
    if weighted_fit_summary_json is not None:
        payload = json.loads(weighted_fit_summary_json.read_text(encoding="utf-8"))
        return extract_candidate_rows_from_weighted_fit_payload(payload)
    if candidate_pack_json is not None:
        payload = json.loads(candidate_pack_json.read_text(encoding="utf-8"))
        return list(payload.get("points") or [])
    if points_csv is not None:
        return read_csv_rows(points_csv)
    if run_dir is not None:
        return read_csv_rows(_latest_points_csv(run_dir))
    raise ValueError("must provide one input source")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Build a V1 CO2 weighted-fit stability audit from candidate or weighted-fit outputs."
    )
    parser.add_argument("--weighted-fit-summary-json", type=Path, help="Existing weighted_fit_summary.json path.")
    parser.add_argument("--candidate-pack-json", type=Path, help="Existing calibration_candidate_pack.json path.")
    parser.add_argument("--points-csv", type=Path, help="Explicit points or points_readable csv path.")
    parser.add_argument("--run-dir", type=Path, help="Completed V1 run directory.")
    parser.add_argument("--output-dir", type=Path, required=True, help="Directory for fit stability audit artifacts.")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(list(argv) if argv is not None else None)
    selected = [
        value
        for value in (
            args.weighted_fit_summary_json,
            args.candidate_pack_json,
            args.points_csv,
            args.run_dir,
        )
        if value is not None
    ]
    if len(selected) != 1:
        parser.error("必须且只能提供 --weighted-fit-summary-json / --candidate-pack-json / --points-csv / --run-dir 其中一个")
    rows = _load_rows(
        weighted_fit_summary_json=args.weighted_fit_summary_json,
        candidate_pack_json=args.candidate_pack_json,
        points_csv=args.points_csv,
        run_dir=args.run_dir,
    )
    payload = build_co2_fit_stability_audit(rows)
    write_co2_fit_stability_artifacts(args.output_dir, payload)
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
