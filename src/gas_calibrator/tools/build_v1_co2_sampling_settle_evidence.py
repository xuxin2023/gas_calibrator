"""Build a V1 CO2 sampling / settle evidence sidecar from completed outputs."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Mapping, Sequence

from ..workflow.co2_calibration_candidate_pack import read_csv_rows
from ..workflow.co2_sampling_settle_evidence import (
    build_co2_sampling_settle_evidence,
    write_co2_sampling_settle_artifacts,
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
    raise FileNotFoundError(f"missing points export under: {run_dir}")


def _load_json_payload(path: Path) -> Mapping[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _load_rows(
    *,
    run_dir: Path | None,
    points_csv: Path | None,
    candidate_pack_json: Path | None,
) -> list[dict]:
    if run_dir is not None:
        return read_csv_rows(_latest_points_csv(run_dir))
    if points_csv is not None:
        return read_csv_rows(points_csv)
    if candidate_pack_json is not None:
        payload = _load_json_payload(candidate_pack_json)
        return list(payload.get("points") or [])
    raise ValueError("must provide one input source")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Build a V1 CO2 sampling / settle evidence sidecar from completed point exports."
    )
    parser.add_argument("--run-dir", type=Path, help="Completed V1 run directory.")
    parser.add_argument("--points-csv", type=Path, help="Explicit points/points_readable csv path.")
    parser.add_argument("--candidate-pack-json", type=Path, help="Existing calibration_candidate_pack.json path.")
    parser.add_argument("--output-dir", type=Path, required=True, help="Directory for sampling/settle evidence artifacts.")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(list(argv) if argv is not None else None)
    selected = [value for value in (args.run_dir, args.points_csv, args.candidate_pack_json) if value is not None]
    if len(selected) != 1:
        parser.error("must provide exactly one of --run-dir / --points-csv / --candidate-pack-json")

    rows = _load_rows(
        run_dir=args.run_dir,
        points_csv=args.points_csv,
        candidate_pack_json=args.candidate_pack_json,
    )
    payload = build_co2_sampling_settle_evidence(rows)
    write_co2_sampling_settle_artifacts(args.output_dir, payload)
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
