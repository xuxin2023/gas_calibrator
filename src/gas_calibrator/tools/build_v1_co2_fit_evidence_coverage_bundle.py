"""Build a V1 CO2 fit evidence coverage / point-to-fit traceability bundle."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Mapping, Sequence

from ..workflow.co2_calibration_candidate_pack import read_csv_rows
from ..workflow.co2_fit_arbitration_bundle import build_co2_fit_arbitration_bundle
from ..workflow.co2_fit_evidence_coverage_bundle import (
    build_co2_fit_evidence_coverage_bundle,
    write_co2_fit_evidence_coverage_artifacts,
)
from ..workflow.co2_release_readiness_bundle import build_co2_release_readiness_bundle


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


def _load_rows(*, run_dir: Path | None, points_csv: Path | None, candidate_pack_json: Path | None) -> list[dict]:
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
        description="Build a V1 CO2 fit evidence coverage / point-to-fit traceability bundle."
    )
    parser.add_argument("--fit-arbitration-summary-json", type=Path, help="Existing fit_arbitration_summary.json path.")
    parser.add_argument("--release-readiness-summary-json", type=Path, help="Existing release_readiness_summary.json path.")
    parser.add_argument("--run-dir", type=Path, help="Completed V1 run directory.")
    parser.add_argument("--points-csv", type=Path, help="Explicit points/points_readable csv path.")
    parser.add_argument("--candidate-pack-json", type=Path, help="Existing calibration_candidate_pack.json path.")
    parser.add_argument("--output-dir", type=Path, required=True, help="Directory for fit evidence coverage artifacts.")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(list(argv) if argv is not None else None)

    json_mode = args.fit_arbitration_summary_json is not None or args.release_readiness_summary_json is not None
    raw_inputs = [value for value in (args.run_dir, args.points_csv, args.candidate_pack_json) if value is not None]

    if json_mode:
        if args.fit_arbitration_summary_json is None or args.release_readiness_summary_json is None:
            parser.error("must provide both --fit-arbitration-summary-json and --release-readiness-summary-json")
        if raw_inputs:
            parser.error("cannot combine summary-json inputs with --run-dir / --points-csv / --candidate-pack-json")
        payload = build_co2_fit_evidence_coverage_bundle(
            fit_arbitration_payload=_load_json_payload(args.fit_arbitration_summary_json),
            release_readiness_payload=_load_json_payload(args.release_readiness_summary_json),
        )
    else:
        if len(raw_inputs) != 1:
            parser.error("must provide exactly one of --run-dir / --points-csv / --candidate-pack-json")
        rows = _load_rows(run_dir=args.run_dir, points_csv=args.points_csv, candidate_pack_json=args.candidate_pack_json)
        fit_payload = build_co2_fit_arbitration_bundle(rows)
        release_payload = build_co2_release_readiness_bundle(rows, fit_arbitration_payload=fit_payload)
        payload = build_co2_fit_evidence_coverage_bundle(
            fit_arbitration_payload=fit_payload,
            release_readiness_payload=release_payload,
        )

    write_co2_fit_evidence_coverage_artifacts(args.output_dir, payload)
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
