"""Build a V1 CO2 fit arbitration / release-candidate advisory bundle."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Mapping, Sequence

from ..workflow.co2_bootstrap_robustness_audit import build_co2_bootstrap_robustness_audit
from ..workflow.co2_calibration_candidate_pack import read_csv_rows
from ..workflow.co2_fit_arbitration_bundle import (
    build_co2_fit_arbitration_bundle,
    write_co2_fit_arbitration_artifacts,
)
from ..workflow.co2_fit_stability_audit import (
    build_co2_fit_stability_audit,
    extract_candidate_rows_from_weighted_fit_payload,
)
from ..workflow.co2_weighted_fit_advisory import build_co2_weighted_fit_advisory


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


def _load_json_payload(path: Path | None) -> Mapping[str, Any] | None:
    if path is None:
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def _load_rows(
    *,
    bootstrap_summary_json: Path | None,
    weighted_fit_summary_json: Path | None,
    candidate_pack_json: Path | None,
    points_csv: Path | None,
    run_dir: Path | None,
) -> list[dict]:
    if bootstrap_summary_json is not None:
        payload = _load_json_payload(bootstrap_summary_json) or {}
        return extract_candidate_rows_from_weighted_fit_payload({"points": payload.get("points") or []})
    if weighted_fit_summary_json is not None:
        payload = _load_json_payload(weighted_fit_summary_json) or {}
        return extract_candidate_rows_from_weighted_fit_payload(payload)
    if candidate_pack_json is not None:
        payload = _load_json_payload(candidate_pack_json) or {}
        return list(payload.get("points") or [])
    if points_csv is not None:
        return read_csv_rows(points_csv)
    if run_dir is not None:
        return read_csv_rows(_latest_points_csv(run_dir))
    raise ValueError("must provide one input source")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Build a V1 CO2 fit arbitration / release-candidate advisory bundle from existing sidecar artifacts."
    )
    parser.add_argument("--bootstrap-summary-json", type=Path, help="Existing bootstrap_fit_summary.json path.")
    parser.add_argument("--fit-stability-summary-json", type=Path, help="Optional fit_stability_summary.json path.")
    parser.add_argument("--weighted-fit-summary-json", type=Path, help="Optional weighted_fit_summary.json path.")
    parser.add_argument("--candidate-pack-json", type=Path, help="Existing calibration_candidate_pack.json path.")
    parser.add_argument("--points-csv", type=Path, help="Explicit points or points_readable csv path.")
    parser.add_argument("--run-dir", type=Path, help="Completed V1 run directory.")
    parser.add_argument("--output-dir", type=Path, required=True, help="Directory for fit arbitration artifacts.")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(list(argv) if argv is not None else None)
    primary_selected = [
        value
        for value in (
            args.bootstrap_summary_json,
            None if args.bootstrap_summary_json is not None else args.weighted_fit_summary_json,
            args.candidate_pack_json,
            args.points_csv,
            args.run_dir,
        )
        if value is not None
    ]
    if len(primary_selected) != 1:
        parser.error(
            "must provide exactly one of --bootstrap-summary-json / --weighted-fit-summary-json / --candidate-pack-json / --points-csv / --run-dir"
        )

    rows = _load_rows(
        bootstrap_summary_json=args.bootstrap_summary_json,
        weighted_fit_summary_json=args.weighted_fit_summary_json,
        candidate_pack_json=args.candidate_pack_json,
        points_csv=args.points_csv,
        run_dir=args.run_dir,
    )

    weighted_fit_payload = _load_json_payload(args.weighted_fit_summary_json)
    fit_stability_payload = _load_json_payload(args.fit_stability_summary_json)
    bootstrap_payload = _load_json_payload(args.bootstrap_summary_json)

    if weighted_fit_payload is None:
        if bootstrap_payload is not None:
            weighted_fit_payload = {
                "summary": dict(bootstrap_payload.get("weighted_fit_advisory_summary") or {}),
                "fit_variants": list(bootstrap_payload.get("fit_variants") or []),
                "points": list(bootstrap_payload.get("points") or []),
                "groups": [],
            }
        else:
            weighted_fit_payload = build_co2_weighted_fit_advisory(rows)

    if fit_stability_payload is None:
        fit_stability_payload = build_co2_fit_stability_audit(rows)
    if bootstrap_payload is None:
        bootstrap_payload = build_co2_bootstrap_robustness_audit(rows, fit_stability_payload=fit_stability_payload)

    payload = build_co2_fit_arbitration_bundle(
        rows,
        weighted_fit_payload=weighted_fit_payload,
        fit_stability_payload=fit_stability_payload,
        bootstrap_payload=bootstrap_payload,
    )
    write_co2_fit_arbitration_artifacts(args.output_dir, payload)
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
