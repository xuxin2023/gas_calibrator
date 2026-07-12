"""Materialize immutable profile queue inputs for mature V1.5 route runners."""

from __future__ import annotations

import csv
import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping, Sequence

from .v1_5_algorithm_route_profiles import build_v1_5_profile_queue_rows

SCHEMA = "v1_5_algorithm_mature_queue_inputs_v1"
CO2_QUEUE_RUNNER = "gas_calibrator.tools.run_v1_5_formal_co2_open_flow_queue"
H2O_QUEUE_RUNNER = "gas_calibrator.tools.run_v1_5_formal_h2o_open_flow_queue"
EXPECTED_COUNTS = {
    "legacy_ratio_production": (45, 13),
    "absorption_ratio_shadow": (47, 14),
}
FORBIDDEN_SOURCE_MARKERS = ("_handoff", "20260624", "0624", "migration")


def _expected_co2_points(profile_id: str) -> set[tuple[float, float]]:
    points = {
        (temp, ppm)
        for temp in (-20.0, -10.0, 0.0, 40.0)
        for ppm in (0.0, 400.0, 1000.0)
    }
    points.update(
        (temp, float(ppm))
        for temp in (10.0, 20.0, 30.0)
        for ppm in range(0, 1001, 100)
    )
    if profile_id == "absorption_ratio_shadow":
        points.update({(-20.0, 600.0), (-10.0, 600.0)})
    return points


def _expected_h2o_points(profile_id: str) -> set[tuple[float, float, float]]:
    points = {
        (0.0, 0.0, 50.0),
        (10.0, 10.0, 30.0),
        (10.0, 10.0, 50.0),
        (10.0, 10.0, 70.0),
        (20.0, 20.0, 30.0),
        (20.0, 20.0, 50.0),
        (20.0, 20.0, 70.0),
        (30.0, 20.0, 30.0),
        (30.0, 20.0, 50.0),
        (30.0, 20.0, 70.0),
        (30.0, 20.0, 90.0),
        (40.0, 30.0, 50.0),
        (40.0, 30.0, 70.0),
    }
    if profile_id == "absorption_ratio_shadow":
        points.add((40.0, 30.0, 30.0))
    return points


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _write_csv(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    fields: list[str] = []
    for row in rows:
        for key in row:
            if key not in fields:
                fields.append(str(key))
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(dict(row) for row in rows)


def build_v1_5_algorithm_mature_queue_inputs(
    *,
    profile_path: str | Path,
    profile_id: str,
) -> dict[str, Any]:
    profile_file = Path(profile_path).resolve()
    if profile_id not in EXPECTED_COUNTS:
        raise ValueError(f"Unsupported V1.5 algorithm profile: {profile_id}")
    rows = build_v1_5_profile_queue_rows(profile_file, profile_id=profile_id)
    if int(rows.get("point_plan_guard_blocker_count") or 0) != 0:
        raise ValueError("V1.5 algorithm point-plan guard contains blockers")
    if set(rows.get("source_runners") or ()) != {CO2_QUEUE_RUNNER, H2O_QUEUE_RUNNER}:
        raise ValueError("Profile does not reference the two mature V1.5 queue runners")
    co2_rows = [dict(row) for row in rows["co2_rows"]]
    h2o_rows = [dict(row) for row in rows["h2o_rows"]]
    expected_co2, expected_h2o = EXPECTED_COUNTS[profile_id]
    if (len(co2_rows), len(h2o_rows)) != (expected_co2, expected_h2o):
        raise ValueError(
            "Profile point count does not match the V1.5 algorithm queue contract"
        )
    observed_co2 = {
        (float(row["temp_c"]), float(row["source_nominal_ppm"]))
        for row in co2_rows
    }
    observed_h2o = {
        (
            float(row["temp_c"]),
            float(row["hgen_temp_c"]),
            float(row["hgen_rh_pct"]),
        )
        for row in h2o_rows
    }
    if observed_co2 != _expected_co2_points(profile_id):
        raise ValueError("CO2 point identities differ from the mature profile contract")
    if observed_h2o != _expected_h2o_points(profile_id):
        raise ValueError("H2O point identities differ from the mature profile contract")
    for row in [*co2_rows, *h2o_rows]:
        source_text = json.dumps(row, ensure_ascii=False).lower()
        if any(marker in source_text for marker in FORBIDDEN_SOURCE_MARKERS):
            raise ValueError("Generated queue row references a forbidden migrated source")
        row["runner_integration_status"] = "profile_generated_mature_queue_input"
        row["queue_source_contract"] = "generated_from_reviewed_profile_only"
    return {
        "schema": SCHEMA,
        "generated_at": datetime.now(UTC).replace(microsecond=0).isoformat(),
        "overall_status": "ready_for_locked_mature_queue_plan",
        "profile_id": profile_id,
        "algorithm_mode": rows["algorithm_mode"],
        "profile_sha256": _sha(profile_file),
        "co2_point_count": len(co2_rows),
        "h2o_point_count": len(h2o_rows),
        "co2_queue_runner": CO2_QUEUE_RUNNER,
        "h2o_queue_runner": H2O_QUEUE_RUNNER,
        "queue_source_contract": "generated_from_reviewed_profile_only",
        "profile_declared_queue_source_is_not_consumed": True,
        "mature_point_execution_is_not_copied_or_modified": True,
        "opens_com_ports": False,
        "controls_water_or_gas_routes": False,
        "writes_coefficients": False,
        "connects_postgresql": False,
        "formal_release_allowed": False,
        "database_import_allowed": False,
        "not_real_acceptance_evidence": True,
        "co2_rows": co2_rows,
        "h2o_rows": h2o_rows,
    }


def write_v1_5_algorithm_mature_queue_inputs(
    *,
    profile_path: str | Path,
    profile_id: str,
    output_dir: str | Path,
    recorded_output_dir: str | Path | None = None,
) -> dict[str, Any]:
    model = build_v1_5_algorithm_mature_queue_inputs(
        profile_path=profile_path,
        profile_id=profile_id,
    )
    output = Path(output_dir)
    recorded_output = Path(recorded_output_dir) if recorded_output_dir else output
    co2_path = output / "co2_runner_queue.csv"
    h2o_path = output / "h2o_runner_queue.csv"
    manifest_path = output / "v1_5_queue_inputs.json"
    _write_csv(co2_path, model.pop("co2_rows"))
    _write_csv(h2o_path, model.pop("h2o_rows"))
    model["co2_queue_csv"] = str((recorded_output / co2_path.name).absolute())
    model["h2o_queue_csv"] = str((recorded_output / h2o_path.name).absolute())
    model["co2_queue_sha256"] = _sha(co2_path)
    model["h2o_queue_sha256"] = _sha(h2o_path)
    manifest_path.write_text(
        json.dumps(model, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    return {
        **model,
        "manifest_json": str((recorded_output / manifest_path.name).absolute()),
        "manifest_sha256": _sha(manifest_path),
    }


__all__ = [
    "CO2_QUEUE_RUNNER",
    "EXPECTED_COUNTS",
    "H2O_QUEUE_RUNNER",
    "SCHEMA",
    "build_v1_5_algorithm_mature_queue_inputs",
    "write_v1_5_algorithm_mature_queue_inputs",
]
