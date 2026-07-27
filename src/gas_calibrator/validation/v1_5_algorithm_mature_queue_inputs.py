"""Materialize immutable profile queue inputs for mature V1.5 route runners."""

from __future__ import annotations

import csv
import hashlib
import json
from datetime import UTC, date, datetime
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


def _portable_catalog_path(catalog_path: Path, profile_path: Path) -> str:
    candidate_root = profile_path.resolve().parent.parent
    try:
        return catalog_path.resolve().relative_to(candidate_root).as_posix()
    except ValueError:
        return str(catalog_path.resolve())


def _bind_reference_sources(
    *,
    co2_rows: list[dict[str, Any]],
    h2o_rows: list[dict[str, Any]],
    catalog_path: Path,
    profile_path: Path,
) -> dict[str, Any]:
    if not catalog_path.is_file():
        raise ValueError("V1.5 reference-source catalog is missing")
    payload = json.loads(catalog_path.read_text(encoding="utf-8-sig"))
    if not isinstance(payload, dict):
        raise ValueError("V1.5 reference-source catalog must be a JSON object")
    if payload.get("schema_version") != "v1_5_reference_source_catalog_v1":
        raise ValueError("V1.5 reference-source catalog schema is invalid")
    if payload.get("not_real_acceptance_evidence") is not True:
        raise ValueError("V1.5 reference-source catalog safety lock is missing")
    assets = [
        dict(row)
        for row in payload.get("assets") or []
        if isinstance(row, Mapping) and str(row.get("route_kind") or "").lower() == "co2"
    ]
    by_nominal: dict[float, list[dict[str, Any]]] = {}
    for asset in assets:
        try:
            nominal = float(asset["nominal_co2_ppm"])
        except (KeyError, TypeError, ValueError) as exc:
            raise ValueError("Reference asset nominal CO2 value is invalid") from exc
        by_nominal.setdefault(nominal, []).append(asset)
    portable_catalog = _portable_catalog_path(catalog_path, profile_path)
    catalog_sha256 = _sha(catalog_path)
    checked_on = date.today()
    bound_asset_ids: set[str] = set()
    for row in co2_rows:
        nominal = float(row["source_nominal_ppm"])
        matches = by_nominal.get(nominal, [])
        if len(matches) != 1:
            raise ValueError(
                f"CO2 nominal {nominal:g} ppm must bind exactly one reference asset"
            )
        asset = matches[0]
        asset_id = str(asset.get("asset_id") or "").strip()
        cylinder_number = str(asset.get("cylinder_number") or "").strip()
        documents = asset.get("documents")
        if not asset_id or not cylinder_number:
            raise ValueError(f"CO2 nominal {nominal:g} ppm reference identity is incomplete")
        if asset.get("documentary_use_status") != "operator_authorized_for_v1_5_evidence":
            raise ValueError(f"CO2 nominal {nominal:g} ppm reference is not operator authorized")
        if asset.get("calibration_fit_reference_allowed") is not True:
            raise ValueError(f"CO2 nominal {nominal:g} ppm reference is not fit-eligible")
        if not isinstance(documents, list) or not documents:
            raise ValueError(f"CO2 nominal {nominal:g} ppm reference documents are missing")
        if any(
            not isinstance(document, Mapping)
            or not str(document.get("sha256") or "").strip()
            or int(document.get("size_bytes") or 0) <= 0
            for document in documents
        ):
            raise ValueError(f"CO2 nominal {nominal:g} ppm document binding is incomplete")
        try:
            issue_date = date.fromisoformat(str(asset.get("issue_date") or ""))
            valid_through = date.fromisoformat(str(asset.get("valid_through") or ""))
        except ValueError as exc:
            raise ValueError(f"CO2 nominal {nominal:g} ppm validity dates are invalid") from exc
        if not issue_date <= checked_on <= valid_through:
            raise ValueError(f"CO2 nominal {nominal:g} ppm reference is outside validity")
        row.update(
            {
                "reference_asset_id": asset_id,
                "certificate_co2_ppm": float(asset["certificate_co2_ppm"]),
                "reference_value_source": str(asset.get("reference_value_source") or ""),
                "reference_physical_role": str(asset.get("physical_role") or ""),
                "reference_cylinder_number": cylinder_number,
                "reference_valid_through": valid_through.isoformat(),
                "reference_document_count": len(documents),
                "co2_value_directly_certified": asset.get(
                    "co2_value_directly_certified"
                ),
                "reference_source_catalog": portable_catalog,
                "reference_source_catalog_sha256": catalog_sha256,
                "reference_gate_mode": "strict_pre_device_construction",
            }
        )
        bound_asset_ids.add(asset_id)
    for row in h2o_rows:
        row.update(
            {
                "reference_asset_id": "dynamic_h2o_dewpoint_pressure_reference",
                "reference_value_source": "measured_dewpoint_plus_measured_pressure",
                "reference_physical_role": "h2o_dynamic_wet_point",
                "route_flow_source_policy": (
                    "dewpoint_meter_output_preferred_hgen_state_fallback"
                ),
                "reference_source_catalog": portable_catalog,
                "reference_source_catalog_sha256": catalog_sha256,
                "reference_gate_mode": "strict_post_sample_evidence_bundle",
            }
        )
    return {
        "reference_source_binding_status": "bound",
        "reference_source_catalog": portable_catalog,
        "reference_source_catalog_sha256": catalog_sha256,
        "reference_source_catalog_schema": payload["schema_version"],
        "reference_source_catalog_asset_count": len(assets),
        "bound_co2_reference_asset_count": len(bound_asset_ids),
        "h2o_reference_policy": "measured_dewpoint_plus_measured_pressure",
        "route_flow_source_policy": (
            "dewpoint_meter_output_preferred_hgen_state_fallback"
        ),
    }


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
    reference_source_catalog: str | Path | None = None,
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
    reference_binding = {
        "reference_source_binding_status": "not_requested",
        "reference_source_catalog": "",
        "reference_source_catalog_sha256": "",
        "reference_source_catalog_schema": "",
        "reference_source_catalog_asset_count": 0,
        "bound_co2_reference_asset_count": 0,
        "h2o_reference_policy": "measured_dewpoint_plus_measured_pressure",
        "route_flow_source_policy": (
            "dewpoint_meter_output_preferred_hgen_state_fallback"
        ),
    }
    if reference_source_catalog is not None:
        reference_binding = _bind_reference_sources(
            co2_rows=co2_rows,
            h2o_rows=h2o_rows,
            catalog_path=Path(reference_source_catalog).resolve(),
            profile_path=profile_file,
        )
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
        **reference_binding,
        "co2_rows": co2_rows,
        "h2o_rows": h2o_rows,
    }


def write_v1_5_algorithm_mature_queue_inputs(
    *,
    profile_path: str | Path,
    profile_id: str,
    output_dir: str | Path,
    recorded_output_dir: str | Path | None = None,
    reference_source_catalog: str | Path | None = None,
) -> dict[str, Any]:
    model = build_v1_5_algorithm_mature_queue_inputs(
        profile_path=profile_path,
        profile_id=profile_id,
        reference_source_catalog=reference_source_catalog,
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
