"""Replay historical V1.5 fit inputs against the selected algorithm profile.

The replay is deliberately offline. It verifies that historical 0613 fitting
evidence and 0620/0621 route evidence keep their physical meaning when they
enter either the legacy ratio model or the absorption/R0(T) candidate model.
It never fits coefficients or authorizes a device write.
"""

from __future__ import annotations

import csv
import hashlib
import json
import math
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping, Sequence

from .v1_5_algorithm_mature_queue_inputs import EXPECTED_COUNTS
from .v1_5_algorithm_profile_lineage_gate import PROFILE_CONTRACTS

SCHEMA = "v1_5_historical_fit_profile_parity_v1"
FIT_BASELINE = "0613"
ROUTE_BASELINES = {"0620", "0621"}
MIN_H2O_DRY_ANCHORS = 3
NUMERIC_TOLERANCE = 1e-9


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8-sig"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        return {}
    return dict(payload) if isinstance(payload, Mapping) else {}


def _read_csv(path: Path) -> list[dict[str, str]]:
    try:
        with path.open("r", encoding="utf-8-sig", newline="") as handle:
            return [dict(row) for row in csv.DictReader(handle)]
    except OSError:
        return []


def _sha256(path: Path) -> str:
    try:
        return hashlib.sha256(path.read_bytes()).hexdigest() if path.is_file() else ""
    except OSError:
        return ""


def _float(value: Any) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def _bool(value: Any) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


def _same_number(left: float | None, right: float | None) -> bool:
    if left is None or right is None:
        return False
    return math.isclose(left, right, rel_tol=NUMERIC_TOLERANCE, abs_tol=NUMERIC_TOLERANCE)


def _check(check_id: str, reasons: Sequence[str], details: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "check_id": check_id,
        "status": "pass" if not reasons else "blocker",
        "reasons": list(dict.fromkeys(reasons)),
        "details": dict(details),
    }


def _co2_identity(row: Mapping[str, Any]) -> tuple[float, float] | None:
    temp = _float(row.get("temp_c"))
    ppm = _float(row.get("source_nominal_ppm"))
    return (temp, ppm) if temp is not None and ppm is not None else None


def _h2o_identity(row: Mapping[str, Any]) -> tuple[float, float, float] | None:
    temp = _float(row.get("temp_c"))
    hgen = _float(row.get("hgen_temp_c"))
    rh = _float(row.get("hgen_rh_pct"))
    return (temp, hgen, rh) if temp is not None and hgen is not None and rh is not None else None


def _expected_points(lineage: Mapping[str, Any]) -> dict[str, set[tuple[float, ...]]]:
    source_paths = lineage.get("source_paths") if isinstance(lineage.get("source_paths"), Mapping) else {}
    queue_inputs = _read_json(Path(str(source_paths.get("queue_inputs_json") or "")))
    co2_rows = _read_csv(Path(str(queue_inputs.get("co2_queue_csv") or "")))
    h2o_rows = _read_csv(Path(str(queue_inputs.get("h2o_queue_csv") or "")))
    return {
        "co2": {identity for row in co2_rows if (identity := _co2_identity(row)) is not None},
        "h2o": {identity for row in h2o_rows if (identity := _h2o_identity(row)) is not None},
    }


def _r0_sources(lineage: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    source_paths = lineage.get("source_paths") if isinstance(lineage.get("source_paths"), Mapping) else {}
    result: dict[str, dict[str, Any]] = {}
    for component in ("co2", "h2o"):
        path = Path(str(source_paths.get(f"{component}_r0_model_json") or ""))
        payload = _read_json(path)
        evaluations: list[tuple[float, float]] = []
        for row in payload.get("evaluated_points", []):
            if not isinstance(row, Mapping):
                continue
            temp = _float(row.get("temperature_c"))
            value = _float(row.get("r0_value"))
            if temp is not None and value is not None and value > 0:
                evaluations.append((temp, value))
        result[component] = {
            "path": str(path),
            "sha256": _sha256(path),
            "payload": payload,
            "evaluations": evaluations,
        }
    return result


def _reviewed_r0_value(source: Mapping[str, Any], temperature_c: float | None) -> float | None:
    if temperature_c is None:
        return None
    for reviewed_temp, reviewed_value in source.get("evaluations", []):
        if _same_number(_float(reviewed_temp), temperature_c):
            return _float(reviewed_value)
    return None


def _row_identity(row: Mapping[str, Any]) -> tuple[float, ...] | None:
    role = str(row.get("sample_role") or "").strip()
    if role == "h2o_dry_gas_anchor":
        temp = _float(row.get("temperature_c"))
        return (temp,) if temp is not None else None
    if str(row.get("component") or "").lower() == "co2":
        return _co2_identity(row)
    return _h2o_identity(row)


def _validate_row(
    row: Mapping[str, Any],
    *,
    profile_id: str,
    profile_sha256: str,
    algorithm_mode: str,
    r0_required: bool,
    r0_sources: Mapping[str, Mapping[str, Any]],
) -> tuple[list[str], dict[str, Any]]:
    reasons: list[str] = []
    component = str(row.get("component") or "").strip().lower()
    role = str(row.get("sample_role") or "").strip()
    if not str(row.get("device_id") or "").strip():
        reasons.append("device_id_missing")
    if not str(row.get("point_id") or "").strip():
        reasons.append("point_id_missing")
    if component not in {"co2", "h2o"}:
        reasons.append("component_invalid")
    if str(row.get("profile_id") or "") != profile_id:
        reasons.append("profile_id_mismatch")
    if str(row.get("profile_sha256") or "") != profile_sha256:
        reasons.append("profile_sha256_mismatch")
    if str(row.get("algorithm_mode") or "") != algorithm_mode:
        reasons.append("algorithm_mode_mismatch")
    if str(row.get("fitting_baseline") or "") != FIT_BASELINE:
        reasons.append("fitting_baseline_must_be_0613")
    if str(row.get("route_baseline") or "") not in ROUTE_BASELINES:
        reasons.append("route_baseline_must_be_0620_or_0621")
    if not _bool(row.get("fit_eligible")):
        reasons.append("row_not_fit_eligible")
    if not str(row.get("quality_grade") or "").upper().startswith("A"):
        reasons.append("quality_grade_not_A")

    ratio = _float(row.get("ratio_r"))
    fit_value = _float(row.get("fit_input_value"))
    pressure = _float(row.get("pressure_kpa"))
    temperature = _float(row.get("temperature_c"))
    if ratio is None or ratio <= 0:
        reasons.append("ratio_r_invalid")
    if temperature is None:
        reasons.append("chamber_T1_missing")

    expected_value: float | None = None
    r0_value = _float(row.get("r0_value"))
    if r0_required:
        if str(row.get("fit_input_variable") or "") != "A":
            reasons.append("absorption_profile_requires_A")
        if pressure is None or pressure <= 0:
            reasons.append("pressure_kpa_invalid")
        if r0_value is None or r0_value <= 0:
            reasons.append("r0_value_invalid")
        source = r0_sources.get(component, {})
        source_sha = str(source.get("sha256") or "")
        if not source_sha:
            reasons.append(f"{component}_r0_model_file_missing")
        if str(row.get("r0_model_sha256") or "") != source_sha:
            reasons.append(f"{component}_r0_model_sha_mismatch")
        reviewed_r0 = _reviewed_r0_value(source, temperature)
        if reviewed_r0 is None:
            reasons.append(f"{component}_r0_temperature_not_reviewed")
        elif not _same_number(r0_value, reviewed_r0):
            reasons.append(f"{component}_r0_value_not_from_reviewed_model")
        if ratio is not None and ratio > 0 and r0_value is not None and r0_value > 0 and pressure and pressure > 0:
            expected_value = -math.log(ratio / r0_value) / (pressure / 100.0)
            if not _same_number(fit_value, expected_value):
                reasons.append("absorption_fit_input_value_mismatch")
    else:
        if str(row.get("fit_input_variable") or "") != "R":
            reasons.append("legacy_profile_requires_R")
        if str(row.get("r0_model_sha256") or "").strip() or str(row.get("r0_value") or "").strip():
            reasons.append("legacy_profile_must_not_consume_R0")
        expected_value = ratio
        if not _same_number(fit_value, ratio):
            reasons.append("legacy_fit_input_must_equal_ratio_R")

    if role == "h2o_dry_gas_anchor":
        if component != "h2o":
            reasons.append("h2o_dry_anchor_component_mismatch")
        if str(row.get("source_route") or "") != "co2_zero_gas":
            reasons.append("h2o_dry_anchor_source_route_invalid")
        if _float(row.get("source_nominal_ppm")) != 0.0:
            reasons.append("h2o_dry_anchor_requires_co2_zero_gas")
        if _float(row.get("dewpoint_c")) is None:
            reasons.append("h2o_dry_anchor_dewpoint_missing")
        if pressure is None or pressure <= 0:
            reasons.append("h2o_dry_anchor_pressure_missing")
    elif component == "co2":
        nominal = _float(row.get("source_nominal_ppm"))
        expected_role = "co2_zero_gas" if nominal == 0.0 else "co2_span"
        if role != expected_role:
            reasons.append("co2_sample_role_mismatch")
    elif component == "h2o" and role != "h2o_wet":
        reasons.append("h2o_wet_sample_role_mismatch")

    return reasons, {
        "component": component,
        "device_id": str(row.get("device_id") or ""),
        "point_id": str(row.get("point_id") or ""),
        "sample_role": role,
        "identity": _row_identity(row),
        "ratio_r": ratio,
        "r0_value": r0_value,
        "pressure_kpa": pressure,
        "fit_input_value": fit_value,
        "expected_fit_input_value": expected_value,
        "status": "pass" if not reasons else "blocker",
        "reasons": reasons,
    }


def build_v1_5_historical_fit_profile_parity(
    *,
    algorithm_profile_lineage_json: str | Path,
    fit_points_csv: str | Path,
) -> dict[str, Any]:
    lineage_path = Path(algorithm_profile_lineage_json).resolve()
    points_path = Path(fit_points_csv).resolve()
    lineage = _read_json(lineage_path)
    rows = _read_csv(points_path)
    contract = lineage.get("fit_input_contract") if isinstance(lineage.get("fit_input_contract"), Mapping) else {}
    profile_id = str(contract.get("profile_id") or "")
    profile_sha256 = str(contract.get("profile_sha256") or "")
    profile_contract = PROFILE_CONTRACTS.get(profile_id, {})
    algorithm_mode = str(profile_contract.get("algorithm_mode") or "")
    r0_required = bool(profile_contract.get("r0_required"))
    expected = _expected_points(lineage)
    r0_sources = _r0_sources(lineage)
    checks: list[dict[str, Any]] = []

    lineage_reasons: list[str] = []
    source_paths = lineage.get("source_paths") if isinstance(lineage.get("source_paths"), Mapping) else {}
    queue_inputs = _read_json(Path(str(source_paths.get("queue_inputs_json") or "")))
    if lineage.get("overall_status") != "pass" or lineage.get("fit_input_allowed") is not True:
        lineage_reasons.append("algorithm_profile_lineage_not_ready")
    if profile_id not in EXPECTED_COUNTS:
        lineage_reasons.append("algorithm_profile_unknown")
    if not profile_sha256:
        lineage_reasons.append("algorithm_profile_sha256_missing")
    if str(contract.get("algorithm_mode") or "") != algorithm_mode:
        lineage_reasons.append("lineage_algorithm_mode_mismatch")
    if bool(contract.get("r0_required")) != r0_required:
        lineage_reasons.append("lineage_r0_required_mismatch")
    if str(contract.get("temperature_source") or "") != "per_analyzer_chamber_T1":
        lineage_reasons.append("lineage_temperature_source_must_be_chamber_T1")
    if contract.get("co2_zero_and_h2o_dry_anchor_are_separate") is not True:
        lineage_reasons.append("lineage_anchor_role_separation_missing")
    expected_contract = PROFILE_CONTRACTS.get(profile_id, {})
    if str(contract.get("co2_fit_input") or "") != str(expected_contract.get("co2_fit_input") or ""):
        lineage_reasons.append("lineage_co2_fit_input_mismatch")
    if str(contract.get("h2o_fit_input") or "") != str(expected_contract.get("h2o_fit_input") or ""):
        lineage_reasons.append("lineage_h2o_fit_input_mismatch")
    for key in ("opens_com_ports", "controls_water_or_gas_routes", "writes_coefficients", "connects_postgresql"):
        if lineage.get(key) is not False:
            lineage_reasons.append(f"lineage_{key}_must_be_false")
    if r0_required:
        for component, variable in (("co2", "R0_CO2(T)"), ("h2o", "R0_H2O(T)")):
            payload = r0_sources.get(component, {}).get("payload", {})
            if str(payload.get("profile_id") or "") != profile_id:
                lineage_reasons.append(f"{component}_r0_profile_mismatch")
            if str(payload.get("profile_sha256") or "") != profile_sha256:
                lineage_reasons.append(f"{component}_r0_profile_sha_mismatch")
            if str(payload.get("component") or "").lower() != component:
                lineage_reasons.append(f"{component}_r0_component_mismatch")
            if str(payload.get("model_variable") or "") != variable:
                lineage_reasons.append(f"{component}_r0_model_variable_mismatch")
            if str(payload.get("status") or "").lower() not in {"pass", "ready"}:
                lineage_reasons.append(f"{component}_r0_status_not_pass")
            if not r0_sources.get(component, {}).get("evaluations"):
                lineage_reasons.append(f"{component}_r0_evaluated_points_missing")
    if not rows:
        lineage_reasons.append("historical_fit_points_missing")
    if str(queue_inputs.get("profile_id") or "") != profile_id:
        lineage_reasons.append("queue_input_profile_id_mismatch")
    if str(queue_inputs.get("profile_sha256") or "") != profile_sha256:
        lineage_reasons.append("queue_input_profile_sha_mismatch")
    if str(queue_inputs.get("algorithm_mode") or "") != algorithm_mode:
        lineage_reasons.append("queue_input_algorithm_mode_mismatch")
    for component in ("co2", "h2o"):
        queue_path = Path(str(queue_inputs.get(f"{component}_queue_csv") or ""))
        if _sha256(queue_path) != str(queue_inputs.get(f"{component}_queue_sha256") or ""):
            lineage_reasons.append(f"{component}_queue_sha_mismatch_after_lineage_review")
    expected_counts = EXPECTED_COUNTS.get(profile_id, (0, 0))
    if len(expected["co2"]) != expected_counts[0] or len(expected["h2o"]) != expected_counts[1]:
        lineage_reasons.append("lineage_queue_point_contract_mismatch")
    checks.append(
        _check(
            "profile_and_queue_lineage",
            lineage_reasons,
            {
                "profile_id": profile_id,
                "profile_sha256": profile_sha256,
                "expected_co2_points": len(expected["co2"]),
                "expected_h2o_points": len(expected["h2o"]),
            },
        )
    )

    replay_rows: list[dict[str, Any]] = []
    for row in rows:
        reasons, replay = _validate_row(
            row,
            profile_id=profile_id,
            profile_sha256=profile_sha256,
            algorithm_mode=algorithm_mode,
            r0_required=r0_required,
            r0_sources=r0_sources,
        )
        replay_rows.append(replay)

    row_reasons = [
        f"{row.get('device_id') or '<missing>'}:{row.get('point_id') or '<missing>'}:{reason}"
        for row in replay_rows
        for reason in row["reasons"]
    ]
    checks.append(
        _check(
            "profile_specific_fit_transform",
            row_reasons,
            {
                "row_count": len(replay_rows),
                "blocked_row_count": sum(row["status"] == "blocker" for row in replay_rows),
                "fit_input_variable": "A" if r0_required else "R",
            },
        )
    )

    device_ids = sorted({str(row.get("device_id") or "") for row in rows if str(row.get("device_id") or "")})
    device_summaries: list[dict[str, Any]] = []
    coverage_reasons: list[str] = []
    for device_id in device_ids:
        device_rows = [row for row in rows if str(row.get("device_id") or "") == device_id]
        co2_route_rows = [
            row
            for row in device_rows
            if str(row.get("component") or "").lower() == "co2"
            and str(row.get("sample_role") or "") != "h2o_dry_gas_anchor"
        ]
        h2o_wet_rows = [
            row
            for row in device_rows
            if str(row.get("component") or "").lower() == "h2o"
            and str(row.get("sample_role") or "") == "h2o_wet"
        ]
        co2_observed = {
            identity
            for row in co2_route_rows
            if (identity := _co2_identity(row)) is not None
        }
        h2o_observed = {
            identity
            for row in h2o_wet_rows
            if (identity := _h2o_identity(row)) is not None
        }
        dry_rows = [row for row in device_rows if str(row.get("sample_role") or "") == "h2o_dry_gas_anchor"]
        dry_temperatures = {
            value for row in dry_rows if (value := _float(row.get("temperature_c"))) is not None
        }
        missing_co2 = sorted(expected["co2"] - co2_observed)
        extra_co2 = sorted(co2_observed - expected["co2"])
        missing_h2o = sorted(expected["h2o"] - h2o_observed)
        extra_h2o = sorted(h2o_observed - expected["h2o"])
        device_reasons: list[str] = []
        if missing_co2:
            device_reasons.append("co2_expected_points_missing")
        if extra_co2:
            device_reasons.append("co2_unexpected_points_present")
        if len(co2_route_rows) != len(expected["co2"]):
            device_reasons.append("co2_row_count_or_duplicate_mismatch")
        if missing_h2o:
            device_reasons.append("h2o_expected_wet_points_missing")
        if extra_h2o:
            device_reasons.append("h2o_unexpected_wet_points_present")
        if len(h2o_wet_rows) != len(expected["h2o"]):
            device_reasons.append("h2o_row_count_or_duplicate_mismatch")
        if len(dry_rows) < MIN_H2O_DRY_ANCHORS or len(dry_temperatures) < MIN_H2O_DRY_ANCHORS:
            device_reasons.append("h2o_dry_gas_anchor_temperature_coverage_insufficient")
        coverage_reasons.extend(f"{device_id}:{reason}" for reason in device_reasons)
        device_summaries.append(
            {
                "device_id": device_id,
                "status": "pass" if not device_reasons else "blocker",
                "co2_observed_count": len(co2_observed),
                "co2_expected_count": len(expected["co2"]),
                "h2o_wet_observed_count": len(h2o_observed),
                "h2o_wet_expected_count": len(expected["h2o"]),
                "h2o_dry_anchor_count": len(dry_rows),
                "h2o_dry_anchor_temperature_count": len(dry_temperatures),
                "missing_co2_points": missing_co2,
                "unexpected_co2_points": extra_co2,
                "missing_h2o_wet_points": missing_h2o,
                "unexpected_h2o_wet_points": extra_h2o,
                "reasons": device_reasons,
            }
        )
    if not device_ids:
        coverage_reasons.append("no_device_fit_rows")
    checks.append(
        _check(
            "per_device_point_and_temperature_coverage",
            coverage_reasons,
            {
                "device_count": len(device_ids),
                "expected_counts": {"co2": expected_counts[0], "h2o_wet": expected_counts[1]},
                "minimum_h2o_dry_anchor_temperatures": MIN_H2O_DRY_ANCHORS,
            },
        )
    )

    anchor_reasons: list[str] = []
    for row in rows:
        role = str(row.get("sample_role") or "")
        component = str(row.get("component") or "").lower()
        if role == "co2_zero_gas" and component != "co2":
            anchor_reasons.append("co2_zero_gas_role_used_outside_co2_component")
        if role == "h2o_dry_gas_anchor" and component != "h2o":
            anchor_reasons.append("h2o_dry_gas_anchor_role_used_outside_h2o_component")
    checks.append(
        _check(
            "co2_zero_and_h2o_dry_anchor_separation",
            anchor_reasons,
            {
                "co2_zero_role": "co2_zero_gas",
                "h2o_dry_role": "h2o_dry_gas_anchor",
                "h2o_dry_source_contract": "co2_zero_gas_with_dewpoint_pressure_water_evidence",
            },
        )
    )

    blocker_count = sum(check["status"] == "blocker" for check in checks)
    return {
        "schema": SCHEMA,
        "generated_at": datetime.now(UTC).replace(microsecond=0).isoformat(),
        "overall_status": "pass" if blocker_count == 0 else "blocked",
        "blocker_count": blocker_count,
        "historical_fit_replay_allowed": blocker_count == 0,
        "profile_id": profile_id,
        "profile_sha256": profile_sha256,
        "algorithm_mode": algorithm_mode,
        "fit_input_variable": "A" if r0_required else "R",
        "fitting_baseline": FIT_BASELINE,
        "allowed_route_baselines": sorted(ROUTE_BASELINES),
        "source_paths": {
            "algorithm_profile_lineage_json": str(lineage_path),
            "fit_points_csv": str(points_path),
        },
        "checks": checks,
        "device_summaries": device_summaries,
        "replay_rows": replay_rows,
        "opens_com_ports": False,
        "controls_pressure": False,
        "controls_water_or_gas_routes": False,
        "writes_coefficients": False,
        "writes_device_id": False,
        "connects_postgresql": False,
        "formal_release_allowed": False,
        "database_import_allowed": False,
        "not_real_acceptance_evidence": True,
    }


def _write_csv(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    fields: list[str] = []
    for row in rows:
        for key in row:
            if key not in fields:
                fields.append(str(key))
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow(
                {
                    key: json.dumps(value, ensure_ascii=False, sort_keys=True)
                    if isinstance(value, (dict, list, tuple))
                    else value
                    for key, value in row.items()
                }
            )


def write_v1_5_historical_fit_profile_parity(
    model: Mapping[str, Any], output_dir: str | Path
) -> dict[str, Path]:
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    paths = {
        "json": output / "v1_5_historical_fit_profile_parity.json",
        "checks": output / "v1_5_historical_fit_profile_parity_checks.csv",
        "devices": output / "v1_5_historical_fit_profile_parity_devices.csv",
        "rows": output / "v1_5_historical_fit_profile_parity_rows.csv",
        "markdown": output / "V1_5_HISTORICAL_FIT_PROFILE_PARITY.md",
    }
    paths["json"].write_text(json.dumps(model, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    _write_csv(paths["checks"], model.get("checks", []))
    _write_csv(paths["devices"], model.get("device_summaries", []))
    _write_csv(paths["rows"], model.get("replay_rows", []))
    lines = [
        "# V1.5 Historical Fit Profile Parity",
        "",
        f"- overall_status: `{model.get('overall_status')}`",
        f"- profile_id: `{model.get('profile_id')}`",
        f"- fit_input_variable: `{model.get('fit_input_variable')}`",
        f"- fitting_baseline: `{model.get('fitting_baseline')}`",
        f"- allowed_route_baselines: `{model.get('allowed_route_baselines')}`",
        f"- blocker_count: `{model.get('blocker_count')}`",
        "- CO2 zero gas and H2O dry-gas anchors remain distinct roles.",
        "- This replay is offline/no-write evidence and never authorizes release or database import.",
    ]
    paths["markdown"].write_text("\n".join(lines) + "\n", encoding="utf-8")
    return paths


__all__ = [
    "FIT_BASELINE",
    "MIN_H2O_DRY_ANCHORS",
    "ROUTE_BASELINES",
    "SCHEMA",
    "build_v1_5_historical_fit_profile_parity",
    "write_v1_5_historical_fit_profile_parity",
]
