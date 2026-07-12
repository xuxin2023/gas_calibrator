"""Normalize point-level V1.5 historical evidence for profile parity replay.

The normalizer reads immutable point artifacts only. It derives per-analyzer
means from machine-readable samples and preserves component-matched formal QC.
Missing fields remain explicit gaps; they are never inferred from displayed
concentration or repaired from another run.
"""

from __future__ import annotations

import csv
import hashlib
import json
import math
import re
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping, Sequence

from .v1_5_algorithm_profile_lineage_gate import PROFILE_CONTRACTS

SCHEMA = "v1_5_historical_fit_evidence_normalizer_v1"
ATTESTATION_SCHEMA = "v1_5_historical_route_baseline_attestation_v1"
FIT_BASELINE = "0613"
ALLOWED_ROUTE_BASELINES = {"0620", "0621"}
GA_ID_RE = re.compile(r"^(ga\d+)_id$", re.IGNORECASE)


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


def _number(value: Any) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def _truthy(value: Any) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


def _mean(values: Sequence[float]) -> float | None:
    return sum(values) / len(values) if values else None


def _same_path(left: Any, right: Any) -> bool:
    if not left or not right:
        return False
    return Path(str(left)).resolve() == Path(str(right)).resolve()


def _is_within(path: Path, root: Path) -> bool:
    try:
        path.resolve().relative_to(root.resolve())
    except (OSError, ValueError):
        return False
    return True


def _root_key(family_id: str, route_kind: str) -> str:
    return f"{family_id}:{route_kind.lower()}"


def _attestations(path: str | Path | None) -> dict[str, dict[str, Any]]:
    if not path:
        return {}
    payload = _read_json(Path(path).resolve())
    rows = payload.get("families") if isinstance(payload.get("families"), list) else []
    result: dict[str, dict[str, Any]] = {}
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        family_id = str(row.get("family_id") or "")
        if not family_id:
            continue
        route_kind = str(row.get("route_kind") or "").lower()
        normalized = {**dict(row), "_schema": payload.get("schema")}
        result[_root_key(family_id, route_kind)] = normalized
        if not route_kind:
            result[family_id] = normalized
    return result


def _route_baseline(root: Mapping[str, Any], attestation: Mapping[str, Any]) -> tuple[str, list[str]]:
    reasons: list[str] = []
    root_path = str(root.get("root_path") or "")
    if attestation:
        baseline = str(attestation.get("route_baseline") or "")
        if str(attestation.get("_schema") or "") != ATTESTATION_SCHEMA:
            reasons.append("route_baseline_attestation_schema_invalid")
        if str(attestation.get("status") or "").lower() != "reviewed":
            reasons.append("route_baseline_attestation_not_reviewed")
        if not _same_path(attestation.get("root_path"), root_path):
            reasons.append("route_baseline_attestation_root_mismatch")
        if str(attestation.get("fitting_baseline") or "") != FIT_BASELINE:
            reasons.append("route_baseline_attestation_fitting_baseline_mismatch")
        if baseline not in ALLOWED_ROUTE_BASELINES:
            reasons.append("route_baseline_attestation_value_invalid")
        if not str(attestation.get("reviewer") or "").strip():
            reasons.append("route_baseline_attestation_reviewer_missing")
        if not str(attestation.get("reviewed_at") or "").strip():
            reasons.append("route_baseline_attestation_time_missing")
        if attestation.get("not_0624_or_migration_source") is not True:
            reasons.append("route_baseline_attestation_migration_exclusion_missing")
        if str(attestation.get("mature_contract") or "") != "0613_fit_0620_0621_route":
            reasons.append("route_baseline_attestation_mature_contract_invalid")
        return (baseline if not reasons else ""), reasons
    reasons.append("route_baseline_reviewed_attestation_missing")
    return "", reasons


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
            temp = _number(row.get("temperature_c"))
            value = _number(row.get("r0_value"))
            if temp is not None and value is not None and value > 0:
                evaluations.append((temp, value))
        result[component] = {
            "path": str(path),
            "sha256": _sha256(path),
            "payload": payload,
            "evaluations": evaluations,
        }
    return result


def _r0_value(source: Mapping[str, Any], temperature_c: float | None) -> float | None:
    if temperature_c is None:
        return None
    for reviewed_temp, reviewed_value in source.get("evaluations", []):
        if math.isclose(reviewed_temp, temperature_c, rel_tol=1e-9, abs_tol=1e-9):
            return reviewed_value
    return None


def _quality_map(path: Path) -> tuple[dict[tuple[str, str], dict[str, Any]], list[str]]:
    result: dict[tuple[str, str], dict[str, Any]] = {}
    duplicates: list[str] = []
    for row in _read_csv(path):
        prefix = str(row.get("prefix") or row.get("label") or "").strip().lower()
        ratio_key = str(row.get("ratio_key") or "").strip().lower()
        component = "h2o" if "h2o" in ratio_key else "co2" if "co2" in ratio_key else ""
        if prefix and component:
            key = (prefix, component)
            if key in result:
                duplicates.append(f"{prefix}:{component}")
            else:
                result[key] = dict(row)
    return result, duplicates


def _prefixes(sample_rows: Sequence[Mapping[str, Any]]) -> list[str]:
    if not sample_rows:
        return []
    prefixes = {
        match.group(1).lower()
        for key in sample_rows[0]
        if (match := GA_ID_RE.match(str(key)))
    }
    return sorted(prefixes)


def _usable_rows(rows: Sequence[Mapping[str, Any]], prefix: str) -> list[Mapping[str, Any]]:
    key = f"{prefix}_frame_usable"
    if rows and key in rows[0]:
        return [row for row in rows if _truthy(row.get(key))]
    return list(rows)


def _values(rows: Sequence[Mapping[str, Any]], key: str) -> list[float]:
    return [value for row in rows if (value := _number(row.get(key))) is not None]


def _identity(point: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "temp_c": point.get("temp_c") if point.get("temp_c") is not None else "",
        "source_nominal_ppm": point.get("co2_ppm") if point.get("co2_ppm") is not None else "",
        "hgen_temp_c": point.get("hgen_c") if point.get("hgen_c") is not None else "",
        "hgen_rh_pct": point.get("rh_pct") if point.get("rh_pct") is not None else "",
    }


def _normalized_row(
    *,
    point: Mapping[str, Any],
    prefix: str,
    component: str,
    sample_role: str,
    route_baseline: str,
    sample_rows: Sequence[Mapping[str, Any]],
    quality: Mapping[str, Any],
    profile_id: str,
    profile_sha256: str,
    algorithm_mode: str,
    r0_required: bool,
    r0_source: Mapping[str, Any],
    sample_path: Path,
    quality_path: Path,
) -> tuple[dict[str, Any], list[str]]:
    rows = _usable_rows(sample_rows, prefix)
    reasons: list[str] = []
    ids = sorted({str(row.get(f"{prefix}_id") or "").strip() for row in rows if str(row.get(f"{prefix}_id") or "").strip()})
    if len(ids) != 1:
        reasons.append("device_id_missing_or_inconsistent")
    ratio_key = f"{prefix}_{component}_ratio_f"
    ratios = _values(rows, ratio_key)
    temperatures = _values(rows, f"{prefix}_chamber_temp_c")
    pressures = _values(rows, f"{prefix}_pressure_kpa")
    ratio = _mean(ratios)
    temperature = _mean(temperatures)
    pressure = _mean(pressures)
    if ratio is None or ratio <= 0:
        reasons.append(f"{component}_ratio_missing")
    if temperature is None:
        reasons.append("chamber_T1_missing")
    if pressure is None or pressure <= 0:
        reasons.append("analyzer_pressure_missing")
    quality_grade = str(quality.get("grade") or "missing_formal_component_quality")
    quality_eligible = _truthy(quality.get("sample_can_enter_calibration_fit"))
    if not quality:
        reasons.append(f"formal_{component}_quality_missing")
    elif not quality_eligible or not quality_grade.upper().startswith("A"):
        reasons.append(f"formal_{component}_quality_not_fit_eligible")
    expected_frames = _number(quality.get("frame_count"))
    expected_usable = _number(quality.get("usable_ratio_count"))
    if quality and expected_frames is None:
        reasons.append(f"formal_{component}_frame_count_missing")
    elif expected_frames is not None and int(expected_frames) != len(rows):
        reasons.append(f"formal_{component}_frame_count_mismatch")
    if quality and expected_usable is None:
        reasons.append(f"formal_{component}_usable_ratio_count_missing")
    elif expected_usable is not None and int(expected_usable) != len(ratios):
        reasons.append(f"formal_{component}_usable_ratio_count_mismatch")

    dewpoint = _mean(_values(rows, "dewpoint_c"))
    if sample_role == "h2o_dry_gas_anchor" and dewpoint is None:
        reasons.append("h2o_dry_anchor_dewpoint_missing")

    r0_value = ""
    r0_sha = ""
    fit_variable = "A" if r0_required else "R"
    fit_value: float | str = ratio if ratio is not None else ""
    if r0_required:
        r0_sha = str(r0_source.get("sha256") or "")
        reviewed_r0 = _r0_value(r0_source, temperature)
        if not r0_sha:
            reasons.append(f"{component}_r0_model_missing")
        if reviewed_r0 is None:
            reasons.append(f"{component}_r0_T1_evaluation_missing")
        else:
            r0_value = reviewed_r0
        if ratio is not None and ratio > 0 and reviewed_r0 is not None and pressure is not None and pressure > 0:
            fit_value = -math.log(ratio / reviewed_r0) / (pressure / 100.0)
        else:
            fit_value = ""

    identity = _identity(point)
    if sample_role == "h2o_dry_gas_anchor":
        identity = {
            "temp_c": "",
            "source_nominal_ppm": 0.0,
            "hgen_temp_c": "",
            "hgen_rh_pct": "",
        }
    fit_eligible = not reasons
    row = {
        "component": component,
        "device_id": ids[0] if len(ids) == 1 else "",
        "point_id": str(point.get("point_id") or "") + ("__h2o_dry" if sample_role == "h2o_dry_gas_anchor" else ""),
        "sample_role": sample_role,
        **identity,
        "temperature_c": temperature if temperature is not None else "",
        "pressure_kpa": pressure if pressure is not None else "",
        "ratio_r": ratio if ratio is not None else "",
        "fit_input_variable": fit_variable,
        "fit_input_value": fit_value,
        "r0_value": r0_value,
        "r0_model_sha256": r0_sha,
        "profile_id": profile_id,
        "profile_sha256": profile_sha256,
        "algorithm_mode": algorithm_mode,
        "fitting_baseline": FIT_BASELINE,
        "route_baseline": route_baseline,
        "fit_eligible": fit_eligible,
        "quality_grade": quality_grade,
        "quality_reason": str(quality.get("reason") or ""),
        "sample_count": len(rows),
        "ratio_sample_count": len(ratios),
        "temperature_sample_count": len(temperatures),
        "pressure_sample_count": len(pressures),
        "source_route": "co2_zero_gas" if sample_role == "h2o_dry_gas_anchor" else str(point.get("route_kind") or ""),
        "dewpoint_c": dewpoint if dewpoint is not None else "",
        "source_point_path": str(point.get("point_path") or ""),
        "source_samples_csv": str(sample_path),
        "source_samples_sha256": _sha256(sample_path),
        "source_quality_csv": str(quality_path) if quality_path.is_file() else "",
        "source_quality_sha256": _sha256(quality_path),
        "normalization_status": "fit_eligible" if fit_eligible else "blocked_from_fit",
        "normalization_reasons": ";".join(dict.fromkeys(reasons)),
    }
    return row, reasons


def build_v1_5_historical_fit_evidence_normalizer(
    *,
    algorithm_profile_lineage_json: str | Path,
    historical_replay_evidence_json: str | Path,
    route_baseline_attestation_json: str | Path | None = None,
) -> dict[str, Any]:
    lineage_path = Path(algorithm_profile_lineage_json).resolve()
    replay_path = Path(historical_replay_evidence_json).resolve()
    lineage = _read_json(lineage_path)
    replay = _read_json(replay_path)
    contract = lineage.get("fit_input_contract") if isinstance(lineage.get("fit_input_contract"), Mapping) else {}
    profile_id = str(contract.get("profile_id") or "")
    profile_sha256 = str(contract.get("profile_sha256") or "")
    profile_contract = PROFILE_CONTRACTS.get(profile_id, {})
    algorithm_mode = str(profile_contract.get("algorithm_mode") or "")
    r0_required = bool(profile_contract.get("r0_required"))
    attestations = _attestations(route_baseline_attestation_json)
    r0_sources = _r0_sources(lineage)
    root_by_key: dict[str, dict[str, Any]] = {}
    duplicate_root_keys: list[str] = []
    for row in replay.get("evidence_roots", []):
        if not isinstance(row, Mapping) or not str(row.get("family_id") or ""):
            continue
        key = _root_key(str(row.get("family_id") or ""), str(row.get("route_kind") or ""))
        if key in root_by_key:
            duplicate_root_keys.append(key)
        else:
            root_by_key[key] = dict(row)
    selected_root_keys = {
        key for key, root in root_by_key.items() if str(root.get("algorithm_profile_id") or "") == profile_id
    }
    selected_families = {key.split(":", 1)[0] for key in selected_root_keys}

    structural_gaps: list[dict[str, Any]] = []
    review_gaps: list[dict[str, Any]] = []
    structural_gaps.extend(
        {"scope": key, "reason": "duplicate_historical_evidence_root_key", "source": str(replay_path)}
        for key in duplicate_root_keys
    )
    if lineage.get("overall_status") != "pass" or lineage.get("fit_input_allowed") is not True:
        structural_gaps.append({"scope": "lineage", "reason": "algorithm_profile_lineage_not_ready", "source": str(lineage_path)})
    if profile_id not in PROFILE_CONTRACTS:
        structural_gaps.append({"scope": "lineage", "reason": "algorithm_profile_unknown", "source": str(lineage_path)})
    if not profile_sha256:
        structural_gaps.append({"scope": "lineage", "reason": "algorithm_profile_sha256_missing", "source": str(lineage_path)})
    expected_contract = PROFILE_CONTRACTS.get(profile_id, {})
    contract_checks = {
        "lineage_algorithm_mode_mismatch": str(contract.get("algorithm_mode") or "") == algorithm_mode,
        "lineage_co2_fit_input_mismatch": str(contract.get("co2_fit_input") or "") == str(expected_contract.get("co2_fit_input") or ""),
        "lineage_h2o_fit_input_mismatch": str(contract.get("h2o_fit_input") or "") == str(expected_contract.get("h2o_fit_input") or ""),
        "lineage_r0_required_mismatch": bool(contract.get("r0_required")) == r0_required,
        "lineage_temperature_source_must_be_chamber_T1": str(contract.get("temperature_source") or "") == "per_analyzer_chamber_T1",
        "lineage_anchor_role_separation_missing": contract.get("co2_zero_and_h2o_dry_anchor_are_separate") is True,
    }
    for reason, passed in contract_checks.items():
        if not passed:
            structural_gaps.append({"scope": "lineage", "reason": reason, "source": str(lineage_path)})
    for key in ("opens_com_ports", "controls_water_or_gas_routes", "writes_coefficients", "connects_postgresql"):
        if lineage.get(key) is not False:
            structural_gaps.append({"scope": "lineage", "reason": f"lineage_{key}_must_be_false", "source": str(lineage_path)})
    if r0_required:
        for component, variable in (("co2", "R0_CO2(T)"), ("h2o", "R0_H2O(T)")):
            source = r0_sources.get(component, {})
            payload = source.get("payload") if isinstance(source.get("payload"), Mapping) else {}
            if not source.get("sha256"):
                structural_gaps.append({"scope": "lineage", "reason": f"{component}_r0_model_file_missing", "source": str(source.get("path") or "")})
                continue
            r0_checks = {
                f"{component}_r0_profile_mismatch": str(payload.get("profile_id") or "") == profile_id,
                f"{component}_r0_profile_sha_mismatch": str(payload.get("profile_sha256") or "") == profile_sha256,
                f"{component}_r0_component_mismatch": str(payload.get("component") or "").lower() == component,
                f"{component}_r0_model_variable_mismatch": str(payload.get("model_variable") or "") == variable,
                f"{component}_r0_status_not_pass": str(payload.get("status") or "").lower() in {"pass", "ready"},
                f"{component}_r0_evaluated_points_missing": bool(source.get("evaluations")),
            }
            for reason, passed in r0_checks.items():
                if not passed:
                    structural_gaps.append({"scope": "lineage", "reason": reason, "source": str(source.get("path") or "")})
    if not selected_families:
        structural_gaps.append({"scope": "replay", "reason": "no_historical_family_for_selected_profile", "source": str(replay_path)})

    family_baselines: dict[str, str] = {}
    for key in sorted(selected_root_keys):
        root = root_by_key[key]
        family_id = str(root.get("family_id") or "")
        baseline, reasons = _route_baseline(root, attestations.get(key, attestations.get(family_id, {})))
        family_baselines[key] = baseline
        structural_gaps.extend(
            {"scope": key, "reason": reason, "source": str(root.get("root_path") or "")}
            for reason in reasons
        )

    fit_rows: list[dict[str, Any]] = []
    source_files: dict[str, dict[str, Any]] = {}
    points = [
        dict(point)
        for point in replay.get("points", [])
        if isinstance(point, Mapping) and str(point.get("family_id") or "") in selected_families
    ]
    for point in points:
        family_id = str(point.get("family_id") or "")
        route = str(point.get("route_kind") or "").lower()
        direct_key = _root_key(family_id, route)
        mixed_key = _root_key(family_id, "mixed")
        root_key = direct_key if direct_key in root_by_key else mixed_key if mixed_key in root_by_key else ""
        root = root_by_key.get(root_key, {})
        point_path = Path(str(point.get("point_path") or "")).resolve()
        family_root = Path(str(root.get("root_path") or "")).resolve()
        sample_path = point_path / "samples_machine_readable.csv"
        quality_path = point_path / "formal_open_flow_data_quality_by_analyzer.csv"
        sample_rows = _read_csv(sample_path)
        if not root_key or root_key not in selected_root_keys:
            structural_gaps.append({"scope": str(point.get("point_id") or ""), "reason": "point_route_root_not_bound_to_selected_profile", "source": str(point_path)})
            continue
        if not _is_within(point_path, family_root):
            structural_gaps.append({"scope": str(point.get("point_id") or ""), "reason": "point_path_outside_evidence_root", "source": str(point_path)})
            continue
        if not point_path.is_dir():
            structural_gaps.append({"scope": str(point.get("point_id") or ""), "reason": "point_directory_missing", "source": str(point_path)})
            continue
        if not sample_rows:
            structural_gaps.append({"scope": str(point.get("point_id") or ""), "reason": "samples_machine_readable_missing_or_empty", "source": str(sample_path)})
            continue
        source_files[str(sample_path)] = {"path": str(sample_path), "role": "samples_machine_readable", "sha256": _sha256(sample_path)}
        if quality_path.is_file():
            source_files[str(quality_path)] = {"path": str(quality_path), "role": "formal_component_quality", "sha256": _sha256(quality_path)}
        quality, duplicate_quality = _quality_map(quality_path)
        structural_gaps.extend(
            {"scope": str(point.get("point_id") or ""), "reason": f"duplicate_formal_quality_row:{item}", "source": str(quality_path)}
            for item in duplicate_quality
        )
        prefixes = _prefixes(sample_rows)
        if not prefixes:
            structural_gaps.append({"scope": str(point.get("point_id") or ""), "reason": "analyzer_prefixes_not_found", "source": str(sample_path)})
            continue
        component = "h2o" if route == "h2o" else "co2" if route == "co2" else ""
        if not component:
            structural_gaps.append({"scope": str(point.get("point_id") or ""), "reason": "route_component_unknown", "source": str(point_path)})
            continue
        for prefix in prefixes:
            role = (
                "h2o_wet"
                if component == "h2o"
                else "co2_zero_gas"
                if _number(point.get("co2_ppm")) == 0.0
                else "co2_span"
            )
            row, reasons = _normalized_row(
                point=point,
                prefix=prefix,
                component=component,
                sample_role=role,
                route_baseline=family_baselines.get(root_key, ""),
                sample_rows=sample_rows,
                quality=quality.get((prefix, component), {}),
                profile_id=profile_id,
                profile_sha256=profile_sha256,
                algorithm_mode=algorithm_mode,
                r0_required=r0_required,
                r0_source=r0_sources.get(component, {}),
                sample_path=sample_path,
                quality_path=quality_path,
            )
            fit_rows.append(row)
            review_gaps.extend(
                {"scope": row["point_id"], "device_id": row["device_id"], "reason": reason, "source": str(sample_path)}
                for reason in reasons
            )
            if component == "co2" and _number(point.get("co2_ppm")) == 0.0:
                anchor, anchor_reasons = _normalized_row(
                    point=point,
                    prefix=prefix,
                    component="h2o",
                    sample_role="h2o_dry_gas_anchor",
                    route_baseline=family_baselines.get(root_key, ""),
                    sample_rows=sample_rows,
                    quality=quality.get((prefix, "h2o"), {}),
                    profile_id=profile_id,
                    profile_sha256=profile_sha256,
                    algorithm_mode=algorithm_mode,
                    r0_required=r0_required,
                    r0_source=r0_sources.get("h2o", {}),
                    sample_path=sample_path,
                    quality_path=quality_path,
                )
                fit_rows.append(anchor)
                review_gaps.extend(
                    {"scope": anchor["point_id"], "device_id": anchor["device_id"], "reason": reason, "source": str(sample_path)}
                    for reason in anchor_reasons
                )

    structural_count = len(structural_gaps)
    review_count = len(review_gaps)
    overall_status = "blocked" if structural_count else "review_required" if review_count else "pass"
    return {
        "schema": SCHEMA,
        "generated_at": datetime.now(UTC).replace(microsecond=0).isoformat(),
        "overall_status": overall_status,
        "structural_blocker_count": structural_count,
        "fit_review_gap_count": review_count,
        "normalization_complete": structural_count == 0,
        "profile_id": profile_id,
        "profile_sha256": profile_sha256,
        "algorithm_mode": algorithm_mode,
        "fitting_baseline": FIT_BASELINE,
        "selected_families": sorted(selected_families),
        "family_route_baselines": family_baselines,
        "point_evidence_count": len(points),
        "normalized_row_count": len(fit_rows),
        "fit_eligible_row_count": sum(_truthy(row.get("fit_eligible")) for row in fit_rows),
        "blocked_fit_row_count": sum(not _truthy(row.get("fit_eligible")) for row in fit_rows),
        "source_paths": {
            "algorithm_profile_lineage_json": str(lineage_path),
            "algorithm_profile_lineage_sha256": _sha256(lineage_path),
            "historical_replay_evidence_json": str(replay_path),
            "historical_replay_evidence_sha256": _sha256(replay_path),
            "route_baseline_attestation_json": str(Path(route_baseline_attestation_json).resolve()) if route_baseline_attestation_json else "",
            "route_baseline_attestation_sha256": _sha256(Path(route_baseline_attestation_json).resolve()) if route_baseline_attestation_json else "",
        },
        "fit_points": fit_rows,
        "source_files": sorted(source_files.values(), key=lambda row: row["path"]),
        "structural_gaps": structural_gaps,
        "fit_review_gaps": review_gaps,
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


def write_v1_5_historical_fit_evidence_normalizer(
    model: Mapping[str, Any], output_dir: str | Path
) -> dict[str, Path]:
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    paths = {
        "json": output / "v1_5_historical_fit_evidence_normalizer.json",
        "fit_points": output / "v1_5_historical_fit_points.csv",
        "source_files": output / "v1_5_historical_fit_source_files.csv",
        "structural_gaps": output / "v1_5_historical_fit_structural_gaps.csv",
        "review_gaps": output / "v1_5_historical_fit_review_gaps.csv",
        "markdown": output / "V1_5_HISTORICAL_FIT_EVIDENCE_NORMALIZER.md",
    }
    manifest = {key: value for key, value in model.items() if key not in {"fit_points", "source_files", "structural_gaps", "fit_review_gaps"}}
    paths["json"].write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    _write_csv(paths["fit_points"], model.get("fit_points", []))
    _write_csv(paths["source_files"], model.get("source_files", []))
    _write_csv(paths["structural_gaps"], model.get("structural_gaps", []))
    _write_csv(paths["review_gaps"], model.get("fit_review_gaps", []))
    lines = [
        "# V1.5 Historical Fit Evidence Normalizer",
        "",
        f"- overall_status: `{model.get('overall_status')}`",
        f"- profile_id: `{model.get('profile_id')}`",
        f"- fitting_baseline: `{model.get('fitting_baseline')}`",
        f"- family_route_baselines: `{model.get('family_route_baselines')}`",
        f"- normalized_row_count: `{model.get('normalized_row_count')}`",
        f"- fit_eligible_row_count: `{model.get('fit_eligible_row_count')}`",
        f"- structural_blocker_count: `{model.get('structural_blocker_count')}`",
        f"- fit_review_gap_count: `{model.get('fit_review_gap_count')}`",
        "- Displayed concentration is never used as fit input.",
        "- CO2 zero gas and H2O dry-gas anchors are emitted as separate roles.",
        "- This is offline/no-write evidence and is not real acceptance.",
    ]
    paths["markdown"].write_text("\n".join(lines) + "\n", encoding="utf-8")
    return paths


__all__ = [
    "ALLOWED_ROUTE_BASELINES",
    "ATTESTATION_SCHEMA",
    "FIT_BASELINE",
    "SCHEMA",
    "build_v1_5_historical_fit_evidence_normalizer",
    "write_v1_5_historical_fit_evidence_normalizer",
]
