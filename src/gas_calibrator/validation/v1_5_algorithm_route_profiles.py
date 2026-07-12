"""Offline V1.5 old/new algorithm route profile review helpers.

This module turns the V1.5 algorithm profile JSON into a reviewer-facing test
point plan. It is intentionally offline: it does not open COM ports, control
routes, or write coefficients.
"""

from __future__ import annotations

import csv
import json
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Sequence


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _write_csv(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields: List[str] = []
    for row in rows:
        for key in row:
            if key not in fields:
                fields.append(str(key))
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows([dict(row) for row in rows])


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8-sig")


def _profile_by_id(config: Mapping[str, Any], profile_id: str) -> Mapping[str, Any]:
    for profile in config.get("profiles", []):
        if str(profile.get("profile_id") or "") == profile_id:
            return profile
    raise ValueError(f"Profile not found: {profile_id}")


def load_v1_5_algorithm_route_profiles(profile_path: str | Path) -> Dict[str, Any]:
    path = Path(profile_path)
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _point_key(*, component: str, temperature_c: Any, value: Any, suffix: str) -> str:
    temp = str(temperature_c).replace("-", "m").replace(".", "p")
    return f"{component}_T{temp}_{value}_{suffix}"


def _co2_formal_points_for_profile(profile: Mapping[str, Any]) -> List[Dict[str, Any]]:
    route = profile.get("co2_route", {})
    plan = route.get("temperature_plan", {})
    rows: List[Dict[str, Any]] = []
    supplemental_by_temp: Dict[float, set[float]] = {}
    policy = route.get("supplement_policy", {})
    for item in policy.get("required_new_algorithm_supplemental_gas_points", []):
        temp = float(item.get("temperature_c"))
        supplemental_by_temp.setdefault(temp, set()).add(float(item.get("co2_ppm")))

    sequence = 1
    for temp_key, values in plan.items():
        temp = float(temp_key)
        ppm_values = {float(value) for value in values or []}
        ppm_values.update(supplemental_by_temp.get(temp, set()))
        for segment_index, ppm in enumerate(sorted(ppm_values), start=1):
            is_supplement = ppm in supplemental_by_temp.get(temp, set()) and ppm not in {
                float(value) for value in values or []
            }
            rows.append(
                {
                    "profile_id": profile.get("profile_id"),
                    "algorithm_mode": profile.get("algorithm_mode"),
                    "route_kind": "co2",
                    "sequence_index": sequence,
                    "temperature_c": int(temp) if temp.is_integer() else temp,
                    "temperature_segment": f"{temp:g}C",
                    "segment_order_index": segment_index,
                    "co2_ppm": int(ppm) if ppm.is_integer() else ppm,
                    "hgen_temp": "",
                    "hgen_rh_pct": "",
                    "point_key": f"{temp:g}/{ppm:g}",
                    "point_role": (
                        "new_algorithm_required_supplemental_formal_point"
                        if is_supplement
                        else "profile_base_formal_point"
                    ),
                    "included_in_legacy_default_queue": bool(profile.get("production_default")),
                    "included_in_new_algorithm_formal_candidate": profile.get("profile_id") == "absorption_ratio_shadow",
                    "historical_missing_point_semantics": (
                        "formal_required_point_not_historical_resampling"
                        if is_supplement
                        else "base_profile_formal_point"
                    ),
                    "source_runner": route.get("runner"),
                    "do_not_modify_mature_runner": True,
                }
            )
            sequence += 1
    return rows


def _parse_hgen_token(token: str) -> tuple[float, float] | None:
    match = re.match(r"HGEN(?P<hgen>m?\d+)C_(?P<rh>\d+)RH", str(token), re.IGNORECASE)
    if not match:
        return None
    hgen_token = match.group("hgen")
    hgen = float(-int(hgen_token[1:]) if hgen_token.lower().startswith("m") else int(hgen_token))
    return hgen, float(match.group("rh"))


def _h2o_formal_points_for_profile(profile: Mapping[str, Any]) -> List[Dict[str, Any]]:
    route = profile.get("h2o_route", {})
    plan = route.get("temperature_plan") or route.get("wet_temperature_plan") or {}
    supplemental_by_temp: Dict[float, set[tuple[float, float]]] = {}
    for item in route.get("required_new_algorithm_supplemental_wet_points", []):
        hgen_text = str(item.get("humidity_generator") or "")
        hgen_match = re.match(r"HGEN(?P<hgen>m?\d+)C", hgen_text, re.IGNORECASE)
        if not hgen_match:
            continue
        hgen_token = hgen_match.group("hgen")
        hgen = float(-int(hgen_token[1:]) if hgen_token.lower().startswith("m") else int(hgen_token))
        temp = float(item.get("temperature_c"))
        supplemental_by_temp.setdefault(temp, set()).add((hgen, float(item.get("relative_humidity_pct"))))

    rows: List[Dict[str, Any]] = []
    sequence = 1
    for temp_key, values in plan.items():
        temp = float(temp_key)
        base_points = {parsed for token in values or [] if (parsed := _parse_hgen_token(str(token))) is not None}
        all_points = set(base_points)
        all_points.update(supplemental_by_temp.get(temp, set()))
        for segment_index, (hgen, rh_pct) in enumerate(sorted(all_points), start=1):
            is_supplement = (hgen, rh_pct) in supplemental_by_temp.get(temp, set()) and (hgen, rh_pct) not in base_points
            rows.append(
                {
                    "profile_id": profile.get("profile_id"),
                    "algorithm_mode": profile.get("algorithm_mode"),
                    "route_kind": "h2o",
                    "sequence_index": sequence,
                    "temperature_c": int(temp) if temp.is_integer() else temp,
                    "temperature_segment": f"{temp:g}C",
                    "segment_order_index": segment_index,
                    "co2_ppm": "",
                    "hgen_temp": f"HGEN{hgen:g}C",
                    "hgen_rh_pct": int(rh_pct) if rh_pct.is_integer() else rh_pct,
                    "point_key": f"{temp:g}/{hgen:g}/{rh_pct:g}",
                    "point_role": (
                        "new_algorithm_required_supplemental_formal_point"
                        if is_supplement
                        else "profile_base_formal_point"
                    ),
                    "included_in_legacy_default_queue": bool(profile.get("production_default")),
                    "included_in_new_algorithm_formal_candidate": profile.get("profile_id") == "absorption_ratio_shadow",
                    "historical_missing_point_semantics": (
                        "formal_required_point_not_historical_resampling"
                        if is_supplement
                        else "base_profile_formal_point"
                    ),
                    "source_runner": route.get("runner"),
                    "do_not_modify_mature_runner": True,
                }
            )
            sequence += 1
    return rows


def _status_check(
    *,
    check_id: str,
    status: str,
    expected: Any,
    observed: Any,
    reason: str,
    physical_meaning: str,
) -> Dict[str, Any]:
    return {
        "check_id": check_id,
        "status": status,
        "expected": json.dumps(expected, ensure_ascii=False, sort_keys=True)
        if isinstance(expected, (dict, list, tuple))
        else expected,
        "observed": json.dumps(observed, ensure_ascii=False, sort_keys=True)
        if isinstance(observed, (dict, list, tuple))
        else observed,
        "reason": reason,
        "physical_meaning": physical_meaning,
        "blocks_formal_point_plan": status == "blocker",
    }


def build_v1_5_algorithm_formal_point_plan_guard(profile_path: str | Path) -> Dict[str, Any]:
    """Build an offline guard for legacy and new-algorithm formal point plans."""

    config = load_v1_5_algorithm_route_profiles(profile_path)
    legacy = _profile_by_id(config, str(config.get("default_profile_id") or "legacy_ratio_production"))
    new_algorithm = _profile_by_id(config, "absorption_ratio_shadow")

    legacy_co2 = _co2_formal_points_for_profile(legacy)
    legacy_h2o = _h2o_formal_points_for_profile(legacy)
    new_co2 = _co2_formal_points_for_profile(new_algorithm)
    new_h2o = _h2o_formal_points_for_profile(new_algorithm)
    all_rows = [*legacy_co2, *legacy_h2o, *new_co2, *new_h2o]

    co2_by_temp: Dict[float, List[float]] = {}
    for row in new_co2:
        co2_by_temp.setdefault(float(row["temperature_c"]), []).append(float(row["co2_ppm"]))
    h2o_40 = [
        int(row["hgen_rh_pct"])
        for row in new_h2o
        if float(row["temperature_c"]) == 40.0 and row["hgen_temp"] == "HGEN30C"
    ]
    new_supplements = [
        row
        for row in all_rows
        if row["profile_id"] == "absorption_ratio_shadow"
        and row["point_role"] == "new_algorithm_required_supplemental_formal_point"
    ]
    checks = [
        _status_check(
            check_id="legacy_default_counts_locked",
            status="pass" if len(legacy_co2) == 45 and len(legacy_h2o) == 13 else "blocker",
            expected={"co2": 45, "h2o": 13},
            observed={"co2": len(legacy_co2), "h2o": len(legacy_h2o)},
            reason="legacy production remains the mature V1.5 45/13 route",
            physical_meaning="New-algorithm candidate points must not pollute the legacy default queue.",
        ),
        _status_check(
            check_id="new_algorithm_candidate_counts_include_required_points",
            status="pass" if len(new_co2) == 47 and len(new_h2o) == 14 else "blocker",
            expected={"co2": 47, "h2o": 14},
            observed={"co2": len(new_co2), "h2o": len(new_h2o)},
            reason="new algorithm formal candidate plan includes required supplemental points",
            physical_meaning="Future new-algorithm production runs must not silently fall back to mature 45/13 coverage.",
        ),
        _status_check(
            check_id="new_algorithm_co2_low_temperature_segments_include_600ppm",
            status=(
                "pass"
                if co2_by_temp.get(-20.0) == [0.0, 400.0, 600.0, 1000.0]
                and co2_by_temp.get(-10.0) == [0.0, 400.0, 600.0, 1000.0]
                else "blocker"
            ),
            expected={"-20C": [0, 400, 600, 1000], "-10C": [0, 400, 600, 1000]},
            observed={"-20C": co2_by_temp.get(-20.0), "-10C": co2_by_temp.get(-10.0)},
            reason="supplemental 600ppm gas points are inserted into their temperature segments",
            physical_meaning="The 600ppm points are normal gas points for the new-algorithm flow, not after-the-fact historical resampling labels.",
        ),
        _status_check(
            check_id="new_algorithm_h2o_40c_hgen30_segment_includes_30rh",
            status="pass" if h2o_40 == [30, 50, 70] else "blocker",
            expected={"40C/HGEN30C": [30, 50, 70]},
            observed={"40C/HGEN30C": h2o_40},
            reason="supplemental 30RH water point is inserted into the 40C/HGEN30C segment",
            physical_meaning="The 40C/HGEN30C/30RH point is a normal new-algorithm water point, not a historical replay resampling label.",
        ),
        _status_check(
            check_id="supplemental_points_are_formal_required_not_resampling",
            status=(
                "pass"
                if len(new_supplements) == 3
                and {row["historical_missing_point_semantics"] for row in new_supplements}
                == {"formal_required_point_not_historical_resampling"}
                else "blocker"
            ),
            expected="3 formal-required supplemental points, not resampling semantics",
            observed=[
                {
                    "route_kind": row["route_kind"],
                    "point_key": row["point_key"],
                    "semantics": row["historical_missing_point_semantics"],
                }
                for row in new_supplements
            ],
            reason="formal new-algorithm point plan separates future required points from historical missing-point audit language",
            physical_meaning="Historical data may need targeted resampling, but a future formal new-algorithm run should schedule these as normal points.",
        ),
        _status_check(
            check_id="formal_point_plan_guard_is_offline",
            status="pass",
            expected={
                "opens_com_ports": False,
                "controls_water_or_gas_routes": False,
                "writes_coefficients": False,
            },
            observed={
                "opens_com_ports": False,
                "controls_water_or_gas_routes": False,
                "writes_coefficients": False,
            },
            reason="this guard writes only JSON/CSV/Markdown artifacts",
            physical_meaning="Point-plan validation must not become hidden route control.",
        ),
    ]
    status = "blocked" if any(row["status"] == "blocker" for row in checks) else (
        "review_required" if any(row["status"] == "review_required" for row in checks) else "pass"
    )
    manifest = {
        "schema_version": 1,
        "generated_at": _now(),
        "status": status,
        "blocker_count": sum(1 for row in checks if row["status"] == "blocker"),
        "review_required_count": sum(1 for row in checks if row["status"] == "review_required"),
        "no_write": True,
        "opens_com_ports": False,
        "connects_postgresql": False,
        "controls_water_or_gas_routes": False,
        "writes_coefficients": False,
        "writes_device_id": False,
        "legacy_co2_formal_point_count": len(legacy_co2),
        "legacy_h2o_formal_point_count": len(legacy_h2o),
        "new_algorithm_co2_formal_candidate_point_count": len(new_co2),
        "new_algorithm_h2o_formal_candidate_point_count": len(new_h2o),
        "formal_point_plan_contract": "legacy=45/13;new_algorithm=47/14;new algorithm supplemental points are required formal points inside temperature segments",
    }
    return {
        "manifest": manifest,
        "formal_point_plan": all_rows,
        "checks": checks,
    }


def build_v1_5_new_algorithm_test_point_plan(
    profile_path: str | Path,
    *,
    profile_id: str = "absorption_ratio_shadow",
) -> Dict[str, Any]:
    """Build the no-write new-algorithm candidate point plan from profile JSON."""

    config = load_v1_5_algorithm_route_profiles(profile_path)
    legacy = _profile_by_id(config, str(config.get("default_profile_id") or "legacy_ratio_production"))
    profile = _profile_by_id(config, profile_id)
    co2_route = profile["co2_route"]
    h2o_route = profile["h2o_route"]
    shared = config.get("shared_route_contract", {})

    rows: List[Dict[str, Any]] = []
    co2_policy = co2_route.get("supplement_policy", {})
    conflict_by_temp_ppm = {
        (item.get("temperature_c"), item.get("co2_ppm")): item
        for item in co2_policy.get("sn01260607_fit_evidence_20260629", {}).get(
            "excluded_conflict_points", []
        )
    }

    for item in co2_policy.get("required_new_algorithm_supplemental_gas_points", []):
        rows.append(
            {
                "plan_id": _point_key(
                    component="co2",
                    temperature_c=item.get("temperature_c"),
                    value=f"{item.get('co2_ppm')}ppm",
                    suffix="supplement",
                ),
                "component": "co2",
                "route_component": "co2",
                "point_role": "new_algorithm_supplemental_gas_point",
                "physical_process": "gas_point",
                "temperature_c": item.get("temperature_c"),
                "co2_ppm": item.get("co2_ppm"),
                "hgen_temp": "",
                "hgen_rh_pct": "",
                "fit_role": item.get("fit_role"),
                "included_in_legacy_default_queue": False,
                "included_in_new_algorithm_candidate": True,
                "counts_as_new_physical_point": True,
                "applies_to_all_new_algorithm_devices": True,
                "diagnostic_source_device_sn": "",
                "diagnostic_source_device_id": "",
                "source_runner": co2_route.get("runner"),
                "do_not_modify_mature_runner": True,
                "release_gate_role": "required_before_new_algorithm_release",
                "notes": "Supplemental low-temperature curvature constraint; run as a normal gas point.",
            }
        )

    source_evidence = co2_policy.get("sn01260607_fit_evidence_20260629", {})
    for item in co2_policy.get("sn01260607_observed_conflict_gas_points_for_diagnostic_recheck", []):
        conflict = conflict_by_temp_ppm.get((item.get("temperature_c"), item.get("co2_ppm")), {})
        rows.append(
            {
                "plan_id": _point_key(
                    component="co2",
                    temperature_c=item.get("temperature_c"),
                    value=f"{item.get('co2_ppm')}ppm",
                    suffix="recheck",
                ),
                "component": "co2",
                "route_component": "co2",
                "point_role": "sn01260607_observed_conflict_gas_point",
                "physical_process": "gas_point",
                "temperature_c": item.get("temperature_c"),
                "co2_ppm": item.get("co2_ppm"),
                "hgen_temp": "",
                "hgen_rh_pct": "",
                "fit_role": "device_specific_diagnostic_recheck_if_present",
                "included_in_legacy_default_queue": True,
                "included_in_new_algorithm_candidate": True,
                "counts_as_new_physical_point": False,
                "applies_to_all_new_algorithm_devices": False,
                "diagnostic_source_device_sn": source_evidence.get("device_sn", "01260607"),
                "diagnostic_source_device_id": source_evidence.get("device_id", "001"),
                "source_runner": co2_route.get("runner"),
                "do_not_modify_mature_runner": True,
                "release_gate_role": "device_specific_diagnostic_only_not_generic_release_gate",
                "notes": (
                    "Observed conflict for SN01260607 only; other new-algorithm devices run the "
                    "full candidate point set and generate their own diagnostic recheck points "
                    f"from residual review. Reason: {conflict.get('reason') or 'observed conflict'}."
                ),
            }
        )

    for item in h2o_route.get("required_new_algorithm_supplemental_wet_points", []):
        rows.append(
            {
                "plan_id": _point_key(
                    component="h2o",
                    temperature_c=item.get("temperature_c"),
                    value=f"{item.get('humidity_generator')}_{item.get('relative_humidity_pct')}RH",
                    suffix="supplement",
                ),
                "component": "h2o",
                "route_component": "h2o",
                "point_role": "new_algorithm_supplemental_wet_point",
                "physical_process": "water_point",
                "temperature_c": item.get("temperature_c"),
                "co2_ppm": "",
                "hgen_temp": item.get("humidity_generator"),
                "hgen_rh_pct": item.get("relative_humidity_pct"),
                "fit_role": item.get("fit_role"),
                "included_in_legacy_default_queue": False,
                "included_in_new_algorithm_candidate": True,
                "counts_as_new_physical_point": True,
                "applies_to_all_new_algorithm_devices": True,
                "diagnostic_source_device_sn": "",
                "diagnostic_source_device_id": "",
                "source_runner": h2o_route.get("runner"),
                "do_not_modify_mature_runner": True,
                "release_gate_role": "required_before_new_algorithm_release",
                "notes": "Supplemental high-temperature mid-water shape constraint.",
            }
        )

    h2o_supplement_policy = h2o_route.get("supplement_policy", {})
    h2o_source_evidence = h2o_route.get("sn01260607_fit_evidence_20260629", {})
    for item in h2o_supplement_policy.get(
        "sn01260607_observed_high_residual_wet_points_for_diagnostic_recheck", []
    ):
        rows.append(
            {
                "plan_id": _point_key(
                    component="h2o",
                    temperature_c=item.get("temperature_c"),
                    value=f"{item.get('humidity_generator')}_{item.get('relative_humidity_pct')}RH",
                    suffix="recheck",
                ),
                "component": "h2o",
                "route_component": "h2o",
                "point_role": "sn01260607_observed_high_residual_wet_point",
                "physical_process": "water_point",
                "temperature_c": item.get("temperature_c"),
                "co2_ppm": "",
                "hgen_temp": item.get("humidity_generator"),
                "hgen_rh_pct": item.get("relative_humidity_pct"),
                "fit_role": "device_specific_diagnostic_recheck_if_present",
                "included_in_legacy_default_queue": True,
                "included_in_new_algorithm_candidate": True,
                "counts_as_new_physical_point": False,
                "applies_to_all_new_algorithm_devices": False,
                "diagnostic_source_device_sn": h2o_source_evidence.get("device_sn", "01260607"),
                "diagnostic_source_device_id": h2o_source_evidence.get("device_id", "001"),
                "source_runner": h2o_route.get("runner"),
                "do_not_modify_mature_runner": True,
                "release_gate_role": "device_specific_diagnostic_only_not_generic_release_gate",
                "notes": (
                    "Observed high residual for SN01260607 only; other new-algorithm devices run "
                    "the full candidate point set and generate their own diagnostic recheck points "
                    f"from residual review. Reason: {item.get('reason') or 'observed high residual'}."
                ),
            }
        )

    for item in h2o_route.get("dry_anchor_policy", {}).get("recommended_future_supplement", []):
        rows.append(
            {
                "plan_id": _point_key(
                    component="h2o",
                    temperature_c=item.get("temperature_c"),
                    value="0ppm_CO2_zero",
                    suffix="low_anchor",
                ),
                "component": "h2o",
                "route_component": "co2",
                "point_role": "h2o_low_water_anchor_from_co2_zero_gas",
                "physical_process": "gas_point_zero_co2_low_water_anchor",
                "temperature_c": item.get("temperature_c"),
                "co2_ppm": 0,
                "hgen_temp": "",
                "hgen_rh_pct": "",
                "fit_role": item.get("purpose"),
                "included_in_legacy_default_queue": True,
                "included_in_new_algorithm_candidate": True,
                "counts_as_new_physical_point": False,
                "applies_to_all_new_algorithm_devices": True,
                "diagnostic_source_device_sn": "",
                "diagnostic_source_device_id": "",
                "source_runner": co2_route.get("runner"),
                "do_not_modify_mature_runner": True,
                "release_gate_role": "required_low_water_anchor_evidence",
                "notes": (
                    "Reuse CO2 zero-gas physical evidence as H2O low-water anchor only when "
                    "R_H2O, dewpoint, pressure, and chamber T1 are traceable; residual water is "
                    "not forced to zero."
                ),
            }
        )

    manifest = {
        "schema_version": 1,
        "generated_at": _now(),
        "profile_id": profile_id,
        "no_write": True,
        "opens_com_ports": False,
        "controls_water_or_gas_routes": False,
        "writes_coefficients": False,
        "legacy_default_profile_id": config.get("default_profile_id"),
        "legacy_co2_formal_point_count": legacy.get("co2_route", {}).get("formal_point_count"),
        "legacy_h2o_formal_point_count": legacy.get("h2o_route", {}).get("formal_point_count"),
        "new_algorithm_co2_candidate_point_count": co2_route.get(
            "production_candidate_point_count_with_supplements"
        ),
        "new_algorithm_h2o_candidate_wet_point_count": h2o_route.get(
            "production_candidate_wet_point_count_with_supplements"
        ),
        "route_behavior": shared.get("route_behavior"),
        "co2_runner": shared.get("co2_runner"),
        "h2o_runner": shared.get("h2o_runner"),
        "physical_contract": (
            "New algorithm differences stay in R-to-A conversion, R0 anchors, supplemental "
            "candidate points, release rechecks, and SENCO write contracts; mature V1.5 runners "
            "and legacy default queues are not modified."
        ),
    }
    role_counts: Dict[str, int] = {}
    for row in rows:
        role = str(row.get("point_role") or "")
        role_counts[role] = role_counts.get(role, 0) + 1

    return {
        "manifest": manifest,
        "point_plan": rows,
        "role_counts": [
            {"point_role": role, "count": count} for role, count in sorted(role_counts.items())
        ],
    }


def _join_values(values: Any) -> str:
    if isinstance(values, (list, tuple)):
        return ";".join(str(item) for item in values)
    return str(values or "")


def _co2_write_contract_row(profile: Mapping[str, Any]) -> Dict[str, Any]:
    co2_route = profile.get("co2_route", {})
    contract = co2_route.get("write_contract", {})
    trim = contract.get("final_linear_trim", {})
    return {
        "profile_id": profile.get("profile_id"),
        "algorithm_mode": profile.get("algorithm_mode"),
        "algorithm_contract": contract.get("algorithm_contract"),
        "firmware_slot_contract": contract.get("firmware_slot_contract", "legacy ratio-temperature contract"),
        "fit_input": contract.get("fit_input", profile.get("fit_input", {}).get("co2", "")),
        "temperature_feature": contract.get("temperature_feature", ""),
        "main_chain_coefficients": _join_values(contract.get("main_chain_coefficients")),
        "main_chain_controlled_writer": contract.get("main_chain_controlled_writer"),
        "main_chain_review_artifact": contract.get("main_chain_review_artifact"),
        "required_review_checks": _join_values(contract.get("required_review_checks")),
        "candidate_write_pack_evidence": contract.get("candidate_write_pack_evidence", ""),
        "final_linear_trim_coefficient": trim.get("coefficient"),
        "senco5_separate_final_affine_layer": bool(trim.get("separate_final_affine_layer")),
        "senco5_must_not_fold_into_main_chain": bool(trim.get("must_not_fold_into_main_chain")),
        "senco5_controlled_writer": trim.get("controlled_writer"),
        "senco5_neutral_writer": trim.get("neutral_writer"),
        "senco5_clear_command_required_for_neutralization": trim.get(
            "clear_command_required_for_neutralization"
        ),
        "senco5_payload_format": trim.get("payload_format"),
        "senco5_review_after_main_chain_reverification": bool(
            trim.get("review_after_main_chain_reverification", False)
        ),
    }


def _h2o_write_contract_row(profile: Mapping[str, Any]) -> Dict[str, Any]:
    h2o_route = profile.get("h2o_route", {})
    contract = h2o_route.get("write_contract", {})
    trim = contract.get("final_linear_trim", {})
    alternate = contract.get("alternate_absorption_slot_contract", {})
    return {
        "profile_id": profile.get("profile_id"),
        "algorithm_mode": profile.get("algorithm_mode"),
        "algorithm_contract": contract.get("algorithm_contract"),
        "contract_status": contract.get("status", "review_ready_after_existing_v1_5_controls"),
        "firmware_slot_contract": contract.get("firmware_slot_contract"),
        "fit_input": contract.get("fit_input", profile.get("fit_input", {}).get("h2o", "")),
        "main_chain_coefficients": _join_values(contract.get("main_chain_coefficients")),
        "main_chain_controlled_writer": contract.get("main_chain_controlled_writer"),
        "main_chain_cli_algorithm_flag": contract.get("main_chain_cli_algorithm_flag"),
        "main_chain_review_artifact": contract.get("main_chain_review_artifact"),
        "required_review_checks": _join_values(contract.get("required_review_checks")),
        "candidate_write_pack_evidence": contract.get("candidate_write_pack_evidence", ""),
        "final_linear_trim_coefficient": trim.get("coefficient"),
        "senco6_separate_final_affine_layer": bool(trim.get("separate_final_affine_layer")),
        "senco6_must_not_fold_into_main_chain": bool(trim.get("must_not_fold_into_main_chain")),
        "senco6_controlled_writer": trim.get("controlled_writer"),
        "senco6_neutral_writer": trim.get("neutral_writer"),
        "senco6_clear_command_required_for_neutralization": trim.get(
            "clear_command_required_for_neutralization"
        ),
        "senco6_payload_format": trim.get("payload_format"),
        "senco6_review_after_main_chain_reverification": bool(
            trim.get("review_after_main_chain_reverification", False)
        ),
        "alternate_absorption_contract": alternate.get("algorithm_contract", ""),
        "alternate_absorption_status": alternate.get("status", ""),
        "alternate_absorption_cli_flag": alternate.get("controlled_writer_algorithm_flag", ""),
        "alternate_absorption_blocker": alternate.get("blocker", ""),
    }


def _r0_write_contract_rows(profile: Mapping[str, Any]) -> List[Dict[str, Any]]:
    contract = profile.get("r0_write_contract", {})
    rows: List[Dict[str, Any]] = []
    for item in contract.get("components", []):
        rows.append(
            {
                "profile_id": profile.get("profile_id"),
                "algorithm_mode": profile.get("algorithm_mode"),
                "component": item.get("component"),
                "coefficient_group": item.get("coefficient_group"),
                "readback_group": item.get("readback_group"),
                "physical_quantity": item.get("physical_quantity"),
                "r0_source": item.get("r0_source"),
                "fit_input_equation": item.get("fit_input_equation"),
                "payload_contract": item.get("payload_contract"),
                "controlled_writer": item.get("controlled_writer", ""),
                "controlled_writer_status": item.get("controlled_writer_status", ""),
                "readback_required": bool(item.get("readback_required", False)),
                "production_blocker": bool(item.get("production_blocker", False)),
                "profile_r0_contract_status": contract.get("status", ""),
                "write_requires": _join_values(contract.get("write_requires")),
                "notes": item.get("notes", ""),
            }
        )
    return rows


def build_v1_5_algorithm_write_contract_tables(profile_path: str | Path) -> Dict[str, Any]:
    """Build offline write-contract tables from V1.5 algorithm profiles."""

    config = load_v1_5_algorithm_route_profiles(profile_path)
    profiles = list(config.get("profiles", []))
    co2_rows = [_co2_write_contract_row(profile) for profile in profiles]
    h2o_rows = [_h2o_write_contract_row(profile) for profile in profiles]
    r0_rows: List[Dict[str, Any]] = []
    for profile in profiles:
        r0_rows.extend(_r0_write_contract_rows(profile))
    manifest = {
        "schema_version": 1,
        "generated_at": _now(),
        "no_write": True,
        "opens_com_ports": False,
        "controls_water_or_gas_routes": False,
        "writes_coefficients": False,
        "default_profile_id": config.get("default_profile_id"),
        "contract_scope": "co2_main_chain_h2o_main_chain_final_linear_trims_and_r0_blockers",
        "physical_contract": (
            "CO2 main-chain writes use reviewed SENCO1/SENCO3 payloads and H2O main-chain writes "
            "use reviewed SENCO2/SENCO4 payloads. SENCO5/SENCO6 remain separate final affine "
            "layers. New algorithm R0(T) dependencies require SENCOA/SENCOB writer/readback "
            "contracts before production completion."
        ),
        "h2o_new_algorithm_status": "blocked_until_firmware_input_scale_and_R0_write_contract_are_closed",
        "r0_contract_status": "blocked_until_controlled_sencoa_sencob_writer_exists",
    }
    return {
        "manifest": manifest,
        "co2_write_contracts": co2_rows,
        "h2o_write_contracts": h2o_rows,
        "r0_write_contracts": r0_rows,
    }


def write_v1_5_algorithm_write_contract_review(
    profile_path: str | Path,
    output_dir: str | Path,
) -> Dict[str, str]:
    tables = build_v1_5_algorithm_write_contract_tables(profile_path)
    out = Path(output_dir)
    outputs = {
        "manifest": out / "v1_5_algorithm_write_contract_manifest.json",
        "co2_write_contracts": out / "v1_5_co2_algorithm_write_contracts.csv",
        "h2o_write_contracts": out / "v1_5_h2o_algorithm_write_contracts.csv",
        "r0_write_contracts": out / "v1_5_r0_algorithm_write_contracts.csv",
        "summary": out / "V1_5_ALGORITHM_WRITE_CONTRACT_REVIEW.md",
    }
    _write_json(outputs["manifest"], tables["manifest"])
    _write_csv(outputs["co2_write_contracts"], tables["co2_write_contracts"])
    _write_csv(outputs["h2o_write_contracts"], tables["h2o_write_contracts"])
    _write_csv(outputs["r0_write_contracts"], tables["r0_write_contracts"])
    summary = [
        "# V1.5 algorithm write contract review",
        "",
        "This is an offline no-write review generated from the V1.5 algorithm route profile.",
        "",
        "- CO2 old and new algorithms both preserve the mature route runners.",
        "- CO2 main-chain payloads are reviewed as SENCO1/SENCO3 paired writes.",
        "- New algorithm uses absorption `A=-ln(R/R0(T))/(P_kPa/100)` inside the old seven slots.",
        "- SENCO5 is a separate final affine layer and must not be folded into SENCO1/SENCO3.",
        "- SENCO5 neutralization requires `CLEARSENCO5,YGAS,FFF`.",
        "- H2O main-chain payloads are reviewed as SENCO2/SENCO4 paired writes.",
        "- SENCO6 is a separate final affine layer and must not be folded into SENCO2/SENCO4.",
        "- SENCO6 neutralization requires `CLEARSENCO6,YGAS,FFF`.",
        "- New algorithm R0(T) depends on SENCOA/SENCOB, but controlled writer/readback contracts are still blockers.",
    ]
    outputs["summary"].write_text("\n".join(summary) + "\n", encoding="utf-8")
    return {key: str(path) for key, path in outputs.items()}


def write_v1_5_algorithm_formal_point_plan_guard(
    profile_path: str | Path,
    output_dir: str | Path,
) -> Dict[str, str]:
    tables = build_v1_5_algorithm_formal_point_plan_guard(profile_path)
    out = Path(output_dir)
    outputs = {
        "manifest": out / "v1_5_algorithm_formal_point_plan_guard_manifest.json",
        "formal_point_plan": out / "v1_5_algorithm_formal_point_plan.csv",
        "checks": out / "v1_5_algorithm_formal_point_plan_guard_checks.csv",
        "summary": out / "V1_5_ALGORITHM_FORMAL_POINT_PLAN_GUARD.md",
    }
    _write_json(outputs["manifest"], tables["manifest"])
    _write_csv(outputs["formal_point_plan"], tables["formal_point_plan"])
    _write_csv(outputs["checks"], tables["checks"])
    summary = [
        "# V1.5 algorithm formal point plan guard",
        "",
        "This is an offline no-write guard generated from the V1.5 algorithm route profile.",
        "",
        f"- status: `{tables['manifest']['status']}`",
        f"- blocker_count: `{tables['manifest']['blocker_count']}`",
        f"- legacy CO2/H2O counts: `{tables['manifest']['legacy_co2_formal_point_count']}` / `{tables['manifest']['legacy_h2o_formal_point_count']}`",
        f"- new-algorithm CO2/H2O counts: `{tables['manifest']['new_algorithm_co2_formal_candidate_point_count']}` / `{tables['manifest']['new_algorithm_h2o_formal_candidate_point_count']}`",
        "- New-algorithm `-20C` and `-10C` CO2 segments include `0/400/600/1000ppm`.",
        "- New-algorithm `40C/HGEN30C` H2O segment includes `30/50/70RH`.",
        "- Supplemental points are required formal new-algorithm points, not historical resampling labels.",
        "- Mature V1.5 CO2/H2O runners are not modified by this guard.",
    ]
    outputs["summary"].write_text("\n".join(summary) + "\n", encoding="utf-8")
    return {key: str(path) for key, path in outputs.items()}


def _format_num(value: Any) -> str:
    try:
        numeric = float(value)
    except Exception:
        return str(value or "")
    if numeric.is_integer():
        return str(int(numeric))
    return f"{numeric:g}"


def _co2_group_for_preview(temp_c: Any, ppm: Any) -> str:
    temp = float(temp_c)
    concentration = float(ppm)
    # Mirror the mature 0620 queue source binding: sparse low/high-temperature
    # segments use group A, while dense 10/20/30C segments alternate by 100 ppm
    # step with odd hundreds on group B.
    if temp in (10.0, 20.0, 30.0) and int(concentration) % 200 == 100:
        return "B"
    return "A"


def _legacy_pressure_exclusions_for_co2_preview(temp_c: Any) -> str:
    temp = float(temp_c)
    if temp in (-20.0, -10.0):
        return "1100,800,550"
    if temp == 0.0:
        return "1100,800,500"
    if temp in (10.0, 20.0, 30.0):
        return "1100,1000,900,800,700,600,550"
    if temp == 40.0:
        return "1100,900,550"
    return ""


def _legacy_pressure_exclusions_for_h2o_preview(rh_pct: Any) -> str:
    rh = float(rh_pct)
    if rh <= 30.0:
        return "1100"
    if rh <= 50.0:
        return "800"
    return "500"


def _co2_formal_runlist_row(row: Mapping[str, Any], *, route_index: int) -> Dict[str, Any]:
    temp = _format_num(row["temperature_c"])
    ppm = _format_num(row["co2_ppm"])
    co2_group = _co2_group_for_preview(row["temperature_c"], row["co2_ppm"])
    runner_args = (
        f"--temp {temp} --co2-source-ppm {ppm} --co2-group {co2_group} "
        "--purge-s 360 --sample-count 10 --analyzer-acquisition active_stream_1hz"
    )
    return {
        "point_id": f"co2_T{temp}_{ppm}ppm_ambient",
        "component": "co2",
        "temp_c": float(row["temperature_c"]),
        "source_nominal_ppm": float(row["co2_ppm"]),
        "co2_group": co2_group,
        "sample_role": "fit",
        "fit_eligible": True,
        "verification_eligible": False,
        "zero_gas_required": abs(float(row["co2_ppm"])) <= 1e-9,
        "standard_role": "zero_air" if abs(float(row["co2_ppm"])) <= 1e-9 else "co2_standard_gas",
        "certificate_required": True,
        "pressure_mode": "ambient_open",
        "target_pressure_hpa": "",
        "pressure_reference_required": True,
        "pressure_channel_precheck_required": True,
        "legacy_pressure_targets_excluded_hpa": _legacy_pressure_exclusions_for_co2_preview(
            row["temperature_c"]
        ),
        "purge_s": 360.0,
        "sample_count": 10,
        "analyzer_acquisition": "active_stream_1hz",
        "runner": "run_v1_5_formal_open_flow_sampling",
        "runner_args": runner_args,
        "physical_meaning": (
            "Open-flow CO2 formal runlist preview: standard gas continuously refreshes "
            "the analyzer cells; pressure is evidence after independent pressure-channel "
            "verification, not a sealed pressure fitting target."
        ),
        "profile_id": row["profile_id"],
        "algorithm_mode": row["algorithm_mode"],
        "route_index": route_index,
        "temperature_segment": row["temperature_segment"],
        "segment_order_index": row["segment_order_index"],
        "point_role": row["point_role"],
        "historical_missing_point_semantics": row["historical_missing_point_semantics"],
        "source_point_key": row["point_key"],
        "do_not_modify_mature_runner": True,
        "runner_integration_status": "preview_only_not_runner_wired",
    }


def _hgen_temp_number(hgen_temp: Any) -> float:
    match = re.match(r"HGEN(?P<hgen>m?\d+(?:\.\d+)?)C", str(hgen_temp or ""), re.IGNORECASE)
    if not match:
        raise ValueError(f"Invalid HGEN temperature: {hgen_temp}")
    hgen_token = match.group("hgen")
    if hgen_token.lower().startswith("m"):
        return -float(hgen_token[1:])
    return float(hgen_token)


def _h2o_formal_runlist_row(row: Mapping[str, Any], *, route_index: int) -> Dict[str, Any]:
    temp = _format_num(row["temperature_c"])
    hgen = _format_num(_hgen_temp_number(row["hgen_temp"]))
    rh = _format_num(row["hgen_rh_pct"])
    runner_args = (
        f"--temp {temp} --hgen-temp {hgen} --hgen-rh {rh} "
        "--purge-s 720 --sample-count 10 --analyzer-acquisition active_stream_1hz "
        "--h2o-pressure-presample-policy skip"
    )
    return {
        "point_id": f"h2o_T{temp}_HGEN{hgen}C_{rh}RH_ambient",
        "component": "h2o",
        "temp_c": float(row["temperature_c"]),
        "hgen_temp_c": float(hgen),
        "hgen_rh_pct": float(row["hgen_rh_pct"]),
        "reference_dewpoint_c": "",
        "reference_h2o_mmol": "",
        "reference_bridge_status": "requires_humidity_reference_bridge_before_fit_or_release",
        "sample_role": "fit",
        "fit_eligible": True,
        "verification_eligible": False,
        "standard_role": "humidity_generator_dewpoint_reference",
        "certificate_required": True,
        "pressure_mode": "ambient_open",
        "target_pressure_hpa": "",
        "pressure_reference_required": True,
        "pressure_channel_precheck_required": True,
        "legacy_pressure_targets_excluded_hpa": _legacy_pressure_exclusions_for_h2o_preview(
            row["hgen_rh_pct"]
        ),
        "purge_s": 720.0,
        "sample_count": 10,
        "analyzer_acquisition": "active_stream_1hz",
        "runner": "run_v1_5_formal_h2o_open_flow_sampling",
        "runner_args": runner_args,
        "physical_meaning": (
            "Open-flow H2O formal runlist preview: humidified gas continuously "
            "refreshes the chain and dewpoint/reference water is the fit quantity; "
            "pressure is evidence for compensation, not a sealed target."
        ),
        "profile_id": row["profile_id"],
        "algorithm_mode": row["algorithm_mode"],
        "route_index": route_index,
        "temperature_segment": row["temperature_segment"],
        "segment_order_index": row["segment_order_index"],
        "point_role": row["point_role"],
        "historical_missing_point_semantics": row["historical_missing_point_semantics"],
        "source_point_key": row["point_key"],
        "do_not_modify_mature_runner": True,
        "runner_integration_status": "preview_only_not_runner_wired",
    }


def build_v1_5_profile_queue_rows(
    profile_path: str | Path,
    *,
    profile_id: str,
) -> Dict[str, Any]:
    """Build queue-compatible rows for one reviewed algorithm profile.

    This function only materializes point metadata. Both profiles continue to
    use the mature CO2/H2O queue modules for every physical point action.
    """

    tables = build_v1_5_algorithm_formal_point_plan_guard(profile_path)
    point_plan = [
        row
        for row in tables["formal_point_plan"]
        if str(row.get("profile_id") or "") == profile_id
    ]
    if not point_plan:
        raise ValueError(f"Profile not found or has no formal points: {profile_id}")
    co2_source = [row for row in point_plan if row["route_kind"] == "co2"]
    h2o_source = [row for row in point_plan if row["route_kind"] == "h2o"]
    co2_rows = [
        _co2_formal_runlist_row(row, route_index=index)
        for index, row in enumerate(co2_source, start=1)
    ]
    h2o_rows = [
        _h2o_formal_runlist_row(row, route_index=index)
        for index, row in enumerate(h2o_source, start=1)
    ]
    return {
        "profile_id": profile_id,
        "algorithm_mode": str(point_plan[0].get("algorithm_mode") or ""),
        "point_plan_guard_status": tables["manifest"]["status"],
        "point_plan_guard_blocker_count": tables["manifest"]["blocker_count"],
        "source_runners": sorted(
            {str(row.get("source_runner") or "") for row in point_plan}
        ),
        "co2_rows": co2_rows,
        "h2o_rows": h2o_rows,
    }


def build_v1_5_algorithm_formal_runlist_preview(
    profile_path: str | Path,
    *,
    profile_id: str = "absorption_ratio_shadow",
) -> Dict[str, Any]:
    """Build offline queue-compatible runlist previews for a profile.

    The output is deliberately not wired to the live queue runners. It proves
    the formal point order and CSV shape before any future runner integration.
    """

    tables = build_v1_5_algorithm_formal_point_plan_guard(profile_path)
    queue_rows = build_v1_5_profile_queue_rows(profile_path, profile_id=profile_id)
    co2_runlist = queue_rows["co2_rows"]
    h2o_runlist = queue_rows["h2o_rows"]
    supplemental_keys = {
        row["source_point_key"]
        for row in [*co2_runlist, *h2o_runlist]
        if row["point_role"] == "new_algorithm_required_supplemental_formal_point"
    }
    legacy_co2_count = tables["manifest"]["legacy_co2_formal_point_count"]
    legacy_h2o_count = tables["manifest"]["legacy_h2o_formal_point_count"]
    checks = [
        _status_check(
            check_id="legacy_default_remains_45_13",
            status="pass" if legacy_co2_count == 45 and legacy_h2o_count == 13 else "blocker",
            expected={"co2": 45, "h2o": 13},
            observed={"co2": legacy_co2_count, "h2o": legacy_h2o_count},
            reason="runlist preview must not change legacy production coverage",
            physical_meaning="Legacy production remains the mature ratio route.",
        ),
        _status_check(
            check_id="new_algorithm_runlist_counts_are_47_14",
            status="pass" if len(co2_runlist) == 47 and len(h2o_runlist) == 14 else "blocker",
            expected={"co2": 47, "h2o": 14},
            observed={"co2": len(co2_runlist), "h2o": len(h2o_runlist)},
            reason="new algorithm runlist preview includes formal supplemental points",
            physical_meaning="Future new-algorithm physical flow must schedule the 600ppm and 30RH points with their temperature segments.",
        ),
        _status_check(
            check_id="supplemental_points_remain_formal_required",
            status=(
                "pass"
                if supplemental_keys == {"-20/600", "-10/600", "40/30/30"}
                else "blocker"
            ),
            expected=["-20/600", "-10/600", "40/30/30"],
            observed=sorted(supplemental_keys),
            reason="formal runlist preview keeps supplemental points as normal scheduled points",
            physical_meaning="These are not historical missing-point audit or targeted-resampling labels.",
        ),
        _status_check(
            check_id="runlist_preview_is_offline",
            status="pass",
            expected={
                "opens_com_ports": False,
                "controls_water_or_gas_routes": False,
                "writes_coefficients": False,
                "runner_wired": False,
            },
            observed={
                "opens_com_ports": False,
                "controls_water_or_gas_routes": False,
                "writes_coefficients": False,
                "runner_wired": False,
            },
            reason="this preview writes only review artifacts",
            physical_meaning="Runlist generation must not become hidden route execution.",
        ),
    ]
    status = "blocked" if any(row["status"] == "blocker" for row in checks) else (
        "review_required" if any(row["status"] == "review_required" for row in checks) else "pass"
    )
    manifest = {
        "schema_version": 1,
        "generated_at": _now(),
        "profile_id": profile_id,
        "status": status,
        "blocker_count": sum(1 for row in checks if row["status"] == "blocker"),
        "review_required_count": sum(1 for row in checks if row["status"] == "review_required"),
        "no_write": True,
        "opens_com_ports": False,
        "connects_postgresql": False,
        "controls_water_or_gas_routes": False,
        "writes_coefficients": False,
        "writes_device_id": False,
        "runner_integration_status": "preview_only_not_runner_wired",
        "queue_csv_schema": "v1_5_formal_queue_compatible_preview",
        "legacy_co2_formal_point_count": legacy_co2_count,
        "legacy_h2o_formal_point_count": legacy_h2o_count,
        "co2_runlist_count": len(co2_runlist),
        "h2o_runlist_count": len(h2o_runlist),
        "formal_runlist_contract": "legacy=45/13;new_algorithm_runlist_preview=47/14;runner_not_modified",
    }
    return {
        "manifest": manifest,
        "co2_runlist": co2_runlist,
        "h2o_runlist": h2o_runlist,
        "checks": checks,
    }


def write_v1_5_algorithm_formal_runlist_preview(
    profile_path: str | Path,
    output_dir: str | Path,
    *,
    profile_id: str = "absorption_ratio_shadow",
) -> Dict[str, str]:
    tables = build_v1_5_algorithm_formal_runlist_preview(profile_path, profile_id=profile_id)
    out = Path(output_dir)
    outputs = {
        "manifest": out / "v1_5_algorithm_formal_runlist_preview_manifest.json",
        "co2_runlist": out / "v1_5_new_algorithm_formal_co2_runlist_preview.csv",
        "h2o_runlist": out / "v1_5_new_algorithm_formal_h2o_runlist_preview.csv",
        "checks": out / "v1_5_algorithm_formal_runlist_preview_checks.csv",
        "summary": out / "V1_5_ALGORITHM_FORMAL_RUNLIST_PREVIEW.md",
    }
    _write_json(outputs["manifest"], tables["manifest"])
    _write_csv(outputs["co2_runlist"], tables["co2_runlist"])
    _write_csv(outputs["h2o_runlist"], tables["h2o_runlist"])
    _write_csv(outputs["checks"], tables["checks"])
    summary = [
        "# V1.5 algorithm formal runlist preview",
        "",
        "This is an offline no-write preview generated from the V1.5 algorithm route profile.",
        "",
        f"- status: `{tables['manifest']['status']}`",
        f"- blocker_count: `{tables['manifest']['blocker_count']}`",
        f"- profile: `{profile_id}`",
        f"- legacy CO2/H2O counts remain: `{tables['manifest']['legacy_co2_formal_point_count']}` / `{tables['manifest']['legacy_h2o_formal_point_count']}`",
        f"- new-algorithm CO2/H2O runlist counts: `{tables['manifest']['co2_runlist_count']}` / `{tables['manifest']['h2o_runlist_count']}`",
        "- CO2 runlist includes `-20C/600ppm` and `-10C/600ppm` inside their temperature segments.",
        "- H2O runlist includes `40C/HGEN30C/30RH` inside the 40C water segment.",
        "- The CSVs use mature formal queue-compatible columns but are preview artifacts only.",
        "- Mature V1.5 CO2/H2O runners are not modified by this preview.",
    ]
    outputs["summary"].write_text("\n".join(summary) + "\n", encoding="utf-8")
    return {key: str(path) for key, path in outputs.items()}


def write_v1_5_new_algorithm_test_point_plan(
    profile_path: str | Path,
    output_dir: str | Path,
    *,
    profile_id: str = "absorption_ratio_shadow",
) -> Dict[str, str]:
    tables = build_v1_5_new_algorithm_test_point_plan(profile_path, profile_id=profile_id)
    out = Path(output_dir)
    outputs = {
        "manifest": out / "v1_5_new_algorithm_test_point_plan_manifest.json",
        "point_plan": out / "v1_5_new_algorithm_test_point_plan.csv",
        "role_counts": out / "v1_5_new_algorithm_test_point_role_counts.csv",
        "summary": out / "V1_5_NEW_ALGORITHM_TEST_POINT_PLAN.md",
    }
    _write_json(outputs["manifest"], tables["manifest"])
    _write_csv(outputs["point_plan"], tables["point_plan"])
    _write_csv(outputs["role_counts"], tables["role_counts"])
    summary = [
        "# V1.5 new algorithm test point plan",
        "",
        "This is an offline no-write plan generated from the V1.5 algorithm route profile.",
        "",
        f"- Profile: `{profile_id}`",
        f"- CO2 candidate count: `{tables['manifest']['new_algorithm_co2_candidate_point_count']}`",
        f"- H2O wet candidate count: `{tables['manifest']['new_algorithm_h2o_candidate_wet_point_count']}`",
        "- Mature V1.5 CO2/H2O runners are preserved.",
        "- Legacy ratio production queues remain the default 45 CO2 points and 13 H2O wet points.",
        "- H2O low-water anchors from CO2 zero-gas evidence require R_H2O, dewpoint, pressure, and T1.",
    ]
    outputs["summary"].write_text("\n".join(summary) + "\n", encoding="utf-8")
    return {key: str(path) for key, path in outputs.items()}
