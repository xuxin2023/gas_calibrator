"""Offline V1.5 old/new algorithm route profile review helpers.

This module turns the V1.5 algorithm profile JSON into a reviewer-facing test
point plan. It is intentionally offline: it does not open COM ports, control
routes, or write coefficients.
"""

from __future__ import annotations

import csv
import json
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


def build_v1_5_algorithm_write_contract_tables(profile_path: str | Path) -> Dict[str, Any]:
    """Build offline write-contract tables from V1.5 algorithm profiles."""

    config = load_v1_5_algorithm_route_profiles(profile_path)
    rows = [_co2_write_contract_row(profile) for profile in config.get("profiles", [])]
    manifest = {
        "schema_version": 1,
        "generated_at": _now(),
        "no_write": True,
        "opens_com_ports": False,
        "controls_water_or_gas_routes": False,
        "writes_coefficients": False,
        "default_profile_id": config.get("default_profile_id"),
        "contract_scope": "co2_main_chain_and_final_linear_trim",
        "physical_contract": (
            "CO2 main-chain writes use reviewed SENCO1/SENCO3 payloads. New algorithm keeps the "
            "old seven coefficient slots but changes the optical input from R to A. SENCO5 remains "
            "a separate final affine layer and is never folded into SENCO1/SENCO3."
        ),
    }
    return {"manifest": manifest, "co2_write_contracts": rows}


def write_v1_5_algorithm_write_contract_review(
    profile_path: str | Path,
    output_dir: str | Path,
) -> Dict[str, str]:
    tables = build_v1_5_algorithm_write_contract_tables(profile_path)
    out = Path(output_dir)
    outputs = {
        "manifest": out / "v1_5_algorithm_write_contract_manifest.json",
        "co2_write_contracts": out / "v1_5_co2_algorithm_write_contracts.csv",
        "summary": out / "V1_5_ALGORITHM_WRITE_CONTRACT_REVIEW.md",
    }
    _write_json(outputs["manifest"], tables["manifest"])
    _write_csv(outputs["co2_write_contracts"], tables["co2_write_contracts"])
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
