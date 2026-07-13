"""Validate the design-only V1.5 component-QC generator contract."""

from __future__ import annotations

import csv
import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping

from .v1_5_component_qc_authority_audit import SCHEMA as AUTHORITY_SCHEMA


SCHEMA = "v1_5_component_qc_generator_contract_review_v1"
CONTRACT_SCHEMA = "v1_5_component_qc_generator_contract_v1"


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _read_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8-sig") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise ValueError(f"Expected JSON object: {path}")
    return payload


def _false_lock_reasons(payload: Mapping[str, Any], keys: tuple[str, ...], prefix: str) -> list[str]:
    return [f"{prefix}_{key}_not_false" for key in keys if payload.get(key) is not False]


def _contract_reasons(contract: Mapping[str, Any]) -> list[str]:
    reasons: list[str] = []
    if contract.get("schema") != CONTRACT_SCHEMA:
        reasons.append("contract_schema_mismatch")
    if contract.get("production_state") != "design_only_generation_blocked":
        reasons.append("contract_production_state_invalid")
    scope = contract.get("scope") or {}
    expected_scope = {
        "grading_scope": "per_analyzer_independent",
        "point_summary_is_informational_only": True,
        "one_analyzer_failure_blocks_other_analyzers": False,
        "changes_preseal_or_sampling_actions": False,
        "uses_summary_outlier_filtered_values": False,
        "ratio_input": "all_raw_sample_window_rows_with_frame_usable_true",
    }
    for key, expected in expected_scope.items():
        if scope.get(key) != expected:
            reasons.append(f"scope_{key}_invalid")
    grades = contract.get("common_grade_contract") or {}
    expected_grades = {
        "A_calibration_eligible": (True, True),
        "B_diagnostic_model_only": (False, True),
        "C_reject": (False, False),
    }
    for grade, expected in expected_grades.items():
        row = grades.get(grade) or {}
        observed = (
            row.get("sample_can_enter_calibration_fit"),
            row.get("sample_can_enter_diagnostic_model"),
        )
        if observed != expected:
            reasons.append(f"grade_semantics_invalid:{grade}")
    routes = contract.get("routes") or {}
    co2 = routes.get("co2") or {}
    h2o = routes.get("h2o") or {}
    if co2.get("ratio_key_suffix") != "co2_ratio_f":
        reasons.append("co2_ratio_key_invalid")
    if co2.get("A_ratio_span_max") != 0.0005 or co2.get("B_ratio_span_max") != 0.001:
        reasons.append("co2_ratio_threshold_contract_invalid")
    if co2.get("above_B_grade") != "C_reject":
        reasons.append("co2_above_hard_threshold_grade_invalid")
    if h2o.get("ratio_key_suffix") != "h2o_ratio_f":
        reasons.append("h2o_ratio_key_invalid")
    if h2o.get("A_ratio_span_max") != 0.001:
        reasons.append("h2o_a_ratio_threshold_must_be_0_001")
    if h2o.get("B_ratio_span_max") is not None:
        reasons.append("h2o_unreviewed_hard_ratio_threshold_must_be_null")
    if h2o.get("above_A_grade") != "B_diagnostic_model_only":
        reasons.append("h2o_above_a_grade_invalid")
    if h2o.get("ratio_span_alone_can_assign_C") is not False:
        reasons.append("h2o_ratio_span_alone_c_reject_not_allowed")
    if co2.get("zero_gas_role") != "co2_low_concentration_anchor_only":
        reasons.append("co2_zero_gas_role_invalid")
    if h2o.get("dry_gas_role") != "h2o_low_water_anchor_requires_dewpoint_pressure_evidence":
        reasons.append("h2o_dry_gas_role_invalid")
    cadence = contract.get("cadence_and_alignment_contract") or {}
    if cadence.get("global_sample_alignment_false_auto_rejects_point") is not False:
        reasons.append("global_alignment_false_must_not_auto_reject_point")
    if cadence.get("cadence_warning_with_required_rows_grade_ceiling") != "B_diagnostic_model_only":
        reasons.append("cadence_warning_grade_ceiling_invalid")
    output = contract.get("output_contract") or {}
    required_fields = set(output.get("required_fields") or [])
    for field in (
        "source_samples_sha256",
        "source_frame_qc_sha256",
        "source_runtime_config_sha256",
        "contract_sha256",
    ):
        if field not in required_fields:
            reasons.append(f"output_traceability_field_missing:{field}")
    if output.get("source_inputs_immutable") is not True:
        reasons.append("source_inputs_must_be_immutable")
    if output.get("idempotent_for_same_input_hashes") is not True:
        reasons.append("generator_must_be_idempotent")
    locks = contract.get("locks") or {}
    reasons.extend(
        _false_lock_reasons(
            locks,
            (
                "implementation_available",
                "component_qc_generation_allowed",
                "component_qc_backfill_allowed",
                "historical_fit_allowed",
                "formal_release_allowed",
                "database_import_allowed",
                "opens_com_ports",
                "controls_pressure",
                "controls_water_or_gas_routes",
                "writes_coefficients",
                "writes_sn_or_device_code",
                "connects_postgresql",
            ),
            "contract",
        )
    )
    if locks.get("not_real_acceptance_evidence") is not True:
        reasons.append("contract_real_acceptance_lock_missing")
    return sorted(set(reasons))


def validate_v1_5_component_qc_generator_contract(
    contract: Mapping[str, Any],
) -> list[str]:
    """Return contract violations without changing the design-only locks."""

    return _contract_reasons(contract)


def build_v1_5_component_qc_generator_contract_review(
    *, authority_audit_json_path: str | Path, contract_json_path: str | Path
) -> dict[str, Any]:
    authority_path = Path(authority_audit_json_path).resolve()
    contract_path = Path(contract_json_path).resolve()
    authority = _read_json(authority_path)
    contract = _read_json(contract_path)
    reasons: list[str] = []
    if authority.get("schema") != AUTHORITY_SCHEMA:
        reasons.append("authority_schema_mismatch")
    if authority.get("overall_status") != "blocked_no_reviewed_mature_component_qc_authority":
        reasons.append("authority_status_invalid")
    if authority.get("tracked_component_qc_writer_present") is not False:
        reasons.append("authority_tracked_writer_state_changed")
    if authority.get("component_qc_generation_allowed") is not False:
        reasons.append("authority_generation_lock_missing")
    if authority.get("component_qc_backfill_allowed") is not False:
        reasons.append("authority_backfill_lock_missing")
    reasons.extend(_contract_reasons(contract))
    status = (
        "blocked_invalid_component_qc_contract"
        if reasons
        else "ready_for_component_qc_generator_contract_manual_review"
    )
    routes = contract.get("routes") or {}
    locks = contract.get("locks") or {}
    return {
        "schema": SCHEMA,
        "generated_at": _now(),
        "overall_status": status,
        "blocker_codes": sorted(set(reasons)),
        "authority_audit_path": str(authority_path),
        "authority_audit_sha256": _sha256(authority_path),
        "contract_path": str(contract_path),
        "contract_sha256": _sha256(contract_path),
        "contract_version": contract.get("contract_version"),
        "p2_candidate_count": authority.get("p2_candidate_count"),
        "rules": {
            "grading_scope": (contract.get("scope") or {}).get("grading_scope"),
            "one_analyzer_failure_blocks_other_analyzers": (
                contract.get("scope") or {}
            ).get("one_analyzer_failure_blocks_other_analyzers"),
            "co2_a_ratio_span_max": (routes.get("co2") or {}).get("A_ratio_span_max"),
            "co2_b_ratio_span_max": (routes.get("co2") or {}).get("B_ratio_span_max"),
            "h2o_a_ratio_span_max": (routes.get("h2o") or {}).get("A_ratio_span_max"),
            "h2o_ratio_span_alone_can_assign_c": (routes.get("h2o") or {}).get(
                "ratio_span_alone_can_assign_C"
            ),
            "changes_preseal_or_sampling_actions": (contract.get("scope") or {}).get(
                "changes_preseal_or_sampling_actions"
            ),
        },
        "grade_contract": contract.get("common_grade_contract"),
        "point_wide_hard_blockers": contract.get("point_wide_hard_blockers"),
        "implementation_available": locks.get("implementation_available"),
        "component_qc_generation_allowed": False,
        "component_qc_backfill_allowed": False,
        "historical_fit_allowed": False,
        "formal_release_allowed": False,
        "database_import_allowed": False,
        "opens_com_ports": False,
        "controls_pressure": False,
        "controls_water_or_gas_routes": False,
        "writes_coefficients": False,
        "writes_sn_or_device_code": False,
        "connects_postgresql": False,
        "not_real_acceptance_evidence": True,
    }


def write_v1_5_component_qc_generator_contract_review(
    model: Mapping[str, Any], output_dir: str | Path
) -> dict[str, Path]:
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    outputs = {
        "json": out / "v1_5_component_qc_generator_contract_review.json",
        "summary_csv": out / "v1_5_component_qc_generator_contract_summary.csv",
        "markdown": out / "V1_5_COMPONENT_QC_GENERATOR_CONTRACT_REVIEW.md",
    }
    outputs["json"].write_text(
        json.dumps(dict(model), ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    rows = [
        {"metric": "overall_status", "value": model.get("overall_status")},
        {"metric": "p2_candidate_count", "value": model.get("p2_candidate_count")},
        {"metric": "implementation_available", "value": model.get("implementation_available")},
        {
            "metric": "component_qc_generation_allowed",
            "value": model.get("component_qc_generation_allowed"),
        },
    ]
    with outputs["summary_csv"].open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["metric", "value"])
        writer.writeheader()
        writer.writerows(rows)
    rules = model.get("rules") or {}
    lines = [
        "# V1.5 Component-QC Generator Contract Review",
        "",
        f"- overall_status: `{model.get('overall_status')}`",
        f"- p2_candidate_count: `{model.get('p2_candidate_count')}`",
        f"- grading_scope: `{rules.get('grading_scope')}`",
        f"- CO2 A/B span: `{rules.get('co2_a_ratio_span_max')} / {rules.get('co2_b_ratio_span_max')}`",
        f"- H2O A span: `{rules.get('h2o_a_ratio_span_max')}`",
        "- implementation_available: `false`",
        "- component_qc_generation_allowed: `false`",
        "- component_qc_backfill_allowed: `false`",
        "- offline_only: `true`",
    ]
    outputs["markdown"].write_text("\n".join(lines) + "\n", encoding="utf-8")
    return outputs


__all__ = [
    "CONTRACT_SCHEMA",
    "SCHEMA",
    "build_v1_5_component_qc_generator_contract_review",
    "validate_v1_5_component_qc_generator_contract",
    "write_v1_5_component_qc_generator_contract_review",
]
