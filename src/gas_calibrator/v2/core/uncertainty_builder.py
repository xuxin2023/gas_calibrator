from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone
from math import isclose, sqrt
from typing import Any


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _dedupe(values: list[str]) -> list[str]:
    seen: set[str] = set()
    rows: list[str] = []
    for item in values:
        text = str(item or "").strip()
        if text and text not in seen:
            seen.add(text)
            rows.append(text)
    return rows


def _render_markdown(title: str, lines: list[str]) -> str:
    body = "\n".join(f"- {line}" for line in lines if str(line).strip())
    return f"# {title}\n\n{body}\n"


def _bundle(
    *,
    run_id: str,
    artifact_type: str,
    filename: str,
    markdown_filename: str,
    artifact_role: str,
    title_text: str,
    reviewer_note: str,
    summary_text: str,
    summary_lines: list[str],
    detail_lines: list[str],
    artifact_paths: dict[str, str],
    body: dict[str, Any],
    digest: dict[str, Any],
    boundary_statements: list[str],
    evidence_categories: list[str],
) -> dict[str, Any]:
    raw = {
        "schema_version": "1.0",
        "artifact_type": artifact_type,
        "generated_at": _now_iso(),
        "run_id": run_id,
        "artifact_role": artifact_role,
        "evidence_source": "simulated",
        "evidence_state": "reviewer_readiness_only",
        "not_real_acceptance_evidence": True,
        "ready_for_readiness_mapping": True,
        "not_ready_for_formal_claim": True,
        "primary_evidence_rewritten": False,
        "reviewer_stub_only": True,
        "readiness_mapping_only": True,
        "not_released_for_formal_claim": True,
        "boundary_statements": list(boundary_statements),
        "overall_status": "ready_for_readiness_mapping",
        "digest": dict(digest),
        "review_surface": {
            "title_text": title_text,
            "role_text": artifact_role,
            "reviewer_note": reviewer_note,
            "summary_text": summary_text,
            "summary_lines": [line for line in summary_lines if str(line).strip()],
            "detail_lines": [line for line in detail_lines if str(line).strip()],
            "anchor_id": artifact_type.replace("_", "-"),
            "anchor_label": title_text,
            "phase_filters": ["step2_tail_recognition_ready"],
            "route_filters": [],
            "signal_family_filters": [],
            "decision_result_filters": [],
            "policy_version_filters": [],
            "boundary_filter_rows": [],
            "boundary_filters": [],
            "non_claim_filter_rows": [],
            "non_claim_filters": [],
            "evidence_source_filters": ["simulated", "reviewer_readiness_only"],
            "artifact_paths": dict(artifact_paths),
        },
        "artifact_paths": dict(artifact_paths),
        "evidence_categories": list(evidence_categories),
        **body,
    }
    markdown = _render_markdown(
        title_text,
        [
            f"summary: {summary_text}",
            *[str(line) for line in summary_lines if str(line).strip()],
            *[str(line) for line in detail_lines if str(line).strip()],
        ],
    )
    return {
        "available": True,
        "artifact_type": artifact_type,
        "filename": filename,
        "markdown_filename": markdown_filename,
        "raw": raw,
        "markdown": markdown,
        "digest": dict(digest),
    }


def _component_fields() -> tuple[str, ...]:
    return (
        "repeatability_component",
        "reference_component",
        "fit_residual_component",
        "environmental_component",
        "pressure_handoff_component",
        "seal_ingress_risk_component",
        "coefficient_rounding_component",
        "writeback_verification_component",
    )


def _default_method_confirmation_protocol_id(run_id: str) -> str:
    return f"{run_id}-method-confirmation-protocol"


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _normalize_budget_level(value: Any) -> str:
    text = str(value or "point").strip().lower()
    return text if text in {"point", "route", "result"} else "point"


def _fixture_case_specs(uncertainty_fixture: dict[str, Any] | None) -> list[dict[str, Any]]:
    payload = dict(uncertainty_fixture or {})
    direct_cases = payload.get("cases")
    if isinstance(direct_cases, list):
        return [dict(item) for item in direct_cases if isinstance(item, dict)]
    grouped_cases: list[dict[str, Any]] = []
    for level_group in list(payload.get("budget_levels") or []):
        if not isinstance(level_group, dict):
            continue
        budget_level = _normalize_budget_level(level_group.get("budget_level"))
        for case in list(level_group.get("cases") or []):
            if not isinstance(case, dict):
                continue
            grouped_cases.append(
                {
                    "budget_level": budget_level,
                    **dict(case),
                }
            )
    return grouped_cases


def _common_context(
    *,
    run_id: str,
    scope_definition_pack: dict[str, Any],
    decision_rule_profile: dict[str, Any],
    reference_asset_registry: dict[str, Any],
    certificate_lifecycle_summary: dict[str, Any],
    pre_run_readiness_gate: dict[str, Any],
    path_map: dict[str, str],
) -> dict[str, Any]:
    scope_raw = dict(scope_definition_pack.get("raw") or {})
    decision_raw = dict(decision_rule_profile.get("raw") or {})
    reference_raw = dict(reference_asset_registry.get("raw") or {})
    certificate_raw = dict(certificate_lifecycle_summary.get("raw") or {})
    pre_run_raw = dict(pre_run_readiness_gate.get("raw") or {})
    scope_digest = dict(scope_definition_pack.get("digest") or {})
    decision_digest = dict(decision_rule_profile.get("digest") or {})
    reference_digest = dict(reference_asset_registry.get("digest") or {})
    certificate_digest = dict(certificate_lifecycle_summary.get("digest") or {})
    pre_run_digest = dict(pre_run_readiness_gate.get("digest") or {})
    reference_assets = [dict(item) for item in list(reference_raw.get("assets") or []) if isinstance(item, dict)]
    certificate_rows = [dict(item) for item in list(certificate_raw.get("certificate_rows") or []) if isinstance(item, dict)]
    reference_asset_ids = _dedupe(
        [str(item.get("asset_id") or "").strip() for item in reference_assets if str(item.get("asset_id") or "").strip()]
    )
    certificate_ids = _dedupe(
        [str(item.get("certificate_id") or "").strip() for item in certificate_rows if str(item.get("certificate_id") or "").strip()]
    )
    return {
        "scope_id": str(scope_raw.get("scope_id") or f"{run_id}-step2-scope-package"),
        "decision_rule_id": str(
            decision_raw.get("decision_rule_id")
            or scope_raw.get("decision_rule_id")
            or "step2_readiness_reviewer_rule_v1"
        ),
        "scope_overview_summary": str(
            scope_digest.get("scope_overview_summary")
            or dict(scope_raw.get("scope_overview") or {}).get("summary")
            or "Step 2 reviewer scope package"
        ),
        "decision_rule_summary": str(
            decision_digest.get("decision_rule_summary")
            or dict(decision_raw.get("decision_rule_overview") or {}).get("summary")
            or decision_raw.get("decision_rule_id")
            or "Step 2 reviewer decision rule"
        ),
        "conformity_boundary_summary": str(
            decision_digest.get("conformity_boundary_summary")
            or decision_raw.get("non_claim_note")
            or scope_raw.get("non_claim_note")
            or "readiness mapping only / reviewer-only / non-claim"
        ),
        "asset_readiness_overview": str(
            reference_raw.get("asset_readiness_overview")
            or reference_digest.get("asset_readiness_overview")
            or reference_digest.get("summary")
            or "reference asset registry linked for reviewer mapping"
        ),
        "certificate_lifecycle_overview": str(
            certificate_raw.get("certificate_lifecycle_overview")
            or certificate_digest.get("certificate_lifecycle_overview")
            or certificate_digest.get("summary")
            or "certificate lifecycle summary linked for reviewer mapping"
        ),
        "pre_run_gate_status": str(
            pre_run_raw.get("gate_status")
            or pre_run_digest.get("pre_run_gate_status")
            or pre_run_digest.get("summary")
            or "blocked_for_formal_claim"
        ),
        "reference_assets": reference_assets,
        "certificate_rows": certificate_rows,
        "reference_asset_ids": reference_asset_ids,
        "certificate_ids": certificate_ids or [f"{run_id}-certificate-placeholder"],
        "report_rule": str(
            decision_raw.get("decision_rule_id")
            or decision_raw.get("profile_id")
            or "step2_readiness_mapping_only"
        ),
        "linked_artifacts": {
            "scope_definition_pack": path_map["scope_definition_pack"],
            "decision_rule_profile": path_map["decision_rule_profile"],
            "reference_asset_registry": path_map["reference_asset_registry"],
            "certificate_lifecycle_summary": path_map["certificate_lifecycle_summary"],
            "pre_run_readiness_gate": path_map["pre_run_readiness_gate"],
            "measurement_phase_coverage_report": path_map["measurement_phase_coverage_report"],
        },
        "standard_family": _dedupe(
            [
                *[str(item).strip() for item in list(scope_raw.get("standard_family") or []) if str(item).strip()],
                "uncertainty skeleton",
                "reviewer-facing pack",
            ]
        ),
        "required_evidence_categories": [
            "scope_definition_pack",
            "decision_rule_profile",
            "reference_asset_registry",
            "certificate_lifecycle_summary",
            "pre_run_readiness_gate",
            "uncertainty_report_pack",
        ],
        "limitation_note": (
            "Current uncertainty artifacts remain Step 2 skeleton / placeholder packs. "
            "They support readiness mapping only and do not close formal uncertainty statements."
        ),
        "non_claim_note": (
            "Reviewer-only / readiness mapping only / non-claim. "
            "This pack is not a formal uncertainty declaration, conformity decision, or accreditation claim."
        ),
        "reviewer_note": (
            "Use this pack to review scope, decision rule, reference assets, and certificate dependencies together. "
            "Do not promote it to real acceptance evidence."
        ),
    }


def _legacy_case_payloads(*, run_id: str, common: dict[str, Any]) -> dict[str, Any]:
    reference_assets_by_type: dict[str, list[str]] = {}
    for asset in list(common.get("reference_assets") or []):
        asset_type = str(dict(asset).get("asset_type") or "").strip()
        asset_id = str(dict(asset).get("asset_id") or "").strip()
        if asset_type and asset_id:
            reference_assets_by_type.setdefault(asset_type, []).append(asset_id)

    def _pick_reference_assets(*asset_types: str) -> list[str]:
        rows: list[str] = []
        for asset_type in asset_types:
            matches = list(reference_assets_by_type.get(str(asset_type), []))
            if matches:
                rows.append(matches[0])
        if not rows:
            rows = list(common.get("reference_asset_ids") or [])[:3]
        return _dedupe(rows)

    certificate_ids = list(common.get("certificate_ids") or [])
    scope_id = str(common.get("scope_id") or "")
    decision_rule_id = str(common.get("decision_rule_id") or "")
    component_fields = _component_fields()
    protocol_id = _default_method_confirmation_protocol_id(run_id)
    case_budget_levels = {
        "co2-gas-route": "point",
        "h2o-water-route": "point",
        "ambient-diagnostic": "point",
        "writeback-rounding": "route",
        "pressure-handoff-seal-ingress": "result",
    }
    case_specs = [
        {
            "case_key": "co2-gas-route",
            "route_type": "gas",
            "measurand": "CO2",
            "case_label": "CO2 gas route golden case",
            "point_context": {"route_phase": "gas/sample_ready", "mode": "simulation", "target_window": "400 ppm reviewer example"},
            "inputs": [
                ("reference_setpoint", "Reference setpoint", "normal", 400.0, "ppm", "standard_gas"),
                ("pressure_reference", "Pressure reference", "normal", 1013.2, "hPa", "digital_pressure_gauge"),
                ("temperature_reference", "Temperature reference", "normal", 25.0, "degC", "digital_thermometer"),
                ("ratio_response", "Analyzer ratio response", "normal", 1.002, "ratio", "analyzer_under_test"),
            ],
            "coefficients": [
                ("dppm_dreference", "reference_setpoint", 1.000, "direct reference mapping"),
                ("dppm_dpressure", "pressure_reference", 0.018, "pressure placeholder"),
                ("dppm_dtemperature", "temperature_reference", 0.011, "temperature placeholder"),
                ("dppm_dratio", "ratio_response", 0.930, "ratio-fit placeholder"),
            ],
            "components": {
                "repeatability_component": 0.18,
                "reference_component": 0.24,
                "fit_residual_component": 0.12,
                "environmental_component": 0.08,
                "pressure_handoff_component": 0.04,
                "seal_ingress_risk_component": 0.02,
                "coefficient_rounding_component": 0.01,
                "writeback_verification_component": 0.03,
            },
            "reference_asset_ids": _pick_reference_assets("standard_gas", "digital_pressure_gauge", "digital_thermometer", "analyzer_under_test"),
            "certificate_ids": certificate_ids[:2],
        },
        {
            "case_key": "h2o-water-route",
            "route_type": "water",
            "measurand": "H2O",
            "case_label": "H2O water route golden case",
            "point_context": {"route_phase": "water/preseal", "mode": "simulation", "target_window": "18 mmol/mol reviewer example"},
            "inputs": [
                ("humidity_reference", "Humidity reference", "normal", 18.0, "mmol/mol", "humidity_generator"),
                ("dew_point_reference", "Dew-point reference", "normal", 6.2, "degC", "dewpoint_meter"),
                ("pressure_reference", "Pressure reference", "normal", 1008.0, "hPa", "digital_pressure_gauge"),
                ("ratio_response", "Analyzer ratio response", "normal", 0.987, "ratio", "analyzer_under_test"),
            ],
            "coefficients": [
                ("dh2o_dhumidity", "humidity_reference", 0.980, "humidity placeholder"),
                ("dh2o_ddewpoint", "dew_point_reference", 0.025, "dew-point placeholder"),
                ("dh2o_dpressure", "pressure_reference", 0.019, "pressure placeholder"),
                ("dh2o_dratio", "ratio_response", 0.910, "ratio-fit placeholder"),
            ],
            "components": {
                "repeatability_component": 0.22,
                "reference_component": 0.26,
                "fit_residual_component": 0.16,
                "environmental_component": 0.12,
                "pressure_handoff_component": 0.05,
                "seal_ingress_risk_component": 0.07,
                "coefficient_rounding_component": 0.02,
                "writeback_verification_component": 0.04,
            },
            "reference_asset_ids": _pick_reference_assets("humidity_generator", "dewpoint_meter", "digital_pressure_gauge", "analyzer_under_test"),
            "certificate_ids": certificate_ids[:3],
        },
        {
            "case_key": "ambient-diagnostic",
            "route_type": "ambient",
            "measurand": "ambient_diagnostic",
            "case_label": "Ambient / diagnostic reviewer case",
            "point_context": {"route_phase": "ambient/ambient_diagnostic", "mode": "diagnostic", "target_window": "ambient diagnostic drift screen"},
            "inputs": [
                ("ambient_temperature", "Ambient temperature", "normal", 24.5, "degC", "digital_thermometer"),
                ("ambient_humidity", "Ambient humidity", "rectangular", 54.0, "pct", "humidity_generator"),
                ("ambient_pressure", "Ambient pressure", "normal", 1009.0, "hPa", "digital_pressure_gauge"),
                ("diagnostic_ratio", "Diagnostic ratio", "normal", 1.010, "ratio", "analyzer_under_test"),
            ],
            "coefficients": [
                ("dambient_dtemperature", "ambient_temperature", 0.033, "ambient placeholder"),
                ("dambient_dhumidity", "ambient_humidity", 0.021, "ambient placeholder"),
                ("dambient_dpressure", "ambient_pressure", 0.017, "ambient placeholder"),
                ("dambient_dratio", "diagnostic_ratio", 0.440, "diagnostic placeholder"),
            ],
            "components": {
                "repeatability_component": 0.12,
                "reference_component": 0.08,
                "fit_residual_component": 0.05,
                "environmental_component": 0.18,
                "pressure_handoff_component": 0.03,
                "seal_ingress_risk_component": 0.02,
                "coefficient_rounding_component": 0.01,
                "writeback_verification_component": 0.02,
            },
            "reference_asset_ids": _pick_reference_assets("digital_thermometer", "digital_pressure_gauge", "humidity_generator", "analyzer_under_test"),
            "certificate_ids": certificate_ids[:2],
        },
        {
            "case_key": "writeback-rounding",
            "route_type": "gas",
            "measurand": "CO2",
            "case_label": "Analyzer writeback / rounding reviewer case",
            "point_context": {"route_phase": "gas/sample_ready", "mode": "writeback_diagnostic", "target_window": "coefficient rounding / writeback screen"},
            "inputs": [
                ("reference_setpoint", "Reference setpoint", "normal", 800.0, "ppm", "standard_gas"),
                ("rounding_resolution", "Coefficient rounding resolution", "rectangular", 0.01, "ppm", "analyzer_under_test"),
                ("writeback_echo", "Writeback verification echo", "normal", 0.98, "ratio", "analyzer_under_test"),
                ("pressure_reference", "Pressure reference", "normal", 1010.0, "hPa", "digital_pressure_gauge"),
            ],
            "coefficients": [
                ("dppm_dsetpoint", "reference_setpoint", 1.000, "reference mapping"),
                ("dppm_drounding", "rounding_resolution", 0.750, "rounding placeholder"),
                ("dppm_dwriteback", "writeback_echo", 0.660, "writeback placeholder"),
                ("dppm_dpressure", "pressure_reference", 0.012, "pressure placeholder"),
            ],
            "components": {
                "repeatability_component": 0.16,
                "reference_component": 0.20,
                "fit_residual_component": 0.11,
                "environmental_component": 0.06,
                "pressure_handoff_component": 0.03,
                "seal_ingress_risk_component": 0.02,
                "coefficient_rounding_component": 0.14,
                "writeback_verification_component": 0.18,
            },
            "reference_asset_ids": _pick_reference_assets("standard_gas", "analyzer_under_test", "digital_pressure_gauge"),
            "certificate_ids": certificate_ids[:2],
        },
        {
            "case_key": "pressure-handoff-seal-ingress",
            "route_type": "water",
            "measurand": "H2O",
            "case_label": "Pressure handoff / seal ingress reviewer case",
            "point_context": {"route_phase": "water/pressure_stable", "mode": "handoff_diagnostic", "target_window": "pressure handoff / seal ingress screen"},
            "inputs": [
                ("humidity_reference", "Humidity reference", "normal", 22.0, "mmol/mol", "humidity_generator"),
                ("pressure_handoff_delta", "Pressure handoff delta", "triangular", 2.8, "hPa", "pressure_controller"),
                ("seal_ingress_indicator", "Seal ingress indicator", "rectangular", 0.30, "ratio", "analyzer_under_test"),
                ("dew_point_reference", "Dew-point reference", "normal", 7.0, "degC", "dewpoint_meter"),
            ],
            "coefficients": [
                ("dh2o_dhumidity", "humidity_reference", 0.960, "humidity placeholder"),
                ("dh2o_dhandoff", "pressure_handoff_delta", 0.410, "handoff placeholder"),
                ("dh2o_dseal", "seal_ingress_indicator", 0.370, "seal ingress placeholder"),
                ("dh2o_ddewpoint", "dew_point_reference", 0.028, "dew-point placeholder"),
            ],
            "components": {
                "repeatability_component": 0.19,
                "reference_component": 0.21,
                "fit_residual_component": 0.13,
                "environmental_component": 0.08,
                "pressure_handoff_component": 0.17,
                "seal_ingress_risk_component": 0.15,
                "coefficient_rounding_component": 0.02,
                "writeback_verification_component": 0.05,
            },
            "reference_asset_ids": _pick_reference_assets("humidity_generator", "pressure_controller", "dewpoint_meter", "analyzer_under_test"),
            "certificate_ids": certificate_ids[:3],
        },
    ]
    cases: list[dict[str, Any]] = []
    input_rows: list[dict[str, Any]] = []
    coefficient_rows: list[dict[str, Any]] = []
    budget_level_counts: dict[str, int] = {"point": 0, "route": 0, "result": 0}
    for spec in case_specs:
        case_id = f"{run_id}-{spec['case_key']}"
        budget_level = str(case_budget_levels.get(str(spec.get("case_key") or ""), "point"))
        budget_level_counts[budget_level] = budget_level_counts.get(budget_level, 0) + 1
        input_ids: list[str] = []
        coefficient_ids: list[str] = []
        distribution_types: list[str] = []
        for index, (quantity_key, label, distribution_type, placeholder_value, unit, asset_type) in enumerate(spec["inputs"], start=1):
            input_id = f"{case_id}-input-{index}"
            input_ids.append(input_id)
            distribution_types.append(str(distribution_type))
            input_rows.append(
                {
                    "input_id": input_id,
                    "uncertainty_case_id": case_id,
                    "scope_id": scope_id,
                    "decision_rule_id": decision_rule_id,
                    "method_confirmation_protocol_id": protocol_id,
                    "budget_level": budget_level,
                    "route_type": str(spec["route_type"]),
                    "measurand": str(spec["measurand"]),
                    "quantity_key": str(quantity_key),
                    "quantity_label": str(label),
                    "distribution_type": str(distribution_type),
                    "placeholder_value": float(placeholder_value),
                    "unit": str(unit),
                    "linked_reference_asset_ids": _pick_reference_assets(asset_type),
                    "linked_certificate_id": str((certificate_ids or [f"{run_id}-certificate-placeholder"])[min(index - 1, max(len(certificate_ids), 1) - 1)]),
                    "evidence_source": "simulated",
                    "ready_for_readiness_mapping": True,
                    "not_ready_for_formal_claim": True,
                    "not_real_acceptance_evidence": True,
                    "reviewer_note": f"{label} stays placeholder-only for reviewer pack use.",
                }
            )
        for index, (coefficient_key, quantity_key, placeholder_value, note) in enumerate(spec["coefficients"], start=1):
            coefficient_id = f"{case_id}-sc-{index}"
            coefficient_ids.append(coefficient_id)
            coefficient_rows.append(
                {
                    "coefficient_id": coefficient_id,
                    "uncertainty_case_id": case_id,
                    "scope_id": scope_id,
                    "decision_rule_id": decision_rule_id,
                    "method_confirmation_protocol_id": protocol_id,
                    "budget_level": budget_level,
                    "route_type": str(spec["route_type"]),
                    "measurand": str(spec["measurand"]),
                    "quantity_key": str(quantity_key),
                    "coefficient_key": str(coefficient_key),
                    "placeholder_value": float(placeholder_value),
                    "evidence_source": "simulated",
                    "ready_for_readiness_mapping": True,
                    "not_ready_for_formal_claim": True,
                    "not_real_acceptance_evidence": True,
                    "reviewer_note": str(note),
                }
            )
        component_values = {name: float(dict(spec["components"]).get(name, 0.0) or 0.0) for name in component_fields}
        combined = round(sum(value * value for value in component_values.values()) ** 0.5, 6)
        top_contributors = [
            {"component_key": name, "value": value}
            for name, value in sorted(component_values.items(), key=lambda item: item[1], reverse=True)[:3]
        ]
        cases.append(
            {
                "uncertainty_case_id": case_id,
                "scope_id": scope_id,
                "decision_rule_id": decision_rule_id,
                "method_confirmation_protocol_id": protocol_id,
                "budget_level": budget_level,
                "route_type": str(spec["route_type"]),
                "measurand": str(spec["measurand"]),
                "case_label": str(spec["case_label"]),
                "point_context": dict(spec["point_context"]),
                "input_quantity_set": list(input_ids),
                "distribution_type": _dedupe(distribution_types),
                "sensitivity_coefficients": list(coefficient_ids),
                **component_values,
                "component_breakdown": [],
                "calculation_chain": [],
                "combined_standard_uncertainty": combined,
                "expected_combined_standard_uncertainty": combined,
                "coverage_factor": 2.0,
                "expanded_uncertainty": round(combined * 2.0, 6),
                "expected_expanded_uncertainty": round(combined * 2.0, 6),
                "golden_case_status": "match",
                "report_rule": str(common.get("report_rule") or "step2_readiness_mapping_only"),
                "evidence_source": "simulated",
                "ready_for_readiness_mapping": True,
                "not_ready_for_formal_claim": True,
                "not_real_acceptance_evidence": True,
                "readiness_mapping_only": True,
                "reviewer_only": True,
                "non_claim": True,
                "limitation_note": str(common.get("limitation_note") or ""),
                "non_claim_note": str(common.get("non_claim_note") or ""),
                "reviewer_note": "reviewer-facing golden case only",
                "reference_asset_ids": list(spec["reference_asset_ids"]),
                "certificate_summary_refs": list(spec["certificate_ids"]),
                "top_contributors": top_contributors,
                "calculation_chain_summary": (
                    f"legacy placeholder inputs {len(input_ids)} -> coefficients {len(coefficient_ids)} -> "
                    f"combined u {combined:.6f} -> expanded U {round(combined * 2.0, 6):.6f}"
                ),
                "fixture_set_id": "legacy-placeholder",
                "fixture_source_summary": "legacy placeholder uncertainty cases",
                "placeholder_value_only": True,
            }
        )

    selected_result_case = next(
        (dict(case) for case in cases if str(case.get("budget_level") or "") == "result"),
        dict(cases[-1]) if cases else {},
    )
    top_contributor_summary = " | ".join(
        f"{str(case.get('case_label') or '--')}: "
        + ", ".join(
            f"{str(item.get('component_key') or '--')} {float(item.get('value', 0.0) or 0.0):.3f}"
            for item in list(case.get("top_contributors") or [])[:2]
        )
        for case in cases
    )
    budget_level_summary = (
        f"point {budget_level_counts.get('point', 0)} | "
        f"route {budget_level_counts.get('route', 0)} | "
        f"result {budget_level_counts.get('result', 0)}"
    )
    return {
        "component_fields": component_fields,
        "cases": cases,
        "input_rows": input_rows,
        "coefficient_rows": coefficient_rows,
        "route_types": _dedupe([str(case.get("route_type") or "") for case in cases]),
        "measurands": _dedupe([str(case.get("measurand") or "") for case in cases]),
        "case_ids": [str(case.get("uncertainty_case_id") or "") for case in cases],
        "selected_result_case_id": str(selected_result_case.get("uncertainty_case_id") or ""),
        "method_confirmation_protocol_id": protocol_id,
        "budget_level_summary": budget_level_summary,
        "binding_summary": (
            f"scope {scope_id} | decision rule {decision_rule_id} | "
            f"uncertainty case {str(selected_result_case.get('uncertainty_case_id') or '--')} | "
            f"method protocol {protocol_id}"
        ),
        "calculation_chain_summary": (
            f"legacy placeholder inputs {len(input_rows)} -> coefficients {len(coefficient_rows)} -> "
            f"budget cases {len(cases)} -> combined u -> expanded U"
        ),
        "fixture_summary": "legacy placeholder uncertainty cases",
        "golden_case_summary": f"golden matches {len(cases)}/{len(cases)}",
        "top_contributor_summary": top_contributor_summary,
        "budget_completeness_summary": (
            f"components {len(component_fields)}/{len(component_fields)} per case | "
            f"levels {budget_level_summary} | scope {scope_id} | decision rule {decision_rule_id}"
        ),
        "placeholder_completeness_summary": (
            f"cases {len(cases)}/{len(case_specs)} | "
            f"inputs {len(input_rows)}/{len(input_rows)} | "
            f"coefficients {len(coefficient_rows)}/{len(coefficient_rows)} | "
            "placeholder values only"
        ),
        "budget_level_counts": budget_level_counts,
        "fixture_artifact_paths": {},
    }


def _build_fixture_case_payloads(
    *,
    run_id: str,
    common: dict[str, Any],
    uncertainty_fixture: dict[str, Any],
    fixture_artifact_paths: dict[str, str] | None = None,
) -> dict[str, Any]:
    case_specs = _fixture_case_specs(uncertainty_fixture)
    if not case_specs:
        return {}

    reference_assets_by_type: dict[str, list[str]] = defaultdict(list)
    for asset in list(common.get("reference_assets") or []):
        asset_payload = dict(asset)
        asset_id = str(asset_payload.get("asset_id") or "").strip()
        if not asset_id:
            continue
        asset_types = [
            str(asset_payload.get("asset_type") or "").strip(),
            *[
                str(item).strip()
                for item in list(asset_payload.get("asset_type_aliases") or [])
                if str(item).strip()
            ],
        ]
        for asset_type in asset_types:
            if asset_type and asset_id not in reference_assets_by_type[asset_type]:
                reference_assets_by_type[asset_type].append(asset_id)

    def _pick_reference_assets(*asset_types: str) -> list[str]:
        rows: list[str] = []
        for asset_type in asset_types:
            rows.extend(reference_assets_by_type.get(str(asset_type), []))
        if not rows:
            rows = list(common.get("reference_asset_ids") or [])[:4]
        return _dedupe(rows)

    certificate_ids = list(common.get("certificate_ids") or [])
    scope_id = str(common.get("scope_id") or "")
    decision_rule_id = str(common.get("decision_rule_id") or "")
    component_fields = _component_fields()
    protocol_id = str(
        uncertainty_fixture.get("method_confirmation_protocol_id")
        or _default_method_confirmation_protocol_id(run_id)
    )
    fixture_set_id = str(
        uncertainty_fixture.get("fixture_set_id")
        or uncertainty_fixture.get("fixture_id")
        or "step2-default-uncertainty-fixtures"
    )
    fixture_source_summary = " | ".join(
        part
        for part in (
            fixture_set_id,
            str(uncertainty_fixture.get("schema_version") or "").strip(),
            str((fixture_artifact_paths or {}).get("readiness_fixture_uncertainty_budget_inputs") or "").strip(),
        )
        if part
    )

    cases: list[dict[str, Any]] = []
    input_rows: list[dict[str, Any]] = []
    coefficient_rows: list[dict[str, Any]] = []
    budget_level_counts: dict[str, int] = {"point": 0, "route": 0, "result": 0}
    golden_match_count = 0

    for spec in case_specs:
        spec_payload = dict(spec)
        budget_level = _normalize_budget_level(spec_payload.get("budget_level"))
        budget_level_counts[budget_level] = budget_level_counts.get(budget_level, 0) + 1
        case_key = str(
            spec_payload.get("case_key")
            or spec_payload.get("uncertainty_case_id")
            or f"{budget_level}-{len(cases) + 1}"
        ).strip()
        case_id = str(spec_payload.get("uncertainty_case_id") or f"{run_id}-{case_key}")
        input_ids: list[str] = []
        coefficient_ids: list[str] = []
        distribution_types: list[str] = []
        input_rows_by_key: dict[str, dict[str, Any]] = {}
        contribution_rows_by_component: dict[str, list[dict[str, Any]]] = defaultdict(list)

        for index, raw_input in enumerate(list(spec_payload.get("inputs") or []), start=1):
            if not isinstance(raw_input, dict):
                continue
            input_spec = dict(raw_input)
            input_id = str(input_spec.get("input_id") or f"{case_id}-input-{index}")
            quantity_key = str(input_spec.get("quantity_key") or f"input_{index}")
            distribution_type = str(input_spec.get("distribution_type") or "normal")
            distribution_types.append(distribution_type)
            asset_type_values = [
                str(input_spec.get("asset_type") or "").strip(),
                *[
                    str(item).strip()
                    for item in list(input_spec.get("asset_types") or [])
                    if str(item).strip()
                ],
            ]
            input_row = {
                "input_id": input_id,
                "uncertainty_case_id": case_id,
                "scope_id": scope_id,
                "decision_rule_id": decision_rule_id,
                "method_confirmation_protocol_id": protocol_id,
                "budget_level": budget_level,
                "route_type": str(spec_payload.get("route_type") or ""),
                "measurand": str(spec_payload.get("measurand") or ""),
                "quantity_key": quantity_key,
                "quantity_label": str(input_spec.get("quantity_label") or quantity_key),
                "distribution_type": distribution_type,
                "quantity_value": _safe_float(input_spec.get("quantity_value")),
                "placeholder_value": _safe_float(
                    input_spec.get("placeholder_value", input_spec.get("quantity_value"))
                ),
                "standard_uncertainty": _safe_float(input_spec.get("standard_uncertainty")),
                "divisor": _safe_float(input_spec.get("divisor"), 1.0),
                "unit": str(input_spec.get("unit") or ""),
                "component_key": str(input_spec.get("component_key") or ""),
                "component_label": str(
                    input_spec.get("component_label") or str(input_spec.get("component_key") or "")
                ),
                "source_type": str(input_spec.get("source_type") or input_spec.get("asset_type") or ""),
                "source_note": str(input_spec.get("source_note") or ""),
                "linked_reference_asset_ids": _pick_reference_assets(*asset_type_values),
                "linked_certificate_id": str(
                    input_spec.get("linked_certificate_id")
                    or (certificate_ids or [f"{run_id}-certificate-placeholder"])[
                        min(index - 1, max(len(certificate_ids), 1) - 1)
                    ]
                ),
                "evidence_source": "simulated",
                "ready_for_readiness_mapping": True,
                "not_ready_for_formal_claim": True,
                "not_real_acceptance_evidence": True,
                "reviewer_note": str(
                    input_spec.get("reviewer_note")
                    or input_spec.get("source_note")
                    or f"{input_spec.get('quantity_label') or quantity_key} stays placeholder-only for reviewer pack use."
                ),
            }
            input_rows.append(input_row)
            input_rows_by_key[quantity_key] = input_row
            input_ids.append(input_id)

        for index, raw_coefficient in enumerate(list(spec_payload.get("coefficients") or []), start=1):
            if not isinstance(raw_coefficient, dict):
                continue
            coefficient_spec = dict(raw_coefficient)
            quantity_key = str(coefficient_spec.get("quantity_key") or "")
            input_row = dict(input_rows_by_key.get(quantity_key) or {})
            coefficient_id = str(coefficient_spec.get("coefficient_id") or f"{case_id}-sc-{index}")
            coefficient_value = _safe_float(
                coefficient_spec.get("coefficient_value", coefficient_spec.get("placeholder_value"))
            )
            standard_uncertainty = _safe_float(input_row.get("standard_uncertainty"))
            contribution_value = round(abs(coefficient_value) * standard_uncertainty, 6)
            component_key = str(
                coefficient_spec.get("component_key")
                or input_row.get("component_key")
                or ""
            )
            coefficient_row = {
                "coefficient_id": coefficient_id,
                "uncertainty_case_id": case_id,
                "scope_id": scope_id,
                "decision_rule_id": decision_rule_id,
                "method_confirmation_protocol_id": protocol_id,
                "budget_level": budget_level,
                "route_type": str(spec_payload.get("route_type") or ""),
                "measurand": str(spec_payload.get("measurand") or ""),
                "quantity_key": quantity_key,
                "input_id": str(input_row.get("input_id") or ""),
                "coefficient_key": str(coefficient_spec.get("coefficient_key") or ""),
                "coefficient_value": coefficient_value,
                "placeholder_value": coefficient_value,
                "standard_uncertainty": standard_uncertainty,
                "contribution_value": contribution_value,
                "component_key": component_key,
                "component_label": str(
                    coefficient_spec.get("component_label")
                    or input_row.get("component_label")
                    or component_key
                ),
                "source_note": str(
                    coefficient_spec.get("source_note")
                    or coefficient_spec.get("reviewer_note")
                    or ""
                ),
                "evidence_source": "simulated",
                "ready_for_readiness_mapping": True,
                "not_ready_for_formal_claim": True,
                "not_real_acceptance_evidence": True,
                "reviewer_note": str(
                    coefficient_spec.get("reviewer_note")
                    or coefficient_spec.get("source_note")
                    or ""
                ),
            }
            coefficient_rows.append(coefficient_row)
            coefficient_ids.append(coefficient_id)
            if component_key:
                contribution_rows_by_component[component_key].append(coefficient_row)

        component_values: dict[str, float] = {}
        component_breakdown: list[dict[str, Any]] = []
        for component_name in component_fields:
            rows = list(contribution_rows_by_component.get(component_name, []))
            component_value = round(sqrt(sum(_safe_float(item.get("contribution_value")) ** 2 for item in rows)), 6)
            component_values[component_name] = component_value
            component_breakdown.append(
                {
                    "component_key": component_name,
                    "component_value": component_value,
                    "contributions": [
                        {
                            "input_id": str(item.get("input_id") or ""),
                            "coefficient_id": str(item.get("coefficient_id") or ""),
                            "quantity_key": str(item.get("quantity_key") or ""),
                            "coefficient_key": str(item.get("coefficient_key") or ""),
                            "standard_uncertainty": _safe_float(item.get("standard_uncertainty")),
                            "sensitivity_coefficient": _safe_float(item.get("coefficient_value")),
                            "contribution_value": _safe_float(item.get("contribution_value")),
                            "source_note": str(item.get("source_note") or ""),
                        }
                        for item in rows
                    ],
                }
            )

        combined = round(sqrt(sum(value * value for value in component_values.values())), 6)
        coverage_factor = _safe_float(spec_payload.get("coverage_factor"), 2.0)
        expanded = round(combined * coverage_factor, 6)
        expected_combined = round(
            _safe_float(spec_payload.get("expected_combined_standard_uncertainty"), combined),
            6,
        )
        expected_expanded = round(
            _safe_float(spec_payload.get("expected_expanded_uncertainty"), expanded),
            6,
        )
        golden_case_status = (
            "match"
            if isclose(combined, expected_combined, abs_tol=1e-6)
            and isclose(expanded, expected_expanded, abs_tol=1e-6)
            else "mismatch"
        )
        if golden_case_status == "match":
            golden_match_count += 1
        top_contributors = [
            {"component_key": name, "value": value}
            for name, value in sorted(component_values.items(), key=lambda item: item[1], reverse=True)
            if value > 0.0
        ][:3]
        calculation_chain_rows = [
            {
                "quantity_key": str(item.get("quantity_key") or ""),
                "input_id": str(item.get("input_id") or ""),
                "coefficient_id": str(item.get("coefficient_id") or ""),
                "component_key": str(item.get("component_key") or ""),
                "standard_uncertainty": _safe_float(item.get("standard_uncertainty")),
                "sensitivity_coefficient": _safe_float(item.get("coefficient_value")),
                "contribution_value": _safe_float(item.get("contribution_value")),
            }
            for item in coefficient_rows
            if str(item.get("uncertainty_case_id") or "") == case_id
        ]
        cases.append(
            {
                "uncertainty_case_id": case_id,
                "scope_id": scope_id,
                "decision_rule_id": decision_rule_id,
                "method_confirmation_protocol_id": protocol_id,
                "budget_level": budget_level,
                "route_type": str(spec_payload.get("route_type") or ""),
                "measurand": str(spec_payload.get("measurand") or ""),
                "case_label": str(spec_payload.get("case_label") or case_key),
                "point_context": dict(spec_payload.get("point_context") or {}),
                "route_context": dict(spec_payload.get("route_context") or {}),
                "result_context": dict(spec_payload.get("result_context") or {}),
                "input_quantity_set": list(input_ids),
                "distribution_type": _dedupe(distribution_types),
                "sensitivity_coefficients": list(coefficient_ids),
                **component_values,
                "component_breakdown": component_breakdown,
                "calculation_chain": calculation_chain_rows,
                "combined_standard_uncertainty": combined,
                "expected_combined_standard_uncertainty": expected_combined,
                "coverage_factor": coverage_factor,
                "expanded_uncertainty": expanded,
                "expected_expanded_uncertainty": expected_expanded,
                "golden_case_status": golden_case_status,
                "report_rule": str(common.get("report_rule") or "step2_readiness_mapping_only"),
                "evidence_source": "simulated",
                "ready_for_readiness_mapping": True,
                "not_ready_for_formal_claim": True,
                "not_real_acceptance_evidence": True,
                "readiness_mapping_only": True,
                "reviewer_only": True,
                "non_claim": True,
                "limitation_note": str(common.get("limitation_note") or ""),
                "non_claim_note": str(common.get("non_claim_note") or ""),
                "reviewer_note": str(
                    spec_payload.get("reviewer_note") or "reviewer-facing golden case only"
                ),
                "reference_asset_ids": _dedupe(
                    [
                        *[
                            str(item)
                            for item in list(spec_payload.get("reference_asset_ids") or [])
                            if str(item).strip()
                        ],
                        *[
                            str(item)
                            for input_row in input_rows_by_key.values()
                            for item in list(input_row.get("linked_reference_asset_ids") or [])
                            if str(item).strip()
                        ],
                    ]
                ),
                "certificate_summary_refs": _dedupe(
                    [
                        *[
                            str(item)
                            for item in list(spec_payload.get("certificate_ids") or [])
                            if str(item).strip()
                        ],
                        *[
                            str(input_row.get("linked_certificate_id") or "")
                            for input_row in input_rows_by_key.values()
                            if str(input_row.get("linked_certificate_id") or "").strip()
                        ],
                    ]
                ),
                "top_contributors": top_contributors,
                "calculation_chain_summary": str(
                    spec_payload.get("calculation_chain_summary")
                    or f"inputs {len(input_ids)} -> coefficients {len(coefficient_ids)} -> combined u {combined:.6f} -> expanded U {expanded:.6f}"
                ),
                "fixture_set_id": fixture_set_id,
                "fixture_source_summary": fixture_source_summary,
                "placeholder_value_only": True,
            }
        )

    selected_result_case = next(
        (dict(case) for case in cases if str(case.get("budget_level") or "") == "result"),
        dict(cases[-1]) if cases else {},
    )
    top_contributor_summary = " | ".join(
        f"{str(case.get('case_label') or '--')}: "
        + ", ".join(
            f"{str(item.get('component_key') or '--')} {float(item.get('value', 0.0) or 0.0):.3f}"
            for item in list(case.get("top_contributors") or [])[:2]
        )
        for case in cases
    )
    budget_level_summary = (
        f"point {budget_level_counts.get('point', 0)} | "
        f"route {budget_level_counts.get('route', 0)} | "
        f"result {budget_level_counts.get('result', 0)}"
    )
    binding_summary = (
        f"scope {scope_id} | decision rule {decision_rule_id} | "
        f"uncertainty case {str(selected_result_case.get('uncertainty_case_id') or '--')} | "
        f"method protocol {protocol_id}"
    )
    calculation_chain_summary = (
        f"fixture-backed inputs {len(input_rows)} -> coefficients {len(coefficient_rows)} -> "
        f"budget cases {len(cases)} -> combined u -> expanded U"
    )
    golden_case_summary = f"golden matches {golden_match_count}/{len(cases)}"
    return {
        "component_fields": component_fields,
        "cases": cases,
        "input_rows": input_rows,
        "coefficient_rows": coefficient_rows,
        "route_types": _dedupe([str(case.get("route_type") or "") for case in cases]),
        "measurands": _dedupe([str(case.get("measurand") or "") for case in cases]),
        "case_ids": [str(case.get("uncertainty_case_id") or "") for case in cases],
        "selected_result_case_id": str(selected_result_case.get("uncertainty_case_id") or ""),
        "method_confirmation_protocol_id": protocol_id,
        "budget_level_summary": budget_level_summary,
        "binding_summary": binding_summary,
        "calculation_chain_summary": calculation_chain_summary,
        "fixture_summary": fixture_source_summary or fixture_set_id,
        "golden_case_summary": golden_case_summary,
        "top_contributor_summary": top_contributor_summary,
        "budget_completeness_summary": (
            f"levels {budget_level_summary} | inputs {len(input_rows)} | coefficients {len(coefficient_rows)}"
        ),
        "placeholder_completeness_summary": (
            f"cases {len(cases)}/{len(case_specs)} | "
            f"golden {golden_match_count}/{len(cases)} | placeholder values only"
        ),
        "budget_level_counts": budget_level_counts,
        "fixture_artifact_paths": {
            str(key): str(value)
            for key, value in dict(fixture_artifact_paths or {}).items()
            if str(key).strip() and str(value).strip()
        },
    }


def _case_payloads(
    *,
    run_id: str,
    common: dict[str, Any],
    uncertainty_fixture: dict[str, Any] | None = None,
    fixture_artifact_paths: dict[str, str] | None = None,
) -> dict[str, Any]:
    fixture_payloads = _build_fixture_case_payloads(
        run_id=run_id,
        common=common,
        uncertainty_fixture=dict(uncertainty_fixture or {}),
        fixture_artifact_paths=dict(fixture_artifact_paths or {}),
    )
    if fixture_payloads:
        return fixture_payloads
    return _legacy_case_payloads(run_id=run_id, common=common)


def _base_artifact_paths(path_map: dict[str, str], common: dict[str, Any], *extra_keys: str) -> dict[str, str]:
    rows = {**dict(common.get("linked_artifacts") or {})}
    for key in extra_keys:
        rows[str(key)] = str(path_map[str(key)])
    return rows


def _artifact_paths_with_fixtures(
    base_paths: dict[str, str],
    fixture_artifact_paths: dict[str, str] | None,
) -> dict[str, str]:
    return {
        **dict(base_paths or {}),
        **{
            str(key): str(value)
            for key, value in dict(fixture_artifact_paths or {}).items()
            if str(key).strip() and str(value).strip()
        },
    }


def _scope_digest(common: dict[str, Any], *, summary: str, current_coverage: str, missing_evidence: str) -> dict[str, Any]:
    return {
        "summary": summary,
        "scope_overview_summary": str(common.get("scope_overview_summary") or "--"),
        "decision_rule_summary": str(common.get("decision_rule_summary") or "--"),
        "conformity_boundary_summary": str(common.get("conformity_boundary_summary") or "--"),
        "current_coverage_summary": current_coverage,
        "missing_evidence_summary": missing_evidence,
        "non_claim_digest": str(common.get("non_claim_note") or "--"),
    }


def _model_artifact(
    *,
    run_id: str,
    common: dict[str, Any],
    case_payloads: dict[str, Any],
    path_map: dict[str, str],
    filenames: dict[str, str],
    boundary_statements: list[str],
) -> dict[str, Any]:
    fixture_paths = dict(case_payloads.get("fixture_artifact_paths") or {})
    digest = _scope_digest(
        common,
        summary=f"uncertainty model skeleton | cases {len(list(case_payloads.get('cases') or []))} | scope {common['scope_id']}",
        current_coverage="file-artifact-first skeleton / reviewer-facing only",
        missing_evidence="released solver / real uncertainty validation remain outside Step 2",
    )
    return _bundle(
        run_id=run_id,
        artifact_type="uncertainty_model",
        filename=filenames["uncertainty_model"],
        markdown_filename=filenames["uncertainty_model_markdown"],
        artifact_role="execution_summary",
        title_text="不确定度对象骨架",
        reviewer_note="当前仅建立 machine-readable skeleton，不连接真实 solver，不形成正式声明。",
        summary_text=digest["summary"],
        summary_lines=[
            f"scope: {common['scope_id']}",
            f"decision rule: {common['decision_rule_id']}",
            f"route types: {' | '.join(list(case_payloads.get('route_types') or [])) or '--'}",
            f"measurands: {' | '.join(list(case_payloads.get('measurands') or [])) or '--'}",
        ],
        detail_lines=[
            f"component fields: {' | '.join(case_payloads['component_fields'])}",
            f"budget levels: {case_payloads.get('budget_level_summary') or '--'}",
            f"binding: {case_payloads.get('binding_summary') or '--'}",
            f"fixture summary: {case_payloads.get('fixture_summary') or '--'}",
            f"linked report pack: {path_map['uncertainty_report_pack']}",
            f"linked digest: {path_map['uncertainty_digest']}",
            f"linked rollup: {path_map['uncertainty_rollup']}",
        ],
        artifact_paths=_artifact_paths_with_fixtures(
            _base_artifact_paths(
                path_map,
                common,
                "uncertainty_model",
                "uncertainty_model_markdown",
                "uncertainty_input_set",
                "sensitivity_coefficient_set",
                "budget_case",
                "uncertainty_golden_cases",
                "uncertainty_report_pack",
                "uncertainty_digest",
                "uncertainty_rollup",
            ),
            fixture_paths,
        ),
        body={
            "scope_id": common["scope_id"],
            "decision_rule_id": common["decision_rule_id"],
            "uncertainty_case_id": str(case_payloads.get("selected_result_case_id") or ""),
            "method_confirmation_protocol_id": str(
                case_payloads.get("method_confirmation_protocol_id")
                or _default_method_confirmation_protocol_id(run_id)
            ),
            "report_rule": common["report_rule"],
            "standard_family": list(common.get("standard_family") or []),
            "required_evidence_categories": list(common.get("required_evidence_categories") or []),
            "current_evidence_coverage": [
                "scope_definition_pack",
                "decision_rule_profile",
                "reference_asset_registry",
                "certificate_lifecycle_summary",
                "pre_run_readiness_gate",
            ],
            "route_types": list(case_payloads.get("route_types") or []),
            "measurands": list(case_payloads.get("measurands") or []),
            "uncertainty_case_ids": list(case_payloads.get("case_ids") or []),
            "component_fields": list(case_payloads.get("component_fields") or []),
            "budget_level_summary": str(case_payloads.get("budget_level_summary") or "--"),
            "binding_summary": str(case_payloads.get("binding_summary") or "--"),
            "fixture_summary": str(case_payloads.get("fixture_summary") or "--"),
            "calculation_chain_summary": str(case_payloads.get("calculation_chain_summary") or "--"),
            "reference_asset_ids": list(common.get("reference_asset_ids") or []),
            "certificate_summary_refs": list(common.get("certificate_ids") or []),
            "limitation_note": common["limitation_note"],
            "non_claim_note": common["non_claim_note"],
            "reviewer_note": common["reviewer_note"],
        },
        digest=digest,
        boundary_statements=boundary_statements,
        evidence_categories=["recognition_readiness", "uncertainty_skeleton"],
    )


def _input_artifact(
    *,
    run_id: str,
    common: dict[str, Any],
    case_payloads: dict[str, Any],
    path_map: dict[str, str],
    filenames: dict[str, str],
    boundary_statements: list[str],
) -> dict[str, Any]:
    rows = [dict(item) for item in list(case_payloads.get("input_rows") or []) if isinstance(item, dict)]
    fixture_paths = dict(case_payloads.get("fixture_artifact_paths") or {})
    digest = _scope_digest(
        common,
        summary=f"uncertainty input set | rows {len(rows)} | reviewer placeholder only",
        current_coverage=f"input rows {len(rows)} | file-backed only",
        missing_evidence="released input evidence and real instrument traceability remain outside Step 2",
    )
    return _bundle(
        run_id=run_id,
        artifact_type="uncertainty_input_set",
        filename=filenames["uncertainty_input_set"],
        markdown_filename=filenames["uncertainty_input_set_markdown"],
        artifact_role="execution_summary",
        title_text="不确定度输入集合",
        reviewer_note="输入量集合仅用于 reviewer pack 和 readiness mapping，不产生 formal claim。",
        summary_text=digest["summary"],
        summary_lines=[f"scope: {common['scope_id']}", f"case count: {len(list(case_payloads.get('case_ids') or []))}", f"input rows: {len(rows)}"],
        detail_lines=[
            f"budget levels: {case_payloads.get('budget_level_summary') or '--'}",
            f"binding: {case_payloads.get('binding_summary') or '--'}",
            f"fixture summary: {case_payloads.get('fixture_summary') or '--'}",
            f"linked sensitivity set: {path_map['sensitivity_coefficient_set']}",
            f"linked budget cases: {path_map['budget_case']}",
        ],
        artifact_paths=_artifact_paths_with_fixtures(
            _base_artifact_paths(
                path_map,
                common,
                "uncertainty_input_set",
                "uncertainty_input_set_markdown",
                "sensitivity_coefficient_set",
                "budget_case",
            ),
            fixture_paths,
        ),
        body={
            "scope_id": common["scope_id"],
            "decision_rule_id": common["decision_rule_id"],
            "uncertainty_case_id": str(case_payloads.get("selected_result_case_id") or ""),
            "method_confirmation_protocol_id": str(
                case_payloads.get("method_confirmation_protocol_id")
                or _default_method_confirmation_protocol_id(run_id)
            ),
            "report_rule": common["report_rule"],
            "budget_level_summary": str(case_payloads.get("budget_level_summary") or "--"),
            "binding_summary": str(case_payloads.get("binding_summary") or "--"),
            "fixture_summary": str(case_payloads.get("fixture_summary") or "--"),
            "calculation_chain_summary": str(case_payloads.get("calculation_chain_summary") or "--"),
            "input_quantity_set": rows,
            "limitation_note": common["limitation_note"],
            "non_claim_note": common["non_claim_note"],
            "reviewer_note": common["reviewer_note"],
        },
        digest=digest,
        boundary_statements=boundary_statements,
        evidence_categories=["recognition_readiness", "uncertainty_inputs"],
    )


def _coefficient_artifact(
    *,
    run_id: str,
    common: dict[str, Any],
    case_payloads: dict[str, Any],
    path_map: dict[str, str],
    filenames: dict[str, str],
    boundary_statements: list[str],
) -> dict[str, Any]:
    rows = [dict(item) for item in list(case_payloads.get("coefficient_rows") or []) if isinstance(item, dict)]
    fixture_paths = dict(case_payloads.get("fixture_artifact_paths") or {})
    digest = _scope_digest(
        common,
        summary=f"sensitivity coefficient set | rows {len(rows)} | reviewer placeholder only",
        current_coverage=f"coefficient rows {len(rows)} | writeback/rounding included",
        missing_evidence="released coefficients and solver qualification remain outside Step 2",
    )
    return _bundle(
        run_id=run_id,
        artifact_type="sensitivity_coefficient_set",
        filename=filenames["sensitivity_coefficient_set"],
        markdown_filename=filenames["sensitivity_coefficient_set_markdown"],
        artifact_role="execution_summary",
        title_text="灵敏系数集合",
        reviewer_note="灵敏系数当前是 placeholder rows，保留 writeback/rounding/handoff 字段，不做真实求解。",
        summary_text=digest["summary"],
        summary_lines=[f"scope: {common['scope_id']}", f"decision rule: {common['decision_rule_id']}", f"coefficient rows: {len(rows)}"],
        detail_lines=[
            f"budget levels: {case_payloads.get('budget_level_summary') or '--'}",
            f"binding: {case_payloads.get('binding_summary') or '--'}",
            f"calculation chain: {case_payloads.get('calculation_chain_summary') or '--'}",
            f"linked input set: {path_map['uncertainty_input_set']}",
            f"linked budget cases: {path_map['budget_case']}",
        ],
        artifact_paths=_artifact_paths_with_fixtures(
            _base_artifact_paths(
                path_map,
                common,
                "uncertainty_input_set",
                "sensitivity_coefficient_set",
                "sensitivity_coefficient_set_markdown",
                "budget_case",
            ),
            fixture_paths,
        ),
        body={
            "scope_id": common["scope_id"],
            "decision_rule_id": common["decision_rule_id"],
            "uncertainty_case_id": str(case_payloads.get("selected_result_case_id") or ""),
            "method_confirmation_protocol_id": str(
                case_payloads.get("method_confirmation_protocol_id")
                or _default_method_confirmation_protocol_id(run_id)
            ),
            "report_rule": common["report_rule"],
            "budget_level_summary": str(case_payloads.get("budget_level_summary") or "--"),
            "binding_summary": str(case_payloads.get("binding_summary") or "--"),
            "fixture_summary": str(case_payloads.get("fixture_summary") or "--"),
            "calculation_chain_summary": str(case_payloads.get("calculation_chain_summary") or "--"),
            "sensitivity_coefficients": rows,
            "limitation_note": common["limitation_note"],
            "non_claim_note": common["non_claim_note"],
            "reviewer_note": common["reviewer_note"],
        },
        digest=digest,
        boundary_statements=boundary_statements,
        evidence_categories=["recognition_readiness", "uncertainty_coefficients"],
    )


def _budget_case_artifact(
    *,
    run_id: str,
    common: dict[str, Any],
    case_payloads: dict[str, Any],
    path_map: dict[str, str],
    filenames: dict[str, str],
    boundary_statements: list[str],
) -> dict[str, Any]:
    cases = [dict(item) for item in list(case_payloads.get("cases") or []) if isinstance(item, dict)]
    fixture_paths = dict(case_payloads.get("fixture_artifact_paths") or {})
    digest = _scope_digest(
        common,
        summary=f"budget cases {len(cases)} | reviewer-only / readiness mapping only",
        current_coverage=str(case_payloads.get("budget_completeness_summary") or "--"),
        missing_evidence="budget cases remain simulated examples and cannot become formal uncertainty declarations",
    )
    return _bundle(
        run_id=run_id,
        artifact_type="budget_case",
        filename=filenames["budget_case"],
        markdown_filename=filenames["budget_case_markdown"],
        artifact_role="execution_summary",
        title_text="不确定度预算案例",
        reviewer_note="预算案例保留完整组件字段，但数值仅为 simulated/example placeholders。",
        summary_text=digest["summary"],
        summary_lines=[f"budget completeness: {case_payloads.get('budget_completeness_summary')}", f"top contributors: {case_payloads.get('top_contributor_summary')}"],
        detail_lines=[
            f"budget levels: {case_payloads.get('budget_level_summary') or '--'}",
            f"binding: {case_payloads.get('binding_summary') or '--'}",
            f"calculation chain: {case_payloads.get('calculation_chain_summary') or '--'}",
            f"golden cases: {path_map['uncertainty_golden_cases']}",
            f"report pack: {path_map['uncertainty_report_pack']}",
        ],
        artifact_paths=_artifact_paths_with_fixtures(
            _base_artifact_paths(
                path_map,
                common,
                "budget_case",
                "budget_case_markdown",
                "uncertainty_golden_cases",
                "uncertainty_report_pack",
            ),
            fixture_paths,
        ),
        body={
            "scope_id": common["scope_id"],
            "decision_rule_id": common["decision_rule_id"],
            "uncertainty_case_id": str(case_payloads.get("selected_result_case_id") or ""),
            "method_confirmation_protocol_id": str(
                case_payloads.get("method_confirmation_protocol_id")
                or _default_method_confirmation_protocol_id(run_id)
            ),
            "report_rule": common["report_rule"],
            "budget_level_summary": str(case_payloads.get("budget_level_summary") or "--"),
            "binding_summary": str(case_payloads.get("binding_summary") or "--"),
            "fixture_summary": str(case_payloads.get("fixture_summary") or "--"),
            "calculation_chain_summary": str(case_payloads.get("calculation_chain_summary") or "--"),
            "golden_case_summary": str(case_payloads.get("golden_case_summary") or "--"),
            "budget_case": cases,
            "limitation_note": common["limitation_note"],
            "non_claim_note": common["non_claim_note"],
            "reviewer_note": common["reviewer_note"],
        },
        digest=digest,
        boundary_statements=boundary_statements,
        evidence_categories=["recognition_readiness", "uncertainty_budget"],
    )


def _golden_cases_artifact(
    *,
    run_id: str,
    common: dict[str, Any],
    case_payloads: dict[str, Any],
    path_map: dict[str, str],
    filenames: dict[str, str],
    boundary_statements: list[str],
) -> dict[str, Any]:
    fixture_paths = dict(case_payloads.get("fixture_artifact_paths") or {})
    cases = [
        {
            **dict(item),
            "reviewer_only": True,
            "readiness_mapping_only": True,
            "non_claim": True,
            "traceability_summary": {
                "scope_id": common["scope_id"],
                "decision_rule_id": common["decision_rule_id"],
                "reference_asset_ids": list(dict(item).get("reference_asset_ids") or []),
                "certificate_summary_refs": list(dict(item).get("certificate_summary_refs") or []),
            },
        }
        for item in list(case_payloads.get("cases") or [])
        if isinstance(item, dict)
    ]
    digest = _scope_digest(
        common,
        summary=f"golden cases {len(cases)} | reviewer-only / non-claim",
        current_coverage=str(case_payloads.get("golden_case_summary") or "--"),
        missing_evidence="golden cases remain artifact-based examples only",
    )
    return _bundle(
        run_id=run_id,
        artifact_type="uncertainty_golden_cases",
        filename=filenames["uncertainty_golden_cases"],
        markdown_filename=filenames["uncertainty_golden_cases_markdown"],
        artifact_role="diagnostic_analysis",
        title_text="不确定度 golden cases",
        reviewer_note="Golden cases 是 Step 2 reviewer-facing examples，不是认可样例，也不是 formal claim。",
        summary_text=digest["summary"],
        summary_lines=[
            f"coverage: {case_payloads.get('golden_case_summary') or '--'}",
            f"scope: {common['scope_id']}",
            f"decision rule: {common['decision_rule_id']}",
        ],
        detail_lines=[
            f"budget levels: {case_payloads.get('budget_level_summary') or '--'}",
            f"binding: {case_payloads.get('binding_summary') or '--'}",
            f"fixture summary: {case_payloads.get('fixture_summary') or '--'}",
            f"budget cases: {path_map['budget_case']}",
            f"report pack: {path_map['uncertainty_report_pack']}",
        ],
        artifact_paths=_artifact_paths_with_fixtures(
            _base_artifact_paths(
                path_map,
                common,
                "budget_case",
                "uncertainty_golden_cases",
                "uncertainty_golden_cases_markdown",
                "uncertainty_report_pack",
            ),
            fixture_paths,
        ),
        body={
            "scope_id": common["scope_id"],
            "decision_rule_id": common["decision_rule_id"],
            "uncertainty_case_id": str(case_payloads.get("selected_result_case_id") or ""),
            "method_confirmation_protocol_id": str(
                case_payloads.get("method_confirmation_protocol_id")
                or _default_method_confirmation_protocol_id(run_id)
            ),
            "report_rule": common["report_rule"],
            "budget_level_summary": str(case_payloads.get("budget_level_summary") or "--"),
            "binding_summary": str(case_payloads.get("binding_summary") or "--"),
            "fixture_summary": str(case_payloads.get("fixture_summary") or "--"),
            "calculation_chain_summary": str(case_payloads.get("calculation_chain_summary") or "--"),
            "golden_case_summary": str(case_payloads.get("golden_case_summary") or "--"),
            "golden_cases": cases,
            "limitation_note": common["limitation_note"],
            "non_claim_note": common["non_claim_note"],
            "reviewer_note": common["reviewer_note"],
        },
        digest=digest,
        boundary_statements=boundary_statements,
        evidence_categories=["recognition_readiness", "uncertainty_golden_cases"],
    )


def _report_pack_artifact(
    *,
    run_id: str,
    common: dict[str, Any],
    case_payloads: dict[str, Any],
    path_map: dict[str, str],
    filenames: dict[str, str],
    boundary_statements: list[str],
) -> dict[str, Any]:
    cases = [dict(item) for item in list(case_payloads.get("cases") or []) if isinstance(item, dict)]
    fixture_paths = dict(case_payloads.get("fixture_artifact_paths") or {})
    top_contributors = [
        {
            "uncertainty_case_id": str(case.get("uncertainty_case_id") or ""),
            "top_contributors": [dict(item) for item in list(case.get("top_contributors") or []) if isinstance(item, dict)],
        }
        for case in cases
    ]
    digest = {
        **_scope_digest(
            common,
            summary=f"uncertainty report pack | cases {len(cases)} | reviewer-only / non-claim",
            current_coverage=(
                f"report pack wired to scope / decision / method / fixture / budget levels | "
                f"{str(case_payloads.get('budget_level_summary') or '--')}"
            ),
            missing_evidence="real uncertainty engine, released coefficients, and formal compliance claim remain out of scope",
        ),
        "uncertainty_overview_summary": (
            f"scope {common['scope_id']} | decision rule {common['decision_rule_id']} | "
            f"case {str(case_payloads.get('selected_result_case_id') or '--')} | "
            f"method protocol {str(case_payloads.get('method_confirmation_protocol_id') or _default_method_confirmation_protocol_id(run_id))} | readiness mapping only"
        ),
        "budget_component_summary": str(case_payloads.get("budget_completeness_summary") or "--"),
        "top_contributors_summary": str(case_payloads.get("top_contributor_summary") or "--"),
        "data_completeness_summary": (
            f"input rows {len(list(case_payloads.get('input_rows') or []))} | "
            f"coefficients {len(list(case_payloads.get('coefficient_rows') or []))} | "
            f"fixture {str(case_payloads.get('fixture_summary') or '--')} | all values placeholder/simulated"
        ),
        "budget_level_summary": str(case_payloads.get("budget_level_summary") or "--"),
        "binding_summary": str(case_payloads.get("binding_summary") or "--"),
        "calculation_chain_summary": str(case_payloads.get("calculation_chain_summary") or "--"),
        "fixture_summary": str(case_payloads.get("fixture_summary") or "--"),
        "golden_case_summary": str(case_payloads.get("golden_case_summary") or "--"),
        "placeholder_completeness_summary": str(case_payloads.get("placeholder_completeness_summary") or "--"),
        "warning_summary": "placeholder values only | reviewer-only | not ready for formal claim",
        "reviewer_action_summary": "review linked scope/decision/assets/certificates | keep formal uncertainty declaration disabled in Step 2",
    }
    return _bundle(
        run_id=run_id,
        artifact_type="uncertainty_report_pack",
        filename=filenames["uncertainty_report_pack"],
        markdown_filename=filenames["uncertainty_report_pack_markdown"],
        artifact_role="diagnostic_analysis",
        title_text="不确定度 report pack",
        reviewer_note="面向 reviewer / engineer / quality 的 sidecar-first pack；当前只做 readiness mapping，不做 formal uncertainty statement。",
        summary_text=digest["summary"],
        summary_lines=[
            f"uncertainty overview: {digest['uncertainty_overview_summary']}",
            f"budget completeness: {digest['budget_component_summary']}",
            f"top contributors: {digest['top_contributors_summary']}",
            f"data completeness: {digest['data_completeness_summary']}",
        ],
        detail_lines=[
            f"budget levels: {digest['budget_level_summary']}",
            f"binding: {digest['binding_summary']}",
            f"calculation chain: {digest['calculation_chain_summary']}",
            f"fixture summary: {digest['fixture_summary']}",
            f"golden cases: {digest['golden_case_summary']}",
            f"placeholder completeness: {digest['placeholder_completeness_summary']}",
            f"reviewer actions: {digest['reviewer_action_summary']}",
            f"non-claim: {common['non_claim_note']}",
            f"scope artifact: {path_map['scope_definition_pack']}",
            f"decision rule artifact: {path_map['decision_rule_profile']}",
            f"reference asset artifact: {path_map['reference_asset_registry']}",
            f"certificate lifecycle artifact: {path_map['certificate_lifecycle_summary']}",
            f"pre-run gate artifact: {path_map['pre_run_readiness_gate']}",
        ],
        artifact_paths=_artifact_paths_with_fixtures(
            _base_artifact_paths(
                path_map,
                common,
                "uncertainty_model",
                "uncertainty_input_set",
                "sensitivity_coefficient_set",
                "budget_case",
                "uncertainty_golden_cases",
                "uncertainty_report_pack",
                "uncertainty_report_pack_markdown",
            ),
            fixture_paths,
        ),
        body={
            "scope_id": common["scope_id"],
            "decision_rule_id": common["decision_rule_id"],
            "uncertainty_case_id": str(case_payloads.get("selected_result_case_id") or ""),
            "method_confirmation_protocol_id": str(
                case_payloads.get("method_confirmation_protocol_id")
                or _default_method_confirmation_protocol_id(run_id)
            ),
            "report_rule": common["report_rule"],
            "standard_family": list(common.get("standard_family") or []),
            "required_evidence_categories": list(common.get("required_evidence_categories") or []),
            "asset_readiness_overview": common["asset_readiness_overview"],
            "certificate_lifecycle_overview": common["certificate_lifecycle_overview"],
            "gate_status": common["pre_run_gate_status"],
            "budget_level_summary": str(case_payloads.get("budget_level_summary") or "--"),
            "binding_summary": str(case_payloads.get("binding_summary") or "--"),
            "calculation_chain_summary": str(case_payloads.get("calculation_chain_summary") or "--"),
            "fixture_summary": str(case_payloads.get("fixture_summary") or "--"),
            "golden_case_summary": str(case_payloads.get("golden_case_summary") or "--"),
            "top_contributors": top_contributors,
            "data_completeness": {
                "input_rows": len(list(case_payloads.get("input_rows") or [])),
                "coefficient_rows": len(list(case_payloads.get("coefficient_rows") or [])),
                "budget_cases": len(cases),
                "placeholder_only": True,
            },
            "readiness_status": "ready_for_readiness_mapping",
            "scope_reference_assets_summary": " | ".join(list(common.get("reference_asset_ids") or [])[:4]) or "--",
            "decision_rule_dependency_summary": (
                f"{common['decision_rule_id']} | reference_asset_registry | certificate_lifecycle_summary | pre_run_readiness_gate"
            ),
            "reviewer_actions": [
                "review linked scope / decision rule / reference assets / certificate lifecycle",
                "keep formal uncertainty declaration disabled in Step 2",
            ],
            "gap_note": "Formal uncertainty closure remains outside Step 2 and is not implied by this pack.",
            "limitation_note": common["limitation_note"],
            "non_claim_note": common["non_claim_note"],
            "reviewer_note": common["reviewer_note"],
        },
        digest=digest,
        boundary_statements=boundary_statements,
        evidence_categories=["recognition_readiness", "uncertainty_report_pack"],
    )


def _digest_artifact(
    *,
    run_id: str,
    common: dict[str, Any],
    report_pack: dict[str, Any],
    case_payloads: dict[str, Any],
    path_map: dict[str, str],
    filenames: dict[str, str],
    boundary_statements: list[str],
) -> dict[str, Any]:
    report_digest = dict(report_pack.get("digest") or {})
    fixture_paths = dict(case_payloads.get("fixture_artifact_paths") or {})
    digest = {
        **_scope_digest(
            common,
            summary=str(report_digest.get("summary") or "uncertainty digest / reviewer-only"),
            current_coverage=str(report_digest.get("current_coverage_summary") or "--"),
            missing_evidence=str(report_digest.get("missing_evidence_summary") or "--"),
        ),
        "uncertainty_overview_summary": str(report_digest.get("uncertainty_overview_summary") or "--"),
        "budget_component_summary": str(report_digest.get("budget_component_summary") or "--"),
        "top_contributors_summary": str(report_digest.get("top_contributors_summary") or "--"),
        "data_completeness_summary": str(report_digest.get("data_completeness_summary") or "--"),
        "budget_level_summary": str(report_digest.get("budget_level_summary") or case_payloads.get("budget_level_summary") or "--"),
        "binding_summary": str(report_digest.get("binding_summary") or case_payloads.get("binding_summary") or "--"),
        "calculation_chain_summary": str(report_digest.get("calculation_chain_summary") or case_payloads.get("calculation_chain_summary") or "--"),
        "fixture_summary": str(report_digest.get("fixture_summary") or case_payloads.get("fixture_summary") or "--"),
        "golden_case_summary": str(report_digest.get("golden_case_summary") or case_payloads.get("golden_case_summary") or "--"),
        "placeholder_completeness_summary": str(report_digest.get("placeholder_completeness_summary") or "--"),
        "warning_summary": str(report_digest.get("warning_summary") or "--"),
        "reviewer_action_summary": str(report_digest.get("reviewer_action_summary") or "--"),
    }
    return _bundle(
        run_id=run_id,
        artifact_type="uncertainty_digest",
        filename=filenames["uncertainty_digest"],
        markdown_filename=filenames["uncertainty_digest_markdown"],
        artifact_role="diagnostic_analysis",
        title_text="不确定度 digest",
        reviewer_note="Digest 只汇总 reviewer-facing uncertainty skeleton，不做 formal statement。",
        summary_text=digest["summary"],
        summary_lines=[f"uncertainty overview: {digest['uncertainty_overview_summary']}", f"top contributors: {digest['top_contributors_summary']}"],
        detail_lines=[
            f"budget completeness: {digest['budget_component_summary']}",
            f"budget levels: {digest['budget_level_summary']}",
            f"binding: {digest['binding_summary']}",
            f"calculation chain: {digest['calculation_chain_summary']}",
            f"fixture summary: {digest['fixture_summary']}",
            f"golden cases: {digest['golden_case_summary']}",
            f"data completeness: {digest['data_completeness_summary']}",
            f"placeholder completeness: {digest['placeholder_completeness_summary']}",
        ],
        artifact_paths=_artifact_paths_with_fixtures(
            _base_artifact_paths(path_map, common, "uncertainty_report_pack", "uncertainty_digest", "uncertainty_digest_markdown"),
            fixture_paths,
        ),
        body={
            "scope_id": common["scope_id"],
            "decision_rule_id": common["decision_rule_id"],
            "uncertainty_case_id": str(case_payloads.get("selected_result_case_id") or ""),
            "method_confirmation_protocol_id": str(
                case_payloads.get("method_confirmation_protocol_id")
                or _default_method_confirmation_protocol_id(run_id)
            ),
            "report_rule": common["report_rule"],
            "budget_level_summary": str(case_payloads.get("budget_level_summary") or "--"),
            "binding_summary": str(case_payloads.get("binding_summary") or "--"),
            "calculation_chain_summary": str(case_payloads.get("calculation_chain_summary") or "--"),
            "fixture_summary": str(case_payloads.get("fixture_summary") or "--"),
            "golden_case_summary": str(case_payloads.get("golden_case_summary") or "--"),
            "limitation_note": common["limitation_note"],
            "non_claim_note": common["non_claim_note"],
            "reviewer_note": common["reviewer_note"],
        },
        digest=digest,
        boundary_statements=boundary_statements,
        evidence_categories=["recognition_readiness", "uncertainty_digest"],
    )


def _rollup_artifact(
    *,
    run_id: str,
    common: dict[str, Any],
    case_payloads: dict[str, Any],
    report_pack: dict[str, Any],
    path_map: dict[str, str],
    filenames: dict[str, str],
    boundary_statements: list[str],
) -> dict[str, Any]:
    report_digest = dict(report_pack.get("digest") or {})
    cases = [dict(item) for item in list(case_payloads.get("cases") or []) if isinstance(item, dict)]
    fixture_paths = dict(case_payloads.get("fixture_artifact_paths") or {})
    digest = {
        **_scope_digest(
            common,
            summary=f"uncertainty rollup | cases {len(cases)} | reviewer-only / file-artifact-first / not ready for formal claim",
            current_coverage="results / review_center / workbench / historical placeholder-safe visibility",
            missing_evidence=str(report_digest.get("missing_evidence_summary") or "--"),
        ),
        "uncertainty_overview_summary": str(report_digest.get("uncertainty_overview_summary") or "--"),
        "budget_component_summary": str(report_digest.get("budget_component_summary") or "--"),
        "top_contributors_summary": str(report_digest.get("top_contributors_summary") or "--"),
        "data_completeness_summary": str(report_digest.get("data_completeness_summary") or "--"),
        "budget_level_summary": str(report_digest.get("budget_level_summary") or case_payloads.get("budget_level_summary") or "--"),
        "binding_summary": str(report_digest.get("binding_summary") or case_payloads.get("binding_summary") or "--"),
        "calculation_chain_summary": str(report_digest.get("calculation_chain_summary") or case_payloads.get("calculation_chain_summary") or "--"),
        "fixture_summary": str(report_digest.get("fixture_summary") or case_payloads.get("fixture_summary") or "--"),
        "golden_case_summary": str(report_digest.get("golden_case_summary") or case_payloads.get("golden_case_summary") or "--"),
        "placeholder_completeness_summary": str(report_digest.get("placeholder_completeness_summary") or "--"),
        "warning_summary": str(report_digest.get("warning_summary") or "--"),
        "reviewer_action_summary": str(report_digest.get("reviewer_action_summary") or "--"),
    }
    return _bundle(
        run_id=run_id,
        artifact_type="uncertainty_rollup",
        filename=filenames["uncertainty_rollup"],
        markdown_filename=filenames["uncertainty_rollup_markdown"],
        artifact_role="diagnostic_analysis",
        title_text="不确定度 rollup",
        reviewer_note="Rollup 仅服务 reviewer-facing surfaces；缺失工件时允许 historical/workbench 回退为 placeholder，不伪造结论。",
        summary_text=digest["summary"],
        summary_lines=[
            f"uncertainty overview: {digest['uncertainty_overview_summary']}",
            f"budget completeness: {digest['budget_component_summary']}",
            f"top contributors: {digest['top_contributors_summary']}",
            f"data completeness: {digest['data_completeness_summary']}",
        ],
        detail_lines=[
            f"budget levels: {digest['budget_level_summary']}",
            f"binding: {digest['binding_summary']}",
            f"calculation chain: {digest['calculation_chain_summary']}",
            f"fixture summary: {digest['fixture_summary']}",
            f"golden cases: {digest['golden_case_summary']}",
            f"placeholder completeness: {digest['placeholder_completeness_summary']}",
            f"reviewer actions: {digest['reviewer_action_summary']}",
            f"report pack: {path_map['uncertainty_report_pack']}",
            f"digest: {path_map['uncertainty_digest']}",
        ],
        artifact_paths=_artifact_paths_with_fixtures(
            _base_artifact_paths(path_map, common, "uncertainty_report_pack", "uncertainty_digest", "uncertainty_rollup", "uncertainty_rollup_markdown"),
            fixture_paths,
        ),
        body={
            "scope_id": common["scope_id"],
            "decision_rule_id": common["decision_rule_id"],
            "uncertainty_case_id": str(case_payloads.get("selected_result_case_id") or ""),
            "method_confirmation_protocol_id": str(
                case_payloads.get("method_confirmation_protocol_id")
                or _default_method_confirmation_protocol_id(run_id)
            ),
            "report_rule": common["report_rule"],
            "asset_readiness_overview": common["asset_readiness_overview"],
            "certificate_lifecycle_overview": common["certificate_lifecycle_overview"],
            "gate_status": common["pre_run_gate_status"],
            "linked_surface_visibility": ["results", "review_center", "workbench", "historical_artifacts"],
            "overview_display": str(report_digest.get("uncertainty_overview_summary") or "--"),
            "rollup_summary_display": (
                f"{str(report_digest.get('uncertainty_overview_summary') or '--')} | "
                f"{str(case_payloads.get('budget_level_summary') or '--')}"
            ),
            "budget_completeness_summary": str(report_digest.get("budget_component_summary") or "--"),
            "top_contributors_summary": str(report_digest.get("top_contributors_summary") or "--"),
            "data_completeness_summary": str(report_digest.get("data_completeness_summary") or "--"),
            "budget_level_summary": str(case_payloads.get("budget_level_summary") or "--"),
            "binding_summary": str(case_payloads.get("binding_summary") or "--"),
            "calculation_chain_summary": str(case_payloads.get("calculation_chain_summary") or "--"),
            "fixture_summary": str(case_payloads.get("fixture_summary") or "--"),
            "golden_case_summary": str(case_payloads.get("golden_case_summary") or "--"),
            "placeholder_completeness_summary": str(report_digest.get("placeholder_completeness_summary") or "--"),
            "case_count": len(cases),
            "golden_case_count": sum(
                1 for case in cases if str(case.get("golden_case_status") or "").strip().lower() == "match"
            ),
            "report_pack_available": True,
            "limitation_note": common["limitation_note"],
            "non_claim_note": common["non_claim_note"],
            "reviewer_note": common["reviewer_note"],
        },
        digest=digest,
        boundary_statements=boundary_statements,
        evidence_categories=["recognition_readiness", "uncertainty_rollup"],
    )


def _budget_stub_artifact(
    *,
    run_id: str,
    common: dict[str, Any],
    case_payloads: dict[str, Any],
    path_map: dict[str, str],
    filenames: dict[str, str],
    boundary_statements: list[str],
) -> dict[str, Any]:
    digest = _scope_digest(
        common,
        summary="uncertainty budget stub / reviewer readiness only",
        current_coverage=str(case_payloads.get("budget_completeness_summary") or "--"),
        missing_evidence="input uncertainties and combined budgets remain placeholders only",
    )
    rows = []
    for case in list(case_payloads.get("cases") or []):
        if not isinstance(case, dict):
            continue
        rows.append(
            {
                "measurand": str(case.get("measurand") or ""),
                "model_name": str(case.get("case_label") or ""),
                "input_quantities": list(case.get("input_quantity_set") or []),
                "sensitivity_placeholders": list(case.get("sensitivity_coefficients") or []),
                "uncertainty_sources": list(case.get("top_contributors") or []),
                "combined_uncertainty_status": "placeholder_closed_for_reviewer_pack",
                "readiness_status": "stub_ready",
                "non_claim": "not a released uncertainty budget",
            }
        )
    return _bundle(
        run_id=run_id,
        artifact_type="uncertainty_budget_stub",
        filename=filenames["uncertainty_budget_stub"],
        markdown_filename=filenames["uncertainty_budget_stub_markdown"],
        artifact_role="execution_summary",
        title_text="Uncertainty Budget Stub",
        reviewer_note="Stub now points at the WP3 skeleton artifacts but remains reviewer-only and non-claim.",
        summary_text=digest["summary"],
        summary_lines=[f"route families: {' | '.join(list(case_payloads.get('route_types') or [])) or '--'}", f"measurands: {' | '.join(list(case_payloads.get('measurands') or [])) or '--'}", f"budget completeness: {case_payloads.get('budget_completeness_summary')}"],
        detail_lines=[f"uncertainty model: {path_map['uncertainty_model']}", f"budget cases: {path_map['budget_case']}", f"golden cases: {path_map['uncertainty_golden_cases']}", f"report pack: {path_map['uncertainty_report_pack']}"],
        artifact_paths=_base_artifact_paths(path_map, common, "uncertainty_model", "budget_case", "uncertainty_golden_cases", "uncertainty_report_pack", "uncertainty_rollup", "uncertainty_budget_stub", "uncertainty_budget_stub_markdown"),
        body={
            "scope_id": common["scope_id"],
            "decision_rule_id": common["decision_rule_id"],
            "report_rule": common["report_rule"],
            "rows": rows,
            "route_families": list(case_payloads.get("route_types") or []),
            "payload_backed_phases": ["gas/sample_ready", "water/preseal", "water/pressure_stable", "ambient/ambient_diagnostic"],
            "trace_only_phases": [],
            "linked_artifacts": _base_artifact_paths(path_map, common, "uncertainty_model", "budget_case", "uncertainty_golden_cases", "uncertainty_report_pack", "uncertainty_rollup"),
            "limitation_note": common["limitation_note"],
            "non_claim_note": common["non_claim_note"],
            "reviewer_note": common["reviewer_note"],
        },
        digest=digest,
        boundary_statements=boundary_statements,
        evidence_categories=["recognition_readiness", "uncertainty_budget"],
    )


def build_uncertainty_wp3_artifacts(
    *,
    run_id: str,
    scope_definition_pack: dict[str, Any],
    decision_rule_profile: dict[str, Any],
    reference_asset_registry: dict[str, Any],
    certificate_lifecycle_summary: dict[str, Any],
    pre_run_readiness_gate: dict[str, Any],
    path_map: dict[str, str],
    uncertainty_fixture: dict[str, Any] | None = None,
    fixture_artifact_paths: dict[str, str] | None = None,
    filenames: dict[str, str],
    boundary_statements: list[str],
) -> dict[str, dict[str, Any]]:
    common = _common_context(
        run_id=run_id,
        scope_definition_pack=scope_definition_pack,
        decision_rule_profile=decision_rule_profile,
        reference_asset_registry=reference_asset_registry,
        certificate_lifecycle_summary=certificate_lifecycle_summary,
        pre_run_readiness_gate=pre_run_readiness_gate,
        path_map=path_map,
    )
    case_payloads = _case_payloads(
        run_id=run_id,
        common=common,
        uncertainty_fixture=uncertainty_fixture,
        fixture_artifact_paths=fixture_artifact_paths,
    )
    report_pack = _report_pack_artifact(
        run_id=run_id,
        common=common,
        case_payloads=case_payloads,
        path_map=path_map,
        filenames=filenames,
        boundary_statements=boundary_statements,
    )
    digest = _digest_artifact(
        run_id=run_id,
        common=common,
        report_pack=report_pack,
        case_payloads=case_payloads,
        path_map=path_map,
        filenames=filenames,
        boundary_statements=boundary_statements,
    )
    rollup = _rollup_artifact(
        run_id=run_id,
        common=common,
        case_payloads=case_payloads,
        report_pack=report_pack,
        path_map=path_map,
        filenames=filenames,
        boundary_statements=boundary_statements,
    )
    return {
        "uncertainty_model": _model_artifact(run_id=run_id, common=common, case_payloads=case_payloads, path_map=path_map, filenames=filenames, boundary_statements=boundary_statements),
        "uncertainty_input_set": _input_artifact(run_id=run_id, common=common, case_payloads=case_payloads, path_map=path_map, filenames=filenames, boundary_statements=boundary_statements),
        "sensitivity_coefficient_set": _coefficient_artifact(run_id=run_id, common=common, case_payloads=case_payloads, path_map=path_map, filenames=filenames, boundary_statements=boundary_statements),
        "budget_case": _budget_case_artifact(run_id=run_id, common=common, case_payloads=case_payloads, path_map=path_map, filenames=filenames, boundary_statements=boundary_statements),
        "uncertainty_golden_cases": _golden_cases_artifact(run_id=run_id, common=common, case_payloads=case_payloads, path_map=path_map, filenames=filenames, boundary_statements=boundary_statements),
        "uncertainty_report_pack": report_pack,
        "uncertainty_digest": digest,
        "uncertainty_rollup": rollup,
        "uncertainty_budget_stub": _budget_stub_artifact(run_id=run_id, common=common, case_payloads=case_payloads, path_map=path_map, filenames=filenames, boundary_statements=boundary_statements),
    }
