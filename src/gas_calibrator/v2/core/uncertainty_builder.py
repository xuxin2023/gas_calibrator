from __future__ import annotations

from datetime import datetime, timezone
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


def _case_payloads(*, run_id: str, common: dict[str, Any]) -> dict[str, Any]:
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
    for spec in case_specs:
        case_id = f"{run_id}-{spec['case_key']}"
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
                "route_type": str(spec["route_type"]),
                "measurand": str(spec["measurand"]),
                "case_label": str(spec["case_label"]),
                "point_context": dict(spec["point_context"]),
                "input_quantity_set": list(input_ids),
                "distribution_type": _dedupe(distribution_types),
                "sensitivity_coefficients": list(coefficient_ids),
                **component_values,
                "combined_standard_uncertainty": combined,
                "coverage_factor": 2.0,
                "expanded_uncertainty": round(combined * 2.0, 6),
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
                "placeholder_value_only": True,
            }
        )

    top_contributor_summary = " | ".join(
        f"{str(case.get('case_label') or '--')}: "
        + ", ".join(
            f"{str(item.get('component_key') or '--')} {float(item.get('value', 0.0) or 0.0):.3f}"
            for item in list(case.get("top_contributors") or [])[:2]
        )
        for case in cases
    )
    return {
        "component_fields": component_fields,
        "cases": cases,
        "input_rows": input_rows,
        "coefficient_rows": coefficient_rows,
        "route_types": _dedupe([str(case.get("route_type") or "") for case in cases]),
        "measurands": _dedupe([str(case.get("measurand") or "") for case in cases]),
        "case_ids": [str(case.get("uncertainty_case_id") or "") for case in cases],
        "top_contributor_summary": top_contributor_summary,
        "budget_completeness_summary": (
            f"components {len(component_fields)}/{len(component_fields)} per case | "
            f"scope {scope_id} | decision rule {decision_rule_id}"
        ),
        "placeholder_completeness_summary": (
            f"cases {len(cases)}/{len(case_specs)} | "
            f"inputs {len(input_rows)}/{len(input_rows)} | "
            f"coefficients {len(coefficient_rows)}/{len(coefficient_rows)} | "
            "placeholder values only"
        ),
    }


def _base_artifact_paths(path_map: dict[str, str], common: dict[str, Any], *extra_keys: str) -> dict[str, str]:
    rows = {**dict(common.get("linked_artifacts") or {})}
    for key in extra_keys:
        rows[str(key)] = str(path_map[str(key)])
    return rows


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
            f"linked report pack: {path_map['uncertainty_report_pack']}",
            f"linked digest: {path_map['uncertainty_digest']}",
            f"linked rollup: {path_map['uncertainty_rollup']}",
        ],
        artifact_paths=_base_artifact_paths(
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
        body={
            "scope_id": common["scope_id"],
            "decision_rule_id": common["decision_rule_id"],
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
        detail_lines=[f"linked sensitivity set: {path_map['sensitivity_coefficient_set']}", f"linked budget cases: {path_map['budget_case']}"],
        artifact_paths=_base_artifact_paths(path_map, common, "uncertainty_input_set", "uncertainty_input_set_markdown", "sensitivity_coefficient_set", "budget_case"),
        body={
            "scope_id": common["scope_id"],
            "decision_rule_id": common["decision_rule_id"],
            "report_rule": common["report_rule"],
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
        detail_lines=[f"linked input set: {path_map['uncertainty_input_set']}", f"linked budget cases: {path_map['budget_case']}"],
        artifact_paths=_base_artifact_paths(path_map, common, "uncertainty_input_set", "sensitivity_coefficient_set", "sensitivity_coefficient_set_markdown", "budget_case"),
        body={
            "scope_id": common["scope_id"],
            "decision_rule_id": common["decision_rule_id"],
            "report_rule": common["report_rule"],
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
        detail_lines=[f"golden cases: {path_map['uncertainty_golden_cases']}", f"report pack: {path_map['uncertainty_report_pack']}"],
        artifact_paths=_base_artifact_paths(path_map, common, "budget_case", "budget_case_markdown", "uncertainty_golden_cases", "uncertainty_report_pack"),
        body={
            "scope_id": common["scope_id"],
            "decision_rule_id": common["decision_rule_id"],
            "report_rule": common["report_rule"],
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
        current_coverage="CO2 gas | H2O water | ambient diagnostic | writeback/rounding | handoff/seal ingress",
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
        summary_lines=["coverage: CO2 gas | H2O water | ambient diagnostic | writeback/rounding | handoff/seal ingress", f"scope: {common['scope_id']}", f"decision rule: {common['decision_rule_id']}"],
        detail_lines=[f"budget cases: {path_map['budget_case']}", f"report pack: {path_map['uncertainty_report_pack']}"],
        artifact_paths=_base_artifact_paths(path_map, common, "budget_case", "uncertainty_golden_cases", "uncertainty_golden_cases_markdown", "uncertainty_report_pack"),
        body={
            "scope_id": common["scope_id"],
            "decision_rule_id": common["decision_rule_id"],
            "report_rule": common["report_rule"],
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
            current_coverage="report pack wired to scope / decision / asset / certificate / pre-run artifacts",
            missing_evidence="real uncertainty engine, released coefficients, and formal compliance claim remain out of scope",
        ),
        "uncertainty_overview_summary": (
            f"scope {common['scope_id']} | decision rule {common['decision_rule_id']} | "
            f"cases {len(cases)} | readiness mapping only"
        ),
        "budget_component_summary": str(case_payloads.get("budget_completeness_summary") or "--"),
        "top_contributors_summary": str(case_payloads.get("top_contributor_summary") or "--"),
        "data_completeness_summary": (
            f"input rows {len(list(case_payloads.get('input_rows') or []))} | "
            f"coefficients {len(list(case_payloads.get('coefficient_rows') or []))} | "
            "all values placeholder/simulated"
        ),
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
            f"placeholder completeness: {digest['placeholder_completeness_summary']}",
            f"reviewer actions: {digest['reviewer_action_summary']}",
            f"non-claim: {common['non_claim_note']}",
            f"scope artifact: {path_map['scope_definition_pack']}",
            f"decision rule artifact: {path_map['decision_rule_profile']}",
            f"reference asset artifact: {path_map['reference_asset_registry']}",
            f"certificate lifecycle artifact: {path_map['certificate_lifecycle_summary']}",
            f"pre-run gate artifact: {path_map['pre_run_readiness_gate']}",
        ],
        artifact_paths=_base_artifact_paths(
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
        body={
            "scope_id": common["scope_id"],
            "decision_rule_id": common["decision_rule_id"],
            "report_rule": common["report_rule"],
            "standard_family": list(common.get("standard_family") or []),
            "required_evidence_categories": list(common.get("required_evidence_categories") or []),
            "asset_readiness_overview": common["asset_readiness_overview"],
            "certificate_lifecycle_overview": common["certificate_lifecycle_overview"],
            "gate_status": common["pre_run_gate_status"],
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
    path_map: dict[str, str],
    filenames: dict[str, str],
    boundary_statements: list[str],
) -> dict[str, Any]:
    report_digest = dict(report_pack.get("digest") or {})
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
        detail_lines=[f"budget completeness: {digest['budget_component_summary']}", f"data completeness: {digest['data_completeness_summary']}", f"placeholder completeness: {digest['placeholder_completeness_summary']}"],
        artifact_paths=_base_artifact_paths(path_map, common, "uncertainty_report_pack", "uncertainty_digest", "uncertainty_digest_markdown"),
        body={
            "scope_id": common["scope_id"],
            "decision_rule_id": common["decision_rule_id"],
            "report_rule": common["report_rule"],
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
        detail_lines=[f"placeholder completeness: {digest['placeholder_completeness_summary']}", f"reviewer actions: {digest['reviewer_action_summary']}", f"report pack: {path_map['uncertainty_report_pack']}", f"digest: {path_map['uncertainty_digest']}"],
        artifact_paths=_base_artifact_paths(path_map, common, "uncertainty_report_pack", "uncertainty_digest", "uncertainty_rollup", "uncertainty_rollup_markdown"),
        body={
            "scope_id": common["scope_id"],
            "decision_rule_id": common["decision_rule_id"],
            "report_rule": common["report_rule"],
            "asset_readiness_overview": common["asset_readiness_overview"],
            "certificate_lifecycle_overview": common["certificate_lifecycle_overview"],
            "gate_status": common["pre_run_gate_status"],
            "linked_surface_visibility": ["results", "review_center", "workbench", "historical_artifacts"],
            "overview_display": str(report_digest.get("uncertainty_overview_summary") or "--"),
            "budget_completeness_summary": str(report_digest.get("budget_component_summary") or "--"),
            "top_contributors_summary": str(report_digest.get("top_contributors_summary") or "--"),
            "data_completeness_summary": str(report_digest.get("data_completeness_summary") or "--"),
            "placeholder_completeness_summary": str(report_digest.get("placeholder_completeness_summary") or "--"),
            "case_count": len(cases),
            "golden_case_count": len(cases),
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
    case_payloads = _case_payloads(run_id=run_id, common=common)
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
