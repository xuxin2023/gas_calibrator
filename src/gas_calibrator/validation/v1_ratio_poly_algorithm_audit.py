"""Offline audit of the legacy V1 ratio-polynomial coefficient algorithm.

The audit is intentionally static and no-write. It checks the reusable parts of
the historical V1 fitting path against the V1.5 production boundary without
opening COM ports, controlling routes, or writing coefficients.
"""

from __future__ import annotations

import csv
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Mapping, Sequence

from ..coefficients.feature_builder import build_feature_terms, default_model_features


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _write_csv(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    header: List[str] = []
    for row in rows:
        for key in row.keys():
            if key not in header:
                header.append(str(key))
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=header)
        writer.writeheader()
        writer.writerows([dict(row) for row in rows])


def _repo_path(relative: str) -> str:
    return relative.replace("/", "\\")


def build_v1_ratio_poly_algorithm_audit_tables() -> Dict[str, List[Dict[str, Any]]]:
    """Build no-write V1 ratio-polynomial audit tables."""

    features = default_model_features(ratio_degree=3, add_intercept=True)
    terms = build_feature_terms(model_features=features)
    formula = " + ".join(f"a{index}*{term}" for index, term in enumerate(terms))
    feature_rows: List[Dict[str, Any]] = [
        {
            "check": "legacy_default_features",
            "status": "pass_legacy_v1_formula_terms_detected",
            "source": _repo_path("src/gas_calibrator/coefficients/feature_builder.py"),
            "observed": ";".join(features),
            "formula_terms": ";".join(terms),
            "physical_meaning": (
                "V1 fits final CO2/H2O values as a linear-in-coefficients polynomial of optical ratio R, "
                "absolute chamber temperature T_k, pressure P, and coupled R*T/P terms."
            ),
        },
        {
            "check": "temperature_unit",
            "status": "pass_temperature_is_kelvin_in_feature_matrix",
            "source": _repo_path("src/gas_calibrator/coefficients/feature_builder.py"),
            "observed": "T_k = temperature_c + 273.15",
            "formula_terms": "T_k;T_k^2;R*T_k;R*T_k*P",
            "physical_meaning": "Manual formula terms using T are physically meaningful only when the temperature unit matches firmware expectations.",
        },
        {
            "check": "linear_solver",
            "status": "pass_mathematically_valid_if_firmware_formula_matches",
            "source": _repo_path("src/gas_calibrator/coefficients/model_fit.py"),
            "observed": "numpy.linalg.lstsq over fixed feature matrix",
            "formula_terms": formula,
            "physical_meaning": (
                "Least-squares fitting is appropriate because the model is linear in coefficients even though "
                "the measured variables include powers and cross terms."
            ),
        },
        {
            "check": "original_main_intercept_contract",
            "status": "same_as_original_reference_add_intercept_by_default",
            "source": r"C:\Users\A\Desktop\新建文件夹\main.py",
            "observed": "LSTSQ_Simplifier(..., add_intercept=True) prepends a constant 1 column when X does not already start with one",
            "formula_terms": "intercept",
            "physical_meaning": (
                "The original fitting helper treats the constant offset as a real model term. "
                "Scaling/simplification must preserve it rather than silently forcing the curve through zero."
            ),
        },
        {
            "check": "v1_v2_intercept_contract",
            "status": "same_intercept_token_default_as_original_reference",
            "source": _repo_path("src/gas_calibrator/coefficients/feature_builder.py"),
            "observed": "default_model_features(add_intercept=True) starts with intercept; build_feature_matrix emits an all-ones column",
            "formula_terms": "intercept;R;R2;R3;T;T2;RT;P;RTP",
            "physical_meaning": (
                "The V1/V2 shared feature builder keeps the same constant term concept as the original helper, "
                "but exposes it through an explicit feature token instead of auto-detecting the first X column."
            ),
        },
        {
            "check": "v1_5_formal_intercept_contract",
            "status": "intercept_preserved_and_transformed_to_absolute_firmware_terms",
            "source": _repo_path("src/gas_calibrator/validation/formal_candidate_coefficients.py"),
            "observed": "SENCO1/SENCO3 candidates include intercept; centered R/T fitting is algebraically transformed back to firmware absolute terms",
            "formula_terms": "intercept;R;R2;R3;T;T2;RT",
            "physical_meaning": (
                "V1.5 may center R and T only to improve conditioning. The exported SENCO payload still contains "
                "the absolute firmware intercept, so low-end bias remains a calibration evidence problem, not a hidden centering artifact."
            ),
        },
    ]

    mapping_rows: List[Dict[str, Any]] = [
        {
            "check": "legacy_co2_senco_mapping",
            "status": "conditional_reuse_for_senco1_senco3_only",
            "source": _repo_path("src/gas_calibrator/tools/run_v1_corrected_autodelivery.py"),
            "observed": "CO2 a0-a3 -> SENCO1; CO2 a4-a8 -> SENCO3; padded to 6 values",
            "v1_5_decision": "reuse_payload_format_after_formula_contract_review",
            "physical_meaning": (
                "The mapping preserves the historical split between ratio response and ratio-temperature/pressure terms, "
                "but it does not by itself prove that firmware uses the same variables or scaling."
            ),
        },
        {
            "check": "v2_inherited_mapping",
            "status": "same_as_legacy_v1",
            "source": _repo_path("src/gas_calibrator/v2/export/ratio_poly_report.py"),
            "observed": "V2 report exporter uses the same SENCO1/SENCO3 split for ratio-poly download plans",
            "v1_5_decision": "not_independent_confirmation",
            "physical_meaning": "V2 matching V1 shows code continuity, not metrological correctness.",
        },
        {
            "check": "pressure_terms",
            "status": "blocked_for_v1_5_formal_open_flow_fit",
            "source": _repo_path("src/gas_calibrator/coefficients/feature_builder.py"),
            "observed": "a7=P, a8=R*T_k*P",
            "v1_5_decision": "freeze_or_exclude_pressure_terms_for_CO2_H2O_fit",
            "physical_meaning": (
                "Pressure is an input to the analyzer algorithm and is verified/calibrated separately; "
                "contaminated sealed-pressure points must not be used to refit CO2/H2O composition."
            ),
        },
        {
            "check": "senco5_scope",
            "status": "legacy_v1_mapping_requires_integrated_senco5_output_layer_review",
            "source": _repo_path("src/gas_calibrator/tools/run_v1_corrected_autodelivery.py"),
            "observed": "gas download plan writes SENCO1/SENCO3 for CO2 and does not identify SENCO5",
            "v1_5_decision": "reuse_senco13_as_lower_layer_and_review_senco5_as_final_output_layer_when_displayed_ppm_is_acceptance_output",
            "physical_meaning": (
                "Manual interpretation puts SENCO5 in the CO2 concentration linear-trim scope "
                "(concentration*C1+C0). If displayed ppm remains biased after the SENCO1/SENCO3 lower layer, "
                "SENCO5 belongs to the same candidate package rather than a later acceptance patch."
            ),
        },
        {
            "check": "zero_gas_use",
            "status": "usable_as_CO2_zero_anchor_with_certificate_uncertainty_not_as_O2_percent_CO2",
            "source": "standard gas certificate",
            "observed": "dry air / oxygen certificates state O2 mole fraction; CO2 is not the listed 20.95% component",
            "v1_5_decision": "include_0ppm_as_CO2_zero_anchor_when_plan_marks_CO2_target_zero_and_uncertainty_policy_exists",
            "physical_meaning": (
                "Zero gas improves optical baseline/R0 evidence. Its O2 percentage is not a CO2 target; "
                "CO2 should be treated as zero/trace according to the calibration plan and uncertainty budget."
            ),
        },
    ]

    decision_rows: List[Dict[str, Any]] = [
        {
            "item": "feature_builder",
            "decision": "reuse",
            "reason": "It preserves the historical a0-a8 ordering and Kelvin temperature handling.",
        },
        {
            "item": "least_squares_solver",
            "decision": "reuse_with_guardrails",
            "reason": "Mathematically valid, but only after stable A-grade open-flow samples and firmware formula contract checks.",
        },
        {
            "item": "scientific_senco_format",
            "decision": "reuse",
            "reason": "Device write payloads should use reviewed scientific notation and readback verification.",
        },
        {
            "item": "pressure_terms_from_legacy_fit",
            "decision": "do_not_reuse_for_formal_CO2_H2O_fit",
            "reason": "V1.5 separates pressure channel validation/SENCO9 from composition fitting.",
        },
        {
            "item": "SENCO1_SENCO3_only_write",
            "decision": "do_not_release_as_complete_output_calibration_when_senco5_bias_remains",
            "reason": "SENCO5 is the final displayed concentration layer and must be reviewed in the same candidate package when needed.",
        },
        {
            "item": "direct_ppm_from_R_T_model_as_firmware_truth",
            "decision": "do_not_assume",
            "reason": "Manual evidence indicates a zero-gas R0/AbsFinal path may sit between raw ratio and ppm.",
        },
        {
            "item": "intercept_term",
            "decision": "keep_but_require_zero_or_low_end_anchor_review",
            "reason": (
                "The original helper, V1/V2 feature builder, and V1.5 formal candidate path all keep an intercept; "
                "dropping it would force a physical zero that the optical ratio model has not proven."
            ),
        },
        {
            "item": "low_end_intercept_dispute",
            "decision": "resolve_with_current_state_low_point_and_certified_zero_evidence",
            "reason": (
                "A low-concentration bias should be treated as zero/low-end anchoring evidence first, "
                "not as a sample-weighting or pressure-term problem."
            ),
        },
    ]

    summary = [
        {
            "audit_status": "legacy_v1_algorithm_conditionally_reusable_but_not_sufficient_for_v1_5_write",
            "feature_count": len(features),
            "formula": formula,
            "reusable_items": "feature_order;least_squares_solver;scientific_senco_format;residual_metrics",
            "blocked_items": "pressure_terms_for_formal_fit;senco5_missing_from_legacy_write_plan;unconfirmed_direct_R_to_ppm_formula",
            "intercept_contract": "original_reference_and_v1_v2_and_v1_5_all_keep_intercept",
            "opens_com_ports": False,
            "controls_water_or_gas_routes": False,
            "writes_coefficients": False,
            "physical_meaning": (
                "The V1 algorithm is coherent as an old ratio-polynomial regression, but current V1.5 formal calibration "
                "must protect clean open-flow composition samples, separate pressure calibration, include zero-gas baseline evidence, "
                "and review SENCO5 as the final displayed-concentration output layer when the acceptance output requires it."
            ),
        }
    ]
    return {
        "v1_ratio_poly_algorithm_summary": summary,
        "v1_ratio_poly_feature_contract": feature_rows,
        "v1_ratio_poly_mapping_contract": mapping_rows,
        "v1_ratio_poly_reuse_decisions": decision_rows,
    }


def write_v1_ratio_poly_algorithm_audit(*, output_dir: str | Path) -> Dict[str, Path]:
    destination = Path(output_dir).resolve()
    destination.mkdir(parents=True, exist_ok=True)
    tables = build_v1_ratio_poly_algorithm_audit_tables()
    outputs: Dict[str, Path] = {}
    for name, rows in tables.items():
        path = destination / f"{name}.csv"
        _write_csv(path, rows)
        outputs[f"{name}_csv"] = path

    meta = {
        "tool_name": "export_v1_5_v1_ratio_poly_algorithm_audit",
        "created_at": _now(),
        "boundary": {
            "opens_com_ports": False,
            "controls_water_or_gas_routes": False,
            "writes_coefficients": False,
        },
    }
    meta_path = destination / "v1_ratio_poly_algorithm_audit_meta.json"
    meta_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
    outputs["meta_json"] = meta_path

    summary = tables["v1_ratio_poly_algorithm_summary"][0]
    report_path = destination / "v1_ratio_poly_algorithm_audit.md"
    lines = [
        "# V1 Ratio-Polynomial Algorithm Audit",
        "",
        f"- Status: {summary['audit_status']}",
        "- Boundary: offline only; no COM, no route control, no coefficient write.",
        f"- Formula: `{summary['formula']}`",
        "",
        "## Conclusion",
        "",
        "- V1 feature order and least-squares mechanics are reusable as an offline fitting foundation.",
        "- V1's historical pressure terms are not acceptable for V1.5 formal CO2/H2O fitting unless pressure-composition evidence is separately justified.",
        "- V1 maps CO2 coefficients to SENCO1/SENCO3 but does not resolve SENCO5; V1.5 must review SENCO5 as the final output layer when displayed ppm still has stable bias.",
        "- Zero gas can participate as a CO2 zero anchor; its O2 percentage must not be interpreted as a CO2 target.",
        "",
        "## Decisions",
        "",
        "| Item | Decision | Reason |",
        "| --- | --- | --- |",
    ]
    for row in tables["v1_ratio_poly_reuse_decisions"]:
        lines.append(f"| {row['item']} | {row['decision']} | {row['reason']} |")
    report_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    outputs["markdown"] = report_path
    return outputs
