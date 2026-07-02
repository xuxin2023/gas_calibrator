"""Formal V1.5 input contracts for offline calibration evidence tools."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence

from .formal_open_flow import validate_plan_snapshot
from .pressure_channel import validate_pressure_reference_traceability


FORMAL_PLAN_TEMPLATE: Dict[str, Any] = {
    "plan_id": "v1_5_formal_YYYYMMDD",
    "plan_version": "2026-05-24",
    "config_hash": "<sha256-runtime-config>",
    "operator": "<operator-name>",
    "analyzer_id": "<analyzer-under-test-id>",
    "environment": {
        "lab": "<lab-name>",
        "ambient_temperature_c": None,
        "ambient_rh_pct": None,
    },
    "allow_candidate_coefficients": True,
    "allow_device_write": False,
    "standard_gases": [
        {
            "component": "co2",
            "cylinder_id": "<co2-cylinder-id>",
            "certificate_value": None,
            "certificate_uncertainty": None,
            "valid_until": "YYYY-MM-DD",
            "supplier": "<supplier>",
            "certificate_hash": "<sha256-certificate-file>",
        },
        {
            "component": "h2o",
            "cylinder_id": "<h2o-source-or-generator-id>",
            "certificate_value": None,
            "certificate_uncertainty": None,
            "valid_until": "YYYY-MM-DD",
            "supplier": "<supplier>",
            "certificate_hash": "<sha256-certificate-file>",
        },
    ],
}


STANDARD_GASES_TEMPLATE: Dict[str, Any] = {
    "standard_gases": [
        {
            "component": "co2",
            "cylinder_id": "<co2-cylinder-id>",
            "certificate_id": "<co2-certificate-id>",
            "certificate_value": None,
            "certificate_unit": "ppm",
            "certificate_uncertainty": None,
            "uncertainty_coverage_factor": 2.0,
            "valid_until": "YYYY-MM-DD",
            "supplier": "<supplier>",
            "certificate_hash": "<sha256-certificate-file>",
            "notes": "CO2 standard gas certificate value used for open-flow main calibration.",
        },
        {
            "component": "h2o",
            "cylinder_id": "<h2o-source-or-generator-id>",
            "certificate_id": "<h2o-certificate-or-reference-id>",
            "certificate_value": None,
            "certificate_unit": "mmol/mol",
            "certificate_uncertainty": None,
            "uncertainty_coverage_factor": 2.0,
            "valid_until": "YYYY-MM-DD",
            "supplier": "<supplier>",
            "certificate_hash": "<sha256-certificate-file>",
            "notes": "H2O standard source or humidity reference used for open-flow main calibration.",
        },
    ],
    "sidecar_only": True,
    "allow_device_write": False,
}


COM22_PRESSURE_REFERENCE_TEMPLATE: Dict[str, Any] = {
    "device_id": "COM22-DPG-001",
    "certificate_id": "<pressure-certificate-id>",
    "certificate_uncertainty": None,
    "valid_until": "YYYY-MM-DD",
    "certificate_hash": "<sha256-certificate-file>",
    "supplier": "<calibration-lab-or-supplier>",
    "unit": "hPa",
}


RELEASED_UNCERTAINTY_INPUTS_TEMPLATE: Dict[str, Any] = {
    "released": False,
    "coverage_factor": 2.0,
    "release_basis": "<reviewed GUM budget identifier>",
    "notes": [
        "Fill all required inputs before formal release.",
        "Leave released=false until reviewer approval.",
        "The report remains draft_only while this template is incomplete.",
    ],
    "inputs": [
        {
            "component": "CO2",
            "input_quantity": "standard_gas_certificate",
            "distribution": "normal",
            "standard_uncertainty": None,
            "sensitivity_coefficient": 1.0,
            "status": "not_evaluated",
            "evidence_source": "<co2-certificate>",
            "missing_reason": "fill_from_certificate",
        },
        {
            "component": "CO2",
            "input_quantity": "repeatability",
            "distribution": "normal",
            "standard_uncertainty": None,
            "sensitivity_coefficient": 1.0,
            "status": "not_evaluated",
            "evidence_source": "<a-grade-sample-statistics>",
            "missing_reason": "fill_after_sampling",
        },
        {
            "component": "CO2",
            "input_quantity": "fit_residual",
            "distribution": "normal",
            "standard_uncertainty": None,
            "sensitivity_coefficient": 1.0,
            "status": "not_evaluated",
            "evidence_source": "<released-fit-budget>",
            "missing_reason": "solver_qualification_pending",
        },
        {
            "component": "CO2",
            "input_quantity": "analyzer_resolution",
            "distribution": "rectangular",
            "standard_uncertainty": None,
            "sensitivity_coefficient": 1.0,
            "status": "not_evaluated",
            "evidence_source": "<resolution-model>",
            "missing_reason": "resolution_budget_pending",
        },
        {
            "component": "H2O",
            "input_quantity": "standard_gas_certificate",
            "distribution": "normal",
            "standard_uncertainty": None,
            "sensitivity_coefficient": 1.0,
            "status": "not_evaluated",
            "evidence_source": "<h2o-certificate-or-reference>",
            "missing_reason": "fill_from_certificate",
        },
        {
            "component": "H2O",
            "input_quantity": "repeatability",
            "distribution": "normal",
            "standard_uncertainty": None,
            "sensitivity_coefficient": 1.0,
            "status": "not_evaluated",
            "evidence_source": "<a-grade-sample-statistics>",
            "missing_reason": "fill_after_sampling",
        },
        {
            "component": "H2O",
            "input_quantity": "fit_residual",
            "distribution": "normal",
            "standard_uncertainty": None,
            "sensitivity_coefficient": 1.0,
            "status": "not_evaluated",
            "evidence_source": "<released-fit-budget>",
            "missing_reason": "solver_qualification_pending",
        },
        {
            "component": "H2O",
            "input_quantity": "analyzer_resolution",
            "distribution": "rectangular",
            "standard_uncertainty": None,
            "sensitivity_coefficient": 1.0,
            "status": "not_evaluated",
            "evidence_source": "<resolution-model>",
            "missing_reason": "resolution_budget_pending",
        },
        {
            "component": "CO2/H2O",
            "input_quantity": "pressure_channel_bias",
            "distribution": "normal",
            "standard_uncertainty": None,
            "sensitivity_coefficient": 1.0,
            "status": "not_evaluated",
            "evidence_source": "<pressure-channel-quick-check>",
            "missing_reason": "fill_after_pressure_quick_check",
        },
        {
            "component": "CO2/H2O",
            "input_quantity": "temperature_effect",
            "distribution": "normal",
            "standard_uncertainty": None,
            "sensitivity_coefficient": 1.0,
            "status": "not_evaluated",
            "evidence_source": "<temperature-sensitivity-budget>",
            "missing_reason": "temperature_budget_pending",
        },
        {
            "component": "CO2/H2O",
            "input_quantity": "sampling_stability",
            "distribution": "qualitative_gate",
            "standard_uncertainty": None,
            "sensitivity_coefficient": 1.0,
            "status": "not_evaluated",
            "evidence_source": "<open-flow-qc-gate>",
            "missing_reason": "fill_after_qc",
        },
        {
            "component": "H2O",
            "input_quantity": "dewpoint_or_humidity_reference",
            "distribution": "normal",
            "standard_uncertainty": None,
            "sensitivity_coefficient": 1.0,
            "status": "not_evaluated",
            "evidence_source": "<dewpoint-or-humidity-reference-certificate>",
            "missing_reason": "humidity_reference_budget_pending",
        },
        {
            "component": "H2O",
            "input_quantity": "water_vapor_correction",
            "distribution": "normal",
            "standard_uncertainty": None,
            "sensitivity_coefficient": 1.0,
            "status": "not_evaluated",
            "evidence_source": "<water-vapor-correction-budget>",
            "missing_reason": "water_vapor_correction_budget_pending",
        },
    ],
}


PRESSURE_QUICK_CHECK_REQUIRED_COLUMNS = (
    "pressure_channel_row_status",
    "verified_quantity",
    "analyzer_prefix",
    "primary_reference",
    "auxiliary_reference",
    "sample_index",
    "pressure_mode",
    "analyzer_pressure_kpa",
    "analyzer_pressure_hpa",
    "com22_pressure_hpa",
    "analyzer_minus_com22_hpa",
    "reject_reasons",
)


@dataclass(frozen=True)
class ContractCheckResult:
    status: str
    reasons: List[str]


def _parse_date(value: Any) -> Optional[date]:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return date.fromisoformat(text[:10])
    except Exception:
        return None


def _today(today: Optional[date | str] = None) -> date:
    if isinstance(today, date):
        return today
    parsed = _parse_date(today)
    return parsed or date.today()


def _check_valid_until(value: Any, *, prefix: str, today: Optional[date | str]) -> List[str]:
    parsed = _parse_date(value)
    if parsed is None:
        return [f"{prefix}_invalid_valid_until"]
    if parsed < _today(today):
        return [f"{prefix}_expired"]
    return []


def validate_formal_plan_contract(
    plan: Mapping[str, Any],
    *,
    today: Optional[date | str] = None,
) -> ContractCheckResult:
    status, reasons = validate_plan_snapshot(plan)
    out = list(reasons)

    if bool(plan.get("allow_device_write", False)):
        out.append("allow_device_write_must_be_false")

    gases = plan.get("standard_gases")
    if isinstance(gases, Sequence) and not isinstance(gases, (str, bytes)):
        for index, gas in enumerate(gases, start=1):
            if not isinstance(gas, Mapping):
                continue
            out.extend(
                _check_valid_until(
                    gas.get("valid_until"),
                    prefix=f"standard_gas_{index}",
                    today=today,
                )
            )
    return ContractCheckResult(status="pass" if not out else "fail", reasons=out)


def validate_pressure_reference_contract(
    reference: Mapping[str, Any],
    *,
    today: Optional[date | str] = None,
) -> ContractCheckResult:
    result = validate_pressure_reference_traceability(reference, today=today)
    return ContractCheckResult(status=result.status, reasons=list(result.reasons))


def validate_pressure_quick_check_contract(
    rows: Sequence[Mapping[str, Any]],
    *,
    min_paired_rows: int = 3,
) -> ContractCheckResult:
    reasons: List[str] = []
    if not rows:
        return ContractCheckResult(status="fail", reasons=["pressure_quick_check_rows_missing"])

    available = {str(key) for row in rows for key in row.keys()}
    missing = [key for key in PRESSURE_QUICK_CHECK_REQUIRED_COLUMNS if key not in available]
    reasons.extend(f"missing_column_{key}" for key in missing)

    paired_count = 0
    for row in rows:
        if str(row.get("pressure_channel_row_status") or "").strip().lower() == "paired":
            paired_count += 1
    if paired_count < int(min_paired_rows):
        reasons.append(f"paired_rows<{int(min_paired_rows)}")

    return ContractCheckResult(status="pass" if not reasons else "fail", reasons=reasons)


def write_contract_templates(output_dir: str | Path) -> Dict[str, Path]:
    import json

    root = Path(output_dir).resolve()
    root.mkdir(parents=True, exist_ok=True)
    plan_path = root / "formal_plan_snapshot_template.json"
    standard_gases_path = root / "standard_gases_template.json"
    reference_path = root / "com22_pressure_reference_template.json"
    uncertainty_path = root / "released_uncertainty_inputs_template.json"
    plan_path.write_text(
        json.dumps(FORMAL_PLAN_TEMPLATE, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    standard_gases_path.write_text(
        json.dumps(STANDARD_GASES_TEMPLATE, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    reference_path.write_text(
        json.dumps(COM22_PRESSURE_REFERENCE_TEMPLATE, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    uncertainty_path.write_text(
        json.dumps(RELEASED_UNCERTAINTY_INPUTS_TEMPLATE, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return {
        "formal_plan_template": plan_path,
        "standard_gases_template": standard_gases_path,
        "pressure_reference_template": reference_path,
        "uncertainty_inputs_template": uncertainty_path,
    }
