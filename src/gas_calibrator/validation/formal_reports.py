"""Generate V1.5 calibration reports from the evidence package.

The report generator is sidecar-only. It reads evidence artifacts and writes
Markdown/DOCX/PDF files; it never opens COM ports, controls routes or valves,
or writes analyzer coefficients.
"""

from __future__ import annotations

import csv
import json
import math
import statistics
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence


@dataclass
class ReportTable:
    title: str
    rows: List[Dict[str, Any]] = field(default_factory=list)


@dataclass
class ReportSection:
    title: str
    paragraphs: List[str] = field(default_factory=list)
    tables: List[ReportTable] = field(default_factory=list)


@dataclass
class ReportDocument:
    title: str
    sections: List[ReportSection] = field(default_factory=list)


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _load_json(path: str | Path) -> Dict[str, Any]:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def _load_csv(path: str | Path) -> List[Dict[str, Any]]:
    with Path(path).open("r", encoding="utf-8-sig", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _safe_float(value: Any) -> Optional[float]:
    if value in (None, ""):
        return None
    try:
        numeric = float(value)
    except Exception:
        return None
    if not math.isfinite(numeric):
        return None
    return numeric


def _fmt(value: Any, digits: int = 3) -> str:
    if value in (None, ""):
        return "not_evaluated"
    if isinstance(value, float):
        return f"{value:.{digits}f}"
    return str(value)


def _bool_value(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value or "").strip().lower() in {"true", "1", "yes", "pass", "ok"}


def _split_reasons(value: Any) -> List[str]:
    if value in (None, ""):
        return []
    if isinstance(value, list):
        return [str(item) for item in value if str(item)]
    return [item for item in str(value).split(";") if item]


def _first_value(row: Mapping[str, Any], keys: Iterable[str]) -> Any:
    for key in keys:
        value = row.get(key)
        if value not in (None, ""):
            return value
    return None


def _first_float(row: Mapping[str, Any], keys: Iterable[str]) -> Optional[float]:
    return _safe_float(_first_value(row, keys))


def _artifact_paths_by_name(bundle: Mapping[str, Any]) -> Dict[str, Path]:
    tables = bundle.get("tables") if isinstance(bundle.get("tables"), Mapping) else {}
    rows = tables.get("sample_files") or []
    out: Dict[str, Path] = {}
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        path_text = str(row.get("path") or "")
        if not path_text:
            continue
        path = Path(path_text)
        out.setdefault(path.name, path)
    return out


def _artifact_rows(bundle: Mapping[str, Any], filename: str) -> List[Dict[str, Any]]:
    path = _artifact_paths_by_name(bundle).get(filename)
    if path is None or not path.exists():
        return []
    try:
        return _load_csv(path)
    except Exception:
        return []


def _artifact_json(bundle: Mapping[str, Any], filename: str) -> Dict[str, Any]:
    path = _artifact_paths_by_name(bundle).get(filename)
    if path is None or not path.exists():
        return {}
    try:
        payload = _load_json(path)
    except Exception:
        return {}
    return payload if isinstance(payload, Mapping) else {}


def _artifact_manifest_paths(bundle: Mapping[str, Any], role_prefix: str) -> List[str]:
    tables = bundle.get("tables") if isinstance(bundle.get("tables"), Mapping) else {}
    rows = tables.get("sample_files") or []
    paths: List[str] = []
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        role = str(row.get("artifact_role") or "")
        if role.startswith(role_prefix):
            paths.append(str(row.get("path") or ""))
    return [path for path in paths if path]


def _post_write_reverification_model(bundle: Mapping[str, Any]) -> Dict[str, Any]:
    review = _artifact_json(bundle, "post_write_reverification_review.json")
    device_summary = _artifact_rows(bundle, "post_write_reverification_device_summary.csv")
    point_results = _artifact_rows(bundle, "post_write_reverification_points.csv")
    status = str(review.get("overall_status") or "").strip()
    if not status and device_summary:
        statuses = {str(row.get("status") or "").strip().lower() for row in device_summary}
        if statuses and statuses <= {"pass"}:
            status = "pass"
        elif "fail" in statuses:
            status = "fail"
        else:
            status = "review_required"
    return {
        "available": bool(review or device_summary or point_results),
        "overall_status": status or "not_available",
        "created_at": review.get("created_at") or "",
        "limits": review.get("limits") or {},
        "warnings": review.get("warnings") or [],
        "device_summary": device_summary,
        "point_results": point_results[:500],
        "source_artifacts": _artifact_manifest_paths(bundle, "post_write_reverification"),
    }


def _table_rows(bundle: Mapping[str, Any], name: str) -> List[Dict[str, Any]]:
    tables = bundle.get("tables") if isinstance(bundle.get("tables"), Mapping) else {}
    rows = tables.get(name) if isinstance(tables, Mapping) else []
    return [dict(row) for row in rows or [] if isinstance(row, Mapping)]


def _mean(values: Sequence[float]) -> Optional[float]:
    return float(statistics.fmean(values)) if values else None


def _stdev(values: Sequence[float]) -> Optional[float]:
    return float(statistics.stdev(values)) if len(values) > 1 else None


def _component_standard_value(standard_gases: Sequence[Mapping[str, Any]], component: str) -> Optional[float]:
    for gas in standard_gases:
        if str(gas.get("component") or "").strip().lower() == component:
            return _safe_float(gas.get("certificate_value"))
    return None


def _component_uncertainty(standard_gases: Sequence[Mapping[str, Any]], component: str) -> Optional[float]:
    for gas in standard_gases:
        if str(gas.get("component") or "").strip().lower() == component:
            return _safe_float(gas.get("certificate_uncertainty"))
    return None


def _group_component_results(
    rows: Sequence[Mapping[str, Any]],
    standard_gases: Sequence[Mapping[str, Any]],
    *,
    analyzer_prefix: str,
) -> List[Dict[str, Any]]:
    grouped: Dict[str, List[Mapping[str, Any]]] = {}
    for row in rows:
        component = str(row.get("component") or row.get("point_phase") or row.get("route") or "").strip().lower()
        if component not in {"co2", "h2o"}:
            continue
        point = str(row.get("point_row") or row.get("point_tag") or row.get("point_phase") or component)
        grouped.setdefault(f"{component}:{point}", []).append(row)

    result_rows: List[Dict[str, Any]] = []
    for key, items in sorted(grouped.items()):
        component, point = key.split(":", 1)
        if component == "h2o":
            values = [
                value
                for value in (
                    _first_float(
                        row,
                        (
                            f"{analyzer_prefix}_h2o_mmol",
                            "h2o_mmol",
                            "h2o_mmol_mol",
                        ),
                    )
                    for row in items
                )
                if value is not None
            ]
            unit = "mmol/mol"
        else:
            values = [
                value
                for value in (
                    _first_float(
                        row,
                        (
                            f"{analyzer_prefix}_co2_ppm",
                            "co2_ppm",
                            "sample_co2_ppm",
                        ),
                    )
                    for row in items
                )
                if value is not None
            ]
            unit = "ppm"
        standard = _component_standard_value(standard_gases, component)
        measured = _mean(values)
        stdev = _stdev(values)
        result_rows.append(
            {
                "component": component.upper(),
                "point_id": point,
                "standard_value": _fmt(standard),
                "measured_mean": _fmt(measured),
                "error": _fmt(None if standard is None or measured is None else measured - standard),
                "std_dev": _fmt(stdev),
                "sample_count": len(values),
                "unit": unit,
                "qc_grade": "A",
                "entered_formal_fit": True,
                "expanded_uncertainty_k2": "not_released",
            }
        )
    return result_rows


def _summarize_points(calibration_points: Sequence[Mapping[str, Any]]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for point in calibration_points:
        rows.append(
            {
                "component": str(point.get("component") or "").upper(),
                "point_id": point.get("point_key") or "",
                "pressure_mode": point.get("pressure_mode") or "",
                "samples": point.get("sample_count") or 0,
                "a_grade": point.get("a_grade_count") or 0,
                "b_grade": point.get("b_grade_count") or 0,
                "rejected": point.get("rejected_count") or 0,
            }
        )
    return rows


def _uncertainty_budget(
    *,
    standard_gases: Sequence[Mapping[str, Any]],
    results: Sequence[Mapping[str, Any]],
    pressure_qc: Sequence[Mapping[str, Any]],
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for component in ("co2", "h2o"):
        cert_u = _component_uncertainty(standard_gases, component)
        component_results = [row for row in results if str(row.get("component") or "").lower() == component]
        stdevs = [_safe_float(row.get("std_dev")) for row in component_results]
        repeatability = max([value for value in stdevs if value is not None], default=None)
        rows.extend(
            [
                {
                    "component": component.upper(),
                    "source": "standard_gas_certificate",
                    "status": "evaluated" if cert_u is not None else "not_evaluated",
                    "value": _fmt(cert_u),
                    "unit": "certificate_unit",
                    "basis": "standard gas certificate snapshot",
                },
                {
                    "component": component.upper(),
                    "source": "repeatability",
                    "status": "estimated_from_a_grade_samples" if repeatability is not None else "not_evaluated",
                    "value": _fmt(repeatability),
                    "unit": "component_unit",
                    "basis": "A-grade sample standard deviation",
                },
                {
                    "component": component.upper(),
                    "source": "fit_residual",
                    "status": "not_evaluated",
                    "value": "not_released",
                    "unit": "component_unit",
                    "basis": "candidate coefficient solver qualification pending",
                },
                {
                    "component": component.upper(),
                    "source": "analyzer_resolution",
                    "status": "not_evaluated",
                    "value": "not_released",
                    "unit": "component_unit",
                    "basis": "resolution model not yet released",
                },
            ]
        )

    pressure_metrics = pressure_qc[0] if pressure_qc else {}
    pressure_bias = _safe_float(
        pressure_metrics.get("analyzer_minus_com22_max_abs_hpa")
        or pressure_metrics.get("analyzer_minus_com22_mean_hpa")
    )
    shared_rows = [
        {
            "component": "CO2/H2O",
            "source": "pressure_channel_bias",
            "status": "evaluated" if pressure_bias is not None else "not_evaluated",
            "value": _fmt(pressure_bias),
            "unit": "hPa",
            "basis": "analyzer pressure P versus COM22 quick check",
        },
        {
            "component": "H2O",
            "source": "dewpoint_or_humidity_reference",
            "status": "not_evaluated",
            "value": "not_released",
            "unit": "humidity_unit",
            "basis": "dewpoint/humidity reference certificate chain must be attached",
        },
        {
            "component": "CO2/H2O",
            "source": "temperature_effect",
            "status": "not_evaluated",
            "value": "not_released",
            "unit": "component_unit",
            "basis": "temperature sensitivity model pending",
        },
        {
            "component": "CO2/H2O",
            "source": "sampling_stability",
            "status": "evaluated",
            "value": "QC_gate",
            "unit": "qualitative",
            "basis": "open-flow stability/QC classification",
        },
        {
            "component": "H2O",
            "source": "water_vapor_correction",
            "status": "not_evaluated",
            "value": "not_released",
            "unit": "component_unit",
            "basis": "dry/wet conversion uncertainty model pending",
        },
    ]
    rows.extend(shared_rows)
    for row in rows:
        quantity = str(row.get("source") or "")
        value = _safe_float(row.get("value"))
        sensitivity = 1.0 if value is not None else None
        contribution = value * sensitivity if value is not None and sensitivity is not None else None
        row.setdefault("input_quantity", quantity)
        row.setdefault("distribution", "normal_or_certificate")
        row.setdefault("standard_uncertainty", value)
        row.setdefault("sensitivity_coefficient", sensitivity)
        row.setdefault("contribution", contribution)
        row.setdefault("evidence_source", row.get("basis") or "")
        row.setdefault("missing_reason", "" if contribution is not None else str(row.get("status") or "not_evaluated"))
    return rows


REQUIRED_UNCERTAINTY_BY_COMPONENT = {
    "CO2": {
        "standard_gas_certificate",
        "repeatability",
        "fit_residual",
        "analyzer_resolution",
        "pressure_channel_bias",
        "temperature_effect",
        "sampling_stability",
    },
    "H2O": {
        "standard_gas_certificate",
        "repeatability",
        "fit_residual",
        "analyzer_resolution",
        "pressure_channel_bias",
        "dewpoint_or_humidity_reference",
        "temperature_effect",
        "sampling_stability",
        "water_vapor_correction",
    },
}


def _load_optional_json(path: str | Path | None) -> Dict[str, Any]:
    if not path:
        return {}
    source = Path(path)
    if not source.exists():
        raise FileNotFoundError(f"JSON file not found: {source}")
    return json.loads(source.read_text(encoding="utf-8"))


def _normalize_uncertainty_input(row: Mapping[str, Any]) -> Dict[str, Any]:
    quantity = str(row.get("input_quantity") or row.get("source") or "")
    component = str(row.get("component") or "CO2/H2O").upper()
    standard_uncertainty = _safe_float(row.get("standard_uncertainty", row.get("value")))
    sensitivity = _safe_float(row.get("sensitivity_coefficient", 1.0))
    contribution = _safe_float(row.get("contribution"))
    if contribution is None and standard_uncertainty is not None and sensitivity is not None:
        contribution = standard_uncertainty * sensitivity
    status = str(row.get("status") or ("released" if contribution is not None else "not_evaluated"))
    return {
        "component": component,
        "source": quantity,
        "input_quantity": quantity,
        "distribution": str(row.get("distribution") or "normal"),
        "status": status,
        "value": _fmt(standard_uncertainty),
        "standard_uncertainty": standard_uncertainty,
        "sensitivity_coefficient": sensitivity,
        "contribution": contribution,
        "unit": str(row.get("unit") or "component_unit"),
        "basis": str(row.get("basis") or row.get("evidence_source") or ""),
        "evidence_source": str(row.get("evidence_source") or row.get("basis") or ""),
        "missing_reason": "" if contribution is not None or quantity == "sampling_stability" else "missing_numeric_contribution",
    }


def _merge_uncertainty_inputs(
    default_rows: Sequence[Mapping[str, Any]],
    uncertainty_payload: Mapping[str, Any],
) -> List[Dict[str, Any]]:
    merged = [dict(row) for row in default_rows]
    inputs = uncertainty_payload.get("inputs") if isinstance(uncertainty_payload, Mapping) else None
    if not isinstance(inputs, Sequence) or isinstance(inputs, (str, bytes)):
        return merged

    by_key: Dict[tuple[str, str], Dict[str, Any]] = {}
    order: List[tuple[str, str]] = []
    for row in merged:
        key = (str(row.get("component") or "CO2/H2O").upper(), str(row.get("input_quantity") or row.get("source") or ""))
        by_key[key] = row
        order.append(key)
    for source in inputs:
        if not isinstance(source, Mapping):
            continue
        row = _normalize_uncertainty_input(source)
        key = (str(row.get("component") or "CO2/H2O").upper(), str(row.get("input_quantity") or ""))
        if key not in by_key:
            order.append(key)
        by_key[key] = row
    return [by_key[key] for key in order]


def _uncertainty_row_for(
    rows: Sequence[Mapping[str, Any]],
    *,
    component: str,
    quantity: str,
) -> Optional[Mapping[str, Any]]:
    allowed_components = {component, "CO2/H2O"}
    for row in rows:
        row_component = str(row.get("component") or "").upper()
        row_quantity = str(row.get("input_quantity") or row.get("source") or "")
        if row_component in allowed_components and row_quantity == quantity:
            return row
    return None


def _evaluate_uncertainty_release(
    rows: Sequence[Mapping[str, Any]],
    uncertainty_payload: Mapping[str, Any],
) -> Dict[str, Any]:
    coverage_factor = _safe_float(uncertainty_payload.get("coverage_factor")) or 2.0
    explicit_release = bool(uncertainty_payload.get("released")) or str(
        uncertainty_payload.get("release_status") or ""
    ).lower() == "released"
    component_summaries: List[Dict[str, Any]] = []
    missing: List[str] = []

    for component, quantities in REQUIRED_UNCERTAINTY_BY_COMPONENT.items():
        contributions: List[float] = []
        for quantity in sorted(quantities):
            row = _uncertainty_row_for(rows, component=component, quantity=quantity)
            if row is None:
                missing.append(f"{component}:{quantity}:missing")
                continue
            contribution = _safe_float(row.get("contribution"))
            status = str(row.get("status") or "")
            if quantity == "sampling_stability" and contribution is None and status in {"released", "evaluated"}:
                contribution = 0.0
            if contribution is None:
                missing.append(f"{component}:{quantity}:{row.get('missing_reason') or status or 'not_evaluated'}")
                continue
            if status not in {"released", "evaluated", "estimated_from_a_grade_samples"}:
                missing.append(f"{component}:{quantity}:status={status}")
                continue
            contributions.append(float(contribution))
        combined = math.sqrt(sum(value * value for value in contributions)) if contributions else None
        component_summaries.append(
            {
                "component": component,
                "combined_standard_uncertainty": combined,
                "coverage_factor": coverage_factor,
                "expanded_uncertainty_k2": None if combined is None else combined * coverage_factor,
                "contribution_count": len(contributions),
            }
        )

    status = "released" if explicit_release and not missing else "not_released"
    return {
        "status": status,
        "released": status == "released",
        "coverage_factor": coverage_factor,
        "missing_required": missing,
        "component_summaries": component_summaries,
        "release_basis": str(uncertainty_payload.get("release_basis") or ""),
    }


def _apply_uncertainty_to_results(
    results: Sequence[Mapping[str, Any]],
    summary: Mapping[str, Any],
) -> List[Dict[str, Any]]:
    expanded_by_component = {
        str(row.get("component") or ""): row.get("expanded_uncertainty_k2")
        for row in summary.get("component_summaries", [])
        if isinstance(row, Mapping)
    }
    updated: List[Dict[str, Any]] = []
    for row in results:
        item = dict(row)
        value = _safe_float(expanded_by_component.get(str(item.get("component") or "")))
        item["expanded_uncertainty_k2"] = _fmt(value) if summary.get("released") and value is not None else "not_released"
        updated.append(item)
    return updated


def _decision(bundle: Mapping[str, Any]) -> Dict[str, Any]:
    run_rows = _table_rows(bundle, "runs")
    run = run_rows[0] if run_rows else {}
    candidates = _table_rows(bundle, "coefficient_candidates")
    write_events = _table_rows(bundle, "coefficient_write_events")
    evidence_status = str(run.get("evidence_status") or "")
    package_status = str(run.get("package_status") or "")
    if evidence_status == "blocked" or package_status == "blocked":
        status = "blocked"
    elif any(str(row.get("status") or "") not in {"not_attempted", "blocked"} for row in write_events):
        status = "coefficients_written_review_required"
    elif candidates:
        status = "candidate_coefficients_generated_no_write"
    else:
        status = "diagnostic_only"
    return {
        "decision_status": status,
        "evidence_status": evidence_status,
        "package_status": package_status,
        "package_blockers": run.get("package_blockers") or [],
    }


def _write_events_release_issue(write_events: Sequence[Mapping[str, Any]]) -> str:
    audited_statuses = {
        "not_attempted",
        "blocked",
        "written_readback_verified",
        "rollback_readback_verified",
    }
    for row in write_events:
        status = str(row.get("status") or "")
        if status not in audited_statuses:
            return f"coefficient_write_event_requires_audit:{status}"
    return ""


def _write_attempts_present(write_events: Sequence[Mapping[str, Any]]) -> bool:
    return any(str(row.get("status") or "") not in {"not_attempted", "blocked"} for row in write_events)


def _post_write_reverification_release_issue(
    write_events: Sequence[Mapping[str, Any]],
    post_write_reverification: Mapping[str, Any],
) -> str:
    if not _write_attempts_present(write_events):
        return ""
    if not post_write_reverification.get("available"):
        return "post_write_reverification_missing_after_coefficient_write"
    if str(post_write_reverification.get("overall_status") or "").lower() != "pass":
        return f"post_write_reverification_not_pass:{post_write_reverification.get('overall_status')}"
    return ""


def _pressure_release_issue(pressure_summary: Sequence[Mapping[str, Any]]) -> str:
    if not pressure_summary:
        return "pressure_channel_validation_missing"
    row = pressure_summary[0]
    if not _bool_value(row.get("allowed_for_co2_h2o_formal_work")):
        return "pressure_channel_not_formal_pass"
    return ""


def _signature_issue(reviewer: str, approver: str) -> str:
    missing: List[str] = []
    if not reviewer or reviewer == "pending_review":
        missing.append("reviewer_missing")
    if not approver or approver == "pending_approval":
        missing.append("approver_missing")
    return ";".join(missing)


def _release_checklist(
    *,
    decision: Mapping[str, Any],
    pressure_summary: Sequence[Mapping[str, Any]],
    uncertainty_summary: Mapping[str, Any],
    write_events: Sequence[Mapping[str, Any]],
    post_write_reverification: Mapping[str, Any],
    reviewer: str,
    approver: str,
) -> List[Dict[str, Any]]:
    write_issue = _write_events_release_issue(write_events)
    post_write_issue = _post_write_reverification_release_issue(write_events, post_write_reverification)
    pressure_issue = _pressure_release_issue(pressure_summary)
    signature_issue = _signature_issue(reviewer, approver)
    return [
        {
            "check": "evidence_package_not_blocked",
            "status": "pass" if decision.get("decision_status") != "blocked" else "fail",
            "reason": ";".join(_split_reasons(decision.get("package_blockers"))) or "",
        },
        {
            "check": "pressure_channel_formal_pass",
            "status": "pass" if not pressure_issue else "fail",
            "reason": pressure_issue,
        },
        {
            "check": "uncertainty_budget_released",
            "status": "pass" if uncertainty_summary.get("released") else "fail",
            "reason": ";".join(uncertainty_summary.get("missing_required") or []),
        },
        {
            "check": "coefficient_write_audited_or_no_write",
            "status": "pass" if not write_issue else "fail",
            "reason": write_issue,
        },
        {
            "check": "post_write_reverification_passed_or_no_write",
            "status": "pass" if not post_write_issue else "fail",
            "reason": post_write_issue,
        },
        {
            "check": "reviewer_and_approver_present",
            "status": "pass" if not signature_issue else "fail",
            "reason": signature_issue,
        },
    ]


def _report_release_decision(
    *,
    decision: Mapping[str, Any],
    pressure_summary: Sequence[Mapping[str, Any]],
    uncertainty_summary: Mapping[str, Any],
    write_events: Sequence[Mapping[str, Any]],
    post_write_reverification: Mapping[str, Any],
    reviewer: str,
    approver: str,
) -> Dict[str, Any]:
    if decision.get("decision_status") == "blocked":
        status = "blocked"
        reasons = _split_reasons(decision.get("package_blockers")) or ["evidence_package_blocked"]
    else:
        write_issue = _write_events_release_issue(write_events)
        post_write_issue = _post_write_reverification_release_issue(write_events, post_write_reverification)
        pressure_issue = _pressure_release_issue(pressure_summary)
        signature_issue = _signature_issue(reviewer, approver)
        reasons = []
        if write_issue:
            status = "not_releasable"
            reasons.append(write_issue)
        elif post_write_issue:
            status = "not_releasable"
            reasons.append(post_write_issue)
        elif pressure_issue:
            status = "blocked"
            reasons.append(pressure_issue)
        elif not uncertainty_summary.get("released"):
            status = "draft_only"
            reasons.append("uncertainty_budget_not_released")
        elif signature_issue:
            status = "review_ready"
            reasons.extend(_split_reasons(signature_issue))
        else:
            status = "formal_release_ready"
            reasons.append("all_release_gates_passed")
    return {
        "release_status": status,
        "issue_mark": "" if status == "formal_release_ready" else "DRAFT / NOT FOR FORMAL ISSUE",
        "reasons": reasons,
        "formal_issue_allowed": status == "formal_release_ready",
    }


def _build_report_model_from_bundle_base(
    bundle: Mapping[str, Any],
    *,
    report_no: str = "",
    reviewer: str = "",
    approver: str = "",
    location: str = "",
    calibration_date: str = "",
    analyzer_prefix: str = "ga01",
    uncertainty_payload: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Any]:
    run_rows = _table_rows(bundle, "runs")
    run = run_rows[0] if run_rows else {}
    standard_gases = _table_rows(bundle, "standard_gases")
    reference_certificates = _table_rows(bundle, "reference_certificates")
    calibration_points = _table_rows(bundle, "calibration_points")
    sample_files = _table_rows(bundle, "sample_files")
    qc_results = _table_rows(bundle, "qc_results")
    candidates = _table_rows(bundle, "coefficient_candidates")
    write_events = _table_rows(bundle, "coefficient_write_events")
    integrity_checks = _table_rows(bundle, "evidence_integrity_checks")
    a_grade_rows = _artifact_rows(bundle, "a_grade_samples.csv")
    rejected_rows = _artifact_rows(bundle, "rejected_samples.csv")
    pressure_summary = _artifact_rows(bundle, "pressure_validation_summary.csv")
    open_flow_summary = _artifact_rows(bundle, "open_flow_run_summary.csv")
    post_write_reverification = _post_write_reverification_model(bundle)
    results = _group_component_results(a_grade_rows, standard_gases, analyzer_prefix=analyzer_prefix)
    decision = _decision(bundle)
    base_uncertainty_budget = _uncertainty_budget(
        standard_gases=standard_gases,
        results=results,
        pressure_qc=pressure_summary,
    )
    uncertainty_inputs = dict(uncertainty_payload or {})
    uncertainty_budget = _merge_uncertainty_inputs(base_uncertainty_budget, uncertainty_inputs)
    uncertainty_summary = _evaluate_uncertainty_release(uncertainty_budget, uncertainty_inputs)
    results = _apply_uncertainty_to_results(results, uncertainty_summary)
    normalized_reviewer = reviewer or "pending_review"
    normalized_approver = approver or "pending_approval"
    report_release_decision = _report_release_decision(
        decision=decision,
        pressure_summary=pressure_summary,
        uncertainty_summary=uncertainty_summary,
        write_events=write_events,
        post_write_reverification=post_write_reverification,
        reviewer=normalized_reviewer,
        approver=normalized_approver,
    )
    release_checklist = _release_checklist(
        decision=decision,
        pressure_summary=pressure_summary,
        uncertainty_summary=uncertainty_summary,
        write_events=write_events,
        post_write_reverification=post_write_reverification,
        reviewer=normalized_reviewer,
        approver=normalized_approver,
    )
    model = {
        "report_no": report_no or f"V15-{str(bundle.get('run_id') or run.get('run_id') or 'run')}",
        "run_id": str(bundle.get("run_id") or run.get("run_id") or ""),
        "run_db_id": str(bundle.get("run_db_id") or run.get("id") or ""),
        "generated_at": _now(),
        "calibration_date": calibration_date or str(run.get("created_at") or "")[:10] or datetime.now().date().isoformat(),
        "location": location or "not_specified",
        "analyzer_id": str(run.get("analyzer_id") or ""),
        "operator": str(run.get("operator_name") or ""),
        "reviewer": normalized_reviewer,
        "approver": normalized_approver,
        "decision": decision,
        "report_release_decision": report_release_decision,
        "release_checklist": release_checklist,
        "standard_gases": standard_gases,
        "reference_certificates": reference_certificates,
        "calibration_points": _summarize_points(calibration_points),
        "result_rows": results,
        "pressure_summary": pressure_summary,
        "open_flow_summary": open_flow_summary,
        "post_write_reverification": post_write_reverification,
        "qc_results": qc_results,
        "candidate_rows": candidates,
        "write_events": write_events,
        "integrity_checks": integrity_checks,
        "rejected_rows": rejected_rows[:200],
        "artifact_manifest": [
            {
                "role": row.get("artifact_role"),
                "path": row.get("path"),
                "sha256": row.get("sha256"),
                "required": row.get("required"),
            }
            for row in sample_files
        ],
    }
    model["uncertainty_budget"] = uncertainty_budget
    model["uncertainty_summary"] = uncertainty_summary
    model["scope_statement"] = (
        "本次 CO2/H2O 主校准基于开放流通、当前大气压附近条件。"
        "封路控压多压力点、PACE OUTPUT 长期开路动态控压、PACE ACT + sink bias、"
        "VENT-hold 均作为工程诊断边界处理，默认不进入正式 CO2/H2O 拟合。"
    )
    model["method_statement"] = (
        "标准气持续开放流通进入分析仪，持续刷新光学腔体和下游管路，"
        "旧气、死体积湿气和残余气被带走。稳定性门禁确认 CO2/H2O、压力、"
        "露点、温度和工厂模式 ratio/signal 在采样窗口内可用于复核。"
    )
    model["coefficient_statement"] = _coefficient_statement(write_events, decision)
    model["pressure_compensation_statement"] = _pressure_compensation_statement(pressure_summary)
    model["limitations"] = _limitations(decision, pressure_summary)
    return model


def _simple_rows(rows: Sequence[Mapping[str, Any]], keys: Sequence[str], limit: int = 20) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for row in list(rows)[:limit]:
        out.append({key: row.get(key, "") for key in keys})
    return out


def _scope_statement() -> str:
    return (
        "本次 CO2/H2O 主校准基于开放流通、当前大气压附近条件。"
        "封路控压多压力点、PACE OUTPUT 长期开路动态控压、PACE ACT + sink bias、"
        "VENT-hold 均作为工程诊断边界处理，默认不进入正式 CO2/H2O 拟合。"
    )


def _method_statement() -> str:
    return (
        "标准气持续开放流通进入分析仪，持续刷新光学腔体和下游管路，"
        "旧气、死体积湿气和残余气被带走。稳定性门禁确认 CO2/H2O、压力、"
        "露点、温度和工厂模式 ratio/signal 在采样窗口内可用于复核。"
    )


def _coefficient_statement(write_events: Sequence[Mapping[str, Any]], decision: Mapping[str, Any]) -> str:
    if not write_events:
        return "本次未发现系数写入事件。候选系数如有生成，也只能进入评审，不能自动写入设备。"
    statuses = {str(row.get("status") or "") for row in write_events}
    if statuses <= {"not_attempted", "blocked"}:
        return (
            "本次仅生成候选系数或候选评审材料，未写入设备。"
            "SENCOx 写入仍需要单独授权、旧系数备份、写入后读回和独立复验。"
        )
    return (
        "本次存在系数写入相关事件。报告必须同时复核旧系数、写入命令、读回值、"
        "审批记录、回滚证据以及写后独立复验结果。"
    )


def _pressure_compensation_statement(pressure_summary: Sequence[Mapping[str, Any]]) -> str:
    if not pressure_summary:
        return "压力通道快速验证证据缺失；压力补偿验证不在本次正式覆盖范围内。"
    row = pressure_summary[0]
    if _bool_value(row.get("allowed_for_co2_h2o_formal_work")):
        return (
            "分析仪内部压力 P 已通过当前大气压快速验证；"
            "多压力压力补偿验证仍为后置、可选的独立验证范围。"
        )
    return "分析仪内部压力 P 未达到正式 CO2/H2O 工作放行条件；压力补偿验证不得解释为正式覆盖。"


def _limitations(decision: Mapping[str, Any], pressure_summary: Sequence[Mapping[str, Any]]) -> List[str]:
    out = [
        "封路控压多压力点未用于正式 CO2/H2O 拟合。",
        "诊断数据可以保留，但不得作为 real acceptance 或正式拟合输入。",
        "不确定度预算第一版保留缺项状态；未释放的分量不得用于最终合格判定。",
    ]
    if decision.get("decision_status") == "blocked":
        out.append("证据包当前为 blocked，报告不得作为正式校准证书签发。")
    if not pressure_summary:
        out.append("压力通道快速验证缺失，压力补偿覆盖范围未建立。")
    return out


def build_report_model_from_bundle(
    bundle: Mapping[str, Any],
    *,
    report_no: str = "",
    reviewer: str = "",
    approver: str = "",
    location: str = "",
    calibration_date: str = "",
    analyzer_prefix: str = "ga01",
    uncertainty_payload: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Any]:
    model = _build_report_model_from_bundle_base(
        bundle,
        report_no=report_no,
        reviewer=reviewer,
        approver=approver,
        location=location,
        calibration_date=calibration_date,
        analyzer_prefix=analyzer_prefix,
        uncertainty_payload=uncertainty_payload,
    )
    model["scope_statement"] = _scope_statement()
    model["method_statement"] = _method_statement()
    model["coefficient_statement"] = _coefficient_statement(
        model.get("write_events") or [],
        model.get("decision") or {},
    )
    model["pressure_compensation_statement"] = _pressure_compensation_statement(
        model.get("pressure_summary") or []
    )
    model["limitations"] = _limitations(
        model.get("decision") or {},
        model.get("pressure_summary") or [],
    )
    return model


def build_run_report(model: Mapping[str, Any]) -> ReportDocument:
    decision = model.get("decision") or {}
    release = model.get("report_release_decision") or {}
    post_write = model.get("post_write_reverification") or {}
    return ReportDocument(
        title="V1.5 气体分析仪校准运行报告",
        sections=[
            ReportSection(
                "运行结论",
                [
                    f"报告编号：{model.get('report_no')}",
                    f"运行编号：{model.get('run_id')}",
                    f"证据状态：{decision.get('evidence_status')} / {decision.get('decision_status')}",
                    f"发布判定：{release.get('release_status')}",
                    str(release.get("issue_mark") or ""),
                    f"被校分析仪：{model.get('analyzer_id')}",
                    "本报告由证据包自动生成；报告生成过程不打开串口、不控制气路/水路、不写入设备。",
                ],
            ),
            ReportSection(
                "发布门禁",
                [
                    f"允许正式签发：{release.get('formal_issue_allowed')}",
                    f"阻断/提示原因：{';'.join(release.get('reasons') or [])}",
                ],
                [ReportTable("发布检查表", list(model.get("release_checklist") or []))],
            ),
            ReportSection(
                "写后复验",
                [
                    f"写后复验证据可用：{post_write.get('available')}",
                    f"写后复验状态：{post_write.get('overall_status')}",
                    "SENCO 写入成功和写后测量一致性是两个独立放行门禁。",
                ],
                [
                    ReportTable(
                        "写后复验设备汇总",
                        _simple_rows(
                            post_write.get("device_summary") or [],
                            (
                                "device_id",
                                "component",
                                "point_count",
                                "pass_count",
                                "fail_count",
                                "max_abs_error",
                                "max_abs_error_pct",
                                "status",
                            ),
                        ),
                    )
                ],
            ),
            ReportSection(
                "点位摘要",
                tables=[ReportTable("点位 QC 摘要", list(model.get("calibration_points") or []))],
            ),
            ReportSection(
                "候选系数评审",
                [model.get("coefficient_statement", "")],
                [
                    ReportTable(
                        "候选系数状态",
                        _simple_rows(
                            model.get("candidate_rows") or [],
                            (
                                "component",
                                "candidate_status",
                                "allowed_for_review",
                                "auto_write_allowed",
                                "blockers",
                            ),
                        ),
                    )
                ],
            ),
            ReportSection(
                "失败和限制",
                list(model.get("limitations") or []),
                [
                    ReportTable(
                        "完整性检查",
                        _simple_rows(
                            model.get("integrity_checks") or [],
                            ("check_name", "status", "severity", "details"),
                        ),
                    )
                ],
            ),
        ],
    )


def build_technical_report(model: Mapping[str, Any]) -> ReportDocument:
    release = model.get("report_release_decision") or {}
    post_write = model.get("post_write_reverification") or {}
    return ReportDocument(
        title="V1.5 气体分析仪校准技术报告",
        sections=[
            ReportSection(
                "发布门禁",
                [
                    f"发布判定：{release.get('release_status')}",
                    str(release.get("issue_mark") or ""),
                    f"允许正式签发：{release.get('formal_issue_allowed')}",
                ],
                [ReportTable("发布检查表", list(model.get("release_checklist") or []))],
            ),
            ReportSection("方法和物理过程", [model.get("method_statement", "")]),
            ReportSection(
                "开放流通数据质量",
                [
                    "CO2/H2O 主校准只使用开放流通且组分稳定的数据。封路污染压力点、动态控压探针和 VENT-hold 证据保留为诊断边界。",
                    "工厂模式 ratio/signal、压力、温度和露点数据用于复核底层物理状态，不等同于全部进入正式拟合。",
                ],
                [
                    ReportTable(
                        "开放流通摘要",
                        _simple_rows(
                            model.get("open_flow_summary") or [],
                            (
                                "component",
                                "plan_status",
                                "pressure_channel_quick_check_status",
                                "a_grade_count",
                                "b_grade_count",
                                "rejected_count",
                                "candidate_fit_allowed",
                                "candidate_fit_blockers",
                            ),
                        ),
                    )
                ],
            ),
            ReportSection(
                "压力通道验证",
                [model.get("pressure_compensation_statement", "")],
                [
                    ReportTable(
                        "压力通道摘要",
                        _simple_rows(
                            model.get("pressure_summary") or [],
                            (
                                "status",
                                "validation_level",
                                "valid_pair_count",
                                "analyzer_minus_com22_mean_hpa",
                                "analyzer_minus_com22_max_abs_hpa",
                                "allowed_for_co2_h2o_formal_work",
                                "reason",
                            ),
                        ),
                    )
                ],
            ),
            ReportSection(
                "写后复验",
                [
                    f"写后复验证据可用：{post_write.get('available')}",
                    f"写后复验状态：{post_write.get('overall_status')}",
                    f"证据文件：{';'.join(post_write.get('source_artifacts') or [])}",
                ],
                [
                    ReportTable(
                        "写后复验点位明细",
                        _simple_rows(
                            post_write.get("point_results") or [],
                            (
                                "device_id",
                                "component",
                                "point_id",
                                "standard_value",
                                "measured_value",
                                "error",
                                "error_pct",
                                "limit_value",
                                "status",
                                "reason",
                            ),
                            limit=200,
                        ),
                    )
                ],
            ),
            ReportSection(
                "拒绝样本",
                ["拒绝样本必须保留原因，不能因为结果不好看而静默删除。"],
                [
                    ReportTable(
                        "拒绝样本前 200 行",
                        _simple_rows(
                            model.get("rejected_rows") or [],
                            ("component", "sample_index", "formal_grade", "formal_reject_reasons", "pressure_mode"),
                            limit=200,
                        ),
                    )
                ],
            ),
            ReportSection(
                "Artifact Hash",
                ["以下 hash 用于从证据包重建报告。"],
                [
                    ReportTable(
                        "证据文件",
                        _simple_rows(
                            model.get("artifact_manifest") or [],
                            ("role", "path", "sha256", "required"),
                            limit=200,
                        ),
                    )
                ],
            ),
        ],
    )


def build_formal_calibration_report(model: Mapping[str, Any]) -> ReportDocument:
    decision = model.get("decision") or {}
    release = model.get("report_release_decision") or {}
    post_write = model.get("post_write_reverification") or {}
    cover = [
        f"报告发布判定：{release.get('release_status')}",
        str(release.get("issue_mark") or ""),
        f"写后复验可用：{post_write.get('available')}",
        f"写后复验状态：{post_write.get('overall_status')}",
        f"报告编号：{model.get('report_no')}",
        f"运行编号：{model.get('run_id')}",
        f"被校仪器：{model.get('analyzer_id')}",
        f"校准日期：{model.get('calibration_date')}",
        f"校准地点：{model.get('location')}",
        f"操作员：{model.get('operator')}",
        f"审核员：{model.get('reviewer')}",
        f"批准人：{model.get('approver')}",
        f"结论状态：{decision.get('decision_status')}",
    ]
    return ReportDocument(
        title="V1.5 气体分析仪正式校准报告",
        sections=[
            ReportSection("封面", cover),
            ReportSection(
                "发布门禁",
                [
                    f"允许正式签发：{release.get('formal_issue_allowed')}",
                    f"阻断/提示原因：{';'.join(release.get('reasons') or [])}",
                ],
                [ReportTable("发布检查表", list(model.get("release_checklist") or []))],
            ),
            ReportSection("校准范围声明", [model.get("scope_statement", ""), model.get("pressure_compensation_statement", "")]),
            ReportSection("校准方法", [model.get("method_statement", "")]),
            ReportSection(
                "标准和参考设备",
                [
                    "溯源围绕测量结果建立。标准气、压力、露点和温度参考应通过证书链和不确定度支撑结果有效性。",
                ],
                [
                    ReportTable(
                        "标准气",
                        _simple_rows(
                            model.get("standard_gases") or [],
                            (
                                "component",
                                "cylinder_id",
                                "certificate_value",
                                "certificate_uncertainty",
                                "valid_until",
                                "supplier",
                                "certificate_hash",
                            ),
                        ),
                    ),
                    ReportTable(
                        "参考设备证书",
                        _simple_rows(
                            model.get("reference_certificates") or [],
                            ("reference_role", "certificate_id", "certificate_hash", "valid_until", "uncertainty", "unit"),
                        ),
                    ),
                ],
            ),
            ReportSection(
                "数据质量摘要",
                tables=[ReportTable("点位质量", list(model.get("calibration_points") or []))],
            ),
            ReportSection(
                "CO2/H2O 结果",
                tables=[ReportTable("正式结果表", list(model.get("result_rows") or []))],
            ),
            ReportSection(
                "不确定度释放状态",
                [
                    f"不确定度状态：{(model.get('uncertainty_summary') or {}).get('status')}",
                    f"缺失必需输入：{';'.join((model.get('uncertainty_summary') or {}).get('missing_required') or [])}",
                ],
                [ReportTable("不确定度分量摘要", list((model.get("uncertainty_summary") or {}).get("component_summaries") or []))],
            ),
            ReportSection(
                "不确定度预算",
                [
                    "本表列出当前证据包可评估和未评估的不确定度分量。未释放的分量不得用于最终合格判定。",
                    "扩展不确定度 k=2 当前为 not_released，除非后续完成 GUM 预算和审核流程释放。",
                ],
                [ReportTable("不确定度分量", list(model.get("uncertainty_budget") or []))],
            ),
            ReportSection(
                "写后复验",
                [
                    "若本次存在 SENCO 写入，写后开放流通复验必须证明更新后的 CO2/H2O 输出仍与标准气或露点参考一致。",
                    f"写后复验状态：{post_write.get('overall_status')}",
                ],
                [
                    ReportTable(
                        "写后复验设备汇总",
                        _simple_rows(
                            post_write.get("device_summary") or [],
                            (
                                "device_id",
                                "component",
                                "point_count",
                                "pass_count",
                                "fail_count",
                                "max_abs_error",
                                "max_abs_error_pct",
                                "status",
                            ),
                        ),
                    )
                ],
            ),
            ReportSection("系数写入声明", [model.get("coefficient_statement", "")]),
            ReportSection("限制和例外", list(model.get("limitations") or [])),
        ],
    )


def _markdown_table(rows: Sequence[Mapping[str, Any]]) -> List[str]:
    if not rows:
        return ["_No rows._"]
    header: List[str] = []
    for row in rows:
        for key in row.keys():
            text = str(key)
            if text not in header:
                header.append(text)
    lines = [
        "| " + " | ".join(header) + " |",
        "| " + " | ".join("---" for _ in header) + " |",
    ]
    for row in rows:
        values = [str(row.get(key, "")).replace("|", "\\|").replace("\n", " ") for key in header]
        lines.append("| " + " | ".join(values) + " |")
    return lines


def render_markdown(document: ReportDocument) -> str:
    lines = [f"# {document.title}", ""]
    for section in document.sections:
        lines.extend([f"## {section.title}", ""])
        for paragraph in section.paragraphs:
            lines.extend([str(paragraph), ""])
        for table in section.tables:
            lines.extend([f"### {table.title}", ""])
            lines.extend(_markdown_table(table.rows))
            lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def _document_lines(document: ReportDocument) -> List[str]:
    lines = [document.title]
    for section in document.sections:
        lines.append(section.title)
        lines.extend(str(paragraph) for paragraph in section.paragraphs)
        for table in section.tables:
            lines.append(table.title)
            if table.rows:
                header = list(table.rows[0].keys())
                lines.append(" | ".join(header))
                for row in table.rows[:40]:
                    lines.append(" | ".join(str(row.get(key, "")) for key in header))
            else:
                lines.append("No rows.")
    return lines


def write_docx(document: ReportDocument, path: str | Path) -> Path:
    target = Path(path).resolve()
    target.parent.mkdir(parents=True, exist_ok=True)
    try:
        from docx import Document
    except Exception as exc:  # pragma: no cover - python-docx is available in test env
        raise RuntimeError("python-docx is required for DOCX report output") from exc
    doc = Document()
    doc.add_heading(document.title, level=0)
    for section in document.sections:
        doc.add_heading(section.title, level=1)
        for paragraph in section.paragraphs:
            doc.add_paragraph(str(paragraph))
        for table in section.tables:
            doc.add_heading(table.title, level=2)
            if not table.rows:
                doc.add_paragraph("No rows.")
                continue
            header: List[str] = []
            for row in table.rows:
                for key in row.keys():
                    if key not in header:
                        header.append(str(key))
            grid = doc.add_table(rows=1, cols=len(header))
            grid.style = "Table Grid"
            for index, key in enumerate(header):
                grid.rows[0].cells[index].text = key
            for row in table.rows:
                cells = grid.add_row().cells
                for index, key in enumerate(header):
                    cells[index].text = str(row.get(key, ""))
    doc.save(target)
    return target


def _pdf_escape(text: str) -> str:
    return text.replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)")


def _write_minimal_pdf(lines: Sequence[str], path: Path) -> None:
    safe_lines = []
    for line in lines:
        encoded = str(line).encode("latin-1", errors="replace").decode("latin-1")
        safe_lines.append(encoded[:110])
    content = ["BT", "/F1 10 Tf", "50 780 Td"]
    first = True
    for line in safe_lines[:90]:
        if not first:
            content.append("0 -14 Td")
        content.append(f"({_pdf_escape(line)}) Tj")
        first = False
    content.append("ET")
    stream = "\n".join(content).encode("latin-1", errors="replace")
    objects: List[bytes] = []
    objects.append(b"<< /Type /Catalog /Pages 2 0 R >>")
    objects.append(b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>")
    objects.append(b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 595 842] /Resources << /Font << /F1 4 0 R >> >> /Contents 5 0 R >>")
    objects.append(b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>")
    objects.append(b"<< /Length " + str(len(stream)).encode("ascii") + b" >>\nstream\n" + stream + b"\nendstream")
    offsets: List[int] = []
    payload = bytearray(b"%PDF-1.4\n")
    for index, obj in enumerate(objects, start=1):
        offsets.append(len(payload))
        payload.extend(f"{index} 0 obj\n".encode("ascii"))
        payload.extend(obj)
        payload.extend(b"\nendobj\n")
    xref = len(payload)
    payload.extend(f"xref\n0 {len(objects) + 1}\n0000000000 65535 f \n".encode("ascii"))
    for offset in offsets:
        payload.extend(f"{offset:010d} 00000 n \n".encode("ascii"))
    payload.extend(
        f"trailer\n<< /Size {len(objects) + 1} /Root 1 0 R >>\nstartxref\n{xref}\n%%EOF\n".encode("ascii")
    )
    path.write_bytes(bytes(payload))


def write_pdf(document: ReportDocument, path: str | Path) -> Path:
    target = Path(path).resolve()
    target.parent.mkdir(parents=True, exist_ok=True)
    lines = _document_lines(document)
    try:
        from reportlab.lib.pagesizes import A4
        from reportlab.pdfbase import pdfmetrics
        from reportlab.pdfbase.cidfonts import UnicodeCIDFont
        from reportlab.pdfgen import canvas

        try:
            pdfmetrics.registerFont(UnicodeCIDFont("STSong-Light"))
            font_name = "STSong-Light"
        except Exception:
            font_name = "Helvetica"
        c = canvas.Canvas(str(target), pagesize=A4)
        width, height = A4
        y = height - 50
        c.setFont(font_name, 10)
        for line in lines:
            c.drawString(40, y, str(line)[:120])
            y -= 14
            if y < 40:
                c.showPage()
                c.setFont(font_name, 10)
                y = height - 50
        c.save()
    except Exception:
        _write_minimal_pdf(lines, target)
    return target


def write_report_document(document: ReportDocument, output_dir: str | Path, prefix: str) -> Dict[str, Path]:
    root = Path(output_dir).resolve()
    root.mkdir(parents=True, exist_ok=True)
    md_path = root / f"{prefix}.md"
    docx_path = root / f"{prefix}.docx"
    pdf_path = root / f"{prefix}.pdf"
    md_path.write_text(render_markdown(document), encoding="utf-8")
    return {
        "markdown": md_path,
        "docx": write_docx(document, docx_path),
        "pdf": write_pdf(document, pdf_path),
    }


def write_v1_5_calibration_reports(
    *,
    evidence_bundle_path: str | Path,
    output_dir: str | Path,
    report_no: str = "",
    reviewer: str = "",
    approver: str = "",
    location: str = "",
    calibration_date: str = "",
    analyzer_prefix: str = "ga01",
    uncertainty_json: str | Path | None = None,
) -> Dict[str, Path]:
    bundle = _load_json(evidence_bundle_path)
    uncertainty_payload = _load_optional_json(uncertainty_json)
    model = build_report_model_from_bundle(
        bundle,
        report_no=report_no,
        reviewer=reviewer,
        approver=approver,
        location=location,
        calibration_date=calibration_date,
        analyzer_prefix=analyzer_prefix,
        uncertainty_payload=uncertainty_payload,
    )
    root = Path(output_dir).resolve()
    root.mkdir(parents=True, exist_ok=True)
    model_path = root / "report_model.json"
    model_path.write_text(json.dumps(model, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    outputs: Dict[str, Path] = {"report_model": model_path}
    for prefix, document in (
        ("run_report", build_run_report(model)),
        ("technical_report", build_technical_report(model)),
        ("formal_calibration_report", build_formal_calibration_report(model)),
    ):
        for key, path in write_report_document(document, root, prefix).items():
            outputs[f"{prefix}_{key}"] = path
    return outputs
