"""Formal V1.5 open-flow calibration evidence contract.

This module is intentionally pure/offline. It does not open COM ports and does
not write analyzer coefficients. The existing V1.5 runner can feed sample rows
into these helpers to produce auditable fit-input evidence.
"""

from __future__ import annotations

import json
import math
from collections import Counter
from dataclasses import asdict, dataclass, field
from statistics import mean
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence


FORMAL_OPEN_FLOW_STATES = (
    "LOAD_PLAN",
    "PRECHECK",
    "PRESSURE_CHANNEL_QUICK_CHECK",
    "OPEN_FLOW_PURGE",
    "STABILITY_GATE",
    "SAMPLE_WINDOW",
    "QC_CLASSIFICATION",
    "POINT_REVIEW",
    "NEXT_POINT_OR_FINISH",
    "RUN_SUMMARY",
)

OPEN_FLOW_PRESSURE_MODES = {
    "",
    "ambient",
    "ambient_open",
    "atmosphere",
    "current_atmosphere",
    "open_flow",
    "open_flow_atmosphere",
}


@dataclass(frozen=True)
class FormalOpenFlowConfig:
    """Configurable formal-evidence thresholds.

    The defaults are deliberately conservative but not a replacement for a site
    uncertainty budget. They provide a deterministic software contract for
    separating fit candidates from diagnostic evidence.
    """

    min_a_grade_samples: int = 10
    min_pressure_check_samples: int = 3
    pressure_mean_abs_delta_hpa: float = 2.0
    pressure_max_abs_delta_hpa: float = 3.0
    co2_ratio_span_max: Optional[float] = 0.01
    h2o_ratio_span_max: Optional[float] = 0.001
    analyzer_pressure_span_hpa_max: Optional[float] = 2.0
    analyzer_pressure_span_affects_grade: bool = False
    reference_pressure_span_hpa_max: Optional[float] = 2.0
    chamber_temp_span_c_max: Optional[float] = 0.10
    dewpoint_span_c_max: Optional[float] = 0.20
    require_pressure_channel_pass: bool = True
    allowed_pressure_modes: Sequence[str] = field(
        default_factory=lambda: tuple(sorted(OPEN_FLOW_PRESSURE_MODES))
    )


@dataclass(frozen=True)
class PressureChannelQuickCheckResult:
    status: str
    reason: str
    sample_count: int
    analyzer_pressure_mean_hpa: Optional[float]
    reference_pressure_mean_hpa: Optional[float]
    mean_delta_hpa: Optional[float]
    max_abs_delta_hpa: Optional[float]
    allowed_for_formal_sampling: bool
    analyzer_prefix: str = ""
    analyzer_device_id: Optional[str] = None
    analyzer_identity_source: str = ""


@dataclass(frozen=True)
class SampleClassification:
    sample_index: str
    grade: str
    accepted_for_candidate_fit: bool
    reject_reasons: List[str] = field(default_factory=list)
    warning_reasons: List[str] = field(default_factory=list)
    report_warning_reasons: List[str] = field(default_factory=list)


@dataclass(frozen=True)
class FormalOpenFlowReport:
    state_sequence: Sequence[str]
    plan_status: str
    plan_reasons: List[str]
    pressure_channel_quick_check: Dict[str, Any]
    qc_summary: Dict[str, Any]
    a_grade_samples: List[Dict[str, Any]]
    b_grade_samples: List[Dict[str, Any]]
    rejected_samples: List[Dict[str, Any]]
    candidate_fit_allowed: bool
    candidate_fit_blockers: List[str]
    formal_fit_boundary: Dict[str, Any]


def state_sequence() -> Sequence[str]:
    return FORMAL_OPEN_FLOW_STATES


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


def _first_value(row: Mapping[str, Any], keys: Iterable[str]) -> Any:
    for key in keys:
        value = row.get(key)
        if value not in (None, ""):
            return value
    return None


def _prefixed_value(row: Mapping[str, Any], prefix: str, key: str) -> Any:
    prefixed = f"{prefix}_{key}" if prefix else key
    value = row.get(prefixed)
    if value in (None, ""):
        value = row.get(key)
    return value


def _normalized_device_id_text(value: Any) -> str:
    text = str(value or "").strip().upper()
    if not text:
        return ""
    if text.isdigit():
        return f"{int(text):03d}"
    return text


def _device_id_from_mode2_tokens(value: Any) -> str:
    tokens = _json_or_empty(value)
    if not isinstance(tokens, list) or len(tokens) < 2:
        return ""
    return _normalized_device_id_text(tokens[1])


def _row_analyzer_identity(row: Mapping[str, Any], analyzer_prefix: str) -> tuple[str, str]:
    prefix = str(analyzer_prefix or "").strip()
    candidates = (
        (f"{prefix}_analyzer_device_id", "prefixed_analyzer_device_id"),
        (f"{prefix}_device_id", "prefixed_device_id"),
        (f"{prefix}_id", "prefixed_mode2_id"),
        ("analyzer_device_id", "analyzer_device_id"),
        ("device_id", "device_id"),
    )
    for key, source in candidates:
        if not key or key == "_id":
            continue
        device_id = _normalized_device_id_text(row.get(key))
        if device_id:
            return device_id, source

    for key, source in (
        (f"{prefix}_mode2_tokens_json", "prefixed_mode2_tokens_json"),
        ("mode2_tokens_json", "mode2_tokens_json"),
    ):
        if not key or key == "_mode2_tokens_json":
            continue
        device_id = _device_id_from_mode2_tokens(row.get(key))
        if device_id:
            return device_id, source

    device_id = _normalized_device_id_text(row.get("id"))
    if device_id:
        return device_id, "mode2_id"
    return "", "missing"


def _common_analyzer_identity(
    rows: Sequence[Mapping[str, Any]],
    analyzer_prefix: str,
) -> tuple[Optional[str], str]:
    identities: List[tuple[str, str]] = []
    for row in rows:
        device_id, source = _row_analyzer_identity(row, analyzer_prefix)
        if device_id:
            identities.append((device_id, source))
    if not identities:
        return None, "missing"
    counts = Counter(device_id for device_id, _source in identities)
    device_id = counts.most_common(1)[0][0]
    for candidate, source in identities:
        if candidate == device_id:
            return device_id, source
    return device_id, "mixed"


def _numeric_series(rows: Sequence[Mapping[str, Any]], keys: Sequence[str]) -> List[float]:
    values: List[float] = []
    for row in rows:
        numeric = _safe_float(_first_value(row, keys))
        if numeric is not None:
            values.append(numeric)
    return values


def _span(values: Sequence[float]) -> Optional[float]:
    if not values:
        return None
    return float(max(values) - min(values))


def _json_or_empty(value: Any) -> Any:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return json.loads(text)
    except Exception:
        return None


def _analyzer_gate_status_for_row(
    row: Mapping[str, Any],
    analyzer_prefix: str,
) -> Optional[Mapping[str, Any]]:
    statuses = _json_or_empty(row.get("analyzer_gate_per_analyzer_status"))
    if not isinstance(statuses, list):
        return None
    wanted = str(analyzer_prefix or "").strip().lower()
    if not wanted:
        return None
    for item in statuses:
        if not isinstance(item, Mapping):
            continue
        label = str(item.get("label") or item.get("analyzer_label") or "").strip().lower()
        if label == wanted:
            return item
    return None


def _point_stability_key(row: Mapping[str, Any], component: str) -> str:
    value = _first_value(
        row,
        (
            "point_id",
            "point_key",
            "point_tag",
            "sample_point",
            "PointTag",
            "PointRow",
            "target_co2_ppm",
            "co2_target_ppm",
            "ppm_CO2_Tank",
            "target_h2o_mmol",
            "h2o_target_mmol",
            "ppm_H2O_Dew",
        ),
    )
    if value not in (None, ""):
        return f"{component}:{value}"
    return f"{component}:default"


def validate_plan_snapshot(plan: Mapping[str, Any]) -> tuple[str, List[str]]:
    """Check that a formal run has traceable plan and standard-gas evidence."""

    missing: List[str] = []
    for key in ("plan_id", "plan_version", "config_hash", "operator"):
        if not str(plan.get(key) or "").strip():
            missing.append(f"missing_{key}")

    gases = plan.get("standard_gases")
    if not isinstance(gases, Sequence) or isinstance(gases, (str, bytes)) or not gases:
        missing.append("missing_standard_gases")
    else:
        for idx, gas in enumerate(gases, start=1):
            if not isinstance(gas, Mapping):
                missing.append(f"standard_gas_{idx}_invalid")
                continue
            for key in (
                "cylinder_id",
                "certificate_value",
                "certificate_uncertainty",
                "valid_until",
                "supplier",
                "certificate_hash",
            ):
                if not str(gas.get(key) or "").strip():
                    missing.append(f"standard_gas_{idx}_missing_{key}")

    return ("pass", []) if not missing else ("fail", missing)


def evaluate_pressure_channel_quick_check(
    rows: Sequence[Mapping[str, Any]],
    *,
    analyzer_prefix: str = "ga01",
    cfg: Optional[FormalOpenFlowConfig] = None,
) -> PressureChannelQuickCheckResult:
    """Compare analyzer internal pressure P against an external reference.

    Analyzer MODE2 reports pressure in kPa. Reference pressure rows are hPa.
    The evaluator normalizes analyzer pressure to hPa before comparing.
    """

    config = cfg or FormalOpenFlowConfig()
    requested_prefix = str(analyzer_prefix or "").strip().lower()
    selected_rows = [
        row
        for row in rows
        if str(row.get("analyzer_prefix") or "").strip().lower() in {"", requested_prefix}
    ]
    deltas: List[float] = []
    analyzer_values: List[float] = []
    reference_values: List[float] = []
    analyzer_device_id, analyzer_identity_source = _common_analyzer_identity(selected_rows, analyzer_prefix)

    for row in selected_rows:
        analyzer_kpa = _safe_float(
            _first_value(
                row,
                (
                    f"{analyzer_prefix}_pressure_kpa",
                    "analyzer_pressure_kpa",
                    "pressure_kpa",
                ),
            )
        )
        reference_hpa = _safe_float(
            _first_value(
                row,
                (
                    "pressure_gauge_hpa",
                    "gauge_pressure",
                    "com22_pressure_hpa",
                    "pressure_reference_hpa",
                    "pressure_hpa",
                    "controller_pressure",
                    "P",
                    "PSample",
                ),
            )
        )
        if analyzer_kpa is None or reference_hpa is None:
            continue
        analyzer_hpa = analyzer_kpa * 10.0
        analyzer_values.append(analyzer_hpa)
        reference_values.append(reference_hpa)
        deltas.append(analyzer_hpa - reference_hpa)

    if len(deltas) < max(1, int(config.min_pressure_check_samples)):
        return PressureChannelQuickCheckResult(
            status="insufficient_evidence",
            reason=f"pressure_pair_count<{int(config.min_pressure_check_samples)}",
            sample_count=len(deltas),
            analyzer_pressure_mean_hpa=mean(analyzer_values) if analyzer_values else None,
            reference_pressure_mean_hpa=mean(reference_values) if reference_values else None,
            mean_delta_hpa=mean(deltas) if deltas else None,
            max_abs_delta_hpa=max((abs(item) for item in deltas), default=None),
            allowed_for_formal_sampling=False,
            analyzer_prefix=analyzer_prefix,
            analyzer_device_id=analyzer_device_id,
            analyzer_identity_source=analyzer_identity_source,
        )

    mean_delta = float(mean(deltas))
    max_abs_delta = float(max(abs(item) for item in deltas))
    issues: List[str] = []
    if abs(mean_delta) > float(config.pressure_mean_abs_delta_hpa):
        issues.append(
            f"mean_delta_hpa={mean_delta:.3f}>limit={float(config.pressure_mean_abs_delta_hpa):.3f}"
        )
    if max_abs_delta > float(config.pressure_max_abs_delta_hpa):
        issues.append(
            f"max_abs_delta_hpa={max_abs_delta:.3f}>limit={float(config.pressure_max_abs_delta_hpa):.3f}"
        )

    status = "fail" if issues else "pass"
    return PressureChannelQuickCheckResult(
        status=status,
        reason=";".join(issues) if issues else "ok",
        sample_count=len(deltas),
        analyzer_pressure_mean_hpa=float(mean(analyzer_values)),
        reference_pressure_mean_hpa=float(mean(reference_values)),
        mean_delta_hpa=mean_delta,
        max_abs_delta_hpa=max_abs_delta,
        allowed_for_formal_sampling=status == "pass",
        analyzer_prefix=analyzer_prefix,
        analyzer_device_id=analyzer_device_id,
        analyzer_identity_source=analyzer_identity_source,
    )


def _window_reasons(
    rows: Sequence[Mapping[str, Any]],
    *,
    component: str,
    analyzer_prefix: str,
    cfg: FormalOpenFlowConfig,
) -> tuple[List[str], List[str]]:
    reasons: List[str] = []
    report_warnings: List[str] = []

    def check_span(label: str, keys: Sequence[str], limit: Optional[float]) -> None:
        if limit is None:
            return
        values = _numeric_series(rows, keys)
        value_span = _span(values)
        if value_span is not None and value_span > float(limit):
            reasons.append(f"{label}_span={value_span:.6g}>limit={float(limit):.6g}")

    component_key = "h2o_ratio_f" if component == "h2o" else "co2_ratio_f"
    component_limit = cfg.h2o_ratio_span_max if component == "h2o" else cfg.co2_ratio_span_max
    check_span(component_key, (f"{analyzer_prefix}_{component_key}", component_key), component_limit)
    check_span(
        "analyzer_pressure_hpa",
        (f"{analyzer_prefix}_pressure_kpa", "pressure_kpa"),
        None,
    )

    analyzer_pressure_values = [
        value * 10.0
        for value in _numeric_series(rows, (f"{analyzer_prefix}_pressure_kpa", "pressure_kpa"))
    ]
    analyzer_pressure_span = _span(analyzer_pressure_values)
    if (
        cfg.analyzer_pressure_span_hpa_max is not None
        and analyzer_pressure_span is not None
        and analyzer_pressure_span > float(cfg.analyzer_pressure_span_hpa_max)
    ):
        message = (
            "analyzer_pressure_hpa_span="
            f"{analyzer_pressure_span:.6g}>limit={float(cfg.analyzer_pressure_span_hpa_max):.6g}"
        )
        if cfg.analyzer_pressure_span_affects_grade:
            reasons.append(message)
        else:
            report_warnings.append(message + ";pressure_not_polynomial_fit_variable")

    reference_pressure_values = _numeric_series(
        rows,
        ("pressure_gauge_hpa", "gauge_pressure", "pressure_hpa"),
    )
    reference_pressure_span = _span(reference_pressure_values)
    if (
        cfg.reference_pressure_span_hpa_max is not None
        and reference_pressure_span is not None
        and reference_pressure_span > float(cfg.reference_pressure_span_hpa_max)
    ):
        report_warnings.append(
            "reference_pressure_hpa_span="
            f"{reference_pressure_span:.6g}>limit={float(cfg.reference_pressure_span_hpa_max):.6g}"
            ";pressure_not_polynomial_fit_variable"
        )
    check_span(
        "chamber_temp_c",
        (f"{analyzer_prefix}_chamber_temp_c", "chamber_temp_c", "thermometer_temp_c"),
        cfg.chamber_temp_span_c_max,
    )
    dewpoint_values = _numeric_series(rows, ("dewpoint_c", "dewpoint_live_c"))
    dewpoint_span = _span(dewpoint_values)
    if (
        cfg.dewpoint_span_c_max is not None
        and dewpoint_span is not None
        and dewpoint_span > float(cfg.dewpoint_span_c_max)
    ):
        message = f"dewpoint_c_span={dewpoint_span:.6g}>limit={float(cfg.dewpoint_span_c_max):.6g}"
        if component == "co2":
            report_warnings.append(message + ";dry_route_dewpoint_report_only")
        else:
            reasons.append(message)
    return reasons, report_warnings


def classify_open_flow_samples(
    rows: Sequence[Mapping[str, Any]],
    *,
    component: str,
    analyzer_prefix: str = "ga01",
    pressure_check: Optional[PressureChannelQuickCheckResult] = None,
    cfg: Optional[FormalOpenFlowConfig] = None,
) -> tuple[List[SampleClassification], List[Dict[str, Any]], List[Dict[str, Any]], Dict[str, Any]]:
    config = cfg or FormalOpenFlowConfig()
    component_key = str(component or "").strip().lower()
    if component_key not in {"co2", "h2o"}:
        raise ValueError("component must be 'co2' or 'h2o'")

    classifications: List[SampleClassification] = []
    a_grade_samples: List[Dict[str, Any]] = []
    rejected_samples: List[Dict[str, Any]] = []
    mode2_present_count = 0
    component_payload_count = 0
    allowed_modes = {str(item or "").strip().lower() for item in config.allowed_pressure_modes}
    window_rows = [
        row
        for row in rows
        if str(_first_value(row, ("pressure_mode", "PressureMode")) or "").strip().lower() in allowed_modes
    ]
    rows_for_window = window_rows or list(rows)
    grouped_window_rows: Dict[str, List[Mapping[str, Any]]] = {}
    for row in rows_for_window:
        grouped_window_rows.setdefault(_point_stability_key(row, component_key), []).append(row)
    window_reasons_by_key: Dict[str, List[str]] = {}
    window_report_warnings_by_key: Dict[str, List[str]] = {}
    for key, grouped_rows in grouped_window_rows.items():
        window_reasons, window_report_warnings = _window_reasons(
            grouped_rows,
            component=component_key,
            analyzer_prefix=analyzer_prefix,
            cfg=config,
        )
        window_reasons_by_key[key] = window_reasons
        window_report_warnings_by_key[key] = window_report_warnings

    for row in rows:
        sample_id = str(_first_value(row, ("sample_index", "sample_ts", "row_index")) or "")
        reject: List[str] = []
        point_key = _point_stability_key(row, component_key)
        warn: List[str] = list(window_reasons_by_key.get(point_key, []))
        report_warn: List[str] = list(window_report_warnings_by_key.get(point_key, []))
        analyzer_device_id, analyzer_identity_source = _row_analyzer_identity(row, analyzer_prefix)
        if not analyzer_device_id:
            reject.append("analyzer_device_id_missing")

        gate_status = _analyzer_gate_status_for_row(row, analyzer_prefix)
        if gate_status is not None:
            filtered_spike_count = _safe_float(gate_status.get("stability_spike_filtered_count"))
            if filtered_spike_count is not None and filtered_spike_count > 0:
                report_warn.append(
                    "analyzer_ratio_gate_spike_filtered_count="
                    f"{int(filtered_spike_count)};raw_frame_retained"
                )
            if bool(gate_status.get("dropped")):
                reason = str(gate_status.get("drop_reason") or "unknown")
                warn.append(f"analyzer_ratio_gate_dropped_for_device({reason})")
            elif not bool(gate_status.get("stable")):
                warn.append("analyzer_ratio_gate_not_stable_for_device")

        mode = str(_first_value(row, ("pressure_mode", "PressureMode")) or "").strip().lower()
        if mode not in allowed_modes:
            reject.append(f"non_open_flow_pressure_mode({mode or '<blank>'})")

        if str(_prefixed_value(row, analyzer_prefix, "frame_usable")).strip().lower() in {"false", "0", "no"}:
            reject.append("analyzer_frame_unusable")

        frame_status = str(_prefixed_value(row, analyzer_prefix, "frame_status") or "").strip().lower()
        if frame_status and ("极值" in frame_status or "extreme" in frame_status):
            report_warn.append(
                "component_output_extreme_report_only;ratio_signal_fit_input_allowed"
            )

        contract_status = str(_prefixed_value(row, analyzer_prefix, "mode2_contract_status") or "").strip().lower()
        if contract_status and contract_status != "pass":
            reject.append(f"mode2_contract_{contract_status}")
        elif not contract_status:
            reject.append("mode2_contract_missing")

        qc_status = str(_prefixed_value(row, analyzer_prefix, "mode2_qc_status") or "").strip().lower()
        if qc_status and qc_status != "pass":
            reject.append(f"mode2_qc_{qc_status}")
        elif not qc_status:
            reject.append("mode2_qc_missing")

        if _json_or_empty(_prefixed_value(row, analyzer_prefix, "mode2_tokens_json")) is None:
            reject.append("mode2_tokens_missing_or_invalid")
        else:
            mode2_present_count += 1

        required_keys = [
            "raw",
            "ref_signal",
            "chamber_temp_c",
            "case_temp_c",
            "pressure_kpa",
            f"{component_key}_ratio_f",
            f"{component_key}_ppm" if component_key == "co2" else "h2o_mmol",
        ]
        for key in required_keys:
            if _prefixed_value(row, analyzer_prefix, key) in (None, ""):
                reject.append(f"missing_{key}")
        component_value_key = f"{component_key}_ppm" if component_key == "co2" else "h2o_mmol"
        if (
            _prefixed_value(row, analyzer_prefix, f"{component_key}_ratio_f") not in (None, "")
            and _prefixed_value(row, analyzer_prefix, component_value_key) not in (None, "")
        ):
            component_payload_count += 1

        prefixed_point_quality = str(
            _first_value(
                row,
                (
                    f"{analyzer_prefix}_point_quality_status",
                    f"{analyzer_prefix}_sample_data_quality_grade",
                ),
            )
            or ""
        ).lower()
        global_point_quality = str(_first_value(row, ("point_quality_status", "sample_data_quality_grade")) or "").lower()
        point_quality = prefixed_point_quality or global_point_quality
        if point_quality == "fail":
            reject.append("point_quality_fail")
        elif point_quality == "warn":
            warn.append("point_quality_warn")

        if config.require_pressure_channel_pass:
            if pressure_check is None:
                reject.append("pressure_channel_quick_check_missing")
            elif pressure_check.status != "pass":
                reject.append(f"pressure_channel_quick_check_{pressure_check.status}")

        if reject:
            grade = "REJECT"
        elif warn:
            grade = "B"
        else:
            grade = "A"

        item = SampleClassification(
            sample_index=sample_id,
            grade=grade,
            accepted_for_candidate_fit=grade == "A",
            reject_reasons=reject,
            warning_reasons=warn,
            report_warning_reasons=report_warn,
        )
        classifications.append(item)

        out_row = dict(row)
        out_row.update(
            {
                "analyzer_prefix": analyzer_prefix,
                "analyzer_device_id": analyzer_device_id,
                "analyzer_identity_source": analyzer_identity_source,
                "formal_sample_grade": grade,
                "formal_candidate_fit_input": grade == "A",
                "formal_reject_reasons": ";".join(reject),
                "formal_warning_reasons": ";".join(warn),
                "formal_report_warning_reasons": ";".join(report_warn),
            }
        )
        if grade == "A":
            a_grade_samples.append(out_row)
        elif grade == "REJECT":
            rejected_samples.append(out_row)

    completion_reasons: List[str] = []
    required_count = int(config.min_a_grade_samples)
    if len(rows) < required_count:
        completion_reasons.append(f"total_samples<{required_count}")
    if mode2_present_count < required_count:
        completion_reasons.append(f"mode2_present_count<{required_count}")
    if component_payload_count < required_count:
        completion_reasons.append(f"component_payload_count<{required_count}")
    completion_status = "pass" if not completion_reasons else "fail"

    qc_summary = {
        "total_samples": len(rows),
        "a_grade_count": len(a_grade_samples),
        "rejected_count": len(rejected_samples),
        "b_grade_count": sum(1 for item in classifications if item.grade == "B"),
        "window_reasons": {
            key: list(reasons)
            for key, reasons in sorted(window_reasons_by_key.items(), key=lambda item: item[0])
            if reasons
        },
        "window_report_warnings": {
            key: list(reasons)
            for key, reasons in sorted(window_report_warnings_by_key.items(), key=lambda item: item[0])
            if reasons
        },
        "pressure_condition_warning_count": sum(
            1
            for reasons in window_report_warnings_by_key.values()
            for reason in reasons
            if "analyzer_pressure_hpa_span" in reason
        ),
        "analyzer_prefix": analyzer_prefix,
        "analyzer_device_id": _common_analyzer_identity(rows, analyzer_prefix)[0],
        "required_sample_count": required_count,
        "mode2_present_count": mode2_present_count,
        "component_payload_count": component_payload_count,
        "sampling_completion_status": completion_status,
        "sampling_completion_reason": ";".join(completion_reasons) if completion_reasons else "ok",
    }
    return classifications, a_grade_samples, rejected_samples, qc_summary


def build_formal_open_flow_report(
    *,
    plan: Mapping[str, Any],
    sample_rows: Sequence[Mapping[str, Any]],
    component: str,
    analyzer_prefix: str = "ga01",
    cfg: Optional[FormalOpenFlowConfig] = None,
    pressure_check_rows: Optional[Sequence[Mapping[str, Any]]] = None,
) -> FormalOpenFlowReport:
    config = cfg or FormalOpenFlowConfig()
    plan_status, plan_reasons = validate_plan_snapshot(plan)
    pressure_rows = list(pressure_check_rows if pressure_check_rows is not None else sample_rows)
    pressure_check = evaluate_pressure_channel_quick_check(
        pressure_rows,
        analyzer_prefix=analyzer_prefix,
        cfg=config,
    )
    classifications, a_grade_samples, rejected_samples, qc_summary = classify_open_flow_samples(
        sample_rows,
        component=component,
        analyzer_prefix=analyzer_prefix,
        pressure_check=pressure_check,
        cfg=config,
    )
    b_grade_samples: List[Dict[str, Any]] = []
    for classification, row in zip(classifications, sample_rows):
        if classification.grade != "B":
            continue
        analyzer_device_id, analyzer_identity_source = _row_analyzer_identity(row, analyzer_prefix)
        out_row = dict(row)
        out_row.update(
            {
                "analyzer_prefix": analyzer_prefix,
                "analyzer_device_id": analyzer_device_id,
                "analyzer_identity_source": analyzer_identity_source,
                "formal_sample_grade": "B",
                "formal_candidate_fit_input": False,
                "formal_reject_reasons": "",
                "formal_warning_reasons": ";".join(classification.warning_reasons),
                "formal_report_warning_reasons": ";".join(classification.report_warning_reasons),
            }
        )
        b_grade_samples.append(out_row)

    blockers: List[str] = []
    if plan_status != "pass":
        blockers.append("plan_traceability_failed")
    if config.require_pressure_channel_pass and pressure_check.status != "pass":
        blockers.append("pressure_channel_quick_check_not_passed")
    open_flow_device_id = str(qc_summary.get("analyzer_device_id") or "").strip()
    pressure_device_id = str(pressure_check.analyzer_device_id or "").strip()
    if config.require_pressure_channel_pass:
        if not open_flow_device_id:
            blockers.append("open_flow_analyzer_identity_missing")
        elif not pressure_device_id:
            blockers.append("pressure_channel_analyzer_identity_missing")
        elif open_flow_device_id != pressure_device_id:
            blockers.append("pressure_channel_identity_mismatch")
    if qc_summary.get("sampling_completion_status") != "pass":
        blockers.append("sampling_completion_not_passed")
    if len(a_grade_samples) < int(config.min_a_grade_samples):
        blockers.append(f"a_grade_samples<{int(config.min_a_grade_samples)}")

    return FormalOpenFlowReport(
        state_sequence=FORMAL_OPEN_FLOW_STATES,
        plan_status=plan_status,
        plan_reasons=plan_reasons,
        pressure_channel_quick_check=asdict(pressure_check),
        qc_summary=qc_summary,
        a_grade_samples=a_grade_samples,
        b_grade_samples=b_grade_samples,
        rejected_samples=rejected_samples,
        candidate_fit_allowed=not blockers,
        candidate_fit_blockers=blockers,
        formal_fit_boundary={
            "analyzer_prefix": analyzer_prefix,
            "analyzer_device_id": pressure_check.analyzer_device_id,
            "fit_input_grade": "A",
            "pressure_modes": ["", "ambient_open"],
            "excluded_by_default": [
                "sealed_controlled",
                "open_flow_dynamic_control",
                "vent_hold",
                "pressure_channel_validation",
                "pressure_compensation_validation",
            ],
        },
    )


def report_to_dict(report: FormalOpenFlowReport) -> Dict[str, Any]:
    return asdict(report)
