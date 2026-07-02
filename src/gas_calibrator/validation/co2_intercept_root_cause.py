"""Offline CO2 intercept/root-cause diagnostic for V1.5.

This module consumes already-recorded open-flow evidence. It does not open COM
ports, control valves, control PACE, or write SENCO coefficients. Its purpose is
to distinguish coefficient/intercept errors from humidity, pressure, route
memory, or stale-candidate problems before any controlled write is considered.
"""

from __future__ import annotations

import csv
import json
import math
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

from .co2_firmware_contract import co2_raw_to_firmware_final_ppm
from .co2_senco_algorithm_audit import direct_ratio_model_prediction


CO2_MODEL_TERMS = ("intercept", "R", "R2", "R3", "T", "T2", "RT")


@dataclass(frozen=True)
class Co2InterceptRootCauseConfig:
    target_device_ids: Tuple[str, ...] = ("022", "030", "033", "051")
    exclude_device_ids: Tuple[str, ...] = ("100",)
    acceptance_pct: float = 1.0
    h2o_low_mmol_mol: float = 2.0
    candidate_delta_limit: float = 1.0e-3
    offset_dominance_ratio: float = 0.70
    ratio_state_conflict_limit: float = 1.5e-3


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _device_id(value: Any) -> str:
    text = str(value or "").strip().upper()
    if text.isdigit():
        return f"{int(text):03d}"
    return text


def _safe_float(value: Any) -> Optional[float]:
    if value in (None, "", "None", "null"):
        return None
    try:
        numeric = float(value)
    except Exception:
        return None
    return numeric if math.isfinite(numeric) else None


def _read_csv(path: str | Path) -> List[Dict[str, Any]]:
    target = Path(path)
    if not target.exists():
        return []
    with target.open("r", encoding="utf-8-sig", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _write_csv(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    keys: List[str] = []
    for row in rows:
        for key in row:
            if key not in keys:
                keys.append(str(key))
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=keys)
        writer.writeheader()
        writer.writerows([dict(row) for row in rows])


def _load_candidate_coefficients(candidate_dir: str | Path | None) -> Dict[str, Dict[str, float]]:
    if not candidate_dir:
        return {}
    rows = _read_csv(Path(candidate_dir) / "candidate_coefficients.csv")
    out: Dict[str, Dict[str, float]] = {}
    for row in rows:
        if str(row.get("component") or "").strip().lower() != "co2":
            continue
        dev = _device_id(row.get("analyzer_device_id") or row.get("device_id") or row.get("device"))
        term = str(row.get("term") or "").strip()
        value = _safe_float(row.get("coefficient"))
        if not dev or term not in CO2_MODEL_TERMS or value is None:
            continue
        out.setdefault(dev, {})[term] = float(value)
    return out


def _candidate_fit_coverage(candidate_dir: str | Path | None) -> tuple[List[Dict[str, Any]], Dict[str, Dict[str, Any]]]:
    if not candidate_dir:
        return [], {}
    rows = _read_csv(Path(candidate_dir) / "candidate_fit_residuals.csv")
    by_device_temp: Dict[tuple[str, str], Dict[str, Any]] = {}
    zero_anchor_by_device: Dict[str, List[str]] = {}
    certified_zero_anchor_by_device: Dict[str, int] = {}
    for row in rows:
        if str(row.get("component") or "").strip().lower() != "co2":
            continue
        dev = _device_id(row.get("analyzer_device_id") or row.get("device_id") or row.get("device"))
        identity = str(row.get("point_identity") or row.get("sample_index") or "")
        match = re.search(r"T(-?\d+|m\d+)", identity)
        temp = match.group(1).replace("m", "-") if match else "unknown"
        target = _safe_float(row.get("target_value") or row.get("target_ppm"))
        error = _safe_float(row.get("error") or row.get("error_ppm"))
        key = (dev, temp)
        bucket = by_device_temp.setdefault(key, {"targets": set(), "errors": []})
        if target is not None:
            bucket["targets"].add(float(target))
            if abs(float(target)) <= 1.0e-9:
                zero_anchor_by_device.setdefault(dev, []).append(identity)
                if _row_has_certificate_trace(row):
                    certified_zero_anchor_by_device[dev] = certified_zero_anchor_by_device.get(dev, 0) + 1
        if error is not None:
            bucket["errors"].append(float(error))

    coverage_rows: List[Dict[str, Any]] = []
    by_device: Dict[str, List[Dict[str, Any]]] = {}
    for (dev, temp), bucket in sorted(by_device_temp.items()):
        errors = bucket["errors"]
        mean_error = sum(errors) / len(errors) if errors else 0.0
        max_abs_error = max((abs(value) for value in errors), default=0.0)
        row = {
            "device_id": dev,
            "temperature_group": temp,
            "target_count": len(bucket["targets"]),
            "targets": ";".join(f"{value:g}" for value in sorted(bucket["targets"])),
            "residual_count": len(errors),
            "mean_error_ppm": mean_error,
            "max_abs_error_ppm": max_abs_error,
        }
        coverage_rows.append(row)
        by_device.setdefault(dev, []).append(row)

    summary_by_device: Dict[str, Dict[str, Any]] = {}
    for dev, dev_rows in by_device.items():
        counts = [int(row["target_count"]) for row in dev_rows]
        means = [float(row["mean_error_ppm"]) for row in dev_rows]
        total_target_slots = sum(counts)
        min_count = min(counts) if counts else 0
        max_count = max(counts) if counts else 0
        mean_span = max(means) - min(means) if means else 0.0
        dominant_row = max(dev_rows, key=lambda item: int(item["target_count"])) if dev_rows else {}
        dominant_fraction = (
            int(dominant_row.get("target_count", 0)) / float(total_target_slots)
            if total_target_slots
            else 0.0
        )
        status = (
            "imbalanced_temperature_target_grid_blocks_final_write"
            if min_count < 4 or (max_count > 0 and min_count / max_count < 0.5)
            else "balanced_temperature_target_grid"
        )
        for row in dev_rows:
            row["target_count_fraction_of_device_fit"] = (
                int(row["target_count"]) / float(total_target_slots) if total_target_slots else 0.0
            )
            row["temperature_weighting_status"] = status
        zero_anchor_identities = zero_anchor_by_device.get(dev, [])
        zero_anchor_count = len(zero_anchor_identities)
        certified_zero_anchor_count = int(certified_zero_anchor_by_device.get(dev, 0))
        if zero_anchor_count <= 0:
            zero_status = "zero_anchor_absent"
        elif certified_zero_anchor_count >= zero_anchor_count:
            zero_status = "zero_anchor_present_with_certificate_trace"
        else:
            zero_status = "zero_anchor_present_needs_certificate_trace_review"
        summary_by_device[dev] = {
            "candidate_fit_temperature_count": len(dev_rows),
            "candidate_fit_min_targets_per_temperature": min_count,
            "candidate_fit_max_targets_per_temperature": max_count,
            "candidate_fit_total_temperature_target_slots": total_target_slots,
            "candidate_fit_dominant_temperature_group": dominant_row.get("temperature_group", ""),
            "candidate_fit_dominant_temperature_weight_fraction": dominant_fraction,
            "candidate_fit_temperature_mean_error_span_ppm": mean_span,
            "candidate_fit_grid_status": status,
            "candidate_fit_zero_anchor_count": zero_anchor_count,
            "candidate_fit_certified_zero_anchor_count": certified_zero_anchor_count,
            "candidate_fit_zero_anchor_status": zero_status,
            "candidate_fit_zero_anchor_identities": ";".join(zero_anchor_identities[:20]),
        }
    return coverage_rows, summary_by_device


def _row_has_certificate_trace(row: Mapping[str, Any]) -> bool:
    for key in (
        "co2_zero_anchor_certified",
        "zero_anchor_certified",
        "standard_gas_certified",
        "certificate_valid",
        "certificate_id",
        "certificate_hash",
        "standard_gas_certificate_hash",
    ):
        value = row.get(key)
        if value in (None, ""):
            continue
        text = str(value).strip().lower()
        if text in {"false", "0", "no", "none", "null", "invalid"}:
            continue
        return True
    return False


def _temperature_group(value: Any, identity: str = "") -> str:
    match = re.search(r"T(-?\d+|m\d+)", identity)
    if match:
        return match.group(1).replace("m", "-")
    numeric = _safe_float(value)
    if numeric is None:
        return "unknown"
    return f"{round(float(numeric) / 10.0) * 10:.0f}"


def _candidate_fit_state_index(candidate_dir: str | Path | None) -> Dict[tuple[str, float, str], Dict[str, Any]]:
    if not candidate_dir:
        return {}
    rows = _read_csv(Path(candidate_dir) / "candidate_fit_residuals.csv")
    grouped: Dict[tuple[str, float, str], List[Mapping[str, Any]]] = {}
    for row in rows:
        if str(row.get("component") or "").strip().lower() != "co2":
            continue
        dev = _device_id(row.get("analyzer_device_id") or row.get("device_id") or row.get("device"))
        target = _safe_float(row.get("target_value") or row.get("target_ppm"))
        ratio = _safe_float(row.get("ratio"))
        temp = _safe_float(row.get("temperature_c"))
        if not dev or target is None or ratio is None or temp is None:
            continue
        identity = str(row.get("point_identity") or row.get("sample_index") or "")
        key = (dev, round(float(target), 6), _temperature_group(temp, identity))
        grouped.setdefault(key, []).append(row)

    index: Dict[tuple[str, float, str], Dict[str, Any]] = {}
    for key, items in grouped.items():
        ratios = [_safe_float(row.get("ratio")) for row in items]
        temps = [_safe_float(row.get("temperature_c")) for row in items]
        errors = [_safe_float(row.get("error") or row.get("error_ppm")) for row in items]
        numeric_ratios = [float(value) for value in ratios if value is not None]
        numeric_temps = [float(value) for value in temps if value is not None]
        numeric_errors = [float(value) for value in errors if value is not None]
        index[key] = {
            "candidate_ratio_mean": sum(numeric_ratios) / len(numeric_ratios) if numeric_ratios else "",
            "candidate_temperature_c_mean": sum(numeric_temps) / len(numeric_temps) if numeric_temps else "",
            "candidate_fit_error_ppm_mean": sum(numeric_errors) / len(numeric_errors) if numeric_errors else "",
            "candidate_point_count": len(items),
            "candidate_point_identities": ";".join(str(row.get("point_identity") or "") for row in items),
        }
    return index


def _numeric_list(value: Any) -> List[float]:
    if value in (None, ""):
        return []
    candidate = value
    if isinstance(value, str):
        try:
            candidate = json.loads(value)
        except Exception:
            candidate = value
    if isinstance(candidate, Sequence) and not isinstance(candidate, (str, bytes)):
        out: List[float] = []
        for item in candidate:
            numeric = _safe_float(item)
            if numeric is not None:
                out.append(float(numeric))
        return out
    return []


def _load_current_getco_coefficients(current_getco_json: str | Path | None) -> Dict[str, Dict[str, float]]:
    if not current_getco_json:
        return {}
    path = Path(current_getco_json)
    if not path.exists():
        return {}
    data = json.loads(path.read_text(encoding="utf-8-sig"))
    if not isinstance(data, Mapping):
        return {}
    out: Dict[str, Dict[str, float]] = {}
    for key, item in data.items():
        if not isinstance(item, Mapping):
            continue
        dev = _device_id(key)
        getco1 = _numeric_list(
            item.get("GETCO1_before_live") or item.get("GETCO1_before") or item.get("GETCO1")
        )
        getco3 = _numeric_list(
            item.get("GETCO3_before_live") or item.get("GETCO3_before") or item.get("GETCO3")
        )
        terms: Dict[str, float] = {}
        for term, value in zip(("intercept", "R", "R2", "R3"), getco1):
            terms[term] = float(value)
        for term, value in zip(("T", "T2", "RT"), getco3):
            terms[term] = float(value)
        if terms:
            out[dev] = terms
    return out


def _group_point_rows(
    rows: Sequence[Mapping[str, Any]], cfg: Co2InterceptRootCauseConfig
) -> tuple[Dict[str, List[Dict[str, Any]]], List[Dict[str, Any]]]:
    target_devices = {_device_id(value) for value in cfg.target_device_ids}
    excluded_devices = {_device_id(value) for value in cfg.exclude_device_ids}
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    rejected: List[Dict[str, Any]] = []
    for row in rows:
        dev = _device_id(row.get("device") or row.get("device_id") or row.get("analyzer_device_id"))
        target = _safe_float(row.get("target_ppm") or row.get("certificate_co2_ppm") or row.get("target_value"))
        measured = _safe_float(row.get("co2_ppm") or row.get("measured_co2_ppm") or row.get("prediction"))
        reasons: List[str] = []
        if dev in excluded_devices:
            reasons.append("excluded_device")
        if target_devices and dev not in target_devices:
            reasons.append("not_in_target_set")
        if target is None:
            reasons.append("target_missing")
        if measured is None:
            reasons.append("co2_missing")
        if reasons:
            rejected.append(
                {
                    "device_id": dev,
                    "point": row.get("point") or row.get("point_run_id") or "",
                    "reject_reasons": ";".join(reasons),
                }
            )
            continue
        item = dict(row)
        if "h2o_mmol_mol" not in item and row.get("h2o_mmol") not in (None, ""):
            item["h2o_mmol_mol"] = row.get("h2o_mmol")
        if "pressure_kpa" not in item and row.get("pressure_hpa") not in (None, ""):
            pressure_hpa = _safe_float(row.get("pressure_hpa"))
            if pressure_hpa is not None:
                item["pressure_kpa"] = pressure_hpa / 10.0
        item["_device_id"] = dev
        item["_target"] = float(target)
        item["_measured"] = float(measured)
        grouped.setdefault(dev, []).append(item)
    return grouped, rejected


def _residual_metrics(rows: Sequence[Mapping[str, Any]], *, c0: float, c1: float) -> Dict[str, float]:
    errors_ppm: List[float] = []
    errors_pct: List[float] = []
    for row in rows:
        target = float(row["_target"])
        measured = float(row["_measured"])
        corrected = measured * float(c1) + float(c0)
        error_ppm = corrected - target
        error_pct = error_ppm / target * 100.0 if target else 0.0
        errors_ppm.append(error_ppm)
        errors_pct.append(error_pct)
    return {
        "mean_error_ppm": sum(errors_ppm) / len(errors_ppm) if errors_ppm else 0.0,
        "max_abs_error_ppm": max((abs(value) for value in errors_ppm), default=0.0),
        "max_abs_error_pct": max((abs(value) for value in errors_pct), default=0.0),
        "rmse_ppm": math.sqrt(sum(value * value for value in errors_ppm) / len(errors_ppm)) if errors_ppm else 0.0,
    }


def _offset_only(rows: Sequence[Mapping[str, Any]]) -> tuple[float, float]:
    raw = _residual_metrics(rows, c0=0.0, c1=1.0)
    return -float(raw["mean_error_ppm"]), 1.0


def _gain_only(rows: Sequence[Mapping[str, Any]]) -> tuple[float, float]:
    numerator = 0.0
    denominator = 0.0
    for row in rows:
        measured = float(row["_measured"])
        target = float(row["_target"])
        numerator += measured * target
        denominator += measured * measured
    if denominator <= 0.0:
        return 0.0, 1.0
    return 0.0, numerator / denominator


def _affine_fit(rows: Sequence[Mapping[str, Any]]) -> tuple[float, float]:
    n = float(len(rows))
    if n <= 0:
        return 0.0, 1.0
    sum_x = sum(float(row["_measured"]) for row in rows)
    sum_y = sum(float(row["_target"]) for row in rows)
    sum_xx = sum(float(row["_measured"]) ** 2 for row in rows)
    sum_xy = sum(float(row["_measured"]) * float(row["_target"]) for row in rows)
    denominator = n * sum_xx - sum_x * sum_x
    if abs(denominator) <= 1.0e-12:
        return _offset_only(rows)
    c1 = (n * sum_xy - sum_x * sum_y) / denominator
    c0 = (sum_y - c1 * sum_x) / n
    return c0, c1


def _candidate_delta(
    current: Mapping[str, float], candidate: Mapping[str, float], *, limit: float
) -> tuple[float, str, str]:
    if not candidate:
        return 0.0, "candidate_missing", ""
    if not current:
        return 0.0, "current_getco_missing", ""
    deltas: List[float] = []
    parts: List[str] = []
    for term in CO2_MODEL_TERMS:
        if term not in candidate:
            continue
        now = current.get(term)
        cand = candidate.get(term)
        if now is None or cand is None:
            continue
        delta = float(cand) - float(now)
        deltas.append(abs(delta))
        if abs(delta) > limit:
            parts.append(f"{term}:{delta:.6g}")
    max_delta = max(deltas, default=0.0)
    if max_delta <= limit and candidate:
        return max_delta, "matches_latest_candidate", ""
    return max_delta, "different_from_latest_candidate", ";".join(parts)


def _predict_with_coefficients(row: Mapping[str, Any], coefficients: Mapping[str, float]) -> Optional[float]:
    ratio = _safe_float(row.get("r_co2") or row.get("ratio"))
    temp_c = _safe_float(row.get("t1_c") or row.get("temperature_c"))
    if ratio is None or temp_c is None or not coefficients:
        return None
    raw = direct_ratio_model_prediction(coefficients, ratio=ratio, temperature_c=temp_c)
    return co2_raw_to_firmware_final_ppm(raw, row.get("h2o_mmol_mol"))


def build_co2_intercept_root_cause_tables(
    *,
    point_errors_csv: str | Path,
    candidate_dir: str | Path | None = None,
    current_getco_json: str | Path | None = None,
    cfg: Co2InterceptRootCauseConfig = Co2InterceptRootCauseConfig(),
) -> Dict[str, List[Dict[str, Any]]]:
    """Build no-write root-cause diagnostic tables."""

    source_rows = _read_csv(point_errors_csv)
    grouped, rejected = _group_point_rows(source_rows, cfg)
    current_coeffs = _load_current_getco_coefficients(current_getco_json)
    candidate_coeffs = _load_candidate_coefficients(candidate_dir)
    fit_coverage_rows, fit_coverage_summary = _candidate_fit_coverage(candidate_dir)
    candidate_fit_state = _candidate_fit_state_index(candidate_dir)
    target_devices = {_device_id(value) for value in cfg.target_device_ids}
    devices = sorted(target_devices or set(grouped))

    summary: List[Dict[str, Any]] = []
    point_rows: List[Dict[str, Any]] = []
    coefficient_rows: List[Dict[str, Any]] = []
    ratio_state_rows: List[Dict[str, Any]] = []

    for dev in devices:
        rows = grouped.get(dev, [])
        raw = _residual_metrics(rows, c0=0.0, c1=1.0)
        offset_c0, offset_c1 = _offset_only(rows)
        gain_c0, gain_c1 = _gain_only(rows)
        affine_c0, affine_c1 = _affine_fit(rows)
        offset_metrics = _residual_metrics(rows, c0=offset_c0, c1=offset_c1)
        gain_metrics = _residual_metrics(rows, c0=gain_c0, c1=gain_c1)
        affine_metrics = _residual_metrics(rows, c0=affine_c0, c1=affine_c1)
        max_h2o = max((_safe_float(row.get("h2o_mmol_mol")) or 0.0 for row in rows), default=0.0)
        pressure_values = [_safe_float(row.get("pressure_kpa")) for row in rows]
        pressure_values = [float(value) for value in pressure_values if value is not None]
        pressure_span = max(pressure_values) - min(pressure_values) if pressure_values else 0.0
        current = current_coeffs.get(dev, {})
        candidate = candidate_coeffs.get(dev, {})
        fit_summary = fit_coverage_summary.get(dev, {})
        max_delta, candidate_status, delta_detail = _candidate_delta(
            current, candidate, limit=float(cfg.candidate_delta_limit)
        )

        raw_max = float(raw["max_abs_error_ppm"])
        offset_max = float(offset_metrics["max_abs_error_ppm"])
        improvement = raw_max - offset_max
        offset_dominance = improvement / raw_max if raw_max > 0.0 else 0.0
        h2o_status = "h2o_too_low_to_explain_bias" if max_h2o <= cfg.h2o_low_mmol_mol else "h2o_can_shift_final_ppm"
        pressure_status = (
            "single_atmosphere_pressure_span_not_fit_basis"
            if pressure_span < 1.0
            else "pressure_span_present_review_separately"
        )
        latest_candidate_max_pct = ""
        current_model_max_pct = ""
        latest_errors_pct: List[float] = []
        current_errors_pct: List[float] = []
        ratio_state_deltas: List[float] = []
        ratio_state_conflict_count = 0
        for row in rows:
            target = float(row["_target"])
            latest_pred = _predict_with_coefficients(row, candidate)
            current_pred = _predict_with_coefficients(row, current)
            latest_error = latest_pred - target if latest_pred is not None else None
            current_error = current_pred - target if current_pred is not None else None
            if latest_error is not None and target:
                latest_errors_pct.append(latest_error / target * 100.0)
            if current_error is not None and target:
                current_errors_pct.append(current_error / target * 100.0)
            ratio = _safe_float(row.get("r_co2") or row.get("ratio"))
            temp = _safe_float(row.get("t1_c") or row.get("temperature_c"))
            temp_group = _temperature_group(temp)
            state_key = (dev, round(target, 6), temp_group)
            state = candidate_fit_state.get(state_key, {})
            candidate_ratio = _safe_float(state.get("candidate_ratio_mean"))
            ratio_delta = None
            ratio_state_status = "fit_state_missing_for_this_target_temperature"
            if ratio is not None and candidate_ratio is not None:
                ratio_delta = float(ratio) - float(candidate_ratio)
                ratio_state_deltas.append(abs(ratio_delta))
                ratio_state_status = (
                    "ratio_state_conflict_blocks_final_write"
                    if abs(ratio_delta) > float(cfg.ratio_state_conflict_limit)
                    else "ratio_state_consistent"
                )
                if ratio_state_status == "ratio_state_conflict_blocks_final_write":
                    ratio_state_conflict_count += 1
            ratio_state_rows.append(
                {
                    "device_id": dev,
                    "point": row.get("point") or row.get("point_run_id") or "",
                    "target_ppm": target,
                    "temperature_group": temp_group,
                    "current_ratio": "" if ratio is None else ratio,
                    "candidate_fit_ratio_mean": "" if candidate_ratio is None else candidate_ratio,
                    "ratio_delta_current_minus_fit": "" if ratio_delta is None else ratio_delta,
                    "ratio_state_status": ratio_state_status,
                    "candidate_point_count": state.get("candidate_point_count", ""),
                    "candidate_point_identities": state.get("candidate_point_identities", ""),
                    "physical_meaning": (
                        "Same gas certificate point and same temperature group should not shift this much in filtered "
                        "CO2 ratio. A large delta means the fitting dataset and retest dataset are different physical "
                        "states, so a higher-order polynomial would be hiding route/gas/sensor memory instead of "
                        "calibrating the analyzer."
                    ),
                }
            )
            point_rows.append(
                {
                    "device_id": dev,
                    "point": row.get("point") or row.get("point_run_id") or "",
                    "target_ppm": target,
                    "measured_ppm": float(row["_measured"]),
                    "observed_error_ppm": float(row["_measured"]) - target,
                    "observed_error_pct": (float(row["_measured"]) - target) / target * 100.0 if target else 0.0,
                    "h2o_mmol_mol": row.get("h2o_mmol_mol", ""),
                    "pressure_kpa": row.get("pressure_kpa", ""),
                    "ratio": row.get("r_co2") or row.get("ratio") or "",
                    "temperature_c": row.get("t1_c") or row.get("temperature_c") or "",
                    "offset_only_ppm": float(row["_measured"]) * offset_c1 + offset_c0,
                    "offset_only_error_pct": (
                        (float(row["_measured"]) * offset_c1 + offset_c0 - target) / target * 100.0 if target else 0.0
                    ),
                    "latest_candidate_predicted_ppm": "" if latest_pred is None else latest_pred,
                    "latest_candidate_error_ppm": "" if latest_error is None else latest_error,
                    "current_getco_model_predicted_ppm": "" if current_pred is None else current_pred,
                    "current_getco_model_error_ppm": "" if current_error is None else current_error,
                }
            )
        if latest_errors_pct:
            latest_candidate_max_pct = max(abs(value) for value in latest_errors_pct)
        if current_errors_pct:
            current_model_max_pct = max(abs(value) for value in current_errors_pct)
        ratio_state_max_abs_delta = max(ratio_state_deltas, default="")
        ratio_state_status = (
            "ratio_state_conflict_blocks_final_write"
            if ratio_state_conflict_count > 0
            else ("ratio_state_consistent" if ratio_state_deltas else "ratio_state_not_comparable")
        )
        latest_candidate_predicts_fail = (
            latest_candidate_max_pct != "" and float(latest_candidate_max_pct) > float(cfg.acceptance_pct)
        )
        latest_candidate_predicts_pass = (
            latest_candidate_max_pct != "" and float(latest_candidate_max_pct) <= float(cfg.acceptance_pct)
        )
        if ratio_state_status == "ratio_state_conflict_blocks_final_write":
            root = "ratio_state_conflict_between_fit_and_retest"
        elif candidate_status == "different_from_latest_candidate" and latest_candidate_predicts_fail:
            root = "latest_candidate_different_but_still_fails_offline_replay"
        elif candidate_status == "different_from_latest_candidate" and latest_candidate_predicts_pass:
            root = "current_getco_not_latest_candidate_first_review_latest_senco13_candidate"
        elif candidate_status == "different_from_latest_candidate":
            root = "current_getco_not_latest_candidate_but_replay_missing"
        elif offset_dominance >= cfg.offset_dominance_ratio:
            root = "predominant_intercept_offset"
        elif max_h2o > cfg.h2o_low_mmol_mol:
            root = "humidity_cross_compensation_or_h2o_channel"
        else:
            root = "mixed_residual_review_temperature_route_and_ratio"

        summary.append(
            {
                "device_id": dev,
                "point_count": len(rows),
                "observed_mean_error_ppm": raw["mean_error_ppm"],
                "observed_max_abs_error_ppm": raw["max_abs_error_ppm"],
                "observed_max_abs_error_pct": raw["max_abs_error_pct"],
                "offset_only_C0_delta_ppm": offset_c0,
                "offset_only_C1": offset_c1,
                "offset_only_max_abs_error_ppm": offset_metrics["max_abs_error_ppm"],
                "offset_only_max_abs_error_pct": offset_metrics["max_abs_error_pct"],
                "gain_only_C0": gain_c0,
                "gain_only_C1": gain_c1,
                "gain_only_max_abs_error_pct": gain_metrics["max_abs_error_pct"],
                "affine_C0": affine_c0,
                "affine_C1": affine_c1,
                "affine_max_abs_error_pct": affine_metrics["max_abs_error_pct"],
                "offset_dominance_ratio": offset_dominance,
                "max_h2o_mmol_mol": max_h2o,
                "h2o_status": h2o_status,
                "pressure_span_kpa": pressure_span,
                "pressure_status": pressure_status,
                "current_vs_latest_candidate_status": candidate_status,
                "current_vs_latest_max_abs_coeff_delta": max_delta,
                "current_vs_latest_delta_terms": delta_detail,
                "latest_candidate_predicted_max_abs_error_pct": latest_candidate_max_pct,
                "latest_candidate_prediction_status": (
                    "predicted_pass"
                    if latest_candidate_predicts_pass
                    else ("predicted_fail" if latest_candidate_predicts_fail else "prediction_missing")
                ),
                "ratio_state_status": ratio_state_status,
                "ratio_state_conflict_count": ratio_state_conflict_count,
                "ratio_state_max_abs_delta": ratio_state_max_abs_delta,
                "ratio_state_conflict_limit": cfg.ratio_state_conflict_limit,
                "candidate_fit_temperature_count": fit_summary.get("candidate_fit_temperature_count", ""),
                "candidate_fit_min_targets_per_temperature": fit_summary.get(
                    "candidate_fit_min_targets_per_temperature", ""
                ),
                "candidate_fit_max_targets_per_temperature": fit_summary.get(
                    "candidate_fit_max_targets_per_temperature", ""
                ),
                "candidate_fit_total_temperature_target_slots": fit_summary.get(
                    "candidate_fit_total_temperature_target_slots", ""
                ),
                "candidate_fit_dominant_temperature_group": fit_summary.get(
                    "candidate_fit_dominant_temperature_group", ""
                ),
                "candidate_fit_dominant_temperature_weight_fraction": fit_summary.get(
                    "candidate_fit_dominant_temperature_weight_fraction", ""
                ),
                "candidate_fit_temperature_mean_error_span_ppm": fit_summary.get(
                    "candidate_fit_temperature_mean_error_span_ppm", ""
                ),
                "candidate_fit_grid_status": fit_summary.get("candidate_fit_grid_status", "fit_coverage_missing"),
                "candidate_fit_zero_anchor_count": fit_summary.get("candidate_fit_zero_anchor_count", ""),
                "candidate_fit_certified_zero_anchor_count": fit_summary.get(
                    "candidate_fit_certified_zero_anchor_count", ""
                ),
                "candidate_fit_zero_anchor_status": fit_summary.get(
                    "candidate_fit_zero_anchor_status", "fit_coverage_missing"
                ),
                "candidate_fit_zero_anchor_identities": fit_summary.get("candidate_fit_zero_anchor_identities", ""),
                "current_getco_model_predicted_max_abs_error_pct": current_model_max_pct,
                "root_cause_class": root,
                "acceptance_pct": cfg.acceptance_pct,
                "recommended_next_step": (
                    "Do not force a higher-order polynomial. Recollect clean open-flow points after N2/dry purge for this analyzer state, then fit SENCO1/SENCO3 from the consistent state."
                    if root == "ratio_state_conflict_between_fit_and_retest"
                    else (
                    "Do not write the latest candidate as-is; its offline replay still exceeds acceptance, so review intercept/temperature terms and route evidence first."
                    if root == "latest_candidate_different_but_still_fails_offline_replay"
                    else (
                        "Review and controlled-write the latest no-pressure SENCO1/SENCO3 candidate before adding a final-output affine layer."
                        if root == "current_getco_not_latest_candidate_first_review_latest_senco13_candidate"
                        else (
                            "Prefer SENCO1 intercept/lower-chain review before any final-output SENCO5 affine layer."
                            if root == "predominant_intercept_offset"
                            else "Use N2 prepurge and repeated dry check to separate route memory from model residual."
                        )
                    )
                    )
                ),
            }
        )

        for term in CO2_MODEL_TERMS:
            coefficient_rows.append(
                {
                    "device_id": dev,
                    "term": term,
                    "current_getco": current.get(term, ""),
                    "latest_candidate": candidate.get(term, ""),
                    "latest_minus_current": (
                        ""
                        if term not in current or term not in candidate
                        else float(candidate[term]) - float(current[term])
                    ),
                }
            )

    run_status = "review_required" if any(row["root_cause_class"] != "predominant_intercept_offset" for row in summary) else "intercept_offset_dominant"
    run_summary = [
        {
            "created_at": _now(),
            "point_errors_csv": str(Path(point_errors_csv).resolve()),
            "candidate_dir": "" if candidate_dir is None else str(Path(candidate_dir).resolve()),
            "current_getco_json": "" if current_getco_json is None else str(Path(current_getco_json).resolve()),
            "run_status": run_status,
            "opens_com_ports": False,
            "controls_water_or_gas_routes": False,
            "writes_coefficients": False,
            "physical_scope": "offline_co2_intercept_humidity_pressure_candidate_consistency_diagnostic",
        }
    ]
    return {
        "run_summary": run_summary,
        "device_summary": summary,
        "point_diagnostics": point_rows,
        "ratio_state_diagnostics": ratio_state_rows,
        "coefficient_deltas": coefficient_rows,
        "candidate_fit_coverage": fit_coverage_rows,
        "rejected_rows": rejected,
    }


def write_co2_intercept_root_cause_report(
    *,
    point_errors_csv: str | Path,
    output_dir: str | Path,
    candidate_dir: str | Path | None = None,
    current_getco_json: str | Path | None = None,
    cfg: Co2InterceptRootCauseConfig = Co2InterceptRootCauseConfig(),
) -> Dict[str, Path]:
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    tables = build_co2_intercept_root_cause_tables(
        point_errors_csv=point_errors_csv,
        candidate_dir=candidate_dir,
        current_getco_json=current_getco_json,
        cfg=cfg,
    )
    paths = {
        "run_summary": output / "co2_intercept_root_cause_run_summary.csv",
        "device_summary": output / "co2_intercept_root_cause_device_summary.csv",
        "point_diagnostics": output / "co2_intercept_root_cause_point_diagnostics.csv",
        "ratio_state_diagnostics": output / "co2_intercept_root_cause_ratio_state_diagnostics.csv",
        "coefficient_deltas": output / "co2_intercept_root_cause_coefficient_deltas.csv",
        "candidate_fit_coverage": output / "co2_intercept_root_cause_candidate_fit_coverage.csv",
        "rejected_rows": output / "co2_intercept_root_cause_rejected_rows.csv",
        "metadata": output / "co2_intercept_root_cause_meta.json",
        "markdown": output / "co2_intercept_root_cause_review.md",
    }
    for key in (
        "run_summary",
        "device_summary",
        "point_diagnostics",
        "ratio_state_diagnostics",
        "coefficient_deltas",
        "candidate_fit_coverage",
        "rejected_rows",
    ):
        _write_csv(paths[key], tables[key])
    paths["metadata"].write_text(
        json.dumps(
            {
                "tool": "co2_intercept_root_cause",
                "created_at": _now(),
                "config": {
                    "target_device_ids": list(cfg.target_device_ids),
                    "exclude_device_ids": list(cfg.exclude_device_ids),
                    "acceptance_pct": cfg.acceptance_pct,
                    "h2o_low_mmol_mol": cfg.h2o_low_mmol_mol,
                    "candidate_delta_limit": cfg.candidate_delta_limit,
                    "offset_dominance_ratio": cfg.offset_dominance_ratio,
                    "ratio_state_conflict_limit": cfg.ratio_state_conflict_limit,
                },
                "opens_com_ports": False,
                "controls_water_or_gas_routes": False,
                "writes_coefficients": False,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    _write_markdown(paths["markdown"], tables)
    return paths


def _write_markdown(path: Path, tables: Mapping[str, Sequence[Mapping[str, Any]]]) -> None:
    lines = [
        "# V1.5 CO2 Intercept Root-Cause Review",
        "",
        "Offline/no-write diagnostic. It does not open COM ports, control gas or water routes, or write SENCO.",
        "",
        "## Device Summary",
        "",
        "| Device | Root Cause | Mean Error ppm | Raw Max % | Offset C0 ppm | Offset Max % | Ratio State | Fit Grid | Temp Weight | Zero Anchor | H2O Status | Candidate Status | Next Step |",
        "| --- | --- | ---: | ---: | ---: | ---: | --- | --- | --- | --- | --- | --- | --- |",
    ]
    for row in tables.get("device_summary", []):
        lines.append(
            "| {device} | {root} | {mean} | {raw_pct} | {c0} | {off_pct} | {ratio_state} | {grid} | {temp_weight} | {zero_anchor} | {h2o} | {cand} | {next_step} |".format(
                device=row.get("device_id", ""),
                root=row.get("root_cause_class", ""),
                mean=_fmt(row.get("observed_mean_error_ppm")),
                raw_pct=_fmt(row.get("observed_max_abs_error_pct")),
                c0=_fmt(row.get("offset_only_C0_delta_ppm")),
                off_pct=_fmt(row.get("offset_only_max_abs_error_pct")),
                ratio_state=row.get("ratio_state_status", ""),
                grid=row.get("candidate_fit_grid_status", ""),
                temp_weight=(
                    f"T{row.get('candidate_fit_dominant_temperature_group', '')}:"
                    f"{_fmt(row.get('candidate_fit_dominant_temperature_weight_fraction'))}"
                ),
                zero_anchor=row.get("candidate_fit_zero_anchor_status", ""),
                h2o=row.get("h2o_status", ""),
                cand=row.get("current_vs_latest_candidate_status", ""),
                next_step=row.get("recommended_next_step", ""),
            )
        )
    lines.extend(
        [
            "",
            "## Ratio State Diagnostics",
            "",
            "| Device | Point | Target ppm | Temp Group | Current R | Fit R | Delta | Status |",
            "| --- | --- | ---: | --- | ---: | ---: | ---: | --- |",
        ]
    )
    for row in tables.get("ratio_state_diagnostics", [])[:80]:
        lines.append(
            "| {device} | {point} | {target} | {temp} | {current_r} | {fit_r} | {delta} | {status} |".format(
                device=row.get("device_id", ""),
                point=row.get("point", ""),
                target=_fmt(row.get("target_ppm")),
                temp=row.get("temperature_group", ""),
                current_r=_fmt(row.get("current_ratio")),
                fit_r=_fmt(row.get("candidate_fit_ratio_mean")),
                delta=_fmt(row.get("ratio_delta_current_minus_fit")),
                status=row.get("ratio_state_status", ""),
            )
        )
    lines.extend(
        [
            "",
            "## Physical Meaning",
            "",
            "- A nearly constant positive or negative ppm error is an intercept problem before it is a slope problem.",
            "- Low H2O in a dry CO2 check means the firmware dry-basis correction cannot explain a tens-of-ppm CO2 bias.",
            "- A small pressure span near current atmosphere cannot identify pressure terms; pressure remains a separate SENCO9 chain.",
            "- If current GETCO differs from the latest candidate package, review the latest SENCO1/SENCO3 candidate before adding a final-output affine layer.",
            "- An imbalanced temperature/target grid weakens T/T2/RT identifiability; do not treat that candidate as final-write ready.",
            "- A missing certified zero CO2 anchor means the low-end intercept must be inferred from low-span gases; do not replace zero CO2 with the O2 balance value.",
            "- Same gas and same temperature group should produce a consistent filtered CO2 ratio; a large ratio-state delta means the fit data and retest data are different physical states.",
            "- If the same offset persists after a nitrogen prepurge and latest-candidate review, then SENCO1 intercept or an integrated SENCO5 final-output candidate can be reviewed under controlled write rules.",
        ]
    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _fmt(value: Any) -> str:
    numeric = _safe_float(value)
    if numeric is None:
        return ""
    return f"{numeric:.6g}"
