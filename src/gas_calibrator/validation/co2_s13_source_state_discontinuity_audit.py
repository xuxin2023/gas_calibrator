"""Offline CO2 S1/S3 source-state discontinuity audit for V1.5.

The audit joins model residuals with recorded open-flow physical-state
evidence. It answers whether large CO2 residuals are more consistent with a
writable S1/S3 model limit, an output-layer S5 issue, or a shared point/source
state discontinuity. It never opens COM ports, controls routes, or writes
coefficients.
"""

from __future__ import annotations

import csv
import json
import math
import re
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from statistics import mean
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple


@dataclass(frozen=True)
class Co2S13SourceStateAuditConfig:
    acceptance_pct: float = 1.0
    dry_dewpoint_gate_c: float = -28.0
    ratio_std_a_gate: float = 0.0005
    common_sign_fraction_threshold: float = 0.8
    common_bias_ppm_threshold: float = 3.0
    pressure_span_warn_hpa: float = 5.0
    pressure_point_outlier_hpa: float = 4.0
    h2o_output_warn_mmol: float = 10.0
    low_end_limit_ppm: float = 400.0


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _safe_float(value: Any) -> Optional[float]:
    if value in (None, "", "None", "null"):
        return None
    try:
        numeric = float(value)
    except Exception:
        return None
    return numeric if math.isfinite(numeric) else None


def _device_id(value: Any) -> str:
    text = str(value or "").strip().upper()
    if text.isdigit():
        return f"{int(text):03d}"
    return text


def _read_csv(path: str | Path) -> List[Dict[str, Any]]:
    target = Path(path)
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
        for row in rows:
            writer.writerow({key: row.get(key, "") for key in keys})


def _mean(values: Iterable[Any]) -> Optional[float]:
    clean = [value for value in (_safe_float(item) for item in values) if value is not None]
    return float(sum(clean) / len(clean)) if clean else None


def _minimum(values: Iterable[Any]) -> Optional[float]:
    clean = [value for value in (_safe_float(item) for item in values) if value is not None]
    return min(clean) if clean else None


def _maximum(values: Iterable[Any]) -> Optional[float]:
    clean = [value for value in (_safe_float(item) for item in values) if value is not None]
    return max(clean) if clean else None


def _span(values: Iterable[Any]) -> Optional[float]:
    clean = [value for value in (_safe_float(item) for item in values) if value is not None]
    if len(clean) < 2:
        return None
    return float(max(clean) - min(clean))


def _same_sign_fraction(values: Sequence[float]) -> Tuple[float, str]:
    signs = [1 if value > 0 else -1 if value < 0 else 0 for value in values]
    signs = [item for item in signs if item != 0]
    if not signs:
        return 0.0, "none"
    counts = Counter(signs)
    dominant, count = counts.most_common(1)[0]
    return count / len(signs), "positive" if dominant > 0 else "negative"


def _point_temperature_group(point_identity: Any, fallback: Any = None) -> str:
    text = str(point_identity or "")
    match = re.search(r"T(-?\d+(?:\.\d+)?)", text)
    if match:
        return f"T{float(match.group(1)):g}"
    value = _safe_float(fallback)
    return f"T{value:g}" if value is not None else ""


def _point_target_group(point_identity: Any, target_ppm: Any = None) -> str:
    text = str(point_identity or "")
    match = re.search(r"(-?\d+(?:\.\d+)?)ppm", text, flags=re.IGNORECASE)
    if match:
        return f"{float(match.group(1)):g}ppm"
    value = _safe_float(target_ppm)
    return f"{value:g}ppm" if value is not None else ""


def _target_numeric(group: Any) -> float:
    value = _safe_float(str(group or "").removesuffix("ppm"))
    return float(value) if value is not None else 999999.0


def _temp_numeric(group: Any) -> float:
    value = _safe_float(str(group or "").removeprefix("T"))
    return float(value) if value is not None else 999.0


def _truthy(value: Any) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


def _fit_lookup(fit_rows: Sequence[Mapping[str, Any]]) -> Dict[Tuple[str, str], Dict[str, Any]]:
    grouped: Dict[Tuple[str, str], List[Mapping[str, Any]]] = defaultdict(list)
    for row in fit_rows:
        device = _device_id(row.get("analyzer_device_id") or row.get("device_id"))
        point = str(row.get("point_identity") or "").strip()
        if device and point:
            grouped[(device, point)].append(row)

    out: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for key, rows in grouped.items():
        first = rows[0]
        out[key] = {
            "source_label": first.get("source_label", ""),
            "source_sample_path": first.get("source_sample_path", ""),
            "fit_inclusion_status": first.get("fit_inclusion_status", ""),
            "fit_inclusion_reason": first.get("fit_inclusion_reason", ""),
            "status_register_qc_values": first.get("status_register_qc_values", ""),
            "source_nominal_ppm": first.get("source_nominal_ppm", ""),
            "temp_set_c": _mean(row.get("temp_set_c") for row in rows),
            "temperature_c": _mean(row.get("temperature_c") for row in rows),
            "thermometer_temp_mean_c": _mean(row.get("thermometer_temp_mean_c") for row in rows),
            "chamber_temp_mean_c": _mean(row.get("analyzer_chamber_temp_mean_c") for row in rows),
            "pressure_hpa": _mean(row.get("pressure_hpa") for row in rows),
            "pressure_gauge_mean_hpa": _mean(row.get("pressure_gauge_mean_hpa") for row in rows),
            "dewpoint_mean_c": _mean(row.get("dewpoint_mean_c") for row in rows),
            "co2_ratio_f_std": _mean(row.get("co2_ratio_f_std") for row in rows),
            "co2_ratio_f_std_max": _maximum(row.get("co2_ratio_f_std") for row in rows),
            "co2_ratio_f_mean": _mean(row.get("co2_ratio_f_mean") for row in rows),
            "h2o_mmol_mean": _mean(row.get("h2o_mmol_mean") for row in rows),
            "ref_signal_mean": _mean(row.get("ref_signal_mean") for row in rows),
            "co2_signal_mean": _mean(row.get("co2_signal_mean") for row in rows),
            "sample_count": _minimum(row.get("sample_count") for row in rows),
            "usable_sample_count": _minimum(row.get("usable_sample_count") for row in rows),
        }
    return out


def _best_summary_by_device(
    summary_rows: Sequence[Mapping[str, Any]],
    *,
    diagnostic_only: Optional[bool],
    structure_id: Optional[str] = None,
) -> Dict[str, Mapping[str, Any]]:
    candidates: Dict[str, List[Mapping[str, Any]]] = defaultdict(list)
    for row in summary_rows:
        if structure_id and str(row.get("structure_id") or "") != structure_id:
            continue
        if diagnostic_only is not None and _truthy(row.get("diagnostic_only")) != diagnostic_only:
            continue
        device = _device_id(row.get("device_id"))
        if device:
            candidates[device].append(row)

    selected: Dict[str, Mapping[str, Any]] = {}
    for device, rows in candidates.items():
        selected[device] = min(
            rows,
            key=lambda row: (
                _safe_float(row.get("max_abs_relative_error_percent")) or float("inf"),
                _safe_float(row.get("rmse_ppm")) or float("inf"),
            ),
        )
    return selected


def _matches_summary(residual: Mapping[str, Any], summary: Mapping[str, Any]) -> bool:
    if str(residual.get("structure_id") or "") != str(summary.get("structure_id") or ""):
        return False
    if str(residual.get("objective_id") or "") != str(summary.get("objective_id") or ""):
        return False
    left = _safe_float(residual.get("zero_offset_ppm"))
    right = _safe_float(summary.get("zero_offset_ppm"))
    return left is not None and right is not None and abs(left - right) < 1.0e-9


def _enrich_residuals(
    residual_rows: Sequence[Mapping[str, Any]],
    fit_rows: Sequence[Mapping[str, Any]],
    summary_rows: Sequence[Mapping[str, Any]],
) -> List[Dict[str, Any]]:
    lookup = _fit_lookup(fit_rows)
    current = _best_summary_by_device(
        summary_rows,
        diagnostic_only=False,
        structure_id="current_writable_senco13",
    )
    diagnostic = _best_summary_by_device(summary_rows, diagnostic_only=True)
    specs = [("current_writable_best", current), ("diagnostic_best", diagnostic)]

    out: List[Dict[str, Any]] = []
    for model_role, selected in specs:
        for row in residual_rows:
            device = _device_id(row.get("device_id"))
            summary = selected.get(device)
            if not summary or not _matches_summary(row, summary):
                continue
            point = str(row.get("point_identity") or "")
            phys = lookup.get((device, point), {})
            target = _safe_float(row.get("certificate_target_ppm") or row.get("target_ppm_for_fit"))
            error = _safe_float(row.get("error_ppm"))
            rel = _safe_float(row.get("relative_error_percent"))
            temp_group = _point_temperature_group(point, row.get("temperature_c"))
            target_group = _point_target_group(point, target)
            item: Dict[str, Any] = {
                "model_role": model_role,
                "device_id": device,
                "structure_id": row.get("structure_id", ""),
                "objective_id": row.get("objective_id", ""),
                "zero_offset_ppm": row.get("zero_offset_ppm", ""),
                "diagnostic_only": summary.get("diagnostic_only", ""),
                "point_identity": point,
                "temperature_group": temp_group,
                "target_group": target_group,
                "target_ppm": target if target is not None else "",
                "error_ppm": error if error is not None else "",
                "relative_error_percent": rel if rel is not None else "",
                "prediction_ppm": row.get("prediction_ppm", ""),
                "ratio": row.get("ratio", ""),
                "temperature_c": row.get("temperature_c", ""),
                "source_label": phys.get("source_label", ""),
                "source_sample_path": phys.get("source_sample_path", ""),
                "fit_inclusion_status": phys.get("fit_inclusion_status", ""),
                "status_register_qc_values": phys.get("status_register_qc_values", ""),
                "physical_temp_set_c": phys.get("temp_set_c", ""),
                "physical_thermometer_temp_mean_c": phys.get("thermometer_temp_mean_c", ""),
                "physical_chamber_temp_mean_c": phys.get("chamber_temp_mean_c", ""),
                "physical_pressure_hpa": phys.get("pressure_hpa", ""),
                "physical_pressure_gauge_mean_hpa": phys.get("pressure_gauge_mean_hpa", ""),
                "physical_dewpoint_mean_c": phys.get("dewpoint_mean_c", ""),
                "physical_co2_ratio_f_std": phys.get("co2_ratio_f_std", ""),
                "physical_co2_ratio_f_std_max": phys.get("co2_ratio_f_std_max", ""),
                "physical_h2o_mmol_mean": phys.get("h2o_mmol_mean", ""),
                "physical_ref_signal_mean": phys.get("ref_signal_mean", ""),
                "physical_co2_signal_mean": phys.get("co2_signal_mean", ""),
                "physical_sample_count": phys.get("sample_count", ""),
                "physical_usable_sample_count": phys.get("usable_sample_count", ""),
            }
            out.append(item)
    return out


def _point_bias_rows(rows: Sequence[Mapping[str, Any]], cfg: Co2S13SourceStateAuditConfig) -> List[Dict[str, Any]]:
    grouped: Dict[Tuple[str, str, str], List[Mapping[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[(str(row["model_role"]), str(row["temperature_group"]), str(row["target_group"]))].append(row)

    out: List[Dict[str, Any]] = []
    for (role, temp, target), group in grouped.items():
        errors = [value for value in (_safe_float(row.get("error_ppm")) for row in group) if value is not None]
        rels = [abs(value) for value in (_safe_float(row.get("relative_error_percent")) for row in group) if value is not None]
        same, sign = _same_sign_fraction(errors)
        source_labels = sorted({str(row.get("source_label") or "") for row in group})
        pressure_span = _span(row.get("physical_pressure_hpa") for row in group)
        ratio_max = _maximum(row.get("physical_co2_ratio_f_std_max") or row.get("physical_co2_ratio_f_std") for row in group)
        dew_max = _maximum(row.get("physical_dewpoint_mean_c") for row in group)
        flags: List[str] = []
        if len(source_labels) > 1:
            flags.append("same_point_multiple_source_labels")
        if same >= cfg.common_sign_fraction_threshold and abs(_mean(errors) or 0.0) >= cfg.common_bias_ppm_threshold:
            flags.append("shared_point_bias")
        if pressure_span is not None and pressure_span >= cfg.pressure_span_warn_hpa:
            flags.append("pressure_span_warning")
        if ratio_max is not None and ratio_max <= cfg.ratio_std_a_gate:
            flags.append("ratio_A")
        if dew_max is not None and dew_max <= cfg.dry_dewpoint_gate_c:
            flags.append("deep_dry")
        out.append(
            {
                "model_role": role,
                "temperature_group": temp,
                "target_group": target,
                "source_labels": ";".join(source_labels),
                "device_count": len({str(row.get("device_id") or "") for row in group}),
                "row_count": len(group),
                "mean_error_ppm": _mean(errors),
                "max_abs_relative_error_percent": max(rels) if rels else "",
                "same_sign_fraction": same,
                "dominant_sign": sign,
                "pressure_span_hpa": pressure_span if pressure_span is not None else "",
                "pressure_mean_hpa": _mean(row.get("physical_pressure_hpa") for row in group),
                "dewpoint_max_c": dew_max if dew_max is not None else "",
                "dewpoint_mean_c": _mean(row.get("physical_dewpoint_mean_c") for row in group),
                "ratio_std_max": ratio_max if ratio_max is not None else "",
                "h2o_output_max_mmol": _maximum(row.get("physical_h2o_mmol_mean") for row in group),
                "state_flags": ";".join(flags),
            }
        )
    return sorted(out, key=lambda row: (row["model_role"], _temp_numeric(row["temperature_group"]), _target_numeric(row["target_group"])))


def _temperature_source_rows(rows: Sequence[Mapping[str, Any]], cfg: Co2S13SourceStateAuditConfig) -> List[Dict[str, Any]]:
    grouped: Dict[Tuple[str, str, str], List[Mapping[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[(str(row["model_role"]), str(row["temperature_group"]), str(row.get("source_label") or ""))].append(row)

    out: List[Dict[str, Any]] = []
    for (role, temp, source), group in grouped.items():
        targets = sorted({str(row.get("target_group") or "") for row in group}, key=_target_numeric)
        rels = [abs(value) for value in (_safe_float(row.get("relative_error_percent")) for row in group) if value is not None]
        errors = [value for value in (_safe_float(row.get("error_ppm")) for row in group) if value is not None]
        flags: List[str] = []
        pressure_span = _span(row.get("physical_pressure_hpa") for row in group)
        if pressure_span is not None and pressure_span >= cfg.pressure_span_warn_hpa:
            flags.append("within_source_pressure_span_warning")
        h2o_max = _maximum(row.get("physical_h2o_mmol_mean") for row in group)
        if h2o_max is not None and h2o_max >= cfg.h2o_output_warn_mmol:
            flags.append("analyzer_h2o_output_not_reference_state")
        out.append(
            {
                "model_role": role,
                "temperature_group": temp,
                "source_label": source,
                "target_groups": ";".join(targets),
                "target_count": len(targets),
                "device_count": len({str(row.get("device_id") or "") for row in group}),
                "row_count": len(group),
                "mean_error_ppm": _mean(errors),
                "max_abs_relative_error_percent": max(rels) if rels else "",
                "pressure_mean_hpa": _mean(row.get("physical_pressure_hpa") for row in group),
                "pressure_span_hpa": pressure_span if pressure_span is not None else "",
                "dewpoint_mean_c": _mean(row.get("physical_dewpoint_mean_c") for row in group),
                "dewpoint_span_c": _span(row.get("physical_dewpoint_mean_c") for row in group),
                "ratio_std_max": _maximum(row.get("physical_co2_ratio_f_std_max") or row.get("physical_co2_ratio_f_std") for row in group),
                "h2o_output_max_mmol": h2o_max if h2o_max is not None else "",
                "source_flags": ";".join(flags),
            }
        )
    return sorted(out, key=lambda row: (row["model_role"], _temp_numeric(row["temperature_group"]), str(row["source_label"])))


def _temperature_partition_rows(rows: Sequence[Mapping[str, Any]], cfg: Co2S13SourceStateAuditConfig) -> List[Dict[str, Any]]:
    grouped: Dict[Tuple[str, str], List[Mapping[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[(str(row["model_role"]), str(row["temperature_group"]))].append(row)

    out: List[Dict[str, Any]] = []
    for (role, temp), group in grouped.items():
        sources = sorted({str(row.get("source_label") or "") for row in group})
        targets_by_source: Dict[str, List[str]] = defaultdict(list)
        for row in group:
            target = str(row.get("target_group") or "")
            source = str(row.get("source_label") or "")
            if target not in targets_by_source[source]:
                targets_by_source[source].append(target)
        pressure_by_point = _point_pressure_deviation(group)
        pressure_outliers = [item for item in pressure_by_point if abs(float(item["pressure_delta_from_temp_mean_hpa"])) >= cfg.pressure_point_outlier_hpa]
        flags: List[str] = []
        if len(sources) > 1:
            flags.append("mixed_source_temperature_group")
        if pressure_outliers:
            flags.append("point_pressure_outlier")
        out.append(
            {
                "model_role": role,
                "temperature_group": temp,
                "source_count": len(sources),
                "source_labels": ";".join(sources),
                "source_target_map": " | ".join(
                    f"{source}:{';'.join(sorted(targets, key=_target_numeric))}" for source, targets in sorted(targets_by_source.items())
                ),
                "row_count": len(group),
                "device_count": len({str(row.get("device_id") or "") for row in group}),
                "pressure_mean_hpa": _mean(row.get("physical_pressure_hpa") for row in group),
                "pressure_span_hpa": _span(row.get("physical_pressure_hpa") for row in group),
                "dewpoint_span_c": _span(row.get("physical_dewpoint_mean_c") for row in group),
                "max_abs_relative_error_percent": max(
                    [abs(value) for value in (_safe_float(row.get("relative_error_percent")) for row in group) if value is not None],
                    default="",
                ),
                "pressure_outlier_points": ";".join(item["point_identity"] for item in pressure_outliers),
                "partition_flags": ";".join(flags),
            }
        )
    return sorted(out, key=lambda row: (row["model_role"], _temp_numeric(row["temperature_group"])))


def _point_pressure_deviation(group: Sequence[Mapping[str, Any]]) -> List[Dict[str, Any]]:
    point_groups: Dict[str, List[Mapping[str, Any]]] = defaultdict(list)
    for row in group:
        point_groups[str(row.get("point_identity") or "")].append(row)
    temp_mean = _mean(row.get("physical_pressure_hpa") for row in group)
    out: List[Dict[str, Any]] = []
    if temp_mean is None:
        return out
    for point, rows in point_groups.items():
        pressure = _mean(row.get("physical_pressure_hpa") for row in rows)
        if pressure is None:
            continue
        out.append(
            {
                "point_identity": point,
                "pressure_mean_hpa": pressure,
                "pressure_delta_from_temp_mean_hpa": pressure - temp_mean,
            }
        )
    return out


def _sawtooth_rows(point_bias: Sequence[Mapping[str, Any]], cfg: Co2S13SourceStateAuditConfig) -> List[Dict[str, Any]]:
    grouped: Dict[Tuple[str, str], List[Mapping[str, Any]]] = defaultdict(list)
    for row in point_bias:
        grouped[(str(row.get("model_role") or ""), str(row.get("temperature_group") or ""))].append(row)

    out: List[Dict[str, Any]] = []
    for (role, temp), group in grouped.items():
        ordered = sorted(group, key=lambda row: _target_numeric(row.get("target_group")))
        nonzero = [row for row in ordered if _target_numeric(row.get("target_group")) > 0.0]
        signs: List[int] = []
        signed_points: List[str] = []
        for row in nonzero:
            mean_error = _safe_float(row.get("mean_error_ppm"))
            if mean_error is None or abs(mean_error) < cfg.common_bias_ppm_threshold:
                continue
            sign = 1 if mean_error > 0.0 else -1
            signs.append(sign)
            signed_points.append(f"{row.get('target_group')}:{'+' if sign > 0 else '-'}")
        sign_changes = sum(1 for left, right in zip(signs, signs[1:]) if left != right)
        flags: List[str] = []
        if sign_changes >= 2:
            flags.append("non_affine_sawtooth_bias")
        if sign_changes and len({str(row.get("source_labels") or "") for row in nonzero}) > 1:
            flags.append("source_split_may_alias_sawtooth")
        out.append(
            {
                "model_role": role,
                "temperature_group": temp,
                "signed_point_sequence": ";".join(signed_points),
                "signed_point_count": len(signed_points),
                "sign_change_count": sign_changes,
                "sawtooth_flags": ";".join(flags),
                "physical_meaning": (
                    "若同一温度组随浓度正负交替，S5 的线性 C0/C1 不能根治；应先查气点状态、目标映射或主模型边界。"
                    if flags
                    else "未见明显锯齿型共同偏差。"
                ),
            }
        )
    return sorted(out, key=lambda row: (row["model_role"], _temp_numeric(row["temperature_group"])))


def _decision_rows(
    point_bias: Sequence[Mapping[str, Any]],
    temp_partition: Sequence[Mapping[str, Any]],
    sawtooth: Sequence[Mapping[str, Any]],
    cfg: Co2S13SourceStateAuditConfig,
) -> List[Dict[str, Any]]:
    current_points = [row for row in point_bias if row.get("model_role") == "current_writable_best"]
    diagnostic_points = [row for row in point_bias if row.get("model_role") == "diagnostic_best"]
    current_max = max(
        [abs(value) for value in (_safe_float(row.get("max_abs_relative_error_percent")) for row in current_points) if value is not None],
        default=None,
    )
    diagnostic_max = max(
        [abs(value) for value in (_safe_float(row.get("max_abs_relative_error_percent")) for row in diagnostic_points) if value is not None],
        default=None,
    )
    mixed_temps = [
        row
        for row in temp_partition
        if row.get("model_role") == "current_writable_best" and "mixed_source_temperature_group" in str(row.get("partition_flags") or "")
    ]
    pressure_outliers = [
        row
        for row in temp_partition
        if row.get("model_role") == "current_writable_best" and "point_pressure_outlier" in str(row.get("partition_flags") or "")
    ]
    sawtooth_rows = [
        row
        for row in sawtooth
        if row.get("model_role") == "current_writable_best" and "non_affine_sawtooth_bias" in str(row.get("sawtooth_flags") or "")
    ]
    shared_bias = [
        row
        for row in current_points
        if "shared_point_bias" in str(row.get("state_flags") or "")
        and "ratio_A" in str(row.get("state_flags") or "")
        and "deep_dry" in str(row.get("state_flags") or "")
    ]
    rows: List[Dict[str, Any]] = []
    rows.append(
        {
            "priority": "P0",
            "topic": "当前可写 S1/S3 不能直接放行",
            "finding": f"当前可写模型最大相对误差约 {current_max:.3f}%；诊断增强模型最好仍约 {diagnostic_max:.3f}%。"
            if current_max is not None and diagnostic_max is not None
            else "当前可写模型仍有大残差。",
            "physical_meaning": "如果高容量诊断模型也不能压到目标，问题不只是少一个 S1/S3 项，而是输入点状态或目标值之间存在共同不一致。",
            "action": "先阻断 CO2 写入；继续按源运行和物理状态审计修正点集，不能用 S5 先掩盖。",
        }
    )
    if mixed_temps:
        rows.append(
            {
                "priority": "P0",
                "topic": "同一温度组混入不同运行来源",
                "finding": "检测到温度组由多个 source_label 拼接：" + "; ".join(
                    f"{row.get('temperature_group')}={row.get('source_labels')}" for row in mixed_temps
                ),
                "physical_meaning": "不同运行片段的露点、压力、阀路、端口映射和设备状态不一定属于同一个校准目标状态，直接合并会把共态偏差塞进系数。",
                "action": "同一温度组优先使用同一运行来源的完整点；补点必须带 bridge/状态一致性门禁，未通过则降级为诊断点。",
            }
        )
    if pressure_outliers:
        rows.append(
            {
                "priority": "P1",
                "topic": "开放流通压力状态存在点位离群",
                "finding": "检测到同温度组压力离群点：" + "; ".join(
                    f"{row.get('temperature_group')}:{row.get('pressure_outlier_points')}" for row in pressure_outliers
                ),
                "physical_meaning": "CO2 主拟合冻结压力项并不等于压力状态无意义；压力离群说明流量/排气/阀路状态可能变了，应作为目标状态差异证据。",
                "action": "压力不作为硬阻断，但进入点位审计；超过状态一致性阈值时不自动纳入 A 级拟合。",
            }
        )
    if sawtooth_rows:
        rows.append(
            {
                "priority": "P0",
                "topic": "同温度组残差呈非线性锯齿",
                "finding": "检测到正负交替的共同偏差：" + "; ".join(
                    f"{row.get('temperature_group')}={row.get('signed_point_sequence')}" for row in sawtooth_rows
                ),
                "physical_meaning": "这种误差不是一个输出层 C0/C1 能修掉的线性偏差，更像气瓶/阀路/补点状态或目标映射的离散不一致。",
                "action": "先查对应气瓶证书、阀位映射、source_label、露点/压力轨迹；S5 只能在残差呈平滑仿射趋势后使用。",
            }
        )
    if shared_bias:
        rows.append(
            {
                "priority": "P1",
                "topic": "ratio 与露点已达 A 级但仍有共同偏差",
                "finding": f"共有 {len(shared_bias)} 个点呈现 ratio_A + deep_dry + shared_point_bias。",
                "physical_meaning": "这说明继续延长吹扫未必解决；测量对象已稳定，但目标状态/模型边界或点位目标存在系统差异。",
                "action": "把这些点作为状态/目标审计重点，而不是盲目重跑整轮。",
            }
        )
    rows.append(
        {
            "priority": "P1",
            "topic": "H2O 输出值不能作为 CO2 干燥状态真值",
            "finding": "部分 CO2 干气点的分析仪 h2o_mmol 输出很高，但露点仪显示深干。",
            "physical_meaning": "这通常来自水通道旧系数或输出层修正，不代表管路真实湿度；CO2 主拟合应以露点仪/压力换算和 ratio 稳定为状态证据。",
            "action": "后续拟合输入中将分析仪 H2O 输出标为诊断字段，不作为 CO2 干燥状态硬依据。",
        }
    )
    return rows


def build_co2_s13_source_state_discontinuity_audit(
    *,
    fit_points_csv: str | Path,
    enhanced_summary_csv: str | Path,
    enhanced_residuals_csv: str | Path,
    cfg: Co2S13SourceStateAuditConfig = Co2S13SourceStateAuditConfig(),
) -> Dict[str, List[Dict[str, Any]]]:
    fit_rows = _read_csv(fit_points_csv)
    summary_rows = _read_csv(enhanced_summary_csv)
    residual_rows = _read_csv(enhanced_residuals_csv)
    enriched = _enrich_residuals(residual_rows, fit_rows, summary_rows)
    point_bias = _point_bias_rows(enriched, cfg)
    temp_source = _temperature_source_rows(enriched, cfg)
    temp_partition = _temperature_partition_rows(enriched, cfg)
    sawtooth = _sawtooth_rows(point_bias, cfg)
    decisions = _decision_rows(point_bias, temp_partition, sawtooth, cfg)
    p0_decisions = [row for row in decisions if str(row.get("priority") or "") == "P0"]
    write_gate_status = "blocked_source_state_discontinuity" if p0_decisions else "review_required"
    run_summary = [
        {
            "created_at": _now(),
            "fit_points_csv": str(Path(fit_points_csv).resolve()),
            "enhanced_summary_csv": str(Path(enhanced_summary_csv).resolve()),
            "enhanced_residuals_csv": str(Path(enhanced_residuals_csv).resolve()),
            "fit_row_count": len(fit_rows),
            "selected_residual_row_count": len(enriched),
            "device_count": len({row.get("device_id") for row in enriched}),
            "opens_com_ports": False,
            "controls_water_or_gas_routes": False,
            "writes_coefficients": False,
            "candidate_write_allowed": False,
            "write_gate_status": write_gate_status,
            "write_gate_blocker_count": len(p0_decisions),
            "write_gate_blocker_topics": ";".join(str(row.get("topic") or "") for row in p0_decisions),
            "not_real_acceptance_evidence": True,
        }
    ]
    return {
        "run_summary": run_summary,
        "selected_residuals_with_source_state": enriched,
        "point_common_bias_with_state": point_bias,
        "temperature_source_state_summary": temp_source,
        "temperature_partition_audit": temp_partition,
        "sawtooth_bias_audit": sawtooth,
        "root_cause_decision": decisions,
    }


def write_co2_s13_source_state_discontinuity_audit(
    *,
    fit_points_csv: str | Path,
    enhanced_summary_csv: str | Path,
    enhanced_residuals_csv: str | Path,
    output_dir: str | Path,
    cfg: Co2S13SourceStateAuditConfig = Co2S13SourceStateAuditConfig(),
) -> Dict[str, Path]:
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    tables = build_co2_s13_source_state_discontinuity_audit(
        fit_points_csv=fit_points_csv,
        enhanced_summary_csv=enhanced_summary_csv,
        enhanced_residuals_csv=enhanced_residuals_csv,
        cfg=cfg,
    )
    paths = {
        "run_summary": output / "co2_s13_source_state_run_summary.csv",
        "selected_residuals_with_source_state": output / "co2_s13_selected_residuals_with_source_state.csv",
        "point_common_bias_with_state": output / "co2_s13_point_common_bias_with_state.csv",
        "temperature_source_state_summary": output / "co2_s13_temperature_source_state_summary.csv",
        "temperature_partition_audit": output / "co2_s13_temperature_partition_audit.csv",
        "sawtooth_bias_audit": output / "co2_s13_sawtooth_bias_audit.csv",
        "root_cause_decision": output / "co2_s13_source_state_root_cause_decision.csv",
        "metadata": output / "co2_s13_source_state_discontinuity_meta.json",
        "markdown": output / "co2_s13_source_state_discontinuity_audit_zh.md",
    }
    for key, path in paths.items():
        if key in {"metadata", "markdown"}:
            continue
        _write_csv(path, tables[key])
    paths["metadata"].write_text(
        json.dumps(
            {
                "tool": "co2_s13_source_state_discontinuity_audit",
                "created_at": _now(),
                "input_files": {
                    "fit_points_csv": str(Path(fit_points_csv).resolve()),
                    "enhanced_summary_csv": str(Path(enhanced_summary_csv).resolve()),
                    "enhanced_residuals_csv": str(Path(enhanced_residuals_csv).resolve()),
                },
                "config": cfg.__dict__,
                "boundary": tables["run_summary"][0],
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8-sig",
    )
    _write_markdown(paths["markdown"], tables)
    return paths


def _fmt(value: Any, digits: int = 4) -> str:
    numeric = _safe_float(value)
    if numeric is None:
        return ""
    return f"{numeric:.{digits}g}"


def _top_abs(rows: Sequence[Mapping[str, Any]], field: str, limit: int = 12) -> List[Mapping[str, Any]]:
    return sorted(
        rows,
        key=lambda row: abs(_safe_float(row.get(field)) or 0.0),
        reverse=True,
    )[:limit]


def _write_markdown(path: Path, tables: Mapping[str, Sequence[Mapping[str, Any]]]) -> None:
    summary = tables.get("run_summary", [{}])[0]
    lines = [
        "# V1.5 CO2 S1/S3 源运行与物理状态不连续审计",
        "",
        "本报告只基于已采集证据离线计算，不打开串口、不控制气路水路、不写 SENCO，也不是 real acceptance 证据。",
        "目标是解释为什么当前可写 S1/S3 以及 S5 输出层修正仍然压不住相对误差。",
        "",
        "## 结论摘要",
        "",
    ]
    for row in tables.get("root_cause_decision", []):
        lines.append(f"- **{row.get('priority')} / {row.get('topic')}**：{row.get('finding')} {row.get('physical_meaning')} 建议：{row.get('action')}")
    lines.extend(
        [
            "",
            "## 同一温度组的来源拼接",
            "",
            "| 模型角色 | 温度组 | 来源数 | 来源与气点 | 压力跨度 hPa | 露点跨度 C | 最大相对误差 % | 标记 |",
            "| --- | --- | ---: | --- | ---: | ---: | ---: | --- |",
        ]
    )
    for row in tables.get("temperature_partition_audit", []):
        if row.get("model_role") != "current_writable_best":
            continue
        lines.append(
            "| {role} | {temp} | {count} | {source_map} | {p_span} | {dew_span} | {rel} | {flags} |".format(
                role=row.get("model_role", ""),
                temp=row.get("temperature_group", ""),
                count=row.get("source_count", ""),
                source_map=row.get("source_target_map", ""),
                p_span=_fmt(row.get("pressure_span_hpa")),
                dew_span=_fmt(row.get("dewpoint_span_c")),
                rel=_fmt(row.get("max_abs_relative_error_percent")),
                flags=row.get("partition_flags", ""),
            )
        )
    lines.extend(
        [
            "",
            "## 共同偏差最大的点位",
            "",
            "| 模型角色 | 点位 | 来源 | 均值误差 ppm | 最大相对误差 % | 同号比例 | 压力跨度 hPa | 露点最大 C | ratio std 最大 | 状态标记 |",
            "| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |",
        ]
    )
    current_points = [row for row in tables.get("point_common_bias_with_state", []) if row.get("model_role") == "current_writable_best"]
    for row in _top_abs(current_points, "mean_error_ppm", limit=15):
        lines.append(
            "| {role} | {point} | {sources} | {mean} | {rel} | {same} | {p_span} | {dew} | {ratio} | {flags} |".format(
                role=row.get("model_role", ""),
                point=f"{row.get('temperature_group')}_{row.get('target_group')}",
                sources=row.get("source_labels", ""),
                mean=_fmt(row.get("mean_error_ppm")),
                rel=_fmt(row.get("max_abs_relative_error_percent")),
                same=_fmt(row.get("same_sign_fraction")),
                p_span=_fmt(row.get("pressure_span_hpa")),
                dew=_fmt(row.get("dewpoint_max_c")),
                ratio=_fmt(row.get("ratio_std_max")),
                flags=row.get("state_flags", ""),
            )
        )
    lines.extend(
        [
            "",
            "## 同温度组非线性锯齿检查",
            "",
            "| 模型角色 | 温度组 | 符号序列 | 符号翻转次数 | 标记 | 物理意义 |",
            "| --- | --- | --- | ---: | --- | --- |",
        ]
    )
    for row in tables.get("sawtooth_bias_audit", []):
        if row.get("model_role") != "current_writable_best":
            continue
        lines.append(
            "| {role} | {temp} | {seq} | {changes} | {flags} | {meaning} |".format(
                role=row.get("model_role", ""),
                temp=row.get("temperature_group", ""),
                seq=row.get("signed_point_sequence", ""),
                changes=row.get("sign_change_count", ""),
                flags=row.get("sawtooth_flags", ""),
                meaning=row.get("physical_meaning", ""),
            )
        )
    lines.extend(
        [
            "",
            "## 审计边界",
            "",
            f"- 输入拟合行数：{summary.get('fit_row_count', '')}",
            f"- 选中残差行数：{summary.get('selected_residual_row_count', '')}",
            f"- 设备数：{summary.get('device_count', '')}",
            "- CO2 零气锚点只约束 CO2 低端；H2O 干气锚点必须按露点/压力换算单独处理，不能混成同一个低端锚点。",
        ]
    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8-sig")
