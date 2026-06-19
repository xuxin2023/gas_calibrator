"""Offline CO2/H2O cross-effect review for V1.5.

This module separates three physically different effects:

1. the mandatory dry-basis dilution correction,
2. residual CO2 error after that correction, and
3. non-water failures such as ratio/temperature/optical-state shifts.

It only reads recorded artifacts. It never opens COM ports, controls routes, or
writes SENCO coefficients.
"""

from __future__ import annotations

import csv
import json
import math
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple


MODEL_SPECS: Tuple[Tuple[str, Tuple[str, ...]], ...] = (
    ("h2o_linear", ("intercept", "h2o_mmol_mol")),
    ("h2o_target", ("intercept", "h2o_mmol_mol", "h2o_x_co2_kppm")),
    (
        "h2o_target_temp_pressure",
        ("intercept", "h2o_mmol_mol", "h2o_x_co2_kppm", "h2o_x_temp_c", "h2o_x_pressure_hpa"),
    ),
)


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


def _normal_device_id(value: Any) -> str:
    text = str(value or "").strip().upper()
    if text.startswith("ID") and text[2:].isdigit():
        text = text[2:]
    if text.isdigit():
        return f"{int(text):03d}"
    return text


def _first_present(row: Mapping[str, Any], names: Sequence[str]) -> Tuple[str, Any]:
    lower_map = {str(key).strip().lower(): key for key in row.keys()}
    for name in names:
        key = lower_map.get(name.lower())
        if key is not None:
            value = row.get(key)
            if value not in (None, ""):
                return str(key), value
    return "", None


def _first_float(row: Mapping[str, Any], names: Sequence[str]) -> Tuple[str, Optional[float]]:
    key, value = _first_present(row, names)
    return key, _safe_float(value)


def _has_float(row: Mapping[str, Any], names: Sequence[str]) -> bool:
    return _first_float(row, names)[1] is not None


def _read_csv(path: str | Path) -> List[Dict[str, Any]]:
    source = Path(path)
    with source.open("r", encoding="utf-8-sig", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _write_csv(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    headers: List[str] = []
    for row in rows:
        for key in row:
            if key not in headers:
                headers.append(str(key))
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=headers)
        writer.writeheader()
        writer.writerows([dict(row) for row in rows])


def _dry_factor(h2o_mmol_mol: Optional[float]) -> Optional[float]:
    if h2o_mmol_mol is None:
        return None
    denominator = 1.0 - h2o_mmol_mol / 1000.0
    if denominator <= 0.0:
        return None
    return 1.0 / denominator


def _normalize_pressure_hpa(value: Optional[float]) -> Optional[float]:
    if value is None:
        return None
    # Analyzer pressure fields are sometimes recorded as kPa. A near-ambient
    # value around 101 therefore needs to be converted to 1010 hPa.
    if 80.0 <= value <= 120.0:
        return value * 10.0
    return value


def _features(row: Mapping[str, Any], terms: Sequence[str]) -> List[float]:
    h2o = float(row.get("h2o_mmol_mol") or 0.0)
    target = float(row.get("co2_target_ppm") or 0.0)
    temp = float(row.get("temperature_c") or 20.0)
    pressure = float(row.get("pressure_hpa") or 1013.25)
    values: List[float] = []
    for term in terms:
        if term == "intercept":
            values.append(1.0)
        elif term == "h2o_mmol_mol":
            values.append(h2o)
        elif term == "h2o_x_co2_kppm":
            values.append(h2o * target / 1000.0)
        elif term == "h2o_x_temp_c":
            values.append(h2o * (temp - 20.0))
        elif term == "h2o_x_pressure_hpa":
            values.append(h2o * (pressure - 1013.25))
        else:
            values.append(0.0)
    return values


def _solve_linear_system(matrix: List[List[float]], vector: List[float]) -> Optional[List[float]]:
    n = len(vector)
    aug = [list(matrix[i]) + [float(vector[i])] for i in range(n)]
    for col in range(n):
        pivot = max(range(col, n), key=lambda row: abs(aug[row][col]))
        if abs(aug[pivot][col]) < 1e-12:
            return None
        if pivot != col:
            aug[col], aug[pivot] = aug[pivot], aug[col]
        pivot_value = aug[col][col]
        for idx in range(col, n + 1):
            aug[col][idx] /= pivot_value
        for row in range(n):
            if row == col:
                continue
            factor = aug[row][col]
            if factor == 0.0:
                continue
            for idx in range(col, n + 1):
                aug[row][idx] -= factor * aug[col][idx]
    return [aug[row][n] for row in range(n)]


def _join_key(row: Mapping[str, Any]) -> Optional[Tuple[str, str]]:
    _, device = _first_present(
        row,
        (
            "device_id",
            "analyzer_device_id",
            "ActualDeviceId",
            "Analyzer",
            "分析仪",
        ),
    )
    _, point = _first_present(row, ("PointRow", "点位行号", "point_row", "index"))
    device_id = _normal_device_id(device)
    point_text = str(point or "").strip()
    if not device_id or not point_text:
        return None
    return device_id, point_text


def _auto_join_complementary_rows(
    labeled_rows: Sequence[Tuple[str, Sequence[Mapping[str, Any]]]]
) -> List[Dict[str, Any]]:
    """Bridge split artifacts such as April CO2 result rows + H2O state rows."""

    target_names = (
        "co2_target_ppm",
        "certificate_co2_ppm",
        "target_ppm",
        "target_value",
        "Y_true",
        "ppm_CO2_Tank",
        "ppm_co2_tank",
    )
    final_names = (
        "co2_final_ppm",
        "measured_co2_ppm",
        "CO2_display",
        "ppm_CO2",
        "Y_pred_simple",
        "prediction",
        "predicted_display_ppm",
        "old_display_ppm",
    )
    h2o_names = (
        "h2o_mmol_mol",
        "H2O_mmol_mol",
        "h2o_mmol",
        "H2O_mmol",
        "ppm_H2O",
        "ppm_H2O_Dew",
        "ppm_h2o_dew",
    )
    h2o_by_key: Dict[Tuple[str, str], Tuple[str, Mapping[str, Any]]] = {}
    co2_candidates: List[Tuple[str, Mapping[str, Any]]] = []
    for label, rows in labeled_rows:
        for row in rows:
            key = _join_key(row)
            if key is None:
                continue
            if _has_float(row, h2o_names):
                h2o_by_key.setdefault(key, (label, row))
            if _has_float(row, target_names) and _has_float(row, final_names) and not _has_float(row, h2o_names):
                co2_candidates.append((label, row))

    joined: List[Dict[str, Any]] = []
    seen: set[Tuple[str, str, str, str]] = set()
    for co2_label, co2_row in co2_candidates:
        key = _join_key(co2_row)
        if key is None or key not in h2o_by_key:
            continue
        h2o_label, h2o_row = h2o_by_key[key]
        dedupe = (co2_label, h2o_label, key[0], key[1])
        if dedupe in seen:
            continue
        seen.add(dedupe)
        merged = dict(h2o_row)
        merged.update(dict(co2_row))
        merged["_auto_join_source_label"] = f"auto_join:{co2_label}+{h2o_label}"
        joined.append(merged)
    return joined


def _least_squares(xs: Sequence[Sequence[float]], ys: Sequence[float]) -> Optional[List[float]]:
    if not xs or not ys:
        return None
    cols = len(xs[0])
    xtx = [[0.0 for _ in range(cols)] for _ in range(cols)]
    xty = [0.0 for _ in range(cols)]
    for xrow, y in zip(xs, ys):
        for i in range(cols):
            xty[i] += xrow[i] * y
            for j in range(cols):
                xtx[i][j] += xrow[i] * xrow[j]
    return _solve_linear_system(xtx, xty)


def _metric(values: Sequence[float]) -> Dict[str, Any]:
    if not values:
        return {"count": 0, "mean": None, "max_abs": None, "rms": None}
    mean = sum(values) / len(values)
    return {
        "count": len(values),
        "mean": mean,
        "max_abs": max(abs(value) for value in values),
        "rms": math.sqrt(sum(value * value for value in values) / len(values)),
    }


def _correlation(xs: Sequence[float], ys: Sequence[float]) -> Optional[float]:
    if len(xs) < 2 or len(xs) != len(ys):
        return None
    mx = sum(xs) / len(xs)
    my = sum(ys) / len(ys)
    dx = [x - mx for x in xs]
    dy = [y - my for y in ys]
    sx = math.sqrt(sum(x * x for x in dx))
    sy = math.sqrt(sum(y * y for y in dy))
    if sx <= 0.0 or sy <= 0.0:
        return None
    return sum(x * y for x, y in zip(dx, dy)) / (sx * sy)


def normalize_cross_effect_rows(
    rows: Iterable[Mapping[str, Any]],
    *,
    source_label: str = "",
) -> List[Dict[str, Any]]:
    """Normalize heterogeneous calibration artifacts into one review schema."""

    normalized: List[Dict[str, Any]] = []
    for index, row in enumerate(rows):
        device_key, device_value = _first_present(
            row,
            (
                "device_id",
                "analyzer_device_id",
                "ActualDeviceId",
                "Analyzer",
                "分析仪",
            ),
        )
        target_key, target = _first_float(
            row,
            (
                "co2_target_ppm",
                "certificate_co2_ppm",
                "target_ppm",
                "target_value",
                "Y_true",
                "ppm_CO2_Tank",
                "ppm_co2_tank",
            ),
        )
        final_key, final_ppm = _first_float(
            row,
            (
                "co2_final_ppm",
                "measured_co2_ppm",
                "CO2_display",
                "ppm_CO2",
                "Y_pred_simple",
                "prediction",
                "predicted_display_ppm",
                "old_display_ppm",
            ),
        )
        raw_key, raw_ppm = _first_float(
            row,
            (
                "co2_uncompensated_ppm",
                "raw_senco13_co2_ppm",
                "raw_predicted_ppm",
                "raw_co2_ppm",
                "Raw_CO2_ppm",
            ),
        )
        h2o_key, h2o = _first_float(
            row,
            (
                "h2o_mmol_mol",
                "H2O_mmol_mol",
                "h2o_mmol",
                "H2O_mmol",
                "ppm_H2O",
                "ppm_H2O_Dew",
                "ppm_h2o_dew",
            ),
        )
        ratio_key, ratio = _first_float(row, ("R_CO2", "ratio", "co2_ratio_f", "co2_ratio_filtered_mean", "R"))
        temp_key, temperature = _first_float(row, ("temperature_c", "Temp", "T1", "t1_c", "chamber_temp_c"))
        pressure_key, pressure_raw = _first_float(row, ("pressure_hpa", "P", "BAR", "P_fit", "pressure"))
        pressure = _normalize_pressure_hpa(pressure_raw)
        ref_key, ref_signal = _first_float(row, ("Raw_REF", "ref_signal", "reference_signal"))
        co2_signal_key, co2_signal = _first_float(row, ("Raw_CO2", "co2_signal"))
        h2o_signal_key, h2o_signal = _first_float(row, ("Raw_H2O", "h2o_signal"))
        factor = _dry_factor(h2o)
        raw_estimated = False
        if raw_ppm is None and final_ppm is not None and factor is not None:
            raw_ppm = final_ppm / factor
            raw_estimated = True
        co2_dry0 = None if raw_ppm is None or factor is None else raw_ppm * factor
        residual_ppm = None if target is None or co2_dry0 is None else target - co2_dry0
        residual_pct = (
            None
            if residual_ppm is None or target is None or abs(target) < 1e-12
            else residual_ppm / target * 100.0
        )
        h2o_effect_pct = None if factor is None else (factor - 1.0) * 100.0
        target_role = (
            "co2_zero_anchor_not_cross_fit"
            if target is not None and abs(target) < 1e-12
            else "co2_nonzero_cross_candidate"
            if target is not None
            else "missing_co2_target"
        )
        if h2o is None:
            row_status = "missing_h2o"
        elif h2o >= 80.0:
            row_status = "h2o_channel_or_humid_state_severe"
        elif residual_pct is None:
            row_status = "diagnostic_only"
        elif h2o_effect_pct is not None and abs(h2o_effect_pct) < 0.2 and abs(residual_pct) >= 2.0:
            row_status = "residual_too_large_for_dry_dilution"
        else:
            row_status = "eligible_cross_review"
        normalized.append(
            {
                "source_label": source_label,
                "source_row_index": index,
                "device_id": _normal_device_id(device_value),
                "co2_target_ppm": target,
                "co2_final_ppm": final_ppm,
                "co2_uncompensated_ppm": raw_ppm,
                "co2_raw_estimated_from_final": raw_estimated,
                "h2o_mmol_mol": h2o,
                "h2o_mole_fraction": None if h2o is None else h2o / 1000.0,
                "co2_dry_correction_factor": factor,
                "h2o_dry_correction_pct": h2o_effect_pct,
                "co2_dry0_ppm": co2_dry0,
                "residual_after_dry_ppm": residual_ppm,
                "residual_after_dry_pct": residual_pct,
                "ratio_co2": ratio,
                "temperature_c": temperature,
                "pressure_hpa": pressure,
                "ref_signal": ref_signal,
                "co2_signal": co2_signal,
                "h2o_signal": h2o_signal,
                "target_role": target_role,
                "row_status": row_status,
                "device_column": device_key,
                "target_column": target_key,
                "final_column": final_key,
                "raw_column": raw_key,
                "h2o_column": h2o_key,
                "ratio_column": ratio_key,
                "temperature_column": temp_key,
                "pressure_column": pressure_key,
                "ref_signal_column": ref_key,
                "co2_signal_column": co2_signal_key,
                "h2o_signal_column": h2o_signal_key,
            }
        )
    return normalized


def _eligible_model_rows(rows: Sequence[Mapping[str, Any]]) -> List[Mapping[str, Any]]:
    return [
        row
        for row in rows
        if row.get("target_role") == "co2_nonzero_cross_candidate"
        and row.get("row_status") in {"eligible_cross_review", "residual_too_large_for_dry_dilution"}
        and _safe_float(row.get("h2o_mmol_mol")) is not None
        and _safe_float(row.get("residual_after_dry_ppm")) is not None
    ]


def _fit_coefficients(rows: Sequence[Mapping[str, Any]], terms: Sequence[str]) -> Optional[List[float]]:
    xs = [_features(row, terms) for row in rows]
    ys = [float(row["residual_after_dry_ppm"]) for row in rows]
    return _least_squares(xs, ys)


def _predict_correction(row: Mapping[str, Any], terms: Sequence[str], coeffs: Sequence[float]) -> float:
    return sum(coef * value for coef, value in zip(coeffs, _features(row, terms)))


def _evaluate_model_rows(
    rows: Sequence[Mapping[str, Any]],
    terms: Sequence[str],
    coeffs: Sequence[float],
) -> Dict[str, Any]:
    before = [float(row["residual_after_dry_ppm"]) for row in rows]
    after = [float(row["residual_after_dry_ppm"]) - _predict_correction(row, terms, coeffs) for row in rows]
    before_metric = _metric(before)
    after_metric = _metric(after)
    improvement = None
    if before_metric["rms"] not in (None, 0):
        improvement = (float(before_metric["rms"]) - float(after_metric["rms"])) / float(before_metric["rms"]) * 100.0
    return {
        "before_rms_ppm": before_metric["rms"],
        "after_rms_ppm": after_metric["rms"],
        "before_max_abs_ppm": before_metric["max_abs"],
        "after_max_abs_ppm": after_metric["max_abs"],
        "rms_improvement_pct": improvement,
    }


def _fit_model(rows: Sequence[Mapping[str, Any]], model_id: str, terms: Sequence[str]) -> Dict[str, Any]:
    eligible = _eligible_model_rows(rows)
    if len(eligible) < len(terms) + 3:
        return {
            "model_id": model_id,
            "term_count": len(terms),
            "fit_point_count": len(eligible),
            "status": "insufficient_points",
            "reason": f"needs_at_least_{len(terms) + 3}_points_to_reduce_overfit_risk",
        }
    coeffs = _fit_coefficients(eligible, terms)
    if coeffs is None:
        return {
            "model_id": model_id,
            "term_count": len(terms),
            "fit_point_count": len(eligible),
            "status": "singular_matrix",
            "reason": "feature_matrix_is_not_independent",
        }
    metrics = _evaluate_model_rows(eligible, terms, coeffs)
    coefficient_text = ";".join(f"{term}={coef:.12g}" for term, coef in zip(terms, coeffs))
    return {
        "model_id": model_id,
        "terms": ";".join(terms),
        "coefficients": coefficient_text,
        "term_count": len(terms),
        "fit_point_count": len(eligible),
        "status": "fit_ok",
        **metrics,
        "reason": "candidate_residual_model_for_offline_review_only",
    }


def _cross_source_validation(rows: Sequence[Mapping[str, Any]]) -> List[Dict[str, Any]]:
    eligible = _eligible_model_rows(rows)
    sources = sorted({str(row.get("source_label") or "") for row in eligible})
    results: List[Dict[str, Any]] = []
    for model_id, terms in MODEL_SPECS:
        for validation_source in sources:
            validation_rows = [row for row in eligible if str(row.get("source_label") or "") == validation_source]
            train_rows = [row for row in eligible if str(row.get("source_label") or "") != validation_source]
            base = {
                "model_id": model_id,
                "validation_source_label": validation_source,
                "term_count": len(terms),
                "train_point_count": len(train_rows),
                "validation_point_count": len(validation_rows),
            }
            if len(validation_rows) < 4:
                results.append(
                    {
                        **base,
                        "status": "skipped_validation_source_too_small",
                        "reason": "needs_at_least_4_validation_points",
                    }
                )
                continue
            if len(train_rows) < len(terms) + 3:
                results.append(
                    {
                        **base,
                        "status": "skipped_train_set_too_small",
                        "reason": f"needs_at_least_{len(terms) + 3}_train_points",
                    }
                )
                continue
            coeffs = _fit_coefficients(train_rows, terms)
            if coeffs is None:
                results.append(
                    {
                        **base,
                        "status": "singular_train_matrix",
                        "reason": "feature_matrix_is_not_independent",
                    }
                )
                continue
            metrics = _evaluate_model_rows(validation_rows, terms, coeffs)
            after_rms = metrics.get("after_rms_ppm")
            before_rms = metrics.get("before_rms_ppm")
            after_max = metrics.get("after_max_abs_ppm")
            before_max = metrics.get("before_max_abs_ppm")
            improvement = metrics.get("rms_improvement_pct")
            if after_rms is not None and before_rms is not None and float(after_rms) > float(before_rms) * 1.05:
                status = "validation_worsened"
            elif after_max is not None and before_max is not None and float(after_max) > float(before_max) * 1.05:
                status = "validation_worsened"
            elif improvement is not None and float(improvement) >= 10.0:
                status = "validation_improved"
            else:
                status = "validation_neutral"
            results.append(
                {
                    **base,
                    "terms": ";".join(terms),
                    "coefficients": ";".join(f"{term}={coef:.12g}" for term, coef in zip(terms, coeffs)),
                    "status": status,
                    **metrics,
                    "reason": "leave_one_source_out_validation",
                }
            )
    return results


def _round_target(value: Any) -> Optional[float]:
    numeric = _safe_float(value)
    if numeric is None:
        return None
    return round(numeric, 6)


def _device_summary(rows: Sequence[Mapping[str, Any]]) -> List[Dict[str, Any]]:
    eligible = _eligible_model_rows(rows)
    devices = sorted({str(row.get("device_id") or "") for row in eligible if str(row.get("device_id") or "")})
    result: List[Dict[str, Any]] = []
    for device_id in devices:
        device_rows = [row for row in eligible if str(row.get("device_id") or "") == device_id]
        residuals = [float(row["residual_after_dry_ppm"]) for row in device_rows]
        h2os = [float(row["h2o_mmol_mol"]) for row in device_rows]
        h2o_span = None if not h2os else max(h2os) - min(h2os)
        corr = _correlation(h2os, residuals)
        status = "diagnostic_only"
        if len(device_rows) >= 6 and h2o_span is not None and h2o_span >= 0.5:
            if corr is not None and abs(corr) >= 0.5:
                status = "device_level_cross_signal_candidate"
            else:
                status = "device_level_no_stable_h2o_correlation"
        elif len(device_rows) < 6:
            status = "insufficient_device_points"
        else:
            status = "insufficient_h2o_span"
        metric = _metric(residuals)
        result.append(
            {
                "device_id": device_id,
                "point_count": len(device_rows),
                "source_count": len({str(row.get("source_label") or "") for row in device_rows}),
                "target_count": len({_round_target(row.get("co2_target_ppm")) for row in device_rows}),
                "h2o_min_mmol_mol": min(h2os) if h2os else None,
                "h2o_max_mmol_mol": max(h2os) if h2os else None,
                "h2o_span_mmol_mol": h2o_span,
                "residual_mean_ppm": metric["mean"],
                "residual_rms_ppm": metric["rms"],
                "residual_max_abs_ppm": metric["max_abs"],
                "h2o_residual_correlation": corr,
                "status": status,
            }
        )
    return result


def _paired_humidity_contrast(rows: Sequence[Mapping[str, Any]]) -> List[Dict[str, Any]]:
    eligible = _eligible_model_rows(rows)
    groups: Dict[Tuple[str, float], List[Mapping[str, Any]]] = {}
    for row in eligible:
        device_id = str(row.get("device_id") or "")
        target = _round_target(row.get("co2_target_ppm"))
        if not device_id or target is None:
            continue
        groups.setdefault((device_id, target), []).append(row)

    result: List[Dict[str, Any]] = []
    for (device_id, target), group_rows in sorted(groups.items()):
        if len(group_rows) < 2:
            continue
        sorted_rows = sorted(group_rows, key=lambda row: float(row["h2o_mmol_mol"]))
        low = sorted_rows[0]
        high = sorted_rows[-1]
        low_h2o = float(low["h2o_mmol_mol"])
        high_h2o = float(high["h2o_mmol_mol"])
        h2o_delta = high_h2o - low_h2o
        if h2o_delta <= 0.0:
            continue
        low_residual = float(low["residual_after_dry_ppm"])
        high_residual = float(high["residual_after_dry_ppm"])
        residual_delta = high_residual - low_residual
        temp_delta = None
        if _safe_float(low.get("temperature_c")) is not None and _safe_float(high.get("temperature_c")) is not None:
            temp_delta = float(high["temperature_c"]) - float(low["temperature_c"])
        pressure_delta = None
        if _safe_float(low.get("pressure_hpa")) is not None and _safe_float(high.get("pressure_hpa")) is not None:
            pressure_delta = float(high["pressure_hpa"]) - float(low["pressure_hpa"])
        ratio_delta = None
        if _safe_float(low.get("ratio_co2")) is not None and _safe_float(high.get("ratio_co2")) is not None:
            ratio_delta = float(high["ratio_co2"]) - float(low["ratio_co2"])
        residual_per_mmol = residual_delta / h2o_delta
        status = "cross_candidate"
        if h2o_delta < 0.5:
            status = "insufficient_h2o_contrast"
        elif temp_delta is not None and abs(temp_delta) > 3.0:
            status = "confounded_by_temperature_state"
        elif pressure_delta is not None and abs(pressure_delta) > 5.0:
            status = "confounded_by_pressure_state"
        elif abs(residual_delta) < 1.0:
            status = "no_material_residual_shift"
        result.append(
            {
                "device_id": device_id,
                "co2_target_ppm": target,
                "point_count": len(group_rows),
                "source_labels": ";".join(sorted({str(row.get("source_label") or "") for row in group_rows})),
                "low_h2o_mmol_mol": low_h2o,
                "high_h2o_mmol_mol": high_h2o,
                "h2o_delta_mmol_mol": h2o_delta,
                "low_residual_ppm": low_residual,
                "high_residual_ppm": high_residual,
                "residual_delta_ppm": residual_delta,
                "residual_per_h2o_mmol_ppm": residual_per_mmol,
                "temperature_delta_c": temp_delta,
                "pressure_delta_hpa": pressure_delta,
                "ratio_delta": ratio_delta,
                "low_source_label": low.get("source_label"),
                "high_source_label": high.get("source_label"),
                "status": status,
            }
        )
    return result


def _firmware_recommendation(
    best: Optional[Mapping[str, Any]], validation_rows: Sequence[Mapping[str, Any]]
) -> str:
    if best is None or float(best.get("fit_point_count") or 0) < 12:
        return "keep_dry_basis_correction_only_until_enough_cross_effect_points_exist"
    model_id = str(best.get("model_id") or "")
    rows_for_model = [row for row in validation_rows if row.get("model_id") == model_id and "skipped" not in str(row.get("status"))]
    improved = [row for row in rows_for_model if row.get("status") == "validation_improved"]
    worsened = [row for row in rows_for_model if row.get("status") == "validation_worsened"]
    if len(improved) >= 2 and not worsened:
        return "review_low_order_residual_cross_model_as_optional_firmware_layer_after_cross_source_validation"
    return "keep_dry_basis_correction_only_collect_dedicated_humid_co2_matrix"


def build_co2_h2o_cross_effect_review(
    *,
    csv_paths: Sequence[str | Path],
    source_labels: Sequence[str] | None = None,
) -> Dict[str, Any]:
    """Build an offline review payload from multiple calibration CSV artifacts."""

    normalized_rows: List[Dict[str, Any]] = []
    labeled_rows: List[Tuple[str, List[Dict[str, Any]]]] = []
    for index, csv_path in enumerate(csv_paths):
        label = (
            str(source_labels[index])
            if source_labels is not None and index < len(source_labels) and str(source_labels[index]).strip()
            else Path(csv_path).stem
        )
        raw_rows = _read_csv(csv_path)
        labeled_rows.append((label, raw_rows))
        normalized_rows.extend(normalize_cross_effect_rows(raw_rows, source_label=label))
    auto_join_rows = _auto_join_complementary_rows(labeled_rows)
    for row in auto_join_rows:
        label = str(row.get("_auto_join_source_label") or "auto_join")
        normalized_rows.extend(normalize_cross_effect_rows([row], source_label=label))

    nonzero = [
        row
        for row in normalized_rows
        if row.get("target_role") == "co2_nonzero_cross_candidate"
        and _safe_float(row.get("h2o_mmol_mol")) is not None
        and _safe_float(row.get("residual_after_dry_ppm")) is not None
    ]
    residuals = [float(row["residual_after_dry_ppm"]) for row in nonzero]
    h2os = [float(row["h2o_mmol_mol"]) for row in nonzero]
    model_rows = [_fit_model(normalized_rows, model_id, terms) for model_id, terms in MODEL_SPECS]
    validation_rows = _cross_source_validation(normalized_rows)
    device_rows = _device_summary(normalized_rows)
    pair_rows = _paired_humidity_contrast(normalized_rows)
    fit_ok_models = [row for row in model_rows if row.get("status") == "fit_ok"]
    best = None
    if fit_ok_models:
        best = min(fit_ok_models, key=lambda row: float(row.get("after_rms_ppm") or float("inf")))
    severe_count = sum(1 for row in normalized_rows if row.get("row_status") == "h2o_channel_or_humid_state_severe")
    zero_count = sum(1 for row in normalized_rows if row.get("target_role") == "co2_zero_anchor_not_cross_fit")
    validation_completed = [row for row in validation_rows if "skipped" not in str(row.get("status"))]
    validation_worsened = [row for row in validation_completed if row.get("status") == "validation_worsened"]
    validation_improved = [row for row in validation_completed if row.get("status") == "validation_improved"]
    material_pairs = [row for row in pair_rows if row.get("status") == "cross_candidate"]
    confounded_pairs = [row for row in pair_rows if str(row.get("status") or "").startswith("confounded")]

    source_rows: List[Dict[str, Any]] = []
    for source in sorted({str(row.get("source_label") or "") for row in normalized_rows}):
        rows_for_source = [row for row in normalized_rows if str(row.get("source_label") or "") == source]
        nonzero_for_source = [
            row
            for row in rows_for_source
            if row.get("target_role") == "co2_nonzero_cross_candidate"
            and _safe_float(row.get("h2o_mmol_mol")) is not None
            and _safe_float(row.get("residual_after_dry_ppm")) is not None
        ]
        source_residuals = [float(row["residual_after_dry_ppm"]) for row in nonzero_for_source]
        source_h2o = [float(row["h2o_mmol_mol"]) for row in nonzero_for_source]
        source_rows.append(
            {
                "source_label": source,
                "normalized_row_count": len(rows_for_source),
                "nonzero_cross_candidate_count": len(nonzero_for_source),
                "zero_anchor_count": sum(
                    1 for row in rows_for_source if row.get("target_role") == "co2_zero_anchor_not_cross_fit"
                ),
                "raw_estimated_from_final_count": sum(
                    1 for row in rows_for_source if row.get("co2_raw_estimated_from_final")
                ),
                "eligible_cross_review_count": sum(
                    1 for row in rows_for_source if row.get("row_status") == "eligible_cross_review"
                ),
                "large_residual_low_h2o_count": sum(
                    1 for row in rows_for_source if row.get("row_status") == "residual_too_large_for_dry_dilution"
                ),
                "h2o_residual_correlation": _correlation(source_h2o, source_residuals),
                "residual_rms_ppm": _metric(source_residuals)["rms"],
                "residual_max_abs_ppm": _metric(source_residuals)["max_abs"],
            }
        )

    summary = [
        {
            "created_at": _now(),
            "source_csv_count": len(csv_paths),
            "auto_joined_row_count": len(auto_join_rows),
            "normalized_row_count": len(normalized_rows),
            "nonzero_cross_candidate_count": len(nonzero),
            "zero_anchor_count": zero_count,
            "severe_h2o_state_count": severe_count,
            "residual_h2o_correlation": _correlation(h2os, residuals),
            "residual_mean_ppm": _metric(residuals)["mean"],
            "residual_rms_ppm": _metric(residuals)["rms"],
            "residual_max_abs_ppm": _metric(residuals)["max_abs"],
            "best_model_id": "" if best is None else best.get("model_id"),
            "best_model_after_rms_ppm": "" if best is None else best.get("after_rms_ppm"),
            "cross_source_validation_count": len(validation_completed),
            "cross_source_validation_improved_count": len(validation_improved),
            "cross_source_validation_worsened_count": len(validation_worsened),
            "device_summary_count": len(device_rows),
            "paired_humidity_contrast_count": len(pair_rows),
            "paired_humidity_cross_candidate_count": len(material_pairs),
            "paired_humidity_confounded_count": len(confounded_pairs),
            "firmware_contract_recommendation": _firmware_recommendation(best, validation_rows),
            "opens_com_ports": False,
            "controls_routes": False,
            "writes_coefficients": False,
        }
    ]
    return {
        "summary": summary,
        "source_summary": source_rows,
        "normalized_rows": normalized_rows,
        "model_candidates": model_rows,
        "cross_source_validation": validation_rows,
        "device_summary": device_rows,
        "paired_humidity_contrast": pair_rows,
    }


def write_co2_h2o_cross_effect_review(
    *,
    csv_paths: Sequence[str | Path],
    output_dir: str | Path,
    source_labels: Sequence[str] | None = None,
) -> Dict[str, Path]:
    payload = build_co2_h2o_cross_effect_review(csv_paths=csv_paths, source_labels=source_labels)
    root = Path(output_dir).resolve()
    root.mkdir(parents=True, exist_ok=True)
    summary_csv = root / "co2_h2o_cross_effect_summary.csv"
    source_csv = root / "co2_h2o_cross_effect_source_summary.csv"
    rows_csv = root / "co2_h2o_cross_effect_normalized_rows.csv"
    models_csv = root / "co2_h2o_cross_effect_model_candidates.csv"
    validation_csv = root / "co2_h2o_cross_effect_cross_source_validation.csv"
    device_csv = root / "co2_h2o_cross_effect_device_summary.csv"
    pair_csv = root / "co2_h2o_cross_effect_paired_humidity_contrast.csv"
    json_path = root / "co2_h2o_cross_effect_review.json"
    md_path = root / "co2_h2o_cross_effect_review_zh.md"
    _write_csv(summary_csv, payload["summary"])
    _write_csv(source_csv, payload["source_summary"])
    _write_csv(rows_csv, payload["normalized_rows"])
    _write_csv(models_csv, payload["model_candidates"])
    _write_csv(validation_csv, payload["cross_source_validation"])
    _write_csv(device_csv, payload["device_summary"])
    _write_csv(pair_csv, payload["paired_humidity_contrast"])
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")

    summary = payload["summary"][0]
    lines = [
        "# CO2/H2O 水汽交叉影响离线评估",
        "",
        "## 结论边界",
        "",
        "- 本报告只读取已有 CSV 证据，不打开串口，不控制气路/水路，不写 SENCO。",
        "- 传感器内部现有 `CO2/(1-H2O*0.001)` 是干基稀释修正，应作为第一层物理模型保留。",
        "- 本评估只研究干基稀释修正后的剩余残差是否还能形成稳定水汽交叉模型。",
        "- CO2 零气锚点不作为水汽交叉拟合点；H2O 干气锚点也不能和 CO2 零气概念混用。",
        "",
        "## 汇总",
        "",
        f"- 输入 CSV 数量：`{summary.get('source_csv_count')}`",
        f"- 自动桥接行数：`{summary.get('auto_joined_row_count')}`",
        f"- 归一化行数：`{summary.get('normalized_row_count')}`",
        f"- 非零 CO2 候选点：`{summary.get('nonzero_cross_candidate_count')}`",
        f"- CO2 零气锚点行数：`{summary.get('zero_anchor_count')}`（不作为水汽交叉拟合点）",
        f"- H2O 严重异常/湿态高风险行数：`{summary.get('severe_h2o_state_count')}`",
        f"- 残差-H2O 相关系数：`{summary.get('residual_h2o_correlation')}`",
        f"- 残差 RMS：`{summary.get('residual_rms_ppm')}` ppm",
        f"- 残差最大绝对值：`{summary.get('residual_max_abs_ppm')}` ppm",
        f"- 跨来源验证完成数：`{summary.get('cross_source_validation_count')}`",
        f"- 跨来源改善数：`{summary.get('cross_source_validation_improved_count')}`",
        f"- 跨来源变差数：`{summary.get('cross_source_validation_worsened_count')}`",
        f"- 设备级汇总数：`{summary.get('device_summary_count')}`",
        f"- 同设备同气点湿度配对数：`{summary.get('paired_humidity_contrast_count')}`",
        f"- 湿度配对交叉候选数：`{summary.get('paired_humidity_cross_candidate_count')}`",
        f"- 湿度配对被温压状态混淆数：`{summary.get('paired_humidity_confounded_count')}`",
        f"- 建议：`{summary.get('firmware_contract_recommendation')}`",
        "",
        "## 候选模型",
        "",
        "| 模型 | 状态 | 点数 | 修正前 RMS ppm | 修正后 RMS ppm | RMS 改善 % | 说明 |",
        "| --- | --- | ---: | ---: | ---: | ---: | --- |",
    ]
    for row in payload["model_candidates"]:
        lines.append(
            "| {model} | {status} | {count} | {before} | {after} | {improve} | {reason} |".format(
                model=row.get("model_id", ""),
                status=row.get("status", ""),
                count=row.get("fit_point_count", ""),
                before="" if row.get("before_rms_ppm") is None else f"{float(row['before_rms_ppm']):.6g}",
                after="" if row.get("after_rms_ppm") is None else f"{float(row['after_rms_ppm']):.6g}",
                improve="" if row.get("rms_improvement_pct") is None else f"{float(row['rms_improvement_pct']):.3f}",
                reason=row.get("reason", ""),
            )
        )
    lines.extend(
        [
            "",
            "## 跨来源训练/验证",
            "",
            "| 模型 | 验证来源 | 训练点 | 验证点 | 状态 | 验证前 RMS ppm | 验证后 RMS ppm | RMS 改善 % |",
            "| --- | --- | ---: | ---: | --- | ---: | ---: | ---: |",
        ]
    )
    for row in payload["cross_source_validation"]:
        lines.append(
            "| {model} | {source} | {train} | {validation} | {status} | {before} | {after} | {improve} |".format(
                model=row.get("model_id", ""),
                source=row.get("validation_source_label", ""),
                train=row.get("train_point_count", ""),
                validation=row.get("validation_point_count", ""),
                status=row.get("status", ""),
                before="" if row.get("before_rms_ppm") is None else f"{float(row['before_rms_ppm']):.6g}",
                after="" if row.get("after_rms_ppm") is None else f"{float(row['after_rms_ppm']):.6g}",
                improve="" if row.get("rms_improvement_pct") is None else f"{float(row['rms_improvement_pct']):.3f}",
            )
        )
    lines.extend(
        [
            "",
            "## 设备级诊断",
            "",
            "| 设备 ID | 点数 | 来源数 | 气点数 | H2O 跨度 mmol/mol | 残差 RMS ppm | 最大残差 ppm | H2O-残差相关 | 状态 |",
            "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |",
        ]
    )
    for row in payload["device_summary"]:
        lines.append(
            "| {device} | {points} | {sources} | {targets} | {h2ospan} | {rms} | {maxabs} | {corr} | {status} |".format(
                device=row.get("device_id", ""),
                points=row.get("point_count", ""),
                sources=row.get("source_count", ""),
                targets=row.get("target_count", ""),
                h2ospan="" if row.get("h2o_span_mmol_mol") is None else f"{float(row['h2o_span_mmol_mol']):.6g}",
                rms="" if row.get("residual_rms_ppm") is None else f"{float(row['residual_rms_ppm']):.6g}",
                maxabs="" if row.get("residual_max_abs_ppm") is None else f"{float(row['residual_max_abs_ppm']):.6g}",
                corr="" if row.get("h2o_residual_correlation") is None else f"{float(row['h2o_residual_correlation']):.6g}",
                status=row.get("status", ""),
            )
        )
    lines.extend(
        [
            "",
            "## 同设备同气点湿度配对",
            "",
            "这张表用同一设备、同一 CO2 标气点的最低 H2O 和最高 H2O 状态做对比，避免把不同气点或不同设备混在一起。",
            "",
            "| 设备 ID | CO2 ppm | 点数 | H2O 差 mmol/mol | 残差差 ppm | 每 mmol/mol 残差斜率 | 温度差 C | 压力差 hPa | 状态 |",
            "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |",
        ]
    )
    for row in payload["paired_humidity_contrast"]:
        lines.append(
            "| {device} | {target} | {points} | {dh2o} | {dres} | {slope} | {dtemp} | {dpress} | {status} |".format(
                device=row.get("device_id", ""),
                target="" if row.get("co2_target_ppm") is None else f"{float(row['co2_target_ppm']):.6g}",
                points=row.get("point_count", ""),
                dh2o="" if row.get("h2o_delta_mmol_mol") is None else f"{float(row['h2o_delta_mmol_mol']):.6g}",
                dres="" if row.get("residual_delta_ppm") is None else f"{float(row['residual_delta_ppm']):.6g}",
                slope="" if row.get("residual_per_h2o_mmol_ppm") is None else f"{float(row['residual_per_h2o_mmol_ppm']):.6g}",
                dtemp="" if row.get("temperature_delta_c") is None else f"{float(row['temperature_delta_c']):.6g}",
                dpress="" if row.get("pressure_delta_hpa") is None else f"{float(row['pressure_delta_hpa']):.6g}",
                status=row.get("status", ""),
            )
        )
    lines.extend(
        [
            "",
            "## 分来源诊断",
            "",
            "| 来源 | 行数 | 非零 CO2 点 | 由 final 反推 raw | 低 H2O 大残差 | H2O-残差相关 | 残差 RMS ppm | 最大残差 ppm |",
            "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
        ]
    )
    for row in payload["source_summary"]:
        lines.append(
            "| {source} | {rows} | {nonzero} | {estimated} | {large} | {corr} | {rms} | {maxabs} |".format(
                source=row.get("source_label", ""),
                rows=row.get("normalized_row_count", ""),
                nonzero=row.get("nonzero_cross_candidate_count", ""),
                estimated=row.get("raw_estimated_from_final_count", ""),
                large=row.get("large_residual_low_h2o_count", ""),
                corr="" if row.get("h2o_residual_correlation") is None else f"{float(row['h2o_residual_correlation']):.6g}",
                rms="" if row.get("residual_rms_ppm") is None else f"{float(row['residual_rms_ppm']):.6g}",
                maxabs="" if row.get("residual_max_abs_ppm") is None else f"{float(row['residual_max_abs_ppm']):.6g}",
            )
        )
    lines.extend(
        [
            "",
            "## 固件算法建议",
            "",
            "```text",
            "w = H2O_mmol_mol * 0.001",
            "CO2_dry0 = CO2_uncompensated / (1 - w)",
            "",
            "# 只有在跨日期/跨设备验证通过后，才启用下面的残差层：",
            "CO2_final = CO2_dry0 + f_residual(H2O, CO2_target, T, P)",
            "```",
            "",
            "如果 H2O 通道异常、CO2/ref 信号异常、ratio 未稳定、S5 未中性或未反算，则该残差层应禁用，只输出诊断。",
        ]
    )
    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return {
        "summary_csv": summary_csv,
        "source_summary_csv": source_csv,
        "normalized_rows_csv": rows_csv,
        "model_candidates_csv": models_csv,
        "cross_source_validation_csv": validation_csv,
        "device_summary_csv": device_csv,
        "paired_humidity_contrast_csv": pair_csv,
        "json": json_path,
        "markdown": md_path,
    }
