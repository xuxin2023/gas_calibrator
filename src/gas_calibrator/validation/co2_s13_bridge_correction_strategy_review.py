"""Offline CO2 S1/S3 bridge-correction strategy review.

This review consumes the selected residual/state rows from the target-state
bridge audit and compares diagnostic correction strategies. It is intentionally
offline/no-write: it never opens COM ports, controls routes, or writes SENCO.
S5 output trim is reported only as a theoretical post-main-model limit.
"""

from __future__ import annotations

import csv
import json
import math
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from statistics import mean
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence

import numpy as np


LOW_END_LIMIT_PPM = 400.0
S5_ROUND_DIGITS = 3


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _read_csv(path: str | Path | None) -> List[Dict[str, Any]]:
    if not path:
        return []
    target = Path(path)
    if not target.exists():
        return []
    with target.open("r", encoding="utf-8-sig", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _write_csv(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    headers: List[str] = []
    for row in rows:
        for key in row:
            if key not in headers:
                headers.append(str(key))
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=headers)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key, "") for key in headers})


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


def _is_zero_anchor(row: Mapping[str, Any]) -> bool:
    value = str(row.get("is_zero_anchor") or "").strip().lower()
    if value in {"true", "1", "yes"}:
        return True
    identity = str(row.get("point_identity") or "").lower()
    return identity.endswith("_0ppm") or "_0ppm" in identity


def _target_ppm(row: Mapping[str, Any]) -> Optional[float]:
    return _safe_float(row.get("target_ppm") or row.get("target_value"))


def _error_ppm(row: Mapping[str, Any]) -> Optional[float]:
    return _safe_float(row.get("error_ppm"))


def _prediction_ppm(row: Mapping[str, Any]) -> Optional[float]:
    explicit = _safe_float(row.get("prediction_ppm"))
    if explicit is not None:
        return explicit
    target = _target_ppm(row)
    error = _error_ppm(row)
    if target is None or error is None:
        return None
    return target + error


def _relative_error(error: float, target: float) -> Optional[float]:
    if abs(float(target)) <= 1.0e-12:
        return None
    return 100.0 * float(error) / float(target)


def _target_segment(row: Mapping[str, Any]) -> str:
    target = _target_ppm(row)
    if target is None:
        return "unknown"
    if _is_zero_anchor(row):
        return "zero_anchor"
    if target <= 200.0:
        return "low_100_200"
    if target <= LOW_END_LIMIT_PPM:
        return "low_300_400"
    return "high_gt_400"


def _group_rows(rows: Sequence[Mapping[str, Any]]) -> Dict[str, List[Mapping[str, Any]]]:
    grouped: Dict[str, List[Mapping[str, Any]]] = defaultdict(list)
    for row in rows:
        device = _device_id(row.get("device_id") or row.get("analyzer_device_id"))
        if device:
            grouped[device].append(row)
    return dict(grouped)


def _mean_other_error(
    rows: Sequence[Mapping[str, Any]],
    index: int,
    key_fields: Sequence[str],
    *,
    include_zero_anchors: bool = False,
) -> tuple[float, int]:
    row = rows[index]
    key = tuple(str(row.get(field) or "") for field in key_fields)
    errors: List[float] = []
    for other_index, other in enumerate(rows):
        if other_index == index:
            continue
        if not include_zero_anchors and _is_zero_anchor(other):
            continue
        if tuple(str(other.get(field) or "") for field in key_fields) != key:
            continue
        error = _error_ppm(other)
        if error is not None:
            errors.append(float(error))
    if not errors:
        return 0.0, 0
    return float(mean(errors)), len(errors)


def _mean_other_relative_error(
    rows: Sequence[Mapping[str, Any]],
    index: int,
    key_fields: Sequence[str],
) -> tuple[float, int]:
    row = rows[index]
    key = tuple(str(row.get(field) or "") for field in key_fields)
    values: List[float] = []
    for other_index, other in enumerate(rows):
        if other_index == index or _is_zero_anchor(other):
            continue
        if tuple(str(other.get(field) or "") for field in key_fields) != key:
            continue
        error = _error_ppm(other)
        target = _target_ppm(other)
        if error is None or target is None or abs(target) <= 1.0e-12:
            continue
        values.append(100.0 * float(error) / float(target))
    if not values:
        return 0.0, 0
    return float(mean(values)), len(values)


def _affine_trim(
    rows: Sequence[Mapping[str, Any]],
    *,
    relative_weighted: bool,
    rounded: bool,
) -> tuple[float, float]:
    pairs: List[tuple[float, float, float]] = []
    for row in rows:
        target = _target_ppm(row)
        prediction = _prediction_ppm(row)
        if target is None or prediction is None:
            continue
        denominator = max(abs(float(target)), 100.0)
        weight = 1.0 / denominator if relative_weighted else 1.0
        pairs.append((float(prediction), float(target), weight))
    if len(pairs) < 2:
        return 0.0, 1.0
    matrix = np.asarray([[1.0, pred] for pred, _, weight in pairs], dtype=float)
    target_vec = np.asarray([target for _, target, _ in pairs], dtype=float)
    weights = np.asarray([weight for _, _, weight in pairs], dtype=float)
    weighted_matrix = matrix * weights[:, None]
    weighted_target = target_vec * weights
    coeffs, *_ = np.linalg.lstsq(weighted_matrix, weighted_target, rcond=None)
    c0 = float(coeffs[0])
    c1 = float(coeffs[1])
    if rounded:
        c0 = round(c0, S5_ROUND_DIGITS)
        c1 = round(c1, S5_ROUND_DIGITS)
    return c0, c1


def _corrected_error(row: Mapping[str, Any], candidate_id: str, rows: Sequence[Mapping[str, Any]], index: int) -> tuple[Optional[float], str, int]:
    error = _error_ppm(row)
    if error is None:
        return None, "", 0
    if candidate_id == "baseline_selected_s13":
        return float(error), "", 0
    if candidate_id == "temperature_group_bias_bridge_loo":
        correction, count = _mean_other_error(rows, index, ("temperature_group",))
        return float(error) - correction, f"minus_other_mean_error_ppm={correction:.6g}", count
    if candidate_id == "temperature_segment_bias_bridge_loo":
        correction, count = _mean_other_error(rows, index, ("temperature_group", "target_segment"))
        return float(error) - correction, f"minus_other_mean_error_ppm={correction:.6g}", count
    if candidate_id == "temperature_segment_relative_bridge_loo":
        target = _target_ppm(row)
        rel_correction, count = _mean_other_relative_error(
            rows,
            index,
            ("temperature_group", "target_segment"),
        )
        if target is None:
            return float(error), "", 0
        correction = float(target) * rel_correction / 100.0
        return (
            float(error) - correction,
            f"minus_other_mean_relative_error_percent={rel_correction:.6g}",
            count,
        )
    if candidate_id == "same_target_bias_bridge_loo":
        correction, count = _mean_other_error(rows, index, ("target_ppm",))
        return float(error) - correction, f"minus_other_mean_error_ppm={correction:.6g}", count
    return float(error), "", 0


def _apply_s5(row: Mapping[str, Any], c0: float, c1: float) -> Optional[float]:
    target = _target_ppm(row)
    prediction = _prediction_ppm(row)
    if target is None or prediction is None:
        return None
    corrected_prediction = float(prediction) * float(c1) + float(c0)
    return corrected_prediction - float(target)


def _metrics(rows: Sequence[Mapping[str, Any]]) -> Dict[str, Any]:
    nonzero_rel: List[float] = []
    low_rel: List[float] = []
    high_rel: List[float] = []
    errors: List[float] = []
    zero_abs: List[float] = []
    worst_point = ""
    worst_rel = -1.0
    for row in rows:
        error = _safe_float(row.get("corrected_error_ppm"))
        target = _target_ppm(row)
        if error is None or target is None:
            continue
        errors.append(float(error))
        if _is_zero_anchor(row):
            zero_abs.append(abs(float(error)))
            continue
        rel = _relative_error(float(error), float(target))
        if rel is None:
            continue
        abs_rel = abs(float(rel))
        nonzero_rel.append(abs_rel)
        if target <= LOW_END_LIMIT_PPM:
            low_rel.append(abs_rel)
        else:
            high_rel.append(abs_rel)
        if abs_rel > worst_rel:
            worst_rel = abs_rel
            worst_point = str(row.get("point_identity") or "")
    rmse = math.sqrt(mean([error * error for error in errors])) if errors else None
    return {
        "point_count": len(rows),
        "max_abs_relative_error_percent": max(nonzero_rel) if nonzero_rel else "",
        "low_end_max_abs_relative_error_percent": max(low_rel) if low_rel else "",
        "high_end_max_abs_relative_error_percent": max(high_rel) if high_rel else "",
        "zero_anchor_max_abs_error_ppm": max(zero_abs) if zero_abs else "",
        "rmse_ppm": rmse if rmse is not None else "",
        "worst_nonzero_point_identity": worst_point,
    }


def _candidate_rows_for_device(
    device: str,
    rows: Sequence[Mapping[str, Any]],
    candidate_id: str,
) -> tuple[List[Dict[str, Any]], Dict[str, Any]]:
    prepared: List[Dict[str, Any]] = []
    s5_c0 = ""
    s5_c1 = ""
    for index, row in enumerate(rows):
        base = dict(row)
        base["device_id"] = device
        base["target_segment"] = _target_segment(row)
        base["candidate_id"] = candidate_id
        if candidate_id.startswith("s5_"):
            relative_weighted = candidate_id == "s5_relative_weighted_rounded"
            c0, c1 = _affine_trim(rows, relative_weighted=relative_weighted, rounded=True)
            corrected = _apply_s5(row, c0, c1)
            base["bridge_evidence"] = f"S5 C0={c0:.3f}, C1={c1:.3f}"
            base["bridge_support_count"] = len(rows)
            s5_c0 = f"{c0:.3f}"
            s5_c1 = f"{c1:.3f}"
        else:
            corrected, evidence, support = _corrected_error(row, candidate_id, rows, index)
            base["bridge_evidence"] = evidence
            base["bridge_support_count"] = support
        base["corrected_error_ppm"] = corrected if corrected is not None else ""
        target = _target_ppm(row)
        if corrected is not None and target is not None and not _is_zero_anchor(row):
            base["corrected_relative_error_percent"] = _relative_error(float(corrected), float(target))
            base["corrected_abs_relative_error_percent"] = abs(float(base["corrected_relative_error_percent"]))
        else:
            base["corrected_relative_error_percent"] = ""
            base["corrected_abs_relative_error_percent"] = ""
        prepared.append(base)
    metrics = _metrics(prepared)
    metrics.update(
        {
            "device_id": device,
            "candidate_id": candidate_id,
            "s5_c0": s5_c0,
            "s5_c1": s5_c1,
        }
    )
    weak_support = sum(
        1
        for row in prepared
        if candidate_id.endswith("_loo") and int(row.get("bridge_support_count") or 0) < 2
    )
    metrics["weak_leave_one_out_support_points"] = weak_support if candidate_id.endswith("_loo") else ""
    metrics["overfit_risk"] = (
        "high" if weak_support and candidate_id.endswith("_loo") else "low"
    )
    metrics["write_meaning"] = _candidate_write_meaning(candidate_id)
    return prepared, metrics


def _candidate_write_meaning(candidate_id: str) -> str:
    if candidate_id == "baseline_selected_s13":
        return "Existing selected no-pressure S1/S3 residuals; primary model baseline."
    if candidate_id.startswith("s5_"):
        return "Theoretical S5 output-layer trim only; not a replacement for S1/S3 physical model review."
    return "Diagnostic bridge only; if it helps, translate the pattern into S1/S3 model/target-state handling before writing."


def _recommend(summary_rows: Sequence[Mapping[str, Any]]) -> Dict[str, str]:
    by_candidate = {str(row.get("candidate_id")): row for row in summary_rows}
    baseline = _safe_float(by_candidate.get("baseline_selected_s13", {}).get("max_abs_relative_error_percent"))
    def _metric_sort_key(row: Mapping[str, Any]) -> float:
        value = _safe_float(row.get("max_abs_relative_error_percent"))
        return float(value) if value is not None else 1e9

    best = min(summary_rows, key=_metric_sort_key, default={})
    best_err = _safe_float(best.get("max_abs_relative_error_percent"))
    s5 = _safe_float(
        by_candidate.get("s5_relative_weighted_rounded", {}).get("max_abs_relative_error_percent")
    )
    if baseline is None:
        return {
            "recommended_action": "insufficient_data",
            "physical_reason": "No baseline residual metrics were available.",
        }
    if best_err is not None and best_err < baseline * 0.7 and str(best.get("candidate_id", "")).endswith("_loo"):
        return {
            "recommended_action": "refit_s13_after_bridge_model_review",
            "physical_reason": "Leave-one-out bridge materially reduces residuals, so the error is structured rather than random point noise.",
        }
    if s5 is not None and s5 < baseline * 0.7:
        return {
            "recommended_action": "s5_can_reduce_display_error_after_s13_review",
            "physical_reason": "S5 can reduce the final display error, but the main S1/S3 residual should still be explained first.",
        }
    return {
        "recommended_action": "do_not_write_yet_review_point_state_or_device",
        "physical_reason": "No simple bridge or S5 trim sufficiently explains the residual pattern.",
    }


def build_co2_s13_bridge_correction_strategy_review(
    *,
    selected_residual_state_csv: str | Path,
) -> Dict[str, List[Dict[str, Any]]]:
    rows = _read_csv(selected_residual_state_csv)
    for row in rows:
        row["device_id"] = _device_id(row.get("device_id") or row.get("analyzer_device_id"))
        row["target_segment"] = _target_segment(row)
    grouped = _group_rows(rows)
    candidate_ids = (
        "baseline_selected_s13",
        "temperature_group_bias_bridge_loo",
        "temperature_segment_bias_bridge_loo",
        "temperature_segment_relative_bridge_loo",
        "same_target_bias_bridge_loo",
        "s5_absolute_rounded",
        "s5_relative_weighted_rounded",
    )
    all_candidate_rows: List[Dict[str, Any]] = []
    summary_rows: List[Dict[str, Any]] = []
    recommendations: List[Dict[str, Any]] = []
    for device, device_rows in sorted(grouped.items()):
        device_summary: List[Dict[str, Any]] = []
        for candidate_id in candidate_ids:
            corrected, metrics = _candidate_rows_for_device(device, device_rows, candidate_id)
            all_candidate_rows.extend(corrected)
            summary_rows.append(metrics)
            device_summary.append(metrics)
        rec = _recommend(device_summary)
        rec["device_id"] = device
        baseline_metric = next(
            (row for row in device_summary if row["candidate_id"] == "baseline_selected_s13"),
            {},
        )
        best_metric = min(
            device_summary,
            key=lambda row: (
                float(value)
                if (value := _safe_float(row.get("max_abs_relative_error_percent"))) is not None
                else 1e9
            ),
            default={},
        )
        rec["baseline_max_abs_relative_error_percent"] = baseline_metric.get(
            "max_abs_relative_error_percent",
            "",
        )
        rec["best_candidate_id"] = best_metric.get("candidate_id", "")
        rec["best_max_abs_relative_error_percent"] = best_metric.get(
            "max_abs_relative_error_percent",
            "",
        )
        recommendations.append(rec)
    return {
        "candidate_point_rows": all_candidate_rows,
        "candidate_summary": summary_rows,
        "device_recommendations": recommendations,
    }


def _fmt(value: Any) -> str:
    number = _safe_float(value)
    if number is None:
        return ""
    return f"{number:.3f}"


def _render_markdown(tables: Mapping[str, Sequence[Mapping[str, Any]]]) -> str:
    recs = list(tables.get("device_recommendations", []))
    summary = list(tables.get("candidate_summary", []))
    lines = [
        "# V1.5 CO2 S1/S3 桥接修正策略评审",
        "",
        f"生成时间：{_now()}",
        "",
        "## 边界",
        "",
        "- 本报告只使用既有 CSV 证据离线计算。",
        "- 不打开 COM、不控制气路/水路、不写 SENCO。",
        "- 压力项不进入 CO2 主校准拟合。",
        "- S5 仅作为后置输出层修正上限，不用于掩盖 S1/S3 主链路问题。",
        "",
        "## 逐台结论",
        "",
    ]
    for rec in recs:
        lines.append(
            "- 设备 {device}: baseline 最大相对误差 {base}%，最佳候选 {candidate} = {best}%；建议：{action}。".format(
                device=rec.get("device_id"),
                base=_fmt(rec.get("baseline_max_abs_relative_error_percent")),
                candidate=rec.get("best_candidate_id"),
                best=_fmt(rec.get("best_max_abs_relative_error_percent")),
                action=rec.get("recommended_action"),
            )
        )
        lines.append(f"  物理原因：{rec.get('physical_reason')}")
    lines.extend(["", "## S5 理论上限", ""])
    for row in summary:
        if not str(row.get("candidate_id", "")).startswith("s5_"):
            continue
        lines.append(
            "- 设备 {device} {candidate}: C0={c0}, C1={c1}, 最大相对误差 {err}%，零点绝对误差 {zero} ppm。".format(
                device=row.get("device_id"),
                candidate=row.get("candidate_id"),
                c0=row.get("s5_c0"),
                c1=row.get("s5_c1"),
                err=_fmt(row.get("max_abs_relative_error_percent")),
                zero=_fmt(row.get("zero_anchor_max_abs_error_ppm")),
            )
        )
    lines.extend(
        [
            "",
            "## 物理解释",
            "",
            "- 如果温度组或温度-气点分段的 leave-one-out 桥接能显著降低误差，说明残差是结构性的，优先应把这种结构转化为 S1/S3 主模型或目标状态处理。",
            "- 如果只有 S5 能降低误差，说明主模型仍未解释物理过程；S5 可以作为最终显示层微调，但不能代替主拟合。",
            "- 如果桥接和 S5 都不能明显降低误差，应回到点位证书、阀路状态、光学信号或设备个体故障排查。",
            "- 0 气仍是 CO2 低端锚点；H2O 干气点不能被混作 CO2 锚点。",
            "",
        ]
    )
    return "\n".join(lines)


def write_co2_s13_bridge_correction_strategy_review(
    *,
    selected_residual_state_csv: str | Path,
    output_dir: str | Path,
) -> Dict[str, Path]:
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    tables = build_co2_s13_bridge_correction_strategy_review(
        selected_residual_state_csv=selected_residual_state_csv,
    )
    outputs = {
        "candidate_point_rows": out_dir / "co2_s13_bridge_correction_candidate_point_rows.csv",
        "candidate_summary": out_dir / "co2_s13_bridge_correction_candidate_summary.csv",
        "device_recommendations": out_dir / "co2_s13_bridge_correction_device_recommendations.csv",
        "metadata": out_dir / "co2_s13_bridge_correction_strategy_meta.json",
        "markdown": out_dir / "co2_s13_bridge_correction_strategy_review_zh.md",
    }
    for key, path in outputs.items():
        if key in {"metadata", "markdown"}:
            continue
        _write_csv(path, tables[key])
    outputs["metadata"].write_text(
        json.dumps(
            {
                "generated_at": _now(),
                "selected_residual_state_csv": str(selected_residual_state_csv),
                "boundary": {
                    "opens_com_ports": False,
                    "controls_water_or_gas_routes": False,
                    "writes_coefficients": False,
                    "uses_pressure_terms": False,
                    "s5_is_theoretical_output_trim_only": True,
                    "not_real_acceptance_evidence": True,
                },
                "tables": {key: str(path) for key, path in outputs.items()},
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    outputs["markdown"].write_text("\ufeff" + _render_markdown(tables), encoding="utf-8")
    return outputs
