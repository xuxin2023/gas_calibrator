"""Offline CO2 S1/S3 large-error root-cause and resolution review.

This module is deliberately no-write.  It reads already-recorded V1.5 open-flow
CO2 evidence plus existing S1/S3/S5 fit-review outputs, then separates
large residuals into device-specific problems, point-level common-mode
problems, and model/target-state boundary problems.  It never opens COM ports,
controls routes, or writes SENCO coefficients.
"""

from __future__ import annotations

import csv
import json
import math
import tempfile
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence

from .co2_s13_multistrategy_fit_review import (
    DEFAULT_LOW_END_TARGET_PPM,
    DEFAULT_MIN_RELATIVE_TARGET_PPM,
    DEFAULT_STRATEGY_PASSES,
    DEFAULT_S5_ACCEPTANCE_PERCENT,
    DEFAULT_S5_C0_DECIMALS,
    DEFAULT_S5_C1_DECIMALS,
    DEFAULT_S5_C1_MAX,
    DEFAULT_S5_C1_MIN,
    DEFAULT_TOP_N,
    StrategyPass,
    build_co2_s13_multistrategy_fit_review,
)


DEFAULT_RATIO_A_THRESHOLD = 0.0005
DEFAULT_DEEP_DRY_DEWPOINT_C = -28.0
DEFAULT_COMMON_MODE_MIN_ABS_REL_PERCENT = 1.0
DEFAULT_COMMON_MODE_MIN_DEVICES = 3
DEFAULT_COMMON_MODE_SAME_SIGN_FRACTION = 0.8


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


def _fmt(value: Any, digits: int = 3) -> str:
    numeric = _safe_float(value)
    if numeric is None:
        return ""
    return f"{float(numeric):.{digits}f}"


def _mean(values: Sequence[float]) -> Optional[float]:
    clean = [float(value) for value in values if math.isfinite(float(value))]
    if not clean:
        return None
    return sum(clean) / len(clean)


def _min(values: Sequence[float]) -> Optional[float]:
    clean = [float(value) for value in values if math.isfinite(float(value))]
    return min(clean) if clean else None


def _max(values: Sequence[float]) -> Optional[float]:
    clean = [float(value) for value in values if math.isfinite(float(value))]
    return max(clean) if clean else None


def _same_sign_fraction(values: Sequence[float]) -> Optional[float]:
    positives = sum(1 for value in values if value > 0.0)
    negatives = sum(1 for value in values if value < 0.0)
    total = positives + negatives
    if not total:
        return None
    return max(positives, negatives) / total


def _point_temp_key(identity: str) -> str:
    text = str(identity or "")
    if text.startswith("T") and "_" in text:
        return text.split("_", 1)[0]
    return "T_unknown"


def _point_gas_key(identity: str) -> str:
    text = str(identity or "")
    if "_" in text:
        return text.rsplit("_", 1)[-1]
    return "unknown"


def _fit_lookup(rows: Sequence[Mapping[str, Any]]) -> Dict[tuple[str, str], Mapping[str, Any]]:
    lookup: Dict[tuple[str, str], Mapping[str, Any]] = {}
    for row in rows:
        device = _device_id(row.get("analyzer_device_id") or row.get("device_id"))
        identity = str(row.get("point_identity") or "").strip()
        if device and identity:
            lookup[(device, identity)] = row
    return lookup


def _fit_meta(residual: Mapping[str, Any], lookup: Mapping[tuple[str, str], Mapping[str, Any]]) -> Mapping[str, Any]:
    device = _device_id(residual.get("device_id"))
    identity = str(residual.get("point_identity") or "").strip()
    return lookup.get((device, identity), {})


def _physical_values(row: Mapping[str, Any], meta: Mapping[str, Any]) -> Dict[str, Any]:
    ratio_std = _safe_float(
        meta.get("co2_ratio_f_std")
        or meta.get("ratio_std")
        or row.get("co2_ratio_f_std")
        or row.get("ratio_std")
    )
    dewpoint = _safe_float(
        meta.get("dewpoint_mean_c")
        or meta.get("dewpoint_c_mean")
        or meta.get("dewpoint_c")
        or row.get("dewpoint_mean_c")
        or row.get("dewpoint_c_mean")
    )
    pressure = _safe_float(meta.get("pressure_hpa") or row.get("pressure_hpa"))
    h2o = _safe_float(meta.get("h2o_mmol_mean") or row.get("h2o_mmol"))
    sample_count = _safe_float(meta.get("sample_count") or row.get("sample_count"))
    usable_count = _safe_float(meta.get("usable_sample_count") or row.get("usable_sample_count"))
    return {
        "ratio_std": ratio_std,
        "dewpoint_c": dewpoint,
        "pressure_hpa": pressure,
        "h2o_mmol_mol": h2o,
        "sample_count": sample_count,
        "usable_sample_count": usable_count,
    }


def _point_state_label(
    *,
    ratio_std_max: Optional[float],
    dewpoint_c_max: Optional[float],
    ratio_a_threshold: float,
    deep_dry_dewpoint_c: float,
) -> str:
    ratio_good = ratio_std_max is not None and ratio_std_max <= float(ratio_a_threshold)
    dry_good = dewpoint_c_max is not None and dewpoint_c_max <= float(deep_dry_dewpoint_c)
    if ratio_good and dry_good:
        return "ratio_A_and_deep_dry"
    if ratio_good:
        return "ratio_A_but_dryness_not_deep"
    if dry_good:
        return "deep_dry_but_ratio_not_A"
    return "physical_state_needs_review"


def _root_cause_label(
    *,
    count: int,
    same_sign_fraction: Optional[float],
    max_abs_relative_error_percent: Optional[float],
    point_state_label: str,
    common_mode_min_devices: int,
    common_mode_same_sign_fraction: float,
    common_mode_min_abs_rel_percent: float,
) -> str:
    if (
        count >= int(common_mode_min_devices)
        and same_sign_fraction is not None
        and same_sign_fraction >= float(common_mode_same_sign_fraction)
        and max_abs_relative_error_percent is not None
        and max_abs_relative_error_percent >= float(common_mode_min_abs_rel_percent)
    ):
        if point_state_label == "ratio_A_and_deep_dry":
            return "point_common_mode_model_or_target_state_boundary"
        return "point_common_mode_physical_state_or_route_review"
    if max_abs_relative_error_percent is not None and max_abs_relative_error_percent >= float(common_mode_min_abs_rel_percent):
        return "device_specific_or_mixed_residual_review"
    return "within_current_residual_attention_range"


def _build_common_mode_rows(
    *,
    residuals: Sequence[Mapping[str, Any]],
    fit_rows: Sequence[Mapping[str, Any]],
    ratio_a_threshold: float = DEFAULT_RATIO_A_THRESHOLD,
    deep_dry_dewpoint_c: float = DEFAULT_DEEP_DRY_DEWPOINT_C,
    common_mode_min_devices: int = DEFAULT_COMMON_MODE_MIN_DEVICES,
    common_mode_same_sign_fraction: float = DEFAULT_COMMON_MODE_SAME_SIGN_FRACTION,
    common_mode_min_abs_rel_percent: float = DEFAULT_COMMON_MODE_MIN_ABS_REL_PERCENT,
) -> List[Dict[str, Any]]:
    lookup = _fit_lookup(fit_rows)
    groups: Dict[str, List[Mapping[str, Any]]] = defaultdict(list)
    for row in residuals:
        target = _safe_float(row.get("target_ppm"))
        if target is None or float(target) <= 0.0:
            continue
        identity = str(row.get("point_identity") or "").strip()
        if identity:
            groups[identity].append(row)

    out: List[Dict[str, Any]] = []
    for identity, items in sorted(groups.items()):
        errors = [float(_safe_float(row.get("error_ppm")) or 0.0) for row in items]
        rels = [
            float(_safe_float(row.get("relative_error_percent")) or 0.0)
            for row in items
            if _safe_float(row.get("relative_error_percent")) is not None
        ]
        physical = [_physical_values(row, _fit_meta(row, lookup)) for row in items]
        ratio_stds = [float(item["ratio_std"]) for item in physical if item["ratio_std"] is not None]
        dewpoints = [float(item["dewpoint_c"]) for item in physical if item["dewpoint_c"] is not None]
        pressures = [float(item["pressure_hpa"]) for item in physical if item["pressure_hpa"] is not None]
        h2os = [float(item["h2o_mmol_mol"]) for item in physical if item["h2o_mmol_mol"] is not None]
        target = _safe_float(items[0].get("target_ppm"))
        positives = sum(1 for value in errors if value > 0.0)
        negatives = sum(1 for value in errors if value < 0.0)
        same_sign = _same_sign_fraction(errors)
        max_abs_rel = max((abs(value) for value in rels), default=None)
        state = _point_state_label(
            ratio_std_max=_max(ratio_stds),
            dewpoint_c_max=_max(dewpoints),
            ratio_a_threshold=ratio_a_threshold,
            deep_dry_dewpoint_c=deep_dry_dewpoint_c,
        )
        root = _root_cause_label(
            count=len(items),
            same_sign_fraction=same_sign,
            max_abs_relative_error_percent=max_abs_rel,
            point_state_label=state,
            common_mode_min_devices=common_mode_min_devices,
            common_mode_same_sign_fraction=common_mode_same_sign_fraction,
            common_mode_min_abs_rel_percent=common_mode_min_abs_rel_percent,
        )
        out.append(
            {
                "point_identity": identity,
                "temperature_group": _point_temp_key(identity),
                "gas_point": _point_gas_key(identity),
                "target_ppm": target if target is not None else "",
                "device_count": len(items),
                "positive_error_count": positives,
                "negative_error_count": negatives,
                "same_sign_fraction": same_sign if same_sign is not None else "",
                "mean_error_ppm": _mean(errors) if errors else "",
                "max_abs_error_ppm": max((abs(value) for value in errors), default=""),
                "mean_relative_error_percent": _mean(rels) if rels else "",
                "max_abs_relative_error_percent": max_abs_rel if max_abs_rel is not None else "",
                "ratio_std_max": _max(ratio_stds) if ratio_stds else "",
                "dewpoint_c_mean": _mean(dewpoints) if dewpoints else "",
                "dewpoint_c_max": _max(dewpoints) if dewpoints else "",
                "pressure_hpa_span": (_max(pressures) - _min(pressures)) if len(pressures) >= 2 else 0.0,
                "h2o_mmol_mol_mean": _mean(h2os) if h2os else "",
                "physical_state_label": state,
                "root_cause_class": root,
                "physical_interpretation": _common_mode_interpretation(root),
                "auto_exclude": False,
                "writes_coefficients": False,
            }
        )
    out.sort(
        key=lambda row: (
            0 if row.get("root_cause_class") == "point_common_mode_model_or_target_state_boundary" else 1,
            -float(row.get("max_abs_relative_error_percent") or 0.0),
        )
    )
    return out


def _common_mode_interpretation(root: str) -> str:
    if root == "point_common_mode_model_or_target_state_boundary":
        return (
            "同一温度/气点在多台设备上同向偏差，且 ratio 已达 A 级、露点已深干；"
            "优先解释为 S1/S3 主模型或目标状态边界问题，不应简单判为采样不稳。"
        )
    if root == "point_common_mode_physical_state_or_route_review":
        return "同一温度/气点存在共模偏差，但物理状态证据不够干净，应复核阀路、露点、ratio 窗口和目标气体状态。"
    if root == "device_specific_or_mixed_residual_review":
        return "偏差较大但方向不一致，更像设备个体差异、局部信号异常或模型局部残差。"
    return "当前残差未形成强共模根因。"


def _device_worst_rows(residuals: Sequence[Mapping[str, Any]], *, top_n: int = 8) -> List[Dict[str, Any]]:
    by_device: Dict[str, List[Mapping[str, Any]]] = defaultdict(list)
    for row in residuals:
        by_device[_device_id(row.get("device_id"))].append(row)
    out: List[Dict[str, Any]] = []
    for device, rows in sorted(by_device.items()):
        ranked = sorted(
            rows,
            key=lambda row: abs(float(_safe_float(row.get("relative_error_percent")) or 0.0)),
            reverse=True,
        )
        for rank, row in enumerate(ranked[: int(top_n)], start=1):
            out.append(
                {
                    "device_id": device,
                    "rank": rank,
                    "point_identity": row.get("point_identity", ""),
                    "target_ppm": row.get("target_ppm", ""),
                    "prediction_ppm": row.get("prediction_ppm", ""),
                    "error_ppm": row.get("error_ppm", ""),
                    "relative_error_percent": row.get("relative_error_percent", ""),
                    "s1s3_error_ppm_before_s5": row.get("s1s3_error_ppm_before_s5", ""),
                    "s1s3_relative_error_percent_before_s5": row.get("s1s3_relative_error_percent_before_s5", ""),
                    "s5_C0": row.get("s5_C0", ""),
                    "s5_C1": row.get("s5_C1", ""),
                    "ratio": row.get("ratio", ""),
                    "temperature_c": row.get("temperature_c", ""),
                    "pressure_hpa": row.get("pressure_hpa", ""),
                    "h2o_mmol": row.get("h2o_mmol", ""),
                }
            )
    return out


def _write_treatment_plan(path: Path, held_points: Sequence[str]) -> None:
    rows = [
        {
            "point_identity": point,
            "fit_policy": "hold_for_common_mode_resolution_review",
            "bridge_policy": "none",
            "review_priority": "high",
            "exclusion_basis": "offline sensitivity only; not an automatic formal exclusion",
        }
        for point in held_points
    ]
    _write_csv(path, rows)


def _variant_specs(common_rows: Sequence[Mapping[str, Any]], *, max_top_common_variants: int) -> List[Dict[str, Any]]:
    strong = [
        str(row.get("point_identity") or "")
        for row in common_rows
        if row.get("root_cause_class") == "point_common_mode_model_or_target_state_boundary"
    ]
    strong = [point for point in strong if point]
    specs: List[Dict[str, Any]] = [
        {"variant_id": "baseline_existing_no_hold", "held_points": []},
    ]
    for count in range(1, min(int(max_top_common_variants), len(strong)) + 1):
        specs.append(
            {
                "variant_id": f"hold_top_common_{count}",
                "held_points": strong[:count],
            }
        )
    for point in ("T20_100ppm", "T30_100ppm", "T20_600ppm", "T-20_1000ppm"):
        if point in strong:
            specs.append({"variant_id": f"hold_{point}", "held_points": [point]})
    ppm600 = [str(row.get("point_identity") or "") for row in common_rows if str(row.get("gas_point") or "") == "600ppm"]
    if ppm600:
        specs.append({"variant_id": "hold_all_600ppm_points", "held_points": sorted(set(ppm600))})

    seen = set()
    unique: List[Dict[str, Any]] = []
    for spec in specs:
        key = (spec["variant_id"], tuple(spec.get("held_points") or []))
        if key not in seen:
            seen.add(key)
            unique.append(spec)
    return unique


def _existing_baseline_variant(
    *,
    best_by_device: Sequence[Mapping[str, Any]],
    s5_best_by_device: Sequence[Mapping[str, Any]],
    held_points: Sequence[str],
) -> tuple[Dict[str, Any], List[Dict[str, Any]]]:
    device_rows: List[Dict[str, Any]] = []
    by_device = {_device_id(row.get("device_id")): row for row in best_by_device}
    s5_by_device = {_device_id(row.get("device_id")): row for row in s5_best_by_device}
    for device in sorted(set(by_device) | set(s5_by_device)):
        best = by_device.get(device, {})
        s5 = s5_by_device.get(device, {})
        device_rows.append(_variant_device_row("baseline_existing_no_hold", held_points, best, s5))
    summary = _variant_summary_row("baseline_existing_no_hold", held_points, device_rows)
    return summary, device_rows


def _variant_device_row(
    variant_id: str,
    held_points: Sequence[str],
    best: Mapping[str, Any],
    s5: Mapping[str, Any],
) -> Dict[str, Any]:
    return {
        "variant_id": variant_id,
        "held_point_count": len(held_points),
        "held_points": ";".join(held_points),
        "device_id": _device_id(best.get("device_id") or s5.get("device_id")),
        "s1s3_strategy_id": best.get("strategy_id", ""),
        "s1s3_max_abs_relative_error_percent": best.get("max_abs_relative_error_percent", ""),
        "s1s3_low_end_max_abs_relative_error_percent": best.get("low_end_max_abs_relative_error_percent", ""),
        "s1s3_rmse_ppm": best.get("rmse_ppm", ""),
        "s1_payload_scientific": best.get("s1_payload_scientific", ""),
        "s3_payload_scientific": best.get("s3_payload_scientific", ""),
        "s5_C0": s5.get("s5_C0", ""),
        "s5_C1": s5.get("s5_C1", ""),
        "s5_command_preview": s5.get("s5_command_preview", ""),
        "s5_max_abs_relative_error_percent": s5.get("s5_max_abs_relative_error_percent", ""),
        "s5_worst_point_identity": s5.get("s5_worst_point_identity", ""),
        "s5_status": s5.get("s5_status", ""),
        "recommended_no_write_action": s5.get("s5_recommended_no_write_action") or best.get("recommended_no_write_action", ""),
        "writes_coefficients": False,
    }


def _variant_summary_row(variant_id: str, held_points: Sequence[str], device_rows: Sequence[Mapping[str, Any]]) -> Dict[str, Any]:
    s1_values = [
        float(value)
        for value in (_safe_float(row.get("s1s3_max_abs_relative_error_percent")) for row in device_rows)
        if value is not None
    ]
    s5_values = [
        float(value)
        for value in (_safe_float(row.get("s5_max_abs_relative_error_percent")) for row in device_rows)
        if value is not None
    ]
    worst_s5 = max(device_rows, key=lambda row: float(_safe_float(row.get("s5_max_abs_relative_error_percent")) or -1.0), default={})
    return {
        "variant_id": variant_id,
        "held_point_count": len(held_points),
        "held_points": ";".join(held_points),
        "device_count": len(device_rows),
        "max_device_s1s3_max_abs_relative_error_percent": max(s1_values) if s1_values else "",
        "mean_device_s1s3_max_abs_relative_error_percent": _mean(s1_values) if s1_values else "",
        "max_device_s5_max_abs_relative_error_percent": max(s5_values) if s5_values else "",
        "mean_device_s5_max_abs_relative_error_percent": _mean(s5_values) if s5_values else "",
        "device_count_s5_within_1_percent": sum(1 for value in s5_values if value <= 1.0),
        "worst_s5_device_id": worst_s5.get("device_id", ""),
        "worst_s5_point_identity": worst_s5.get("s5_worst_point_identity", ""),
        "review_meaning": _variant_meaning(variant_id, held_points, s5_values),
        "writes_coefficients": False,
    }


def _variant_meaning(variant_id: str, held_points: Sequence[str], s5_values: Sequence[float]) -> str:
    if not held_points:
        return "当前既有策略基线。"
    if s5_values and max(s5_values) <= 1.0:
        return "剔除这些共模点后可被 S5 压到 1% 内；这些点需要明确物理原因后才可正式降级。"
    if s5_values:
        return "剔除这些共模点后仍不能全部进 1%；说明不只是单点异常，还存在模型结构或目标状态合同问题。"
    return f"{variant_id} 未产生可评估 S5 结果。"


def _run_variant_sensitivity(
    *,
    fit_points_csv: str | Path,
    common_rows: Sequence[Mapping[str, Any]],
    best_by_device: Sequence[Mapping[str, Any]],
    s5_best_by_device: Sequence[Mapping[str, Any]],
    work_dir: Path,
    strategy_passes: Sequence[StrategyPass],
    max_top_common_variants: int,
    min_relative_target_ppm: float,
    low_end_target_ppm: float,
    top_n: int,
    s5_acceptance_percent: float,
    s5_c0_decimals: int,
    s5_c1_decimals: int,
    s5_c1_min: float,
    s5_c1_max: float,
) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Path]]:
    summary_rows: List[Dict[str, Any]] = []
    device_rows: List[Dict[str, Any]] = []
    plan_paths: List[Path] = []
    for spec in _variant_specs(common_rows, max_top_common_variants=max_top_common_variants):
        variant_id = str(spec["variant_id"])
        held = list(spec.get("held_points") or [])
        if not held:
            summary, rows = _existing_baseline_variant(
                best_by_device=best_by_device,
                s5_best_by_device=s5_best_by_device,
                held_points=held,
            )
            summary_rows.append(summary)
            device_rows.extend(rows)
            continue
        plan = work_dir / "treatment_plans" / f"{variant_id}.csv"
        _write_treatment_plan(plan, held)
        plan_paths.append(plan)
        tables = build_co2_s13_multistrategy_fit_review(
            fit_points_csv=fit_points_csv,
            fit_point_treatment_plan_csv=plan,
            strategy_passes=strategy_passes,
            min_relative_target_ppm=float(min_relative_target_ppm),
            low_end_target_ppm=float(low_end_target_ppm),
            top_n=int(top_n),
            s5_acceptance_percent=float(s5_acceptance_percent),
            s5_c0_decimals=int(s5_c0_decimals),
            s5_c1_decimals=int(s5_c1_decimals),
            s5_c1_min=float(s5_c1_min),
            s5_c1_max=float(s5_c1_max),
        )
        by_device = {_device_id(row.get("device_id")): row for row in tables.get("best_by_device", [])}
        s5_by_device = {_device_id(row.get("device_id")): row for row in tables.get("s5_best_by_device", [])}
        rows = [
            _variant_device_row(variant_id, held, by_device.get(device, {}), s5_by_device.get(device, {}))
            for device in sorted(set(by_device) | set(s5_by_device))
        ]
        summary_rows.append(_variant_summary_row(variant_id, held, rows))
        device_rows.extend(rows)
    summary_rows.sort(
        key=lambda row: (
            float(_safe_float(row.get("max_device_s5_max_abs_relative_error_percent")) or 1.0e9),
            int(row.get("held_point_count") or 0),
        )
    )
    return summary_rows, device_rows, plan_paths


def _render_markdown(
    *,
    common_rows: Sequence[Mapping[str, Any]],
    device_rows: Sequence[Mapping[str, Any]],
    variant_summary_rows: Sequence[Mapping[str, Any]],
    s5_best_by_device: Sequence[Mapping[str, Any]],
) -> str:
    lines = [
        "# V1.5 CO2 S1/S3 大误差根因收敛评审",
        "",
        f"生成时间：{_now()}",
        "",
        "## 边界",
        "",
        "- 本报告只使用既有离线证据，不打开 COM，不控制气路/水路/压力，不写 SENCO。",
        "- S5 只作为输出层线性修正评审，不用于掩盖 S1/S3 主模型根因。",
        "- 压力项保持冻结，因为本轮 CO2 主校准是当前大气压开放流通数据。",
        "- CO2 零气锚点与 H2O 干气锚点不能混用；本报告只分析 CO2 S1/S3。",
        "",
        "## 当前结论",
        "",
    ]
    worst_current = max(
        (
            float(_safe_float(row.get("s5_max_abs_relative_error_percent")) or 0.0)
            for row in s5_best_by_device
        ),
        default=0.0,
    )
    top_common = [row for row in common_rows if row.get("root_cause_class") == "point_common_mode_model_or_target_state_boundary"]
    if worst_current > 1.0:
        lines.append(f"- 现有 S1/S3 加 S5 后最大相对误差仍约 `{worst_current:.3f}%`，不能直接进入写入验收。")
    else:
        lines.append(f"- 现有 S1/S3 加 S5 后最大相对误差约 `{worst_current:.3f}%`，可进入写入前复核。")
    if top_common:
        first = top_common[0]
        lines.append(
            "- 最大残差呈点位共模特征："
            f"`{first.get('point_identity')}` 最大相对误差 `{_fmt(first.get('max_abs_relative_error_percent'))}%`，"
            f"同向比例 `{_fmt(first.get('same_sign_fraction'))}`。"
        )
        lines.append("- 这些点 ratio 已达 A 级且露点深干，根因更像 S1/S3 模型结构或目标状态合同，而不是简单吹扫不够。")
    else:
        lines.append("- 未发现强共模点位，优先查设备个体信号或局部采样证据。")
    best_variant = variant_summary_rows[0] if variant_summary_rows else {}
    if best_variant:
        lines.append(
            "- 最优离线敏感性方案："
            f"`{best_variant.get('variant_id')}`，S5 后全设备最大相对误差 "
            f"`{_fmt(best_variant.get('max_device_s5_max_abs_relative_error_percent'))}%`。"
        )
        lines.append(
            "- 注意：hold 点方案只是根因定位工具；除非有明确坏物理状态证据，否则不能把物理状态良好的点静默删掉。"
        )
    lines.extend(
        [
            "",
            "## 点位共模根因表（前 12）",
            "",
            "| 点位 | 设备数 | 同向比例 | 平均误差 ppm | 最大相对误差 % | ratio std 最大 | 平均露点 °C | 根因分类 |",
            "| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |",
        ]
    )
    for row in common_rows[:12]:
        lines.append(
            "| {point} | {count} | {same} | {mean_error} | {max_rel} | {ratio_std} | {dewpoint} | {root} |".format(
                point=row.get("point_identity", ""),
                count=row.get("device_count", ""),
                same=_fmt(row.get("same_sign_fraction")),
                mean_error=_fmt(row.get("mean_error_ppm")),
                max_rel=_fmt(row.get("max_abs_relative_error_percent")),
                ratio_std=_fmt(row.get("ratio_std_max"), 6),
                dewpoint=_fmt(row.get("dewpoint_c_mean")),
                root=row.get("root_cause_class", ""),
            )
        )
    lines.extend(
        [
            "",
            "## 逐设备最差点（前 18）",
            "",
            "| 设备 | 排名 | 点位 | 目标 ppm | 误差 ppm | 相对误差 % | S5 C0 | S5 C1 |",
            "| --- | ---: | --- | ---: | ---: | ---: | ---: | ---: |",
        ]
    )
    for row in device_rows[:18]:
        lines.append(
            "| {device} | {rank} | {point} | {target} | {error} | {rel} | {c0} | {c1} |".format(
                device=row.get("device_id", ""),
                rank=row.get("rank", ""),
                point=row.get("point_identity", ""),
                target=_fmt(row.get("target_ppm")),
                error=_fmt(row.get("error_ppm")),
                rel=_fmt(row.get("relative_error_percent")),
                c0=_fmt(row.get("s5_C0")),
                c1=_fmt(row.get("s5_C1")),
            )
        )
    lines.extend(
        [
            "",
            "## Hold 敏感性比较",
            "",
            "| 方案 | hold 点数 | hold 点 | S1/S3 后最大相对误差 % | S5 后最大相对误差 % | 1% 内设备数 | 说明 |",
            "| --- | ---: | --- | ---: | ---: | ---: | --- |",
        ]
    )
    for row in variant_summary_rows:
        lines.append(
            "| {variant} | {count} | {points} | {s13} | {s5} | {within} | {meaning} |".format(
                variant=row.get("variant_id", ""),
                count=row.get("held_point_count", ""),
                points=row.get("held_points", ""),
                s13=_fmt(row.get("max_device_s1s3_max_abs_relative_error_percent")),
                s5=_fmt(row.get("max_device_s5_max_abs_relative_error_percent")),
                within=row.get("device_count_s5_within_1_percent", ""),
                meaning=row.get("review_meaning", ""),
            )
        )
    lines.extend(
        [
            "",
            "## 物理解释",
            "",
            "1. 如果同一温度/气点在多台设备上同向偏差，并且 ratio 和露点证据都合格，说明问题不在某一台传感器，也不应优先归因于吹扫不足。",
            "2. 低端 100/200 ppm 对截距和零气赋值极敏感；即便零气 CO2 只偏几 ppm，也会在低端形成明显相对误差。",
            "3. T20/T30 的同气点符号相反或共同偏移，通常说明目标状态、温度项边界或低端模型曲率没有被当前 S1/S3 合同完全表达。",
            "4. S5 可以作为最终显示层微调；但如果 S5 仍不能把共模误差压进指标，必须回到 S1/S3 的模型结构、低端锚点和目标状态桥接。",
            "",
            "## 建议",
            "",
            "- 不自动剔除 A 级且深干的共模点；先把这些点标为“目标状态/模型边界复核点”。",
            "- 对最差共模点执行目标状态桥接审计：证书值、阀位映射、露点、H2O、温度、ratio 窗口、采样时间戳全部核对。",
            "- 若 hold 敏感性显示某一类点影响巨大，而物理状态无异常，应考虑主模型合同扩展或重跑少量桥接点，而不是靠 S5 强行压平。",
        ]
    )
    return "\n".join(lines)


def build_co2_s13_error_root_cause_resolution_review(
    *,
    fit_points_csv: str | Path,
    baseline_review_dir: str | Path,
    run_sensitivity: bool = True,
    strategy_passes: Sequence[StrategyPass] | None = None,
    ratio_a_threshold: float = DEFAULT_RATIO_A_THRESHOLD,
    deep_dry_dewpoint_c: float = DEFAULT_DEEP_DRY_DEWPOINT_C,
    common_mode_min_abs_rel_percent: float = DEFAULT_COMMON_MODE_MIN_ABS_REL_PERCENT,
    max_top_common_variants: int = 3,
    min_relative_target_ppm: float = DEFAULT_MIN_RELATIVE_TARGET_PPM,
    low_end_target_ppm: float = DEFAULT_LOW_END_TARGET_PPM,
    top_n: int = DEFAULT_TOP_N,
    s5_acceptance_percent: float = DEFAULT_S5_ACCEPTANCE_PERCENT,
    s5_c0_decimals: int = DEFAULT_S5_C0_DECIMALS,
    s5_c1_decimals: int = DEFAULT_S5_C1_DECIMALS,
    s5_c1_min: float = DEFAULT_S5_C1_MIN,
    s5_c1_max: float = DEFAULT_S5_C1_MAX,
) -> Dict[str, List[Dict[str, Any]]]:
    baseline = Path(baseline_review_dir)
    fit_rows = _read_csv(fit_points_csv)
    best_by_device = _read_csv(baseline / "co2_s13_multistrategy_best_by_device.csv")
    s5_best_by_device = _read_csv(baseline / "co2_s13_multistrategy_s5_best_by_device.csv")
    s5_best_residuals = _read_csv(baseline / "co2_s13_multistrategy_s5_best_residuals.csv")
    if not s5_best_residuals:
        raise FileNotFoundError(f"missing s5 residuals under {baseline}")
    common_rows = _build_common_mode_rows(
        residuals=s5_best_residuals,
        fit_rows=fit_rows,
        ratio_a_threshold=float(ratio_a_threshold),
        deep_dry_dewpoint_c=float(deep_dry_dewpoint_c),
        common_mode_min_abs_rel_percent=float(common_mode_min_abs_rel_percent),
    )
    device_worst = _device_worst_rows(s5_best_residuals, top_n=8)
    if run_sensitivity:
        with tempfile.TemporaryDirectory(prefix="co2_s13_error_review_") as tmp:
            variant_summary, variant_devices, plan_paths = _run_variant_sensitivity(
                fit_points_csv=fit_points_csv,
                common_rows=common_rows,
                best_by_device=best_by_device,
                s5_best_by_device=s5_best_by_device,
                work_dir=Path(tmp),
                strategy_passes=tuple(strategy_passes or DEFAULT_STRATEGY_PASSES),
                max_top_common_variants=int(max_top_common_variants),
                min_relative_target_ppm=float(min_relative_target_ppm),
                low_end_target_ppm=float(low_end_target_ppm),
                top_n=int(top_n),
                s5_acceptance_percent=float(s5_acceptance_percent),
                s5_c0_decimals=int(s5_c0_decimals),
                s5_c1_decimals=int(s5_c1_decimals),
                s5_c1_min=float(s5_c1_min),
                s5_c1_max=float(s5_c1_max),
            )
    else:
        baseline_summary, variant_devices = _existing_baseline_variant(
            best_by_device=best_by_device,
            s5_best_by_device=s5_best_by_device,
            held_points=[],
        )
        variant_summary = [baseline_summary]
        plan_paths = []
    return {
        "common_mode_points": common_rows,
        "device_worst_points": device_worst,
        "variant_summary": variant_summary,
        "variant_device_summary": variant_devices,
        "baseline_s5_best_by_device": list(s5_best_by_device),
        "run_summary": [
            {
                "created_at": _now(),
                "fit_points_csv": str(Path(fit_points_csv).resolve()),
                "baseline_review_dir": str(Path(baseline_review_dir).resolve()),
                "fit_point_count": len(fit_rows),
                "device_count": len({_device_id(row.get("analyzer_device_id") or row.get("device_id")) for row in fit_rows}),
                "common_mode_point_count": len(common_rows),
                "variant_count": len(variant_summary),
                "temporary_treatment_plan_count": len(plan_paths),
                "opens_com_ports": False,
                "controls_water_or_gas_routes": False,
                "writes_coefficients": False,
                "uses_pressure_terms": False,
                "uses_s5_output_trim": "review_only_no_write",
                "not_real_acceptance_evidence": True,
            }
        ],
    }


def write_co2_s13_error_root_cause_resolution_review(
    *,
    fit_points_csv: str | Path,
    baseline_review_dir: str | Path,
    output_dir: str | Path,
    run_sensitivity: bool = True,
    strategy_passes: Sequence[StrategyPass] | None = None,
    ratio_a_threshold: float = DEFAULT_RATIO_A_THRESHOLD,
    deep_dry_dewpoint_c: float = DEFAULT_DEEP_DRY_DEWPOINT_C,
    common_mode_min_abs_rel_percent: float = DEFAULT_COMMON_MODE_MIN_ABS_REL_PERCENT,
    max_top_common_variants: int = 3,
    min_relative_target_ppm: float = DEFAULT_MIN_RELATIVE_TARGET_PPM,
    low_end_target_ppm: float = DEFAULT_LOW_END_TARGET_PPM,
    top_n: int = DEFAULT_TOP_N,
    s5_acceptance_percent: float = DEFAULT_S5_ACCEPTANCE_PERCENT,
    s5_c0_decimals: int = DEFAULT_S5_C0_DECIMALS,
    s5_c1_decimals: int = DEFAULT_S5_C1_DECIMALS,
    s5_c1_min: float = DEFAULT_S5_C1_MIN,
    s5_c1_max: float = DEFAULT_S5_C1_MAX,
) -> Dict[str, Path]:
    output = Path(output_dir).resolve()
    output.mkdir(parents=True, exist_ok=True)
    tables = build_co2_s13_error_root_cause_resolution_review(
        fit_points_csv=fit_points_csv,
        baseline_review_dir=baseline_review_dir,
        run_sensitivity=run_sensitivity,
        strategy_passes=strategy_passes,
        ratio_a_threshold=ratio_a_threshold,
        deep_dry_dewpoint_c=deep_dry_dewpoint_c,
        common_mode_min_abs_rel_percent=common_mode_min_abs_rel_percent,
        max_top_common_variants=max_top_common_variants,
        min_relative_target_ppm=min_relative_target_ppm,
        low_end_target_ppm=low_end_target_ppm,
        top_n=top_n,
        s5_acceptance_percent=s5_acceptance_percent,
        s5_c0_decimals=s5_c0_decimals,
        s5_c1_decimals=s5_c1_decimals,
        s5_c1_min=s5_c1_min,
        s5_c1_max=s5_c1_max,
    )
    outputs = {
        "run_summary": output / "co2_s13_error_root_cause_run_summary.csv",
        "common_mode_points": output / "co2_s13_error_common_mode_points.csv",
        "device_worst_points": output / "co2_s13_error_device_worst_points.csv",
        "variant_summary": output / "co2_s13_error_variant_summary.csv",
        "variant_device_summary": output / "co2_s13_error_variant_device_summary.csv",
        "baseline_s5_best_by_device": output / "co2_s13_error_baseline_s5_best_by_device.csv",
        "metadata": output / "co2_s13_error_root_cause_meta.json",
        "markdown": output / "co2_s13_error_root_cause_resolution_review_zh.md",
    }
    for key in (
        "run_summary",
        "common_mode_points",
        "device_worst_points",
        "variant_summary",
        "variant_device_summary",
        "baseline_s5_best_by_device",
    ):
        _write_csv(outputs[key], tables[key])
    outputs["metadata"].write_text(
        json.dumps(
            {
                "tool": "co2_s13_error_root_cause_resolution_review",
                "created_at": _now(),
                "inputs": {
                    "fit_points_csv": str(Path(fit_points_csv).resolve()),
                    "baseline_review_dir": str(Path(baseline_review_dir).resolve()),
                    "run_sensitivity": bool(run_sensitivity),
                    "ratio_a_threshold": float(ratio_a_threshold),
                    "deep_dry_dewpoint_c": float(deep_dry_dewpoint_c),
                    "common_mode_min_abs_rel_percent": float(common_mode_min_abs_rel_percent),
                    "max_top_common_variants": int(max_top_common_variants),
                    "min_relative_target_ppm": float(min_relative_target_ppm),
                    "low_end_target_ppm": float(low_end_target_ppm),
                    "top_n": int(top_n),
                    "s5_acceptance_percent": float(s5_acceptance_percent),
                    "s5_c0_decimals": int(s5_c0_decimals),
                    "s5_c1_decimals": int(s5_c1_decimals),
                    "s5_c1_min": float(s5_c1_min),
                    "s5_c1_max": float(s5_c1_max),
                },
                "boundary": {
                    "opens_com_ports": False,
                    "controls_water_or_gas_routes": False,
                    "writes_coefficients": False,
                    "uses_pressure_terms": False,
                    "uses_s5_output_trim": "review_only_no_write",
                    "not_real_acceptance_evidence": True,
                },
                "outputs": {key: str(path) for key, path in outputs.items()},
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    outputs["markdown"].write_text(
        "\ufeff"
        + _render_markdown(
            common_rows=tables["common_mode_points"],
            device_rows=tables["device_worst_points"],
            variant_summary_rows=tables["variant_summary"],
            s5_best_by_device=tables["baseline_s5_best_by_device"],
        ),
        encoding="utf-8",
    )
    return outputs
