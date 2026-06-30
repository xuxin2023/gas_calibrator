"""Offline CO2 SENCO1/SENCO3 low-end correction strategy review.

This module compares no-pressure S1/S3 fitting strategies on already-recorded
V1.5 open-flow CO2 evidence. It deliberately excludes S5 output-layer trim and
never opens COM ports, controls routes, or writes coefficients.
"""

from __future__ import annotations

import csv
import json
import math
from dataclasses import dataclass, replace
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence

from .co2_fit_algorithm_matrix import (
    CORE_TERMS,
    TEMP_TERMS,
    FitPoint,
    ModelVariant,
    _load_fit_points,
    _safe_float,
)
from .co2_relative_s13_objective_review import _apply_zero_offset, _fit_objective


DEFAULT_ZERO_OFFSETS_PPM = (0.0, 2.0, 5.0, 8.0, 10.0)
DEFAULT_LOW_END_MULTIPLIERS = (1.0, 2.0, 3.0, 5.0, 8.0, 12.0)
DEFAULT_DIAGNOSTIC_HOLDOUT_POINTS = (
    "T20_100ppm",
    "T20_200ppm",
    "T30_0ppm",
    "T30_100ppm",
    "T40_0ppm",
)

TERMS_BY_STRUCTURE: Mapping[str, tuple[str, ...]] = {
    "core_plus_linear_temp": CORE_TERMS + ("T", "RT"),
    "core_plus_full_temp": CORE_TERMS + TEMP_TERMS,
}
OBJECTIVES = (
    "absolute_lstsq",
    "relative_weighted_lstsq",
    "low_end_priority_lstsq",
    "relative_irls_lstsq",
)


@dataclass(frozen=True)
class StrategySpec:
    strategy_id: str
    structure_id: str
    terms: tuple[str, ...]
    objective_id: str
    zero_offset_ppm: float
    low_end_multiplier: float
    held_out_point_identity: str = ""
    diagnostic_only: bool = False


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


def _fmt(value: Any, digits: int = 5) -> str:
    number = _safe_float(value)
    if number is None:
        return ""
    return f"{number:.{digits}g}"


def _device_groups(points: Sequence[FitPoint]) -> Dict[str, List[FitPoint]]:
    grouped: Dict[str, List[FitPoint]] = {}
    for point in points:
        grouped.setdefault(point.device_id, []).append(point)
    return grouped


def _make_variant(spec: StrategySpec) -> ModelVariant:
    return ModelVariant(
        model_id=spec.strategy_id,
        terms=spec.terms,
        pressure_unit="kpa",
        preserve_existing_pressure_slots=False,
        use_celsius_temperature=False,
        apply_h2o_dry_basis_target_bridge=False,
        write_contract=(
            "diagnostic_holdout_only_no_write"
            if spec.diagnostic_only
            else "no_pressure_senco13_low_end_strategy_no_s5_no_write"
        ),
    )


def _build_regular_specs(
    *,
    zero_offsets_ppm: Sequence[float],
    low_end_multipliers: Sequence[float],
) -> List[StrategySpec]:
    specs: List[StrategySpec] = []
    for structure_id, terms in TERMS_BY_STRUCTURE.items():
        for zero in zero_offsets_ppm:
            for objective_id in OBJECTIVES:
                multipliers = (
                    low_end_multipliers
                    if objective_id == "low_end_priority_lstsq"
                    else (1.0,)
                )
                for multiplier in multipliers:
                    suffix = (
                        f"__m{float(multiplier):g}"
                        if objective_id == "low_end_priority_lstsq"
                        else ""
                    )
                    strategy_id = (
                        f"{structure_id}__{objective_id}__zero{float(zero):g}{suffix}"
                    )
                    specs.append(
                        StrategySpec(
                            strategy_id=strategy_id,
                            structure_id=structure_id,
                            terms=terms,
                            objective_id=objective_id,
                            zero_offset_ppm=float(zero),
                            low_end_multiplier=float(multiplier),
                        )
                    )
    return specs


def _diagnostic_specs_from_best(
    best_rows: Sequence[Mapping[str, Any]],
    *,
    holdout_points: Sequence[str],
) -> List[StrategySpec]:
    specs: List[StrategySpec] = []
    for row in best_rows:
        structure_id = str(row.get("structure_id") or "")
        terms = TERMS_BY_STRUCTURE.get(structure_id)
        if not terms:
            continue
        for point_identity in holdout_points:
            strategy_id = (
                f"{structure_id}__{row.get('objective_id')}__zero"
                f"{float(row.get('zero_offset_ppm') or 0.0):g}"
                f"__holdout_{point_identity}"
            )
            specs.append(
                StrategySpec(
                    strategy_id=strategy_id,
                    structure_id=structure_id,
                    terms=terms,
                    objective_id=str(row.get("objective_id") or "absolute_lstsq"),
                    zero_offset_ppm=float(row.get("zero_offset_ppm") or 0.0),
                    low_end_multiplier=float(row.get("low_end_multiplier") or 1.0),
                    held_out_point_identity=point_identity,
                    diagnostic_only=True,
                )
            )
    return specs


def _apply_holdout(points: Sequence[FitPoint], point_identity: str) -> List[FitPoint]:
    if not point_identity:
        return list(points)
    out: List[FitPoint] = []
    for point in points:
        if str(point.point_identity) == point_identity:
            out.append(replace(point, source_role="diagnostic"))
        else:
            out.append(point)
    return out


def _score(row: Mapping[str, Any]) -> tuple[float, float, float, float]:
    max_rel = _safe_float(row.get("max_abs_relative_error_percent"))
    low_rel = _safe_float(row.get("low_end_max_abs_relative_error_percent"))
    zero_abs = _safe_float(row.get("zero_anchor_max_abs_error_ppm"))
    rmse = _safe_float(row.get("rmse_ppm"))
    return (
        float(max_rel) if max_rel is not None else float("inf"),
        float(low_rel) if low_rel is not None else float("inf"),
        float(zero_abs) if zero_abs is not None else float("inf"),
        float(rmse) if rmse is not None else float("inf"),
    )


def _summarize_strategy(
    *,
    device_id: str,
    spec: StrategySpec,
    summary: Mapping[str, Any],
) -> Dict[str, Any]:
    row = dict(summary)
    row.update(
        {
            "device_id": device_id,
            "strategy_id": spec.strategy_id,
            "structure_id": spec.structure_id,
            "terms": ";".join(spec.terms),
            "objective_id": spec.objective_id,
            "zero_offset_ppm": float(spec.zero_offset_ppm),
            "low_end_multiplier": (
                float(spec.low_end_multiplier)
                if spec.objective_id == "low_end_priority_lstsq"
                else ""
            ),
            "held_out_point_identity": spec.held_out_point_identity,
            "diagnostic_only": spec.diagnostic_only,
            "uses_pressure_terms": False,
            "uses_s5_output_trim": False,
            "opens_com_ports": False,
            "controls_water_or_gas_routes": False,
            "writes_coefficients": False,
            "auto_write_allowed": False,
        }
    )
    return row


def _residual_row(
    row: Mapping[str, Any],
    *,
    spec: StrategySpec,
    selected_strategy: bool,
) -> Dict[str, Any]:
    out = dict(row)
    out.update(
        {
            "strategy_id": spec.strategy_id,
            "structure_id": spec.structure_id,
            "held_out_point_identity": spec.held_out_point_identity,
            "diagnostic_only": spec.diagnostic_only,
            "selected_strategy": selected_strategy,
            "uses_pressure_terms": False,
            "uses_s5_output_trim": False,
        }
    )
    return out


def _recommend_action(best: Mapping[str, Any], baseline: Mapping[str, Any] | None) -> str:
    best_rel = _safe_float(best.get("max_abs_relative_error_percent"))
    baseline_rel = _safe_float(baseline.get("max_abs_relative_error_percent")) if baseline else None
    if best_rel is None:
        return "人工复核：候选策略缺少有效误差指标"
    if best_rel <= 1.5:
        return "S1/S3 主链路候选可进入写入前评审，仍需保留零气指定值和不确定度说明"
    if baseline_rel is not None and best_rel < baseline_rel * 0.8:
        return "低端目标函数或零气假设有明显改善，但尚未达到目标，应继续查低端共同偏差来源"
    return "S1/S3 主模型仍不足，先查零气指定值、低端共同偏差和温度项，不建议用 S5 掩盖"


def _diagnostic_interpretation(
    holdout: Mapping[str, Any],
    best: Mapping[str, Any] | None,
) -> str:
    if not best:
        return "缺少常规最佳策略，不能解释删点敏感性"
    h_rel = _safe_float(holdout.get("max_abs_relative_error_percent"))
    b_rel = _safe_float(best.get("max_abs_relative_error_percent"))
    point = str(holdout.get("held_out_point_identity") or "")
    if h_rel is None or b_rel is None:
        return "删点诊断缺少有效指标，不能作为剔除依据"
    delta = float(b_rel) - float(h_rel)
    if delta > 0.5:
        return (
            f"{point} 暂不参与拟合会改善最大相对误差 {delta:.3g} 个百分点，"
            "但这只是敏感性证据；只有找到阀路、露点、ratio、标准气或状态寄存器异常，才允许降级/剔除"
        )
    if delta < -0.2:
        return f"移出 {point} 会恶化拟合，说明该点对主模型约束有正面作用，应保留"
    return f"移出 {point} 对拟合影响有限，应保留并作为普通拟合点处理"


def build_co2_s13_low_end_correction_strategy_review(
    *,
    fit_points_csv: str | Path,
    fit_point_treatment_plan_csv: str | Path | None = None,
    exclude_device_ids: Sequence[str] = (),
    zero_offsets_ppm: Sequence[float] = DEFAULT_ZERO_OFFSETS_PPM,
    low_end_multipliers: Sequence[float] = DEFAULT_LOW_END_MULTIPLIERS,
    diagnostic_holdout_points: Sequence[str] = DEFAULT_DIAGNOSTIC_HOLDOUT_POINTS,
    min_relative_target_ppm: float = 50.0,
    low_end_target_ppm: float = 300.0,
    irls_iterations: int = 5,
) -> Dict[str, List[Dict[str, Any]]]:
    """Build no-write low-end correction strategy tables."""

    points = _load_fit_points(
        fit_points_csv,
        exclude_device_ids=exclude_device_ids,
        treatment_plan_csv=fit_point_treatment_plan_csv,
    )
    groups = _device_groups(points)
    regular_specs = _build_regular_specs(
        zero_offsets_ppm=zero_offsets_ppm,
        low_end_multipliers=low_end_multipliers,
    )

    strategy_rows: List[Dict[str, Any]] = []
    residual_rows: List[Dict[str, Any]] = []
    best_rows: List[Dict[str, Any]] = []
    recommendation_rows: List[Dict[str, Any]] = []

    per_device_best: Dict[str, Dict[str, Any]] = {}
    per_device_baseline: Dict[str, Dict[str, Any]] = {}
    per_device_spec: Dict[str, StrategySpec] = {}

    for device_id, device_points in sorted(groups.items()):
        device_strategy_rows: List[Dict[str, Any]] = []
        rows_by_strategy: Dict[str, List[Dict[str, Any]]] = {}
        specs_by_strategy: Dict[str, StrategySpec] = {}
        for spec in regular_specs:
            adjusted = _apply_zero_offset(device_points, spec.zero_offset_ppm)
            variant = _make_variant(spec)
            summary, rows = _fit_objective(
                adjusted,
                variant=variant,
                objective_id=spec.objective_id,
                zero_offset_ppm=spec.zero_offset_ppm,
                min_relative_target_ppm=float(min_relative_target_ppm),
                low_end_target_ppm=float(low_end_target_ppm),
                low_end_multiplier=float(spec.low_end_multiplier),
                irls_iterations=int(irls_iterations),
            )
            if not summary:
                continue
            srow = _summarize_strategy(device_id=device_id, spec=spec, summary=summary)
            device_strategy_rows.append(srow)
            strategy_rows.append(srow)
            rows_by_strategy[spec.strategy_id] = [
                _residual_row(row, spec=spec, selected_strategy=False) for row in rows
            ]
            specs_by_strategy[spec.strategy_id] = spec

        if not device_strategy_rows:
            continue

        baseline = next(
            (
                row
                for row in device_strategy_rows
                if row.get("structure_id") == "core_plus_full_temp"
                and row.get("objective_id") == "absolute_lstsq"
                and float(row.get("zero_offset_ppm") or 0.0) == 0.0
            ),
            device_strategy_rows[0],
        )
        best = min(device_strategy_rows, key=_score)
        per_device_baseline[device_id] = dict(baseline)
        per_device_best[device_id] = dict(best)
        per_device_spec[device_id] = specs_by_strategy[str(best["strategy_id"])]
        best_rows.append(
            {
                "device_id": device_id,
                "baseline_strategy_id": baseline.get("strategy_id", ""),
                "baseline_max_abs_relative_error_percent": baseline.get(
                    "max_abs_relative_error_percent", ""
                ),
                "baseline_low_end_max_abs_relative_error_percent": baseline.get(
                    "low_end_max_abs_relative_error_percent", ""
                ),
                "best_strategy_id": best.get("strategy_id", ""),
                "structure_id": best.get("structure_id", ""),
                "objective_id": best.get("objective_id", ""),
                "zero_offset_ppm": best.get("zero_offset_ppm", ""),
                "low_end_multiplier": best.get("low_end_multiplier", ""),
                "max_abs_relative_error_percent": best.get(
                    "max_abs_relative_error_percent", ""
                ),
                "low_end_max_abs_relative_error_percent": best.get(
                    "low_end_max_abs_relative_error_percent", ""
                ),
                "zero_anchor_max_abs_error_ppm": best.get("zero_anchor_max_abs_error_ppm", ""),
                "rmse_ppm": best.get("rmse_ppm", ""),
                "s1_payload_scientific": best.get("s1_payload_scientific", ""),
                "s3_payload_scientific": best.get("s3_payload_scientific", ""),
                "auto_write_allowed": False,
                "recommended_action": _recommend_action(best, baseline),
                "physical_basis": (
                    "当前策略只修 S1/S3 主链路；压力项冻结为 0，S5 输出层不参与。"
                    "若低端共同偏差仍大，应先查零气和温度/气点共同偏差。"
                ),
            }
        )
        best_strategy_id = str(best["strategy_id"])
        for row in rows_by_strategy.get(best_strategy_id, []):
            selected = dict(row)
            selected["selected_strategy"] = True
            residual_rows.append(selected)

    holdout_rows: List[Dict[str, Any]] = []
    if diagnostic_holdout_points:
        for device_id, device_points in sorted(groups.items()):
            best = per_device_best.get(device_id)
            if not best:
                continue
            best_spec = per_device_spec[device_id]
            for point_identity in diagnostic_holdout_points:
                if not any(point.point_identity == point_identity for point in device_points):
                    continue
                spec = StrategySpec(
                    strategy_id=f"{best_spec.strategy_id}__holdout_{point_identity}",
                    structure_id=best_spec.structure_id,
                    terms=best_spec.terms,
                    objective_id=best_spec.objective_id,
                    zero_offset_ppm=best_spec.zero_offset_ppm,
                    low_end_multiplier=best_spec.low_end_multiplier,
                    held_out_point_identity=point_identity,
                    diagnostic_only=True,
                )
                adjusted = _apply_zero_offset(device_points, spec.zero_offset_ppm)
                adjusted = _apply_holdout(adjusted, point_identity)
                summary, _rows = _fit_objective(
                    adjusted,
                    variant=_make_variant(spec),
                    objective_id=spec.objective_id,
                    zero_offset_ppm=spec.zero_offset_ppm,
                    min_relative_target_ppm=float(min_relative_target_ppm),
                    low_end_target_ppm=float(low_end_target_ppm),
                    low_end_multiplier=float(spec.low_end_multiplier),
                    irls_iterations=int(irls_iterations),
                )
                if not summary:
                    continue
                hrow = _summarize_strategy(device_id=device_id, spec=spec, summary=summary)
                hrow.update(
                    {
                        "regular_best_strategy_id": best.get("strategy_id", ""),
                        "regular_best_max_abs_relative_error_percent": best.get(
                            "max_abs_relative_error_percent", ""
                        ),
                        "max_relative_error_delta_vs_best_percent_points": (
                            (float(best.get("max_abs_relative_error_percent") or 0.0)
                             - float(summary.get("max_abs_relative_error_percent") or 0.0))
                        ),
                        "auto_exclude_allowed": False,
                        "diagnostic_interpretation": _diagnostic_interpretation(hrow, best),
                    }
                )
                holdout_rows.append(hrow)

    for best in best_rows:
        device_id = str(best.get("device_id") or "")
        baseline = per_device_baseline.get(device_id, {})
        recommendation_rows.append(
            {
                "device_id": device_id,
                "priority": "P0" if _safe_float(best.get("max_abs_relative_error_percent")) and float(best.get("max_abs_relative_error_percent")) > 1.5 else "P1",
                "topic": "S1/S3 低端主模型",
                "finding": (
                    f"最佳策略 {best.get('best_strategy_id')} 最大相对误差 "
                    f"{_fmt(best.get('max_abs_relative_error_percent'), 4)}%，"
                    f"基线为 {_fmt(baseline.get('max_abs_relative_error_percent'), 4)}%。"
                ),
                "action": best.get("recommended_action", ""),
                "writes_coefficients": False,
            }
        )

    run_summary = [
        {
            "created_at": _now(),
            "fit_points_csv": str(Path(fit_points_csv).resolve()),
            "fit_point_treatment_plan_csv": (
                str(Path(fit_point_treatment_plan_csv).resolve())
                if fit_point_treatment_plan_csv
                else ""
            ),
            "device_count": len(groups),
            "regular_strategy_count": len(strategy_rows),
            "diagnostic_holdout_strategy_count": len(holdout_rows),
            "zero_offsets_ppm": ";".join(f"{float(value):g}" for value in zero_offsets_ppm),
            "low_end_multipliers": ";".join(f"{float(value):g}" for value in low_end_multipliers),
            "diagnostic_holdout_points": ";".join(diagnostic_holdout_points),
            "opens_com_ports": False,
            "controls_water_or_gas_routes": False,
            "writes_coefficients": False,
            "uses_pressure_terms": False,
            "uses_s5_output_trim": False,
            "not_real_acceptance_evidence": True,
        }
    ]
    return {
        "run_summary": run_summary,
        "strategy_summary": strategy_rows,
        "best_regular_by_device": best_rows,
        "selected_residuals": residual_rows,
        "diagnostic_holdout_review": holdout_rows,
        "recommended_actions": recommendation_rows,
    }


def _write_markdown(path: Path, tables: Mapping[str, Sequence[Mapping[str, Any]]]) -> None:
    lines = [
        "# V1.5 CO2 S1/S3 低端修正策略评审",
        "",
        f"- 生成时间：{_now()}",
        "- 边界：离线 no-write；不打开 COM；不控制气路/水路；不写 SENCO。",
        "- 物理合同：CO2 主拟合只使用滤波后 CO2 ratio 与温度项；当前大气压开放流通数据不引入压力项；S5 输出层修正不参与本轮主模型判断。",
        "- 目的：比较零气估计、低端权重和温度项结构对每台设备最大相对误差的影响，并把“删点改善”限制为诊断证据。",
        "",
        "## 1. 每台最佳常规策略",
        "",
        "| 设备ID | 最佳策略 | 结构 | 目标函数 | 零气估计 ppm | 低端权重 | 最大相对误差 % | 低端最大相对误差 % | 建议 |",
        "| --- | --- | --- | --- | ---: | ---: | ---: | ---: | --- |",
    ]
    for row in tables.get("best_regular_by_device", []):
        lines.append(
            "| {device} | {strategy} | {structure} | {objective} | {zero} | {mult} | {max_rel} | {low_rel} | {action} |".format(
                device=row.get("device_id", ""),
                strategy=row.get("best_strategy_id", ""),
                structure=row.get("structure_id", ""),
                objective=row.get("objective_id", ""),
                zero=_fmt(row.get("zero_offset_ppm"), 4),
                mult=_fmt(row.get("low_end_multiplier"), 4),
                max_rel=_fmt(row.get("max_abs_relative_error_percent"), 4),
                low_rel=_fmt(row.get("low_end_max_abs_relative_error_percent"), 4),
                action=row.get("recommended_action", ""),
            )
        )

    lines.extend(
        [
            "",
            "## 2. 诊断性删点敏感性",
            "",
            "| 设备ID | 暂不拟合点 | 常规最大相对误差 % | 暂不拟合后最大相对误差 % | 改善百分点 | 解释 |",
            "| --- | --- | ---: | ---: | ---: | --- |",
        ]
    )
    for row in tables.get("diagnostic_holdout_review", []):
        lines.append(
            "| {device} | {point} | {base} | {holdout} | {delta} | {interp} |".format(
                device=row.get("device_id", ""),
                point=row.get("held_out_point_identity", ""),
                base=_fmt(row.get("regular_best_max_abs_relative_error_percent"), 4),
                holdout=_fmt(row.get("max_abs_relative_error_percent"), 4),
                delta=_fmt(row.get("max_relative_error_delta_vs_best_percent_points"), 4),
                interp=row.get("diagnostic_interpretation", ""),
            )
        )

    lines.extend(
        [
            "",
            "## 3. 物理结论",
            "",
            "- 如果低端点在深干露点、ratio A 级、状态正常条件下仍存在共同偏差，优先解释为零气指定值、低端温度形状或标准气/阀路目标状态问题，而不是直接删除点。",
            "- 低端权重可以改善某些设备的相对误差，但若高端误差被明显牺牲，不能作为正式写入策略。",
            "- 零气 CO2 估计是灵敏度参数，不是证书值；正式报告需要写明零气指定值和不确定度。",
            "- S5 只能在 S1/S3 主模型评审通过后，作为最终显示层线性微调单独评审。",
            "",
            "## 4. 建议动作",
            "",
        ]
    )
    for row in tables.get("recommended_actions", []):
        lines.append(
            f"- {row.get('priority', '')} {row.get('device_id', '')}: {row.get('finding', '')} {row.get('action', '')}"
        )
    path.write_text("\ufeff" + "\n".join(lines) + "\n", encoding="utf-8")


def write_co2_s13_low_end_correction_strategy_review(
    *,
    fit_points_csv: str | Path,
    output_dir: str | Path,
    fit_point_treatment_plan_csv: str | Path | None = None,
    exclude_device_ids: Sequence[str] = (),
    zero_offsets_ppm: Sequence[float] = DEFAULT_ZERO_OFFSETS_PPM,
    low_end_multipliers: Sequence[float] = DEFAULT_LOW_END_MULTIPLIERS,
    diagnostic_holdout_points: Sequence[str] = DEFAULT_DIAGNOSTIC_HOLDOUT_POINTS,
    min_relative_target_ppm: float = 50.0,
    low_end_target_ppm: float = 300.0,
    irls_iterations: int = 5,
) -> Dict[str, Path]:
    output = Path(output_dir).resolve()
    output.mkdir(parents=True, exist_ok=True)
    tables = build_co2_s13_low_end_correction_strategy_review(
        fit_points_csv=fit_points_csv,
        fit_point_treatment_plan_csv=fit_point_treatment_plan_csv,
        exclude_device_ids=exclude_device_ids,
        zero_offsets_ppm=zero_offsets_ppm,
        low_end_multipliers=low_end_multipliers,
        diagnostic_holdout_points=diagnostic_holdout_points,
        min_relative_target_ppm=min_relative_target_ppm,
        low_end_target_ppm=low_end_target_ppm,
        irls_iterations=irls_iterations,
    )
    outputs = {
        "run_summary": output / "co2_s13_low_end_correction_strategy_run_summary.csv",
        "strategy_summary": output / "co2_s13_low_end_correction_strategy_summary.csv",
        "best_regular_by_device": output / "co2_s13_low_end_correction_best_by_device.csv",
        "selected_residuals": output / "co2_s13_low_end_correction_selected_residuals.csv",
        "diagnostic_holdout_review": output / "co2_s13_low_end_correction_holdout_review.csv",
        "recommended_actions": output / "co2_s13_low_end_correction_recommended_actions.csv",
        "metadata": output / "co2_s13_low_end_correction_strategy_meta.json",
        "markdown": output / "co2_s13_low_end_correction_strategy_review_zh.md",
    }
    for key in (
        "run_summary",
        "strategy_summary",
        "best_regular_by_device",
        "selected_residuals",
        "diagnostic_holdout_review",
        "recommended_actions",
    ):
        _write_csv(outputs[key], tables[key])
    outputs["metadata"].write_text(
        json.dumps(
            {
                "tool": "co2_s13_low_end_correction_strategy_review",
                "created_at": _now(),
                "inputs": {
                    "fit_points_csv": str(Path(fit_points_csv).resolve()),
                    "fit_point_treatment_plan_csv": (
                        str(Path(fit_point_treatment_plan_csv).resolve())
                        if fit_point_treatment_plan_csv
                        else ""
                    ),
                    "exclude_device_ids": list(exclude_device_ids),
                    "zero_offsets_ppm": list(zero_offsets_ppm),
                    "low_end_multipliers": list(low_end_multipliers),
                    "diagnostic_holdout_points": list(diagnostic_holdout_points),
                    "min_relative_target_ppm": min_relative_target_ppm,
                    "low_end_target_ppm": low_end_target_ppm,
                    "irls_iterations": irls_iterations,
                },
                "boundary": {
                    "opens_com_ports": False,
                    "controls_water_or_gas_routes": False,
                    "writes_coefficients": False,
                    "uses_pressure_terms": False,
                    "uses_s5_output_trim": False,
                    "not_real_acceptance_evidence": True,
                },
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    _write_markdown(outputs["markdown"], tables)
    return outputs
