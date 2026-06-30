"""CO2 SENCO1/SENCO3 zero-anchor contract review.

This module is offline-only. It reviews how the assigned CO2 value of the
zero-gas/low-end anchor affects the no-pressure SENCO1/SENCO3 main model.
SENCO5 is intentionally excluded here: final output-layer trim must not hide a
bad S1/S3 intercept or low-end shape.
"""

from __future__ import annotations

import csv
import json
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Sequence

from .co2_fit_algorithm_matrix import _safe_float
from .co2_s13_model_structure_review import build_co2_s13_model_structure_review
from .co2_zero_s5_sensitivity_review import DEFAULT_ZERO_OFFSETS_PPM

DEFAULT_ZERO_ANCHOR_OBJECTIVES = (
    "absolute_lstsq",
    "relative_weighted_lstsq",
    "low_end_priority_lstsq",
    "relative_irls_lstsq",
)


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


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
    numeric = _safe_float(value)
    if numeric is None:
        return ""
    return f"{numeric:.{digits}g}"


def _device_id(value: Any) -> str:
    text = str(value or "").strip().upper()
    if text.isdigit():
        return f"{int(text):03d}"
    return text


def _score(row: Mapping[str, Any]) -> tuple[float, float, float, float]:
    def metric(key: str) -> float:
        value = _safe_float(row.get(key))
        return abs(float(value)) if value is not None else float("inf")

    return (
        metric("max_abs_relative_error_percent"),
        metric("low_end_max_abs_relative_error_percent"),
        metric("rmse_ppm"),
        metric("zero_anchor_max_abs_error_ppm"),
    )


def _zero_offset(row: Mapping[str, Any]) -> float:
    return float(_safe_float(row.get("zero_offset_ppm")) or 0.0)


def _group_rows(rows: Sequence[Mapping[str, Any]], key: str) -> Dict[str, List[Mapping[str, Any]]]:
    grouped: Dict[str, List[Mapping[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[str(row.get(key) or "")].append(row)
    return dict(grouped)


def _residual_model_key(row: Mapping[str, Any]) -> tuple[str, str]:
    return (str(row.get("device_id") or ""), str(row.get("model_id") or ""))


def _is_zero_anchor_row(row: Mapping[str, Any]) -> bool:
    target = _safe_float(row.get("target_ppm"))
    marker = str(row.get("zero_anchor_class") or "").lower()
    identity = str(row.get("point_identity") or "").lower()
    return (
        target is not None
        and (
            abs(float(target)) <= 1.0e-9
            or "zero" in marker
            or identity.endswith("_0ppm")
        )
    )


def _temperature_group(row: Mapping[str, Any]) -> str:
    identity = str(row.get("point_identity") or "").strip()
    if identity.startswith("T") and "_" in identity:
        return identity.split("_", 1)[0]
    temp = _safe_float(row.get("temperature_c"))
    return f"T{float(temp):g}" if temp is not None else "T_unknown"


def _max_abs(values: Iterable[float]) -> float | str:
    collected = [abs(float(value)) for value in values]
    return max(collected) if collected else ""


def _zero_anchor_temperature_rows(
    selected_rows: Sequence[Mapping[str, Any]],
    residuals: Sequence[Mapping[str, Any]],
) -> List[Dict[str, Any]]:
    residual_by_model: Dict[tuple[str, str], List[Mapping[str, Any]]] = defaultdict(list)
    for row in residuals:
        residual_by_model[_residual_model_key(row)].append(row)

    rows: List[Dict[str, Any]] = []
    for selected in selected_rows:
        device_id = str(selected.get("device_id") or "")
        model_id = str(selected.get("best_model_id") or "")
        grouped: Dict[str, List[Mapping[str, Any]]] = defaultdict(list)
        for row in residual_by_model.get((device_id, model_id), []):
            if _is_zero_anchor_row(row):
                grouped[_temperature_group(row)].append(row)
        for temp_group, items in sorted(grouped.items()):
            errors = [
                float(_safe_float(item.get("error_ppm")) or 0.0)
                for item in items
            ]
            rows.append(
                {
                    "device_id": device_id,
                    "selected_zero_offset_ppm": selected.get("best_zero_offset_ppm"),
                    "temperature_group": temp_group,
                    "zero_anchor_point_count": len(items),
                    "zero_anchor_mean_error_ppm": (
                        sum(errors) / len(errors) if errors else ""
                    ),
                    "zero_anchor_max_abs_error_ppm": _max_abs(errors),
                    "physical_meaning": (
                        "Zero-gas anchors constrain the CO2 intercept at each "
                        "temperature; residual sign consistency indicates whether "
                        "the assigned zero CO2 value needs review."
                    ),
                }
            )
    return rows


def _low_end_driver_rows(
    selected_rows: Sequence[Mapping[str, Any]],
    residuals: Sequence[Mapping[str, Any]],
    *,
    low_end_target_ppm: float,
    max_rows_per_device: int = 10,
) -> List[Dict[str, Any]]:
    residual_by_model: Dict[tuple[str, str], List[Mapping[str, Any]]] = defaultdict(list)
    for row in residuals:
        residual_by_model[_residual_model_key(row)].append(row)

    rows: List[Dict[str, Any]] = []
    for selected in selected_rows:
        device_id = str(selected.get("device_id") or "")
        model_id = str(selected.get("best_model_id") or "")
        candidates: List[Mapping[str, Any]] = []
        for row in residual_by_model.get((device_id, model_id), []):
            if _is_zero_anchor_row(row):
                continue
            target = _safe_float(row.get("target_ppm"))
            if target is None or target <= 0.0 or target > float(low_end_target_ppm):
                continue
            candidates.append(row)
        candidates.sort(
            key=lambda item: abs(float(_safe_float(item.get("relative_error_percent")) or 0.0)),
            reverse=True,
        )
        for row in candidates[:max_rows_per_device]:
            rows.append(
                {
                    "device_id": device_id,
                    "point_identity": row.get("point_identity"),
                    "target_ppm": row.get("target_ppm"),
                    "temperature_group": _temperature_group(row),
                    "prediction_ppm": row.get("prediction_ppm"),
                    "error_ppm": row.get("error_ppm"),
                    "relative_error_percent": row.get("relative_error_percent"),
                    "ratio": row.get("ratio"),
                    "temperature_c": row.get("temperature_c"),
                    "h2o_mmol": row.get("h2o_mmol"),
                    "physical_meaning": (
                        "Low-end non-zero points are the main stress test for "
                        "S1/S3 intercept and low-concentration shape."
                    ),
                }
            )
    return rows


def _scenario_by_zero_offset_rows(summary_rows: Sequence[Mapping[str, Any]]) -> List[Dict[str, Any]]:
    grouped: Dict[tuple[str, float], List[Mapping[str, Any]]] = defaultdict(list)
    for row in summary_rows:
        grouped[(str(row.get("device_id") or ""), _zero_offset(row))].append(row)
    out: List[Dict[str, Any]] = []
    for (device_id, zero_offset), rows in sorted(grouped.items()):
        best = min(rows, key=_score)
        out.append(
            {
                "device_id": device_id,
                "zero_offset_ppm": zero_offset,
                "best_objective_id": best.get("objective_id"),
                "max_abs_relative_error_percent": best.get("max_abs_relative_error_percent"),
                "low_end_max_abs_relative_error_percent": best.get(
                    "low_end_max_abs_relative_error_percent"
                ),
                "zero_anchor_max_abs_error_ppm": best.get("zero_anchor_max_abs_error_ppm"),
                "rmse_ppm": best.get("rmse_ppm"),
                "model_id": best.get("model_id"),
                "physical_meaning": (
                    "This row keeps S1/S3 only and compares how an assumed "
                    "zero-gas CO2 value moves the low-end residuals."
                ),
            }
        )
    return out


def _recommendation(best: Mapping[str, Any], zero0_best: Mapping[str, Any] | None) -> str:
    best_zero = _zero_offset(best)
    if not zero0_best:
        return "review_zero_anchor_contract_no_write"
    best_score = _safe_float(best.get("max_abs_relative_error_percent"))
    zero0_score = _safe_float(zero0_best.get("max_abs_relative_error_percent"))
    if best_zero != 0.0 and best_score is not None and zero0_score is not None:
        if best_score < zero0_score * 0.9:
            return "review_estimated_zero_co2_assigned_value_before_s13_write"
        return "zero_offset_sensitivity_exists_but_gain_is_small"
    return "keep_zero_anchor_0ppm_assumption_review_s13_residuals"


def _selected_contract_rows(summary_rows: Sequence[Mapping[str, Any]]) -> List[Dict[str, Any]]:
    grouped = _group_rows(summary_rows, "device_id")
    selected: List[Dict[str, Any]] = []
    for device_id, rows in sorted(grouped.items()):
        best = min(rows, key=_score)
        zero0_rows = [row for row in rows if _zero_offset(row) == 0.0]
        zero0_best = min(zero0_rows, key=_score) if zero0_rows else None
        baseline_abs = next(
            (
                row
                for row in zero0_rows
                if str(row.get("objective_id") or "") == "absolute_lstsq"
            ),
            zero0_best,
        )
        selected.append(
            {
                "device_id": device_id,
                "baseline_zero0_objective_id": (
                    zero0_best.get("objective_id") if zero0_best else ""
                ),
                "baseline_zero0_max_abs_relative_error_percent": (
                    zero0_best.get("max_abs_relative_error_percent") if zero0_best else ""
                ),
                "baseline_zero0_low_end_max_abs_relative_error_percent": (
                    zero0_best.get("low_end_max_abs_relative_error_percent") if zero0_best else ""
                ),
                "baseline_zero0_absolute_lstsq_max_abs_relative_error_percent": (
                    baseline_abs.get("max_abs_relative_error_percent") if baseline_abs else ""
                ),
                "best_objective_id": best.get("objective_id"),
                "best_zero_offset_ppm": best.get("zero_offset_ppm"),
                "best_max_abs_relative_error_percent": best.get("max_abs_relative_error_percent"),
                "best_low_end_max_abs_relative_error_percent": best.get(
                    "low_end_max_abs_relative_error_percent"
                ),
                "best_zero_anchor_max_abs_error_ppm": best.get("zero_anchor_max_abs_error_ppm"),
                "best_rmse_ppm": best.get("rmse_ppm"),
                "best_model_id": best.get("model_id"),
                "best_s1_payload_scientific": best.get("s1_payload_scientific"),
                "best_s3_payload_scientific": best.get("s3_payload_scientific"),
                "recommended_no_write_action": _recommendation(best, zero0_best),
                "requires_zero_gas_traceability_review": best.get(
                    "requires_zero_gas_traceability_review"
                ),
                "uses_pressure_terms": False,
                "uses_s5_output_trim": False,
                "auto_write_allowed": False,
                "physical_meaning": (
                    "Selected candidate is S1/S3-only. Non-zero zero-gas ppm "
                    "is a traceability assumption, not a write approval."
                ),
            }
        )
    return selected


def build_co2_s13_zero_anchor_contract_review(
    *,
    fit_points_csv: str | Path,
    fit_point_treatment_plan_csv: str | Path | None = None,
    exclude_device_ids: Sequence[str] = (),
    zero_offsets_ppm: Sequence[float] = DEFAULT_ZERO_OFFSETS_PPM,
    objectives: Sequence[str] = DEFAULT_ZERO_ANCHOR_OBJECTIVES,
    min_relative_target_ppm: float = 50.0,
    low_end_target_ppm: float = 300.0,
    low_end_multiplier: float = 3.0,
) -> Dict[str, List[Dict[str, Any]]]:
    """Build no-write zero-anchor contract tables for S1/S3 only."""

    base = build_co2_s13_model_structure_review(
        fit_points_csv=fit_points_csv,
        fit_point_treatment_plan_csv=fit_point_treatment_plan_csv,
        exclude_device_ids=exclude_device_ids,
        structures=("core_plus_full_temp",),
        objectives=tuple(objectives),
        zero_offsets_ppm=tuple(float(value) for value in zero_offsets_ppm),
        min_relative_target_ppm=float(min_relative_target_ppm),
        low_end_target_ppm=float(low_end_target_ppm),
        low_end_multiplier=float(low_end_multiplier),
    )
    summary = list(base.get("structure_summary") or [])
    residuals = list(base.get("structure_residuals") or [])
    selected = _selected_contract_rows(summary)
    zero_sensitivity = _scenario_by_zero_offset_rows(summary)
    return {
        "run_summary": [
            {
                "created_at": _now(),
                "fit_points_csv": str(Path(fit_points_csv).resolve()),
                "fit_point_treatment_plan_csv": (
                    str(Path(fit_point_treatment_plan_csv).resolve())
                    if fit_point_treatment_plan_csv
                    else ""
                ),
                "device_count": len(_group_rows(summary, "device_id")),
                "zero_offsets_ppm": ";".join(f"{float(value):g}" for value in zero_offsets_ppm),
                "objectives": ";".join(str(value) for value in objectives),
                "model_structure": "core_plus_full_temp",
                "opens_com_ports": False,
                "controls_water_or_gas_routes": False,
                "writes_coefficients": False,
                "uses_pressure_terms": False,
                "uses_s5_output_trim": False,
                "not_real_acceptance_evidence": True,
            }
        ],
        "zero_anchor_contract_selection": selected,
        "zero_offset_sensitivity": zero_sensitivity,
        "zero_anchor_temperature_residuals": _zero_anchor_temperature_rows(selected, residuals),
        "low_end_residual_drivers": _low_end_driver_rows(
            selected,
            residuals,
            low_end_target_ppm=float(low_end_target_ppm),
        ),
        "s13_only_scenario_summary": summary,
        "s13_only_residuals": residuals,
    }


def _write_markdown(path: Path, tables: Mapping[str, Sequence[Mapping[str, Any]]]) -> None:
    selected = list(tables.get("zero_anchor_contract_selection") or [])
    sensitivity = list(tables.get("zero_offset_sensitivity") or [])
    drivers = list(tables.get("low_end_residual_drivers") or [])
    lines = [
        "# V1.5 CO2 S1/S3 零气锚定合同评审",
        "",
        "本报告只做离线评审：不打开 COM、不控制气路/水路、不写 SENCO。评审范围限定为 CO2 S1/S3 主模型；S5 输出层线性修正在这里被明确排除。",
        "",
        "## 逐台结论",
        "",
        "| 设备 ID | 零气 0ppm 基线最大相对误差(%) | 推荐零气假设(ppm) | 推荐目标函数 | 推荐后最大相对误差(%) | 低端最大相对误差(%) | 建议 |",
        "| --- | ---: | ---: | --- | ---: | ---: | --- |",
    ]
    for row in selected:
        lines.append(
            "| {device} | {base} | {zero} | {objective} | {best} | {low} | {action} |".format(
                device=row.get("device_id", ""),
                base=_fmt(row.get("baseline_zero0_max_abs_relative_error_percent"), 4),
                zero=_fmt(row.get("best_zero_offset_ppm"), 4),
                objective=row.get("best_objective_id", ""),
                best=_fmt(row.get("best_max_abs_relative_error_percent"), 4),
                low=_fmt(row.get("best_low_end_max_abs_relative_error_percent"), 4),
                action=row.get("recommended_no_write_action", ""),
            )
        )
    lines.extend(
        [
            "",
            "## 零气假设敏感性",
            "",
            "| 设备 ID | 零气假设(ppm) | 最优目标函数 | 最大相对误差(%) | 低端最大相对误差(%) | 零气最大绝对误差(ppm) |",
            "| --- | ---: | --- | ---: | ---: | ---: |",
        ]
    )
    for row in sensitivity:
        lines.append(
            "| {device} | {zero} | {objective} | {max_rel} | {low_rel} | {zero_abs} |".format(
                device=row.get("device_id", ""),
                zero=_fmt(row.get("zero_offset_ppm"), 4),
                objective=row.get("best_objective_id", ""),
                max_rel=_fmt(row.get("max_abs_relative_error_percent"), 4),
                low_rel=_fmt(row.get("low_end_max_abs_relative_error_percent"), 4),
                zero_abs=_fmt(row.get("zero_anchor_max_abs_error_ppm"), 4),
            )
        )
    lines.extend(
        [
            "",
            "## 低端残差驱动点",
            "",
            "下表只列推荐 S1/S3 候选下的低端非零点，用来判断截距和低浓度形状问题；这些点不因残差大而自动剔除。",
            "",
            "| 设备 ID | 点位 | 温度组 | 目标(ppm) | 预测(ppm) | 误差(ppm) | 相对误差(%) |",
            "| --- | --- | --- | ---: | ---: | ---: | ---: |",
        ]
    )
    for row in drivers[:80]:
        lines.append(
            "| {device} | {point} | {temp} | {target} | {pred} | {err} | {rel} |".format(
                device=row.get("device_id", ""),
                point=row.get("point_identity", ""),
                temp=row.get("temperature_group", ""),
                target=_fmt(row.get("target_ppm"), 4),
                pred=_fmt(row.get("prediction_ppm"), 5),
                err=_fmt(row.get("error_ppm"), 5),
                rel=_fmt(row.get("relative_error_percent"), 4),
            )
        )
    lines.extend(
        [
            "",
            "## 结论边界",
            "",
            "- 本评审不引入压力项；压力由独立 SENCO9 流程处理，当前大气压开放流通 CO2 主校准不能用污染压力项拟合。",
            "- CO2 零气锚点用于低端截距约束；若采用非 0ppm assigned value，必须在正式写入前给出证书、估算依据或不确定度声明。",
            "- H2O 干气锚点和 CO2 零气锚点不是同一个概念，不能因为水很干就自动认定 CO2 为 0ppm。",
            "- S5/S6 是最终输出层修正，应在 S1/S3 与 S2/S4 主模型评审后再单独评审，不能用于掩盖主模型结构残差。",
        ]
    )
    path.write_text("\ufeff" + "\n".join(lines) + "\n", encoding="utf-8")


def write_co2_s13_zero_anchor_contract_review(
    *,
    fit_points_csv: str | Path,
    output_dir: str | Path,
    fit_point_treatment_plan_csv: str | Path | None = None,
    exclude_device_ids: Sequence[str] = (),
    zero_offsets_ppm: Sequence[float] = DEFAULT_ZERO_OFFSETS_PPM,
    objectives: Sequence[str] = DEFAULT_ZERO_ANCHOR_OBJECTIVES,
    min_relative_target_ppm: float = 50.0,
    low_end_target_ppm: float = 300.0,
    low_end_multiplier: float = 3.0,
) -> Dict[str, Path]:
    """Write zero-anchor contract review artifacts."""

    destination = Path(output_dir).resolve()
    destination.mkdir(parents=True, exist_ok=True)
    tables = build_co2_s13_zero_anchor_contract_review(
        fit_points_csv=fit_points_csv,
        fit_point_treatment_plan_csv=fit_point_treatment_plan_csv,
        exclude_device_ids=exclude_device_ids,
        zero_offsets_ppm=zero_offsets_ppm,
        objectives=objectives,
        min_relative_target_ppm=float(min_relative_target_ppm),
        low_end_target_ppm=float(low_end_target_ppm),
        low_end_multiplier=float(low_end_multiplier),
    )
    outputs: Dict[str, Path] = {}
    for name, rows in tables.items():
        path = destination / f"{name}.csv"
        _write_csv(path, rows)
        outputs[name] = path
    meta = {
        "tool_name": "export_v1_5_co2_s13_zero_anchor_contract_review",
        "created_at": _now(),
        "inputs": {
            "fit_points_csv": str(Path(fit_points_csv).resolve()),
            "fit_point_treatment_plan_csv": (
                str(Path(fit_point_treatment_plan_csv).resolve())
                if fit_point_treatment_plan_csv
                else ""
            ),
            "exclude_device_ids": [_device_id(value) for value in exclude_device_ids],
            "zero_offsets_ppm": [float(value) for value in zero_offsets_ppm],
            "objectives": list(objectives),
        },
        "boundary": {
            "opens_com_ports": False,
            "controls_water_or_gas_routes": False,
            "writes_coefficients": False,
            "uses_pressure_terms": False,
            "uses_s5_output_trim": False,
            "not_real_acceptance_evidence": True,
        },
    }
    metadata_path = destination / "co2_s13_zero_anchor_contract_review_meta.json"
    metadata_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
    outputs["metadata"] = metadata_path
    markdown_path = destination / "co2_s13_zero_anchor_contract_review_zh.md"
    _write_markdown(markdown_path, tables)
    outputs["markdown"] = markdown_path
    return outputs
