"""Fast offline CO2 S1/S3 + S5 error-closure review.

This review is deliberately offline and no-write. It consumes an already
generated CO2 S1/S3 strategy result plus the matching residual table, then asks
one narrow question: can the final SENCO5 affine layer close the remaining
display error enough to enter controlled write review?
"""

from __future__ import annotations

import csv
import json
import math
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence

from .co2_senco5_linear_trim_review import _fit_quantized_command_trim


DEFAULT_ACCEPTANCE_PERCENT = 1.0
DEFAULT_MIN_RELATIVE_TARGET_PPM = 50.0
DEFAULT_S5_C0_DECIMALS = 3
DEFAULT_S5_C1_DECIMALS = 3
DEFAULT_S5_C1_MIN = 0.5
DEFAULT_S5_C1_MAX = 1.5


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


def _prediction(row: Mapping[str, Any]) -> Optional[float]:
    return _safe_float(
        row.get("prediction_ppm")
        or row.get("s5_corrected_prediction_ppm")
        or row.get("raw_senco13_prediction_ppm")
    )


def _target(row: Mapping[str, Any]) -> Optional[float]:
    return _safe_float(row.get("target_ppm"))


def _trim_rows(
    residuals: Sequence[Mapping[str, Any]],
    *,
    min_relative_target_ppm: float,
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for row in residuals:
        target = _target(row)
        predicted = _prediction(row)
        if target is None or predicted is None:
            continue
        if abs(float(target)) < float(min_relative_target_ppm):
            continue
        item = dict(row)
        item["_target"] = float(target)
        item["_measured"] = float(predicted)
        rows.append(item)
    return rows


def _evaluate_s5(
    residuals: Sequence[Mapping[str, Any]],
    *,
    c0: float,
    c1: float,
    min_relative_target_ppm: float,
) -> Dict[str, Any]:
    rel_errors: List[float] = []
    abs_errors: List[float] = []
    zero_abs_errors: List[float] = []
    worst_rel_point = ""
    worst_rel = -1.0
    worst_abs_point = ""
    worst_abs = -1.0
    corrected_rows: List[Dict[str, Any]] = []
    for row in residuals:
        target = _target(row)
        predicted = _prediction(row)
        if target is None or predicted is None:
            continue
        corrected = float(predicted) * float(c1) + float(c0)
        error = corrected - float(target)
        abs_error = abs(error)
        abs_errors.append(abs_error)
        point = str(row.get("point_identity") or "")
        rel_error: float | str = ""
        if abs(float(target)) >= float(min_relative_target_ppm):
            rel = error / float(target) * 100.0
            rel_errors.append(abs(rel))
            rel_error = rel
            if abs(rel) > worst_rel:
                worst_rel = abs(rel)
                worst_rel_point = point
        else:
            zero_abs_errors.append(abs_error)
        if abs_error > worst_abs:
            worst_abs = abs_error
            worst_abs_point = point
        corrected_rows.append(
            {
                **dict(row),
                "s5_C0": float(c0),
                "s5_C1": float(c1),
                "s5_corrected_prediction_ppm": corrected,
                "s5_error_ppm": error,
                "s5_relative_error_percent": rel_error,
            }
        )
    rmse = math.sqrt(sum(value * value for value in abs_errors) / len(abs_errors)) if abs_errors else 0.0
    return {
        "s5_max_abs_relative_error_percent": max(rel_errors) if rel_errors else "",
        "s5_mean_abs_relative_error_percent": sum(rel_errors) / len(rel_errors) if rel_errors else "",
        "s5_zero_anchor_max_abs_error_ppm": max(zero_abs_errors) if zero_abs_errors else "",
        "s5_max_abs_error_ppm": max(abs_errors) if abs_errors else "",
        "s5_rmse_ppm": rmse,
        "s5_worst_relative_point_identity": worst_rel_point,
        "s5_worst_abs_point_identity": worst_abs_point,
        "corrected_rows": corrected_rows,
    }


def _decision(row: Mapping[str, Any], *, acceptance_percent: float) -> str:
    max_rel = _safe_float(row.get("s5_max_abs_relative_error_percent"))
    if max_rel is None:
        return "blocked_no_relative_points"
    if max_rel <= float(acceptance_percent):
        return "candidate_for_controlled_write_review_s1s3_plus_s5"
    if max_rel <= float(acceptance_percent) * 1.5:
        return "near_target_but_not_formal_write_ready"
    return "blocked_fast_error_not_closed"


def build_co2_s13_fast_error_closure_candidate(
    *,
    best_by_device_csv: str | Path,
    residuals_csv: str | Path,
    acceptance_percent: float = DEFAULT_ACCEPTANCE_PERCENT,
    min_relative_target_ppm: float = DEFAULT_MIN_RELATIVE_TARGET_PPM,
    s5_c0_decimals: int = DEFAULT_S5_C0_DECIMALS,
    s5_c1_decimals: int = DEFAULT_S5_C1_DECIMALS,
    s5_c1_min: float = DEFAULT_S5_C1_MIN,
    s5_c1_max: float = DEFAULT_S5_C1_MAX,
) -> Dict[str, Any]:
    best_rows = _read_csv(best_by_device_csv)
    residual_rows = _read_csv(residuals_csv)
    residuals_by_key: Dict[tuple[str, str], List[Dict[str, Any]]] = {}
    for row in residual_rows:
        key = (_device_id(row.get("device_id")), str(row.get("strategy_id") or ""))
        residuals_by_key.setdefault(key, []).append(row)

    summary: List[Dict[str, Any]] = []
    corrected_residuals: List[Dict[str, Any]] = []
    for candidate in best_rows:
        device = _device_id(candidate.get("device_id"))
        strategy_id = str(candidate.get("strategy_id") or "")
        residuals = residuals_by_key.get((device, strategy_id), [])
        trim_input = _trim_rows(residuals, min_relative_target_ppm=float(min_relative_target_ppm))
        row: Dict[str, Any] = {
            "device_id": device,
            "strategy_id": strategy_id,
            "s1s3_strategy_profile_id": candidate.get("strategy_profile_id", ""),
            "s1s3_objective_id": candidate.get("objective_id", ""),
            "s1s3_zero_offset_ppm": candidate.get("zero_offset_ppm", ""),
            "s1s3_fit_point_count": candidate.get("fit_point_count", ""),
            "s1s3_max_abs_relative_error_percent": candidate.get("max_abs_relative_error_percent", ""),
            "s1s3_low_end_max_abs_relative_error_percent": candidate.get("low_end_max_abs_relative_error_percent", ""),
            "s1_payload_scientific": candidate.get("s1_payload_scientific", ""),
            "s3_payload_scientific": candidate.get("s3_payload_scientific", ""),
            "s5_search_c1_min": float(s5_c1_min),
            "s5_search_c1_max": float(s5_c1_max),
            "s5_c0_decimals": int(s5_c0_decimals),
            "s5_c1_decimals": int(s5_c1_decimals),
            "relative_trim_point_count": len(trim_input),
            "opens_com_ports": False,
            "controls_water_or_gas_routes": False,
            "writes_coefficients": False,
            "auto_write_allowed": False,
        }
        if len(trim_input) < 2:
            row.update(
                {
                    "s5_status": "blocked_relative_points_lt_2",
                    "s5_C0": "",
                    "s5_C1": "",
                    "s5_command_preview": "",
                    "recommended_action": "blocked_relative_points_lt_2",
                }
            )
            summary.append(row)
            continue
        c0, c1, fit_max_pct, fit_max_ppm, fit_rmse = _fit_quantized_command_trim(
            trim_input,
            c0_decimals=int(s5_c0_decimals),
            c1_decimals=int(s5_c1_decimals),
            c1_min=float(s5_c1_min),
            c1_max=float(s5_c1_max),
        )
        metrics = _evaluate_s5(
            residuals,
            c0=float(c0),
            c1=float(c1),
            min_relative_target_ppm=float(min_relative_target_ppm),
        )
        row.update(
            {
                "s5_status": "reviewable_no_write",
                "s5_C0": float(c0),
                "s5_C1": float(c1),
                "s5_command_preview": f"SENCO5,YGAS,FFF,{float(c0):.{int(s5_c0_decimals)}f},{float(c1):.{int(s5_c1_decimals)}f}",
                "s5_relative_fit_max_abs_error_percent": fit_max_pct,
                "s5_relative_fit_max_abs_error_ppm": fit_max_ppm,
                "s5_relative_fit_rmse_ppm": fit_rmse,
                **{key: value for key, value in metrics.items() if key != "corrected_rows"},
            }
        )
        row["recommended_action"] = _decision(row, acceptance_percent=float(acceptance_percent))
        summary.append(row)
        corrected_residuals.extend(metrics["corrected_rows"])

    return {
        "run_summary": [
            {
                "created_at": _now(),
                "best_by_device_csv": str(Path(best_by_device_csv).resolve()),
                "residuals_csv": str(Path(residuals_csv).resolve()),
                "device_count": len(summary),
                "acceptance_percent": float(acceptance_percent),
                "min_relative_target_ppm": float(min_relative_target_ppm),
                "s5_c0_decimals": int(s5_c0_decimals),
                "s5_c1_decimals": int(s5_c1_decimals),
                "s5_c1_min": float(s5_c1_min),
                "s5_c1_max": float(s5_c1_max),
                "opens_com_ports": False,
                "controls_water_or_gas_routes": False,
                "writes_coefficients": False,
                "not_real_acceptance_evidence": True,
            }
        ],
        "summary": summary,
        "corrected_residuals": corrected_residuals,
    }


def _fmt(value: Any, digits: int = 3) -> str:
    number = _safe_float(value)
    if number is None:
        return ""
    return f"{number:.{digits}f}"


def render_markdown(tables: Mapping[str, Sequence[Mapping[str, Any]]]) -> str:
    summary = list(tables.get("summary") or [])
    lines = [
        "# CO2 S1/S3 + S5 快速误差闭合评审",
        "",
        "本报告只使用既有 V1.5 开放流通采样证据做离线重算；不打开 COM、不控制气路/水路、不写 SENCO。",
        "",
        "## 物理边界",
        "",
        "- CO2 主拟合仍冻结压力项，压力由 SENCO9 独立处理。",
        "- 零气只作为 CO2 低端锚点敏感性；零点误差按 ppm 绝对误差看，不按相对百分比放大。",
        "- S5 是最终显示浓度线性层：`CO2_display = CO2_S1S3 * C1 + C0`，不能替代 S1/S3 主模型。",
        "",
        "## 逐设备快速结论",
        "",
        "| 设备ID | S1/S3最大相对误差% | S5后最大相对误差% | 最差点 | S5命令预览 | 建议 |",
        "| --- | ---: | ---: | --- | --- | --- |",
    ]
    for row in summary:
        lines.append(
            "| {device} | {base} | {s5} | {worst} | `{cmd}` | {action} |".format(
                device=row.get("device_id", ""),
                base=_fmt(row.get("s1s3_max_abs_relative_error_percent")),
                s5=_fmt(row.get("s5_max_abs_relative_error_percent")),
                worst=row.get("s5_worst_relative_point_identity", ""),
                cmd=row.get("s5_command_preview", ""),
                action=row.get("recommended_action", ""),
            )
        )
    ready = [row for row in summary if str(row.get("recommended_action")) == "candidate_for_controlled_write_review_s1s3_plus_s5"]
    blocked = [row for row in summary if str(row.get("recommended_action")).startswith("blocked")]
    near = [row for row in summary if str(row.get("recommended_action")).startswith("near_target")]
    lines.extend(
        [
            "",
            "## 总结",
            "",
            f"- 可进入受控写入评审：{len(ready)} 台。",
            f"- 接近目标但未正式闭合：{len(near)} 台。",
            f"- 快速误差闭合仍失败：{len(blocked)} 台。",
            "",
            "如果所有设备仍未闭合到目标，说明当前误差不能靠 S5 线性层解决，必须回到 S1/S3 主模型输入点或源状态桥接处理。",
        ]
    )
    return "\n".join(lines) + "\n"


def write_co2_s13_fast_error_closure_candidate(
    *,
    best_by_device_csv: str | Path,
    residuals_csv: str | Path,
    output_dir: str | Path,
    acceptance_percent: float = DEFAULT_ACCEPTANCE_PERCENT,
    min_relative_target_ppm: float = DEFAULT_MIN_RELATIVE_TARGET_PPM,
    s5_c0_decimals: int = DEFAULT_S5_C0_DECIMALS,
    s5_c1_decimals: int = DEFAULT_S5_C1_DECIMALS,
    s5_c1_min: float = DEFAULT_S5_C1_MIN,
    s5_c1_max: float = DEFAULT_S5_C1_MAX,
) -> Dict[str, str]:
    output = Path(output_dir)
    tables = build_co2_s13_fast_error_closure_candidate(
        best_by_device_csv=best_by_device_csv,
        residuals_csv=residuals_csv,
        acceptance_percent=acceptance_percent,
        min_relative_target_ppm=min_relative_target_ppm,
        s5_c0_decimals=s5_c0_decimals,
        s5_c1_decimals=s5_c1_decimals,
        s5_c1_min=s5_c1_min,
        s5_c1_max=s5_c1_max,
    )
    paths = {
        "run_summary": output / "co2_s13_fast_error_closure_run_summary.csv",
        "summary": output / "co2_s13_fast_error_closure_summary.csv",
        "corrected_residuals": output / "co2_s13_fast_error_closure_corrected_residuals.csv",
        "metadata": output / "co2_s13_fast_error_closure_meta.json",
        "markdown": output / "co2_s13_fast_error_closure_review_zh.md",
    }
    _write_csv(paths["run_summary"], tables["run_summary"])
    _write_csv(paths["summary"], tables["summary"])
    _write_csv(paths["corrected_residuals"], tables["corrected_residuals"])
    output.mkdir(parents=True, exist_ok=True)
    with paths["metadata"].open("w", encoding="utf-8") as handle:
        json.dump(
            {
                "tool": "co2_s13_fast_error_closure_candidate",
                "created_at": _now(),
                "inputs": {
                    "best_by_device_csv": str(Path(best_by_device_csv).resolve()),
                    "residuals_csv": str(Path(residuals_csv).resolve()),
                    "acceptance_percent": acceptance_percent,
                    "min_relative_target_ppm": min_relative_target_ppm,
                    "s5_c0_decimals": s5_c0_decimals,
                    "s5_c1_decimals": s5_c1_decimals,
                    "s5_c1_min": s5_c1_min,
                    "s5_c1_max": s5_c1_max,
                },
                "boundary": {
                    "opens_com_ports": False,
                    "controls_water_or_gas_routes": False,
                    "writes_coefficients": False,
                    "not_real_acceptance_evidence": True,
                },
                "outputs": {key: str(value.resolve()) for key, value in paths.items()},
            },
            handle,
            ensure_ascii=False,
            indent=2,
        )
    with paths["markdown"].open("w", encoding="utf-8") as handle:
        handle.write(render_markdown(tables))
    return {key: str(value) for key, value in paths.items()}
