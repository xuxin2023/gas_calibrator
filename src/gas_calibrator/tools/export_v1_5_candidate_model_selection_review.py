"""Export V1.5 candidate model-selection and S5/S6 trim review artifacts.

This tool is offline-only. It consumes already-exported candidate residual
tables and never opens COM ports, changes routes, controls PACE, or writes
SENCO values.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

import numpy as np

from ..validation.formal_candidate_coefficients import (
    _fit_candidate_coefficients,
    _fit_target_array,
    _prediction_array,
)


MODEL_FAMILIES: Tuple[Tuple[str, Tuple[str, ...]], ...] = (
    ("linear_R", ("intercept", "R")),
    ("quadratic_R", ("intercept", "R", "R2")),
    ("cubic_R", ("intercept", "R", "R2", "R3")),
    ("quadratic_R_T_RT", ("intercept", "R", "R2", "T", "RT")),
    ("cubic_R_T", ("intercept", "R", "R2", "R3", "T")),
    ("cubic_R_T_RT", ("intercept", "R", "R2", "R3", "T", "RT")),
    ("cubic_R_T_T2_RT", ("intercept", "R", "R2", "R3", "T", "T2", "RT")),
)


@dataclass(frozen=True)
class LinearTrim:
    method: str
    c0: float
    c1: float
    rmse: float
    max_abs_error: float
    max_abs_relative_error_pct: Optional[float]
    zero_max_abs_error: Optional[float]


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _safe_float(value: Any) -> Optional[float]:
    if value in (None, "", "null", "None"):
        return None
    try:
        numeric = float(value)
    except Exception:
        return None
    if not math.isfinite(numeric):
        return None
    return numeric


def _read_csv(path: Path) -> List[Dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _write_csv(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    keys: List[str] = []
    for row in rows:
        for key in row:
            if key not in keys:
                keys.append(key)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=keys)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key, "") for key in keys})


def _component_from_candidate_dir(candidate_dir: Path, explicit: str | None) -> str:
    if explicit:
        return explicit.strip().lower()
    summary_path = candidate_dir / "candidate_run_summary.csv"
    if summary_path.exists():
        rows = _read_csv(summary_path)
        if rows:
            component = str(rows[0].get("component") or "").strip().lower()
            if component:
                return component
    name = candidate_dir.name.lower()
    return "h2o" if "h2o" in name else "co2"


def _policy_by_device(candidate_dir: Path) -> Dict[Tuple[str, str], Dict[str, str]]:
    path = candidate_dir / "candidate_policy_summary.csv"
    if not path.exists():
        return {}
    out: Dict[Tuple[str, str], Dict[str, str]] = {}
    for row in _read_csv(path):
        key = (
            str(row.get("analyzer_prefix") or "").strip(),
            str(row.get("analyzer_device_id") or "").strip(),
        )
        out[key] = row
    return out


def _load_point_rows(candidate_dir: Path, component: str) -> Dict[Tuple[str, str], List[Dict[str, Any]]]:
    path = candidate_dir / "candidate_fit_residuals.csv"
    if not path.exists():
        raise FileNotFoundError(f"Missing candidate residual table: {path}")
    groups: Dict[Tuple[str, str], List[Dict[str, Any]]] = {}
    for row in _read_csv(path):
        if str(row.get("component") or "").strip().lower() != component:
            continue
        target = _safe_float(row.get("target_value"))
        ratio = _safe_float(row.get("ratio"))
        temperature = _safe_float(row.get("temperature_c"))
        pressure = _safe_float(row.get("pressure_hpa"))
        if target is None or ratio is None or temperature is None or pressure is None:
            continue
        h2o_mmol = _safe_float(row.get("h2o_mmol"))
        item = {
            "sample_index": row.get("sample_index", ""),
            "_point_identity": row.get("point_identity", row.get("sample_index", "")),
            "_target": float(target),
            "_ratio": float(ratio),
            "_temperature_c": float(temperature),
            "_pressure_hpa": float(pressure),
            "_h2o_mmol": h2o_mmol,
            "_h2o_mmol_source": row.get("h2o_mmol_source", ""),
        }
        key = (
            str(row.get("analyzer_prefix") or "").strip(),
            str(row.get("analyzer_device_id") or "").strip(),
        )
        groups.setdefault(key, []).append(item)
    return groups


def _metrics(prediction: np.ndarray, target: np.ndarray) -> Dict[str, Any]:
    error = prediction - target
    rmse = float(np.sqrt(np.mean(error**2))) if len(error) else math.nan
    max_abs = float(np.max(np.abs(error))) if len(error) else math.nan
    relative_values = [
        abs(float(err) / abs(float(truth)) * 100.0)
        for err, truth in zip(error, target)
        if abs(float(truth)) > 1.0e-12
    ]
    zero_errors = [
        abs(float(err))
        for err, truth in zip(error, target)
        if abs(float(truth)) <= 1.0e-12
    ]
    return {
        "rmse": rmse,
        "max_abs_error": max_abs,
        "max_abs_relative_error_pct": max(relative_values) if relative_values else "",
        "zero_max_abs_error": max(zero_errors) if zero_errors else "",
    }


def _evaluate_model(
    *,
    component: str,
    rows: Sequence[Mapping[str, Any]],
    model_name: str,
    terms: Sequence[str],
) -> Tuple[Dict[str, Any], Optional[np.ndarray], Optional[np.ndarray]]:
    target_for_fit = _fit_target_array(component=component, rows=rows, preserved_secondary_coefficients=())
    try:
        coefficients, rank, condition, absolute_condition, fit_basis = _fit_candidate_coefficients(
            rows,
            terms,
            target_for_fit,
        )
    except Exception as exc:
        return {
            "model_name": model_name,
            "model_terms": ";".join(terms),
            "fit_status": "error",
            "fit_error": str(exc),
        }, None, None
    status = "ok"
    if rank < len(terms):
        status = "rank_deficient"
    prediction = _prediction_array(
        component=component,
        rows=rows,
        terms=terms,
        coefficients=coefficients,
        preserved_secondary_coefficients=(),
    )
    truth = np.asarray([float(row["_target"]) for row in rows], dtype=float)
    metrics = _metrics(prediction, truth)
    summary: Dict[str, Any] = {
        "model_name": model_name,
        "model_terms": ";".join(terms),
        "fit_status": status,
        "rank": rank,
        "term_count": len(terms),
        "condition_number": condition,
        "absolute_condition_number": absolute_condition,
        "fit_basis": fit_basis.get("fit_basis", ""),
        "ratio_center": fit_basis.get("ratio_center", ""),
        "temperature_k_center": fit_basis.get("temperature_k_center", ""),
        **metrics,
    }
    for term, value in zip(terms, coefficients):
        summary[f"coef_{term}"] = float(value)
    return summary, coefficients, prediction


def _round3(value: float) -> float:
    return float(f"{float(value):.3f}")


def _linear_trim_metrics(method: str, c0: float, c1: float, prediction: np.ndarray, target: np.ndarray) -> LinearTrim:
    corrected = prediction * float(c1) + float(c0)
    metrics = _metrics(corrected, target)
    max_rel = metrics["max_abs_relative_error_pct"]
    return LinearTrim(
        method=method,
        c0=_round3(c0),
        c1=_round3(c1),
        rmse=float(metrics["rmse"]),
        max_abs_error=float(metrics["max_abs_error"]),
        max_abs_relative_error_pct=float(max_rel) if max_rel != "" else None,
        zero_max_abs_error=float(metrics["zero_max_abs_error"]) if metrics["zero_max_abs_error"] != "" else None,
    )


def _least_squares_trim(prediction: np.ndarray, target: np.ndarray) -> LinearTrim:
    design = np.column_stack([np.ones(len(prediction)), prediction])
    coeffs, _, _, _ = np.linalg.lstsq(design, target, rcond=None)
    c0 = _round3(float(coeffs[0]))
    c1 = _round3(float(coeffs[1]))
    return _linear_trim_metrics("least_squares_rounded_3dp", c0, c1, prediction, target)


def _feasible_c0_for_relative_error(
    prediction: np.ndarray,
    target: np.ndarray,
    *,
    c1: float,
    rel_fraction: float,
) -> Optional[float]:
    lower = -math.inf
    upper = math.inf
    for pred, truth in zip(prediction, target):
        if abs(float(truth)) <= 1.0e-12:
            continue
        radius = float(rel_fraction) * abs(float(truth))
        lo = float(truth) - radius - float(c1) * float(pred)
        hi = float(truth) + radius - float(c1) * float(pred)
        lower = max(lower, lo)
        upper = min(upper, hi)
        if lower > upper:
            return None
    if not math.isfinite(lower) and not math.isfinite(upper):
        return 0.0
    if not math.isfinite(lower):
        return upper
    if not math.isfinite(upper):
        return lower
    return (lower + upper) / 2.0


def _minimax_relative_trim(prediction: np.ndarray, target: np.ndarray) -> LinearTrim:
    ls = _least_squares_trim(prediction, target)
    center = float(ls.c1)
    start = max(-2.0, center - 0.25)
    stop = min(2.0, center + 0.25)
    best: Optional[LinearTrim] = None
    for i in range(int(round((stop - start) / 0.001)) + 1):
        c1 = _round3(start + i * 0.001)
        lo = 0.0
        hi = 5.0
        while _feasible_c0_for_relative_error(prediction, target, c1=c1, rel_fraction=hi) is None:
            hi *= 2.0
            if hi > 100.0:
                break
        for _ in range(36):
            mid = (lo + hi) / 2.0
            if _feasible_c0_for_relative_error(prediction, target, c1=c1, rel_fraction=mid) is None:
                lo = mid
            else:
                hi = mid
        c0 = _feasible_c0_for_relative_error(prediction, target, c1=c1, rel_fraction=hi)
        if c0 is None:
            continue
        trim = _linear_trim_metrics("minimax_relative_rounded_3dp", _round3(c0), c1, prediction, target)
        if best is None:
            best = trim
            continue
        current = best.max_abs_relative_error_pct if best.max_abs_relative_error_pct is not None else math.inf
        candidate = trim.max_abs_relative_error_pct if trim.max_abs_relative_error_pct is not None else math.inf
        if (candidate, trim.max_abs_error, abs(trim.c1 - 1.0), abs(trim.c0)) < (
            current,
            best.max_abs_error,
            abs(best.c1 - 1.0),
            abs(best.c0),
        ):
            best = trim
    return best or ls


def _trim_rows(
    *,
    component: str,
    prefix: str,
    device_id: str,
    model_summary: Mapping[str, Any],
    prediction: np.ndarray,
    target: np.ndarray,
) -> List[Dict[str, Any]]:
    base = _linear_trim_metrics("neutral_no_s5_s6", 0.0, 1.0, prediction, target)
    ls = _least_squares_trim(prediction, target)
    minimax = _minimax_relative_trim(prediction, target)
    out: List[Dict[str, Any]] = []
    for trim in (base, ls, minimax):
        out.append(
            {
                "component": component,
                "analyzer_prefix": prefix,
                "analyzer_device_id": device_id,
                "base_model_name": model_summary.get("model_name", ""),
                "base_model_terms": model_summary.get("model_terms", ""),
                "trim_method": trim.method,
                "senco_channel": "SENCO6" if component == "h2o" else "SENCO5",
                "c0": trim.c0,
                "c1": trim.c1,
                "rmse": trim.rmse,
                "max_abs_error": trim.max_abs_error,
                "max_abs_relative_error_pct": trim.max_abs_relative_error_pct
                if trim.max_abs_relative_error_pct is not None
                else "",
                "zero_max_abs_error": trim.zero_max_abs_error if trim.zero_max_abs_error is not None else "",
            }
        )
    return out


def _best_model(rows: Sequence[Mapping[str, Any]], summaries: Sequence[Mapping[str, Any]]) -> Optional[Mapping[str, Any]]:
    candidates = [
        row
        for row in summaries
        if row.get("fit_status") == "ok"
        and _safe_float(row.get("max_abs_relative_error_pct")) is not None
        and _safe_float(row.get("condition_number")) is not None
    ]
    if not candidates:
        return None

    def key(row: Mapping[str, Any]) -> Tuple[float, float, int, float]:
        max_rel = float(_safe_float(row.get("max_abs_relative_error_pct")) or math.inf)
        max_abs = float(_safe_float(row.get("max_abs_error")) or math.inf)
        term_count = len(str(row.get("model_terms") or "").split(";"))
        condition = float(_safe_float(row.get("condition_number")) or math.inf)
        return (max_rel, max_abs, term_count, condition)

    return min(candidates, key=key)


def _report_markdown(
    *,
    component: str,
    model_rows: Sequence[Mapping[str, Any]],
    trim_rows: Sequence[Mapping[str, Any]],
    output_dir: Path,
) -> str:
    recommended = [row for row in model_rows if str(row.get("recommended_model") or "") == "true"]
    lines = [
        f"# V1.5 {component.upper()} 候选模型选择与 S5/S6 离线评审",
        "",
        f"- 生成时间：{_now()}",
        "- 性质：离线 no-write 评审；不打开 COM，不写 SENCO，不控制气路/水路。",
        "- 物理边界：开放流通主校准仍不拟合压力项；压力通道由独立压力校准处理。",
        "- 复验顺序建议：先写主系数并在 S5/S6 中性状态下复验，再决定是否写 S5/S6。",
        "",
        "## 推荐主模型（不含 S5/S6）",
        "",
        "| 组分 | 设备ID | 推荐模型 | 最大绝对误差 | 最大相对误差 | 零点最大绝对误差 | 备注 |",
        "|---|---:|---|---:|---:|---:|---|",
    ]
    for row in recommended:
        note = str(row.get("review_note") or "")
        lines.append(
            "| {component} | {device} | {model} | {max_abs} | {max_rel} | {zero_abs} | {note} |".format(
                component=row.get("component", ""),
                device=row.get("analyzer_device_id", ""),
                model=row.get("model_name", ""),
                max_abs=row.get("max_abs_error", ""),
                max_rel=row.get("max_abs_relative_error_pct", ""),
                zero_abs=row.get("zero_max_abs_error", ""),
                note=note,
            )
        )
    lines.extend(
        [
            "",
            "## S5/S6 输出层修正评估",
            "",
            "| 组分 | 设备ID | 方法 | C0 | C1 | 最大绝对误差 | 最大相对误差 | 零点最大绝对误差 |",
            "|---|---:|---|---:|---:|---:|---:|---:|",
        ]
    )
    for row in trim_rows:
        if row.get("trim_method") not in {"neutral_no_s5_s6", "minimax_relative_rounded_3dp"}:
            continue
        lines.append(
            "| {component} | {device} | {method} | {c0} | {c1} | {max_abs} | {max_rel} | {zero_abs} |".format(
                component=row.get("component", ""),
                device=row.get("analyzer_device_id", ""),
                method=row.get("trim_method", ""),
                c0=row.get("c0", ""),
                c1=row.get("c1", ""),
                max_abs=row.get("max_abs_error", ""),
                max_rel=row.get("max_abs_relative_error_pct", ""),
                zero_abs=row.get("zero_max_abs_error", ""),
            )
        )
    lines.extend(
        [
            "",
            "## 判断",
            "",
            "- 不写 S5/S6 时，如果主模型最大相对误差已经满足目标，优先保持 S5/S6 中性，避免输出层补偿掩盖主链路问题。",
            "- 如果主模型误差呈稳定线性偏差，S5/S6 可作为最后一层显示修正；但写入前仍需先完成主系数写入后的独立复验。",
            "- 073/079 这类异常设备不能通过 S5/S6 强行拉平，应先查光路、参考信号、ratio 饱和、状态寄存器和固件输出链。",
            "",
            "## 输出文件",
            "",
            f"- 模型比较：`{(output_dir / 'model_selection_summary.csv').as_posix()}`",
            f"- S5/S6 评估：`{(output_dir / 'linear_trim_review.csv').as_posix()}`",
        ]
    )
    return "\n".join(lines) + "\n"


def build_review(
    *,
    candidate_dir: Path,
    output_dir: Path,
    component: str | None = None,
) -> Dict[str, Any]:
    component_key = _component_from_candidate_dir(candidate_dir, component)
    groups = _load_point_rows(candidate_dir, component_key)
    policies = _policy_by_device(candidate_dir)
    model_rows: List[Dict[str, Any]] = []
    trim_review_rows: List[Dict[str, Any]] = []
    residual_rows: List[Dict[str, Any]] = []
    for (prefix, device_id), rows in sorted(groups.items()):
        per_device_model_rows: List[Dict[str, Any]] = []
        predictions_by_model: Dict[str, np.ndarray] = {}
        policy = policies.get((prefix, device_id), {})
        for model_name, terms in MODEL_FAMILIES:
            summary, coefficients, prediction = _evaluate_model(
                component=component_key,
                rows=rows,
                model_name=model_name,
                terms=terms,
            )
            summary.update(
                {
                    "component": component_key,
                    "analyzer_prefix": prefix,
                    "analyzer_device_id": device_id,
                    "point_count": len(rows),
                    "candidate_status_before_model_selection": policy.get("candidate_status", ""),
                    "factory_signal_health_gate": policy.get("factory_signal_health_gate", ""),
                    "candidate_final_review_blockers": policy.get("final_review_blockers", ""),
                    "recommended_model": "false",
                    "review_note": "",
                }
            )
            per_device_model_rows.append(summary)
            if prediction is not None:
                predictions_by_model[model_name] = prediction
                truth = np.asarray([float(row["_target"]) for row in rows], dtype=float)
                for sample_row, pred, target in zip(rows, prediction, truth):
                    error = float(pred - target)
                    rel = "" if abs(float(target)) <= 1.0e-12 else abs(error / abs(float(target)) * 100.0)
                    residual_rows.append(
                        {
                            "component": component_key,
                            "analyzer_prefix": prefix,
                            "analyzer_device_id": device_id,
                            "model_name": model_name,
                            "point_identity": sample_row.get("_point_identity", ""),
                            "target_value": target,
                            "ratio": sample_row.get("_ratio", ""),
                            "temperature_c": sample_row.get("_temperature_c", ""),
                            "pressure_hpa": sample_row.get("_pressure_hpa", ""),
                            "prediction": float(pred),
                            "error": error,
                            "absolute_relative_error_pct": rel,
                        }
                    )
        best = _best_model(rows, per_device_model_rows)
        if best is not None:
            for item in per_device_model_rows:
                if item["model_name"] == best["model_name"]:
                    item["recommended_model"] = "true"
                    max_rel = _safe_float(item.get("max_abs_relative_error_pct"))
                    gate = str(item.get("factory_signal_health_gate") or "")
                    if gate and gate != "pass_factory_signal_health":
                        item["review_note"] = f"blocked_by_factory_signal_health:{gate}"
                    elif max_rel is not None and max_rel > 10.0:
                        item["review_note"] = "large_residual_requires_root_cause_review"
                    else:
                        item["review_note"] = "main_model_candidate_for_senco1234_review"
                    prediction = predictions_by_model.get(item["model_name"])
                    if prediction is not None:
                        truth = np.asarray([float(row["_target"]) for row in rows], dtype=float)
                        trim_review_rows.extend(
                            _trim_rows(
                                component=component_key,
                                prefix=prefix,
                                device_id=device_id,
                                model_summary=item,
                                prediction=prediction,
                                target=truth,
                            )
                        )
        model_rows.extend(per_device_model_rows)

    output_dir.mkdir(parents=True, exist_ok=True)
    _write_csv(output_dir / "model_selection_summary.csv", model_rows)
    _write_csv(output_dir / "model_selection_residuals.csv", residual_rows)
    _write_csv(output_dir / "linear_trim_review.csv", trim_review_rows)
    meta = {
        "component": component_key,
        "generated_at": _now(),
        "candidate_dir": str(candidate_dir.resolve()),
        "output_dir": str(output_dir.resolve()),
        "model_families": [{"name": name, "terms": list(terms)} for name, terms in MODEL_FAMILIES],
        "no_write": True,
        "opens_com": False,
        "writes_senco": False,
        "physical_contract": "current_atmosphere_open_flow_no_pressure_terms",
    }
    (output_dir / "model_selection_meta.json").write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
    report = _report_markdown(
        component=component_key,
        model_rows=model_rows,
        trim_rows=trim_review_rows,
        output_dir=output_dir,
    )
    (output_dir / "model_selection_review_zh.md").write_text(report, encoding="utf-8")
    return meta


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Export offline V1.5 candidate model selection and S5/S6 trim review artifacts."
    )
    parser.add_argument("--candidate-dir", required=True, help="Directory containing candidate_fit_residuals.csv.")
    parser.add_argument("--output-dir", required=True, help="Output directory for model-selection artifacts.")
    parser.add_argument("--component", choices=["co2", "h2o"], help="Component override. Defaults from candidate run.")
    args = parser.parse_args(argv)
    try:
        meta = build_review(
            candidate_dir=Path(args.candidate_dir),
            output_dir=Path(args.output_dir),
            component=args.component,
        )
    except Exception as exc:
        print(f"V1.5 candidate model selection review failed: {exc}", file=sys.stderr, flush=True)
        return 2
    print(json.dumps(meta, ensure_ascii=False, indent=2), flush=True)
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
