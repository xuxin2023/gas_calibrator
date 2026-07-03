"""Offline SENCO1/SENCO3 candidate review across CO2 training scopes.

The review is deliberately narrow: it compares how different historical
training scopes affect the CO2 main optical/temperature chain.  Pressure terms
are frozen to zero because V1.5 handles pressure through the independent
SENCO9 pressure-channel workflow.

This module is offline-only.  It never opens COM ports, controls gas/water
routes, or writes coefficients.
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

import numpy as np

from ..senco_format import format_senco_values, rounded_senco_values


TERMS: Tuple[str, ...] = ("intercept", "R", "R2", "R3", "T", "T2", "RT")


@dataclass(frozen=True)
class Co2ScopePoint:
    source_set: str
    point_identity: str
    device_id: str
    role: str
    target_ppm: float
    ratio: float
    temperature_c: float
    pressure_hpa: float
    h2o_mmol_mol: float
    sample_count: int
    usable_count: int

    @property
    def temp_group_c(self) -> Optional[float]:
        match = re.search(r"_T(m?\d+(?:\.\d+)?)_", self.point_identity)
        if not match:
            return None
        token = match.group(1)
        return -float(token[1:]) if token.startswith("m") else float(token)


@dataclass(frozen=True)
class CandidateScope:
    scope_id: str
    description: str
    physical_meaning: str


CANDIDATE_SCOPES: Tuple[CandidateScope, ...] = (
    CandidateScope(
        "fit_only_previous_candidate_subset",
        "Use only rows marked fit.",
        "Historical candidate subset. Keeps old fit/verification split but ignores sampled even targets.",
    ),
    CandidateScope(
        "all_sampled_points",
        "Use all sampled old full-temperature CO2 points.",
        "All standard-gas open-flow evidence constrains the main R/T surface; verification must be external.",
    ),
    CandidateScope(
        "central_full_grid_T10_T20_T30",
        "Use T10/T20/T30, where each temperature contains all 11 gas targets.",
        "Balanced full gas grid; endpoint 0/400/1000 groups remain diagnostic anchors.",
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
        writer.writerows([dict(row) for row in rows])


def _load_points(points_csv: str | Path, *, target_device_id: str) -> List[Co2ScopePoint]:
    target_id = _device_id(target_device_id)
    points: List[Co2ScopePoint] = []
    for row in _read_csv(points_csv):
        device = _device_id(row.get("device_id") or row.get("analyzer_device_id"))
        if device != target_id:
            continue
        target = _safe_float(row.get("target_ppm") or row.get("target_value") or row.get("certificate_co2_ppm"))
        ratio = _safe_float(row.get("ratio") or row.get("co2_ratio_f_mean") or row.get("R_CO2"))
        temp = _safe_float(row.get("temperature_c") or row.get("chamber_temp_mean_c") or row.get("T1"))
        pressure = _safe_float(row.get("pressure_hpa") or row.get("pressure_gauge_hpa"))
        if pressure is None:
            kpa = _safe_float(row.get("pressure_kpa") or row.get("BAR"))
            pressure = kpa * 10.0 if kpa is not None else None
        h2o = _safe_float(row.get("h2o_mmol_mol") or row.get("h2o_mmol") or row.get("h2o_mmol_mean"))
        if None in (target, ratio, temp, pressure, h2o):
            continue
        points.append(
            Co2ScopePoint(
                source_set=str(row.get("source_set") or row.get("evidence_set") or "").strip(),
                point_identity=str(row.get("point_identity") or row.get("sample_index") or "").strip(),
                device_id=device,
                role=str(row.get("role") or row.get("source_role") or row.get("sample_role") or "fit").strip().lower(),
                target_ppm=float(target),
                ratio=float(ratio),
                temperature_c=float(temp),
                pressure_hpa=float(pressure),
                h2o_mmol_mol=float(h2o),
                sample_count=int(float(row.get("sample_count") or 0)),
                usable_count=int(float(row.get("usable_count") or 0)),
            )
        )
    return points


def _scope_points(points: Sequence[Co2ScopePoint], scope_id: str) -> List[Co2ScopePoint]:
    if scope_id == "fit_only_previous_candidate_subset":
        return [point for point in points if point.role == "fit"]
    if scope_id == "all_sampled_points":
        return list(points)
    if scope_id == "central_full_grid_T10_T20_T30":
        return [point for point in points if point.temp_group_c in {10.0, 20.0, 30.0}]
    return []


def _feature(point: Co2ScopePoint) -> np.ndarray:
    r = point.ratio
    t = point.temperature_c + 273.15
    return np.asarray([1.0, r, r * r, r**3, t, t * t, r * t], dtype=float)


def _centered_matrix(points: Sequence[Co2ScopePoint], *, ratio_center: float, temp_center_k: float) -> np.ndarray:
    rows: List[List[float]] = []
    for point in points:
        rd = point.ratio - ratio_center
        td = point.temperature_c + 273.15 - temp_center_k
        rows.append([1.0, rd, rd * rd, rd**3, td, td * td, rd * td])
    return np.asarray(rows, dtype=float)


def _centered_to_absolute(coefficients: Sequence[float], *, ratio_center: float, temp_center_k: float) -> np.ndarray:
    b0, b1, b2, b3, bt, bt2, brt = [float(value) for value in coefficients]
    r0 = float(ratio_center)
    t0 = float(temp_center_k)
    return np.asarray(
        [
            b0 - r0 * b1 + (r0**2) * b2 - (r0**3) * b3 - t0 * bt + (t0**2) * bt2 + r0 * t0 * brt,
            b1 - 2.0 * r0 * b2 + 3.0 * (r0**2) * b3 - t0 * brt,
            b2 - 3.0 * r0 * b3,
            b3,
            bt - 2.0 * t0 * bt2 - r0 * brt,
            bt2,
            brt,
        ],
        dtype=float,
    )


def _scaled_lstsq(matrix: np.ndarray, target: np.ndarray) -> Tuple[np.ndarray, int, float]:
    scales = np.linalg.norm(matrix, axis=0)
    scales = np.where(np.isfinite(scales) & (scales > 0.0), scales, 1.0)
    scaled = matrix / scales
    rank = int(np.linalg.matrix_rank(scaled))
    condition = float(np.linalg.cond(scaled))
    fitted, *_ = np.linalg.lstsq(scaled, target, rcond=None)
    return np.asarray(fitted, dtype=float) / scales, rank, condition


def _fit_senco13(points: Sequence[Co2ScopePoint]) -> Tuple[np.ndarray, int, float]:
    ratio_center = float(np.mean([point.ratio for point in points]))
    temp_center = float(np.mean([point.temperature_c + 273.15 for point in points]))
    centered = _centered_matrix(points, ratio_center=ratio_center, temp_center_k=temp_center)
    centered_coeffs, rank, condition = _scaled_lstsq(
        centered,
        np.asarray([point.target_ppm for point in points], dtype=float),
    )
    absolute = _centered_to_absolute(centered_coeffs, ratio_center=ratio_center, temp_center_k=temp_center)
    return absolute, rank, condition


def _relative_error_pct(predicted: float, target: float) -> Optional[float]:
    if abs(target) <= 1.0e-9:
        return None
    return 100.0 * (predicted - target) / target


def _metrics(errors: Sequence[float], relative_errors: Sequence[float], zero_errors: Sequence[float]) -> Dict[str, Any]:
    return {
        "count": len(errors),
        "rmse_ppm": float(np.sqrt(np.mean(np.asarray(errors, dtype=float) ** 2))) if errors else "",
        "max_abs_error_ppm": max((abs(item) for item in errors), default=""),
        "mean_abs_error_ppm": (sum(abs(item) for item in errors) / len(errors)) if errors else "",
        "max_abs_relative_error_pct": max((abs(item) for item in relative_errors), default=""),
        "mean_abs_relative_error_pct": (
            sum(abs(item) for item in relative_errors) / len(relative_errors)
            if relative_errors
            else ""
        ),
        "max_zero_abs_error_ppm": max((abs(item) for item in zero_errors), default=""),
    }


def _eval(
    *,
    points: Sequence[Co2ScopePoint],
    coeffs: Sequence[float],
    scope_id: str,
    eval_set: str,
    rounded: bool,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    coeff_array = np.asarray(coeffs, dtype=float)
    errors: List[float] = []
    relative_errors: List[float] = []
    zero_errors: List[float] = []
    rows: List[Dict[str, Any]] = []
    for point in points:
        predicted = float(_feature(point) @ coeff_array)
        error = predicted - point.target_ppm
        relative_error = _relative_error_pct(predicted, point.target_ppm)
        errors.append(error)
        if relative_error is None:
            zero_errors.append(error)
        else:
            relative_errors.append(relative_error)
        rows.append(
            {
                "training_scope": scope_id,
                "eval_set": eval_set,
                "rounded_payload": rounded,
                "source_set": point.source_set,
                "point_identity": point.point_identity,
                "role": point.role,
                "target_ppm": point.target_ppm,
                "predicted_ppm": predicted,
                "error_ppm": error,
                "relative_error_pct": "" if relative_error is None else relative_error,
                "ratio": point.ratio,
                "temperature_c": point.temperature_c,
                "pressure_hpa": point.pressure_hpa,
                "h2o_mmol_mol": point.h2o_mmol_mol,
            }
        )
    return rows, _metrics(errors, relative_errors, zero_errors)


def _target_distribution(points: Sequence[Co2ScopePoint]) -> Dict[str, int]:
    out: Dict[str, int] = {}
    for point in points:
        key = f"{point.target_ppm:g}"
        out[key] = out.get(key, 0) + 1
    return dict(sorted(out.items(), key=lambda item: float(item[0])))


def _format_number(value: Any) -> Any:
    if isinstance(value, float):
        return f"{value:.6f}"
    return value


def build_co2_senco13_scope_candidate_review_tables(
    *,
    points_csv: str | Path,
    target_device_id: str,
    old_source_set: str = "old_fulltemp_prewrite",
) -> Dict[str, List[Dict[str, Any]]]:
    points = _load_points(points_csv, target_device_id=target_device_id)
    old_points = [point for point in points if point.source_set == old_source_set]
    current_points = [point for point in points if point.source_set != old_source_set]
    eval_sets = {
        "old_fit": [point for point in old_points if point.role == "fit"],
        "old_verification": [point for point in old_points if point.role == "verification"],
        "old_all": old_points,
        "current_bridge": current_points,
    }

    summary_rows: List[Dict[str, Any]] = []
    coefficient_rows: List[Dict[str, Any]] = []
    prediction_rows: List[Dict[str, Any]] = []

    for scope in CANDIDATE_SCOPES:
        train_points = _scope_points(old_points, scope.scope_id)
        if len(train_points) < len(TERMS):
            summary_rows.append(
                {
                    "device_id": _device_id(target_device_id),
                    "training_scope": scope.scope_id,
                    "status": "blocked_insufficient_training_points",
                    "train_count": len(train_points),
                    "physical_meaning": scope.physical_meaning,
                }
            )
            continue
        coeffs, rank, condition = _fit_senco13(train_points)
        rounded_coeffs = np.asarray(rounded_senco_values(coeffs), dtype=float)
        primary = [float(coeffs[0]), float(coeffs[1]), float(coeffs[2]), float(coeffs[3]), 0.0, 0.0]
        secondary = [float(coeffs[4]), float(coeffs[5]), float(coeffs[6]), 0.0, 0.0, 0.0]
        rounded_primary = [float(rounded_coeffs[0]), float(rounded_coeffs[1]), float(rounded_coeffs[2]), float(rounded_coeffs[3]), 0.0, 0.0]
        rounded_secondary = [float(rounded_coeffs[4]), float(rounded_coeffs[5]), float(rounded_coeffs[6]), 0.0, 0.0, 0.0]
        for term, coeff, rounded_coeff in zip(TERMS, coeffs, rounded_coeffs):
            coefficient_rows.append(
                {
                    "device_id": _device_id(target_device_id),
                    "training_scope": scope.scope_id,
                    "term": term,
                    "coefficient": float(coeff),
                    "rounded_coefficient": float(rounded_coeff),
                    "senco_group": "SENCO1" if term in {"intercept", "R", "R2", "R3"} else "SENCO3",
                }
            )
        for eval_set, eval_points in eval_sets.items():
            eval_rows, metric = _eval(
                points=eval_points,
                coeffs=coeffs,
                scope_id=scope.scope_id,
                eval_set=eval_set,
                rounded=False,
            )
            rounded_eval_rows, rounded_metric = _eval(
                points=eval_points,
                coeffs=rounded_coeffs,
                scope_id=scope.scope_id,
                eval_set=eval_set,
                rounded=True,
            )
            prediction_rows.extend(eval_rows)
            prediction_rows.extend(rounded_eval_rows)
            summary_rows.append(
                {
                    "device_id": _device_id(target_device_id),
                    "training_scope": scope.scope_id,
                    "eval_set": eval_set,
                    "status": "reviewable_no_write",
                    "train_count": len(train_points),
                    "eval_count": metric["count"],
                    "matrix_rank": rank,
                    "term_count": len(TERMS),
                    "condition_number_scaled": condition,
                    "target_distribution": json.dumps(_target_distribution(train_points), ensure_ascii=False, separators=(",", ":")),
                    "pressure_terms": "frozen_zero",
                    "senco1_payload_scientific": ",".join(format_senco_values(primary)),
                    "senco3_payload_scientific": ",".join(format_senco_values(secondary)),
                    "rounded_senco1_payload_json": json.dumps(rounded_primary, separators=(",", ":")),
                    "rounded_senco3_payload_json": json.dumps(rounded_secondary, separators=(",", ":")),
                    "rmse_ppm": metric["rmse_ppm"],
                    "max_abs_error_ppm": metric["max_abs_error_ppm"],
                    "mean_abs_error_ppm": metric["mean_abs_error_ppm"],
                    "max_abs_relative_error_pct": metric["max_abs_relative_error_pct"],
                    "mean_abs_relative_error_pct": metric["mean_abs_relative_error_pct"],
                    "max_zero_abs_error_ppm": metric["max_zero_abs_error_ppm"],
                    "rounded_rmse_ppm": rounded_metric["rmse_ppm"],
                    "rounded_max_abs_error_ppm": rounded_metric["max_abs_error_ppm"],
                    "rounded_max_abs_relative_error_pct": rounded_metric["max_abs_relative_error_pct"],
                    "training_scope_description": scope.description,
                    "physical_meaning": scope.physical_meaning,
                    "opens_com_ports": False,
                    "controls_water_or_gas_routes": False,
                    "writes_coefficients": False,
                }
            )

    return {
        "co2_senco13_scope_candidate_summary": summary_rows,
        "co2_senco13_scope_candidate_coefficients": coefficient_rows,
        "co2_senco13_scope_candidate_predictions": prediction_rows,
        "co2_senco13_scope_candidate_manifest_rows": [
            {
                "target_device_id": _device_id(target_device_id),
                "old_source_set": old_source_set,
                "old_point_count": len(old_points),
                "current_point_count": len(current_points),
                "old_target_distribution": json.dumps(_target_distribution(old_points), ensure_ascii=False, separators=(",", ":")),
                "pressure_terms": "frozen_zero_because_pressure_channel_is_independent_senco9_workflow",
                "boundary": "offline_no_com_no_senco_write_no_route_control",
            }
        ],
    }


def write_co2_senco13_scope_candidate_review(
    *,
    points_csv: str | Path,
    output_dir: str | Path,
    target_device_id: str,
    old_source_set: str = "old_fulltemp_prewrite",
) -> Dict[str, Path]:
    output = Path(output_dir).resolve()
    output.mkdir(parents=True, exist_ok=True)
    tables = build_co2_senco13_scope_candidate_review_tables(
        points_csv=points_csv,
        target_device_id=target_device_id,
        old_source_set=old_source_set,
    )
    outputs: Dict[str, Path] = {}
    for name, rows in tables.items():
        path = output / f"{name}.csv"
        _write_csv(path, [{key: _format_number(value) for key, value in row.items()} for row in rows])
        outputs[f"{name}_csv"] = path
    meta = {
        "tool_name": "export_v1_5_co2_senco13_scope_candidate_review",
        "created_at": _now(),
        "inputs": {
            "points_csv": str(Path(points_csv).resolve()),
            "target_device_id": _device_id(target_device_id),
            "old_source_set": old_source_set,
        },
        "boundary": {
            "opens_com_ports": False,
            "controls_water_or_gas_routes": False,
            "writes_coefficients": False,
        },
    }
    meta_path = output / "co2_senco13_scope_candidate_manifest.json"
    meta_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
    outputs["manifest_json"] = meta_path
    md_path = output / "co2_senco13_scope_candidate_review.md"
    md_path.write_text(_markdown(tables, target_device_id=_device_id(target_device_id)), encoding="utf-8")
    outputs["markdown"] = md_path
    return outputs


def _markdown(tables: Mapping[str, Sequence[Mapping[str, Any]]], *, target_device_id: str) -> str:
    current_rows = [
        row
        for row in tables["co2_senco13_scope_candidate_summary"]
        if row.get("eval_set") == "current_bridge"
    ]
    current_rows = sorted(
        current_rows,
        key=lambda row: float(row.get("rounded_max_abs_relative_error_pct") or row.get("max_abs_relative_error_pct") or 1.0e12),
    )
    old_all_rows = {
        row["training_scope"]: row
        for row in tables["co2_senco13_scope_candidate_summary"]
        if row.get("eval_set") == "old_all"
    }
    lines = [
        f"# ID{target_device_id} CO2 SENCO1/SENCO3 训练口径候选评审",
        "",
        "## 边界",
        "",
        "- 离线评审；不打开 COM、不控制气路/水路、不写 SENCO。",
        "- 正式 V1.5 当前大气开放流通 CO2 拟合不包含压力项；压力通道由 SENCO9 独立流程处理。",
        "- 本报告只比较 SENCO1/SENCO3 主链路训练口径；SENCO5 最终线性层另行评审。",
        "",
        "## 当前复验反推误差",
        "",
        "| 训练口径 | 训练点数 | 当前最大相对误差 | 当前最大绝对误差 | 旧全量最大绝对误差 | SENCO1 payload | SENCO3 payload |",
        "|---|---:|---:|---:|---:|---|---|",
    ]
    for row in current_rows:
        old_all = old_all_rows.get(row["training_scope"], {})
        lines.append(
            "| {scope} | {train_count} | {rel}% | {abs_err} ppm | {old_abs} ppm | `{s1}` | `{s3}` |".format(
                scope=row["training_scope"],
                train_count=row["train_count"],
                rel=_fmt(row.get("rounded_max_abs_relative_error_pct")),
                abs_err=_fmt(row.get("rounded_max_abs_error_ppm")),
                old_abs=_fmt(old_all.get("rounded_max_abs_error_ppm", "")),
                s1=row["senco1_payload_scientific"],
                s3=row["senco3_payload_scientific"],
            )
        )
    return "\n".join(lines) + "\n"


def _fmt(value: Any) -> str:
    numeric = _safe_float(value)
    if numeric is None:
        return ""
    return f"{numeric:.6f}"
