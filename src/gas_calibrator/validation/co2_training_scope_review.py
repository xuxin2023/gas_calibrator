"""Offline CO2 training-scope review for V1.5 open-flow evidence.

This review prevents a common fitting mistake: treating the historical
``fit`` subset as if it were the complete sampled gas-point matrix.  It is
offline-only evidence.  It never opens COM ports, controls routes, or writes
SENCO coefficients.
"""

from __future__ import annotations

import csv
import json
import math
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

import numpy as np


@dataclass(frozen=True)
class ScopePoint:
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
        if token.startswith("m"):
            return -float(token[1:])
        return float(token)


@dataclass(frozen=True)
class ModelSpec:
    model_id: str
    terms: Tuple[str, ...]
    write_scope: str


@dataclass(frozen=True)
class TrainingScope:
    scope_id: str
    description: str
    physical_meaning: str


MODEL_SPECS: Tuple[ModelSpec, ...] = (
    ModelSpec("ratio_cubic", ("1", "R", "R2", "R3"), "diagnostic_senco1_shape_only"),
    ModelSpec(
        "ratio_temperature_no_pressure",
        ("1", "R", "R2", "R3", "T", "T2", "RT"),
        "candidate_senco13_current_atmosphere_pressure_frozen",
    ),
    ModelSpec(
        "ratio_temperature_h2o_no_pressure",
        ("1", "R", "R2", "R3", "T", "T2", "RT", "H", "RH"),
        "diagnostic_h2o_cross_sensitivity_not_direct_senco13_contract",
    ),
)

TRAINING_SCOPES: Tuple[TrainingScope, ...] = (
    TrainingScope(
        "fit_only_previous_candidate_subset",
        "Use only rows marked fit, matching the previous candidate subset.",
        "This keeps historical holdout separation but does not represent the complete gas matrix.",
    ),
    TrainingScope(
        "all_sampled_points",
        "Use all sampled old full-temperature CO2 points.",
        "All certified/open-flow gas points constrain the fitted surface; holdout evidence must be external.",
    ),
    TrainingScope(
        "central_full_grid_T10_T20_T30",
        "Use central temperature groups that each contain the full 11-gas-point sweep.",
        "This is the most balanced R/T grid; endpoint groups remain diagnostic anchors.",
    ),
    TrainingScope(
        "all_nonzero_points",
        "Use all nonzero old full-temperature points.",
        "Sensitivity check for low-end/zero-anchor influence; not a default release contract.",
    ),
)


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _safe_float(value: Any) -> Optional[float]:
    if value in (None, "", "None", "null"):
        return None
    try:
        number = float(value)
    except Exception:
        return None
    return number if math.isfinite(number) else None


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


def _load_points(points_csv: str | Path, *, target_device_id: str) -> List[ScopePoint]:
    target_id = _device_id(target_device_id)
    points: List[ScopePoint] = []
    for row in _read_csv(points_csv):
        device_id = _device_id(row.get("device_id") or row.get("analyzer_device_id"))
        if target_id and device_id != target_id:
            continue
        target = _safe_float(row.get("target_ppm") or row.get("target_value") or row.get("certificate_co2_ppm"))
        ratio = _safe_float(row.get("ratio") or row.get("co2_ratio_f_mean") or row.get("R_CO2"))
        temp = _safe_float(row.get("temperature_c") or row.get("chamber_temp_mean_c") or row.get("T1"))
        pressure = _safe_float(row.get("pressure_hpa") or row.get("pressure_gauge_hpa"))
        h2o = _safe_float(row.get("h2o_mmol_mol") or row.get("h2o_mmol") or row.get("h2o_mmol_mean"))
        if pressure is None:
            kpa = _safe_float(row.get("pressure_kpa") or row.get("BAR"))
            pressure = kpa * 10.0 if kpa is not None else None
        if None in (target, ratio, temp, pressure, h2o):
            continue
        points.append(
            ScopePoint(
                source_set=str(row.get("source_set") or row.get("evidence_set") or "").strip(),
                point_identity=str(row.get("point_identity") or row.get("sample_index") or "").strip(),
                device_id=device_id,
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


def _terms(point: ScopePoint, spec: ModelSpec) -> List[float]:
    r = point.ratio
    t = point.temperature_c + 273.15
    h = point.h2o_mmol_mol
    values = {
        "1": 1.0,
        "R": r,
        "R2": r * r,
        "R3": r**3,
        "T": t,
        "T2": t * t,
        "RT": r * t,
        "H": h,
        "RH": r * h,
    }
    return [values[term] for term in spec.terms]


def _scope_points(points: Sequence[ScopePoint], scope_id: str) -> List[ScopePoint]:
    if scope_id == "fit_only_previous_candidate_subset":
        return [point for point in points if point.role == "fit"]
    if scope_id == "all_sampled_points":
        return list(points)
    if scope_id == "central_full_grid_T10_T20_T30":
        return [point for point in points if point.temp_group_c in {10.0, 20.0, 30.0}]
    if scope_id == "all_nonzero_points":
        return [point for point in points if abs(point.target_ppm) > 1.0e-9]
    return []


def _fit(train: Sequence[ScopePoint], spec: ModelSpec) -> Tuple[np.ndarray, float]:
    x = np.asarray([_terms(point, spec) for point in train], dtype=float)
    y = np.asarray([point.target_ppm for point in train], dtype=float)
    coeffs, *_ = np.linalg.lstsq(x, y, rcond=None)
    condition = float(np.linalg.cond(x)) if len(train) >= len(spec.terms) else float("inf")
    return coeffs, condition


def _relative_error_pct(predicted: float, target: float) -> Optional[float]:
    if abs(target) <= 1.0e-9:
        return None
    return 100.0 * (predicted - target) / target


def _metrics(errors: Sequence[float], relative_errors: Sequence[float], zero_errors: Sequence[float]) -> Dict[str, Any]:
    return {
        "count": len(errors),
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


def _format_number(value: Any) -> Any:
    if isinstance(value, float):
        return f"{value:.6f}"
    return value


def _target_distribution(points: Sequence[ScopePoint]) -> Dict[str, int]:
    out: Dict[str, int] = {}
    for point in points:
        key = f"{point.target_ppm:g}"
        out[key] = out.get(key, 0) + 1
    return dict(sorted(out.items(), key=lambda item: float(item[0])))


def _temperature_matrix(points: Sequence[ScopePoint]) -> List[Dict[str, Any]]:
    groups: Dict[float, List[ScopePoint]] = {}
    for point in points:
        if point.temp_group_c is None:
            continue
        groups.setdefault(point.temp_group_c, []).append(point)
    rows: List[Dict[str, Any]] = []
    for temp in sorted(groups):
        group = groups[temp]
        targets = sorted({point.target_ppm for point in group})
        fit_targets = sorted({point.target_ppm for point in group if point.role == "fit"})
        verification_targets = sorted({point.target_ppm for point in group if point.role == "verification"})
        rows.append(
            {
                "temperature_group_c": f"{temp:g}",
                "target_count": len(targets),
                "targets": ",".join(f"{target:g}" for target in targets),
                "fit_targets": ",".join(f"{target:g}" for target in fit_targets),
                "verification_targets": ",".join(f"{target:g}" for target in verification_targets),
            }
        )
    return rows


def build_co2_training_scope_review_tables(
    *,
    points_csv: str | Path,
    target_device_id: str,
    old_source_set: str = "old_fulltemp_prewrite",
) -> Dict[str, List[Dict[str, Any]]]:
    all_points = _load_points(points_csv, target_device_id=target_device_id)
    old_points = [point for point in all_points if point.source_set == old_source_set]
    current_points = [point for point in all_points if point.source_set != old_source_set]

    matrix_rows = _temperature_matrix(old_points)
    summary_rows: List[Dict[str, Any]] = []
    prediction_rows: List[Dict[str, Any]] = []
    coefficient_rows: List[Dict[str, Any]] = []

    eval_sets = {
        "old_fit": [point for point in old_points if point.role == "fit"],
        "old_verification": [point for point in old_points if point.role == "verification"],
        "old_all": old_points,
        "current_bridge": current_points,
    }

    for scope in TRAINING_SCOPES:
        train_points = _scope_points(old_points, scope.scope_id)
        for spec in MODEL_SPECS:
            if len(train_points) < len(spec.terms):
                continue
            coeffs, condition = _fit(train_points, spec)
            for term, coeff in zip(spec.terms, coeffs):
                coefficient_rows.append(
                    {
                        "training_scope": scope.scope_id,
                        "model_id": spec.model_id,
                        "term": term,
                        "coefficient": f"{float(coeff):.12g}",
                    }
                )
            for eval_id, eval_points in eval_sets.items():
                errors: List[float] = []
                relative_errors: List[float] = []
                zero_errors: List[float] = []
                for point in eval_points:
                    predicted = float(np.asarray(_terms(point, spec), dtype=float) @ coeffs)
                    error = predicted - point.target_ppm
                    relative_error = _relative_error_pct(predicted, point.target_ppm)
                    errors.append(error)
                    if relative_error is None:
                        zero_errors.append(error)
                    else:
                        relative_errors.append(relative_error)
                    prediction_rows.append(
                        {
                            "training_scope": scope.scope_id,
                            "model_id": spec.model_id,
                            "eval_set": eval_id,
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
                row = {
                    "training_scope": scope.scope_id,
                    "model_id": spec.model_id,
                    "write_scope": spec.write_scope,
                    "eval_set": eval_id,
                    "train_count": len(train_points),
                    "term_count": len(spec.terms),
                    "condition_number": condition,
                    "training_scope_description": scope.description,
                    "physical_meaning": scope.physical_meaning,
                }
                row.update(_metrics(errors, relative_errors, zero_errors))
                summary_rows.append(row)

    return {
        "co2_training_scope_temperature_matrix": matrix_rows,
        "co2_training_scope_summary": summary_rows,
        "co2_training_scope_predictions": prediction_rows,
        "co2_training_scope_coefficients": coefficient_rows,
        "co2_training_scope_manifest_rows": [
            {
                "target_device_id": _device_id(target_device_id),
                "old_source_set": old_source_set,
                "old_point_count": len(old_points),
                "current_point_count": len(current_points),
                "old_target_distribution": json.dumps(_target_distribution(old_points), ensure_ascii=False, separators=(",", ":")),
                "boundary": "offline_no_com_no_senco_write_no_route_control",
            }
        ],
    }


def write_co2_training_scope_review(
    *,
    points_csv: str | Path,
    output_dir: str | Path,
    target_device_id: str,
    old_source_set: str = "old_fulltemp_prewrite",
) -> Dict[str, Path]:
    destination = Path(output_dir).resolve()
    destination.mkdir(parents=True, exist_ok=True)
    tables = build_co2_training_scope_review_tables(
        points_csv=points_csv,
        target_device_id=target_device_id,
        old_source_set=old_source_set,
    )
    outputs: Dict[str, Path] = {}
    for table_name, rows in tables.items():
        path = destination / f"{table_name}.csv"
        _write_csv(path, [{key: _format_number(value) for key, value in row.items()} for row in rows])
        outputs[f"{table_name}_csv"] = path

    manifest = {
        "tool_name": "export_v1_5_co2_training_scope_review",
        "created_at": _now(),
        "points_csv": str(Path(points_csv).resolve()),
        "target_device_id": _device_id(target_device_id),
        "old_source_set": old_source_set,
        "boundary": {
            "opens_com_ports": False,
            "controls_water_or_gas_routes": False,
            "writes_coefficients": False,
        },
    }
    manifest_path = destination / "co2_training_scope_review_manifest.json"
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    outputs["manifest_json"] = manifest_path

    markdown_path = destination / "co2_training_scope_review.md"
    markdown_path.write_text(
        _markdown(tables, target_device_id=_device_id(target_device_id)),
        encoding="utf-8",
    )
    outputs["markdown"] = markdown_path
    return outputs


def _markdown(tables: Mapping[str, Sequence[Mapping[str, Any]]], *, target_device_id: str) -> str:
    matrix_rows = tables["co2_training_scope_temperature_matrix"]
    summary_rows = [
        row
        for row in tables["co2_training_scope_summary"]
        if row["eval_set"] == "current_bridge"
        and row["model_id"] in {"ratio_cubic", "ratio_temperature_no_pressure"}
    ]
    summary_rows = sorted(
        summary_rows,
        key=lambda row: float(row["max_abs_relative_error_pct"] or 1.0e12),
    )
    lines = [
        f"# ID{target_device_id} CO2 训练口径审计",
        "",
        "## 结论",
        "",
        "- 旧全温度 CO2 采样必须按全量证据理解，不能把 `fit` 子集误当作全量气点。",
        "- `verification` 行仍然是开放流通标准气采样证据；是否参与拟合是拟合合同选择，不是采样是否存在的问题。",
        "- 当前报告仅做离线评审，不打开 COM、不写 SENCO、不控制气路或水路。",
        "",
        "## 温度-气点矩阵",
        "",
        "| 温度组 | 气点数量 | 气点 | fit 子集 | verification 子集 |",
        "|---:|---:|---|---|---|",
    ]
    for row in matrix_rows:
        lines.append(
            "| {temperature_group_c} | {target_count} | {targets} | {fit_targets} | {verification_targets} |".format(
                **row
            )
        )
    lines.extend(
        [
            "",
            "## 当前复验桥接误差节选",
            "",
            "| 训练口径 | 模型 | 训练点数 | 当前最大相对误差 | 当前平均相对误差 |",
            "|---|---|---:|---:|---:|",
        ]
    )
    for row in summary_rows:
        lines.append(
            "| {training_scope} | {model_id} | {train_count} | {max_abs_relative_error_pct}% | {mean_abs_relative_error_pct}% |".format(
                **{key: _format_number(value) for key, value in row.items()}
            )
        )
    return "\n".join(lines) + "\n"
