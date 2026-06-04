"""Offline CO2 state-normalization bridge review for V1.5.

The bridge answers a narrow question: can earlier open-flow CO2 evidence
recorded under one firmware/coefficient state explain a later verification
state if we compare only physical inputs (CO2 ratio, temperature, pressure,
and H2O), instead of comparing displayed CO2 output directly?

This module is no-COM and no-write. It is diagnostic evidence only.
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
class Co2BridgePoint:
    source_set: str
    point_identity: str
    analyzer_prefix: str
    device_id: str
    role: str
    target_ppm: float
    displayed_co2_ppm: Optional[float]
    ratio: float
    ratio_span: Optional[float]
    temperature_c: float
    pressure_hpa: float
    h2o_mmol_mol: float
    sample_count: int
    usable_count: int


@dataclass(frozen=True)
class BridgeModelVariant:
    model_id: str
    terms: Tuple[str, ...]
    write_scope: str


MODEL_VARIANTS: Tuple[BridgeModelVariant, ...] = (
    BridgeModelVariant(
        model_id="ratio_cubic",
        terms=("R", "R2", "R3"),
        write_scope="diagnostic_senco1_shape_only",
    ),
    BridgeModelVariant(
        model_id="ratio_temperature",
        terms=("R", "R2", "R3", "T", "T2", "RT"),
        write_scope="diagnostic_senco13_no_pressure",
    ),
    BridgeModelVariant(
        model_id="ratio_temperature_h2o",
        terms=("R", "R2", "R3", "T", "T2", "RT", "H", "RH"),
        write_scope="diagnostic_state_normalization_h2o_cross_sensitivity",
    ),
    BridgeModelVariant(
        model_id="ratio_temperature_h2o_pressure",
        terms=("R", "R2", "R3", "T", "T2", "RT", "H", "RH", "P", "RP"),
        write_scope="diagnostic_only_pressure_is_not_formal_v1_5_co2_fit_variable",
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


def _mean(values: Sequence[Optional[float]]) -> Optional[float]:
    clean = [value for value in values if value is not None and math.isfinite(value)]
    if not clean:
        return None
    return float(sum(clean) / len(clean))


def _span(values: Sequence[Optional[float]]) -> Optional[float]:
    clean = [value for value in values if value is not None and math.isfinite(value)]
    if not clean:
        return None
    return float(max(clean) - min(clean))


def _role_from_point_name(name: str) -> str:
    text = name.lower()
    if "verification" in text or "verify" in text:
        return "verification"
    if "diagnostic" in text:
        return "diagnostic"
    return "fit"


def _target_from_point_name(name: str) -> Optional[float]:
    match = re.search(r"_(\d+(?:\.\d+)?)ppm_", name)
    return float(match.group(1)) if match else None


def _prefixes_from_row(row: Mapping[str, Any]) -> List[str]:
    prefixes: List[str] = []
    for key in row:
        match = re.match(r"(ga\d+)_analyzer_device_id$", key or "")
        if match:
            prefixes.append(match.group(1))
    return sorted(prefixes)


def _point_from_samples(
    *,
    rows: Sequence[Mapping[str, Any]],
    point_identity: str,
    source_set: str,
    prefix: str,
    target_device_id: str,
) -> Optional[Co2BridgePoint]:
    matched = [
        row
        for row in rows
        if _device_id(row.get(f"{prefix}_analyzer_device_id")) == target_device_id
    ]
    if not matched:
        return None
    target = _mean([_safe_float(row.get("co2_ppm_target")) for row in matched])
    if target is None:
        target = _target_from_point_name(point_identity)
    displayed = _mean([_safe_float(row.get(f"{prefix}_co2_ppm")) for row in matched])
    ratio_values = [_safe_float(row.get(f"{prefix}_co2_ratio_f")) for row in matched]
    ratio = _mean(ratio_values)
    temperature = _mean([_safe_float(row.get(f"{prefix}_chamber_temp_c")) for row in matched])
    pressure = _mean([_safe_float(row.get("pressure_gauge_hpa") or row.get("pressure_hpa")) for row in matched])
    if pressure is None:
        pressure = _mean([_safe_float(row.get(f"{prefix}_pressure_kpa")) for row in matched])
        pressure = pressure * 10.0 if pressure is not None else None
    h2o = _mean([_safe_float(row.get(f"{prefix}_h2o_mmol")) for row in matched])
    usable = sum(1 for row in matched if str(row.get(f"{prefix}_frame_usable") or "").lower() == "true")
    if target is None or ratio is None or temperature is None or pressure is None or h2o is None:
        return None
    return Co2BridgePoint(
        source_set=source_set,
        point_identity=point_identity,
        analyzer_prefix=prefix.upper(),
        device_id=target_device_id,
        role=_role_from_point_name(point_identity),
        target_ppm=float(target),
        displayed_co2_ppm=displayed,
        ratio=float(ratio),
        ratio_span=_span(ratio_values),
        temperature_c=float(temperature),
        pressure_hpa=float(pressure),
        h2o_mmol_mol=float(h2o),
        sample_count=len(matched),
        usable_count=usable,
    )


def load_points_from_open_flow_run(
    run_dir: str | Path,
    *,
    target_device_id: str,
    source_set: str,
) -> List[Co2BridgePoint]:
    """Load per-point means from V1.5 open-flow sampling directories."""

    root = Path(run_dir)
    target_id = _device_id(target_device_id)
    points: List[Co2BridgePoint] = []
    for point_dir in sorted(path for path in root.glob("p*") if path.is_dir()):
        samples = point_dir / "samples_machine_readable.csv"
        if not samples.exists():
            continue
        rows = _read_csv(samples)
        if not rows:
            continue
        for prefix in _prefixes_from_row(rows[0]):
            point = _point_from_samples(
                rows=rows,
                point_identity=point_dir.name,
                source_set=source_set,
                prefix=prefix,
                target_device_id=target_id,
            )
            if point is not None:
                points.append(point)
    return points


def load_points_from_verification_summary(
    summary_csv: str | Path,
    *,
    target_device_id: str,
    source_set: str,
) -> List[Co2BridgePoint]:
    """Load point means from a post-run per-device verification summary."""

    target_id = _device_id(target_device_id)
    points: List[Co2BridgePoint] = []
    for row in _read_csv(summary_csv):
        if _device_id(row.get("device_id")) != target_id:
            continue
        target = _safe_float(row.get("certificate_co2_ppm") or row.get("source_nominal_ppm"))
        ratio = _safe_float(row.get("co2_ratio_f"))
        temperature = _safe_float(row.get("chamber_temp_c"))
        pressure = _safe_float(row.get("pressure_hpa"))
        h2o = _safe_float(row.get("h2o_mmol_mol"))
        if target is None or ratio is None or temperature is None or pressure is None or h2o is None:
            continue
        points.append(
            Co2BridgePoint(
                source_set=source_set,
                point_identity=str(row.get("point_run_id") or "").strip(),
                analyzer_prefix=str(row.get("analyzer_label") or "").strip(),
                device_id=target_id,
                role=_role_from_point_name(str(row.get("point_run_id") or "")),
                target_ppm=float(target),
                displayed_co2_ppm=_safe_float(row.get("measured_co2_ppm")),
                ratio=float(ratio),
                ratio_span=_safe_float(row.get("co2_ratio_f_dev")),
                temperature_c=float(temperature),
                pressure_hpa=float(pressure),
                h2o_mmol_mol=float(h2o),
                sample_count=int(_safe_float(row.get("total_frames")) or 0),
                usable_count=int(_safe_float(row.get("valid_frames")) or 0),
            )
        )
    return points


def _raw_feature(point: Co2BridgePoint, term: str) -> float:
    r = point.ratio
    t = point.temperature_c + 273.15
    p = point.pressure_hpa / 10.0
    h = point.h2o_mmol_mol
    if term == "R":
        return r
    if term == "R2":
        return r * r
    if term == "R3":
        return r**3
    if term == "T":
        return t
    if term == "T2":
        return t * t
    if term == "RT":
        return r * t
    if term == "P":
        return p
    if term == "RP":
        return r * p
    if term == "H":
        return h
    if term == "RH":
        return r * h
    raise KeyError(term)


def _feature_matrix(
    points: Sequence[Co2BridgePoint],
    terms: Sequence[str],
    *,
    centers: Optional[np.ndarray] = None,
    scales: Optional[np.ndarray] = None,
) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
    raw = np.asarray([[float(_raw_feature(point, term)) for term in terms] for point in points], dtype=float)
    if centers is None:
        centers = np.mean(raw, axis=0) if raw.size else np.asarray([], dtype=float)
    centered = raw - centers
    if scales is None:
        scales = np.std(centered, axis=0)
        scales = np.where(np.isfinite(scales) & (scales > 1.0e-12), scales, 1.0)
    normalized = centered / scales
    return np.column_stack([np.ones(len(points)), normalized]), centers, scales


def _fit_variant(points: Sequence[Co2BridgePoint], variant: BridgeModelVariant) -> Tuple[np.ndarray, int, float, np.ndarray, np.ndarray]:
    matrix, centers, scales = _feature_matrix(points, variant.terms)
    target = np.asarray([point.target_ppm for point in points], dtype=float)
    rank = int(np.linalg.matrix_rank(matrix))
    condition = float(np.linalg.cond(matrix)) if matrix.size else math.inf
    coefficients, *_ = np.linalg.lstsq(matrix, target, rcond=None)
    return np.asarray(coefficients, dtype=float), rank, condition, centers, scales


def _predict(
    points: Sequence[Co2BridgePoint],
    variant: BridgeModelVariant,
    coefficients: np.ndarray,
    centers: np.ndarray,
    scales: np.ndarray,
) -> np.ndarray:
    matrix, _, _ = _feature_matrix(points, variant.terms, centers=centers, scales=scales)
    return matrix @ coefficients


def _error_rows(
    points: Sequence[Co2BridgePoint],
    predictions: Sequence[float],
    *,
    model_id: str,
    train_source: str,
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for point, prediction in zip(points, predictions):
        error = float(prediction) - point.target_ppm
        error_pct = error / point.target_ppm * 100.0 if abs(point.target_ppm) > 1.0e-12 else None
        rows.append(
            {
                "model_id": model_id,
                "train_source": train_source,
                "source_set": point.source_set,
                "point_identity": point.point_identity,
                "role": point.role,
                "device_id": point.device_id,
                "analyzer_prefix": point.analyzer_prefix,
                "target_ppm": point.target_ppm,
                "displayed_co2_ppm": point.displayed_co2_ppm,
                "bridge_prediction_ppm": float(prediction),
                "bridge_error_ppm": error,
                "bridge_error_pct": error_pct,
                "co2_ratio_f": point.ratio,
                "co2_ratio_span": point.ratio_span,
                "chamber_temp_c": point.temperature_c,
                "pressure_hpa": point.pressure_hpa,
                "h2o_mmol_mol": point.h2o_mmol_mol,
                "sample_count": point.sample_count,
                "usable_count": point.usable_count,
            }
        )
    return rows


def _metrics(rows: Sequence[Mapping[str, Any]]) -> Dict[str, Optional[float]]:
    ppm_errors = [_safe_float(row.get("bridge_error_ppm")) for row in rows]
    ppm_values = [value for value in ppm_errors if value is not None]
    pct_errors = [_safe_float(row.get("bridge_error_pct")) for row in rows]
    pct_values = [value for value in pct_errors if value is not None]
    return {
        "point_count": float(len(rows)),
        "rmse_ppm": float(np.sqrt(np.mean(np.asarray(ppm_values) ** 2))) if ppm_values else None,
        "max_abs_error_ppm": max((abs(value) for value in ppm_values), default=None),
        "mean_error_ppm": float(np.mean(ppm_values)) if ppm_values else None,
        "max_abs_error_pct_nonzero": max((abs(value) for value in pct_values), default=None),
    }


def _best_model(summary_rows: Sequence[Mapping[str, Any]]) -> Optional[Mapping[str, Any]]:
    candidates = [
        row
        for row in summary_rows
        if str(row.get("eval_set")) == "current_bridge"
        and _safe_float(row.get("max_abs_error_pct_nonzero")) is not None
    ]
    if not candidates:
        return None
    return min(candidates, key=lambda row: float(row["max_abs_error_pct_nonzero"]))


def _bridge_status(best: Optional[Mapping[str, Any]], *, rel_limit_pct: float) -> Tuple[str, str]:
    if not best:
        return "insufficient_bridge_evidence", "No current bridge prediction rows were available."
    max_pct = _safe_float(best.get("max_abs_error_pct_nonzero"))
    model_id = str(best.get("model_id") or "")
    if max_pct is None:
        return "insufficient_bridge_evidence", "Current bridge rows contain no non-zero target for relative-error review."
    if max_pct <= rel_limit_pct:
        return (
            "bridge_explained_by_r_t_p_h2o",
            f"Best model {model_id} predicts current verification within {max_pct:.3f}% <= {rel_limit_pct:.3f}%.",
        )
    return (
        "bridge_not_explained_by_r_t_p_h2o",
        f"Best model {model_id} current max error {max_pct:.3f}% exceeds {rel_limit_pct:.3f}%.",
    )


def build_co2_state_bridge_tables(
    *,
    old_run_dir: str | Path,
    current_summary_csv: str | Path,
    target_device_id: str = "100",
    bridge_rel_limit_pct: float = 1.5,
) -> Dict[str, Any]:
    target_id = _device_id(target_device_id)
    old_points = load_points_from_open_flow_run(old_run_dir, target_device_id=target_id, source_set="old_fulltemp_prewrite")
    current_points = load_points_from_verification_summary(
        current_summary_csv,
        target_device_id=target_id,
        source_set="current_postwrite_freshgate",
    )
    old_fit = [point for point in old_points if point.role == "fit"]
    old_verification = [point for point in old_points if point.role == "verification"]
    prediction_rows: List[Dict[str, Any]] = []
    summary_rows: List[Dict[str, Any]] = []
    for variant in MODEL_VARIANTS:
        if len(old_fit) <= len(variant.terms) + 1:
            summary_rows.append(
                {
                    "model_id": variant.model_id,
                    "status": "skipped_insufficient_old_fit_points",
                    "term_count": len(variant.terms),
                    "old_fit_count": len(old_fit),
                    "write_scope": variant.write_scope,
                }
            )
            continue
        coeffs, rank, condition, centers, scales = _fit_variant(old_fit, variant)
        eval_sets = (
            ("old_fit", old_fit),
            ("old_verification", old_verification),
            ("current_bridge", current_points),
        )
        for eval_name, eval_points in eval_sets:
            if not eval_points:
                continue
            rows = _error_rows(
                eval_points,
                _predict(eval_points, variant, coeffs, centers, scales),
                model_id=variant.model_id,
                train_source="old_fit_points",
            )
            prediction_rows.extend(rows)
            metrics = _metrics(rows)
            summary_rows.append(
                {
                    "model_id": variant.model_id,
                    "eval_set": eval_name,
                    "status": "evaluated",
                    "term_count": len(variant.terms),
                    "rank": rank,
                    "condition_number": condition,
                    "old_fit_count": len(old_fit),
                    "write_scope": variant.write_scope,
                    **metrics,
                }
            )
    best = _best_model(summary_rows)
    bridge_status, bridge_reason = _bridge_status(best, rel_limit_pct=bridge_rel_limit_pct)
    manifest = {
        "schema": "v1_5_co2_state_bridge_v1",
        "generated_at": _now(),
        "target_device_id": target_id,
        "old_run_dir": str(old_run_dir),
        "current_summary_csv": str(current_summary_csv),
        "old_point_count": len(old_points),
        "old_fit_count": len(old_fit),
        "old_verification_count": len(old_verification),
        "current_point_count": len(current_points),
        "bridge_rel_limit_pct": bridge_rel_limit_pct,
        "bridge_status": bridge_status,
        "bridge_reason": bridge_reason,
        "best_current_bridge_model": dict(best) if best else {},
        "physical_contract": (
            "Displayed CO2 output may differ across coefficient states. Bridge review compares certificate CO2 "
            "against R/T/P/H2O state variables only; it does not authorize SENCO writes."
        ),
    }
    return {
        "manifest": manifest,
        "points": [point.__dict__ for point in old_points + current_points],
        "summary_rows": summary_rows,
        "prediction_rows": prediction_rows,
    }


def _markdown_report(tables: Mapping[str, Any]) -> str:
    manifest = dict(tables["manifest"])
    summary_rows = list(tables["summary_rows"])
    current_predictions = [
        row
        for row in tables["prediction_rows"]
        if str(row.get("source_set")) == "current_postwrite_freshgate"
    ]
    lines = [
        "# CO2 状态归一化桥接评估（ID100，no-write）",
        "",
        "## 结论",
        f"- 设备 ID：`{manifest['target_device_id']}`",
        f"- 桥接状态：`{manifest['bridge_status']}`",
        f"- 原因：{manifest['bridge_reason']}",
        "- 本报告只比较 `R_CO2 / T / P / H2O` 能否解释证书值，不比较旧/新最终显示 CO2 输出。",
        "- 本报告不打开 COM、不写 SENCO、不控制水路或气路。",
        "",
        "## 数据规模",
        f"- 旧全温度点：{manifest['old_point_count']}，其中 fit 点 {manifest['old_fit_count']}，verification 点 {manifest['old_verification_count']}",
        f"- 当前复验点：{manifest['current_point_count']}",
        "",
        "## 模型评估摘要",
        "| 模型 | 评估集 | 点数 | 最大相对误差% | RMSE ppm | 写入范围说明 |",
        "|---|---|---:|---:|---:|---|",
    ]
    for row in summary_rows:
        if row.get("status") != "evaluated":
            continue
        max_pct = row.get("max_abs_error_pct_nonzero")
        rmse = row.get("rmse_ppm")
        lines.append(
            "| {model} | {eval_set} | {count:.0f} | {pct} | {rmse} | {scope} |".format(
                model=row.get("model_id", ""),
                eval_set=row.get("eval_set", ""),
                count=float(row.get("point_count") or 0),
                pct="" if max_pct is None else f"{float(max_pct):.3f}",
                rmse="" if rmse is None else f"{float(rmse):.3f}",
                scope=row.get("write_scope", ""),
            )
        )
    lines.extend(
        [
            "",
            "## 当前复验点桥接预测",
            "| 模型 | 点位 | 目标 ppm | 显示 ppm | 桥接预测 ppm | 误差 ppm | 误差% | R_CO2 | H2O mmol/mol | T°C | P hPa |",
            "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
        ]
    )
    for row in current_predictions:
        pct = row.get("bridge_error_pct")
        lines.append(
            "| {model} | {point} | {target:.3f} | {displayed} | {pred:.3f} | {err:.3f} | {pct} | {ratio:.6f} | {h2o:.3f} | {temp:.3f} | {pressure:.3f} |".format(
                model=row.get("model_id", ""),
                point=row.get("point_identity", ""),
                target=float(row.get("target_ppm") or 0.0),
                displayed="" if row.get("displayed_co2_ppm") in (None, "") else f"{float(row['displayed_co2_ppm']):.3f}",
                pred=float(row.get("bridge_prediction_ppm") or 0.0),
                err=float(row.get("bridge_error_ppm") or 0.0),
                pct="" if pct in (None, "") else f"{float(pct):.3f}",
                ratio=float(row.get("co2_ratio_f") or 0.0),
                h2o=float(row.get("h2o_mmol_mol") or 0.0),
                temp=float(row.get("chamber_temp_c") or 0.0),
                pressure=float(row.get("pressure_hpa") or 0.0),
            )
        )
    lines.extend(
        [
            "",
            "## 物理解释",
            "- 旧全温度数据未写新 S1/S3，当前复验数据已写过 S1/S3，因此最终显示 CO2 不能直接跨批比较。",
            "- 如果旧全温度 fit 点训练出的 `R/T/P/H2O -> CO2证书值` 模型能准确预测当前复验点，说明底层光学状态可桥接，旧数据仍有复用价值。",
            "- 如果预测当前复验点明显超差，则说明两批数据的底层响应面也不一致，应冻结当前设备状态重新补足当前状态数据，而不是把旧数据混入正式候选。",
            "- 压力项在这里只作为诊断状态量；V1.5 当前大气压 CO2 主拟合仍不得把压力当作正式多压力补偿拟合变量。",
        ]
    )
    return "\n".join(lines) + "\n"


def write_co2_state_bridge_report(
    *,
    old_run_dir: str | Path,
    current_summary_csv: str | Path,
    output_dir: str | Path,
    target_device_id: str = "100",
    bridge_rel_limit_pct: float = 1.5,
) -> Dict[str, str]:
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    tables = build_co2_state_bridge_tables(
        old_run_dir=old_run_dir,
        current_summary_csv=current_summary_csv,
        target_device_id=target_device_id,
        bridge_rel_limit_pct=bridge_rel_limit_pct,
    )
    manifest_path = output / "co2_state_bridge_manifest.json"
    points_path = output / "co2_state_bridge_points.csv"
    summary_path = output / "co2_state_bridge_model_summary.csv"
    predictions_path = output / "co2_state_bridge_predictions.csv"
    markdown_path = output / "co2_state_bridge_review.md"
    manifest_path.write_text(json.dumps(tables["manifest"], ensure_ascii=False, indent=2), encoding="utf-8-sig")
    _write_csv(points_path, tables["points"])
    _write_csv(summary_path, tables["summary_rows"])
    _write_csv(predictions_path, tables["prediction_rows"])
    markdown_path.write_text(_markdown_report(tables), encoding="utf-8-sig")
    return {
        "manifest": str(manifest_path),
        "points": str(points_path),
        "summary": str(summary_path),
        "predictions": str(predictions_path),
        "markdown": str(markdown_path),
    }
