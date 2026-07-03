"""ID-scoped CO2 three-point state bridge review for V1.5.

This module compares old full-temperature open-flow evidence with later
current-state verification evidence at the same CO2 target family.  It uses
factory-mode ratio and physical state variables as the bridge evidence.  The
displayed CO2 output is retained only as auxiliary evidence because the old
run may have been captured while the analyzer already had internal SENCO/S5
coefficients.

No COM is opened here.  No SENCO is written here.
"""

from __future__ import annotations

import csv
import json
import math
import re
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

import numpy as np


@dataclass(frozen=True)
class Co2StatePoint:
    source_set: str
    point_identity: str
    analyzer_prefix: str
    device_id: str
    role: str
    target_ppm: float
    nominal_ppm: float
    displayed_co2_ppm: Optional[float]
    co2_ratio_f: float
    co2_ratio_raw: Optional[float]
    co2_ratio_f_span: Optional[float]
    h2o_ratio_f: Optional[float]
    h2o_mmol_mol: float
    chamber_temp_c: float
    case_temp_c: Optional[float]
    pressure_hpa: float
    analyzer_pressure_kpa: Optional[float]
    dewpoint_c: Optional[float]
    sample_count: int
    usable_count: int
    source_path: str


@dataclass(frozen=True)
class BridgeVariant:
    model_id: str
    terms: Tuple[str, ...]
    note: str


BRIDGE_VARIANTS: Tuple[BridgeVariant, ...] = (
    BridgeVariant(
        model_id="old_all_ratio_temp",
        terms=("R", "R2", "R3", "T", "T2", "RT"),
        note="No pressure term; closest to current V1.5 CO2 SENCO1/SENCO3 main-chain review.",
    ),
    BridgeVariant(
        model_id="old_all_ratio_temp_h2o",
        terms=("R", "R2", "R3", "T", "T2", "RT", "H", "RH"),
        note="Diagnostic only; checks whether dry/wet state explains current ratio shift.",
    ),
    BridgeVariant(
        model_id="old_all_ratio_temp_h2o_pressure_diag",
        terms=("R", "R2", "R3", "T", "T2", "RT", "H", "RH", "P", "RP"),
        note="Diagnostic only; pressure is not a formal current-atmosphere CO2 fitting term.",
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


def _mean(values: Iterable[Optional[float]]) -> Optional[float]:
    clean = [value for value in values if value is not None and math.isfinite(value)]
    if not clean:
        return None
    return float(sum(clean) / len(clean))


def _span(values: Iterable[Optional[float]]) -> Optional[float]:
    clean = [value for value in values if value is not None and math.isfinite(value)]
    if not clean:
        return None
    return float(max(clean) - min(clean))


def _target_from_point_name(text: str) -> Optional[float]:
    match = re.search(r"_(\d+(?:\.\d+)?)ppm_", text)
    if match:
        return float(match.group(1))
    match = re.search(r"(\d+(?:\.\d+)?)ppm", text, flags=re.IGNORECASE)
    return float(match.group(1)) if match else None


def _role_from_point_name(text: str) -> str:
    lower = text.lower()
    if "verification" in lower or "verify" in lower:
        return "verification"
    if "diagnostic" in lower:
        return "diagnostic"
    return "fit"


def _prefixes_from_row(row: Mapping[str, Any]) -> List[str]:
    prefixes: List[str] = []
    for key in row:
        match = re.match(r"(ga\d+)_analyzer_device_id$", key or "")
        if match:
            prefixes.append(match.group(1))
    return sorted(prefixes)


def _point_from_samples(
    samples_csv: str | Path,
    *,
    source_set: str,
    point_identity: Optional[str],
    target_device_id: str,
) -> Optional[Co2StatePoint]:
    path = Path(samples_csv)
    rows = _read_csv(path)
    if not rows:
        return None
    target_id = _device_id(target_device_id)
    identity = point_identity or path.parent.name
    for prefix in _prefixes_from_row(rows[0]):
        matched = [
            row
            for row in rows
            if _device_id(row.get(f"{prefix}_analyzer_device_id")) == target_id
        ]
        if not matched:
            continue
        target = _mean(
            _safe_float(row.get("certificate_co2_ppm") or row.get("co2_ppm_target") or row.get("target_value"))
            for row in matched
        )
        if target is None:
            target = _target_from_point_name(identity)
        nominal = _target_from_point_name(identity)
        if nominal is None:
            nominal = target
        displayed = _mean(_safe_float(row.get(f"{prefix}_co2_ppm")) for row in matched)
        ratio_f_values = [_safe_float(row.get(f"{prefix}_co2_ratio_f")) for row in matched]
        ratio_f = _mean(ratio_f_values)
        ratio_raw = _mean(_safe_float(row.get(f"{prefix}_co2_ratio_raw")) for row in matched)
        h2o_ratio = _mean(_safe_float(row.get(f"{prefix}_h2o_ratio_f")) for row in matched)
        h2o = _mean(_safe_float(row.get(f"{prefix}_h2o_mmol")) for row in matched)
        chamber_temp = _mean(_safe_float(row.get(f"{prefix}_chamber_temp_c")) for row in matched)
        case_temp = _mean(_safe_float(row.get(f"{prefix}_case_temp_c")) for row in matched)
        pressure = _mean(
            _safe_float(row.get("pressure_gauge_hpa") or row.get("pressure_hpa")) for row in matched
        )
        analyzer_pressure = _mean(_safe_float(row.get(f"{prefix}_pressure_kpa")) for row in matched)
        if pressure is None and analyzer_pressure is not None:
            pressure = analyzer_pressure * 10.0
        dewpoint = _mean(_safe_float(row.get("dewpoint_c") or row.get("dewpoint_live_c")) for row in matched)
        usable = sum(
            1
            for row in matched
            if str(row.get(f"{prefix}_frame_usable") or row.get("frame_usable") or "").lower() == "true"
        )
        if None in (target, nominal, ratio_f, h2o, chamber_temp, pressure):
            return None
        return Co2StatePoint(
            source_set=source_set,
            point_identity=identity,
            analyzer_prefix=prefix.upper(),
            device_id=target_id,
            role=_role_from_point_name(identity),
            target_ppm=float(target),
            nominal_ppm=float(nominal),
            displayed_co2_ppm=displayed,
            co2_ratio_f=float(ratio_f),
            co2_ratio_raw=ratio_raw,
            co2_ratio_f_span=_span(ratio_f_values),
            h2o_ratio_f=h2o_ratio,
            h2o_mmol_mol=float(h2o),
            chamber_temp_c=float(chamber_temp),
            case_temp_c=case_temp,
            pressure_hpa=float(pressure),
            analyzer_pressure_kpa=analyzer_pressure,
            dewpoint_c=dewpoint,
            sample_count=len(matched),
            usable_count=usable,
            source_path=str(path),
        )
    return None


def load_points_from_run_root(
    run_dir: str | Path,
    *,
    source_set: str,
    target_device_id: str,
) -> List[Co2StatePoint]:
    root = Path(run_dir)
    points: List[Co2StatePoint] = []
    for samples_csv in sorted(root.glob("p*/samples_machine_readable.csv")):
        point = _point_from_samples(
            samples_csv,
            source_set=source_set,
            point_identity=samples_csv.parent.name,
            target_device_id=target_device_id,
        )
        if point is not None:
            points.append(point)
    return points


def load_current_points_from_sample_files(
    sample_files: Sequence[str | Path],
    *,
    source_set: str,
    target_device_id: str,
) -> List[Co2StatePoint]:
    points: List[Co2StatePoint] = []
    for samples_csv in sample_files:
        point = _point_from_samples(
            samples_csv,
            source_set=source_set,
            point_identity=Path(samples_csv).parent.name,
            target_device_id=target_device_id,
        )
        if point is not None:
            points.append(point)
    return points


def _feature(point: Co2StatePoint, term: str) -> float:
    r = point.co2_ratio_f
    t = point.chamber_temp_c + 273.15
    h = point.h2o_mmol_mol
    p = point.pressure_hpa / 10.0
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
    if term == "H":
        return h
    if term == "RH":
        return r * h
    if term == "P":
        return p
    if term == "RP":
        return r * p
    raise KeyError(term)


def _matrix(points: Sequence[Co2StatePoint], terms: Sequence[str]) -> np.ndarray:
    rows = [[1.0] + [_feature(point, term) for term in terms] for point in points]
    return np.asarray(rows, dtype=float)


def _fit(points: Sequence[Co2StatePoint], variant: BridgeVariant) -> Tuple[np.ndarray, int, float]:
    x = _matrix(points, variant.terms)
    y = np.asarray([point.target_ppm for point in points], dtype=float)
    coeffs, _, rank, _ = np.linalg.lstsq(x, y, rcond=None)
    condition = float(np.linalg.cond(x)) if x.size else math.inf
    return coeffs, int(rank), condition


def _predict(points: Sequence[Co2StatePoint], variant: BridgeVariant, coeffs: np.ndarray) -> np.ndarray:
    return _matrix(points, variant.terms) @ coeffs


def _metrics(rows: Sequence[Mapping[str, Any]]) -> Dict[str, Optional[float]]:
    errors = [float(row["prediction_error_ppm"]) for row in rows]
    pct_errors = [
        abs(float(row["prediction_error_pct"]))
        for row in rows
        if row.get("prediction_error_pct") not in (None, "")
    ]
    return {
        "point_count": float(len(rows)),
        "rmse_ppm": float(np.sqrt(np.mean(np.asarray(errors) ** 2))) if errors else None,
        "max_abs_error_ppm": max((abs(value) for value in errors), default=None),
        "mean_error_ppm": float(np.mean(errors)) if errors else None,
        "max_abs_error_pct_nonzero": max(pct_errors, default=None),
    }


def _prediction_rows(
    points: Sequence[Co2StatePoint],
    predictions: Sequence[float],
    *,
    model_id: str,
    eval_set: str,
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for point, prediction in zip(points, predictions):
        error = float(prediction) - point.target_ppm
        error_pct = None if abs(point.target_ppm) < 1e-12 else error / point.target_ppm * 100.0
        rows.append(
            {
                "model_id": model_id,
                "eval_set": eval_set,
                "source_set": point.source_set,
                "point_identity": point.point_identity,
                "device_id": point.device_id,
                "target_ppm": point.target_ppm,
                "nominal_ppm": point.nominal_ppm,
                "displayed_co2_ppm_aux": point.displayed_co2_ppm,
                "prediction_ppm": float(prediction),
                "prediction_error_ppm": error,
                "prediction_error_pct": error_pct,
                "co2_ratio_f": point.co2_ratio_f,
                "co2_ratio_f_span": point.co2_ratio_f_span,
                "h2o_mmol_mol": point.h2o_mmol_mol,
                "dewpoint_c": point.dewpoint_c,
                "chamber_temp_c": point.chamber_temp_c,
                "pressure_hpa": point.pressure_hpa,
                "sample_count": point.sample_count,
                "usable_count": point.usable_count,
            }
        )
    return rows


def _nearest_old_t20(current: Co2StatePoint, old_points: Sequence[Co2StatePoint]) -> Optional[Co2StatePoint]:
    candidates = [
        point
        for point in old_points
        if abs(point.nominal_ppm - current.nominal_ppm) <= 5.0
        and 15.0 <= point.chamber_temp_c <= 25.0
    ]
    if not candidates:
        candidates = [point for point in old_points if abs(point.nominal_ppm - current.nominal_ppm) <= 5.0]
    if not candidates:
        return None
    return min(candidates, key=lambda point: abs(point.chamber_temp_c - current.chamber_temp_c))


def _state_shift_rows(
    old_points: Sequence[Co2StatePoint],
    current_points: Sequence[Co2StatePoint],
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for current in sorted(current_points, key=lambda point: point.nominal_ppm):
        old = _nearest_old_t20(current, old_points)
        if old is None:
            continue
        rows.append(
            {
                "nominal_ppm": current.nominal_ppm,
                "current_target_ppm": current.target_ppm,
                "old_point": old.point_identity,
                "current_point": current.point_identity,
                "old_ratio_f": old.co2_ratio_f,
                "current_ratio_f": current.co2_ratio_f,
                "delta_ratio_f": current.co2_ratio_f - old.co2_ratio_f,
                "old_h2o_mmol_mol": old.h2o_mmol_mol,
                "current_h2o_mmol_mol": current.h2o_mmol_mol,
                "delta_h2o_mmol_mol": current.h2o_mmol_mol - old.h2o_mmol_mol,
                "old_dewpoint_c": old.dewpoint_c,
                "current_dewpoint_c": current.dewpoint_c,
                "delta_dewpoint_c": (
                    None
                    if old.dewpoint_c is None or current.dewpoint_c is None
                    else current.dewpoint_c - old.dewpoint_c
                ),
                "old_chamber_temp_c": old.chamber_temp_c,
                "current_chamber_temp_c": current.chamber_temp_c,
                "delta_chamber_temp_c": current.chamber_temp_c - old.chamber_temp_c,
                "old_pressure_hpa": old.pressure_hpa,
                "current_pressure_hpa": current.pressure_hpa,
                "delta_pressure_hpa": current.pressure_hpa - old.pressure_hpa,
                "old_displayed_co2_ppm_aux": old.displayed_co2_ppm,
                "current_displayed_co2_ppm_aux": current.displayed_co2_ppm,
                "old_source_path": old.source_path,
                "current_source_path": current.source_path,
            }
        )
    return rows


def _best_current_model(summary_rows: Sequence[Mapping[str, Any]]) -> Optional[Mapping[str, Any]]:
    candidates = [
        row
        for row in summary_rows
        if row.get("eval_set") == "current_three_point"
        and row.get("max_abs_error_pct_nonzero") not in (None, "")
    ]
    if not candidates:
        return None
    return min(candidates, key=lambda row: float(row["max_abs_error_pct_nonzero"]))


def build_three_point_state_bridge_tables(
    *,
    old_run_dir: str | Path,
    current_sample_files: Sequence[str | Path],
    target_device_id: str = "100",
) -> Dict[str, Any]:
    target_id = _device_id(target_device_id)
    old_points = load_points_from_run_root(
        old_run_dir,
        source_set="old_fulltemp_prewrite_coefficients_present",
        target_device_id=target_id,
    )
    current_points = load_current_points_from_sample_files(
        current_sample_files,
        source_set="current_postwrite_verification",
        target_device_id=target_id,
    )
    current_nominals = {round(point.nominal_ppm) for point in current_points}
    old_three_point = [
        point
        for point in old_points
        if round(point.nominal_ppm) in current_nominals
    ]
    prediction_rows: List[Dict[str, Any]] = []
    summary_rows: List[Dict[str, Any]] = []
    for variant in BRIDGE_VARIANTS:
        if len(old_points) <= len(variant.terms) + 1:
            summary_rows.append(
                {
                    "model_id": variant.model_id,
                    "status": "skipped_insufficient_old_points",
                    "term_count": len(variant.terms),
                    "old_point_count": len(old_points),
                    "note": variant.note,
                }
            )
            continue
        coeffs, rank, condition = _fit(old_points, variant)
        for eval_set, points in (
            ("old_all", old_points),
            ("old_same_three_targets", old_three_point),
            ("current_three_point", current_points),
        ):
            if not points:
                continue
            rows = _prediction_rows(
                points,
                _predict(points, variant, coeffs),
                model_id=variant.model_id,
                eval_set=eval_set,
            )
            prediction_rows.extend(rows)
            summary_rows.append(
                {
                    "model_id": variant.model_id,
                    "eval_set": eval_set,
                    "status": "evaluated",
                    "term_count": len(variant.terms),
                    "rank": rank,
                    "condition_number": condition,
                    "old_train_count": len(old_points),
                    "note": variant.note,
                    **_metrics(rows),
                }
            )
    shift_rows = _state_shift_rows(old_points, current_points)
    best = _best_current_model(summary_rows)
    status = "insufficient_current_bridge_points"
    reason = "No current 100/800/900 point set was available."
    if len(current_points) >= 3 and best:
        max_pct = float(best["max_abs_error_pct_nonzero"])
        status = "state_bridge_explained" if max_pct <= 1.5 else "state_bridge_not_explained"
        reason = (
            f"Best diagnostic model {best['model_id']} current max error {max_pct:.3f}%."
        )
    manifest = {
        "schema": "v1_5_co2_three_point_state_bridge_v1",
        "generated_at": _now(),
        "target_device_id": target_id,
        "old_run_dir": str(old_run_dir),
        "current_sample_files": [str(path) for path in current_sample_files],
        "old_point_count": len(old_points),
        "old_same_three_target_count": len(old_three_point),
        "current_point_count": len(current_points),
        "current_nominal_targets_ppm": sorted(current_nominals),
        "bridge_status": status,
        "bridge_reason": reason,
        "best_current_model": dict(best) if best else {},
        "physical_contract": (
            "Old displayed CO2/H2O outputs were produced while internal coefficients may already "
            "have been present.  They are auxiliary evidence only.  CO2 bridge/fitting evidence "
            "uses certificate target plus factory-mode CO2 ratio and physical state variables."
        ),
        "no_com_no_write": True,
    }
    return {
        "manifest": manifest,
        "old_points": [asdict(point) for point in old_points],
        "current_points": [asdict(point) for point in current_points],
        "state_shift_rows": shift_rows,
        "summary_rows": summary_rows,
        "prediction_rows": prediction_rows,
    }


def _fmt(value: Any, digits: int = 3) -> str:
    if value in (None, ""):
        return ""
    try:
        number = float(value)
    except Exception:
        return str(value)
    return f"{number:.{digits}f}"


def _markdown_report(tables: Mapping[str, Any]) -> str:
    manifest = tables["manifest"]
    shift_rows = list(tables["state_shift_rows"])
    summary_rows = [row for row in tables["summary_rows"] if row.get("status") == "evaluated"]
    current_predictions = [
        row
        for row in tables["prediction_rows"]
        if row.get("eval_set") == "current_three_point"
    ]
    lines = [
        "# ID100 CO2 三点状态桥接评估（no-write）",
        "",
        "## 结论",
        f"- 设备 ID：`{manifest['target_device_id']}`",
        f"- 桥接状态：`{manifest['bridge_status']}`",
        f"- 结论理由：{manifest['bridge_reason']}",
        "- 旧全温数据采集时设备内部已经可能带有 SENCO/S5 等系数，所以旧 `displayed_co2_ppm` 只作为辅助证据。",
        "- 本评估用于比较 `CO2 证书值 / CO2 滤波后比值 / H2O / 露点 / 温度 / 压力` 的状态关系，不打开 COM、不写 SENCO、不控制气路或水路。",
        "",
        "## 数据规模",
        f"- 旧全温点数：{manifest['old_point_count']}",
        f"- 旧数据中与当前三点同族目标点数：{manifest['old_same_three_target_count']}",
        f"- 当前三点数：{manifest['current_point_count']}，目标：{manifest['current_nominal_targets_ppm']}",
        "",
        "## 旧 T20 附近点与当前点状态差异",
        "| 标称 ppm | 当前证书 ppm | 旧点 | 当前点 | ΔR_f | 旧 R_f | 当前 R_f | ΔH2O mmol/mol | 旧 H2O | 当前 H2O | 旧露点 °C | 当前露点 °C | ΔT °C | ΔP hPa |",
        "|---:|---:|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for row in shift_rows:
        lines.append(
            "| {nom} | {target} | {old_point} | {cur_point} | {dr} | {old_r} | {cur_r} | {dh} | {old_h} | {cur_h} | {old_dp} | {cur_dp} | {dt} | {dp} |".format(
                nom=_fmt(row.get("nominal_ppm"), 0),
                target=_fmt(row.get("current_target_ppm"), 3),
                old_point=row.get("old_point", ""),
                cur_point=row.get("current_point", ""),
                dr=_fmt(row.get("delta_ratio_f"), 6),
                old_r=_fmt(row.get("old_ratio_f"), 6),
                cur_r=_fmt(row.get("current_ratio_f"), 6),
                dh=_fmt(row.get("delta_h2o_mmol_mol"), 3),
                old_h=_fmt(row.get("old_h2o_mmol_mol"), 3),
                cur_h=_fmt(row.get("current_h2o_mmol_mol"), 3),
                old_dp=_fmt(row.get("old_dewpoint_c"), 2),
                cur_dp=_fmt(row.get("current_dewpoint_c"), 2),
                dt=_fmt(row.get("delta_chamber_temp_c"), 3),
                dp=_fmt(row.get("delta_pressure_hpa"), 3),
            )
        )
    lines.extend(
        [
            "",
            "## 旧数据训练模型对当前三点的解释能力",
            "| 模型 | 评估集 | 点数 | 最大绝对误差 ppm | 最大相对误差 % | RMSE ppm | 说明 |",
            "|---|---|---:|---:|---:|---:|---|",
        ]
    )
    for row in summary_rows:
        lines.append(
            "| {model} | {eval_set} | {count} | {maxppm} | {maxpct} | {rmse} | {note} |".format(
                model=row.get("model_id", ""),
                eval_set=row.get("eval_set", ""),
                count=_fmt(row.get("point_count"), 0),
                maxppm=_fmt(row.get("max_abs_error_ppm"), 3),
                maxpct=_fmt(row.get("max_abs_error_pct_nonzero"), 3),
                rmse=_fmt(row.get("rmse_ppm"), 3),
                note=row.get("note", ""),
            )
        )
    lines.extend(
        [
            "",
            "## 当前三点预测明细",
            "| 模型 | 当前点 | 证书 ppm | 显示 ppm（辅助） | 预测 ppm | 误差 ppm | 误差 % | R_f | H2O mmol/mol | 露点 °C | T °C | P hPa |",
            "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
        ]
    )
    for row in current_predictions:
        lines.append(
            "| {model} | {point} | {target} | {shown} | {pred} | {err} | {pct} | {r} | {h} | {dew} | {t} | {p} |".format(
                model=row.get("model_id", ""),
                point=row.get("point_identity", ""),
                target=_fmt(row.get("target_ppm"), 3),
                shown=_fmt(row.get("displayed_co2_ppm_aux"), 3),
                pred=_fmt(row.get("prediction_ppm"), 3),
                err=_fmt(row.get("prediction_error_ppm"), 3),
                pct=_fmt(row.get("prediction_error_pct"), 3),
                r=_fmt(row.get("co2_ratio_f"), 6),
                h=_fmt(row.get("h2o_mmol_mol"), 3),
                dew=_fmt(row.get("dewpoint_c"), 2),
                t=_fmt(row.get("chamber_temp_c"), 3),
                p=_fmt(row.get("pressure_hpa"), 3),
            )
        )
    lines.extend(
        [
            "",
            "## 物理解释",
            "- CO2 主校准的底层证据应是标准气证书值与工厂模式 CO2 比值、温度、压力、H2O 状态的关系；旧显示浓度不能直接跨系数状态比较。",
            "- 当前 100/800/900 三点若相对旧 T20 点都出现同向 R_f 偏移，并且 H2O/露点明显不同，说明当前气体干燥状态或吹扫状态与旧全温数据不完全一致。",
            "- 如果旧全温模型对当前三点仍有大残差，不能简单把旧全温点和当前点混在一起重算 SENCO1/SENCO3；应先做状态分层或补足当前状态锚点。",
            "- 压力项仅作为诊断变量。V1.5 当前大气压开放流通 CO2 主拟合仍不应把多压力补偿项混进正式 SENCO1/SENCO3 计算。",
        ]
    )
    return "\n".join(lines) + "\n"


def write_three_point_state_bridge_report(
    *,
    old_run_dir: str | Path,
    current_sample_files: Sequence[str | Path],
    output_dir: str | Path,
    target_device_id: str = "100",
) -> Dict[str, str]:
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    tables = build_three_point_state_bridge_tables(
        old_run_dir=old_run_dir,
        current_sample_files=current_sample_files,
        target_device_id=target_device_id,
    )
    manifest_path = output / "co2_three_point_state_bridge_manifest.json"
    old_points_path = output / "co2_three_point_old_points.csv"
    current_points_path = output / "co2_three_point_current_points.csv"
    shift_path = output / "co2_three_point_state_shift.csv"
    summary_path = output / "co2_three_point_model_summary.csv"
    predictions_path = output / "co2_three_point_predictions.csv"
    markdown_path = output / "co2_three_point_state_bridge.md"
    manifest_path.write_text(json.dumps(tables["manifest"], ensure_ascii=False, indent=2), encoding="utf-8-sig")
    _write_csv(old_points_path, tables["old_points"])
    _write_csv(current_points_path, tables["current_points"])
    _write_csv(shift_path, tables["state_shift_rows"])
    _write_csv(summary_path, tables["summary_rows"])
    _write_csv(predictions_path, tables["prediction_rows"])
    markdown_path.write_text(_markdown_report(tables), encoding="utf-8-sig")
    return {
        "manifest": str(manifest_path),
        "old_points": str(old_points_path),
        "current_points": str(current_points_path),
        "state_shift": str(shift_path),
        "summary": str(summary_path),
        "predictions": str(predictions_path),
        "markdown": str(markdown_path),
    }
