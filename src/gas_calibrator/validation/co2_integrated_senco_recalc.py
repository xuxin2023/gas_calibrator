"""Integrated CO2 SENCO layer recalculation for V1.5 offline review.

This module explains how an existing final CO2 affine layer (SENCO5) would
change the target seen by the SENCO1/SENCO3 optical-temperature chain. It is
offline diagnostic evidence only when SENCO5 is non-neutral: SENCO5 must remain
a separate final affine layer and must not be folded into the SENCO1/SENCO3
main-chain write candidate. No COM ports, route control, or coefficient writes
are performed.
"""

from __future__ import annotations

import csv
import json
import math
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

import numpy as np

from ..senco_format import format_senco_values, rounded_senco_values

TERMS: Tuple[str, ...] = ("intercept", "R", "R2", "R3", "T", "T2", "RT")
SENCO5_SEPARATE_LAYER_CONTRACT = "senco5_separate_final_affine_layer_do_not_fold_into_senco13"
SENCO13_MAIN_CHAIN_CONTRACT = "senco13_ratio_temperature_main_chain_only"


@dataclass(frozen=True)
class Co2FitPoint:
    device_id: str
    analyzer_prefix: str
    point_identity: str
    target_ppm: float
    ratio: float
    temperature_c: float
    pressure_hpa: float


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
    source = Path(path)
    if not source.exists():
        return []
    with source.open("r", encoding="utf-8-sig", newline="") as handle:
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


def _load_points(path: str | Path, *, target_devices: Iterable[str] = ()) -> List[Co2FitPoint]:
    targets = {_device_id(item) for item in target_devices if str(item or "").strip()}
    points: List[Co2FitPoint] = []
    for row in _read_csv(path):
        if str(row.get("component") or "co2").strip().lower() != "co2":
            continue
        role_text = " ".join(str(row.get(key) or "") for key in ("source_role", "sample_role", "residual_role")).lower()
        if role_text and "fit" not in role_text:
            continue
        device = _device_id(row.get("analyzer_device_id") or row.get("device_id"))
        if not device or (targets and device not in targets):
            continue
        ratio = _safe_float(row.get("ratio") or row.get("co2_ratio_f_mean") or row.get("R_CO2"))
        target = _safe_float(row.get("target_value") or row.get("certificate_co2_ppm") or row.get("target_ppm"))
        temp = _safe_float(row.get("temperature_c") or row.get("chamber_temp_mean_c") or row.get("T1"))
        pressure = _safe_float(row.get("pressure_hpa") or row.get("pressure_gauge_hpa"))
        if pressure is None:
            pressure_kpa = _safe_float(row.get("pressure_kpa") or row.get("BAR"))
            pressure = pressure_kpa * 10.0 if pressure_kpa is not None else None
        if ratio is None or target is None or temp is None or pressure is None:
            continue
        points.append(
            Co2FitPoint(
                device_id=device,
                analyzer_prefix=str(row.get("analyzer_prefix") or "").strip(),
                point_identity=str(row.get("point_identity") or row.get("sample_index") or "").strip(),
                target_ppm=float(target),
                ratio=float(ratio),
                temperature_c=float(temp),
                pressure_hpa=float(pressure),
            )
        )
    return points


def _load_senco5_snapshot(path: str | Path | None) -> Dict[str, Tuple[float, float]]:
    if not path:
        return {}
    rows = _read_csv(path)
    out: Dict[str, Tuple[float, float]] = {}
    for row in rows:
        group = str(row.get("getco_group") or row.get("group") or "").strip()
        if group != "5":
            continue
        device = _device_id(row.get("analyzer_device_id") or row.get("device_id"))
        values_text = row.get("coefficient_values_json") or row.get("values_json")
        parsed: Any = None
        if values_text:
            try:
                parsed = json.loads(str(values_text))
            except Exception:
                parsed = None
        if not isinstance(parsed, Sequence) or isinstance(parsed, (str, bytes)) or len(parsed) < 2:
            parsed_json = row.get("parsed_coefficients_json")
            if parsed_json:
                try:
                    coeffs = json.loads(str(parsed_json))
                    parsed = [coeffs.get("C0"), coeffs.get("C1")]
                except Exception:
                    parsed = None
        if not device or not isinstance(parsed, Sequence) or isinstance(parsed, (str, bytes)) or len(parsed) < 2:
            continue
        c0 = _safe_float(parsed[0])
        c1 = _safe_float(parsed[1])
        if c0 is None or c1 is None:
            continue
        out[device] = (float(c0), float(c1))
    return out


def _centered_matrix(points: Sequence[Co2FitPoint], *, ratio_center: float, temp_center_k: float) -> np.ndarray:
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


def _absolute_features(point: Co2FitPoint) -> np.ndarray:
    r = float(point.ratio)
    t = float(point.temperature_c) + 273.15
    return np.asarray([1.0, r, r * r, r**3, t, t * t, r * t], dtype=float)


def _scaled_lstsq(matrix: np.ndarray, target: np.ndarray) -> Tuple[np.ndarray, int, float]:
    scales = np.linalg.norm(matrix, axis=0)
    scales = np.where(np.isfinite(scales) & (scales > 0.0), scales, 1.0)
    scaled = matrix / scales
    rank = int(np.linalg.matrix_rank(scaled))
    condition = float(np.linalg.cond(scaled))
    fitted, *_ = np.linalg.lstsq(scaled, target, rcond=None)
    return np.asarray(fitted, dtype=float) / scales, rank, condition


def _fit_senco13(points: Sequence[Co2FitPoint], raw_targets: Sequence[float]) -> Tuple[np.ndarray, int, float]:
    ratio_center = float(np.mean([point.ratio for point in points]))
    temp_center = float(np.mean([point.temperature_c + 273.15 for point in points]))
    centered = _centered_matrix(points, ratio_center=ratio_center, temp_center_k=temp_center)
    centered_coeffs, rank, condition = _scaled_lstsq(centered, np.asarray(raw_targets, dtype=float))
    absolute = _centered_to_absolute(centered_coeffs, ratio_center=ratio_center, temp_center_k=temp_center)
    return absolute, rank, condition


def _metrics(errors: Sequence[float]) -> Dict[str, float]:
    values = np.asarray(list(errors), dtype=float)
    if values.size == 0:
        return {"rmse_ppm": 0.0, "max_abs_error_ppm": 0.0, "mean_error_ppm": 0.0}
    return {
        "rmse_ppm": float(np.sqrt(np.mean(values**2))),
        "max_abs_error_ppm": float(np.max(np.abs(values))),
        "mean_error_ppm": float(np.mean(values)),
    }


def _linear_status(c0: float, c1: float) -> str:
    if not math.isfinite(c0) or not math.isfinite(c1) or abs(c1) <= 1.0e-12:
        return "invalid_linear_layer_blocks_preserve"
    if c1 <= 0.0:
        return "nonmonotonic_negative_slope_high_risk_final_affine_layer_diagnostic"
    if abs(c1 - 1.0) > 0.2 or abs(c0) > 50.0:
        return "large_existing_linear_layer_preserve_only_with_explicit_review"
    if abs(c1 - 1.0) <= 1.0e-9 and abs(c0) <= 1.0e-9:
        return "neutral_linear_layer"
    return "preservable_final_affine_layer_requires_separate_review"


def _senco13_write_status(*, scenario_name: str, linear_status: str) -> Tuple[str, bool, bool, str]:
    """Classify whether a scenario can be used as a SENCO1/SENCO3 write candidate.

    Inverting a non-neutral SENCO5 is useful to explain historical behavior, but
    writing the resulting coefficients would hide the final affine layer inside
    the optical/temperature polynomial. That breaks the V1.5 physical contract.
    """

    if scenario_name == "force_neutral_senco5" and linear_status == "neutral_linear_layer":
        return (
            "reviewable_no_write",
            True,
            False,
            "SENCO5 is neutral, so SENCO1/SENCO3 can be fitted directly to certificate CO2 as the main ratio-temperature chain.",
        )
    if linear_status == "neutral_linear_layer":
        return (
            "reviewable_no_write",
            True,
            False,
            "The referenced SENCO5 layer is neutral; this scenario is equivalent to the main-chain direct target.",
        )
    return (
        "diagnostic_only_senco5_final_affine_replay_not_senco13_write_candidate",
        False,
        True,
        "Non-neutral SENCO5 was inverted only to explain previous data. Its effect must be reviewed or written as SENCO5, not merged into SENCO1/SENCO3.",
    )


def build_co2_integrated_senco_recalc_tables(
    *,
    fit_residuals_csv: str | Path,
    sampling_senco5_snapshot_csv: str | Path | None = None,
    preclear_senco5_snapshot_csv: str | Path | None = None,
    current_senco5_snapshot_csv: str | Path | None = None,
    target_device_ids: Sequence[str] = ("022", "030", "033", "051"),
) -> Dict[str, List[Dict[str, Any]]]:
    points = _load_points(fit_residuals_csv, target_devices=target_device_ids)
    by_device: Dict[str, List[Co2FitPoint]] = {}
    for point in points:
        by_device.setdefault(point.device_id, []).append(point)

    sampling = _load_senco5_snapshot(sampling_senco5_snapshot_csv)
    preclear = _load_senco5_snapshot(preclear_senco5_snapshot_csv)
    current = _load_senco5_snapshot(current_senco5_snapshot_csv)
    scenarios = [
        (
            "sampling_snapshot_preserve_senco5",
            sampling,
            "fit SENCO1/SENCO3 to the raw target implied by the SENCO5 layer present during sampling",
            "preserve_existing_final_linear_layer",
        ),
        (
            "current_neutral_or_snapshot_senco5",
            current,
            "fit SENCO1/SENCO3 target through the latest available CO2 affine layer snapshot",
            "latest_snapshot_or_neutral_final_linear_layer",
        ),
        (
            "preclear_preserve_senco5",
            preclear,
            "diagnostic: if old SENCO5 were preserved, invert it before fitting SENCO1/SENCO3",
            "preserve_existing_final_linear_layer",
        ),
        ("force_neutral_senco5", {}, "set/assume SENCO5 neutral, fit SENCO1/SENCO3 directly to certificate ppm", "absolute_replace_main_chain"),
    ]

    summary_rows: List[Dict[str, Any]] = []
    coefficient_rows: List[Dict[str, Any]] = []
    prediction_rows: List[Dict[str, Any]] = []

    for device in sorted({_device_id(item) for item in target_device_ids}):
        device_points = by_device.get(device, [])
        if not device_points:
            summary_rows.append(
                {
                    "device_id": device,
                    "scenario": "all",
                    "status": "blocked_no_fit_points",
                    "fit_point_count": 0,
                    "physical_meaning": "No offline fit points were found for this analyzer.",
                }
            )
            continue
        for scenario_name, linear_map, meaning, fit_strategy in scenarios:
            c0, c1 = linear_map.get(device, (0.0, 1.0))
            if scenario_name == "force_neutral_senco5":
                c0, c1 = 0.0, 1.0
            linear_status = _linear_status(c0, c1)
            status, senco13_write_candidate, wrong_layer_merge_blocked, layer_policy_meaning = _senco13_write_status(
                scenario_name=scenario_name,
                linear_status=linear_status,
            )
            if "blocks" in linear_status:
                summary_rows.append(
                    {
                        "device_id": device,
                        "scenario": scenario_name,
                        "status": "blocked",
                        "fit_point_count": len(device_points),
                        "senco5_C0": c0,
                        "senco5_C1": c1,
                        "linear_layer_status": linear_status,
                        "fit_strategy": fit_strategy,
                        "senco13_main_chain_contract": SENCO13_MAIN_CHAIN_CONTRACT,
                        "senco5_layer_contract": SENCO5_SEPARATE_LAYER_CONTRACT,
                        "senco13_write_candidate": False,
                        "wrong_layer_merge_blocked": True,
                        "layer_policy_meaning": "Invalid SENCO5 cannot be inverted or silently neutralized for a main-chain write.",
                        "physical_meaning": meaning,
                    }
                )
                continue
            raw_targets = [(point.target_ppm - c0) / c1 for point in device_points]
            coeffs, rank, condition = _fit_senco13(device_points, raw_targets)
            rounded = np.asarray(rounded_senco_values(coeffs), dtype=float)
            errors: List[float] = []
            rounded_errors: List[float] = []
            for point in device_points:
                raw = float(_absolute_features(point) @ coeffs)
                final = raw * c1 + c0
                error = final - point.target_ppm
                rounded_raw = float(_absolute_features(point) @ rounded)
                rounded_final = rounded_raw * c1 + c0
                rounded_error = rounded_final - point.target_ppm
                errors.append(error)
                rounded_errors.append(rounded_error)
                prediction_rows.append(
                    {
                        "device_id": device,
                        "analyzer_prefix": point.analyzer_prefix,
                        "scenario": scenario_name,
                        "point_identity": point.point_identity,
                        "target_ppm": point.target_ppm,
                        "ratio": point.ratio,
                        "temperature_c": point.temperature_c,
                        "pressure_hpa": point.pressure_hpa,
                        "senco5_C0": c0,
                        "senco5_C1": c1,
                        "fit_strategy": fit_strategy,
                        "senco13_main_chain_contract": SENCO13_MAIN_CHAIN_CONTRACT,
                        "senco5_layer_contract": SENCO5_SEPARATE_LAYER_CONTRACT,
                        "senco13_write_candidate": senco13_write_candidate,
                        "wrong_layer_merge_blocked": wrong_layer_merge_blocked,
                        "layer_policy_meaning": layer_policy_meaning,
                        "raw_target_ppm": (point.target_ppm - c0) / c1,
                        "predicted_final_ppm": final,
                        "error_ppm": error,
                        "rounded_predicted_final_ppm": rounded_final,
                        "rounded_error_ppm": rounded_error,
                    }
                )
            metric = _metrics(errors)
            rounded_metric = _metrics(rounded_errors)
            primary = [float(coeffs[0]), float(coeffs[1]), float(coeffs[2]), float(coeffs[3]), 0.0, 0.0]
            secondary = [float(coeffs[4]), float(coeffs[5]), float(coeffs[6]), 0.0, 0.0, 0.0]
            rounded_primary = [float(rounded[0]), float(rounded[1]), float(rounded[2]), float(rounded[3]), 0.0, 0.0]
            rounded_secondary = [float(rounded[4]), float(rounded[5]), float(rounded[6]), 0.0, 0.0, 0.0]
            summary_rows.append(
                {
                    "device_id": device,
                    "scenario": scenario_name,
                    "status": status,
                    "fit_point_count": len(device_points),
                    "senco5_C0": c0,
                    "senco5_C1": c1,
                    "linear_layer_status": linear_status,
                    "fit_strategy": fit_strategy,
                    "senco13_main_chain_contract": SENCO13_MAIN_CHAIN_CONTRACT,
                    "senco5_layer_contract": SENCO5_SEPARATE_LAYER_CONTRACT,
                    "senco13_write_candidate": senco13_write_candidate,
                    "wrong_layer_merge_blocked": wrong_layer_merge_blocked,
                    "layer_policy_meaning": layer_policy_meaning,
                    "matrix_rank": rank,
                    "condition_number_scaled": condition,
                    "rmse_ppm": metric["rmse_ppm"],
                    "max_abs_error_ppm": metric["max_abs_error_ppm"],
                    "mean_error_ppm": metric["mean_error_ppm"],
                    "rounded_rmse_ppm": rounded_metric["rmse_ppm"],
                    "rounded_max_abs_error_ppm": rounded_metric["max_abs_error_ppm"],
                    "rounded_mean_error_ppm": rounded_metric["mean_error_ppm"],
                    "primary_payload_scientific": ",".join(format_senco_values(primary)),
                    "secondary_payload_scientific": ",".join(format_senco_values(secondary)),
                    "rounded_primary_payload_json": json.dumps(rounded_primary, separators=(",", ":")),
                    "rounded_secondary_payload_json": json.dumps(rounded_secondary, separators=(",", ":")),
                    "physical_meaning": meaning,
                    "opens_com_ports": False,
                    "controls_water_or_gas_routes": False,
                    "writes_coefficients": False,
                }
            )
            for term, coefficient in zip(TERMS, coeffs):
                coefficient_rows.append(
                    {
                        "device_id": device,
                        "scenario": scenario_name,
                        "term": term,
                        "coefficient": float(coefficient),
                        "senco_group": "SENCO1" if term in ("intercept", "R", "R2", "R3") else "SENCO3",
                    }
                )

    return {
        "integrated_senco_recalc_summary": summary_rows,
        "integrated_senco_recalc_coefficients": coefficient_rows,
        "integrated_senco_recalc_predictions": prediction_rows,
    }


def write_co2_integrated_senco_recalc_report(
    *,
    fit_residuals_csv: str | Path,
    output_dir: str | Path,
    sampling_senco5_snapshot_csv: str | Path | None = None,
    preclear_senco5_snapshot_csv: str | Path | None = None,
    current_senco5_snapshot_csv: str | Path | None = None,
    target_device_ids: Sequence[str] = ("022", "030", "033", "051"),
) -> Dict[str, Path]:
    output = Path(output_dir).resolve()
    output.mkdir(parents=True, exist_ok=True)
    tables = build_co2_integrated_senco_recalc_tables(
        fit_residuals_csv=fit_residuals_csv,
        sampling_senco5_snapshot_csv=sampling_senco5_snapshot_csv,
        preclear_senco5_snapshot_csv=preclear_senco5_snapshot_csv,
        current_senco5_snapshot_csv=current_senco5_snapshot_csv,
        target_device_ids=target_device_ids,
    )
    outputs: Dict[str, Path] = {}
    for name, rows in tables.items():
        path = output / f"{name}.csv"
        _write_csv(path, rows)
        outputs[name] = path
    meta = {
        "tool": "co2_integrated_senco_recalc",
        "created_at": _now(),
        "inputs": {
            "fit_residuals_csv": str(Path(fit_residuals_csv).resolve()),
            "sampling_senco5_snapshot_csv": str(Path(sampling_senco5_snapshot_csv).resolve()) if sampling_senco5_snapshot_csv else "",
            "preclear_senco5_snapshot_csv": str(Path(preclear_senco5_snapshot_csv).resolve()) if preclear_senco5_snapshot_csv else "",
            "current_senco5_snapshot_csv": str(Path(current_senco5_snapshot_csv).resolve()) if current_senco5_snapshot_csv else "",
            "target_device_ids": list(target_device_ids),
        },
        "boundary": {
            "opens_com_ports": False,
            "controls_water_or_gas_routes": False,
            "writes_coefficients": False,
        },
    }
    meta_path = output / "integrated_senco_recalc_meta.json"
    meta_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
    outputs["meta"] = meta_path
    md_path = output / "integrated_senco_recalc_review.md"
    outputs["markdown"] = _write_markdown(md_path, tables)
    return outputs


def _write_markdown(path: Path, tables: Mapping[str, Sequence[Mapping[str, Any]]]) -> Path:
    lines = [
        "# V1.5 CO2 Integrated SENCO Recalculation Review",
        "",
        "- Boundary: offline only; no COM, no gas/water route control, no coefficient write.",
        "- Contract: final CO2 = raw(SENCO1/SENCO3) * SENCO5.C1 + SENCO5.C0.",
        "- SENCO5 is a separate final affine layer; non-neutral SENCO5 must not be folded into a SENCO1/SENCO3 write candidate.",
        "- Preserve/invert scenarios are diagnostic evidence for explaining historical output only.",
        "- A SENCO1/SENCO3 write candidate must use the neutral/direct main-chain contract; SENCO5 trim is reviewed separately.",
        "",
        "| Device | Scenario | Strategy | C0 | C1 | RMSE ppm | Max ppm | Rounded Max ppm | S1/3 write candidate | Status |",
        "| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | --- | --- |",
    ]
    for row in tables.get("integrated_senco_recalc_summary", []):
        lines.append(
            "| {device} | {scenario} | {strategy} | {c0} | {c1} | {rmse} | {maxerr} | {rmax} | {candidate} | {status} |".format(
                device=row.get("device_id", ""),
                scenario=row.get("scenario", ""),
                strategy=row.get("fit_strategy", ""),
                c0=_fmt(row.get("senco5_C0")),
                c1=_fmt(row.get("senco5_C1")),
                rmse=_fmt(row.get("rmse_ppm")),
                maxerr=_fmt(row.get("max_abs_error_ppm")),
                rmax=_fmt(row.get("rounded_max_abs_error_ppm")),
                candidate=row.get("senco13_write_candidate", ""),
                status=row.get("status", ""),
            )
        )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def _fmt(value: Any) -> str:
    numeric = _safe_float(value)
    if numeric is None:
        return ""
    return f"{numeric:.6g}"
