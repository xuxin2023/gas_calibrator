"""Offline V1.5 temperature-channel review from existing open-flow evidence."""

from __future__ import annotations

import csv
import math
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Sequence

import numpy as np

from ..calibration.temperature_compensation_fit import fit_temperature_compensation
from ..export.temperature_compensation_export import export_temperature_compensation_artifacts


DIGITAL_THERMOMETER_MEAN_KEY = "数字温度计温度C_平均值"
DIGITAL_THERMOMETER_AGE_MS_KEY = "数字温度计缓存年龄ms_平均值"
TEMP_SETPOINT_KEY = "温箱目标温度C"
POINT_TITLE_KEY = "点位标题"
POINT_TAG_KEY = "点位标签"
CELL_TEMP_SUFFIX = "温度箱温度C_平均值"
SHELL_TEMP_SUFFIX = "机壳温度C_平均值"

DEFAULT_TARGET_DEVICE_IDS = ("022", "030", "033", "051")
DEFAULT_EXCLUDED_DEVICE_IDS = ("023", "100")
HARD_BAD_TEMP_VALUES_C = (60.0, -40.0)


def _safe_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        numeric = float(value)
    except Exception:
        return None
    if not math.isfinite(numeric):
        return None
    return numeric


def _device_column(device_id: str, suffix: str) -> str:
    return f"气体分析仪{int(str(device_id))}_{suffix}"


def _read_first_row(path: Path) -> Dict[str, str] | None:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            return dict(row)
    return None


def _is_hard_bad_temperature(value: float, *, tolerance_c: float = 0.05) -> bool:
    return any(abs(float(value) - bad) <= tolerance_c for bad in HARD_BAD_TEMP_VALUES_C)


def _temperature_gate(
    *,
    raw_temp_c: float | None,
    ref_temp_c: float | None,
    max_abs_delta_from_ref_c: float,
    raw_temp_min_c: float,
    raw_temp_max_c: float,
) -> tuple[bool, str]:
    if raw_temp_c is None:
        return False, "missing_raw_temperature"
    if ref_temp_c is None:
        return False, "missing_reference_temperature"
    if _is_hard_bad_temperature(raw_temp_c):
        return False, "hard_bad_value"
    if raw_temp_c < raw_temp_min_c or raw_temp_c > raw_temp_max_c:
        return False, "raw_temperature_out_of_range"
    if abs(raw_temp_c - ref_temp_c) > max_abs_delta_from_ref_c:
        return False, "raw_reference_delta_too_large"
    return True, ""


def build_temperature_observations_from_point_dirs(
    point_dirs: Iterable[Path],
    *,
    target_device_ids: Sequence[str] = DEFAULT_TARGET_DEVICE_IDS,
    excluded_device_ids: Sequence[str] = DEFAULT_EXCLUDED_DEVICE_IDS,
    ref_temp_source: str = "digital_thermometer_from_h2o_full_temp",
    max_abs_delta_from_ref_c: float = 8.0,
    raw_temp_min_c: float = -35.0,
    raw_temp_max_c: float = 85.0,
    max_reference_age_ms: float | None = 5000.0,
) -> List[Dict[str, Any]]:
    """Extract SENCO7/8 observations from V1.5 point summary CSV files.

    The source files are already per-point evidence artifacts. This function does
    not open COM ports, control routes, or infer hidden state.
    """

    target_ids = {str(item).zfill(3) for item in target_device_ids}
    excluded_ids = {str(item).zfill(3) for item in excluded_device_ids}
    observations: List[Dict[str, Any]] = []

    for point_dir in sorted(Path(p) for p in point_dirs):
        if not point_dir.is_dir():
            continue
        point_csv = next(iter(sorted(point_dir.glob("points_*.csv"))), None)
        if point_csv is None:
            continue
        row = _read_first_row(point_csv)
        if not row:
            continue

        ref_temp_c = _safe_float(row.get(DIGITAL_THERMOMETER_MEAN_KEY))
        ref_age_ms = _safe_float(row.get(DIGITAL_THERMOMETER_AGE_MS_KEY))
        temp_setpoint_c = _safe_float(row.get(TEMP_SETPOINT_KEY))
        point_tag = str(row.get(POINT_TAG_KEY) or point_dir.name)

        for device_id in sorted(target_ids | excluded_ids):
            cell_temp_c = _safe_float(row.get(_device_column(device_id, CELL_TEMP_SUFFIX)))
            shell_temp_c = _safe_float(row.get(_device_column(device_id, SHELL_TEMP_SUFFIX)))
            if ref_temp_c is None and cell_temp_c is None and shell_temp_c is None:
                continue

            ref_age_ok = True
            ref_age_reason = ""
            if max_reference_age_ms is not None and ref_age_ms is not None and ref_age_ms > max_reference_age_ms:
                ref_age_ok = False
                ref_age_reason = "reference_temperature_stale"

            cell_ok, cell_reason = _temperature_gate(
                raw_temp_c=cell_temp_c,
                ref_temp_c=ref_temp_c,
                max_abs_delta_from_ref_c=max_abs_delta_from_ref_c,
                raw_temp_min_c=raw_temp_min_c,
                raw_temp_max_c=raw_temp_max_c,
            )
            shell_ok, shell_reason = _temperature_gate(
                raw_temp_c=shell_temp_c,
                ref_temp_c=ref_temp_c,
                max_abs_delta_from_ref_c=max_abs_delta_from_ref_c,
                raw_temp_min_c=raw_temp_min_c,
                raw_temp_max_c=raw_temp_max_c,
            )

            excluded = device_id in excluded_ids
            valid_for_cell = bool(cell_ok and ref_age_ok and not excluded)
            valid_for_shell = bool(shell_ok and ref_age_ok and not excluded)
            if excluded:
                excluded_reason = "excluded_device_id"
                if not cell_reason:
                    cell_reason = excluded_reason
                if not shell_reason:
                    shell_reason = excluded_reason
            if not ref_age_ok:
                if not cell_reason:
                    cell_reason = ref_age_reason
                if not shell_reason:
                    shell_reason = ref_age_reason

            observations.append(
                {
                    "snapshot_time": row.get("采样时间") or row.get("保存时间") or "",
                    "timestamp": row.get("采样时间") or row.get("保存时间") or "",
                    "analyzer_id": device_id,
                    "analyzer_device_id": device_id,
                    "temp_setpoint_c": temp_setpoint_c,
                    "temperature_setpoint_c": temp_setpoint_c,
                    "chamber_temperature_box_c": temp_setpoint_c,
                    "chamber_temperature_env_c": ref_temp_c,
                    "ref_temp_c": ref_temp_c,
                    "ref_temp_source": ref_temp_source,
                    "cell_temp_raw_c": cell_temp_c,
                    "shell_temp_raw_c": shell_temp_c,
                    "analyzer_cell_temp_raw_c": cell_temp_c,
                    "analyzer_shell_temp_raw_c": shell_temp_c,
                    "route_type": "h2o_open_flow_full_temperature",
                    "is_temp_calibration_snapshot": True,
                    "valid_for_cell_fit": valid_for_cell,
                    "valid_for_shell_fit": valid_for_shell,
                    "cell_fit_gate_reason": "" if valid_for_cell else (cell_reason or "not_valid_for_cell_fit"),
                    "shell_fit_gate_reason": "" if valid_for_shell else (shell_reason or "not_valid_for_shell_fit"),
                    "snapshot_window_s": "",
                    "env_temp_span_c": "",
                    "box_temp_span_c": "",
                    "cell_temp_span_c": "",
                    "shell_temp_span_c": "",
                    "source_point_dir": str(point_dir),
                    "point_tag": point_tag,
                    "point_title": row.get(POINT_TITLE_KEY) or point_dir.name,
                    "digital_thermometer_age_ms": ref_age_ms,
                    "cell_delta_from_ref_c": (
                        cell_temp_c - ref_temp_c if cell_temp_c is not None and ref_temp_c is not None else None
                    ),
                    "shell_delta_from_ref_c": (
                        shell_temp_c - ref_temp_c if shell_temp_c is not None and ref_temp_c is not None else None
                    ),
                    "excluded_device_id": excluded,
                }
            )

    return observations


def _fit_temperature_map(rows: Sequence[Mapping[str, Any]], raw_key: str) -> Dict[str, Any]:
    valid_key = "valid_for_cell_fit" if raw_key == "cell_temp_raw_c" else "valid_for_shell_fit"
    valid = [row for row in rows if row.get(valid_key)]
    return fit_temperature_compensation(
        [row.get(raw_key) for row in valid],
        [row.get("ref_temp_c") for row in valid],
        polynomial_order=3,
    )


def _predict_temperature(coefficients: Mapping[str, Any], raw_temp_c: float) -> float:
    return (
        float(coefficients.get("A", 0.0))
        + float(coefficients.get("B", 1.0)) * raw_temp_c
        + float(coefficients.get("C", 0.0)) * raw_temp_c * raw_temp_c
        + float(coefficients.get("D", 0.0)) * raw_temp_c * raw_temp_c * raw_temp_c
    )


def build_temperature_channel_summary(
    observations: Sequence[Mapping[str, Any]],
    *,
    target_device_ids: Sequence[str] = DEFAULT_TARGET_DEVICE_IDS,
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for device_id in [str(item).zfill(3) for item in target_device_ids]:
        device_rows = [row for row in observations if str(row.get("analyzer_id") or "").zfill(3) == device_id]
        if not device_rows:
            continue
        cell_valid = [row for row in device_rows if row.get("valid_for_cell_fit")]
        shell_valid = [row for row in device_rows if row.get("valid_for_shell_fit")]
        cell_deltas = [
            float(row["cell_delta_from_ref_c"])
            for row in cell_valid
            if row.get("cell_delta_from_ref_c") not in (None, "")
        ]
        shell_deltas = [
            float(row["shell_delta_from_ref_c"])
            for row in shell_valid
            if row.get("shell_delta_from_ref_c") not in (None, "")
        ]
        temp_setpoints = sorted(
            {
                float(row["temp_setpoint_c"])
                for row in cell_valid
                if row.get("temp_setpoint_c") not in (None, "")
            }
        )
        cell_fit = _fit_temperature_map(device_rows, "cell_temp_raw_c")
        shell_fit = _fit_temperature_map(device_rows, "shell_temp_raw_c")
        rows.append(
            {
                "analyzer_id": device_id,
                "cell_valid_points": len(cell_valid),
                "shell_valid_points": len(shell_valid),
                "distinct_temp_setpoints": ";".join(f"{item:g}" for item in temp_setpoints),
                "cell_delta_mean_c": float(np.mean(cell_deltas)) if cell_deltas else "",
                "cell_delta_min_c": float(np.min(cell_deltas)) if cell_deltas else "",
                "cell_delta_max_c": float(np.max(cell_deltas)) if cell_deltas else "",
                "shell_delta_mean_c": float(np.mean(shell_deltas)) if shell_deltas else "",
                "shell_delta_min_c": float(np.min(shell_deltas)) if shell_deltas else "",
                "shell_delta_max_c": float(np.max(shell_deltas)) if shell_deltas else "",
                "cell_fit_rmse_c": cell_fit.get("rmse"),
                "cell_fit_max_abs_error_c": cell_fit.get("max_abs_error"),
                "shell_fit_rmse_c": shell_fit.get("rmse"),
                "shell_fit_max_abs_error_c": shell_fit.get("max_abs_error"),
                "coverage_status": "pass_0_to_40_only" if temp_setpoints == [0.0, 10.0, 20.0, 30.0, 40.0] else "review",
                "physical_note": (
                    "Digital-thermometer evidence covers 0..40 C H2O run only; "
                    "negative CO2 temperature groups need separate reference evidence before full-range temperature writes."
                ),
            }
        )
    return rows


def evaluate_co2_residual_temperature_impact(
    residual_csv: Path,
    observations: Sequence[Mapping[str, Any]],
    *,
    target_device_ids: Sequence[str] = DEFAULT_TARGET_DEVICE_IDS,
) -> List[Dict[str, Any]]:
    """Compare raw-T and corrected-T offline CO2 fits using existing residual rows."""

    by_device: Dict[str, List[Mapping[str, Any]]] = {}
    for row in observations:
        device_id = str(row.get("analyzer_id") or "").zfill(3)
        if device_id:
            by_device.setdefault(device_id, []).append(row)

    temp_fits = {
        device_id: _fit_temperature_map(rows, "cell_temp_raw_c")
        for device_id, rows in by_device.items()
    }
    supported_ranges: Dict[str, tuple[float, float]] = {}
    for device_id, rows in by_device.items():
        raw_values = [
            float(row["cell_temp_raw_c"])
            for row in rows
            if row.get("valid_for_cell_fit") and row.get("cell_temp_raw_c") not in (None, "")
        ]
        if raw_values:
            supported_ranges[device_id] = (min(raw_values), max(raw_values))

    residual_rows: List[Dict[str, Any]] = []
    with Path(residual_csv).open("r", encoding="utf-8-sig", newline="") as handle:
        for row in csv.DictReader(handle):
            if row.get("component") not in (None, "", "co2"):
                continue
            if row.get("residual_role") not in (None, "", "fit"):
                continue
            device_id = str(row.get("analyzer_device_id") or row.get("analyzer_id") or "").zfill(3)
            if device_id not in {str(item).zfill(3) for item in target_device_ids}:
                continue
            try:
                residual_rows.append(
                    {
                        "analyzer_id": device_id,
                        "target_value": float(row["target_value"]),
                        "ratio": float(row["ratio"]),
                        "temperature_c": float(row["temperature_c"]),
                    }
                )
            except Exception:
                continue

    out: List[Dict[str, Any]] = []
    for device_id in [str(item).zfill(3) for item in target_device_ids]:
        rows = [row for row in residual_rows if row["analyzer_id"] == device_id]
        if not rows:
            continue
        for subset_name, subset_rows in (
            ("all_rows_with_extrapolation", rows),
            (
                "supported_temperature_range_only",
                [
                    row
                    for row in rows
                    if device_id in supported_ranges
                    and supported_ranges[device_id][0] <= row["temperature_c"] <= supported_ranges[device_id][1]
                ],
            ),
        ):
            for temp_mode in ("raw_internal_temperature", "candidate_corrected_temperature"):
                stats = _least_squares_co2_stats(
                    subset_rows,
                    temp_fit=temp_fits.get(device_id),
                    use_corrected_temperature=temp_mode == "candidate_corrected_temperature",
                )
                out.append(
                    {
                        "analyzer_id": device_id,
                        "subset": subset_name,
                        "temperature_mode": temp_mode,
                        "sample_count": stats["sample_count"],
                        "rmse_ppm": stats["rmse_ppm"],
                        "max_abs_error_ppm": stats["max_abs_error_ppm"],
                        "note": stats["note"],
                    }
                )
    return out


def _least_squares_co2_stats(
    rows: Sequence[Mapping[str, Any]],
    *,
    temp_fit: Mapping[str, Any] | None,
    use_corrected_temperature: bool,
) -> Dict[str, Any]:
    if len(rows) < 7:
        return {
            "sample_count": len(rows),
            "rmse_ppm": "",
            "max_abs_error_ppm": "",
            "note": "insufficient_rows",
        }

    x_rows: List[List[float]] = []
    y_values: List[float] = []
    for row in rows:
        ratio = float(row["ratio"])
        temp = float(row["temperature_c"])
        if use_corrected_temperature:
            if not temp_fit or not temp_fit.get("fit_ok"):
                continue
            temp = _predict_temperature(temp_fit, temp)
        x_rows.append([1.0, ratio, ratio**2, ratio**3, temp, temp**2, ratio * temp])
        y_values.append(float(row["target_value"]))

    if len(x_rows) < 7:
        return {
            "sample_count": len(x_rows),
            "rmse_ppm": "",
            "max_abs_error_ppm": "",
            "note": "insufficient_rows_after_temperature_mapping",
        }
    x = np.asarray(x_rows, dtype=float)
    y = np.asarray(y_values, dtype=float)
    coeffs = np.linalg.lstsq(x, y, rcond=None)[0]
    residuals = x @ coeffs - y
    return {
        "sample_count": len(x_rows),
        "rmse_ppm": float(np.sqrt(np.mean(residuals**2))),
        "max_abs_error_ppm": float(np.max(np.abs(residuals))),
        "note": "diagnostic_only_not_firmware_write_model",
    }


def export_temperature_channel_review(
    output_dir: Path,
    *,
    h2o_points_parent: Path,
    co2_residual_csv: Path | None = None,
    target_device_ids: Sequence[str] = DEFAULT_TARGET_DEVICE_IDS,
    excluded_device_ids: Sequence[str] = DEFAULT_EXCLUDED_DEVICE_IDS,
    export_commands: bool = True,
) -> Dict[str, Any]:
    point_dirs = sorted(path for path in Path(h2o_points_parent).glob("p*_h2o") if path.is_dir())
    observations = build_temperature_observations_from_point_dirs(
        point_dirs,
        target_device_ids=target_device_ids,
        excluded_device_ids=excluded_device_ids,
    )
    output_dir.mkdir(parents=True, exist_ok=True)
    temp_bundle = export_temperature_compensation_artifacts(
        output_dir,
        observations,
        polynomial_order=3,
        export_commands=export_commands,
    )
    summary_rows = build_temperature_channel_summary(
        observations,
        target_device_ids=target_device_ids,
    )
    summary_csv = output_dir / "temperature_channel_summary.csv"
    _write_dicts(summary_csv, summary_rows)

    impact_rows: List[Dict[str, Any]] = []
    impact_csv = output_dir / "co2_residual_temperature_impact.csv"
    if co2_residual_csv and Path(co2_residual_csv).exists():
        impact_rows = evaluate_co2_residual_temperature_impact(
            Path(co2_residual_csv),
            observations,
            target_device_ids=target_device_ids,
        )
        _write_dicts(impact_csv, impact_rows)
    else:
        impact_csv.write_text("", encoding="utf-8")

    report_path = output_dir / "temperature_channel_review.md"
    report_path.write_text(
        _render_markdown_report(
            summary_rows,
            impact_rows,
            h2o_points_parent=Path(h2o_points_parent),
            co2_residual_csv=Path(co2_residual_csv) if co2_residual_csv else None,
        ),
        encoding="utf-8",
    )

    paths = dict(temp_bundle["paths"])
    paths.update(
        {
            "summary_csv": summary_csv,
            "co2_residual_temperature_impact_csv": impact_csv,
            "report": report_path,
        }
    )
    return {
        "observations": observations,
        "temperature_results": temp_bundle["results"],
        "summary_rows": summary_rows,
        "impact_rows": impact_rows,
        "paths": paths,
    }


def _write_dicts(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    fieldnames: List[str] = []
    for row in rows:
        for key in row.keys():
            if key not in fieldnames:
                fieldnames.append(str(key))
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(dict(row))


def _render_markdown_report(
    summary_rows: Sequence[Mapping[str, Any]],
    impact_rows: Sequence[Mapping[str, Any]],
    *,
    h2o_points_parent: Path,
    co2_residual_csv: Path | None,
) -> str:
    lines = [
        "# V1.5 Temperature Channel Review",
        "",
        "This is an offline no-write review. It does not open COM ports, does not write SENCO, and does not control gas or water routes.",
        "",
        "## Physical Meaning",
        "",
        "- SENCO7 is the analyzer chamber/cell temperature input compensation.",
        "- SENCO8 is the analyzer case/shell temperature input compensation.",
        "- CO2 SENCO1/3 and H2O SENCO2/4 use temperature as a model input, so the temperature input must be validated independently.",
        "- The H2O full-temperature run provides digital-thermometer evidence for 0..40 C. It does not cover the negative CO2 groups.",
        "",
        f"H2O evidence parent: `{h2o_points_parent}`",
    ]
    if co2_residual_csv:
        lines.append(f"CO2 residual input: `{co2_residual_csv}`")
    lines.extend(["", "## Temperature Summary", ""])
    lines.append("| analyzer | cell valid | shell valid | temp setpoints | cell delta mean C | cell delta min/max C | shell delta mean C | shell delta min/max C | note |")
    lines.append("|---|---:|---:|---|---:|---|---:|---|---|")
    for row in summary_rows:
        lines.append(
            "| {analyzer} | {cell_valid} | {shell_valid} | {setpoints} | {cell_mean} | {cell_min}/{cell_max} | {shell_mean} | {shell_min}/{shell_max} | {coverage} |".format(
                analyzer=_fmt_value(row.get("analyzer_id")),
                cell_valid=_fmt_value(row.get("cell_valid_points")),
                shell_valid=_fmt_value(row.get("shell_valid_points")),
                setpoints=_fmt_value(row.get("distinct_temp_setpoints")),
                cell_mean=_fmt_number(row.get("cell_delta_mean_c")),
                cell_min=_fmt_number(row.get("cell_delta_min_c")),
                cell_max=_fmt_number(row.get("cell_delta_max_c")),
                shell_mean=_fmt_number(row.get("shell_delta_mean_c")),
                shell_min=_fmt_number(row.get("shell_delta_min_c")),
                shell_max=_fmt_number(row.get("shell_delta_max_c")),
                coverage=_fmt_value(row.get("coverage_status")),
            )
        )
    if impact_rows:
        lines.extend(["", "## CO2 Residual Temperature Impact", ""])
        lines.append("| analyzer | subset | temperature mode | n | RMSE ppm | max abs ppm | note |")
        lines.append("|---|---|---|---:|---:|---:|---|")
        for row in impact_rows:
            lines.append(
                "| {analyzer_id} | {subset} | {temperature_mode} | {sample_count} | {rmse_ppm} | {max_abs_error_ppm} | {note} |".format(
                    analyzer_id=_fmt_value(row.get("analyzer_id")),
                    subset=_fmt_value(row.get("subset")),
                    temperature_mode=_fmt_value(row.get("temperature_mode")),
                    sample_count=_fmt_value(row.get("sample_count")),
                    rmse_ppm=_fmt_number(row.get("rmse_ppm")),
                    max_abs_error_ppm=_fmt_number(row.get("max_abs_error_ppm")),
                    note=_fmt_value(row.get("note")),
                )
            )
    lines.extend(
        [
            "",
            "## Review Conclusion",
            "",
            "- Temperature-channel calibration is physically relevant and should be reviewed before final CO2/H2O coefficient approval.",
            "- The current H2O-derived evidence can generate SENCO7/SENCO8 candidates for 0..40 C, but it should be treated as partial-range evidence for the full CO2 plan because -20 C and -10 C were not covered by the digital thermometer in this extraction.",
            "- If the CO2 residual impact table shows little improvement after candidate-corrected temperature refit, the remaining CO2 error should not be blamed primarily on simple chamber-temperature offset. Continue with ratio/zero/route/model residual analysis.",
            "- Any live SENCO7/SENCO8 write still requires a controlled write plan, old GETCO7/8 backup, readback, and post-write verification.",
        ]
    )
    return "\n".join(lines) + "\n"


def _fmt_value(value: Any) -> Any:
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return ""
        return f"{value:.3f}"
    if value in (None, ""):
        return ""
    return value


def _fmt_number(value: Any) -> str:
    numeric = _safe_float(value)
    if numeric is None:
        return ""
    return f"{numeric:.3f}"
