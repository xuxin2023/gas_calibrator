"""Export helpers for temperature compensation observations and fitted coefficients."""

from __future__ import annotations

import csv
import os
import tempfile
from pathlib import Path
from typing import Any, Dict, Iterable, List, Sequence

from openpyxl import Workbook

from ..calibration.temperature_compensation_fit import (
    fit_temperature_compensation,
    format_senco_coeffs,
)


OBSERVATION_FIELDS: List[str] = [
    "snapshot_time",
    "timestamp",
    "analyzer_id",
    "analyzer_device_id",
    "temp_setpoint_c",
    "temperature_setpoint_c",
    "chamber_temperature_box_c",
    "chamber_temperature_env_c",
    "ref_temp_c",
    "ref_temp_source",
    "cell_temp_raw_c",
    "shell_temp_raw_c",
    "analyzer_cell_temp_raw_c",
    "analyzer_shell_temp_raw_c",
    "route_type",
    "is_temp_calibration_snapshot",
    "valid_for_cell_fit",
    "valid_for_shell_fit",
    "cell_fit_gate_reason",
    "shell_fit_gate_reason",
    "snapshot_window_s",
    "env_temp_span_c",
    "box_temp_span_c",
    "cell_temp_span_c",
    "shell_temp_span_c",
]

RESULT_FIELDS: List[str] = [
    "analyzer_id",
    "fit_type",
    "senco_channel",
    "ref_temp_source",
    "n_points",
    "fit_ok",
    "availability",
    "polynomial_degree_used",
    "rmse",
    "max_abs_error",
    "A",
    "B",
    "C",
    "D",
    "command_string",
]


def _mean(values: Iterable[Any]) -> float | None:
    numbers: list[float] = []
    for value in values:
        try:
            number = float(value)
        except Exception:
            continue
        numbers.append(number)
    if not numbers:
        return None
    return sum(numbers) / float(len(numbers))


def _unique_sources(rows: Sequence[Dict[str, Any]], valid_key: str) -> str:
    sources = sorted(
        {
            str(row.get("ref_temp_source") or "").strip().lower()
            for row in rows
            if row.get(valid_key) and str(row.get("ref_temp_source") or "").strip()
        }
    )
    if not sources:
        return "none"
    if len(sources) == 1:
        return sources[0]
    return "mixed"


def _build_command_string(senco_channel: str, coeffs: Sequence[Any], *, export_commands: bool) -> str:
    if not export_commands:
        return ""
    a_str, b_str, c_str, d_str = format_senco_coeffs(coeffs)
    return f"{senco_channel},YGAS,FFF,{a_str},{b_str},{c_str},{d_str}"


def _build_fit_result(
    analyzer_id: str,
    fit_type: str,
    rows: Sequence[Dict[str, Any]],
    *,
    export_commands: bool,
    polynomial_order: int,
) -> Dict[str, Any]:
    valid_key = "valid_for_cell_fit" if fit_type == "cell" else "valid_for_shell_fit"
    temp_key = "cell_temp_raw_c" if fit_type == "cell" else "shell_temp_raw_c"
    senco_channel = "SENCO7" if fit_type == "cell" else "SENCO8"

    valid_rows = [row for row in rows if row.get(valid_key)]
    availability = "available" if valid_rows else "unavailable"
    ref_source = _unique_sources(rows, valid_key)
    raw_temps = [row.get(temp_key) for row in valid_rows]
    ref_temps = [row.get("ref_temp_c") for row in valid_rows]
    fit_result = fit_temperature_compensation(raw_temps, ref_temps, polynomial_order=polynomial_order)
    coeffs = (
        fit_result["A"],
        fit_result["B"],
        fit_result["C"],
        fit_result["D"],
    )
    command_string = _build_command_string(
        senco_channel,
        coeffs,
        export_commands=export_commands and availability == "available",
    )
    return {
        "analyzer_id": analyzer_id,
        "fit_type": fit_type,
        "senco_channel": senco_channel,
        "ref_temp_source": ref_source,
        "n_points": int(fit_result["n_points"]),
        "fit_ok": bool(fit_result["fit_ok"]),
        "availability": availability,
        "polynomial_degree_used": int(fit_result["polynomial_degree_used"]),
        "rmse": fit_result["rmse"],
        "max_abs_error": fit_result["max_abs_error"],
        "A": float(fit_result["A"]),
        "B": float(fit_result["B"]),
        "C": float(fit_result["C"]),
        "D": float(fit_result["D"]),
        "command_string": command_string,
    }


def build_temperature_compensation_results(
    observations: Sequence[Dict[str, Any]],
    *,
    polynomial_order: int,
    export_commands: bool,
) -> List[Dict[str, Any]]:
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for row in observations:
        analyzer_id = str(row.get("analyzer_id") or "").strip()
        if not analyzer_id:
            continue
        grouped.setdefault(analyzer_id, []).append(dict(row))

    results: List[Dict[str, Any]] = []
    for analyzer_id in sorted(grouped.keys()):
        rows = grouped[analyzer_id]
        results.append(
            _build_fit_result(
                analyzer_id,
                "cell",
                rows,
                export_commands=export_commands,
                polynomial_order=polynomial_order,
            )
        )
        results.append(
            _build_fit_result(
                analyzer_id,
                "shell",
                rows,
                export_commands=export_commands,
                polynomial_order=polynomial_order,
            )
        )
    return results


def _write_csv(path: Path, rows: Sequence[Dict[str, Any]], fieldnames: Sequence[str]) -> None:
    """Write CSV atomically using temp file + os.replace to prevent truncation on crash."""
    fd, tmp_path = tempfile.mkstemp(suffix=".csv", dir=path.parent)
    os.close(fd)
    tmp = Path(tmp_path)
    try:
        with tmp.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(fieldnames), extrasaction="ignore")
            writer.writeheader()
            for row in rows:
                writer.writerow(row)
        os.replace(tmp, path)
    except Exception:
        try:
            tmp.unlink(missing_ok=True)
        except Exception:
            pass
        raise


def _append_sheet(wb: Workbook, name: str, rows: Sequence[Dict[str, Any]], fieldnames: Sequence[str]) -> None:
    ws = wb.create_sheet(title=name[:31])
    ws.append(list(fieldnames))
    for row in rows:
        ws.append([row.get(field) for field in fieldnames])


def export_temperature_compensation_artifacts(
    run_dir: Path,
    observations: Sequence[Dict[str, Any]],
    *,
    polynomial_order: int = 3,
    export_commands: bool = True,
) -> Dict[str, Any]:
    run_dir.mkdir(parents=True, exist_ok=True)
    observations_rows = [dict(row) for row in observations]
    results = build_temperature_compensation_results(
        observations_rows,
        polynomial_order=polynomial_order,
        export_commands=export_commands,
    )

    observations_csv = run_dir / "temperature_calibration_observations.csv"
    results_csv = run_dir / "temperature_compensation_coefficients.csv"
    commands_txt = run_dir / "temperature_compensation_commands.txt"
    workbook_path = run_dir / "temperature_compensation.xlsx"

    _write_csv(observations_csv, observations_rows, OBSERVATION_FIELDS)
    _write_csv(results_csv, results, RESULT_FIELDS)

    commands = [str(row.get("command_string") or "").strip() for row in results if str(row.get("command_string") or "").strip()]
    # Atomic write for commands text
    fd, tmp_cmd = tempfile.mkstemp(suffix=".txt", dir=run_dir)
    os.close(fd)
    try:
        Path(tmp_cmd).write_text("\n".join(commands), encoding="utf-8")
        os.replace(tmp_cmd, commands_txt)
    except Exception:
        try:
            Path(tmp_cmd).unlink(missing_ok=True)
        except Exception:
            pass
        raise

    # Atomic write for workbook
    fd, tmp_wb = tempfile.mkstemp(suffix=".xlsx", dir=run_dir)
    os.close(fd)
    tmp_wb_path = Path(tmp_wb)
    try:
        wb = Workbook()
        wb.remove(wb.active)
        _append_sheet(wb, "温度校准观测", observations_rows, OBSERVATION_FIELDS)
        _append_sheet(wb, "温度补偿系数", results, RESULT_FIELDS)
        wb.save(tmp_wb_path)
        wb.close()
        os.replace(tmp_wb_path, workbook_path)
    except Exception:
        try:
            tmp_wb_path.unlink(missing_ok=True)
        except Exception:
            pass
        raise

    return {
        "observations": observations_rows,
        "results": results,
        "paths": {
            "observations_csv": observations_csv,
            "results_csv": results_csv,
            "commands_txt": commands_txt,
            "workbook": workbook_path,
        },
    }
