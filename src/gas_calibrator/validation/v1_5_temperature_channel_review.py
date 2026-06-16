"""Offline V1.5 temperature-channel review from existing open-flow evidence."""

from __future__ import annotations

import csv
import json
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

ZH_SAMPLE_TIME_KEY = "\u91c7\u6837\u65f6\u95f4"
ZH_TEMP_SET_KEY = "\u6e29\u7bb1\u8bbe\u5b9a\u6e29\u5ea6C"
ZH_ANALYZER_PREFIX = "\u6c14\u4f53\u5206\u6790\u4eea"
ZH_DEVICE_ID_SUFFIX = "\u8bbe\u5907ID"
ZH_CELL_TEMP_SUFFIX = "\u6e29\u5ea6\u7bb1\u6e29\u5ea6C"
ZH_SHELL_TEMP_SUFFIX = "\u673a\u58f3\u6e29\u5ea6C"
ZH_ANALYZER_PRESSURE_SUFFIX = "\u5206\u6790\u4eea\u538b\u529bkPa"
ZH_ANALYZER_CACHE_AGE_SUFFIX = "\u5206\u6790\u4eea\u7f13\u5b58\u5e74\u9f84ms"
ZH_DIGITAL_THERMOMETER_TEMP_KEY = "\u6570\u5b57\u6e29\u5ea6\u8ba1\u6e29\u5ea6C"
ZH_DIGITAL_THERMOMETER_AGE_KEY = "\u6570\u5b57\u6e29\u5ea6\u8ba1\u7f13\u5b58\u5e74\u9f84ms"
ZH_CHAMBER_TEMP_KEY = "\u6e29\u5ea6\u7bb1\u73af\u5883\u6e29\u5ea6C"


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


def _mean(values: Iterable[Any]) -> float | None:
    numbers: list[float] = []
    for value in values:
        numeric = _safe_float(value)
        if numeric is not None:
            numbers.append(numeric)
    if not numbers:
        return None
    return float(sum(numbers) / len(numbers))


def _min(values: Iterable[Any]) -> float | None:
    numbers = [number for number in (_safe_float(value) for value in values) if number is not None]
    return float(min(numbers)) if numbers else None


def _max(values: Iterable[Any]) -> float | None:
    numbers = [number for number in (_safe_float(value) for value in values) if number is not None]
    return float(max(numbers)) if numbers else None


def _read_json(path: Path) -> Dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _summary_metric(summary_payload: Mapping[str, Any], metric: str, key: str = "mean") -> float | None:
    for row in summary_payload.get("summary") or []:
        if str(row.get("metric") or "") == metric:
            return _safe_float(row.get(key))
    return None


def _slot_column(slot: int, suffix: str) -> str:
    return f"{ZH_ANALYZER_PREFIX}{slot}_{suffix}"


def _ga_column(slot: int, suffix: str) -> str:
    return f"ga{slot:02d}_{suffix}"


def _first_existing_sample_csv(run_dir: Path) -> Path | None:
    candidates = sorted(Path(run_dir).glob("samples_*.csv"))
    if candidates:
        return candidates[0]
    candidates = sorted(Path(run_dir).glob("point_*_samples.csv"))
    return candidates[0] if candidates else None


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


def build_temperature_observations_from_open_flow_point_dirs(
    point_dirs: Iterable[Path],
    *,
    target_device_ids: Sequence[str] = DEFAULT_TARGET_DEVICE_IDS,
    excluded_device_ids: Sequence[str] = DEFAULT_EXCLUDED_DEVICE_IDS,
    ref_temp_source: str = "digital_thermometer_from_co2_open_flow_samples",
    max_abs_delta_from_ref_c: float = 8.0,
    raw_temp_min_c: float = -35.0,
    raw_temp_max_c: float = 85.0,
    max_reference_age_ms: float | None = 5000.0,
) -> List[Dict[str, Any]]:
    """Extract SENCO7/8 observations from V1.5 open-flow sample artifacts.

    The CO2/H2O open-flow runners write one `samples_machine_readable.csv` per
    point. Each row freezes the digital thermometer, route pressure/dewpoint,
    and all active analyzer MODE2 frames at the same sampling instant. For
    temperature-channel review we use only existing evidence; this function does
    not open COM ports, control valves, or write coefficients.
    """

    target_ids = {str(item).zfill(3) for item in target_device_ids}
    excluded_ids = {str(item).zfill(3) for item in excluded_device_ids}
    requested_ids = target_ids | excluded_ids
    observations: List[Dict[str, Any]] = []

    for point_dir in sorted(Path(p) for p in point_dirs):
        if not point_dir.is_dir():
            continue
        samples_csv = point_dir / "samples_machine_readable.csv"
        if not samples_csv.exists():
            continue

        with samples_csv.open("r", encoding="utf-8-sig", newline="") as handle:
            rows = [dict(row) for row in csv.DictReader(handle)]
        if not rows:
            continue

        fresh_ref_rows = []
        for row in rows:
            ref_value = _safe_float(row.get("thermometer_temp_c"))
            if ref_value is None:
                continue
            ref_age_ms = _safe_float(row.get("thermometer_cache_age_ms"))
            if max_reference_age_ms is not None and ref_age_ms is not None and ref_age_ms > max_reference_age_ms:
                continue
            fresh_ref_rows.append(row)

        ref_rows = fresh_ref_rows or [row for row in rows if _safe_float(row.get("thermometer_temp_c")) is not None]
        ref_temp_c = _mean(row.get("thermometer_temp_c") for row in ref_rows)
        ref_age_ms = _mean(row.get("thermometer_cache_age_ms") for row in ref_rows)
        reference_fresh = bool(fresh_ref_rows)
        temp_setpoint_c = _mean(row.get("temp_set_c") for row in rows)
        snapshot_time = str(rows[0].get("sample_ts") or rows[0].get("sample_begin_ts") or "")
        point_tag = str(rows[0].get("point_tag") or point_dir.name)
        point_title = str(rows[0].get("point_title") or point_dir.name)
        route = str(rows[0].get("route") or rows[0].get("step") or "")
        route_type = f"{route or 'open'}_open_flow_full_temperature"

        slot_rows: Dict[str, Dict[str, Any]] = {}
        for slot in range(1, 17):
            device_values = [row.get(_ga_column(slot, "analyzer_device_id")) for row in rows]
            device_ids = [
                str(value).strip().zfill(3)
                for value in device_values
                if str(value or "").strip()
            ]
            if not device_ids:
                continue
            device_id = max(set(device_ids), key=device_ids.count)
            if requested_ids and device_id not in requested_ids:
                continue
            device_rows = [
                row
                for row in rows
                if str(row.get(_ga_column(slot, "analyzer_device_id")) or "").strip().zfill(3) == device_id
            ]
            if not device_rows:
                continue
            cell_values = [row.get(_ga_column(slot, "chamber_temp_c")) for row in device_rows]
            shell_values = [row.get(_ga_column(slot, "case_temp_c")) for row in device_rows]
            slot_rows[device_id] = {
                "slot": slot,
                "device_id": device_id,
                "cell_temp_c": _mean(cell_values),
                "shell_temp_c": _mean(shell_values),
                "cell_temp_min_c": _min(cell_values),
                "cell_temp_max_c": _max(cell_values),
                "shell_temp_min_c": _min(shell_values),
                "shell_temp_max_c": _max(shell_values),
                "pressure_kpa": _mean(row.get(_ga_column(slot, "pressure_kpa")) for row in device_rows),
                "max_cache_age_ms": _max(row.get(_ga_column(slot, "frame_cache_age_ms")) for row in device_rows),
                "usable_count": len(
                    [
                        row
                        for row in device_rows
                        if str(row.get(_ga_column(slot, "frame_usable")) or "").strip().lower() == "true"
                    ]
                ),
                "frame_count": len(device_rows),
            }

        for device_id in sorted(requested_ids | set(slot_rows.keys())):
            slot_payload = slot_rows.get(device_id, {})
            cell_temp_c = _safe_float(slot_payload.get("cell_temp_c"))
            shell_temp_c = _safe_float(slot_payload.get("shell_temp_c"))
            pressure_kpa = _safe_float(slot_payload.get("pressure_kpa"))
            cache_age_ms = _safe_float(slot_payload.get("max_cache_age_ms"))
            frame_count = int(slot_payload.get("frame_count") or 0)
            usable_count = int(slot_payload.get("usable_count") or 0)
            cell_min = _safe_float(slot_payload.get("cell_temp_min_c"))
            cell_max = _safe_float(slot_payload.get("cell_temp_max_c"))
            shell_min = _safe_float(slot_payload.get("shell_temp_min_c"))
            shell_max = _safe_float(slot_payload.get("shell_temp_max_c"))

            ref_age_ok = reference_fresh
            ref_age_reason = ""
            if not ref_age_ok:
                ref_age_reason = "reference_temperature_stale_or_missing"

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
            if frame_count <= 0:
                cell_ok = False
                shell_ok = False
                cell_reason = "missing_analyzer_temperature_evidence"
                shell_reason = "missing_analyzer_temperature_evidence"
            if excluded:
                if not cell_reason:
                    cell_reason = "excluded_device_id"
                if not shell_reason:
                    shell_reason = "excluded_device_id"
            if not ref_age_ok:
                if not cell_reason:
                    cell_reason = ref_age_reason
                if not shell_reason:
                    shell_reason = ref_age_reason

            valid_for_cell = bool(cell_ok and ref_age_ok and not excluded)
            valid_for_shell = bool(shell_ok and ref_age_ok and not excluded)
            observations.append(
                {
                    "snapshot_time": snapshot_time,
                    "timestamp": snapshot_time,
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
                    "route_type": route_type,
                    "is_temp_calibration_snapshot": True,
                    "valid_for_cell_fit": valid_for_cell,
                    "valid_for_shell_fit": valid_for_shell,
                    "cell_fit_gate_reason": "" if valid_for_cell else (cell_reason or "not_valid_for_cell_fit"),
                    "shell_fit_gate_reason": "" if valid_for_shell else (shell_reason or "not_valid_for_shell_fit"),
                    "snapshot_window_s": "",
                    "env_temp_span_c": "",
                    "box_temp_span_c": "",
                    "cell_temp_span_c": (
                        cell_max - cell_min if cell_min is not None and cell_max is not None else ""
                    ),
                    "shell_temp_span_c": (
                        shell_max - shell_min if shell_min is not None and shell_max is not None else ""
                    ),
                    "source_point_dir": str(point_dir),
                    "source_samples_csv": str(samples_csv),
                    "point_tag": point_tag,
                    "point_title": point_title,
                    "digital_thermometer_age_ms": ref_age_ms,
                    "configured_temp_setpoint_c": temp_setpoint_c,
                    "analyzer_pressure_kpa": pressure_kpa,
                    "analyzer_max_cache_age_ms": cache_age_ms,
                    "analyzer_frame_count": frame_count,
                    "analyzer_usable_frame_count": usable_count,
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


def build_temperature_observations_from_snapshot_run_dirs(
    snapshot_run_dirs: Iterable[Path],
    *,
    target_device_ids: Sequence[str] = DEFAULT_TARGET_DEVICE_IDS,
    excluded_device_ids: Sequence[str] = DEFAULT_EXCLUDED_DEVICE_IDS,
    ref_temp_source: str = "digital_thermometer_from_validate_dry_collect_snapshot",
    max_abs_delta_from_ref_c: float = 8.0,
    raw_temp_min_c: float = -35.0,
    raw_temp_max_c: float = 85.0,
    max_reference_age_ms: float | None = 5000.0,
) -> List[Dict[str, Any]]:
    """Extract temperature observations from validate_dry_collect snapshot runs.

    These runs are no-write evidence snapshots. They are useful for channel
    health and evidence-gap review, but one setpoint is not enough for a formal
    SENCO7/SENCO8 compensation fit.
    """

    target_ids = {str(item).zfill(3) for item in target_device_ids}
    excluded_ids = {str(item).zfill(3) for item in excluded_device_ids}
    requested_ids = target_ids | excluded_ids
    observations: List[Dict[str, Any]] = []

    for run_dir in sorted(Path(p) for p in snapshot_run_dirs):
        if not run_dir.is_dir():
            continue
        samples_csv = _first_existing_sample_csv(run_dir)
        if samples_csv is None:
            continue

        summary_payload = _read_json(run_dir / "temperature_evidence_from_io_summary.json")
        ref_temp_c = _summary_metric(summary_payload, "digital_thermometer_temp_c")
        chamber_temp_c = _summary_metric(summary_payload, "temperature_chamber_temp_c")

        rows: List[Dict[str, str]] = []
        with samples_csv.open("r", encoding="utf-8-sig", newline="") as handle:
            reader = csv.DictReader(handle)
            rows = [dict(row) for row in reader]
        if not rows:
            continue

        if ref_temp_c is None:
            ref_temp_c = _mean(row.get(ZH_DIGITAL_THERMOMETER_TEMP_KEY) for row in rows)
        ref_age_ms = _mean(row.get(ZH_DIGITAL_THERMOMETER_AGE_KEY) for row in rows)
        configured_temp_setpoint_c = _mean(row.get(ZH_TEMP_SET_KEY) for row in rows)
        temp_setpoint_c = chamber_temp_c if chamber_temp_c is not None else configured_temp_setpoint_c
        snapshot_time = str(rows[0].get(ZH_SAMPLE_TIME_KEY) or "")

        slot_rows: Dict[str, Dict[str, Any]] = {}
        for slot in range(1, 17):
            device_values = [row.get(_slot_column(slot, ZH_DEVICE_ID_SUFFIX)) for row in rows]
            device_ids = [
                str(value).strip().zfill(3)
                for value in device_values
                if str(value or "").strip()
            ]
            if not device_ids:
                continue
            device_id = max(set(device_ids), key=device_ids.count)
            if requested_ids and device_id not in requested_ids:
                continue
            slot_rows[device_id] = {
                "slot": slot,
                "device_id": device_id,
                "cell_temp_c": _mean(row.get(_slot_column(slot, ZH_CELL_TEMP_SUFFIX)) for row in rows),
                "shell_temp_c": _mean(row.get(_slot_column(slot, ZH_SHELL_TEMP_SUFFIX)) for row in rows),
                "pressure_kpa": _mean(row.get(_slot_column(slot, ZH_ANALYZER_PRESSURE_SUFFIX)) for row in rows),
                "max_cache_age_ms": _max(row.get(_slot_column(slot, ZH_ANALYZER_CACHE_AGE_SUFFIX)) for row in rows),
                "frame_count": len(
                    [
                        row
                        for row in rows
                        if str(row.get(_slot_column(slot, ZH_DEVICE_ID_SUFFIX)) or "").strip()
                    ]
                ),
            }

        for device_id in sorted(requested_ids | set(slot_rows.keys())):
            slot_payload = slot_rows.get(device_id, {})
            cell_temp_c = _safe_float(slot_payload.get("cell_temp_c"))
            shell_temp_c = _safe_float(slot_payload.get("shell_temp_c"))
            pressure_kpa = _safe_float(slot_payload.get("pressure_kpa"))
            cache_age_ms = _safe_float(slot_payload.get("max_cache_age_ms"))
            frame_count = int(slot_payload.get("frame_count") or 0)

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
            if frame_count <= 0:
                cell_ok = False
                shell_ok = False
                cell_reason = "missing_analyzer_temperature_evidence"
                shell_reason = "missing_analyzer_temperature_evidence"
            if excluded:
                if not cell_reason:
                    cell_reason = "excluded_device_id"
                if not shell_reason:
                    shell_reason = "excluded_device_id"
            if not ref_age_ok:
                if not cell_reason:
                    cell_reason = ref_age_reason
                if not shell_reason:
                    shell_reason = ref_age_reason

            valid_for_cell = bool(cell_ok and ref_age_ok and not excluded)
            valid_for_shell = bool(shell_ok and ref_age_ok and not excluded)
            observations.append(
                {
                    "snapshot_time": snapshot_time,
                    "timestamp": snapshot_time,
                    "analyzer_id": device_id,
                    "analyzer_device_id": device_id,
                    "temp_setpoint_c": temp_setpoint_c,
                    "temperature_setpoint_c": temp_setpoint_c,
                    "chamber_temperature_box_c": chamber_temp_c,
                    "chamber_temperature_env_c": ref_temp_c,
                    "ref_temp_c": ref_temp_c,
                    "ref_temp_source": ref_temp_source,
                    "cell_temp_raw_c": cell_temp_c,
                    "shell_temp_raw_c": shell_temp_c,
                    "analyzer_cell_temp_raw_c": cell_temp_c,
                    "analyzer_shell_temp_raw_c": shell_temp_c,
                    "route_type": "validate_dry_collect_temperature_snapshot",
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
                    "source_snapshot_run_dir": str(run_dir),
                    "source_samples_csv": str(samples_csv),
                    "point_tag": run_dir.name,
                    "point_title": run_dir.name,
                    "digital_thermometer_age_ms": ref_age_ms,
                    "configured_temp_setpoint_c": configured_temp_setpoint_c,
                    "analyzer_pressure_kpa": pressure_kpa,
                    "analyzer_max_cache_age_ms": cache_age_ms,
                    "analyzer_frame_count": frame_count,
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
        if not cell_valid and not shell_valid:
            coverage_status = "blocked_missing_analyzer_temperature_evidence"
            physical_note = (
                "Digital-thermometer/reference temperature exists, but no analyzer chamber/case "
                "temperature evidence was available for this device. Do not generate SENCO7/SENCO8 "
                "write candidates from this run."
            )
        elif len(temp_setpoints) < 3:
            coverage_status = "blocked_insufficient_temperature_setpoints"
            physical_note = (
                "Analyzer and reference temperature evidence exists, but fewer than three distinct "
                "temperature setpoints were available. This is suitable for a fast channel sanity "
                "check only; do not generate SENCO7/SENCO8 write candidates from this evidence."
            )
        elif temp_setpoints == [0.0, 10.0, 20.0, 30.0, 40.0]:
            coverage_status = "pass_0_to_40_only"
            physical_note = (
                "Digital-thermometer evidence covers 0..40 C H2O run only; "
                "negative CO2 temperature groups need separate reference evidence before full-range temperature writes."
            )
        else:
            coverage_status = "review"
            physical_note = (
                "Temperature evidence is partial or uneven. Review setpoint coverage and raw/reference "
                "temperature residuals before any SENCO7/SENCO8 write."
            )
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
                "coverage_status": coverage_status,
                "physical_note": physical_note,
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
    h2o_points_parent: Path | None = None,
    open_flow_points_parent: Path | None = None,
    snapshot_run_dirs: Sequence[Path] = (),
    co2_residual_csv: Path | None = None,
    target_device_ids: Sequence[str] = DEFAULT_TARGET_DEVICE_IDS,
    excluded_device_ids: Sequence[str] = DEFAULT_EXCLUDED_DEVICE_IDS,
    export_commands: bool = True,
) -> Dict[str, Any]:
    observations: List[Dict[str, Any]] = []
    if h2o_points_parent is not None:
        point_dirs = sorted(path for path in Path(h2o_points_parent).glob("p*_h2o") if path.is_dir())
        observations.extend(
            build_temperature_observations_from_point_dirs(
                point_dirs,
                target_device_ids=target_device_ids,
                excluded_device_ids=excluded_device_ids,
            )
        )
    if open_flow_points_parent is not None:
        point_dirs = sorted(
            path
            for path in Path(open_flow_points_parent).glob("p*")
            if path.is_dir() and (path / "samples_machine_readable.csv").exists()
        )
        observations.extend(
            build_temperature_observations_from_open_flow_point_dirs(
                point_dirs,
                target_device_ids=target_device_ids,
                excluded_device_ids=excluded_device_ids,
            )
        )
    if snapshot_run_dirs:
        observations.extend(
            build_temperature_observations_from_snapshot_run_dirs(
                snapshot_run_dirs,
                target_device_ids=target_device_ids,
                excluded_device_ids=excluded_device_ids,
            )
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
            h2o_points_parent=Path(h2o_points_parent) if h2o_points_parent is not None else None,
            open_flow_points_parent=Path(open_flow_points_parent) if open_flow_points_parent is not None else None,
            snapshot_run_dirs=[Path(item) for item in snapshot_run_dirs],
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
    h2o_points_parent: Path | None,
    open_flow_points_parent: Path | None,
    snapshot_run_dirs: Sequence[Path] = (),
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
        "- Open-flow CO2/H2O point samples can provide digital-thermometer evidence without opening COM ports or re-running routes.",
        "",
    ]
    if h2o_points_parent is not None:
        lines.append(f"H2O evidence parent: `{h2o_points_parent}`")
    if open_flow_points_parent is not None:
        lines.append(f"Open-flow evidence parent: `{open_flow_points_parent}`")
    if snapshot_run_dirs:
        lines.append("Snapshot evidence run dirs:")
        for run_dir in snapshot_run_dirs:
            lines.append(f"- `{run_dir}`")
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
    has_any_valid_temperature_evidence = any(
        int(row.get("cell_valid_points") or 0) > 0 or int(row.get("shell_valid_points") or 0) > 0
        for row in summary_rows
    )
    has_candidate_grade_temperature_evidence = any(
        str(row.get("coverage_status") or "") in {"pass_0_to_40_only", "review"}
        and int(row.get("cell_valid_points") or 0) >= 3
        and int(row.get("shell_valid_points") or 0) >= 3
        for row in summary_rows
    )
    if has_candidate_grade_temperature_evidence:
        evidence_conclusion = (
            "- The current extracted evidence can generate SENCO7/SENCO8 candidates for devices with non-zero "
            "valid cell/shell points. Confirm the covered setpoints match the intended CO2/H2O temperature range "
            "before any controlled live write."
        )
    elif has_any_valid_temperature_evidence:
        evidence_conclusion = (
            "- The current extracted evidence contains valid analyzer/reference temperature pairs, but the temperature "
            "setpoint coverage is insufficient for formal SENCO7/SENCO8 fitting. Treat it as channel sanity evidence "
            "and collect a multi-temperature evidence set before any write."
        )
    else:
        evidence_conclusion = (
            "- The current extracted evidence has zero valid analyzer chamber/case temperature points for the requested "
            "target devices. Do not generate or write SENCO7/SENCO8 candidates from this run."
        )
    lines.extend(
        [
            "",
            "## Review Conclusion",
            "",
            "- Temperature-channel calibration is physically relevant and should be reviewed before final CO2/H2O coefficient approval.",
            evidence_conclusion,
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
