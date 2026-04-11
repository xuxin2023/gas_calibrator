from __future__ import annotations

import csv
import json
import math
import time
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

import pandas as pd
from openpyxl import load_workbook

from ..config import load_config
from ..devices.gas_analyzer import GasAnalyzer
from ..export import build_corrected_water_points_report
from ..senco_format import format_senco_values, senco_readback_matches
from .run_v1_no500_postprocess import _filter_no_500_frame

_PRESSURE_WRITE_MIN_GAUGE_CONTROLLER_OVERLAP = 5
_PRESSURE_WRITE_MAX_GAUGE_CONTROLLER_MEAN_ABS_HPA = 3.0
_PRESSURE_WRITE_MAX_GAUGE_CONTROLLER_MAX_ABS_HPA = 8.0


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


def _safe_bool(value: Any) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "y", "on", "ok"}


def _normalize_device_id(value: Any) -> str:
    text = str(value or "").strip().upper()
    if not text:
        return ""
    if text.isdigit():
        return f"{int(text):03d}"
    return text


def _normalize_analyzer(value: Any, *, index: Optional[int] = None) -> str:
    text = str(value or "").strip().upper()
    if text:
        return text
    if index is not None:
        return f"GA{int(index):02d}"
    return ""


def _latest_matching(run_dir: Path, pattern: str) -> Optional[Path]:
    matches = [path for path in run_dir.glob(pattern) if path.is_file()]
    if not matches:
        return None
    return max(matches, key=lambda item: item.stat().st_mtime)


def _resolve_summary_paths(run_dir: Path) -> List[Path]:
    gas = _latest_matching(run_dir, "分析仪汇总_气路_*.xlsx") or _latest_matching(run_dir, "分析仪汇总_气路_*.csv")
    water = _latest_matching(run_dir, "分析仪汇总_水路_*.xlsx") or _latest_matching(run_dir, "分析仪汇总_水路_*.csv")
    if gas and water:
        return [gas, water]
    raise FileNotFoundError(f"未找到气路/水路汇总文件: {run_dir}")


def _resolve_samples_path(run_dir: Path) -> Path:
    path = _latest_matching(run_dir, "samples_*.csv") or (run_dir / "samples.csv")
    if path is None or not path.exists():
        raise FileNotFoundError(f"未找到 samples csv: {run_dir}")
    return path


def _write_csv(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    header: List[str] = []
    for row in rows:
        for key in row.keys():
            if key not in header:
                header.append(str(key))
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=header)
        writer.writeheader()
        for row in rows:
            writer.writerow(dict(row))


def _append_dataframe_sheet(workbook_path: Path, sheet_name: str, frame: pd.DataFrame) -> None:
    wb = load_workbook(workbook_path)
    try:
        if sheet_name in wb.sheetnames:
            del wb[sheet_name]
        ws = wb.create_sheet(title=sheet_name[:31])
        if frame.empty:
            ws.append(["说明"])
            ws.append(["无数据"])
        else:
            ws.append(list(frame.columns))
            for row in frame.itertuples(index=False, name=None):
                ws.append(list(row))
        wb.save(workbook_path)
    finally:
        wb.close()


def _filter_no_500_summary_paths(run_dir: Path, output_dir: Path) -> tuple[List[Path], List[Dict[str, Any]]]:
    filtered_paths: List[Path] = []
    stats_rows: List[Dict[str, Any]] = []
    for source_path in _resolve_summary_paths(run_dir):
        if source_path.suffix.lower() == ".csv":
            frame = pd.read_csv(source_path, encoding="utf-8-sig")
            filtered_frame, stats = _filter_no_500_frame(frame)
            out_path = output_dir / f"{source_path.stem}_no_500hpa.csv"
            filtered_frame.to_csv(out_path, index=False, encoding="utf-8-sig")
        else:
            workbook = pd.read_excel(source_path, sheet_name=None)
            sheets: Dict[str, pd.DataFrame] = {}
            original_rows = 0
            removed_rows = 0
            kept_rows = 0
            for ws_name, frame in workbook.items():
                filtered_frame, one_stats = _filter_no_500_frame(frame)
                sheets[str(ws_name)] = filtered_frame
                original_rows += int(one_stats["original_rows"])
                removed_rows += int(one_stats["removed_rows"])
                kept_rows += int(one_stats["kept_rows"])
            out_path = output_dir / f"{source_path.stem}_no_500hpa.xlsx"
            with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
                for ws_name, frame in sheets.items():
                    frame.to_excel(writer, sheet_name=str(ws_name)[:31], index=False)
            stats = {"original_rows": original_rows, "removed_rows": removed_rows, "kept_rows": kept_rows}
        filtered_paths.append(out_path)
        stats_rows.append({"source": str(source_path), "filtered": str(out_path), **stats})
    return filtered_paths, stats_rows


def extract_run_device_ids(run_dir: str | Path) -> Dict[str, str]:
    run_dir = Path(run_dir)
    frame = pd.read_csv(_resolve_samples_path(run_dir), encoding="utf-8-sig")
    mapping: Dict[str, str] = {}
    for index in range(1, 9):
        analyzer = f"GA{index:02d}"
        values: List[str] = []
        for column in (
            f"气体分析仪{index}_设备ID",
            f"gas_analyzer_{index}_device_id",
            f"ga{index:02d}_device_id",
        ):
            if column not in frame.columns:
                continue
            for value in frame[column].tolist():
                normalized = _normalize_device_id(value)
                if normalized:
                    values.append(normalized)
        if values:
            mapping[analyzer] = Counter(values).most_common(1)[0][0]
    return mapping


def _coeff_value(row: Mapping[str, Any], index: int) -> float:
    numeric = _safe_float(row.get(f"a{index}"))
    return 0.0 if numeric is None else float(numeric)


def build_corrected_download_plan_rows(simplified_frame: pd.DataFrame) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    if simplified_frame.empty:
        return rows
    for row in simplified_frame.to_dict(orient="records"):
        analyzer = _normalize_analyzer(row.get("分析仪"))
        gas = str(row.get("气体") or "").strip().upper()
        if not analyzer or gas not in {"CO2", "H2O"}:
            continue
        primary_group = 1 if gas == "CO2" else 2
        secondary_group = 3 if gas == "CO2" else 4
        primary_values = [_coeff_value(row, idx) for idx in range(4)] + [0.0, 0.0]
        secondary_values = [_coeff_value(row, idx) for idx in range(4, 9)] + [0.0]
        payload: Dict[str, Any] = {
            "Analyzer": analyzer,
            "Gas": gas,
            "ModeEnterCommand": "MODE,YGAS,FFF,2",
            "ModeExitCommand": "MODE,YGAS,FFF,1",
            "PrimarySENCO": str(primary_group),
            "PrimaryValues": ",".join(format_senco_values(primary_values)),
            "PrimaryCommand": f"SENCO{primary_group},YGAS,FFF," + ",".join(format_senco_values(primary_values)),
            "SecondarySENCO": str(secondary_group),
            "SecondaryValues": ",".join(format_senco_values(secondary_values)),
            "SecondaryCommand": f"SENCO{secondary_group},YGAS,FFF," + ",".join(format_senco_values(secondary_values)),
        }
        for idx, value in enumerate(primary_values):
            payload[f"PrimaryC{idx}"] = format_senco_values([value])[0]
        for idx, value in enumerate(secondary_values):
            payload[f"SecondaryC{idx}"] = format_senco_values([value])[0]
        for idx in range(9):
            payload[f"a{idx}"] = _coeff_value(row, idx)
        rows.append(payload)
    rows.sort(key=lambda item: (str(item.get("Analyzer") or ""), str(item.get("Gas") or "")))
    return rows


def _temperature_coefficients_path(run_dir: Path) -> Optional[Path]:
    direct = run_dir / "temperature_compensation_coefficients.csv"
    if direct.exists():
        return direct
    return _latest_matching(run_dir, "temperature_compensation_coefficients*.csv")


def load_temperature_coefficient_rows(run_dir: str | Path) -> List[Dict[str, Any]]:
    run_dir = Path(run_dir)
    path = _temperature_coefficients_path(run_dir)
    if path is None:
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def _latest_startup_pressure_calibration_summary(run_dir: Path) -> Optional[Path]:
    summaries = [
        path
        for path in run_dir.glob("startup_pressure_sensor_calibration_*/summary.csv")
        if path.is_file()
    ]
    if not summaries:
        return None
    return max(summaries, key=lambda item: item.stat().st_mtime)


def load_startup_pressure_calibration_rows(run_dir: str | Path) -> List[Dict[str, Any]]:
    run_dir = Path(run_dir)
    summary_path = _latest_startup_pressure_calibration_summary(run_dir)
    if summary_path is None:
        return []
    with summary_path.open("r", encoding="utf-8-sig", newline="") as handle:
        source_rows = list(csv.DictReader(handle))

    rows: List[Dict[str, Any]] = []
    for row in source_rows:
        analyzer = _normalize_analyzer(row.get("Analyzer"))
        device_id = _normalize_device_id(row.get("DeviceId"))
        offset = _safe_float(row.get("OffsetA_kPa"))
        if not analyzer or offset is None:
            continue
        rows.append(
            {
                "Analyzer": analyzer,
                "DeviceId": device_id,
                "ReferenceSource": "startup_pressure_sensor_calibration",
                "Samples": int(_safe_float(row.get("Samples")) or 0),
                "OffsetA_kPa": float(offset),
                "ResidualMeanAbs_kPa": "",
                "ResidualMaxAbs_kPa": "",
                "WriteApplied": row.get("WriteApplied", ""),
                "ReadbackOk": row.get("ReadbackOk", ""),
                "Status": row.get("Status", ""),
                "Error": row.get("Error", ""),
                "Command": "SENCO9,YGAS,FFF," + ",".join(format_senco_values((float(offset), 1.0, 0.0, 0.0))),
                "SourceSummary": str(summary_path),
            }
        )
    rows.sort(key=lambda item: str(item.get("Analyzer") or ""))
    return rows


def compute_pressure_offset_rows(
    run_dir: str | Path,
    *,
    fallback_to_controller: bool = False,
) -> List[Dict[str, Any]]:
    run_dir = Path(run_dir)
    frame = pd.read_csv(_resolve_samples_path(run_dir), encoding="utf-8-sig")
    pressure_mode_col = "压力执行模式" if "压力执行模式" in frame.columns else "PressureMode"
    gauge_candidates = [
        "数字压力计压力hPa",
        "pressure_gauge_hpa",
        "PressureGaugeHpa",
    ]
    controller_candidates = ["压力控制器压力hPa", "pressure_controller_hpa", "PressureControllerHpa"]
    gauge_col = next((column for column in gauge_candidates if column in frame.columns), None)
    controller_col = next((column for column in controller_candidates if column in frame.columns), None)
    ref_candidates = list(gauge_candidates)
    if fallback_to_controller:
        ref_candidates.extend(["压力控制器压力hPa", "pressure_controller_hpa", "PressureControllerHpa"])
    ref_col = next((column for column in ref_candidates if column in frame.columns), None)
    if ref_col is None:
        return []

    ambient_mask = (
        frame[pressure_mode_col].astype(str).str.strip().str.lower().eq("ambient_open")
        if pressure_mode_col in frame.columns
        else pd.Series([True] * len(frame))
    )
    rows: List[Dict[str, Any]] = []
    for index in range(1, 9):
        analyzer = f"GA{index:02d}"
        device_id_col = f"气体分析仪{index}_设备ID"
        pressure_col = f"气体分析仪{index}_分析仪压力kPa"
        usable_col = f"气体分析仪{index}_分析仪可用帧"
        if device_id_col not in frame.columns or pressure_col not in frame.columns:
            continue
        subset = frame.loc[ambient_mask].copy()
        if usable_col in subset.columns:
            subset = subset[subset[usable_col].map(_safe_bool)]
        subset["ref_hpa"] = pd.to_numeric(subset[ref_col], errors="coerce")
        subset["analyzer_kpa"] = pd.to_numeric(subset[pressure_col], errors="coerce")
        subset = subset.dropna(subset=["ref_hpa", "analyzer_kpa"])
        if subset.empty:
            continue
        device_ids = [_normalize_device_id(value) for value in subset[device_id_col].tolist() if _normalize_device_id(value)]
        if not device_ids:
            continue
        device_id = Counter(device_ids).most_common(1)[0][0]
        residuals = (subset["ref_hpa"] / 10.0) - subset["analyzer_kpa"]
        offset = float(residuals.mean())
        corrected = subset["analyzer_kpa"] + offset
        abs_error_kpa = ((subset["ref_hpa"] / 10.0) - corrected).abs()
        overlap_samples = 0
        overlap_mean_abs_hpa: float | str = ""
        overlap_max_abs_hpa: float | str = ""
        pressure_write_recommended = True
        pressure_write_reason = ""
        if ref_col == controller_col:
            pressure_write_recommended = False
            pressure_write_reason = "controller_reference_fallback_used"
        elif gauge_col and controller_col:
            overlap = subset.copy()
            overlap["gauge_hpa"] = pd.to_numeric(overlap[gauge_col], errors="coerce")
            overlap["controller_hpa"] = pd.to_numeric(overlap[controller_col], errors="coerce")
            overlap = overlap.dropna(subset=["gauge_hpa", "controller_hpa"])
            overlap_samples = int(len(overlap))
            if overlap_samples < _PRESSURE_WRITE_MIN_GAUGE_CONTROLLER_OVERLAP:
                pressure_write_recommended = False
                pressure_write_reason = "insufficient_gauge_controller_overlap"
            else:
                gauge_controller_diff_hpa = (overlap["gauge_hpa"] - overlap["controller_hpa"]).abs()
                overlap_mean_abs_hpa = float(gauge_controller_diff_hpa.mean())
                overlap_max_abs_hpa = float(gauge_controller_diff_hpa.max())
                if (
                    float(overlap_mean_abs_hpa) > _PRESSURE_WRITE_MAX_GAUGE_CONTROLLER_MEAN_ABS_HPA
                    or float(overlap_max_abs_hpa) > _PRESSURE_WRITE_MAX_GAUGE_CONTROLLER_MAX_ABS_HPA
                ):
                    pressure_write_recommended = False
                    pressure_write_reason = "ambient_open_backpressure_too_large"
        else:
            pressure_write_recommended = False
            pressure_write_reason = "insufficient_gauge_controller_overlap"
        rows.append(
            {
                "Analyzer": analyzer,
                "DeviceId": device_id,
                "ReferenceSource": ref_col,
                "Samples": int(len(subset)),
                "OffsetA_kPa": offset,
                "ResidualMeanAbs_kPa": float(abs_error_kpa.mean()),
                "ResidualMaxAbs_kPa": float(abs_error_kpa.max()),
                "GaugeControllerOverlapSamples": overlap_samples,
                "GaugeControllerMeanAbsDiff_hPa": overlap_mean_abs_hpa,
                "GaugeControllerMaxAbsDiff_hPa": overlap_max_abs_hpa,
                "PressureWriteRecommended": pressure_write_recommended,
                "PressureWriteReason": pressure_write_reason,
                "Command": "SENCO9,YGAS,FFF," + ",".join(format_senco_values((offset, 1.0, 0.0, 0.0))),
            }
        )
    rows.sort(key=lambda item: str(item.get("Analyzer") or ""))
    return rows


def _load_targets_from_cfg(cfg: Mapping[str, Any]) -> List[Dict[str, Any]]:
    devices_cfg = dict(cfg.get("devices", {}) or {})
    gas_cfg = devices_cfg.get("gas_analyzers")
    if not isinstance(gas_cfg, list) or not gas_cfg:
        single = devices_cfg.get("gas_analyzer")
        gas_cfg = [single] if isinstance(single, Mapping) else []
    defaults = dict(devices_cfg.get("gas_analyzer", {}) or {})
    targets: List[Dict[str, Any]] = []
    for idx, item in enumerate(gas_cfg, start=1):
        if not isinstance(item, Mapping) or not item.get("enabled", True):
            continue
        port = str(item.get("port") or "").strip()
        if not port:
            continue
        targets.append(
            {
                "Analyzer": _normalize_analyzer(item.get("name"), index=idx),
                "Port": port,
                "Baudrate": int(item.get("baud", item.get("baudrate", defaults.get("baud", 115200))) or 115200),
                "Timeout": float(item.get("timeout", defaults.get("timeout", 0.6)) or 0.6),
                "ConfiguredDeviceId": _normalize_device_id(item.get("device_id") or defaults.get("device_id") or "000"),
                "ActiveSend": bool(item.get("active_send", defaults.get("active_send", True))),
                "FtdHz": int(item.get("ftd_hz", defaults.get("ftd_hz", 1)) or 1),
                "AverageFilter": int(item.get("average_filter", defaults.get("average_filter", 49)) or 49),
            }
        )
    return targets


def scan_live_targets(cfg: Mapping[str, Any], output_dir: str | Path) -> List[Dict[str, Any]]:
    output_dir = Path(output_dir)
    rows: List[Dict[str, Any]] = []
    for target in _load_targets_from_cfg(cfg):
        live_id = ""
        raw_line = ""
        error = ""
        ga = GasAnalyzer(
            target["Port"],
            target["Baudrate"],
            timeout=float(target["Timeout"]),
            device_id=target["ConfiguredDeviceId"] or "000",
        )
        try:
            ga.open()
            try:
                ga.set_comm_way_with_ack(False, require_ack=False)
            except Exception:
                pass
            for _ in range(4):
                raw_line = str(
                    ga.read_latest_data(
                        prefer_stream=True,
                        drain_s=0.2,
                        read_timeout_s=0.05,
                        allow_passive_fallback=True,
                    )
                    or ""
                )
                parsed = ga.parse_line(raw_line)
                live_id = _normalize_device_id((parsed or {}).get("id"))
                if live_id:
                    break
                time.sleep(0.1)
        except Exception as exc:
            error = str(exc)
        finally:
            try:
                ga.close()
            except Exception:
                pass
        rows.append({**target, "LiveDeviceId": live_id, "Raw": raw_line, "Error": error})
    _write_csv(output_dir / "scan.csv", rows)
    return rows


def _parse_senco_command(command: str) -> tuple[int, List[float]]:
    parts = [part.strip() for part in str(command or "").split(",") if str(part).strip()]
    if len(parts) < 4 or not parts[0].upper().startswith("SENCO"):
        raise ValueError(f"invalid SENCO command: {command}")
    return int(parts[0].upper().replace("SENCO", "")), [float(part) for part in parts[3:]]


def _read_group_as_list(ga: GasAnalyzer, group: int, expected_len: int) -> List[float]:
    parsed = ga.read_coefficient_group(int(group))
    return [float(parsed.get(f"C{idx}")) for idx in range(expected_len)]


def _read_group_with_match_retry(
    ga: GasAnalyzer,
    group: int,
    coeffs: Sequence[float],
    *,
    attempts: int = 3,
    retry_delay_s: float = 0.15,
) -> tuple[List[float], Optional[str]]:
    expected = [float(value) for value in coeffs]
    expected_len = len(expected)
    last_values: List[float] = []
    last_error = ""
    for idx in range(max(1, int(attempts))):
        try:
            values = _read_group_as_list(ga, int(group), expected_len)
            last_values = values
            last_error = ""
            if senco_readback_matches(expected, values):
                return values, None
        except Exception as exc:
            last_error = str(exc)
        if idx + 1 < max(1, int(attempts)) and retry_delay_s > 0:
            time.sleep(max(0.01, float(retry_delay_s)))
    if last_values:
        return last_values, last_error or "READBACK_MISMATCH"
    return [], last_error or "READBACK_MISSING"


def _restore_stream_settings(ga: GasAnalyzer, target_cfg: Mapping[str, Any]) -> None:
    ga.set_mode_with_ack(2, require_ack=False)
    ga.set_active_freq_with_ack(int(target_cfg.get("FtdHz", 1) or 1), require_ack=False)
    ga.set_average_filter_with_ack(int(target_cfg.get("AverageFilter", 49) or 49), require_ack=False)
    ga.set_comm_way_with_ack(bool(target_cfg.get("ActiveSend", True)), require_ack=False)


def write_coefficients_to_live_devices(
    *,
    cfg: Mapping[str, Any],
    output_dir: str | Path,
    download_plan_rows: Sequence[Mapping[str, Any]],
    temperature_rows: Sequence[Mapping[str, Any]],
    pressure_rows: Sequence[Mapping[str, Any]],
    actual_device_ids: Mapping[str, str],
    write_pressure_rows: bool = True,
) -> Dict[str, Any]:
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    scan_rows = scan_live_targets(cfg, output_dir)
    live_by_id = {str(row.get("LiveDeviceId") or ""): row for row in scan_rows if str(row.get("LiveDeviceId") or "")}

    temp_by_analyzer: Dict[str, Dict[int, List[float]]] = {}
    for row in temperature_rows:
        analyzer = _normalize_analyzer(row.get("analyzer_id"))
        channel = str(row.get("senco_channel") or "").strip().upper()
        if analyzer and channel in {"SENCO7", "SENCO8"}:
            group = 7 if channel == "SENCO7" else 8
            temp_by_analyzer.setdefault(analyzer, {})[group] = [
                float(row.get("A") or 0.0),
                float(row.get("B") or 0.0),
                float(row.get("C") or 0.0),
                float(row.get("D") or 0.0),
            ]

    gas_by_analyzer: Dict[str, Dict[int, List[float]]] = {}
    for row in download_plan_rows:
        analyzer = _normalize_analyzer(row.get("Analyzer"))
        for key in ("PrimaryCommand", "SecondaryCommand"):
            command = str(row.get(key) or "").strip()
            if not command:
                continue
            group, coeffs = _parse_senco_command(command)
            gas_by_analyzer.setdefault(analyzer, {})[group] = coeffs

    pressure_by_analyzer: Dict[str, Dict[int, List[float]]] = {}
    skipped_pressure_by_analyzer: Dict[str, str] = {}
    if write_pressure_rows:
        for row in pressure_rows:
            analyzer = _normalize_analyzer(row.get("Analyzer"))
            offset = _safe_float(row.get("OffsetA_kPa"))
            if not analyzer or offset is None:
                continue
            recommended = row.get("PressureWriteRecommended")
            if recommended not in (None, "") and not _safe_bool(recommended):
                skipped_pressure_by_analyzer[analyzer] = str(row.get("PressureWriteReason") or "pressure_write_not_recommended")
                continue
            pressure_by_analyzer.setdefault(analyzer, {})[9] = [float(offset), 1.0, 0.0, 0.0]

    summary_rows: List[Dict[str, Any]] = []
    detail_rows: List[Dict[str, Any]] = []
    analyzers = sorted(set(actual_device_ids) | set(temp_by_analyzer) | set(gas_by_analyzer) | set(pressure_by_analyzer) | set(skipped_pressure_by_analyzer))
    for analyzer in analyzers:
        target_device_id = _normalize_device_id(actual_device_ids.get(analyzer))
        live_target = live_by_id.get(target_device_id)
        if analyzer in skipped_pressure_by_analyzer:
            detail_rows.append(
                {
                    "Analyzer": analyzer,
                    "Port": live_target["Port"] if live_target is not None else "",
                    "TargetDeviceId": target_device_id,
                    "LiveDeviceId": live_target.get("LiveDeviceId") if live_target is not None else "",
                    "Group": 9,
                    "Expected": "",
                    "Readback": "",
                    "ReadbackOk": False,
                    "Error": f"SKIPPED_PRESSURE_WRITE:{skipped_pressure_by_analyzer[analyzer]}",
                }
            )
        expected_groups: Dict[int, List[float]] = {}
        expected_groups.update(temp_by_analyzer.get(analyzer, {}))
        expected_groups.update(gas_by_analyzer.get(analyzer, {}))
        expected_groups.update(pressure_by_analyzer.get(analyzer, {}))
        if not target_device_id or live_target is None:
            summary_rows.append({"Analyzer": analyzer, "TargetDeviceId": target_device_id, "ExpectedGroups": len(expected_groups), "MatchedGroups": 0, "Status": "missing_live_device"})
            continue
        matched = 0
        status = "ok"
        ga = GasAnalyzer(
            live_target["Port"],
            live_target["Baudrate"],
            timeout=float(live_target["Timeout"]),
            device_id=target_device_id,
        )
        try:
            ga.open()
            ga.set_comm_way_with_ack(False, require_ack=False)
            ga.set_mode_with_ack(2, require_ack=True)
            for group, coeffs in sorted(expected_groups.items()):
                readback = ""
                readback_ok = False
                error = ""
                try:
                    acked = bool(ga.set_senco(int(group), coeffs))
                    if not acked:
                        raise RuntimeError("WRITE_ACK_FAILED")
                    values, readback_error = _read_group_with_match_retry(ga, int(group), coeffs)
                    readback = json.dumps(values, ensure_ascii=False)
                    readback_ok = senco_readback_matches(coeffs, values)
                    if readback_ok:
                        matched += 1
                    else:
                        status = "partial"
                        error = str(readback_error or "READBACK_MISMATCH")
                except Exception as exc:
                    error = str(exc)
                    status = "partial"
                detail_rows.append(
                    {
                        "Analyzer": analyzer,
                        "Port": live_target["Port"],
                        "TargetDeviceId": target_device_id,
                        "LiveDeviceId": live_target.get("LiveDeviceId"),
                        "Group": group,
                        "Expected": json.dumps([float(value) for value in coeffs], ensure_ascii=False),
                        "Readback": readback,
                        "ReadbackOk": readback_ok,
                        "Error": error,
                    }
                )
            _restore_stream_settings(ga, live_target)
        except Exception as exc:
            status = "error"
            detail_rows.append(
                {
                    "Analyzer": analyzer,
                    "Port": live_target["Port"],
                    "TargetDeviceId": target_device_id,
                    "LiveDeviceId": live_target.get("LiveDeviceId"),
                    "Group": "session",
                    "Expected": "",
                    "Readback": "",
                    "ReadbackOk": False,
                    "Error": str(exc),
                }
            )
        finally:
            try:
                ga.close()
            except Exception:
                pass
        summary_rows.append(
            {
                "Analyzer": analyzer,
                "Port": live_target["Port"],
                "TargetDeviceId": target_device_id,
                "LiveDeviceId": live_target.get("LiveDeviceId"),
                "ExpectedGroups": len(expected_groups),
                "MatchedGroups": matched,
                "Status": status if matched < len(expected_groups) else "ok",
            }
        )
    _write_csv(output_dir / "summary.csv", summary_rows)
    _write_csv(output_dir / "detail.csv", detail_rows)
    return {"scan_rows": scan_rows, "summary_rows": summary_rows, "detail_rows": detail_rows}


def build_corrected_delivery(
    *,
    run_dir: str | Path,
    output_dir: str | Path,
    fallback_pressure_to_controller: bool = False,
    pressure_row_source: str = "startup_calibration",
) -> Dict[str, Any]:
    run_dir = Path(run_dir).resolve()
    output_dir = Path(output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    filtered_paths, filter_stats = _filter_no_500_summary_paths(run_dir, output_dir)
    report_path = output_dir / "calibration_coefficients.xlsx"
    report = build_corrected_water_points_report(filtered_paths, output_path=report_path, temperature_key="Temp")
    simplified = report["simplified"].copy()
    summary = report["summary"].copy()
    actual_device_ids = extract_run_device_ids(run_dir)

    analyzer_summary_rows = []
    analyzer_values = summary.get("分析仪")
    analyzers = set(actual_device_ids)
    if isinstance(analyzer_values, pd.Series):
        analyzers.update(str(item) for item in analyzer_values.dropna().unique())
    for analyzer in sorted(analyzers):
        analyzer_summary_rows.append({"Analyzer": analyzer, "ActualDeviceId": actual_device_ids.get(analyzer, "")})

    download_plan_rows = build_corrected_download_plan_rows(simplified)
    temperature_rows = load_temperature_coefficient_rows(run_dir)
    pressure_mode = str(pressure_row_source or "startup_calibration").strip().lower()
    if pressure_mode == "current_ambient":
        pressure_rows = compute_pressure_offset_rows(run_dir, fallback_to_controller=fallback_pressure_to_controller)
    elif pressure_mode == "none":
        pressure_rows = []
    else:
        pressure_rows = load_startup_pressure_calibration_rows(run_dir)
        if not pressure_rows and fallback_pressure_to_controller:
            pressure_rows = compute_pressure_offset_rows(run_dir, fallback_to_controller=True)

    _append_dataframe_sheet(report_path, "download_plan", pd.DataFrame(download_plan_rows))
    _append_dataframe_sheet(report_path, "分析仪汇总", pd.DataFrame(analyzer_summary_rows))
    _append_dataframe_sheet(report_path, "temperature_plan", pd.DataFrame(temperature_rows))
    _append_dataframe_sheet(report_path, "pressure_plan", pd.DataFrame(pressure_rows))

    _write_csv(output_dir / "download_plan_no_500.csv", download_plan_rows)
    _write_csv(output_dir / "fit_summary_no_500.csv", summary.to_dict(orient="records"))
    _write_csv(output_dir / "simplified_coefficients_no_500.csv", simplified.to_dict(orient="records"))
    _write_csv(output_dir / "temperature_coefficients_target.csv", temperature_rows)
    _write_csv(output_dir / "pressure_offset_current_ambient_summary.csv", pressure_rows)
    _write_csv(output_dir / "filter_summary.csv", filter_stats)
    summary_lines = [
        "# corrected-entry no-500 summary",
        "",
        f"- run_dir: {run_dir}",
        f"- output_dir: {output_dir}",
        f"- pressure_row_source: {pressure_mode}",
        "",
        "## filter summary",
    ]
    for row in filter_stats:
        summary_lines.append(
            f"- {Path(str(row.get('source') or '')).name}: original={row.get('original_rows')} removed={row.get('removed_rows')} kept={row.get('kept_rows')}"
        )
    (output_dir / "summary.md").write_text("\n".join(summary_lines) + "\n", encoding="utf-8")
    return {
        "report_path": str(report_path),
        "output_dir": str(output_dir),
        "filtered_summary_paths": [str(path) for path in filtered_paths],
        "filter_stats": filter_stats,
        "actual_device_ids": actual_device_ids,
        "download_plan_rows": download_plan_rows,
        "temperature_rows": temperature_rows,
        "pressure_rows": pressure_rows,
        "pressure_row_source": pressure_mode,
    }


def run_from_cli(
    *,
    run_dir: str,
    config_path: Optional[str] = None,
    output_dir: Optional[str] = None,
    write_devices: bool = False,
    verify_report: bool = False,
    verification_template: Optional[str] = None,
    fallback_pressure_to_controller: bool = False,
    pressure_row_source: str = "startup_calibration",
    write_pressure_coefficients: bool = False,
) -> Dict[str, Any]:
    run_dir_path = Path(run_dir).resolve()
    target_dir = Path(output_dir).resolve() if output_dir else run_dir_path / f"corrected_autodelivery_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    target_dir.mkdir(parents=True, exist_ok=True)
    delivery = build_corrected_delivery(
        run_dir=run_dir_path,
        output_dir=target_dir,
        fallback_pressure_to_controller=fallback_pressure_to_controller,
        pressure_row_source=pressure_row_source,
    )

    cfg_path = Path(config_path).resolve() if config_path else (run_dir_path / "runtime_config_snapshot.json")
    cfg = load_config(str(cfg_path))
    write_result: Dict[str, Any] = {}
    if write_devices:
        write_result = write_coefficients_to_live_devices(
            cfg=cfg,
            output_dir=target_dir / "device_write",
            download_plan_rows=delivery["download_plan_rows"],
            temperature_rows=delivery["temperature_rows"],
            pressure_rows=delivery["pressure_rows"],
            actual_device_ids=delivery["actual_device_ids"],
            write_pressure_rows=write_pressure_coefficients,
        )

    verify_outputs: Dict[str, Any] = {}
    if verify_report and verification_template:
        from . import validate_verification_doc

        verification_targets = []
        for row in list(write_result.get("scan_rows") or []):
            live_id = _normalize_device_id(row.get("LiveDeviceId"))
            analyzer = _normalize_analyzer(row.get("Analyzer"))
            target_id = _normalize_device_id(delivery["actual_device_ids"].get(analyzer))
            if not live_id or live_id != target_id:
                continue
            verification_targets.append(
                {
                    "name": analyzer.lower(),
                    "port": row.get("Port"),
                    "baud": int(row.get("Baudrate") or 115200),
                    "device_id": live_id,
                    "enabled": True,
                }
            )
        targets_json_path = target_dir / "verification_targets.json"
        targets_json_path.write_text(json.dumps({"devices": {"gas_analyzers": verification_targets}}, ensure_ascii=False, indent=2), encoding="utf-8")
        verify_dir = target_dir / "verification_report"
        verify_dir.mkdir(parents=True, exist_ok=True)
        validate_verification_doc.run_from_cli(
            config=str(cfg_path),
            template=str(Path(verification_template).resolve()),
            targets_json=str(targets_json_path),
            output_dir=str(verify_dir),
        )
        verify_outputs = {"targets_json": str(targets_json_path), "output_dir": str(verify_dir)}

    payload = {**delivery, "write_result": write_result, "verify_outputs": verify_outputs}
    (target_dir / "autodelivery_summary.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, default=str),
        encoding="utf-8",
    )
    return payload
