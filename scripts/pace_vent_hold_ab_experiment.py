from __future__ import annotations

import argparse
import csv
import json
import re
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from gas_calibrator.config import load_config
from gas_calibrator.devices.pace5000 import Pace5000
from gas_calibrator.devices.paroscientific import ParoscientificGauge


ROW_FIELDS = [
    "timestamp",
    "group",
    "baseline_for_group",
    "elapsed_s",
    "vent_command_sent",
    "vent_command_error",
    "gauge_pressure_hpa",
    "pace_pressure_hpa",
    "barometric_pressure_hpa",
    "in_limits_pressure_hpa",
    "vent_status",
    "outp_state",
    "isol_state",
    "syst_err",
    "gauge_minus_baro_hpa",
    "pace_minus_baro_hpa",
]


def _empty_summary_paths(run_dir: Path) -> None:
    _write_csv(run_dir / "rows.csv", [])
    _write_json(run_dir / "rows.json", [])
    _write_csv(run_dir / "arm_rows.csv", [])
    _write_json(run_dir / "arm_rows.json", [])


def _timestamp() -> str:
    return datetime.now().isoformat(timespec="milliseconds")


def _safe_float(value: Any) -> Optional[float]:
    try:
        if value in (None, ""):
            return None
        return float(value)
    except Exception:
        return None


def _safe_int(value: Any) -> Optional[int]:
    numeric = _safe_float(value)
    if numeric is None:
        return None
    try:
        return int(round(numeric))
    except Exception:
        return None


def _parse_first_number(text: Any) -> Optional[float]:
    raw = str(text or "").strip()
    if not raw:
        return None
    match = re.search(r"[-+]?\d+(?:\.\d+)?(?:[eE][-+]?\d+)?", raw)
    if not match:
        return None
    try:
        return float(match.group(0))
    except Exception:
        return None


def _parse_first_int(text: Any) -> Optional[int]:
    numeric = _parse_first_number(text)
    if numeric is None:
        return None
    try:
        return int(numeric)
    except Exception:
        return None


def _normalize_path(path_like: str | Path) -> Path:
    path = Path(path_like)
    if not path.is_absolute():
        path = (ROOT / path).resolve()
    return path.resolve()


def _safe_call(label: str, fn) -> Tuple[Any, str]:
    try:
        return fn(), ""
    except Exception as exc:
        return None, f"{label}_error:{exc}"


def _safe_query_text(pace: Optional[Pace5000], command: str) -> Tuple[str, str]:
    if pace is None:
        return "", "pace_unavailable"
    try:
        return str(pace.query(command) or "").strip(), ""
    except Exception as exc:
        return "", f"{command}_error:{exc}"


def _safe_gauge_read(gauge: Optional[ParoscientificGauge]) -> Tuple[Optional[float], str]:
    if gauge is None:
        return None, "gauge_unavailable"
    try:
        return float(gauge.read_pressure_fast()), ""
    except Exception as exc:
        return None, f"gauge_read_error:{exc}"


def _safe_pace_pressure(pace: Optional[Pace5000]) -> Tuple[Optional[float], str]:
    if pace is None:
        return None, "pace_unavailable"
    try:
        return float(pace.read_pressure()), ""
    except Exception as exc:
        return None, f"pace_pressure_error:{exc}"


def _safe_pace_barometric(pace: Optional[Pace5000]) -> Tuple[Optional[float], str]:
    if pace is None:
        return None, "pace_unavailable"
    try:
        return float(pace.get_barometric_pressure()), ""
    except Exception as exc:
        return None, f"barometric_error:{exc}"


def _safe_pace_in_limits(pace: Optional[Pace5000]) -> Tuple[Optional[float], str]:
    if pace is None:
        return None, "pace_unavailable"
    try:
        pressure_hpa, _state = pace.get_in_limits()
        return float(pressure_hpa), ""
    except Exception as exc:
        return None, f"in_limits_error:{exc}"


def _safe_pace_vent_status(pace: Optional[Pace5000]) -> Tuple[Optional[int], str]:
    if pace is None:
        return None, "pace_unavailable"
    try:
        return int(pace.get_vent_status()), ""
    except Exception as exc:
        return None, f"vent_status_error:{exc}"


def _safe_pace_outp_state(pace: Optional[Pace5000]) -> Tuple[Optional[int], str]:
    if pace is None:
        return None, "pace_unavailable"
    try:
        return int(pace.get_output_state()), ""
    except Exception as exc:
        return None, f"outp_state_error:{exc}"


def _safe_pace_isol_state(pace: Optional[Pace5000]) -> Tuple[Optional[int], str]:
    if pace is None:
        return None, "pace_unavailable"
    try:
        return int(pace.get_isolation_state()), ""
    except Exception as exc:
        return None, f"isol_state_error:{exc}"


def _safe_pace_syst_err(pace: Optional[Pace5000]) -> Tuple[str, str]:
    if pace is None:
        return "", "pace_unavailable"
    try:
        return str(pace.get_system_error() or "").strip(), ""
    except Exception as exc:
        return "", f"syst_err_query_error:{exc}"


def _system_error_is_nonzero(text: Any) -> bool:
    raw = str(text or "").strip()
    if not raw:
        return False
    normalized = raw.replace(" ", "").lower()
    if normalized.startswith(":syst:err0") or normalized.startswith("0,") or normalized == "0" or "noerror" in normalized:
        return False
    match = re.search(r"(?<!\d)-?\d+", raw)
    if match:
        try:
            return int(match.group(0)) != 0
        except Exception:
            return True
    return True


def _build_sample_targets(duration_s: float, interval_s: float) -> List[float]:
    duration = max(0.0, float(duration_s))
    interval = max(0.1, float(interval_s))
    targets: List[float] = []
    current = 0.0
    while current < duration - 1e-9:
        targets.append(round(current, 6))
        current += interval
    if not targets or abs(targets[-1] - duration) > 1e-9:
        targets.append(round(duration, 6))
    return targets


def _sleep_until(start_mono: float, target_elapsed_s: float) -> None:
    deadline = start_mono + max(0.0, float(target_elapsed_s))
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return
        time.sleep(min(remaining, 0.2))


def _collect_row(
    *,
    pace: Optional[Pace5000],
    gauge: Optional[ParoscientificGauge],
    group: str,
    baseline_for_group: str,
    elapsed_s: float,
    vent_command_sent: bool,
    vent_command_error: str,
) -> Dict[str, Any]:
    gauge_pressure_hpa, _ = _safe_gauge_read(gauge)
    pace_pressure_hpa, _ = _safe_pace_pressure(pace)
    barometric_pressure_hpa, _ = _safe_pace_barometric(pace)
    in_limits_pressure_hpa, _ = _safe_pace_in_limits(pace)
    vent_status, _ = _safe_pace_vent_status(pace)
    outp_state, _ = _safe_pace_outp_state(pace)
    isol_state, _ = _safe_pace_isol_state(pace)
    syst_err, syst_err_error = _safe_pace_syst_err(pace)
    if syst_err_error and not syst_err:
        syst_err = syst_err_error

    gauge_minus_baro_hpa = None
    if gauge_pressure_hpa is not None and barometric_pressure_hpa is not None:
        gauge_minus_baro_hpa = round(gauge_pressure_hpa - barometric_pressure_hpa, 6)

    pace_minus_baro_hpa = None
    if pace_pressure_hpa is not None and barometric_pressure_hpa is not None:
        pace_minus_baro_hpa = round(pace_pressure_hpa - barometric_pressure_hpa, 6)

    return {
        "timestamp": _timestamp(),
        "group": group,
        "baseline_for_group": baseline_for_group,
        "elapsed_s": round(float(elapsed_s), 6),
        "vent_command_sent": bool(vent_command_sent),
        "vent_command_error": str(vent_command_error or ""),
        "gauge_pressure_hpa": gauge_pressure_hpa,
        "pace_pressure_hpa": pace_pressure_hpa,
        "barometric_pressure_hpa": barometric_pressure_hpa,
        "in_limits_pressure_hpa": in_limits_pressure_hpa,
        "vent_status": vent_status,
        "outp_state": outp_state,
        "isol_state": isol_state,
        "syst_err": syst_err,
        "gauge_minus_baro_hpa": gauge_minus_baro_hpa,
        "pace_minus_baro_hpa": pace_minus_baro_hpa,
    }


def _capture_window(
    *,
    pace: Optional[Pace5000],
    gauge: Optional[ParoscientificGauge],
    group: str,
    baseline_for_group: str = "",
    duration_s: float,
    sample_interval_s: float,
    send_initial_vent: bool = False,
    reissue_interval_s: Optional[float] = None,
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    sample_targets = _build_sample_targets(duration_s, sample_interval_s)
    reissue_targets = (
        _build_sample_targets(duration_s, reissue_interval_s)
        if send_initial_vent and reissue_interval_s is not None
        else ([0.0] if send_initial_vent else [])
    )
    reissue_index = 0
    phase_start = time.monotonic()

    for sample_target in sample_targets:
        _sleep_until(phase_start, sample_target)
        vent_command_sent = False
        vent_command_error = ""
        while reissue_index < len(reissue_targets) and reissue_targets[reissue_index] <= sample_target + 1e-9:
            if pace is None:
                if not vent_command_error:
                    vent_command_error = "pace_unavailable"
            else:
                try:
                    pace.vent(True)
                except Exception as exc:
                    if not vent_command_error:
                        vent_command_error = f"vent_write_error:{exc}"
                else:
                    vent_command_sent = True
            reissue_index += 1

        rows.append(
            _collect_row(
                pace=pace,
                gauge=gauge,
                group=group,
                baseline_for_group=baseline_for_group,
                elapsed_s=sample_target,
                vent_command_sent=vent_command_sent,
                vent_command_error=vent_command_error,
            )
        )
    return rows


def _arm_until_pressurized(
    *,
    pace: Optional[Pace5000],
    gauge: Optional[ParoscientificGauge],
    threshold_hpa: float,
    max_wait_s: float,
    sample_interval_s: float,
) -> Tuple[List[Dict[str, Any]], bool, str, float]:
    rows: List[Dict[str, Any]] = []
    sample_targets = _build_sample_targets(max_wait_s, sample_interval_s)
    phase_start = time.monotonic()
    trigger_reason = ""
    wait_duration_s = 0.0

    for sample_target in sample_targets:
        _sleep_until(phase_start, sample_target)
        row = _collect_row(
            pace=pace,
            gauge=gauge,
            group="arm",
            baseline_for_group="",
            elapsed_s=sample_target,
            vent_command_sent=False,
            vent_command_error="",
        )
        rows.append(row)
        wait_duration_s = float(row.get("elapsed_s") or sample_target or 0.0)

        gauge_delta = _safe_float(row.get("gauge_minus_baro_hpa"))
        pace_delta = _safe_float(row.get("pace_minus_baro_hpa"))
        if gauge_delta is not None and gauge_delta >= float(threshold_hpa):
            trigger_reason = f"gauge_minus_baro_hpa>={float(threshold_hpa):g}"
            return rows, True, trigger_reason, wait_duration_s
        if pace_delta is not None and pace_delta >= float(threshold_hpa):
            trigger_reason = f"pace_minus_baro_hpa>={float(threshold_hpa):g}"
            return rows, True, trigger_reason, wait_duration_s

    return rows, False, "max_arm_wait_s_exceeded", wait_duration_s


def _group_rows(rows: Sequence[Dict[str, Any]], group: str) -> List[Dict[str, Any]]:
    return [dict(row) for row in rows if str(row.get("group")) == group]


def _baseline_rows(rows: Sequence[Dict[str, Any]], for_group: str) -> List[Dict[str, Any]]:
    return [
        dict(row)
        for row in rows
        if str(row.get("group")) == "baseline" and str(row.get("baseline_for_group")) == for_group
    ]


def _last_non_none(rows: Sequence[Dict[str, Any]], key: str) -> Optional[float]:
    for row in reversed(list(rows)):
        value = _safe_float(row.get(key))
        if value is not None:
            return value
    return None


def _min_non_none(rows: Sequence[Dict[str, Any]], key: str) -> Optional[float]:
    values = [_safe_float(row.get(key)) for row in rows]
    numeric = [value for value in values if value is not None]
    if not numeric:
        return None
    return min(numeric)


def _collect_measurement_window(rows: Sequence[Dict[str, Any]], key: str) -> List[Tuple[float, float]]:
    points: List[Tuple[float, float]] = []
    for row in rows:
        elapsed_s = _safe_float(row.get("elapsed_s"))
        value = _safe_float(row.get(key))
        if elapsed_s is None or value is None:
            continue
        points.append((elapsed_s, value))
    return points


def _window_has_atmospheric_hold(
    rows: Sequence[Dict[str, Any]],
    *,
    delta_key: str,
    pressure_key: str,
    duration_s: float,
    sample_interval_s: float,
    hold_window_s: float = 30.0,
    delta_threshold_hpa: float = 20.0,
    span_threshold_hpa: float = 10.0,
) -> bool:
    points = _collect_measurement_window(rows, pressure_key)
    deltas = _collect_measurement_window(rows, delta_key)
    if not points or not deltas:
        return False

    end_elapsed = max(point[0] for point in points)
    start_elapsed = max(0.0, end_elapsed - float(hold_window_s))
    required_span = max(0.0, float(hold_window_s) - max(0.0, float(sample_interval_s)) * 1.5)

    window_points = [(elapsed, value) for elapsed, value in points if elapsed >= start_elapsed - 1e-9]
    window_deltas = [(elapsed, value) for elapsed, value in deltas if elapsed >= start_elapsed - 1e-9]
    if not window_points or not window_deltas:
        return False
    if window_points[-1][0] - window_points[0][0] < required_span:
        return False
    if window_deltas[-1][0] - window_deltas[0][0] < required_span:
        return False

    pressure_values = [value for _, value in window_points]
    delta_values = [value for _, value in window_deltas]
    if any(abs(value) > float(delta_threshold_hpa) for value in delta_values):
        return False
    span_hpa = max(pressure_values) - min(pressure_values)
    return span_hpa <= float(span_threshold_hpa)


def _mean_non_none(rows: Sequence[Dict[str, Any]], key: str) -> Optional[float]:
    numeric = [_safe_float(row.get(key)) for row in rows]
    values = [value for value in numeric if value is not None]
    if not values:
        return None
    return sum(values) / len(values)


def _summarize_preprobe(
    pace: Optional[Pace5000],
    gauge: Optional[ParoscientificGauge],
) -> Dict[str, Any]:
    idn, idn_error = _safe_query_text(pace, "*IDN?")
    inst_vers, inst_vers_error = _safe_query_text(pace, ":INST:VERS?")
    barometric_raw, barometric_error = _safe_query_text(pace, ":SENS:PRES:BAR?")
    in_limits_raw, in_limits_error = _safe_query_text(pace, ":SENS:PRES:INL?")
    outp_raw, outp_error = _safe_query_text(pace, ":OUTP:STAT?")
    isol_raw, isol_error = _safe_query_text(pace, ":OUTP:ISOL:STAT?")
    vent_raw, vent_error = _safe_query_text(pace, ":SOUR:PRES:LEV:IMM:AMPL:VENT?")
    syst_err_raw, syst_err_error = _safe_query_text(pace, ":SYST:ERR?")
    gauge_pressure_hpa, gauge_error = _safe_gauge_read(gauge)

    return {
        "device_idn": idn,
        "device_idn_error": idn_error,
        "inst_vers": inst_vers,
        "inst_vers_error": inst_vers_error,
        "barometric_raw": barometric_raw,
        "barometric_error": barometric_error,
        "barometric_pressure_hpa": _parse_first_number(barometric_raw),
        "in_limits_raw": in_limits_raw,
        "in_limits_error": in_limits_error,
        "in_limits_pressure_hpa": _parse_first_number(in_limits_raw),
        "outp_raw": outp_raw,
        "outp_error": outp_error,
        "outp_state": _parse_first_int(outp_raw),
        "isol_raw": isol_raw,
        "isol_error": isol_error,
        "isol_state": _parse_first_int(isol_raw),
        "vent_raw": vent_raw,
        "vent_error": vent_error,
        "vent_status": _parse_first_int(vent_raw),
        "syst_err": syst_err_raw if syst_err_raw else syst_err_error,
        "syst_err_query_error": syst_err_error,
        "gauge_pressure_hpa": gauge_pressure_hpa,
        "gauge_error": gauge_error,
    }


def _preconditions_from_initial_state(
    preprobe: Dict[str, Any],
    baseline_rows: Sequence[Dict[str, Any]],
) -> Tuple[str, str]:
    reasons: List[str] = []

    device_idn = str(preprobe.get("device_idn") or "").strip()
    inst_vers = str(preprobe.get("inst_vers") or "").strip()
    initial_row = baseline_rows[-1] if baseline_rows else {}
    barometric = _safe_float(initial_row.get("barometric_pressure_hpa"))
    pace_delta = _safe_float(initial_row.get("pace_minus_baro_hpa"))
    gauge_delta = _safe_float(initial_row.get("gauge_minus_baro_hpa"))

    if not device_idn or not inst_vers or barometric is None or pace_delta is None:
        reasons.append("scpi_not_available")
    if pace_delta is not None and abs(pace_delta) < 50.0:
        reasons.append("pace_delta_below_50hpa")
    if gauge_delta is not None and abs(gauge_delta) < 50.0:
        reasons.append("gauge_delta_below_50hpa")

    if reasons:
        return "test_preconditions_not_met", ";".join(reasons)
    return "met", ""


def _comparison_conclusion(a_ok: bool, b_ok: bool, precondition_status: str) -> str:
    if precondition_status != "met":
        return "test_preconditions_not_met"
    if a_ok:
        return "single_vent_sufficient"
    if b_ok:
        return "periodic_vent_reissue_needed"
    return "neither_group_reached_atmosphere"


def _write_csv(path: Path, rows: Sequence[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=ROW_FIELDS)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key, "") for key in ROW_FIELDS})


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _resolve_device_settings(
    cfg: Dict[str, Any],
    args: argparse.Namespace,
) -> Dict[str, Any]:
    devices_cfg = cfg.get("devices", {}) if isinstance(cfg, dict) else {}
    pace_cfg = devices_cfg.get("pressure_controller", {}) if isinstance(devices_cfg, dict) else {}
    gauge_cfg = devices_cfg.get("pressure_gauge", {}) if isinstance(devices_cfg, dict) else {}

    pace_port = str(args.pace_port or pace_cfg.get("port") or "").strip()
    if not pace_port:
        raise RuntimeError("PACE port is required")

    gauge_port_raw = str(args.gauge_port or gauge_cfg.get("port") or "").strip()
    gauge_port = gauge_port_raw or ""

    return {
        "pace_port": pace_port,
        "pace_baudrate": int(args.baudrate or pace_cfg.get("baud", 9600) or 9600),
        "pace_timeout": float(args.timeout if args.timeout is not None else pace_cfg.get("timeout", 1.0) or 1.0),
        "pace_line_ending": pace_cfg.get("line_ending"),
        "pace_query_line_endings": pace_cfg.get("query_line_endings"),
        "pace_pressure_queries": pace_cfg.get("pressure_queries"),
        "gauge_port": gauge_port,
        "gauge_baudrate": int(
            args.gauge_baudrate if args.gauge_baudrate is not None else gauge_cfg.get("baud", 9600) or 9600
        ),
        "gauge_timeout": float(
            args.gauge_timeout if args.gauge_timeout is not None else gauge_cfg.get("timeout", 1.0) or 1.0
        ),
        "gauge_dest_id": str(args.gauge_dest_id or gauge_cfg.get("dest_id") or "01"),
        "gauge_response_timeout_s": float(
            args.gauge_response_timeout_s
            if args.gauge_response_timeout_s is not None
            else gauge_cfg.get("response_timeout_s", max(1.2, gauge_cfg.get("timeout", 1.0) or 1.0))
        ),
    }


def _make_pace(settings: Dict[str, Any]) -> Pace5000:
    return Pace5000(
        settings["pace_port"],
        baudrate=settings["pace_baudrate"],
        timeout=settings["pace_timeout"],
        line_ending=settings.get("pace_line_ending"),
        query_line_endings=settings.get("pace_query_line_endings"),
        pressure_queries=settings.get("pace_pressure_queries"),
    )


def _make_gauge(settings: Dict[str, Any]) -> Optional[ParoscientificGauge]:
    gauge_port = str(settings.get("gauge_port") or "").strip()
    if not gauge_port:
        return None
    return ParoscientificGauge(
        gauge_port,
        baudrate=settings["gauge_baudrate"],
        timeout=settings["gauge_timeout"],
        dest_id=settings["gauge_dest_id"],
        response_timeout_s=settings["gauge_response_timeout_s"],
    )


def run_experiment(args: argparse.Namespace) -> Dict[str, Any]:
    config_path = _normalize_path(args.config)
    cfg = load_config(config_path)
    settings = _resolve_device_settings(cfg, args)

    output_root = _normalize_path(args.output_root)
    run_dir = output_root / datetime.now().strftime("%Y%m%d_%H%M%S")
    run_dir.mkdir(parents=True, exist_ok=True)

    _empty_summary_paths(run_dir)

    rows: List[Dict[str, Any]] = []
    arm_rows: List[Dict[str, Any]] = []
    pace: Optional[Pace5000] = None
    gauge: Optional[ParoscientificGauge] = None
    pace_open_error = ""
    gauge_open_error = ""
    arm_enabled = bool(args.arm_until_pressurized)
    arm_triggered = False
    arm_trigger_reason = ""
    arm_wait_duration_s = 0.0

    try:
        pace = _make_pace(settings)
        pace.open()
    except Exception as exc:
        pace_open_error = f"pace_open_error:{exc}"
        pace = None

    try:
        gauge = _make_gauge(settings)
        if gauge is not None:
            gauge.open()
    except Exception as exc:
        gauge_open_error = f"gauge_open_error:{exc}"
        gauge = None

    preprobe = _summarize_preprobe(pace, gauge)
    if pace_open_error and not preprobe.get("device_idn_error"):
        preprobe["device_idn_error"] = pace_open_error
    if gauge_open_error and not preprobe.get("gauge_error"):
        preprobe["gauge_error"] = gauge_open_error

    if arm_enabled:
        arm_rows, arm_triggered, arm_trigger_reason, arm_wait_duration_s = _arm_until_pressurized(
            pace=pace,
            gauge=gauge,
            threshold_hpa=float(args.precondition_threshold_hpa),
            max_wait_s=float(args.max_arm_wait_s),
            sample_interval_s=float(args.arm_sample_interval_s),
        )

    _write_csv(run_dir / "arm_rows.csv", arm_rows)
    _write_json(run_dir / "arm_rows.json", arm_rows)

    baseline_a: List[Dict[str, Any]] = []
    if (not arm_enabled) or arm_triggered:
        baseline_a = _capture_window(
            pace=pace,
            gauge=gauge,
            group="baseline",
            baseline_for_group="A",
            duration_s=float(args.baseline_duration_s),
            sample_interval_s=float(args.sample_interval_s),
        )
        rows.extend(baseline_a)

    precondition_status, precondition_reason = (
        _preconditions_from_initial_state(preprobe, baseline_a)
        if baseline_a
        else ("test_preconditions_not_met", "baseline_not_started")
    )
    comparison_conclusion_override = ""

    if arm_enabled and not arm_triggered:
        precondition_status = "test_preconditions_not_met"
        precondition_reason = (
            f"arm_timeout;threshold_hpa={float(args.precondition_threshold_hpa):g};"
            f"max_arm_wait_s={float(args.max_arm_wait_s):g}"
        )
        comparison_conclusion_override = "arm_timeout_preconditions_not_met"

    group_a_rows: List[Dict[str, Any]] = []
    baseline_b: List[Dict[str, Any]] = []
    group_b_rows: List[Dict[str, Any]] = []

    if precondition_status == "met":
        group_a_rows = _capture_window(
            pace=pace,
            gauge=gauge,
            group="A",
            duration_s=float(args.group_duration_s),
            sample_interval_s=float(args.sample_interval_s),
            send_initial_vent=True,
            reissue_interval_s=None,
        )
        rows.extend(group_a_rows)

        baseline_b = _capture_window(
            pace=pace,
            gauge=gauge,
            group="baseline",
            baseline_for_group="B",
            duration_s=float(args.baseline_duration_s),
            sample_interval_s=float(args.sample_interval_s),
        )
        rows.extend(baseline_b)

        group_b_rows = _capture_window(
            pace=pace,
            gauge=gauge,
            group="B",
            duration_s=float(args.group_duration_s),
            sample_interval_s=float(args.sample_interval_s),
            send_initial_vent=True,
            reissue_interval_s=float(args.b_reissue_interval_s or args.sample_interval_s),
        )
        rows.extend(group_b_rows)

    _write_csv(run_dir / "rows.csv", rows)
    _write_json(run_dir / "rows.json", rows)

    gauge_missing_fallback_used = not any(
        _safe_float(row.get("gauge_pressure_hpa")) is not None for row in rows if str(row.get("group")) in {"A", "B"}
    )

    a_reached_atmosphere = _window_has_atmospheric_hold(
        group_a_rows,
        delta_key="pace_minus_baro_hpa" if gauge_missing_fallback_used else "gauge_minus_baro_hpa",
        pressure_key="pace_pressure_hpa" if gauge_missing_fallback_used else "gauge_pressure_hpa",
        duration_s=float(args.group_duration_s),
        sample_interval_s=float(args.sample_interval_s),
    )
    b_reached_atmosphere = _window_has_atmospheric_hold(
        group_b_rows,
        delta_key="pace_minus_baro_hpa" if gauge_missing_fallback_used else "gauge_minus_baro_hpa",
        pressure_key="pace_pressure_hpa" if gauge_missing_fallback_used else "gauge_pressure_hpa",
        duration_s=float(args.group_duration_s),
        sample_interval_s=float(args.sample_interval_s),
    )

    comparison_conclusion = (
        comparison_conclusion_override
        or _comparison_conclusion(
            a_reached_atmosphere,
            b_reached_atmosphere,
            precondition_status,
        )
    )

    baseline_reference_hpa = _mean_non_none(
        _baseline_rows(rows, "A") + _baseline_rows(rows, "B") + arm_rows,
        "barometric_pressure_hpa",
    )

    combined_rows = list(arm_rows) + list(rows)

    summary = {
        "pace_port": settings["pace_port"],
        "gauge_port": settings["gauge_port"] or None,
        "device_idn": preprobe.get("device_idn") or "",
        "inst_vers": preprobe.get("inst_vers") or "",
        "sample_interval_s": float(args.sample_interval_s),
        "baseline_duration_s": float(args.baseline_duration_s),
        "a_group_duration_s": float(args.group_duration_s),
        "b_group_duration_s": float(args.group_duration_s),
        "b_reissue_interval_s": float(args.b_reissue_interval_s or args.sample_interval_s),
        "barometric_reference_hpa": baseline_reference_hpa,
        "a_group_min_gauge_delta_to_bar_hpa": _min_non_none(group_a_rows, "gauge_minus_baro_hpa"),
        "a_group_final_gauge_delta_to_bar_hpa": _last_non_none(group_a_rows, "gauge_minus_baro_hpa"),
        "b_group_min_gauge_delta_to_bar_hpa": _min_non_none(group_b_rows, "gauge_minus_baro_hpa"),
        "b_group_final_gauge_delta_to_bar_hpa": _last_non_none(group_b_rows, "gauge_minus_baro_hpa"),
        "a_group_min_pace_delta_to_bar_hpa": _min_non_none(group_a_rows, "pace_minus_baro_hpa"),
        "a_group_final_pace_delta_to_bar_hpa": _last_non_none(group_a_rows, "pace_minus_baro_hpa"),
        "b_group_min_pace_delta_to_bar_hpa": _min_non_none(group_b_rows, "pace_minus_baro_hpa"),
        "b_group_final_pace_delta_to_bar_hpa": _last_non_none(group_b_rows, "pace_minus_baro_hpa"),
        "a_group_reached_atmospheric_hold": bool(a_reached_atmosphere),
        "b_group_reached_atmospheric_hold": bool(b_reached_atmosphere),
        "comparison_conclusion": comparison_conclusion,
        "saw_vent_status_3": any(_safe_int(row.get("vent_status")) == 3 for row in combined_rows),
        "saw_nonzero_syst_err": any(_system_error_is_nonzero(row.get("syst_err")) for row in combined_rows),
        "gauge_missing_fallback_used": bool(gauge_missing_fallback_used),
        "arm_enabled": arm_enabled,
        "arm_triggered": arm_triggered,
        "arm_trigger_reason": arm_trigger_reason,
        "arm_wait_duration_s": arm_wait_duration_s,
        "precondition_status": precondition_status,
        "precondition_reason": precondition_reason,
        "pace_open_error": pace_open_error,
        "gauge_open_error": gauge_open_error,
        "preprobe": preprobe,
        "arm_rows_csv_path": str(run_dir / "arm_rows.csv"),
        "arm_rows_json_path": str(run_dir / "arm_rows.json"),
        "rows_csv_path": str(run_dir / "rows.csv"),
        "rows_json_path": str(run_dir / "rows.json"),
        "summary_json_path": str(run_dir / "summary.json"),
    }
    _write_json(run_dir / "summary.json", summary)
    return summary


def parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Minimal PACE VENT 1 hold A/B experiment without calibration workflow dependencies."
    )
    parser.add_argument("--config", default=str(ROOT / "configs" / "default_config.json"))
    parser.add_argument("--pace-port", default=None)
    parser.add_argument("--gauge-port", default=None)
    parser.add_argument("--baudrate", type=int, default=9600)
    parser.add_argument("--timeout", type=float, default=1.0)
    parser.add_argument("--gauge-baudrate", type=int, default=None)
    parser.add_argument("--gauge-timeout", type=float, default=None)
    parser.add_argument("--gauge-dest-id", default=None)
    parser.add_argument("--gauge-response-timeout-s", type=float, default=None)
    parser.add_argument("--baseline-duration-s", type=float, default=10.0)
    parser.add_argument("--sample-interval-s", type=float, default=2.0)
    parser.add_argument("--group-duration-s", type=float, default=180.0)
    parser.add_argument("--b-reissue-interval-s", type=float, default=None)
    parser.add_argument("--arm-until-pressurized", action="store_true")
    parser.add_argument("--precondition-threshold-hpa", type=float, default=50.0)
    parser.add_argument("--max-arm-wait-s", type=float, default=1800.0)
    parser.add_argument("--arm-sample-interval-s", type=float, default=2.0)
    parser.add_argument("--output-root", default=str(ROOT / "results" / "pace_vent_hold_ab_test"))
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    summary = run_experiment(args)
    print(f"rows_csv={summary['rows_csv_path']}")
    print(f"rows_json={summary['rows_json_path']}")
    print(f"summary_json={summary['summary_json_path']}")
    print(f"comparison_conclusion={summary['comparison_conclusion']}")
    if str(summary.get("precondition_status")) != "met":
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
