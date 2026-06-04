"""Pressure-source validation without humidity generator or gas cylinders.

This tool samples analyzers plus pressure devices. For ambient quick checks it
keeps the pressure controller continuously vented to atmosphere, matching the
V1.5 open-route water/gas route invariant. It does not switch water/gas routes
or write analyzer coefficients.
"""

from __future__ import annotations

import argparse
import copy
import csv
import json
import re
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence

from ..config import load_config
from ..logging_utils import RunLogger
from ..validation.artifact_rows import normalize_sample_row
from ..validation.common import analyze_sample_rows, build_validation_point, load_csv_rows
from ..validation.pressure_channel import (
    evaluate_pressure_channel_ambient,
    write_pressure_channel_report,
    write_pressure_quick_check_csv,
)
from ..validation.reporting import ValidationMetadata, write_validation_report
from ..workflow.runner import CalibrationRunner
from .run_headless import _build_devices, _close_devices


def _log(message: str) -> None:
    print(message, flush=True)


def _parse_pressure_points(raw: str | None) -> List[Optional[float]]:
    if not raw:
        return [None]
    out: List[Optional[float]] = []
    for part in str(raw).split(","):
        text = part.strip()
        if not text or text.lower() == "ambient":
            out.append(None)
            continue
        out.append(float(text))
    return out or [None]


def _truthy(value: Any) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "y", "on", "pass", "ok", "verified"}


def _decorate_pressure_validation_point(
    point: Any,
    target: Optional[float],
    *,
    controlled: bool = False,
) -> Any:
    if target is None:
        setattr(point, "_pressure_mode", "ambient_open")
        setattr(point, "_pressure_target_label", "ambient")
        setattr(point, "_pressure_selection_token", "ambient")
    else:
        setattr(
            point,
            "_pressure_mode",
            "pace_no_write_controlled" if controlled else "manual_pressure_only",
        )
        setattr(point, "_pressure_target_label", f"{float(target):g}hPa")
        setattr(point, "_pressure_selection_token", f"{float(target):g}")
    return point


def _apply_analyzer_active_upload_hz(runtime_cfg: Dict[str, Any], hz: int) -> None:
    """Configure analyzers for controlled active MODE2 upload during pressure work."""

    target_hz = max(1, int(hz))
    devices_cfg = runtime_cfg.setdefault("devices", {})
    if isinstance(devices_cfg.get("gas_analyzer"), dict):
        devices_cfg["gas_analyzer"]["active_send"] = True
        devices_cfg["gas_analyzer"]["ftd_hz"] = target_hz
    if isinstance(devices_cfg.get("gas_analyzers"), list):
        for analyzer_cfg in devices_cfg["gas_analyzers"]:
            if not isinstance(analyzer_cfg, dict):
                continue
            analyzer_cfg["active_send"] = True
            analyzer_cfg["ftd_hz"] = target_hz

    init_cfg = runtime_cfg.setdefault("workflow", {}).setdefault("analyzer_mode2_init", {})
    init_cfg["read_first_before_config"] = False
    init_cfg["sniff_stream_before_config"] = False
    init_cfg["write_config_on_read_first_fail"] = True
    init_cfg["send_active_freq"] = True
    runtime_cfg.setdefault("metadata", {})["pressure_only_analyzer_startup_policy"] = (
        f"controlled_active_upload_{target_hz}hz_startup_config"
    )
    runtime_cfg["metadata"]["ftd_write_enabled"] = True
    runtime_cfg["metadata"]["analyzer_stream_native_hz"] = float(target_hz)


def _prepare_runtime_cfg(
    cfg: Dict[str, Any],
    *,
    analyzer_active_upload_hz: Optional[int] = None,
) -> Dict[str, Any]:
    runtime_cfg = copy.deepcopy(cfg)
    devices_cfg = runtime_cfg.setdefault("devices", {})
    for key in ("humidity_generator", "dewpoint_meter", "relay", "relay_8", "temperature_chamber", "thermometer"):
        if isinstance(devices_cfg.get(key), dict):
            devices_cfg[key]["enabled"] = False
    workflow_cfg = runtime_cfg.setdefault("workflow", {})
    workflow_cfg["collect_only"] = True
    init_cfg = workflow_cfg.setdefault("analyzer_mode2_init", {})
    init_cfg["read_first_before_config"] = True
    init_cfg["sniff_stream_before_config"] = True
    init_cfg["write_config_on_read_first_fail"] = False
    init_cfg["send_active_freq"] = False
    init_cfg["read_first_attempts"] = max(10, int(init_cfg.get("read_first_attempts", 0) or 0))
    init_cfg["ready_consecutive_frames"] = max(1, int(init_cfg.get("ready_consecutive_frames", 0) or 0))
    init_cfg["read_first_retry_delay_s"] = max(
        0.2,
        float(init_cfg.get("read_first_retry_delay_s", 0.0) or 0.0),
    )
    runtime_cfg.setdefault("metadata", {})["pressure_only_analyzer_startup_policy"] = (
        "read_first_no_startup_config_writes"
    )
    if analyzer_active_upload_hz is not None:
        _apply_analyzer_active_upload_hz(runtime_cfg, int(analyzer_active_upload_hz))
    return runtime_cfg


def _apply_pressure_only_sampling_runtime_defaults(
    runtime_cfg: Dict[str, Any],
    *,
    pre_sample_freshness_timeout_s: float,
    pre_sample_signal_max_age_s: float,
) -> None:
    sampling_cfg = runtime_cfg.setdefault("workflow", {}).setdefault("sampling", {})
    sampling_cfg["pre_sample_freshness_timeout_s"] = max(
        0.0,
        float(pre_sample_freshness_timeout_s),
    )
    sampling_cfg["pre_sample_signal_max_age_s"] = max(
        0.0,
        float(pre_sample_signal_max_age_s),
    )


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Pressure-only validation under ambient air.")
    parser.add_argument("--config", default="configs/default_config.json")
    parser.add_argument("--output-dir", default=None)
    parser.add_argument("--run-id", default=None)
    parser.add_argument(
        "--pressure-points",
        default="ambient",
        help="Comma-separated metadata pressure points, e.g. ambient,500,800,1100. The tool does not control pressure hardware by default.",
    )
    parser.add_argument(
        "--control-pressure-points",
        action="store_true",
        help="Explicitly let PACE generate non-ambient pressure plateaus for pressure-channel no-write evaluation.",
    )
    parser.add_argument(
        "--pressure-control-tolerance-hpa",
        type=float,
        default=1.0,
        help="COM22/PACE reference tolerance for a controlled pressure plateau.",
    )
    parser.add_argument(
        "--pressure-control-stable-s",
        type=float,
        default=5.0,
        help="Required continuous in-tolerance time before sampling a controlled pressure point.",
    )
    parser.add_argument(
        "--pressure-control-timeout-s",
        type=float,
        default=180.0,
        help="Maximum wait time for each controlled pressure point.",
    )
    parser.add_argument(
        "--pressure-control-poll-s",
        type=float,
        default=0.5,
        help="Polling interval while waiting for controlled pressure stability.",
    )
    parser.add_argument(
        "--pressure-control-slew-hpa-per-s",
        type=float,
        default=10.0,
        help="PACE slew rate used only when --control-pressure-points is set.",
    )
    parser.add_argument(
        "--pressure-control-atmosphere-release-wait-s",
        type=float,
        default=1.5,
        help="Wait after stopping continuous atmosphere hold and before enabling PACE output control.",
    )
    parser.add_argument(
        "--pressure-control-post-stable-wait-s",
        type=float,
        default=3.0,
        help="Extra wait after COM22/PACE pressure stability before analyzer pressure sampling starts.",
    )
    parser.add_argument(
        "--pressure-control-analyzer-stream-flush-s",
        type=float,
        default=1.2,
        help="Drain active analyzer upload streams after pressure stability wait to avoid stale pressure frames.",
    )
    parser.add_argument(
        "--pre-sample-freshness-timeout-s",
        type=float,
        default=3.0,
        help="Pressure-only sampling freshness wait for PACE/COM22/analyzer workers before the first row.",
    )
    parser.add_argument(
        "--pre-sample-signal-max-age-s",
        type=float,
        default=1.0,
        help="Maximum age for pressure-only PACE/COM22 fast-signal frames at sampling start.",
    )
    parser.add_argument(
        "--analyzer-active-upload-hz",
        type=int,
        default=1,
        help=(
            "Explicitly configure all analyzers to active MODE2 upload at this rate before pressure sampling. "
            "The V1.5 default is 1 Hz for controlled pressure/SENCO9 workflows."
        ),
    )
    parser.add_argument(
        "--no-analyzer-active-upload-config",
        action="store_true",
        help="Do not send FTD/SETCOMWAY startup configuration; use read-first fragile-serial fallback.",
    )
    parser.add_argument("--count", type=int, default=None)
    parser.add_argument("--interval-s", type=float, default=None)
    parser.add_argument(
        "--pressure-reference-json",
        default=None,
        help="Optional COM22 pressure-reference certificate snapshot JSON for formal pressure-channel report.",
    )
    parser.add_argument(
        "--continuous-atmosphere-hold",
        dest="continuous_atmosphere_hold",
        action="store_true",
        default=True,
        help="For ambient pressure quick checks, keep PACE continuously vented to atmosphere before/during sampling.",
    )
    parser.add_argument(
        "--no-continuous-atmosphere-hold",
        dest="continuous_atmosphere_hold",
        action="store_false",
        help="Disable PACE continuous atmosphere hold; resulting ambient rows are diagnostic only.",
    )
    parser.add_argument(
        "--require-continuous-atmosphere-hold",
        action="store_true",
        help="Fail before sampling when ambient continuous atmosphere hold cannot be verified.",
    )
    parser.add_argument(
        "--no-prompt",
        action="store_true",
        help="Do not prompt between batches. Useful for a single ambient batch or tests.",
    )
    return parser.parse_args(list(argv) if argv is not None else None)


def _pressure_atmosphere_hold_fields_disabled(reason: str) -> Dict[str, Any]:
    return {
        "pressure_atmosphere_hold_required": False,
        "pressure_atmosphere_hold_status": "disabled",
        "pressure_atmosphere_hold_active": False,
        "pressure_atmosphere_hold_strategy": "",
        "pressure_atmosphere_hold_reason": reason,
        "pressure_atmosphere_hold_vent_status": "",
        "pressure_atmosphere_hold_output_state": "",
        "pressure_atmosphere_hold_isolation_state": "",
    }


def _pressure_atmosphere_hold_fields_not_applicable(reason: str) -> Dict[str, Any]:
    fields = _pressure_atmosphere_hold_fields_disabled(reason)
    fields.update(
        {
            "pressure_atmosphere_hold_required": False,
            "pressure_atmosphere_hold_status": "not_applicable",
        }
    )
    return fields


def _ensure_pressure_atmosphere_hold(
    runner: CalibrationRunner,
    *,
    enabled: bool,
    require: bool,
    reason: str,
) -> Dict[str, Any]:
    """Enter the same continuous-atmosphere state used by V1.5 open routes."""

    if not enabled:
        return _pressure_atmosphere_hold_fields_disabled("continuous_atmosphere_hold_disabled_by_cli")

    pace = runner.devices.get("pace")
    if pace is None:
        fields = {
            "pressure_atmosphere_hold_required": True,
            "pressure_atmosphere_hold_status": "unavailable",
            "pressure_atmosphere_hold_active": False,
            "pressure_atmosphere_hold_strategy": "",
            "pressure_atmosphere_hold_reason": "pace_unavailable",
            "pressure_atmosphere_hold_vent_status": "",
            "pressure_atmosphere_hold_output_state": "",
            "pressure_atmosphere_hold_isolation_state": "",
        }
        if require:
            raise RuntimeError("PRESSURE_CHANNEL_ATMOSPHERE_HOLD_UNAVAILABLE")
        return fields

    vent_ok = bool(runner._set_pressure_controller_vent(True, reason=reason))
    runner._refresh_pressure_controller_atmosphere_hold(force=True, reason=reason)
    snapshot = runner._pace_state_snapshot(pace, refresh=True)
    hold_active = bool(runner._pressure_controller_atmosphere_hold_active(pace))
    status = "verified" if vent_ok and hold_active else "not_verified"
    fields = {
        "pressure_atmosphere_hold_required": True,
        "pressure_atmosphere_hold_status": status,
        "pressure_atmosphere_hold_active": hold_active,
        "pressure_atmosphere_hold_strategy": str(getattr(runner, "_pressure_atmosphere_hold_strategy", "") or ""),
        "pressure_atmosphere_hold_reason": reason,
        "pressure_atmosphere_hold_vent_status": snapshot.get("pace_vent_status", ""),
        "pressure_atmosphere_hold_output_state": snapshot.get("pace_output_state", ""),
        "pressure_atmosphere_hold_isolation_state": snapshot.get("pace_isolation_state", ""),
    }
    if require and status != "verified":
        raise RuntimeError("PRESSURE_CHANNEL_ATMOSPHERE_HOLD_NOT_VERIFIED")
    return fields


def _annotate_rows_with_pressure_atmosphere_hold(
    rows: Sequence[Dict[str, Any]],
    fields: Mapping[str, Any],
) -> None:
    for row in rows:
        row.update(dict(fields))


def _pressure_control_fields(
    *,
    enabled: bool,
    status: str,
    reason: str,
    target_hpa: Optional[float] = None,
    reference_hpa: Optional[float] = None,
    pace_hpa: Optional[float] = None,
    error_hpa: Optional[float] = None,
    stable_s: float = 0.0,
    elapsed_s: float = 0.0,
    output_state: Any = "",
    vent_status: Any = "",
    isolation_state: Any = "",
) -> Dict[str, Any]:
    return {
        "pressure_control_enabled": bool(enabled),
        "pressure_control_status": status,
        "pressure_control_reason": reason,
        "pressure_control_target_hpa": target_hpa if target_hpa is not None else "",
        "pressure_control_reference_hpa": reference_hpa if reference_hpa is not None else "",
        "pressure_control_pace_hpa": pace_hpa if pace_hpa is not None else "",
        "pressure_control_error_hpa": error_hpa if error_hpa is not None else "",
        "pressure_control_stable_s": round(float(stable_s), 3),
        "pressure_control_elapsed_s": round(float(elapsed_s), 3),
        "pressure_control_output_state": output_state,
        "pressure_control_vent_status": vent_status,
        "pressure_control_isolation_state": isolation_state,
        "pressure_control_atmosphere_release_status": "not_applicable",
        "pressure_control_atmosphere_release_reason": "",
        "pressure_control_atmosphere_release_wait_s": "",
        "pressure_control_atmosphere_release_hold_active": "",
        "pressure_control_atmosphere_release_vent_status": "",
        "pressure_control_atmosphere_release_output_state": "",
        "pressure_control_atmosphere_release_isolation_state": "",
        "pressure_control_atmosphere_release_vent_after_valve_open": "",
        "pressure_control_post_stable_wait_s": "",
        "pressure_control_analyzer_stream_flush_s": "",
        "pressure_control_analyzer_stream_flush_status": "not_applicable",
        "pressure_control_analyzer_stream_flush_count": "",
        "pressure_control_controls_water_or_gas_routes": False,
        "pressure_control_writes_senco": False,
        "pressure_control_writes_device_id": False,
        "pressure_control_not_real_acceptance_evidence": True,
    }


def _call_pressure_gauge_read(pressure_gauge: Any) -> float:
    read_fast = getattr(pressure_gauge, "read_pressure_fast", None)
    if callable(read_fast):
        try:
            return float(read_fast(response_timeout_s=0.5, retries=1, clear_buffer=False))
        except TypeError:
            return float(read_fast())
        except Exception:
            pass
    read = getattr(pressure_gauge, "read_pressure", None)
    if not callable(read):
        raise RuntimeError("PRESSURE_GAUGE_READ_UNAVAILABLE")
    try:
        return float(read(response_timeout_s=2.2, retries=3, clear_buffer=True))
    except TypeError:
        return float(read())


def _read_pressure_reference_pair(runner: CalibrationRunner) -> tuple[Optional[float], Optional[float], str]:
    pressure_gauge = runner.devices.get("pressure_gauge")
    pace = runner.devices.get("pace")
    com22_hpa: Optional[float] = None
    pace_hpa: Optional[float] = None
    errors: List[str] = []
    if pressure_gauge is not None:
        try:
            com22_hpa = _call_pressure_gauge_read(pressure_gauge)
        except Exception as exc:
            errors.append(f"com22:{exc}")
    else:
        errors.append("com22:unavailable")
    if pace is not None:
        read_pressure = getattr(pace, "read_pressure", None)
        if callable(read_pressure):
            try:
                pace_hpa = float(read_pressure())
            except Exception as exc:
                errors.append(f"pace:{exc}")
        else:
            errors.append("pace:read_unavailable")
    else:
        errors.append("pace:unavailable")
    return com22_hpa, pace_hpa, ";".join(errors)


def _safe_pace_state(pace: Any, name: str) -> Any:
    getter = getattr(pace, name, None)
    if not callable(getter):
        return ""
    try:
        return getter()
    except Exception:
        return ""


def _pace_hold_active(pace: Any) -> Any:
    checker = getattr(pace, "is_atmosphere_hold_active", None)
    if not callable(checker):
        return ""
    try:
        return bool(checker())
    except Exception:
        return ""


def _pace_vent_after_valve_open(pace: Any) -> Any:
    getter = getattr(pace, "get_vent_after_valve_open", None)
    if not callable(getter):
        return ""
    try:
        return bool(getter())
    except Exception:
        return ""


def _pace_vent_status_allows_control(pace: Any, status: Any) -> bool:
    checker = getattr(pace, "vent_status_allows_control", None)
    if callable(checker):
        try:
            return bool(checker(status))
        except Exception:
            return False
    try:
        return int(float(str(status).strip())) in {0, 2, 3, 4}
    except Exception:
        return False


def _pace_state_is_int(value: Any, expected: int) -> bool:
    try:
        return int(float(str(value).strip())) == int(expected)
    except Exception:
        return False


def _close_vent_after_valve_if_supported(pace: Any) -> tuple[Any, str]:
    setter = getattr(pace, "set_vent_after_valve_open", None)
    getter = getattr(pace, "get_vent_after_valve_open", None)
    if not callable(setter):
        return "", "unsupported"
    try:
        setter(False)
    except Exception as exc:
        if "VENT_AFTER_VALVE_UNSUPPORTED" in str(exc):
            return "", "unsupported"
        return "", f"set_failed:{exc}"
    if not callable(getter):
        return "", "set_without_readback"
    try:
        return bool(getter()), ""
    except Exception as exc:
        return "", f"readback_failed:{exc}"


def _release_pressure_atmosphere_before_control(
    runner: CalibrationRunner,
    *,
    wait_s: float,
) -> Dict[str, Any]:
    pace = runner.devices.get("pace")
    if pace is None:
        raise RuntimeError("PRESSURE_CONTROL_PACE_UNAVAILABLE")

    reason_parts: List[str] = []
    vent_off_ok = bool(
        runner._set_pressure_controller_vent(
            False,
            reason="pressure channel no-write control: close atmosphere before setpoint",
        )
    )
    if not vent_off_ok:
        reason_parts.append("vent_off_failed")

    vent_after_valve_open, aux_reason = _close_vent_after_valve_if_supported(pace)
    if aux_reason and aux_reason != "unsupported":
        reason_parts.append(f"vent_after_valve:{aux_reason}")

    settle_s = max(0.0, float(wait_s or 0.0))
    if settle_s > 0:
        time.sleep(settle_s)

    wait_idle = getattr(pace, "wait_for_vent_idle", None)
    if callable(wait_idle):
        try:
            wait_idle(timeout_s=10.0, poll_s=0.2)
        except Exception as exc:
            reason_parts.append(f"vent_idle_wait:{exc}")

    hold_active = _pace_hold_active(pace)
    vent_status = _safe_pace_state(pace, "get_vent_status")
    output_state = _safe_pace_state(pace, "get_output_state")
    isolation_state = _safe_pace_state(pace, "get_isolation_state")
    if vent_after_valve_open == "":
        vent_after_valve_open = _pace_vent_after_valve_open(pace)

    hold_stopped = hold_active is False or hold_active == ""
    vent_ready = _pace_vent_status_allows_control(pace, vent_status)
    output_off = _pace_state_is_int(output_state, 0) or output_state == ""
    isolation_open = _pace_state_is_int(isolation_state, 1) or isolation_state == ""
    aux_closed = vent_after_valve_open is not True
    status = "verified" if vent_off_ok and hold_stopped and vent_ready and output_off and isolation_open and aux_closed else "not_verified"
    if not hold_stopped:
        reason_parts.append("atmosphere_hold_still_active")
    if not vent_ready:
        reason_parts.append(f"vent_status_not_ready:{vent_status}")
    if not output_off:
        reason_parts.append(f"output_not_off:{output_state}")
    if not isolation_open:
        reason_parts.append(f"isolation_not_open:{isolation_state}")
    if not aux_closed:
        reason_parts.append("vent_after_valve_still_open")

    fields = {
        "pressure_control_atmosphere_release_status": status,
        "pressure_control_atmosphere_release_reason": ";".join(reason_parts) or "closed_before_control",
        "pressure_control_atmosphere_release_wait_s": settle_s,
        "pressure_control_atmosphere_release_hold_active": hold_active,
        "pressure_control_atmosphere_release_vent_status": vent_status,
        "pressure_control_atmosphere_release_output_state": output_state,
        "pressure_control_atmosphere_release_isolation_state": isolation_state,
        "pressure_control_atmosphere_release_vent_after_valve_open": vent_after_valve_open,
    }
    if status != "verified":
        raise RuntimeError(f"PRESSURE_ATMOSPHERE_RELEASE_NOT_VERIFIED:{fields['pressure_control_atmosphere_release_reason']}")
    return fields


def _analyzer_entries_for_stream_flush(runner: CalibrationRunner) -> List[tuple[str, Any]]:
    entries: List[tuple[str, Any]] = []
    all_analyzers = getattr(runner, "_active_gas_analyzers", None)
    if callable(all_analyzers):
        for item in list(all_analyzers() or []):
            if len(item) >= 2:
                entries.append((str(item[0]), item[1]))
    if entries:
        return entries
    ga = runner.devices.get("gas_analyzer")
    if ga is not None:
        entries.append(("ga01", ga))
    return entries


def _settle_analyzer_pressure_stream_after_control(
    runner: CalibrationRunner,
    *,
    wait_s: float,
    drain_s: float,
) -> Dict[str, Any]:
    wait_value = max(0.0, float(wait_s or 0.0))
    drain_value = max(0.0, float(drain_s or 0.0))
    if wait_value > 0:
        time.sleep(wait_value)
    flush_count = 0
    errors: List[str] = []
    if drain_value > 0:
        for label, analyzer in _analyzer_entries_for_stream_flush(runner):
            drain_lines = getattr(analyzer, "_drain_stream_lines", None)
            if not callable(drain_lines):
                continue
            try:
                lines = drain_lines(drain_s=drain_value, read_timeout_s=0.05)
                flush_count += len(lines or [])
            except TypeError:
                try:
                    lines = drain_lines()
                    flush_count += len(lines or [])
                except Exception as exc:
                    errors.append(f"{label}:{exc}")
            except Exception as exc:
                errors.append(f"{label}:{exc}")
    return {
        "pressure_control_post_stable_wait_s": wait_value,
        "pressure_control_analyzer_stream_flush_s": drain_value,
        "pressure_control_analyzer_stream_flush_status": "error" if errors else "done",
        "pressure_control_analyzer_stream_flush_count": flush_count,
        "pressure_control_analyzer_stream_flush_reason": ";".join(errors),
    }


def _prepare_pace_control(
    runner: CalibrationRunner,
    *,
    target_hpa: float,
    slew_hpa_per_s: float,
    atmosphere_release_wait_s: float,
) -> Dict[str, Any]:
    pace = runner.devices.get("pace")
    if pace is None:
        raise RuntimeError("PRESSURE_CONTROL_PACE_UNAVAILABLE")
    release_fields = _release_pressure_atmosphere_before_control(
        runner,
        wait_s=atmosphere_release_wait_s,
    )
    for method_name, args in (
        ("set_units_hpa", ()),
        ("set_slew_mode_linear", ()),
        ("set_slew_rate", (float(slew_hpa_per_s),)),
        ("set_overshoot_allowed", (False,)),
        ("set_in_limits", (0.02, 10.0)),
    ):
        method = getattr(pace, method_name, None)
        if callable(method):
            try:
                method(*args)
            except Exception as exc:
                _log(f"Pressure control warning: {method_name} failed: {exc}")
    setpoint = getattr(pace, "set_setpoint", None)
    if not callable(setpoint):
        raise RuntimeError("PRESSURE_CONTROL_SETPOINT_UNAVAILABLE")
    setpoint(float(target_hpa))
    enable_output = getattr(pace, "enable_control_output", None)
    if callable(enable_output):
        enable_output(timeout_s=3.0, poll_s=0.1)
    else:
        set_output_mode_active = getattr(pace, "set_output_mode_active", None)
        set_output = getattr(pace, "set_output", None)
        set_isolation_open = getattr(pace, "set_isolation_open", None)
        if callable(set_isolation_open):
            set_isolation_open(True)
        if callable(set_output_mode_active):
            set_output_mode_active()
        if callable(set_output):
            set_output(True)
    return release_fields


def _wait_for_controlled_pressure_point(
    runner: CalibrationRunner,
    *,
    target_hpa: float,
    tolerance_hpa: float,
    stable_s: float,
    timeout_s: float,
    poll_s: float,
    slew_hpa_per_s: float,
    atmosphere_release_wait_s: float,
    post_stable_wait_s: float,
    analyzer_stream_flush_s: float,
) -> Dict[str, Any]:
    pace = runner.devices.get("pace")
    if pace is None:
        raise RuntimeError("PRESSURE_CONTROL_PACE_UNAVAILABLE")
    release_fields = _prepare_pace_control(
        runner,
        target_hpa=target_hpa,
        slew_hpa_per_s=slew_hpa_per_s,
        atmosphere_release_wait_s=atmosphere_release_wait_s,
    )

    deadline = time.monotonic() + max(1.0, float(timeout_s))
    start = time.monotonic()
    stable_start: Optional[float] = None
    last_reference: Optional[float] = None
    last_pace: Optional[float] = None
    last_error: Optional[float] = None
    last_reason = "not_started"
    stable_for = 0.0
    required_stable_s = max(0.0, float(stable_s))
    allowed_error = max(0.01, abs(float(tolerance_hpa)))
    poll_interval = max(0.05, float(poll_s))

    while time.monotonic() <= deadline:
        com22_hpa, pace_hpa, read_error = _read_pressure_reference_pair(runner)
        reference_hpa = com22_hpa if com22_hpa is not None else pace_hpa
        last_reference = reference_hpa
        last_pace = pace_hpa
        if reference_hpa is None:
            stable_start = None
            stable_for = 0.0
            last_reason = read_error or "pressure_reference_unavailable"
        else:
            last_error = float(reference_hpa) - float(target_hpa)
            if abs(last_error) <= allowed_error:
                now = time.monotonic()
                if stable_start is None:
                    stable_start = now
                stable_for = now - stable_start
                last_reason = "in_tolerance"
                if stable_for >= required_stable_s:
                    settle_fields = _settle_analyzer_pressure_stream_after_control(
                        runner,
                        wait_s=post_stable_wait_s,
                        drain_s=analyzer_stream_flush_s,
                    )
                    elapsed_s = time.monotonic() - start
                    fields = _pressure_control_fields(
                        enabled=True,
                        status="verified",
                        reason="reference_pressure_in_tolerance",
                        target_hpa=float(target_hpa),
                        reference_hpa=reference_hpa,
                        pace_hpa=pace_hpa,
                        error_hpa=last_error,
                        stable_s=stable_for,
                        elapsed_s=elapsed_s,
                        output_state=_safe_pace_state(pace, "get_output_state"),
                        vent_status=_safe_pace_state(pace, "get_vent_status"),
                        isolation_state=_safe_pace_state(pace, "get_isolation_state"),
                    )
                    fields.update(release_fields)
                    fields.update(settle_fields)
                    return fields
            else:
                stable_start = None
                stable_for = 0.0
                last_reason = "outside_tolerance"
        time.sleep(poll_interval)

    elapsed_s = time.monotonic() - start
    fields = _pressure_control_fields(
        enabled=True,
        status="timeout",
        reason=last_reason,
        target_hpa=float(target_hpa),
        reference_hpa=last_reference,
        pace_hpa=last_pace,
        error_hpa=last_error,
        stable_s=stable_for,
        elapsed_s=elapsed_s,
        output_state=_safe_pace_state(pace, "get_output_state"),
        vent_status=_safe_pace_state(pace, "get_vent_status"),
        isolation_state=_safe_pace_state(pace, "get_isolation_state"),
    )
    fields.update(release_fields)
    raise RuntimeError(
        "PRESSURE_CONTROL_TARGET_NOT_STABLE "
        f"target={target_hpa:g}hPa status={fields['pressure_control_status']} "
        f"reason={fields['pressure_control_reason']} error={fields['pressure_control_error_hpa']}"
    )


def _restore_pressure_controller_to_atmosphere(devices: Mapping[str, Any]) -> None:
    pace = devices.get("pace") if isinstance(devices, Mapping) else None
    if pace is None:
        return
    enter_atmosphere = getattr(pace, "enter_atmosphere_mode", None)
    if callable(enter_atmosphere):
        try:
            enter_atmosphere(hold_open=False, timeout_s=30.0, poll_s=0.25)
            return
        except TypeError:
            try:
                enter_atmosphere(hold_open=False)
                return
            except Exception:
                pass
        except Exception as exc:
            _log(f"Pressure control warning: atmosphere restore failed: {exc}")
    set_output = getattr(pace, "set_output", None)
    vent = getattr(pace, "vent", None)
    try:
        if callable(set_output):
            set_output(False)
        if callable(vent):
            vent(True)
    except Exception as exc:
        _log(f"Pressure control warning: fallback vent restore failed: {exc}")


def _rewrite_logger_samples_with_current_schema(logger: RunLogger) -> None:
    """Rewrite the main sample CSV after late evidence fields are added."""

    logger_rows = getattr(logger, "_samples_rows", None)
    rewrite_samples = getattr(logger, "_rewrite_samples_csv", None)
    if not isinstance(logger_rows, list) or not callable(rewrite_samples):
        return
    header: List[str] = []
    for row in logger_rows:
        if not isinstance(row, Mapping):
            continue
        for key in row:
            text = str(key)
            if text not in header:
                header.append(text)
    rewrite_samples(target_header=header)


def _normalize_analyzer_prefix(label: Any) -> str:
    text = str(label or "").strip().lower()
    match = re.fullmatch(r"ga0*(\d{1,2})", text)
    if match:
        index = int(match.group(1))
        if 1 <= index <= 8:
            return f"ga{index:02d}"
    return text


def _discover_analyzer_prefixes(
    rows: Sequence[Mapping[str, Any]],
    runner: CalibrationRunner,
) -> List[str]:
    prefixes: List[str] = []

    analyzer_entries = []
    active = getattr(runner, "_active_gas_analyzers", None)
    all_analyzers = getattr(runner, "_gas_analyzers", None)
    if callable(active):
        analyzer_entries = list(active() or [])
    if not analyzer_entries and callable(all_analyzers):
        analyzer_entries = list(all_analyzers() or [])

    for label, *_rest in analyzer_entries:
        prefix = _normalize_analyzer_prefix(label)
        if prefix and prefix not in prefixes:
            prefixes.append(prefix)

    key_pattern = re.compile(r"^(ga\d{2})_(?:pressure_kpa|frame_usable|co2_ppm|h2o_mmol)$", re.IGNORECASE)
    for row in rows:
        for key in row.keys():
            match = key_pattern.match(str(key))
            if not match:
                continue
            prefix = _normalize_analyzer_prefix(match.group(1))
            if prefix and prefix not in prefixes:
                prefixes.append(prefix)

    if not prefixes:
        prefixes.append("ga01")
    return sorted(prefixes[:8])


def _load_pressure_reference_snapshot(path: str | None) -> Dict[str, Any]:
    if not path:
        return {}
    source = Path(path)
    if not source.exists():
        return {}
    return json.loads(source.read_text(encoding="utf-8"))


def _write_multi_analyzer_pressure_summary(
    output_dir: Path,
    rows: Sequence[Mapping[str, Any]],
    *,
    analyzer_prefixes: Sequence[str],
    pressure_reference: Mapping[str, Any],
) -> Path:
    summary_rows: List[Dict[str, Any]] = []
    for prefix in analyzer_prefixes:
        result = evaluate_pressure_channel_ambient(
            rows,
            pressure_reference=pressure_reference,
            analyzer_prefix=prefix,
        )
        summary_rows.append(
            {
                "analyzer_prefix": prefix,
                "analyzer_device_id": result.analyzer_device_id,
                "identity_note": "analyzer_prefix is the acquisition channel; analyzer_device_id is the analyzer identity",
                "status": result.status,
                "validation_level": result.validation_level,
                "allowed_for_co2_h2o_formal_work": result.allowed_for_co2_h2o_formal_work,
                "reason": result.reason,
                "sample_count": result.sample_count,
                "valid_pair_count": result.valid_pair_count,
                "rejected_pair_count": result.rejected_pair_count,
                "analyzer_pressure_mean_hpa": result.analyzer_pressure_mean_hpa,
                "com22_pressure_mean_hpa": result.com22_pressure_mean_hpa,
                "pace_pressure_mean_hpa": result.pace_pressure_mean_hpa,
                "analyzer_minus_com22_mean_hpa": result.analyzer_minus_com22_mean_hpa,
                "analyzer_minus_com22_max_abs_hpa": result.analyzer_minus_com22_max_abs_hpa,
                "pace_minus_com22_mean_hpa": result.pace_minus_com22_mean_hpa,
            }
        )

    path = output_dir / "pressure_channel_multi_analyzer_summary.csv"
    header: List[str] = []
    for row in summary_rows:
        for key in row:
            if key not in header:
                header.append(key)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=header)
        writer.writeheader()
        writer.writerows(summary_rows)
    return path


def _analyzer_sampling_worker_fields(runner: CalibrationRunner) -> Dict[str, Any]:
    active_entries = []
    all_analyzers = getattr(runner, "_active_gas_analyzers", None)
    if callable(all_analyzers):
        active_entries = list(all_analyzers() or [])
    active_send_labels: List[str] = []
    passive_labels: List[str] = []
    settings_reader = getattr(runner, "_gas_analyzer_runtime_settings", None)
    for label, _ga, cfg in active_entries:
        settings = settings_reader(cfg) if callable(settings_reader) else dict(cfg or {})
        if bool(settings.get("active_send")):
            active_send_labels.append(str(label))
        else:
            passive_labels.append(str(label))
    mode = "per_device" if len(active_send_labels) > 1 else "shared_or_passive"
    return {
        "analyzer_sampling_worker_mode": mode,
        "analyzer_sampling_active_count": len(active_send_labels),
        "analyzer_sampling_passive_count": len(passive_labels),
        "analyzer_sampling_active_labels": ",".join(active_send_labels),
        "analyzer_sampling_passive_labels": ",".join(passive_labels),
        "analyzer_sampling_identity_note": "parallelism is by acquisition channel; identity is analyzer_device_id",
    }


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    cfg = load_config(args.config)
    runtime_cfg = _prepare_runtime_cfg(
        cfg,
        analyzer_active_upload_hz=None
        if bool(args.no_analyzer_active_upload_config)
        else args.analyzer_active_upload_hz,
    )
    _apply_pressure_only_sampling_runtime_defaults(
        runtime_cfg,
        pre_sample_freshness_timeout_s=float(args.pre_sample_freshness_timeout_s),
        pre_sample_signal_max_age_s=float(args.pre_sample_signal_max_age_s),
    )
    if args.count is not None:
        runtime_cfg.setdefault("workflow", {}).setdefault("sampling", {})["stable_count"] = int(args.count)
        runtime_cfg["workflow"]["sampling"]["count"] = int(args.count)
    if args.interval_s is not None:
        runtime_cfg.setdefault("workflow", {}).setdefault("sampling", {})["interval_s"] = float(args.interval_s)

    output_dir = Path(args.output_dir).resolve() if args.output_dir else Path(runtime_cfg["paths"]["output_dir"]).resolve()
    run_id = args.run_id or f"pressure_only_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    logger = RunLogger(output_dir, run_id=run_id, cfg=runtime_cfg)
    devices: Dict[str, Any] = {}

    try:
        pressure_points = _parse_pressure_points(args.pressure_points)
        _log(
            "Validation mode: pressure-only ambient verification. "
            "No water/gas route switching. No gas-cylinder or humidity-generator dependency. "
            "Ambient rows require continuous atmosphere-hold evidence. "
            "Non-ambient pressure control is enabled only with --control-pressure-points."
        )
        devices = _build_devices(runtime_cfg, io_logger=logger)
        if "pressure_gauge" not in devices and "pace" not in devices:
            _log("Pressure-only validation warning: no pressure_gauge / pace device available; report will only include analyzer BAR if present.")
        runner = CalibrationRunner(runtime_cfg, devices, logger, _log, lambda *_: None)
        if any(target is None for target in pressure_points):
            _ensure_pressure_atmosphere_hold(
                runner,
                enabled=bool(args.continuous_atmosphere_hold),
                require=bool(args.require_continuous_atmosphere_hold),
                reason="pressure channel quick check pre-startup continuous atmosphere",
            )
        runner._configure_devices()

        for index, target in enumerate(pressure_points, start=1):
            if not args.no_prompt:
                prompt = "ambient pressure" if target is None else f"{target:g} hPa"
                input(f"Prepare current pressure condition at {prompt}, then press Enter to sample...")
            point_tag = "pressure_only_ambient" if target is None else f"pressure_only_{target:g}hpa"
            point = build_validation_point(
                index=index,
                temp_c=20.0,
                pressure_hpa=target,
                point_tag=point_tag,
            )
            point = _decorate_pressure_validation_point(
                point,
                target,
                controlled=bool(args.control_pressure_points),
            )
            if target is None:
                hold_fields = _ensure_pressure_atmosphere_hold(
                    runner,
                    enabled=bool(args.continuous_atmosphere_hold),
                    require=bool(args.require_continuous_atmosphere_hold),
                    reason="pressure channel quick check continuous atmosphere",
                )
                control_fields = _pressure_control_fields(
                    enabled=False,
                    status="not_applicable",
                    reason="ambient_open_point",
                    target_hpa=target,
                )
            else:
                hold_fields = _pressure_atmosphere_hold_fields_not_applicable(
                    (
                        "non_ambient_pace_no_write_controlled_pressure_point"
                        if args.control_pressure_points
                        else "non_ambient_manual_pressure_point"
                    )
                )
                if args.control_pressure_points:
                    control_fields = _wait_for_controlled_pressure_point(
                        runner,
                        target_hpa=float(target),
                        tolerance_hpa=float(args.pressure_control_tolerance_hpa),
                        stable_s=float(args.pressure_control_stable_s),
                        timeout_s=float(args.pressure_control_timeout_s),
                        poll_s=float(args.pressure_control_poll_s),
                        slew_hpa_per_s=float(args.pressure_control_slew_hpa_per_s),
                        atmosphere_release_wait_s=float(args.pressure_control_atmosphere_release_wait_s),
                        post_stable_wait_s=float(args.pressure_control_post_stable_wait_s),
                        analyzer_stream_flush_s=float(args.pressure_control_analyzer_stream_flush_s),
                    )
                else:
                    control_fields = _pressure_control_fields(
                        enabled=False,
                        status="manual_or_uncontrolled",
                        reason="control_pressure_points_not_enabled",
                        target_hpa=float(target),
                    )
            all_samples_start = len(runner._all_samples)
            logger_rows_start = len(getattr(logger, "_samples_rows", []) or [])
            runner._sample_and_log(point, phase="co2", point_tag=point_tag)
            analyzer_worker_fields = _analyzer_sampling_worker_fields(runner)
            _annotate_rows_with_pressure_atmosphere_hold(
                runner._all_samples[all_samples_start:],
                {**hold_fields, **control_fields, **analyzer_worker_fields},
            )
            logger_rows = getattr(logger, "_samples_rows", None)
            if isinstance(logger_rows, list):
                _annotate_rows_with_pressure_atmosphere_hold(
                    logger_rows[logger_rows_start:],
                    {**hold_fields, **control_fields, **analyzer_worker_fields},
                )
                _rewrite_logger_samples_with_current_schema(logger)

        if logger.samples_path.exists():
            pressure_rows = [normalize_sample_row(row) for row in load_csv_rows(logger.samples_path)]
        else:
            pressure_rows = [normalize_sample_row(row) for row in runner._all_samples]
        analyzer_prefixes = _discover_analyzer_prefixes(pressure_rows, runner)
        pressure_reference = _load_pressure_reference_snapshot(args.pressure_reference_json)
        pressure_quick_check_paths: Dict[str, Path] = {}
        pressure_quick_check_all_path: Optional[Path] = None
        if len(analyzer_prefixes) > 1:
            pressure_quick_check_all_path = write_pressure_quick_check_csv(
                logger.run_dir,
                pressure_rows,
                analyzer_prefix="all",
                run_id=f"{run_id}_all",
            )
        for prefix in analyzer_prefixes:
            quick_check_run_id = run_id if prefix == "ga01" else f"{run_id}_{prefix}"
            pressure_quick_check_paths[prefix] = write_pressure_quick_check_csv(
                logger.run_dir,
                pressure_rows,
                analyzer_prefix=prefix,
                run_id=quick_check_run_id,
            )
        pressure_quick_check_path = pressure_quick_check_paths.get("ga01") or next(iter(pressure_quick_check_paths.values()))
        multi_analyzer_summary_path = _write_multi_analyzer_pressure_summary(
            logger.run_dir,
            pressure_rows,
            analyzer_prefixes=analyzer_prefixes,
            pressure_reference=pressure_reference,
        )

        tables = analyze_sample_rows(
            runner._all_samples,
            cfg=runtime_cfg,
            gas="both",
            modes=("current",),
        )
        metadata = ValidationMetadata(
            tool_name="validate_pressure_only",
            analyzers=sorted({str(row.get("Analyzer") or "") for row in tables["frame_quality_summary"] if row.get("Analyzer")}),
            input_paths=[str(logger.samples_path), str(logger.points_path), str(logger.analyzer_summary_csv_path)],
            output_dir=str(logger.run_dir),
            config_path=str(Path(args.config).resolve()),
            config_summary={
                "pressure_points": ["ambient" if item is None else item for item in pressure_points],
                "sample_count": int(runtime_cfg["workflow"]["sampling"].get("stable_count", runtime_cfg["workflow"]["sampling"].get("count", 10))),
                "sample_interval_s": float(runtime_cfg["workflow"]["sampling"].get("interval_s", 1.0)),
                "prompt_between_batches": not bool(args.no_prompt),
                "continuous_atmosphere_hold": bool(args.continuous_atmosphere_hold),
                "require_continuous_atmosphere_hold": bool(args.require_continuous_atmosphere_hold),
                "control_pressure_points": bool(args.control_pressure_points),
                "pressure_control_tolerance_hpa": float(args.pressure_control_tolerance_hpa),
                "pressure_control_stable_s": float(args.pressure_control_stable_s),
                "pressure_control_timeout_s": float(args.pressure_control_timeout_s),
                "pressure_control_slew_hpa_per_s": float(args.pressure_control_slew_hpa_per_s),
                "pressure_control_atmosphere_release_wait_s": float(args.pressure_control_atmosphere_release_wait_s),
                "pressure_control_post_stable_wait_s": float(args.pressure_control_post_stable_wait_s),
                "pressure_control_analyzer_stream_flush_s": float(args.pressure_control_analyzer_stream_flush_s),
                "pre_sample_freshness_timeout_s": float(args.pre_sample_freshness_timeout_s),
                "pre_sample_signal_max_age_s": float(args.pre_sample_signal_max_age_s),
                "pressure_quick_check_csv": str(pressure_quick_check_path),
                "pressure_quick_check_csv_all": str(pressure_quick_check_all_path or pressure_quick_check_path),
                "pressure_quick_check_csv_by_analyzer": {
                    prefix: str(path) for prefix, path in pressure_quick_check_paths.items()
                },
                "pressure_channel_multi_analyzer_summary_csv": str(multi_analyzer_summary_path),
                "analyzer_prefixes": list(analyzer_prefixes),
                "analyzer_sampling_worker": _analyzer_sampling_worker_fields(runner),
            },
            notes=[
                "Ambient pressure-channel quick checks keep PACE continuously vented to atmosphere when available.",
                "Controlled non-ambient pressure points are pressure-channel no-write evidence only when explicitly enabled.",
                "The tool does not switch water/gas routes and does not write analyzer coefficients.",
                "Rows without verified continuous atmosphere-hold evidence are diagnostic only.",
            ],
        )
        outputs = write_validation_report(
            logger.run_dir,
            prefix="pressure_only_validation",
            metadata=metadata,
            tables=tables,
        )
        pressure_outputs: Dict[str, Path] = {}
        for prefix, quick_check_path in pressure_quick_check_paths.items():
            report_samples_csv = pressure_quick_check_all_path or quick_check_path
            output_dir = logger.run_dir / (
                "pressure_channel_validation"
                if prefix == "ga01"
                else f"pressure_channel_validation_{prefix}"
            )
            prefix_outputs = write_pressure_channel_report(
                run_dir=logger.run_dir,
                output_dir=output_dir,
                pressure_reference_path=args.pressure_reference_json,
                samples_csv=report_samples_csv,
                analyzer_prefix=prefix,
            )
            if prefix == "ga01":
                pressure_outputs = prefix_outputs
        pressure_outputs_all: Dict[str, Path] = {}
        if pressure_quick_check_all_path is not None:
            pressure_outputs_all = write_pressure_channel_report(
                run_dir=logger.run_dir,
                output_dir=logger.run_dir / "pressure_channel_validation_all",
                pressure_reference_path=args.pressure_reference_json,
                samples_csv=pressure_quick_check_all_path,
                analyzer_prefix="all",
            )
        _log(f"Pressure-only validation saved: {outputs['workbook']}")
        if pressure_outputs:
            _log(f"Pressure-channel validation saved: {pressure_outputs['workbook']}")
        if pressure_outputs_all:
            _log(f"Pressure-channel fleet validation saved: {pressure_outputs_all['workbook']}")
        _log(f"Pressure-channel multi-analyzer summary saved: {multi_analyzer_summary_path}")
        return 0
    except KeyboardInterrupt:
        _log("Pressure-only validation cancelled by user.")
        return 130
    except Exception as exc:
        _log(f"Pressure-only validation failed: {exc}")
        return 1
    finally:
        _restore_pressure_controller_to_atmosphere(devices)
        _close_devices(devices)
        try:
            logger.close()
        except Exception:
            pass


if __name__ == "__main__":
    sys.exit(main())
