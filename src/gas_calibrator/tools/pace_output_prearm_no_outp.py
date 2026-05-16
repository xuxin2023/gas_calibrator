"""Guarded PACE output pre-arm for V1.5 no-OUTP engineering runs."""

from __future__ import annotations

import argparse
import csv
import json
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, Optional

from ..config import load_config
from ..devices import Pace5000, RelayController


FINAL_PASS = "PACE_OUTPUT_PREARM_PASS"
BLOCKED_CONFIG = "BLOCKED_CONFIG_PRECHECK_FAILED"
BLOCKED_ROUTE = "BLOCKED_ROUTE_NOT_CLOSED"
BLOCKED_PACE = "BLOCKED_PACE_UNREACHABLE"
BLOCKED_OUTPUT = "BLOCKED_PACE_OUTPUT_PREARM_FAILED"
BLOCKED_CONFIRMATION = "BLOCKED_OPERATOR_CONFIRMATION_REQUIRED"
BLOCKED_COMMAND_VIOLATION = "BLOCKED_PREARM_COMMAND_VIOLATION"


class PrearmIoLogger:
    def __init__(self, path: Path):
        self.path = path
        self._file = path.open("w", newline="", encoding="utf-8")
        self._writer = csv.DictWriter(
            self._file,
            fieldnames=["timestamp", "port", "device", "direction", "command", "response", "error"],
        )
        self._writer.writeheader()
        self._file.flush()

    @staticmethod
    def _text(value: Any) -> str:
        if value is None:
            return ""
        try:
            return str(value)
        except Exception:
            return repr(value)

    def log_io(
        self,
        *,
        port: str,
        device: str,
        direction: str,
        command: Any = None,
        response: Any = None,
        error: Any = None,
    ) -> None:
        self._writer.writerow(
            {
                "timestamp": datetime.now().isoformat(timespec="milliseconds"),
                "port": self._text(port),
                "device": self._text(device),
                "direction": self._text(direction),
                "command": self._text(command),
                "response": self._text(response),
                "error": self._text(error),
            }
        )
        self._file.flush()

    def close(self) -> None:
        self._file.close()


def _git_value(args: list[str], cwd: Path) -> str:
    try:
        result = subprocess.run(
            ["git", *args],
            cwd=str(cwd),
            check=True,
            capture_output=True,
            text=True,
        )
        return result.stdout.strip()
    except Exception:
        return ""


def _bool(value: Any) -> bool:
    return bool(value)


def _no_write_issues(cfg: Dict[str, Any]) -> list[str]:
    workflow = cfg.get("workflow", {}) if isinstance(cfg.get("workflow"), dict) else {}
    coefficients = cfg.get("coefficients", {}) if isinstance(cfg.get("coefficients"), dict) else {}
    startup = (
        workflow.get("startup_pressure_sensor_calibration", {})
        if isinstance(workflow.get("startup_pressure_sensor_calibration"), dict)
        else {}
    )
    postrun = (
        workflow.get("postrun_corrected_delivery", {})
        if isinstance(workflow.get("postrun_corrected_delivery"), dict)
        else {}
    )

    checks = {
        "workflow.collect_only": workflow.get("collect_only") is True,
        "coefficients.enabled": coefficients.get("enabled") is False,
        "coefficients.sencos": coefficients.get("sencos") == {},
        "startup_pressure_sensor_calibration.enabled": startup.get("enabled") is False,
        "startup_pressure_sensor_calibration.apply_write": startup.get("apply_write") is False,
        "postrun_corrected_delivery.enabled": postrun.get("enabled") is False,
        "postrun_corrected_delivery.write_devices": postrun.get("write_devices") is False,
        "postrun_corrected_delivery.write_pressure_coefficients": postrun.get("write_pressure_coefficients") is False,
    }
    return [key for key, ok in checks.items() if not ok]


def _count_commands(io_path: Path) -> Dict[str, int]:
    counts = {
        "outp1_sent_count": 0,
        "outp0_sent_count": 0,
        "vent0_sent_count": 0,
        "vent1_sent_count": 0,
        "setpoint_sent_count": 0,
        "output_mode_active_sent_count": 0,
        "isolation_open_sent_count": 0,
    }
    if not io_path.exists():
        return counts
    with io_path.open("r", newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            if str(row.get("direction", "")).upper() != "TX":
                continue
            cmd = str(row.get("command", "")).strip().upper()
            if cmd.startswith(":OUTP 1"):
                counts["outp1_sent_count"] += 1
            elif cmd.startswith(":OUTP 0"):
                counts["outp0_sent_count"] += 1
            elif cmd.startswith(":OUTP:MODE ACT"):
                counts["output_mode_active_sent_count"] += 1
            elif cmd.startswith(":OUTP:ISOL:STAT 1"):
                counts["isolation_open_sent_count"] += 1
            elif cmd.startswith(":SOUR:PRES:LEV:IMM:AMPL:VENT 0"):
                counts["vent0_sent_count"] += 1
            elif cmd.startswith(":SOUR:PRES:LEV:IMM:AMPL:VENT 1"):
                counts["vent1_sent_count"] += 1
            elif cmd.startswith(":SOUR:PRES:LEV:IMM:AMPL "):
                counts["setpoint_sent_count"] += 1
    return counts


def _run_output_prearm_sequence(pace: Any, summary: Dict[str, Any]) -> None:
    enable_control_output = getattr(pace, "enable_control_output", None)
    if callable(enable_control_output):
        summary["enable_control_output_used"] = True
        enable_control_output()
        return

    summary["fallback_output_sequence_used"] = True
    set_isolation_open = getattr(pace, "set_isolation_open", None)
    if callable(set_isolation_open):
        set_isolation_open(True)

    wait_for_vent_idle = getattr(pace, "wait_for_vent_idle", None)
    if callable(wait_for_vent_idle):
        wait_for_vent_idle()

    set_output_mode_active = getattr(pace, "set_output_mode_active", None)
    if callable(set_output_mode_active):
        set_output_mode_active()

    pace.set_output(True)


def _read_optional_int(obj: Any, method_name: str) -> Optional[int]:
    method = getattr(obj, method_name, None)
    if not callable(method):
        return None
    try:
        return int(method())
    except Exception:
        return None


def _open_device(device: Any) -> None:
    opener = getattr(device, "open", None) or getattr(device, "connect", None)
    if callable(opener):
        opener()


def _close_device(device: Any) -> None:
    closer = getattr(device, "close", None)
    if callable(closer):
        try:
            closer()
        except Exception:
            pass


def _make_pace(cfg: Dict[str, Any], io_logger: PrearmIoLogger, pace_factory: Callable[..., Any]) -> Any:
    pcfg = cfg["devices"]["pressure_controller"]
    return pace_factory(
        pcfg["port"],
        pcfg.get("baud", 9600),
        timeout=float(pcfg.get("timeout", 1.0)),
        line_ending=pcfg.get("line_ending"),
        query_line_endings=pcfg.get("query_line_endings"),
        pressure_queries=pcfg.get("pressure_queries"),
        io_logger=io_logger,
    )


def _make_relay(
    cfg: Dict[str, Any],
    key: str,
    io_logger: PrearmIoLogger,
    relay_factory: Callable[..., Any],
) -> Optional[Any]:
    rcfg = cfg.get("devices", {}).get(key, {})
    if not isinstance(rcfg, dict) or not rcfg.get("enabled", False):
        return None
    return relay_factory(
        rcfg["port"],
        rcfg.get("baud", 38400),
        addr=rcfg.get("addr", 1),
        io_logger=io_logger,
    )


def _read_relay_states(relay: Any, count: int) -> list[bool]:
    states = relay.read_coils(0, count)
    return [bool(item) for item in list(states)[:count]]


def _baseline_all_off(relay: Any, count: int) -> None:
    setter = getattr(relay, "set_valve", None)
    if not callable(setter):
        raise RuntimeError("relay set_valve unavailable")
    for channel in range(1, count + 1):
        setter(channel, False)


def _confirm_relay_baseline(relay: Optional[Any], count: int) -> tuple[bool, bool, Optional[str]]:
    if relay is None:
        return False, False, "relay disabled"
    try:
        states = _read_relay_states(relay, count)
    except Exception:
        try:
            _baseline_all_off(relay, count)
            states = _read_relay_states(relay, count)
        except Exception as exc:
            return False, True, str(exc)
    if len(states) < count:
        return False, False, "relay state count too short"
    return all(state is False for state in states[:count]), False, None


def _write_summary(path: Path, summary: Dict[str, Any]) -> None:
    path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def run_prearm(
    *,
    config_path: str | Path,
    confirm_route_closed: bool,
    confirm_no_calibration_running: bool,
    output_root: str | Path | None = None,
    pace_factory: Callable[..., Any] = Pace5000,
    relay_factory: Callable[..., Any] = RelayController,
    cwd: str | Path | None = None,
) -> tuple[int, Dict[str, Any]]:
    cwd_path = Path(cwd or Path.cwd())
    cfg_path = Path(config_path)
    cfg = load_config(str(cfg_path))
    base_output = Path(output_root) if output_root is not None else Path("logs") / "pace_output_prearm"
    run_dir = base_output / ("run_" + datetime.now().strftime("%Y%m%d_%H%M%S"))
    run_dir.mkdir(parents=True, exist_ok=True)
    io_path = run_dir / "prearm_io_log.csv"
    summary_path = run_dir / "prearm_summary.json"
    io_logger = PrearmIoLogger(io_path)

    workflow = cfg.get("workflow", {}) if isinstance(cfg.get("workflow"), dict) else {}
    pressure = workflow.get("pressure", {}) if isinstance(workflow.get("pressure"), dict) else {}
    no_outp_transition_mode = pressure.get("no_outp_transition_mode") is True
    no_write_issues = _no_write_issues(cfg)
    no_write_preflight = not no_write_issues

    summary: Dict[str, Any] = {
        "final_decision": "",
        "run_dir": str(run_dir),
        "config_path": str(config_path),
        "branch": _git_value(["branch", "--show-current"], cwd_path),
        "head": _git_value(["rev-parse", "HEAD"], cwd_path),
        "no_outp_transition_mode": no_outp_transition_mode,
        "no_write_preflight": no_write_preflight,
        "no_write_issues": no_write_issues,
        "operator_confirm_route_closed": bool(confirm_route_closed),
        "operator_confirm_no_calibration_running": bool(confirm_no_calibration_running),
        "route_closed_confirmed": False,
        "relay_baseline_confirmed": False,
        "relay8_baseline_confirmed": False,
        "relay_baseline_forced": False,
        "relay8_baseline_forced": False,
        "pace_pressure_before": None,
        "pace_pressure_after": None,
        "pace_output_before": None,
        "pace_output_after": None,
        "pace_isolation_before": None,
        "pace_isolation_after": None,
        "pace_vent_status_before": None,
        "pace_vent_status_after": None,
        "outp1_sent_count": 0,
        "outp0_sent_count": 0,
        "vent0_sent_count": 0,
        "vent1_sent_count": 0,
        "setpoint_sent_count": 0,
        "output_mode_active_sent_count": 0,
        "isolation_open_sent_count": 0,
        "enable_control_output_used": False,
        "fallback_output_sequence_used": False,
        "calibration_path_started": False,
        "allowed_prearm_only": True,
        "real_primary_latest_refresh": False,
        "not_real_acceptance": True,
    }

    pace = None
    relay = None
    relay8 = None
    exit_code = 1

    try:
        if not confirm_route_closed or not confirm_no_calibration_running:
            summary["final_decision"] = BLOCKED_CONFIRMATION
            return exit_code, summary
        if not no_outp_transition_mode or not no_write_preflight:
            summary["final_decision"] = BLOCKED_CONFIG
            return exit_code, summary

        relay = _make_relay(cfg, "relay", io_logger, relay_factory)
        relay8 = _make_relay(cfg, "relay_8", io_logger, relay_factory)
        for device in (relay, relay8):
            if device is not None:
                _open_device(device)

        relay_ok, relay_forced, relay_err = _confirm_relay_baseline(relay, 16)
        relay8_ok, relay8_forced, relay8_err = _confirm_relay_baseline(relay8, 8)
        summary["relay_baseline_confirmed"] = relay_ok
        summary["relay8_baseline_confirmed"] = relay8_ok
        summary["relay_baseline_forced"] = relay_forced
        summary["relay8_baseline_forced"] = relay8_forced
        if relay_err:
            summary["relay_baseline_error"] = relay_err
        if relay8_err:
            summary["relay8_baseline_error"] = relay8_err
        summary["route_closed_confirmed"] = bool(relay_ok and relay8_ok)
        if not summary["route_closed_confirmed"]:
            summary["final_decision"] = BLOCKED_ROUTE
            return exit_code, summary

        pace = _make_pace(cfg, io_logger, pace_factory)
        _open_device(pace)
        try:
            summary["pace_pressure_before"] = float(pace.read_pressure())
            summary["pace_output_before"] = int(pace.get_output_state())
        except Exception as exc:
            summary["pace_error"] = str(exc)
            summary["final_decision"] = BLOCKED_PACE
            return exit_code, summary
        summary["pace_isolation_before"] = _read_optional_int(pace, "get_isolation_state")
        summary["pace_vent_status_before"] = _read_optional_int(pace, "get_vent_status")

        _run_output_prearm_sequence(pace, summary)

        try:
            summary["pace_output_after"] = int(pace.get_output_state())
            summary["pace_pressure_after"] = float(pace.read_pressure())
        except Exception as exc:
            summary["pace_error"] = str(exc)
            summary["final_decision"] = BLOCKED_PACE
            return exit_code, summary
        summary["pace_isolation_after"] = _read_optional_int(pace, "get_isolation_state")
        summary["pace_vent_status_after"] = _read_optional_int(pace, "get_vent_status")

        counts = _count_commands(io_path)
        summary.update(counts)
        if (
            counts["outp1_sent_count"] != 1
            or counts["outp0_sent_count"] != 0
            or counts["vent0_sent_count"] != 0
            or counts["vent1_sent_count"] != 0
            or counts["setpoint_sent_count"] != 0
        ):
            summary["final_decision"] = BLOCKED_COMMAND_VIOLATION
            return exit_code, summary

        if summary["pace_output_after"] != 1:
            summary["final_decision"] = BLOCKED_OUTPUT
            return exit_code, summary

        summary["final_decision"] = FINAL_PASS
        exit_code = 0
        return exit_code, summary
    finally:
        summary.update(_count_commands(io_path))
        _write_summary(summary_path, summary)
        for device in (pace, relay, relay8):
            if device is not None:
                _close_device(device)
        io_logger.close()


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Pre-arm PACE output for V1.5 no-OUTP engineering runs.")
    parser.add_argument("--config", required=True)
    parser.add_argument("--confirm-route-closed", action="store_true")
    parser.add_argument("--confirm-no-calibration-running", action="store_true")
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    code, summary = run_prearm(
        config_path=args.config,
        confirm_route_closed=args.confirm_route_closed,
        confirm_no_calibration_running=args.confirm_no_calibration_running,
    )
    print(f"run_dir={summary.get('run_dir', '')}", flush=True)
    print(f"final_decision={summary.get('final_decision', '')}", flush=True)
    print(f"pace_output_before={summary.get('pace_output_before')}", flush=True)
    print(f"pace_output_after={summary.get('pace_output_after')}", flush=True)
    return code


if __name__ == "__main__":
    sys.exit(main())
