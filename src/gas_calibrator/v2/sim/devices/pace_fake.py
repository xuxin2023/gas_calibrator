from __future__ import annotations

import math
import time
from typing import Any, Optional


def _approach(current: float, target: float, step: float) -> float:
    delta = float(target) - float(current)
    if abs(delta) <= step:
        return float(target)
    return float(current) + math.copysign(step, delta)


class PACE5000Fake:
    """
    Protocol-level SCPI fake for the PACE pressure controller.

    The fake only implements the subset the current code path really consumes,
    and returns SCPI-style errors for selected unsupported headers.
    """

    VENT_STATUS_IDLE = 0
    VENT_STATUS_IN_PROGRESS = 1
    VENT_STATUS_TIMED_OUT = 2
    VENT_STATUS_TRAPPED_PRESSURE = 3
    VENT_STATUS_ABORTED = 4
    VALID_MODES = {
        "stable",
        "venting",
        "slewing",
        "in_limits",
        "no_response",
        "parse_fail",
        "unsupported_header",
        "hardware_missing",
        "cleanup_no_response",
    }

    def __init__(
        self,
        port: str = "SIM-PACE5000",
        *,
        baudrate: int = 9600,
        timeout: float = 1.0,
        io_logger: Optional[Any] = None,
        plant_state: Optional[Any] = None,
        mode: str = "stable",
        unit: str = "HPA",
        unsupported_headers: Optional[list[str]] = None,
        slew_hpa_per_s: float = 800.0,
        ambient_pressure_hpa: float = 1013.25,
        current_pressure_hpa: float = 1013.25,
        target_pressure_hpa: float = 1013.25,
        faults: Optional[list[dict[str, Any]]] = None,
        **_: Any,
    ) -> None:
        self.port = str(port or "SIM-PACE5000")
        self.baudrate = int(baudrate or 9600)
        self.timeout = float(timeout if timeout is not None else 1.0)
        self.io_logger = io_logger
        self.plant_state = plant_state
        if self.plant_state is not None:
            setattr(self.plant_state, "dynamic_protocol", True)
        self.connected = False
        self.mode = str(mode or "stable").strip().lower()
        if self.mode not in self.VALID_MODES:
            self.mode = "stable"
        self.unit = str(unit or "HPA").strip().upper() or "HPA"
        self.unsupported_headers = [str(item or "").strip().upper() for item in list(unsupported_headers or []) if str(item or "").strip()]
        for fault in list(faults or []):
            if not bool(fault.get("active", False)):
                continue
            name = str(fault.get("name") or "").strip().lower()
            if name == "cleanup_no_response":
                self.mode = "cleanup_no_response"
            if name == "unsupported_header":
                detail = str(fault.get("detail") or "").strip()
                if detail:
                    self.unsupported_headers.append(detail.upper())
            if name == "no_response":
                self.mode = "no_response"
        self.slew_hpa_per_s = max(1.0, float(slew_hpa_per_s))
        self.ambient_pressure_hpa = float(ambient_pressure_hpa)
        self.current_pressure_hpa = float(current_pressure_hpa)
        self.target_pressure_hpa = float(target_pressure_hpa)
        self.output_enabled = False
        self.output_mode = "ACT"
        self.isolation_open = True
        self.vent_enabled = True
        self.vent_status = self.VENT_STATUS_IDLE
        self._vent_until_ts = 0.0
        self._in_limits_pct = 0.02
        self._in_limits_time_s = 10.0
        self._error_queue: list[str] = []
        self._last_update_ts = time.monotonic()
        self._setpoint_seen = False
        self._control_cycle_started = False
        self._cleanup_fault_active = False
        self._sync_plant_state()

    def open(self) -> None:
        self.connected = True

    def connect(self) -> bool:
        self.connected = True
        return True

    def close(self) -> None:
        self.connected = False

    def write(self, cmd: str) -> None:
        self.process_command(cmd)

    def query(self, cmd: str, *, line_ending: Optional[str] = None) -> str:
        del line_ending
        return self.process_command(cmd).strip()

    def process_command(self, cmd: str) -> str:
        text = str(cmd or "").strip()
        if not text:
            return ""
        normalized = " ".join(text.replace("\r", "").replace("\n", "").split())
        upper = normalized.upper()
        self._update_state()
        self._check_mode_before_command(upper)
        if self._is_unsupported(upper):
            return self._push_error(-113, "Undefined header")
        if upper == "*IDN?":
            return f"Druck,PACE5000,{self.port},SIM-1.0\r\n"
        if upper == "*CLS":
            self._error_queue.clear()
            return ""
        if upper == "*OPC?":
            return "1\r\n"
        if upper == ":OUTP:STAT?":
            return f"{1 if self.output_enabled else 0}\r\n"
        if upper == ":OUTP:ISOL:STAT?":
            return f"{1 if self.isolation_open else 0}\r\n"
        if upper.startswith(":OUTP:ISOL:STAT "):
            self.isolation_open = bool(int(float(upper.split()[-1])))
            return ""
        if upper.startswith(":OUTP "):
            enabled = bool(int(float(upper.split()[-1])))
            self.output_enabled = enabled
            if enabled:
                self._control_cycle_started = True
            return ""
        if upper.startswith(":OUTP:MODE "):
            self.output_mode = upper.split()[-1]
            return ""
        if upper.startswith(":UNIT:PRES "):
            self.unit = upper.split()[-1]
            return ""
        if upper == ":UNIT:CONV?":
            return f"{self.unit},1.0\r\n"
        if upper.startswith(":SOUR:PRES:LEV:IMM:AMPL:VENT "):
            vent_on = bool(int(float(upper.split()[-1])))
            self._set_vent_state(vent_on)
            return ""
        if upper == ":SOUR:PRES:LEV:IMM:AMPL:VENT?":
            self._update_state()
            self._check_cleanup_fault(":SOUR:PRES:LEV:IMM:AMPL:VENT?")
            return f"{int(self.vent_status)}\r\n"
        if upper.startswith(":SOUR:PRES:LEV:IMM:AMPL "):
            self.target_pressure_hpa = self._to_hpa(float(upper.split()[-1]))
            self.output_enabled = True
            self.vent_enabled = False
            self.vent_status = self.VENT_STATUS_IDLE
            self._setpoint_seen = True
            self._control_cycle_started = True
            self._sync_plant_state()
            return ""
        if upper.startswith(":SOUR:PRES:SLEW:MODE "):
            return ""
        if upper.startswith(":SOUR:PRES:SLEW "):
            try:
                self.slew_hpa_per_s = max(1.0, self._to_hpa(float(upper.split()[-1])))
            except Exception:
                return self._push_error(-222, "Data out of range")
            return ""
        if upper.startswith(":SOUR:PRES:INL:TIME "):
            self._in_limits_time_s = max(0.0, float(upper.split()[-1]))
            return ""
        if upper.startswith(":SOUR:PRES:INL "):
            self._in_limits_pct = max(0.0, float(upper.split()[-1]))
            return ""
        if upper == ":SENS:PRES:INL?":
            if self.mode == "parse_fail":
                return "NOT_A_NUMBER\r\n"
            value, state = self.get_in_limits()
            return f"{value:.3f},{state}\r\n"
        if upper in {":SENS:PRES:CONT?", ":SENS:PRES?", ":MEAS:PRES?"}:
            if self.mode == "parse_fail":
                return "PRES:??\r\n"
            self._update_state()
            return f"{self._from_hpa(self.current_pressure_hpa):.3f}\r\n"
        if upper == ":SYST:ERR?":
            if self._error_queue:
                return self._error_queue.pop(0) + "\r\n"
            return '0,"No error"\r\n'
        return self._push_error(-102, "Syntax error")

    def _push_error(self, code: int, message: str) -> str:
        text = f'{int(code)},"{message}"'
        self._error_queue.append(text)
        return text + "\r\n"

    def _is_unsupported(self, upper: str) -> bool:
        if self.mode == "unsupported_header" and not self.unsupported_headers:
            return upper.startswith(":SENS:PRES:INL")
        return any(upper.startswith(header) for header in self.unsupported_headers)

    def _check_mode_before_command(self, upper: str) -> None:
        if self.mode == "hardware_missing":
            raise RuntimeError("HARDWARE_MISSING")
        if self.mode == "no_response":
            raise RuntimeError("NO_RESPONSE")
        if upper == ":SYST:ERR?":
            return

    def _check_cleanup_fault(self, upper: str) -> None:
        if self.mode != "cleanup_no_response":
            return
        if self._cleanup_fault_active and upper.startswith(":SOUR:PRES:LEV:IMM:AMPL:VENT?"):
            raise RuntimeError("NO_RESPONSE")

    def _set_vent_state(self, enabled: bool) -> None:
        self.vent_enabled = bool(enabled)
        if enabled:
            self.output_enabled = False
            self.vent_status = self.VENT_STATUS_IN_PROGRESS
            self._vent_until_ts = time.monotonic() + 0.15
            if self._setpoint_seen and self._control_cycle_started:
                self._cleanup_fault_active = self.mode == "cleanup_no_response"
        else:
            self.vent_status = self.VENT_STATUS_IN_PROGRESS
            self._vent_until_ts = time.monotonic() + 0.08

    def _update_state(self) -> None:
        now = time.monotonic()
        elapsed_s = max(0.0, now - self._last_update_ts)
        self._last_update_ts = now
        if elapsed_s <= 0:
            self._sync_plant_state()
            return

        if self.vent_status == self.VENT_STATUS_IN_PROGRESS and now >= self._vent_until_ts:
            self.vent_status = self.VENT_STATUS_IDLE

        if self.vent_enabled:
            self.current_pressure_hpa = _approach(
                self.current_pressure_hpa,
                self.ambient_pressure_hpa,
                max(5.0, self.slew_hpa_per_s * elapsed_s),
            )
        else:
            self.current_pressure_hpa = _approach(
                self.current_pressure_hpa,
                self.target_pressure_hpa,
                max(1.0, self.slew_hpa_per_s * elapsed_s),
            )
        self._sync_plant_state()

    def _sync_plant_state(self) -> None:
        if self.plant_state is None:
            return
        try:
            self.plant_state.ambient_pressure_hpa = float(self.ambient_pressure_hpa)
            self.plant_state.pressure_hpa = float(self.current_pressure_hpa)
            self.plant_state.target_pressure_hpa = float(self.target_pressure_hpa)
            self.plant_state.vent_on = bool(self.vent_enabled)
            if hasattr(self.plant_state, "sync"):
                self.plant_state.sync()
        except Exception:
            pass

    def _to_hpa(self, value: float) -> float:
        unit = self.unit.upper()
        if unit == "KPA":
            return float(value) * 10.0
        if unit == "BAR":
            return float(value) * 1000.0
        return float(value)

    def _from_hpa(self, value_hpa: float) -> float:
        unit = self.unit.upper()
        if unit == "KPA":
            return float(value_hpa) / 10.0
        if unit == "BAR":
            return float(value_hpa) / 1000.0
        return float(value_hpa)

    def set_units_hpa(self) -> None:
        self.process_command(":UNIT:PRES HPA")

    def set_output(self, on: bool) -> None:
        self.process_command(f":OUTP {1 if on else 0}")

    def set_output_mode_active(self) -> None:
        self.process_command(":OUTP:MODE ACT")

    def set_output_mode_idle(self) -> None:
        self.process_command(":OUTP:MODE IDLE")

    def get_output_state(self) -> int:
        return int(float(self.query(":OUTP:STAT?")))

    def get_isolation_state(self) -> int:
        return int(float(self.query(":OUTP:ISOL:STAT?")))

    def set_isolation_open(self, is_open: bool) -> None:
        self.process_command(f":OUTP:ISOL:STAT {1 if is_open else 0}")

    def set_setpoint(self, value_hpa: float) -> None:
        self.process_command(f":SOUR:PRES:LEV:IMM:AMPL {self._from_hpa(float(value_hpa))}")

    def set_pressure_hpa(self, value_hpa: float) -> None:
        self.set_setpoint(value_hpa)

    def set_pressure(self, value_hpa: float) -> None:
        self.set_setpoint(value_hpa)

    def read_pressure(self) -> float:
        return float(self.query(":SENS:PRES:CONT?"))

    def vent(self, on: bool = True) -> None:
        self.process_command(f":SOUR:PRES:LEV:IMM:AMPL:VENT {1 if on else 0}")

    def get_vent_status(self) -> int:
        return int(float(self.query(":SOUR:PRES:LEV:IMM:AMPL:VENT?")))

    def wait_for_vent_idle(
        self,
        *,
        timeout_s: float = 30.0,
        poll_s: float = 0.25,
        ok_statuses: Optional[list[int]] = None,
    ) -> int:
        accepted = set(
            ok_statuses
            or [
                self.VENT_STATUS_IDLE,
                self.VENT_STATUS_TIMED_OUT,
                self.VENT_STATUS_TRAPPED_PRESSURE,
                self.VENT_STATUS_ABORTED,
            ]
        )
        deadline = time.monotonic() + max(0.2, float(timeout_s))
        last_status = self.VENT_STATUS_IN_PROGRESS
        while time.monotonic() < deadline:
            last_status = self.get_vent_status()
            if last_status != self.VENT_STATUS_IN_PROGRESS:
                if last_status in accepted:
                    return last_status
                raise RuntimeError(f"VENT_STATUS_{last_status}")
            time.sleep(max(0.01, float(poll_s)))
        raise RuntimeError(f"VENT_TIMEOUT(last_status={last_status})")

    def enter_atmosphere_mode(
        self,
        *,
        timeout_s: float = 30.0,
        poll_s: float = 0.25,
        hold_open: bool = False,
        hold_interval_s: float = 2.0,
    ) -> int:
        del hold_open, hold_interval_s
        self.set_output(False)
        self.set_isolation_open(True)
        self.vent(True)
        return self.wait_for_vent_idle(timeout_s=timeout_s, poll_s=poll_s)

    def exit_atmosphere_mode(
        self,
        *,
        timeout_s: float = 30.0,
        poll_s: float = 0.25,
    ) -> int:
        self.set_output(False)
        self.vent(False)
        self.set_isolation_open(True)
        return self.wait_for_vent_idle(timeout_s=timeout_s, poll_s=poll_s)

    def enable_control_output(self) -> None:
        self.set_output_mode_active()
        self.set_output(True)

    def disable_control_output(self) -> None:
        self.set_output(False)

    def set_in_limits(self, pct_full_scale: float, time_s: float) -> None:
        self.process_command(f":SOUR:PRES:INL {float(pct_full_scale)}")
        self.process_command(f":SOUR:PRES:INL:TIME {float(time_s)}")

    def get_in_limits(self) -> tuple[float, int]:
        response = self.query(":SENS:PRES:INL?")
        parts = [item.strip() for item in str(response or "").split(",")]
        if len(parts) < 2:
            raise RuntimeError(f"PACE_IN_LIMITS_PARSE:{response}")
        return float(parts[0]), int(float(parts[1]))

    def status(self) -> dict[str, Any]:
        self._update_state()
        try:
            pressure_hpa, in_limits = self.get_in_limits()
        except Exception:
            pressure_hpa = float(self.current_pressure_hpa)
            in_limits = 0
        return {
            "ok": self.mode not in {"hardware_missing", "no_response"},
            "connected": self.connected or True,
            "port": self.port,
            "pressure_hpa": pressure_hpa,
            "target_pressure_hpa": float(self.target_pressure_hpa),
            "output_state": 1 if self.output_enabled else 0,
            "isolation_state": 1 if self.isolation_open else 0,
            "vent_status": int(self.vent_status),
            "unit": self.unit,
            "in_limits": int(in_limits),
            "mode": self.mode,
        }

    def selftest(self) -> dict[str, Any]:
        status = self.status()
        return {"ok": bool(status["ok"]), "mode": self.mode, "connected": status["connected"]}
