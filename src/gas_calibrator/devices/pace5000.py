"""PACE5000 pressure controller driver."""

from __future__ import annotations

import re
import threading
import time
from typing import Any, Iterable, List, Optional, Tuple

from .serial_base import SerialDevice


class Pace5000:
    """PACE5000 wrapper."""

    VENT_STATUS_IDLE = 0
    VENT_STATUS_IN_PROGRESS = 1
    VENT_STATUS_COMPLETED = 2
    VENT_STATUS_TIMED_OUT = 5
    VENT_STATUS_TRAPPED_PRESSURE = 3
    VENT_STATUS_ABORTED = 4

    def __init__(
        self,
        port: str,
        baudrate: int = 9600,
        timeout: float = 1.0,
        line_ending: Optional[str] = None,
        query_line_endings: Optional[Iterable[str]] = None,
        pressure_queries: Optional[Iterable[str]] = None,
        io_logger: Optional[Any] = None,
        serial_factory: Optional[Any] = None,
    ):
        self.ser = SerialDevice(
            port,
            baudrate=baudrate,
            timeout=timeout,
            device_name="pace5000",
            io_logger=io_logger,
            serial_factory=serial_factory,
        )
        self._cmd_lock = threading.RLock()
        self._vent_hold_thread: Optional[threading.Thread] = None
        self._device_identity: Optional[str] = None
        self._device_identity_probed = False
        self._instrument_version: Optional[str] = None
        self._instrument_version_probed = False
        self._legacy_vent_status_model: Optional[bool] = None
        self._vent_after_valve_supported: Optional[bool] = None
        self._vent_popup_ack_supported: Optional[bool] = None
        self._vent_elapsed_time_supported: Optional[bool] = None
        self._vent_orpv_supported: Optional[bool] = None
        self._vent_pupv_supported: Optional[bool] = None
        self.line_ending = self._normalize_line_ending(line_ending or "LF")
        self.query_line_endings = self._normalize_line_endings(query_line_endings)
        self.pressure_queries = self._normalize_pressure_queries(pressure_queries)
        self._last_pressure_query_hint: Optional[tuple] = None

    @staticmethod
    def _normalize_line_ending(value: str) -> str:
        raw = str(value or "").strip()
        text = raw.upper()
        if raw in ("\n", "\r", "\r\n"):
            return raw
        if text in {"LF", "\\N", "10"}:
            return "\n"
        if text in {"CR", "\\R", "13"}:
            return "\r"
        if text in {"CRLF", "\\R\\N", "13,10"}:
            return "\r\n"
        return "\n"

    def _normalize_line_endings(self, values: Optional[Iterable[str]]) -> List[str]:
        ordered = [self.line_ending, "\n", "\r\n", "\r"]
        if values is not None:
            for one in values:
                ordered.append(self._normalize_line_ending(str(one)))

        out: List[str] = []
        for one in ordered:
            if one not in out:
                out.append(one)
        return out

    @staticmethod
    def _normalize_pressure_queries(values: Optional[Iterable[str]]) -> List[str]:
        out: List[str] = []
        raw = list(values) if values is not None else []
        preferred = [
            ":SENS:PRES:INL?",
            ":SENS:PRES:CONT?",
            ":SENS:PRES?",
            ":MEAS:PRES?",
        ]
        for one in preferred + raw + preferred:
            text = str(one or "").strip()
            if not text:
                continue
            if text not in out:
                out.append(text)
        return out

    @staticmethod
    def _strip_echo(cmd: str, resp: str) -> str:
        text = (resp or "").strip()
        if not text:
            return ""
        if text.upper().startswith(cmd.upper()):
            return text[len(cmd) :].strip()
        return text

    @staticmethod
    def _parse_first_float(text: str) -> Optional[float]:
        m = re.search(r"[-+]?\d+(?:\.\d+)?(?:[eE][-+]?\d+)?", text or "")
        if not m:
            return None
        try:
            return float(m.group(0))
        except Exception:
            return None

    @staticmethod
    def _parse_last_float(text: str) -> Optional[float]:
        matches = re.findall(r"[-+]?\d+(?:\.\d+)?(?:[eE][-+]?\d+)?", text or "")
        if not matches:
            return None
        try:
            return float(matches[-1])
        except Exception:
            return None

    @classmethod
    def _parse_first_int(cls, text: str) -> Optional[int]:
        value = cls._parse_first_float(text)
        if value is None:
            return None
        try:
            return int(float(value))
        except Exception:
            return None

    @staticmethod
    def _parse_bool_state(text: str) -> Optional[bool]:
        raw = str(text or "").strip().upper()
        if not raw:
            return None
        if re.search(r"\b(ERR(?:OR)?|FAIL(?:ED)?|INVALID|UNDEFINED|SYNTAX|COMMAND|NO_RESPONSE)\b", raw):
            return None
        if raw in {"1", "ON", "OPEN", "OPENED", "ENABLE", "ENABLED"}:
            return True
        if raw in {"0", "OFF", "CLOSE", "CLOSED", "DISABLE", "DISABLED"}:
            return False
        if re.search(r"\bOPEN\b", raw) or re.search(r"\bENAB(?:LE|LED)\b", raw):
            return True
        if re.search(r"\bCLOSE(?:D)?\b", raw) or re.search(r"\bDISAB(?:LE|LED)\b", raw):
            return False
        if re.fullmatch(r"[+-]?(?:0|1)(?:\.0+)?", raw):
            return bool(int(float(raw)))
        return None

    def open(self) -> None:
        with self._cmd_lock:
            self.ser.open()

    def connect(self) -> None:
        self.open()

    def close(self) -> None:
        self.stop_atmosphere_hold()
        with self._cmd_lock:
            self.ser.close()

    def write(self, cmd: str) -> None:
        with self._cmd_lock:
            self.ser.write(cmd + self.line_ending)

    def clear_status(self) -> None:
        self.write("*CLS")

    def get_system_error(self) -> str:
        return str(self.query(":SYST:ERR?") or "").strip()

    def drain_system_errors(self, *, max_reads: int = 8) -> List[str]:
        errors: List[str] = []
        for _ in range(max(1, int(max_reads))):
            try:
                entry = self.get_system_error()
            except Exception:
                break
            text = str(entry or "").strip()
            if not text:
                break
            normalized = text.replace(" ", "").lower()
            if normalized.startswith(":syst:err0") or normalized.startswith("0,") or "noerror" in normalized:
                break
            errors.append(text)
        return errors

    def read(self) -> float:
        return self.read_pressure()

    def query(self, cmd: str, *, line_ending: Optional[str] = None) -> str:
        with self._cmd_lock:
            term = line_ending if line_ending is not None else self.line_ending
            first = self.ser.query(cmd + term)
            first_clean = self._strip_echo(cmd, first)
            if first_clean:
                return first_clean

            # In SCPI legacy mode, first line may be only command echo.
            try:
                second = self.ser.readline()
            except Exception:
                second = ""
            return self._strip_echo(cmd, second)

    def set_units_hpa(self) -> None:
        self.write(":UNIT:PRES HPA")

    def set_output(self, on: bool) -> None:
        self.write(f":OUTP {1 if on else 0}")

    def set_output_enabled(self, enabled: bool) -> None:
        self.set_output(bool(enabled))

    def set_output_mode_active(self) -> None:
        self.write(":OUTP:MODE ACT")

    def set_output_mode_passive(self) -> None:
        self.write(":OUTP:MODE PASS")

    def get_output_mode(self) -> str:
        resp = self.query(":OUTP:MODE?")
        text = str(resp or "").strip().upper()
        if "ACT" in text:
            return "ACT"
        if "PASS" in text:
            return "PASS"
        if "GAUG" in text:
            return "GAUG"
        raise RuntimeError("NO_RESPONSE")

    def get_output_mode_query(self) -> str:
        return self.get_output_mode()

    def set_slew_mode_linear(self) -> None:
        self.write(":SOUR:PRES:SLEW:MODE LIN")

    def set_slew_mode_max(self) -> None:
        self.write(":SOUR:PRES:SLEW:MODE MAX")

    def get_slew_mode(self) -> str:
        resp = self.query(":SOUR:PRES:SLEW:MODE?")
        text = str(resp or "").strip().upper()
        if "LIN" in text:
            return "LIN"
        if "MAX" in text:
            return "MAX"
        raise RuntimeError("NO_RESPONSE")

    def set_slew_rate(self, value_hpa_per_s: float) -> None:
        self.write(f":SOUR:PRES:SLEW {float(value_hpa_per_s)}")

    def get_slew_rate(self) -> float:
        resp = self.query(":SOUR:PRES:SLEW?")
        value = self._parse_first_float(resp)
        if value is None:
            raise RuntimeError("NO_RESPONSE")
        return float(value)

    def set_overshoot_allowed(self, enabled: bool) -> None:
        self.write(f":SOUR:PRES:SLEW:OVER {1 if enabled else 0}")

    def get_overshoot_allowed(self) -> bool:
        resp = self.query(":SOUR:PRES:SLEW:OVER?")
        value = self._parse_bool_state(resp)
        if value is None:
            raise RuntimeError("NO_RESPONSE")
        return bool(value)

    def get_output_state(self) -> int:
        resp = self.query(":OUTP:STAT?")
        value = self._parse_first_int(resp)
        if value is None:
            raise RuntimeError("NO_RESPONSE")
        return value

    def verify_output_enabled(
        self,
        enabled: bool,
        *,
        timeout_s: float = 5.0,
        poll_s: float = 0.1,
    ) -> int:
        return self._wait_for_int_state(
            self.get_output_state,
            1 if enabled else 0,
            timeout_s=timeout_s,
            poll_s=poll_s,
            label="OUTPUT",
        )

    def set_output_enabled_verified(
        self,
        enabled: bool,
        *,
        timeout_s: float = 5.0,
        poll_s: float = 0.1,
    ) -> int:
        self.set_output_enabled(enabled)
        return self.verify_output_enabled(enabled, timeout_s=timeout_s, poll_s=poll_s)

    def get_isolation_state(self) -> int:
        resp = self.query(":OUTP:ISOL:STAT?")
        value = self._parse_first_int(resp)
        if value is None:
            raise RuntimeError("NO_RESPONSE")
        return value

    def set_isolation_open(self, is_open: bool) -> None:
        self.write(f":OUTP:ISOL:STAT {1 if is_open else 0}")

    def set_output_isolated(self, isolated: bool) -> None:
        # SCPI uses 1=open path, 0=closed/isolated.
        self.set_isolation_open(not bool(isolated))

    def verify_output_isolated(
        self,
        isolated: bool,
        *,
        timeout_s: float = 5.0,
        poll_s: float = 0.1,
    ) -> int:
        return self._wait_for_int_state(
            self.get_isolation_state,
            0 if isolated else 1,
            timeout_s=timeout_s,
            poll_s=poll_s,
            label="ISOLATION",
        )

    def set_output_isolated_verified(
        self,
        isolated: bool,
        *,
        timeout_s: float = 5.0,
        poll_s: float = 0.1,
    ) -> int:
        self.set_output_isolated(isolated)
        return self.verify_output_isolated(isolated, timeout_s=timeout_s, poll_s=poll_s)

    def set_setpoint(self, value_hpa: float) -> None:
        # Per SCPI manual, control setpoint is written via LEV:IMM:AMPL.
        self.write(f":SOUR:PRES:LEV:IMM:AMPL {value_hpa}")

    def read_pressure(self) -> float:
        last_exc: Optional[Exception] = None
        # Try cached hint first for fast path
        hint = self._last_pressure_query_hint
        if hint is not None:
            try:
                resp = self.query(hint[0], line_ending=hint[1])
                value = self._parse_first_float(resp)
                if value is not None:
                    return value
            except Exception:
                pass
        for idx in range(2):
            try:
                for cmd in self.pressure_queries:
                    for term in self.query_line_endings:
                        resp = self.query(cmd, line_ending=term)
                        value = self._parse_first_float(resp)
                        if value is not None:
                            self._last_pressure_query_hint = (cmd, term)
                            return value
                last_exc = RuntimeError("NO_RESPONSE_OR_PARSE")
            except Exception as exc:
                last_exc = exc
            if idx < 1:
                time.sleep(0.1)
        if last_exc:
            raise last_exc
        raise RuntimeError("NO_RESPONSE")

    def vent(self, on: bool = True) -> None:
        # Per SCPI manual, vent command requires explicit 1(start)/0(abort-close).
        self.write(f":SOUR:PRES:LEV:IMM:AMPL:VENT {1 if on else 0}")

    def get_vent_status(self) -> int:
        resp = self.query(":SOUR:PRES:LEV:IMM:AMPL:VENT?")
        value = self._parse_first_int(resp)
        if value is None:
            raise RuntimeError("NO_RESPONSE")
        return value

    def get_vent_status_query(self) -> int:
        return self.get_vent_status()

    @staticmethod
    def vent_status_is_completed_latched(status: Any) -> bool:
        value = Pace5000._parse_first_int(str(status))
        return value == Pace5000.VENT_STATUS_COMPLETED

    @staticmethod
    def _looks_like_legacy_vent_status_identity(identity: str) -> bool:
        text = str(identity or "").strip().upper()
        if not text:
            return False
        return "GE DRUCK" in text or "PACE5000 USER INTERFACE" in text

    def _probe_device_identity(self) -> Optional[str]:
        if self._device_identity_probed:
            return self._device_identity
        self._device_identity_probed = True
        try:
            identity = str(self.query("*IDN?") or "").strip()
        except Exception:
            identity = ""
        if identity:
            self._device_identity = identity
            self._legacy_vent_status_model = self._looks_like_legacy_vent_status_identity(identity)
        return self._device_identity

    def get_device_identity(self) -> str:
        return str(self._probe_device_identity() or "")

    def _probe_instrument_version(self) -> Optional[str]:
        if self._instrument_version_probed:
            return self._instrument_version
        self._instrument_version_probed = True
        try:
            version = str(self.query(":INST:VERS?") or "").strip()
        except Exception:
            version = ""
        if version:
            self._instrument_version = version
        return self._instrument_version

    def get_instrument_version(self) -> str:
        return str(self._probe_instrument_version() or "")

    def has_legacy_vent_status_model(self) -> bool:
        if self._legacy_vent_status_model is None:
            self._probe_device_identity()
        return bool(self._legacy_vent_status_model)

    def has_legacy_vent_state_3_compatibility(self) -> bool:
        if not self.has_legacy_vent_status_model():
            return False
        return self.get_instrument_version() == "02.00.07"

    def vent_status_allows_control(self, status: Any) -> bool:
        value = self._parse_first_int(str(status))
        if value is None:
            return False
        if self.has_legacy_vent_state_3_compatibility():
            # 02.00.07 can expose VENT?=3 as a legacy pending-acknowledgement
            # state. Keep it observable via vent_terminal_statuses(), but do not
            # treat it as control-ready.
            return value == self.VENT_STATUS_IDLE
        return value == self.VENT_STATUS_IDLE

    def vent_terminal_statuses(self) -> List[int]:
        if self.has_legacy_vent_state_3_compatibility():
            return [
                self.VENT_STATUS_IDLE,
                self.VENT_STATUS_TRAPPED_PRESSURE,
            ]
        return [
            self.VENT_STATUS_IDLE,
            self.VENT_STATUS_ABORTED,
        ]

    def _ensure_vent_aux_supported(self, cache_attr: str, unsupported_error: str) -> None:
        supported = getattr(self, cache_attr, None)
        if supported is False:
            raise RuntimeError(unsupported_error)

    def supports_vent_after_valve_open(self) -> bool:
        if self._vent_after_valve_supported is False:
            return False
        if self.has_legacy_vent_status_model():
            self._vent_after_valve_supported = False
            return False
        return True

    def supports_vent_popup_ack(self) -> bool:
        if self._vent_popup_ack_supported is False:
            return False
        if self.has_legacy_vent_status_model():
            self._vent_popup_ack_supported = False
            return False
        return True

    def supports_vent_elapsed_time(self) -> bool:
        return self._vent_elapsed_time_supported is not False

    def supports_vent_over_range_protect(self) -> bool:
        return self._vent_orpv_supported is not False

    def supports_vent_power_up_protect(self) -> bool:
        return self._vent_pupv_supported is not False

    def set_vent_after_valve_open(self, open_after_vent: bool) -> None:
        self._ensure_vent_aux_supported("_vent_after_valve_supported", "VENT_AFTER_VALVE_UNSUPPORTED")
        state = "OPEN" if open_after_vent else "CLOSED"
        try:
            self.write(f":SOUR:PRES:LEV:IMM:AMPL:VENT:AFT:VVAL:STAT {state}")
        except Exception:
            self._vent_after_valve_supported = False
            raise
        self._vent_after_valve_supported = True

    def get_vent_after_valve_open(self) -> bool:
        self._ensure_vent_aux_supported("_vent_after_valve_supported", "VENT_AFTER_VALVE_UNSUPPORTED")
        try:
            resp = self.query(":SOUR:PRES:LEV:IMM:AMPL:VENT:AFT:VVAL:STAT?")
            value = self._parse_bool_state(resp)
        except Exception:
            self._vent_after_valve_supported = False
            raise
        if value is None:
            self._vent_after_valve_supported = False
            raise RuntimeError("NO_RESPONSE")
        self._vent_after_valve_supported = True
        return value

    def get_vent_after_valve_state(self) -> str:
        value = self.get_vent_after_valve_open()
        return "OPEN" if bool(value) else "CLOSED"

    def set_vent_popup_ack_enabled(self, enabled: bool) -> None:
        self._ensure_vent_aux_supported("_vent_popup_ack_supported", "VENT_POPUP_ACK_UNSUPPORTED")
        state = "ENABled" if enabled else "DISabled"
        try:
            self.write(f":SOUR:PRES:LEV:IMM:AMPL:VENT:APOP:STAT {state}")
        except Exception:
            self._vent_popup_ack_supported = False
            raise
        self._vent_popup_ack_supported = True

    def get_vent_popup_ack_enabled(self) -> bool:
        self._ensure_vent_aux_supported("_vent_popup_ack_supported", "VENT_POPUP_ACK_UNSUPPORTED")
        try:
            resp = self.query(":SOUR:PRES:LEV:IMM:AMPL:VENT:APOP:STAT?")
            value = self._parse_bool_state(resp)
        except Exception:
            self._vent_popup_ack_supported = False
            raise
        if value is None:
            self._vent_popup_ack_supported = False
            raise RuntimeError("NO_RESPONSE")
        self._vent_popup_ack_supported = True
        return value

    def get_vent_popup_state(self) -> str:
        value = self.get_vent_popup_ack_enabled()
        return "ENABLED" if bool(value) else "DISABLED"

    def get_vent_elapsed_time_s(self) -> float:
        self._ensure_vent_aux_supported("_vent_elapsed_time_supported", "VENT_ELAPSED_TIME_UNSUPPORTED")
        try:
            resp = self.query(":SOUR:PRES:LEV:IMM:AMPL:VENT:ETIM?")
            value = self._parse_first_float(resp)
        except Exception:
            self._vent_elapsed_time_supported = False
            raise
        if value is None:
            self._vent_elapsed_time_supported = False
            raise RuntimeError("NO_RESPONSE")
        self._vent_elapsed_time_supported = True
        return float(value)

    def get_vent_over_range_protect_state(self) -> str:
        self._ensure_vent_aux_supported("_vent_orpv_supported", "VENT_ORPV_UNSUPPORTED")
        try:
            resp = self.query(":SOUR:PRES:LEV:IMM:AMPL:VENT:ORPV:STAT?")
            value = self._parse_bool_state(resp)
        except Exception:
            self._vent_orpv_supported = False
            raise
        if value is None:
            self._vent_orpv_supported = False
            raise RuntimeError("NO_RESPONSE")
        self._vent_orpv_supported = True
        return "ENABLED" if bool(value) else "DISABLED"

    def get_vent_power_up_protect_state(self) -> str:
        self._ensure_vent_aux_supported("_vent_pupv_supported", "VENT_PUPV_UNSUPPORTED")
        try:
            resp = self.query(":SOUR:PRES:LEV:IMM:AMPL:VENT:PUPV:STAT?")
            value = self._parse_bool_state(resp)
        except Exception:
            self._vent_pupv_supported = False
            raise
        if value is None:
            self._vent_pupv_supported = False
            raise RuntimeError("NO_RESPONSE")
        self._vent_pupv_supported = True
        return "ENABLED" if bool(value) else "DISABLED"

    def get_effort(self) -> float:
        resp = self.query(":SOUR:PRES:EFF?")
        value = self._parse_first_float(resp)
        if value is None:
            raise RuntimeError("NO_RESPONSE")
        return float(value)

    def get_effort_query(self) -> float:
        return self.get_effort()

    def get_compensation_pressure(self, source_index: int) -> float:
        if int(source_index) not in (1, 2):
            raise ValueError("source_index must be 1 (+supply) or 2 (-vacuum)")
        resp = self.query(f":SOUR:PRES:COMP{int(source_index)}?")
        value = self._parse_last_float(resp)
        if value is None:
            raise RuntimeError("NO_RESPONSE")
        return float(value)

    def get_comp1(self) -> float:
        return self.get_compensation_pressure(1)

    def get_comp2(self) -> float:
        return self.get_compensation_pressure(2)

    def get_control_pressure(self) -> float:
        resp = self.query(":SENS:PRES:CONT?")
        value = self._parse_first_float(resp)
        if value is None:
            raise RuntimeError("NO_RESPONSE")
        return float(value)

    def get_barometric_pressure(self) -> float:
        resp = self.query(":SENS:PRES:BAR?")
        value = self._parse_first_float(resp)
        if value is None:
            raise RuntimeError("NO_RESPONSE")
        return float(value)

    def get_in_limits_setting(self) -> float:
        resp = self.query(":SOUR:PRES:INL?")
        value = self._parse_first_float(resp)
        if value is None:
            raise RuntimeError("NO_RESPONSE")
        return float(value)

    def get_in_limits_time_s(self) -> float:
        resp = self.query(":SENS:PRES:INL:TIME?")
        value = self._parse_first_float(resp)
        if value is None:
            raise RuntimeError("NO_RESPONSE")
        return float(value)

    def get_measured_slew_rate(self) -> float:
        resp = self.query(":SENS:PRES:SLEW?")
        value = self._parse_first_float(resp)
        if value is None:
            raise RuntimeError("NO_RESPONSE")
        return float(value)

    def get_oper_condition(self) -> int:
        resp = self.query(":STAT:OPER:COND?")
        value = self._parse_first_int(resp)
        if value is None:
            raise RuntimeError("NO_RESPONSE")
        return value

    def get_oper_pressure_condition(self) -> int:
        resp = self.query(":STAT:OPER:PRES:COND?")
        value = self._parse_first_int(resp)
        if value is None:
            raise RuntimeError("NO_RESPONSE")
        return value

    def get_oper_pressure_event(self) -> int:
        resp = self.query(":STAT:OPER:PRES:EVEN?")
        value = self._parse_first_int(resp)
        if value is None:
            raise RuntimeError("NO_RESPONSE")
        return value

    @staticmethod
    def oper_pressure_bit_is_set(register_value: Any, bit_index: int) -> Optional[bool]:
        value = Pace5000._parse_first_int(str(register_value))
        if value is None:
            return None
        return bool(int(value) & (1 << int(bit_index)))

    def clear_completed_vent_latch_if_present(
        self,
        *,
        timeout_s: float = 5.0,
        poll_s: float = 0.25,
    ) -> dict[str, Any]:
        before_status = self.get_vent_status()
        result = {
            "before_status": before_status,
            "clear_attempted": False,
            "after_status": before_status,
            "cleared": before_status == self.VENT_STATUS_IDLE,
            "command": "",
            "front_panel_ack_required": False,
        }
        if not self.vent_status_is_completed_latched(before_status):
            return result

        self.vent(False)
        result["clear_attempted"] = True
        result["command"] = ":SOUR:PRES:LEV:IMM:AMPL:VENT 0"
        deadline = time.time() + max(0.5, float(timeout_s))
        last_status = before_status
        while time.time() < deadline:
            last_status = self.get_vent_status()
            result["after_status"] = last_status
            if last_status == self.VENT_STATUS_IDLE:
                result["cleared"] = True
                return result
            if (
                self.has_legacy_vent_state_3_compatibility()
                and last_status == self.VENT_STATUS_TRAPPED_PRESSURE
            ):
                result["front_panel_ack_required"] = True
                return result
            time.sleep(max(0.05, float(poll_s)))
        raise RuntimeError(f"VENT_COMPLETED_LATCH_CLEAR_TIMEOUT(last_status={last_status})")

    def diagnostic_status(self) -> dict[str, Any]:
        status = self.status()
        try:
            status["device_identity"] = self.get_device_identity()
        except Exception:
            status["device_identity"] = ""
        try:
            status["instrument_version"] = self.get_instrument_version()
        except Exception:
            status["instrument_version"] = ""
        status["legacy_vent_state_3_compatibility"] = self.has_legacy_vent_state_3_compatibility()
        try:
            status["output_mode"] = self.get_output_mode()
        except Exception:
            status["output_mode"] = ""
        try:
            status["vent_after_valve_state"] = self.get_vent_after_valve_state()
        except Exception:
            status["vent_after_valve_state"] = ""
        try:
            status["vent_popup_state"] = self.get_vent_popup_state()
        except Exception:
            status["vent_popup_state"] = ""
        try:
            status["oper_condition"] = self.get_oper_condition()
        except Exception:
            status["oper_condition"] = ""
        try:
            status["oper_pressure_condition"] = self.get_oper_pressure_condition()
        except Exception:
            status["oper_pressure_condition"] = ""
        try:
            status["oper_pressure_event"] = self.get_oper_pressure_event()
        except Exception:
            status["oper_pressure_event"] = ""
        vent_complete_cond = self.oper_pressure_bit_is_set(status.get("oper_pressure_condition"), 0)
        vent_complete_event = self.oper_pressure_bit_is_set(status.get("oper_pressure_event"), 0)
        in_limits_cond = self.oper_pressure_bit_is_set(status.get("oper_pressure_condition"), 2)
        in_limits_event = self.oper_pressure_bit_is_set(status.get("oper_pressure_event"), 2)
        status["oper_pressure_vent_complete_bit"] = (
            True
            if vent_complete_cond is True or vent_complete_event is True
            else False
            if vent_complete_cond is not None or vent_complete_event is not None
            else ""
        )
        status["oper_pressure_in_limits_bit"] = (
            True
            if in_limits_cond is True or in_limits_event is True
            else False
            if in_limits_cond is not None or in_limits_event is not None
            else ""
        )
        try:
            status["effort"] = self.get_effort()
        except Exception:
            status["effort"] = ""
        try:
            status["comp1"] = self.get_comp1()
        except Exception:
            status["comp1"] = ""
        try:
            status["comp2"] = self.get_comp2()
        except Exception:
            status["comp2"] = ""
        try:
            status["control_pressure_hpa"] = self.get_control_pressure()
        except Exception:
            status["control_pressure_hpa"] = ""
        try:
            status["barometric_pressure_hpa"] = self.get_barometric_pressure()
        except Exception:
            status["barometric_pressure_hpa"] = ""
        try:
            inl_pressure, inl_state = self.get_in_limits()
            status["in_limits_pressure_hpa"] = inl_pressure
            status["in_limits_state"] = inl_state
        except Exception:
            status["in_limits_pressure_hpa"] = ""
            status["in_limits_state"] = ""
        try:
            status["in_limits_time_s"] = self.get_in_limits_time_s()
        except Exception:
            status["in_limits_time_s"] = ""
        try:
            status["measured_slew_hpa_s"] = self.get_measured_slew_rate()
        except Exception:
            status["measured_slew_hpa_s"] = ""
        status["vent_completed_latched"] = self.vent_status_is_completed_latched(status.get("vent_status"))
        try:
            status["vent_elapsed_time_s"] = self.get_vent_elapsed_time_s()
        except Exception:
            status["vent_elapsed_time_s"] = ""
        try:
            status["vent_orpv_state"] = self.get_vent_over_range_protect_state()
        except Exception:
            status["vent_orpv_state"] = ""
        try:
            status["vent_pupv_state"] = self.get_vent_power_up_protect_state()
        except Exception:
            status["vent_pupv_state"] = ""
        return status

    def wait_for_vent_idle(
        self,
        *,
        timeout_s: float = 30.0,
        poll_s: float = 0.25,
        ok_statuses: Optional[Iterable[int]] = None,
    ) -> int:
        # Default accepted terminal statuses are for vent command completion
        # observability only. Callers that need control-ready must pass
        # ok_statuses=[VENT_STATUS_IDLE].
        accepted = set(ok_statuses or self.vent_terminal_statuses())
        deadline = time.time() + max(0.5, timeout_s)
        last_status: Optional[int] = None
        clear_sent = False
        clear_retries = 0
        max_clear_retries = 5
        while time.time() < deadline:
            status = self.get_vent_status()
            last_status = status
            if status == self.VENT_STATUS_IN_PROGRESS:
                time.sleep(max(0.05, poll_s))
                continue
            if status == self.VENT_STATUS_COMPLETED:
                if not clear_sent or clear_retries < max_clear_retries:
                    self.vent(False)
                    clear_sent = True
                    clear_retries += 1
                time.sleep(max(0.05, poll_s))
                continue
            if status in accepted:
                return status
            raise RuntimeError(f"VENT_STATUS_{status}")
        raise RuntimeError(f"VENT_TIMEOUT(last_status={last_status},clear_sent={clear_sent},clear_retries={clear_retries})")

    def _wait_for_int_state(
        self,
        read_fn,
        expected: int,
        *,
        timeout_s: float = 5.0,
        poll_s: float = 0.1,
        label: str,
    ) -> int:
        deadline = time.time() + max(0.5, timeout_s)
        last_value: Optional[int] = None
        while time.time() < deadline:
            value = int(read_fn())
            last_value = value
            if value == expected:
                return value
            time.sleep(max(0.05, poll_s))
        raise RuntimeError(f"{label}_STATE_{last_value}")

    def _wait_for_text_state(
        self,
        read_fn,
        expected: str,
        *,
        timeout_s: float = 5.0,
        poll_s: float = 0.1,
        label: str,
    ) -> str:
        expected_norm = str(expected or "").strip().upper()
        deadline = time.time() + max(0.5, timeout_s)
        last_value: Optional[str] = None
        while time.time() < deadline:
            value = str(read_fn() or "").strip().upper()
            last_value = value
            if value == expected_norm:
                return value
            time.sleep(max(0.05, poll_s))
        raise RuntimeError(f"{label}_STATE_{last_value}")

    def start_atmosphere_hold(self, *, interval_s: float = 2.0) -> None:
        self._vent_hold_thread = None

    def is_atmosphere_hold_active(self) -> bool:
        return False

    def stop_atmosphere_hold(self, *, timeout_s: float = 2.0) -> bool:
        self._vent_hold_thread = None
        return True

    def enter_atmosphere_mode_with_open_vent_valve(
        self,
        *,
        timeout_s: float = 30.0,
        poll_s: float = 0.25,
        popup_ack_enabled: Optional[bool] = None,
    ) -> int:
        self.stop_atmosphere_hold()
        self.set_output(False)
        self.set_isolation_open(True)
        self.set_vent_after_valve_open(True)
        if not self.get_vent_after_valve_open():
            raise RuntimeError("VENT_AFTER_VALVE_OPEN_NOT_CONFIRMED")
        if popup_ack_enabled is not None:
            self.set_vent_popup_ack_enabled(bool(popup_ack_enabled))
        self.vent(True)
        status = self.wait_for_vent_idle(timeout_s=timeout_s, poll_s=poll_s)
        self._wait_for_int_state(
            self.get_output_state,
            0,
            timeout_s=min(timeout_s, 5.0),
            poll_s=poll_s,
            label="OUTPUT",
        )
        self._wait_for_int_state(
            self.get_isolation_state,
            1,
            timeout_s=min(timeout_s, 5.0),
            poll_s=poll_s,
            label="ISOLATION",
        )
        return status

    def begin_atmosphere_handoff(self) -> int:
        if not self.stop_atmosphere_hold():
            raise RuntimeError("ATMOSPHERE_HOLD_STOP_FAILED")
        self.set_output(False)
        self.set_isolation_open(True)
        self.vent(True)
        return self.get_vent_status()

    def enter_atmosphere_mode(
        self,
        *,
        timeout_s: float = 30.0,
        poll_s: float = 0.25,
        hold_open: bool = False,
        hold_interval_s: float = 2.0,
    ) -> int:
        self.stop_atmosphere_hold()
        self.set_output(False)
        self.set_isolation_open(True)
        self.vent(True)
        status = self.wait_for_vent_idle(timeout_s=timeout_s, poll_s=poll_s)
        self._wait_for_int_state(
            self.get_output_state,
            0,
            timeout_s=min(timeout_s, 5.0),
            poll_s=poll_s,
            label="OUTPUT",
        )
        self._wait_for_int_state(
            self.get_isolation_state,
            1,
            timeout_s=min(timeout_s, 5.0),
            poll_s=poll_s,
            label="ISOLATION",
        )
        return status

    def exit_atmosphere_mode(
        self,
        *,
        timeout_s: float = 30.0,
        poll_s: float = 0.25,
    ) -> int:
        self.stop_atmosphere_hold()
        self.set_output(False)
        self.vent(False)
        # Keep the output path open so controlled pressure reaches the external line.
        self.set_isolation_open(True)
        status = self.wait_for_vent_idle(timeout_s=timeout_s, poll_s=poll_s)
        self._wait_for_int_state(
            self.get_output_state,
            0,
            timeout_s=min(timeout_s, 5.0),
            poll_s=poll_s,
            label="OUTPUT",
        )
        self._wait_for_int_state(
            self.get_isolation_state,
            1,
            timeout_s=min(timeout_s, 5.0),
            poll_s=poll_s,
            label="ISOLATION",
        )
        return status

    def enable_control_output(
        self,
        *,
        timeout_s: float = 2.0,
        poll_s: float = 0.1,
    ) -> None:
        self.set_isolation_open(True)
        self.wait_for_vent_idle(
            timeout_s=max(0.5, timeout_s),
            poll_s=poll_s,
            ok_statuses=[self.VENT_STATUS_IDLE],
        )
        self.set_output_mode_active()
        try:
            self._wait_for_text_state(
                self.get_output_mode,
                "ACT",
                timeout_s=min(timeout_s, 2.0),
                poll_s=poll_s,
                label="OUTPUT_MODE",
            )
        except Exception:
            # Best effort: proceed to output-on and let the caller verify the final state.
            pass
        self.set_output(True)

    def set_in_limits(self, pct_full_scale: float, time_s: float) -> None:
        self.write(f":SOUR:PRES:INL {pct_full_scale}")
        self.write(f":SOUR:PRES:INL:TIME {time_s}")

    def get_in_limits(self) -> Tuple[float, int]:
        resp = self.query(":SENS:PRES:INL?")
        nums = re.findall(r"[-+]?\d+(?:\.\d+)?(?:[eE][-+]?\d+)?", resp or "")
        if not nums:
            raise RuntimeError("NO_RESPONSE")
        if len(nums) >= 2:
            return float(nums[0]), int(float(nums[1]))
        return float(nums[0]), 0

    def status(self) -> dict[str, Any]:
        status = {
            "pressure_hpa": self.read_pressure(),
            "output_state": self.get_output_state(),
            "isolation_state": self.get_isolation_state(),
            "vent_status": self.get_vent_status(),
        }
        try:
            status["output_mode"] = self.get_output_mode()
        except Exception:
            status["output_mode"] = ""
        return status

    def selftest(self) -> dict[str, Any]:
        return self.status()
