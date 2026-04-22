"""PACE5000 pressure controller driver."""

from __future__ import annotations

import os
import re
import threading
import time
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from .serial_base import SerialDevice


class Pace5000:
    """PACE5000 wrapper."""

    LEGACY_AUTO_ABORT_VENT_OVERRIDE_ENV = "GAS_CALIBRATOR_PACE5000_LEGACY_ALLOW_AUTO_VENT_ABORT"
    PROFILE_OLD_PACE5000 = "OLD_PACE5000"
    PROFILE_PACE5000E = "PACE5000E"
    PROFILE_UNKNOWN = "UNKNOWN"

    VENT_STATUS_IDLE = 0
    VENT_STATUS_IN_PROGRESS = 1
    VENT_STATUS_COMPLETED = 2
    VENT_STATUS_TIMED_OUT = 2
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
        self._device_model: Optional[str] = None
        self._device_model_probed = False
        self._device_profile: Optional[str] = None
        self._device_profile_probed = False
        self._supports_sens_pres_cont: Optional[bool] = None
        self._last_in_limits_pressure_hpa: Optional[float] = None
        self._last_in_limits_flag: Optional[int] = None
        self._last_in_limits_monotonic: Optional[float] = None
        self._last_in_limits_generation: Optional[int] = None
        self._last_in_limits_setpoint_hpa: Optional[float] = None
        self._last_in_limits_invalidation_reason: str = ""
        self._last_in_limits_invalidation_count = 0
        self._pressure_control_generation = 0
        self._last_commanded_setpoint_hpa: Optional[float] = None
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
            ":SENS:PRES?",
            ":MEAS:PRES?",
            ":SENS:PRES:CONT?",
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
    def _strip_matching_quotes(text: Any) -> str:
        raw = str(text or "").strip()
        if len(raw) >= 2 and raw[0] == raw[-1] and raw[0] in {'"', "'"}:
            return raw[1:-1]
        return raw

    @staticmethod
    def _looks_like_scpi_header_token(token: Any) -> bool:
        raw = str(token or "").strip()
        if not raw or re.match(r"^[+-]?\d", raw):
            return False
        if raw[0] in {":", "*"}:
            return bool(re.fullmatch(r"[:*][A-Za-z0-9:_?*]+", raw))
        return bool(re.fullmatch(r"[A-Za-z][A-Za-z0-9]*(?::[A-Za-z0-9?]+)+", raw))

    @classmethod
    def _response_payload(cls, text: Any) -> str:
        raw = str(text or "").strip()
        if not raw:
            return ""
        if re.match(r"^[+-]?\d", raw):
            return raw
        parts = raw.split(None, 1)
        if len(parts) > 1 and cls._looks_like_scpi_header_token(parts[0]):
            return parts[1].strip()
        return raw

    @classmethod
    def _parse_system_error(cls, text: Any) -> Tuple[Optional[int], str]:
        raw = str(text or "").strip()
        if not raw:
            return None, ""
        payload = cls._response_payload(raw)
        match = re.match(r"\s*([-+]?\d+)\s*(?:,\s*(.*))?\s*$", payload)
        if match:
            code = cls._parse_first_int(match.group(1))
            message = cls._strip_matching_quotes(match.group(2) or "")
            return code, message
        prefix = re.match(r"\s*([-+]?\d+)\s*(.*)\s*$", payload)
        if not prefix:
            return None, cls._strip_matching_quotes(payload)
        code = cls._parse_first_int(prefix.group(1))
        if code is None:
            return None, cls._strip_matching_quotes(payload)
        message = str(prefix.group(2) or "").strip()
        if message.startswith(","):
            message = message[1:].strip()
        return code, cls._strip_matching_quotes(message)

    @classmethod
    def _is_zero_system_error(cls, text: Any) -> bool:
        code, _message = cls._parse_system_error(text)
        return code == 0

    @staticmethod
    def _normalize_profile(value: Any) -> str:
        text = str(value or "").strip().upper()
        if text == Pace5000.PROFILE_OLD_PACE5000:
            return Pace5000.PROFILE_OLD_PACE5000
        if text == Pace5000.PROFILE_PACE5000E:
            return Pace5000.PROFILE_PACE5000E
        return Pace5000.PROFILE_UNKNOWN

    @staticmethod
    def _normalize_exact_range_token(value: Any) -> str:
        text = str(value or "").strip().strip("\"'")
        text = re.sub(r"\s+", "", text)
        return text.upper()

    @classmethod
    def _parse_range_upper_hpa(cls, text: Any) -> Optional[float]:
        raw = cls._normalize_exact_range_token(cls._response_payload(text))
        if not raw or raw == "BAROMETER":
            return None
        match = re.fullmatch(r"([-+]?\d+(?:\.\d+)?)(MBAR|BAR|HPA|KPA)([AG])?$", raw)
        if not match:
            return None
        value = float(match.group(1))
        unit = match.group(2)
        scale = {
            "BAR": 1000.0,
            "MBAR": 1.0,
            "HPA": 1.0,
            "KPA": 10.0,
        }.get(unit)
        if scale is None:
            return None
        return float(value) * scale

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

    def send_command(self, cmd: str) -> None:
        self.write(str(cmd or "").strip())

    def sendCommand(self, cmd: str) -> None:  # noqa: N802 - compatibility for existing field naming
        self.send_command(cmd)

    def clear_status(self) -> None:
        self.send_command("*CLS")

    def get_system_error(self) -> str:
        return str(self.query(":SYST:ERR?") or "").strip()

    def send_and_check_error(
        self,
        cmd: str,
        *,
        tolerated_error_codes: Optional[Iterable[int]] = None,
    ) -> str:
        command_text = str(cmd or "").strip()
        self.send_command(command_text)
        err = self.get_system_error()
        code, message = self._parse_system_error(err)
        allowed_codes = {0}
        if tolerated_error_codes is not None:
            for item in tolerated_error_codes:
                parsed = self._parse_first_int(str(item))
                if parsed is not None:
                    allowed_codes.add(parsed)
        if code in allowed_codes:
            return err
        if code is None:
            raise RuntimeError(f"PACE_COMMAND_ERROR(command={command_text}, error={err})")
        raise RuntimeError(
            "PACE_COMMAND_ERROR("
            f"command={command_text},code={code},message={message or 'unknown'})"
        )

    def _consume_optional_query_error(
        self,
        cmd: str,
        *,
        tolerated_error_codes: Optional[Iterable[int]] = None,
    ) -> str:
        allowed = {0, None}
        for default_code in (-102, -113):
            allowed.add(default_code)
        if tolerated_error_codes is not None:
            for item in tolerated_error_codes:
                parsed = self._parse_first_int(str(item))
                if parsed is not None:
                    allowed.add(parsed)
        last_text = ""
        for _ in range(8):
            try:
                err = self.get_system_error()
            except Exception:
                return last_text
            text = str(err or "").strip()
            if not text:
                return last_text
            last_text = text
            code, message = self._parse_system_error(text)
            if code in (None, 0):
                return text
            if code in allowed:
                continue
            raise RuntimeError(
                "PACE_QUERY_ERROR("
                f"command={str(cmd or '').strip()},code={code},message={message or 'unknown'})"
            )
        return last_text

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

    def query_command(self, cmd: str) -> str:
        return self.query(cmd)

    def parse_response(self, text: Any) -> str:
        return self._response_payload(text)

    def get_pressure_unit(self) -> str:
        return self._response_payload(self.query(":UNIT:PRES?")).strip().strip("\"'").upper()

    def set_unit(self, unit: str) -> str:
        target = str(unit or "HPA").strip().upper()
        if not target:
            raise ValueError("unit must not be empty")
        try:
            current = self.get_pressure_unit()
        except Exception:
            current = ""
        if current == target:
            return current
        self.send_and_check_error(f":UNIT:PRES {target}")
        self._invalidate_in_limits_cache(f"set_unit:{target}")
        readback = self.get_pressure_unit()
        if readback != target:
            raise RuntimeError(f"UNIT_READBACK_MISMATCH(expected={target},actual={readback})")
        return readback

    def set_units_hpa(self) -> None:
        self.set_unit("HPA")

    def set_output(self, on: bool) -> None:
        target = 1 if on else 0
        try:
            current = self.get_output_state()
        except Exception:
            current = None
        if current == target:
            return
        self.send_and_check_error(f":OUTP:STAT {target}")
        self._invalidate_in_limits_cache(f"set_output:{target}")

    def set_output_enabled(self, enabled: bool) -> None:
        self.set_output(bool(enabled))

    def set_output_mode_active(self) -> None:
        try:
            if self.get_output_mode() == "ACT":
                return
        except Exception:
            pass
        self.send_and_check_error(":OUTP:MODE ACT")
        self._invalidate_in_limits_cache("set_output_mode:ACT")

    def set_output_mode_passive(self) -> None:
        try:
            if self.get_output_mode() == "PASS":
                return
        except Exception:
            pass
        self.send_and_check_error(":OUTP:MODE PASS")
        self._invalidate_in_limits_cache("set_output_mode:PASS")

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
        target = 1 if is_open else 0
        try:
            current = self.get_isolation_state()
        except Exception:
            current = None
        if current == target:
            return
        self.send_and_check_error(f":OUTP:ISOL:STAT {target}")
        self._invalidate_in_limits_cache(f"set_isolation_open:{target}")

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
        target = float(value_hpa)
        command = f":SOUR:PRES {target}"
        self.send_and_check_error(command)
        self._invalidate_in_limits_cache(f"set_setpoint:{target}")
        readback = self.get_setpoint()
        if readback is None or abs(float(readback) - target) > max(0.05, abs(target) * 1e-6):
            raise RuntimeError(
                "SETPOINT_READBACK_MISMATCH("
                f"expected={target},actual={readback})"
            )
        self._last_commanded_setpoint_hpa = target

    def get_setpoint(self) -> Optional[float]:
        for command in (":SOUR:PRES?", ":SOUR:PRES:LEV:IMM:AMPL?"):
            try:
                response = self.query(command)
            except Exception:
                continue
            value = self._parse_first_float(response)
            if value is not None:
                return float(value)
            self._consume_optional_query_error(command)
        return None

    def supports_sens_pres_cont(self) -> bool:
        if self._supports_sens_pres_cont is not None:
            return bool(self._supports_sens_pres_cont)
        profile = self._normalize_profile(self._device_profile)
        if profile == self.PROFILE_PACE5000E:
            self._supports_sens_pres_cont = True
            return True
        if profile == self.PROFILE_OLD_PACE5000:
            self._supports_sens_pres_cont = False
        return False

    def _recent_in_limits_pressure(self, *, max_age_s: float = 30.0) -> Optional[float]:
        if not self._in_limits_cache_is_current(max_age_s=max_age_s):
            return None
        return float(self._last_in_limits_pressure_hpa)

    def _in_limits_cache_is_current(self, *, max_age_s: float = 30.0) -> bool:
        if self._last_in_limits_pressure_hpa is None or self._last_in_limits_monotonic is None:
            return False
        if self._last_in_limits_generation != self._pressure_control_generation:
            return False
        current_setpoint = self._last_commanded_setpoint_hpa
        cached_setpoint = self._last_in_limits_setpoint_hpa
        if current_setpoint is None and cached_setpoint is None:
            pass
        elif current_setpoint is None or cached_setpoint is None:
            return False
        elif abs(float(current_setpoint) - float(cached_setpoint)) > max(0.05, abs(float(current_setpoint)) * 1e-6):
            return False
        age_s = time.monotonic() - self._last_in_limits_monotonic
        if age_s > max(0.1, float(max_age_s)):
            return False
        return True

    def _invalidate_in_limits_cache(self, reason: str) -> None:
        self._pressure_control_generation += 1
        self._last_in_limits_pressure_hpa = None
        self._last_in_limits_flag = None
        self._last_in_limits_monotonic = None
        self._last_in_limits_generation = None
        self._last_in_limits_setpoint_hpa = None
        self._last_in_limits_invalidation_reason = str(reason or "").strip() or "unspecified"
        self._last_in_limits_invalidation_count += 1

    def _pressure_read_candidates(self) -> List[str]:
        preferred = [":SENS:PRES?", ":MEAS:PRES?"]
        if self.supports_sens_pres_cont():
            preferred.append(":SENS:PRES:CONT?")

        ordered: List[str] = []
        for cmd in preferred + list(self.pressure_queries or []):
            text = str(cmd or "").strip().upper()
            if not text or text == ":SENS:PRES:INL?":
                continue
            if text == ":SENS:PRES:CONT?" and not self.supports_sens_pres_cont():
                continue
            if text not in ordered:
                ordered.append(text)
        return ordered

    def _read_pressure_from_candidates(self, candidates: Sequence[str]) -> float:
        last_exc: Optional[Exception] = None
        hint = self._last_pressure_query_hint
        if hint is not None and hint[0] in candidates:
            try:
                resp = self.query(hint[0], line_ending=hint[1])
                value = self._parse_first_float(resp)
                if value is not None:
                    return value
            except Exception:
                pass
        for idx in range(2):
            try:
                for cmd in candidates:
                    normalized_cmd = str(cmd or "").strip().upper()
                    if normalized_cmd == ":SENS:PRES:CONT?" and not self.supports_sens_pres_cont():
                        continue
                    for term in self.query_line_endings:
                        resp = self.query(cmd, line_ending=term)
                        value = self._parse_first_float(resp)
                        if value is not None:
                            if normalized_cmd == ":SENS:PRES:CONT?":
                                self._supports_sens_pres_cont = True
                            self._last_pressure_query_hint = (cmd, term)
                            return value
                        self._consume_optional_query_error(cmd)
                        if normalized_cmd == ":SENS:PRES:CONT?":
                            self._supports_sens_pres_cont = False
                last_exc = RuntimeError("NO_RESPONSE_OR_PARSE")
            except Exception as exc:
                last_exc = exc
            if idx < 1:
                time.sleep(0.1)
        if last_exc:
            raise last_exc
        raise RuntimeError("NO_RESPONSE")

    def read_pressure(self) -> float:
        cached = self._recent_in_limits_pressure()
        if cached is not None:
            return float(cached)
        try:
            return self._read_pressure_from_candidates(self._pressure_read_candidates())
        except Exception:
            current_pressure_hpa, _in_limit_flag = self.get_in_limits()
            return float(current_pressure_hpa)

    def vent(self, on: bool = True) -> None:
        # Per SCPI manual, vent command requires explicit 1(start)/0(abort-close).
        self.send_and_check_error(f":SOUR:PRES:LEV:IMM:AMPL:VENT {1 if on else 0}")
        self._invalidate_in_limits_cache(f"vent:{1 if on else 0}")

    def get_vent_status(self) -> int:
        resp = self.query(":SOUR:PRES:LEV:IMM:AMPL:VENT?")
        value = self._parse_first_int(resp)
        if value is None:
            raise RuntimeError("NO_RESPONSE")
        return value

    def get_vent_status_query(self) -> int:
        return self.get_vent_status()

    @classmethod
    def parse_vent_status_value(cls, response: Any) -> Optional[int]:
        return cls._parse_first_int(str(response))

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

    def _probe_device_model(self) -> Optional[str]:
        if self._device_model_probed:
            return self._device_model
        self._device_model_probed = True
        try:
            model = str(self.query(":INST:MOD?") or "").strip()
        except Exception:
            model = ""
        if not model:
            model = str(self._consume_optional_query_error(":INST:MOD?") or "").strip()
        if model:
            self._device_model = model
        return self._device_model

    def get_device_model(self) -> str:
        return str(self._probe_device_model() or "")

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

    def _reset_profile_probe_caches(self, *, clear_version: bool) -> None:
        self._device_identity = None
        self._device_identity_probed = False
        self._device_model = None
        self._device_model_probed = False
        self._device_profile = None
        self._device_profile_probed = False
        self._legacy_vent_status_model = None
        self._supports_sens_pres_cont = None
        if clear_version:
            self._instrument_version = None
            self._instrument_version_probed = False

    def _cache_detected_profile(self, profile: Any) -> str:
        normalized = self._normalize_profile(profile)
        self._device_profile = normalized
        self._device_profile_probed = normalized != self.PROFILE_UNKNOWN
        self._legacy_vent_status_model = normalized == self.PROFILE_OLD_PACE5000
        if normalized == self.PROFILE_OLD_PACE5000:
            self._supports_sens_pres_cont = False
        elif normalized == self.PROFILE_PACE5000E:
            self._supports_sens_pres_cont = True
        else:
            self._supports_sens_pres_cont = None
        return normalized

    @staticmethod
    def _normalize_instrument_version_text(version_text: Any) -> str:
        text = str(version_text or "").strip()
        if not text:
            return ""
        match = re.search(r"(\d+\.\d+\.\d+)", text)
        return match.group(1) if match else text.strip("\"'")

    def has_legacy_vent_status_model(self) -> bool:
        return self.detect_profile() == self.PROFILE_OLD_PACE5000

    def vent_status_is_idle(self, status: Any) -> bool:
        return self._parse_first_int(str(status)) == self.VENT_STATUS_IDLE

    def vent_status_is_in_progress(self, status: Any) -> bool:
        return self._parse_first_int(str(status)) == self.VENT_STATUS_IN_PROGRESS

    def vent_status_is_completed_latched(self, status: Any) -> bool:
        value = self._parse_first_int(str(status))
        return self.detect_profile() == self.PROFILE_OLD_PACE5000 and value == self.VENT_STATUS_COMPLETED

    def vent_status_is_timed_out(self, status: Any) -> bool:
        value = self._parse_first_int(str(status))
        return self.detect_profile() == self.PROFILE_PACE5000E and value == self.VENT_STATUS_TIMED_OUT

    def vent_status_is_trapped_pressure(self, status: Any) -> bool:
        value = self._parse_first_int(str(status))
        return self.detect_profile() == self.PROFILE_PACE5000E and value == self.VENT_STATUS_TRAPPED_PRESSURE

    def vent_status_is_aborted(self, status: Any) -> bool:
        value = self._parse_first_int(str(status))
        return self.detect_profile() == self.PROFILE_PACE5000E and value == self.VENT_STATUS_ABORTED

    def vent_status_is_unexpected_legacy_watchlist(self, status: Any) -> bool:
        value = self._parse_first_int(str(status))
        return self.detect_profile() == self.PROFILE_OLD_PACE5000 and value not in {
            self.VENT_STATUS_IDLE,
            self.VENT_STATUS_IN_PROGRESS,
            self.VENT_STATUS_COMPLETED,
        }

    def classify_vent_status(self, status: Any) -> str:
        value = self.parse_vent_status_value(status)
        if value is None:
            return "unknown"
        if self.vent_status_is_idle(value):
            return "idle"
        if self.vent_status_is_in_progress(value):
            return "in_progress"
        if self.vent_status_is_completed_latched(value):
            return "completed_latched"
        if self.vent_status_is_timed_out(value):
            return "timed_out"
        if self.vent_status_is_trapped_pressure(value):
            return "trapped_pressure"
        if self.vent_status_is_aborted(value):
            return "aborted"
        if self.vent_status_is_unexpected_legacy_watchlist(value):
            return "unexpected_legacy_watchlist"
        return "unknown"

    def vent_status_text(self, status: Any) -> str:
        classification = self.classify_vent_status(status)
        if classification == "completed_latched":
            return "completed"
        if classification == "timed_out":
            return "timeout"
        if classification == "unexpected_legacy_watchlist":
            value = self.parse_vent_status_value(status)
            return f"watchlist_status_{value}" if value is not None else "watchlist"
        return classification

    def describe_vent_status(self, status: Any) -> dict[str, Any]:
        value = self.parse_vent_status_value(status)
        classification = self.classify_vent_status(value)
        profile = self.detect_profile()
        if classification == "unknown" and profile != self.PROFILE_UNKNOWN:
            classification = self.classify_vent_status(value)
        return {
            "value": value,
            "classification": classification,
            "text": self.vent_status_text(value),
            "profile": profile,
        }

    def detect_profile(self, *, refresh: bool = False) -> str:
        cached_profile = self._normalize_profile(self._device_profile)
        if refresh:
            self._reset_profile_probe_caches(clear_version=True)
        elif self._device_profile_probed and cached_profile != self.PROFILE_UNKNOWN:
            return cached_profile
        elif cached_profile == self.PROFILE_UNKNOWN:
            self._reset_profile_probe_caches(clear_version=False)

        profile = self.PROFILE_UNKNOWN
        identity = self.get_device_identity()
        if identity and self._looks_like_legacy_vent_status_identity(identity):
            return self._cache_detected_profile(self.PROFILE_OLD_PACE5000)

        model = self.get_device_model()
        model_payload = self._response_payload(model).strip().strip("\"'").upper()
        model_code, _model_message = self._parse_system_error(model)
        if model_payload == self.PROFILE_PACE5000E:
            profile = self.PROFILE_PACE5000E
        elif model_code == -113 and profile == self.PROFILE_UNKNOWN:
            profile = self.PROFILE_OLD_PACE5000

        if profile == self.PROFILE_UNKNOWN:
            try:
                echo_status = str(self.query(":SYST:ECHO?") or "").strip()
            except Exception:
                echo_status = ""
            if not echo_status:
                echo_status = str(self._consume_optional_query_error(":SYST:ECHO?") or "").strip()
            echo_code, _echo_message = self._parse_system_error(echo_status)
            if echo_code == -113:
                profile = self.PROFILE_OLD_PACE5000

        return self._cache_detected_profile(profile)

    def has_legacy_vent_state_3_compatibility(self) -> bool:
        if not self.has_legacy_vent_status_model():
            return False
        return self._normalize_instrument_version_text(self.get_instrument_version()) == "02.00.07"

    def legacy_auto_abort_vent_override_enabled(self) -> bool:
        raw = os.getenv(self.LEGACY_AUTO_ABORT_VENT_OVERRIDE_ENV, "")
        return str(raw or "").strip().lower() in {"1", "true", "yes", "on"}

    def probe_identity(
        self,
        commands: Sequence[str],
    ) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        for command in list(commands or []):
            cmd = str(command or "").strip()
            if not cmd:
                continue
            started = time.perf_counter()
            response = ""
            error = ""
            try:
                if cmd.endswith("?"):
                    response = str(self.query(cmd) or "").strip()
                else:
                    self.send_command(cmd)
            except Exception as exc:
                error = str(exc)
            elapsed_ms = (time.perf_counter() - started) * 1000.0
            rows.append(
                {
                    "command": cmd,
                    "response": response,
                    "duration_ms": round(elapsed_ms, 3),
                    "error": error,
                }
            )
        return rows

    def legacy_auto_abort_vent_blocked(self, status: Any = None) -> bool:
        del status
        return self.has_legacy_vent_status_model() and not self.legacy_auto_abort_vent_override_enabled()

    def _legacy_auto_abort_vent_block_error(self, *, action: str, last_status: Any) -> str:
        status_text = "unknown" if last_status is None else str(last_status)
        return (
            "LEGACY_AUTO_ABORT_VENT_BLOCKED("
            f"action={action},last_status={status_text},manual_intervention_required=true,"
            f"recoverable=true,override_env={self.LEGACY_AUTO_ABORT_VENT_OVERRIDE_ENV})"
        )

    def _legacy_safe_vent_block_error(
        self,
        *,
        action: str,
        last_status: Any,
        output_state: Any,
        isolation_state: Any,
        step: str,
    ) -> str:
        status_text = "unknown" if last_status is None else str(last_status)
        output_text = "unknown" if output_state is None else str(output_state)
        isolation_text = "unknown" if isolation_state is None else str(isolation_state)
        return (
            "legacy_safe_vent_blocked("
            f"action={action},step={step},last_status={status_text},"
            f"output_state={output_text},isolation_state={isolation_text},recoverable=true)"
        )

    def legacy_completed_latch_auto_clear_blocked(self, status: Any) -> bool:
        value = self._parse_first_int(str(status))
        return (
            value == self.VENT_STATUS_COMPLETED
            and self.has_legacy_vent_state_3_compatibility()
            and not self.legacy_auto_abort_vent_override_enabled()
        )

    def _legacy_completed_latch_auto_clear_result(self, before_status: Any) -> dict[str, Any]:
        status_value = self._parse_first_int(str(before_status))
        return {
            "before_status": status_value,
            "clear_attempted": False,
            "after_status": status_value,
            "cleared": False,
            "command": "",
            "vent3_watchlist_observed": False,
            "skipped": True,
            "blocked": True,
            "reason": "legacy_completed_latch_auto_clear_blocked",
            "manual_intervention_required": True,
            "vent_command_sent": False,
        }

    def vent_status_allows_control(self, status: Any) -> bool:
        value = self._parse_first_int(str(status))
        if value is None:
            return False
        if self.vent_status_is_trapped_pressure(value):
            return False
        if self.has_legacy_vent_state_3_compatibility() and value == self.VENT_STATUS_TRAPPED_PRESSURE:
            # Real read-only runs do not provide a reliable closed loop proving
            # that VENT?=3 maps to the front-panel confirmation popup. We have also
            # observed the popup while SCPI still reported VENT?=2. Keep VENT=3
            # as a watchlist-only observation and never treat it as control-ready.
            return False
        if self.detect_profile() == self.PROFILE_OLD_PACE5000:
            return value in {self.VENT_STATUS_IDLE, self.VENT_STATUS_COMPLETED}
        return value == self.VENT_STATUS_IDLE

    def vent_terminal_statuses(self) -> List[int]:
        if self.detect_profile() == self.PROFILE_PACE5000E:
            return [
                self.VENT_STATUS_IDLE,
                self.VENT_STATUS_ABORTED,
            ]
        return [self.VENT_STATUS_IDLE]

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
            self.send_and_check_error(f":SOUR:PRES:LEV:IMM:AMPL:VENT:AFT:VVAL:STAT {state}")
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
            self.send_and_check_error(f":SOUR:PRES:LEV:IMM:AMPL:VENT:APOP:STAT {state}")
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
        if self.supports_sens_pres_cont():
            try:
                return self._read_pressure_from_candidates([":SENS:PRES:CONT?", ":SENS:PRES?", ":MEAS:PRES?"])
            except Exception:
                pass
        return self.read_pressure()

    def get_control_range(self) -> str:
        text = self.query(":SOUR:PRES:RANG?")
        payload = self._response_payload(text).strip().strip("\"'")
        if not payload:
            raise RuntimeError("NO_RESPONSE")
        return payload

    def query_available_ranges(self) -> List[str]:
        text = self.query(":INST:CAT:ALL?")
        payload = self._response_payload(text)
        quoted = [str(item or "").strip() for item in re.findall(r'"([^"]+)"', payload)]
        if quoted:
            return [item for item in quoted if item]
        compact = [part.strip().strip("\"'") for part in payload.split(",")]
        return [item for item in compact if item]

    def choose_control_range_for_target(self, target_hpa: float) -> Optional[str]:
        target = float(target_hpa)
        try:
            available = self.query_available_ranges()
        except Exception:
            return None
        candidates: List[Tuple[float, str]] = []
        for entry in available:
            upper_hpa = self._parse_range_upper_hpa(entry)
            if upper_hpa is None:
                continue
            if upper_hpa + 1e-9 >= target:
                candidates.append((upper_hpa, entry))
        if not candidates:
            return None
        candidates.sort(key=lambda item: (item[0], self._normalize_exact_range_token(item[1])))
        return candidates[0][1]

    def select_control_range(self, range_name: str) -> str:
        exact_name = str(range_name or "").strip().strip("\"'")
        if not exact_name:
            raise ValueError("range_name must not be empty")
        available = self.query_available_ranges()
        normalized_exact = self._normalize_exact_range_token(exact_name)
        match = next(
            (
                item
                for item in available
                if self._normalize_exact_range_token(item) == normalized_exact
            ),
            None,
        )
        if not match:
            raise RuntimeError(
                "CONTROL_RANGE_NOT_IN_CATALOG("
                f"requested={exact_name},available={available})"
            )
        try:
            current = self.get_control_range()
        except Exception:
            current = ""
        if self._normalize_exact_range_token(current) == self._normalize_exact_range_token(match):
            return current
        self.send_and_check_error(f':SOUR:PRES:RANG "{match}"')
        self._invalidate_in_limits_cache(f"select_control_range:{match}")
        readback = self.get_control_range()
        if self._normalize_exact_range_token(readback) != self._normalize_exact_range_token(match):
            raise RuntimeError(
                "CONTROL_RANGE_READBACK_MISMATCH("
                f"expected={match},actual={readback})"
            )
        return readback

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

    def get_in_limits_time_setting_s(self) -> float:
        resp = self.query(":SOUR:PRES:INL:TIME?")
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
            "vent3_watchlist_observed": False,
            "skipped": False,
            "blocked": False,
            "reason": "",
            "manual_intervention_required": False,
            "vent_command_sent": False,
        }
        if self.vent_status_is_timed_out(before_status):
            result["blocked"] = True
            result["reason"] = "vent_timed_out"
            result["manual_intervention_required"] = True
            return result
        if not self.vent_status_is_completed_latched(before_status):
            return result

        if self.legacy_completed_latch_auto_clear_blocked(before_status):
            return self._legacy_completed_latch_auto_clear_result(before_status)

        self.vent(False)
        result["clear_attempted"] = True
        result["command"] = ":SOUR:PRES:LEV:IMM:AMPL:VENT 0"
        result["vent_command_sent"] = True
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
                # Keep VENT=3 observable for diagnostics, but do not infer any
                # proven mapping to a front-panel popup or acknowledgement flow.
                result["vent3_watchlist_observed"] = True
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
            if self.vent_status_is_in_progress(status):
                time.sleep(max(0.05, poll_s))
                continue
            if status in accepted:
                return status
            if self.vent_status_is_completed_latched(status):
                if self.legacy_completed_latch_auto_clear_blocked(status):
                    raise RuntimeError("VENT_COMPLETED_LATCH_AUTO_CLEAR_BLOCKED(last_status=2)")
                if not clear_sent or clear_retries < max_clear_retries:
                    self.vent(False)
                    clear_sent = True
                    clear_retries += 1
                time.sleep(max(0.05, poll_s))
                continue
            if self.vent_status_is_timed_out(status):
                raise RuntimeError(f"VENT_TIMED_OUT(last_status={status})")
            if self.vent_status_is_trapped_pressure(status):
                raise RuntimeError(f"VENT_TRAPPED_PRESSURE(last_status={status})")
            if self.vent_status_is_unexpected_legacy_watchlist(status):
                raise RuntimeError(f"VENT_STATUS_{status}_WATCHLIST_OLD")
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
        ok_statuses = (
            [self.VENT_STATUS_IDLE, self.VENT_STATUS_COMPLETED]
            if self.detect_profile() == self.PROFILE_OLD_PACE5000
            else None
        )
        status = self.wait_for_vent_idle(timeout_s=timeout_s, poll_s=poll_s, ok_statuses=ok_statuses)
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
        ok_statuses = (
            [self.VENT_STATUS_IDLE, self.VENT_STATUS_COMPLETED]
            if self.detect_profile() == self.PROFILE_OLD_PACE5000
            else None
        )
        status = self.wait_for_vent_idle(timeout_s=timeout_s, poll_s=poll_s, ok_statuses=ok_statuses)
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

    def enter_legacy_diagnostic_safe_vent_mode(
        self,
        *,
        timeout_s: float = 30.0,
        poll_s: float = 0.25,
        action: str = "diagnostic_safe_vent",
    ) -> dict[str, Any]:
        legacy_identity = self.has_legacy_vent_status_model()
        result = {
            "action": action,
            "legacy_identity": legacy_identity,
            "ok": False,
            "recoverable": True,
            "reason": "",
            "vent_command_sent": False,
            "before_status": None,
            "after_status": None,
            "output_state": None,
            "isolation_state": None,
        }
        if not legacy_identity:
            result["reason"] = "non_legacy_identity"
            return result

        self.stop_atmosphere_hold()
        try:
            self.set_output(False)
            self.set_isolation_open(True)
        except Exception as exc:
            result["reason"] = self._legacy_safe_vent_block_error(
                action=action,
                last_status=None,
                output_state=None,
                isolation_state=None,
                step=f"prepare:{exc}",
            )
            return result

        try:
            output_state = self.get_output_state()
        except Exception:
            output_state = None
        try:
            isolation_state = self.get_isolation_state()
        except Exception:
            isolation_state = None
        result["output_state"] = output_state
        result["isolation_state"] = isolation_state

        try:
            before_status = self.get_vent_status()
        except Exception as exc:
            result["reason"] = self._legacy_safe_vent_block_error(
                action=action,
                last_status=None,
                output_state=output_state,
                isolation_state=isolation_state,
                step=f"read_status:{exc}",
            )
            return result
        result["before_status"] = before_status
        result["after_status"] = before_status

        if output_state != 0:
            result["reason"] = self._legacy_safe_vent_block_error(
                action=action,
                last_status=before_status,
                output_state=output_state,
                isolation_state=isolation_state,
                step="prepare_verify",
            )
            return result
        if isolation_state != 1:
            result["reason"] = self._legacy_safe_vent_block_error(
                action=action,
                last_status=before_status,
                output_state=output_state,
                isolation_state=isolation_state,
                step="prepare_verify",
            )
            return result

        if before_status == self.VENT_STATUS_TRAPPED_PRESSURE:
            result["reason"] = self._legacy_safe_vent_block_error(
                action=action,
                last_status=before_status,
                output_state=output_state,
                isolation_state=isolation_state,
                step="watchlist_status_3",
            )
            return result

        if before_status in {self.VENT_STATUS_IDLE, self.VENT_STATUS_COMPLETED}:
            try:
                self.vent(True)
            except Exception as exc:
                result["reason"] = self._legacy_safe_vent_block_error(
                    action=action,
                    last_status=before_status,
                    output_state=output_state,
                    isolation_state=isolation_state,
                    step=f"vent_on:{exc}",
                )
                return result
            result["vent_command_sent"] = True
        elif before_status != self.VENT_STATUS_IN_PROGRESS:
            result["reason"] = self._legacy_safe_vent_block_error(
                action=action,
                last_status=before_status,
                output_state=output_state,
                isolation_state=isolation_state,
                step="unsupported_status",
            )
            return result

        deadline = time.time() + max(0.5, float(timeout_s))
        last_status = before_status
        while time.time() < deadline:
            try:
                last_status = self.get_vent_status()
            except Exception as exc:
                result["reason"] = self._legacy_safe_vent_block_error(
                    action=action,
                    last_status=last_status,
                    output_state=output_state,
                    isolation_state=isolation_state,
                    step=f"observe:{exc}",
                )
                return result
            result["after_status"] = last_status
            if last_status in {self.VENT_STATUS_IN_PROGRESS, self.VENT_STATUS_COMPLETED}:
                result["ok"] = True
                result["reason"] = (
                    "legacy_safe_vent_observed("
                    f"action={action},last_status={last_status},"
                    f"vent_command_sent={str(bool(result['vent_command_sent'])).lower()})"
                )
                return result
            if last_status == self.VENT_STATUS_TRAPPED_PRESSURE:
                break
            time.sleep(max(0.05, float(poll_s)))

        result["reason"] = self._legacy_safe_vent_block_error(
            action=action,
            last_status=last_status,
            output_state=output_state,
            isolation_state=isolation_state,
            step="observe_timeout",
        )
        return result

    def exit_atmosphere_mode(
        self,
        *,
        timeout_s: float = 30.0,
        poll_s: float = 0.25,
    ) -> int:
        self.stop_atmosphere_hold()
        self.set_output(False)
        current_status = None
        try:
            current_status = self.get_vent_status()
        except Exception:
            current_status = None
        if self.legacy_auto_abort_vent_blocked(current_status):
            # Keep the output path open for diagnostics/flush visibility, but do
            # not auto-send raw VENT 0 on legacy GE Druck units unless the
            # explicit rollback override is set.
            self.set_isolation_open(True)
            if current_status in {self.VENT_STATUS_IDLE, self.VENT_STATUS_COMPLETED}:
                status = current_status
            else:
                raise RuntimeError(
                    self._legacy_auto_abort_vent_block_error(
                        action="exit_atmosphere_mode",
                        last_status=current_status,
                    )
                )
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
        if self.legacy_completed_latch_auto_clear_blocked(current_status):
            raise RuntimeError("VENT_COMPLETED_LATCH_AUTO_CLEAR_BLOCKED(last_status=2)")
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
        current_vent_status = self.get_vent_status()
        if self.vent_status_is_in_progress(current_vent_status):
            raise RuntimeError(f"VENT_IN_PROGRESS(last_status={current_vent_status})")
        if self.vent_status_is_timed_out(current_vent_status):
            raise RuntimeError(f"VENT_TIMED_OUT(last_status={current_vent_status})")
        if self.vent_status_is_aborted(current_vent_status):
            raise RuntimeError(f"VENT_ABORTED(last_status={current_vent_status})")
        if self.vent_status_is_trapped_pressure(current_vent_status):
            raise RuntimeError(f"VENT_TRAPPED_PRESSURE(last_status={current_vent_status})")
        if self.vent_status_is_unexpected_legacy_watchlist(current_vent_status):
            raise RuntimeError(f"VENT_STATUS_{current_vent_status}_WATCHLIST_OLD")
        if (
            self.has_legacy_vent_state_3_compatibility()
            and current_vent_status == self.VENT_STATUS_TRAPPED_PRESSURE
        ):
            raise RuntimeError(f"VENT_STATUS_{current_vent_status}")
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
        self.verify_output_enabled(True, timeout_s=max(0.5, timeout_s), poll_s=poll_s)

    def enable_controller(
        self,
        *,
        timeout_s: float = 2.0,
        poll_s: float = 0.1,
    ) -> int:
        self.enable_control_output(timeout_s=timeout_s, poll_s=poll_s)
        return self.get_output_state()

    def set_in_limits(self, pct_full_scale: float, time_s: float) -> None:
        target_pct = float(pct_full_scale)
        target_time_s = float(time_s)
        try:
            current_pct = self.get_in_limits_setting()
        except Exception:
            current_pct = None
        if current_pct is None or abs(float(current_pct) - target_pct) > 1e-9:
            self.send_and_check_error(f":SOUR:PRES:INL {target_pct}")
            self._invalidate_in_limits_cache(f"set_in_limits_pct:{target_pct}")
        try:
            current_time_s = self.get_in_limits_time_setting_s()
        except Exception:
            current_time_s = None
        if current_time_s is None or abs(float(current_time_s) - target_time_s) > 1e-9:
            self.send_and_check_error(f":SOUR:PRES:INL:TIME {target_time_s}")
            self._invalidate_in_limits_cache(f"set_in_limits_time:{target_time_s}")

    def get_in_limits(self) -> Tuple[float, int]:
        resp = self.query(":SENS:PRES:INL?")
        nums = re.findall(r"[-+]?\d+(?:\.\d+)?(?:[eE][-+]?\d+)?", resp or "")
        if not nums:
            raise RuntimeError("NO_RESPONSE")
        pressure = float(nums[0])
        flag = int(float(nums[1])) if len(nums) >= 2 else 0
        self._last_in_limits_pressure_hpa = pressure
        self._last_in_limits_flag = flag
        self._last_in_limits_monotonic = time.monotonic()
        self._last_in_limits_generation = self._pressure_control_generation
        self._last_in_limits_setpoint_hpa = self._last_commanded_setpoint_hpa
        return pressure, flag

    def wait_for_pressure_ready(
        self,
        *,
        target_hpa: Optional[float] = None,
        timeout_s: float = 10.0,
        poll_s: float = 0.25,
        consecutive_in_limits_required: int = 1,
        ready_dwell_s: float = 0.0,
        require_output_enabled: bool = True,
    ) -> Dict[str, Any]:
        expected_target_hpa = None if target_hpa is None else float(target_hpa)
        target_tolerance_hpa = (
            0.05
            if expected_target_hpa is None
            else max(0.05, abs(float(expected_target_hpa)) * 1e-6)
        )
        result: Dict[str, Any] = {
            "ok": False,
            "reason": "",
            "target_hpa": expected_target_hpa,
            "setpoint_hpa": None,
            "output_state": None,
            "last_pressure_hpa": None,
            "last_in_limit_flag": None,
            "poll_count": 0,
            "timeout_s": max(0.0, float(timeout_s)),
            "poll_s": max(0.0, float(poll_s)),
            "consecutive_in_limits_required": max(1, int(consecutive_in_limits_required)),
            "ready_dwell_s": max(0.0, float(ready_dwell_s)),
            "ready_hold_elapsed_s": 0.0,
            "recent_states": [],
        }
        try:
            setpoint_hpa = self.get_setpoint()
        except Exception as exc:
            result["reason"] = f"SetpointQueryFailed:{exc}"
            return result
        result["setpoint_hpa"] = setpoint_hpa
        if setpoint_hpa is None:
            result["reason"] = "SetpointUnavailable"
            return result
        if expected_target_hpa is not None and abs(float(setpoint_hpa) - expected_target_hpa) > target_tolerance_hpa:
            result["reason"] = "SetpointReadbackMismatch"
            return result

        try:
            output_state = self.get_output_state()
        except Exception as exc:
            result["reason"] = f"OutputStateQueryFailed:{exc}"
            return result
        result["output_state"] = output_state
        if require_output_enabled and int(output_state) != 1:
            result["reason"] = "OutputNotEnabled"
            return result

        deadline = time.monotonic() + max(0.05, float(timeout_s))
        consecutive_ok = 0
        ready_since_ts: Optional[float] = None
        while time.monotonic() < deadline:
            try:
                current_pressure_hpa, in_limit_flag = self.get_in_limits()
            except Exception as exc:
                result["reason"] = f"InLimitsQueryFailed:{exc}"
                return result
            result["poll_count"] = int(result["poll_count"]) + 1
            result["last_pressure_hpa"] = float(current_pressure_hpa)
            result["last_in_limit_flag"] = int(in_limit_flag)
            state = {
                "pressure_hpa": float(current_pressure_hpa),
                "in_limit_flag": int(in_limit_flag),
            }
            recent_states = list(result.get("recent_states") or [])
            recent_states.append(state)
            result["recent_states"] = recent_states[-20:]

            if int(in_limit_flag) == 1:
                consecutive_ok += 1
                if ready_since_ts is None:
                    ready_since_ts = time.monotonic()
            else:
                consecutive_ok = 0
                ready_since_ts = None

            ready_hold_elapsed_s = (
                0.0
                if ready_since_ts is None
                else max(0.0, time.monotonic() - ready_since_ts)
            )
            result["ready_hold_elapsed_s"] = ready_hold_elapsed_s
            if (
                consecutive_ok >= int(result["consecutive_in_limits_required"])
                and ready_hold_elapsed_s >= float(result["ready_dwell_s"])
            ):
                result["ok"] = True
                return result
            time.sleep(max(0.0, float(poll_s)))

        result["reason"] = "PressureInLimitsTimeout"
        return result

    def set_point_and_wait_stable(
        self,
        target_hpa: float,
        *,
        tolerance_hpa: float,
        timeout_s: float = 120.0,
        poll_s: float = 0.25,
        stable_count_required: int = 5,
        stable_hold_s: float = 2.0,
        select_range: bool = True,
        control_range: Optional[str] = None,
        unit: str = "HPA",
    ) -> Dict[str, Any]:
        target = float(target_hpa)
        tolerance = max(0.0, float(tolerance_hpa))
        stable_need = max(1, int(stable_count_required))
        hold_needed = max(0.0, float(stable_hold_s))
        self.detect_profile()
        self.set_unit(unit)

        chosen_range = None
        if control_range:
            chosen_range = self.select_control_range(control_range)
        elif select_range:
            candidate = self.choose_control_range_for_target(target)
            if candidate:
                chosen_range = self.select_control_range(candidate)

        current_range = None
        try:
            current_range = self.get_control_range()
        except Exception:
            current_range = chosen_range
        upper_hpa = self._parse_range_upper_hpa(current_range)
        if upper_hpa is not None and target > upper_hpa + 1e-9:
            raise RuntimeError(
                "TARGET_OUT_OF_CONTROL_RANGE("
                f"target_hpa={target},range={current_range},range_upper_hpa={upper_hpa})"
            )

        if self.get_output_state() != 1:
            self.enable_controller(timeout_s=min(timeout_s, 5.0), poll_s=poll_s)

        self.set_setpoint(target)
        setpoint_readback = self.get_setpoint()
        if setpoint_readback is None or abs(float(setpoint_readback) - target) > max(0.05, abs(target) * 1e-6):
            raise RuntimeError(
                "SETPOINT_READBACK_MISMATCH("
                f"expected={target},actual={setpoint_readback})"
            )

        deadline = time.time() + max(0.5, float(timeout_s))
        consecutive_ok = 0
        stable_since_ts: Optional[float] = None
        last_system_error = ""
        recent_states: List[Dict[str, Any]] = []
        control_pressure_hpa: Optional[float] = None
        while time.time() < deadline:
            current_pressure_hpa, in_limit_flag = self.get_in_limits()
            state = {
                "ts": time.time(),
                "current_pressure_hpa": current_pressure_hpa,
                "in_limit_flag": in_limit_flag,
            }
            recent_states.append(state)
            if len(recent_states) > 20:
                recent_states = recent_states[-20:]

            within_tol = abs(float(current_pressure_hpa) - target) <= tolerance
            stable_now = int(in_limit_flag) == 1 and within_tol
            if stable_now:
                consecutive_ok += 1
                if stable_since_ts is None:
                    stable_since_ts = time.time()
            else:
                consecutive_ok = 0
                stable_since_ts = None

            hold_elapsed = 0.0 if stable_since_ts is None else max(0.0, time.time() - stable_since_ts)
            if consecutive_ok >= stable_need or (stable_since_ts is not None and hold_elapsed >= hold_needed):
                control_pressure_hpa = float(current_pressure_hpa)
                return {
                    "ok": True,
                    "target_hpa": target,
                    "control_range": current_range,
                    "setpoint_readback_hpa": setpoint_readback,
                    "control_pressure_hpa": control_pressure_hpa,
                    "in_limit_pressure_hpa": float(current_pressure_hpa),
                    "stable_count": consecutive_ok,
                    "stable_hold_s": hold_elapsed,
                    "recent_states": list(recent_states),
                }

            time.sleep(max(0.05, float(poll_s)))

        try:
            last_system_error = self.get_system_error()
        except Exception:
            last_system_error = ""
        timeout_diag: Dict[str, Any] = {}
        for key, reader in (
            ("output_state", self.get_output_state),
            ("vent_status", self.get_vent_status),
            ("setpoint_hpa", self.get_setpoint),
        ):
            try:
                timeout_diag[key] = reader()
            except Exception as exc:
                timeout_diag[key] = f"ERROR:{exc}"
        safe_stop_result: Dict[str, Any] = {}
        try:
            safe_stop_result = self.safe_stop(
                vent_on=True,
                timeout_s=min(max(5.0, float(timeout_s)), 30.0),
                poll_s=poll_s,
            )
        except Exception as exc:
            safe_stop_result = {"error": str(exc)}
        raise RuntimeError(
            "SETPOINT_STABILITY_TIMEOUT("
            f"target_hpa={target},tolerance_hpa={tolerance},last_system_error={last_system_error},"
            f"recent_states={recent_states},timeout_diag={timeout_diag},safe_stop={safe_stop_result})"
        )

    def safe_stop(
        self,
        *,
        vent_on: bool = True,
        timeout_s: float = 30.0,
        poll_s: float = 0.25,
    ) -> Dict[str, Any]:
        self.detect_profile()
        self.set_output(False)
        self.set_isolation_open(True)
        vent_status = None
        if vent_on:
            self.vent(True)
            try:
                vent_status = self.wait_for_vent_idle(timeout_s=timeout_s, poll_s=poll_s)
            except Exception:
                vent_status = self.get_vent_status()
        else:
            try:
                vent_status = self.get_vent_status()
            except Exception:
                vent_status = None
        return {
            "profile": self.detect_profile(),
            "output_state": self.get_output_state(),
            "isolation_state": self.get_isolation_state(),
            "vent_status": vent_status,
            "vent_command_sent": bool(vent_on),
            "system_error": self.get_system_error(),
        }

    def detectProfile(self, *, refresh: bool = False) -> str:  # noqa: N802
        return self.detect_profile(refresh=refresh)

    def sendAndCheckError(self, cmd: str, *, tolerated_error_codes: Optional[Iterable[int]] = None) -> str:  # noqa: N802
        return self.send_and_check_error(cmd, tolerated_error_codes=tolerated_error_codes)

    def setUnit(self, unit: str) -> str:  # noqa: N802
        return self.set_unit(unit)

    def selectControlRange(self, range_name: str) -> str:  # noqa: N802
        return self.select_control_range(range_name)

    def setPointAndWaitStable(  # noqa: N802
        self,
        target_hpa: float,
        *,
        tolerance_hpa: float,
        timeout_s: float = 120.0,
        poll_s: float = 0.25,
        stable_count_required: int = 5,
        stable_hold_s: float = 2.0,
        select_range: bool = True,
        control_range: Optional[str] = None,
        unit: str = "HPA",
    ) -> Dict[str, Any]:
        return self.set_point_and_wait_stable(
            target_hpa,
            tolerance_hpa=tolerance_hpa,
            timeout_s=timeout_s,
            poll_s=poll_s,
            stable_count_required=stable_count_required,
            stable_hold_s=stable_hold_s,
            select_range=select_range,
            control_range=control_range,
            unit=unit,
        )

    def safeStop(  # noqa: N802
        self,
        *,
        vent_on: bool = True,
        timeout_s: float = 30.0,
        poll_s: float = 0.25,
    ) -> Dict[str, Any]:
        return self.safe_stop(vent_on=vent_on, timeout_s=timeout_s, poll_s=poll_s)

    def status(self) -> dict[str, Any]:
        status = {
            "pressure_hpa": self.read_pressure(),
            "output_state": self.get_output_state(),
            "isolation_state": self.get_isolation_state(),
            "vent_status": self.get_vent_status(),
            "in_limits_cache_generation": self._pressure_control_generation,
            "in_limits_cache_valid": self._in_limits_cache_is_current(),
            "in_limits_cache_invalidation_reason": self._last_in_limits_invalidation_reason,
            "in_limits_cache_invalidation_count": self._last_in_limits_invalidation_count,
        }
        try:
            status["output_mode"] = self.get_output_mode()
        except Exception:
            status["output_mode"] = ""
        return status

    def selftest(self) -> dict[str, Any]:
        return self.status()
