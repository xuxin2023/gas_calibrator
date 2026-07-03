"""Gas analyzer driver."""

from __future__ import annotations

import json
import re
import time
from typing import Any, Dict, Optional

from ..senco_format import format_senco_value
from .serial_base import SerialDevice


class GasAnalyzer:
    """Gas analyzer protocol wrapper."""

    COMMAND_TARGET_ID = "FFF"
    SOFTWARE_VERSION_PRE_V5 = "pre_v5"
    SOFTWARE_VERSION_V5_PLUS = "v5_plus"
    PASSIVE_READ_RETRY_COUNT = 1
    PASSIVE_READ_DELAY_S = 0.05
    ACTIVE_READ_RETRY_COUNT = 4
    ACTIVE_READ_RETRY_DELAY_S = 0.01
    CONFIG_ACK_RETRY_COUNT = 1
    CONFIG_ACK_RETRY_DELAY_S = 0.1
    COMM_WAY_ACK_RETRY_COUNT = 3
    COMM_WAY_ACK_RETRY_DELAY_S = 0.2
    COEFFICIENT_COMM_QUIET_DELAY_S = 0.15
    COEFFICIENT_READ_RETRY_COUNT = 2
    COEFFICIENT_READ_DELAY_S = 0.1
    COEFFICIENT_READ_TIMEOUT_S = 0.3
    CHECK_MONITOR_READ_RETRY_COUNT = 0
    CHECK_MONITOR_READ_DELAY_S = 0.1
    CHECK_MONITOR_READ_TIMEOUT_S = 0.6
    CHECK_MONITOR_COMMAND_GAP_S = 1.0
    _COEFFICIENT_TOKEN_RE = re.compile(
        r"C(?P<index>\d+)\s*:\s*(?P<value>[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?)"
    )
    _MODE2_KEYS = [
        "co2_ppm",
        "h2o_mmol",
        "co2_density",
        "h2o_density",
        "co2_ratio_f",
        "co2_ratio_raw",
        "h2o_ratio_f",
        "h2o_ratio_raw",
        "ref_signal",
        "co2_signal",
        "h2o_signal",
        "chamber_temp_c",
        "case_temp_c",
        "pressure_kpa",
    ]
    MODE2_SCHEMA_VERSION = "factory_mode2_16_v1"
    MODE2_COMPACT_NO_CASE_TEMP_SCHEMA_VERSION = "factory_mode2_15_no_case_temp_v1"
    MODE2_MIN_FIELD_COUNT = 2 + len(_MODE2_KEYS)
    MODE2_COMPACT_NO_CASE_TEMP_FIELD_COUNT = MODE2_MIN_FIELD_COUNT - 1
    _MODE2_TOKEN_LABELS = ["device_marker", "id"] + _MODE2_KEYS + ["status"]

    def __init__(
        self,
        port: str,
        baudrate: int = 115200,
        timeout: float = 1.0,
        device_id: str = "000",
        io_logger: Optional[Any] = None,
        serial_factory: Optional[Any] = None,
        ):
        self.ser = SerialDevice(
            port,
            baudrate=baudrate,
            timeout=timeout,
            device_name="gas_analyzer",
            io_logger=io_logger,
            serial_factory=serial_factory,
        )
        self.device_id = device_id
        self.active_send = False
        self._warning_phase = ""

    def set_warning_phase(self, phase: Optional[str]) -> None:
        text = str(phase or "").strip().lower()
        if text in {"startup", "runtime"}:
            self._warning_phase = text
        else:
            self._warning_phase = ""

    def _warning_code(self, code: str) -> str:
        base = str(code or "").strip().upper()
        if self._warning_phase == "startup":
            return f"STARTUP_{base}"
        if self._warning_phase == "runtime":
            return f"RUNTIME_{base}"
        return base

    @staticmethod
    def _is_success_ack(line: str) -> bool:
        text = str(line or "").strip().strip("<>").upper()
        return bool(re.search(r"YGAS,[0-9A-F]{3},T", text))

    def _log_no_ack(self, payload: str) -> None:
        logger = getattr(self.ser, "_log_io", None)
        if callable(logger):
            logger("WARN", command=payload + "\r\n", response=self._warning_code("NO_ACK"))

    def _log_retry(self, payload: str, attempt: int, attempts: int) -> None:
        logger = getattr(self.ser, "_log_io", None)
        if callable(logger):
            logger(
                "WARN",
                command=payload + "\r\n",
                response=f"{self._warning_code('NO_ACK_RETRY')} {attempt}/{attempts}",
            )

    def _send_config(
        self,
        cmd: str,
        *,
        timeout_s: float = 1.2,
        broadcast: bool = False,
        require_ack: bool = False,
    ) -> bool:
        try:
            self.ser.flush_input()
        except Exception:
            pass

        payload = cmd if broadcast else self._cmd(cmd).strip()
        self.ser.write(payload + "\r\n")
        if not require_ack:
            return True

        deadline = time.time() + max(0.2, timeout_s)
        while time.time() < deadline:
            remaining = max(0.05, min(0.25, deadline - time.time()))
            lines = self.ser.drain_input_nonblock(drain_s=remaining, read_timeout_s=0.05)
            for line in lines:
                if self._is_success_ack(line):
                    return True
            time.sleep(0.01)

        return False

    def _send_config_with_retries(
        self,
        cmd: str,
        *,
        timeout_s: float = 1.2,
        broadcast: bool = False,
        require_ack: bool = False,
        attempts: int = 1,
        retry_delay_s: float = 0.1,
    ) -> bool:
        total_attempts = max(1, int(attempts))
        for idx in range(total_attempts):
            acked = self._send_config(
                cmd,
                timeout_s=timeout_s,
                broadcast=broadcast,
                require_ack=require_ack,
            )
            if acked or not require_ack:
                return acked
            if idx + 1 < total_attempts:
                self._log_retry(cmd, idx + 1, total_attempts)
                time.sleep(max(0.01, float(retry_delay_s)))
        return False

    def open(self) -> None:
        self.ser.open()

    def connect(self) -> None:
        self.open()

    def close(self) -> None:
        self.ser.close()

    def write(self, data: str) -> None:
        self.ser.write(data)

    def _cmd(self, cmd: str) -> str:
        return f"{cmd},YGAS,{self.COMMAND_TARGET_ID}\r\n"

    def _cmd_with_args(self, cmd: str, *args: Any) -> str:
        suffix = ",".join(str(arg) for arg in args)
        return f"{cmd},YGAS,{self.COMMAND_TARGET_ID},{suffix}\r\n"

    @staticmethod
    def _format_senco_value(value: Any) -> str:
        return format_senco_value(value)

    def _average_cmd(self, channel: int, value: int) -> str:
        return f"AVERAGE{int(channel)},YGAS,{self.COMMAND_TARGET_ID},{int(value)}"

    @classmethod
    def normalize_software_version(cls, value: Any) -> str:
        normalized = str(value or "").strip().lower()
        if normalized in {"pre_v5", "pre-v5", "legacy", "v4"}:
            return cls.SOFTWARE_VERSION_PRE_V5
        return cls.SOFTWARE_VERSION_V5_PLUS

    @staticmethod
    def normalize_device_id(value: Any) -> str:
        text = str(value or "").strip()
        if not text:
            raise ValueError("device_id is required")
        if text.isdigit():
            return f"{int(text):03d}"
        return text.upper()

    def set_device_id(self, device_id: Any) -> bool:
        return self.set_device_id_with_ack(device_id, require_ack=True)

    def set_device_id_with_ack(self, device_id: Any, *, require_ack: bool = True) -> bool:
        normalized_id = self.normalize_device_id(device_id)
        payload = self._cmd_with_args("ID", normalized_id).strip()
        acked = self._send_config_with_retries(
            payload,
            broadcast=True,
            require_ack=require_ack,
            attempts=1 + max(0, int(self.CONFIG_ACK_RETRY_COUNT)),
            retry_delay_s=self.CONFIG_ACK_RETRY_DELAY_S,
        )
        if require_ack and not acked:
            self._log_no_ack(payload)
        if acked or not require_ack:
            self.device_id = normalized_id
        return acked

    def set_mode(self, mode: int) -> bool:
        return self.set_mode_with_ack(mode, require_ack=True)

    def set_mode_with_ack(self, mode: int, *, require_ack: bool = True) -> bool:
        payload = self._cmd_with_args("MODE", int(mode)).strip()
        acked = self._send_config_with_retries(
            payload,
            broadcast=True,
            require_ack=require_ack,
            attempts=1 + max(0, int(self.CONFIG_ACK_RETRY_COUNT)),
            retry_delay_s=self.CONFIG_ACK_RETRY_DELAY_S,
        )
        if require_ack and not acked:
            self._log_no_ack(payload)
        return acked

    def set_comm_way(self, active: bool) -> bool:
        return self.set_comm_way_with_ack(active, require_ack=True)

    def set_comm_way_with_ack(self, active: bool, *, require_ack: bool = True) -> bool:
        payload = self._cmd_with_args("SETCOMWAY", 1 if active else 0).strip()
        acked = self._send_config_with_retries(
            payload,
            broadcast=True,
            require_ack=require_ack,
            timeout_s=2.0,
            attempts=1 + max(0, int(self.COMM_WAY_ACK_RETRY_COUNT)),
            retry_delay_s=self.COMM_WAY_ACK_RETRY_DELAY_S,
        )
        self.active_send = bool(active)
        if require_ack and not acked:
            self._log_no_ack(payload)
        return acked

    def _prepare_coefficient_io(self) -> None:
        """Quiet active uploads before coefficient read/write commands."""
        try:
            self.set_comm_way_with_ack(False, require_ack=False)
        except Exception:
            pass
        quiet_delay_s = max(0.0, float(self.COEFFICIENT_COMM_QUIET_DELAY_S))
        if quiet_delay_s > 0:
            time.sleep(quiet_delay_s)
        try:
            self.ser.flush_input()
        except Exception:
            pass

    def set_active_freq(self, hz: int) -> bool:
        return self.set_active_freq_with_ack(hz, require_ack=True)

    def set_active_freq_with_ack(self, hz: int, *, require_ack: bool = True) -> bool:
        payload = self._cmd_with_args("FTD", int(hz)).strip()
        acked = self._send_config_with_retries(
            payload,
            broadcast=True,
            require_ack=require_ack,
            attempts=1 + max(0, int(self.CONFIG_ACK_RETRY_COUNT)),
            retry_delay_s=self.CONFIG_ACK_RETRY_DELAY_S,
        )
        if require_ack and not acked:
            self._log_no_ack(payload)
        return acked

    def set_average(self, co2_n: int, h2o_n: int) -> bool:
        return self.set_average_with_ack(co2_n=co2_n, h2o_n=h2o_n, require_ack=True)

    def set_average_with_ack(self, co2_n: int, h2o_n: int, *, require_ack: bool = True) -> bool:
        # Per bench manual: AVERAGE1 controls H2O channel, AVERAGE2 controls CO2 channel.
        payload_h2o = self._average_cmd(1, h2o_n)
        payload_co2 = self._average_cmd(2, co2_n)
        ack_h2o = self._send_config_with_retries(
            payload_h2o,
            broadcast=True,
            require_ack=require_ack,
            attempts=1 + max(0, int(self.CONFIG_ACK_RETRY_COUNT)),
            retry_delay_s=self.CONFIG_ACK_RETRY_DELAY_S,
        )
        if require_ack and not ack_h2o:
            self._log_no_ack(payload_h2o)
        ack_co2 = self._send_config_with_retries(
            payload_co2,
            broadcast=True,
            require_ack=require_ack,
            attempts=1 + max(0, int(self.CONFIG_ACK_RETRY_COUNT)),
            retry_delay_s=self.CONFIG_ACK_RETRY_DELAY_S,
        )
        if require_ack and not ack_co2:
            self._log_no_ack(payload_co2)
        return ack_h2o and ack_co2

    def set_average_filter(self, window_n: int) -> bool:
        return self.set_average_filter_with_ack(window_n, require_ack=True)

    def set_average_filter_channel_with_ack(
        self,
        channel: int,
        window_n: int,
        *,
        require_ack: bool = True,
    ) -> bool:
        payload = self._average_cmd(channel, window_n)
        acked = self._send_config_with_retries(
            payload,
            broadcast=True,
            require_ack=require_ack,
            attempts=1 + max(0, int(self.CONFIG_ACK_RETRY_COUNT)),
            retry_delay_s=self.CONFIG_ACK_RETRY_DELAY_S,
        )
        if require_ack and not acked:
            self._log_no_ack(payload)
        return acked

    def set_average_filter_with_ack(self, window_n: int, *, require_ack: bool = True) -> bool:
        all_acked = True
        for channel in (1, 2):
            acked = self.set_average_filter_channel_with_ack(
                channel,
                window_n,
                require_ack=require_ack,
            )
            if not acked:
                all_acked = False
            if channel == 1:
                time.sleep(0.05)
        return all_acked

    def set_senco(
        self,
        index: int,
        *coefficients: Any,
    ) -> bool:
        self._prepare_coefficient_io()
        values = list(coefficients)
        if len(values) == 1 and isinstance(values[0], (list, tuple)):
            values = list(values[0])
        if not values:
            raise ValueError("set_senco requires at least one coefficient")
        if len(values) > 6:
            raise ValueError("set_senco supports at most 6 coefficients")
        formatted = [self._format_senco_value(value) for value in values]
        payload = self._cmd_with_args(f"SENCO{index}", *formatted).strip()
        acked = self._send_config_with_retries(
            payload,
            broadcast=True,
            require_ack=True,
            attempts=1 + max(0, int(self.CONFIG_ACK_RETRY_COUNT)),
            retry_delay_s=self.CONFIG_ACK_RETRY_DELAY_S,
        )
        if not acked:
            self._log_no_ack(payload)
        return acked

    @classmethod
    def parse_coefficient_group_line(cls, line: str) -> Optional[Dict[str, float]]:
        text = str(line or "").strip().strip("<>")
        if not text:
            return None
        if re.search(r"YGAS,[0-9A-F]{3},F", text, re.IGNORECASE):
            return None
        matches = list(cls._COEFFICIENT_TOKEN_RE.finditer(text))
        if not matches:
            return None
        parsed: Dict[str, float] = {}
        for match in matches:
            parsed[f"C{int(match.group('index'))}"] = float(match.group("value"))
        return parsed or None

    def read_coefficient_group(
        self,
        index: int,
        *,
        delay_s: Optional[float] = None,
        timeout_s: Optional[float] = None,
        retries: Optional[int] = None,
    ) -> Dict[str, float]:
        self._prepare_coefficient_io()
        payload = self._cmd_with_args("GETCO", int(index)).strip()
        attempts = 1 + max(0, int(retries if retries is not None else self.COEFFICIENT_READ_RETRY_COUNT))
        read_delay_s = float(delay_s if delay_s is not None else self.COEFFICIENT_READ_DELAY_S)
        read_timeout_s = float(timeout_s if timeout_s is not None else self.COEFFICIENT_READ_TIMEOUT_S)
        last_line = ""
        try:
            self.ser.flush_input()
        except Exception:
            pass

        for attempt in range(attempts):
            self.ser.write(payload + "\r\n")
            if read_delay_s > 0:
                time.sleep(read_delay_s)
            # Some firmware revisions emit ACK/noise before the actual coefficient line,
            # so keep scanning the short response window until a parseable <C0:...> line appears.
            deadline = time.time() + max(0.05, read_timeout_s)
            saw_any = False
            saw_ack = False
            saw_non_ack = False

            while time.time() < deadline:
                lines: list[str] = []
                line = str(self.ser.readline() or "").strip()
                if line:
                    lines.extend(self._split_stream_lines(line))

                remaining = max(0.0, deadline - time.time())
                if remaining > 0:
                    drain = getattr(self.ser, "drain_input_nonblock", None)
                    if callable(drain):
                        lines.extend(self._split_stream_lines(drain(drain_s=min(0.05, remaining), read_timeout_s=0.05)))

                if not lines:
                    time.sleep(min(0.01, max(0.0, deadline - time.time())))
                    continue

                for candidate in lines:
                    text = str(candidate or "").strip()
                    if not text:
                        continue
                    saw_any = True
                    last_line = text
                    if self._is_success_ack(text):
                        saw_ack = True
                        continue
                    parsed = self.parse_coefficient_group_line(text)
                    if parsed:
                        return parsed
                    saw_non_ack = True

            if not saw_any:
                last_line = "NO_RESPONSE"
            elif saw_ack and not saw_non_ack:
                last_line = "ACK_ONLY"
            else:
                last_line = "NO_VALID_COEFFICIENT_LINE"
            if attempt + 1 < attempts:
                time.sleep(max(0.01, read_delay_s))

        raise RuntimeError(f"GETCO{int(index)} read failed: {last_line or 'NO_RESPONSE'}")

    @staticmethod
    def _to_voltage_float(token: Any) -> Optional[float]:
        text = str(token or "").strip()
        if not text:
            return None
        for sep in ("=", ":"):
            if sep in text:
                text = text.rsplit(sep, 1)[-1]
        match = re.search(r"[-+]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][-+]?\d+)?", text)
        if not match:
            return None
        try:
            return float(match.group(0))
        except Exception:
            return None

    @classmethod
    def parse_check_monitor_line(cls, line: str) -> Optional[Dict[str, Any]]:
        """Parse CHECK,YGAS,FFF thermostat-monitor response into two voltages."""

        for candidate in cls._iter_frame_candidates(line):
            parts = cls._split_frame_parts(candidate)
            if len(parts) < 4:
                continue
            upper_parts = [str(part or "").strip().upper() for part in parts]
            has_check = any(part == "CHECK" or part.startswith("CHECK") for part in upper_parts)
            has_ygas = any(part == "YGAS" for part in upper_parts)
            has_labeled_ntc = any(part.startswith(("NTC1", "NTC2")) for part in upper_parts)
            if not has_check and not has_labeled_ntc and not (has_ygas and len(parts) <= 6):
                continue

            device_id = ""
            labeled_voltages: dict[int, float] = {}
            generic_voltages: list[float] = []
            for idx, (part, upper) in enumerate(zip(parts, upper_parts)):
                if not part or upper in {"CHECK", "YGAS", "T", "F", "OK", "ACK"}:
                    continue
                if upper.startswith(("LOCK1", "NTC1")):
                    value = cls._to_voltage_float(part)
                    if value is not None:
                        labeled_voltages[1] = float(value)
                    continue
                if upper.startswith(("LOCK2", "NTC2")):
                    value = cls._to_voltage_float(part)
                    if value is not None:
                        labeled_voltages[2] = float(value)
                    continue
                previous = upper_parts[idx - 1] if idx > 0 else ""
                if previous == "YGAS" and re.fullmatch(r"[0-9A-F]{3}", upper):
                    if upper != cls.COMMAND_TARGET_ID:
                        device_id = upper
                    continue
                if upper == cls.COMMAND_TARGET_ID:
                    continue
                if upper.startswith(("TEMP", "PRESS", "V")):
                    continue
                value = cls._to_voltage_float(part)
                if value is None:
                    continue
                generic_voltages.append(float(value))

            voltages = (
                [labeled_voltages[1], labeled_voltages[2]]
                if 1 in labeled_voltages and 2 in labeled_voltages
                else generic_voltages
            )
            if len(voltages) < 2:
                continue
            return {
                "raw": str(line or "").strip(),
                "id": device_id,
                "thermostat_chip1_voltage_v": voltages[0],
                "thermostat_chip2_voltage_v": voltages[1],
                "voltage_count": len(voltages),
            }
        return None

    def read_check_monitor(
        self,
        *,
        delay_s: Optional[float] = None,
        timeout_s: Optional[float] = None,
        retries: Optional[int] = None,
        retry_gap_s: Optional[float] = None,
    ) -> Dict[str, Any]:
        """Read the firmware CHECK monitor without changing analyzer runtime mode."""

        payload = self._cmd("CHECK").strip()
        attempts = 1 + max(0, int(retries if retries is not None else self.CHECK_MONITOR_READ_RETRY_COUNT))
        read_delay_s = float(delay_s if delay_s is not None else self.CHECK_MONITOR_READ_DELAY_S)
        read_timeout_s = float(timeout_s if timeout_s is not None else self.CHECK_MONITOR_READ_TIMEOUT_S)
        retry_command_gap_s = max(
            self.CHECK_MONITOR_COMMAND_GAP_S,
            float(retry_gap_s if retry_gap_s is not None else self.CHECK_MONITOR_COMMAND_GAP_S),
        )
        raw_lines: list[str] = []

        try:
            self.ser.flush_input()
        except Exception:
            pass

        for attempt in range(attempts):
            self.ser.write(payload + "\r\n")
            if read_delay_s > 0:
                time.sleep(read_delay_s)

            deadline = time.time() + max(0.05, read_timeout_s)
            while time.time() < deadline:
                lines: list[str] = []
                line = str(self.ser.readline() or "").strip()
                if line:
                    lines.extend(self._split_stream_lines(line))

                remaining = max(0.0, deadline - time.time())
                if remaining > 0:
                    drain = getattr(self.ser, "drain_input_nonblock", None)
                    if callable(drain):
                        lines.extend(
                            self._split_stream_lines(
                                drain(drain_s=min(0.05, remaining), read_timeout_s=0.05)
                            )
                        )

                if not lines:
                    time.sleep(min(0.01, max(0.0, deadline - time.time())))
                    continue

                for candidate in lines:
                    text = str(candidate or "").strip()
                    if not text:
                        continue
                    raw_lines.append(text)
                    parsed = self.parse_check_monitor_line(text)
                    if parsed:
                        parsed.update(
                            {
                                "ok": True,
                                "command": payload,
                                "raw_lines": list(raw_lines),
                            }
                        )
                        return parsed

            if attempt + 1 < attempts:
                time.sleep(retry_command_gap_s)

        return {
            "ok": False,
            "command": payload,
            "raw": raw_lines[-1] if raw_lines else "",
            "raw_lines": raw_lines,
            "error": "NO_VALID_CHECK_LINE" if raw_lines else "NO_RESPONSE",
        }

    def read_data_passive(self) -> str:
        payload = self._cmd("READDATA")
        attempts = 1 + max(0, int(self.PASSIVE_READ_RETRY_COUNT))
        for idx in range(attempts):
            self.ser.write(payload)
            time.sleep(self.PASSIVE_READ_DELAY_S)
            line = self.ser.readline()
            if str(line or "").strip():
                return line
            if idx + 1 < attempts:
                time.sleep(self.PASSIVE_READ_DELAY_S)
        return ""

    @staticmethod
    def _split_stream_lines(raw: Any) -> list[str]:
        if raw is None:
            return []
        if isinstance(raw, (list, tuple)):
            lines: list[str] = []
            for item in raw:
                lines.extend(GasAnalyzer._split_stream_lines(item))
            return lines
        text = str(raw or "").replace("\r", "\n")
        return [line.strip() for line in text.split("\n") if line.strip()]

    def _drain_stream_lines(self, drain_s: float = 0.35, read_timeout_s: float = 0.05) -> list[str]:
        drain = getattr(self.ser, "drain_input_nonblock", None)
        if callable(drain):
            return self._split_stream_lines(drain(drain_s=drain_s, read_timeout_s=read_timeout_s))

        lines: list[str] = []
        read_available = getattr(self.ser, "read_available", None)
        if callable(read_available):
            lines.extend(self._split_stream_lines(read_available()))
            if lines:
                return lines

        readline = getattr(self.ser, "readline", None)
        if not callable(readline):
            return lines

        deadline = time.time() + max(0.0, float(drain_s))
        while True:
            line = readline()
            chunk_lines = self._split_stream_lines(line)
            if chunk_lines:
                lines.extend(chunk_lines)
            now = time.time()
            if now >= deadline:
                break
            if not chunk_lines:
                if lines:
                    break
                time.sleep(min(0.01, max(0.0, deadline - now)))
        return lines

    def read_data_active(self, drain_s: float = 0.35, read_timeout_s: float = 0.05) -> str:
        attempts = 1 + max(0, int(self.ACTIVE_READ_RETRY_COUNT))
        last_lines: list[str] = []
        for idx in range(attempts):
            lines = self._drain_stream_lines(drain_s=drain_s, read_timeout_s=read_timeout_s)
            if lines:
                last_lines = lines
                for line in reversed(lines):
                    if self.parse_line_mode2(line):
                        return line
            if idx + 1 < attempts:
                time.sleep(max(0.0, float(self.ACTIVE_READ_RETRY_DELAY_S)))

        if not last_lines:
            return ""
        for line in reversed(last_lines):
            if self.parse_line(line):
                return line
        return last_lines[-1]

    def read_latest_data(
        self,
        *,
        prefer_stream: Optional[bool] = None,
        drain_s: float = 0.35,
        read_timeout_s: float = 0.05,
        allow_passive_fallback: bool = False,
    ) -> str:
        use_stream = self.active_send if prefer_stream is None else bool(prefer_stream)
        if use_stream:
            line = self.read_data_active(drain_s=drain_s, read_timeout_s=read_timeout_s)
            if line or not allow_passive_fallback:
                return line
        return self.read_data_passive()

    def read(self) -> Optional[Dict[str, Any]]:
        return self.parse_line(self.read_latest_data())

    def read_current_mode_snapshot(
        self,
        *,
        prefer_stream: Optional[bool] = None,
        drain_s: float = 0.2,
        read_timeout_s: float = 0.05,
        allow_passive_fallback: bool = True,
    ) -> Optional[Dict[str, Any]]:
        line = self.read_latest_data(
            prefer_stream=prefer_stream,
            drain_s=drain_s,
            read_timeout_s=read_timeout_s,
            allow_passive_fallback=allow_passive_fallback,
        )
        parsed = self.parse_line(line)
        if not isinstance(parsed, dict) or not parsed:
            return None
        return {
            "mode": parsed.get("mode"),
            "id": parsed.get("id"),
            "raw": parsed.get("raw") or line,
        }

    def status(self) -> Dict[str, Any]:
        data = self.read() or {}
        return {
            "ok": bool(data),
            "mode": data.get("mode"),
            "co2_ppm": data.get("co2_ppm"),
            "h2o_mmol": data.get("h2o_mmol"),
            "status": data.get("status"),
            "raw": data.get("raw"),
        }

    def selftest(self) -> Dict[str, Any]:
        return self.status()

    @staticmethod
    def _to_float(value: str) -> Optional[float]:
        text = str(value or "").strip()
        try:
            return float(text)
        except Exception:
            match = re.search(r"[-+]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][-+]?\d+)?", text)
            if not match:
                return None
            try:
                return float(match.group(0))
            except Exception:
                return None

    @staticmethod
    def _to_float_strict(value: str) -> Optional[float]:
        text = str(value or "").strip()
        if not re.fullmatch(r"[-+]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][-+]?\d+)?", text):
            return None
        try:
            return float(text)
        except Exception:
            return None

    @staticmethod
    def _clean_token(token: Any) -> str:
        text = str(token or "").strip()
        text = text.lstrip("<>[](){} \t\r\n")
        for marker in (">", "]", ")", "}", "\r", "\n"):
            if marker in text:
                text = text.split(marker, 1)[0]
        return text.strip().strip("<>[](){} \t\r\n")

    @classmethod
    def _split_frame_parts(cls, frame: str) -> list[str]:
        return [cls._clean_token(part) for part in str(frame or "").strip().split(",")]

    @staticmethod
    def _json_compact(value: Any) -> str:
        return json.dumps(value, ensure_ascii=False, separators=(",", ":"))

    @classmethod
    def _mode2_fields_payload(cls, cleaned: list[str], numeric_keys: Optional[list[str]] = None) -> Dict[str, str]:
        payload: Dict[str, str] = {}
        for idx, token in enumerate(cleaned, start=1):
            payload[f"field_{idx:02d}"] = token
        token_labels = ["device_marker", "id"] + list(numeric_keys or cls._MODE2_KEYS) + ["status"]
        for idx, label in enumerate(token_labels):
            if idx < len(cleaned):
                payload[label] = cleaned[idx]
        return payload

    @staticmethod
    def _iter_frame_candidates(line: str) -> list[str]:
        text = str(line or "").strip()
        if not text:
            return []

        candidates: list[str] = []
        seen = set()
        upper_text = text.upper()
        for match in re.finditer(r"YGAS\s*,", upper_text):
            candidate = text[match.start() :].strip()
            if candidate and candidate not in seen:
                seen.add(candidate)
                candidates.append(candidate)
        if text not in seen:
            candidates.append(text)
        return candidates

    @staticmethod
    def _parse_mode2(parts: list[str], line: str) -> Optional[Dict[str, Any]]:
        cleaned = GasAnalyzer._split_frame_parts(",".join(parts))
        if len(cleaned) < GasAnalyzer.MODE2_COMPACT_NO_CASE_TEMP_FIELD_COUNT:
            return None
        head = (cleaned[0] or "").strip().upper()
        if "YGAS" not in head:
            return None
        numeric_keys = list(GasAnalyzer._MODE2_KEYS)
        schema_version = GasAnalyzer.MODE2_SCHEMA_VERSION
        min_field_count = GasAnalyzer.MODE2_MIN_FIELD_COUNT
        omitted_fields: list[str] = []
        compact_status_like = (
            len(cleaned) == GasAnalyzer.MODE2_MIN_FIELD_COUNT
            and GasAnalyzer._to_float_strict(cleaned[-1]) is None
            and GasAnalyzer._to_float_strict(cleaned[-2]) is not None
        )
        if len(cleaned) == GasAnalyzer.MODE2_COMPACT_NO_CASE_TEMP_FIELD_COUNT or compact_status_like:
            numeric_keys = [key for key in GasAnalyzer._MODE2_KEYS if key != "case_temp_c"]
            schema_version = GasAnalyzer.MODE2_COMPACT_NO_CASE_TEMP_SCHEMA_VERSION
            min_field_count = GasAnalyzer.MODE2_COMPACT_NO_CASE_TEMP_FIELD_COUNT
            omitted_fields = ["case_temp_c"]
        elif len(cleaned) < GasAnalyzer.MODE2_MIN_FIELD_COUNT:
            return None
        status_index = 2 + len(numeric_keys)
        extra_start_index = status_index + 1 if len(cleaned) > status_index else status_index
        data = {
            "raw": line,
            "id": cleaned[1] if len(cleaned) > 1 else None,
            "mode": 2,
            "mode2_schema_version": schema_version,
            "mode2_field_count": len(cleaned),
            "mode2_min_field_count": min_field_count,
            "mode2_known_field_count": len(numeric_keys),
            "mode2_extra_count": max(0, len(cleaned) - extra_start_index),
            "mode2_tokens_json": GasAnalyzer._json_compact(cleaned),
            "mode2_fields_json": GasAnalyzer._json_compact(GasAnalyzer._mode2_fields_payload(cleaned, numeric_keys)),
            "mode2_omitted_fields_json": GasAnalyzer._json_compact(omitted_fields),
        }
        for key in GasAnalyzer._MODE2_KEYS:
            data[key] = None
        for idx, key in enumerate(numeric_keys, start=2):
            if len(cleaned) > idx:
                data[key] = GasAnalyzer._to_float_strict(cleaned[idx])
        data["status"] = cleaned[status_index] if len(cleaned) > status_index and cleaned[status_index] else None
        if any(data[key] is None for key in numeric_keys):
            return None
        extras: Dict[str, str] = {}
        if len(cleaned) > extra_start_index:
            for idx, token in enumerate(cleaned[extra_start_index:], start=1):
                key = f"mode2_extra_{idx:02d}"
                data[key] = token
                extras[key] = token
        data["mode2_unknown_fields_json"] = GasAnalyzer._json_compact(extras)
        return data

    @staticmethod
    def _parse_legacy(parts: list[str], line: str) -> Optional[Dict[str, Any]]:
        cleaned = GasAnalyzer._split_frame_parts(",".join(parts))
        if len(cleaned) < 6:
            return None
        head = (cleaned[0] or "").strip().upper()
        if "YGAS" not in head:
            return None
        data = {"raw": line, "id": cleaned[1] if len(cleaned) > 1 else None, "mode": 1}
        data["co2_ppm"] = GasAnalyzer._to_float(cleaned[2])
        data["h2o_mmol"] = GasAnalyzer._to_float(cleaned[3])
        data["co2_sig"] = GasAnalyzer._to_float(cleaned[4]) if len(cleaned) > 4 else None
        data["h2o_sig"] = GasAnalyzer._to_float(cleaned[5]) if len(cleaned) > 5 else None
        data["temp_c"] = GasAnalyzer._to_float(cleaned[6]) if len(cleaned) > 6 else None
        data["pressure_kpa"] = GasAnalyzer._to_float(cleaned[7]) if len(cleaned) > 7 else None
        data["status"] = cleaned[8] if len(cleaned) > 8 and cleaned[8] else None
        if data["co2_ppm"] is None or data["h2o_mmol"] is None:
            return None
        return data

    def parse_line_mode2(self, line: str) -> Optional[Dict[str, Any]]:
        try:
            for candidate in self._iter_frame_candidates(line):
                parts = self._split_frame_parts(candidate)
                parsed = self._parse_mode2(parts, line)
                if parsed is not None:
                    return parsed
            return None
        except Exception:
            return None

    def parse_line(self, line: str) -> Optional[Dict[str, Any]]:
        try:
            for candidate in self._iter_frame_candidates(line):
                parts = self._split_frame_parts(candidate)
                mode2 = self._parse_mode2(parts, line)
                if mode2 is not None:
                    return mode2
                legacy = self._parse_legacy(parts, line)
                if legacy is not None:
                    return legacy
            return None
        except Exception:
            return None
