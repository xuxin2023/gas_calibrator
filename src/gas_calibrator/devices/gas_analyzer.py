"""Gas analyzer driver."""

from __future__ import annotations

import re
import time
from typing import Any, Dict, Optional

from ..senco_format import format_senco_value
from .serial_base import SerialDevice


class GasAnalyzer:
    """Gas analyzer protocol wrapper."""

    COMMAND_TARGET_ID = "FFF"
    READBACK_SOURCE_EXPLICIT_C0 = "parsed_from_explicit_c0_line"
    READBACK_SOURCE_AMBIGUOUS = "parsed_from_ambiguous_line"
    READBACK_SOURCE_NONE = "no_valid_coefficient_line"
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
    _COEFFICIENT_VALUE_RE = r"[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?"
    _COEFFICIENT_TOKEN_RE = re.compile(
        rf"C(?P<index>\d+)\s*:\s*(?P<value>{_COEFFICIENT_VALUE_RE})"
    )
    _EXPLICIT_COEFFICIENT_LINE_RE = re.compile(
        rf"^C0\s*:\s*{_COEFFICIENT_VALUE_RE}(?:\s*,\s*C\d+\s*:\s*{_COEFFICIENT_VALUE_RE})*$",
        re.IGNORECASE,
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
    MODE2_MIN_FIELD_COUNT = 2 + len(_MODE2_KEYS)

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

    def _cmd_for_target(self, target_id: Any, cmd: str, *args: Any) -> str:
        normalized_target = self.normalize_device_id(target_id or self.COMMAND_TARGET_ID)
        suffix = ",".join(str(arg) for arg in args)
        if suffix:
            return f"{cmd},YGAS,{normalized_target},{suffix}\r\n"
        return f"{cmd},YGAS,{normalized_target}\r\n"

    def build_getco_command(
        self,
        index: int,
        *,
        target_id: Any = None,
        command_style: str = "parameterized",
    ) -> str:
        normalized_target = self.normalize_device_id(target_id or self.device_id or self.COMMAND_TARGET_ID)
        style = str(command_style or "parameterized").strip().lower()
        if style == "compact":
            return f"GETCO{int(index)},YGAS,{normalized_target}\r\n"
        return self._cmd_for_target(normalized_target, "GETCO", int(index))

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

    @staticmethod
    def _strip_enclosing_wrappers(text: str) -> str:
        stripped = str(text or "").strip()
        wrapper_pairs = {"<": ">", "[": "]", "(": ")", "{": "}"}
        while len(stripped) >= 2 and stripped[0] in wrapper_pairs and stripped[-1] == wrapper_pairs[stripped[0]]:
            stripped = stripped[1:-1].strip()
        return stripped

    @classmethod
    def inspect_coefficient_group_line(cls, line: str) -> Dict[str, Any]:
        raw_line = str(line or "").strip()
        if not raw_line:
            return {
                "source": cls.READBACK_SOURCE_NONE,
                "coefficients": {},
                "source_line": "",
                "source_line_has_explicit_c0": False,
            }

        matches = list(cls._COEFFICIENT_TOKEN_RE.finditer(raw_line))
        if not matches:
            return {
                "source": cls.READBACK_SOURCE_NONE,
                "coefficients": {},
                "source_line": raw_line,
                "source_line_has_explicit_c0": False,
            }

        parsed: Dict[str, float] = {}
        for match in matches:
            parsed[f"C{int(match.group('index'))}"] = float(match.group("value"))

        normalized = cls._strip_enclosing_wrappers(raw_line)
        explicit_c0 = bool("C0" in parsed and cls._EXPLICIT_COEFFICIENT_LINE_RE.fullmatch(normalized))
        return {
            "source": cls.READBACK_SOURCE_EXPLICIT_C0 if explicit_c0 else cls.READBACK_SOURCE_AMBIGUOUS,
            "coefficients": parsed,
            "source_line": raw_line,
            "source_line_has_explicit_c0": explicit_c0,
        }

    @classmethod
    def parse_coefficient_group_line(cls, line: str) -> Optional[Dict[str, float]]:
        inspected = cls.inspect_coefficient_group_line(line)
        parsed = dict(inspected.get("coefficients") or {})
        return parsed or None

    def capture_getco_command(
        self,
        index: int,
        *,
        delay_s: Optional[float] = None,
        timeout_s: Optional[float] = None,
        retries: Optional[int] = None,
        target_id: Any = None,
        command_style: str = "parameterized",
        prepare_io: bool = True,
    ) -> Dict[str, Any]:
        if prepare_io:
            self._prepare_coefficient_io()
        normalized_target = self.normalize_device_id(target_id or self.device_id or self.COMMAND_TARGET_ID)
        payload = self.build_getco_command(
            int(index),
            target_id=normalized_target,
            command_style=command_style,
        ).strip()
        attempts = 1 + max(0, int(retries if retries is not None else self.COEFFICIENT_READ_RETRY_COUNT))
        read_delay_s = float(delay_s if delay_s is not None else self.COEFFICIENT_READ_DELAY_S)
        read_timeout_s = float(timeout_s if timeout_s is not None else self.COEFFICIENT_READ_TIMEOUT_S)
        transcript_lines: list[str] = []
        attempt_transcripts: list[Dict[str, Any]] = []
        first_ambiguous: Optional[Dict[str, Any]] = None
        last_error = "NO_RESPONSE"
        try:
            self.ser.flush_input()
        except Exception:
            pass

        for attempt in range(attempts):
            attempt_lines: list[str] = []
            self.ser.write(payload + "\r\n")
            if read_delay_s > 0:
                time.sleep(read_delay_s)
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
                    attempt_lines.append(text)
                    transcript_lines.append(text)
                    if self._is_success_ack(text):
                        saw_ack = True
                        continue
                    inspected = self.inspect_coefficient_group_line(text)
                    source = str(inspected.get("source") or self.READBACK_SOURCE_NONE)
                    if source == self.READBACK_SOURCE_EXPLICIT_C0:
                        attempt_transcripts.append({"attempt": int(attempt + 1), "lines": list(attempt_lines)})
                        return {
                            "command": payload + "\r\n",
                            "group": int(index),
                            "target_id": normalized_target,
                            "attempts": attempts,
                            "attempt_index": int(attempt + 1),
                            "source": source,
                            "coefficients": dict(inspected.get("coefficients") or {}),
                            "source_line": str(inspected.get("source_line") or ""),
                            "source_line_has_explicit_c0": bool(inspected.get("source_line_has_explicit_c0", False)),
                            "raw_transcript_lines": list(transcript_lines),
                            "attempt_transcripts": list(attempt_transcripts) + [{"attempt": int(attempt + 1), "lines": list(attempt_lines)}],
                            "error": "",
                        }
                    if source == self.READBACK_SOURCE_AMBIGUOUS:
                        saw_non_ack = True
                        if first_ambiguous is None:
                            first_ambiguous = {
                                "command": payload + "\r\n",
                                "group": int(index),
                                "target_id": normalized_target,
                                "attempts": attempts,
                                "attempt_index": int(attempt + 1),
                                "source": source,
                                "coefficients": dict(inspected.get("coefficients") or {}),
                                "source_line": str(inspected.get("source_line") or ""),
                                "source_line_has_explicit_c0": bool(inspected.get("source_line_has_explicit_c0", False)),
                                "raw_transcript_lines": [],
                                "attempt_transcripts": [],
                                "error": "AMBIGUOUS_COEFFICIENT_LINE",
                            }
                        continue
                    saw_non_ack = True

            attempt_transcripts.append({"attempt": int(attempt + 1), "lines": list(attempt_lines)})
            if not saw_any:
                last_error = "NO_RESPONSE"
            elif saw_ack and not saw_non_ack:
                last_error = "ACK_ONLY"
            else:
                last_error = "NO_VALID_COEFFICIENT_LINE"
            if attempt + 1 < attempts:
                time.sleep(max(0.01, read_delay_s))

        if first_ambiguous is not None:
            first_ambiguous["raw_transcript_lines"] = list(transcript_lines)
            first_ambiguous["attempt_transcripts"] = list(attempt_transcripts)
            return first_ambiguous

        return {
            "command": payload + "\r\n",
            "group": int(index),
            "target_id": normalized_target,
            "attempts": attempts,
            "attempt_index": int(attempts),
            "source": self.READBACK_SOURCE_NONE,
            "coefficients": {},
            "source_line": "",
            "source_line_has_explicit_c0": False,
            "raw_transcript_lines": list(transcript_lines),
            "attempt_transcripts": list(attempt_transcripts),
            "error": last_error,
        }

    def read_coefficient_group_capture(
        self,
        index: int,
        *,
        delay_s: Optional[float] = None,
        timeout_s: Optional[float] = None,
        retries: Optional[int] = None,
        target_id: Any = None,
        command_style: str = "parameterized",
        prepare_io: bool = True,
    ) -> Dict[str, Any]:
        return self.capture_getco_command(
            int(index),
            delay_s=delay_s,
            timeout_s=timeout_s,
            retries=retries,
            target_id=target_id,
            command_style=command_style,
            prepare_io=prepare_io,
        )

    def read_coefficient_group(
        self,
        index: int,
        *,
        delay_s: Optional[float] = None,
        timeout_s: Optional[float] = None,
        retries: Optional[int] = None,
        target_id: Any = None,
        command_style: str = "parameterized",
        prepare_io: bool = True,
        require_explicit_c0: bool = False,
    ) -> Dict[str, float]:
        capture = self.read_coefficient_group_capture(
            int(index),
            delay_s=delay_s,
            timeout_s=timeout_s,
            retries=retries,
            target_id=target_id,
            command_style=command_style,
            prepare_io=prepare_io,
        )
        source = str(capture.get("source") or self.READBACK_SOURCE_NONE)
        parsed = dict(capture.get("coefficients") or {})
        if parsed and (source == self.READBACK_SOURCE_EXPLICIT_C0 or not require_explicit_c0):
            return parsed
        error = str(capture.get("error") or "NO_VALID_COEFFICIENT_LINE")
        raise RuntimeError(f"GETCO{int(index)} read failed: {error}")

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
        if len(cleaned) < GasAnalyzer.MODE2_MIN_FIELD_COUNT:
            return None
        head = (cleaned[0] or "").strip().upper()
        if "YGAS" not in head:
            return None
        data = {
            "raw": line,
            "id": cleaned[1] if len(cleaned) > 1 else None,
            "mode": 2,
            "mode2_field_count": len(cleaned),
        }
        for key in GasAnalyzer._MODE2_KEYS:
            data[key] = None
        for idx, key in enumerate(GasAnalyzer._MODE2_KEYS, start=2):
            if len(cleaned) > idx:
                data[key] = GasAnalyzer._to_float(cleaned[idx])
        data["status"] = cleaned[16] if len(cleaned) > 16 and cleaned[16] else None
        if data["co2_ppm"] is None or data["h2o_mmol"] is None:
            return None
        if len(cleaned) > 17:
            for idx, token in enumerate(cleaned[17:], start=1):
                key = f"mode2_extra_{idx:02d}"
                data[key] = token
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
