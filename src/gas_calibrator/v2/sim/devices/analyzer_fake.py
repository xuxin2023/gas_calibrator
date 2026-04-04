from __future__ import annotations

from collections import deque
import math
import re
import time
from typing import Any, Optional


MODE2_KEYS = (
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
)
SUCCESS_ACK_RE = re.compile(r"YGAS,[0-9A-F]{3},T", re.IGNORECASE)


def _clean_token(value: Any) -> str:
    text = str(value or "").strip()
    text = text.strip("<>[](){} \t\r\n")
    return text


def _parse_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


class AnalyzerFake:
    """
    Protocol-level YGAS analyzer fake.

    This fake models the subset of the gas-analyzer protocol that V2 actually
    uses today. It intentionally behaves like a driver-facing device object,
    but also exposes text command processing so tests can validate command-level
    behavior without touching a real serial port.
    """

    COMMAND_TARGET_ID = "FFF"
    FRAME_TERMINATOR = "\r\n"
    SOFTWARE_VERSION_PRE_V5 = "pre_v5"
    SOFTWARE_VERSION_V5_PLUS = "v5_plus"
    VALID_STREAM_MODES = {
        "stable",
        "continuous_ok",
        "partial_frame",
        "truncated_frame",
        "corrupted_frame",
        "buffer_stale",
        "no_response",
        "mode_switch_not_applied",
        "active_send_disabled",
        "sensor_precheck_fail",
        "sensor_precheck_relaxed_allows_entry",
    }
    VALID_SENSOR_PRECHECK = {"strict_pass", "strict_fail", "relaxed_pass"}

    def __init__(
        self,
        port: str = "SIM-YGAS",
        *,
        baudrate: int = 115200,
        timeout: float = 1.0,
        io_logger: Optional[Any] = None,
        plant_state: Optional[Any] = None,
        name: str = "",
        device_id: str = "001",
        serial: str = "",
        software_version: str = SOFTWARE_VERSION_V5_PLUS,
        mode: int = 2,
        active_send: bool = True,
        ftd_hz: int = 5,
        average_filter: int = 1,
        average_co2: Optional[int] = None,
        average_h2o: Optional[int] = None,
        mode2_stream: str = "stable",
        sensor_precheck: str = "strict_pass",
        status_bits: Optional[str] = None,
        co2_signal: float = 400.0,
        h2o_signal: float = 10.0,
        chamber_temp_c: Optional[float] = None,
        case_temp_c: Optional[float] = None,
        pressure_hpa: Optional[float] = None,
        simulation_context: Optional[dict[str, Any]] = None,
        **_: Any,
    ) -> None:
        self.port = str(port or "SIM-YGAS")
        self.baudrate = int(baudrate or 115200)
        self.timeout = float(timeout if timeout is not None else 1.0)
        self.io_logger = io_logger
        self.plant_state = plant_state
        if self.plant_state is not None:
            setattr(self.plant_state, "dynamic_protocol", True)
        self.simulation_context = dict(simulation_context or {})
        self.name = str(name or "")
        self.connected = False

        self.device_id = self.normalize_device_id(device_id)
        self.serial = str(serial or f"SIM-{self.device_id}")
        self.software_version = self.normalize_software_version(software_version)
        self._requested_mode = int(mode or 2)
        self._effective_mode = int(mode or 2)
        self._active_send = bool(active_send)
        self._comm_way = 1 if self._active_send else 0
        self._ftd_hz = max(1, int(ftd_hz or 5))
        self._average_filter = int(average_filter or 1)
        self._average_co2 = int(average_co2 if average_co2 is not None else self._average_filter)
        self._average_h2o = int(average_h2o if average_h2o is not None else self._average_filter)
        self._mode2_stream = str(mode2_stream or "stable").strip().lower()
        if self._mode2_stream not in self.VALID_STREAM_MODES:
            self._mode2_stream = "stable"
        self._sensor_precheck = str(sensor_precheck or "strict_pass").strip().lower()
        if self._sensor_precheck not in self.VALID_SENSOR_PRECHECK:
            self._sensor_precheck = "strict_pass"
        self._status_bits = str(status_bits or "0000")
        self._co2_signal = float(co2_signal)
        self._h2o_signal = float(h2o_signal)
        self._chamber_temp_override = chamber_temp_c
        self._case_temp_override = case_temp_c
        self._pressure_hpa_override = pressure_hpa
        self._warning_phase = ""
        self._last_response = ""
        self._frame_index = 0
        self._buffer: deque[str] = deque()
        self._last_stream_emit_ts = time.monotonic()
        self._coefficients: dict[int, list[float]] = {}
        self._sentemps: dict[int, float] = {}
        self._mode_switch_applied = self._mode2_stream != "mode_switch_not_applied"

    @staticmethod
    def normalize_software_version(value: Any) -> str:
        normalized = str(value or "").strip().lower()
        if normalized in {"pre_v5", "pre-v5", "legacy", "v4"}:
            return AnalyzerFake.SOFTWARE_VERSION_PRE_V5
        return AnalyzerFake.SOFTWARE_VERSION_V5_PLUS

    @staticmethod
    def normalize_device_id(value: Any) -> str:
        text = str(value or "").strip()
        if not text:
            return AnalyzerFake.COMMAND_TARGET_ID
        if text.isdigit():
            return f"{int(text):03d}"
        return text.upper()

    @staticmethod
    def _is_success_ack(line: str) -> bool:
        return bool(SUCCESS_ACK_RE.search(str(line or "").strip()))

    def open(self) -> None:
        self.connected = True

    def connect(self) -> bool:
        self.connected = True
        return True

    def close(self) -> None:
        self.connected = False
        self._buffer.clear()

    def write(self, data: str) -> None:
        self._last_response = self.process_command(data)

    def query(self, data: str) -> str:
        return self.process_command(data).strip()

    def set_warning_phase(self, phase: Optional[str]) -> None:
        self._warning_phase = str(phase or "").strip().lower()

    def process_command(self, payload: str) -> str:
        text = str(payload or "").strip()
        if not text:
            return ""
        parts = [_clean_token(item) for item in text.replace("\r", "").replace("\n", "").split(",")]
        if not parts:
            return ""
        cmd = parts[0].upper()
        if cmd.startswith("FETC?"):
            return ""
        if len(parts) >= 3 and parts[1].upper() == "YGAS":
            target = self.normalize_device_id(parts[2] or self.COMMAND_TARGET_ID)
            if target not in {self.COMMAND_TARGET_ID, self.device_id}:
                return ""
            args = parts[3:]
        else:
            target = self.device_id
            args = parts[1:]

        if cmd in {"MODE"}:
            if args:
                self._requested_mode = int(float(args[0]))
                if self._mode_switch_applied:
                    self._effective_mode = self._requested_mode
            return self._ack()
        if cmd in {"SETCOMWAY", "SETCOM"}:
            if args:
                enabled = bool(int(float(args[0])))
                if self._mode2_stream != "active_send_disabled":
                    self._active_send = enabled
                self._comm_way = 1 if enabled else 0
            return self._ack()
        if cmd == "FTD":
            if args:
                self._ftd_hz = max(1, int(float(args[0])))
            return self._ack()
        if cmd in {"AVERAGE", "AVERAGE1", "AVERAGE2"}:
            self._handle_average_command(cmd, args)
            return self._ack()
        if cmd == "ID":
            if args:
                self.device_id = self.normalize_device_id(args[0])
                if not self.serial.startswith("SIM-"):
                    self.serial = f"SIM-{self.device_id}"
                return self._ack()
            return f"YGAS,{self.device_id},ID,{self.device_id}{self.FRAME_TERMINATOR}"
        if cmd.startswith("SENCO"):
            index = int(cmd.replace("SENCO", "") or 0)
            self._coefficients[index] = [_parse_float(item) for item in args]
            return self._ack()
        if cmd == "GETCO":
            index = int(args[0]) if args else 1
            coeffs = self._coefficients.get(index, [1.0, 0.0, 0.0, 0.0, 0.0, 0.0])
            tokens = [f"C{idx + 1}:{value:.6g}" for idx, value in enumerate(coeffs)]
            return ",".join(tokens) + self.FRAME_TERMINATOR
        if cmd.startswith("CLEARSENCO"):
            index = int(cmd.replace("CLEARSENCO", "") or 0)
            self._coefficients.pop(index, None)
            return self._ack()
        if cmd in {"SENTEMP1", "SENTEMP2"}:
            sensor_index = 1 if cmd.endswith("1") else 2
            if args:
                self._sentemps[sensor_index] = _parse_float(args[0])
            return self._ack()
        if cmd == "READDATA":
            return self._next_frame(passive=True)
        return self._error_ack(-113)

    def _handle_average_command(self, cmd: str, args: list[str]) -> None:
        if not args:
            return
        value = max(1, int(float(args[0])))
        if cmd == "AVERAGE":
            self._average_h2o = value
            self._average_co2 = value
            self._average_filter = value
            return
        if cmd == "AVERAGE1":
            self._average_h2o = value
            self._average_filter = value
            return
        self._average_co2 = value
        self._average_filter = value

    def _ack(self) -> str:
        return f"YGAS,{self.device_id},T{self.FRAME_TERMINATOR}"

    def _error_ack(self, code: int) -> str:
        return f"YGAS,{self.device_id},F,{int(code)}{self.FRAME_TERMINATOR}"

    def read_device_id(self) -> str:
        return self.device_id

    def get_device_id(self) -> str:
        return self.device_id

    def set_device_id(self, device_id: Any) -> bool:
        return self.set_device_id_with_ack(device_id, require_ack=True)

    def set_device_id_with_ack(self, device_id: Any, *, require_ack: bool = True) -> bool:
        self.device_id = self.normalize_device_id(device_id)
        if self.serial.startswith("SIM-"):
            self.serial = f"SIM-{self.device_id}"
        return True

    def set_mode(self, mode: int) -> bool:
        return self.set_mode_with_ack(mode, require_ack=True)

    def set_mode_with_ack(self, mode: int, *, require_ack: bool = True) -> bool:
        self.process_command(f"MODE,YGAS,{self.COMMAND_TARGET_ID},{int(mode)}")
        return True

    def set_comm_way(self, active: bool) -> bool:
        return self.set_comm_way_with_ack(active, require_ack=True)

    def set_comm_way_with_ack(self, active: bool, *, require_ack: bool = True) -> bool:
        self.process_command(f"SETCOMWAY,YGAS,{self.COMMAND_TARGET_ID},{1 if active else 0}")
        return True

    def get_comm_way(self) -> int:
        return int(self._comm_way)

    def set_active_send(self, enabled: bool) -> None:
        self.set_active_send_with_ack(enabled, require_ack=True)

    def set_active_send_with_ack(self, enabled: bool, *, require_ack: bool = True) -> bool:
        self.set_comm_way_with_ack(bool(enabled), require_ack=require_ack)
        return True

    def get_active_send(self) -> bool:
        return bool(self._active_send)

    def set_active_freq(self, hz: int) -> bool:
        return self.set_active_freq_with_ack(hz, require_ack=True)

    def set_active_freq_with_ack(self, hz: int, *, require_ack: bool = True) -> bool:
        self.process_command(f"FTD,YGAS,{self.COMMAND_TARGET_ID},{int(hz)}")
        return True

    def set_ftd(self, hz: int) -> bool:
        return self.set_active_freq_with_ack(hz, require_ack=True)

    def set_ftd_with_ack(self, hz: int, *, require_ack: bool = True) -> bool:
        return self.set_active_freq_with_ack(hz, require_ack=require_ack)

    def set_average_filter_channel(self, channel: int, window_n: int) -> None:
        self.set_average_filter_channel_with_ack(channel, window_n, require_ack=True)

    def set_average_filter_channel_with_ack(
        self,
        channel: int,
        window_n: int,
        *,
        require_ack: bool = True,
    ) -> bool:
        self.process_command(f"AVERAGE{int(channel)},YGAS,{self.COMMAND_TARGET_ID},{int(window_n)}")
        return True

    def set_average_filter(self, window_n: int) -> bool:
        return self.set_average_filter_with_ack(window_n, require_ack=True)

    def set_average_filter_with_ack(self, window_n: int, *, require_ack: bool = True) -> bool:
        self.process_command(f"AVERAGE,YGAS,{self.COMMAND_TARGET_ID},{int(window_n)}")
        return True

    def set_average(self, co2_n: int, h2o_n: int) -> bool:
        return self.set_average_with_ack(co2_n=co2_n, h2o_n=h2o_n, require_ack=True)

    def set_average_with_ack(self, co2_n: int, h2o_n: int, *, require_ack: bool = True) -> bool:
        self.process_command(f"AVERAGE1,YGAS,{self.COMMAND_TARGET_ID},{int(h2o_n)}")
        self.process_command(f"AVERAGE2,YGAS,{self.COMMAND_TARGET_ID},{int(co2_n)}")
        return True

    def set_senco(self, index: int, *coefficients: Any) -> bool:
        values = list(coefficients)
        if len(values) == 1 and isinstance(values[0], (list, tuple)):
            values = list(values[0])
        joined = ",".join(str(item) for item in values)
        self.process_command(f"SENCO{int(index)},YGAS,{self.COMMAND_TARGET_ID},{joined}")
        return True

    def read_coefficient_group(self, index: int, **_: Any) -> dict[str, float]:
        response = self.process_command(f"GETCO,YGAS,{self.COMMAND_TARGET_ID},{int(index)}")
        parsed: dict[str, float] = {}
        for token in str(response or "").split(","):
            if ":" not in token:
                continue
            name, raw_value = token.split(":", 1)
            parsed[str(name).strip()] = _parse_float(raw_value)
        return parsed

    def read_latest_data(
        self,
        *,
        prefer_stream: Optional[bool] = None,
        drain_s: float = 0.35,
        read_timeout_s: float = 0.05,
        allow_passive_fallback: bool = False,
    ) -> str:
        use_stream = self._active_send if prefer_stream is None else bool(prefer_stream)
        if use_stream:
            line = self.read_data_active(drain_s=drain_s, read_timeout_s=read_timeout_s)
            if line or not allow_passive_fallback:
                return line
        return self.read_data_passive()

    def read_data_active(self, *args: Any, **kwargs: Any) -> str:
        if self._mode2_stream in {"active_send_disabled", "no_response"}:
            return ""
        lines = self._drain_stream_lines(*args, **kwargs)
        return lines[-1] if lines else ""

    def read_data_passive(self) -> str:
        if self._mode2_stream == "no_response":
            return ""
        return self._next_frame(passive=True)

    def read_data_active_once(self) -> str:
        return self.read_data_active()

    def _drain_stream_lines(self, drain_s: float = 0.35, read_timeout_s: float = 0.05) -> list[str]:
        del read_timeout_s
        self._fill_active_buffer(drain_s=drain_s)
        lines = list(self._buffer)
        self._buffer.clear()
        return [line for line in lines if str(line or "").strip()]

    def fetch_all(self) -> dict[str, Any]:
        data = self._snapshot()
        return {"data": data}

    def read(self) -> Optional[dict[str, Any]]:
        payload = self.read_latest_data(allow_passive_fallback=True)
        parsed = self.parse_line(payload)
        return parsed if parsed else self.fetch_all()["data"]

    def status(self) -> dict[str, Any]:
        snapshot = self.fetch_all()["data"]
        snapshot.update(
            {
                "ok": self.connected or True,
                "connected": self.connected,
                "mode_requested": self._requested_mode,
                "mode_effective": self._effective_mode,
                "active_send": self._active_send,
                "comm_way": self._comm_way,
                "ftd_hz": self._ftd_hz,
                "mode2_stream": self._mode2_stream,
                "sensor_precheck": self._sensor_precheck,
            }
        )
        return snapshot

    def selftest(self) -> dict[str, Any]:
        return {"ok": self._mode2_stream != "no_response", "connected": self.connected or True}

    def parse_line_mode2(self, line: str) -> Optional[dict[str, Any]]:
        text = str(line or "").strip()
        if not text:
            return None
        parts = [_clean_token(item) for item in text.split(",")]
        if len(parts) < 2 + len(MODE2_KEYS):
            return None
        if "YGAS" not in str(parts[0]).upper():
            return None
        data: dict[str, Any] = {
            "raw": text,
            "id": parts[1] if len(parts) > 1 else None,
            "device_id": parts[1] if len(parts) > 1 else None,
            "mode": 2,
        }
        for index, key in enumerate(MODE2_KEYS, start=2):
            if len(parts) <= index:
                return None
            value = parts[index]
            try:
                data[key] = float(value)
            except Exception:
                return None
        data["status"] = parts[16] if len(parts) > 16 else None
        data["temperature_c"] = data.get("chamber_temp_c")
        data["temp_c"] = data.get("chamber_temp_c")
        data["pressure_hpa"] = None if data.get("pressure_kpa") is None else float(data["pressure_kpa"]) * 10.0
        return data

    def parse_line(self, line: str) -> Optional[dict[str, Any]]:
        parsed = self.parse_line_mode2(line)
        if parsed is not None:
            return parsed
        text = str(line or "").strip()
        if not text:
            return None
        return {"raw": text}

    def _fill_active_buffer(self, *, drain_s: float) -> None:
        if not self._active_send or self._effective_mode != 2:
            return
        now = time.monotonic()
        period_s = 1.0 / max(1, self._ftd_hz)
        emit_count = 1
        if now - self._last_stream_emit_ts > period_s:
            emit_count = max(1, min(4, int((now - self._last_stream_emit_ts) / period_s)))
        emit_count = max(emit_count, int(max(0.0, float(drain_s)) * self._ftd_hz))
        for _ in range(max(1, emit_count)):
            frame = self._next_frame(passive=False)
            if frame:
                self._buffer.append(frame)
        self._last_stream_emit_ts = now

    def _next_frame(self, *, passive: bool) -> str:
        self._frame_index += 1
        behavior = self._frame_behavior(passive=passive)
        if behavior == "no_response":
            return ""
        if behavior == "partial_frame":
            return f"YGAS,{self.device_id},400.0,{self.FRAME_TERMINATOR}"
        if behavior == "truncated_frame":
            snapshot = self._mode2_fields()
            raw = self._format_mode2_frame(snapshot).strip().split(",")
            return ",".join(raw[:-3]) + self.FRAME_TERMINATOR
        if behavior == "corrupted_frame":
            return f"YGAS,{self.device_id},BAD,FRAME,XYZ{self.FRAME_TERMINATOR}"
        if behavior == "buffer_stale":
            if getattr(self, "_stale_frame", ""):
                return self._stale_frame
            frame = self._format_mode2_frame(self._mode2_fields())
            self._stale_frame = frame
            return frame
        return self._format_mode2_frame(self._mode2_fields())

    def _frame_behavior(self, *, passive: bool) -> str:
        stream = self._mode2_stream
        if stream == "stable" or stream == "continuous_ok":
            return "stable"
        if stream == "mode_switch_not_applied":
            return "no_response" if self._effective_mode != 2 else "stable"
        if stream == "active_send_disabled":
            return "no_response" if not passive else "stable"
        if stream == "sensor_precheck_fail":
            return "partial_frame"
        if stream == "sensor_precheck_relaxed_allows_entry":
            return "partial_frame" if (self._frame_index % 3) != 0 else "stable"
        if stream in {"partial_frame", "truncated_frame", "corrupted_frame", "buffer_stale", "no_response"}:
            if self._sensor_precheck == "relaxed_pass" and (self._frame_index % 3) == 0:
                return "stable"
            if self._sensor_precheck == "strict_pass" and self._frame_index > 2 and stream != "no_response":
                return "stable"
            return stream
        return "stable"

    def _snapshot(self) -> dict[str, Any]:
        if self.plant_state is not None and hasattr(self.plant_state, "analyzer_snapshot"):
            base = dict(self.plant_state.analyzer_snapshot())
        else:
            base = {
                "route": "ambient",
                "co2_ppm": self._co2_signal,
                "h2o_mmol": self._h2o_signal,
                "co2_signal": self._co2_signal,
                "h2o_signal": self._h2o_signal,
                "co2_ratio_f": self._co2_signal / 1000.0,
                "co2_ratio_raw": self._co2_signal / 1000.0,
                "h2o_ratio_f": self._h2o_signal / 100.0,
                "h2o_ratio_raw": self._h2o_signal / 100.0,
                "chamber_temp_c": 25.0,
                "case_temp_c": 25.0,
                "temperature_c": 25.0,
                "temp_c": 25.0,
                "pressure_hpa": 1000.0,
                "pressure_kpa": 100.0,
                "humidity_pct": 35.0,
                "dewpoint_c": 5.0,
            }
        analyzer_index = self._analyzer_index()
        co2_offset = float((analyzer_index - 1) * 2.0)
        h2o_offset = float((analyzer_index - 1) * 0.15)
        co2_ppm = float(base.get("co2_ppm", self._co2_signal)) + co2_offset
        h2o_mmol = max(0.0, float(base.get("h2o_mmol", self._h2o_signal)) + h2o_offset)
        chamber_temp_c = float(
            self._chamber_temp_override
            if self._chamber_temp_override is not None
            else base.get("chamber_temp_c", base.get("temperature_c", 25.0))
        )
        case_temp_c = float(
            self._case_temp_override
            if self._case_temp_override is not None
            else base.get("case_temp_c", chamber_temp_c)
        )
        pressure_hpa = float(
            self._pressure_hpa_override
            if self._pressure_hpa_override is not None
            else base.get("pressure_hpa", 1000.0)
        )
        data = {
            "route": base.get("route", "ambient"),
            "device_id": self.device_id,
            "serial": self.serial,
            "software_version": self.software_version,
            "mode": self._effective_mode,
            "co2_ppm": co2_ppm,
            "h2o_mmol": h2o_mmol,
            "co2_density": co2_ppm * 1.1,
            "h2o_density": h2o_mmol * 1.05,
            "co2_ratio_f": co2_ppm / 1000.0,
            "co2_ratio_raw": co2_ppm / 1000.0,
            "h2o_ratio_f": h2o_mmol / 100.0,
            "h2o_ratio_raw": h2o_mmol / 100.0,
            "ref_signal": 1000.0 + analyzer_index,
            "co2_signal": float(base.get("co2_signal", co2_ppm)) + co2_offset,
            "h2o_signal": float(base.get("h2o_signal", h2o_mmol)) + h2o_offset,
            "temperature_c": chamber_temp_c,
            "temp_c": chamber_temp_c,
            "chamber_temp_c": chamber_temp_c,
            "case_temp_c": case_temp_c,
            "pressure_hpa": pressure_hpa,
            "pressure_kpa": pressure_hpa / 10.0,
            "humidity_pct": float(base.get("humidity_pct", 35.0)),
            "dewpoint_c": float(base.get("dewpoint_c", 5.0)),
            "status": self._status_bits,
            "active_send": self._active_send,
            "ftd_hz": self._ftd_hz,
            "average_filter": self._average_filter,
            "average_co2": self._average_co2,
            "average_h2o": self._average_h2o,
        }
        return data

    def _mode2_fields(self) -> dict[str, Any]:
        snapshot = self._snapshot()
        return {key: snapshot[key] for key in MODE2_KEYS} | {"status": snapshot["status"]}

    def _format_mode2_frame(self, data: dict[str, Any]) -> str:
        values = ["YGAS", self.device_id]
        for key in MODE2_KEYS:
            values.append(f"{float(data[key]):.5f}".rstrip("0").rstrip("."))
        values.append(str(data.get("status", self._status_bits)))
        return ",".join(values) + self.FRAME_TERMINATOR

    def _analyzer_index(self) -> int:
        if self.device_id.isdigit():
            return max(1, int(self.device_id))
        try:
            return max(1, int(self.device_id, 16))
        except Exception:
            return 1
