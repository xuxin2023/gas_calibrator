from __future__ import annotations

from collections import deque
from typing import Any, Optional


UNIT_SCALE_TO_HPA = {
    "HPA": 1.0,
    "PA": 0.01,
    "KPA": 10.0,
    "BAR": 1000.0,
    "MBAR": 1.0,
    "PSIA": 68.9475729318,
    "PSIG": 68.9475729318,
}


class ParoscientificPressureGaugeFake:
    """Protocol-level Paroscientific 735/745 style fake."""

    FRAME_TERMINATOR = "\r\n"
    VALID_MODES = {
        "stable",
        "continuous_stream",
        "sample_hold",
        "unit_switch",
        "no_response",
        "unsupported_command",
        "display_interrupted",
        "wrong_unit_configuration",
    }
    VALID_COMMANDS = {"P1", "P3", "P4", "P5", "P7", "Q1", "Q3", "Q4", "DB", "EW", "UN", "TU", "MD"}

    def __init__(
        self,
        port: str = "SIM-PARO",
        *,
        baudrate: int = 9600,
        timeout: float = 1.0,
        parity: str = "N",
        stopbits: float = 1.0,
        bytesize: int = 8,
        dest_id: str = "01",
        source_id: str = "00",
        response_timeout_s: Optional[float] = None,
        io_logger: Optional[Any] = None,
        plant_state: Optional[Any] = None,
        mode: str = "stable",
        unit: str = "HPA",
        temperature_unit: str = "C",
        measurement_mode: str = "single",
        faults: Optional[list[dict[str, Any]]] = None,
        unsupported_commands: Optional[list[str]] = None,
        **_: Any,
    ) -> None:
        self.port = str(port or "SIM-PAROSCIENTIFIC")
        self.baudrate = int(baudrate or 9600)
        self.timeout = float(timeout if timeout is not None else 1.0)
        self.parity = str(parity or "N")
        self.stopbits = float(stopbits if stopbits is not None else 1.0)
        self.bytesize = int(bytesize or 8)
        self.dest_id = str(dest_id or "01").zfill(2)
        self.source_id = str(source_id or "00").zfill(2)
        self.response_timeout_s = float(response_timeout_s or max(1.2, self.timeout))
        self.io_logger = io_logger
        self.plant_state = plant_state
        self.connected = False
        self.mode = self._normalize_mode(mode)
        self.unit = self._normalize_unit(unit)
        self.temperature_unit = self._normalize_temperature_unit(temperature_unit)
        default_measurement_mode = measurement_mode
        if self.mode == "sample_hold":
            default_measurement_mode = "sample_hold"
        elif self.mode == "continuous_stream":
            default_measurement_mode = "continuous"
        self.measurement_mode = self._normalize_measurement_mode(default_measurement_mode)
        if self.mode == "wrong_unit_configuration":
            self.unit = "PSIA"
        if self.plant_state is not None:
            setattr(self.plant_state, "dynamic_protocol", True)
        self._faults = list(faults or [])
        self._unsupported_commands = {str(item or "").strip().upper() for item in list(unsupported_commands or []) if str(item or "").strip()}
        self._write_enabled = False
        self._held_pressure_hpa: Optional[float] = None
        self._response_queue: deque[str] = deque()

    def open(self) -> None:
        self.connected = True

    def connect(self) -> None:
        self.connected = True

    def close(self) -> None:
        self.connected = False
        self._response_queue.clear()

    def write(self, data: str) -> None:
        text = str(data or "")
        if not text:
            return
        if self.mode == "no_response":
            return
        response = self.handle_command(text)
        self._response_queue.append(text.strip())
        if response:
            self._response_queue.extend(line for line in response.splitlines() if line.strip())

    def readline(self) -> str:
        if self.mode == "no_response":
            return ""
        if self._response_queue:
            return f"{self._response_queue.popleft()}{self.FRAME_TERMINATOR}"
        if self.measurement_mode == "continuous":
            return self._measurement_frame(command="P3")
        return ""

    def read_available(self) -> str:
        lines = [self.readline() for _ in range(3)]
        return "".join(item for item in lines if item)

    def read_pressure(self) -> float:
        command = self._cmd("P3")
        self.write(command)
        echoed = command.strip().upper()
        attempts = 0
        while attempts < 6:
            attempts += 1
            raw = self.readline()
            text = str(raw or "").strip()
            if not text:
                continue
            if text.upper() == echoed:
                continue
            value = self._parse_measurement_value(text)
            if value is not None:
                return float(value)
        raise RuntimeError("NO_RESPONSE")

    def read_pressure_hpa(self) -> float:
        return self.read_pressure()

    def read(self) -> float:
        return self.read_pressure()

    def status(self) -> dict[str, Any]:
        reference_status = self.reference_status()
        payload = {
            "connected": self.connected,
            "port": self.port,
            "pressure_gauge_hpa": None,
            "pressure_hpa": None,
            "unit": self.unit,
            "temperature_unit": self.temperature_unit,
            "measurement_mode": self.measurement_mode,
            "pressure_reference_status": reference_status,
            "protocol_profile": "paroscientific_735_745",
            "dest_id": self.dest_id,
            "source_id": self.source_id,
            "serial_profile": {
                "baudrate": self.baudrate,
                "bytesize": self.bytesize,
                "stopbits": self.stopbits,
                "parity": self.parity,
                "framing": "8N1",
            },
        }
        pressure_hpa = None
        if reference_status not in {"no_response", "unsupported_command", "display_interrupted"}:
            try:
                pressure_hpa = self._measurement_hpa()
            except Exception:
                pressure_hpa = None
        if pressure_hpa is not None:
            payload["pressure_gauge_hpa"] = float(pressure_hpa)
            payload["pressure_hpa"] = float(pressure_hpa)
        return payload

    def selftest(self) -> dict[str, Any]:
        payload = self.status()
        payload["status"] = self.reference_status()
        return payload

    def handle_command(self, frame: str) -> str:
        target_id, _source_id, command, args = self._parse_frame(frame)
        if not self._accepts_target(target_id):
            return ""
        if self.mode == "unsupported_command":
            return self._response("ERR:UNSUPPORTED_COMMAND")
        if command in self._unsupported_commands:
            return self._response("ERR:UNSUPPORTED_COMMAND")
        if command not in self.VALID_COMMANDS:
            return self._response("ERR:UNKNOWN_COMMAND")
        if self.mode == "display_interrupted" and command in {"P1", "P3", "P4", "P5", "P7"}:
            return self._response("DISPLAY_INTERRUPTED")
        if command == "DB":
            return self._response("PAROSCIENTIFIC,745,FAKE")
        if command == "EW":
            self._write_enabled = True
            return self._response("EW,OK")
        if command == "UN":
            return self._handle_unit_command(args)
        if command == "TU":
            return self._handle_temperature_unit_command(args)
        if command == "MD":
            return self._handle_mode_command(args)
        if command in {"P1", "P3", "P4", "P5", "P7"}:
            return self._measurement_frame(command=command)
        if command == "Q1":
            return self._response(f"UN={self.unit}")
        if command == "Q3":
            return self._response(f"MD={self.measurement_mode.upper()}")
        if command == "Q4":
            return self._response(f"TU={self.temperature_unit}")
        return self._response("ERR:UNKNOWN_COMMAND")

    def reference_status(self) -> str:
        if self.mode in {"stable", "continuous_stream", "sample_hold", "unit_switch"}:
            return "healthy"
        return self.mode

    def _handle_unit_command(self, args: str) -> str:
        if args:
            if not self._write_enabled:
                return self._response("ERR:EW_REQUIRED")
            self.unit = self._normalize_unit(args)
            self._write_enabled = False
        return self._response(self.unit)

    def _handle_temperature_unit_command(self, args: str) -> str:
        if args:
            if not self._write_enabled:
                return self._response("ERR:EW_REQUIRED")
            self.temperature_unit = self._normalize_temperature_unit(args)
            self._write_enabled = False
        return self._response(self.temperature_unit)

    def _handle_mode_command(self, args: str) -> str:
        if args:
            if not self._write_enabled:
                return self._response("ERR:EW_REQUIRED")
            self.measurement_mode = self._normalize_measurement_mode(args)
            self._write_enabled = False
        return self._response(self.measurement_mode.upper())

    def _measurement_frame(self, *, command: str) -> str:
        value_hpa = self._measurement_hpa()
        value_in_unit = self._convert_hpa_to_unit(value_hpa, self.unit)
        suffix = self.unit
        if command == "P4":
            suffix = f"{suffix},HOLD={self.measurement_mode == 'sample_hold'}"
        elif command == "P7":
            suffix = f"{suffix},TU={self.temperature_unit}"
        elif command == "P5":
            suffix = f"{suffix},MD={self.measurement_mode.upper()}"
        return self._response(f"{value_in_unit:.3f},{suffix}")

    def _measurement_hpa(self) -> float:
        if self.plant_state is not None:
            current = float(getattr(self.plant_state, "pressure_hpa", 1000.0))
        else:
            current = 1000.0
        if self.measurement_mode == "sample_hold":
            if self._held_pressure_hpa is None:
                self._held_pressure_hpa = current
            return float(self._held_pressure_hpa)
        self._held_pressure_hpa = None
        return float(current)

    def _parse_frame(self, frame: str) -> tuple[str, str, str, str]:
        text = str(frame or "").strip()
        if not text.startswith("*") or len(text) < 5:
            raise ValueError(f"invalid Paroscientific frame: {frame!r}")
        target_id = text[1:3]
        source_id = text[3:5]
        command_text = text[5:]
        command = command_text[:2].upper()
        args = command_text[2:].strip()
        return target_id, source_id, command, args

    def _accepts_target(self, target_id: str) -> bool:
        text = str(target_id or "").zfill(2)
        return text in {self.dest_id, "99"}

    def _cmd(self, command: str) -> str:
        return f"*{self.dest_id}{self.source_id}{str(command).strip()}{self.FRAME_TERMINATOR}"

    def _response(self, payload: str) -> str:
        return f"*{self.dest_id}{self.source_id}{payload}{self.FRAME_TERMINATOR}"

    @staticmethod
    def _normalize_mode(mode: Any) -> str:
        text = str(mode or "stable").strip().lower()
        return text if text in ParoscientificPressureGaugeFake.VALID_MODES else "stable"

    @staticmethod
    def _normalize_unit(unit: Any) -> str:
        text = str(unit or "HPA").strip().upper()
        return text if text in UNIT_SCALE_TO_HPA else "HPA"

    @staticmethod
    def _normalize_temperature_unit(unit: Any) -> str:
        text = str(unit or "C").strip().upper()
        return text if text in {"C", "F", "K"} else "C"

    @staticmethod
    def _normalize_measurement_mode(value: Any) -> str:
        text = str(value or "single").strip().lower().replace("-", "_")
        aliases = {
            "s": "single",
            "single": "single",
            "single_measurement": "single",
            "c": "continuous",
            "continuous": "continuous",
            "continuous_stream": "continuous",
            "h": "sample_hold",
            "hold": "sample_hold",
            "sample_hold": "sample_hold",
            "sample_and_hold": "sample_hold",
        }
        return aliases.get(text, "single")

    @staticmethod
    def _convert_hpa_to_unit(value_hpa: float, unit: str) -> float:
        scale = UNIT_SCALE_TO_HPA.get(str(unit or "HPA").strip().upper(), 1.0)
        return float(value_hpa) / float(scale)

    @staticmethod
    def _convert_unit_to_hpa(value: float, unit: str) -> float:
        scale = UNIT_SCALE_TO_HPA.get(str(unit or "HPA").strip().upper(), 1.0)
        return float(value) * float(scale)

    def _parse_measurement_value(self, response: str) -> Optional[float]:
        text = str(response or "").strip()
        if not text or not text.startswith("*") or len(text) <= 5:
            return None
        payload = text[5:]
        number_text = payload.split(",", 1)[0].strip()
        if not number_text:
            return None
        try:
            numeric = float(number_text)
        except Exception:
            return None
        return self._convert_unit_to_hpa(numeric, self.unit)
