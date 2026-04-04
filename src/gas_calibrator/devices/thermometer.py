"""Digital thermometer serial driver."""

from __future__ import annotations

import re
from typing import Any, Dict, Optional

from .serial_base import SerialDevice


class Thermometer:
    """Thermometer protocol wrapper."""

    LATEST_FRAME_DRAIN_S = 0.8
    LATEST_FRAME_READ_TIMEOUT_S = 0.01

    def __init__(
        self,
        port: str,
        baudrate: int = 2400,
        timeout: float = 1.0,
        parity: str = "N",
        stopbits: float = 1,
        bytesize: int = 8,
        io_logger: Optional[Any] = None,
        serial_factory: Optional[Any] = None,
    ):
        self.ser = SerialDevice(
            port,
            baudrate=baudrate,
            timeout=timeout,
            parity=parity,
            stopbits=stopbits,
            bytesize=bytesize,
            device_name="thermometer",
            io_logger=io_logger,
            serial_factory=serial_factory,
        )

    def open(self) -> None:
        self.ser.open()

    def connect(self) -> None:
        self.open()

    def close(self) -> None:
        self.ser.close()

    def write(self, data: str) -> None:
        self.ser.write(data)

    def flush_input(self) -> None:
        self.ser.flush_input()

    @staticmethod
    def parse_line(line: str) -> Dict[str, Any]:
        raw = (line or "").strip()
        out: Dict[str, Any] = {"raw": raw, "ok": False, "temp_c": None}
        if not raw:
            return out

        m = re.search(r"([+-]?\d+(?:\.\d+)?)", raw)
        if not m:
            return out

        try:
            out["temp_c"] = float(m.group(1))
            out["ok"] = True
        except Exception:
            pass
        return out

    def _latest_valid_from_lines(self, lines: list[str]) -> Dict[str, Any] | None:
        for line in reversed(list(lines or [])):
            parsed = self.parse_line(line)
            if parsed.get("ok"):
                return parsed
        return None

    def read_current(self) -> Dict[str, Any]:
        drain = getattr(self.ser, "drain_input_nonblock", None)
        if callable(drain):
            drained = drain(
                drain_s=float(self.LATEST_FRAME_DRAIN_S),
                read_timeout_s=float(self.LATEST_FRAME_READ_TIMEOUT_S),
            )
            latest = self._latest_valid_from_lines(drained)
            if latest is not None:
                return latest

        line = self.ser.readline()
        parsed = self.parse_line(line)
        if parsed.get("ok"):
            return parsed

        # Some units stream frames without newline; pull buffered bytes as fallback.
        raw = self.ser.read_available()
        if raw:
            lines = [seg.strip() for seg in raw.replace("\r", "\n").split("\n") if seg.strip()]
            if lines:
                parsed2 = self._latest_valid_from_lines(lines) or self.parse_line(lines[-1])
                if parsed2.get("ok"):
                    return parsed2

        return parsed

    def read_temp_c(self) -> Optional[float]:
        data = self.read_current()
        return data.get("temp_c")

    def read(self) -> Dict[str, Any]:
        return self.read_current()

    def status(self) -> Dict[str, Any]:
        return self.read_current()

    def selftest(self) -> Dict[str, Any]:
        return self.status()
