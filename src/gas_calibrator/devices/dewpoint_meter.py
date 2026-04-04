"""Dewpoint meter serial driver."""

from __future__ import annotations

import time
from typing import Any, Dict, List, Optional

from .serial_base import SerialDevice


class DewpointMeter:
    """Dewpoint meter protocol wrapper."""

    def __init__(
        self,
        port: str,
        baudrate: int = 115200,
        timeout: float = 1.0,
        station: str = "001",
        io_logger: Optional[Any] = None,
        serial_factory: Optional[Any] = None,
    ):
        self.ser = SerialDevice(
            port,
            baudrate=baudrate,
            timeout=timeout,
            device_name="dewpoint_meter",
            io_logger=io_logger,
            serial_factory=serial_factory,
        )
        self.station = station

    def open(self) -> None:
        self.ser.open()

    def connect(self) -> None:
        self.open()

    def close(self) -> None:
        self.ser.close()

    def write(self, data: str) -> None:
        self.ser.write(data)

    def _build_get_current_cmd_variants(self) -> List[str]:
        base = f"{self.station}_GetCurData_END"
        return [base + "\r\n", base + "\n", base + "\r", base]

    @staticmethod
    def _to_float(value: str) -> Optional[float]:
        try:
            return float(value)
        except Exception:
            return None

    @staticmethod
    def _to_bool(value: str) -> Optional[bool]:
        up = str(value).strip().upper()
        if up == "TRUE":
            return True
        if up == "FALSE":
            return False
        return None

    def parse_response(self, resp: str) -> Dict[str, Any]:
        frame = (resp or "").strip()
        out: Dict[str, Any] = {"raw": frame}
        if not frame:
            return out

        parts = frame.split("_")
        if len(parts) < 3:
            return out

        out["station"] = parts[0]
        out["cmd"] = parts[1]
        payload = parts[2:-1] if parts[-1].upper() == "END" else parts[2:]
        out["payload"] = payload

        for i, token in enumerate(payload, start=1):
            value = self._to_float(token)
            out[f"value_{i}"] = value if value is not None else token

        if len(payload) > 0:
            out["dewpoint_c"] = self._to_float(payload[0])
        if len(payload) > 1:
            out["temp_c"] = self._to_float(payload[1])
        if len(payload) > 7:
            out["rh_pct"] = self._to_float(payload[7])

        flags_raw = payload[8:12] if len(payload) >= 12 else []
        flags = [self._to_bool(v) for v in flags_raw]
        if flags:
            out["flags"] = flags
            out["flags_raw"] = flags_raw

        return out

    def get_current(self, timeout_s: float = 2.0, attempts: int = 2) -> Dict[str, Any]:
        cmd_variants = self._build_get_current_cmd_variants()
        all_lines: List[str] = []
        frame = ""
        cmd_used = ""

        for _ in range(max(1, attempts)):
            for cmd in cmd_variants:
                try:
                    self.ser.flush_input()
                except Exception:
                    pass

                self.ser.write(cmd)
                cmd_used = cmd
                deadline = time.time() + max(0.2, timeout_s)
                lines: List[str] = []

                while time.time() < deadline:
                    line = self.ser.readline().strip()
                    if not line:
                        raw = self.ser.read_available().strip()
                        if raw:
                            lines.append(raw)
                            if "_GetCurData_" in raw and "_END" in raw:
                                frame = raw
                                break
                        continue

                    lines.append(line)
                    if "_GetCurData_" in line and "_END" in line:
                        frame = line
                        break

                all_lines.extend(lines)
                if frame:
                    break
            if frame:
                break

        if not frame:
            for line in reversed(all_lines):
                if "_GetCurData_" in line:
                    frame = line
                    break

        out = self.parse_response(frame)
        out["ok"] = bool(out.get("raw"))
        out["cmd"] = cmd_used.strip()
        out["lines"] = all_lines
        return out

    def get_current_fast(self, timeout_s: float = 0.35, clear_buffer: bool = False) -> Dict[str, Any]:
        cmd = self._build_get_current_cmd_variants()[0]
        all_lines: List[str] = []
        frame = ""

        exchange_readlines = getattr(self.ser, "exchange_readlines", None)
        if callable(exchange_readlines):
            try:
                all_lines = list(
                    exchange_readlines(
                        cmd,
                        response_timeout_s=max(0.05, float(timeout_s)),
                        read_timeout_s=min(0.05, max(0.01, float(timeout_s) / 3.0)),
                        clear_input=bool(clear_buffer),
                    )
                    or []
                )
            except Exception:
                all_lines = []
            for line in all_lines:
                text = (line or "").strip()
                if "_GetCurData_" in text and "_END" in text:
                    frame = text
                    break
        else:
            if clear_buffer:
                try:
                    self.ser.flush_input()
                except Exception:
                    pass
            self.ser.write(cmd)
            deadline = time.time() + max(0.05, float(timeout_s))
            while time.time() < deadline:
                line = self.ser.readline().strip()
                if not line:
                    continue
                all_lines.append(line)
                if "_GetCurData_" in line and "_END" in line:
                    frame = line
                    break

        out = self.parse_response(frame)
        out["ok"] = bool(out.get("raw"))
        out["cmd"] = cmd.strip()
        out["lines"] = all_lines
        return out

    def read(self) -> Dict[str, Any]:
        return self.get_current()

    def status(self) -> Dict[str, Any]:
        data = self.get_current()
        return {
            "ok": bool(data.get("ok")),
            "station": data.get("station", self.station),
            "dewpoint_c": data.get("dewpoint_c"),
            "temp_c": data.get("temp_c"),
            "rh_pct": data.get("rh_pct"),
            "raw": data.get("raw"),
        }

    def selftest(self) -> Dict[str, Any]:
        return self.status()
