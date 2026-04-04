from __future__ import annotations

import math
from collections import deque
from typing import Any, Optional


class ThermometerFake:
    """Protocol-level continuous stream fake for the RCY-A thermometer."""

    FRAME_TERMINATOR = "\r\n"
    UNIT_SUFFIX = "\N{DEGREE SIGN}C"
    VALID_MODES = {
        "stable",
        "drift",
        "stale",
        "no_response",
        "warmup_unstable",
        "plus_200_mode",
        "skipped_by_profile",
        "corrupted_ascii",
        "truncated_ascii",
    }
    WARMUP_FRAME_COUNT = 4

    def __init__(
        self,
        port: str = "SIM-THERMOMETER",
        *,
        baudrate: int = 2400,
        timeout: float = 1.0,
        parity: str = "N",
        stopbits: float = 1.0,
        bytesize: int = 8,
        io_logger: Optional[Any] = None,
        plant_state: Optional[Any] = None,
        mode: str = "stable",
        temperature_c: float = 25.0,
        drift_step_c: float = 0.05,
        plus_200_mode: bool = False,
        skipped_by_profile: bool = False,
        **_: Any,
    ) -> None:
        self.port = str(port or "SIM-THERMOMETER")
        self.baudrate = int(baudrate or 2400)
        self.timeout = float(timeout if timeout is not None else 1.0)
        self.parity = str(parity or "N")
        self.stopbits = float(stopbits if stopbits is not None else 1.0)
        self.bytesize = int(bytesize or 8)
        self.io_logger = io_logger
        self.plant_state = plant_state
        if self.plant_state is not None:
            setattr(self.plant_state, "dynamic_protocol", True)
        self.connected = False
        self.mode = self._normalize_mode(mode)
        self.plus_200_mode = bool(plus_200_mode or self.mode == "plus_200_mode")
        self.skipped_by_profile = bool(skipped_by_profile or self.mode == "skipped_by_profile")
        self.current_temp_c = float(temperature_c)
        self._stale_temp_c = float(temperature_c)
        self.drift_step_c = float(drift_step_c if drift_step_c is not None else 0.05)
        self._buffer: deque[str] = deque(maxlen=16)
        self._frame_index = 0
        self._warmup_frames_remaining = self.WARMUP_FRAME_COUNT
        self._last_good_temp_c: Optional[float] = None

    def open(self) -> None:
        self.connected = True

    def connect(self) -> bool:
        self.connected = True
        return True

    def close(self) -> None:
        self.connected = False
        self._buffer.clear()

    def readline(self) -> str:
        line = self._emit_frame()
        if line:
            self._buffer.append(line)
        return line

    def read_available(self) -> str:
        return "".join(self._emit_frames(3))

    def drain_input_nonblock(self, *, drain_s: float = 0.0, read_timeout_s: float = 0.0) -> list[str]:
        del drain_s, read_timeout_s
        return self._emit_frames(3)

    def flush_input(self) -> None:
        self._buffer.clear()

    def read_current(self) -> dict[str, Any]:
        first = self.parse_line(self.readline())
        if first.get("ok"):
            return first
        for line in self._emit_frames(3):
            parsed = self.parse_line(line)
            if parsed.get("ok"):
                return parsed
        return first

    def read_temp_c(self) -> Optional[float]:
        payload = self.read_current()
        return payload.get("temp_c")

    def read(self) -> dict[str, Any]:
        return self.read_current()

    def status(self) -> dict[str, Any]:
        payload = self.read_current()
        payload.update(
            {
                "connected": self.connected,
                "port": self.port,
                "mode": self.mode,
                "skipped_by_profile": self.skipped_by_profile,
                "thermometer_reference_status": self.reference_status(),
                "serial_profile": {
                    "baudrate": self.baudrate,
                    "bytesize": self.bytesize,
                    "stopbits": self.stopbits,
                    "parity": self.parity,
                    "stream": "continuous_ascii",
                },
            }
        )
        return payload

    def selftest(self) -> dict[str, Any]:
        payload = self.status()
        payload["status"] = self.reference_status()
        return payload

    def reference_status(self) -> str:
        if self.skipped_by_profile:
            return "skipped_by_profile"
        if self.mode == "plus_200_mode":
            return "healthy"
        if self.mode in {"stable", "drift", "stale", "no_response", "warmup_unstable", "corrupted_ascii", "truncated_ascii"}:
            return self.mode if self.mode != "stable" else "healthy"
        return self.mode

    def _emit_frames(self, count: int) -> list[str]:
        frames: list[str] = []
        for _ in range(max(0, int(count))):
            line = self._emit_frame()
            if not line:
                continue
            self._buffer.append(line)
            frames.append(line)
        return frames

    def _emit_frame(self) -> str:
        if self.mode == "no_response":
            return ""
        self._frame_index += 1
        temp_c = self._next_temperature_c()
        encoded = temp_c + 200.0 if self.plus_200_mode else temp_c
        if self.mode == "corrupted_ascii":
            return f"@{encoded:+06.1f}??{self.UNIT_SUFFIX}{self.FRAME_TERMINATOR}"
        if self.mode == "truncated_ascii":
            return f"{encoded:+07.2f}{self.UNIT_SUFFIX[:-1]}\r"
        return self._format_frame(encoded)

    def _next_temperature_c(self) -> float:
        if self.mode == "stale":
            return float(self._stale_temp_c)
        if self.plant_state is not None and self.mode not in {"drift", "plus_200_mode"}:
            try:
                temp = float(getattr(self.plant_state, "temperature_c", self.current_temp_c))
            except Exception:
                temp = float(self.current_temp_c)
        else:
            temp = float(self.current_temp_c)
        if self.mode == "drift":
            self.current_temp_c = float(self.current_temp_c) + float(self.drift_step_c)
            temp = float(self.current_temp_c)
        elif self.mode == "warmup_unstable" and self._warmup_frames_remaining > 0:
            offset = 3.0 if self._warmup_frames_remaining % 2 else -2.2
            temp = float(temp) + offset
            self._warmup_frames_remaining -= 1
        else:
            self.current_temp_c = temp
        self._stale_temp_c = temp if self.mode != "warmup_unstable" or self._warmup_frames_remaining <= 0 else self._stale_temp_c
        return temp

    @classmethod
    def _format_frame(cls, value: float) -> str:
        clipped = max(-999.99, min(999.99, float(value)))
        return f"{clipped:+07.2f}{cls.UNIT_SUFFIX}{cls.FRAME_TERMINATOR}"

    @staticmethod
    def _normalize_mode(mode: Any) -> str:
        text = str(mode or "stable").strip().lower()
        return text if text in ThermometerFake.VALID_MODES else "stable"

    def parse_line(self, line: str) -> dict[str, Any]:
        raw = str(line or "")
        payload = {
            "raw": raw.strip(),
            "ok": False,
            "temp_c": None,
            "thermometer_reference_status": self.reference_status(),
        }
        text = raw.strip()
        if not text:
            return payload
        if text.endswith(self.UNIT_SUFFIX):
            text = text[: -len(self.UNIT_SUFFIX)]
        elif text.endswith("C"):
            text = text[:-1]
        text = text.strip()
        try:
            temp_c = float(text)
        except Exception:
            return payload
        if self.plus_200_mode and math.isfinite(temp_c) and temp_c >= 200.0:
            temp_c -= 200.0
        payload["temp_c"] = float(temp_c)
        payload["ok"] = True
        self._last_good_temp_c = float(temp_c)
        return payload
