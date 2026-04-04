"""Paroscientific pressure gauge driver."""

from __future__ import annotations

import re
import threading
import time
from typing import Any, Optional

from .serial_base import SerialDevice


class ParoscientificGauge:
    """Paroscientific protocol wrapper."""

    def __init__(
        self,
        port: str,
        baudrate: int = 9600,
        timeout: float = 1.0,
        dest_id: str = "01",
        response_timeout_s: Optional[float] = None,
        io_logger: Optional[Any] = None,
        serial_factory: Optional[Any] = None,
    ):
        self.ser = SerialDevice(
            port,
            baudrate=baudrate,
            timeout=timeout,
            device_name="paroscientific_gauge",
            io_logger=io_logger,
            serial_factory=serial_factory,
        )
        self.dest_id = dest_id
        self.response_timeout_s = float(response_timeout_s or max(1.2, timeout))
        self._query_lock = threading.RLock()
        self._continuous_pressure_mode: str = ""

    def open(self) -> None:
        self.ser.open()

    def connect(self) -> None:
        self.open()

    def close(self) -> None:
        self.ser.close()

    def write(self, data: str) -> None:
        self.ser.write(data)

    def _cmd(self, cmd: str) -> str:
        return f"*{self.dest_id}00{cmd}\r\n"

    @staticmethod
    def _parse_pressure_value(resp: str) -> Optional[float]:
        text = (resp or "").strip()
        if not text:
            return None
        if text.startswith("*") and len(text) > 5:
            try:
                return float(text[5:].strip())
            except Exception:
                pass
        m = re.search(r"[-+]?\d+(?:\.\d+)?(?:[eE][-+]?\d+)?", text)
        if not m:
            return None
        try:
            return float(m.group(0))
        except Exception:
            return None

    @classmethod
    def _parse_pressure_lines(cls, responses: list[str], *, cmd_echo: str = "") -> Optional[float]:
        for resp in responses:
            text = (resp or "").strip()
            if not text:
                continue
            if cmd_echo and text.upper() == cmd_echo:
                continue
            value = cls._parse_pressure_value(text)
            if value is not None:
                return value
        return None

    @classmethod
    def _parse_latest_pressure_lines(cls, responses: list[str], *, cmd_echo: str = "") -> Optional[float]:
        for resp in reversed(list(responses or [])):
            text = (resp or "").strip()
            if not text:
                continue
            if cmd_echo and text.upper() == cmd_echo:
                continue
            value = cls._parse_pressure_value(text)
            if value is not None:
                return value
        return None

    def _read_pressure_query_locked(self, *, cmd: str, timeout_s: float, clear_buffer: bool) -> float:
        cmd_echo = cmd.strip().upper()
        exchange_readlines = getattr(self.ser, "exchange_readlines", None)
        if callable(exchange_readlines):
            lines = exchange_readlines(
                cmd,
                response_timeout_s=timeout_s,
                read_timeout_s=min(0.1, timeout_s),
                clear_input=bool(clear_buffer),
            )
            value = self._parse_pressure_lines(list(lines or []), cmd_echo=cmd_echo)
            if value is not None:
                return value
            raise RuntimeError("NO_RESPONSE")

        if clear_buffer:
            reset_input_buffer = getattr(self.ser, "reset_input_buffer", None)
            if callable(reset_input_buffer):
                reset_input_buffer()
        self.ser.write(cmd)
        deadline = time.time() + timeout_s
        while time.time() < deadline:
            resp = self.ser.readline()
            value = self._parse_pressure_lines([(resp or "").strip()], cmd_echo=cmd_echo)
            if value is not None:
                return value
        raise RuntimeError("NO_RESPONSE")

    def read_pressure(
        self,
        *,
        response_timeout_s: Optional[float] = None,
        retries: int = 3,
        retry_sleep_s: float = 0.1,
        clear_buffer: bool = False,
    ) -> float:
        last_exc: Optional[Exception] = None
        cmd = self._cmd("P3")
        timeout_s = float(response_timeout_s or self.response_timeout_s)
        attempts = max(1, int(retries or 0))

        with self._query_lock:
            for idx in range(attempts):
                try:
                    return self._read_pressure_query_locked(
                        cmd=cmd,
                        timeout_s=timeout_s,
                        clear_buffer=bool(clear_buffer and idx == 0),
                    )
                except Exception as exc:
                    last_exc = exc
                    if idx + 1 < attempts and retry_sleep_s > 0:
                        time.sleep(float(retry_sleep_s))

        if last_exc:
            raise last_exc
        raise RuntimeError("NO_RESPONSE")

    def read_pressure_fast(
        self,
        *,
        response_timeout_s: Optional[float] = None,
        retries: int = 1,
        retry_sleep_s: float = 0.0,
        clear_buffer: bool = False,
        buffered_drain_s: float = 0.08,
    ) -> float:
        last_exc: Optional[Exception] = None
        cmd = self._cmd("P3")
        timeout_s = float(response_timeout_s or self.response_timeout_s)
        attempts = max(1, int(retries or 0))

        with self._query_lock:
            drain_input_nonblock = getattr(self.ser, "drain_input_nonblock", None)
            for idx in range(attempts):
                try:
                    if callable(drain_input_nonblock) and buffered_drain_s > 0:
                        drained = drain_input_nonblock(
                            drain_s=max(0.01, float(buffered_drain_s)),
                            read_timeout_s=min(0.05, max(0.01, timeout_s / 4.0)),
                        )
                        value = self._parse_pressure_lines(list(drained or []))
                        if value is not None:
                            return value
                    return self._read_pressure_query_locked(
                        cmd=cmd,
                        timeout_s=timeout_s,
                        clear_buffer=bool(clear_buffer and idx == 0),
                    )
                except Exception as exc:
                    last_exc = exc
                    if idx + 1 < attempts and retry_sleep_s > 0:
                        time.sleep(float(retry_sleep_s))

        if last_exc:
            raise last_exc
        raise RuntimeError("NO_RESPONSE")

    def pressure_continuous_active(self) -> bool:
        return bool(self._continuous_pressure_mode)

    def start_pressure_continuous(
        self,
        *,
        mode: str = "P4",
        clear_buffer: bool = True,
    ) -> bool:
        cmd_name = str(mode or "P4").strip().upper()
        if cmd_name not in {"P4", "P7"}:
            raise ValueError(f"unsupported pressure continuous mode: {mode}")
        cmd = self._cmd(cmd_name)
        with self._query_lock:
            if clear_buffer:
                reset_input_buffer = getattr(self.ser, "reset_input_buffer", None)
                if callable(reset_input_buffer):
                    reset_input_buffer()
            self.ser.write(cmd)
            self._continuous_pressure_mode = cmd_name
        return True

    def read_pressure_continuous_latest(
        self,
        *,
        drain_s: float = 0.12,
        read_timeout_s: float = 0.02,
    ) -> Optional[float]:
        with self._query_lock:
            if not self._continuous_pressure_mode:
                return None
            lines: list[str] = []
            drain_input_nonblock = getattr(self.ser, "drain_input_nonblock", None)
            if callable(drain_input_nonblock):
                lines = list(
                    drain_input_nonblock(
                        drain_s=max(0.01, float(drain_s)),
                        read_timeout_s=max(0.01, float(read_timeout_s)),
                    )
                    or []
                )
            else:
                deadline = time.time() + max(0.01, float(drain_s))
                while time.time() < deadline:
                    line = self.ser.readline()
                    if not line:
                        time.sleep(0.005)
                        continue
                    lines.append((line or "").strip())
            return self._parse_latest_pressure_lines(lines)

    def stop_pressure_continuous(
        self,
        *,
        response_timeout_s: Optional[float] = None,
    ) -> bool:
        with self._query_lock:
            if not self._continuous_pressure_mode:
                return True
            try:
                timeout_s = max(float(response_timeout_s or self.response_timeout_s), 1.2)
                drain_input_nonblock = getattr(self.ser, "drain_input_nonblock", None)
                if callable(drain_input_nonblock):
                    drain_input_nonblock(drain_s=0.05, read_timeout_s=0.01)
                cmd = self._cmd("P3")
                exchange_readlines = getattr(self.ser, "exchange_readlines", None)
                if callable(exchange_readlines):
                    exchange_readlines(
                        cmd,
                        response_timeout_s=timeout_s,
                        read_timeout_s=min(0.1, timeout_s / 4.0),
                        clear_input=False,
                    )
                else:
                    self.ser.write(cmd)
                # The manual specifies that any valid command cancels continuous output.
                # We therefore treat a successfully sent P3 command as a clean stop even if
                # the response stream does not yield a fresh numeric line within this window.
                if callable(drain_input_nonblock):
                    drain_input_nonblock(
                        drain_s=min(0.1, timeout_s / 2.0),
                        read_timeout_s=0.01,
                    )
            except Exception:
                self._continuous_pressure_mode = ""
                return False
            self._continuous_pressure_mode = ""
        return True

    def read(self) -> float:
        return self.read_pressure()

    def status(self) -> dict[str, Any]:
        return {"pressure_hpa": self.read_pressure()}

    def selftest(self) -> dict[str, Any]:
        return self.status()
