"""Serial communication base driver with thread-safe IO tracing."""

from __future__ import annotations

from collections import deque
import threading
import time
from typing import Any, Callable, Deque, List, Optional, TypeVar

try:
    import serial as _serial
except ModuleNotFoundError:  # pragma: no cover - depends on optional dependency set
    _serial = None

# Backward-compatible module alias used by older tests and drivers.
serial = _serial


T = TypeVar("T")


class ReplaySerial:
    """In-memory serial transport used for simulation and deterministic replays."""

    def __init__(
        self,
        *,
        port: str = "REPLAY",
        baudrate: int = 9600,
        timeout: float = 1.0,
        parity: str = "N",
        stopbits: float = 1,
        bytesize: int = 8,
        script: Optional[List[dict[str, Any]]] = None,
        read_lines: Optional[List[Any]] = None,
        read_buffer: Any = b"",
        on_write: Optional[Callable[[bytes, "ReplaySerial"], None]] = None,
    ):
        self.port = port
        self.baudrate = baudrate
        self.timeout = timeout
        self.parity = parity
        self.stopbits = stopbits
        self.bytesize = bytesize
        self.is_open = True
        self.writes: List[bytes] = []
        self.events: List[dict[str, Any]] = []
        self._script = list(script or [])
        self._script_idx = 0
        self._line_queue: Deque[bytes] = deque()
        self._read_buf = bytearray()
        self._on_write = on_write
        for line in read_lines or []:
            self.queue_line(line)
        self.queue_buffer(read_buffer)

    @staticmethod
    def _to_bytes(value: Any) -> bytes:
        if value is None:
            return b""
        if isinstance(value, bytes):
            return value
        return str(value).encode("ascii", errors="ignore")

    def queue_line(self, line: Any) -> None:
        payload = self._to_bytes(line)
        if payload and not payload.endswith((b"\r", b"\n")):
            payload += b"\r\n"
        self._line_queue.append(payload)

    def queue_buffer(self, data: Any) -> None:
        payload = self._to_bytes(data)
        if payload:
            self._read_buf.extend(payload)

    @property
    def in_waiting(self) -> int:
        return len(self._read_buf)

    def write(self, data: bytes) -> int:
        if not self.is_open:
            raise RuntimeError("serial not open")
        payload = self._to_bytes(data)
        self.writes.append(payload)
        self.events.append({"method": "write", "data": payload})
        if self._script_idx < len(self._script):
            step = self._script[self._script_idx]
            self._script_idx += 1
            expected = step.get("expect", step.get("write"))
            if expected is not None and payload != self._to_bytes(expected):
                raise RuntimeError("REPLAY_WRITE_MISMATCH")
            error = step.get("error")
            if error is not None:
                if isinstance(error, BaseException):
                    raise error
                raise RuntimeError(str(error))
            responses = step.get("responses")
            if responses is None and "response" in step:
                responses = [step["response"]]
            for line in responses or []:
                self.queue_line(line)
            self.queue_buffer(step.get("buffer"))
        if callable(self._on_write):
            self._on_write(payload, self)
        return len(payload)

    def flush(self) -> None:
        return None

    def readline(self) -> bytes:
        if not self.is_open:
            raise RuntimeError("serial not open")
        if self._line_queue:
            return self._line_queue.popleft()
        return b""

    def read(self, n: int) -> bytes:
        if not self.is_open:
            raise RuntimeError("serial not open")
        count = max(0, int(n))
        chunk = bytes(self._read_buf[:count])
        del self._read_buf[:count]
        return chunk

    def reset_input_buffer(self) -> None:
        self._line_queue.clear()
        self._read_buf.clear()

    def close(self) -> None:
        self.is_open = False


class SerialDevice:
    """Common serial wrapper used by multiple device drivers."""

    OPEN_RETRY_COUNT = 2
    OPEN_RETRY_DELAY_S = 0.2
    IO_RECOVERY_RETRY_COUNT = 1
    IO_RECOVERY_RETRY_DELAY_S = 0.2

    def __init__(
        self,
        port: str,
        baudrate: int,
        timeout: float = 1.0,
        parity: str = "N",
        stopbits: float = 1,
        bytesize: int = 8,
        device_name: str = "serial_device",
        io_logger: Optional[Any] = None,
        serial_factory: Optional[Callable[..., Any]] = None,
    ):
        self.port = port
        self.baudrate = baudrate
        self.timeout = timeout
        self.parity = parity
        self.stopbits = stopbits
        self.bytesize = bytesize
        self.device_name = device_name
        self.io_logger = io_logger
        self.serial_factory = serial_factory

        self._ser: Optional[Any] = None
        self._lock = threading.RLock()

    @staticmethod
    def _safe_log_field(value: Any) -> Optional[str]:
        if value is None:
            return None
        try:
            return str(value)
        except Exception:
            try:
                return repr(value)
            except Exception:
                return f"<unprintable {type(value).__name__}>"

    def _log_io(self, direction: str, command: Any = None, response: Any = None, error: Any = None) -> None:
        logger = self.io_logger
        if not logger or not hasattr(logger, "log_io"):
            return
        try:
            logger.log_io(
                port=self._safe_log_field(self.port) or "",
                device=self._safe_log_field(self.device_name) or "",
                direction=self._safe_log_field(direction) or "",
                command=self._safe_log_field(command),
                response=self._safe_log_field(response),
                error=self._safe_log_field(error),
            )
        except Exception:
            # Logging must never break device communication.
            pass

    @classmethod
    def _is_recoverable_serial_error(cls, exc: BaseException) -> bool:
        serial_exception = getattr(_serial, "SerialException", None)
        if isinstance(exc, (PermissionError, OSError)):
            return True
        if serial_exception is not None and isinstance(exc, serial_exception):
            return True

        text = cls._safe_log_field(exc) or ""
        lowered = text.lower()
        return any(
            marker in lowered
            for marker in (
                "cannot configure port",
                "permissionerror(13",
                "access is denied",
                "拒绝访问",
                "device reports readiness to read but returned no data",
            )
        )

    def _close_handle_locked(self) -> None:
        if not self._ser:
            return
        try:
            self._ser.close()
        finally:
            self._ser = None

    def _open_once_locked(self) -> None:
        if self.serial_factory is not None:
            factory = self.serial_factory
        else:
            if _serial is None:
                raise ModuleNotFoundError(
                    "pyserial is required to open real serial devices. "
                    "Install pyserial or use ReplaySerial/simulation instead."
                )
            factory = _serial.Serial
        self._ser = factory(
            port=self.port,
            baudrate=self.baudrate,
            timeout=self.timeout,
            parity=self.parity,
            stopbits=self.stopbits,
            bytesize=self.bytesize,
        )

    def open(self) -> None:
        with self._lock:
            if self._ser and getattr(self._ser, "is_open", True):
                return
            self._close_handle_locked()

            attempts = 1 + max(0, int(self.OPEN_RETRY_COUNT))
            last_exc: Optional[Exception] = None
            for idx in range(attempts):
                try:
                    self._open_once_locked()
                    self._log_io("OPEN", command="open")
                    return
                except Exception as exc:
                    last_exc = exc
                    self._close_handle_locked()
                    if idx + 1 < attempts:
                        self._log_io(
                            "WARN",
                            command="open",
                            response=f"OPEN_RETRY {idx + 1}/{attempts}",
                            error=exc,
                        )
                        time.sleep(max(0.01, float(self.OPEN_RETRY_DELAY_S)))
                        continue
                    self._log_io("ERROR", command="open", error=exc)
                    raise

            if last_exc is not None:
                raise last_exc

    def connect(self) -> None:
        self.open()

    def close(self) -> None:
        with self._lock:
            if self._ser:
                try:
                    self._ser.close()
                    self._log_io("CLOSE", command="close")
                except Exception as exc:
                    self._log_io("ERROR", command="close", error=exc)
                    raise
                finally:
                    self._ser = None

    def _recover_after_io_error_locked(self, operation: str, exc: Exception, attempt: int, attempts: int) -> bool:
        if attempt + 1 >= attempts or not self._is_recoverable_serial_error(exc):
            return False

        self._log_io(
            "WARN",
            command=operation,
            response=f"IO_RECOVER_REOPEN {attempt + 1}/{attempts}",
            error=exc,
        )
        self._close_handle_locked()
        time.sleep(max(0.01, float(self.IO_RECOVERY_RETRY_DELAY_S)))
        self.open()
        return True

    def _run_io(self, operation: str, fn: Callable[[], T]) -> T:
        attempts = 1 + max(0, int(self.IO_RECOVERY_RETRY_COUNT))
        last_exc: Optional[Exception] = None
        with self._lock:
            for idx in range(attempts):
                if not self._ser:
                    raise RuntimeError("serial not open")
                try:
                    return fn()
                except Exception as exc:
                    last_exc = exc
                    self._log_io("ERROR", command=operation, error=exc)
                    if self._recover_after_io_error_locked(operation, exc, idx, attempts):
                        continue
                    raise

        if last_exc is not None:
            raise last_exc
        raise RuntimeError(f"{operation} failed")

    def write(self, data: str) -> None:
        if not self._ser:
            raise RuntimeError("serial not open")

        def _write_once() -> None:
            self._ser.write(data.encode("ascii", errors="ignore"))
            self._ser.flush()
            self._log_io("TX", command=data)

        self._run_io(data, _write_once)

    def readline(self) -> str:
        if not self._ser:
            raise RuntimeError("serial not open")

        def _readline_once() -> str:
            line = self._ser.readline()
            decoded = line.decode("ascii", errors="ignore").strip()
            self._log_io("RX", response=decoded)
            return decoded

        return self._run_io("readline", _readline_once)

    def read(self) -> str:
        return self.readline()

    def read_available(self) -> str:
        if not self._ser:
            raise RuntimeError("serial not open")

        def _read_available_once() -> str:
            waiting = self._ser.in_waiting
            if waiting <= 0:
                return ""
            raw = self._ser.read(waiting)
            decoded = raw.decode("ascii", errors="ignore").strip()
            self._log_io("RX", response=decoded)
            return decoded

        return self._run_io("read_available", _read_available_once)

    def query(self, data: str, delay_s: float = 0.05) -> str:
        self.write(data)
        if delay_s:
            time.sleep(delay_s)
        return self.readline()

    def exchange_readlines(
        self,
        data: str,
        *,
        response_timeout_s: float,
        read_timeout_s: float = 0.1,
        clear_input: bool = False,
    ) -> List[str]:
        if not self._ser:
            raise RuntimeError("serial not open")

        total_timeout_s = max(0.01, float(response_timeout_s))
        poll_timeout_s = max(0.01, float(read_timeout_s or total_timeout_s))

        def _exchange_once() -> List[str]:
            if not self._ser:
                raise RuntimeError("serial not open")
            lines: List[str] = []
            old_timeout = getattr(self._ser, "timeout", None)
            try:
                if clear_input:
                    reset_input_buffer = getattr(self._ser, "reset_input_buffer", None)
                    if callable(reset_input_buffer):
                        reset_input_buffer()
                        self._log_io("RX", response="<flush_input>")
                self._ser.write(data.encode("ascii", errors="ignore"))
                self._ser.flush()
                self._log_io("TX", command=data)

                deadline = time.monotonic() + total_timeout_s
                while True:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        break
                    if old_timeout is not None:
                        self._ser.timeout = max(0.01, min(poll_timeout_s, remaining))
                    raw = self._ser.readline()
                    decoded = raw.decode("ascii", errors="ignore").strip()
                    self._log_io("RX", response=decoded)
                    lines.append(decoded)
                return lines
            finally:
                if old_timeout is not None:
                    self._ser.timeout = old_timeout

        return self._run_io(f"exchange_readlines:{data}", _exchange_once)

    def status(self) -> dict[str, Any]:
        ser = self._ser
        return {
            "port": self.port,
            "device": self.device_name,
            "is_open": bool(ser and getattr(ser, "is_open", True)),
        }

    def selftest(self) -> dict[str, Any]:
        return self.status()

    def flush_input(self) -> None:
        if not self._ser:
            raise RuntimeError("serial not open")

        def _flush_once() -> None:
            self._ser.reset_input_buffer()
            self._log_io("RX", response="<flush_input>")

        self._run_io("flush_input", _flush_once)

    def drain_input_nonblock(self, drain_s: float = 0.35, read_timeout_s: float = 0.05) -> List[str]:
        if not self._ser:
            raise RuntimeError("serial not open")

        def _drain_once() -> List[str]:
            lines: List[str] = []
            t0 = time.time()
            old_timeout = self._ser.timeout
            self._ser.timeout = read_timeout_s
            try:
                while time.time() - t0 < drain_s:
                    raw = self._ser.readline()
                    if raw:
                        decoded = raw.decode("ascii", errors="ignore").strip()
                        lines.append(decoded)
                        self._log_io("RX", response=decoded)
                    else:
                        time.sleep(0.005)
            finally:
                self._ser.timeout = old_timeout
            return lines

        return self._run_io("drain_input_nonblock", _drain_once)

