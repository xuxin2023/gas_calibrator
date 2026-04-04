"""Shared simulation and replay helpers for device-driver tests and development."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Optional


@dataclass
class ReplayModbusResponse:
    """Lightweight pymodbus-like response object for simulations."""

    registers: list[int] = field(default_factory=list)
    bits: list[bool] = field(default_factory=list)
    error: bool = False
    text: str = ""

    def isError(self) -> bool:
        return bool(self.error)

    def __str__(self) -> str:
        return self.text or ("ReplayModbusError" if self.error else "ReplayModbusResponse")


class ReplayModbusClient:
    """In-memory Modbus client that replays scripted method calls."""

    def __init__(
        self,
        *,
        script: Optional[list[dict[str, Any]]] = None,
        connect_result: bool = True,
        on_call: Optional[Callable[[str, tuple[Any, ...], dict[str, Any], "ReplayModbusClient"], Any]] = None,
    ):
        self.connected = False
        self.connect_result = bool(connect_result)
        self.on_call = on_call
        self.script = list(script or [])
        self._script_idx = 0
        self.calls: list[dict[str, Any]] = []

    @staticmethod
    def _normalize_response(value: Any, *, method: str) -> Any:
        if isinstance(value, ReplayModbusResponse):
            return value
        if isinstance(value, dict):
            return ReplayModbusResponse(
                registers=list(value.get("registers", [])),
                bits=list(value.get("bits", [])),
                error=bool(value.get("error", False)),
                text=str(value.get("text", "")),
            )
        if value is None and method == "connect":
            return True
        if value is None and method.startswith("read_"):
            return ReplayModbusResponse()
        if value is None and method.startswith("write_"):
            return ReplayModbusResponse()
        return value

    @staticmethod
    def _matches(step: dict[str, Any], method: str, args: tuple[Any, ...], kwargs: dict[str, Any]) -> bool:
        if step.get("method") not in (None, method):
            return False
        if "args" in step and list(step["args"]) != list(args):
            return False
        expected_kwargs = step.get("kwargs")
        if expected_kwargs is not None:
            for key, value in dict(expected_kwargs).items():
                if kwargs.get(key) != value:
                    return False
        return True

    def _next_script_step(self, method: str, args: tuple[Any, ...], kwargs: dict[str, Any]) -> Optional[dict[str, Any]]:
        if self._script_idx >= len(self.script):
            return None
        step = self.script[self._script_idx]
        if not self._matches(step, method, args, kwargs):
            raise RuntimeError(f"REPLAY_CALL_MISMATCH(method={method})")
        self._script_idx += 1
        return step

    def _handle(self, method: str, *args: Any, **kwargs: Any) -> Any:
        self.calls.append({"method": method, "args": args, "kwargs": dict(kwargs)})
        step = self._next_script_step(method, args, kwargs)
        if step is not None:
            error = step.get("error")
            if error is not None:
                if isinstance(error, BaseException):
                    raise error
                raise RuntimeError(str(error))
            response = self._normalize_response(step.get("response"), method=method)
        else:
            response = self._normalize_response(None, method=method)

        if callable(self.on_call):
            override = self.on_call(method, args, kwargs, self)
            if override is not None:
                response = override
        return response

    def connect(self) -> bool:
        result = self._handle("connect")
        ok = self.connect_result if result is True else bool(result)
        self.connected = ok
        return ok

    def close(self) -> None:
        self._handle("close")
        self.connected = False

    def read_coils(self, address: int, count: int, **kwargs: Any) -> ReplayModbusResponse:
        return self._handle("read_coils", address, count, **kwargs)

    def write_coil(self, address: int, value: Any, **kwargs: Any) -> ReplayModbusResponse:
        return self._handle("write_coil", address, value, **kwargs)

    def read_input_registers(self, address: int, count: int, **kwargs: Any) -> ReplayModbusResponse:
        return self._handle("read_input_registers", address, count, **kwargs)

    def write_register(self, address: int, value: Any, **kwargs: Any) -> ReplayModbusResponse:
        return self._handle("write_register", address, value, **kwargs)
