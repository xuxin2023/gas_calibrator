"""Relay controller Modbus driver."""

from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional, Tuple

try:
    from pymodbus.client import ModbusSerialClient
except ModuleNotFoundError:  # pragma: no cover - depends on optional dependency set
    ModbusSerialClient = None


class RelayController:
    """Relay valve controller."""

    @staticmethod
    def _default_client_factory() -> Any:
        if ModbusSerialClient is None:
            raise ModuleNotFoundError(
                "pymodbus is required to open real relay devices. "
                "Install pymodbus or inject a replay/simulation client instead."
            )
        return ModbusSerialClient

    def __init__(
        self,
        port: str,
        baudrate: int = 38400,
        addr: int = 1,
        io_logger: Optional[Any] = None,
        client: Optional[Any] = None,
        client_factory: Optional[Any] = None,
    ):
        self.port = port
        self.baudrate = baudrate
        self.addr = addr
        self.io_logger = io_logger
        if client is not None:
            self.client = client
        else:
            factory = client_factory or self._default_client_factory()
            self.client = factory(
                port=port,
                baudrate=baudrate,
                bytesize=8,
                parity="N",
                stopbits=1,
                timeout=1.0,
            )

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
                device="relay_controller",
                direction=self._safe_log_field(direction) or "",
                command=self._safe_log_field(command),
                response=self._safe_log_field(response),
                error=self._safe_log_field(error),
            )
        except Exception:
            pass

    @staticmethod
    def _raise_on_modbus_error(resp: Any) -> None:
        if resp is None:
            raise RuntimeError("NO_RESPONSE")
        if hasattr(resp, "isError") and resp.isError():
            raise RuntimeError(str(resp))

    def _call_with_addr(self, method_name: str, *args):
        fn = getattr(self.client, method_name)
        last_exc: Optional[Exception] = None
        for kw in ({"slave": self.addr}, {"unit": self.addr}, {"device_id": self.addr}):
            try:
                return fn(*args, **kw)
            except TypeError as exc:
                last_exc = exc
                # Some pymodbus versions make "count" keyword-only.
                if len(args) == 2 and method_name.startswith("read_"):
                    try:
                        return fn(args[0], count=args[1], **kw)
                    except TypeError as exc2:
                        last_exc = exc2
                # Some versions accept/require "value" keyword.
                if len(args) == 2 and method_name.startswith("write_"):
                    try:
                        return fn(args[0], value=args[1], **kw)
                    except TypeError as exc2:
                        last_exc = exc2
                if len(args) == 2 and method_name == "write_coils":
                    try:
                        return fn(args[0], values=args[1], **kw)
                    except TypeError as exc2:
                        last_exc = exc2
                text = str(exc)
                if "unexpected keyword argument" in text or "positional arguments" in text:
                    continue
                raise
        if last_exc:
            raise last_exc
        return fn(*args)

    def open(self) -> None:
        cmd = "connect"
        self._log_io("TX", command=cmd)
        try:
            ok = self.client.connect()
            if not ok:
                raise RuntimeError("CONNECT_FAILED")
            self._log_io("RX", response="connected")
        except Exception as exc:
            self._log_io("ERROR", command=cmd, error=exc)
            raise

    def connect(self) -> None:
        self.open()

    def close(self) -> None:
        cmd = "close"
        self._log_io("TX", command=cmd)
        try:
            self.client.close()
            self._log_io("RX", response="closed")
        except Exception as exc:
            self._log_io("ERROR", command=cmd, error=exc)
            raise

    def read_coils(self, start: int = 0, count: int = 1):
        cmd = f"read_coils({start},{count},addr={self.addr})"
        self._log_io("TX", command=cmd)
        try:
            rr = self._call_with_addr("read_coils", start, count)
            self._raise_on_modbus_error(rr)
            bits = getattr(rr, "bits", None)
            self._log_io("RX", response=bits)
            return bits
        except Exception as exc:
            self._log_io("ERROR", command=cmd, error=exc)
            raise

    def read(self, start: int = 0, count: int = 1):
        return self.read_coils(start=start, count=count)

    def write(self, command: Any) -> None:
        if isinstance(command, dict):
            channel = command.get("channel")
            open_ = command.get("open")
            if channel is None or open_ is None:
                raise TypeError("relay write expects dict with channel/open")
            self.set_valve(int(channel), bool(open_))
            return
        if isinstance(command, (tuple, list)) and len(command) >= 2:
            self.set_valve(int(command[0]), bool(command[1]))
            return
        raise TypeError("relay write expects (channel, open_) or {'channel': ..., 'open': ...}")

    def set_valve(self, channel: int, open_: bool) -> None:
        coil_addr = channel - 1
        cmd = f"write_coil({coil_addr},{open_},addr={self.addr})"
        self._log_io("TX", command=cmd)
        try:
            rr = self._call_with_addr("write_coil", coil_addr, open_)
            self._raise_on_modbus_error(rr)
            self._log_io("RX", response="ok")
        except Exception as exc:
            self._log_io("ERROR", command=cmd, error=exc)
            raise

    @staticmethod
    def _normalize_bulk_updates(updates: Any) -> List[Tuple[int, bool]]:
        if isinstance(updates, dict):
            items = list(updates.items())
        else:
            items = list(updates or [])

        normalized: Dict[int, bool] = {}
        for item in items:
            if isinstance(item, dict):
                channel = item.get("channel")
                state = item.get("open")
            elif isinstance(item, (tuple, list)) and len(item) >= 2:
                channel, state = item[0], item[1]
            else:
                raise TypeError("bulk relay update expects (channel, open_) pairs or {'channel', 'open'} items")
            normalized[int(channel)] = bool(state)
        return sorted(normalized.items(), key=lambda pair: pair[0])

    def set_valves_bulk(self, updates: Any) -> None:
        normalized = self._normalize_bulk_updates(updates)
        if not normalized:
            return

        write_coils = getattr(self.client, "write_coils", None)
        if not callable(write_coils):
            for channel, state in normalized:
                self.set_valve(channel, state)
            return

        blocks: List[Tuple[int, List[bool]]] = []
        block_start: Optional[int] = None
        block_values: List[bool] = []
        previous_addr: Optional[int] = None
        for channel, state in normalized:
            coil_addr = int(channel) - 1
            if block_start is None or previous_addr is None or coil_addr != previous_addr + 1:
                if block_start is not None:
                    blocks.append((block_start, list(block_values)))
                block_start = coil_addr
                block_values = [bool(state)]
            else:
                block_values.append(bool(state))
            previous_addr = coil_addr
        if block_start is not None:
            blocks.append((block_start, list(block_values)))

        for start_addr, values in blocks:
            cmd = f"write_coils({start_addr},{values},addr={self.addr})"
            self._log_io("TX", command=cmd)
            try:
                rr = self._call_with_addr("write_coils", start_addr, values)
                self._raise_on_modbus_error(rr)
                self._log_io("RX", response="ok")
            except Exception as exc:
                self._log_io("ERROR", command=cmd, error=exc)
                raise

    def close_all(self, channels: Iterable[int]) -> None:
        for ch in channels:
            self.set_valve(ch, False)

    def open_only(self, open_channels: Iterable[int], all_channels: Iterable[int]) -> None:
        open_set = set(open_channels)
        for ch in all_channels:
            self.set_valve(ch, ch in open_set)

    def status(self) -> dict[str, Any]:
        coils = self.read_coils(0, 8)
        return {"ok": True, "coils": coils}

    def selftest(self) -> dict[str, Any]:
        return self.status()
