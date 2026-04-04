"""Temperature chamber Modbus driver."""

from __future__ import annotations

import time
from typing import Any, Optional

try:
    from pymodbus.client import ModbusSerialClient
except ModuleNotFoundError:  # pragma: no cover - depends on optional dependency set
    ModbusSerialClient = None


class TemperatureChamber:
    """Temperature chamber controller."""

    _RUN_STATE_POLL_INTERVAL_S = 0.2
    _RUN_STATE_POLL_ATTEMPTS = 10

    @staticmethod
    def _default_client_factory() -> Any:
        if ModbusSerialClient is None:
            raise ModuleNotFoundError(
                "pymodbus is required to open real temperature chamber devices. "
                "Install pymodbus or inject a replay/simulation client instead."
            )
        return ModbusSerialClient

    def __init__(
        self,
        port: str,
        baudrate: int = 9600,
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
                device="temperature_chamber",
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

    @staticmethod
    def _decode_signed_tenth(register_value: Any) -> float:
        raw = int(register_value) & 0xFFFF
        if raw >= 0x8000:
            raw -= 0x10000
        return raw / 10.0

    @staticmethod
    def _encode_signed_tenth(value: float) -> int:
        scaled = int(round(float(value) * 10.0))
        return scaled & 0xFFFF

    def _call_with_addr(self, method_name: str, *args):
        fn = getattr(self.client, method_name)
        last_exc: Optional[Exception] = None
        for kw in ({"slave": self.addr}, {"unit": self.addr}, {"device_id": self.addr}):
            try:
                return fn(*args, **kw)
            except TypeError as exc:
                last_exc = exc
                if len(args) == 2 and method_name.startswith("read_"):
                    try:
                        return fn(args[0], count=args[1], **kw)
                    except TypeError as exc2:
                        last_exc = exc2
                if len(args) == 2 and method_name.startswith("write_"):
                    try:
                        return fn(args[0], value=args[1], **kw)
                    except TypeError as exc2:
                        last_exc = exc2
                text = str(exc)
                if "unexpected keyword argument" in text or "positional arguments" in text:
                    continue
                raise
        if last_exc:
            raise last_exc
        return fn(*args)

    def _wait_for_run_state(self, expected: int, attempts: Optional[int] = None) -> bool:
        polls = int(attempts or self._RUN_STATE_POLL_ATTEMPTS)
        for idx in range(max(1, polls)):
            state = self.read_run_state()
            if int(state) == int(expected):
                return True
            if idx + 1 < polls:
                time.sleep(self._RUN_STATE_POLL_INTERVAL_S)
        return False

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

    def read_temp_c(self) -> float:
        cmd = f"read_input_registers(7991,1,addr={self.addr})"
        self._log_io("TX", command=cmd)
        try:
            rr = self._call_with_addr("read_input_registers", 7991, 1)
            self._raise_on_modbus_error(rr)
            value = self._decode_signed_tenth(rr.registers[0])
            self._log_io("RX", response=f"temp_c={value}")
            return value
        except Exception as exc:
            self._log_io("ERROR", command=cmd, error=exc)
            raise

    def read_rh_pct(self) -> float:
        cmd = f"read_input_registers(7992,1,addr={self.addr})"
        self._log_io("TX", command=cmd)
        try:
            rr = self._call_with_addr("read_input_registers", 7992, 1)
            self._raise_on_modbus_error(rr)
            value = rr.registers[0] / 10.0
            self._log_io("RX", response=f"rh_pct={value}")
            return value
        except Exception as exc:
            self._log_io("ERROR", command=cmd, error=exc)
            raise

    def read_run_state(self) -> int:
        """
        Read run status register.
        0 = stopped, 1 = running.
        """
        cmd = f"read_input_registers(7990,1,addr={self.addr})"
        self._log_io("TX", command=cmd)
        try:
            rr = self._call_with_addr("read_input_registers", 7990, 1)
            self._raise_on_modbus_error(rr)
            state = int(rr.registers[0])
            self._log_io("RX", response=f"run_state={state}")
            return state
        except Exception as exc:
            self._log_io("ERROR", command=cmd, error=exc)
            raise

    def read_set_temp_c(self) -> float:
        cmd = f"read_holding_registers(8100,1,addr={self.addr})"
        self._log_io("TX", command=cmd)
        try:
            rr = self._call_with_addr("read_holding_registers", 8100, 1)
            self._raise_on_modbus_error(rr)
            value = self._decode_signed_tenth(rr.registers[0])
            self._log_io("RX", response=f"set_temp_c={value}")
            return value
        except Exception as exc:
            self._log_io("ERROR", command=cmd, error=exc)
            raise

    def read_set_rh_pct(self) -> float:
        cmd = f"read_holding_registers(8101,1,addr={self.addr})"
        self._log_io("TX", command=cmd)
        try:
            rr = self._call_with_addr("read_holding_registers", 8101, 1)
            self._raise_on_modbus_error(rr)
            value = rr.registers[0] / 10.0
            self._log_io("RX", response=f"set_rh_pct={value}")
            return value
        except Exception as exc:
            self._log_io("ERROR", command=cmd, error=exc)
            raise

    def set_temp_c(self, value_c: float) -> None:
        v = self._encode_signed_tenth(value_c)
        cmd = f"write_register(8100,{v},addr={self.addr})"
        self._log_io("TX", command=cmd)
        try:
            rr = self._call_with_addr("write_register", 8100, v)
            self._raise_on_modbus_error(rr)
            self._log_io("RX", response="ok")
        except Exception as exc:
            self._log_io("ERROR", command=cmd, error=exc)
            raise

    def set_rh_pct(self, value_pct: float) -> None:
        v = int(round(value_pct * 10))
        cmd = f"write_register(8101,{v},addr={self.addr})"
        self._log_io("TX", command=cmd)
        try:
            rr = self._call_with_addr("write_register", 8101, v)
            self._raise_on_modbus_error(rr)
            self._log_io("RX", response="ok")
        except Exception as exc:
            self._log_io("ERROR", command=cmd, error=exc)
            raise

    def start(self) -> None:
        cmd = f"write_coil(8000,True,addr={self.addr})"
        self._log_io("TX", command=cmd)
        try:
            rr = self._call_with_addr("write_coil", 8000, True)
            self._raise_on_modbus_error(rr)
            self._log_io("RX", response="ok")

            if self._wait_for_run_state(1):
                return

            # Fallback path for models requiring register-based run command.
            cmd2 = f"write_register(8010,1,addr={self.addr})"
            self._log_io("TX", command=cmd2)
            rr2 = self._call_with_addr("write_register", 8010, 1)
            self._raise_on_modbus_error(rr2)
            self._log_io("RX", response="ok(reg8010=1)")
            if not self._wait_for_run_state(1):
                raise RuntimeError("START_STATE_MISMATCH")
        except Exception as exc:
            self._log_io("ERROR", command=cmd, error=exc)
            raise

    def stop(self) -> None:
        cmd = f"write_coil(8001,True,addr={self.addr})"
        self._log_io("TX", command=cmd)
        try:
            rr = self._call_with_addr("write_coil", 8001, True)
            self._raise_on_modbus_error(rr)
            self._log_io("RX", response="ok")

            if self._wait_for_run_state(0):
                return

            # Fallback path for models requiring register-based stop command.
            cmd2 = f"write_register(8010,2,addr={self.addr})"
            self._log_io("TX", command=cmd2)
            rr2 = self._call_with_addr("write_register", 8010, 2)
            self._raise_on_modbus_error(rr2)
            self._log_io("RX", response="ok(reg8010=2)")
            if not self._wait_for_run_state(0):
                raise RuntimeError("STOP_STATE_MISMATCH")
        except Exception as exc:
            self._log_io("ERROR", command=cmd, error=exc)
            raise

    def read(self) -> dict[str, Any]:
        return {
            "temp_c": self.read_temp_c(),
            "rh_pct": self.read_rh_pct(),
            "run_state": self.read_run_state(),
        }

    def write(self, command: Any) -> None:
        if isinstance(command, str):
            text = command.strip().upper()
            if text == "START":
                self.start()
                return
            if text == "STOP":
                self.stop()
                return
            raise TypeError("temperature chamber write string must be START or STOP")
        if isinstance(command, dict):
            if "temp_c" in command:
                self.set_temp_c(float(command["temp_c"]))
            if "rh_pct" in command:
                self.set_rh_pct(float(command["rh_pct"]))
            if command.get("start"):
                self.start()
            if command.get("stop"):
                self.stop()
            return
        raise TypeError("temperature chamber write expects START/STOP or config dict")

    def status(self) -> dict[str, Any]:
        data = self.read()
        data["ok"] = True
        return data

    def selftest(self) -> dict[str, Any]:
        return self.status()
