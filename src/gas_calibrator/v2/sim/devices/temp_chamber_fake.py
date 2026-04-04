from __future__ import annotations

from dataclasses import dataclass
import math
import time
from typing import Any, Optional


def _approach(current: float, target: float, step: float) -> float:
    delta = float(target) - float(current)
    if abs(delta) <= step:
        return float(target)
    return float(current) + math.copysign(step, delta)


@dataclass
class FakeModbusResponse:
    registers: list[int] | None = None
    bits: list[bool] | None = None
    error: Optional[str] = None

    def isError(self) -> bool:
        return bool(self.error)

    def __str__(self) -> str:
        return str(self.error or "OK")


class TemperatureChamberFake:
    """
    Protocol-level Modbus-style fake for the temperature chamber.

    It models the subset that the real driver consumes:
    - 03 / 04 read registers
    - 05 write coil
    - 06 write single register
    """

    MODE_STABLE = "stable"
    MODE_RAMP_TO_TARGET = "ramp_to_target"
    MODE_SOAK_PENDING = "soak_pending"
    MODE_ON_TARGET = "on_target"
    MODE_TEMP_DRIFT = "temp_drift"
    MODE_ALARM = "alarm"
    MODE_STALLED = "stalled"
    VALID_MODES = {
        MODE_STABLE,
        MODE_RAMP_TO_TARGET,
        MODE_SOAK_PENDING,
        MODE_ON_TARGET,
        MODE_TEMP_DRIFT,
        MODE_ALARM,
        MODE_STALLED,
    }

    REG_RUN_STATUS = 7990
    REG_CURRENT_TEMP = 7991
    REG_CURRENT_RH = 7992
    REG_START_STOP = 8010
    REG_SET_TEMP = 8100
    REG_SET_RH = 8101
    REG_CONTROL_TYPE = 8107
    REG_SETPOINT_READBACK_TEMP = 8024
    REG_SETPOINT_READBACK_RH = 8025
    COIL_START = 8000
    COIL_STOP = 8001

    def __init__(
        self,
        port: str = "SIM-TEMP-CHAMBER",
        *,
        baudrate: int = 9600,
        addr: int = 1,
        io_logger: Optional[Any] = None,
        plant_state: Optional[Any] = None,
        mode: str = MODE_STABLE,
        soak_behavior: str = "on_target",
        temperature_c: float = 25.0,
        humidity_pct: float = 40.0,
        target_temperature_c: float = 25.0,
        target_humidity_pct: float = 40.0,
        ambient_temperature_c: float = 25.0,
        ambient_humidity_pct: float = 40.0,
        ramp_rate_c_per_s: float = 10.0,
        humidity_rate_pct_per_s: float = 20.0,
        soak_s: float = 0.5,
        control_type: int = 0,
        **_: Any,
    ) -> None:
        self.port = str(port or "SIM-TEMP-CHAMBER")
        self.baudrate = int(baudrate or 9600)
        self.addr = int(addr or 1)
        self.io_logger = io_logger
        self.plant_state = plant_state
        if self.plant_state is not None:
            setattr(self.plant_state, "dynamic_protocol", True)
        self.connected = False
        self.mode = str(mode or self.MODE_STABLE).strip().lower()
        if self.mode not in self.VALID_MODES:
            self.mode = self.MODE_STABLE
        self.soak_behavior = str(soak_behavior or "on_target").strip().lower()
        self.current_temp_c = float(temperature_c if temperature_c is not None else 25.0)
        self.current_rh_pct = float(humidity_pct if humidity_pct is not None else 40.0)
        self.target_temp_c = float(target_temperature_c if target_temperature_c is not None else self.current_temp_c)
        self.target_rh_pct = float(target_humidity_pct if target_humidity_pct is not None else self.current_rh_pct)
        self.ambient_temp_c = float(ambient_temperature_c if ambient_temperature_c is not None else self.current_temp_c)
        self.ambient_rh_pct = float(ambient_humidity_pct if ambient_humidity_pct is not None else self.current_rh_pct)
        self.ramp_rate_c_per_s = max(0.1, float(ramp_rate_c_per_s))
        self.humidity_rate_pct_per_s = max(0.1, float(humidity_rate_pct_per_s))
        self.soak_s = max(0.0, float(soak_s))
        self.control_type = int(control_type or 0)
        self.running = False
        self._phase = self.mode if self.mode in {self.MODE_ALARM, self.MODE_STALLED} else self.MODE_ON_TARGET
        self._last_update_ts = time.monotonic()
        self._on_target_since: Optional[float] = None
        self._sync_plant_state()

    def open(self) -> None:
        self.connected = True

    def connect(self) -> bool:
        self.connected = True
        return True

    def close(self) -> None:
        self.connected = False

    def read_input_registers(self, address: int, count: int = 1, **_: Any) -> FakeModbusResponse:
        self._update_state()
        values = [self._read_input_register(int(address) + offset) for offset in range(int(count))]
        return FakeModbusResponse(registers=values)

    def read_holding_registers(self, address: int, count: int = 1, **_: Any) -> FakeModbusResponse:
        self._update_state()
        values = [self._read_holding_register(int(address) + offset) for offset in range(int(count))]
        return FakeModbusResponse(registers=values)

    def write_coil(self, address: int, value: bool, **_: Any) -> FakeModbusResponse:
        self._update_state()
        if int(address) == self.COIL_START and bool(value):
            self.running = True
            self._phase = self.MODE_RAMP_TO_TARGET
        elif int(address) == self.COIL_STOP and bool(value):
            self.running = False
            self._phase = self.MODE_STABLE
        return FakeModbusResponse(bits=[bool(value)])

    def write_register(self, address: int, value: int, **_: Any) -> FakeModbusResponse:
        self._update_state()
        register = int(address)
        if register == self.REG_SET_TEMP:
            self.target_temp_c = self._decode_signed_tenth(value)
            if self.running:
                self._phase = self.MODE_RAMP_TO_TARGET
        elif register == self.REG_SET_RH:
            self.target_rh_pct = float(int(value)) / 10.0
        elif register == self.REG_CONTROL_TYPE:
            self.control_type = int(value)
        elif register == self.REG_START_STOP:
            if int(value) == 1:
                self.running = True
                self._phase = self.MODE_RAMP_TO_TARGET
            elif int(value) == 2:
                self.running = False
                self._phase = self.MODE_STABLE
        return FakeModbusResponse(registers=[int(value)])

    def read_temp_c(self) -> float:
        response = self.read_input_registers(self.REG_CURRENT_TEMP, 1)
        return self._decode_signed_tenth(response.registers[0])

    def read_rh_pct(self) -> float:
        response = self.read_input_registers(self.REG_CURRENT_RH, 1)
        return float(response.registers[0]) / 10.0

    def read_run_state(self) -> int:
        response = self.read_input_registers(self.REG_RUN_STATUS, 1)
        return int(response.registers[0])

    def read_set_temp_c(self) -> float:
        response = self.read_holding_registers(self.REG_SET_TEMP, 1)
        return self._decode_signed_tenth(response.registers[0])

    def read_set_rh_pct(self) -> float:
        response = self.read_holding_registers(self.REG_SET_RH, 1)
        return float(response.registers[0]) / 10.0

    def set_temp_c(self, value_c: float) -> None:
        self.write_register(self.REG_SET_TEMP, self._encode_signed_tenth(value_c))

    def set_temperature_c(self, value_c: float) -> None:
        self.set_temp_c(value_c)

    def set_temperature(self, value_c: float) -> None:
        self.set_temp_c(value_c)

    def set_rh_pct(self, value_pct: float) -> None:
        self.write_register(self.REG_SET_RH, int(round(float(value_pct) * 10.0)))

    def start(self) -> None:
        self.write_coil(self.COIL_START, True)

    def stop(self) -> None:
        self.write_coil(self.COIL_STOP, True)

    def read(self) -> dict[str, Any]:
        self._update_state()
        return {
            "temp_c": self.current_temp_c,
            "rh_pct": self.current_rh_pct,
            "run_state": self.read_run_state(),
            "phase": self._phase,
        }

    def status(self) -> dict[str, Any]:
        self._update_state()
        return {
            "ok": self.mode != self.MODE_ALARM,
            "connected": self.connected,
            "port": self.port,
            "status": self._phase,
            "running": self.running,
            "temp_c": self.current_temp_c,
            "rh_pct": self.current_rh_pct,
            "target_temp_c": self.target_temp_c,
            "target_rh_pct": self.target_rh_pct,
            "control_type": self.control_type,
        }

    def selftest(self) -> dict[str, Any]:
        return {"ok": self.mode != self.MODE_ALARM, "status": self._phase, "connected": self.connected}

    def _read_input_register(self, address: int) -> int:
        if address == self.REG_RUN_STATUS:
            if self.mode == self.MODE_ALARM:
                return 2
            return 1 if self.running else 0
        if address == self.REG_CURRENT_TEMP:
            return self._encode_signed_tenth(self.current_temp_c)
        if address == self.REG_CURRENT_RH:
            return int(round(self.current_rh_pct * 10.0))
        return 0

    def _read_holding_register(self, address: int) -> int:
        if address in {self.REG_SET_TEMP, self.REG_SETPOINT_READBACK_TEMP}:
            return self._encode_signed_tenth(self.target_temp_c)
        if address in {self.REG_SET_RH, self.REG_SETPOINT_READBACK_RH}:
            return int(round(self.target_rh_pct * 10.0))
        if address == self.REG_CONTROL_TYPE:
            return int(self.control_type)
        return 0

    def _update_state(self) -> None:
        now = time.monotonic()
        elapsed_s = max(0.0, now - self._last_update_ts)
        self._last_update_ts = now
        if elapsed_s <= 0:
            self._sync_plant_state()
            return

        if not self.running:
            self.current_temp_c = _approach(self.current_temp_c, self.ambient_temp_c, max(0.05, elapsed_s * 3.0))
            self.current_rh_pct = _approach(self.current_rh_pct, self.ambient_rh_pct, max(0.05, elapsed_s * 5.0))
            self._on_target_since = None
            self._sync_plant_state()
            return

        if self.mode == self.MODE_ALARM:
            self._phase = self.MODE_ALARM
            self._sync_plant_state()
            return

        if self.mode == self.MODE_STALLED:
            self._phase = self.MODE_STALLED
            self.current_temp_c = _approach(
                self.current_temp_c,
                self.target_temp_c,
                max(0.01, self.ramp_rate_c_per_s * 0.03 * elapsed_s),
            )
            self._sync_plant_state()
            return

        if self.mode in {self.MODE_TEMP_DRIFT, self.MODE_STABLE, self.MODE_RAMP_TO_TARGET, self.MODE_SOAK_PENDING, self.MODE_ON_TARGET}:
            self.current_temp_c = _approach(
                self.current_temp_c,
                self.target_temp_c,
                max(0.05, self.ramp_rate_c_per_s * elapsed_s),
            )
            self.current_rh_pct = _approach(
                self.current_rh_pct,
                self.target_rh_pct,
                max(0.05, self.humidity_rate_pct_per_s * elapsed_s),
            )
            on_target = abs(self.current_temp_c - self.target_temp_c) <= 0.2
            if on_target:
                if self._on_target_since is None:
                    self._on_target_since = now
                if self.mode == self.MODE_TEMP_DRIFT:
                    drift = math.sin(now) * 0.35
                    self.current_temp_c = self.target_temp_c + drift
                    self._phase = self.MODE_TEMP_DRIFT
                elif (now - self._on_target_since) < self.soak_s:
                    self._phase = self.MODE_SOAK_PENDING
                else:
                    self._phase = self.MODE_ON_TARGET
            else:
                self._phase = self.MODE_RAMP_TO_TARGET
                self._on_target_since = None
        self._sync_plant_state()

    def _sync_plant_state(self) -> None:
        if self.plant_state is None:
            return
        try:
            self.plant_state.target_temperature_c = float(self.target_temp_c)
            self.plant_state.temperature_c = float(self.current_temp_c)
            self.plant_state.target_humidity_pct = float(self.target_rh_pct)
            self.plant_state.humidity_pct = float(self.current_rh_pct)
            self.plant_state.running = bool(self.running)
            if hasattr(self.plant_state, "sync"):
                self.plant_state.sync()
        except Exception:
            pass

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
