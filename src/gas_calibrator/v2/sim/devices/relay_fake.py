from __future__ import annotations

from typing import Any, Optional

from .temp_chamber_fake import FakeModbusResponse


class RelayFake:
    """
    Protocol-level Modbus RTU relay fake for 8/16-channel relay boards.

    The fake exposes the same driver-facing methods the V2 route layer uses
    (`read_coils`, `write_coil`, `set_valve`, `close_all`, `open_only`) while
    preserving basic Modbus-style register/bit semantics for direct tests.
    """

    VALID_MODES = {
        "stable",
        "stuck_channel",
        "write_fail",
        "read_fail",
        "skipped_by_profile",
    }

    REG_DEVICE_ADDR = 0x0100
    REG_BAUDRATE = 0x0101
    REG_PARITY = 0x0102
    REG_CHANNEL_COUNT = 0x0103
    REG_BATCH_CONTROL = 0x0110

    def __init__(
        self,
        port: str = "SIM-RELAY",
        *,
        baudrate: int = 38400,
        timeout: float = 1.0,
        addr: int = 1,
        io_logger: Optional[Any] = None,
        plant_state: Optional[Any] = None,
        name: str = "relay",
        channel_count: int = 16,
        mode: str = "stable",
        stuck_channels: Optional[list[int]] = None,
        skipped_by_profile: bool = False,
        **_: Any,
    ) -> None:
        self.port = str(port or "SIM-RELAY")
        self.baudrate = int(baudrate or 38400)
        self.timeout = float(timeout if timeout is not None else 1.0)
        self.addr = int(addr or 1)
        self.io_logger = io_logger
        self.plant_state = plant_state
        self.name = str(name or "relay")
        self.connected = False
        self.channel_count = max(1, int(channel_count or 16))
        self.mode = str(mode or "stable").strip().lower()
        if self.mode not in self.VALID_MODES:
            self.mode = "stable"
        self.skipped_by_profile = bool(skipped_by_profile or self.mode == "skipped_by_profile")
        self.stuck_channels = {max(1, int(channel)) for channel in list(stuck_channels or [])}
        if self.mode == "stuck_channel" and not self.stuck_channels:
            self.stuck_channels = {1}
        self._coils: list[bool] = [False] * self.channel_count
        self._discrete_inputs: list[bool] = [False] * self.channel_count
        self._input_registers: dict[int, int] = {}
        self._holding_registers: dict[int, int] = {
            self.REG_DEVICE_ADDR: self.addr,
            self.REG_BAUDRATE: self.baudrate,
            self.REG_PARITY: 0,
            self.REG_CHANNEL_COUNT: self.channel_count,
            self.REG_BATCH_CONTROL: 0,
        }

    def open(self) -> None:
        self.connected = True

    def connect(self) -> bool:
        self.connected = True
        return True

    def close(self) -> None:
        self.connected = False

    def read_coils(self, start: int, count: int = 1, **_: Any) -> FakeModbusResponse:
        if self.mode == "read_fail":
            raise RuntimeError("RELAY_READ_FAIL")
        start_index = max(0, int(start))
        values = [self._coils[index] if 0 <= index < self.channel_count else False for index in range(start_index, start_index + int(count))]
        return FakeModbusResponse(bits=values)

    def read_discrete_inputs(self, start: int, count: int = 1, **_: Any) -> FakeModbusResponse:
        if self.mode == "read_fail":
            raise RuntimeError("RELAY_READ_FAIL")
        start_index = max(0, int(start))
        values = [
            self._discrete_inputs[index] if 0 <= index < self.channel_count else False
            for index in range(start_index, start_index + int(count))
        ]
        return FakeModbusResponse(bits=values)

    def read_holding_registers(self, address: int, count: int = 1, **_: Any) -> FakeModbusResponse:
        if self.mode == "read_fail":
            raise RuntimeError("RELAY_READ_FAIL")
        address = int(address)
        return FakeModbusResponse(registers=[int(self._holding_registers.get(address + offset, 0)) for offset in range(int(count))])

    def read_input_registers(self, address: int, count: int = 1, **_: Any) -> FakeModbusResponse:
        if self.mode == "read_fail":
            raise RuntimeError("RELAY_READ_FAIL")
        address = int(address)
        return FakeModbusResponse(registers=[int(self._input_registers.get(address + offset, 0)) for offset in range(int(count))])

    def write_coil(self, address: int, value: bool, **_: Any) -> FakeModbusResponse:
        if self.mode == "write_fail":
            raise RuntimeError("RELAY_WRITE_FAIL")
        index = max(0, int(address))
        desired = bool(value)
        actual = desired
        if 0 <= index < self.channel_count and (index + 1) in self.stuck_channels:
            actual = self._coils[index]
        if 0 <= index < self.channel_count:
            self._coils[index] = actual
            self._discrete_inputs[index] = actual
            self._sync_plant_physical(index + 1, actual)
        return FakeModbusResponse(bits=[actual])

    def write_coils(self, address: int, values: list[bool], **_: Any) -> FakeModbusResponse:
        if self.mode == "write_fail":
            raise RuntimeError("RELAY_WRITE_FAIL")
        start = max(0, int(address))
        bits: list[bool] = []
        for offset, raw in enumerate(list(values or [])):
            response = self.write_coil(start + offset, bool(raw))
            bits.extend(list(response.bits or []))
        return FakeModbusResponse(bits=bits)

    def write_register(self, address: int, value: int, **_: Any) -> FakeModbusResponse:
        if self.mode == "write_fail":
            raise RuntimeError("RELAY_WRITE_FAIL")
        register = int(address)
        value = int(value)
        self._holding_registers[register] = value
        if register == self.REG_DEVICE_ADDR:
            self.addr = value
        elif register == self.REG_BAUDRATE:
            self.baudrate = value
        elif register == self.REG_BATCH_CONTROL:
            self._apply_batch_control(value)
        return FakeModbusResponse(registers=[value])

    def write_registers(self, address: int, values: list[int], **_: Any) -> FakeModbusResponse:
        if self.mode == "write_fail":
            raise RuntimeError("RELAY_WRITE_FAIL")
        written: list[int] = []
        for offset, value in enumerate(list(values or [])):
            response = self.write_register(int(address) + offset, int(value))
            written.extend(list(response.registers or []))
        return FakeModbusResponse(registers=written)

    def set_valve(self, channel: int, open_: bool) -> None:
        self.write_coil(max(0, int(channel) - 1), bool(open_))

    def close_all(self) -> None:
        self.write_coils(0, [False] * self.channel_count)

    def open_only(self, channel: int) -> None:
        states = [False] * self.channel_count
        index = max(0, int(channel) - 1)
        if 0 <= index < self.channel_count:
            states[index] = True
        self.write_coils(0, states)

    def set_logical_valve_state(self, logical_valve: int, desired: bool, *, physical_channel: Optional[int] = None) -> None:
        if self.plant_state is None:
            return
        actual = bool(desired)
        if physical_channel is not None:
            channel_index = max(0, int(physical_channel) - 1)
            if 0 <= channel_index < self.channel_count:
                actual = bool(self._coils[channel_index])
        setter = getattr(self.plant_state, "set_logical_valve_state", None)
        if callable(setter):
            setter(int(logical_valve), actual)

    def status(self) -> dict[str, Any]:
        return {
            "ok": self.mode not in {"read_fail", "write_fail"},
            "connected": self.connected,
            "port": self.port,
            "device_name": self.name,
            "mode": self.mode,
            "channel_count": self.channel_count,
            "coils": {str(index + 1): bool(state) for index, state in enumerate(self._coils)},
            "skipped_by_profile": self.skipped_by_profile,
        }

    def selftest(self) -> dict[str, Any]:
        payload = self.status()
        payload["status"] = "skipped_by_profile" if self.skipped_by_profile else self.mode
        return payload

    def _apply_batch_control(self, value: int) -> None:
        for index in range(self.channel_count):
            desired = bool(value & (1 << index))
            self.write_coil(index, desired)

    def _sync_plant_physical(self, channel: int, state: bool) -> None:
        if self.plant_state is None:
            return
        setter = getattr(self.plant_state, "set_valve_state", None)
        if callable(setter):
            setter(int(channel), bool(state), device_name=self.name)
