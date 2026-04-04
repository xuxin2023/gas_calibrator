from __future__ import annotations

import copy
from dataclasses import asdict, dataclass, field
from typing import Any


@dataclass(frozen=True)
class FakeTransportFaultSpec:
    name: str
    active: bool = False
    detail: str = ""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class FakeAnalyzerSpec:
    protocol: str = "ygas"
    count: int = 8
    mode2_stream: str = "stable"
    active_send: bool = True
    sensor_precheck: str = "strict_pass"
    versions: list[str] = field(default_factory=lambda: ["v5_plus"] * 8)
    status_bits: list[str] = field(default_factory=lambda: ["0000"] * 8)
    mode_switch_not_applied: bool = False

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class FakeHumidityGeneratorSpec:
    protocol: str = "grz5013"
    mode: str = "stable"
    skipped_by_profile: bool = False

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class FakeDewpointMeterSpec:
    mode: str = "stable"
    skipped_by_profile: bool = False

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class FakePressureControllerSpec:
    protocol: str = "scpi"
    mode: str = "stable"
    unit: str = "HPA"
    unsupported_headers: list[str] = field(default_factory=list)
    faults: list[FakeTransportFaultSpec] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["faults"] = [fault.to_dict() for fault in self.faults]
        return payload


@dataclass(frozen=True)
class FakePressureGaugeSpec:
    protocol: str = "paroscientific_735_745"
    mode: str = "stable"
    dest_id: str = "01"
    source_id: str = "00"
    unit: str = "HPA"
    temperature_unit: str = "C"
    measurement_mode: str = "single"
    unsupported_commands: list[str] = field(default_factory=list)
    faults: list[FakeTransportFaultSpec] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["faults"] = [fault.to_dict() for fault in self.faults]
        return payload


@dataclass(frozen=True)
class FakeTemperatureChamberSpec:
    protocol: str = "modbus"
    mode: str = "stable"
    soak_behavior: str = "on_target"
    ramp_rate_c_per_s: float = 10.0
    soak_s: float = 0.5

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class FakeRelaySpec:
    protocol: str = "modbus_rtu"
    channel_count: int = 16
    mode: str = "stable"
    stuck_channels: list[int] = field(default_factory=list)
    skipped_by_profile: bool = False

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class FakeThermometerSpec:
    protocol: str = "ascii_stream"
    mode: str = "stable"
    plus_200_mode: bool = False
    drift_step_c: float = 0.05
    skipped_by_profile: bool = False

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class SimulatedDeviceMatrix:
    analyzers: FakeAnalyzerSpec = field(default_factory=FakeAnalyzerSpec)
    humidity_generator: FakeHumidityGeneratorSpec = field(default_factory=FakeHumidityGeneratorSpec)
    dewpoint_meter: FakeDewpointMeterSpec = field(default_factory=FakeDewpointMeterSpec)
    pressure_controller: FakePressureControllerSpec = field(default_factory=FakePressureControllerSpec)
    pressure_gauge: FakePressureGaugeSpec = field(default_factory=FakePressureGaugeSpec)
    temperature_chamber: FakeTemperatureChamberSpec = field(default_factory=FakeTemperatureChamberSpec)
    relay: FakeRelaySpec = field(default_factory=FakeRelaySpec)
    relay_8: FakeRelaySpec = field(default_factory=lambda: FakeRelaySpec(channel_count=8))
    thermometer: FakeThermometerSpec = field(default_factory=FakeThermometerSpec)
    device_overrides: dict[str, dict[str, Any]] = field(default_factory=dict)
    transport_faults: list[FakeTransportFaultSpec] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "analyzers": self.analyzers.to_dict(),
            "humidity_generator": self.humidity_generator.to_dict(),
            "dewpoint_meter": self.dewpoint_meter.to_dict(),
            "pressure_controller": self.pressure_controller.to_dict(),
            "pressure_gauge": self.pressure_gauge.to_dict(),
            "temperature_chamber": self.temperature_chamber.to_dict(),
            "relay": self.relay.to_dict(),
            "relay_8": self.relay_8.to_dict(),
            "thermometer": self.thermometer.to_dict(),
            "device_overrides": copy.deepcopy(self.device_overrides),
            "transport_faults": [fault.to_dict() for fault in self.transport_faults],
        }
