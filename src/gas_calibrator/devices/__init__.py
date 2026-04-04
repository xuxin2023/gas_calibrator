"""
Device package entrypoints.

Exports are resolved lazily so importing ``gas_calibrator.devices`` does not
eagerly pull optional Modbus dependencies into collect-only, simulation, or
offline paths that do not need real drivers.
"""

from __future__ import annotations

from importlib import import_module
from typing import Any

_LAZY_EXPORTS = {
    "Pace5000": ("gas_calibrator.devices.pace5000", "Pace5000"),
    "ParoscientificGauge": ("gas_calibrator.devices.paroscientific", "ParoscientificGauge"),
    "DewpointMeter": ("gas_calibrator.devices.dewpoint_meter", "DewpointMeter"),
    "HumidityGenerator": ("gas_calibrator.devices.humidity_generator", "HumidityGenerator"),
    "GasAnalyzer": ("gas_calibrator.devices.gas_analyzer", "GasAnalyzer"),
    "TemperatureChamber": ("gas_calibrator.devices.temperature_chamber", "TemperatureChamber"),
    "Thermometer": ("gas_calibrator.devices.thermometer", "Thermometer"),
    "RelayController": ("gas_calibrator.devices.relay", "RelayController"),
    "ReplaySerial": ("gas_calibrator.devices.serial_base", "ReplaySerial"),
    "ReplayModbusClient": ("gas_calibrator.devices.simulation", "ReplayModbusClient"),
    "ReplayModbusResponse": ("gas_calibrator.devices.simulation", "ReplayModbusResponse"),
}

__all__ = list(_LAZY_EXPORTS)


def __getattr__(name: str) -> Any:
    target = _LAZY_EXPORTS.get(name)
    if target is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module_name, attr_name = target
    value = getattr(import_module(module_name), attr_name)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted(set(globals()) | set(__all__))
