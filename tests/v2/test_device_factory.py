import time

import pytest

from gas_calibrator.v2.config import DeviceConfig
from gas_calibrator.v2.core.device_factory import (
    DeviceDriverImportError,
    DeviceFactory,
    DeviceType,
)
from gas_calibrator.v2.core.device_manager import DeviceManager


class FakeDevice:
    def __init__(self, port: str, baudrate: int = 9600, timeout: float = 1.0, io_logger=None, **kwargs):
        self.port = port
        self.baudrate = baudrate
        self.timeout = timeout
        self.io_logger = io_logger
        self.kwargs = dict(kwargs)

    def open(self) -> None:
        return None

    def close(self) -> None:
        return None


def test_device_factory_register_and_create_custom_device() -> None:
    factory = DeviceFactory(io_logger="logger")
    factory.register(DeviceType.THERMOMETER, FakeDevice)

    device = factory.create(
        DeviceType.THERMOMETER,
        {"port": "COM9", "baud": 4800, "timeout": 2.5},
    )

    assert isinstance(device, FakeDevice)
    assert device.port == "COM9"
    assert device.baudrate == 4800
    assert device.timeout == 2.5
    assert device.io_logger == "logger"


def test_device_factory_has_legacy_driver_defaults() -> None:
    factory = DeviceFactory()

    assert factory._registry[DeviceType.PRESSURE_CONTROLLER].__name__ == "Pace5000"
    assert factory._registry[DeviceType.PRESSURE_METER].__name__ == "ParoscientificGauge"
    assert factory._registry[DeviceType.GAS_ANALYZER].__name__ == "GasAnalyzer"


def test_device_factory_simulation_mode_uses_simulated_devices() -> None:
    factory = DeviceFactory(simulation_mode=True)

    chamber = factory.create(
        DeviceType.TEMPERATURE_CHAMBER,
        {"port": "COM3", "baud": 9600},
    )
    relay = factory.create(
        DeviceType.RELAY,
        {"port": "COM4", "baud": 38400},
    )
    analyzer = factory.create(
        DeviceType.GAS_ANALYZER,
        {"port": "COM5", "baud": 115200},
    )
    pressure_controller = factory.create(
        DeviceType.PRESSURE_CONTROLLER,
        {"port": "COM6", "baud": 9600},
    )
    pressure_meter = factory.create(
        DeviceType.PRESSURE_METER,
        {"port": "COM6A", "baud": 9600},
    )
    thermometer = factory.create(
        DeviceType.THERMOMETER,
        {"port": "COM7", "baud": 2400},
    )

    assert chamber.__class__.__name__ == "TemperatureChamberFake"
    assert relay.__class__.__name__ == "RelayFake"
    assert analyzer.__class__.__name__ == "AnalyzerFake"
    assert pressure_controller.__class__.__name__ == "PACE5000Fake"
    assert pressure_meter.__class__.__name__ == "ParoscientificPressureGaugeFake"
    assert thermometer.__class__.__name__ == "ThermometerFake"
    assert chamber.plant_state is factory.simulation_plant_state
    assert relay.plant_state is chamber.plant_state
    assert analyzer.plant_state is chamber.plant_state
    assert pressure_controller.plant_state is chamber.plant_state
    assert pressure_meter.plant_state is chamber.plant_state
    assert thermometer.plant_state is chamber.plant_state
    assert pressure_meter.dest_id == "01"


def test_simulated_devices_share_plant_state_and_follow_temperature_humidity_pressure() -> None:
    factory = DeviceFactory(simulation_mode=True)
    chamber = factory.create(DeviceType.TEMPERATURE_CHAMBER, {"port": "COM3", "baud": 9600})
    humidity_generator = factory.create(DeviceType.HUMIDITY_GENERATOR, {"port": "COM4", "baud": 9600})
    dewpoint_meter = factory.create(DeviceType.DEWPOINT_METER, {"port": "COM5", "baud": 9600})
    pressure_controller = factory.create(DeviceType.PRESSURE_CONTROLLER, {"port": "COM6", "baud": 9600})
    pressure_meter = factory.create(DeviceType.PRESSURE_METER, {"port": "COM7", "baud": 9600})
    analyzer = factory.create(DeviceType.GAS_ANALYZER, {"port": "COM8", "baud": 115200})

    chamber.start()
    chamber.set_temperature_c(30.0)
    humidity_generator.enable_control(True)
    humidity_generator.set_humidity_pct(55.0)
    pressure_controller.set_pressure_hpa(980.0)
    pressure_controller.enable_control_output()
    time.sleep(0.15)
    chamber.read_temp_c()
    humidity_generator.fetch_all()
    pressure_controller.read_pressure()

    dewpoint = dewpoint_meter.get_current()
    analyzer_data = analyzer.fetch_all()["data"]

    assert chamber.read_temp_c() >= 25.0
    assert dewpoint["temp_c"] >= 25.0
    assert dewpoint["rh_pct"] > 35.0
    assert pressure_meter.read_pressure() < 1013.25
    assert analyzer_data["chamber_temp_c"] >= 25.0
    assert analyzer_data["pressure_hpa"] < 1013.25
    assert analyzer_data["humidity_pct"] > 35.0
    assert analyzer_data["dewpoint_c"] == dewpoint["dewpoint_c"]


def test_simulated_relay_route_changes_affect_analyzer_signal() -> None:
    factory = DeviceFactory(simulation_mode=True)
    relay = factory.create(DeviceType.RELAY, {"port": "COM4", "baud": 38400, "name": "relay_8"})
    analyzer = factory.create(DeviceType.GAS_ANALYZER, {"port": "COM5", "baud": 115200, "co2_signal": 400.0})

    relay.set_valve(8, True)
    relay.set_valve(1, True)
    relay.set_valve(2, True)
    h2o_data = analyzer.fetch_all()["data"]

    relay.close_all()
    relay.set_valve(3, True)
    co2_data = analyzer.fetch_all()["data"]

    assert h2o_data["route"] == "h2o"
    assert h2o_data["co2_ppm"] == 0.0
    assert h2o_data["h2o_mmol"] > 0.0
    assert co2_data["route"] == "co2"
    assert co2_data["co2_ppm"] == 400.0
    assert co2_data["co2_ppm"] > h2o_data["co2_ppm"]


def test_device_manager_can_create_devices_via_factory() -> None:
    factory = DeviceFactory()
    factory.register(DeviceType.GAS_ANALYZER, FakeDevice)
    manager = DeviceManager(DeviceConfig(), device_factory=factory)

    device = manager.create_device(
        "gas_analyzer_0",
        DeviceType.GAS_ANALYZER,
        {"port": "COM7", "baud": 115200},
    )

    assert isinstance(device, FakeDevice)
    assert manager.get_device("gas_analyzer_0") is device
    by_type = manager.get_devices_by_type(DeviceType.GAS_ANALYZER)
    assert by_type == {"gas_analyzer_0": device}


def test_device_factory_and_manager_init_do_not_eager_import_real_drivers(monkeypatch) -> None:
    calls: list[str] = []

    def _guard_import(module_name: str):
        calls.append(module_name)
        raise AssertionError(f"unexpected eager import: {module_name}")

    monkeypatch.setattr("gas_calibrator.v2.core.device_factory.import_module", _guard_import)

    factory = DeviceFactory()
    manager = DeviceManager(DeviceConfig())

    assert isinstance(factory, DeviceFactory)
    assert isinstance(manager, DeviceManager)
    assert calls == []


def test_device_factory_simulation_mode_does_not_import_real_drivers(monkeypatch) -> None:
    def _guard_import(module_name: str):
        raise AssertionError(f"simulation path should not import real driver: {module_name}")

    monkeypatch.setattr("gas_calibrator.v2.core.device_factory.import_module", _guard_import)

    factory = DeviceFactory(simulation_mode=True)
    analyzer = factory.create(DeviceType.GAS_ANALYZER, {"port": "SIM-GA1", "baud": 115200})

    assert analyzer.__class__.__name__ == "AnalyzerFake"


def test_device_factory_raises_clear_error_only_when_real_device_is_requested(monkeypatch) -> None:
    def _missing_dependency(module_name: str):
        raise ModuleNotFoundError("No module named 'serial'", name="serial")

    monkeypatch.setattr("gas_calibrator.v2.core.device_factory.import_module", _missing_dependency)

    factory = DeviceFactory()

    with pytest.raises(DeviceDriverImportError) as exc_info:
        factory.create(DeviceType.PRESSURE_CONTROLLER, {"port": "COM1", "baud": 9600})

    message = str(exc_info.value)
    assert "pressure_controller" in message
    assert "serial" in message
    assert "simulation_mode/offline helpers" in message
