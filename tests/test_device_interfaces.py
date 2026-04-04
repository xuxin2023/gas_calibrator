from gas_calibrator.devices.dewpoint_meter import DewpointMeter
from gas_calibrator.devices.gas_analyzer import GasAnalyzer
from gas_calibrator.devices.humidity_generator import HumidityGenerator
from gas_calibrator.devices.pace5000 import Pace5000
from gas_calibrator.devices.paroscientific import ParoscientificGauge
from gas_calibrator.devices.relay import RelayController
from gas_calibrator.devices.serial_base import ReplaySerial
from gas_calibrator.devices.temperature_chamber import TemperatureChamber
from gas_calibrator.devices.thermometer import Thermometer


class _FakeModbusClient:
    def connect(self):
        return True

    def close(self):
        return None

    def read_coils(self, address, count, **kwargs):
        class _Resp:
            bits = [False] * count

            def isError(self):
                return False

        return _Resp()

    def write_coil(self, address, value, **kwargs):
        class _Resp:
            def isError(self):
                return False

        return _Resp()

    def read_input_registers(self, address, count, **kwargs):
        class _Resp:
            registers = [0]

            def isError(self):
                return False

        return _Resp()

    def write_register(self, address, value, **kwargs):
        class _Resp:
            def isError(self):
                return False

        return _Resp()


def test_all_device_drivers_expose_unified_interface_methods() -> None:
    replay_factory = lambda **_: ReplaySerial()
    instances = [
        GasAnalyzer("COM1", serial_factory=replay_factory),
        DewpointMeter("COM1", serial_factory=replay_factory),
        HumidityGenerator("COM1", serial_factory=replay_factory),
        Pace5000("COM1", serial_factory=replay_factory),
        ParoscientificGauge("COM1", serial_factory=replay_factory),
        TemperatureChamber("COM1", client=_FakeModbusClient()),
        Thermometer("COM1", serial_factory=replay_factory),
        RelayController("COM1", client=_FakeModbusClient()),
    ]

    for dev in instances:
        for name in ("connect", "open", "read", "write", "close", "status", "selftest"):
            assert hasattr(dev, name), f"{type(dev).__name__} missing {name}"
