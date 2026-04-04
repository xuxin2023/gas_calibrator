from typing import Any

from gas_calibrator.v2.config import DeviceConfig
from gas_calibrator.v2.core.device_manager import DeviceManager, DeviceStatus


class FakeDevice:
    def __init__(
        self,
        *,
        port: str = "COM1",
        open_exception: Exception | None = None,
        connect_result: bool | None = None,
        selftest_result: Any = None,
        selftest_exception: Exception | None = None,
        status_result: Any = None,
    ) -> None:
        self.port = port
        self.open_exception = open_exception
        self.connect_result = connect_result
        self.selftest_result = selftest_result
        self.selftest_exception = selftest_exception
        self.status_result = status_result
        self.open_calls = 0
        self.close_calls = 0

    def open(self) -> None:
        self.open_calls += 1
        if self.open_exception is not None:
            raise self.open_exception

    def connect(self) -> bool | None:
        return self.connect_result

    def close(self) -> None:
        self.close_calls += 1

    def selftest(self) -> Any:
        if self.selftest_exception is not None:
            raise self.selftest_exception
        if self.selftest_result is not None:
            return self.selftest_result
        return {"ok": True}

    def status(self) -> Any:
        if self.status_result is not None:
            return self.status_result
        return {"is_open": True}


def _make_config() -> DeviceConfig:
    return DeviceConfig.from_dict(
        {
            "pressure_controller": {"port": "COM1", "enabled": True},
            "pressure_meter": {"port": "COM2", "enabled": True},
            "relay_a": {"port": "COM3", "enabled": False},
            "gas_analyzers": [
                {"port": "COM4", "enabled": True},
                {"port": "COM5", "enabled": True},
            ],
        }
    )


def test_open_all_opens_registered_enabled_devices() -> None:
    manager = DeviceManager(_make_config())
    pressure_controller = FakeDevice(port="COM1")
    pressure_meter = FakeDevice(port="COM2")

    manager.register_device("pressure_controller", pressure_controller)
    manager.register_device("pressure_meter", pressure_meter)

    results = manager.open_all()

    assert results["pressure_controller"] is True
    assert results["pressure_meter"] is True
    assert manager.get_status("pressure_controller") is DeviceStatus.ONLINE
    assert manager.get_status("pressure_meter") is DeviceStatus.ONLINE
    assert manager.get_status("relay_a") is DeviceStatus.DISABLED
    assert manager.get_status("gas_analyzer_0") is DeviceStatus.OFFLINE


def test_close_all_marks_enabled_devices_offline() -> None:
    manager = DeviceManager(_make_config())
    device = FakeDevice(port="COM1")
    manager.register_device("pressure_controller", device)
    manager.open_all()

    manager.close_all()

    assert device.close_calls == 1
    assert manager.get_status("pressure_controller") is DeviceStatus.OFFLINE


def test_context_manager_opens_and_closes_devices() -> None:
    manager = DeviceManager(_make_config())
    device = FakeDevice(port="COM1")
    manager.register_device("pressure_controller", device)

    with manager as active_manager:
        assert active_manager.get_status("pressure_controller") is DeviceStatus.ONLINE
        assert device.open_calls == 1

    assert device.close_calls == 1
    assert manager.get_status("pressure_controller") is DeviceStatus.OFFLINE


def test_disable_and_enable_device_supports_recovery() -> None:
    manager = DeviceManager(_make_config())
    device = FakeDevice(port="COM1")
    manager.register_device("pressure_controller", device)
    manager.open_all()

    manager.disable_device("pressure_controller", "manual isolation")

    assert manager.get_status("pressure_controller") is DeviceStatus.DISABLED
    assert manager.get_info("pressure_controller").error_message == "manual isolation"

    enabled = manager.enable_device("pressure_controller")

    assert enabled is True
    assert manager.get_status("pressure_controller") is DeviceStatus.ONLINE
    assert device.open_calls == 2


def test_enable_device_returns_false_when_open_fails() -> None:
    manager = DeviceManager(_make_config())
    device = FakeDevice(port="COM1", open_exception=RuntimeError("open failed"))
    manager.register_device("pressure_controller", device)

    enabled = manager.enable_device("pressure_controller")

    assert enabled is False
    assert manager.get_status("pressure_controller") is DeviceStatus.ERROR
    assert "open failed" in (manager.get_info("pressure_controller").error_message or "")


def test_health_check_uses_selftest_result() -> None:
    manager = DeviceManager(_make_config())
    device = FakeDevice(port="COM1", selftest_result={"ok": True})
    manager.register_device("pressure_controller", device)

    results = manager.health_check()

    assert results["pressure_controller"] is True
    assert manager.get_status("pressure_controller") is DeviceStatus.ONLINE


def test_health_check_marks_device_offline_when_selftest_reports_false() -> None:
    manager = DeviceManager(_make_config())
    device = FakeDevice(port="COM1", selftest_result={"is_open": False})
    manager.register_device("pressure_controller", device)

    results = manager.health_check()

    assert results["pressure_controller"] is False
    assert manager.get_status("pressure_controller") is DeviceStatus.OFFLINE


def test_health_check_marks_error_on_exception() -> None:
    manager = DeviceManager(_make_config())
    device = FakeDevice(port="COM1", selftest_exception=RuntimeError("probe failed"))
    manager.register_device("pressure_controller", device)

    results = manager.health_check()

    assert results["pressure_controller"] is False
    assert manager.get_status("pressure_controller") is DeviceStatus.ERROR
    assert "probe failed" in (manager.get_info("pressure_controller").error_message or "")


def test_get_device_returns_registered_instance() -> None:
    manager = DeviceManager(_make_config())
    device = FakeDevice(port="COM1")
    manager.register_device("pressure_controller", device)

    assert manager.get_device("pressure_controller") is device
    assert manager.get_device("missing") is None
    assert manager.get_status("missing") is DeviceStatus.UNKNOWN
