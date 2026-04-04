import json
from types import SimpleNamespace

from gas_calibrator.v2.scripts import test_v2_safe
from gas_calibrator.v2.sim.devices import GRZ5013Fake, RelayFake, TemperatureChamberFake, ThermometerFake


class _FakeManager:
    def __init__(self, devices):
        self._devices = devices
        self._device_info = {
            name: SimpleNamespace(port=f"SIM-{index + 1}")
            for index, name in enumerate(devices.keys())
        }

    def get_device(self, name: str):
        return self._devices[name]

    def open_all(self):
        return {name: True for name in self._devices}

    def health_check(self):
        return {name: True for name in self._devices}

    def close_all(self) -> None:
        return None


def test_is_simulated_device_accepts_protocol_fakes() -> None:
    assert test_v2_safe._is_simulated_device(GRZ5013Fake())
    assert test_v2_safe._is_simulated_device(TemperatureChamberFake())
    assert test_v2_safe._is_simulated_device(RelayFake())
    assert test_v2_safe._is_simulated_device(ThermometerFake())


def test_connection_test_treats_protocol_fakes_as_simulated(monkeypatch) -> None:
    devices = {
        "humidity_generator": GRZ5013Fake(),
        "temperature_chamber": TemperatureChamberFake(),
        "relay": RelayFake(),
        "thermometer": ThermometerFake(),
    }
    fake_service = SimpleNamespace(
        orchestrator=SimpleNamespace(_create_devices=lambda: None),
        device_manager=_FakeManager(devices),
    )
    monkeypatch.setattr(test_v2_safe, "_create_mainline_service", lambda raw_cfg, runtime_cfg: fake_service)

    result = test_v2_safe._connection_test({}, SimpleNamespace())

    assert result["passed"] is True
    assert result["all_simulated"] is True
    assert result["devices"]["humidity_generator"]["simulated"] is True
    assert result["devices"]["temperature_chamber"]["simulated"] is True
    assert result["devices"]["relay"]["simulated"] is True
    assert result["devices"]["thermometer"]["simulated"] is True


def test_write_report_adds_step2_evidence_boundary(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(test_v2_safe, "OUTPUT_ROOT", tmp_path)

    report_path = test_v2_safe._write_report({"overall_passed": True, "simulation_mode": True})
    payload = json.loads(report_path.read_text(encoding="utf-8"))

    assert payload["evidence_source"] == "simulated_protocol"
    assert payload["not_real_acceptance_evidence"] is True
    assert payload["acceptance_level"] == "offline_regression"
    assert payload["promotion_state"] == "dry_run_only"


def test_write_report_backfills_config_safety_review(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(test_v2_safe, "OUTPUT_ROOT", tmp_path)

    report_path = test_v2_safe._write_report(
        {
            "overall_passed": True,
            "simulation_mode": True,
            "config_safety": {
                "classification": "simulation_real_port_inventory_risk",
                "summary": "safe suite safety",
                "execution_gate": {"status": "blocked"},
            },
        }
    )
    payload = json.loads(report_path.read_text(encoding="utf-8"))

    assert payload["config_safety"]["classification"] == "simulation_real_port_inventory_risk"
    assert payload["config_safety_review"]["execution_gate"]["status"] == "blocked"
    assert payload["config_safety_review"]["summary"]
