import json
from types import SimpleNamespace

from gas_calibrator.v2.scripts import test_v2_safe
from gas_calibrator.v2.scripts._cli_safety import build_step2_cli_safety_lines
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


def test_cli_safety_lines_keep_step2_gate_readable() -> None:
    lines = build_step2_cli_safety_lines(
        {
            "classification": "simulation_real_port_inventory_risk",
            "simulation_only": True,
            "real_port_device_count": 1,
            "engineering_only_flag_count": 0,
            "execution_gate": {
                "status": "blocked",
                "requires_dual_unlock": True,
                "allow_unsafe_step2_config_flag": False,
                "allow_unsafe_step2_config_env": False,
                "summary": "Step 2 默认工作流已拦截当前配置；必须显式双重解锁。",
            },
        }
    )

    assert any("[Step2 safety]" in line for line in lines)
    assert any("[Step2 gate]" in line and "requires_dual_unlock=true" in line for line in lines)
    assert any("[Step2 boundary]" in line and "real_com=1" in line for line in lines)
