from __future__ import annotations

import time

from gas_calibrator.v2.core.simulated_devices import SimulationPlantState
from gas_calibrator.v2.sim.devices import GRZ5013Fake


def test_grz5013_fake_accepts_target_commands_and_reports_fetch_values() -> None:
    plant = SimulationPlantState()
    fake = GRZ5013Fake(plant_state=plant)

    assert fake.process_command("Target:TA=30") == "OK\r\n"
    assert fake.process_command("target:uwa=55") == "OK\r\n"
    assert fake.process_command("Target:FA=2.5") == "OK\r\n"
    assert fake.process_command("Target:CTRL=ON") == "OK\r\n"
    assert fake.process_command("Target:HEAT=ON") == "OK\r\n"
    assert fake.process_command("Target:COOL=OFF") == "OK\r\n"

    time.sleep(0.05)
    all_response = fake.process_command("FETC? (@All)")
    snapshot = fake.fetch_all()["data"]

    assert "TA=30" in all_response
    assert "UwA=55" in all_response
    assert "FA=2.5" in all_response
    assert "CTRL=ON" in all_response
    assert fake.process_command("FETC? (@Tc)").strip()
    assert fake.process_command("FETC? (@Td)").strip()
    assert fake.process_command("FETC? (@Uw)").strip()
    assert fake.process_command("FETC? (@Fl)").strip()
    assert fake.process_command("FETC? (@st)").strip() == "0"
    assert snapshot["TA"] == 30.0
    assert snapshot["UwA"] == 55.0
    assert snapshot["FA"] == 2.5
    assert plant.dynamic_protocol is True


def test_grz5013_fake_humidity_static_fault_keeps_humidity_flat_while_temperature_moves() -> None:
    fake = GRZ5013Fake(
        mode="humidity_static_fault",
        current_temp_c=20.0,
        current_rh_pct=25.0,
        target_temp_c=20.0,
        target_rh_pct=25.0,
    )
    fake.enable_control(True)
    fake.set_target_temp(35.0)
    fake.set_target_rh(70.0)

    time.sleep(0.12)
    snapshot = fake.fetch_all()["data"]

    assert snapshot["Tc"] > 20.0
    assert snapshot["Uw"] == 25.0
    assert snapshot["UwA"] == 70.0


def test_grz5013_fake_timeout_mode_never_reaches_target_humidity() -> None:
    fake = GRZ5013Fake(
        mode="timeout",
        current_temp_c=22.0,
        current_rh_pct=20.0,
        target_temp_c=22.0,
        target_rh_pct=20.0,
    )
    fake.enable_control(True)
    fake.set_target_rh(60.0)
    fake.set_flow_target(1.2)

    time.sleep(0.15)
    snapshot = fake.fetch_all()["data"]
    ensure_result = fake.ensure_run(min_flow_lpm=0.2, tries=1, wait_s=0.2, poll_s=0.02)

    assert snapshot["Uw"] < snapshot["UwA"]
    assert snapshot["Uw"] <= 55.0
    assert ensure_result["ok"] is True
    assert ensure_result["flow_lpm"] >= 0.2
