from __future__ import annotations

import time

from gas_calibrator.v2.core.simulated_devices import SimulationPlantState
from gas_calibrator.v2.sim.devices import TemperatureChamberFake


def test_temp_chamber_fake_modbus_write_and_read_registers() -> None:
    plant = SimulationPlantState()
    fake = TemperatureChamberFake(
        plant_state=plant,
        temperature_c=20.0,
        humidity_pct=30.0,
        target_temperature_c=20.0,
        target_humidity_pct=30.0,
    )

    fake.open()
    fake.write_register(fake.REG_SET_TEMP, TemperatureChamberFake._encode_signed_tenth(35.0))
    fake.write_register(fake.REG_SET_RH, 450)
    fake.write_register(fake.REG_CONTROL_TYPE, 2)
    fake.write_coil(fake.COIL_START, True)

    set_temp = fake.read_holding_registers(fake.REG_SET_TEMP, 1)
    set_rh = fake.read_holding_registers(fake.REG_SET_RH, 1)
    run_state = fake.read_input_registers(fake.REG_RUN_STATUS, 1)

    assert set_temp.isError() is False
    assert set_rh.registers == [450]
    assert TemperatureChamberFake._decode_signed_tenth(set_temp.registers[0]) == 35.0
    assert run_state.registers == [1]
    assert fake.control_type == 2
    assert plant.dynamic_protocol is True


def test_temp_chamber_fake_reaches_on_target_after_ramp_and_soak() -> None:
    fake = TemperatureChamberFake(
        mode="ramp_to_target",
        temperature_c=20.0,
        target_temperature_c=20.0,
        ramp_rate_c_per_s=200.0,
        soak_s=0.05,
    )
    fake.start()
    fake.set_temp_c(30.0)

    time.sleep(0.03)
    first_phase = fake.read()["phase"]
    time.sleep(0.07)
    second_phase = fake.read()["phase"]
    time.sleep(0.08)
    final_state = fake.read()

    assert first_phase in {"ramp_to_target", "soak_pending"}
    assert second_phase in {"soak_pending", "on_target"}
    assert final_state["phase"] == "on_target"
    assert abs(final_state["temp_c"] - 30.0) <= 0.2


def test_temp_chamber_fake_stalled_and_alarm_modes_are_reported() -> None:
    stalled = TemperatureChamberFake(
        mode="stalled",
        temperature_c=20.0,
        target_temperature_c=20.0,
        ramp_rate_c_per_s=50.0,
    )
    stalled.start()
    stalled.set_temp_c(35.0)
    time.sleep(0.08)
    stalled_state = stalled.read()

    alarm = TemperatureChamberFake(mode="alarm")
    alarm.start()
    alarm_state = alarm.status()

    assert stalled_state["phase"] == "stalled"
    assert stalled_state["temp_c"] < 35.0
    assert alarm.read_run_state() == 2
    assert alarm_state["ok"] is False
