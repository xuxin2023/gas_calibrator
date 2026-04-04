from __future__ import annotations

import time

import pytest

from gas_calibrator.v2.core.simulated_devices import SimulationPlantState
from gas_calibrator.v2.sim.devices import PACE5000Fake


def test_pace_fake_switches_output_isolation_and_pressure_state() -> None:
    plant = SimulationPlantState()
    fake = PACE5000Fake(plant_state=plant, current_pressure_hpa=1013.25, target_pressure_hpa=1013.25)

    fake.open()
    fake.set_units_hpa()
    fake.set_setpoint(900.0)
    fake.enable_control_output()
    time.sleep(0.12)
    pressure = fake.read_pressure()

    assert fake.get_output_state() == 1
    assert fake.get_isolation_state() == 1
    assert pressure < 1013.25
    assert plant.dynamic_protocol is True

    fake.enter_atmosphere_mode(timeout_s=1.0, poll_s=0.02)
    assert fake.get_output_state() == 0
    assert fake.get_vent_status() == fake.VENT_STATUS_IDLE


def test_pace_fake_supports_unit_conversion_and_scpi_error_queue() -> None:
    fake = PACE5000Fake(mode="unsupported_header", unsupported_headers=[":UNIT:CONV?"])

    fake.process_command("*CLS")
    response = fake.process_command(":UNIT:CONV?")
    error = fake.process_command(":SYST:ERR?")

    assert response.startswith('-113')
    assert error.startswith('-113')
    assert fake.process_command(":SYST:ERR?").startswith('0,')


def test_pace_fake_cleanup_no_response_fault_is_repeatable() -> None:
    fake = PACE5000Fake(mode="cleanup_no_response")

    fake.set_setpoint(920.0)
    fake.enable_control_output()

    with pytest.raises(RuntimeError, match="NO_RESPONSE"):
        fake.enter_atmosphere_mode(timeout_s=1.0, poll_s=0.02)
