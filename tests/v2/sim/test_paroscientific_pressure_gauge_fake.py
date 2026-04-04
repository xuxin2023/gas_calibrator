from __future__ import annotations

import pytest

from gas_calibrator.v2.core.simulated_devices import SimulationPlantState
from gas_calibrator.v2.sim.devices.paroscientific_fake import ParoscientificPressureGaugeFake


def test_paroscientific_pressure_gauge_fake_supports_protocol_commands() -> None:
    plant = SimulationPlantState(pressure_hpa=998.5, target_pressure_hpa=998.5)
    fake = ParoscientificPressureGaugeFake(plant_state=plant)

    fake.open()
    fake.write("*0100P3\r\n")
    echo = fake.readline()
    measurement = fake.readline()

    assert fake.baudrate == 9600
    assert fake.bytesize == 8
    assert fake.stopbits == 1.0
    assert fake.parity == "N"
    assert echo.strip() == "*0100P3"
    assert measurement.startswith("*0100")
    assert fake.read_pressure() == pytest.approx(998.5, abs=1e-6)


def test_paroscientific_pressure_gauge_fake_supports_ew_un_tu_md_and_db() -> None:
    fake = ParoscientificPressureGaugeFake()

    assert fake.handle_command("*0100DB\r\n").strip().endswith("PAROSCIENTIFIC,745,FAKE")
    assert fake.handle_command("*0100UNPSIA\r\n").strip().endswith("ERR:EW_REQUIRED")
    assert fake.handle_command("*0100EW\r\n").strip().endswith("EW,OK")
    assert fake.handle_command("*0100UNPSIA\r\n").strip().endswith("PSIA")
    assert fake.unit == "PSIA"
    assert fake.handle_command("*0100EW\r\n").strip().endswith("EW,OK")
    assert fake.handle_command("*0100TUF\r\n").strip().endswith("F")
    assert fake.temperature_unit == "F"
    assert fake.handle_command("*0100EW\r\n").strip().endswith("EW,OK")
    assert fake.handle_command("*0100MDC\r\n").strip().endswith("CONTINUOUS")
    assert fake.measurement_mode == "continuous"


def test_paroscientific_pressure_gauge_fake_supports_sample_hold_continuous_and_global_99() -> None:
    plant = SimulationPlantState(pressure_hpa=1000.0, target_pressure_hpa=1000.0)
    fake = ParoscientificPressureGaugeFake(plant_state=plant, mode="sample_hold")

    first = fake.read_pressure()
    plant.pressure_hpa = 950.0
    second = fake.read_pressure()

    assert first == pytest.approx(second, abs=1e-6)

    continuous = ParoscientificPressureGaugeFake(plant_state=plant, mode="continuous_stream")
    assert continuous.readline().startswith("*0100")
    assert "HPA" in continuous.readline()

    global_frame = continuous.handle_command("*9900P3\r\n")
    assert global_frame.startswith("*0100")


def test_paroscientific_pressure_gauge_fake_exposes_wrong_unit_and_error_modes() -> None:
    wrong_unit = ParoscientificPressureGaugeFake(mode="wrong_unit_configuration")
    unsupported = ParoscientificPressureGaugeFake(mode="unsupported_command")
    interrupted = ParoscientificPressureGaugeFake(mode="display_interrupted")
    no_response = ParoscientificPressureGaugeFake(mode="no_response")

    assert wrong_unit.status()["pressure_reference_status"] == "wrong_unit_configuration"
    assert wrong_unit.status()["unit"] == "PSIA"
    assert unsupported.handle_command("*0100P3\r\n").strip().endswith("ERR:UNSUPPORTED_COMMAND")
    assert interrupted.handle_command("*0100P3\r\n").strip().endswith("DISPLAY_INTERRUPTED")
    with pytest.raises(RuntimeError, match="NO_RESPONSE"):
        no_response.read_pressure()
