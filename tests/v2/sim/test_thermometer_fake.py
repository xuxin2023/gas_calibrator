from __future__ import annotations

from gas_calibrator.v2.core.simulated_devices import SimulationPlantState
from gas_calibrator.v2.sim.devices import ThermometerFake


def test_thermometer_fake_emits_continuous_ascii_frames() -> None:
    fake = ThermometerFake(temperature_c=25.12)

    line = fake.readline()
    burst = fake.read_available()

    assert fake.baudrate == 2400
    assert fake.bytesize == 8
    assert fake.stopbits == 1.0
    assert fake.parity == "N"
    assert line.endswith("\r\n")
    assert "\N{DEGREE SIGN}C" in line
    assert fake.parse_line(line)["ok"] is True
    assert burst.count("\N{DEGREE SIGN}C") >= 3


def test_thermometer_fake_modes_stable_drift_stale_and_plus_200_are_repeatable() -> None:
    plant = SimulationPlantState(temperature_c=20.0, target_temperature_c=20.0)
    stable = ThermometerFake(plant_state=plant, mode="stable")
    drift = ThermometerFake(mode="drift", temperature_c=20.0, drift_step_c=0.5)
    stale = ThermometerFake(mode="stale", temperature_c=21.5)
    plus_200 = ThermometerFake(mode="plus_200_mode", temperature_c=25.0)

    plant.temperature_c = 23.0

    assert stable.read_temp_c() == 23.0
    assert stable.read_temp_c() == 23.0
    assert drift.read_temp_c() < drift.read_temp_c()
    assert stale.read_temp_c() == 21.5
    assert stale.read_temp_c() == 21.5
    raw_line = plus_200.readline()
    assert "+225.00" in raw_line
    assert plus_200.parse_line(raw_line)["temp_c"] == 25.0


def test_thermometer_fake_warmup_unstable_and_no_response() -> None:
    warmup = ThermometerFake(mode="warmup_unstable", temperature_c=25.0)
    no_response = ThermometerFake(mode="no_response")

    readings = [warmup.read_temp_c() for _ in range(4)]

    assert len({round(value or 0.0, 2) for value in readings}) > 1
    assert warmup.status()["thermometer_reference_status"] == "warmup_unstable"
    assert no_response.readline() == ""
    assert no_response.read_temp_c() is None


def test_thermometer_fake_read_temp_c_is_parsed_from_stream() -> None:
    fake = ThermometerFake(temperature_c=24.56)

    payload = fake.read_current()
    value = fake.read_temp_c()

    assert payload["ok"] is True
    assert payload["temp_c"] == 24.56
    assert value == 24.56


def test_thermometer_fake_corrupted_and_truncated_ascii_fail_cleanly() -> None:
    corrupted = ThermometerFake(mode="corrupted_ascii", temperature_c=25.0)
    truncated = ThermometerFake(mode="truncated_ascii", temperature_c=25.0)

    corrupted_line = corrupted.readline()
    truncated_line = truncated.readline()

    assert corrupted.parse_line(corrupted_line)["ok"] is False
    assert truncated.parse_line(truncated_line)["ok"] is False
    assert corrupted.read_temp_c() is None
    assert truncated.read_temp_c() is None
