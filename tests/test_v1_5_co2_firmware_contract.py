from __future__ import annotations

import pytest

from gas_calibrator.validation.co2_firmware_contract import (
    co2_dry_route_h2o_status,
    co2_firmware_final_to_raw_ppm,
    co2_h2o_dry_correction_factor,
    co2_raw_to_firmware_final_ppm,
    co2_senco13_raw_ppm,
    co2_senco3_temperature_compensation_ppm,
)


def test_co2_h2o_dry_correction_matches_recorded_firmware_output() -> None:
    # Device 033 in the 2026-05-29 900 ppm replay:
    # raw SENCO1/3 ~= 897.2176, analyzer H2O ~= 25.3819 mmol/mol,
    # displayed firmware CO2 ~= 920.5776 ppm.
    predicted = co2_raw_to_firmware_final_ppm(897.2176, 25.3819)

    assert predicted == pytest.approx(920.5838, abs=0.01)


def test_co2_firmware_final_can_be_reduced_back_to_raw_senco13_layer() -> None:
    raw = co2_firmware_final_to_raw_ppm(961.1663, 72.0)

    assert raw == pytest.approx(891.9623, abs=0.01)


def test_dry_route_h2o_status_separates_low_high_and_blocking_bias() -> None:
    assert co2_dry_route_h2o_status(3.9) == "h2o_low_for_dry_co2_route"
    assert co2_dry_route_h2o_status(25.0) == "h2o_high_bias_explains_co2_final_shift"
    assert co2_dry_route_h2o_status(72.0) == "h2o_severe_bias_blocks_co2_acceptance"
    assert co2_h2o_dry_correction_factor(1000.0) is None


def test_senco3_temperature_compensation_uses_absolute_kelvin_terms() -> None:
    # The preserved SENCO3 terms are part of the raw firmware CO2 layer even
    # when only SENCO1 is being recalculated. Temperature is absolute Kelvin,
    # not Celsius or delta-T.
    value = co2_senco3_temperature_compensation_ppm(
        [20.6216, 0.0235468, -22.1346, 0.0, 0.0, 0.0],
        ratio=1.32,
        temperature_c=22.91,
        pressure_hpa=1002.4,
    )

    assert value == pytest.approx(-481.04, abs=0.05)


def test_senco13_raw_layer_is_primary_ratio_plus_preserved_senco3() -> None:
    raw = co2_senco13_raw_ppm(
        [26103.4152, -50713.0054, 32137.8827, -6150.50246, 0.0, 0.0],
        [20.6216, 0.0235468, -22.1346, 0.0, 0.0, 0.0],
        ratio=1.3187,
        temperature_c=22.91,
        pressure_hpa=1002.4,
    )

    assert raw == pytest.approx(538.25, abs=0.05)
