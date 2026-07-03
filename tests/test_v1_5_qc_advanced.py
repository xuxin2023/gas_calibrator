from gas_calibrator.v1_5.qc_advanced.control_charts import build_control_chart
from gas_calibrator.v1_5.qc_advanced.factory_signal_health import evaluate_factory_signal_health
from gas_calibrator.v1_5.qc_advanced.humidity_diagnostics import classify_humidity_behavior
from gas_calibrator.v1_5.qc_advanced.pressure_trend import evaluate_pressure_trend
from gas_calibrator.v1_5.qc_advanced.root_cause_classifier import classify_root_cause
from gas_calibrator.v1_5.qc_advanced.steady_state_selector import select_steady_state_window
from gas_calibrator.v1_5.qc_advanced.uncertainty_budget import build_uncertainty_budget


def _row(index: int, **overrides):
    row = {
        "co2_ppm": 900.0 + index * 0.01,
        "h2o_mmol": 0.5 + index * 0.001,
        "dewpoint_c": -30.0 + index * 0.001,
        "h2o_dry_ppmv": 500.0,
        "h2o_wet_ppmv": 505.0,
        "com22_pressure_hpa": 1000.0,
        "analyzer_pressure_hpa": 1000.1,
        "analyzer_minus_com22_hpa": 0.1,
        "pace_minus_com22_hpa": 0.2,
        "co2_ratio": 1.3 + index * 0.00001,
        "h2o_ratio": 0.7 + index * 0.00001,
        "ref_signal": 3300.0,
        "chamber_temp_c": 25.0,
        "case_temp_c": 25.5,
    }
    row.update(overrides)
    return row


def test_steady_state_selector_chooses_low_variation_window():
    drifting = [_row(i, co2_ppm=900 + i * 2.0) for i in range(10)]
    stable = [_row(10 + i, co2_ppm=920.0 + i * 0.01) for i in range(10)]

    result = select_steady_state_window(drifting + stable, window_size=10)

    assert result["status"] == "pass"
    assert result["start_index"] == 10
    assert result["sample_count"] == 10


def test_humidity_diagnostics_distinguishes_pressure_effect_from_real_moisture():
    pressure_effect = [_row(i, dewpoint_c=-30 + i * 0.1, h2o_dry_ppmv=500.0) for i in range(10)]
    real_moisture = [_row(i, dewpoint_c=-30 + i * 0.1, h2o_dry_ppmv=500 + i * 0.2) for i in range(10)]

    assert classify_humidity_behavior(pressure_effect)["classification"] == "pressure_effect_possible"
    real = classify_humidity_behavior(real_moisture)
    assert real["classification"] == "real_moisture_release"
    assert real["status"] == "fail"


def test_factory_signal_health_flags_compensation_and_ref_signal_risks():
    rows = [
        _row(i, co2_ppm=900 + i * 0.2, co2_ratio=1.3, ref_signal=3300 + i * 2.0)
        for i in range(10)
    ]
    result = evaluate_factory_signal_health(rows)

    assert result["status"] == "review"
    assert "co2_pressure_or_temperature_compensation_suspect" in result["findings"]
    assert "optical_reference_signal_drift" in result["findings"]


def test_pressure_trend_and_control_chart_explain_history():
    pressure = evaluate_pressure_trend([_row(i, analyzer_minus_com22_hpa=2.0 + i * 0.1) for i in range(10)])
    chart = build_control_chart([0] * 20 + [10])

    assert pressure["status"] == "review"
    assert "analyzer_pressure_mean_bias_exceeds_limit" in pressure["reasons"]
    assert any(point["violation"] for point in chart["points"])


def test_uncertainty_budget_and_root_cause_classifier():
    budget = build_uncertainty_budget(
        [
            {"input_quantity": "standard_gas_certificate", "standard_uncertainty": 0.9, "sensitivity_coefficient": 1.0},
            {"input_quantity": "repeatability", "standard_uncertainty": 0.1, "sensitivity_coefficient": 1.0},
        ]
    )
    root = classify_root_cause(
        humidity={"classification": "real_moisture_release"},
        factory_signal={"findings": ["optical_reference_signal_drift"]},
        pressure_trend={"reasons": ["analyzer_pressure_mean_bias_exceeds_limit"]},
    )

    assert budget["status"] == "released"
    assert budget["expanded_uncertainty"] is not None
    assert root["status"] == "reject_point"
    assert "real_moisture_release" in root["root_cause_codes"]
    assert "ref_signal" in root["summary"]
