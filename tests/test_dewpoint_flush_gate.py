from __future__ import annotations

from datetime import datetime, timedelta

from gas_calibrator.validation.dewpoint_flush_gate import (
    dewpoint_to_h2o_mmol_per_mol,
    evaluate_dewpoint_flush_gate,
    predict_pressure_scaled_dewpoint_c,
)


def _row(ts: datetime, elapsed_s: float, dewpoint_c: float) -> dict[str, object]:
    return {
        "timestamp": ts.isoformat(timespec="seconds"),
        "phase_elapsed_s": elapsed_s,
        "controller_vent_state": "VENT_ON",
        "dewpoint_c": dewpoint_c,
    }


def test_evaluate_dewpoint_flush_gate_never_passes_before_min_flush() -> None:
    start = datetime(2026, 4, 3, 9, 0, 0)
    rows = [_row(start + timedelta(seconds=step * 10), float(step * 10), -30.0) for step in range(6)]

    gate = evaluate_dewpoint_flush_gate(
        rows,
        min_flush_s=60.0,
        gate_window_s=60.0,
    )

    assert gate["gate_pass"] is False
    assert gate["gate_status"] == "waiting"
    assert "flush_duration_below_min" in str(gate["gate_reason"])


def test_evaluate_dewpoint_flush_gate_waits_when_tail_is_not_stable() -> None:
    start = datetime(2026, 4, 3, 9, 0, 0)
    rows = [
        _row(start + timedelta(seconds=idx * 10), 60.0 + float(idx * 10), -30.0 + idx * 0.25)
        for idx in range(7)
    ]

    gate = evaluate_dewpoint_flush_gate(
        rows,
        min_flush_s=60.0,
        gate_window_s=60.0,
        max_tail_span_c=0.2,
        max_abs_tail_slope_c_per_s=0.002,
    )

    assert gate["gate_pass"] is False
    assert "dewpoint_tail_span_too_large" in str(gate["gate_reason"])


def test_evaluate_dewpoint_flush_gate_passes_after_min_flush_when_tail_is_stable() -> None:
    start = datetime(2026, 4, 3, 9, 0, 0)
    rows = [
        _row(start + timedelta(seconds=idx * 10), 60.0 + float(idx * 10), -30.0 + idx * 0.005)
        for idx in range(7)
    ]

    gate = evaluate_dewpoint_flush_gate(
        rows,
        min_flush_s=60.0,
        gate_window_s=60.0,
        max_tail_span_c=0.2,
        max_abs_tail_slope_c_per_s=0.002,
    )

    assert gate["gate_pass"] is True
    assert gate["gate_status"] == "pass"
    assert float(gate["dewpoint_time_to_gate"]) >= 60.0


def test_evaluate_dewpoint_flush_gate_reports_rebound_when_enabled() -> None:
    start = datetime(2026, 4, 3, 9, 0, 0)
    rows = [
        _row(start + timedelta(seconds=idx * 10), 60.0 + float(idx * 10), dewpoint_c)
        for idx, dewpoint_c in enumerate((-30.0, -30.2, -29.0, -28.9, -28.9, -28.9, -28.9))
    ]

    gate = evaluate_dewpoint_flush_gate(
        rows,
        min_flush_s=60.0,
        gate_window_s=60.0,
        max_tail_span_c=2.0,
        max_abs_tail_slope_c_per_s=1.0,
        rebound_window_s=120.0,
        rebound_min_rise_c=0.8,
        include_rebound_in_gate=True,
    )

    assert gate["gate_pass"] is False
    assert gate["dewpoint_rebound_detected"] is True
    assert "dewpoint_rebound_detected" in str(gate["gate_reason"])


def test_predict_pressure_scaled_dewpoint_c_preserves_h2o_mole_fraction() -> None:
    preseal_dewpoint_c = -18.0
    preseal_pressure_hpa = 1140.0
    target_pressure_hpa = 700.0

    predicted_dewpoint_c = predict_pressure_scaled_dewpoint_c(
        preseal_dewpoint_c,
        preseal_pressure_hpa,
        target_pressure_hpa,
    )

    assert predicted_dewpoint_c is not None
    expected_mmol = dewpoint_to_h2o_mmol_per_mol(preseal_dewpoint_c, preseal_pressure_hpa)
    actual_mmol = dewpoint_to_h2o_mmol_per_mol(predicted_dewpoint_c, target_pressure_hpa)
    assert expected_mmol is not None
    assert actual_mmol is not None
    assert abs(expected_mmol - actual_mmol) <= 1e-6
