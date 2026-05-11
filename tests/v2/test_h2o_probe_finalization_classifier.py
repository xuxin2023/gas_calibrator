from __future__ import annotations

from gas_calibrator.v2.core.run001_h2o_only_1_point_no_write_probe import (
    _classify_downstream_engineering_green,
)


def _completed_service_summary(**overrides) -> dict:
    base = {
        "points_completed": 8,
        "sample_count": 32,
        "route_completed": True,
        "pressure_completed": True,
        "wait_gate_completed": True,
        "sample_completed": True,
        "service_status_phase": "completed",
        "service_status_error": "",
    }
    base.update(overrides)
    return base


def _clean_no_write_guard() -> dict:
    return {
        "attempted_write_count": 0,
        "identity_write_command_sent": False,
        "persistent_write_command_sent": False,
    }


def test_completed_downstream_summary_overrides_stale_readiness_failed():
    summary = _completed_service_summary()
    result = _classify_downstream_engineering_green(summary, _clean_no_write_guard(), False)
    assert result is True


def test_calibration_completed_is_not_fail_reason():
    summary = _completed_service_summary(
        service_status_message="Calibration completed",
    )
    result = _classify_downstream_engineering_green(summary, _clean_no_write_guard(), False)
    assert result is True


def test_points_completed_below_7_returns_false():
    summary = _completed_service_summary(points_completed=6)
    result = _classify_downstream_engineering_green(summary, _clean_no_write_guard(), False)
    assert result is False


def test_sample_count_zero_returns_false():
    summary = _completed_service_summary(sample_count=0, sample_completed=False)
    result = _classify_downstream_engineering_green(summary, _clean_no_write_guard(), False)
    assert result is False


def test_route_not_completed_returns_false():
    summary = _completed_service_summary(route_completed=False)
    result = _classify_downstream_engineering_green(summary, _clean_no_write_guard(), False)
    assert result is False


def test_pressure_not_completed_returns_false():
    summary = _completed_service_summary(pressure_completed=False)
    result = _classify_downstream_engineering_green(summary, _clean_no_write_guard(), False)
    assert result is False


def test_wait_gate_not_completed_returns_false():
    summary = _completed_service_summary(wait_gate_completed=False)
    result = _classify_downstream_engineering_green(summary, _clean_no_write_guard(), False)
    assert result is False


def test_sample_not_completed_returns_false():
    summary = _completed_service_summary(sample_completed=False)
    result = _classify_downstream_engineering_green(summary, _clean_no_write_guard(), False)
    assert result is False


def test_service_status_phase_not_completed_returns_false():
    summary = _completed_service_summary(service_status_phase="running")
    result = _classify_downstream_engineering_green(summary, _clean_no_write_guard(), False)
    assert result is False


def test_service_status_error_present_returns_false():
    summary = _completed_service_summary(service_status_error="some error")
    result = _classify_downstream_engineering_green(summary, _clean_no_write_guard(), False)
    assert result is False


def test_no_write_guard_attempted_write_returns_false():
    summary = _completed_service_summary()
    ng = {"attempted_write_count": 1}
    result = _classify_downstream_engineering_green(summary, ng, False)
    assert result is False


def test_any_write_command_sent_true_returns_false():
    summary = _completed_service_summary()
    result = _classify_downstream_engineering_green(summary, _clean_no_write_guard(), True)
    assert result is False


def test_no_write_still_required_for_engineering_green():
    summary = _completed_service_summary()
    result_clean = _classify_downstream_engineering_green(summary, _clean_no_write_guard(), False)
    assert result_clean is True

    result_write_attempted = _classify_downstream_engineering_green(summary, {"attempted_write_count": 1}, False)
    assert result_write_attempted is False

    result_write_sent = _classify_downstream_engineering_green(summary, _clean_no_write_guard(), True)
    assert result_write_sent is False


def test_none_service_summary_returns_false():
    result = _classify_downstream_engineering_green(None, {}, False)
    assert result is False


def test_not_real_acceptance_markers_preserved():
    summary = _completed_service_summary(
        service_status_message="Calibration completed",
    )
    result = _classify_downstream_engineering_green(summary, _clean_no_write_guard(), False)
    assert result is True
