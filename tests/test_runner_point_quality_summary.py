from pathlib import Path

from gas_calibrator.data.points import CalibrationPoint
from gas_calibrator.logging_utils import RunLogger
from gas_calibrator.workflow.runner import CalibrationRunner


def _point_co2_low_pressure() -> CalibrationPoint:
    return CalibrationPoint(
        index=1,
        temp_chamber_c=20.0,
        co2_ppm=1000.0,
        hgen_temp_c=None,
        hgen_rh_pct=None,
        target_pressure_hpa=700.0,
        dewpoint_c=None,
        h2o_mmol=None,
        raw_h2o=None,
        co2_group="A",
    )


def test_point_quality_summary_integrates_warn_and_fail_sources(tmp_path: Path) -> None:
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(
        {
            "workflow": {
                "pressure": {
                    "co2_postseal_physical_qc_policy": "warn",
                    "preseal_trigger_overshoot_warn_hpa": 10.0,
                    "preseal_trigger_overshoot_reject_hpa": 25.0,
                },
                "sampling": {
                    "pressure_gauge_stale_ratio_warn_max": 0.2,
                    "pressure_gauge_stale_ratio_reject_max": 0.5,
                },
            }
        },
        {},
        logger,
        lambda *_: None,
        lambda *_: None,
    )
    point = _point_co2_low_pressure()
    runner._set_point_runtime_fields(
        point,
        phase="co2",
        dewpoint_gate_result="stable",
        postseal_physical_qc_status="fail",
        postseal_physical_qc_reason="abs_delta_c=0.300>max_abs_delta_c=0.200;policy=warn",
        postseal_timeout_policy="warn",
        point_quality_timeout_flag=True,
        presample_long_guard_status="warn",
        presample_long_guard_reason="timeout_elapsed_s=20.000;rise_c=0.180>max_rise_c=0.120;policy=warn",
        postsample_late_rebound_status="warn",
        postsample_late_rebound_reason="rise_c=0.500>max_rise_c=0.120;policy=warn",
        sampling_window_qc_status="fail",
        sampling_window_qc_reason="range_c=0.600>max_range_c=0.200;policy=reject",
        pressure_gauge_stale_ratio=0.75,
        preseal_trigger_overshoot_hpa=12.0,
    )

    summary = runner._update_point_quality_summary(point, phase="co2")
    logger.close()

    flags = set(filter(None, str(summary["point_quality_flags"]).split(",")))
    assert summary["point_quality_status"] == "fail"
    assert summary["point_quality_blocked"] is True
    assert {
        "postseal_physical_qc",
        "postseal_timeout",
        "presample_long_guard",
        "postsample_late_rebound",
        "sampling_window_qc",
        "pressure_gauge_stale_ratio",
        "preseal_trigger_overshoot",
    }.issubset(flags)
    assert "abs_delta_c=0.300>max_abs_delta_c=0.200;policy=warn" in summary["point_quality_reason"]
    assert "postseal_timeout(policy=warn)" in summary["point_quality_reason"]
    assert "timeout_elapsed_s=20.000;rise_c=0.180>max_rise_c=0.120;policy=warn" in summary["point_quality_reason"]
    assert "rise_c=0.500>max_rise_c=0.120;policy=warn" in summary["point_quality_reason"]
    assert "range_c=0.600>max_range_c=0.200;policy=reject" in summary["point_quality_reason"]
    assert "pressure_gauge_stale_ratio=0.750>reject_max=0.500" in summary["point_quality_reason"]
    assert "preseal_trigger_overshoot_hpa=12.000>warn_hpa=10.000" in summary["point_quality_reason"]


def test_point_quality_summary_marks_rebound_veto_as_blocked(tmp_path: Path) -> None:
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner({}, {}, logger, lambda *_: None, lambda *_: None)
    point = _point_co2_low_pressure()
    runner._set_point_runtime_fields(point, phase="co2", dewpoint_gate_result="rebound_veto")

    summary = runner._update_point_quality_summary(point, phase="co2")
    logger.close()

    assert summary["point_quality_status"] == "fail"
    assert summary["point_quality_blocked"] is True
    assert summary["point_quality_flags"] == "postseal_rebound_veto"
    assert summary["point_quality_reason"] == "dewpoint_gate_result=rebound_veto"
