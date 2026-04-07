from __future__ import annotations

from pathlib import Path

from gas_calibrator.data.points import CalibrationPoint
from gas_calibrator.logging_utils import RunLogger
from gas_calibrator.workflow import runner as runner_module
from gas_calibrator.workflow.runner import CalibrationRunner


def _point() -> CalibrationPoint:
    return CalibrationPoint(
        index=1,
        temp_chamber_c=20.0,
        co2_ppm=0.0,
        hgen_temp_c=None,
        hgen_rh_pct=None,
        target_pressure_hpa=1000.0,
        dewpoint_c=None,
        h2o_mmol=None,
        raw_h2o=None,
    )


def test_run_calls_startup_pressure_calibration_and_postrun_delivery_in_order(tmp_path: Path, monkeypatch) -> None:
    points_path = tmp_path / "points.xlsx"
    points_path.write_text("stub", encoding="utf-8")
    cfg = {
        "paths": {"points_excel": str(points_path)},
        "workflow": {
            "startup_pressure_sensor_calibration": {"enabled": True},
            "postrun_corrected_delivery": {"enabled": True},
        },
    }
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(cfg, {}, logger, lambda *_: None, lambda *_: None)
    order: list[str] = []

    monkeypatch.setattr(runner_module, "load_points_from_excel", lambda *args, **kwargs: [_point()])
    monkeypatch.setattr(runner_module, "reorder_points", lambda points, *args, **kwargs: points)
    monkeypatch.setattr(runner_module, "validate_points", lambda points, *args, **kwargs: [])

    runner._log_data_quality_effective_config = lambda: order.append("log_cfg")
    runner._sensor_precheck = lambda: order.append("sensor_precheck")
    runner._configure_devices = lambda: order.append("configure_devices")
    runner._startup_preflight_reset = lambda: order.append("startup_preflight_reset")
    runner._startup_pressure_precheck = lambda points: order.append("startup_pressure_precheck")
    runner._startup_pressure_sensor_calibration = lambda points: order.append("startup_pressure_sensor_calibration")
    runner._run_points = lambda points: order.append("run_points")
    runner._flush_deferred_sample_exports = lambda **kwargs: order.append("flush_samples")
    runner._flush_deferred_point_exports = lambda **kwargs: order.append("flush_points")
    runner._maybe_write_coefficients = lambda: order.append("maybe_write_coefficients")
    runner._finalize_temperature_calibration_outputs = lambda: order.append("finalize_temperature")
    runner._cleanup = lambda: order.append("cleanup")
    runner._maybe_run_postrun_corrected_delivery = lambda: order.append("postrun_delivery")

    runner.run()

    assert order.index("startup_pressure_precheck") < order.index("startup_pressure_sensor_calibration") < order.index("run_points")
    assert order.index("cleanup") < order.index("postrun_delivery")
    logger.close()
