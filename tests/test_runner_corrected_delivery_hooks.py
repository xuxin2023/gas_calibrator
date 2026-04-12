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


def test_run_calls_startup_pressure_work_when_ambient_only_selected(tmp_path: Path, monkeypatch) -> None:
    points_path = tmp_path / "points.xlsx"
    points_path.write_text("stub", encoding="utf-8")
    cfg = {
        "paths": {"points_excel": str(points_path)},
        "workflow": {
            "selected_pressure_points": ["ambient"],
            "startup_pressure_precheck": {"enabled": True},
            "startup_pressure_sensor_calibration": {"enabled": True},
            "postrun_corrected_delivery": {"enabled": False},
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

    runner.run()

    assert order.index("startup_preflight_reset") < order.index("startup_pressure_precheck") < order.index("startup_pressure_sensor_calibration") < order.index("run_points")
    logger.close()


def test_postrun_corrected_delivery_passes_pressure_handoff_options(tmp_path: Path, monkeypatch) -> None:
    from gas_calibrator.tools import run_v1_corrected_autodelivery as tool_module

    logger = RunLogger(tmp_path)
    cfg = {
        "workflow": {
            "postrun_corrected_delivery": {
                "enabled": True,
                "strict": True,
                "write_devices": True,
                "verify_report": False,
                "verification_template": "",
                "fallback_pressure_to_controller": False,
                "pressure_row_source": "startup_calibration",
                "write_pressure_coefficients": False,
                "verify_short_run": {
                    "enabled": True,
                    "temp_c": 20.0,
                    "skip_co2_ppm": [500],
                    "enable_connect_check": False,
                    "points_excel": "configs/points_tiny_short_run_20c_even500.xlsx",
                },
            }
        }
    }
    runner = CalibrationRunner(cfg, {}, logger, lambda *_: None, lambda *_: None)
    captured: dict[str, object] = {}

    class _StubTool:
        @staticmethod
        def run_from_cli(**kwargs):
            captured.update(kwargs)
            return {"output_dir": str(tmp_path / "done")}

    monkeypatch.setattr(tool_module, "run_from_cli", _StubTool.run_from_cli)
    runner._maybe_run_postrun_corrected_delivery()

    assert captured["pressure_row_source"] == "startup_calibration"
    assert captured["write_pressure_coefficients"] is False
    assert captured["verify_short_run_cfg"]["enabled"] is True
    assert captured["verify_short_run_cfg"]["temp_c"] == 20.0
    assert captured["verify_short_run_cfg"]["skip_co2_ppm"] == [500]
    logger.close()
