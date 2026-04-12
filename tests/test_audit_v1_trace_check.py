import csv
import inspect
import types
from pathlib import Path

from gas_calibrator.data.points import CalibrationPoint
from gas_calibrator.logging_utils import RunLogger, _field_label
from gas_calibrator.workflow.runner import CalibrationRunner


def _co2_point(index: int, ppm: float, pressure_hpa: float) -> CalibrationPoint:
    return CalibrationPoint(
        index=index,
        temp_chamber_c=20.0,
        co2_ppm=ppm,
        hgen_temp_c=None,
        hgen_rh_pct=None,
        target_pressure_hpa=pressure_hpa,
        dewpoint_c=None,
        h2o_mmol=None,
        raw_h2o=None,
        co2_group="A",
    )


def test_v1_point_trace_row_contains_expected_fields(tmp_path: Path) -> None:
    class _FakePace:
        def read_pressure(self):
            return 1000.5

    class _FakeGauge:
        def read_pressure(self):
            return 1000.2

    class _FakeDew:
        def get_current(self, timeout_s=None, attempts=None):
            return {"dewpoint_c": -11.5, "temp_c": 22.0, "rh_pct": 14.0}

    class _FakeChamber:
        def read_temp_c(self):
            return 20.1

        def read_rh_pct(self):
            return 45.0

    cfg = {
        "workflow": {
            "sampling": {
                "stable_count": 1,
                "interval_s": 0.0,
                "quality": {"enabled": False},
            }
        }
    }
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(
        cfg,
        {
            "gas_analyzer": object(),
            "pace": _FakePace(),
            "pressure_gauge": _FakeGauge(),
            "dewpoint": _FakeDew(),
            "temp_chamber": _FakeChamber(),
        },
        logger,
        lambda *_: None,
        lambda *_: None,
    )
    runner._read_sensor_parsed = types.MethodType(
        lambda self, _ga, required_key=None, **_kwargs: (
            "ROW",
            {
                "co2_ppm": 401.1,
                "h2o_mmol": 6.2,
                "co2_ratio_f": 1.001,
                "h2o_ratio_f": 0.123,
            },
        ),
        runner,
    )

    rows = runner._collect_samples(
        _co2_point(index=11, ppm=400.0, pressure_hpa=1000.0),
        1,
        0.0,
        phase="co2",
        point_tag="co2_groupa_400ppm_1000hpa",
    )
    logger.close()

    assert rows is not None
    row = rows[0]
    assert row["point_row"] == 11
    assert row["point_phase"] == "co2"
    assert row["point_tag"] == "co2_groupa_400ppm_1000hpa"
    assert row["sample_ts"]
    assert row["sample_start_ts"]
    assert row["sample_end_ts"]
    assert row["co2_ppm_target"] == 400.0
    assert row["pressure_target_hpa"] == 1000.0
    assert row["co2_ppm"] == 401.1
    assert row["pressure_hpa"] == 1000.5
    assert row["pressure_gauge_hpa"] == 1000.2
    assert row["dewpoint_live_c"] == -11.5


def test_v1_point_trace_distinct_points_do_not_overwrite_each_other(tmp_path: Path) -> None:
    cfg = {
        "workflow": {
            "sampling": {
                "stable_count": 1,
                "interval_s": 0.0,
                "quality": {"enabled": False},
            },
            "reporting": {"defer_heavy_exports_during_handoff": False},
        }
    }
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(cfg, {}, logger, lambda *_: None, lambda *_: None)

    def _fake_collect(self, point, _count, _interval, phase="", point_tag=""):
        return [
            {
                "point_title": f"point-{point.index}",
                "point_row": point.index,
                "point_phase": phase or "co2",
                "point_tag": point_tag,
                "sample_ts": f"2026-04-12T12:00:0{point.index}.000",
                "sample_start_ts": f"2026-04-12T12:00:0{point.index}.000",
                "sample_end_ts": f"2026-04-12T12:00:0{point.index}.100",
                "co2_ppm_target": point.co2_ppm,
                "pressure_target_hpa": point.target_pressure_hpa,
                "co2_ppm": float(point.co2_ppm or 0.0) + 1.0,
                "pressure_hpa": point.target_pressure_hpa,
            }
        ]

    runner._collect_samples = types.MethodType(_fake_collect, runner)

    point_1 = _co2_point(index=1, ppm=400.0, pressure_hpa=1000.0)
    point_2 = _co2_point(index=2, ppm=800.0, pressure_hpa=1000.0)

    runner._sample_and_log(point_1, phase="co2", point_tag="co2_groupa_400ppm_1000hpa")
    runner._sample_and_log(point_2, phase="co2", point_tag="co2_groupa_800ppm_1000hpa")
    logger.close()

    first_path = logger.run_dir / "point_0001_co2_co2_groupa_400ppm_1000hpa_samples.csv"
    second_path = logger.run_dir / "point_0002_co2_co2_groupa_800ppm_1000hpa_samples.csv"
    assert first_path.exists()
    assert second_path.exists()

    with logger.points_path.open("r", encoding="utf-8", newline="") as handle:
        point_rows = list(csv.DictReader(handle))
    assert len(point_rows) == 2
    assert point_rows[0][_field_label("point_row")] == "1"
    assert point_rows[1][_field_label("point_row")] == "2"


def test_v1_trace_code_keeps_stability_and_freshness_guards() -> None:
    sample_and_log_src = inspect.getsource(CalibrationRunner._sample_and_log)
    wait_after_pressure_src = inspect.getsource(CalibrationRunner._wait_after_pressure_stable_before_sampling)
    run_co2_point_src = inspect.getsource(CalibrationRunner._run_co2_point)
    run_h2o_group_src = inspect.getsource(CalibrationRunner._run_h2o_group)

    assert "_wait_for_sampling_freshness_gate" in sample_and_log_src
    assert sample_and_log_src.index("_wait_for_sampling_freshness_gate") < sample_and_log_src.index("_collect_samples")

    assert "_wait_postseal_dewpoint_gate" in wait_after_pressure_src
    assert "_wait_co2_presample_long_guard" in wait_after_pressure_src
    assert "_wait_pressure_and_primary_sensor_ready" in wait_after_pressure_src

    assert "_wait_after_pressure_stable_before_sampling" in run_co2_point_src
    assert run_co2_point_src.index("_wait_after_pressure_stable_before_sampling") < run_co2_point_src.index("_sample_and_log")

    assert "_wait_after_pressure_stable_before_sampling" in run_h2o_group_src
    assert run_h2o_group_src.index("_wait_after_pressure_stable_before_sampling") < run_h2o_group_src.index("_sample_and_log")
