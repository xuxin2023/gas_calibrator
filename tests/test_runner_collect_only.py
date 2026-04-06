import types
import csv
import json
import types
from pathlib import Path
from types import SimpleNamespace

import pytest

from gas_calibrator.data.points import CalibrationPoint
from gas_calibrator.logging_utils import RunLogger, _field_label
from gas_calibrator.workflow.runner import CalibrationRunner
from gas_calibrator.workflow import runner as runner_module


def _point() -> CalibrationPoint:
    return CalibrationPoint(
        index=1,
        temp_chamber_c=20.0,
        co2_ppm=400.0,
        hgen_temp_c=None,
        hgen_rh_pct=None,
        target_pressure_hpa=1000.0,
        dewpoint_c=None,
        h2o_mmol=None,
        raw_h2o=None,
    )


def test_collect_only_skips_coefficient_stage(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(runner_module, "load_points_from_excel", lambda *_args, **_kwargs: [_point()])
    monkeypatch.setattr(runner_module, "reorder_points", lambda points, *_args, **_kwargs: points)
    monkeypatch.setattr(runner_module, "validate_points", lambda *_args, **_kwargs: [])

    cfg = {
        "paths": {"points_excel": "demo.xlsx"},
        "workflow": {"collect_only": True, "missing_pressure_policy": "carry_forward"},
    }
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(cfg, {}, logger, lambda *_: None, lambda *_: None)

    called = {"fit": False}
    runner._sensor_precheck = types.MethodType(lambda self: None, runner)
    runner._configure_devices = types.MethodType(lambda self: None, runner)
    runner._run_points = types.MethodType(lambda self, _points: None, runner)
    runner._maybe_write_coefficients = types.MethodType(
        lambda self: called.__setitem__("fit", True),
        runner,
    )

    runner.run()
    logger.close()
    assert called["fit"] is False


def test_sample_and_log_writes_point_csv(tmp_path: Path) -> None:
    cfg = {
        "workflow": {
            "sampling": {
                "stable_count": 2,
                "interval_s": 0.0,
                "quality": {"enabled": False},
            }
        }
    }
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(cfg, {}, logger, lambda *_: None, lambda *_: None)

    rows = [
        {"point_title": "20°C环境，二氧化碳400ppm，气压1000hPa", "co2_ppm": 400.0, "pressure_hpa": 1000.1, "point_row": 1},
        {"point_title": "20°C环境，二氧化碳400ppm，气压1000hPa", "co2_ppm": 401.0, "pressure_hpa": 1000.2, "point_row": 1},
    ]
    runner._collect_samples = types.MethodType(lambda self, *_args, **_kwargs: list(rows), runner)

    runner._sample_and_log(_point())

    point_csv = logger.run_dir / "point_0001_samples.csv"
    logger.close()
    assert point_csv.exists()

    import csv

    with point_csv.open("r", encoding="utf-8", newline="") as f:
        header = next(csv.reader(f))
    assert header[0] == "点位标题"


def test_sample_and_log_writes_tagged_point_csv(tmp_path: Path) -> None:
    cfg = {
        "workflow": {
            "sampling": {
                "stable_count": 2,
                "interval_s": 0.0,
                "quality": {"enabled": False},
            }
        }
    }
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(cfg, {}, logger, lambda *_: None, lambda *_: None)

    rows = [
        {"co2_ppm": 400.0, "pressure_hpa": 1000.1, "point_row": 1, "point_tag": "co2_groupa_400ppm_1000hpa"},
        {"co2_ppm": 401.0, "pressure_hpa": 1000.2, "point_row": 1, "point_tag": "co2_groupa_400ppm_1000hpa"},
    ]
    runner._collect_samples = types.MethodType(lambda self, *_args, **_kwargs: list(rows), runner)

    runner._sample_and_log(_point(), phase="co2", point_tag="co2_groupa_400ppm_1000hpa")

    point_csv = logger.run_dir / "point_0001_co2_co2_groupa_400ppm_1000hpa_samples.csv"
    logger.close()
    assert point_csv.exists()


def test_sample_and_log_uses_co2_interval_override(tmp_path: Path) -> None:
    cfg = {
        "workflow": {
            "sampling": {
                "stable_count": 10,
                "interval_s": 10.0,
                "co2_interval_s": 1.0,
                "quality": {"enabled": False},
            }
        }
    }
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(cfg, {}, logger, lambda *_: None, lambda *_: None)

    captured = {}

    def _fake_collect(self, point, count, interval, phase="", point_tag=""):
        captured["count"] = count
        captured["interval"] = interval
        captured["phase"] = phase
        return [{"co2_ppm": 400.0, "pressure_hpa": 1000.1, "point_row": point.index}]

    runner._collect_samples = types.MethodType(_fake_collect, runner)

    runner._sample_and_log(_point(), phase="co2", point_tag="co2_groupa_400ppm_1000hpa")
    logger.close()

    assert captured == {"count": 10, "interval": 1.0, "phase": "co2"}


def test_sample_and_log_aligns_first_sample_trace_and_keeps_first_row_fresh(tmp_path: Path) -> None:
    class _FakePace:
        pressure_queries = [":SENS:PRES:INL?"]
        query_line_endings = ["\n"]

        def read_pressure(self):
            return 1000.0

        def query(self, cmd, line_ending=None):
            return ":SENS:PRES:INL 1000.0, 0"

        @staticmethod
        def _parse_first_float(text):
            return 1000.0 if text else None

        def get_output_state(self):
            return 1

        def get_isolation_state(self):
            return 1

        def get_vent_status(self):
            return 0

    class _FakeGauge:
        def read_pressure(self, *args, **kwargs):
            return 1000.1

    class _FakeDew:
        def get_current_fast(self, timeout_s=0.35):
            return {"dewpoint_c": -10.2, "temp_c": 21.0, "rh_pct": 45.0}

    class _FakeAnalyzer:
        def read_latest_data(self, *args, **kwargs):
            return "YGAS,001,0400.000,02.000,1.000,0.200,1.0000,1.0000,0.2000,0.2000,03328,03543,02535,022.54,022.07,101.30"

        def parse_line_mode2(self, _line):
            return {
                "co2_ppm": 400.0,
                "h2o_mmol": 2.0,
                "co2_ratio_f": 1.0,
                "h2o_ratio_f": 0.2,
                "pressure_kpa": 101.3,
                "mode2_field_count": 16,
            }

    cfg = {
        "devices": {
            "gas_analyzer": {"active_send": True, "ftd_hz": 10, "average_co2": 1, "average_h2o": 1},
            "gas_analyzers": [{"name": "ga01", "active_send": True, "ftd_hz": 10}],
        },
        "workflow": {
            "sampling": {
                "stable_count": 10,
                "interval_s": 0.0,
                "pre_sample_freshness_timeout_s": 1.0,
                "quality": {"enabled": False},
            }
        },
    }
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(
        cfg,
        {
            "pace": _FakePace(),
            "pressure_gauge": _FakeGauge(),
            "dewpoint": _FakeDew(),
            "gas_analyzer_01": _FakeAnalyzer(),
        },
        logger,
        lambda *_: None,
        lambda *_: None,
    )
    captured: dict[str, list[dict]] = {}
    runner._perform_light_point_exports = types.MethodType(
        lambda self, point, samples, **kwargs: captured.__setitem__("samples", [dict(row) for row in samples]),
        runner,
    )
    runner._perform_heavy_point_exports = types.MethodType(lambda self, *args, **kwargs: None, runner)

    runner._sample_and_log(_point(), phase="co2", point_tag="freshness_demo")
    logger.close()

    samples = captured["samples"]
    assert len(samples) == 10
    assert all(sample.get("pressure_hpa") is not None for sample in samples)
    assert all(sample.get("pressure_gauge_hpa") is not None for sample in samples)
    assert all(sample.get("dewpoint_live_c") is not None for sample in samples)
    assert all(sample.get("ga01_co2_ppm") is not None for sample in samples)
    assert samples[0]["pressure_hpa"] == 1000.0
    assert samples[0]["pressure_gauge_hpa"] == 1000.1
    assert samples[0]["dewpoint_live_c"] == -10.2
    assert samples[0]["ga01_co2_ppm"] == 400.0

    with logger.run_dir.joinpath("pressure_transition_trace.csv").open("r", encoding="utf-8-sig", newline="") as handle:
        trace_rows = list(csv.DictReader(handle))

    stage_order = [row["trace_stage"] for row in trace_rows]
    assert "sampling_collection_begin" in stage_order
    assert "sampling_prime_begin" in stage_order
    assert "sampling_collection_ready" in stage_order
    assert "sampling_prime_ready" in stage_order
    assert "first_effective_sample" in stage_order
    assert stage_order.index("sampling_collection_begin") < stage_order.index("sampling_prime_begin")
    assert stage_order.index("sampling_prime_begin") < stage_order.index("sampling_collection_ready")
    assert stage_order.index("sampling_collection_ready") < stage_order.index("sampling_prime_ready")
    first_sample_rows = [row for row in trace_rows if row["trace_stage"] == "first_sample_begin"]
    assert len(first_sample_rows) == 1
    assert first_sample_rows[0]["ts"] == samples[0]["sample_start_ts"]
    first_effective_rows = [row for row in trace_rows if row["trace_stage"] == "first_effective_sample"]
    assert len(first_effective_rows) == 1
    assert first_effective_rows[0]["ts"] == samples[0]["sample_start_ts"]
    assert "pace_first_valid_ms=" in first_effective_rows[0]["note"]
    assert "pressure_gauge_first_valid_ms=" in first_effective_rows[0]["note"]
    assert "dewpoint_first_valid_ms=" in first_effective_rows[0]["note"]
    assert "analyzer_first_valid_ms=" in first_effective_rows[0]["note"]
    prime_ready_rows = [row for row in trace_rows if row["trace_stage"] == "sampling_prime_ready"]
    assert len(prime_ready_rows) == 1
    assert "status=ready" in prime_ready_rows[0]["note"]
    assert float(prime_ready_rows[0]["pace_pressure_hpa"]) == 1000.0
    assert float(prime_ready_rows[0]["pressure_gauge_hpa"]) == 1000.1
    assert float(prime_ready_rows[0]["dewpoint_live_c"]) == -10.2


def test_sample_and_log_relabels_prime_timeout_when_effective_sample_arrives_after_start(tmp_path: Path) -> None:
    cfg = {
        "workflow": {
            "sampling": {
                "stable_count": 3,
                "interval_s": 0.0,
                "quality": {"enabled": False},
            }
        }
    }
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(cfg, {}, logger, lambda *_: None, lambda *_: None)
    captured: dict[str, list[dict]] = {}
    runner._perform_light_point_exports = types.MethodType(
        lambda self, point, samples, **kwargs: captured.__setitem__("samples", [dict(row) for row in samples]),
        runner,
    )
    runner._perform_heavy_point_exports = types.MethodType(lambda self, *args, **kwargs: None, runner)

    def _fake_freshness(self, point, phase="", point_tag="", context=None):
        return {
            "status": "timeout",
            "elapsed_s": 1.0,
            "missing": ["pace", "pressure_gauge"],
            "ready_values": {},
        }

    def _fake_collect(self, point, count, interval, phase="", point_tag=""):
        phase_text = str(phase or ("h2o" if point.is_h2o_point else "co2")).strip().lower()
        samples = [
            {
                "point_row": point.index,
                "co2_ppm": 400.0,
                "pressure_hpa": None,
                "pressure_gauge_hpa": None,
                "dewpoint_live_c": -10.2,
                "dew_temp_live_c": 21.0,
                "dew_rh_live_pct": 45.0,
                "sample_start_ts": "2026-04-06T12:00:00",
                "sample_end_ts": "2026-04-06T12:00:00.050000",
            },
            {
                "point_row": point.index,
                "co2_ppm": 400.0,
                "pressure_hpa": 1000.0,
                "pressure_gauge_hpa": None,
                "dewpoint_live_c": -10.2,
                "dew_temp_live_c": 21.0,
                "dew_rh_live_pct": 45.0,
                "sample_start_ts": "2026-04-06T12:00:01",
                "sample_end_ts": "2026-04-06T12:00:01.050000",
            },
            {
                "point_row": point.index,
                "co2_ppm": 400.0,
                "pressure_hpa": 1000.0,
                "pressure_gauge_hpa": 1000.1,
                "dewpoint_c": -10.2,
                "dew_temp_c": 21.0,
                "dew_rh_pct": 45.0,
                "dewpoint_live_c": -10.2,
                "dew_temp_live_c": 21.0,
                "dew_rh_live_pct": 45.0,
                "ga01_co2_ppm": 400.0,
                "sample_start_ts": "2026-04-06T12:00:02",
                "sample_end_ts": "2026-04-06T12:00:02.050000",
                "pace_output_state": 1,
                "pace_isolation_state": 1,
                "pace_vent_status": 0,
            },
        ]
        self._append_pressure_trace_row(
            point=point,
            route=phase_text,
            point_phase=phase_text,
            point_tag=point_tag,
            trace_stage="first_effective_sample",
            pressure_target_hpa=point.target_pressure_hpa,
            pace_pressure_hpa=1000.0,
            pressure_gauge_hpa=1000.1,
            dewpoint_c=-10.2,
            dew_temp_c=21.0,
            dew_rh_pct=45.0,
            refresh_pace_state=False,
            dewpoint_live_c=-10.2,
            dew_temp_live_c=21.0,
            dew_rh_live_pct=45.0,
            event_ts=self._sample_row_wall_ts(samples[2], key="sample_start_ts"),
            note=(
                "sample_index=3/3 "
                "pace_first_valid_ms=0.012 "
                "pressure_gauge_first_valid_ms=0.012 "
                "dewpoint_first_valid_ms=0.012 "
                "analyzer_first_valid_ms=0.012"
            ),
        )
        self._set_point_runtime_fields(
            point,
            phase=phase_text,
            first_valid_pace_ms=0.012,
            first_valid_pressure_gauge_ms=0.012,
            first_valid_dewpoint_ms=0.012,
            first_valid_analyzer_ms=0.012,
            effective_sample_started_on_row=3,
        )
        return samples

    runner._wait_for_sampling_freshness_gate = types.MethodType(_fake_freshness, runner)
    runner._collect_samples = types.MethodType(_fake_collect, runner)

    runner._sample_and_log(_point(), phase="co2", point_tag="prime_timeout_demo")
    logger.close()

    assert len(captured["samples"]) == 3
    with logger.run_dir.joinpath("pressure_transition_trace.csv").open("r", encoding="utf-8-sig", newline="") as handle:
        trace_rows = list(csv.DictReader(handle))

    prime_ready_rows = [row for row in trace_rows if row["trace_stage"] == "sampling_prime_ready"]
    assert len(prime_ready_rows) == 1
    assert "status=ready_after_start" in prime_ready_rows[0]["note"]
    assert "originally_missing=pace,pressure_gauge" in prime_ready_rows[0]["note"]
    assert "effective_row=3" in prime_ready_rows[0]["note"]
    assert "pace_first_valid_ms=0.012" in prime_ready_rows[0]["note"]
    assert "pressure_gauge_first_valid_ms=0.012" in prime_ready_rows[0]["note"]
    assert prime_ready_rows[0]["ts"] == "2026-04-06T12:00:02.000"
    assert float(prime_ready_rows[0]["pace_pressure_hpa"]) == 1000.0
    assert float(prime_ready_rows[0]["pressure_gauge_hpa"]) == 1000.1

    stage_order = [row["trace_stage"] for row in trace_rows]
    assert stage_order.index("first_effective_sample") < stage_order.index("sampling_prime_ready")


def test_sample_and_log_exports_co2_preseal_dew_pressure_to_running_summaries(tmp_path: Path) -> None:
    class _FakePace:
        pressure_queries = [":SENS:PRES:INL?"]
        query_line_endings = ["\n"]

        def read_pressure(self):
            return 1100.0

        def query(self, cmd, line_ending=None):
            return ":SENS:PRES:INL 1100.0, 0"

        @staticmethod
        def _parse_first_float(text):
            return 1100.0 if text else None

        def get_output_state(self):
            return 1

        def get_isolation_state(self):
            return 1

        def get_vent_status(self):
            return 0

    class _FakeGauge:
        def read_pressure(self, *args, **kwargs):
            return 1099.8

    class _FakeDew:
        def get_current_fast(self, timeout_s=0.35):
            return {"dewpoint_c": -18.2, "temp_c": 20.5, "rh_pct": 38.0}

    class _FakeAnalyzer:
        def read_latest_data(self, *args, **kwargs):
            return "YGAS,001,0401.000,02.100,1.000,0.200,1.0000,1.0000,0.2000,0.2000,03328,03543,02535,022.54,022.07,101.30"

        def parse_line_mode2(self, _line):
            return {
                "co2_ppm": 401.0,
                "h2o_mmol": 2.1,
                "co2_ratio_f": 1.0,
                "h2o_ratio_f": 0.2,
                "pressure_kpa": 101.3,
                "mode2_field_count": 16,
            }

    cfg = {
        "devices": {
            "gas_analyzer": {"active_send": True, "ftd_hz": 10, "average_co2": 1, "average_h2o": 1},
            "gas_analyzers": [{"name": "ga01", "active_send": True, "ftd_hz": 10}],
        },
        "workflow": {
            "sampling": {
                "stable_count": 1,
                "interval_s": 0.0,
                "pre_sample_freshness_timeout_s": 1.0,
                "quality": {"enabled": False},
            },
            "reporting": {"defer_heavy_exports_during_handoff": False},
        },
    }
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(
        cfg,
        {
            "pace": _FakePace(),
            "pressure_gauge": _FakeGauge(),
            "dewpoint": _FakeDew(),
            "gas_analyzer_01": _FakeAnalyzer(),
        },
        logger,
        lambda *_: None,
        lambda *_: None,
    )
    runner._preseal_dewpoint_snapshot = {
        "sample_ts": "2026-04-01T01:00:00.000",
        "dewpoint_c": -20.1,
        "temp_c": 20.0,
        "rh_pct": 35.0,
        "pressure_hpa": 1112.3,
    }
    now = runner_module.time.time()
    runner._point_runtime_summary[("co2", 1)] = {
        "dewpoint_gate_result": "stable",
        "dewpoint_gate_elapsed_s": 1.25,
        "dewpoint_gate_count": 4,
        "dewpoint_gate_span_c": 0.08,
        "dewpoint_gate_slope_c_per_s": 0.015,
        "configured_route_soak_s": 180.0,
        "timing_stages": {
            "prev_sampling_end": now - 15.0,
            "atmosphere_enter_begin": now - 14.7,
            "atmosphere_enter_verified": now - 11.0,
            "route_open": now - 10.5,
            "soak_begin": now - 10.4,
            "soak_end": now - 1.2,
            "preseal_vent_off_begin": now - 1.0,
            "preseal_trigger_reached": now - 0.9,
            "route_sealed": now - 0.8,
            "control_prepare_begin": now - 0.7,
            "control_ready_snapshot_acquired": now - 0.65,
            "control_ready_wait_begin": now - 0.6,
            "control_ready_wait_end": now - 0.45,
            "control_ready_verified": now - 0.4,
            "control_output_on_begin": now - 0.35,
            "control_output_on_command_sent": now - 0.3,
            "control_output_verify_wait_begin": now - 0.25,
            "control_output_verify_wait_end": now - 0.15,
            "control_output_on_verified": now - 0.1,
            "pressure_in_limits": now - 0.08,
            "dewpoint_gate_begin": now - 0.05,
            "dewpoint_gate_end": now - 0.02,
            "sampling_begin": now - 0.01,
        },
    }

    runner._sample_and_log(_point(), phase="co2", point_tag="co2_dew_pressure_demo")
    logger.close()

    with logger.points_path.open("r", encoding="utf-8", newline="") as handle:
        point_rows = list(csv.DictReader(handle))
    assert len(point_rows) == 1
    point_key = _field_label("dew_pressure_hpa_mean")
    assert point_key in point_rows[0]
    assert float(point_rows[0][point_key]) == 1112.3
    assert point_rows[0][_field_label("dewpoint_gate_result")] == "stable"
    assert float(point_rows[0][_field_label("dewpoint_gate_elapsed_s")]) == 1.25
    assert point_rows[0][_field_label("effective_sample_started_on_row")] == "1"
    assert float(point_rows[0][_field_label("first_valid_pace_ms")]) < 50.0

    with logger.points_readable_path.open("r", encoding="utf-8", newline="") as handle:
        readable_rows = list(csv.DictReader(handle))
    assert len(readable_rows) == 1
    assert point_key in readable_rows[0]
    assert float(readable_rows[0][point_key]) == 1112.3
    assert readable_rows[0][_field_label("dewpoint_gate_result")] == "stable"
    assert float(readable_rows[0][_field_label("dewpoint_gate_count")]) == 4.0
    assert readable_rows[0][_field_label("effective_sample_started_on_row")] == "1"

    with logger.analyzer_summary_csv_path.open("r", encoding="utf-8", newline="") as handle:
        summary_rows = list(csv.DictReader(handle))
    assert len(summary_rows) == 1
    assert float(summary_rows[0]["DewPressurePreseal"]) == 1112.3

    timing_path = logger.run_dir / "point_timing_summary.csv"
    with timing_path.open("r", encoding="utf-8", newline="") as handle:
        timing_rows = list(csv.DictReader(handle))
    assert len(timing_rows) == 1
    assert timing_rows[0]["point_phase"] == "co2"
    assert float(timing_rows[0]["configured_route_soak_s"]) == 180.0
    assert float(timing_rows[0]["soak_begin_to_soak_end_ms"]) > 0.0
    assert float(timing_rows[0]["sampling_begin_to_first_effective_sample_ms"]) >= 0.0


def test_sample_and_log_continues_when_log_sample_fails(tmp_path: Path) -> None:
    cfg = {
        "workflow": {
            "sampling": {
                "stable_count": 2,
                "interval_s": 0.0,
                "quality": {"enabled": False},
            }
        }
    }
    logs = []
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(cfg, {}, logger, lambda msg: logs.append(str(msg)), lambda *_: None)

    rows = [
        {"point_title": "点1", "co2_ppm": 400.0, "pressure_hpa": 1000.1, "point_row": 1},
        {"point_title": "点1", "co2_ppm": 401.0, "pressure_hpa": 1000.2, "point_row": 1},
    ]
    runner._collect_samples = types.MethodType(lambda self, *_args, **_kwargs: list(rows), runner)

    def _boom(_row):
        raise RuntimeError("sample-boom")

    runner.logger.log_sample = _boom

    runner._sample_and_log(_point())
    logger.close()

    assert len(runner._all_samples) == 2
    assert any("sample export failed" in msg for msg in logs)


def test_sample_and_log_continues_when_log_point_fails(tmp_path: Path) -> None:
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
    logs = []
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(cfg, {}, logger, lambda msg: logs.append(str(msg)), lambda *_: None)

    rows = [{"point_title": "点1", "co2_ppm": 400.0, "pressure_hpa": 1000.1, "point_row": 1}]
    runner._collect_samples = types.MethodType(lambda self, *_args, **_kwargs: list(rows), runner)

    def _boom(_row):
        raise RuntimeError("point-boom")

    runner.logger.log_point = _boom

    runner._sample_and_log(_point())
    logger.close()

    assert any("point export failed" in msg for msg in logs)


def test_sample_and_log_still_calls_analyzer_summary_when_workbook_fails(tmp_path: Path) -> None:
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
    logs = []
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(cfg, {}, logger, lambda msg: logs.append(str(msg)), lambda *_: None)

    rows = [{"point_title": "点1", "co2_ppm": 400.0, "pressure_hpa": 1000.1, "point_row": 1}]
    runner._collect_samples = types.MethodType(lambda self, *_args, **_kwargs: list(rows), runner)

    called = {"summary": 0, "workbook": 0}

    def _summary(*_args, **_kwargs):
        called["summary"] += 1
        return logger.analyzer_summary_book_path

    def _workbook(*_args, **_kwargs):
        called["workbook"] += 1
        raise RuntimeError("workbook-boom")

    runner.logger.log_analyzer_summary = _summary
    runner.logger.log_analyzer_workbook = _workbook

    runner._sample_and_log(_point())
    logger.close()

    assert called["summary"] == 1
    assert called["workbook"] == 1
    assert any("analyzer summary updated" in msg for msg in logs)
    assert any("analyzer workbook save failed" in msg for msg in logs)


def test_runner_uses_fixed_zero_degree_water_first_threshold(monkeypatch, tmp_path: Path) -> None:
    point = _point()
    monkeypatch.setattr(runner_module, "load_points_from_excel", lambda *_args, **_kwargs: [point])

    called = {"water_first": None, "descending_temperatures": None}

    def _fake_reorder(points, water_first, **kwargs):
        called["water_first"] = water_first
        called["descending_temperatures"] = kwargs.get("descending_temperatures")
        return points

    monkeypatch.setattr(runner_module, "reorder_points", _fake_reorder)
    monkeypatch.setattr(runner_module, "validate_points", lambda *_args, **_kwargs: [])

    cfg = {
        "paths": {"points_excel": "demo.xlsx"},
        "workflow": {
            "collect_only": True,
            "missing_pressure_policy": "carry_forward",
        },
    }
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(cfg, {}, logger, lambda *_: None, lambda *_: None)
    runner._sensor_precheck = types.MethodType(lambda self: None, runner)
    runner._configure_devices = types.MethodType(lambda self: None, runner)
    runner._run_points = types.MethodType(lambda self, _points: None, runner)
    runner._maybe_write_coefficients = types.MethodType(lambda self: None, runner)

    runner.run()
    logger.close()
    assert called["water_first"] == 0.0
    assert called["descending_temperatures"] is True


def test_run_filters_selected_temperatures(monkeypatch, tmp_path: Path) -> None:
    points = [
        CalibrationPoint(
            index=1,
            temp_chamber_c=20.0,
            co2_ppm=400.0,
            hgen_temp_c=None,
            hgen_rh_pct=None,
            target_pressure_hpa=1000.0,
            dewpoint_c=None,
            h2o_mmol=None,
            raw_h2o=None,
        ),
        CalibrationPoint(
            index=2,
            temp_chamber_c=30.0,
            co2_ppm=600.0,
            hgen_temp_c=None,
            hgen_rh_pct=None,
            target_pressure_hpa=1000.0,
            dewpoint_c=None,
            h2o_mmol=None,
            raw_h2o=None,
        ),
    ]

    monkeypatch.setattr(runner_module, "load_points_from_excel", lambda *_args, **_kwargs: list(points))
    monkeypatch.setattr(runner_module, "reorder_points", lambda pts, *_args, **_kwargs: pts)
    monkeypatch.setattr(runner_module, "validate_points", lambda *_args, **_kwargs: [])

    cfg = {
        "paths": {"points_excel": "demo.xlsx"},
        "workflow": {
            "collect_only": True,
            "missing_pressure_policy": "carry_forward",
            "selected_temps_c": [20.0],
        },
    }
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(cfg, {}, logger, lambda *_: None, lambda *_: None)
    captured = {}
    runner._sensor_precheck = types.MethodType(lambda self: None, runner)
    runner._configure_devices = types.MethodType(lambda self: None, runner)
    runner._run_points = types.MethodType(lambda self, pts: captured.setdefault("indices", [p.index for p in pts]), runner)
    runner._maybe_write_coefficients = types.MethodType(lambda self: None, runner)

    runner.run()
    logger.close()

    assert captured["indices"] == [1]


def test_runner_ratio_poly_autofit_reads_analyzer_summary(tmp_path: Path) -> None:
    summary_path = tmp_path / "summary.csv"
    samples_path = tmp_path / "samples.csv"
    samples_path.write_text("", encoding="utf-8")

    headers = [
        "Analyzer",
        "PointRow",
        "PointPhase",
        "PointTag",
        "PointTitle",
        "ppm_CO2_Tank",
        "R_CO2",
        "thermometer_temp_c",
        "T1",
        "BAR",
    ]
    rows = []
    for idx, (ratio_value, temp_c, pressure_value) in enumerate(
        [
            (0.82, 10.0, 96.0),
            (0.87, 15.0, 98.0),
            (0.92, 20.0, 100.0),
            (0.97, 25.0, 102.0),
            (1.02, 30.0, 104.0),
            (1.07, 35.0, 106.0),
            (1.12, 40.0, 108.0),
            (1.17, 12.0, 99.0),
            (1.22, 18.0, 101.0),
        ],
        start=1,
    ):
        temp_k = temp_c + 273.15
        target = (
            42.5
            + 120.0 * ratio_value
            - 35.0 * (ratio_value**2)
            + 6.0 * (ratio_value**3)
            + 0.11 * temp_k
            - 0.00012 * (temp_k**2)
            + 0.32 * ratio_value * temp_k
            - 0.75 * pressure_value
            + 0.0045 * ratio_value * temp_k * pressure_value
        )
        rows.append(
            {
                "Analyzer": "GA07",
                "PointRow": idx,
                "PointPhase": "CO2",
                "PointTag": f"co2_point_{idx}",
                "PointTitle": f"Point {idx}",
                "ppm_CO2_Tank": target,
                "R_CO2": ratio_value,
                "thermometer_temp_c": temp_c,
                "T1": temp_c + 40.0,
                "BAR": pressure_value,
            }
        )

    with summary_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=headers)
        writer.writeheader()
        writer.writerows(rows)

    logger = SimpleNamespace(
        analyzer_summary_csv_path=summary_path,
        samples_path=samples_path,
    )
    messages = []
    cfg = {
        "coefficients": {
            "enabled": True,
            "auto_fit": True,
            "model": "ratio_poly_rt_p",
            "ratio_degree": 3,
            "simplify_coefficients": False,
        }
    }
    runner = CalibrationRunner(cfg, {"gas_analyzer": object()}, logger, messages.append, lambda *_: None)

    runner._all_samples = [{"dummy": 1}]
    runner._maybe_write_coefficients()

    outputs = list(tmp_path.glob("co2_GA07_ratio_poly_fit_*.json"))
    assert outputs
    payload = json.loads(outputs[0].read_text(encoding="utf-8"))
    assert payload["stats"]["validation_metrics"]["sample_count"] >= 0
    assert payload["stats"]["test_metrics"]["sample_count"] >= 0
    assert payload["stats"]["rmse_original"] < 1e-6
    assert any("Auto-fit CO2 [GA07]" in message for message in messages)


def test_runner_ratio_poly_evolved_autofit_reads_analyzer_summary(tmp_path: Path) -> None:
    summary_path = tmp_path / "summary.csv"
    samples_path = tmp_path / "samples.csv"
    samples_path.write_text("", encoding="utf-8")

    headers = [
        "Analyzer",
        "PointRow",
        "PointPhase",
        "PointTag",
        "PointTitle",
        "ppm_CO2_Tank",
        "R_CO2",
        "thermometer_temp_c",
        "T1",
        "BAR",
    ]
    rows = []
    for idx, (ratio_value, temp_c, pressure_value) in enumerate(
        [
            (0.82, 10.0, 96.0),
            (0.87, 15.0, 98.0),
            (0.92, 20.0, 100.0),
            (0.97, 25.0, 102.0),
            (1.02, 30.0, 104.0),
            (1.07, 35.0, 106.0),
            (1.12, 40.0, 108.0),
            (1.17, 12.0, 99.0),
            (1.22, 18.0, 101.0),
        ],
        start=1,
    ):
        temp_k = temp_c + 273.15
        target = (
            42.5
            + 120.0 * ratio_value
            - 35.0 * (ratio_value**2)
            + 6.0 * (ratio_value**3)
            + 0.11 * temp_k
            - 0.00012 * (temp_k**2)
            + 0.32 * ratio_value * temp_k
            - 0.75 * pressure_value
            + 0.0045 * ratio_value * temp_k * pressure_value
        )
        rows.append(
            {
                "Analyzer": "GA07",
                "PointRow": idx,
                "PointPhase": "CO2",
                "PointTag": f"pt_{idx}",
                "PointTitle": f"Point {idx}",
                "ppm_CO2_Tank": target,
                "R_CO2": ratio_value,
                "thermometer_temp_c": temp_c,
                "T1": temp_c + 40.0,
                "BAR": pressure_value,
            }
        )
    rows.append(
        {
            "Analyzer": "GA07",
            "PointRow": 99,
            "PointPhase": "CO2",
            "PointTag": "outlier",
            "PointTitle": "Outlier",
            "ppm_CO2_Tank": 6000.0,
            "R_CO2": 1.12,
            "thermometer_temp_c": 22.0,
            "T1": 62.0,
            "BAR": 101.0,
        }
    )

    with summary_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=headers)
        writer.writeheader()
        writer.writerows(rows)

    logger = SimpleNamespace(
        analyzer_summary_csv_path=summary_path,
        samples_path=samples_path,
    )
    messages = []
    cfg = {
        "coefficients": {
            "enabled": True,
            "auto_fit": True,
            "model": "ratio_poly_rt_p_evolved",
            "ratio_degree": 3,
            "simplify_coefficients": True,
            "simplification_method": "auto",
        }
    }
    runner = CalibrationRunner(cfg, {"gas_analyzer": object()}, logger, messages.append, lambda *_: None)

    runner._all_samples = [{"dummy": 1}]
    runner._maybe_write_coefficients()

    outputs = list(tmp_path.glob("co2_GA07_ratio_poly_fit_*.json"))
    assert outputs
    payload = json.loads(outputs[0].read_text(encoding="utf-8"))
    assert payload["model"] == "ratio_poly_rt_p_evolved"
    assert any("ratio_poly_rt_p_evolved" in message for message in messages)


def test_runner_ratio_poly_autofit_h2o_includes_h2o_and_selected_co2_zero_rows(
    monkeypatch,
    tmp_path: Path,
) -> None:
    summary_path = tmp_path / "summary.csv"
    samples_path = tmp_path / "samples.csv"
    samples_path.write_text("", encoding="utf-8")

    headers = [
        "Analyzer",
        "PointRow",
        "PointPhase",
        "PointTag",
        "PointTitle",
        "ppm_CO2_Tank",
        "ppm_H2O_Dew",
        "R_H2O",
        "thermometer_temp_c",
        "T1",
        "TempSet",
        "BAR",
    ]
    rows = [
        {
            "Analyzer": "GA07",
            "PointRow": 1,
            "PointPhase": "水路",
            "PointTag": "h2o_20",
            "PointTitle": "H2O 20",
            "ppm_CO2_Tank": 400.0,
            "ppm_H2O_Dew": 1200.0,
            "R_H2O": 0.51,
            "thermometer_temp_c": 20.0,
            "T1": 60.0,
            "TempSet": 20.0,
            "BAR": 100.0,
        },
        {
            "Analyzer": "GA07",
            "PointRow": 2,
            "PointPhase": "H2O",
            "PointTag": "h2o_30",
            "PointTitle": "H2O 30",
            "ppm_CO2_Tank": 400.0,
            "ppm_H2O_Dew": 1800.0,
            "R_H2O": 0.62,
            "thermometer_temp_c": 30.0,
            "T1": 70.0,
            "TempSet": 30.0,
            "BAR": 101.0,
        },
        {
            "Analyzer": "GA07",
            "PointRow": 3,
            "PointPhase": "气路",
            "PointTag": "co2_zero_m20",
            "PointTitle": "CO2 zero -20",
            "ppm_CO2_Tank": 0.0,
            "ppm_H2O_Dew": 300.0,
            "R_H2O": 0.21,
            "thermometer_temp_c": -20.0,
            "T1": 25.0,
            "TempSet": -20.0,
            "BAR": 99.0,
        },
        {
            "Analyzer": "GA07",
            "PointRow": 4,
            "PointPhase": "CO2",
            "PointTag": "co2_zero_m10",
            "PointTitle": "CO2 zero -10",
            "ppm_CO2_Tank": 0.0,
            "ppm_H2O_Dew": 450.0,
            "R_H2O": 0.26,
            "thermometer_temp_c": -10.0,
            "T1": 35.0,
            "TempSet": -10.0,
            "BAR": 99.5,
        },
        {
            "Analyzer": "GA07",
            "PointRow": 5,
            "PointPhase": "CO2",
            "PointTag": "co2_zero_0",
            "PointTitle": "CO2 zero 0",
            "ppm_CO2_Tank": 0.0,
            "ppm_H2O_Dew": 700.0,
            "R_H2O": 0.31,
            "thermometer_temp_c": 0.0,
            "T1": 45.0,
            "TempSet": 0.0,
            "BAR": 100.5,
        },
        {
            "Analyzer": "GA07",
            "PointRow": 6,
            "PointPhase": "CO2",
            "PointTag": "co2_zero_10",
            "PointTitle": "CO2 zero 10",
            "ppm_CO2_Tank": 0.0,
            "ppm_H2O_Dew": 900.0,
            "R_H2O": 0.36,
            "thermometer_temp_c": 10.1,
            "T1": 54.4,
            "TempSet": None,
            "BAR": 101.0,
        },
        {
            "Analyzer": "GA07",
            "PointRow": 7,
            "PointPhase": "CO2",
            "PointTag": "co2_400_10",
            "PointTitle": "CO2 400 10",
            "ppm_CO2_Tank": 400.0,
            "ppm_H2O_Dew": 920.0,
            "R_H2O": 0.37,
            "thermometer_temp_c": 10.2,
            "T1": 54.5,
            "TempSet": None,
            "BAR": 101.1,
        },
        {
            "Analyzer": "GA07",
            "PointRow": 8,
            "PointPhase": "CO2",
            "PointTag": "co2_100_m20",
            "PointTitle": "CO2 100 -20",
            "ppm_CO2_Tank": 100.0,
            "ppm_H2O_Dew": 320.0,
            "R_H2O": 0.22,
            "thermometer_temp_c": -20.2,
            "T1": 23.6,
            "TempSet": None,
            "BAR": 99.0,
        },
        {
            "Analyzer": "GA07",
            "PointRow": 9,
            "PointPhase": "CO2",
            "PointTag": "co2_20_zero",
            "PointTitle": "CO2 zero 20",
            "ppm_CO2_Tank": 0.0,
            "ppm_H2O_Dew": 1100.0,
            "R_H2O": 0.41,
            "thermometer_temp_c": 20.1,
            "T1": 64.6,
            "TempSet": None,
            "BAR": 102.0,
        },
    ]

    with summary_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=headers)
        writer.writeheader()
        writer.writerows(rows)

    captured = {}

    def _fake_fit(analyzer_rows, **kwargs):
        captured["rows"] = list(analyzer_rows)
        captured["kwargs"] = dict(kwargs)
        return SimpleNamespace(
            model="ratio_poly_rt_p",
            gas="h2o",
            n=len(captured["rows"]),
            stats={"rmse_simplified": 0.0, "max_abs_simplified": 0.0},
        )

    def _fake_save(result, out_dir, prefix, include_residuals=True):
        payload = {"model": result.model, "n": result.n}
        out_path = Path(out_dir) / f"{prefix}_fit_test.json"
        out_path.write_text(json.dumps(payload), encoding="utf-8")
        return {"json": out_path}

    monkeypatch.setattr(runner_module, "fit_ratio_poly_rt_p", _fake_fit)
    monkeypatch.setattr(runner_module, "save_ratio_poly_report", _fake_save)

    logger = SimpleNamespace(
        analyzer_summary_csv_path=summary_path,
        samples_path=samples_path,
    )
    messages = []
    cfg = {
        "coefficients": {
            "enabled": True,
            "auto_fit": True,
            "model": "ratio_poly_rt_p",
            "ratio_degree": 3,
            "simplify_coefficients": False,
            "fit_h2o": True,
        }
    }
    runner = CalibrationRunner(cfg, {"gas_analyzer": object()}, logger, messages.append, lambda *_: None)

    runner._all_samples = [{"dummy": 1}]
    runner._maybe_write_coefficients()

    selected_tags = [row["PointTag"] for row in captured["rows"]]
    assert selected_tags == ["h2o_20", "h2o_30", "co2_zero_m20", "co2_zero_m10", "co2_zero_0", "co2_zero_10", "co2_100_m20"]
    assert captured["kwargs"]["gas"] == "h2o"
    assert captured["kwargs"]["ratio_keys"] == ("R_H2O",)
    assert captured["kwargs"]["target_key"] == "ppm_H2O_Dew"
    assert captured["kwargs"]["temp_keys"][0] == "thermometer_temp_c"
    assert any(
        "H2O ratio-poly summary selection: h2o_phase=2 co2_temp_group=4 co2_zero_temp=1 total=7" in message
        for message in messages
    )


def test_runner_static_sencos_support_variable_length_payloads(tmp_path: Path) -> None:
    writes: list[tuple[int, tuple[float, ...]]] = []
    mode_calls: list[int] = []

    class _FakeGasAnalyzer:
        def set_mode(self, mode: int) -> bool:
            mode_calls.append(int(mode))
            return True

        def set_senco(self, index: int, *coeffs: float) -> bool:
            writes.append((int(index), tuple(float(value) for value in coeffs)))
            return True

    logger = SimpleNamespace(
        analyzer_summary_csv_path=tmp_path / "summary.csv",
        samples_path=tmp_path / "samples.csv",
    )
    logger.samples_path.write_text("", encoding="utf-8")
    cfg = {
        "coefficients": {
            "enabled": True,
            "auto_fit": False,
            "sencos": {
                "1": {"values": [1.0, 2.0, 3.0, 4.0, 0.0, 0.0]},
                "7": {"A": 0.0, "B": 1.0, "C": 0.0, "D": 0.0},
            },
        }
    }
    messages: list[str] = []
    runner = CalibrationRunner(cfg, {"gas_analyzer": _FakeGasAnalyzer()}, logger, messages.append, lambda *_: None)

    runner._maybe_write_coefficients()

    assert writes == [
        (1, (1.0, 2.0, 3.0, 4.0, 0.0, 0.0)),
        (7, (0.0, 1.0, 0.0, 0.0)),
    ]
    assert mode_calls == [2, 1]
    assert any(
        "Wrote SENCO1 1.00000e00,2.00000e00,3.00000e00,4.00000e00,0.00000e00,0.00000e00" in message
        for message in messages
    )


def test_runner_static_sencos_attempts_mode_exit_after_write_failure(tmp_path: Path) -> None:
    mode_calls: list[int] = []

    class _FakeGasAnalyzer:
        def set_mode(self, mode: int) -> bool:
            mode_calls.append(int(mode))
            return True

        def set_senco(self, index: int, *coeffs: float) -> bool:
            raise RuntimeError(f"boom-{index}")

    logger = SimpleNamespace(
        analyzer_summary_csv_path=tmp_path / "summary.csv",
        samples_path=tmp_path / "samples.csv",
    )
    logger.samples_path.write_text("", encoding="utf-8")
    cfg = {
        "coefficients": {
            "enabled": True,
            "auto_fit": False,
            "sencos": {"7": {"A": 0.0, "B": 1.0, "C": 0.0, "D": 0.0}},
        }
    }
    messages: list[str] = []
    runner = CalibrationRunner(cfg, {"gas_analyzer": _FakeGasAnalyzer()}, logger, messages.append, lambda *_: None)

    runner._maybe_write_coefficients()

    assert mode_calls == [2, 1]
    assert any("Failed to write SENCO7: boom-7" in message for message in messages)


def test_sample_and_log_writes_mode2_field_means(tmp_path: Path) -> None:
    cfg = {
        "workflow": {
            "sampling": {
                "stable_count": 2,
                "interval_s": 0.0,
                "quality": {"enabled": False},
            },
            "reporting": {"defer_heavy_exports_during_handoff": False},
        }
    }
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(cfg, {"gas_analyzer": object()}, logger, lambda *_: None, lambda *_: None)

    rows = [
        {
            "co2_ppm": 400.0,
            "h2o_mmol": 6.0,
            "co2_signal": 1000.0,
            "mode": 2,
            "ga01_co2_ppm": 400.0,
            "ga01_h2o_mmol": 6.0,
            "ga01_co2_signal": 1000.0,
            "ga01_mode": 2,
            "point_row": 1,
        },
        {
            "co2_ppm": 402.0,
            "h2o_mmol": 8.0,
            "co2_signal": 1100.0,
            "mode": 2,
            "ga01_co2_ppm": 402.0,
            "ga01_h2o_mmol": 8.0,
            "ga01_co2_signal": 1100.0,
            "ga01_mode": 2,
            "point_row": 1,
        },
    ]
    runner._collect_samples = types.MethodType(lambda self, *_args, **_kwargs: list(rows), runner)

    runner._sample_and_log(_point())
    logger.close()

    import csv

    with logger.points_path.open("r", encoding="utf-8", newline="") as f:
        rr = list(csv.DictReader(f))
    assert len(rr) == 1
    row = rr[0]

    assert float(row["二氧化碳浓度ppm_平均值"]) == 401.0
    assert float(row["水浓度mmol每mol_平均值"]) == 7.0
    assert float(row["二氧化碳信号_平均值"]) == 1050.0
    assert float(row["模式_平均值"]) == 2.0
    assert float(row["气体分析仪1_二氧化碳浓度ppm_平均值"]) == 401.0
    assert float(row["气体分析仪1_水浓度mmol每mol_平均值"]) == 7.0
    assert float(row["气体分析仪1_二氧化碳信号_平均值"]) == 1050.0
    assert float(row["气体分析仪1_模式_平均值"]) == 2.0


def test_sample_and_log_falls_back_to_first_usable_analyzer_for_legacy_means(tmp_path: Path) -> None:
    cfg = {
        "workflow": {
            "sampling": {
                "stable_count": 2,
                "interval_s": 0.0,
                "quality": {"enabled": True, "retries": 0, "max_span_co2_ppm": 0.5},
            },
            "reporting": {"defer_heavy_exports_during_handoff": False},
        }
    }
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(cfg, {"gas_analyzer": object()}, logger, lambda *_: None, lambda *_: None)
    runner._gas_analyzers = types.MethodType(
        lambda self: [("GA01", object(), {}), ("GA02", object(), {})],
        runner,
    )
    runner._all_gas_analyzers = types.MethodType(
        lambda self: [("GA01", object(), {}), ("GA02", object(), {})],
        runner,
    )

    rows = [
        {
            "point_row": 1,
            "point_title": "demo",
            "frame_usable": False,
            "co2_ppm": None,
            "ga01_frame_usable": False,
            "ga01_co2_ppm": 9999.0,
            "ga02_frame_usable": True,
            "ga02_co2_ppm": 400.0,
            "pressure_hpa": 1000.0,
        },
        {
            "point_row": 1,
            "point_title": "demo",
            "frame_usable": False,
            "co2_ppm": None,
            "ga01_frame_usable": False,
            "ga01_co2_ppm": 9998.0,
            "ga02_frame_usable": True,
            "ga02_co2_ppm": 401.0,
            "pressure_hpa": 1000.0,
        },
    ]
    runner._collect_samples = types.MethodType(lambda self, *_args, **_kwargs: list(rows), runner)

    captured: dict[str, object] = {}
    runner.logger.log_point = lambda row: captured.setdefault("row", row)

    runner._sample_and_log(_point())
    logger.close()

    row = captured["row"]
    assert row["co2_mean"] == 400.5
    assert row["co2_std"] == pytest.approx(0.7071067811865476)
    assert runner._evaluate_sample_quality(rows) == (False, {"co2_ppm": 1.0})


def test_collect_samples_uses_preseal_dewpoint_snapshot_for_h2o(tmp_path: Path) -> None:
    class _FakePace:
        def read_pressure(self):
            return 1000.5

    class _FakeGauge:
        def read_pressure(self):
            return 1000.2

    class _FakeDew:
        def get_current(self, timeout_s=None, attempts=None):
            return {"dewpoint_c": 9.9, "temp_c": 25.0, "rh_pct": 88.0}

    class _FakeChamber:
        def read_temp_c(self):
            return 20.1

        def read_rh_pct(self):
            return 99.0

    class _FakeHgen:
        def fetch_all(self):
            return {"raw": "demo", "data": {"Tc": 20.2, "Uw": 30.1}}

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
            "humidity_gen": _FakeHgen(),
        },
        logger,
        lambda *_: None,
        lambda *_: None,
    )
    runner._preseal_dewpoint_snapshot = {
        "sample_ts": "2026-03-07T12:00:00.000",
        "dewpoint_c": 1.23,
        "temp_c": 20.0,
        "rh_pct": 28.5,
        "pressure_hpa": 999.8,
    }
    runner._read_sensor_parsed = types.MethodType(
        lambda self, _ga, required_key=None: ("ROW", {"co2_ppm": 400.0, "h2o_mmol": 6.0}),
        runner,
    )

    rows = runner._collect_samples(_point(), 1, 0.0, phase="h2o", point_tag="")
    logger.close()

    assert rows is not None
    row = rows[0]
    assert row["dewpoint_c"] == 1.23
    assert row["dew_temp_c"] == 20.0
    assert row["dew_rh_pct"] == 28.5
    assert row["dew_pressure_hpa"] == 999.8
    assert row["dewpoint_sample_ts"] == "2026-03-07T12:00:00.000"
    assert row["dewpoint_live_c"] == 9.9
    assert row["dew_temp_live_c"] == 25.0
    assert row["dew_rh_live_pct"] == 88.0
    assert row["dewpoint_live_sample_ts"]
    assert row["pressure_hpa"] == 1000.5
    assert row["pressure_gauge_hpa"] == 1000.2
    assert row["chamber_temp_c"] == 20.1
    assert row["hgen_Tc"] == 20.2
    assert row["sample_due_ts"]
    assert row["sample_start_ts"]
    assert row["sample_end_ts"]
    assert row["fast_group_anchor_ts"] == row["sample_ts"]
    assert row["fast_group_span_ms"] >= 0.0
    assert row["point_title"] == "20°C环境，气压1000hPa"

def test_collect_samples_records_digital_thermometer_temperature(tmp_path: Path) -> None:
    class _FakeThermometer:
        def read_temp_c(self):
            return 20.56

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
            "thermometer": _FakeThermometer(),
        },
        logger,
        lambda *_: None,
        lambda *_: None,
    )
    runner._read_sensor_parsed = types.MethodType(
        lambda self, _ga, required_key=None: ("ROW", {"co2_ppm": 400.0, "h2o_mmol": 6.0}),
        runner,
    )

    rows = runner._collect_samples(_point(), 1, 0.0, phase="co2", point_tag="")
    logger.close()

    assert rows is not None
    assert rows[0]["thermometer_temp_c"] == 20.56


def test_collect_samples_keeps_environment_chamber_temperature_and_prefixed_analyzer_temp(tmp_path: Path) -> None:
    class _FakeChamber:
        def read_temp_c(self):
            return 20.0

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
            "temp_chamber": _FakeChamber(),
        },
        logger,
        lambda *_: None,
        lambda *_: None,
    )
    runner._read_sensor_parsed = types.MethodType(
        lambda self, _ga, require_usable=False: (
            "ROW",
            {
                "co2_ppm": 400.0,
                "h2o_mmol": 6.0,
                "co2_ratio_f": 1.0,
                "h2o_ratio_f": 0.2,
                "pressure_kpa": 101.2,
                "chamber_temp_c": 19.0,
            },
        ),
        runner,
    )
    runner._assess_analyzer_frame = types.MethodType(lambda self, _parsed: (True, "可用"), runner)

    rows = runner._collect_samples(_point(), 1, 0.0, phase="co2", point_tag="")
    logger.close()

    assert rows is not None
    row = rows[0]
    assert row["chamber_temp_c"] == 20.0
    assert row["env_chamber_temp_c"] == 20.0
    assert row["ga01_chamber_temp_c"] == 19.0


def test_collect_samples_reads_temperature_snapshot_before_analyzer_frames(tmp_path: Path) -> None:
    call_order = []

    class _FakeThermometer:
        def read_temp_c(self):
            call_order.append("thermometer")
            return 20.56

    class _FakeChamber:
        def read_temp_c(self):
            call_order.append("chamber_temp")
            return 20.1

        def read_rh_pct(self):
            call_order.append("chamber_rh")
            return 55.0

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
            "thermometer": _FakeThermometer(),
            "temp_chamber": _FakeChamber(),
        },
        logger,
        lambda *_: None,
        lambda *_: None,
    )

    def _fake_read_sensor_parsed(self, _ga, require_usable=False):
        call_order.append("analyzer")
        return "ROW", {"co2_ppm": 400.0, "h2o_mmol": 6.0}

    runner._read_sensor_parsed = types.MethodType(_fake_read_sensor_parsed, runner)

    rows = runner._collect_samples(_point(), 1, 0.0, phase="co2", point_tag="")
    logger.close()

    assert rows is not None
    assert call_order[0] == "analyzer"
    assert call_order[1:] == ["chamber_temp", "chamber_rh", "thermometer"]


def test_collect_samples_does_not_sleep_after_last_sample(monkeypatch, tmp_path: Path) -> None:
    sleep_calls: list[float] = []
    monkeypatch.setattr(runner_module.time, "sleep", lambda seconds: sleep_calls.append(seconds))

    cfg = {
        "workflow": {
            "sampling": {
                "stable_count": 3,
                "interval_s": 1.5,
                "quality": {"enabled": False},
            }
        }
    }
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(
        cfg,
        {"gas_analyzer": object()},
        logger,
        lambda *_: None,
        lambda *_: None,
    )
    runner._read_sensor_parsed = types.MethodType(
        lambda self, _ga, require_usable=False: ("ROW", {"co2_ppm": 400.0, "h2o_mmol": 6.0}),
        runner,
    )

    rows = runner._collect_samples(_point(), 3, 1.5, phase="co2", point_tag="")
    logger.close()

    assert rows is not None
    assert len(rows) == 3
    assert len(sleep_calls) == 2
    assert sleep_calls[0] == pytest.approx(1.5, abs=0.05)
    assert sleep_calls[1] >= sleep_calls[0]
    assert rows[0]["sample_due_ts"] != rows[1]["sample_due_ts"]
    assert rows[1]["sample_due_ts"] != rows[2]["sample_due_ts"]
    assert rows[0]["sample_lag_ms"] >= 0.0
    assert rows[1]["sample_lag_ms"] >= 0.0
    assert rows[2]["sample_lag_ms"] >= 0.0


def test_point_title_for_h2o_contains_temp_rh_and_pressure(tmp_path: Path) -> None:
    point = CalibrationPoint(
        index=21,
        temp_chamber_c=20.0,
        co2_ppm=None,
        hgen_temp_c=20.0,
        hgen_rh_pct=30.0,
        target_pressure_hpa=1100.0,
        dewpoint_c=-10.0,
        h2o_mmol=2.0,
        raw_h2o="demo",
    )
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner({}, {}, logger, lambda *_: None, lambda *_: None)
    try:
        assert runner._point_title(point, phase="h2o") == "20°C环境，湿度发生器20°C/30%RH，气压1100hPa"
    finally:
        logger.close()


def test_runner_logs_stop_request_and_stop_exit_to_io(monkeypatch, tmp_path: Path) -> None:
    point = _point()
    monkeypatch.setattr(runner_module, "load_points_from_excel", lambda *_args, **_kwargs: [point])
    monkeypatch.setattr(runner_module, "reorder_points", lambda points, *_args, **_kwargs: points)
    monkeypatch.setattr(runner_module, "validate_points", lambda *_args, **_kwargs: [])

    cfg = {
        "paths": {"points_excel": "demo.xlsx"},
        "workflow": {"collect_only": True, "missing_pressure_policy": "carry_forward"},
    }
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(cfg, {}, logger, lambda *_: None, lambda *_: None)
    runner._sensor_precheck = types.MethodType(lambda self: None, runner)
    runner._configure_devices = types.MethodType(lambda self: None, runner)
    runner._startup_preflight_reset = types.MethodType(lambda self: None, runner)
    runner._startup_pressure_precheck = types.MethodType(lambda self, _points: None, runner)
    runner._run_points = types.MethodType(lambda self, _points: self.stop(), runner)
    runner._maybe_write_coefficients = types.MethodType(lambda self: None, runner)

    runner.run()
    logger.close()

    with logger.io_path.open("r", encoding="utf-8", newline="") as f:
        rows = list(csv.DictReader(f))

    runner_events = [row for row in rows if row["port"] == "RUN" and row["device"] == "runner"]
    commands = [row["command"] for row in runner_events]
    assert "run-start" in commands
    assert "stop-request" in commands
    assert "run-stop" in commands
    assert "run-cleanup" in commands
    assert "run-finished" not in commands


def test_runner_cleanup_restores_baseline_before_close(monkeypatch, tmp_path: Path) -> None:
    point = _point()
    monkeypatch.setattr(runner_module, "load_points_from_excel", lambda *_args, **_kwargs: [point])
    monkeypatch.setattr(runner_module, "reorder_points", lambda points, *_args, **_kwargs: points)
    monkeypatch.setattr(runner_module, "validate_points", lambda *_args, **_kwargs: [])

    called = {}

    def _fake_safe_stop(devices, log_fn=None, cfg=None):
        called["devices"] = devices
        called["cfg"] = cfg
        return {"hgen_stop_check": {"ok": True, "flow_lpm": 0.0}}

    monkeypatch.setattr(runner_module, "_perform_safe_stop", _fake_safe_stop)

    cfg = {
        "paths": {"points_excel": "demo.xlsx"},
        "workflow": {"collect_only": True, "missing_pressure_policy": "carry_forward"},
    }
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(cfg, {"pace": object()}, logger, lambda *_: None, lambda *_: None)
    runner._sensor_precheck = types.MethodType(lambda self: None, runner)
    runner._configure_devices = types.MethodType(lambda self: None, runner)
    runner._startup_preflight_reset = types.MethodType(lambda self: None, runner)
    runner._startup_pressure_precheck = types.MethodType(lambda self, _points: None, runner)
    runner._run_points = types.MethodType(lambda self, _points: None, runner)
    runner._maybe_write_coefficients = types.MethodType(lambda self: None, runner)

    runner.run()

    assert called["devices"] is runner.devices
    assert called["cfg"] is runner.cfg


def test_runner_cleanup_logs_incomplete_baseline_restore(monkeypatch, tmp_path: Path) -> None:
    point = _point()
    monkeypatch.setattr(runner_module, "load_points_from_excel", lambda *_args, **_kwargs: [point])
    monkeypatch.setattr(runner_module, "reorder_points", lambda points, *_args, **_kwargs: points)
    monkeypatch.setattr(runner_module, "validate_points", lambda *_args, **_kwargs: [])

    logs = []

    def _fake_safe_stop(devices, log_fn=None, cfg=None):
        return {
            "hgen_safe_stop": {
                "flow_off": "ok",
                "ctrl_off": "ok",
                "cool_off": "ok",
                "heat_off": "ok",
            },
            "hgen_stop_check": {"ok": False, "flow_lpm": 1.0},
            "safe_stop_verified": False,
            "safe_stop_issues": ["humidity generator stop check failed"],
        }

    monkeypatch.setattr(runner_module, "_perform_safe_stop", _fake_safe_stop)

    cfg = {
        "paths": {"points_excel": "demo.xlsx"},
        "workflow": {"collect_only": True, "missing_pressure_policy": "carry_forward"},
    }
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(cfg, {"pace": object()}, logger, lambda msg: logs.append(str(msg)), lambda *_: None)
    runner._sensor_precheck = types.MethodType(lambda self: None, runner)
    runner._configure_devices = types.MethodType(lambda self: None, runner)
    runner._startup_preflight_reset = types.MethodType(lambda self: None, runner)
    runner._startup_pressure_precheck = types.MethodType(lambda self, _points: None, runner)
    runner._run_points = types.MethodType(lambda self, _points: None, runner)
    runner._maybe_write_coefficients = types.MethodType(lambda self: None, runner)

    runner.run()
    logger.close()

    assert any("Baseline restore incomplete: humidity generator stop check failed" in msg for msg in logs)


def test_sampling_window_analyzer_worker_uses_finish_time_for_next_due(monkeypatch, tmp_path: Path) -> None:
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(
        {
            "workflow": {
                "analyzer_live_snapshot": {
                    "sampling_worker_enabled": True,
                    "sampling_worker_interval_s": 0.2,
                    "passive_round_robin_enabled": True,
                    "passive_round_robin_interval_s": 0.25,
                }
            }
        },
        {},
        logger,
        lambda *_: None,
        lambda *_: None,
    )

    active_ga = object()
    passive_ga = object()
    runner._all_gas_analyzers = types.MethodType(
        lambda self: [
            ("ga_active", active_ga, {"active_send": True, "ftd_hz": 10}),
            ("ga_passive", passive_ga, {"active_send": False, "ftd_hz": 10}),
        ],
        runner,
    )

    clock = {"t": 10.0}
    monkeypatch.setattr(runner_module.time, "monotonic", lambda: clock["t"])

    refresh_calls: list[tuple[str, float]] = []

    def _fake_refresh(self, label, _ga, _cfg, context=None, reason=""):
        refresh_calls.append((label, round(clock["t"], 3)))
        if label == "ga_active":
            clock["t"] = 11.0
        else:
            clock["t"] = 11.05

    runner._refresh_sampling_analyzer_cache_entry = types.MethodType(_fake_refresh, runner)

    context = runner._new_sampling_window_context(point=_point(), phase="co2", point_tag="demo")
    context["worker_plan"] = runner._sampling_window_worker_plan()
    wait_deadlines: list[float] = []

    def _fake_wait_until(self, deadline_monotonic, *, stop_event=None):
        wait_deadlines.append(float(deadline_monotonic))
        context["stop_event"].set()
        return False

    runner._sampling_window_wait_until = types.MethodType(_fake_wait_until, runner)

    runner._sampling_window_analyzer_worker(context)
    logger.close()

    assert [label for label, _ in refresh_calls] == ["ga_active", "ga_passive"]
    assert wait_deadlines
    assert wait_deadlines[0] > 11.05


def test_start_sampling_window_context_respects_slow_aux_cache_enabled_false(tmp_path: Path) -> None:
    logs: list[str] = []
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(
        {"workflow": {"sampling": {"slow_aux_cache_enabled": False}}},
        {"temp_chamber": object()},
        logger,
        lambda msg: logs.append(str(msg)),
        lambda *_: None,
    )
    runner._all_gas_analyzers = types.MethodType(lambda self: [], runner)

    context = runner._start_sampling_window_context(point=_point(), phase="co2", point_tag="demo")
    runner._stop_sampling_window_context(context)
    logger.close()

    assert context["workers"] == []
    assert any("Sampling window start:" in msg and "slow_aux_worker_enabled=False" in msg for msg in logs)
    assert any("Sampling window stop:" in msg for msg in logs)


def test_start_sampling_window_context_starts_one_fast_signal_worker_per_device(tmp_path: Path) -> None:
    class _FakePace:
        def read_pressure(self):
            return 1000.0

    class _FakeGauge:
        def read_pressure(self):
            return 1000.0

    class _FakeDew:
        def get_current(self):
            return {"dewpoint_c": -10.0, "temp_c": 20.0, "rh_pct": 50.0}

    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(
        {"workflow": {"sampling": {"fast_signal_worker_enabled": True}}},
        {"pace": _FakePace(), "pressure_gauge": _FakeGauge(), "dewpoint": _FakeDew()},
        logger,
        lambda *_: None,
        lambda *_: None,
    )
    runner._all_gas_analyzers = types.MethodType(lambda self: [], runner)
    runner._prime_sampling_window_context = types.MethodType(lambda self, context, **kwargs: None, runner)

    context = runner._start_sampling_window_context(point=_point(), phase="co2", point_tag="demo")
    worker_keys = {entry["key"] for entry in context["workers"]}
    runner._stop_sampling_window_context(context)
    logger.close()

    assert {"fast_signal:pace", "fast_signal:pressure_gauge", "fast_signal:dewpoint"} <= worker_keys


def test_prime_sampling_window_context_skips_analyzer_prime_when_workers_enabled(tmp_path: Path) -> None:
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner({}, {}, logger, lambda *_: None, lambda *_: None)
    context = runner._new_sampling_window_context(point=_point(), phase="co2", point_tag="demo")
    plan = {
        "active_entries": [("ga01", object(), {})],
        "passive_entries": [("ga02", object(), {})],
        "active_enabled": True,
        "passive_enabled": True,
        "analyzer_worker_enabled": True,
        "skip_analyzer_prime": True,
        "fast_signal_enabled": False,
        "slow_aux_enabled": False,
    }
    called: list[str] = []
    runner._prime_sampling_analyzer_cache_once = types.MethodType(
        lambda self, *args, **kwargs: called.append(str(kwargs.get("reason") or "")),
        runner,
    )

    runner._prime_sampling_window_context(context, worker_plan=plan, reason="sampling window start")
    logger.close()

    assert called == []


def test_prime_sampling_window_context_skips_fast_signal_and_pace_state_prime_when_requested(
    tmp_path: Path,
) -> None:
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(
        {},
        {"pace": object(), "pressure_gauge": object(), "dewpoint": object()},
        logger,
        lambda *_: None,
        lambda *_: None,
    )
    context = runner._new_sampling_window_context(point=_point(), phase="co2", point_tag="demo")
    plan = {
        "fast_signal_enabled": True,
        "skip_fast_signal_prime": True,
        "skip_pace_state_prime": True,
        "skip_slow_aux_prime": True,
        "active_entries": [],
        "passive_entries": [],
    }
    called: list[str] = []
    runner._refresh_fast_signal_cache_once = types.MethodType(
        lambda self, *_args, **_kwargs: called.append("fast_signal"),
        runner,
    )
    runner._pace_state_snapshot = types.MethodType(
        lambda self, *_args, **_kwargs: called.append("pace_state") or {},
        runner,
    )

    runner._prime_sampling_window_context(context, worker_plan=plan, reason="sampling window start")
    logger.close()

    assert called == []


def test_start_sampling_window_context_marks_reusable_pace_state_as_non_blocking_prime(tmp_path: Path) -> None:
    class _FakePace:
        def read_pressure(self):
            return 1000.0

    class _FakeGauge:
        def read_pressure(self):
            return 1000.0

    class _FakeDew:
        def get_current(self):
            return {"dewpoint_c": -10.0, "temp_c": 20.0, "rh_pct": 50.0}

    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(
        {"workflow": {"sampling": {"fast_signal_worker_enabled": True}}},
        {"pace": _FakePace(), "pressure_gauge": _FakeGauge(), "dewpoint": _FakeDew()},
        logger,
        lambda *_: None,
        lambda *_: None,
    )
    runner._all_gas_analyzers = types.MethodType(lambda self: [], runner)
    runner._last_sample_completion = {
        "pace_output_state": 1,
        "pace_isolation_state": 1,
        "pace_vent_status": 0,
    }
    captured: dict[str, object] = {}
    runner._prime_sampling_window_context = types.MethodType(
        lambda self, context, **kwargs: captured.update({"worker_plan": dict(kwargs.get("worker_plan") or {})}),
        runner,
    )

    context = runner._start_sampling_window_context(point=_point(), phase="co2", point_tag="demo")
    runner._stop_sampling_window_context(context)
    logger.close()

    worker_plan = dict(captured.get("worker_plan") or {})
    assert worker_plan["skip_fast_signal_prime"] is True
    assert worker_plan["skip_pace_state_prime"] is True


def test_sampling_window_fast_signal_device_worker_refreshes_immediately_before_wait(tmp_path: Path) -> None:
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner({}, {}, logger, lambda *_: None, lambda *_: None)
    context = runner._new_sampling_window_context(point=_point(), phase="co2", point_tag="demo")
    calls: list[tuple[str, str]] = []
    waits: list[float] = []

    def _fake_refresh(self, context, signal_key, *, reason=""):
        calls.append((str(signal_key), str(reason)))
        context["stop_event"].set()

    runner._refresh_fast_signal_entry = types.MethodType(_fake_refresh, runner)
    runner._sampling_window_wait = types.MethodType(
        lambda self, interval_s, stop_event=None: waits.append(float(interval_s)) or False,
        runner,
    )

    runner._sampling_window_fast_signal_device_worker(context, signal_key="pace")
    logger.close()

    assert calls == [("pace", "sampling window start")]
    assert len(waits) == 1


def test_refresh_fast_signal_entry_uses_fast_pace_query_path_when_available(tmp_path: Path) -> None:
    class _FastPace:
        pressure_queries = [":SENS:PRES:INL?"]
        query_line_endings = ["\n"]

        def read_pressure(self):
            raise AssertionError("fast sampling path should not call the slow PACE reader")

        def query(self, cmd, line_ending=None):
            assert cmd == ":SENS:PRES:INL?"
            assert line_ending == "\n"
            return ":SENS:PRES:INL 1001.25, 0"

        @staticmethod
        def _parse_first_float(text):
            return 1001.25 if text else None

    logger = RunLogger(tmp_path)
    runner = CalibrationRunner({}, {"pace": _FastPace()}, logger, lambda *_: None, lambda *_: None)
    context = runner._new_sampling_window_context(point=_point(), phase="co2", point_tag="demo")

    runner._refresh_fast_signal_entry(context, "pace", reason="test")
    logger.close()

    frames = runner._sampling_window_fast_signal_frames(context, "pace")
    assert len(frames) == 1
    assert frames[0]["values"]["pressure_hpa"] == 1001.25


def test_wait_for_sampling_freshness_gate_uses_async_workers_without_sync_refresh(tmp_path: Path) -> None:
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(
        {},
        {"pace": object(), "pressure_gauge": object(), "dewpoint": object()},
        logger,
        lambda *_: None,
        lambda *_: None,
    )
    point = _point()
    context = runner._new_sampling_window_context(point=point, phase="co2", point_tag="demo")
    context["worker_plan"] = {
        "fast_signal_enabled": True,
        "analyzer_worker_enabled": False,
        "active_entries": [],
        "passive_entries": [],
    }
    base_mono = runner_module.time.monotonic()
    with context["lock"]:
        context["fast_signal_buffers"]["pace"].append(
            {
                "recv_wall_ts": "2026-04-05T10:51:28.307",
                "timestamp": 1000.0,
                "recv_mono_s": base_mono - 0.05,
                "values": {"pressure_hpa": 1000.0},
                "source": "pace_read_pressure",
                "seq": 1,
            }
        )
        context["fast_signal_buffers"]["pressure_gauge"].append(
            {
                "recv_wall_ts": "2026-04-05T10:51:28.307",
                "timestamp": 1000.0,
                "recv_mono_s": base_mono - 0.04,
                "values": {"pressure_gauge_raw": 1000.1, "pressure_gauge_hpa": 1000.1},
                "source": "pressure_gauge_read",
                "seq": 2,
            }
        )
        context["fast_signal_buffers"]["dewpoint"].append(
            {
                "recv_wall_ts": "2026-04-05T10:51:28.307",
                "timestamp": 1000.0,
                "recv_mono_s": base_mono - 0.03,
                "values": {
                    "dewpoint_live_c": -10.2,
                    "dew_temp_live_c": 23.4,
                    "dew_rh_live_pct": 44.5,
                },
                "source": "dewpoint_live_read",
                "seq": 3,
            }
        )
    runner._refresh_fast_signal_cache_once = types.MethodType(
        lambda self, *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("unexpected sync fast-signal prime")),
        runner,
    )
    runner._refresh_fast_signal_entry = types.MethodType(
        lambda self, *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("unexpected sync fast-signal refresh")),
        runner,
    )

    metrics = runner._wait_for_sampling_freshness_gate(
        point=point,
        phase="co2",
        point_tag="demo",
        context=context,
    )
    logger.close()

    assert metrics["status"] == "ready"
    assert metrics["missing"] == []
    assert metrics["ready_values"]["pace_pressure_hpa"] == 1000.0
    assert metrics["ready_values"]["pressure_gauge_hpa"] == 1000.1
    assert metrics["ready_values"]["dewpoint_live_c"] == -10.2


def test_collect_samples_uses_cached_pace_state_when_row_refresh_disabled(tmp_path: Path) -> None:
    class _FakePace:
        def __init__(self) -> None:
            self.read_pressure_calls = 0
            self.output_calls = 0
            self.isolation_calls = 0
            self.vent_calls = 0

        def read_pressure(self):
            self.read_pressure_calls += 1
            return 1000.0 + (self.read_pressure_calls / 10.0)

        def get_output_state(self):
            self.output_calls += 1
            return 1

        def get_isolation_state(self):
            self.isolation_calls += 1
            return 1

        def get_vent_status(self):
            self.vent_calls += 1
            return 0

    pace = _FakePace()
    cfg = {
        "workflow": {
            "sampling": {
                "stable_count": 2,
                "interval_s": 0.0,
                "pace_state_every_n_samples": 0,
                "pace_state_cache_enabled": True,
                "quality": {"enabled": False},
            }
        }
    }
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(cfg, {"pace": pace}, logger, lambda *_: None, lambda *_: None)

    rows = runner._collect_samples(_point(), 2, 0.0, phase="co2", point_tag="")
    logger.close()

    assert rows is not None
    assert [row["pace_output_state"] for row in rows] == [1, 1]
    assert pace.read_pressure_calls == 1
    assert pace.output_calls == 1
    assert pace.isolation_calls == 1
    assert pace.vent_calls == 1


def test_collect_samples_refreshes_pace_state_on_configured_stride(tmp_path: Path) -> None:
    class _FakePace:
        def __init__(self) -> None:
            self.output_calls = 0
            self.isolation_calls = 0
            self.vent_calls = 0

        def read_pressure(self):
            return 1000.0

        def get_output_state(self):
            self.output_calls += 1
            return self.output_calls

        def get_isolation_state(self):
            self.isolation_calls += 1
            return 10 + self.isolation_calls

        def get_vent_status(self):
            self.vent_calls += 1
            return 20 + self.vent_calls

    pace = _FakePace()
    cfg = {
        "workflow": {
            "sampling": {
                "stable_count": 3,
                "interval_s": 0.0,
                "pace_state_every_n_samples": 2,
                "pace_state_cache_enabled": True,
                "quality": {"enabled": False},
            }
        }
    }
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(cfg, {"pace": pace}, logger, lambda *_: None, lambda *_: None)

    rows = runner._collect_samples(_point(), 3, 0.0, phase="co2", point_tag="")
    logger.close()

    assert rows is not None
    assert [row["pace_output_state"] for row in rows] == [1, 1, 2]
    assert [row["pace_isolation_state"] for row in rows] == [11, 11, 12]
    assert [row["pace_vent_status"] for row in rows] == [21, 21, 22]
    assert pace.output_calls == 2
    assert pace.isolation_calls == 2
    assert pace.vent_calls == 2


def test_collect_samples_uses_fast_signal_ring_buffers_when_sampling_context_is_active(tmp_path: Path) -> None:
    class _FailIfReadPace:
        def read_pressure(self):
            raise AssertionError("pace.read_pressure should not run in 1Hz row assembly")

        def get_output_state(self):
            return 1

        def get_isolation_state(self):
            return 1

        def get_vent_status(self):
            return 0

    class _FailIfReadGauge:
        def read_pressure(self):
            raise AssertionError("gauge.read_pressure should not run in 1Hz row assembly")

    class _FailIfReadDew:
        def get_current(self):
            raise AssertionError("dew.get_current should not run in 1Hz row assembly")

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
            "pace": _FailIfReadPace(),
            "pressure_gauge": _FailIfReadGauge(),
            "dewpoint": _FailIfReadDew(),
        },
        logger,
        lambda *_: None,
        lambda *_: None,
    )
    context = runner._new_sampling_window_context(point=_point(), phase="co2", point_tag="demo")
    base_mono = runner_module.time.monotonic()
    with context["lock"]:
        context["fast_signal_buffers"]["pace"].append(
            {
                "recv_wall_ts": "2026-03-30T12:00:00.000",
                "timestamp": 1000.0,
                "recv_mono_s": base_mono - 0.03,
                "values": {"pressure_hpa": 999.4},
                "source": "pace_read_pressure",
                "seq": 1,
            }
        )
        context["fast_signal_buffers"]["pressure_gauge"].append(
            {
                "recv_wall_ts": "2026-03-30T12:00:00.010",
                "timestamp": 1000.01,
                "recv_mono_s": base_mono - 0.02,
                "values": {"pressure_gauge_raw": 1000.1, "pressure_gauge_hpa": 1000.1},
                "source": "pressure_gauge_read",
                "seq": 2,
            }
        )
        context["fast_signal_buffers"]["dewpoint"].append(
            {
                "recv_wall_ts": "2026-03-30T12:00:00.020",
                "timestamp": 1000.02,
                "recv_mono_s": base_mono - 0.01,
                "values": {
                    "dewpoint_live_c": -11.2,
                    "dew_temp_live_c": 23.4,
                    "dew_rh_live_pct": 44.5,
                },
                "source": "dewpoint_live_read",
                "seq": 3,
            }
        )
    runner._sampling_window_context = context

    try:
        rows = runner._collect_samples(_point(), 1, 0.0, phase="co2", point_tag="demo")
    finally:
        runner._sampling_window_context = None
        logger.close()

    assert rows is not None
    row = rows[0]
    assert row["pressure_hpa"] == 999.4
    assert row["pressure_gauge_hpa"] == 1000.1
    assert row["dewpoint_live_c"] == -11.2
    assert row["pace_sample_ts"] == "2026-03-30T12:00:00.000"
    assert row["pressure_gauge_sample_ts"] == "2026-03-30T12:00:00.010"
    assert row["dewpoint_live_sample_ts"] == "2026-03-30T12:00:00.020"
    assert row["pace_anchor_delta_ms"] is not None
    assert row["pressure_gauge_anchor_delta_ms"] is not None
    assert row["dewpoint_live_anchor_delta_ms"] is not None


def test_collect_samples_keeps_fixed_rate_when_active_analyzer_frame_is_missing(
    monkeypatch,
    tmp_path: Path,
) -> None:
    class _FailIfReadAnalyzer:
        def read_latest_data(self, *args, **kwargs):
            raise AssertionError("active analyzer should not be read in 1Hz row assembly")

        def parse_line_mode2(self, line):
            return None

    clock = {"t": 0.0}
    sleep_calls: list[float] = []

    def _fake_monotonic():
        return clock["t"]

    def _fake_sleep(seconds):
        sleep_calls.append(float(seconds))
        clock["t"] += float(seconds)

    monkeypatch.setattr(runner_module.time, "monotonic", _fake_monotonic)
    monkeypatch.setattr(runner_module.time, "sleep", _fake_sleep)
    monkeypatch.setattr(runner_module.time, "time", lambda: 1000.0 + clock["t"])

    cfg = {
        "devices": {
            "gas_analyzer": {"active_send": True, "ftd_hz": 10, "average_co2": 1, "average_h2o": 1},
            "gas_analyzers": [{"name": "ga01", "active_send": True, "ftd_hz": 10}],
        },
        "workflow": {
            "sampling": {
                "stable_count": 3,
                "interval_s": 1.0,
                "quality": {"enabled": False},
            }
        },
    }
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(cfg, {"gas_analyzer_01": _FailIfReadAnalyzer()}, logger, lambda *_: None, lambda *_: None)
    runner._sampling_window_context = runner._new_sampling_window_context(point=_point(), phase="co2", point_tag="demo")

    try:
        rows = runner._collect_samples(_point(), 3, 1.0, phase="co2", point_tag="demo")
    finally:
        runner._sampling_window_context = None
        logger.close()

    assert rows is not None
    assert sleep_calls == [1.0, 1.0]
    assert [row["sample_lag_ms"] for row in rows] == [0.0, 0.0, 0.0]
    assert [row["ga01_frame_anchor_side"] for row in rows] == ["missing", "missing", "missing"]
