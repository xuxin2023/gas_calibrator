from pathlib import Path
from types import SimpleNamespace

import gas_calibrator.workflow.runner as runner_module
from gas_calibrator.coefficients.model_feature_policy import AMBIENT_ONLY_MODEL_FEATURES
from gas_calibrator.data.points import CalibrationPoint
from gas_calibrator.logging_utils import RunLogger
from gas_calibrator.workflow.runner import CalibrationRunner


def _runner_with_quality(
    tmp_path: Path,
    quality_cfg: dict,
    *,
    workflow_extra: dict | None = None,
    coefficients_cfg: dict | None = None,
    logs: list[str] | None = None,
) -> CalibrationRunner:
    cfg = {
        "valves": {
            "h2o_path": 8,
            "co2_map": {"400": 3},
        },
        "workflow": {
            "sampling": {
                "quality": quality_cfg,
            }
        },
        "coefficients": coefficients_cfg or {},
    }
    if workflow_extra:
        cfg["workflow"].update(workflow_extra)
    logger = RunLogger(tmp_path, cfg=cfg)
    log_fn = logs.append if logs is not None else (lambda *_: None)
    return CalibrationRunner(cfg, {}, logger, log_fn, lambda *_: None)


def test_evaluate_sample_quality_disabled(tmp_path: Path) -> None:
    runner = _runner_with_quality(tmp_path, {"enabled": False})
    ok, spans = runner._evaluate_sample_quality([{"co2_ppm": 100.0}, {"co2_ppm": 110.0}])
    runner.logger.close()

    assert ok is True
    assert spans == {}


def test_evaluate_sample_quality_exceed_limit(tmp_path: Path) -> None:
    runner = _runner_with_quality(
        tmp_path,
        {
            "enabled": True,
            "max_span_co2_ppm": 3.0,
            "max_span_pressure_hpa": 0.5,
        },
    )

    ok, spans = runner._evaluate_sample_quality(
        [
            {"co2_ppm": 100.0, "pressure_hpa": 1000.1},
            {"co2_ppm": 104.2, "pressure_hpa": 1000.9},
            {"co2_ppm": 102.1, "pressure_hpa": 1000.4},
        ]
    )
    runner.logger.close()

    assert ok is False
    assert spans["co2_ppm"] > 3.0
    assert spans["pressure_hpa"] > 0.5


def test_source_valve_for_co2_and_h2o(tmp_path: Path) -> None:
    runner = _runner_with_quality(tmp_path, {"enabled": False})

    co2_point = CalibrationPoint(
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
    h2o_point = CalibrationPoint(
        index=2,
        temp_chamber_c=20.0,
        co2_ppm=None,
        hgen_temp_c=20.0,
        hgen_rh_pct=50.0,
        target_pressure_hpa=1000.0,
        dewpoint_c=-10.0,
        h2o_mmol=2.0,
        raw_h2o="demo",
    )

    assert runner._source_valve_for_point(co2_point) == 3
    assert runner._source_valve_for_point(h2o_point) == 8
    runner.logger.close()


def test_assess_analyzer_frame_rejects_obvious_invalid_values(tmp_path: Path) -> None:
    runner = _runner_with_quality(tmp_path, {"enabled": False})
    base = {
        "mode2_field_count": 16,
        "status": "OK",
        "co2_ppm": 400.0,
        "h2o_mmol": 1.2,
        "co2_ratio_f": 1.001,
        "h2o_ratio_f": 0.201,
        "pressure_kpa": 101.3,
    }

    ok, status = runner._assess_analyzer_frame(dict(base))
    assert ok is True
    assert status == "可用"

    ok, status = runner._assess_analyzer_frame(dict(base, co2_ppm=-0.1))
    assert ok is False
    assert "co2<0" in status

    ok, status = runner._assess_analyzer_frame(dict(base, h2o_ratio_f=0.0))
    assert ok is False
    assert "R_H2O<=0" in status

    ok, status = runner._assess_analyzer_frame(dict(base, co2_ratio_f=-1001.0))
    assert ok is False
    assert "sentinel" in status

    ok, status = runner._assess_analyzer_frame(dict(base, pressure_kpa=500.0))
    assert ok is False
    assert "P_kPa>" in status
    runner.logger.close()


def test_assess_analyzer_frame_soft_marks_extreme_values_when_ratio_is_usable(tmp_path: Path) -> None:
    runner = _runner_with_quality(tmp_path, {"enabled": False})
    ok, status = runner._assess_analyzer_frame(
        {
            "mode2_field_count": 16,
            "status": "OK",
            "co2_ppm": 3000.0,
            "h2o_mmol": 72.0,
            "co2_ratio_f": 1.002,
            "h2o_ratio_f": 0.201,
            "pressure_kpa": 101.3,
        }
    )
    runner.logger.close()

    assert ok is True
    assert status == "极值已标记"


def test_assess_analyzer_frame_soft_marks_legacy_extreme_values_when_signal_is_usable(tmp_path: Path) -> None:
    runner = _runner_with_quality(tmp_path, {"enabled": False})
    ok, status = runner._assess_analyzer_frame(
        {
            "mode": 1,
            "status": "0003",
            "co2_ppm": 3000.0,
            "h2o_mmol": 72.0,
            "co2_sig": 0.71,
            "h2o_sig": 0.71,
            "pressure_kpa": 101.31,
        }
    )
    runner.logger.close()

    assert ok is True
    assert status == "极值已标记"


def test_assess_mode2_frame_for_startup_accepts_protocol_ready_high_pressure_frame(tmp_path: Path) -> None:
    runner = _runner_with_quality(tmp_path, {"enabled": False})
    parsed = {
        "mode2_field_count": 16,
        "co2_ppm": 1.066,
        "h2o_mmol": 0.762,
        "co2_ratio_f": 1.0654,
        "h2o_ratio_f": 0.7617,
        "pressure_kpa": 190.57,
    }

    startup_ok, startup_status = runner._assess_mode2_frame_for_startup(parsed)
    strict_ok, strict_status = runner._assess_analyzer_frame(parsed)
    runner.logger.close()

    assert startup_ok is True
    assert startup_status == "启动MODE2可用"
    assert strict_ok is False
    assert "P_kPa>150" in strict_status


def test_read_sensor_parsed_accepts_temp_key_in_relaxed_runtime_mode(tmp_path: Path) -> None:
    runner = _runner_with_quality(tmp_path, {"enabled": False})
    ga = SimpleNamespace()
    parsed_frame = {
        "mode2_field_count": 17,
        "status": "OK",
        "co2_ppm": 401.0,
        "h2o_mmol": 1.1,
        "co2_ratio_f": 1.002,
        "h2o_ratio_f": 0.0,
        "chamber_temp_c": 32.5,
        "pressure_kpa": 101.3,
    }
    runner._read_runtime_sensor_line = lambda _ga: "YGAS,001,frame"
    runner._parse_sensor_line = lambda _ga, _line: dict(parsed_frame)

    _, parsed = runner._read_sensor_parsed(
        ga,
        required_key="chamber_temp_c",
        frame_acceptance_mode="required_key_relaxed",
    )
    strict_ok, strict_status = runner._assess_analyzer_frame(parsed_frame)
    runner.logger.close()

    assert parsed is not None
    assert parsed["chamber_temp_c"] == 32.5
    assert strict_ok is False
    assert "R_H2O<=0" in strict_status


def test_sensor_read_reject_logs_are_throttled_without_changing_rejection(tmp_path: Path) -> None:
    runner = _runner_with_quality(
        tmp_path,
        {"enabled": False},
        workflow_extra={
            "sensor_read_retry": {"retries": 0, "delay_s": 0.0},
            "analyzer_frame_quality": {"reject_log_window_s": 15.0},
        },
    )
    ga = SimpleNamespace(ser=SimpleNamespace(port="COM9"), device_id="001")
    events: list[tuple[object, object, object]] = []
    runner._read_runtime_sensor_line = lambda _ga: ""
    runner._parse_sensor_line = lambda _ga, _line: None
    runner._log_run_event = lambda command=None, response=None, error=None: events.append((command, response, error))

    _, first = runner._read_sensor_parsed(
        ga,
        required_key="chamber_temp_c",
        frame_acceptance_mode="required_key_relaxed",
    )
    _, second = runner._read_sensor_parsed(
        ga,
        required_key="chamber_temp_c",
        frame_acceptance_mode="required_key_relaxed",
    )
    runner.logger.close()

    assert first is None
    assert second is None
    assert [item[0] for item in events].count("sensor-read-reject") == 1


def test_read_sensor_parsed_relaxed_can_fallback_to_fresh_live_cache(tmp_path: Path) -> None:
    runner = _runner_with_quality(
        tmp_path,
        {"enabled": False},
        workflow_extra={
            "sensor_read_retry": {"retries": 0, "delay_s": 0.0},
            "analyzer_live_snapshot": {"enabled": True, "cache_ttl_s": 1.0},
        },
    )
    ga = SimpleNamespace(ser=SimpleNamespace(port="COM7"), device_id="007")
    cached_parsed = {
        "mode2_field_count": 17,
        "status": "OK",
        "co2_ppm": 401.0,
        "h2o_mmol": 1.1,
        "co2_ratio_f": 1.002,
        "h2o_ratio_f": 0.2,
        "chamber_temp_c": 31.8,
        "pressure_kpa": 101.2,
    }
    runner._live_analyzer_frame_cache[runner._analyzer_runtime_key(ga)] = {
        "line": "cached-line",
        "parsed": dict(cached_parsed),
        "timestamp": runner_module.time.time(),
        "category": "parsed",
    }
    runner._read_runtime_sensor_line = lambda _ga: ""
    runner._parse_sensor_line = lambda _ga, _line: None

    line, parsed = runner._read_sensor_parsed(
        ga,
        required_key="chamber_temp_c",
        frame_acceptance_mode="required_key_relaxed",
    )
    runner.logger.close()

    assert line == "cached-line"
    assert parsed is not None
    assert parsed["chamber_temp_c"] == 31.8


def test_strict_read_never_uses_live_cache_fallback(tmp_path: Path) -> None:
    runner = _runner_with_quality(
        tmp_path,
        {"enabled": False},
        workflow_extra={
            "sensor_read_retry": {"retries": 0, "delay_s": 0.0},
            "analyzer_live_snapshot": {"enabled": True, "cache_ttl_s": 1.0},
        },
    )
    ga = SimpleNamespace(ser=SimpleNamespace(port="COM8"), device_id="008")
    runner._live_analyzer_frame_cache[runner._analyzer_runtime_key(ga)] = {
        "line": "cached-line",
        "parsed": {
            "mode2_field_count": 17,
            "status": "OK",
            "co2_ppm": 401.0,
            "h2o_mmol": 1.1,
            "co2_ratio_f": 1.002,
            "h2o_ratio_f": 0.2,
            "chamber_temp_c": 31.8,
            "pressure_kpa": 101.2,
        },
        "timestamp": runner_module.time.time(),
        "category": "parsed",
    }
    runner._read_runtime_sensor_line = lambda _ga: ""
    runner._parse_sensor_line = lambda _ga, _line: None

    _, parsed = runner._read_sensor_parsed(ga, required_key="co2_ratio_f", frame_acceptance_mode="strict")
    runner.logger.close()

    assert parsed is None


def test_read_sensor_parsed_keeps_ratio_required_key_strict(tmp_path: Path) -> None:
    runner = _runner_with_quality(tmp_path, {"enabled": False})
    ga = SimpleNamespace()
    parsed_frame = {
        "mode2_field_count": 17,
        "status": "OK",
        "co2_ppm": 401.0,
        "h2o_mmol": 1.1,
        "co2_ratio_f": 0.0,
        "h2o_ratio_f": 0.2,
        "chamber_temp_c": 32.5,
        "pressure_kpa": 101.3,
    }
    runner._read_runtime_sensor_line = lambda _ga: "YGAS,001,frame"
    runner._parse_sensor_line = lambda _ga, _line: dict(parsed_frame)

    _, parsed = runner._read_sensor_parsed(ga, required_key="co2_ratio_f")
    runner.logger.close()

    assert parsed is None


def test_runtime_relaxed_status_soft_token_allows_required_key_but_hard_token_rejects(tmp_path: Path) -> None:
    runner = _runner_with_quality(
        tmp_path,
        {"enabled": False},
        workflow_extra={
            "analyzer_frame_quality": {
                "runtime_hard_bad_status_tokens": ["FAIL", "INVALID", "ERROR"],
                "runtime_soft_bad_status_tokens": ["NO_ACK"],
            }
        },
    )
    soft_ok, soft_status = runner._assess_runtime_required_key_frame(
        {
            "status": "NO_ACK",
            "chamber_temp_c": 32.5,
        },
        "chamber_temp_c",
    )
    hard_ok, hard_status = runner._assess_runtime_required_key_frame(
        {
            "status": "ERROR",
            "chamber_temp_c": 32.5,
        },
        "chamber_temp_c",
    )
    runner.logger.close()

    assert soft_ok is True
    assert "状态告警" in soft_status
    assert hard_ok is False
    assert "状态异常" in hard_status


def test_sampling_path_still_marks_invalid_frame_unusable_under_strict_rules(tmp_path: Path) -> None:
    runner = _runner_with_quality(tmp_path, {"enabled": False})
    ga = SimpleNamespace()
    parsed_frame = {
        "mode2_field_count": 17,
        "status": "OK",
        "co2_ppm": 401.0,
        "h2o_mmol": 1.1,
        "co2_ratio_f": 1.002,
        "h2o_ratio_f": -1001.0,
        "chamber_temp_c": 32.5,
        "pressure_kpa": 101.3,
    }
    runner._read_runtime_sensor_line = lambda _ga: "YGAS,001,frame"
    runner._parse_sensor_line = lambda _ga, _line: dict(parsed_frame)

    _, parsed = runner._read_sensor_parsed(ga, require_usable=False)
    usable, status = runner._assess_analyzer_frame(parsed)
    runner.logger.close()

    assert parsed is not None
    assert usable is False
    assert "sentinel" in status


def test_evaluate_sample_quality_supports_per_analyzer_mode(tmp_path: Path) -> None:
    runner = _runner_with_quality(
        tmp_path,
        {
            "enabled": True,
            "per_analyzer": True,
            "max_span_co2_ppm": 3.0,
        },
    )

    ok, spans = runner._evaluate_sample_quality(
        [
            {
                "co2_ppm": 100.0,
                "ga01_frame_usable": True,
                "ga01_co2_ppm": 100.0,
                "ga02_frame_usable": True,
                "ga02_co2_ppm": 100.0,
            },
            {
                "co2_ppm": 101.0,
                "ga01_frame_usable": True,
                "ga01_co2_ppm": 101.0,
                "ga02_frame_usable": True,
                "ga02_co2_ppm": 106.5,
            },
        ]
    )
    runner.logger.close()

    assert ok is False
    assert spans["co2_ppm"] <= 3.0
    assert spans["ga02.co2_ppm"] > 3.0


def test_ratio_poly_pressure_source_selection_prefers_reference_then_falls_back(tmp_path: Path) -> None:
    runner = _runner_with_quality(tmp_path, {"enabled": False})
    base_rows = [
        {"ppm_CO2_Tank": 400.0, "R_CO2": 1.0, "T1": 20.0, "P": 1000.0, "BAR": 101.0},
        {"ppm_CO2_Tank": 500.0, "R_CO2": 1.1, "T1": 21.0, "P": 1001.0, "BAR": 101.2},
    ]

    resolved = runner._resolve_ratio_poly_columns(
        base_rows,
        gas="co2",
        target_key="ppm_CO2_Tank",
        ratio_key="R_CO2",
        temp_keys=("T1",),
        pressure_keys=runner._ratio_poly_pressure_candidates("BAR", "reference_first"),
    )
    assert resolved["pressure_column"] == "P"

    resolved = runner._resolve_ratio_poly_columns(
        [{key: value for key, value in row.items() if key != "P"} for row in base_rows],
        gas="co2",
        target_key="ppm_CO2_Tank",
        ratio_key="R_CO2",
        temp_keys=("T1",),
        pressure_keys=runner._ratio_poly_pressure_candidates("BAR", "reference_first"),
    )
    assert resolved["pressure_column"] == "BAR"

    resolved = runner._resolve_ratio_poly_columns(
        [{key: value for key, value in row.items() if key != "BAR"} for row in base_rows],
        gas="co2",
        target_key="ppm_CO2_Tank",
        ratio_key="R_CO2",
        temp_keys=("T1",),
        pressure_keys=runner._ratio_poly_pressure_candidates("BAR", "reference_first"),
    )
    assert resolved["pressure_column"] == "P"

    resolved = runner._resolve_ratio_poly_columns(
        [
            {"ppm_CO2_Tank": 400.0, "R_CO2": 1.0, "T1": 20.0, "P": 1000.0, "BAR": 101.0},
            {"ppm_CO2_Tank": 500.0, "R_CO2": 1.1, "T1": 21.0, "P": None, "BAR": 101.2},
        ],
        gas="co2",
        target_key="ppm_CO2_Tank",
        ratio_key="R_CO2",
        temp_keys=("T1",),
        pressure_keys=runner._ratio_poly_pressure_candidates("BAR", "reference_first"),
    )
    assert resolved["pressure_column"] == "P"
    runner.logger.close()


def test_auto_fit_ratio_poly_uses_configurable_pressure_candidates(monkeypatch, tmp_path: Path) -> None:
    logs: list[str] = []
    runner = _runner_with_quality(
        tmp_path,
        {"enabled": False},
        coefficients_cfg={
            "summary_columns": {
                "co2": {
                    "target": "ppm_CO2_Tank",
                    "ratio": "R_CO2",
                    "temperature": "T1",
                    "pressure": "BAR",
                    "pressure_scale": 1.0,
                }
            },
            "ratio_poly_fit": {
                "pressure_source_preference": "reference_first",
            },
        },
        logs=logs,
    )
    rows = [
        {
            "Analyzer": "GA01",
            "PointPhase": "气路",
            "ppm_CO2_Tank": 400.0,
            "R_CO2": 1.0,
            "T1": 20.0,
            "P": 1000.0,
            "BAR": 101.0,
        },
        {
            "Analyzer": "GA01",
            "PointPhase": "气路",
            "ppm_CO2_Tank": 500.0,
            "R_CO2": 1.1,
            "T1": 21.0,
            "P": 1001.0,
            "BAR": 101.1,
        },
    ]
    captured: dict[str, object] = {}

    def fake_fit(rows, **kwargs):
        captured["pressure_keys"] = kwargs["pressure_keys"]
        return SimpleNamespace(
            model="ratio_poly_rt_p",
            n=len(rows),
            stats={"rmse_simplified": 0.1, "max_abs_simplified": 0.2},
        )

    def fake_save(*args, **kwargs):
        return {"json": tmp_path / "dummy.json"}

    monkeypatch.setattr(runner, "_load_analyzer_summary_rows", lambda: rows)
    monkeypatch.setattr(runner_module, "fit_ratio_poly_rt_p", fake_fit)
    monkeypatch.setattr(runner_module, "save_ratio_poly_report", fake_save)

    runner._auto_fit_ratio_poly_from_summary(runner.cfg["coefficients"], gas="co2", model="ratio_poly_rt_p")
    runner.logger.close()

    assert captured["pressure_keys"] == ("P", "BAR")
    assert any("selected_pressure_key=P" in message for message in logs)


def test_auto_fit_ratio_poly_reads_nested_pressure_source_preference(monkeypatch, tmp_path: Path) -> None:
    logs: list[str] = []
    runner = _runner_with_quality(
        tmp_path,
        {"enabled": False},
        coefficients_cfg={
            "pressure_source_preference": "analyzer_only",
            "summary_columns": {
                "co2": {
                    "target": "ppm_CO2_Tank",
                    "ratio": "R_CO2",
                    "temperature": "T1",
                    "pressure": "BAR",
                    "pressure_scale": 1.0,
                }
            },
            "ratio_poly_fit": {
                "pressure_source_preference": "reference_first",
            },
        },
        logs=logs,
    )
    rows = [
        {
            "Analyzer": "GA01",
            "PointPhase": "气路",
            "ppm_CO2_Tank": 400.0,
            "R_CO2": 1.0,
            "T1": 20.0,
            "P": 1000.0,
            "BAR": 101.0,
        },
        {
            "Analyzer": "GA01",
            "PointPhase": "气路",
            "ppm_CO2_Tank": 500.0,
            "R_CO2": 1.1,
            "T1": 21.0,
            "P": 1001.0,
            "BAR": 101.1,
        },
    ]
    captured: dict[str, object] = {}

    def fake_fit(rows, **kwargs):
        captured["pressure_keys"] = kwargs["pressure_keys"]
        return SimpleNamespace(
            model="ratio_poly_rt_p",
            n=len(rows),
            stats={"rmse_simplified": 0.1, "max_abs_simplified": 0.2},
        )

    def fake_save(*args, **kwargs):
        return {"json": tmp_path / "dummy.json"}

    monkeypatch.setattr(runner, "_load_analyzer_summary_rows", lambda: rows)
    monkeypatch.setattr(runner_module, "fit_ratio_poly_rt_p", fake_fit)
    monkeypatch.setattr(runner_module, "save_ratio_poly_report", fake_save)

    runner._auto_fit_ratio_poly_from_summary(runner.cfg["coefficients"], gas="co2", model="ratio_poly_rt_p")
    runner.logger.close()

    assert captured["pressure_keys"] == ("P", "BAR")
    assert any("pressure_source_preference=reference_first" in message for message in logs)


def test_auto_fit_ratio_poly_ambient_only_uses_seven_feature_model(monkeypatch, tmp_path: Path) -> None:
    runner = _runner_with_quality(
        tmp_path,
        {"enabled": False},
        workflow_extra={"selected_pressure_points": ["ambient"]},
        coefficients_cfg={},
    )
    rows = [
        {"Analyzer": "GA01", "PointPhase": "气路", "ppm_CO2_Tank": 400.0, "R_CO2": 1.0, "T1": 20.0, "BAR": 101.0},
        {"Analyzer": "GA01", "PointPhase": "气路", "ppm_CO2_Tank": 500.0, "R_CO2": 1.1, "T1": 21.0, "BAR": 101.1},
    ]
    captured: dict[str, object] = {}

    def fake_fit(rows, **kwargs):
        captured["model_features"] = kwargs.get("model_features")
        return SimpleNamespace(
            model="ratio_poly_rt_p",
            n=len(rows),
            stats={"rmse_simplified": 0.1, "max_abs_simplified": 0.2},
        )

    monkeypatch.setattr(runner, "_load_analyzer_summary_rows", lambda: rows)
    monkeypatch.setattr(runner_module, "fit_ratio_poly_rt_p", fake_fit)
    monkeypatch.setattr(runner_module, "save_ratio_poly_report", lambda *args, **kwargs: {"json": tmp_path / "dummy.json"})

    runner._auto_fit_ratio_poly_from_summary(runner.cfg["coefficients"], gas="co2", model="ratio_poly_rt_p")
    runner.logger.close()

    assert captured["model_features"] == AMBIENT_ONLY_MODEL_FEATURES


def test_auto_fit_ratio_poly_explicit_model_features_override_ambient_only(monkeypatch, tmp_path: Path) -> None:
    explicit_features = ["intercept", "R", "T"]
    runner = _runner_with_quality(
        tmp_path,
        {"enabled": False},
        workflow_extra={"selected_pressure_points": ["ambient"]},
        coefficients_cfg={"model_features": explicit_features},
    )
    rows = [
        {"Analyzer": "GA01", "PointPhase": "气路", "ppm_CO2_Tank": 400.0, "R_CO2": 1.0, "T1": 20.0, "BAR": 101.0},
        {"Analyzer": "GA01", "PointPhase": "气路", "ppm_CO2_Tank": 500.0, "R_CO2": 1.1, "T1": 21.0, "BAR": 101.1},
    ]
    captured: dict[str, object] = {}

    def fake_fit(rows, **kwargs):
        captured["model_features"] = kwargs.get("model_features")
        return SimpleNamespace(
            model="ratio_poly_rt_p",
            n=len(rows),
            stats={"rmse_simplified": 0.1, "max_abs_simplified": 0.2},
        )

    monkeypatch.setattr(runner, "_load_analyzer_summary_rows", lambda: rows)
    monkeypatch.setattr(runner_module, "fit_ratio_poly_rt_p", fake_fit)
    monkeypatch.setattr(runner_module, "save_ratio_poly_report", lambda *args, **kwargs: {"json": tmp_path / "dummy.json"})

    runner._auto_fit_ratio_poly_from_summary(runner.cfg["coefficients"], gas="co2", model="ratio_poly_rt_p")
    runner.logger.close()

    assert captured["model_features"] == explicit_features


def test_auto_fit_ratio_poly_multi_pressure_keeps_full_model_default(monkeypatch, tmp_path: Path) -> None:
    runner = _runner_with_quality(
        tmp_path,
        {"enabled": False},
        workflow_extra={"selected_pressure_points": ["ambient", 1100, 1000]},
        coefficients_cfg={},
    )
    rows = [
        {"Analyzer": "GA01", "PointPhase": "气路", "ppm_CO2_Tank": 400.0, "R_CO2": 1.0, "T1": 20.0, "BAR": 101.0, "P": 1000.0},
        {"Analyzer": "GA01", "PointPhase": "气路", "ppm_CO2_Tank": 500.0, "R_CO2": 1.1, "T1": 21.0, "BAR": 101.1, "P": 1100.0},
    ]
    captured: dict[str, object] = {}

    def fake_fit(rows, **kwargs):
        captured["model_features"] = kwargs.get("model_features")
        return SimpleNamespace(
            model="ratio_poly_rt_p",
            n=len(rows),
            stats={"rmse_simplified": 0.1, "max_abs_simplified": 0.2},
        )

    monkeypatch.setattr(runner, "_load_analyzer_summary_rows", lambda: rows)
    monkeypatch.setattr(runner_module, "fit_ratio_poly_rt_p", fake_fit)
    monkeypatch.setattr(runner_module, "save_ratio_poly_report", lambda *args, **kwargs: {"json": tmp_path / "dummy.json"})

    runner._auto_fit_ratio_poly_from_summary(runner.cfg["coefficients"], gas="co2", model="ratio_poly_rt_p")
    runner.logger.close()

    assert "model_features" in captured
    assert captured["model_features"] is None


def test_h2o_ratio_poly_summary_selection_defaults_to_zero_ppm_only_at_minus20_minus10_and_zero(tmp_path: Path) -> None:
    runner = _runner_with_quality(tmp_path, {"enabled": False})
    rows = [
        {"RowId": "h2o", "PointPhase": "h2o", "Temp": 10.0},
        {"RowId": "minus20_zero", "PointPhase": "co2", "TempSet": -20.0, "ppm_CO2_Tank": 0.0},
        {"RowId": "minus10_zero", "PointPhase": "co2", "TempSet": -10.0, "ppm_CO2_Tank": 0.0},
        {"RowId": "zero_zero", "PointPhase": "co2", "TempSet": 0.0, "ppm_CO2_Tank": 0.0},
        {"RowId": "zero_400", "PointPhase": "co2", "TempSet": 0.0, "ppm_CO2_Tank": 400.0},
        {"RowId": "ten_zero", "PointPhase": "co2", "TempSet": 10.0, "ppm_CO2_Tank": 0.0},
    ]

    filtered = runner._filter_ratio_poly_summary_rows(
        rows,
        gas="h2o",
        cfg={"h2o_summary_selection": {}},
    )
    runner.logger.close()

    assert [row["RowId"] for row in filtered] == ["h2o", "minus20_zero", "minus10_zero", "zero_zero"]
