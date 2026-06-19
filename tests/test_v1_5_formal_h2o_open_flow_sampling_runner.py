import csv
import json

from gas_calibrator.tools import run_v1_5_formal_h2o_open_flow_sampling as h2o_tool
from gas_calibrator.tools.run_v1_5_formal_h2o_open_flow_sampling import (
    _build_h2o_open_flow_point,
    _build_humidity_reference_check,
    _close_pace_vent_after_valve_if_opened,
    _collect_h2o_pressure_stability_diagnostic,
    _enter_h2o_pressure_diagnostic_atmosphere,
    _parse_args,
    _prepare_runtime_cfg,
    _read_dewpoint_snapshot_for_evidence,
    _set_h2o_open_flow_hgen_flow,
    _validation_report_prefix,
    _wait_h2o_analyzer_pressure_presample_gate,
    _write_gate_failure,
    _write_humidity_reference_review,
)


def test_h2o_open_flow_point_is_water_point():
    point = _build_h2o_open_flow_point(
        temp_c=20.0,
        hgen_temp_c=20.0,
        hgen_rh_pct=70.0,
        certificate_dewpoint_c=14.36,
        certificate_h2o_mmol=16.3715,
    )

    assert point.is_h2o_point is True
    assert point.co2_ppm is None
    assert point.hgen_temp_c == 20.0
    assert point.hgen_rh_pct == 70.0
    assert point.dewpoint_c == 14.36
    assert point.h2o_mmol == 16.3715
    assert point.target_pressure_hpa is None


def test_formal_h2o_open_flow_default_purge_is_720s():
    args = _parse_args(["--config", "config.json", "--hgen-temp", "20", "--hgen-rh", "70"])

    assert args.purge_s == 720.0
    assert args.minimum_purge_s == 720.0
    assert args.analyzer_acquisition == "active_stream_1hz"
    assert args.allow_ftd_write is True
    assert args.open_flow_pressure_transient_grace_s == 30.0
    assert args.open_flow_pressure_safety_hard_limit_hpa == 1300.0


def test_h2o_validation_report_prefix_compacts_deep_windows_paths(monkeypatch, tmp_path):
    monkeypatch.setattr(h2o_tool.os, "name", "nt")
    deep = tmp_path / ("very_long_evidence_path_" * 12)

    assert _validation_report_prefix(deep) == "h2o_validation"
    assert _validation_report_prefix(tmp_path) == "formal_h2o_open_flow_sampling_validation"


def test_prepare_runtime_cfg_blocks_writes_and_uses_1hz_active_stream_for_h2o():
    cfg = {
        "devices": {
            "humidity_generator": {"enabled": False},
            "gas_analyzer": {"active_send": True},
            "gas_analyzers": [
                {"name": "ga01", "active_send": True},
                {"name": "ga02", "active_send": True},
            ],
        },
        "workflow": {
            "pressure": {"continuous_atmosphere_hold": True},
            "stability": {"temperature": {"analyzer_chamber_temp_span_c": 0.02}},
            "analyzer_mode2_init": {
                "read_first_before_config": False,
                "write_config_on_read_first_fail": True,
                "send_active_freq": True,
            },
            "postrun_corrected_delivery": {"enabled": True, "write_devices": True},
            "startup_pressure_sensor_calibration": {"enabled": True, "apply_write": True},
        },
        "paths": {"output_dir": "logs/example"},
        "metadata": {"writes_senco": True, "writes_device_id": True},
    }

    out = _prepare_runtime_cfg(
        cfg,
        output_dir=None,
        sample_count=10,
        sample_interval_s=1.0,
        sensor_read_interval_s=5.0,
    )

    assert out["workflow"]["route_mode"] == "h2o_open_flow_sidecar"
    assert out["workflow"]["skip_h2o"] is False
    assert out["workflow"]["pressure"]["continuous_atmosphere_hold"] is False
    assert out["workflow"]["analyzer_mode2_init"]["read_first_before_config"] is True
    assert out["workflow"]["analyzer_mode2_init"]["write_config_on_read_first_fail"] is True
    assert out["workflow"]["analyzer_mode2_init"]["send_active_freq"] is True
    assert out["workflow"]["analyzer_mode2_init"]["skip_config_when_read_first_ready"] is True
    assert out["workflow"]["stability"]["temperature"]["analyzer_chamber_temp_span_c"] == 0.08
    assert out["workflow"]["postrun_corrected_delivery"]["enabled"] is False
    assert out["workflow"]["postrun_corrected_delivery"]["write_devices"] is False
    assert out["workflow"]["startup_pressure_sensor_calibration"]["apply_write"] is False
    assert out["metadata"]["writes_senco"] is False
    assert out["metadata"]["writes_device_id"] is False
    assert out["metadata"]["analyzer_acquisition_policy"] == "active_mode2_stream_1hz_ftd01_controlled"
    assert out["metadata"]["analyzer_stream_target_hz"] == 1.0
    assert out["metadata"]["analyzer_stream_native_hz"] == 1.0
    assert out["metadata"]["analyzer_stream_frequency_control"] == "FTD01_written"
    assert out["metadata"]["formal_sample_decimation"] == (
        "nearest_usable_mode2_frame_at_1hz_anchor_from_1hz_stream"
    )
    assert out["metadata"]["ftd_write_enabled"] is True
    assert out["metadata"]["idle_continuous_atmosphere_hold"] is False
    assert out["metadata"]["startup_mode2_missing_policy"] == "mode2_stream_config_then_sampling_qc"
    assert out["metadata"]["h2o_open_flow_hgen_flow_control"] == "not_controlled_by_default"
    assert out["metadata"]["h2o_open_flow_hgen_flow_lpm"] is None
    assert out["metadata"]["h2o_hgen_shutdown_policy"] == "safe_stop_after_point"
    contract = out["metadata"]["h2o_open_flow_sampling_physical_contract"]
    assert contract["sample_window_requires_route_open"] is True
    assert contract["sample_window_requires_humidity_reference_flow"] is True
    assert contract["route_close_allowed_only_after_sample_window"] is True
    assert contract["dewpoint_reference_gate_required"] is True
    assert contract["per_analyzer_h2o_ratio_stability_required"] is True
    assert contract["per_analyzer_status_register_qc_required"] is True
    assert contract["unstable_analyzer_handling"] == (
        "prefer_all_stable_with_bounded_grace_then_independent_grade_or_reject"
    )
    assert contract["pressure_role"] == "diagnostic_or_qc_input_not_h2o_fit_hard_blocker"
    assert out["metadata"]["humidity_reference_role"] == "dewpoint_meter_primary_hgen_state_review"
    assert out["metadata"]["h2o_open_flow_wait_contract"] == (
        "v1_5_dewpoint_tail_h2o_ratio_with_pressure_diagnostic_only"
    )
    assert out["metadata"]["h2o_pressure_kpa_presample_policy"] == "skip"
    assert out["workflow"]["stability"]["sensor"]["h2o_pressure_kpa_presample_policy"] == "skip"
    assert out["workflow"]["stability"]["sensor"]["h2o_pressure_kpa_presample_required_for_a_grade"] is False
    assert out["devices"]["gas_analyzer"]["active_send"] is True
    assert out["devices"]["gas_analyzer"]["ftd_hz"] == 1
    assert [item["active_send"] for item in out["devices"]["gas_analyzers"]] == [True, True]
    assert out["devices"]["humidity_generator"]["enabled"] is True
    assert out["workflow"]["stability"]["dewpoint"]["timeout_s"] == 1800.0
    assert out["workflow"]["stability"]["water_route_dewpoint_gate_max_total_wait_s"] == 1800.0
    assert out["workflow"]["stability"]["sensor"]["h2o_ratio_f_preseal_timeout_s"] == 300.0
    assert out["workflow"]["stability"]["sensor"]["h2o_ratio_f_preseal_window_s"] == 60.0
    assert out["workflow"]["stability"]["sensor"]["h2o_ratio_f_preseal_tol"] == 0.001
    assert out["workflow"]["stability"]["sensor"]["h2o_pressure_kpa_presample_enabled"] is False
    assert out["workflow"]["stability"]["sensor"]["h2o_pressure_kpa_presample_tol"] == 0.2
    assert out["workflow"]["stability"]["sensor"]["h2o_pressure_kpa_presample_window_s"] == 60.0
    assert out["workflow"]["stability"]["sensor"]["h2o_pressure_kpa_presample_timeout_s"] == 300.0
    assert out["workflow"]["stability"]["sensor"]["h2o_pressure_kpa_presample_min_samples"] == 10
    assert out["workflow"]["stability"]["analyzer_gate_min_valid_analyzers"] == 1
    assert out["workflow"]["stability"]["analyzer_gate_optional_labels"] == ["ga01", "ga02"]
    assert out["workflow"]["stability"]["analyzer_gate_required_labels"] == []
    assert out["workflow"]["stability"]["analyzer_gate_allow_pass_with_dropped_optional"] is True
    assert out["workflow"]["stability"]["analyzer_gate_prefer_all_stable_grace_s"] == 120.0


def test_h2o_runtime_cfg_caps_route_dewpoint_wait_to_half_hour():
    out = _prepare_runtime_cfg(
        {
            "devices": {"gas_analyzers": [{"name": "ga01"}], "humidity_generator": {"enabled": True}},
            "workflow": {"stability": {"water_route_dewpoint_gate_max_total_wait_s": 3600.0}},
            "paths": {"output_dir": "logs/example"},
        },
        output_dir=None,
        sample_count=10,
        sample_interval_s=1.0,
        sensor_read_interval_s=1.0,
    )

    assert out["workflow"]["stability"]["water_route_dewpoint_gate_max_total_wait_s"] == 1800.0


def test_h2o_keep_hgen_running_flag_records_queue_managed_shutdown_policy():
    args = _parse_args(
        [
            "--config",
            "config.json",
            "--hgen-temp",
            "20",
            "--hgen-rh",
            "70",
            "--keep-hgen-running-after-point",
        ]
    )
    out = _prepare_runtime_cfg(
        {
            "devices": {"humidity_generator": {"enabled": False}},
            "workflow": {},
            "paths": {"output_dir": "logs/example"},
        },
        output_dir=None,
        sample_count=10,
        sample_interval_s=1.0,
        sensor_read_interval_s=5.0,
        keep_hgen_running_after_point=args.keep_hgen_running_after_point,
    )

    assert args.keep_hgen_running_after_point is True
    assert out["metadata"]["h2o_hgen_shutdown_policy"] == "queue_managed_keep_running_between_points"
    assert out["workflow"]["stability"]["analyzer_gate_disable_dropped_optional"] is False
    assert out["workflow"]["stability"]["analyzer_gate_zero_value_policy"] == "drop_optional_not_block"
    live_cfg = out["workflow"]["analyzer_live_snapshot"]
    assert live_cfg["passive_round_robin_enabled"] is False
    assert live_cfg["sampling_worker_interval_s"] == 0.2
    summary_filter = out["workflow"]["sampling"]["summary_outlier_filter"]
    assert summary_filter["enabled"] is True
    assert summary_filter["scope"] == "per_analyzer_sample_window_summary_only"
    assert summary_filter["raw_frame_retention"] == "all_raw_frames_kept"
    assert summary_filter["keys"] == ["co2_ratio_f", "h2o_ratio_f"]
    assert summary_filter["absolute_thresholds"]["co2_ratio_f"] == 0.001
    assert summary_filter["absolute_thresholds"]["h2o_ratio_f"] == 0.001
    assert summary_filter["max_outliers_per_key"] == 1


def test_h2o_analyzer_pressure_gate_default_skip_does_not_wait_for_internal_p():
    point = _build_h2o_open_flow_point(
        temp_c=20.0,
        hgen_temp_c=20.0,
        hgen_rh_pct=70.0,
        certificate_dewpoint_c=14.36,
        certificate_h2o_mmol=16.3715,
    )

    class Runner:
        def __init__(self):
            self.cfg = {"workflow": {"stability": {"sensor": {}}}}
            self.runtime_fields = {}
            self.logs = []

        def _wait_primary_sensor_stable(self, point, **kwargs):
            raise AssertionError("default H2O open-flow pressure policy must not wait for pressure")

        def _set_point_runtime_fields(self, point, *, phase, **fields):
            self.runtime_fields.update(fields)

        def log(self, message):
            self.logs.append(message)

    runner = Runner()
    assert _wait_h2o_analyzer_pressure_presample_gate(runner, point) is True
    assert runner.runtime_fields["h2o_pressure_presample_gate_status"] == "skipped"
    assert runner.runtime_fields["h2o_pressure_presample_gate_policy"] == "skip"
    assert runner.runtime_fields["h2o_pressure_presample_gate_reason"] == "skipped_by_policy"
    assert any("skipped by policy" in message for message in runner.logs)


def test_h2o_analyzer_pressure_gate_warn_policy_waits_for_internal_p_without_pressure_control():
    point = _build_h2o_open_flow_point(
        temp_c=20.0,
        hgen_temp_c=20.0,
        hgen_rh_pct=70.0,
        certificate_dewpoint_c=14.36,
        certificate_h2o_mmol=16.3715,
    )

    class Runner:
        def __init__(self):
            self.cfg = {
                "workflow": {
                    "stability": {
                        "sensor": {
                            "h2o_pressure_kpa_presample_policy": "warn",
                            "h2o_pressure_kpa_presample_tol": 0.15,
                            "h2o_pressure_kpa_presample_window_s": 90.0,
                            "h2o_pressure_kpa_presample_timeout_s": 360.0,
                            "h2o_pressure_kpa_presample_min_samples": 12,
                            "h2o_pressure_kpa_presample_read_interval_s": 2.0,
                        }
                    }
                }
            }
            self.calls = []
            self.logs = []

        def _wait_primary_sensor_stable(self, point, **kwargs):
            self.calls.append(kwargs)
            return True

        def _append_pressure_trace_row(self, **kwargs):
            self.calls.append({"trace_stage": kwargs.get("trace_stage"), "note": kwargs.get("note")})

        def log(self, message):
            self.logs.append(message)

    runner = Runner()
    assert _wait_h2o_analyzer_pressure_presample_gate(runner, point) is True

    wait_call = next(call for call in runner.calls if call.get("value_key") == "pressure_kpa")
    assert wait_call["require_pressure_in_limits"] is False
    assert wait_call["tol_override"] == 0.15
    assert wait_call["window_override"] == 90.0
    assert wait_call["timeout_override"] == 360.0
    assert wait_call["min_samples_override"] == 12
    assert wait_call["read_interval_override"] == 2.0
    assert any(call.get("trace_stage") == "h2o_presample_analyzer_pressure_gate_begin" for call in runner.calls)
    assert any("pressure pre-sample gate passed" in message for message in runner.logs)


def test_h2o_analyzer_pressure_gate_warn_policy_continues_with_diagnostic_pressure_evidence():
    point = _build_h2o_open_flow_point(
        temp_c=20.0,
        hgen_temp_c=20.0,
        hgen_rh_pct=70.0,
        certificate_dewpoint_c=14.36,
        certificate_h2o_mmol=16.3715,
    )

    class Runner:
        def __init__(self):
            self.cfg = {
                "workflow": {
                    "stability": {
                        "sensor": {
                            "h2o_pressure_kpa_presample_policy": "warn",
                            "h2o_pressure_kpa_presample_tol": 0.2,
                            "h2o_pressure_kpa_presample_window_s": 60.0,
                            "h2o_pressure_kpa_presample_timeout_s": 10.0,
                            "h2o_pressure_kpa_presample_min_samples": 4,
                        }
                    }
                }
            }
            self.runtime_fields = {}
            self.logs = []

        def _wait_primary_sensor_stable(self, point, **kwargs):
            return False

        def _append_pressure_trace_row(self, **kwargs):
            return None

        def _set_point_runtime_fields(self, point, *, phase, **fields):
            self.runtime_fields.update(fields)

        def log(self, message):
            self.logs.append(message)

    runner = Runner()

    assert _wait_h2o_analyzer_pressure_presample_gate(runner, point) is True
    assert runner.runtime_fields["h2o_pressure_presample_gate_status"] == "warn"
    assert (
        runner.runtime_fields["h2o_pressure_presample_fit_scope"]
        == "diagnostic_not_fit_gate"
    )
    assert (
        runner.runtime_fields["h2o_pressure_presample_grade_scope"]
        == "not_required_for_a_grade_by_default"
    )
    assert "review_h2o_ratio_dewpoint_and_flow_evidence" in runner.runtime_fields[
        "h2o_pressure_presample_report_warning"
    ]
    assert "sample_can_enter_calibration_fit" not in runner.runtime_fields
    assert "point_quality_status" not in runner.runtime_fields
    assert any("continuing with diagnostic pressure evidence" in message for message in runner.logs)


def test_h2o_analyzer_pressure_gate_warn_policy_restores_pressure_dropped_analyzers():
    point = _build_h2o_open_flow_point(
        temp_c=20.0,
        hgen_temp_c=20.0,
        hgen_rh_pct=70.0,
        certificate_dewpoint_c=14.36,
        certificate_h2o_mmol=16.3715,
    )

    class Runner:
        def __init__(self):
            self.cfg = {
                "workflow": {
                    "stability": {
                        "sensor": {
                            "h2o_pressure_kpa_presample_policy": "warn",
                            "h2o_pressure_kpa_presample_tol": 0.2,
                            "h2o_pressure_kpa_presample_window_s": 60.0,
                            "h2o_pressure_kpa_presample_timeout_s": 10.0,
                            "h2o_pressure_kpa_presample_min_samples": 4,
                        }
                    }
                }
            }
            self._disabled_analyzers = {"ga01"}
            self._disabled_analyzer_reasons = {"ga01": "startup_mode2_verify_failed"}
            self._disabled_analyzer_last_reprobe_ts = {"ga01": 123.0}
            self.runtime_fields = {}
            self.logs = []

        def _wait_primary_sensor_stable(self, point, **kwargs):
            self._disabled_analyzers.add("ga06")
            self._disabled_analyzer_reasons["ga06"] = "pressure_kpa_timeout"
            self._disabled_analyzer_last_reprobe_ts["ga06"] = 456.0
            return True

        def _append_pressure_trace_row(self, **kwargs):
            return None

        def _set_point_runtime_fields(self, point, *, phase, **fields):
            self.runtime_fields.update(fields)

        def log(self, message):
            self.logs.append(message)

    runner = Runner()

    assert _wait_h2o_analyzer_pressure_presample_gate(runner, point) is True
    assert runner._disabled_analyzers == {"ga01"}
    assert runner._disabled_analyzer_reasons == {"ga01": "startup_mode2_verify_failed"}
    assert runner._disabled_analyzer_last_reprobe_ts == {"ga01": 123.0}
    assert runner.runtime_fields["h2o_pressure_presample_gate_status"] == "warn"
    assert runner.runtime_fields["h2o_pressure_presample_restored_analyzers"] == "ga06"
    assert any("restored analyzers under warn policy" in message for message in runner.logs)


def test_h2o_analyzer_pressure_gate_fail_policy_blocks_sampling():
    point = _build_h2o_open_flow_point(
        temp_c=20.0,
        hgen_temp_c=20.0,
        hgen_rh_pct=70.0,
        certificate_dewpoint_c=14.36,
        certificate_h2o_mmol=16.3715,
    )

    class Runner:
        def __init__(self):
            self.cfg = {
                "workflow": {"stability": {"sensor": {"h2o_pressure_kpa_presample_policy": "fail"}}}
            }
            self.runtime_fields = {}
            self.logs = []

        def _wait_primary_sensor_stable(self, point, **kwargs):
            return False

        def _append_pressure_trace_row(self, **kwargs):
            return None

        def _set_point_runtime_fields(self, point, *, phase, **fields):
            self.runtime_fields.update(fields)

        def log(self, message):
            self.logs.append(message)

    runner = Runner()

    assert _wait_h2o_analyzer_pressure_presample_gate(runner, point) is False
    assert runner.runtime_fields["h2o_pressure_presample_gate_status"] == "fail"
    assert runner.runtime_fields["point_quality_blocked"] is True


def test_h2o_pressure_diagnostic_after_purge_flag_requires_diagnostic_only_intent():
    args = _parse_args(
        [
            "--config",
            "site.json",
            "--hgen-temp",
            "28",
            "--hgen-rh",
            "70",
            "--pressure-diagnostic-only",
            "--pressure-diagnostic-after-purge",
            "--no-prompt",
        ]
    )

    assert args.pressure_diagnostic_only is True
    assert args.pressure_diagnostic_after_purge is True
    assert args.pressure_diagnostic_reference_interval_s == 5.0
    assert args.pressure_diagnostic_analyzer_drain_s == 0.18


def test_h2o_pressure_diagnostic_observe_hgen_only_does_not_command_hgen(tmp_path, monkeypatch):
    clock = [0.0]
    monkeypatch.setattr(h2o_tool.time, "time", lambda: clock[0])
    monkeypatch.setattr(h2o_tool.time, "sleep", lambda seconds: clock.__setitem__(0, clock[0] + float(seconds)))
    monkeypatch.setattr(
        h2o_tool,
        "load_config",
        lambda path: {"paths": {"output_dir": str(tmp_path)}, "devices": {}, "workflow": {}},
    )
    monkeypatch.setattr(h2o_tool, "_defer_startup_mode2_disabled_analyzers", lambda runner: None)
    monkeypatch.setattr(
        h2o_tool,
        "_enter_h2o_pressure_diagnostic_atmosphere",
        lambda pace, **kwargs: False,
    )
    monkeypatch.setattr(h2o_tool, "_stop_continuous_atmosphere", lambda pace: None)
    monkeypatch.setattr(
        h2o_tool,
        "_write_purge_trace",
        lambda path, **kwargs: path.write_text("timestamp\n", encoding="utf-8"),
    )

    class Hgen:
        def __init__(self):
            self.commands = []

        def fetch_all(self):
            return {"data": {"Tc": 20.1, "Uw": 69.8, "Fl": 0.0}}

        def set_target_temp(self, value):
            raise AssertionError("observe-only diagnostic must not set humidity-generator temperature")

        def set_target_rh(self, value):
            raise AssertionError("observe-only diagnostic must not set humidity-generator RH")

        def enable_control(self, value):
            raise AssertionError("observe-only diagnostic must not enable humidity-generator control")

        def set_flow_target(self, value):
            raise AssertionError("observe-only diagnostic must not set humidity-generator flow")

        def safe_stop(self):
            raise AssertionError("observe-only diagnostic must not safe-stop humidity generator")

    hgen = Hgen()

    class PressureDevice:
        def __init__(self, value):
            self.value = value

        def read_pressure(self):
            return self.value

    class Analyzer:
        def __init__(self):
            self.ser = type("Serial", (), {"port": "COM35"})()

        def _drain_stream_lines(self, drain_s=0.18, read_timeout_s=0.02):
            return ["104.10"]

        def parse_line_mode2(self, line):
            return {"id": "023", "pressure_kpa": float(line), "h2o_ratio_f": 0.6405}

    class Runner:
        def __init__(self, cfg, devices, logger, log_fn, prompt_fn):
            self.cfg = cfg
            self.devices = devices
            self.logger = logger
            self.ga = Analyzer()
            self.valve_calls = []

        def _configure_devices(self):
            return None

        def _startup_preflight_reset(self):
            return None

        def _h2o_open_valves(self, point):
            return [8, 9, 10]

        def _apply_valve_states(self, valves):
            self.valve_calls.append(list(valves))

        def _all_gas_analyzers(self):
            return [("ga01", self.ga, {})]

        @staticmethod
        def _as_float(value):
            return None if value is None else float(value)

        def _read_sensor_parsed(self, ga, **kwargs):
            raise AssertionError("active stream should be used for pressure diagnostic")

    monkeypatch.setattr(h2o_tool, "CalibrationRunner", Runner)
    monkeypatch.setattr(
        h2o_tool,
        "_build_devices",
        lambda cfg, io_logger=None: {
            "humidity_gen": hgen,
            "pace": PressureDevice(1014.0),
            "pressure_gauge": PressureDevice(1021.6),
        },
    )
    monkeypatch.setattr(h2o_tool, "_close_devices", lambda devices: None)

    rc = h2o_tool.main(
        [
            "--config",
            "site.json",
            "--run-id",
            "observe_hgen",
            "--hgen-temp",
            "20",
            "--hgen-rh",
            "70",
            "--purge-s",
            "0",
            "--pressure-diagnostic-only",
            "--pressure-diagnostic-after-purge",
            "--pressure-diagnostic-observe-hgen-only",
            "--pressure-diagnostic-s",
            "1",
            "--pressure-diagnostic-interval-s",
            "1",
            "--pressure-diagnostic-window-s",
            "1",
            "--pressure-diagnostic-min-samples",
            "1",
            "--no-prompt",
        ]
    )

    assert rc == 0
    run_dir = tmp_path / "observe_hgen"
    observe = json.loads((run_dir / "h2o_pressure_diagnostic_hgen_observe_only.json").read_text(encoding="utf-8"))
    metadata = json.loads((run_dir / "formal_h2o_open_flow_sidecar_metadata.json").read_text(encoding="utf-8"))
    flow = json.loads((run_dir / "formal_h2o_open_flow_hgen_flow_set.json").read_text(encoding="utf-8"))
    assert observe["control_role"] == "observe_only_no_prepare_no_flow_no_safe_stop"
    assert observe["no_write_assertion"]["sends_hgen_safe_stop"] is False
    assert metadata["pressure_diagnostic_observe_hgen_only"] is True
    assert metadata["h2o_pressure_diagnostic_hgen_control_role"] == "observe_only_no_prepare_no_flow_no_safe_stop"
    assert flow["flow_control_role"] == "observe_only_no_hgen_flow_command"


def test_h2o_pressure_route_closed_baseline_keeps_valves_closed_and_does_not_command_hgen(tmp_path, monkeypatch):
    clock = [0.0]
    monkeypatch.setattr(h2o_tool.time, "time", lambda: clock[0])
    monkeypatch.setattr(h2o_tool.time, "sleep", lambda seconds: clock.__setitem__(0, clock[0] + float(seconds)))
    monkeypatch.setattr(
        h2o_tool,
        "load_config",
        lambda path: {"paths": {"output_dir": str(tmp_path)}, "devices": {}, "workflow": {}},
    )
    monkeypatch.setattr(h2o_tool, "_defer_startup_mode2_disabled_analyzers", lambda runner: None)
    monkeypatch.setattr(h2o_tool, "_stop_continuous_atmosphere", lambda pace: None)

    atmosphere_calls = []
    monkeypatch.setattr(
        h2o_tool,
        "_enter_h2o_pressure_diagnostic_atmosphere",
        lambda pace, **kwargs: atmosphere_calls.append(kwargs) or False,
    )

    class Hgen:
        def fetch_all(self):
            return {"data": {"Tc": 20.1, "Uw": 69.8, "Fl": 0.0}}

        def set_target_temp(self, value):
            raise AssertionError("route-closed baseline must not set humidity-generator temperature")

        def set_target_rh(self, value):
            raise AssertionError("route-closed baseline must not set humidity-generator RH")

        def enable_control(self, value):
            raise AssertionError("route-closed baseline must not enable humidity-generator control")

        def set_flow_target(self, value):
            raise AssertionError("route-closed baseline must not set humidity-generator flow")

        def safe_stop(self):
            raise AssertionError("route-closed baseline must not safe-stop humidity generator")

    class PressureDevice:
        def __init__(self, value):
            self.value = value

        def read_pressure(self):
            return self.value

    class Analyzer:
        def __init__(self):
            self.ser = type("Serial", (), {"port": "COM35"})()

        def _drain_stream_lines(self, drain_s=0.18, read_timeout_s=0.02):
            return ["104.10"]

        def parse_line_mode2(self, line):
            return {"id": "023", "pressure_kpa": float(line), "h2o_ratio_f": 0.6405}

    runner_box = {}

    class Runner:
        def __init__(self, cfg, devices, logger, log_fn, prompt_fn):
            self.cfg = cfg
            self.devices = devices
            self.logger = logger
            self.ga = Analyzer()
            self.valve_calls = []
            runner_box["runner"] = self

        def _configure_devices(self):
            return None

        def _startup_preflight_reset(self):
            self._apply_valve_states([])

        def _h2o_open_valves(self, point):
            raise AssertionError("route-closed baseline must not compute/open H2O valves")

        def _apply_valve_states(self, valves):
            self.valve_calls.append(list(valves))

        def _all_gas_analyzers(self):
            return [("ga01", self.ga, {})]

        @staticmethod
        def _as_float(value):
            return None if value is None else float(value)

        def _read_sensor_parsed(self, ga, **kwargs):
            raise AssertionError("active stream should be used for route-closed baseline")

    monkeypatch.setattr(h2o_tool, "CalibrationRunner", Runner)
    monkeypatch.setattr(
        h2o_tool,
        "_build_devices",
        lambda cfg, io_logger=None: {
            "humidity_gen": Hgen(),
            "pace": PressureDevice(1014.0),
            "pressure_gauge": PressureDevice(1021.6),
        },
    )
    monkeypatch.setattr(h2o_tool, "_close_devices", lambda devices: None)

    rc = h2o_tool.main(
        [
            "--config",
            "site.json",
            "--run-id",
            "route_closed",
            "--hgen-temp",
            "20",
            "--hgen-rh",
            "70",
            "--pressure-diagnostic-only",
            "--pressure-diagnostic-route-closed-baseline",
            "--route-closed-baseline-settle-s",
            "0",
            "--pressure-diagnostic-s",
            "1",
            "--pressure-diagnostic-interval-s",
            "1",
            "--pressure-diagnostic-window-s",
            "1",
            "--pressure-diagnostic-min-samples",
            "1",
            "--no-prompt",
        ]
    )

    assert rc == 0
    assert runner_box["runner"].valve_calls == [[], []]
    assert atmosphere_calls == [
        {
            "hold_interval_s": h2o_tool.FORMAL_OPEN_FLOW_ATMOSPHERE_HOLD_INTERVAL_S,
            "vent_after_valve": False,
        }
    ]
    run_dir = tmp_path / "route_closed"
    baseline = json.loads((run_dir / "h2o_pressure_route_closed_baseline.json").read_text(encoding="utf-8"))
    assert baseline["route_opened"] is False
    assert baseline["open_valves"] == []
    assert baseline["no_write_assertion"]["opens_h2o_route"] is False
    assert baseline["no_write_assertion"]["sends_hgen_safe_stop"] is False


def test_h2o_pressure_diagnostic_can_use_vent_after_valve_then_close_it():
    class Pace:
        def __init__(self):
            self.calls = []

        def enter_atmosphere_mode_with_open_vent_valve(self, **kwargs):
            self.calls.append(("enter_open", kwargs))

        def start_atmosphere_hold(self, interval_s):
            self.calls.append(("hold", interval_s))

        def set_vent_after_valve_open(self, value):
            self.calls.append(("set_after_valve", value))

    pace = Pace()

    opened = _enter_h2o_pressure_diagnostic_atmosphere(
        pace,
        hold_interval_s=2.0,
        vent_after_valve=True,
    )
    _close_pace_vent_after_valve_if_opened(pace, opened)

    assert opened is True
    assert pace.calls[0][0] == "enter_open"
    assert pace.calls[1] == ("hold", 2.0)
    assert pace.calls[-1] == ("set_after_valve", False)


def test_h2o_pressure_diagnostic_default_keepalive_is_one_second():
    class Pace:
        def __init__(self):
            self.calls = []

        def enter_atmosphere_mode_with_open_vent_valve(self, **kwargs):
            self.calls.append(("enter_open", kwargs))

        def start_atmosphere_hold(self, interval_s):
            self.calls.append(("hold", interval_s))

    pace = Pace()

    opened = _enter_h2o_pressure_diagnostic_atmosphere(pace, vent_after_valve=True)

    assert opened is True
    assert pace.calls[1] == ("hold", h2o_tool.FORMAL_OPEN_FLOW_ATMOSPHERE_HOLD_INTERVAL_S)
    assert pace.calls[1][1] <= 1.0


def test_h2o_pressure_stability_diagnostic_classifies_stable_local_backpressure(tmp_path, monkeypatch):
    clock = [0.0]
    monkeypatch.setattr(h2o_tool.time, "time", lambda: clock[0])
    monkeypatch.setattr(h2o_tool.time, "sleep", lambda seconds: clock.__setitem__(0, clock[0] + float(seconds)))

    class Analyzer:
        def __init__(self, label, port, device_id):
            self.label = label
            self.device_id = device_id
            self.ser = type("Serial", (), {"port": port})()

    class Runner:
        def __init__(self):
            self.ga = Analyzer("ga01", "COM35", "023")
            self.values = [104.10, 104.12]

        def _all_gas_analyzers(self):
            return [("ga01", self.ga, {})]

        def _read_sensor_parsed(self, ga, **kwargs):
            value = self.values.pop(0) if self.values else 104.12
            return "YGAS,...", {
                "id": ga.device_id,
                "pressure_kpa": value,
                "h2o_ratio_f": 0.6405,
                "h2o_mmol": 1038.0,
                "co2_ppm": 24.4,
                "chamber_temp_c": 20.0,
                "case_temp_c": 20.3,
                "status": "OK",
            }

        @staticmethod
        def _as_float(value):
            return None if value is None else float(value)

    class PressureDevice:
        def __init__(self, value):
            self.value = value

        def read_pressure(self):
            return self.value

    class Dewpoint:
        def get_current_fast(self, timeout_s=0.5):
            return {"dewpoint_c": 16.46, "temp_c": 20.35, "rh_pct": 78.35, "flow_lpm": 1.58}

    class Hgen:
        def fetch_all(self):
            return {"data": {"Tc": 20.0, "Uw": 70.0, "Fl": 0.9, "Pc": 1005.5, "Ps": 1435.7}}

    payload = _collect_h2o_pressure_stability_diagnostic(
        Runner(),
        {
            "pace": PressureDevice(1014.0),
            "pressure_gauge": PressureDevice(1021.6),
            "dewpoint": Dewpoint(),
            "humidity_gen": Hgen(),
        },
        output_dir=tmp_path,
        duration_s=1.0,
        interval_s=1.0,
        window_s=1.0,
        analyzer_span_hpa=2.0,
        min_samples=2,
        min_valid_analyzers=1,
    )

    assert payload["status"] == "review"
    assert payload["physical_interpretation"] == "stable_wet_route_local_backpressure_observed"
    assert payload["no_write_assertion"]["writes_senco"] is False
    assert payload["per_analyzer"][0]["stable"] is True
    assert payload["tail_dewpoint_flow_lpm"]["mean"] == 1.58
    assert (tmp_path / "h2o_pressure_stability_diagnostic_trace.csv").exists()
    assert (tmp_path / "h2o_pressure_stability_diagnostic_summary.json").exists()
    with (tmp_path / "h2o_pressure_stability_diagnostic_trace.csv").open(encoding="utf-8-sig") as handle:
        rows = list(csv.DictReader(handle))
    assert rows[0]["dewpoint_flow_lpm"] == "1.58"


def test_h2o_pressure_diagnostic_reads_active_stream_without_polling_analyzer(tmp_path, monkeypatch):
    clock = [0.0]
    monkeypatch.setattr(h2o_tool.time, "time", lambda: clock[0])
    monkeypatch.setattr(h2o_tool.time, "sleep", lambda seconds: clock.__setitem__(0, clock[0] + float(seconds)))

    class Analyzer:
        def __init__(self):
            self.ser = type("Serial", (), {"port": "COM35"})()
            self.device_id = "023"
            self.values = [104.10, 104.11, 104.12, 104.12, 104.11]

        def _drain_stream_lines(self, drain_s=0.18, read_timeout_s=0.02):
            value = self.values.pop(0) if self.values else 104.11
            return [str(value)]

        def parse_line_mode2(self, line):
            return {"id": self.device_id, "pressure_kpa": float(line), "h2o_ratio_f": 0.6405}

    class Runner:
        def __init__(self):
            self.ga = Analyzer()

        def _all_gas_analyzers(self):
            return [("ga01", self.ga, {})]

        def _read_sensor_parsed(self, ga, **kwargs):
            raise AssertionError("diagnostic should read active stream frames before passive/poll fallback")

        @staticmethod
        def _as_float(value):
            return None if value is None else float(value)

    class PressureDevice:
        def read_pressure(self):
            return 1021.6

    payload = _collect_h2o_pressure_stability_diagnostic(
        Runner(),
        {"pace": PressureDevice(), "pressure_gauge": PressureDevice()},
        output_dir=tmp_path,
        duration_s=2.0,
        interval_s=0.5,
        window_s=2.0,
        analyzer_span_hpa=2.0,
        min_samples=4,
        min_valid_analyzers=1,
        reference_interval_s=5.0,
        analyzer_drain_s=0.18,
    )

    assert payload["per_analyzer"][0]["tail_pressure_hpa"]["count"] >= 4
    assert payload["per_analyzer"][0]["stable"] is True
    assert payload["status"] == "review"

    with (tmp_path / "h2o_pressure_stability_diagnostic_trace.csv").open(encoding="utf-8-sig") as handle:
        rows = list(csv.DictReader(handle))
    assert rows
    assert {row["analyzer_frame_source"] for row in rows} == {"active_stream_drain"}


def test_h2o_pressure_stability_diagnostic_fails_when_analyzer_internal_p_moves(tmp_path, monkeypatch):
    clock = [0.0]
    monkeypatch.setattr(h2o_tool.time, "time", lambda: clock[0])
    monkeypatch.setattr(h2o_tool.time, "sleep", lambda seconds: clock.__setitem__(0, clock[0] + float(seconds)))

    class Analyzer:
        def __init__(self):
            self.ser = type("Serial", (), {"port": "COM35"})()

    class Runner:
        def __init__(self):
            self.ga = Analyzer()
            self.values = [104.0, 105.2]

        def _all_gas_analyzers(self):
            return [("ga01", self.ga, {})]

        def _read_sensor_parsed(self, ga, **kwargs):
            value = self.values.pop(0) if self.values else 105.2
            return "YGAS,...", {"id": "023", "pressure_kpa": value, "h2o_ratio_f": 0.64}

        @staticmethod
        def _as_float(value):
            return None if value is None else float(value)

    class PressureDevice:
        def read_pressure(self):
            return 1021.6

    payload = _collect_h2o_pressure_stability_diagnostic(
        Runner(),
        {"pace": PressureDevice(), "pressure_gauge": PressureDevice()},
        output_dir=tmp_path,
        duration_s=1.0,
        interval_s=1.0,
        window_s=1.0,
        analyzer_span_hpa=2.0,
        min_samples=2,
        min_valid_analyzers=1,
    )

    assert payload["status"] == "fail"
    assert payload["physical_interpretation"] == "analyzer_internal_pressure_unstable_or_cache_not_refreshed"
    assert payload["per_analyzer"][0]["tail_pressure_hpa"]["span"] == 12.0


def test_prepare_runtime_cfg_h2o_supports_explicit_1hz_active_stream_ftd_trial():
    cfg = {
        "devices": {
            "humidity_generator": {"enabled": False},
            "gas_analyzer": {"active_send": False, "ftd_hz": 10},
        },
        "workflow": {"analyzer_mode2_init": {"send_active_freq": False}},
        "paths": {"output_dir": "logs/example"},
    }

    out = _prepare_runtime_cfg(
        cfg,
        output_dir=None,
        sample_count=10,
        sample_interval_s=1.0,
        sensor_read_interval_s=5.0,
        analyzer_acquisition="active_stream_1hz",
        allow_ftd_write=True,
    )

    assert out["metadata"]["analyzer_acquisition_policy"] == "active_mode2_stream_1hz_ftd01_controlled"
    assert out["metadata"]["ftd_write_enabled"] is True
    assert out["workflow"]["analyzer_mode2_init"]["send_active_freq"] is True
    assert out["workflow"]["analyzer_mode2_init"]["skip_config_when_read_first_ready"] is True
    assert out["devices"]["gas_analyzer"]["active_send"] is True
    assert out["devices"]["gas_analyzer"]["ftd_hz"] == 1
    assert out["devices"]["humidity_generator"]["enabled"] is True


def test_set_h2o_open_flow_hgen_flow_records_before_after_snapshots():
    class Hgen:
        def __init__(self):
            self.calls = []
            self.flow = 2.4

        def fetch_all(self):
            return {"data": {"Tc": 20.1, "Uw": 69.2, "Fl": self.flow}}

        def set_flow_target(self, value):
            self.calls.append(("set_flow_target", value))
            self.flow = float(value)

    hgen = Hgen()
    result = _set_h2o_open_flow_hgen_flow(
        {"humidity_gen": hgen},
        3.0,
        readback_timeout_s=0.0,
    )

    assert hgen.calls == [("set_flow_target", 3.0)]
    assert result["ok"] is True
    assert result["requested_flow_lpm"] == 3.0
    assert result["before_snapshot"]["data"]["Fl"] == 2.4
    assert result["after_snapshot"]["data"]["Fl"] == 3.0
    assert result["observed_flow_lpm"] == 3.0


def test_set_h2o_open_flow_hgen_flow_retries_empty_readback(monkeypatch):
    sleeps = []

    class Hgen:
        def __init__(self):
            self.calls = []
            self.fetch_count = 0
            self.flow = 2.4

        def fetch_all(self):
            self.fetch_count += 1
            if self.fetch_count == 2:
                return {"raw": "", "data": {}}
            return {"data": {"Flux": self.flow}}

        def set_flow_target(self, value):
            self.calls.append(("set_flow_target", value))
            self.flow = float(value)

    monkeypatch.setattr(
        "gas_calibrator.tools.run_v1_5_formal_h2o_open_flow_sampling.time.sleep",
        lambda value: sleeps.append(value),
    )
    hgen = Hgen()
    result = _set_h2o_open_flow_hgen_flow(
        {"humidity_gen": hgen},
        3.0,
        readback_timeout_s=1.0,
        readback_poll_s=0.1,
    )

    assert hgen.calls == [("set_flow_target", 3.0)]
    assert sleeps == [0.1]
    assert result["observed_flow_lpm"] == 3.0
    assert result["target_reached"] is True


def test_set_h2o_open_flow_hgen_flow_waits_for_target_not_stale_flow(monkeypatch):
    sleeps = []

    class Hgen:
        def __init__(self):
            self.calls = []
            self.fetch_count = 0

        def fetch_all(self):
            self.fetch_count += 1
            flow = 12.4 if self.fetch_count < 4 else 2.9
            return {"data": {"Flux": flow}}

        def set_flow_target(self, value):
            self.calls.append(("set_flow_target", value))

    monkeypatch.setattr(
        "gas_calibrator.tools.run_v1_5_formal_h2o_open_flow_sampling.time.sleep",
        lambda value: sleeps.append(value),
    )
    hgen = Hgen()
    result = _set_h2o_open_flow_hgen_flow(
        {"humidity_gen": hgen},
        3.0,
        readback_timeout_s=5.0,
        readback_poll_s=0.2,
        readback_tolerance_lpm=0.5,
    )

    assert hgen.calls == [("set_flow_target", 3.0)]
    assert sleeps == [0.2, 0.2]
    assert result["before_snapshot"]["data"]["Flux"] == 12.4
    assert result["after_snapshot"]["data"]["Flux"] == 2.9
    assert result["observed_flow_lpm"] == 2.9
    assert result["target_reached"] is True
    assert result["ok"] is True


def test_set_h2o_open_flow_hgen_flow_records_unconfirmed_target_as_warning():
    class Hgen:
        def __init__(self):
            self.calls = []

        def fetch_all(self):
            return {"data": {"Fl": 12.4}}

        def set_flow_target(self, value):
            self.calls.append(("set_flow_target", value))

    result = _set_h2o_open_flow_hgen_flow(
        {"humidity_gen": Hgen()},
        3.0,
        readback_timeout_s=0.0,
        readback_tolerance_lpm=0.5,
    )

    assert result["ok"] is True
    assert result["target_reached"] is False
    assert result["observed_flow_lpm"] == 12.4
    assert result["warning"] == "flow_target_readback_not_within_tolerance"


def test_humidity_reference_check_explains_saturation_mismatch():
    result = _build_humidity_reference_check(
        {"rh_pct": 100.0, "temp_c": 20.75},
        {"data": {"Uw": 66.8, "Tc": 20.69}},
    )

    assert result["status"] == "fail"
    assert "dewpoint_meter_reports_saturation_but_hgen_is_not_saturated" in result["reasons"]
    assert "dewpoint_rh_not_consistent_with_humidity_generator" in result["reasons"]
    assert result["rh_diff_pct"] == 33.2
    assert result["hard_block"] is True
    assert "block formal H2O sampling" in result["human_summary"]
    return
    assert "水汽参考不一致" in result["human_summary"]


def test_read_dewpoint_snapshot_for_evidence_retries_transient_blank(monkeypatch):
    sleeps = []

    class Dewpoint:
        def __init__(self):
            self.calls = 0

        def get_current_fast(self, timeout_s=0.5):
            self.calls += 1
            if self.calls == 1:
                return {}
            return {"dewpoint_c": 16.11, "temp_c": 20.08, "rh_pct": 77.91}

    monkeypatch.setattr(
        "gas_calibrator.tools.run_v1_5_formal_h2o_open_flow_sampling.time.sleep",
        lambda value: sleeps.append(value),
    )

    dewpoint = Dewpoint()
    result = _read_dewpoint_snapshot_for_evidence(dewpoint, attempts=3, sleep_s=0.2)

    assert result["dewpoint_c"] == 16.11
    assert result["_evidence_read_attempt"] == 2
    assert result["_evidence_read_attempts_max"] == 3
    assert sleeps == [0.2]


def test_humidity_reference_check_keeps_non_saturation_rh_gap_as_review_warning():
    result = _build_humidity_reference_check(
        {"rh_pct": 77.91, "temp_c": 20.08},
        {"data": {"Uw": 69.6, "Tc": 20.101}},
    )

    assert result["status"] == "warn"
    assert result["hard_block"] is False
    assert result["warnings"] == ["dewpoint_rh_not_consistent_with_humidity_generator"]
    assert result["hard_reasons"] == []
    assert result["rh_diff_pct"] == 8.310000000000002
    assert "primary H2O reference" in result["human_summary"]


def test_write_humidity_reference_review_records_dewpoint_primary_policy(tmp_path):
    class Logger:
        run_dir = tmp_path

    class Dewpoint:
        def get_current_fast(self, timeout_s=0.5):
            return {"dewpoint_c": 16.11, "temp_c": 20.08, "rh_pct": 77.91}

    class Hgen:
        def fetch_all(self):
            return {"data": {"Tc": 20.101, "Uw": 69.6}}

    point = _build_h2o_open_flow_point(
        temp_c=20.0,
        hgen_temp_c=20.0,
        hgen_rh_pct=70.0,
        certificate_dewpoint_c=None,
        certificate_h2o_mmol=None,
    )

    payload = _write_humidity_reference_review(
        Logger(),
        point=point,
        devices={"dewpoint": Dewpoint(), "humidity_gen": Hgen()},
        route_opened=True,
    )

    path = tmp_path / "h2o_humidity_reference_review.json"
    assert path.exists()
    assert payload["humidity_reference_check"]["status"] == "warn"
    assert payload["humidity_reference_check"]["hard_block"] is False
    assert "primary H2O reference" in payload["physical_interpretation"]


def test_write_gate_failure_records_humidity_evidence(tmp_path):
    class Logger:
        run_dir = tmp_path

    class Dewpoint:
        def get_current_fast(self, timeout_s=0.5):
            return {"dewpoint_c": 16.1, "temp_c": 20.2, "rh_pct": 76.0}

    class Hgen:
        def fetch_all(self):
            return {"data": {"Tc": 20.1, "Uw": 69.2}}

    class PressureDevice:
        def __init__(self, value):
            self.value = value

        def read_pressure(self):
            return self.value

    point = _build_h2o_open_flow_point(
        temp_c=20.0,
        hgen_temp_c=20.0,
        hgen_rh_pct=70.0,
        certificate_dewpoint_c=14.36,
        certificate_h2o_mmol=16.3715,
    )

    path = _write_gate_failure(
        Logger(),
        reason="dewpoint_alignment_gate_failed",
        point=point,
        devices={
            "dewpoint": Dewpoint(),
            "humidity_gen": Hgen(),
            "pace": PressureDevice(1007.2),
            "pressure_gauge": PressureDevice(1006.8),
        },
        route_opened=True,
    )

    text = path.read_text(encoding="utf-8")
    assert "dewpoint_alignment_gate_failed" in text
    assert '"route_opened": true' in text
    assert '"h2o_mmol": 16.3715' in text
    assert '"humidity_reference_check"' in text
    assert '"human_reject_reason"' in text
    assert '"pace_pressure_hpa": 1007.2' in text
