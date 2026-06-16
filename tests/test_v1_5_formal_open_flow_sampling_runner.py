import csv
import json

from gas_calibrator.tools.run_v1_5_formal_open_flow_sampling import (
    _apply_certificate_target_after_valve_selection,
    _build_open_flow_point,
    _build_nitrogen_purge_open_valves,
    _configured_analyzer_labels,
    _defer_startup_mode2_disabled_analyzers,
    _nitrogen_purge_source_valve,
    _parse_args,
    _prepare_runtime_cfg,
    _wait_open_flow_co2_dewpoint_gate,
    _write_machine_readable_samples,
    _write_purge_trace,
    _write_route_timing,
)


class _SequencePressure:
    def __init__(self, values):
        self._values = list(values)
        self._last = self._values[-1] if self._values else None

    def read_pressure(self):
        if self._values:
            self._last = self._values.pop(0)
        return self._last


def _read_trace_rows(path):
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def test_open_flow_point_keeps_source_nominal_for_group_b_valve_selection():
    point = _build_open_flow_point(
        temp_c=20.0,
        co2_source_ppm=900.0,
        co2_group="B",
        pressure_target_hpa=None,
    )

    assert point.co2_ppm == 900.0
    assert point.co2_group == "B"
    assert point.hgen_temp_c is None
    assert point.hgen_rh_pct is None


def test_formal_co2_open_flow_default_purge_is_360s():
    args = _parse_args(["--config", "config.json", "--co2-source-ppm", "100"])

    assert args.purge_s == 360.0
    assert args.minimum_purge_s == 360.0
    assert args.n2_prepurge_s == 0.0
    assert args.n2_purge_source_valve is None
    assert args.analyzer_acquisition == "active_stream_1hz"
    assert args.allow_ftd_write is True
    assert args.analyzer_gate_required_labels == ""
    assert args.co2_ratio_f_preseal_tol is None
    assert args.co2_ratio_f_preseal_window_s is None
    assert args.co2_ratio_f_preseal_timeout_s is None
    assert args.co2_ratio_f_preseal_min_samples is None
    assert args.co2_ratio_f_preseal_policy is None
    assert args.gas_route_dewpoint_gate_enabled is None
    assert args.open_flow_pressure_transient_grace_s == 30.0
    assert args.open_flow_pressure_safety_hard_limit_hpa == 1300.0


def test_purge_trace_records_short_pressure_spike_without_abort(tmp_path):
    trace_path = tmp_path / "purge_trace.csv"
    devices = {
        "pace": _SequencePressure([1250.0, 1013.2, 1013.1]),
        "pressure_gauge": _SequencePressure([1240.0, 1013.0, 1013.0]),
    }

    _write_purge_trace(
        trace_path,
        devices=devices,
        open_valves=[1, 2],
        purge_s=0.45,
        interval_s=0.2,
        max_pressure_hpa=1100.0,
        transient_grace_s=30.0,
        hard_limit_hpa=1300.0,
    )

    rows = _read_trace_rows(trace_path)
    assert rows
    assert rows[0]["pressure_transient_status"] == "transient_over_limit"
    assert any(row["pressure_transient_status"] == "in_limit" for row in rows)


def test_purge_trace_aborts_only_when_pressure_spike_persists(tmp_path):
    trace_path = tmp_path / "purge_trace.csv"
    devices = {
        "pace": _SequencePressure([1250.0] * 10),
        "pressure_gauge": _SequencePressure([1240.0] * 10),
    }

    try:
        _write_purge_trace(
            trace_path,
            devices=devices,
            open_valves=[1, 2],
            purge_s=0.45,
            interval_s=0.2,
            max_pressure_hpa=1100.0,
            transient_grace_s=0.05,
            hard_limit_hpa=1300.0,
        )
    except RuntimeError as exc:
        assert "OPEN_FLOW_PRESSURE_PERSISTENT_LIMIT_EXCEEDED" in str(exc)
    else:
        raise AssertionError("expected persistent open-flow pressure abort")


def test_purge_trace_hard_pressure_limit_is_immediate(tmp_path):
    trace_path = tmp_path / "purge_trace.csv"
    devices = {"pace": _SequencePressure([1300.1])}

    try:
        _write_purge_trace(
            trace_path,
            devices=devices,
            open_valves=[1, 2],
            purge_s=0.02,
            interval_s=0.001,
            max_pressure_hpa=1100.0,
            transient_grace_s=30.0,
            hard_limit_hpa=1300.0,
        )
    except RuntimeError as exc:
        assert "OPEN_FLOW_PRESSURE_HARD_LIMIT_EXCEEDED" in str(exc)
    else:
        raise AssertionError("expected hard open-flow pressure abort")


def test_route_timing_evidence_proves_sampling_before_route_close(tmp_path):
    path = tmp_path / "formal_open_flow_route_timing.json"

    _write_route_timing(
        path,
        run_id="run001",
        co2_source_ppm=900.0,
        co2_group="B",
        route_opened=True,
        co2_route_opened_at="2026-06-13T10:00:00.000",
        sample_window_started_at="2026-06-13T10:06:00.000",
        sample_window_ended_at="2026-06-13T10:06:10.000",
        co2_route_closed_at="2026-06-13T10:06:12.000",
    )

    payload = json.loads(path.read_text(encoding="utf-8"))
    assert payload["schema_version"] == "v1_5_formal_open_flow_route_timing_v0"
    assert payload["route_opened"] is True
    assert payload["sampling_before_route_close"] is True
    assert "route remains open during the formal sample window" in payload["physical_meaning"]


def test_n2_purge_route_uses_same_co2_path_without_target_source():
    point = _build_open_flow_point(
        temp_c=20.0,
        co2_source_ppm=900.0,
        co2_group="B",
        pressure_target_hpa=None,
    )

    class Runner:
        cfg = {"valves": {"nitrogen_purge_source": 27}}

        def _co2_open_valves(self, point, include_total_valve):
            assert include_total_valve is True
            return [8, 11, 16, 26]

        def _source_valve_for_point(self, point):
            return 26

    assert _nitrogen_purge_source_valve(Runner.cfg) == 27
    assert _build_nitrogen_purge_open_valves(Runner(), point) == [8, 11, 16, 27]


def test_n2_purge_route_rejects_source_conflict():
    point = _build_open_flow_point(
        temp_c=20.0,
        co2_source_ppm=900.0,
        co2_group="B",
        pressure_target_hpa=None,
    )

    class Runner:
        cfg = {"valves": {"nitrogen_purge_source": 26}}

        def _co2_open_valves(self, point, include_total_valve):
            return [8, 11, 16, 26]

        def _source_valve_for_point(self, point):
            return 26

    try:
        _build_nitrogen_purge_open_valves(Runner(), point)
    except RuntimeError as exc:
        assert "N2_PURGE_SOURCE_CONFLICTS_WITH_CO2_SOURCE" in str(exc)
    else:
        raise AssertionError("expected N2/CO2 source conflict")


def test_n2_purge_route_rejects_unmanaged_source():
    point = _build_open_flow_point(
        temp_c=20.0,
        co2_source_ppm=900.0,
        co2_group="B",
        pressure_target_hpa=None,
    )

    class Runner:
        cfg = {"valves": {"nitrogen_purge_source": 27}}

        def _managed_valves(self):
            return [8, 11, 16, 26]

        def _co2_open_valves(self, point, include_total_valve):
            return [8, 11, 16, 26]

        def _source_valve_for_point(self, point):
            return 26

    try:
        _build_nitrogen_purge_open_valves(Runner(), point)
    except RuntimeError as exc:
        assert "N2_PURGE_SOURCE_NOT_MANAGED" in str(exc)
    else:
        raise AssertionError("expected unmanaged N2 source rejection")


def test_certificate_target_replaces_nominal_after_valve_selection():
    point = _build_open_flow_point(
        temp_c=20.0,
        co2_source_ppm=900.0,
        co2_group="B",
        pressure_target_hpa=None,
    )
    assert point.co2_ppm == 900.0

    _apply_certificate_target_after_valve_selection(point, 897.04)

    assert point.co2_ppm == 897.04
    assert point.co2_group == "B"


def test_prepare_runtime_cfg_blocks_writes_and_uses_1hz_active_stream_with_ftd01():
    cfg = {
        "devices": {
            "humidity_generator": {"enabled": True},
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
        min_valid_analyzers=1,
    )

    assert out["workflow"]["route_mode"] == "co2_open_flow_sidecar"
    assert out["workflow"]["skip_h2o"] is True
    assert out["workflow"]["analyzer_mode2_init"]["read_first_before_config"] is True
    assert out["workflow"]["analyzer_mode2_init"]["write_config_on_read_first_fail"] is True
    assert out["workflow"]["analyzer_mode2_init"]["send_active_freq"] is True
    assert out["workflow"]["analyzer_mode2_init"]["skip_config_when_read_first_ready"] is False
    assert out["workflow"]["analyzer_mode2_init"]["reapply_attempts"] >= 2
    assert out["workflow"]["analyzer_mode2_init"]["stream_attempts"] >= 15
    assert out["workflow"]["analyzer_mode2_init"]["post_enable_stream_wait_s"] >= 4.0
    assert out["workflow"]["analyzer_mode2_init"]["post_enable_stream_ack_wait_s"] >= 10.0
    assert out["workflow"]["pressure"]["continuous_atmosphere_hold"] is False
    assert out["workflow"]["stability"]["temperature"]["analyzer_chamber_temp_span_c"] == 0.08
    assert out["workflow"]["postrun_corrected_delivery"]["enabled"] is False
    assert out["workflow"]["postrun_corrected_delivery"]["write_devices"] is False
    assert out["workflow"]["startup_pressure_sensor_calibration"]["apply_write"] is False
    assert out["metadata"]["writes_senco"] is False
    assert out["metadata"]["writes_device_id"] is False
    contract = out["metadata"]["open_flow_sampling_physical_contract"]
    assert contract["sample_window_requires_route_open"] is True
    assert contract["sample_window_requires_standard_gas_open_flow"] is True
    assert contract["route_close_allowed_only_after_sample_window"] is True
    assert contract["per_analyzer_ratio_stability_required"] is True
    assert contract["per_analyzer_status_register_qc_required"] is True
    assert contract["unstable_analyzer_handling"] == (
        "independent_grade_or_reject_do_not_block_all_when_min_valid_met"
    )
    assert contract["pressure_role"] == "traceability_and_qc_input_not_co2_fit_variable"
    assert out["metadata"]["analyzer_acquisition_policy"] == "active_mode2_stream_1hz_ftd01_controlled"
    assert out["metadata"]["analyzer_stream_target_hz"] == 1.0
    assert out["metadata"]["analyzer_stream_native_hz"] == 1.0
    assert out["metadata"]["analyzer_stream_frequency_control"] == "FTD01_written"
    assert out["metadata"]["formal_sample_anchor_interval_s"] == 1.0
    assert out["metadata"]["formal_sample_decimation"] == (
        "nearest_usable_mode2_frame_at_1hz_anchor_from_1hz_stream"
    )
    assert out["metadata"]["ftd_write_enabled"] is True
    assert out["metadata"]["idle_continuous_atmosphere_hold"] is False
    assert out["metadata"]["startup_mode2_missing_policy"] == "mode2_stream_config_then_sampling_qc"
    assert out["devices"]["gas_analyzer"]["active_send"] is True
    assert out["devices"]["gas_analyzer"]["ftd_hz"] == 1
    assert [item["active_send"] for item in out["devices"]["gas_analyzers"]] == [True, True]
    assert [item["ftd_hz"] for item in out["devices"]["gas_analyzers"]] == [1, 1]
    assert out["devices"]["humidity_generator"]["enabled"] is False
    live_cfg = out["workflow"]["analyzer_live_snapshot"]
    assert live_cfg["enabled"] is True
    assert live_cfg["passive_round_robin_enabled"] is False
    assert live_cfg["sampling_worker_interval_s"] == 0.2
    assert live_cfg["active_ring_buffer_size"] == 128
    assert live_cfg["active_frame_max_anchor_delta_ms"] == 800.0
    assert live_cfg["active_frame_stale_ms"] == 2500.0
    stability = out["workflow"]["stability"]
    assert stability["analyzer_gate_min_valid_analyzers"] == 1
    assert stability["analyzer_gate_optional_labels"] == ["ga01", "ga02"]
    assert stability["analyzer_gate_required_labels"] == []
    assert stability["analyzer_gate_allow_pass_with_dropped_optional"] is True
    assert stability["analyzer_gate_disable_dropped_optional"] is False
    assert stability["analyzer_gate_zero_value_policy"] == "drop_optional_not_block"
    assert stability["sensor"]["co2_ratio_f_preseal_tol"] == 0.001
    assert stability["sensor"]["co2_ratio_f_preseal_flush_active_stream_before_gate"] is True
    summary_filter = out["workflow"]["sampling"]["summary_outlier_filter"]
    assert summary_filter["enabled"] is True
    assert summary_filter["scope"] == "per_analyzer_sample_window_summary_only"
    assert summary_filter["raw_frame_retention"] == "all_raw_frames_kept"
    assert summary_filter["keys"] == ["co2_ratio_f", "h2o_ratio_f"]
    assert summary_filter["absolute_thresholds"]["co2_ratio_f"] == 0.001
    assert summary_filter["absolute_thresholds"]["h2o_ratio_f"] == 0.001
    assert summary_filter["max_outliers_per_key"] == 1
    assert out["workflow"]["sampling"]["pre_sample_freshness_timeout_s"] == 5.0
    assert out["workflow"]["sampling"]["pre_sample_signal_max_age_s"] == 1.5
    assert out["workflow"]["sampling"]["pre_sample_analyzer_max_age_s"] == 2.5


def test_prepare_runtime_cfg_accepts_formal_co2_dewpoint_gate_options():
    cfg = {
        "devices": {"gas_analyzers": [{"name": "ga01"}, {"name": "ga02"}]},
        "workflow": {"stability": {"sensor": {}}},
        "paths": {"output_dir": "logs/example"},
    }

    out = _prepare_runtime_cfg(
        cfg,
        output_dir=None,
        sample_count=10,
        sample_interval_s=1.0,
        sensor_read_interval_s=5.0,
        min_valid_analyzers=2,
        gas_route_dewpoint_gate_enabled=True,
        gas_route_dewpoint_gate_policy="reject",
        gas_route_dewpoint_require_dry_enough=True,
        gas_route_dewpoint_dry_enough_c=-25.0,
        gas_route_dewpoint_gate_max_total_wait_s=1200.0,
        gas_route_dewpoint_gate_window_s=60.0,
        gas_route_dewpoint_gate_tail_span_max_c=0.45,
        gas_route_dewpoint_gate_tail_slope_abs_max_c_per_s=0.005,
    )

    stability = out["workflow"]["stability"]
    assert stability["gas_route_dewpoint_gate_enabled"] is True
    assert stability["gas_route_dewpoint_gate_policy"] == "reject"
    assert stability["gas_route_dewpoint_gate_require_dry_enough"] is True
    assert stability["gas_route_dewpoint_gate_dry_enough_c"] == -25.0
    assert stability["gas_route_dewpoint_gate_max_total_wait_s"] == 1200.0
    assert stability["gas_route_dewpoint_gate_window_s"] == 60.0
    assert stability["gas_route_dewpoint_gate_tail_span_max_c"] == 0.45
    assert stability["gas_route_dewpoint_gate_tail_slope_abs_max_c_per_s"] == 0.005


def test_open_flow_co2_dewpoint_gate_runs_after_purge_before_ratio_gate():
    calls = []

    class Runner:
        def _gas_route_dewpoint_gate_enabled(self):
            calls.append("enabled")
            return True

        def _wait_co2_route_dewpoint_gate_before_seal(self, point, **kwargs):
            calls.append(("dewpoint", point, kwargs))
            return True

    point = _build_open_flow_point(
        temp_c=20.0,
        co2_source_ppm=900.0,
        co2_group="B",
        pressure_target_hpa=None,
    )

    assert _wait_open_flow_co2_dewpoint_gate(
        Runner(),
        point,
        purge_s=360.0,
        purge_begin_wall_s=10.0,
        purge_end_wall_s=370.0,
    )

    assert calls[0] == "enabled"
    assert calls[1][0] == "dewpoint"
    assert calls[1][2]["base_soak_s"] == 360.0
    assert calls[1][2]["log_context"] == "open-flow sidecar after minimum purge"


def test_prepare_runtime_cfg_keeps_passive_query_as_explicit_fallback():
    cfg = {
        "devices": {
            "gas_analyzer": {"active_send": True},
            "gas_analyzers": [
                {"name": "ga01", "active_send": True},
                {"name": "ga02", "active_send": True},
            ],
        },
        "workflow": {"stability": {"sensor": {}}},
        "paths": {"output_dir": "logs/example"},
    }

    out = _prepare_runtime_cfg(
        cfg,
        output_dir=None,
        sample_count=10,
        sample_interval_s=1.0,
        sensor_read_interval_s=5.0,
        min_valid_analyzers=1,
        analyzer_acquisition="passive_query",
    )

    assert out["metadata"]["analyzer_acquisition_policy"] == "passive_query_per_device"
    assert out["devices"]["gas_analyzer"]["active_send"] is False
    assert [item["active_send"] for item in out["devices"]["gas_analyzers"]] == [False, False]
    live_cfg = out["workflow"]["analyzer_live_snapshot"]
    assert live_cfg["enabled"] is False
    assert live_cfg["passive_round_robin_enabled"] is True
    assert live_cfg["passive_per_device_workers_enabled"] is True
    assert live_cfg["passive_round_robin_interval_s"] == 5.0
    assert live_cfg["cache_ttl_s"] == 7.5
    assert out["workflow"]["sampling"]["pre_sample_analyzer_max_age_s"] == 7.5


def test_prepare_runtime_cfg_supports_explicit_1hz_active_stream_ftd_trial():
    cfg = {
        "devices": {
            "gas_analyzer": {"active_send": False, "ftd_hz": 10},
            "gas_analyzers": [
                {"name": "ga01", "active_send": False, "ftd_hz": 10},
                {"name": "ga02", "active_send": False, "ftd_hz": 10},
            ],
        },
        "workflow": {
            "analyzer_mode2_init": {"send_active_freq": False},
            "stability": {"sensor": {}},
        },
        "paths": {"output_dir": "logs/example"},
    }

    out = _prepare_runtime_cfg(
        cfg,
        output_dir=None,
        sample_count=10,
        sample_interval_s=1.0,
        sensor_read_interval_s=5.0,
        min_valid_analyzers=1,
        analyzer_acquisition="active_stream_1hz",
        allow_ftd_write=True,
    )

    assert out["metadata"]["analyzer_acquisition_policy"] == "active_mode2_stream_1hz_ftd01_controlled"
    assert out["metadata"]["analyzer_stream_native_hz"] == 1.0
    assert out["metadata"]["ftd_write_enabled"] is True
    assert out["metadata"]["formal_sample_decimation"] == (
        "nearest_usable_mode2_frame_at_1hz_anchor_from_1hz_stream"
    )
    assert out["workflow"]["analyzer_mode2_init"]["send_active_freq"] is True
    assert out["workflow"]["analyzer_mode2_init"]["skip_config_when_read_first_ready"] is False
    assert out["workflow"]["analyzer_mode2_init"]["reapply_attempts"] >= 2
    assert out["workflow"]["analyzer_mode2_init"]["stream_attempts"] >= 15
    assert out["devices"]["gas_analyzer"]["active_send"] is True
    assert out["devices"]["gas_analyzer"]["ftd_hz"] == 1
    assert [item["ftd_hz"] for item in out["devices"]["gas_analyzers"]] == [1, 1]
    live_cfg = out["workflow"]["analyzer_live_snapshot"]
    assert live_cfg["sampling_worker_interval_s"] == 0.2
    assert live_cfg["active_ring_buffer_size"] == 128
    assert live_cfg["active_frame_max_anchor_delta_ms"] == 800.0
    assert live_cfg["active_frame_stale_ms"] == 2500.0
    assert out["workflow"]["sampling"]["pre_sample_analyzer_max_age_s"] == 2.5


def test_prepare_runtime_cfg_1hz_active_stream_does_not_write_ftd_without_explicit_allow():
    cfg = {
        "devices": {"gas_analyzer": {"active_send": False, "ftd_hz": 10}},
        "workflow": {"analyzer_mode2_init": {"send_active_freq": True}, "stability": {"sensor": {}}},
        "paths": {"output_dir": "logs/example"},
    }

    out = _prepare_runtime_cfg(
        cfg,
        output_dir=None,
        sample_count=10,
        sample_interval_s=1.0,
        sensor_read_interval_s=5.0,
        min_valid_analyzers=1,
        analyzer_acquisition="active_stream_1hz",
        allow_ftd_write=False,
    )

    assert out["metadata"]["analyzer_acquisition_policy"] == "active_mode2_stream_existing_rate_no_ftd_requested_1hz"
    assert out["metadata"]["analyzer_stream_target_hz"] == 1.0
    assert out["metadata"]["analyzer_stream_native_hz"] is None
    assert out["metadata"]["analyzer_stream_frequency_control"] == "existing_device_setting_no_ftd_write"
    assert out["metadata"]["ftd_write_enabled"] is False
    assert out["workflow"]["analyzer_mode2_init"]["send_active_freq"] is False
    assert out["workflow"]["analyzer_mode2_init"]["skip_config_when_read_first_ready"] is False


def test_prepare_runtime_cfg_clamps_min_valid_to_configured_analyzers():
    cfg = {
        "devices": {
            "gas_analyzers": [
                {"name": "ga01"},
                {"name": "ga02"},
                {"name": "ga03"},
            ],
        },
        "workflow": {"stability": {"sensor": {}}},
        "paths": {"output_dir": "logs/example"},
    }

    out = _prepare_runtime_cfg(
        cfg,
        output_dir=None,
        sample_count=10,
        sample_interval_s=1.0,
        sensor_read_interval_s=5.0,
        min_valid_analyzers=8,
    )

    assert out["workflow"]["stability"]["analyzer_gate_min_valid_analyzers"] == 3
    assert out["workflow"]["stability"]["analyzer_gate_optional_labels"] == ["ga01", "ga02", "ga03"]


def test_prepare_runtime_cfg_accepts_strict_co2_ratio_gate_for_required_analyzers():
    cfg = {
        "devices": {
            "gas_analyzers": [
                {"name": "ga01"},
                {"name": "ga02"},
                {"name": "ga03"},
                {"name": "ga04"},
            ],
        },
        "workflow": {"stability": {"sensor": {}}},
        "paths": {"output_dir": "logs/example"},
    }

    out = _prepare_runtime_cfg(
        cfg,
        output_dir=None,
        sample_count=40,
        sample_interval_s=1.0,
        sensor_read_interval_s=5.0,
        min_valid_analyzers=1,
        analyzer_gate_required_labels=["ga02", "ga04", "unknown"],
        co2_ratio_f_preseal_tol=0.0002,
        co2_ratio_f_preseal_window_s=120.0,
        co2_ratio_f_preseal_timeout_s=900.0,
        co2_ratio_f_preseal_min_samples=20,
        co2_ratio_f_preseal_policy="warn",
    )

    sensor_cfg = out["workflow"]["stability"]["sensor"]
    assert sensor_cfg["co2_ratio_f_preseal_tol"] == 0.0002
    assert sensor_cfg["co2_ratio_f_preseal_window_s"] == 120.0
    assert sensor_cfg["co2_ratio_f_preseal_timeout_s"] == 900.0
    assert sensor_cfg["co2_ratio_f_preseal_min_samples"] == 20
    assert sensor_cfg["co2_ratio_f_preseal_policy"] == "warn"
    stability = out["workflow"]["stability"]
    assert stability["analyzer_gate_required_labels"] == ["ga02", "ga04"]
    assert stability["analyzer_gate_optional_labels"] == ["ga01", "ga03"]
    assert stability["analyzer_gate_min_valid_analyzers"] == 2
    assert stability["analyzer_gate_max_wait_s"] == 900.0


def test_configured_analyzer_labels_fall_back_to_stable_names():
    assert _configured_analyzer_labels(
        {"devices": {"gas_analyzers": [{}, {"label": "lane-b"}]}}
    ) == ["ga01", "lane-b"]


def test_defer_startup_mode2_disabled_analyzers_restores_for_sampling_qc():
    events = []
    logs = []

    class Runner:
        _disabled_analyzers = {"ga02", "ga03"}
        _disabled_analyzer_reasons = {
            "ga02": "startup_mode2_verify_failed",
            "ga03": "operator_disabled",
        }
        _disabled_analyzer_last_reprobe_ts = {"ga02": 123.0, "ga03": 456.0}

        def _log_run_event(self, **kwargs):
            events.append(kwargs)

        def log(self, message):
            logs.append(message)

    restored = _defer_startup_mode2_disabled_analyzers(Runner())

    assert restored == ["ga02"]
    assert Runner._disabled_analyzers == {"ga03"}
    assert Runner._disabled_analyzer_reasons == {"ga03": "operator_disabled"}
    assert Runner._disabled_analyzer_last_reprobe_ts == {"ga03": 456.0}
    assert events[0]["command"] == "analyzer-startup-mode2-deferred-to-sampling-qc"
    assert "ga02" in logs[0]


def test_write_machine_readable_samples_preserves_raw_keys(tmp_path):
    paths = _write_machine_readable_samples(
        tmp_path,
        [
            {
                "ga01_device_id": "023",
                "ga01_co2_ppm": 897.04,
                "nested": {"source": "mode2"},
            }
        ],
    )

    csv_text = (tmp_path / "samples_machine_readable.csv").read_text(encoding="utf-8-sig")
    jsonl_text = (tmp_path / "samples_machine_readable.jsonl").read_text(encoding="utf-8")
    assert paths["csv"].endswith("samples_machine_readable.csv")
    assert "ga01_device_id" in csv_text
    assert "897.04" in csv_text
    assert '"source": "mode2"' in jsonl_text
