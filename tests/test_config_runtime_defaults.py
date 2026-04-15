import json
from pathlib import Path

from gas_calibrator.config import load_config


def test_load_config_injects_minimal_runtime_defaults_for_new_fields(tmp_path: Path) -> None:
    cfg_path = tmp_path / "headless_minimal.json"
    cfg_path.write_text(
        json.dumps(
            {
                "paths": {"output_dir": "logs", "points_excel": "points.xlsx"},
                "workflow": {"sampling": {"quality": {"enabled": False}}},
                "coefficients": {"summary_columns": {"co2": {"pressure": "BAR"}}},
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    cfg = load_config(cfg_path)

    assert cfg["workflow"]["selected_pressure_points"] == []
    assert cfg["workflow"]["summary_alignment"]["reference_on_aligned_rows"] is True
    assert cfg["workflow"]["sampling"]["quality"]["per_analyzer"] is False
    assert cfg["workflow"]["analyzer_frame_quality"]["pressure_kpa_min"] == 30.0
    assert cfg["workflow"]["analyzer_frame_quality"]["runtime_relaxed_for_required_key"] is True
    assert cfg["workflow"]["analyzer_frame_quality"]["runtime_hard_bad_status_tokens"] == [
        "FAIL",
        "INVALID",
        "ERROR",
    ]
    assert cfg["workflow"]["analyzer_frame_quality"]["runtime_soft_bad_status_tokens"] == [
        "NO_RESPONSE",
        "NO_ACK",
    ]
    assert cfg["workflow"]["analyzer_frame_quality"]["strict_required_keys"] == [
        "co2_ratio_f",
        "h2o_ratio_f",
        "co2_ppm",
        "h2o_mmol",
    ]
    assert cfg["workflow"]["analyzer_frame_quality"]["relaxed_required_keys"] == [
        "chamber_temp_c",
        "case_temp_c",
        "temp_c",
    ]
    assert cfg["workflow"]["analyzer_frame_quality"]["reject_log_window_s"] == 15.0
    assert cfg["workflow"]["analyzer_live_snapshot"]["enabled"] is True
    assert cfg["workflow"]["analyzer_live_snapshot"]["interval_s"] == 5.0
    assert cfg["workflow"]["analyzer_live_snapshot"]["cache_ttl_s"] == 2.5
    assert cfg["workflow"]["analyzer_live_snapshot"]["sampling_worker_enabled"] is True
    assert cfg["workflow"]["analyzer_live_snapshot"]["sampling_worker_interval_s"] == 0.2
    assert cfg["workflow"]["analyzer_live_snapshot"]["passive_round_robin_enabled"] is True
    assert cfg["workflow"]["analyzer_live_snapshot"]["passive_round_robin_interval_s"] == 0.25
    assert cfg["workflow"]["analyzer_live_snapshot"]["active_ring_buffer_size"] == 128
    assert cfg["workflow"]["analyzer_live_snapshot"]["active_frame_max_anchor_delta_ms"] == 250.0
    assert cfg["workflow"]["analyzer_live_snapshot"]["active_frame_right_match_max_ms"] == 120.0
    assert cfg["workflow"]["analyzer_live_snapshot"]["active_frame_stale_ms"] == 500.0
    assert cfg["workflow"]["analyzer_live_snapshot"]["active_drain_poll_s"] == 0.05
    assert cfg["workflow"]["analyzer_live_snapshot"]["anchor_match_enabled"] is True
    assert cfg["workflow"]["sampling"]["interval_s"] == 1.0
    assert cfg["workflow"]["sampling"]["co2_interval_s"] == 1.0
    assert cfg["workflow"]["sampling"]["h2o_interval_s"] == 1.0
    assert cfg["workflow"]["sampling"]["fixed_rate_enabled"] is True
    assert cfg["workflow"]["sampling"]["fast_sync_warn_span_ms"] == 1000.0
    assert cfg["workflow"]["sampling"]["fast_signal_worker_enabled"] is True
    assert cfg["workflow"]["sampling"]["fast_signal_worker_interval_s"] == 0.1
    assert cfg["workflow"]["sampling"]["fast_signal_ring_buffer_size"] == 128
    assert cfg["workflow"]["sampling"]["pressure_gauge_continuous_enabled"] is True
    assert cfg["workflow"]["sampling"]["pressure_gauge_continuous_mode"] == "P4"
    assert cfg["workflow"]["sampling"]["pressure_gauge_continuous_drain_s"] == 0.12
    assert cfg["workflow"]["sampling"]["pressure_gauge_continuous_read_timeout_s"] == 0.02
    assert cfg["workflow"]["sampling"]["pressure_gauge_stale_ratio_warn_max"] is None
    assert cfg["workflow"]["sampling"]["pressure_gauge_stale_ratio_reject_max"] is None
    assert cfg["workflow"]["sampling"]["slow_aux_cache_enabled"] is True
    assert cfg["workflow"]["sampling"]["slow_aux_cache_interval_s"] == 5.0
    assert cfg["workflow"]["sampling"]["pace_state_every_n_samples"] == 0
    assert cfg["workflow"]["sampling"]["pace_state_cache_enabled"] is True
    assert cfg["workflow"]["reporting"]["include_fleet_stats"] is False
    assert cfg["workflow"]["reporting"]["defer_heavy_exports_during_handoff"] is True
    assert cfg["workflow"]["reporting"]["flush_deferred_exports_on_next_route_soak"] is True
    assert cfg["workflow"]["relay"]["bulk_write_enabled"] is True
    assert cfg["workflow"]["humidity_generator"]["safe_stop_verify_flow"] is True
    assert cfg["workflow"]["humidity_generator"]["safe_stop_enforce_flow_check"] is True
    assert cfg["workflow"]["humidity_generator"]["safe_stop_max_flow_lpm"] == 0.05
    assert cfg["workflow"]["humidity_generator"]["safe_stop_timeout_s"] == 15.0
    assert cfg["workflow"]["humidity_generator"]["safe_stop_poll_s"] == 0.5
    assert cfg["workflow"]["humidity_generator"]["activation_verify_enabled"] is True
    assert cfg["workflow"]["humidity_generator"]["activation_verify_min_flow_lpm"] == 0.5
    assert cfg["workflow"]["humidity_generator"]["activation_verify_timeout_s"] == 30.0
    assert cfg["workflow"]["humidity_generator"]["activation_verify_poll_s"] == 1.0
    assert cfg["workflow"]["humidity_generator"]["activation_verify_expect_cooling_margin_c"] == 1.0
    assert cfg["workflow"]["humidity_generator"]["activation_verify_cooling_min_drop_c"] == 0.2
    assert cfg["workflow"]["humidity_generator"]["activation_verify_cooling_min_delta_c"] == 0.5
    assert cfg["workflow"]["safe_stop"]["perform_attempts"] == 4
    assert cfg["workflow"]["safe_stop"]["retry_delay_s"] == 2.0
    assert cfg["workflow"]["pressure"]["capture_then_hold_enabled"] is True
    assert cfg["workflow"]["pressure"]["disable_output_during_sampling"] is True
    assert cfg["workflow"]["pressure"]["atmosphere_hold_strategy"] == "legacy_hold_thread"
    assert cfg["workflow"]["pressure"]["continuous_atmosphere_hold"] is True
    assert cfg["workflow"]["pressure"]["vent_after_valve_open"] is False
    assert cfg["workflow"]["pressure"]["vent_popup_ack_disable_for_automation"] is False
    assert cfg["workflow"]["pressure"]["co2_preseal_pressure_gauge_trigger_hpa"] == 1110.0
    assert cfg["workflow"]["pressure"]["h2o_preseal_pressure_gauge_trigger_hpa"] == 1110.0
    assert cfg["workflow"]["pressure"]["preseal_timeout_requires_invalid_gauge"] is True
    assert cfg["workflow"]["pressure"]["preseal_valid_gauge_stall_window_s"] == 20.0
    assert cfg["workflow"]["pressure"]["preseal_valid_gauge_min_rise_hpa"] == 0.5
    assert cfg["workflow"]["pressure"]["transition_pressure_gauge_continuous_enabled"] is True
    assert cfg["workflow"]["pressure"]["transition_pressure_gauge_continuous_mode"] == "P4"
    assert cfg["workflow"]["pressure"]["transition_pressure_gauge_continuous_drain_s"] == 0.12
    assert cfg["workflow"]["pressure"]["transition_pressure_gauge_continuous_read_timeout_s"] == 0.02
    assert cfg["workflow"]["pressure"]["post_stable_sample_delay_s"] == 10.0
    assert cfg["workflow"]["pressure"]["co2_post_stable_sample_delay_s"] == 10.0
    assert cfg["workflow"]["pressure"]["transition_trace_enabled"] is True
    assert cfg["workflow"]["pressure"]["transition_trace_poll_s"] == 0.5
    assert cfg["workflow"]["pressure"]["handoff_fast_enabled"] is False
    assert cfg["workflow"]["pressure"]["handoff_safe_open_delta_hpa"] == 3.0
    assert cfg["workflow"]["pressure"]["handoff_use_pressure_gauge"] is True
    assert cfg["workflow"]["pressure"]["handoff_require_vent_completed"] is False
    assert cfg["workflow"]["pressure"]["fast_gauge_response_timeout_s"] == 0.6
    assert cfg["workflow"]["pressure"]["transition_gauge_response_timeout_s"] == 1.5
    assert cfg["workflow"]["pressure"]["fast_gauge_read_retries"] == 1
    assert cfg["workflow"]["stability"]["gas_route_dewpoint_gate_enabled"] is True
    assert cfg["workflow"]["stability"]["gas_route_dewpoint_gate_policy"] == "warn"
    assert cfg["workflow"]["stability"]["water_route_dewpoint_gate_enabled"] is True
    assert cfg["workflow"]["stability"]["water_route_dewpoint_gate_policy"] == "warn"
    assert cfg["workflow"]["stability"]["gas_route_dewpoint_gate_window_s"] == 60.0
    assert cfg["workflow"]["stability"]["water_route_dewpoint_gate_window_s"] == 60.0
    assert cfg["workflow"]["stability"]["gas_route_dewpoint_gate_max_total_wait_s"] == 1080.0
    assert cfg["workflow"]["stability"]["water_route_dewpoint_gate_max_total_wait_s"] == 1080.0
    assert cfg["workflow"]["stability"]["gas_route_dewpoint_gate_poll_s"] == 2.0
    assert cfg["workflow"]["stability"]["water_route_dewpoint_gate_poll_s"] == 2.0
    assert cfg["workflow"]["stability"]["gas_route_dewpoint_gate_tail_span_max_c"] == 0.45
    assert cfg["workflow"]["stability"]["water_route_dewpoint_gate_tail_span_max_c"] == 0.45
    assert cfg["workflow"]["stability"]["gas_route_dewpoint_gate_tail_slope_abs_max_c_per_s"] == 0.005
    assert cfg["workflow"]["stability"]["water_route_dewpoint_gate_tail_slope_abs_max_c_per_s"] == 0.005
    assert cfg["workflow"]["stability"]["gas_route_dewpoint_gate_rebound_window_s"] == 180.0
    assert cfg["workflow"]["stability"]["water_route_dewpoint_gate_rebound_window_s"] == 180.0
    assert cfg["workflow"]["stability"]["gas_route_dewpoint_gate_rebound_min_rise_c"] == 1.3
    assert cfg["workflow"]["stability"]["water_route_dewpoint_gate_rebound_min_rise_c"] == 1.3
    assert cfg["workflow"]["stability"]["gas_route_dewpoint_gate_log_interval_s"] == 15.0
    assert cfg["workflow"]["stability"]["water_route_dewpoint_gate_log_interval_s"] == 15.0
    assert cfg["workflow"]["stability"]["co2_cold_quality_gate"]["enabled"] is True
    assert cfg["workflow"]["stability"]["co2_cold_quality_gate"]["policy"] == "warn"
    assert cfg["workflow"]["stability"]["co2_cold_quality_gate"]["apply_temp_max_c"] == 0.0
    assert cfg["workflow"]["stability"]["co2_cold_quality_gate"]["raw_temp_min_c"] == -30.0
    assert cfg["workflow"]["stability"]["co2_cold_quality_gate"]["raw_temp_max_c"] == 85.0
    assert cfg["workflow"]["stability"]["co2_cold_quality_gate"]["max_abs_delta_from_ref_c"] == 20.0
    assert cfg["workflow"]["stability"]["co2_cold_quality_gate"]["max_cell_shell_gap_c"] == 15.0
    assert cfg["workflow"]["stability"]["co2_cold_quality_gate"]["hard_bad_values_c"] == [-40.0, 60.0]
    assert cfg["workflow"]["stability"]["co2_cold_quality_gate"]["hard_bad_value_tolerance_c"] == 0.05
    assert cfg["workflow"]["stability"]["dewpoint"]["enabled"] is True
    assert cfg["workflow"]["stability"]["dewpoint"]["rh_match_tol_pct"] == 3.3
    assert cfg["workflow"]["postrun_corrected_delivery"]["enabled"] is False
    assert cfg["workflow"]["postrun_corrected_delivery"]["write_devices"] is False
    assert cfg["workflow"]["postrun_corrected_delivery"]["write_pressure_coefficients"] is True
    assert cfg["workflow"]["postrun_corrected_delivery"]["pressure_row_source"] == "startup_calibration"
    assert cfg["workflow"]["postrun_corrected_delivery"]["run_structure_hints"]["enabled"] is True
    assert cfg["workflow"]["postrun_corrected_delivery"]["verify_short_run"]["enabled"] is True
    assert cfg["workflow"]["postrun_corrected_delivery"]["verify_short_run"]["temp_c"] == 20.0
    assert cfg["workflow"]["postrun_corrected_delivery"]["verify_short_run"]["skip_co2_ppm"] == []
    assert cfg["workflow"]["postrun_corrected_delivery"]["verify_short_run"]["enable_connect_check"] is False
    assert cfg["workflow"]["postrun_corrected_delivery"]["verify_short_run"]["points_excel"] == "configs/points_tiny_short_run_20c_even500.xlsx"
    assert cfg["temperature_calibration"]["plausibility"]["enabled"] is True
    assert cfg["temperature_calibration"]["plausibility"]["raw_temp_min_c"] == -30.0
    assert cfg["temperature_calibration"]["plausibility"]["raw_temp_max_c"] == 85.0
    assert cfg["temperature_calibration"]["plausibility"]["max_abs_delta_from_ref_c"] == 15.0
    assert cfg["temperature_calibration"]["plausibility"]["max_cell_shell_gap_c"] == 12.0
    assert cfg["temperature_calibration"]["plausibility"]["hard_bad_values_c"] == [-40.0, 60.0]
    assert cfg["temperature_calibration"]["plausibility"]["hard_bad_value_tolerance_c"] == 0.05
    assert cfg["coefficients"]["h2o_summary_selection"]["include_co2_temp_groups_c"] == []
    assert cfg["coefficients"]["h2o_summary_selection"]["include_co2_zero_ppm_temp_groups_c"] == [-20.0, -10.0, 0.0]
    assert cfg["coefficients"]["h2o_summary_selection"]["co2_zero_ppm_anchor_quality_gate_enabled"] is True
    assert cfg["coefficients"]["h2o_summary_selection"]["co2_zero_ppm_anchor_require_h2o_dew"] is True
    assert cfg["coefficients"]["h2o_summary_selection"]["co2_zero_ppm_anchor_max_ppm_h2o_dew_default"] == 0.5
    assert cfg["coefficients"]["h2o_summary_selection"]["co2_zero_ppm_anchor_max_ppm_h2o_dew_by_temp_c"] == {
        "-20": 0.2,
        "-10": 0.05,
        "0": 0.5,
    }
    assert cfg["workflow"]["pressure"]["strict_control_ready_check"] is True
    assert cfg["workflow"]["pressure"]["abort_on_vent_off_failure"] is True
    assert cfg["workflow"]["pressure"]["output_off_prefer_gauge"] is True
    assert cfg["workflow"]["pressure"]["output_off_sample_interval_s"] == 0.5
    assert cfg["workflow"]["pressure"]["output_off_retry_count"] == 1
    assert cfg["workflow"]["pressure"]["co2_output_off_hold_s"] == 6.0
    assert cfg["workflow"]["pressure"]["h2o_output_off_hold_s"] == 10.0
    assert cfg["workflow"]["pressure"]["co2_output_off_max_abs_drift_hpa"] == 0.25
    assert cfg["workflow"]["pressure"]["h2o_output_off_max_abs_drift_hpa"] == 0.40
    assert cfg["workflow"]["pressure"]["co2_post_isolation_diagnostic_enabled"] is True
    assert cfg["workflow"]["pressure"]["co2_post_isolation_window_s"] == 10.0
    assert cfg["workflow"]["pressure"]["co2_post_isolation_poll_s"] == 0.5
    assert cfg["workflow"]["pressure"]["co2_post_isolation_pressure_drift_hpa"] == 0.35
    assert cfg["workflow"]["pressure"]["co2_post_isolation_pressure_stable_span_hpa"] == 0.20
    assert cfg["workflow"]["pressure"]["co2_post_isolation_dewpoint_rise_c"] == 0.12
    assert cfg["workflow"]["pressure"]["co2_post_isolation_dewpoint_slope_c_per_s"] == 0.01
    assert cfg["workflow"]["pressure"]["co2_post_isolation_ambient_recovery_min_hpa"] == 0.20
    assert cfg["workflow"]["pressure"]["postseal_same_gas_dead_volume_purge_enabled"] is False
    assert cfg["workflow"]["pressure"]["post_isolation_fast_capture_enabled"] is True
    assert cfg["workflow"]["pressure"]["post_isolation_fast_capture_allow_early_sample"] is False
    assert cfg["workflow"]["pressure"]["post_isolation_fast_capture_min_s"] == 5.0
    assert cfg["workflow"]["pressure"]["post_isolation_fast_capture_require_vent_zero"] is True
    assert cfg["workflow"]["pressure"]["post_isolation_fast_capture_require_isol_closed"] is True
    assert cfg["workflow"]["pressure"]["post_isolation_fast_capture_fallback_to_extended_diag"] is True
    assert cfg["workflow"]["pressure"]["post_isolation_extended_diag_window_s"] == 20.0
    assert cfg["workflow"]["pressure"]["fast_capture_pressure_drift_max_hpa"] == 0.18
    assert cfg["workflow"]["pressure"]["fast_capture_pressure_slope_max_hpa_s"] == 0.05
    assert cfg["workflow"]["pressure"]["fast_capture_dewpoint_rise_max_c"] == 0.06
    assert cfg["workflow"]["pressure"]["adaptive_pressure_sampling_enabled"] is True
    assert cfg["workflow"]["pressure"]["use_pressure_gauge_for_sampling_gate"] is True
    assert cfg["workflow"]["pressure"]["sampling_gate_poll_s"] == 0.5
    assert cfg["workflow"]["pressure"]["co2_sampling_gate_window_s"] == 8.0
    assert cfg["workflow"]["pressure"]["h2o_sampling_gate_window_s"] == 12.0
    assert cfg["workflow"]["pressure"]["co2_sampling_gate_pressure_span_hpa"] == 0.20
    assert cfg["workflow"]["pressure"]["h2o_sampling_gate_pressure_span_hpa"] == 0.30
    assert cfg["workflow"]["pressure"]["co2_sampling_gate_pressure_fill_s"] == 5.0
    assert cfg["workflow"]["pressure"]["h2o_sampling_gate_pressure_fill_s"] == 8.0
    assert cfg["workflow"]["pressure"]["co2_sampling_gate_min_samples"] == 6
    assert cfg["workflow"]["pressure"]["h2o_sampling_gate_min_samples"] == 8
    assert cfg["workflow"]["pressure"]["co2_postseal_dewpoint_window_s"] == 4.0
    assert cfg["workflow"]["pressure"]["co2_postseal_dewpoint_timeout_s"] == 6.0
    assert cfg["workflow"]["pressure"]["co2_postseal_dewpoint_span_c"] == 0.12
    assert cfg["workflow"]["pressure"]["co2_postseal_dewpoint_slope_c_per_s"] == 0.04
    assert cfg["workflow"]["pressure"]["co2_postseal_dewpoint_min_samples"] == 6
    assert cfg["workflow"]["pressure"]["co2_postseal_rebound_guard_enabled"] is True
    assert cfg["workflow"]["pressure"]["co2_postseal_rebound_window_s"] == 8.0
    assert cfg["workflow"]["pressure"]["co2_postseal_rebound_min_rise_c"] == 0.12
    assert cfg["workflow"]["pressure"]["superambient_precharge_enabled"] is True
    assert cfg["workflow"]["pressure"]["superambient_trigger_margin_hpa"] == 5.0
    assert cfg["workflow"]["pressure"]["superambient_precharge_margin_hpa"] == 8.0
    assert cfg["workflow"]["pressure"]["superambient_precharge_timeout_s"] == 30.0
    assert cfg["workflow"]["pressure"]["superambient_precharge_same_gas_only"] is True
    assert cfg["workflow"]["pressure"]["superambient_reject_without_closed_path"] is True
    assert cfg["workflow"]["pressure"]["superambient_forbid_atmosphere_fallback"] is True
    assert cfg["workflow"]["pressure"]["co2_postseal_physical_qc_enabled"] is True
    assert cfg["workflow"]["pressure"]["co2_postseal_physical_qc_max_abs_delta_c"] == 1.0
    assert cfg["workflow"]["pressure"]["co2_postseal_physical_qc_policy"] == "warn"
    assert cfg["workflow"]["pressure"]["co2_postseal_timeout_policy"] == "pass"
    assert cfg["workflow"]["pressure"]["co2_presample_long_guard_enabled"] is True
    assert cfg["workflow"]["pressure"]["co2_presample_long_guard_window_s"] == 20.0
    assert cfg["workflow"]["pressure"]["co2_presample_long_guard_timeout_s"] == 90.0
    assert cfg["workflow"]["pressure"]["co2_presample_long_guard_max_span_c"] == 1.2
    assert cfg["workflow"]["pressure"]["co2_presample_long_guard_max_abs_slope_c_per_s"] == 0.08
    assert cfg["workflow"]["pressure"]["co2_presample_long_guard_max_rise_c"] == 1.0
    assert cfg["workflow"]["pressure"]["co2_presample_long_guard_policy"] == "warn"
    assert cfg["workflow"]["pressure"]["co2_postsample_late_rebound_guard_enabled"] is True
    assert cfg["workflow"]["pressure"]["co2_postsample_late_rebound_max_rise_c"] == 0.12
    assert cfg["workflow"]["pressure"]["co2_postsample_late_rebound_policy"] == "warn"
    assert cfg["workflow"]["pressure"]["co2_sampling_window_qc_enabled"] is True
    assert cfg["workflow"]["pressure"]["co2_sampling_window_qc_max_range_c"] == 0.20
    assert cfg["workflow"]["pressure"]["co2_sampling_window_qc_max_rise_c"] == 0.12
    assert cfg["workflow"]["pressure"]["co2_sampling_window_qc_max_abs_slope_c_per_s"] == 0.02
    assert cfg["workflow"]["pressure"]["co2_sampling_window_qc_policy"] == "warn"
    assert cfg["workflow"]["pressure"]["co2_no_topoff_vent_off_open_wait_s"] == 2.0
    assert cfg["workflow"]["pressure"]["h2o_postseal_dewpoint_window_s"] == 2.5
    assert cfg["workflow"]["pressure"]["h2o_postseal_dewpoint_timeout_s"] == 5.5
    assert cfg["workflow"]["pressure"]["h2o_postseal_dewpoint_span_c"] == 0.18
    assert cfg["workflow"]["pressure"]["h2o_postseal_dewpoint_slope_c_per_s"] == 0.06
    assert cfg["workflow"]["pressure"]["h2o_postseal_dewpoint_min_samples"] == 4
    assert cfg["workflow"]["pressure"]["preseal_trigger_overshoot_warn_hpa"] == 10.0
    assert cfg["workflow"]["pressure"]["preseal_trigger_overshoot_reject_hpa"] is None
    assert cfg["workflow"]["pressure"]["skip_fixed_post_stable_delay_when_adaptive"] is True
    assert cfg["workflow"]["pressure"]["soft_control_enabled"] is False
    assert cfg["workflow"]["pressure"]["soft_control_use_active_mode"] is True
    assert cfg["workflow"]["pressure"]["soft_control_linear_slew_hpa_per_s"] == 10.0
    assert cfg["workflow"]["pressure"]["soft_control_disallow_overshoot"] is True
    assert cfg["coefficients"]["ratio_poly_fit"]["pressure_source_preference"] == "reference_first"
    assert cfg["validation"]["offline"]["mode"] == "both"
    assert cfg["validation"]["dry_collect"]["write_coefficients"] is False
    assert cfg["validation"]["coefficient_roundtrip"]["allow_write_modified"] is False
    assert Path(cfg["paths"]["output_dir"]).is_absolute()


def test_analyzer_chain_isolation_4ch_enables_focused_quality_guards() -> None:
    root = Path(__file__).resolve().parents[1]
    cfg = load_config(root / "configs" / "analyzer_chain_isolation_4ch.json")

    assert cfg["workflow"]["stability"]["gas_route_dewpoint_gate_enabled"] is True
    assert cfg["workflow"]["sampling"]["pressure_gauge_continuous_enabled"] is True
    assert cfg["workflow"]["sampling"]["pressure_gauge_continuous_mode"] == "P4"
    assert cfg["workflow"]["sampling"]["pressure_gauge_stale_ratio_warn_max"] == 0.2
    assert cfg["workflow"]["sampling"]["pressure_gauge_stale_ratio_reject_max"] == 0.5
    assert cfg["workflow"]["pressure"]["co2_postseal_rebound_guard_enabled"] is True
    assert cfg["workflow"]["pressure"]["co2_postseal_physical_qc_enabled"] is True
    assert cfg["workflow"]["pressure"]["co2_postseal_physical_qc_policy"] == "warn"
    assert cfg["workflow"]["pressure"]["co2_postseal_timeout_policy"] == "warn"
    assert cfg["workflow"]["pressure"]["co2_presample_long_guard_enabled"] is True
    assert cfg["workflow"]["pressure"]["co2_presample_long_guard_policy"] == "warn"
    assert cfg["workflow"]["pressure"]["co2_sampling_window_qc_enabled"] is True
    assert cfg["workflow"]["pressure"]["co2_sampling_window_qc_policy"] == "warn"
    assert cfg["workflow"]["pressure"]["co2_postsample_late_rebound_guard_enabled"] is True
    assert cfg["workflow"]["pressure"]["co2_postsample_late_rebound_policy"] == "warn"
    assert cfg["workflow"]["pressure"]["preseal_trigger_overshoot_warn_hpa"] == 10.0
    assert cfg["workflow"]["pressure"]["preseal_trigger_overshoot_reject_hpa"] == 25.0
    assert cfg["workflow"]["stability"]["co2_route"]["first_point_preseal_soak_s"] == 180
    assert cfg["workflow"]["stability"]["co2_route"]["post_h2o_zero_ppm_soak_s"] == 900
    assert cfg["workflow"]["stability"]["temperature"]["analyzer_chamber_temp_span_c"] == 0.08
    assert cfg["valves"]["co2_path_group2"] == 16
    assert cfg["valves"]["co2_map_group2"]["500"] == 24


def test_default_config_shortens_h2o_preseal_soak_to_30s() -> None:
    root = Path(__file__).resolve().parents[1]
    cfg = load_config(root / "configs" / "default_config.json")

    assert cfg["workflow"]["stability"]["h2o_route"]["preseal_soak_s"] == 30
    assert cfg["workflow"]["stability"]["sensor"]["h2o_ratio_f_preseal_policy"] == "warn"
    assert cfg["workflow"]["stability"]["sensor"]["h2o_ratio_f_preseal_window_s"] == 60
    assert cfg["workflow"]["stability"]["sensor"]["h2o_ratio_f_preseal_timeout_s"] == 300
    assert cfg["workflow"]["stability"]["sensor"]["h2o_ratio_f_preseal_min_samples"] == 10
    assert cfg["workflow"]["stability"]["sensor"]["h2o_ratio_f_preseal_read_interval_s"] == 1.0
    assert cfg["workflow"]["stability"]["co2_route"]["preseal_soak_s"] == 180
    assert cfg["workflow"]["stability"]["co2_route"]["first_point_preseal_soak_s"] == 180
    assert cfg["workflow"]["stability"]["co2_route"]["post_h2o_zero_ppm_soak_s"] == 900
    assert cfg["workflow"]["stability"]["dewpoint"]["rh_match_tol_pct"] == 3.3
    assert cfg["workflow"]["stability"]["temperature"]["analyzer_chamber_temp_span_c"] == 0.08
    assert cfg["workflow"]["stability"]["gas_route_dewpoint_gate_max_total_wait_s"] == 1080.0
    assert cfg["workflow"]["pressure"]["transition_pressure_gauge_continuous_enabled"] is True
    assert cfg["workflow"]["sampling"]["pressure_gauge_continuous_enabled"] is True
    assert cfg["workflow"]["pressure"]["preseal_trigger_overshoot_warn_hpa"] == 10.0
