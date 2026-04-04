import json
from pathlib import Path

from gas_calibrator.v2.config import AppConfig
from gas_calibrator.v2.config.models import (
    TemperatureStabilityConfig,
    build_step2_config_safety_review,
    summarize_step2_config_safety,
)
from gas_calibrator.v2.domain.pressure_selection import AMBIENT_PRESSURE_TOKEN


def test_temperature_stability_config_supports_synced_fields() -> None:
    config = TemperatureStabilityConfig.from_dict(
        {
            "wait_after_reach_s": 8.0,
            "wait_for_target_before_continue": False,
            "restart_on_target_change": True,
            "reuse_running_in_tol_without_soak": False,
            "precondition_next_group_enabled": True,
            "transition_check_window_s": 45.0,
            "transition_min_delta_c": 0.6,
            "command_offset_c": 1.5,
            "analyzer_chamber_temp_enabled": False,
            "analyzer_chamber_temp_window_s": 12.5,
            "analyzer_chamber_temp_span_c": 0.05,
            "analyzer_chamber_temp_target_tol_c": 0.3,
            "analyzer_chamber_temp_timeout_s": 123.0,
            "analyzer_chamber_temp_first_valid_timeout_s": 9.0,
            "analyzer_chamber_temp_poll_s": 0.2,
        }
    )

    assert config.soak_after_reach_s == 8.0
    assert config.wait_after_reach_s == 8.0
    assert config.wait_for_target_before_continue is False
    assert config.restart_on_target_change is True
    assert config.reuse_running_in_tol_without_soak is False
    assert config.precondition_next_group_enabled is True
    assert config.transition_check_window_s == 45.0
    assert config.transition_min_delta_c == 0.6
    assert config.command_offset_c == 1.5
    assert config.analyzer_chamber_temp_enabled is False
    assert config.analyzer_chamber_temp_window_s == 12.5
    assert config.analyzer_chamber_temp_span_c == 0.05
    assert config.analyzer_chamber_temp_target_tol_c == 0.3
    assert config.analyzer_chamber_temp_timeout_s == 123.0
    assert config.analyzer_chamber_temp_first_valid_timeout_s == 9.0
    assert config.analyzer_chamber_temp_poll_s == 0.2


def test_app_config_supports_legacy_humidity_generator_precondition_key() -> None:
    config = AppConfig.from_dict(
        {
            "workflow": {
                "stability": {
                    "humidity_generator": {
                        "precondition_next_group_enabled": False,
                    }
                }
            }
        }
    )

    assert config.workflow.stability.humidity.precondition_next_group_enabled is False


def test_app_config_normalizes_sensor_precheck_v1_compatible_mode_to_scope() -> None:
    config = AppConfig.from_dict(
        {
            "workflow": {
                "sensor_precheck": {
                    "enabled": True,
                    "mode": "v1_compatible",
                }
            }
        }
    )

    assert config.workflow.sensor_precheck["mode"] == "v1_compatible"
    assert config.workflow.sensor_precheck["profile"] == "mode2_like"
    assert config.workflow.sensor_precheck["scope"] == "first_analyzer_only"
    assert config.workflow.sensor_precheck["validation_mode"] == "v1_mode2_like"


def test_app_config_normalizes_sensor_precheck_validation_mode() -> None:
    config = AppConfig.from_dict(
        {
            "workflow": {
                "sensor_precheck": {
                    "enabled": True,
                    "validation_mode": "v1_mode2_like",
                }
            }
        }
    )

    assert config.workflow.sensor_precheck["profile"] == "mode2_like"
    assert config.workflow.sensor_precheck["validation_mode"] == "v1_mode2_like"
    assert config.workflow.sensor_precheck["scope"] == "first_analyzer_only"

    config2 = AppConfig.from_dict(
        {
            "workflow": {
                "sensor_precheck": {
                    "enabled": True,
                    "validation_mode": "snapshot",
                }
            }
        }
    )

    assert config2.workflow.sensor_precheck["profile"] == "snapshot"
    assert config2.workflow.sensor_precheck["validation_mode"] == "snapshot"


def test_app_config_normalizes_sensor_precheck_validation_mode_v1_frame_like() -> None:
    config = AppConfig.from_dict(
        {
            "workflow": {
                "sensor_precheck": {
                    "enabled": True,
                    "validation_mode": "v1_frame_like",
                }
            }
        }
    )

    assert config.workflow.sensor_precheck["profile"] == "raw_frame_first"
    assert config.workflow.sensor_precheck["validation_mode"] == "v1_frame_like"
    assert config.workflow.sensor_precheck["scope"] == "first_analyzer_only"


def test_app_config_normalizes_sensor_precheck_explicit_profile_aliases() -> None:
    config = AppConfig.from_dict(
        {
            "workflow": {
                "sensor_precheck": {
                    "enabled": True,
                    "profile": "mode2_like",
                }
            }
        }
    )
    config2 = AppConfig.from_dict(
        {
            "workflow": {
                "sensor_precheck": {
                    "enabled": True,
                    "profile": "raw_frame_first",
                }
            }
        }
    )

    assert config.workflow.sensor_precheck["profile"] == "mode2_like"
    assert config.workflow.sensor_precheck["validation_mode"] == "v1_mode2_like"
    assert config.workflow.sensor_precheck["scope"] == "first_analyzer_only"
    assert config2.workflow.sensor_precheck["profile"] == "raw_frame_first"
    assert config2.workflow.sensor_precheck["validation_mode"] == "v1_frame_like"
    assert config2.workflow.sensor_precheck["scope"] == "first_analyzer_only"


def test_app_config_normalizes_run_mode_aliases() -> None:
    config = AppConfig.from_dict({"workflow": {"run_mode": "co2"}})
    config2 = AppConfig.from_dict({"workflow": {"run_mode": "water_measurement"}})
    config3 = AppConfig.from_dict({"workflow": {}})

    assert config.workflow.run_mode == "co2_measurement"
    assert config2.workflow.run_mode == "h2o_measurement"
    assert config3.workflow.run_mode == "auto_calibration"


def test_app_config_preserves_analyzer_mode2_init_control_parameters() -> None:
    config = AppConfig.from_dict(
        {
            "workflow": {
                "analyzer_mode2_init": {
                    "reapply_attempts": 2,
                    "stream_attempts": 5,
                    "passive_attempts": 3,
                    "retry_delay_s": 0.1,
                    "reapply_delay_s": 0.25,
                    "command_gap_s": 0.05,
                    "post_enable_stream_wait_s": 1.5,
                    "post_enable_stream_ack_wait_s": 4.0,
                }
            }
        }
    )

    payload = config.workflow.analyzer_mode2_init
    assert payload["enabled"] is True
    assert payload["reapply_attempts"] == 2
    assert payload["stream_attempts"] == 5
    assert payload["passive_attempts"] == 3
    assert payload["retry_delay_s"] == 0.1
    assert payload["reapply_delay_s"] == 0.25
    assert payload["command_gap_s"] == 0.05
    assert payload["post_enable_stream_wait_s"] == 1.5
    assert payload["post_enable_stream_ack_wait_s"] == 4.0


def test_app_config_preserves_live_snapshot_and_co2_route_sync_fields() -> None:
    config = AppConfig.from_dict(
        {
            "workflow": {
                "analyzer_live_snapshot": {"interval_s": 2.5},
                "stability": {
                    "co2_route": {
                        "preseal_soak_s": 180.0,
                        "first_point_preseal_soak_s": 300.0,
                    }
                },
            }
        }
    )

    assert config.workflow.analyzer_live_snapshot["interval_s"] == 2.5
    assert config.workflow.stability.co2_route["preseal_soak_s"] == 180.0
    assert config.workflow.stability.co2_route["first_point_preseal_soak_s"] == 300.0


def test_app_config_normalizes_selected_pressure_points_with_ambient_aliases() -> None:
    config = AppConfig.from_dict(
        {
            "workflow": {
                "selected_pressure_points": [
                    "ambient_open",
                    "ambient",
                    900.0,
                    "900.0",
                ]
            }
        }
    )

    assert config.workflow.selected_pressure_points == [AMBIENT_PRESSURE_TOKEN, 900.0]


def test_app_config_supports_spectral_quality_feature_fields() -> None:
    config = AppConfig.from_dict(
        {
            "features": {
                "enable_spectral_quality_analysis": True,
                "spectral_min_samples": 96,
                "spectral_min_duration_s": 45.0,
                "spectral_low_freq_max_hz": 0.02,
            }
        }
    )

    assert config.features.enable_spectral_quality_analysis is True
    assert config.features.spectral_min_samples == 96
    assert config.features.spectral_min_duration_s == 45.0
    assert config.features.spectral_low_freq_max_hz == 0.02


def test_app_config_normalizes_analyzer_setup_profile_fields() -> None:
    config = AppConfig.from_dict(
        {
            "workflow": {
                "analyzer_setup": {
                    "analyzer_version": "legacy",
                    "id_assignment_mode": "manual",
                    "starting_device_id": "7",
                    "manual_ids": ["8", "009"],
                }
            }
        }
    )

    assert config.workflow.analyzer_setup["software_version"] == "pre_v5"
    assert config.workflow.analyzer_setup["device_id_assignment_mode"] == "manual"
    assert config.workflow.analyzer_setup["start_device_id"] == "007"
    assert config.workflow.analyzer_setup["manual_device_ids"] == ["008", "009"]


def test_smoke_v2_minimal_contains_temperature_sync_fields() -> None:
    smoke_path = (
        Path(__file__).resolve().parents[2]
        / "src"
        / "gas_calibrator"
        / "v2"
        / "configs"
        / "smoke_v2_minimal.json"
    )
    payload = json.loads(smoke_path.read_text(encoding="utf-8"))
    temperature = payload["workflow"]["stability"]["temperature"]

    assert temperature["precondition_next_group_enabled"] is False
    assert temperature["command_offset_c"] == 0.0
    assert temperature["analyzer_chamber_temp_enabled"] is False
    assert temperature["analyzer_chamber_temp_window_s"] == 0.5
    assert temperature["analyzer_chamber_temp_span_c"] == 2.0
    assert temperature["analyzer_chamber_temp_timeout_s"] == 2.0
    assert temperature["analyzer_chamber_temp_first_valid_timeout_s"] == 0.5
    assert temperature["analyzer_chamber_temp_poll_s"] == 0.1


def test_step2_safe_v2_configs_use_sim_ports_only() -> None:
    config_root = Path(__file__).resolve().parents[2] / "src" / "gas_calibrator" / "v2" / "configs"
    for name in ("smoke_v2_minimal.json", "fit_ready_smoke.json", "test_v2_safe.json"):
        payload = json.loads((config_root / name).read_text(encoding="utf-8"))
        summary = summarize_step2_config_safety(AppConfig.from_dict(payload))

        assert summary["simulation_only"] is True
        assert summary["real_port_device_count"] == 0
        assert summary["requires_explicit_unlock"] is False
        assert summary["step2_default_workflow_allowed"] is True
        assert summary["execution_gate"]["status"] == "open"
        assert summary["classification"] == "operator_safe_simulation_only"
        assert "simulation_only" in summary["badge_ids"]
        assert "operator_safe" in summary["badge_ids"]
        assert summary["inventory"]["real_port_device_count"] == 0
        assert summary["inventory"]["engineering_only_flag_count"] == 0
        assert summary["inventory"]["shared_pressure_flag_count"] == 3
        assert summary["inventory"]["shared_pressure_flags_enabled_count"] == 0
        assert len(summary["inventory"]["shared_pressure_flags"]) == 3
        assert all(item["default_enabled"] is False for item in summary["inventory"]["shared_pressure_flags"])
        assert all(item["enabled"] is False for item in summary["inventory"]["shared_pressure_flags"])
        assert {
            item["config_path"] for item in summary["inventory"]["shared_pressure_flags"]
        } == {
            "workflow.pressure.capture_then_hold_enabled",
            "workflow.pressure.adaptive_pressure_sampling_enabled",
            "workflow.pressure.soft_control_enabled",
        }
        assert "real-COM 0" in summary["inventory"]["summary"]
        assert any("shared runner" in line for line in summary["review_lines"])
        assert summary["review_lines"]


def test_step2_config_safety_requires_dual_unlock_for_real_ports() -> None:
    config = AppConfig.from_dict(
        {
            "features": {"simulation_mode": True},
            "devices": {
                "pressure_controller": {"port": "COM31", "enabled": True},
                "gas_analyzers": [{"port": "SIM-GA01", "enabled": True}],
            },
        }
    )

    blocked = summarize_step2_config_safety(config)
    blocked_review = build_step2_config_safety_review(blocked)
    unlocked = summarize_step2_config_safety(
        config,
        allow_unsafe_step2_config=True,
        unsafe_config_env_enabled=True,
    )
    unlocked_review = build_step2_config_safety_review(unlocked)

    assert blocked["status"] == "warn"
    assert blocked["requires_explicit_unlock"] is True
    assert blocked["step2_default_workflow_allowed"] is False
    assert blocked["execution_gate"]["status"] == "blocked"
    assert blocked["execution_gate"]["requires_dual_unlock"] is True
    assert "real_ports_detected" in blocked["execution_gate"]["blocked_reasons"]
    assert blocked["classification"] == "simulation_real_port_inventory_risk"
    assert "real_com_risk" in blocked["badge_ids"]
    assert "requires_dual_unlock" in blocked["badge_ids"]
    assert "step2_blocked" in blocked["badge_ids"]
    assert blocked["inventory"]["enabled_device_count"] == 2
    assert blocked["inventory"]["real_port_device_count"] == 1
    assert blocked["blocked_reason_details"][0]["code"] == "real_ports_detected"
    assert blocked["blocked_reason_details"][0]["severity"] == "warn"
    assert any("real-COM" in line or "real-COM 风险设备" in line for line in blocked["review_lines"])
    assert blocked_review["execution_gate"]["status"] == "blocked"
    assert blocked_review["execution_gate"]["requires_dual_unlock"] is True
    assert blocked_review["warnings"]
    assert blocked_review["blocked_reasons"] == ["real_ports_detected"]
    assert blocked_review["real_port_device_count"] == 1
    assert blocked_review["engineering_only_flag_count"] == 0
    assert blocked_review["devices_with_real_ports"][0]["port"] == "COM31"
    assert blocked_review["inventory"]["real_port_device_count"] == 1
    assert unlocked["step2_default_workflow_allowed"] is True
    assert unlocked["execution_gate"]["status"] == "unlocked_override"
    assert "step2_override" in unlocked["badge_ids"]
    assert unlocked_review["execution_gate"]["status"] == "unlocked_override"
    assert unlocked_review["step2_default_workflow_allowed"] is True
    assert unlocked_review["warnings"]


def test_step2_config_safety_inventory_tracks_shared_pressure_flags_and_blocked_reasons() -> None:
    config = AppConfig.from_dict(
        {
            "features": {"simulation_mode": True},
            "workflow": {
                "pressure": {
                    "capture_then_hold_enabled": True,
                    "adaptive_pressure_sampling_enabled": False,
                    "soft_control_enabled": True,
                }
            },
            "devices": {
                "pressure_controller": {"port": "SIM-PACE5000", "enabled": True},
            },
        }
    )

    summary = summarize_step2_config_safety(config)
    review = build_step2_config_safety_review(summary)
    shared_flags = {item["config_path"]: dict(item) for item in summary["inventory"]["shared_pressure_flags"]}

    assert summary["classification"] == "simulation_engineering_only_risk"
    assert summary["inventory"]["engineering_only_flag_count"] == 2
    assert summary["inventory"]["shared_pressure_flags_enabled_count"] == 2
    assert shared_flags["workflow.pressure.capture_then_hold_enabled"]["enabled"] is True
    assert shared_flags["workflow.pressure.capture_then_hold_enabled"]["status"] == "engineering_only_enabled"
    assert shared_flags["workflow.pressure.adaptive_pressure_sampling_enabled"]["enabled"] is False
    assert shared_flags["workflow.pressure.adaptive_pressure_sampling_enabled"]["status"] == "default_safe"
    assert shared_flags["workflow.pressure.soft_control_enabled"]["enabled"] is True
    assert any(item["code"] == "engineering_only_flags_enabled" for item in summary["blocked_reason_details"])
    assert any("shared runner" in line for line in summary["review_lines"])
    assert review["inventory"]["shared_pressure_flags_enabled_count"] == 2
    assert review["blocked_reasons"] == ["engineering_only_flags_enabled"]


def test_step2_config_safety_requires_dual_unlock_for_capture_then_hold_flag() -> None:
    config = AppConfig.from_dict(
        {
            "features": {"simulation_mode": True},
            "workflow": {
                "pressure": {
                    "capture_then_hold_enabled": True,
                }
            },
            "devices": {
                "pressure_controller": {"port": "SIM-PACE5000", "enabled": True},
            },
        }
    )

    blocked = summarize_step2_config_safety(config)
    cli_only = summarize_step2_config_safety(
        config,
        allow_unsafe_step2_config=True,
    )
    unlocked = summarize_step2_config_safety(
        config,
        allow_unsafe_step2_config=True,
        unsafe_config_env_enabled=True,
    )

    assert blocked["execution_gate"]["status"] == "blocked"
    assert blocked["step2_default_workflow_allowed"] is False
    assert blocked["requires_explicit_unlock"] is True
    assert blocked["execution_gate"]["blocked_reasons"] == ["engineering_only_flags_enabled"]
    assert cli_only["execution_gate"]["status"] == "blocked"
    assert cli_only["step2_default_workflow_allowed"] is False
    assert cli_only["requires_explicit_unlock"] is True
    assert cli_only["execution_gate"]["blocked_reasons"] == ["engineering_only_flags_enabled"]
    assert unlocked["execution_gate"]["status"] == "unlocked_override"
    assert unlocked["step2_default_workflow_allowed"] is True
    assert unlocked["requires_explicit_unlock"] is True
