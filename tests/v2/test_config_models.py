import json
from pathlib import Path

import gas_calibrator.v2.config as config_package
import gas_calibrator.v2.config.models as config_models
from gas_calibrator.validation.simulation.config import (
    AIConfig,
    AIFeaturesConfig,
    CoefficientSummaryColumnConfig,
    CoefficientsConfig,
    FeaturesConfig,
    H2OSummarySelectionConfig,
    HumidityStabilityConfig,
    PathsConfig,
    PrecheckConfig,
    PressureControlConfig,
    PressureStabilityConfig,
    QCConfig,
    QCRuleConfig,
    SamplingConfig,
    SignalStabilityConfig,
    StabilityConfig,
    STEP2_ENGINEERING_ONLY_PRESSURE_FLAG_SPECS,
    TemperatureStabilityConfig,
    ValveConfig,
    _build_shared_pressure_flag_inventory,
    _build_step2_blocked_reason_details,
    _build_step2_config_safety_badges,
    _build_step2_config_safety_inventory,
    _normalize_run_mode,
    _normalize_sensor_precheck_config,
    _step2_config_safety_badge_spec,
    _step2_config_safety_classification,
    _step2_config_safety_classification_display,
    build_step2_config_governance_handoff,
    build_step2_config_safety_review,
    enabled_engineering_only_flags,
    hydrate_step2_config_safety_summary,
    iter_config_device_ports,
    port_requires_real_device_review,
)
from gas_calibrator.storage.settings import StorageConfig
from gas_calibrator.v2.config.models import (
    AIConfig as LegacyAIConfig,
    AIFeaturesConfig as LegacyAIFeaturesConfig,
    AppConfig,
    CoefficientSummaryColumnConfig as LegacyCoefficientSummaryColumnConfig,
    CoefficientsConfig as LegacyCoefficientsConfig,
    FeaturesConfig as LegacyFeaturesConfig,
    H2OSummarySelectionConfig as LegacyH2OSummarySelectionConfig,
    HumidityStabilityConfig as LegacyHumidityStabilityConfig,
    PathsConfig as LegacyPathsConfig,
    PrecheckConfig as LegacyPrecheckConfig,
    PressureControlConfig as LegacyPressureControlConfig,
    PressureStabilityConfig as LegacyPressureStabilityConfig,
    QCConfig as LegacyQCConfig,
    QCRuleConfig as LegacyQCRuleConfig,
    SamplingConfig as LegacySamplingConfig,
    SignalStabilityConfig as LegacySignalStabilityConfig,
    StabilityConfig as LegacyStabilityConfig,
    STEP2_ENGINEERING_ONLY_PRESSURE_FLAG_SPECS as LegacyStep2EngineeringOnlyPressureFlagSpecs,
    StorageConfig as LegacyStorageConfig,
    TemperatureStabilityConfig as LegacyTemperatureStabilityConfig,
    ValveConfig as LegacyValveConfig,
    _build_shared_pressure_flag_inventory as legacy_build_shared_pressure_flag_inventory,
    _build_step2_blocked_reason_details as legacy_build_step2_blocked_reason_details,
    _build_step2_config_safety_badges as legacy_build_step2_config_safety_badges,
    _build_step2_config_safety_inventory as legacy_build_step2_config_safety_inventory,
    _normalize_run_mode as legacy_normalize_run_mode,
    _normalize_sensor_precheck_config as legacy_normalize_sensor_precheck_config,
    _step2_config_safety_badge_spec as legacy_step2_config_safety_badge_spec,
    _step2_config_safety_classification as legacy_step2_config_safety_classification,
    _step2_config_safety_classification_display as legacy_step2_config_safety_classification_display,
    build_step2_config_governance_handoff as legacy_build_step2_config_governance_handoff,
    build_step2_config_safety_review as legacy_build_step2_config_safety_review,
    enabled_engineering_only_flags as legacy_enabled_engineering_only_flags,
    hydrate_step2_config_safety_summary as legacy_hydrate_step2_config_safety_summary,
    iter_config_device_ports as legacy_iter_config_device_ports,
    port_requires_real_device_review as legacy_port_requires_real_device_review,
    summarize_step2_config_safety,
)
from gas_calibrator.validation.simulation.pressure_selection import AMBIENT_PRESSURE_TOKEN


def test_config_package_is_namespace_without_model_reexports() -> None:
    package_init = (
        Path(__file__).resolve().parents[2]
        / "src"
        / "gas_calibrator"
        / "v2"
        / "config"
        / "__init__.py"
    )

    assert not package_init.exists()
    assert config_package.__spec__.submodule_search_locations is not None
    assert not hasattr(config_package, "AppConfig")
    assert not hasattr(config_package, "summarize_step2_config_safety")


def test_stability_configs_have_one_validation_owned_identity() -> None:
    assert LegacyTemperatureStabilityConfig is TemperatureStabilityConfig
    assert LegacyHumidityStabilityConfig is HumidityStabilityConfig
    assert LegacyPressureStabilityConfig is PressureStabilityConfig
    assert LegacySignalStabilityConfig is SignalStabilityConfig
    assert LegacyStabilityConfig is StabilityConfig


def test_ai_configs_have_one_validation_owned_identity() -> None:
    assert LegacyAIFeaturesConfig is AIFeaturesConfig
    assert LegacyAIConfig is AIConfig


def test_paths_config_has_one_validation_owned_identity() -> None:
    assert LegacyPathsConfig is PathsConfig
    assert type(AppConfig().paths) is PathsConfig


def test_features_config_has_one_validation_owned_identity() -> None:
    assert LegacyFeaturesConfig is FeaturesConfig
    assert type(AppConfig().features) is FeaturesConfig


def test_storage_config_has_one_shared_identity() -> None:
    assert LegacyStorageConfig is StorageConfig
    assert type(AppConfig().storage) is StorageConfig


def test_valve_config_keeps_only_active_raw_route_snapshot_fields() -> None:
    assert LegacyValveConfig is ValveConfig
    assert type(AppConfig().valves) is ValveConfig

    config = AppConfig.from_dict(
        {
            "valves": {
                "group_a": {"concentrations": [0, 400]},
                "group_b": {"concentrations": [600, 800]},
                "valve_mapping": {"legacy_zero": 99},
                "co2_path": 7,
                "co2_path_group2": 16,
                "gas_main": 11,
                "h2o_path": 8,
                "flow_switch": 10,
                "hold": 9,
                "relay_map": {"7": {"device": "relay", "channel": 7}},
                "co2_map": {"0": 1, "400": 2},
                "co2_map_group2": {"600": 3},
            }
        }
    )

    assert vars(config.valves) == {
        "co2_path": 7,
        "co2_path_group2": 16,
        "gas_main": 11,
        "h2o_path": 8,
        "flow_switch": 10,
        "hold": 9,
        "relay_map": {"7": {"device": "relay", "channel": 7}},
        "co2_map": {"0": 1, "400": 2},
        "co2_map_group2": {"600": 3},
    }
    assert not hasattr(config.valves, "group_a")
    assert not hasattr(config.valves, "group_b")
    assert not hasattr(config.valves, "valve_mapping")


def test_coefficients_config_keeps_only_v2_ratio_poly_runtime_fields() -> None:
    raw_coefficients = {
        "enabled": True,
        "auto_fit": True,
        "model": "ratio_poly_rt_p",
        "order": 4,
        "ratio_degree": 5,
        "temperature_offset_c": 273.0,
        "add_intercept": False,
        "simplify_coefficients": False,
        "simplification_method": "none",
        "target_digits": 8,
        "report_temperature_key": "ReferenceThermometerTempC",
        "report_pressure_key": "ReferencePressureHpa",
        "report_output_name": "ratio_poly.xlsx",
        "signal_keys": {"co2": ["R_CO2"], "h2o": ["R_H2O"]},
        "summary_columns": {
            "co2": {
                "target": "co2_target",
                "ratio": "co2_ratio",
                "temperature": "legacy_co2_temperature",
                "pressure": "legacy_co2_pressure",
                "pressure_scale": 0.1,
            },
            "h2o": {
                "target": "h2o_target",
                "ratio": "h2o_ratio",
                "temperature": "legacy_h2o_temperature",
                "pressure": "legacy_h2o_pressure",
                "pressure_scale": 0.2,
            },
        },
    }

    config = AppConfig.from_dict({"coefficients": raw_coefficients})

    assert {
        "enabled": config.coefficients.enabled,
        "auto_fit": config.coefficients.auto_fit,
        "model": config.coefficients.model,
        "ratio_degree": config.coefficients.ratio_degree,
        "temperature_offset_c": config.coefficients.temperature_offset_c,
        "add_intercept": config.coefficients.add_intercept,
        "simplify_coefficients": config.coefficients.simplify_coefficients,
        "simplification_method": config.coefficients.simplification_method,
        "target_digits": config.coefficients.target_digits,
        "report_temperature_key": config.coefficients.report_temperature_key,
        "report_pressure_key": config.coefficients.report_pressure_key,
        "report_output_name": config.coefficients.report_output_name,
    } == {
        "enabled": True,
        "auto_fit": True,
        "model": "ratio_poly_rt_p",
        "ratio_degree": 5,
        "temperature_offset_c": 273.0,
        "add_intercept": False,
        "simplify_coefficients": False,
        "simplification_method": "none",
        "target_digits": 8,
        "report_temperature_key": "ReferenceThermometerTempC",
        "report_pressure_key": "ReferencePressureHpa",
        "report_output_name": "ratio_poly.xlsx",
    }
    assert not hasattr(config.coefficients, "order")
    assert not hasattr(config.coefficients, "signal_keys")
    assert vars(config.coefficients.summary_columns["co2"]) == {
        "target": "co2_target",
        "ratio": "co2_ratio",
        "pressure_scale": 0.1,
    }
    assert vars(config.coefficients.summary_columns["h2o"]) == {
        "target": "h2o_target",
        "ratio": "h2o_ratio",
        "pressure_scale": 0.2,
    }
    assert not hasattr(config.coefficients.summary_columns["co2"], "temperature")
    assert not hasattr(config.coefficients.summary_columns["co2"], "pressure")
    assert raw_coefficients["order"] == 4
    assert raw_coefficients["signal_keys"] == {"co2": ["R_CO2"], "h2o": ["R_H2O"]}
    assert raw_coefficients["summary_columns"]["co2"]["temperature"] == "legacy_co2_temperature"
    assert raw_coefficients["summary_columns"]["co2"]["pressure"] == "legacy_co2_pressure"


def test_coefficient_configs_have_one_validation_owned_identity() -> None:
    assert LegacyCoefficientSummaryColumnConfig is CoefficientSummaryColumnConfig
    assert LegacyH2OSummarySelectionConfig is H2OSummarySelectionConfig
    assert LegacyCoefficientsConfig is CoefficientsConfig
    assert type(AppConfig().coefficients) is CoefficientsConfig
    assert type(AppConfig().coefficients.summary_columns["co2"]) is CoefficientSummaryColumnConfig
    assert type(AppConfig().coefficients.h2o_summary_selection) is H2OSummarySelectionConfig


def test_qc_configs_have_one_validation_owned_identity_and_unchanged_contract() -> None:
    assert LegacyQCConfig is QCConfig
    assert LegacyQCRuleConfig is QCRuleConfig
    config = AppConfig.from_dict(
        {
            "qc": {
                "min_sample_count": 7,
                "max_outlier_ratio": 0.15,
                "spike_threshold": 2.5,
                "drift_threshold": 0.08,
                "quality_threshold": 0.82,
                "rule_config": {
                    "default_rule": "co2_strict",
                    "route_rules": {"co2": "co2_strict", "h2o": "h2o_strict"},
                    "mode_rules": {"verify": "verify_mode"},
                    "custom_rules": [{"name": "review_only"}],
                },
            }
        }
    )

    assert type(config.qc) is QCConfig
    assert type(config.qc.rule_config) is QCRuleConfig
    assert config.qc.min_sample_count == 7
    assert config.qc.max_outlier_ratio == 0.15
    assert config.qc.spike_threshold == 2.5
    assert config.qc.drift_threshold == 0.08
    assert config.qc.quality_threshold == 0.82
    assert config.qc.rule_config.default_rule == "co2_strict"
    assert config.qc.rule_config.route_rules == {
        "co2": "co2_strict",
        "h2o": "h2o_strict",
    }
    assert config.qc.rule_config.mode_rules == {"verify": "verify_mode"}
    assert config.qc.rule_config.custom_rules == [{"name": "review_only"}]


def test_sampling_config_has_one_validation_owned_identity_and_unchanged_contract() -> None:
    assert LegacySamplingConfig is SamplingConfig
    config = AppConfig.from_dict(
        {
            "workflow": {
                "sampling": {
                    "interval_s": 0.25,
                    "count": 17,
                    "discard_first_n": 3,
                }
            }
        }
    )

    assert type(config.workflow.sampling) is SamplingConfig
    assert config.workflow.sampling.interval_s == 0.25
    assert config.workflow.sampling.count == 17
    assert config.workflow.sampling.discard_first_n == 3
    assert SamplingConfig.from_dict(None) == SamplingConfig(
        interval_s=1.0,
        count=10,
        discard_first_n=0,
    )


def test_precheck_config_has_one_validation_owned_identity_and_unchanged_contract() -> None:
    assert LegacyPrecheckConfig is PrecheckConfig
    config = AppConfig.from_dict(
        {
            "workflow": {
                "precheck": {
                    "enabled": False,
                    "pressure_leak_test": False,
                    "sensor_check": False,
                    "device_connection": False,
                }
            }
        }
    )

    assert type(config.workflow.precheck) is PrecheckConfig
    assert config.workflow.precheck.enabled is False
    assert config.workflow.precheck.pressure_leak_test is False
    assert config.workflow.precheck.sensor_check is False
    assert config.workflow.precheck.device_connection is False
    assert PrecheckConfig.from_dict(None) == PrecheckConfig(
        enabled=True,
        pressure_leak_test=True,
        sensor_check=True,
        device_connection=True,
    )


def test_pressure_control_config_has_one_validation_owned_active_contract() -> None:
    assert LegacyPressureControlConfig is PressureControlConfig
    config = AppConfig.from_dict(
        {
            "workflow": {
                "pressure_control": {
                    "setpoint_tolerance_hpa": 0.75,
                    "ramp_rate_hpa_per_s": 20.0,
                    "max_pressure_hpa": 1200.0,
                    "min_pressure_hpa": 400.0,
                }
            }
        }
    )

    assert type(config.workflow.pressure_control) is PressureControlConfig
    assert vars(config.workflow.pressure_control) == {
        "setpoint_tolerance_hpa": 0.75,
    }
    assert PressureControlConfig.from_dict(None) == PressureControlConfig(
        setpoint_tolerance_hpa=0.5,
    )
    assert not hasattr(config.workflow.pressure_control, "ramp_rate_hpa_per_s")
    assert not hasattr(config.workflow.pressure_control, "max_pressure_hpa")
    assert not hasattr(config.workflow.pressure_control, "min_pressure_hpa")


def test_workflow_config_ignores_dead_v2_startup_connect_check_mirror() -> None:
    raw_config = {
        "workflow": {
            "startup_connect_check": {
                "enabled": False,
                "retries": 3,
            }
        }
    }

    config = AppConfig.from_dict(raw_config)

    assert not hasattr(config.workflow, "startup_connect_check")
    assert raw_config["workflow"]["startup_connect_check"] == {
        "enabled": False,
        "retries": 3,
    }


def test_features_config_ignores_historical_use_v2_label() -> None:
    config = FeaturesConfig.from_dict(
        {
            "use_v2": True,
            "simulation_mode": True,
            "debug_mode": True,
        }
    )

    assert config.simulation_mode is True
    assert config.debug_mode is True
    assert not hasattr(config, "use_v2")


def test_app_config_ignores_inert_historical_algorithm_block() -> None:
    config = AppConfig.from_dict(
        {
            "algorithm": {
                "default_algorithm": "linear",
                "candidates": ["linear"],
                "auto_select": False,
                "validation_tolerance": 0.2,
            }
        }
    )

    assert not hasattr(config, "algorithm")
    assert not hasattr(config_models, "AlgorithmConfig")


def test_app_config_ignores_inert_historical_storage_schema_field() -> None:
    config = AppConfig.from_dict(
        {
            "storage": {
                "backend": "file",
                "schema": "historical_unused_schema",
            }
        }
    )

    assert config.storage.backend == "file"
    assert not hasattr(config.storage, "schema")


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


def test_app_config_ignores_retired_spectral_quality_feature_fields() -> None:
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

    assert not hasattr(config.features, "enable_spectral_quality_analysis")
    assert not hasattr(config.features, "spectral_min_samples")
    assert not hasattr(config.features, "spectral_min_duration_s")
    assert not hasattr(config.features, "spectral_low_freq_max_hz")


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
    assert config.workflow.analyzer_setup["apply_device_id"] is True

    disabled = AppConfig.from_dict({"workflow": {"analyzer_setup": {"apply_device_id": "false"}}})
    assert disabled.workflow.analyzer_setup["apply_device_id"] is False


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
    assert legacy_normalize_run_mode is _normalize_run_mode
    assert legacy_normalize_sensor_precheck_config is _normalize_sensor_precheck_config
    assert (
        LegacyStep2EngineeringOnlyPressureFlagSpecs
        is STEP2_ENGINEERING_ONLY_PRESSURE_FLAG_SPECS
    )
    assert legacy_port_requires_real_device_review is port_requires_real_device_review
    assert legacy_iter_config_device_ports is iter_config_device_ports
    assert legacy_enabled_engineering_only_flags is enabled_engineering_only_flags
    assert legacy_step2_config_safety_classification is _step2_config_safety_classification
    assert (
        legacy_step2_config_safety_classification_display
        is _step2_config_safety_classification_display
    )
    assert legacy_step2_config_safety_badge_spec is _step2_config_safety_badge_spec
    assert legacy_build_step2_config_safety_badges is _build_step2_config_safety_badges
    assert (
        legacy_build_shared_pressure_flag_inventory
        is _build_shared_pressure_flag_inventory
    )
    assert (
        legacy_build_step2_config_safety_inventory
        is _build_step2_config_safety_inventory
    )
    assert (
        legacy_build_step2_blocked_reason_details
        is _build_step2_blocked_reason_details
    )
    assert (
        legacy_hydrate_step2_config_safety_summary
        is hydrate_step2_config_safety_summary
    )
    assert (
        legacy_build_step2_config_safety_review
        is build_step2_config_safety_review
    )
    assert (
        legacy_build_step2_config_governance_handoff
        is build_step2_config_governance_handoff
    )

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
