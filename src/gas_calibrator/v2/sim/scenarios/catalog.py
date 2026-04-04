from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from ..devices import (
    FakeAnalyzerSpec,
    FakeDewpointMeterSpec,
    FakeHumidityGeneratorSpec,
    FakePressureControllerSpec,
    FakePressureGaugeSpec,
    FakeRelaySpec,
    FakeTemperatureChamberSpec,
    FakeThermometerSpec,
    FakeTransportFaultSpec,
    SimulatedDeviceMatrix,
)
from ...scripts.compare_v1_v2_control_flow import (
    FULL_ROUTE_SIMULATED_DIAGNOSTIC_VALIDATION_PROFILE,
    FULL_ROUTE_SIMULATED_VALIDATION_PROFILE,
    H2O_ONLY_SIMULATED_VALIDATION_PROFILE,
    SKIP0_CO2_ONLY_SIMULATED_VALIDATION_PROFILE,
)


@dataclass(frozen=True)
class SimulatedScenarioDefinition:
    name: str
    validation_profile: str
    fixture_name: str
    description: str
    diagnostic_only: bool
    target_route: str
    device_matrix: SimulatedDeviceMatrix
    execution_mode: str = "fixture"
    baseline_mode: str = "fixture"
    runtime_overrides: dict[str, Any] = field(default_factory=dict)

    def simulation_context(self) -> dict[str, Any]:
        return {
            "scenario": self.name,
            "description": self.description,
            "diagnostic_only": self.diagnostic_only,
            "target_route": self.target_route,
            "execution_mode": self.execution_mode,
            "baseline_mode": self.baseline_mode,
            "simulation_backend": "protocol" if self.execution_mode == "protocol" else "fixture",
            "device_matrix": self.device_matrix.to_dict(),
            "runtime_overrides": dict(self.runtime_overrides),
        }


def _merge_dicts(*payloads: dict[str, Any]) -> dict[str, Any]:
    merged: dict[str, Any] = {}
    for payload in payloads:
        for key, value in payload.items():
            if isinstance(value, dict) and isinstance(merged.get(key), dict):
                merged[key] = _merge_dicts(dict(merged[key]), value)
            elif isinstance(value, dict):
                merged[key] = _merge_dicts(value)
            else:
                merged[key] = value
    return merged


FAST_TEMPERATURE_OVERRIDES = {
    "workflow": {
        "startup_pressure_precheck": {
            "enabled": False,
        },
        "stability": {
            "temperature": {
                "tol": 1.0,
                "window_s": 0.4,
                "timeout_s": 2.5,
                "soak_after_reach_s": 0.1,
                "transition_check_window_s": 0.8,
                "transition_min_delta_c": 0.05,
                "analyzer_chamber_temp_enabled": False,
            }
        }
    }
}

FAST_HUMIDITY_OVERRIDES = {
    "workflow": {
        "stability": {
            "humidity_generator": {
                "timeout_s": 3.0,
                "window_s": 0.4,
                "rh_stable_window_s": 0.4,
                "rh_stable_span_pct": 0.8,
                "poll_s": 0.05,
                "temp_tol_c": 1.0,
                "rh_tol_pct": 2.0,
            }
        }
    }
}

FAST_DEWPOINT_OVERRIDES = {
    "workflow": {
        "stability": {
            "dewpoint": {
                "window_s": 0.5,
                "timeout_s": 2.0,
                "poll_s": 0.05,
                "temp_match_tol_c": 1.0,
                "rh_match_tol_pct": 2.0,
                "stability_tol_c": 0.5,
                "min_samples": 2,
            }
        }
    }
}

FAST_PRESSURE_OVERRIDES = {
    "workflow": {
        "pressure": {
            "pressurize_high_hpa": 1000.0,
            "pressurize_wait_after_vent_off_s": 0.0,
            "pressurize_timeout_s": 1.5,
            "post_stable_sample_delay_s": 0.0,
            "co2_post_stable_sample_delay_s": 0.0,
            "co2_post_h2o_vent_off_wait_s": 0.0,
            "vent_time_s": 0.0,
            "vent_transition_timeout_s": 1.0,
            "continuous_atmosphere_hold": True,
            "vent_hold_interval_s": 0.05,
            "stabilize_timeout_s": 1.5,
            "restabilize_retries": 0,
            "restabilize_retry_interval_s": 0.1,
        }
    }
}

FAST_SAMPLING_OVERRIDES = {
    "workflow": {
        "sampling": {
            "count": 2,
            "stable_count": 2,
            "interval_s": 0.05,
            "h2o_interval_s": 0.05,
            "co2_interval_s": 0.05,
            "quality": {
                "enabled": False,
            },
        }
    }
}

FAST_ROUTE_SOAK_OVERRIDES = {
    "workflow": {
        "stability": {
            "h2o_route": {
                "preseal_soak_s": 0.05,
                "humidity_timeout_policy": "abort_like_v1",
            },
            "co2_route": {
                "preseal_soak_s": 0.05,
                "first_point_preseal_soak_s": 0.05,
                "post_h2o_zero_ppm_soak_s": 0.05,
            },
        }
    }
}

FAST_CO2_ROUTE_OVERRIDES = _merge_dicts(
    FAST_TEMPERATURE_OVERRIDES,
    FAST_PRESSURE_OVERRIDES,
    FAST_SAMPLING_OVERRIDES,
    FAST_ROUTE_SOAK_OVERRIDES,
)

FAST_CO2_ONLY_DIAGNOSTIC_OVERRIDES = _merge_dicts(
    FAST_CO2_ROUTE_OVERRIDES,
    {
        "devices": {
            "humidity_generator": {"enabled": False},
            "dewpoint_meter": {"enabled": False},
        },
        "workflow": {
            "route_mode": "co2_only",
            "selected_temps_c": [20.0],
            "skip_co2_ppm": [0],
            "humidity_generator": {
                "ensure_run": False,
            },
            "stability": {
                "humidity_generator": {
                    "enabled": False,
                }
            },
        },
    },
)

RELAXED_SENSOR_PRECHECK_OVERRIDES = {
    "workflow": {
        "sensor_precheck": {
            "strict": False,
            "min_valid_frames": 1,
        }
    }
}

FAST_FULL_ROUTE_OVERRIDES = _merge_dicts(
    FAST_TEMPERATURE_OVERRIDES,
    FAST_HUMIDITY_OVERRIDES,
    FAST_DEWPOINT_OVERRIDES,
    FAST_PRESSURE_OVERRIDES,
    FAST_SAMPLING_OVERRIDES,
    FAST_ROUTE_SOAK_OVERRIDES,
)
FAST_RELAXED_ROUTE_OVERRIDES = _merge_dicts(
    FAST_TEMPERATURE_OVERRIDES,
    FAST_DEWPOINT_OVERRIDES,
    FAST_PRESSURE_OVERRIDES,
    FAST_SAMPLING_OVERRIDES,
    FAST_ROUTE_SOAK_OVERRIDES,
    RELAXED_SENSOR_PRECHECK_OVERRIDES,
)


PROFILE_DEFAULTS: dict[str, dict[str, Any]] = {
    FULL_ROUTE_SIMULATED_VALIDATION_PROFILE: {
        "scenario": "full_route_success_all_temps_all_sources",
        "diagnostic_only": False,
    },
    FULL_ROUTE_SIMULATED_DIAGNOSTIC_VALIDATION_PROFILE: {
        "scenario": "pace_no_response_on_cleanup",
        "diagnostic_only": True,
    },
    SKIP0_CO2_ONLY_SIMULATED_VALIDATION_PROFILE: {
        "scenario": "co2_only_skip0_success_single_temp",
        "diagnostic_only": False,
    },
    H2O_ONLY_SIMULATED_VALIDATION_PROFILE: {
        "scenario": "h2o_route_success_single_temp",
        "diagnostic_only": True,
    },
}


SCENARIOS: dict[str, SimulatedScenarioDefinition] = {
    "full_route_success_all_temps_all_sources": SimulatedScenarioDefinition(
        name="full_route_success_all_temps_all_sources",
        validation_profile=FULL_ROUTE_SIMULATED_VALIDATION_PROFILE,
        fixture_name="full_route_success_all_temps_all_sources",
        description="Full-route success across H2O + CO2, 0 ppm, and multiple temperature groups.",
        diagnostic_only=False,
        target_route="h2o_then_co2",
        device_matrix=SimulatedDeviceMatrix(),
        execution_mode="protocol",
        baseline_mode="mirror_v2",
        runtime_overrides=FAST_FULL_ROUTE_OVERRIDES,
    ),
    "co2_only_skip0_success_single_temp": SimulatedScenarioDefinition(
        name="co2_only_skip0_success_single_temp",
        validation_profile=SKIP0_CO2_ONLY_SIMULATED_VALIDATION_PROFILE,
        fixture_name="co2_only_skip0_success_single_temp",
        description="CO2-only parity coverage for the real skip0 acceptance route.",
        diagnostic_only=False,
        target_route="co2",
        device_matrix=SimulatedDeviceMatrix(
            humidity_generator=FakeHumidityGeneratorSpec(mode="stable", skipped_by_profile=True),
            dewpoint_meter=FakeDewpointMeterSpec(mode="stable", skipped_by_profile=True),
        ),
        execution_mode="protocol",
        baseline_mode="mirror_v2",
        runtime_overrides=FAST_CO2_ROUTE_OVERRIDES,
    ),
    "relay_route_switch_co2_success": SimulatedScenarioDefinition(
        name="relay_route_switch_co2_success",
        validation_profile=SKIP0_CO2_ONLY_SIMULATED_VALIDATION_PROFILE,
        fixture_name="co2_only_skip0_success_single_temp",
        description="CO2-only route switching where relay states drive the in-scope path transition.",
        diagnostic_only=False,
        target_route="co2",
        device_matrix=SimulatedDeviceMatrix(
            relay=FakeRelaySpec(channel_count=16, mode="stable"),
            relay_8=FakeRelaySpec(channel_count=8, mode="stable"),
            thermometer=FakeThermometerSpec(mode="stable"),
            humidity_generator=FakeHumidityGeneratorSpec(mode="stable", skipped_by_profile=True),
            dewpoint_meter=FakeDewpointMeterSpec(mode="stable", skipped_by_profile=True),
        ),
        execution_mode="protocol",
        baseline_mode="mirror_v2",
        runtime_overrides=FAST_CO2_ROUTE_OVERRIDES,
    ),
    "co2_only_skip0_success_eight_analyzers": SimulatedScenarioDefinition(
        name="co2_only_skip0_success_eight_analyzers",
        validation_profile=SKIP0_CO2_ONLY_SIMULATED_VALIDATION_PROFILE,
        fixture_name="co2_only_skip0_success_single_temp",
        description="CO2-only protocol success with eight analyzers exercising the simulated analyzer fleet.",
        diagnostic_only=False,
        target_route="co2",
        device_matrix=SimulatedDeviceMatrix(
            analyzers=FakeAnalyzerSpec(count=8, mode2_stream="stable", sensor_precheck="strict_pass"),
            relay=FakeRelaySpec(channel_count=16, mode="stable"),
            relay_8=FakeRelaySpec(channel_count=8, mode="stable"),
            thermometer=FakeThermometerSpec(mode="stable"),
            humidity_generator=FakeHumidityGeneratorSpec(mode="stable", skipped_by_profile=True),
            dewpoint_meter=FakeDewpointMeterSpec(mode="stable", skipped_by_profile=True),
        ),
        execution_mode="protocol",
        baseline_mode="mirror_v2",
        runtime_overrides=FAST_CO2_ROUTE_OVERRIDES,
    ),
    "co2_only_skip0_success_eight_analyzers_with_relay": SimulatedScenarioDefinition(
        name="co2_only_skip0_success_eight_analyzers_with_relay",
        validation_profile=SKIP0_CO2_ONLY_SIMULATED_VALIDATION_PROFILE,
        fixture_name="co2_only_skip0_success_single_temp",
        description="CO2-only success with eight analyzers and relay-driven route switching.",
        diagnostic_only=False,
        target_route="co2",
        device_matrix=SimulatedDeviceMatrix(
            analyzers=FakeAnalyzerSpec(count=8, mode2_stream="stable", sensor_precheck="strict_pass"),
            relay=FakeRelaySpec(channel_count=16, mode="stable"),
            relay_8=FakeRelaySpec(channel_count=8, mode="stable"),
            thermometer=FakeThermometerSpec(mode="stable"),
            humidity_generator=FakeHumidityGeneratorSpec(mode="stable", skipped_by_profile=True),
            dewpoint_meter=FakeDewpointMeterSpec(mode="stable", skipped_by_profile=True),
        ),
        execution_mode="protocol",
        baseline_mode="mirror_v2",
        runtime_overrides=FAST_CO2_ROUTE_OVERRIDES,
    ),
    "h2o_route_success_single_temp": SimulatedScenarioDefinition(
        name="h2o_route_success_single_temp",
        validation_profile=H2O_ONLY_SIMULATED_VALIDATION_PROFILE,
        fixture_name="h2o_route_success_single_temp",
        description="H2O-only simulated success for single-temperature route coverage.",
        diagnostic_only=True,
        target_route="h2o",
        device_matrix=SimulatedDeviceMatrix(),
        execution_mode="protocol",
        baseline_mode="mirror_v2",
        runtime_overrides=FAST_FULL_ROUTE_OVERRIDES,
    ),
    "relay_route_switch_h2o_success": SimulatedScenarioDefinition(
        name="relay_route_switch_h2o_success",
        validation_profile=H2O_ONLY_SIMULATED_VALIDATION_PROFILE,
        fixture_name="h2o_route_success_single_temp",
        description="H2O-only route switching where relay states drive the in-scope path transition.",
        diagnostic_only=True,
        target_route="h2o",
        device_matrix=SimulatedDeviceMatrix(
            relay=FakeRelaySpec(channel_count=16, mode="stable"),
            relay_8=FakeRelaySpec(channel_count=8, mode="stable"),
            thermometer=FakeThermometerSpec(mode="stable"),
        ),
        execution_mode="protocol",
        baseline_mode="mirror_v2",
        runtime_overrides=FAST_FULL_ROUTE_OVERRIDES,
    ),
    "full_route_success_with_relay_and_thermometer": SimulatedScenarioDefinition(
        name="full_route_success_with_relay_and_thermometer",
        validation_profile=FULL_ROUTE_SIMULATED_VALIDATION_PROFILE,
        fixture_name="full_route_success_all_temps_all_sources",
        description="Full-route success with relay and thermometer protocol devices exercised end-to-end.",
        diagnostic_only=False,
        target_route="h2o_then_co2",
        device_matrix=SimulatedDeviceMatrix(
            relay=FakeRelaySpec(channel_count=16, mode="stable"),
            relay_8=FakeRelaySpec(channel_count=8, mode="stable"),
            thermometer=FakeThermometerSpec(mode="stable"),
        ),
        execution_mode="protocol",
        baseline_mode="mirror_v2",
        runtime_overrides=FAST_FULL_ROUTE_OVERRIDES,
    ),
    "sensor_precheck_mode2_partial_frame_fail": SimulatedScenarioDefinition(
        name="sensor_precheck_mode2_partial_frame_fail",
        validation_profile=SKIP0_CO2_ONLY_SIMULATED_VALIDATION_PROFILE,
        fixture_name="sensor_precheck_mode2_partial_frame_fail",
        description="MODE2 partial-frame startup failure before entering the in-scope CO2 route.",
        diagnostic_only=False,
        target_route="co2",
        device_matrix=SimulatedDeviceMatrix(
            analyzers=FakeAnalyzerSpec(mode2_stream="partial_frame", sensor_precheck="strict_fail"),
        ),
        execution_mode="protocol",
        baseline_mode="mirror_v2",
        runtime_overrides=FAST_CO2_ROUTE_OVERRIDES,
    ),
    "analyzer_mode2_partial_frame_protocol": SimulatedScenarioDefinition(
        name="analyzer_mode2_partial_frame_protocol",
        validation_profile=SKIP0_CO2_ONLY_SIMULATED_VALIDATION_PROFILE,
        fixture_name="sensor_precheck_mode2_partial_frame_fail",
        description="Protocol-level analyzer MODE2 partial-frame failure before route entry.",
        diagnostic_only=True,
        target_route="co2",
        device_matrix=SimulatedDeviceMatrix(
            analyzers=FakeAnalyzerSpec(count=8, mode2_stream="partial_frame", sensor_precheck="strict_fail"),
            humidity_generator=FakeHumidityGeneratorSpec(mode="stable", skipped_by_profile=True),
            dewpoint_meter=FakeDewpointMeterSpec(mode="stable", skipped_by_profile=True),
        ),
        execution_mode="protocol",
        baseline_mode="mirror_v2",
        runtime_overrides=FAST_CO2_ROUTE_OVERRIDES,
    ),
    "relay_stuck_channel_causes_route_mismatch": SimulatedScenarioDefinition(
        name="relay_stuck_channel_causes_route_mismatch",
        validation_profile=H2O_ONLY_SIMULATED_VALIDATION_PROFILE,
        fixture_name="compare_generates_partial_artifacts_on_failure",
        description="H2O route command succeeds logically but a stuck relay channel prevents the route from entering H2O.",
        diagnostic_only=True,
        target_route="h2o",
        device_matrix=SimulatedDeviceMatrix(
            relay=FakeRelaySpec(channel_count=16, mode="stable"),
            relay_8=FakeRelaySpec(channel_count=8, mode="stuck_channel", stuck_channels=[1, 2, 8]),
            thermometer=FakeThermometerSpec(mode="stable"),
        ),
        execution_mode="protocol",
        baseline_mode="mirror_v2",
        runtime_overrides=FAST_FULL_ROUTE_OVERRIDES,
    ),
    "thermometer_stable_reference": SimulatedScenarioDefinition(
        name="thermometer_stable_reference",
        validation_profile=FULL_ROUTE_SIMULATED_VALIDATION_PROFILE,
        fixture_name="full_route_success_all_temps_all_sources",
        description="Reference thermometer remains stable and is consumed by the aligned summary/export path.",
        diagnostic_only=False,
        target_route="h2o_then_co2",
        device_matrix=SimulatedDeviceMatrix(
            thermometer=FakeThermometerSpec(mode="stable"),
            relay=FakeRelaySpec(channel_count=16, mode="stable"),
            relay_8=FakeRelaySpec(channel_count=8, mode="stable"),
        ),
        execution_mode="protocol",
        baseline_mode="mirror_v2",
        runtime_overrides=FAST_FULL_ROUTE_OVERRIDES,
    ),
    "thermometer_stale_reference": SimulatedScenarioDefinition(
        name="thermometer_stale_reference",
        validation_profile=FULL_ROUTE_SIMULATED_DIAGNOSTIC_VALIDATION_PROFILE,
        fixture_name="full_route_success_all_temps_all_sources",
        description="Reference thermometer remains stale while the rest of the protocol stack keeps running.",
        diagnostic_only=True,
        target_route="h2o_then_co2",
        device_matrix=SimulatedDeviceMatrix(
            thermometer=FakeThermometerSpec(mode="stale"),
            relay=FakeRelaySpec(channel_count=16, mode="stable"),
            relay_8=FakeRelaySpec(channel_count=8, mode="stable"),
        ),
        execution_mode="protocol",
        baseline_mode="mirror_v2",
        runtime_overrides=FAST_FULL_ROUTE_OVERRIDES,
    ),
    "thermometer_no_response": SimulatedScenarioDefinition(
        name="thermometer_no_response",
        validation_profile=FULL_ROUTE_SIMULATED_DIAGNOSTIC_VALIDATION_PROFILE,
        fixture_name="compare_generates_partial_artifacts_on_failure",
        description="Reference thermometer stops streaming while the rest of the simulated stack remains healthy.",
        diagnostic_only=True,
        target_route="co2",
        device_matrix=SimulatedDeviceMatrix(
            thermometer=FakeThermometerSpec(mode="no_response"),
            humidity_generator=FakeHumidityGeneratorSpec(mode="stable", skipped_by_profile=True),
            dewpoint_meter=FakeDewpointMeterSpec(mode="stable", skipped_by_profile=True),
            relay=FakeRelaySpec(channel_count=16, mode="stable"),
            relay_8=FakeRelaySpec(channel_count=8, mode="stable"),
        ),
        execution_mode="protocol",
        baseline_mode="mirror_v2",
        runtime_overrides=FAST_CO2_ONLY_DIAGNOSTIC_OVERRIDES,
    ),
    "sensor_precheck_relaxed_allows_route_entry": SimulatedScenarioDefinition(
        name="sensor_precheck_relaxed_allows_route_entry",
        validation_profile=FULL_ROUTE_SIMULATED_DIAGNOSTIC_VALIDATION_PROFILE,
        fixture_name="sensor_precheck_relaxed_allows_route_entry",
        description="Strict MODE2 precheck fails, relaxed diagnostic precheck still enters route.",
        diagnostic_only=True,
        target_route="co2",
        device_matrix=SimulatedDeviceMatrix(
            analyzers=FakeAnalyzerSpec(mode2_stream="partial_frame", sensor_precheck="relaxed_pass"),
        ),
        execution_mode="protocol",
        baseline_mode="mirror_v2",
        runtime_overrides=FAST_RELAXED_ROUTE_OVERRIDES,
    ),
    "cleanup_restores_all_relays_off": SimulatedScenarioDefinition(
        name="cleanup_restores_all_relays_off",
        validation_profile=SKIP0_CO2_ONLY_SIMULATED_VALIDATION_PROFILE,
        fixture_name="co2_only_skip0_success_single_temp",
        description="Successful CO2-only run that verifies cleanup returns all relay coils to OFF.",
        diagnostic_only=False,
        target_route="co2",
        device_matrix=SimulatedDeviceMatrix(
            relay=FakeRelaySpec(channel_count=16, mode="stable"),
            relay_8=FakeRelaySpec(channel_count=8, mode="stable"),
            thermometer=FakeThermometerSpec(mode="stable"),
            humidity_generator=FakeHumidityGeneratorSpec(mode="stable", skipped_by_profile=True),
            dewpoint_meter=FakeDewpointMeterSpec(mode="stable", skipped_by_profile=True),
        ),
        execution_mode="protocol",
        baseline_mode="mirror_v2",
        runtime_overrides=FAST_CO2_ROUTE_OVERRIDES,
    ),
    "pace_no_response_cleanup": SimulatedScenarioDefinition(
        name="pace_no_response_cleanup",
        validation_profile=FULL_ROUTE_SIMULATED_DIAGNOSTIC_VALIDATION_PROFILE,
        fixture_name="pace_no_response_on_cleanup",
        description="PACE cleanup fault after a successful route run.",
        diagnostic_only=True,
        target_route="co2",
        device_matrix=SimulatedDeviceMatrix(
            pressure_controller=FakePressureControllerSpec(
                mode="cleanup_no_response",
                faults=[FakeTransportFaultSpec(name="cleanup_no_response", active=True, detail="timeout on vent/isolation")],
            ),
            humidity_generator=FakeHumidityGeneratorSpec(mode="stable", skipped_by_profile=True),
            dewpoint_meter=FakeDewpointMeterSpec(mode="stable", skipped_by_profile=True),
        ),
        execution_mode="protocol",
        baseline_mode="mirror_v2",
        runtime_overrides=FAST_CO2_ONLY_DIAGNOSTIC_OVERRIDES,
    ),
    "pace_no_response_on_cleanup": SimulatedScenarioDefinition(
        name="pace_no_response_on_cleanup",
        validation_profile=FULL_ROUTE_SIMULATED_DIAGNOSTIC_VALIDATION_PROFILE,
        fixture_name="pace_no_response_on_cleanup",
        description="Alias for the PACE cleanup no-response diagnostic scenario.",
        diagnostic_only=True,
        target_route="co2",
        device_matrix=SimulatedDeviceMatrix(
            pressure_controller=FakePressureControllerSpec(
                mode="cleanup_no_response",
                faults=[FakeTransportFaultSpec(name="cleanup_no_response", active=True, detail="timeout on vent/isolation")],
            ),
            humidity_generator=FakeHumidityGeneratorSpec(mode="stable", skipped_by_profile=True),
            dewpoint_meter=FakeDewpointMeterSpec(mode="stable", skipped_by_profile=True),
        ),
        execution_mode="protocol",
        baseline_mode="mirror_v2",
        runtime_overrides=FAST_CO2_ONLY_DIAGNOSTIC_OVERRIDES,
    ),
    "pace_unsupported_header": SimulatedScenarioDefinition(
        name="pace_unsupported_header",
        validation_profile=FULL_ROUTE_SIMULATED_DIAGNOSTIC_VALIDATION_PROFILE,
        fixture_name="compare_generates_partial_artifacts_on_failure",
        description="PACE rejects an in-limits query with a SCPI undefined-header error.",
        diagnostic_only=True,
        target_route="co2",
        device_matrix=SimulatedDeviceMatrix(
            pressure_controller=FakePressureControllerSpec(
                mode="unsupported_header",
                unsupported_headers=[":SOUR:PRES:LEV:IMM:AMPL"],
            ),
            humidity_generator=FakeHumidityGeneratorSpec(mode="stable", skipped_by_profile=True),
            dewpoint_meter=FakeDewpointMeterSpec(mode="stable", skipped_by_profile=True),
        ),
        execution_mode="protocol",
        baseline_mode="mirror_v2",
        runtime_overrides=FAST_CO2_ONLY_DIAGNOSTIC_OVERRIDES,
    ),
    "gauge_no_response": SimulatedScenarioDefinition(
        name="gauge_no_response",
        validation_profile=FULL_ROUTE_SIMULATED_DIAGNOSTIC_VALIDATION_PROFILE,
        fixture_name="gauge_no_response",
        description="Pressure gauge transport fails while the rest of the simulated route remains healthy.",
        diagnostic_only=True,
        target_route="co2",
        device_matrix=SimulatedDeviceMatrix(
            pressure_gauge=FakePressureGaugeSpec(
                mode="no_response",
                faults=[FakeTransportFaultSpec(name="no_response", active=True, detail="read timeout")],
            )
        ),
    ),
    "pressure_reference_degraded": SimulatedScenarioDefinition(
        name="pressure_reference_degraded",
        validation_profile=FULL_ROUTE_SIMULATED_DIAGNOSTIC_VALIDATION_PROFILE,
        fixture_name="gauge_no_response",
        description="Pressure reference degrades while the rest of the simulated route remains healthy.",
        diagnostic_only=True,
        target_route="co2",
        device_matrix=SimulatedDeviceMatrix(
            pressure_gauge=FakePressureGaugeSpec(
                mode="no_response",
                faults=[FakeTransportFaultSpec(name="no_response", active=True, detail="reference read timeout")],
            ),
            humidity_generator=FakeHumidityGeneratorSpec(mode="stable", skipped_by_profile=True),
            dewpoint_meter=FakeDewpointMeterSpec(mode="stable", skipped_by_profile=True),
            relay=FakeRelaySpec(channel_count=16, mode="stable"),
            relay_8=FakeRelaySpec(channel_count=8, mode="stable"),
            thermometer=FakeThermometerSpec(mode="stable"),
        ),
        execution_mode="protocol",
        baseline_mode="mirror_v2",
        runtime_overrides=FAST_CO2_ONLY_DIAGNOSTIC_OVERRIDES,
    ),
    "pressure_gauge_wrong_unit_configuration": SimulatedScenarioDefinition(
        name="pressure_gauge_wrong_unit_configuration",
        validation_profile=FULL_ROUTE_SIMULATED_DIAGNOSTIC_VALIDATION_PROFILE,
        fixture_name="gauge_no_response",
        description="Pressure gauge keeps responding, but the engineering unit configuration is wrong for the route.",
        diagnostic_only=True,
        target_route="co2",
        device_matrix=SimulatedDeviceMatrix(
            pressure_gauge=FakePressureGaugeSpec(
                mode="wrong_unit_configuration",
                unit="PSIA",
            ),
            humidity_generator=FakeHumidityGeneratorSpec(mode="stable", skipped_by_profile=True),
            dewpoint_meter=FakeDewpointMeterSpec(mode="stable", skipped_by_profile=True),
            relay=FakeRelaySpec(channel_count=16, mode="stable"),
            relay_8=FakeRelaySpec(channel_count=8, mode="stable"),
            thermometer=FakeThermometerSpec(mode="stable"),
        ),
        execution_mode="protocol",
        baseline_mode="mirror_v2",
        runtime_overrides=FAST_CO2_ONLY_DIAGNOSTIC_OVERRIDES,
    ),
    "humidity_generator_timeout": SimulatedScenarioDefinition(
        name="humidity_generator_timeout",
        validation_profile=FULL_ROUTE_SIMULATED_DIAGNOSTIC_VALIDATION_PROFILE,
        fixture_name="humidity_generator_timeout",
        description="Humidity generator temperature changes but humidity never converges.",
        diagnostic_only=True,
        target_route="h2o",
        device_matrix=SimulatedDeviceMatrix(
            humidity_generator=FakeHumidityGeneratorSpec(mode="timeout"),
        ),
        execution_mode="protocol",
        baseline_mode="mirror_v2",
        runtime_overrides=_merge_dicts(
            FAST_FULL_ROUTE_OVERRIDES,
            {
                "workflow": {
                    "stability": {
                        "humidity_generator": {
                            "timeout_s": 1.5,
                            "window_s": 0.4,
                            "rh_stable_window_s": 0.4,
                            "rh_stable_span_pct": 0.3,
                            "poll_s": 0.05,
                            "temp_tol_c": 0.5,
                            "rh_tol_pct": 0.5,
                        },
                    }
                }
            },
        ),
    ),
    "resource_locked_serial_port": SimulatedScenarioDefinition(
        name="resource_locked_serial_port",
        validation_profile=SKIP0_CO2_ONLY_SIMULATED_VALIDATION_PROFILE,
        fixture_name="resource_locked_serial_port",
        description="Port busy diagnostic without opening a real serial port.",
        diagnostic_only=True,
        target_route="co2",
        device_matrix=SimulatedDeviceMatrix(
            transport_faults=[FakeTransportFaultSpec(name="serial_port_busy", active=True, detail="simulated port lock")]
        ),
    ),
    "profile_skips_h2o_devices": SimulatedScenarioDefinition(
        name="profile_skips_h2o_devices",
        validation_profile=SKIP0_CO2_ONLY_SIMULATED_VALIDATION_PROFILE,
        fixture_name="profile_skips_h2o_devices",
        description="CO2-only simulated route where H2O devices are explicitly skipped by profile.",
        diagnostic_only=False,
        target_route="co2",
        device_matrix=SimulatedDeviceMatrix(
            humidity_generator=FakeHumidityGeneratorSpec(mode="stable", skipped_by_profile=True),
            dewpoint_meter=FakeDewpointMeterSpec(mode="stable", skipped_by_profile=True),
        ),
        execution_mode="protocol",
        baseline_mode="mirror_v2",
        runtime_overrides=FAST_CO2_ROUTE_OVERRIDES,
    ),
    "primary_latest_missing": SimulatedScenarioDefinition(
        name="primary_latest_missing",
        validation_profile=SKIP0_CO2_ONLY_SIMULATED_VALIDATION_PROFILE,
        fixture_name="primary_latest_missing",
        description="Synthetic snapshot where the primary real latest is missing but stale/diagnostic evidence exists.",
        diagnostic_only=True,
        target_route="co2",
        device_matrix=SimulatedDeviceMatrix(),
    ),
    "stale_h2o_latest_present_but_not_primary": SimulatedScenarioDefinition(
        name="stale_h2o_latest_present_but_not_primary",
        validation_profile=H2O_ONLY_SIMULATED_VALIDATION_PROFILE,
        fixture_name="stale_h2o_latest_present_but_not_primary",
        description="Stale H2O evidence exists but must not be promoted to primary validation state.",
        diagnostic_only=True,
        target_route="h2o",
        device_matrix=SimulatedDeviceMatrix(),
    ),
    "compare_generates_partial_artifacts_on_failure": SimulatedScenarioDefinition(
        name="compare_generates_partial_artifacts_on_failure",
        validation_profile=SKIP0_CO2_ONLY_SIMULATED_VALIDATION_PROFILE,
        fixture_name="compare_generates_partial_artifacts_on_failure",
        description="Failure before route compare still writes bundle/latest/report artifacts.",
        diagnostic_only=True,
        target_route="co2",
        device_matrix=SimulatedDeviceMatrix(
            analyzers=FakeAnalyzerSpec(mode2_stream="no_response", sensor_precheck="strict_fail"),
        ),
        execution_mode="protocol",
        baseline_mode="mirror_v2",
        runtime_overrides=FAST_CO2_ROUTE_OVERRIDES,
    ),
    "co2_route_entered_but_sample_count_mismatch": SimulatedScenarioDefinition(
        name="co2_route_entered_but_sample_count_mismatch",
        validation_profile=SKIP0_CO2_ONLY_SIMULATED_VALIDATION_PROFILE,
        fixture_name="co2_route_entered_but_sample_count_mismatch",
        description="Route enters correctly, but downstream sample counts diverge.",
        diagnostic_only=False,
        target_route="co2",
        device_matrix=SimulatedDeviceMatrix(),
    ),
    "co2_route_entered_sample_mismatch": SimulatedScenarioDefinition(
        name="co2_route_entered_sample_mismatch",
        validation_profile=SKIP0_CO2_ONLY_SIMULATED_VALIDATION_PROFILE,
        fixture_name="co2_route_entered_but_sample_count_mismatch",
        description="Alias for the sample-count mismatch CO2 route scenario.",
        diagnostic_only=False,
        target_route="co2",
        device_matrix=SimulatedDeviceMatrix(),
    ),
    "temperature_chamber_stalled": SimulatedScenarioDefinition(
        name="temperature_chamber_stalled",
        validation_profile=FULL_ROUTE_SIMULATED_DIAGNOSTIC_VALIDATION_PROFILE,
        fixture_name="compare_generates_partial_artifacts_on_failure",
        description="Temperature chamber stalls before reaching target during simulated diagnostic coverage.",
        diagnostic_only=True,
        target_route="co2",
        device_matrix=SimulatedDeviceMatrix(
            temperature_chamber=FakeTemperatureChamberSpec(mode="stalled", soak_behavior="stalled", soak_s=0.2),
        ),
        execution_mode="protocol",
        baseline_mode="mirror_v2",
        runtime_overrides=_merge_dicts(
            FAST_FULL_ROUTE_OVERRIDES,
            {
                "workflow": {
                    "stability": {
                        "temperature": {
                            "timeout_s": 1.2,
                            "soak_after_reach_s": 0.1,
                            "transition_check_window_s": 0.6,
                            "transition_min_delta_c": 0.2,
                            "analyzer_chamber_temp_enabled": False,
                        }
                    }
                }
            },
        ),
    ),
}


def list_simulated_scenarios() -> list[str]:
    return sorted(SCENARIOS)


def get_simulated_scenario(name: str) -> SimulatedScenarioDefinition:
    try:
        return SCENARIOS[str(name)]
    except KeyError as exc:
        raise KeyError(f"unknown simulated scenario: {name}") from exc


def list_simulated_profiles() -> list[str]:
    return sorted(PROFILE_DEFAULTS)


def simulated_profile_defaults(profile: str) -> dict[str, Any]:
    try:
        return dict(PROFILE_DEFAULTS[str(profile)])
    except KeyError as exc:
        raise KeyError(f"unknown simulated profile: {profile}") from exc
