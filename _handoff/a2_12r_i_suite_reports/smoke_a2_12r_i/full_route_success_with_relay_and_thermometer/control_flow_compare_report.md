# V1/V2 Control Flow Compare

- Generated at: 2026-04-30T04:10:48.200078+00:00
- Evidence source: simulated
- Evidence state: simulated_protocol
- Diagnostic only: False
- Acceptance evidence: False
- Not real acceptance evidence: True
- Compare status: MATCH
- Overall status: MATCH
- Validation profile: replacement_full_route_simulated
- Temp filter: None
- skip_co2_ppm: []
- Key action registry: gas_calibrator.v2.scripts.route_trace_diff.KEY_ACTION_GROUPS
- V1 route trace: D:\gas_calibrator\_handoff\a2_12r_i_suite_reports\smoke_a2_12r_i\full_route_success_with_relay_and_thermometer\v1_route_trace.jsonl
- V2 route trace: D:\gas_calibrator\_handoff\a2_12r_i_suite_reports\smoke_a2_12r_i\full_route_success_with_relay_and_thermometer\v2_route_trace.jsonl
- Route trace diff: D:\gas_calibrator\_handoff\a2_12r_i_suite_reports\smoke_a2_12r_i\full_route_success_with_relay_and_thermometer\route_trace_diff.txt
- Point presence diff: D:\gas_calibrator\_handoff\a2_12r_i_suite_reports\smoke_a2_12r_i\full_route_success_with_relay_and_thermometer\point_presence_diff.json
- Sample count diff: D:\gas_calibrator\_handoff\a2_12r_i_suite_reports\smoke_a2_12r_i\full_route_success_with_relay_and_thermometer\sample_count_diff.json
- Artifact inventory complete: True

## Route Execution
- Target route: -
- valid_for_route_diff: True
- first_failure_phase: -
- entered_target_route: {'v1': True, 'v2': True}
- target_route_event_count: {'v1': 0, 'v2': 0}
- bench_context: {'co2_0ppm_available': True, 'other_gases_available': True, 'h2o_route_available': True, 'humidity_generator_humidity_feedback_valid': True, 'primary_replacement_route': 'skip0_co2_only_replacement', 'validation_role': 'simulated_acceptance_like_coverage', 'target_route': 'h2o_then_co2', 'diagnostic_only': False, 'acceptance_evidence': False}
- simulation_context: {'scenario': 'full_route_success_with_relay_and_thermometer', 'description': 'Full-route success with relay and thermometer protocol devices exercised end-to-end.', 'diagnostic_only': False, 'target_route': 'h2o_then_co2', 'execution_mode': 'protocol', 'baseline_mode': 'mirror_v2', 'simulation_backend': 'protocol', 'device_matrix': {'analyzers': {'protocol': 'ygas', 'count': 8, 'mode2_stream': 'stable', 'active_send': True, 'sensor_precheck': 'strict_pass', 'versions': ['v5_plus', 'v5_plus', 'v5_plus', 'v5_plus', 'v5_plus', 'v5_plus', 'v5_plus', 'v5_plus'], 'status_bits': ['0000', '0000', '0000', '0000', '0000', '0000', '0000', '0000'], 'mode_switch_not_applied': False}, 'humidity_generator': {'protocol': 'grz5013', 'mode': 'stable', 'skipped_by_profile': False}, 'dewpoint_meter': {'mode': 'stable', 'skipped_by_profile': False}, 'pressure_controller': {'protocol': 'scpi', 'mode': 'stable', 'unit': 'HPA', 'unsupported_headers': [], 'faults': []}, 'pressure_gauge': {'protocol': 'paroscientific_735_745', 'mode': 'stable', 'dest_id': '01', 'source_id': '00', 'unit': 'HPA', 'temperature_unit': 'C', 'measurement_mode': 'single', 'unsupported_commands': [], 'faults': []}, 'temperature_chamber': {'protocol': 'modbus', 'mode': 'stable', 'soak_behavior': 'on_target', 'ramp_rate_c_per_s': 10.0, 'soak_s': 0.5}, 'relay': {'protocol': 'modbus_rtu', 'channel_count': 16, 'mode': 'stable', 'stuck_channels': [], 'skipped_by_profile': False}, 'relay_8': {'protocol': 'modbus_rtu', 'channel_count': 8, 'mode': 'stable', 'stuck_channels': [], 'skipped_by_profile': False}, 'thermometer': {'protocol': 'ascii_stream', 'mode': 'stable', 'plus_200_mode': False, 'drift_step_c': 0.05, 'skipped_by_profile': False}, 'device_overrides': {}, 'transport_faults': []}, 'runtime_overrides': {'workflow': {'startup_pressure_precheck': {'enabled': False}, 'stability': {'temperature': {'tol': 1.0, 'window_s': 0.4, 'timeout_s': 2.5, 'soak_after_reach_s': 0.1, 'transition_check_window_s': 0.8, 'transition_min_delta_c': 0.05, 'analyzer_chamber_temp_enabled': False}, 'humidity_generator': {'timeout_s': 3.0, 'window_s': 0.4, 'rh_stable_window_s': 0.4, 'rh_stable_span_pct': 0.8, 'poll_s': 0.05, 'temp_tol_c': 1.0, 'rh_tol_pct': 2.0}, 'dewpoint': {'window_s': 0.5, 'timeout_s': 2.0, 'poll_s': 0.05, 'temp_match_tol_c': 1.0, 'rh_match_tol_pct': 2.0, 'stability_tol_c': 0.5, 'min_samples': 2}, 'h2o_route': {'preseal_soak_s': 0.05, 'humidity_timeout_policy': 'abort_like_v1'}, 'co2_route': {'preseal_soak_s': 0.05, 'first_point_preseal_soak_s': 0.05, 'post_h2o_zero_ppm_soak_s': 0.05}}, 'pressure': {'pressurize_high_hpa': 1000.0, 'pressurize_wait_after_vent_off_s': 0.0, 'pressurize_timeout_s': 1.5, 'post_stable_sample_delay_s': 0.0, 'co2_post_stable_sample_delay_s': 0.0, 'co2_post_h2o_vent_off_wait_s': 0.0, 'vent_time_s': 0.0, 'vent_transition_timeout_s': 1.0, 'continuous_atmosphere_hold': True, 'vent_hold_interval_s': 0.05, 'stabilize_timeout_s': 1.5, 'restabilize_retries': 0, 'restabilize_retry_interval_s': 0.1}, 'sampling': {'count': 2, 'stable_count': 2, 'interval_s': 0.05, 'h2o_interval_s': 0.05, 'co2_interval_s': 0.05, 'quality': {'enabled': False}}}}, 'protocol_devices': {'analyzer': 'ygas', 'pressure_controller': 'pace_scpi', 'humidity_generator': 'grz5013', 'temperature_chamber': 'modbus', 'relay': 'modbus_rtu', 'relay_8': 'modbus_rtu', 'thermometer': 'ascii_stream'}}
- route_physical_state_match: {'v1': True, 'v2': True}
- relay_physical_mismatch: {'v1': False, 'v2': False}

## Reference Quality
- reference_quality: healthy
- reference_integrity: healthy
- reference_quality_degraded: False
- thermometer_reference_status: healthy
- pressure_reference_status: healthy
- reasons: []
- V1: ok=True phase=simulated.protocol_baseline entered_target_route=True target_route_event_count=0 first_failure_phase=-
- V1 target_open_valves: []
- V1 actual_open_valves: []
- V1 target_relay_state: {'relay_a': {'1': False, '2': False, '3': False, '4': False, '5': False, '6': False, '7': False, '8': False, '9': False, '10': False, '11': False, '12': False, '15': False, '16': False}, 'relay_b': {'1': False, '2': False, '3': False, '8': False}}
- V1 actual_relay_state: {'relay_a': {'1': False, '2': False, '3': False, '4': False, '5': False, '6': False, '7': False, '8': False, '9': False, '10': False, '11': False, '12': False, '15': False, '16': False}, 'relay_b': {'1': False, '2': False, '3': False, '8': False}}
- V1 route_physical_state_match: True
- V1 relay_physical_mismatch: False
- V1 mismatched_valves: []
- V1 mismatched_channels: []
- V1 cleanup_all_relays_off: True
- V1 cleanup_relay_state: {'relay_a': {'1': False, '2': False, '3': False, '4': False, '5': False, '6': False, '7': False, '8': False, '9': False, '10': False, '11': False, '12': False, '15': False, '16': False}, 'relay_b': {'1': False, '2': False, '3': False, '8': False}}
- V1 runtime_policy: collect_only=None collect_only_fast_path=None precheck_device_connection=None precheck_sensor_check=None sensor_precheck_enabled=None sensor_precheck_profile=None sensor_precheck_scope=None sensor_precheck_validation_mode=None sensor_precheck_active_send=None sensor_precheck_strict=None expected_disabled_devices=None h2o_humidity_timeout_policy=None
- V2: ok=True phase=completed entered_target_route=True target_route_event_count=0 first_failure_phase=-
- V2 target_open_valves: []
- V2 actual_open_valves: []
- V2 target_relay_state: {'relay_a': {'1': False, '2': False, '3': False, '4': False, '5': False, '6': False, '7': False, '8': False, '9': False, '10': False, '11': False, '12': False, '15': False, '16': False}, 'relay_b': {'1': False, '2': False, '3': False, '8': False}}
- V2 actual_relay_state: {'relay_a': {'1': False, '2': False, '3': False, '4': False, '5': False, '6': False, '7': False, '8': False, '9': False, '10': False, '11': False, '12': False, '15': False, '16': False}, 'relay_b': {'1': False, '2': False, '3': False, '8': False}}
- V2 route_physical_state_match: True
- V2 relay_physical_mismatch: False
- V2 mismatched_valves: []
- V2 mismatched_channels: []
- V2 cleanup_all_relays_off: True
- V2 cleanup_relay_state: {'relay_a': {'1': False, '2': False, '3': False, '4': False, '5': False, '6': False, '7': False, '8': False, '9': False, '10': False, '11': False, '12': False, '15': False, '16': False}, 'relay_b': {'1': False, '2': False, '3': False, '8': False}}
- V2 runtime_policy: collect_only=True collect_only_fast_path=False precheck_device_connection=True precheck_sensor_check=False sensor_precheck_enabled=True sensor_precheck_profile=raw_frame_first sensor_precheck_scope=all_analyzers sensor_precheck_validation_mode=v1_frame_like sensor_precheck_active_send=True sensor_precheck_strict=True expected_disabled_devices=[] h2o_humidity_timeout_policy=abort_like_v1
- V2 effective_v2_compare_config: D:\gas_calibrator\src\gas_calibrator\v2\configs\validation\simulated\replacement_full_route_simulated.json

## Validation Scope
- Summary: Full-route simulated replacement coverage exercises 0 ppm, H2O, CO2, multiple temperatures, multiple pressures, and full route/action ordering without touching real devices.
- Proves: Full logical route/action coverage for H2O + CO2 + 0 ppm in simulation.
- Proves: Compare/report/latest/bundle generation can be regression-tested without bench access.
- Proves: UI validation cockpit can consume full-route evidence in a device-free environment.
- Does not prove: Real-device acceptance evidence.
- Does not prove: Bench cutover readiness.
- Does not prove: Physical device timing, transport, or numeric equivalence.

## Replacement Validation
- scope: standard_compare
- scope_statement: -
- conclusion: replacement-validation path usable
- path_usable: True
- cutover_ready: False
- default_replacement_ready: False
- full_equivalence_established: False
- numeric_equivalence_established: False
- evidence_state: simulated_protocol
- first_failure_phase: -
- presence_evaluable: True
- sample_count_evaluable: True
- route_action_order_evaluable: True
- only_in_v1: -
- only_in_v2: -
- missing_points: {'missing_in_v1': [], 'missing_in_v2': []}
- sample_count_mismatch: False
- sample_count_matches: True
- route_action_order_matches: True

## Presence
- Match: True
- V1 only: -
- V2 only: -

## Sample Count
- Match: True
- No sample-count mismatches

## Route Sequence
- Match: True

## Route Review Stages
- baseline_restore: match=True v1={'route_baseline': 14} v2={'route_baseline': 14}
- cleanup: match=True v1={'cleanup': 10} v2={'cleanup': 10}
- post_pressure_hold: match=True v1={'wait_post_pressure': 40} v2={'wait_post_pressure': 40}
- route_soak: match=True v1={'wait_dewpoint': 4, 'wait_humidity': 4, 'wait_route_ready': 4, 'wait_route_soak': 6} v2={'wait_dewpoint': 4, 'wait_humidity': 4, 'wait_route_ready': 4, 'wait_route_soak': 6}
- sample: match=True v1={'sample_end': 40, 'sample_start': 40} v2={'sample_end': 40, 'sample_start': 40}
- seal: match=True v1={'preseal_final_atmosphere_exit': 10, 'seal_route': 10, 'seal_transition': 10} v2={'preseal_final_atmosphere_exit': 10, 'seal_route': 10, 'seal_transition': 10}
- setpoint_control: match=True v1={'pressure_control_ready_gate': 40, 'set_pressure': 40} v2={'pressure_control_ready_gate': 40, 'set_pressure': 40}
- source_valve_selection: match=True v1={'set_co2_valves': 8, 'set_h2o_path': 12} v2={'set_co2_valves': 8, 'set_h2o_path': 12}

## Key Actions
- pressure: match=True v1={'seal_route': 10, 'set_pressure': 40, 'wait_post_pressure': 40} v2={'seal_route': 10, 'set_pressure': 40, 'wait_post_pressure': 40}
- sample: match=True v1={'sample_end': 40, 'sample_start': 40} v2={'sample_end': 40, 'sample_start': 40}
- valves: match=True v1={'cleanup': 10, 'route_baseline': 14, 'set_co2_valves': 8, 'set_h2o_path': 12} v2={'cleanup': 10, 'route_baseline': 14, 'set_co2_valves': 8, 'set_h2o_path': 12}
- vent: match=True v1={'set_vent': 42} v2={'set_vent': 42}
