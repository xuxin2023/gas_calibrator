# V1/V2 Control Flow Compare

- Generated at: 2026-04-30T08:33:46.397526+00:00
- Evidence source: simulated
- Evidence state: simulated_protocol
- Diagnostic only: True
- Acceptance evidence: False
- Not real acceptance evidence: True
- Compare status: MATCH
- Overall status: MATCH
- Validation profile: replacement_full_route_simulated_diagnostic
- Temp filter: None
- skip_co2_ppm: [0]
- Key action registry: gas_calibrator.v2.scripts.route_trace_diff.KEY_ACTION_GROUPS
- V1 route trace: D:\gas_calibrator\_handoff\a2_16_crosscheck_suites\smoke_a2_16_crosscheck_20260430\pressure_reference_degraded\v1_route_trace.jsonl
- V2 route trace: D:\gas_calibrator\_handoff\a2_16_crosscheck_suites\smoke_a2_16_crosscheck_20260430\pressure_reference_degraded\v2_route_trace.jsonl
- Route trace diff: D:\gas_calibrator\_handoff\a2_16_crosscheck_suites\smoke_a2_16_crosscheck_20260430\pressure_reference_degraded\route_trace_diff.txt
- Point presence diff: D:\gas_calibrator\_handoff\a2_16_crosscheck_suites\smoke_a2_16_crosscheck_20260430\pressure_reference_degraded\point_presence_diff.json
- Sample count diff: D:\gas_calibrator\_handoff\a2_16_crosscheck_suites\smoke_a2_16_crosscheck_20260430\pressure_reference_degraded\sample_count_diff.json
- Artifact inventory complete: True

## Route Execution
- Target route: co2
- valid_for_route_diff: True
- first_failure_phase: -
- entered_target_route: {'v1': True, 'v2': True}
- target_route_event_count: {'v1': 66, 'v2': 66}
- bench_context: {'co2_0ppm_available': True, 'other_gases_available': True, 'h2o_route_available': True, 'humidity_generator_humidity_feedback_valid': True, 'primary_replacement_route': 'skip0_co2_only_replacement', 'validation_role': 'simulated_diagnostic', 'target_route': 'h2o_then_co2', 'diagnostic_only': True, 'acceptance_evidence': False}
- simulation_context: {'scenario': 'pressure_reference_degraded', 'description': 'Pressure reference degrades while the rest of the simulated route remains healthy.', 'diagnostic_only': True, 'target_route': 'co2', 'execution_mode': 'protocol', 'baseline_mode': 'mirror_v2', 'simulation_backend': 'protocol', 'device_matrix': {'analyzers': {'protocol': 'ygas', 'count': 8, 'mode2_stream': 'stable', 'active_send': True, 'sensor_precheck': 'strict_pass', 'versions': ['v5_plus', 'v5_plus', 'v5_plus', 'v5_plus', 'v5_plus', 'v5_plus', 'v5_plus', 'v5_plus'], 'status_bits': ['0000', '0000', '0000', '0000', '0000', '0000', '0000', '0000'], 'mode_switch_not_applied': False}, 'humidity_generator': {'protocol': 'grz5013', 'mode': 'stable', 'skipped_by_profile': True}, 'dewpoint_meter': {'mode': 'stable', 'skipped_by_profile': True}, 'pressure_controller': {'protocol': 'scpi', 'mode': 'stable', 'unit': 'HPA', 'unsupported_headers': [], 'faults': []}, 'pressure_gauge': {'protocol': 'paroscientific_735_745', 'mode': 'no_response', 'dest_id': '01', 'source_id': '00', 'unit': 'HPA', 'temperature_unit': 'C', 'measurement_mode': 'single', 'unsupported_commands': [], 'faults': [{'name': 'no_response', 'active': True, 'detail': 'reference read timeout'}]}, 'temperature_chamber': {'protocol': 'modbus', 'mode': 'stable', 'soak_behavior': 'on_target', 'ramp_rate_c_per_s': 10.0, 'soak_s': 0.5}, 'relay': {'protocol': 'modbus_rtu', 'channel_count': 16, 'mode': 'stable', 'stuck_channels': [], 'skipped_by_profile': False}, 'relay_8': {'protocol': 'modbus_rtu', 'channel_count': 8, 'mode': 'stable', 'stuck_channels': [], 'skipped_by_profile': False}, 'thermometer': {'protocol': 'ascii_stream', 'mode': 'stable', 'plus_200_mode': False, 'drift_step_c': 0.05, 'skipped_by_profile': False}, 'device_overrides': {}, 'transport_faults': []}, 'runtime_overrides': {'workflow': {'startup_pressure_precheck': {'enabled': False}, 'stability': {'temperature': {'tol': 1.0, 'window_s': 0.4, 'timeout_s': 2.5, 'soak_after_reach_s': 0.1, 'transition_check_window_s': 0.8, 'transition_min_delta_c': 0.05, 'analyzer_chamber_temp_enabled': False}, 'h2o_route': {'preseal_soak_s': 0.05, 'humidity_timeout_policy': 'abort_like_v1'}, 'co2_route': {'preseal_soak_s': 0.05, 'first_point_preseal_soak_s': 0.05, 'post_h2o_zero_ppm_soak_s': 0.05}, 'humidity_generator': {'enabled': False}}, 'pressure': {'pressurize_high_hpa': 1000.0, 'pressurize_wait_after_vent_off_s': 0.0, 'pressurize_timeout_s': 1.5, 'post_stable_sample_delay_s': 0.0, 'co2_post_stable_sample_delay_s': 0.0, 'co2_post_h2o_vent_off_wait_s': 0.0, 'vent_time_s': 0.0, 'vent_transition_timeout_s': 1.0, 'continuous_atmosphere_hold': True, 'vent_hold_interval_s': 0.05, 'stabilize_timeout_s': 1.5, 'restabilize_retries': 0, 'restabilize_retry_interval_s': 0.1}, 'sampling': {'count': 2, 'stable_count': 2, 'interval_s': 0.05, 'h2o_interval_s': 0.05, 'co2_interval_s': 0.05, 'quality': {'enabled': False}}, 'route_mode': 'co2_only', 'selected_temps_c': [20.0], 'skip_co2_ppm': [0], 'humidity_generator': {'ensure_run': False}}, 'devices': {'humidity_generator': {'enabled': False}, 'dewpoint_meter': {'enabled': False}}}, 'protocol_devices': {'analyzer': 'ygas', 'pressure_controller': 'pace_scpi', 'humidity_generator': 'grz5013', 'temperature_chamber': 'modbus', 'relay': 'modbus_rtu', 'relay_8': 'modbus_rtu', 'thermometer': 'ascii_stream'}}
- route_physical_state_match: {'v1': True, 'v2': True}
- relay_physical_mismatch: {'v1': False, 'v2': False}

## Reference Quality
- reference_quality: failed
- reference_integrity: failed
- reference_quality_degraded: True
- thermometer_reference_status: healthy
- pressure_reference_status: no_response
- reasons: ['pressure:no_response']
- V1: ok=True phase=simulated.protocol_baseline entered_target_route=True target_route_event_count=66 first_failure_phase=-
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
- V2: ok=True phase=completed entered_target_route=True target_route_event_count=66 first_failure_phase=-
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
- V2 runtime_policy: collect_only=True collect_only_fast_path=False precheck_device_connection=False precheck_sensor_check=False sensor_precheck_enabled=True sensor_precheck_profile=raw_frame_first sensor_precheck_scope=all_analyzers sensor_precheck_validation_mode=v1_frame_like sensor_precheck_active_send=True sensor_precheck_strict=False expected_disabled_devices=['dewpoint_meter', 'humidity_generator'] h2o_humidity_timeout_policy=abort_like_v1
- V2 effective_v2_compare_config: D:\gas_calibrator\src\gas_calibrator\v2\configs\validation\simulated\replacement_full_route_simulated_diagnostic.json

## Validation Scope
- Summary: Full-route simulated diagnostic coverage injects failures into the complete H2O + CO2 flow to verify failure classification, partial artifact generation, and recovery reporting.
- Proves: Failure classification and artifact integrity across injected faults.
- Proves: Diagnostic compare/report/UI behavior for full-route scenarios.
- Proves: Golden regression of first_failure_phase and evidence-state semantics.
- Does not prove: Real-device acceptance evidence.
- Does not prove: Bench cutover readiness.
- Does not prove: Physical transport or hardware recovery behavior.

## Replacement Validation
- scope: mixed_route_skip0_review_aid
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
- baseline_restore: match=True v1={'route_baseline': 4} v2={'route_baseline': 4}
- cleanup: match=True v1={'cleanup': 2} v2={'cleanup': 2}
- post_pressure_hold: match=True v1={'wait_post_pressure': 8} v2={'wait_post_pressure': 8}
- route_soak: match=True v1={'wait_route_soak': 2} v2={'wait_route_soak': 2}
- sample: match=True v1={'sample_end': 8, 'sample_start': 8} v2={'sample_end': 8, 'sample_start': 8}
- seal: match=True v1={'preseal_final_atmosphere_exit': 2, 'seal_route': 2, 'seal_transition': 2} v2={'preseal_final_atmosphere_exit': 2, 'seal_route': 2, 'seal_transition': 2}
- setpoint_control: match=True v1={'pressure_control_ready_gate': 8, 'set_pressure': 8} v2={'pressure_control_ready_gate': 8, 'set_pressure': 8}
- source_valve_selection: match=True v1={'set_co2_valves': 2} v2={'set_co2_valves': 2}

## Key Actions
- pressure: match=True v1={'seal_route': 2, 'set_pressure': 8, 'wait_post_pressure': 8} v2={'seal_route': 2, 'set_pressure': 8, 'wait_post_pressure': 8}
- sample: match=True v1={'sample_end': 8, 'sample_start': 8} v2={'sample_end': 8, 'sample_start': 8}
- valves: match=True v1={'cleanup': 2, 'route_baseline': 4, 'set_co2_valves': 2} v2={'cleanup': 2, 'route_baseline': 4, 'set_co2_valves': 2}
- vent: match=True v1={'set_vent': 8} v2={'set_vent': 8}
