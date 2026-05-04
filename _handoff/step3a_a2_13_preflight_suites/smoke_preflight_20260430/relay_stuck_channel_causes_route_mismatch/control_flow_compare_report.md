# V1/V2 Control Flow Compare

- Generated at: 2026-04-30T06:13:41.651670+00:00
- Evidence source: simulated
- Evidence state: simulated_protocol
- Diagnostic only: True
- Acceptance evidence: False
- Not real acceptance evidence: True
- Compare status: MISMATCH
- Overall status: MISMATCH
- Validation profile: replacement_h2o_only_simulated
- Temp filter: None
- skip_co2_ppm: []
- Key action registry: gas_calibrator.v2.scripts.route_trace_diff.KEY_ACTION_GROUPS
- V1 route trace: D:\gas_calibrator\_handoff\step3a_a2_13_preflight_suites\smoke_preflight_20260430\relay_stuck_channel_causes_route_mismatch\v1_route_trace.jsonl
- V2 route trace: D:\gas_calibrator\_handoff\step3a_a2_13_preflight_suites\smoke_preflight_20260430\relay_stuck_channel_causes_route_mismatch\v2_route_trace.jsonl
- Route trace diff: D:\gas_calibrator\_handoff\step3a_a2_13_preflight_suites\smoke_preflight_20260430\relay_stuck_channel_causes_route_mismatch\route_trace_diff.txt
- Point presence diff: D:\gas_calibrator\_handoff\step3a_a2_13_preflight_suites\smoke_preflight_20260430\relay_stuck_channel_causes_route_mismatch\point_presence_diff.json
- Sample count diff: D:\gas_calibrator\_handoff\step3a_a2_13_preflight_suites\smoke_preflight_20260430\relay_stuck_channel_causes_route_mismatch\sample_count_diff.json
- Artifact inventory complete: True

## Route Execution
- Target route: h2o
- valid_for_route_diff: False
- first_failure_phase: -
- entered_target_route: {'v1': True, 'v2': True}
- target_route_event_count: {'v1': 50, 'v2': 50}
- bench_context: {'co2_0ppm_available': True, 'other_gases_available': True, 'h2o_route_available': True, 'humidity_generator_humidity_feedback_valid': True, 'primary_replacement_route': 'skip0_co2_only_replacement', 'validation_role': 'simulated_diagnostic', 'target_route': 'h2o', 'diagnostic_only': True, 'acceptance_evidence': False}
- simulation_context: {'scenario': 'relay_stuck_channel_causes_route_mismatch', 'description': 'H2O route command succeeds logically but a stuck relay channel prevents the route from entering H2O.', 'diagnostic_only': True, 'target_route': 'h2o', 'execution_mode': 'protocol', 'baseline_mode': 'mirror_v2', 'simulation_backend': 'protocol', 'device_matrix': {'analyzers': {'protocol': 'ygas', 'count': 8, 'mode2_stream': 'stable', 'active_send': True, 'sensor_precheck': 'strict_pass', 'versions': ['v5_plus', 'v5_plus', 'v5_plus', 'v5_plus', 'v5_plus', 'v5_plus', 'v5_plus', 'v5_plus'], 'status_bits': ['0000', '0000', '0000', '0000', '0000', '0000', '0000', '0000'], 'mode_switch_not_applied': False}, 'humidity_generator': {'protocol': 'grz5013', 'mode': 'stable', 'skipped_by_profile': False}, 'dewpoint_meter': {'mode': 'stable', 'skipped_by_profile': False}, 'pressure_controller': {'protocol': 'scpi', 'mode': 'stable', 'unit': 'HPA', 'unsupported_headers': [], 'faults': []}, 'pressure_gauge': {'protocol': 'paroscientific_735_745', 'mode': 'stable', 'dest_id': '01', 'source_id': '00', 'unit': 'HPA', 'temperature_unit': 'C', 'measurement_mode': 'single', 'unsupported_commands': [], 'faults': []}, 'temperature_chamber': {'protocol': 'modbus', 'mode': 'stable', 'soak_behavior': 'on_target', 'ramp_rate_c_per_s': 10.0, 'soak_s': 0.5}, 'relay': {'protocol': 'modbus_rtu', 'channel_count': 16, 'mode': 'stable', 'stuck_channels': [], 'skipped_by_profile': False}, 'relay_8': {'protocol': 'modbus_rtu', 'channel_count': 8, 'mode': 'stuck_channel', 'stuck_channels': [1, 2, 8], 'skipped_by_profile': False}, 'thermometer': {'protocol': 'ascii_stream', 'mode': 'stable', 'plus_200_mode': False, 'drift_step_c': 0.05, 'skipped_by_profile': False}, 'device_overrides': {}, 'transport_faults': []}, 'runtime_overrides': {'workflow': {'startup_pressure_precheck': {'enabled': False}, 'stability': {'temperature': {'tol': 1.0, 'window_s': 0.4, 'timeout_s': 2.5, 'soak_after_reach_s': 0.1, 'transition_check_window_s': 0.8, 'transition_min_delta_c': 0.05, 'analyzer_chamber_temp_enabled': False}, 'humidity_generator': {'timeout_s': 3.0, 'window_s': 0.4, 'rh_stable_window_s': 0.4, 'rh_stable_span_pct': 0.8, 'poll_s': 0.05, 'temp_tol_c': 1.0, 'rh_tol_pct': 2.0}, 'dewpoint': {'window_s': 0.5, 'timeout_s': 2.0, 'poll_s': 0.05, 'temp_match_tol_c': 1.0, 'rh_match_tol_pct': 2.0, 'stability_tol_c': 0.5, 'min_samples': 2}, 'h2o_route': {'preseal_soak_s': 0.05, 'humidity_timeout_policy': 'abort_like_v1'}, 'co2_route': {'preseal_soak_s': 0.05, 'first_point_preseal_soak_s': 0.05, 'post_h2o_zero_ppm_soak_s': 0.05}}, 'pressure': {'pressurize_high_hpa': 1000.0, 'pressurize_wait_after_vent_off_s': 0.0, 'pressurize_timeout_s': 1.5, 'post_stable_sample_delay_s': 0.0, 'co2_post_stable_sample_delay_s': 0.0, 'co2_post_h2o_vent_off_wait_s': 0.0, 'vent_time_s': 0.0, 'vent_transition_timeout_s': 1.0, 'continuous_atmosphere_hold': True, 'vent_hold_interval_s': 0.05, 'stabilize_timeout_s': 1.5, 'restabilize_retries': 0, 'restabilize_retry_interval_s': 0.1}, 'sampling': {'count': 2, 'stable_count': 2, 'interval_s': 0.05, 'h2o_interval_s': 0.05, 'co2_interval_s': 0.05, 'quality': {'enabled': False}}}}, 'protocol_devices': {'analyzer': 'ygas', 'pressure_controller': 'pace_scpi', 'humidity_generator': 'grz5013', 'temperature_chamber': 'modbus', 'relay': 'modbus_rtu', 'relay_8': 'modbus_rtu', 'thermometer': 'ascii_stream'}}
- route_physical_state_match: {'v1': False, 'v2': False}
- relay_physical_mismatch: {'v1': True, 'v2': True}
- Reason: target route physical relay state did not match commanded valves on: v1, v2

## Reference Quality
- reference_quality: healthy
- reference_integrity: healthy
- reference_quality_degraded: False
- thermometer_reference_status: healthy
- pressure_reference_status: healthy
- reasons: []
- V1: ok=True phase=simulated.protocol_baseline entered_target_route=True target_route_event_count=50 first_failure_phase=-
- V1 target_open_valves: []
- V1 actual_open_valves: []
- V1 target_relay_state: {'relay_a': {'1': False, '2': False, '3': False, '4': False, '5': False, '6': False, '7': False, '8': False, '9': False, '10': False, '11': False, '12': False, '15': False, '16': False}, 'relay_b': {'1': False, '2': False, '3': False, '8': False}}
- V1 actual_relay_state: {'relay_a': {'1': False, '2': False, '3': False, '4': False, '5': False, '6': False, '7': False, '8': False, '9': False, '10': False, '11': False, '12': False, '15': False, '16': False}, 'relay_b': {'1': False, '2': False, '3': False, '8': False}}
- V1 route_physical_state_match: False
- V1 relay_physical_mismatch: True
- V1 mismatched_valves: []
- V1 mismatched_channels: []
- V1 cleanup_all_relays_off: True
- V1 cleanup_relay_state: {'relay_a': {'1': False, '2': False, '3': False, '4': False, '5': False, '6': False, '7': False, '8': False, '9': False, '10': False, '11': False, '12': False, '15': False, '16': False}, 'relay_b': {'1': False, '2': False, '3': False, '8': False}}
- V1 runtime_policy: collect_only=None collect_only_fast_path=None precheck_device_connection=None precheck_sensor_check=None sensor_precheck_enabled=None sensor_precheck_profile=None sensor_precheck_scope=None sensor_precheck_validation_mode=None sensor_precheck_active_send=None sensor_precheck_strict=None expected_disabled_devices=None h2o_humidity_timeout_policy=None
- V2: ok=True phase=completed entered_target_route=True target_route_event_count=50 first_failure_phase=-
- V2 target_open_valves: []
- V2 actual_open_valves: []
- V2 target_relay_state: {'relay_a': {'1': False, '2': False, '3': False, '4': False, '5': False, '6': False, '7': False, '8': False, '9': False, '10': False, '11': False, '12': False, '15': False, '16': False}, 'relay_b': {'1': False, '2': False, '3': False, '8': False}}
- V2 actual_relay_state: {'relay_a': {'1': False, '2': False, '3': False, '4': False, '5': False, '6': False, '7': False, '8': False, '9': False, '10': False, '11': False, '12': False, '15': False, '16': False}, 'relay_b': {'1': False, '2': False, '3': False, '8': False}}
- V2 route_physical_state_match: False
- V2 relay_physical_mismatch: True
- V2 mismatched_valves: []
- V2 mismatched_channels: []
- V2 cleanup_all_relays_off: True
- V2 cleanup_relay_state: {'relay_a': {'1': False, '2': False, '3': False, '4': False, '5': False, '6': False, '7': False, '8': False, '9': False, '10': False, '11': False, '12': False, '15': False, '16': False}, 'relay_b': {'1': False, '2': False, '3': False, '8': False}}
- V2 runtime_policy: collect_only=True collect_only_fast_path=False precheck_device_connection=False precheck_sensor_check=False sensor_precheck_enabled=True sensor_precheck_profile=raw_frame_first sensor_precheck_scope=first_analyzer_only sensor_precheck_validation_mode=v1_frame_like sensor_precheck_active_send=True sensor_precheck_strict=False expected_disabled_devices=[] h2o_humidity_timeout_policy=abort_like_v1
- V2 effective_v2_compare_config: D:\gas_calibrator\src\gas_calibrator\v2\configs\validation\simulated\replacement_h2o_only_simulated.json

## Validation Scope
- Summary: Simulated H2O-only route coverage exercises H2O route-entry, stability, timeout, and cleanup logic without the current humidity-generator hardware constraints.
- Proves: H2O route logic can be regression-tested without a healthy humidity generator.
- Proves: Diagnostic H2O compare/report/UI paths remain stable while H2O is out of scope on the real bench.
- Proves: Timeout and early-stop classification can be replayed consistently.
- Does not prove: Real H2O acceptance evidence.
- Does not prove: Humidity-generator physical behavior on the bench.
- Does not prove: Bench cutover readiness.

## Replacement Validation
- scope: standard_compare
- scope_statement: -
- conclusion: replacement-validation path not usable
- path_usable: False
- cutover_ready: False
- default_replacement_ready: False
- full_equivalence_established: False
- numeric_equivalence_established: False
- evidence_state: simulated_protocol
- first_failure_phase: -
- presence_evaluable: False
- sample_count_evaluable: False
- route_action_order_evaluable: False
- only_in_v1: -
- only_in_v2: -
- missing_points: {'missing_in_v1': [], 'missing_in_v2': []}
- sample_count_mismatch: None
- sample_count_matches: None
- route_action_order_matches: None

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
- baseline_restore: match=True v1={} v2={}
- cleanup: match=True v1={'cleanup': 2} v2={'cleanup': 2}
- post_pressure_hold: match=True v1={'wait_post_pressure': 4} v2={'wait_post_pressure': 4}
- route_soak: match=True v1={'wait_dewpoint': 2, 'wait_humidity': 2, 'wait_route_ready': 2} v2={'wait_dewpoint': 2, 'wait_humidity': 2, 'wait_route_ready': 2}
- sample: match=True v1={'sample_end': 4, 'sample_start': 4} v2={'sample_end': 4, 'sample_start': 4}
- seal: match=True v1={'preseal_final_atmosphere_exit': 2, 'seal_route': 2, 'seal_transition': 2} v2={'preseal_final_atmosphere_exit': 2, 'seal_route': 2, 'seal_transition': 2}
- setpoint_control: match=True v1={'pressure_control_ready_gate': 4, 'set_pressure': 4} v2={'pressure_control_ready_gate': 4, 'set_pressure': 4}
- source_valve_selection: match=True v1={'set_h2o_path': 6} v2={'set_h2o_path': 6}

## Key Actions
- pressure: match=True v1={'seal_route': 2, 'set_pressure': 4, 'wait_post_pressure': 4} v2={'seal_route': 2, 'set_pressure': 4, 'wait_post_pressure': 4}
- sample: match=True v1={'sample_end': 4, 'sample_start': 4} v2={'sample_end': 4, 'sample_start': 4}
- valves: match=True v1={'cleanup': 2, 'set_h2o_path': 6} v2={'cleanup': 2, 'set_h2o_path': 6}
- vent: match=True v1={'set_vent': 8} v2={'set_vent': 8}
