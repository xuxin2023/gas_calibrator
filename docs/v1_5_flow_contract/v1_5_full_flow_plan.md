# V1.5 Full Calibration Flow Plan

- run_id: `v1_5_contract_reference`
- contract: `pressure_first_temperature_review_then_open_flow_components`
- dry_run_only: `True`

## Safety Contract

- `does_not_modify_run_app`: `True`
- `planner_opens_com_ports`: `False`
- `planner_controls_routes`: `False`
- `planner_writes_coefficients`: `False`
- `planner_writes_device_id`: `False`
- `v2_real_com_forbidden`: `True`
- `uses_validated_v1_5_behavior_not_folder_name_only`: `True`
- `reference_serial_bank_shift_default_enabled`: `False`
- `reference_serial_bank_shift_allowed_scope`: `COM24-COM31_between_COM16-COM23_only`
- `reference_serial_protocol_match_default_enabled`: `False`
- `reference_serial_protocol_match_source`: `operator_or_ui_supplied_inventory_only_no_default_real_COM_probe`
- `gas_analyzer_serial_ports_protected`: `COM35-COM42_use_MODE2_identity_binding`

## Coefficient Epoch Contract

- `initialization`: `plan_initialization_contract_then_read_and_freeze_GETCO1_to_GETCO9_before_auxiliary_neutralization_and_sampling`
- `do_not_clear_existing_coefficients_on_startup`: `False`
- `clear_or_neutralize_auxiliary_groups_after_epoch0_snapshot`: `SENCO5,SENCO6,SENCO7,SENCO8,SENCO9`
- `displayed_values_are_coefficient_affected`: `True`
- `fit_primary_evidence`: `MODE2_factory_ratio_raw_ratio_signal_plus_traceable_reference`
- `new_epoch_after_verified_write`: `True`
- `identity_key`: `analyzer_device_id_not_com_port_or_ga_alias`

## Steps

### 1. Prepare V1.5 formal plan and certificate snapshots

- step_id: `load_plan_and_traceability`
- phase: `LOAD_PLAN`
- execution_mode: `offline_sidecar`
- gate: `required_before_sampling`
- opens_com_ports: `False`
- controls_gas_route: `False`
- controls_water_route: `False`
- writes_coefficients: `False`
- coefficient_epoch_event: `none`
- physical_meaning: Freeze traceability inputs before any physical sampling.

```powershell
python -m gas_calibrator.tools.prepare_v1_5_formal_run_package --output-dir D:\gas_calibrator\_worktrees\v1_5_fixed_wait_window_gate_1aee26d_clean\docs\v1_5_flow_contract\formal_run_package --operator V1.5-contract-refresh --analyzer-id multi_device --run-id v1_5_contract_reference --config D:\gas_calibrator\_worktrees\v1_5_fixed_wait_window_gate_1aee26d_clean\configs\default_config.json
```

### 2. Generate formal initialization contract and DB bundle

- step_id: `formal_initialization_contract_plan`
- phase: `INITIALIZATION_CONTRACT`
- execution_mode: `offline_sidecar`
- gate: `required_before_identity_getco_snapshot`
- opens_com_ports: `False`
- controls_gas_route: `False`
- controls_water_route: `False`
- writes_coefficients: `False`
- coefficient_epoch_event: `none`
- physical_meaning: Before any real analyzer contact, freeze the V1.5 initialization contract: SN/device_code identity, PostgreSQL 18 DB preflight, MODE2/1Hz/filter setup, S7/S8 neutral policy, CHECK monitor timing, and route-readiness gates. This planner command does not execute SN writes, COM reads, SENCO writes, pressure control, or open-flow routes.

```powershell
python -m gas_calibrator.tools.run_v1_5_formal_initialization_runner --config D:\gas_calibrator\_worktrees\v1_5_fixed_wait_window_gate_1aee26d_clean\configs\default_config.json --output-dir D:\gas_calibrator\_worktrees\v1_5_fixed_wait_window_gate_1aee26d_clean\docs\v1_5_flow_contract\formal_initialization --run-id v1_5_contract_reference_initialization --operator V1.5-contract-refresh
```

- note: Do not pass --execute from the full-flow supervised planner.
- note: The generated initialization commands remain a review artifact until a dedicated controlled tool is authorized.

### 3. Export initialization readiness sidecar before live gates

- step_id: `initialization_readiness_snapshot`
- phase: `INITIALIZATION_READINESS`
- execution_mode: `offline_sidecar`
- gate: `required_before_identity_getco_snapshot`
- opens_com_ports: `False`
- controls_gas_route: `False`
- controls_water_route: `False`
- writes_coefficients: `False`
- coefficient_epoch_event: `none`
- physical_meaning: Generate the offline readiness/sidecar view that explains which initialization evidence is present or missing before the first real COM identity gate. Missing live evidence remains a gate; this step only reports it.

```powershell
python -m gas_calibrator.tools.export_v1_5_initialization_readiness --run-dir D:\gas_calibrator\_worktrees\v1_5_fixed_wait_window_gate_1aee26d_clean\docs\v1_5_flow_contract\formal_initialization --config D:\gas_calibrator\_worktrees\v1_5_fixed_wait_window_gate_1aee26d_clean\configs\default_config.json --getco-snapshot-dir D:\gas_calibrator\_worktrees\v1_5_fixed_wait_window_gate_1aee26d_clean\docs\v1_5_flow_contract\coefficient_epoch_0_getco_snapshot --aux-neutralization-dir D:\gas_calibrator\_worktrees\v1_5_fixed_wait_window_gate_1aee26d_clean\docs\v1_5_flow_contract\auxiliary_senco56789_neutralization --output-dir D:\gas_calibrator\_worktrees\v1_5_fixed_wait_window_gate_1aee26d_clean\docs\v1_5_flow_contract\formal_initialization
```

- note: This readiness exporter reads files only; it does not open COM or connect to PostgreSQL.
- note: PostgreSQL 18 and SN/device_code identity remain pre-open-flow requirements, not implicit repairs.

### 4. Summarize pre-gas readiness gates before live identity

- step_id: `pre_gas_readiness_snapshot`
- phase: `PRE_GAS_READINESS`
- execution_mode: `offline_sidecar`
- gate: `required_before_identity_getco_snapshot`
- opens_com_ports: `False`
- controls_gas_route: `False`
- controls_water_route: `False`
- writes_coefficients: `False`
- coefficient_epoch_event: `none`
- physical_meaning: Before any live identity or open-flow action, collapse initialization readiness into a single pre-gas gap list: SN/device_code, PostgreSQL 18, MODE2/1Hz, GETCO epoch 0, S7/S8 neutral, SENCO9 pressure completion, route readiness, and CHECK timing. This is only a sidecar; pending live gates are not treated as release evidence.

```powershell
python -m gas_calibrator.tools.export_v1_5_pre_gas_readiness --run-dir D:\gas_calibrator\_worktrees\v1_5_fixed_wait_window_gate_1aee26d_clean\docs\v1_5_flow_contract --initialization-dir D:\gas_calibrator\_worktrees\v1_5_fixed_wait_window_gate_1aee26d_clean\docs\v1_5_flow_contract\formal_initialization --config D:\gas_calibrator\_worktrees\v1_5_fixed_wait_window_gate_1aee26d_clean\configs\default_config.json --initialization-readiness-json D:\gas_calibrator\_worktrees\v1_5_fixed_wait_window_gate_1aee26d_clean\docs\v1_5_flow_contract\formal_initialization\v1_5_initialization_readiness.json --database-sidecar-json D:\gas_calibrator\_worktrees\v1_5_fixed_wait_window_gate_1aee26d_clean\docs\v1_5_flow_contract\formal_initialization\v1_5_initialization_database_sidecar.json --output-dir D:\gas_calibrator\_worktrees\v1_5_fixed_wait_window_gate_1aee26d_clean\docs\v1_5_flow_contract\pre_gas_readiness
```

- note: This sidecar does not open COM, connect PostgreSQL, control PACE/valves, or write coefficients.
- note: CO2/H2O route control remains only in the mature V1.5 queue runners.

### 5. Read device IDs and GETCO1-9 before calibration

- step_id: `device_identity_and_getco_snapshot`
- phase: `PRECHECK`
- execution_mode: `read_only_real_com_requires_authorization`
- gate: `required_before_any_write`
- opens_com_ports: `True`
- controls_gas_route: `False`
- controls_water_route: `False`
- writes_coefficients: `False`
- coefficient_epoch_event: `start_epoch_0`
- physical_meaning: Existing internal coefficients affect displayed CO2/H2O, pressure, and temperature values. The pre-run GETCO snapshot defines coefficient epoch 0 and freezes the runtime port-to-device-ID binding; COM ports are only transport.

```powershell
python -m gas_calibrator.tools.probe_v1_5_getco_component_snapshot --config D:\gas_calibrator\_worktrees\v1_5_fixed_wait_window_gate_1aee26d_clean\configs\default_config.json --output-dir D:\gas_calibrator\_worktrees\v1_5_fixed_wait_window_gate_1aee26d_clean\docs\v1_5_flow_contract\coefficient_epoch_0_getco_snapshot --groups 1,2,3,4,5,6,7,8,9 --response-timeout-s 2.5 --command-gap-s 1.2 --attempts-per-group 3 --pre-drain-s 0.5 --identity-timeout-s 5.0 --include-legacy --allow-runtime-identity-rebind
```

- note: Do not write analyzer IDs.
- note: After this immutable epoch-0 snapshot, auxiliary groups SENCO5/6/7/8/9 must be neutralized or cleared by controlled tools before pressure/component sampling.
- note: Subsequent physical stages use runtime_identity_bound_config.json generated by this step.

### 6. Validate identity-bound GETCO epoch-0 evidence

- step_id: `identity_getco_readiness_snapshot`
- phase: `IDENTITY_GETCO_READINESS`
- execution_mode: `offline_sidecar`
- gate: `required_after_epoch0_getco_before_auxiliary_neutralization`
- opens_com_ports: `False`
- controls_gas_route: `False`
- controls_water_route: `False`
- writes_coefficients: `False`
- coefficient_epoch_event: `none`
- physical_meaning: After the authorized read-only GETCO probe, verify that every active analyzer has a bound runtime device ID, complete GETCO1-9 epoch-0 backup, no-write conclusion, and frozen runtime_identity_bound_config before auxiliary SENCO5/6/7/8/9 neutralization.

```powershell
python -m gas_calibrator.tools.export_v1_5_getco_identity_readiness --getco-dir D:\gas_calibrator\_worktrees\v1_5_fixed_wait_window_gate_1aee26d_clean\docs\v1_5_flow_contract\coefficient_epoch_0_getco_snapshot --output-dir D:\gas_calibrator\_worktrees\v1_5_fixed_wait_window_gate_1aee26d_clean\docs\v1_5_flow_contract\identity_getco_readiness --fail-on-not-ready
```

- note: This sidecar only reads artifacts; it does not open COM, write device IDs, write SENCO, or control routes.
- note: Missing GETCO artifacts remain a live identity gate, not automatic release evidence.

### 7. Neutralize auxiliary SENCO5-9 after epoch-0 GETCO backup

- step_id: `auxiliary_senco56789_neutralization_gate`
- phase: `AUXILIARY_COEFFICIENT_NEUTRALIZATION`
- execution_mode: `blocked_pending_explicit_authorization`
- gate: `required_after_epoch0_getco_before_pressure_and_component_sampling`
- opens_com_ports: `True`
- controls_gas_route: `False`
- controls_water_route: `False`
- writes_coefficients: `True`
- coefficient_epoch_event: `start_epoch_auxiliary_neutralized_after_epoch_0`
- physical_meaning: SENCO5/SENCO6 are final CO2/H2O displayed-concentration affine trims; SENCO7/SENCO8 shape analyzer temperature inputs; SENCO9 shapes analyzer pressure input. These auxiliary layers must not silently contaminate the pressure quick-check, CO2 ratio fit, or H2O ratio fit. The correct sequence is immutable backup first, then controlled neutralization.

- note: Use run_v1_5_co2_senco5_neutral_controlled_write for SENCO5.
- note: Use run_v1_5_h2o_senco6_neutral_controlled_write for SENCO6.
- note: Use run_v1_5_temperature_senco78_neutral_controlled_write for SENCO7/SENCO8.
- note: Use run_v1_5_pressure_senco9_clear_controlled_write for SENCO9 neutral/clear before pressure-channel recovery.
- note: The full-flow planner records this as a required gate but never auto-executes these writes.

### 8. Verify analyzer pressure input against COM22 at atmosphere

- step_id: `pressure_quick_check`
- phase: `PRESSURE_CHANNEL_QUICK_CHECK`
- execution_mode: `real_com_pressure_only_requires_authorization`
- gate: `block_component_write_if_failed`
- opens_com_ports: `True`
- controls_gas_route: `False`
- controls_water_route: `False`
- writes_coefficients: `False`
- coefficient_epoch_event: `none`
- physical_meaning: Pressure P is an input to analyzer compensation and must be verified before component fitting.

```powershell
python -m gas_calibrator.tools.validate_pressure_only --config D:\gas_calibrator\_worktrees\v1_5_fixed_wait_window_gate_1aee26d_clean\docs\v1_5_flow_contract\coefficient_epoch_0_getco_snapshot\runtime_identity_bound_config.json --output-dir D:\gas_calibrator\_worktrees\v1_5_fixed_wait_window_gate_1aee26d_clean\docs\v1_5_flow_contract\pressure_channel --run-id v1_5_contract_reference_pressure_quick --pressure-points ambient --count 10 --interval-s 1 --continuous-atmosphere-hold --require-continuous-atmosphere-hold --no-prompt
```

### 9. If pressure fails, collect SENCO9 no-write multi-pressure evidence

- step_id: `pressure_senco9_no_write_acquisition`
- phase: `PRESSURE_CHANNEL_SENCO9_ACQUISITION`
- execution_mode: `real_com_pressure_only_requires_authorization`
- gate: `only_needed_when_pressure_quick_check_fails_before_component_sampling`
- opens_com_ports: `True`
- controls_gas_route: `False`
- controls_water_route: `False`
- writes_coefficients: `False`
- coefficient_epoch_event: `none`
- physical_meaning: This is the validated V1.5 SENCO9 no-write pressure runner: ambient is checked by the preceding quick-check; for sealed pressure points, stop continuous atmosphere, wait 1.5 s, require the pressure volume to already be externally/manual sealed, command PACE OUTP ACT with the K0472/PACE legacy MAX slew control contract, record pressure_transition_trace.csv, then wait for analyzer internal P cache refresh before sampling. It is not a diagnostic smoke test.

```powershell
python -m gas_calibrator.tools.validate_pressure_only --config D:\gas_calibrator\_worktrees\v1_5_fixed_wait_window_gate_1aee26d_clean\docs\v1_5_flow_contract\coefficient_epoch_0_getco_snapshot\runtime_identity_bound_config.json --output-dir D:\gas_calibrator\_worktrees\v1_5_fixed_wait_window_gate_1aee26d_clean\docs\v1_5_flow_contract\pressure_channel\senco9_no_write_acquisition --run-id v1_5_contract_reference_pressure_senco9_no_write --pressure-points 1100,1000,900,800,700,600,500 --count 12 --interval-s 1 --continuous-atmosphere-hold --require-continuous-atmosphere-hold --control-pressure-points --pressure-control-tolerance-hpa 1.0 --pressure-control-stable-s 8.0 --pressure-control-timeout-s 240.0 --pressure-control-poll-s 0.5 --pressure-control-slew-mode max --pressure-control-atmosphere-release-wait-s 1.5 --pressure-control-post-stable-wait-s 8.0 --pressure-control-analyzer-stream-flush-s 2.0 --pre-sample-freshness-timeout-s 4.0 --pre-sample-signal-max-age-s 1.0 --analyzer-active-upload-hz 1 --no-prompt
```

- note: No SENCO write is performed by this step.
- note: Do not substitute open-flow dynamic pressure diagnostic tools for this acquisition.
- note: 600 hPa and 500 hPa are included to preserve the full pressure span used for SENCO9 review.

### 10. If pressure acquisition ran, evaluate SENCO9 no-write candidates

- step_id: `pressure_senco9_no_write_review`
- phase: `PRESSURE_CHANNEL_SENCO9_REVIEW`
- execution_mode: `offline_sidecar`
- gate: `only_needed_when_pressure_quick_check_fails_after_no_write_acquisition`
- opens_com_ports: `False`
- controls_gas_route: `False`
- controls_water_route: `False`
- writes_coefficients: `False`
- coefficient_epoch_event: `none`
- physical_meaning: Pressure correction is independent from CO2/H2O fitting. This offline review uses the multi-pressure no-write trace and COM22 reference evidence to decide whether a SENCO9 offset candidate is justified.

```powershell
python -m gas_calibrator.tools.export_v1_5_pressure_senco9_evaluation --run-dir D:\gas_calibrator\_worktrees\v1_5_fixed_wait_window_gate_1aee26d_clean\docs\v1_5_flow_contract\pressure_channel\senco9_no_write_acquisition\v1_5_contract_reference_pressure_senco9_no_write --output-dir D:\gas_calibrator\_worktrees\v1_5_fixed_wait_window_gate_1aee26d_clean\docs\v1_5_flow_contract\pressure_channel\senco9_no_write_evaluation --analyzer-prefix all --min-distinct-pressure-points 3 --min-pressure-span-hpa 300.0 --discard-initial-samples-per-pressure-point 1
```

### 11. Export pressure-channel completion package after SENCO9 write and post-write verification

- step_id: `pressure_channel_completion_audit`
- phase: `PRESSURE_CHANNEL_COMPLETION`
- execution_mode: `offline_sidecar_after_controlled_senco9_write_and_reverify`
- gate: `required_when_senco9_was_written_before_component_sampling`
- opens_com_ports: `False`
- controls_gas_route: `False`
- controls_water_route: `False`
- writes_coefficients: `False`
- coefficient_epoch_event: `none`
- physical_meaning: This offline audit is the bridge between pressure-channel repair and component calibration. It proves the analyzer pressure input P is traceably ready before CO2/H2O fitting consumes it.

```powershell
python -m gas_calibrator.tools.export_v1_5_pressure_channel_completion --senco9-write-summary D:\gas_calibrator\_worktrees\v1_5_fixed_wait_window_gate_1aee26d_clean\docs\v1_5_flow_contract\pressure_channel\senco9_controlled_write\senco9_write_summary.csv --post-write-fit-summary D:\gas_calibrator\_worktrees\v1_5_fixed_wait_window_gate_1aee26d_clean\docs\v1_5_flow_contract\pressure_channel\post_write_pressure_verify\pressure_fit_summary.csv --pressure-reference-json "<pressure_reference_json>" --old-getco-json D:\gas_calibrator\_worktrees\v1_5_fixed_wait_window_gate_1aee26d_clean\docs\v1_5_flow_contract\coefficient_epoch_0_getco_snapshot\old_component_coefficients_snapshot.json --output-dir D:\gas_calibrator\_worktrees\v1_5_fixed_wait_window_gate_1aee26d_clean\docs\v1_5_flow_contract\pressure_channel\pressure_channel_completion
```

- note: No COM is opened and no SENCO is written by this step.
- note: If pressure quick-check passes and no SENCO9 write was needed, this step documents that completion evidence is not applicable.

### 12. Review SENCO7/SENCO8 temperature input evidence

- step_id: `temperature_channel_fast_review`
- phase: `TEMPERATURE_CHANNEL_REVIEW`
- execution_mode: `offline_review`
- gate: `review_before_final_component_write`
- opens_com_ports: `False`
- controls_gas_route: `False`
- controls_water_route: `False`
- writes_coefficients: `False`
- coefficient_epoch_event: `none`
- physical_meaning: Temperature T is a model input for CO2/H2O ratio compensation. This review determines whether SENCO7/8 must be corrected before final coefficient approval.

```powershell
python -m gas_calibrator.tools.export_v1_5_temperature_channel_review --h2o-points-parent D:\gas_calibrator\_worktrees\v1_5_fixed_wait_window_gate_1aee26d_clean\docs\v1_5_flow_contract\h2o_open_flow --output-dir D:\gas_calibrator\_worktrees\v1_5_fixed_wait_window_gate_1aee26d_clean\docs\v1_5_flow_contract\temperature_channel_review
```

### 13. Run V1.5 CO2 open-flow multi-temperature queue

- step_id: `co2_open_flow_sampling`
- phase: `CO2_OPEN_FLOW`
- execution_mode: `real_com_route_requires_authorization`
- gate: `requires_pressure_pass`
- opens_com_ports: `True`
- controls_gas_route: `True`
- controls_water_route: `False`
- writes_coefficients: `False`
- coefficient_epoch_event: `none`
- physical_meaning: Open flow continuously refreshes the optical cavity. CO2 fitting uses factory ratio evidence, not old displayed concentration affected by existing coefficients.

```powershell
python -m gas_calibrator.tools.run_v1_5_formal_co2_open_flow_queue --config D:\gas_calibrator\_worktrees\v1_5_fixed_wait_window_gate_1aee26d_clean\docs\v1_5_flow_contract\coefficient_epoch_0_getco_snapshot\runtime_identity_bound_config.json --queue-csv "<co2_runner_queue.csv>" --output-dir D:\gas_calibrator\_worktrees\v1_5_fixed_wait_window_gate_1aee26d_clean\docs\v1_5_flow_contract\co2_open_flow --run-id v1_5_contract_reference_co2 --analyzer-acquisition active_stream_1hz --temperature-order desc --no-prompt
```

### 14. Run V1.5 H2O open-flow multi-temperature queue

- step_id: `h2o_open_flow_sampling`
- phase: `H2O_OPEN_FLOW`
- execution_mode: `real_com_route_requires_authorization`
- gate: `requires_pressure_pass`
- opens_com_ports: `True`
- controls_gas_route: `False`
- controls_water_route: `True`
- writes_coefficients: `False`
- coefficient_epoch_event: `none`
- physical_meaning: H2O fitting must use dewpoint/reference-backed water evidence and preserve dry-gas low-water anchors separately from CO2 zero-gas anchors.

```powershell
python -m gas_calibrator.tools.run_v1_5_formal_h2o_open_flow_queue --config D:\gas_calibrator\_worktrees\v1_5_fixed_wait_window_gate_1aee26d_clean\docs\v1_5_flow_contract\coefficient_epoch_0_getco_snapshot\runtime_identity_bound_config.json --queue-csv "<h2o_runner_queue.csv>" --output-dir D:\gas_calibrator\_worktrees\v1_5_fixed_wait_window_gate_1aee26d_clean\docs\v1_5_flow_contract\h2o_open_flow --run-id v1_5_contract_reference_h2o --analyzer-acquisition active_stream_1hz --temperature-order asc --h2o-pressure-presample-policy skip --no-prompt
```

### 15. Review factory-mode optical reference and signal health

- step_id: `factory_signal_health_review`
- phase: `FACTORY_SIGNAL_HEALTH_REVIEW`
- execution_mode: `offline_review`
- gate: `required_before_component_write_review`
- opens_com_ports: `False`
- controls_gas_route: `False`
- controls_water_route: `False`
- writes_coefficients: `False`
- coefficient_epoch_event: `none`
- physical_meaning: A stable ratio is not sufficient if the reference, CO2, or H2O optical signals are in an abnormal working region. This offline gate prevents optical reference-chain faults from being absorbed into SENCO1/2/3/4.

```powershell
python -m gas_calibrator.tools.export_v1_5_factory_signal_health_review --point-means-csv "<offline_fit_point_means.csv>" --residuals-csv "<candidate_fit_residuals.csv>" --output-dir D:\gas_calibrator\_worktrees\v1_5_fixed_wait_window_gate_1aee26d_clean\docs\v1_5_flow_contract\factory_signal_health_review
```

- note: Devices whose factory-signal gate is not pass_factory_signal_health must not enter formal component coefficient review.
- note: SETILLUM no-argument readback is not treated as numeric evidence; use MODE2 ref_signal and explicit configuration snapshots.

### 16. Audit CO2/H2O fit inputs before candidate coefficients

- step_id: `fit_input_quality_review`
- phase: `QC_AND_FIT_INPUT_REVIEW`
- execution_mode: `offline_review`
- gate: `requires_factory_signal_health_review`
- opens_com_ports: `False`
- controls_gas_route: `False`
- controls_water_route: `False`
- writes_coefficients: `False`
- coefficient_epoch_event: `none`
- physical_meaning: Only traceable, stable, role-eligible samples should enter candidate coefficient fitting.

```powershell
python -m gas_calibrator.tools.export_v1_5_fit_input_quality --co2-policy-csv "<co2_candidate_policy.csv>" --co2-residuals-csv "<co2_candidate_residuals.csv>" --h2o-policy-csv "<h2o_candidate_policy.csv>" --h2o-residuals-csv "<h2o_candidate_residuals.csv>" --output-dir D:\gas_calibrator\_worktrees\v1_5_fixed_wait_window_gate_1aee26d_clean\docs\v1_5_flow_contract\fit_input_quality
```

### 17. Build post-run coefficient review, write, reverify, and archive gap plan

- step_id: `post_run_coefficient_executor`
- phase: `POST_RUN_COEFFICIENT_EXECUTOR`
- execution_mode: `offline_review`
- gate: `required_after_component_acquisition_before_controlled_write`
- opens_com_ports: `False`
- controls_gas_route: `False`
- controls_water_route: `False`
- writes_coefficients: `False`
- coefficient_epoch_event: `none`
- physical_meaning: After gas and water acquisition, coefficient closure must be deterministic: pressure and temperature input quantities are checked first, all eligible stable CO2/H2O points are fit, S5/S6 trims are reviewed after main coefficients, and missing post-write reverification or archive evidence remains an explicit gap.

```powershell
python -m gas_calibrator.tools.export_v1_5_post_run_coefficient_executor --run-dir "<completed_v1_5_run_dir>" --output-dir D:\gas_calibrator\_worktrees\v1_5_fixed_wait_window_gate_1aee26d_clean\docs\v1_5_flow_contract\post_run_coefficient_executor --plan-json D:\gas_calibrator\_worktrees\v1_5_fixed_wait_window_gate_1aee26d_clean\docs\v1_5_flow_contract\formal_run_package\formal_plan_snapshot.json --pressure-reference-json D:\gas_calibrator\_worktrees\v1_5_fixed_wait_window_gate_1aee26d_clean\docs\v1_5_flow_contract\formal_run_package\com22_pressure_reference.json --run-evidence-status-json D:\gas_calibrator\_worktrees\v1_5_fixed_wait_window_gate_1aee26d_clean\docs\v1_5_flow_contract\v1_5_run_evidence_status.json --pressure-review-json D:\gas_calibrator\_worktrees\v1_5_fixed_wait_window_gate_1aee26d_clean\docs\v1_5_flow_contract\pressure_channel\pressure_senco9_review.json --pressure-completion-summary-csv D:\gas_calibrator\_worktrees\v1_5_fixed_wait_window_gate_1aee26d_clean\docs\v1_5_flow_contract\pressure_channel\pressure_channel_completion\pressure_channel_completion_summary.csv --pressure-device-readiness-csv D:\gas_calibrator\_worktrees\v1_5_fixed_wait_window_gate_1aee26d_clean\docs\v1_5_flow_contract\pressure_channel\pressure_channel_completion\pressure_channel_device_readiness.csv --temperature-review-csv D:\gas_calibrator\_worktrees\v1_5_fixed_wait_window_gate_1aee26d_clean\docs\v1_5_flow_contract\temperature_channel_review\temperature_current_point_review.csv --main-precheck-meta-json "<main_senco_write_precheck_meta.json>" --post-write-reverification-json D:\gas_calibrator\_worktrees\v1_5_fixed_wait_window_gate_1aee26d_clean\docs\v1_5_flow_contract\post_write_reverification\post_write_reverification_review.json --archive-closure-json D:\gas_calibrator\_worktrees\v1_5_fixed_wait_window_gate_1aee26d_clean\docs\v1_5_flow_contract\formal_archive_closure_from_full_chain\v1_5_formal_archive_closure_index.json
```

- note: This exporter is no-write and does not open COM ports.
- note: Missing H2O post-write reverification blocks final acceptance but does not block CO2 candidate review.
- note: A failed device must be excluded per-device with a reason; it must not drag down all other devices.

### 18. Review full-flow closure readiness before controlled writes

- step_id: `full_flow_closure_readiness`
- phase: `FULL_FLOW_CLOSURE_READINESS`
- execution_mode: `offline_review`
- gate: `required_before_controlled_write_review`
- opens_com_ports: `False`
- controls_gas_route: `False`
- controls_water_route: `False`
- writes_coefficients: `False`
- coefficient_epoch_event: `none`
- physical_meaning: This offline gate checks whether plan, evidence index, candidate write package, post-write reverification plan, and archive gaps form one auditable chain before any controlled SENCO write is considered.

```powershell
python -m gas_calibrator.tools.export_v1_5_full_flow_closure_readiness --run-dir D:\gas_calibrator\_worktrees\v1_5_fixed_wait_window_gate_1aee26d_clean\docs\v1_5_flow_contract --output-dir D:\gas_calibrator\_worktrees\v1_5_fixed_wait_window_gate_1aee26d_clean\docs\v1_5_flow_contract\full_flow_closure_readiness --full-flow-plan-json D:\gas_calibrator\_worktrees\v1_5_fixed_wait_window_gate_1aee26d_clean\docs\v1_5_flow_contract\v1_5_full_flow_plan.json --run-evidence-status-json D:\gas_calibrator\_worktrees\v1_5_fixed_wait_window_gate_1aee26d_clean\docs\v1_5_flow_contract\v1_5_run_evidence_status.json --post-run-executor-json D:\gas_calibrator\_worktrees\v1_5_fixed_wait_window_gate_1aee26d_clean\docs\v1_5_flow_contract\post_run_coefficient_executor\executor_manifest.json --controlled-write-package-csv D:\gas_calibrator\_worktrees\v1_5_fixed_wait_window_gate_1aee26d_clean\docs\v1_5_flow_contract\post_run_coefficient_executor\controlled_write_package.csv --post-write-reverification-plan-csv D:\gas_calibrator\_worktrees\v1_5_fixed_wait_window_gate_1aee26d_clean\docs\v1_5_flow_contract\post_run_coefficient_executor\post_write_reverification_plan.csv --archive-gap-list-csv D:\gas_calibrator\_worktrees\v1_5_fixed_wait_window_gate_1aee26d_clean\docs\v1_5_flow_contract\post_run_coefficient_executor\archive_gap_list.csv --archive-closure-json D:\gas_calibrator\_worktrees\v1_5_fixed_wait_window_gate_1aee26d_clean\docs\v1_5_flow_contract\formal_archive_closure_from_full_chain\v1_5_formal_archive_closure_index.json
```

- note: No COM ports are opened and no coefficient is written.
- note: Per-device blockers remain per-device; one failed analyzer must not hide ready analyzers.
- note: Fit/verification labels do not exclude otherwise stable traceable samples by default.

### 19. Review CO2 SENCO1/SENCO3 and optional SENCO5 candidates

- step_id: `co2_candidate_write_review`
- phase: `CO2_CANDIDATE_REVIEW`
- execution_mode: `offline_review`
- gate: `reviewer_approver_required_before_write`
- opens_com_ports: `False`
- controls_gas_route: `False`
- controls_water_route: `False`
- writes_coefficients: `False`
- coefficient_epoch_event: `none`
- physical_meaning: CO2 main coefficients must freeze pressure terms under current-atmosphere V1.5 policy; SENCO5 is final displayed concentration trim.

```powershell
python -m gas_calibrator.tools.export_v1_5_co2_senco_pair_model_scope --original-points-xlsx "<v1_5_original_points.xlsx>" --candidate-dir "<co2_candidate_dir>" --output-dir D:\gas_calibrator\_worktrees\v1_5_fixed_wait_window_gate_1aee26d_clean\docs\v1_5_flow_contract\co2_candidate_write_review
```

### 20. Controlled component writes create new coefficient epochs

- step_id: `controlled_component_write_placeholder`
- phase: `CONTROLLED_WRITE`
- execution_mode: `blocked_pending_explicit_authorization`
- gate: `never_auto_execute_from_full_flow_planner`
- opens_com_ports: `False`
- controls_gas_route: `False`
- controls_water_route: `False`
- writes_coefficients: `True`
- coefficient_epoch_event: `start_new_epoch_after_each_verified_write`
- physical_meaning: SENCO1/3, SENCO2/4, and optional SENCO5/6 writes are high-risk model changes. Each successful write starts a new coefficient epoch and must be followed by verification.

- note: Use run_v1_5_co2_senco13_controlled_write only after approval.
- note: Use run_v1_5_h2o_senco24_controlled_write only after approval.
- note: Use linear SENCO5/SENCO6 writers only when final-output trim is reviewed.

### 21. Verify analyzer outputs after controlled coefficient writes

- step_id: `post_write_reverification_placeholder`
- phase: `POST_WRITE_REVERIFY`
- execution_mode: `blocked_pending_post_write_reverification`
- gate: `required_after_controlled_write_before_final_archive`
- opens_com_ports: `False`
- controls_gas_route: `False`
- controls_water_route: `False`
- writes_coefficients: `False`
- coefficient_epoch_event: `verify_current_epoch_before_archive`
- physical_meaning: A coefficient write changes the analyzer measurement model. Before final evidence bundling, the updated model must be checked against independent open-flow verification points.

```powershell
python -m gas_calibrator.tools.export_v1_5_post_write_reverification --verification-csv "<post_write_verification_points.csv>" --output-dir D:\gas_calibrator\_worktrees\v1_5_fixed_wait_window_gate_1aee26d_clean\docs\v1_5_flow_contract\post_write_reverification --write-event-json "<coefficient_write_events.json>" --coefficient-snapshot-json "<coefficient_epoch_n_snapshot.json>"
```

- note: Use short V1.5 open-flow reverification after any approved SENCO write.
- note: Keep verification points separate from the fit-training points when possible.
- note: Do not treat engineering diagnostic pressure points as post-write component acceptance.

### 22. Build formal evidence bundle from completed run artifacts

- step_id: `formal_evidence_sidecar`
- phase: `EVIDENCE_BUNDLE`
- execution_mode: `offline_sidecar`
- gate: `required_before_report_and_database_import`
- opens_com_ports: `False`
- controls_gas_route: `False`
- controls_water_route: `False`
- writes_coefficients: `False`
- coefficient_epoch_event: `none`
- physical_meaning: Bundle raw frames, QC, traceability, coefficient snapshots, and reports so the result can be reconstructed.

```powershell
python -m gas_calibrator.tools.run_v1_5_formal_evidence_sidecar --run-dir "<completed_v1_5_run_dir>" --plan-json D:\gas_calibrator\_worktrees\v1_5_fixed_wait_window_gate_1aee26d_clean\docs\v1_5_flow_contract\formal_run_package\formal_plan_snapshot.json --pressure-reference-json D:\gas_calibrator\_worktrees\v1_5_fixed_wait_window_gate_1aee26d_clean\docs\v1_5_flow_contract\formal_run_package\com22_pressure_reference.json --config D:\gas_calibrator\_worktrees\v1_5_fixed_wait_window_gate_1aee26d_clean\docs\v1_5_flow_contract\coefficient_epoch_0_getco_snapshot\runtime_identity_bound_config.json
```

### 23. Preview V1.5 PostgreSQL 18 schema and insert contract

- step_id: `formal_database_dry_run_snapshot`
- phase: `FORMAL_DATABASE_DRY_RUN`
- execution_mode: `offline_sidecar`
- gate: `required_before_database_import_authorization`
- opens_com_ports: `False`
- controls_gas_route: `False`
- controls_water_route: `False`
- writes_coefficients: `False`
- coefficient_epoch_event: `none`
- physical_meaning: Before any production database import, verify the PostgreSQL 18 schema, SN/device_code primary identity, protocol ID alias, and stage-by-stage insert contract as a dry-run preview only.

```powershell
python -m gas_calibrator.tools.export_v1_5_formal_database_dry_run --output-dir D:\gas_calibrator\_worktrees\v1_5_fixed_wait_window_gate_1aee26d_clean\docs\v1_5_flow_contract\formal_database_dry_run --fail-on-blocker
```

- note: This dry-run never connects PostgreSQL and never imports production data.
- note: A passing database dry-run is schema/insert-preview evidence only; archive release and database import remain separate gates.

### 24. Review PostgreSQL 18 import preflight without connecting

- step_id: `formal_database_import_preflight_snapshot`
- phase: `FORMAL_DATABASE_IMPORT_PREFLIGHT`
- execution_mode: `offline_sidecar`
- gate: `required_before_database_import_execution`
- opens_com_ports: `False`
- controls_gas_route: `False`
- controls_water_route: `False`
- writes_coefficients: `False`
- coefficient_epoch_event: `none`
- physical_meaning: Before a real production import, confirm the dry-run contract, DSN presence/fingerprint, migration lock, and explicit authorization boundary without opening a database connection.

```powershell
python -m gas_calibrator.tools.export_v1_5_formal_database_import_preflight --formal-database-dry-run-json D:\gas_calibrator\_worktrees\v1_5_fixed_wait_window_gate_1aee26d_clean\docs\v1_5_flow_contract\formal_database_dry_run\v1_5_formal_database_dry_run.json --dsn-env V1_5_POSTGRES_DSN --output-dir D:\gas_calibrator\_worktrees\v1_5_fixed_wait_window_gate_1aee26d_clean\docs\v1_5_flow_contract\formal_database_import_preflight --fail-on-blocker
```

- note: This preflight never connects PostgreSQL, applies migrations, or imports rows.
- note: A passing import preflight is still not database import authorization; archive release and operator authorization remain separate.

### 25. Review PostgreSQL 18 manual import authorization without connecting

- step_id: `formal_database_import_authorization_snapshot`
- phase: `FORMAL_DATABASE_IMPORT_AUTHORIZATION`
- execution_mode: `offline_sidecar`
- gate: `required_before_database_import_execution`
- opens_com_ports: `False`
- controls_gas_route: `False`
- controls_water_route: `False`
- writes_coefficients: `False`
- coefficient_epoch_event: `none`
- physical_meaning: Before any production database import command can run, confirm archive release, preflight readiness, and explicit operator/reviewer/approver authorization without opening a database connection.

```powershell
python -m gas_calibrator.tools.export_v1_5_formal_database_import_authorization --formal-database-import-preflight-json D:\gas_calibrator\_worktrees\v1_5_fixed_wait_window_gate_1aee26d_clean\docs\v1_5_flow_contract\formal_database_import_preflight\v1_5_formal_database_import_preflight.json --archive-closure-json D:\gas_calibrator\_worktrees\v1_5_fixed_wait_window_gate_1aee26d_clean\docs\v1_5_flow_contract\formal_archive_closure_from_full_chain\v1_5_formal_archive_closure_index.json --operator D:\gas_calibrator\_worktrees\v1_5_fixed_wait_window_gate_1aee26d_clean\V1.5-contract-refresh --authorization-id "<database_import_authorization_id>" --output-dir D:\gas_calibrator\_worktrees\v1_5_fixed_wait_window_gate_1aee26d_clean\docs\v1_5_flow_contract\formal_database_import_authorization --fail-on-blocker
```

- note: This authorization guard never connects PostgreSQL, applies migrations, or imports rows.
- note: A ready authorization artifact must still be consumed by a separate controlled import command.

### 26. Import evidence bundle into V1.5 PostgreSQL registry

- step_id: `database_import`
- phase: `DATABASE_IMPORT`
- execution_mode: `offline_database_requires_configured_dsn`
- gate: `required_for_formal_archive`
- opens_com_ports: `False`
- controls_gas_route: `False`
- controls_water_route: `False`
- writes_coefficients: `False`
- coefficient_epoch_event: `none`
- physical_meaning: The database indexes traceability and audit state; raw evidence remains in hashed evidence packages.

```powershell
python -m gas_calibrator.tools.import_v1_5_evidence_package --run-dir "<completed_v1_5_run_dir>"
```

### 27. Generate Chinese V1.5 calibration reports

- step_id: `zh_calibration_reports`
- phase: `REPORTS`
- execution_mode: `offline_sidecar`
- gate: `final_deliverable`
- opens_com_ports: `False`
- controls_gas_route: `False`
- controls_water_route: `False`
- writes_coefficients: `False`
- coefficient_epoch_event: `none`
- physical_meaning: Reports summarize the physical open-flow method, QC decisions, traceability, uncertainty, and write status. Per-device certificates are generated from the same frozen evidence bundle and include artifact hashes for audit.

```powershell
python -m gas_calibrator.tools.export_v1_5_calibration_reports --evidence-bundle-json "<completed_v1_5_run_dir>\formal_evidence_sidecar\evidence_bundle.json" --output-dir D:\gas_calibrator\_worktrees\v1_5_fixed_wait_window_gate_1aee26d_clean\docs\v1_5_flow_contract\reports
```

- note: H2O queue abort/exclusion evidence remains diagnostic-only and blocks affected rows from formal fit/acceptance.
- note: This report exporter does not open COM ports, control routes, or write coefficients.

### 28. Refresh final V1.5 evidence status after reports and per-device certificates

- step_id: `final_evidence_status_refresh`
- phase: `FINAL_EVIDENCE_STATUS`
- execution_mode: `offline_sidecar`
- gate: `required_after_reports_for_archive_closure`
- opens_com_ports: `False`
- controls_gas_route: `False`
- controls_water_route: `False`
- writes_coefficients: `False`
- coefficient_epoch_event: `none`
- physical_meaning: The final evidence-status tree must be rebuilt after certificates are generated so audit state, hashes, H2O exclusions, and report readiness all point to the same evidence package.

```powershell
python -m gas_calibrator.tools.export_v1_5_run_evidence_status --run-dir D:\gas_calibrator\_worktrees\v1_5_fixed_wait_window_gate_1aee26d_clean\docs\v1_5_flow_contract --output-dir D:\gas_calibrator\_worktrees\v1_5_fixed_wait_window_gate_1aee26d_clean\docs\v1_5_flow_contract --full-flow-plan-json D:\gas_calibrator\_worktrees\v1_5_fixed_wait_window_gate_1aee26d_clean\docs\v1_5_flow_contract\v1_5_full_flow_plan.json --contract-json D:\gas_calibrator\_worktrees\v1_5_fixed_wait_window_gate_1aee26d_clean\docs\v1_5_flow_contract\v1_5_formal_flow_contract.json --evidence-bundle-json "<completed_v1_5_run_dir>\formal_evidence_sidecar\evidence_bundle.json"
```

### 29. Generate new-algorithm profile runner dry-run bundle

- step_id: `algorithm_profile_runner_dry_run_snapshot`
- phase: `ALGORITHM_PROFILE_RUNNER_DRY_RUN`
- execution_mode: `offline_sidecar`
- gate: `optional_new_algorithm_runner_preflight_before_status_rollup`
- opens_com_ports: `False`
- controls_gas_route: `False`
- controls_water_route: `False`
- writes_coefficients: `False`
- coefficient_epoch_event: `none`
- physical_meaning: For the new absorption algorithm, generate the profile-driven CO2 47 / H2O 14 runlist preview, readiness gate, and dry-run mature-queue handoff evidence without executing the queues.

```powershell
python -m gas_calibrator.tools.export_v1_5_algorithm_profile_runner_dry_run --profile-path D:\gas_calibrator\_worktrees\v1_5_fixed_wait_window_gate_1aee26d_clean\configs\v1_5_algorithm_route_profiles.json --output-dir D:\gas_calibrator\_worktrees\v1_5_fixed_wait_window_gate_1aee26d_clean\docs\v1_5_flow_contract\algorithm_profile_runner_dry_run --fail-on-blocker
```

- note: This bundle does not open COM, connect PostgreSQL, control gas/water routes, write SN/device IDs, write coefficients, or modify mature runners.
- note: A passing dry-run bundle does not authorize live runner wiring or real acceptance.

### 30. Export top-level formal run status dashboard

- step_id: `formal_run_status_snapshot`
- phase: `FORMAL_RUN_STATUS`
- execution_mode: `offline_sidecar`
- gate: `final_reviewer_status_overview`
- opens_com_ports: `False`
- controls_gas_route: `False`
- controls_water_route: `False`
- writes_coefficients: `False`
- coefficient_epoch_event: `none`
- physical_meaning: After evidence status and closure sidecars are refreshed, this dashboard answers the production question directly: current stage, next action, whether physical flow can continue, and whether formal archive/database release is allowed.

```powershell
python -m gas_calibrator.tools.export_v1_5_formal_run_status --run-dir D:\gas_calibrator\_worktrees\v1_5_fixed_wait_window_gate_1aee26d_clean\docs\v1_5_flow_contract --output-dir D:\gas_calibrator\_worktrees\v1_5_fixed_wait_window_gate_1aee26d_clean\docs\v1_5_flow_contract\formal_run_status --initialization-readiness-json D:\gas_calibrator\_worktrees\v1_5_fixed_wait_window_gate_1aee26d_clean\docs\v1_5_flow_contract\formal_initialization\v1_5_initialization_readiness.json --pre-gas-readiness-json D:\gas_calibrator\_worktrees\v1_5_fixed_wait_window_gate_1aee26d_clean\docs\v1_5_flow_contract\pre_gas_readiness\v1_5_pre_gas_readiness.json --getco-readiness-json D:\gas_calibrator\_worktrees\v1_5_fixed_wait_window_gate_1aee26d_clean\docs\v1_5_flow_contract\identity_getco_readiness\v1_5_getco_identity_readiness.json --run-evidence-status-json D:\gas_calibrator\_worktrees\v1_5_fixed_wait_window_gate_1aee26d_clean\docs\v1_5_flow_contract\v1_5_run_evidence_status.json --full-flow-closure-readiness-json D:\gas_calibrator\_worktrees\v1_5_fixed_wait_window_gate_1aee26d_clean\docs\v1_5_flow_contract\full_flow_closure_readiness\v1_5_full_flow_closure_readiness.json --archive-closure-json D:\gas_calibrator\_worktrees\v1_5_fixed_wait_window_gate_1aee26d_clean\docs\v1_5_flow_contract\formal_archive_closure_from_full_chain\v1_5_formal_archive_closure_index.json --algorithm-profile-runner-dry-run-json D:\gas_calibrator\_worktrees\v1_5_fixed_wait_window_gate_1aee26d_clean\docs\v1_5_flow_contract\algorithm_profile_runner_dry_run\v1_5_algorithm_profile_runner_dry_run.json --formal-database-dry-run-json D:\gas_calibrator\_worktrees\v1_5_fixed_wait_window_gate_1aee26d_clean\docs\v1_5_flow_contract\formal_database_dry_run\v1_5_formal_database_dry_run.json --formal-database-import-preflight-json D:\gas_calibrator\_worktrees\v1_5_fixed_wait_window_gate_1aee26d_clean\docs\v1_5_flow_contract\formal_database_import_preflight\v1_5_formal_database_import_preflight.json --formal-database-import-authorization-json D:\gas_calibrator\_worktrees\v1_5_fixed_wait_window_gate_1aee26d_clean\docs\v1_5_flow_contract\formal_database_import_authorization\v1_5_formal_database_import_authorization.json
```

- note: This exporter reads sidecars only; it does not open COM, connect PostgreSQL, control routes, or write coefficients.
- note: A release-ready status is still reviewer evidence, not an implicit real-device action.

## Warnings

- CO2 queue CSV is not specified; command uses a placeholder.
- H2O queue CSV is not specified; command uses a placeholder.
- Pressure reference JSON is not specified; formal pressure review will need it.
- Standard-gas certificate JSON is not specified; formal run package will use templates.
