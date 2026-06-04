# V1.5 full-flow dry-run command list.
# Review each command and gate before executing. Controlled writes are placeholders only.
$env:PYTHONPATH = "src"

# load_plan_and_traceability: Prepare V1.5 formal plan and certificate snapshots
python -m gas_calibrator.tools.prepare_v1_5_formal_run_package --output-dir D:\gas_calibrator\_worktrees\v1_5_fixed_wait_window_gate_1aee26d_clean\docs\v1_5_flow_contract\formal_run_package --operator "<operator>" --analyzer-id multi_device --run-id v1_5_contract_reference --config D:\gas_calibrator\_worktrees\v1_5_fixed_wait_window_gate_1aee26d_clean\configs\default_config.json

# device_identity_and_getco_snapshot: Read device IDs and GETCO1-9 before calibration
python -m gas_calibrator.tools.probe_v1_5_getco_component_snapshot --config D:\gas_calibrator\_worktrees\v1_5_fixed_wait_window_gate_1aee26d_clean\configs\default_config.json --output-dir D:\gas_calibrator\_worktrees\v1_5_fixed_wait_window_gate_1aee26d_clean\docs\v1_5_flow_contract\coefficient_epoch_0_getco_snapshot --groups 1,2,3,4,5,6,7,8,9 --response-timeout-s 2.5 --command-gap-s 1.2 --attempts-per-group 3 --pre-drain-s 0.5 --identity-timeout-s 5.0 --include-legacy --allow-runtime-identity-rebind

# pressure_quick_check: Verify analyzer pressure input against COM22 at atmosphere
python -m gas_calibrator.tools.validate_pressure_only --config D:\gas_calibrator\_worktrees\v1_5_fixed_wait_window_gate_1aee26d_clean\docs\v1_5_flow_contract\coefficient_epoch_0_getco_snapshot\runtime_identity_bound_config.json --output-dir D:\gas_calibrator\_worktrees\v1_5_fixed_wait_window_gate_1aee26d_clean\docs\v1_5_flow_contract\pressure_channel --run-id v1_5_contract_reference_pressure_quick --pressure-points ambient --count 10 --interval-s 1 --continuous-atmosphere-hold --require-continuous-atmosphere-hold --no-prompt

# pressure_senco9_no_write_review: If pressure fails, prepare SENCO9 no-write evaluation
python -m gas_calibrator.tools.export_v1_5_pressure_senco9_no_write_preflight --config D:\gas_calibrator\_worktrees\v1_5_fixed_wait_window_gate_1aee26d_clean\docs\v1_5_flow_contract\coefficient_epoch_0_getco_snapshot\runtime_identity_bound_config.json --output-dir D:\gas_calibrator\_worktrees\v1_5_fixed_wait_window_gate_1aee26d_clean\docs\v1_5_flow_contract\pressure_channel\senco9_no_write_preflight

# temperature_channel_fast_review: Review SENCO7/SENCO8 temperature input evidence
python -m gas_calibrator.tools.export_v1_5_temperature_channel_review --h2o-points-parent D:\gas_calibrator\_worktrees\v1_5_fixed_wait_window_gate_1aee26d_clean\docs\v1_5_flow_contract\h2o_open_flow --output-dir D:\gas_calibrator\_worktrees\v1_5_fixed_wait_window_gate_1aee26d_clean\docs\v1_5_flow_contract\temperature_channel_review

# co2_open_flow_sampling: Run V1.5 CO2 open-flow multi-temperature queue
python -m gas_calibrator.tools.run_v1_5_formal_co2_open_flow_queue --config D:\gas_calibrator\_worktrees\v1_5_fixed_wait_window_gate_1aee26d_clean\docs\v1_5_flow_contract\coefficient_epoch_0_getco_snapshot\runtime_identity_bound_config.json --queue-csv "<co2_runner_queue.csv>" --output-dir D:\gas_calibrator\_worktrees\v1_5_fixed_wait_window_gate_1aee26d_clean\docs\v1_5_flow_contract\co2_open_flow --run-id v1_5_contract_reference_co2 --analyzer-acquisition active_stream_1hz --temperature-order asc --no-prompt

# h2o_open_flow_sampling: Run V1.5 H2O open-flow multi-temperature queue
python -m gas_calibrator.tools.run_v1_5_formal_h2o_open_flow_queue --config D:\gas_calibrator\_worktrees\v1_5_fixed_wait_window_gate_1aee26d_clean\docs\v1_5_flow_contract\coefficient_epoch_0_getco_snapshot\runtime_identity_bound_config.json --queue-csv "<h2o_runner_queue.csv>" --output-dir D:\gas_calibrator\_worktrees\v1_5_fixed_wait_window_gate_1aee26d_clean\docs\v1_5_flow_contract\h2o_open_flow --run-id v1_5_contract_reference_h2o --analyzer-acquisition active_stream_1hz --temperature-order asc --h2o-pressure-presample-policy warn --no-prompt

# fit_input_quality_review: Audit CO2/H2O fit inputs before candidate coefficients
python -m gas_calibrator.tools.export_v1_5_fit_input_quality --co2-policy-csv "<co2_candidate_policy.csv>" --co2-residuals-csv "<co2_candidate_residuals.csv>" --h2o-policy-csv "<h2o_candidate_policy.csv>" --h2o-residuals-csv "<h2o_candidate_residuals.csv>" --output-dir D:\gas_calibrator\_worktrees\v1_5_fixed_wait_window_gate_1aee26d_clean\docs\v1_5_flow_contract\fit_input_quality

# co2_candidate_write_review: Review CO2 SENCO1/SENCO3 and optional SENCO5 candidates
python -m gas_calibrator.tools.export_v1_5_co2_senco_pair_model_scope --original-points-xlsx "<v1_5_original_points.xlsx>" --candidate-dir "<co2_candidate_dir>" --output-dir D:\gas_calibrator\_worktrees\v1_5_fixed_wait_window_gate_1aee26d_clean\docs\v1_5_flow_contract\co2_candidate_write_review

# controlled_component_write_placeholder: Controlled component writes create new coefficient epochs
# blocked/manual: blocked_pending_explicit_authorization

# post_write_reverification_placeholder: Verify analyzer outputs after controlled coefficient writes
python -m gas_calibrator.tools.export_v1_5_post_write_reverification --verification-csv "<post_write_verification_points.csv>" --output-dir D:\gas_calibrator\_worktrees\v1_5_fixed_wait_window_gate_1aee26d_clean\docs\v1_5_flow_contract\post_write_reverification --write-event-json "<coefficient_write_events.json>" --coefficient-snapshot-json "<coefficient_epoch_n_snapshot.json>"

# formal_evidence_sidecar: Build formal evidence bundle from completed run artifacts
python -m gas_calibrator.tools.run_v1_5_formal_evidence_sidecar --run-dir "<completed_v1_5_run_dir>" --plan-json D:\gas_calibrator\_worktrees\v1_5_fixed_wait_window_gate_1aee26d_clean\docs\v1_5_flow_contract\formal_run_package\formal_plan_snapshot.json --pressure-reference-json D:\gas_calibrator\_worktrees\v1_5_fixed_wait_window_gate_1aee26d_clean\docs\v1_5_flow_contract\formal_run_package\com22_pressure_reference.json --config D:\gas_calibrator\_worktrees\v1_5_fixed_wait_window_gate_1aee26d_clean\docs\v1_5_flow_contract\coefficient_epoch_0_getco_snapshot\runtime_identity_bound_config.json

# database_import: Import evidence bundle into V1.5 PostgreSQL registry
python -m gas_calibrator.tools.import_v1_5_evidence_package --run-dir "<completed_v1_5_run_dir>"

# zh_calibration_reports: Generate Chinese V1.5 calibration reports
python -m gas_calibrator.tools.export_v1_5_calibration_reports --evidence-bundle-json "<completed_v1_5_run_dir>\formal_evidence_sidecar\evidence_bundle.json" --output-dir D:\gas_calibrator\_worktrees\v1_5_fixed_wait_window_gate_1aee26d_clean\docs\v1_5_flow_contract\reports
