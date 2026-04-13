# Acceptance Summary

- generated_at: 2026-04-13T14:58:16+08:00
- head: `367a1089ebaca1388dbb9d11648f74513316e502`
- evidence_source = simulated
- not_real_acceptance_evidence = true

## Summary

- CO2 zero/span: PASS
- H2O zero/span: NOT_SUPPORTED on this HEAD; route/point collection exists but explicit zero/span business chain does not.
- device writeback safety closure: PASS in offline tests
- protocol fault injection: PASS offline
- real-device abnormal recovery evidence: ONLINE_EVIDENCE_REQUIRED
- guarded online acceptance bundle: generated under `audit/v1_calibration_acceptance_online/`; dry-run by default, dual gate required for real-device runs.
- shared path old failure in tests/test_runner_collect_only.py: resolved as stale expectation; focused test and full file now pass.

## Evidence

- H2O capability state: `src/gas_calibrator/workflow/runner.py:4647-4660`, `src/gas_calibrator/workflow/runner.py:4662-4672`, `tests/test_runner_v1_writeback_safety.py:140-159`
- offline fault injection: `tests/test_v1_writeback_fault_injection.py:108-119`, `tests/test_v1_writeback_fault_injection.py:122-138`, `tests/test_v1_writeback_fault_injection.py:150-165`, `tests/test_v1_writeback_fault_injection.py:168-182`, `tests/test_v1_writeback_fault_injection.py:185-200`, `tests/test_v1_writeback_fault_injection.py:203-215`, `tests/test_v1_writeback_fault_injection.py:218-231`
- online acceptance gate/tooling: `src/gas_calibrator/tools/run_v1_online_acceptance.py:611-763`, `src/gas_calibrator/tools/run_v1_online_acceptance.py:528-560`, `tests/test_v1_online_acceptance_tool.py:78-104`, `tests/test_v1_online_acceptance_tool.py:172-196`, `tests/test_v1_online_acceptance_tool.py:199-218`
- shared path stale test fixed: `tests/test_runner_collect_only.py:938-1144`
