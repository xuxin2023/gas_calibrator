# Acceptance Summary

- generated_at: 2026-04-13T12:47:29+08:00
- head: `f41b7b20c35a5051943fecd35bdaf62c05ae8d34`
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

- H2O capability state: `src/gas_calibrator/workflow/runner.py:4646-4659`, `src/gas_calibrator/workflow/runner.py:4661-4671`, `tests/test_runner_v1_writeback_safety.py:140-159`
- offline fault injection: `tests/test_v1_writeback_fault_injection.py:108-119`, `tests/test_v1_writeback_fault_injection.py:122-138`, `tests/test_v1_writeback_fault_injection.py:150-165`, `tests/test_v1_writeback_fault_injection.py:168-182`, `tests/test_v1_writeback_fault_injection.py:185-200`, `tests/test_v1_writeback_fault_injection.py:203-215`, `tests/test_v1_writeback_fault_injection.py:218-231`
- online acceptance gate/tooling: `src/gas_calibrator/tools/run_v1_online_acceptance.py:608-760`, `src/gas_calibrator/tools/run_v1_online_acceptance.py:525-557`, `tests/test_v1_online_acceptance_tool.py:78-104`, `tests/test_v1_online_acceptance_tool.py:172-196`, `tests/test_v1_online_acceptance_tool.py:199-218`
- shared path stale test fixed: `tests/test_runner_collect_only.py:938-1144`
