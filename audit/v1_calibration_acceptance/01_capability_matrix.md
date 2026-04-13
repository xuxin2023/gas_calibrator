# Capability Matrix

- generated_at: 2026-04-13T12:47:29+08:00
- head: `f41b7b20c35a5051943fecd35bdaf62c05ae8d34`

| capability | status | evidence |
| --- | --- | --- |
| CO2 zero | PASS | `src/gas_calibrator/workflow/runner.py:11262-11279`, `src/gas_calibrator/workflow/runner.py:11600-11686` |
| CO2 span | PASS | `src/gas_calibrator/workflow/runner.py:5302-5393`, `src/gas_calibrator/workflow/runner.py:11033-11241` |
| H2O zero | NOT_SUPPORTED | `src/gas_calibrator/workflow/runner.py:11765-11917`, `src/gas_calibrator/workflow/runner.py:16218-16290`, `src/gas_calibrator/workflow/runner.py:4661-4671` |
| H2O span | NOT_SUPPORTED | `src/gas_calibrator/workflow/runner.py:11765-11917`, `src/gas_calibrator/workflow/runner.py:16218-16290`, `src/gas_calibrator/workflow/runner.py:4661-4671` |
| device writeback | PASS | `src/gas_calibrator/workflow/runner.py:16460-16595`, `src/gas_calibrator/tools/run_v1_corrected_autodelivery.py:774-1006`, `tests/test_runner_v1_writeback_safety.py:162-199` |
| readback verify | PASS | `src/gas_calibrator/tools/run_v1_corrected_autodelivery.py:774-1006`, `src/gas_calibrator/devices/gas_analyzer.py:390-459`, `tests/test_runner_v1_writeback_safety.py:162-199` |
| rollback | PASS | `src/gas_calibrator/tools/run_v1_corrected_autodelivery.py:774-1006`, `tests/test_runner_v1_writeback_safety.py:202-231`, `tests/test_v1_writeback_fault_injection.py:150-165` |
| mode restore | PASS | `src/gas_calibrator/tools/run_v1_corrected_autodelivery.py:774-1006`, `tests/test_v1_writeback_fault_injection.py:108-119`, `tests/test_v1_writeback_fault_injection.py:185-200` |
| point save traceability | PASS | `src/gas_calibrator/workflow/runner.py:13814-13968`, `src/gas_calibrator/workflow/runner.py:14017-14047`, `tests/test_runner_v1_writeback_safety.py:262-334` |
| offline fault injection coverage | PASS | `tests/test_v1_writeback_fault_injection.py:108-119`, `tests/test_v1_writeback_fault_injection.py:122-138`, `tests/test_v1_writeback_fault_injection.py:150-165`, `tests/test_v1_writeback_fault_injection.py:168-182`, `tests/test_v1_writeback_fault_injection.py:185-200`, `tests/test_v1_writeback_fault_injection.py:203-215`, `tests/test_v1_writeback_fault_injection.py:218-231` |
| real-device abnormal recovery evidence | ONLINE_EVIDENCE_REQUIRED | `src/gas_calibrator/tools/run_v1_corrected_autodelivery.py:774-1006`, `src/gas_calibrator/devices/gas_analyzer.py:555-576`, `src/gas_calibrator/tools/run_v1_online_acceptance.py:608-760`, `src/gas_calibrator/tools/run_v1_online_acceptance.py:525-557` |
