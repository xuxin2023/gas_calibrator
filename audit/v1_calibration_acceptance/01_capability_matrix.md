# Capability Matrix

- generated_at: 2026-04-13T14:58:16+08:00
- head: `367a1089ebaca1388dbb9d11648f74513316e502`

| capability | status | evidence |
| --- | --- | --- |
| CO2 zero | PASS | `src/gas_calibrator/workflow/runner.py:11270-11287`, `src/gas_calibrator/workflow/runner.py:11928-12024` |
| CO2 span | PASS | `src/gas_calibrator/workflow/runner.py:5303-5394`, `src/gas_calibrator/workflow/runner.py:11034-11243` |
| H2O zero | NOT_SUPPORTED | `src/gas_calibrator/workflow/runner.py:12175-12327`, `src/gas_calibrator/workflow/runner.py:16632-16704`, `src/gas_calibrator/workflow/runner.py:4662-4672` |
| H2O span | NOT_SUPPORTED | `src/gas_calibrator/workflow/runner.py:12175-12327`, `src/gas_calibrator/workflow/runner.py:16632-16704`, `src/gas_calibrator/workflow/runner.py:4662-4672` |
| device writeback | PASS | `src/gas_calibrator/workflow/runner.py:16874-17009`, `src/gas_calibrator/tools/run_v1_corrected_autodelivery.py:781-1013`, `tests/test_runner_v1_writeback_safety.py:162-199` |
| readback verify | PASS | `src/gas_calibrator/tools/run_v1_corrected_autodelivery.py:781-1013`, `src/gas_calibrator/devices/gas_analyzer.py:390-459`, `tests/test_runner_v1_writeback_safety.py:162-199` |
| rollback | PASS | `src/gas_calibrator/tools/run_v1_corrected_autodelivery.py:781-1013`, `tests/test_runner_v1_writeback_safety.py:202-231`, `tests/test_v1_writeback_fault_injection.py:150-165` |
| mode restore | PASS | `src/gas_calibrator/tools/run_v1_corrected_autodelivery.py:781-1013`, `tests/test_v1_writeback_fault_injection.py:108-119`, `tests/test_v1_writeback_fault_injection.py:185-200` |
| point save traceability | PASS | `src/gas_calibrator/workflow/runner.py:14228-14382`, `src/gas_calibrator/workflow/runner.py:14431-14461`, `tests/test_runner_v1_writeback_safety.py:262-334` |
| offline fault injection coverage | PASS | `tests/test_v1_writeback_fault_injection.py:108-119`, `tests/test_v1_writeback_fault_injection.py:122-138`, `tests/test_v1_writeback_fault_injection.py:150-165`, `tests/test_v1_writeback_fault_injection.py:168-182`, `tests/test_v1_writeback_fault_injection.py:185-200`, `tests/test_v1_writeback_fault_injection.py:203-215`, `tests/test_v1_writeback_fault_injection.py:218-231` |
| real-device abnormal recovery evidence | ONLINE_EVIDENCE_REQUIRED | `src/gas_calibrator/tools/run_v1_corrected_autodelivery.py:781-1013`, `src/gas_calibrator/devices/gas_analyzer.py:555-576`, `src/gas_calibrator/tools/run_v1_online_acceptance.py:611-763`, `src/gas_calibrator/tools/run_v1_online_acceptance.py:528-560` |
