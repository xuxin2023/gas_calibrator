# Fault Injection Matrix

- generated_at: 2026-04-13T12:47:29+08:00

| scenario | offline result | expectation | evidence |
| --- | --- | --- | --- |
| set_mode(2) 失败 | PASS | 会尝试退出模式，不把 attempted 当成 confirmed | `tests/test_v1_writeback_fault_injection.py:108-119`, `src/gas_calibrator/tools/run_v1_corrected_autodelivery.py:774-1006` |
| set_senco 中途异常 | PASS | 会回滚并恢复模式 | `tests/test_v1_writeback_fault_injection.py:122-138`, `src/gas_calibrator/tools/run_v1_corrected_autodelivery.py:774-1006` |
| GETCO 超时 | PASS | verify 失败后回滚，最终模式恢复 | `tests/test_v1_writeback_fault_injection.py:150-165`, `src/gas_calibrator/tools/run_v1_corrected_autodelivery.py:774-1006` |
| GETCO 空返回 | PASS | 空返回不算成功，会进入失败/回滚路径 | `tests/test_v1_writeback_fault_injection.py:150-165`, `src/gas_calibrator/tools/run_v1_corrected_autodelivery.py:774-1006` |
| GETCO 解析异常 | PASS | 解析异常不算成功，会进入失败/回滚路径 | `tests/test_v1_writeback_fault_injection.py:150-165`, `src/gas_calibrator/tools/run_v1_corrected_autodelivery.py:774-1006` |
| 回读不一致 | PASS | readback mismatch 会失败并尝试回滚 | `tests/test_v1_writeback_fault_injection.py:150-165`, `src/gas_calibrator/tools/run_v1_corrected_autodelivery.py:774-1006` |
| rollback 写入失败 | PASS | rollback_confirmed=False，unsafe=True | `tests/test_v1_writeback_fault_injection.py:168-182`, `src/gas_calibrator/tools/run_v1_corrected_autodelivery.py:774-1006` |
| set_mode(1) 退出失败 | PASS | mode_exit_confirmed=False，unsafe=True | `tests/test_v1_writeback_fault_injection.py:185-200`, `src/gas_calibrator/tools/run_v1_corrected_autodelivery.py:774-1006` |
| read_current_mode_snapshot 不可用 | PASS | 无法确认安全退出时标记 unsafe=True | `tests/test_v1_writeback_fault_injection.py:203-215`, `src/gas_calibrator/tools/run_v1_corrected_autodelivery.py:774-1006` |
| 已尝试退出但无法确认最终模式 | PASS | mode_exit_attempted=True 且 mode_exit_confirmed=False | `tests/test_v1_writeback_fault_injection.py:218-231`, `src/gas_calibrator/tools/run_v1_corrected_autodelivery.py:774-1006` |
