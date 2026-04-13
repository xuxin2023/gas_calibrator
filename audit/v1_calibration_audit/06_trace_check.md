# 只读 Trace 检查

- 生成时间: 2026-04-13T12:47:29+08:00
- 命令: `python -m pytest -q tests/test_audit_v1_trace_check.py tests/test_runner_v1_writeback_safety.py tests/test_v1_writeback_fault_injection.py tests/test_v1_online_acceptance_tool.py`
- 总结论: PASS

## 结果

- 点位样本字段完整性: PASS | 证据: `tests/test_audit_v1_trace_check.py:26-104`, `src/gas_calibrator/workflow/runner.py:15056-15422`
- 不同点位不会互相覆盖: PASS | 证据: `tests/test_audit_v1_trace_check.py:107-156`, `src/gas_calibrator/logging_utils.py:1778-1814`, `src/gas_calibrator/logging_utils.py:1016-1032`
- 保存前存在稳定/窗口/新鲜度门禁: PASS | 证据: `tests/test_audit_v1_trace_check.py:159-176`, `src/gas_calibrator/workflow/runner.py:12761-12948`, `src/gas_calibrator/workflow/runner.py:12143-12262`
- 主流程写回系数有回读/回滚闭环: PASS | 证据: `tests/test_runner_v1_writeback_safety.py:162-199`, `tests/test_runner_v1_writeback_safety.py:202-231`, `src/gas_calibrator/workflow/runner.py:16460-16595`, `src/gas_calibrator/tools/run_v1_corrected_autodelivery.py:774-1006`
- 点位导出含 save_ts 且保留连续点位: PASS | 证据: `tests/test_runner_v1_writeback_safety.py:262-334`, `src/gas_calibrator/workflow/runner.py:13814-13968`, `src/gas_calibrator/workflow/runner.py:14095-14117`
- 异常恢复 fault injection 覆盖: PASS | 证据: `tests/test_v1_writeback_fault_injection.py:108-119`, `tests/test_v1_writeback_fault_injection.py:122-138`, `tests/test_v1_writeback_fault_injection.py:150-165`, `tests/test_v1_writeback_fault_injection.py:168-182`, `tests/test_v1_writeback_fault_injection.py:185-200`, `tests/test_v1_writeback_fault_injection.py:203-215`, `tests/test_v1_writeback_fault_injection.py:218-231`
- H2O zero/span 状态已显式收敛: PASS | 证据: `tests/test_runner_v1_writeback_safety.py:128-137`, `tests/test_runner_v1_writeback_safety.py:140-159`, `src/gas_calibrator/workflow/runner.py:4661-4671`
- online acceptance 双开关与 CO2-only 保护: PASS | 证据: `tests/test_v1_online_acceptance_tool.py:78-104`, `tests/test_v1_online_acceptance_tool.py:107-127`, `tests/test_v1_online_acceptance_tool.py:172-196`, `tests/test_v1_online_acceptance_tool.py:199-218`, `src/gas_calibrator/tools/run_v1_online_acceptance.py:608-760`

## 原始输出

```text
$ C:\Users\A\AppData\Local\Microsoft\WindowsApps\PythonSoftwareFoundation.Python.3.13_qbz5n2kfra8p0\python.exe -m pytest -q tests/test_audit_v1_trace_check.py tests/test_runner_v1_writeback_safety.py tests/test_v1_writeback_fault_injection.py tests/test_v1_online_acceptance_tool.py
..........................                                               [100%]
26 passed in 2.53s
```
