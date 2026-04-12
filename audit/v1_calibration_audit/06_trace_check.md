# 只读 Trace 检查

- 生成时间: 2026-04-12T21:39:47+08:00
- 命令: `python -m pytest -q tests/test_audit_v1_trace_check.py`
- 总结论: PASS

## 结果

- 点位样本字段完整性: PASS | 证据: `tests/test_audit_v1_trace_check.py:26-104`, `src/gas_calibrator/workflow/runner.py:14815-15181`
- 不同点位不会互相覆盖: PASS | 证据: `tests/test_audit_v1_trace_check.py:107-156`, `src/gas_calibrator/logging_utils.py:1720-1756`, `src/gas_calibrator/logging_utils.py:991-1007`
- 保存前存在稳定/窗口/新鲜度门禁: PASS | 证据: `tests/test_audit_v1_trace_check.py:159-176`, `src/gas_calibrator/workflow/runner.py:12624-12811`, `src/gas_calibrator/workflow/runner.py:12006-12125`

## 原始输出

```text
$ C:\Users\A\AppData\Local\Microsoft\WindowsApps\PythonSoftwareFoundation.Python.3.13_qbz5n2kfra8p0\python.exe -m pytest -q tests/test_audit_v1_trace_check.py
...                                                                      [100%]
3 passed in 1.24s
```
