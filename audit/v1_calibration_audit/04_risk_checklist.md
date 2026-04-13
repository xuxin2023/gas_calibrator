# 风险清单

| 检查项 | 结论 | 风险等级 | 触发条件 / 说明 | 证据 |
| --- | --- | --- | --- | --- |
| V1 主流程有明确入口 | PASS | - | - | `run_app.py:1-24`, `src/gas_calibrator/ui/app.py:9760-9808`, `src/gas_calibrator/workflow/runner.py:4674-4757` |
| 流程顺序完整且闭环 | PASS | - | - | `src/gas_calibrator/workflow/runner.py:4674-4757`, `src/gas_calibrator/workflow/runner.py:4759-4766`, `src/gas_calibrator/workflow/runner.py:5303-5394`, `src/gas_calibrator/workflow/runner.py:5738-5780` |
| CO2 零点检查存在 | PASS | - | - | `src/gas_calibrator/workflow/runner.py:11270-11287`, `src/gas_calibrator/workflow/runner.py:11928-12024` |
| CO2 跨度存在 | PASS | - | - | `src/gas_calibrator/workflow/runner.py:5303-5394`, `src/gas_calibrator/workflow/runner.py:11034-11243` |
| H2O 零点存在 | NOT_SUPPORTED | - | 当前 HEAD 只确认 H2O 路由和 H2O ratio-poly 摘要选择存在，没有与 CO2 对等的 H2O zero 业务步骤。 | `src/gas_calibrator/workflow/runner.py:12175-12327`, `src/gas_calibrator/workflow/runner.py:16632-16704`, `src/gas_calibrator/h2o_summary_selection.py:52-78`, `src/gas_calibrator/workflow/runner.py:4647-4660`, `src/gas_calibrator/workflow/runner.py:4662-4672` |
| H2O 跨度存在 | NOT_SUPPORTED | - | 当前 HEAD 只确认 H2O 路由和 H2O ratio-poly 摘要选择存在，没有与 CO2 对等的 H2O span 业务步骤。 | `src/gas_calibrator/workflow/runner.py:12175-12327`, `src/gas_calibrator/workflow/runner.py:16632-16704`, `src/gas_calibrator/h2o_summary_selection.py:52-78`, `src/gas_calibrator/workflow/runner.py:4647-4660`, `src/gas_calibrator/workflow/runner.py:4662-4672` |
| 进入校准模式存在 | PASS | - | - | `src/gas_calibrator/workflow/runner.py:16874-17009`, `src/gas_calibrator/devices/gas_analyzer.py:225-236` |
| 退出校准模式存在 | PASS | - | - | `src/gas_calibrator/workflow/runner.py:16874-17009`, `src/gas_calibrator/tools/run_v1_corrected_autodelivery.py:781-1013` |
| 系数写入存在 | PASS | - | - | `src/gas_calibrator/workflow/runner.py:16874-17009`, `src/gas_calibrator/devices/gas_analyzer.py:349-373` |
| 系数写入后回读验证存在 | PASS | - | - | `src/gas_calibrator/workflow/runner.py:16874-17009`, `src/gas_calibrator/tools/run_v1_corrected_autodelivery.py:781-1013`, `src/gas_calibrator/devices/gas_analyzer.py:390-459` |
| 每个点位有唯一标识 | PASS | - | - | `src/gas_calibrator/workflow/runner.py:14228-14382`, `src/gas_calibrator/workflow/runner.py:5246-5256`, `src/gas_calibrator/workflow/runner.py:5258-5268` |
| 每个点位有原始时间戳 | PASS | - | - | `src/gas_calibrator/workflow/runner.py:15470-15836`, `src/gas_calibrator/logging_utils.py:354-474` |
| 每个点位有保存时间戳 | PASS | - | - | `src/gas_calibrator/workflow/runner.py:14509-14531`, `src/gas_calibrator/workflow/runner.py:14463-14507`, `tests/test_runner_v1_writeback_safety.py:262-334` |
| 点位保存前有稳态/等待/滤波逻辑 | PASS | - | - | `src/gas_calibrator/workflow/runner.py:9350-9621`, `src/gas_calibrator/workflow/runner.py:13171-13358`, `src/gas_calibrator/workflow/runner.py:12553-12672` |
| 点位保存不会覆盖前一点位 | PASS | - | - | `src/gas_calibrator/logging_utils.py:963-965`, `src/gas_calibrator/logging_utils.py:1016-1032`, `src/gas_calibrator/logging_utils.py:1778-1814`, `tests/test_audit_v1_trace_check.py:107-156` |
| 点位保存不会混入上一点位过渡态数据 | PASS | - | - | `src/gas_calibrator/workflow/runner.py:13171-13358`, `src/gas_calibrator/workflow/runner.py:12553-12672`, `src/gas_calibrator/workflow/runner.py:14533-14552`, `tests/test_runner_route_handoff.py:76-142` |
| 报表导出/过程表生成存在 | PASS | - | - | `src/gas_calibrator/logging_utils.py:967-985`, `src/gas_calibrator/logging_utils.py:1778-1814`, `src/gas_calibrator/logging_utils.py:1320-1451` |
| 标定前后系数可追溯 | PASS | - | - | `src/gas_calibrator/workflow/runner.py:17011-17054`, `src/gas_calibrator/logging_utils.py:1045-1061`, `tests/test_runner_v1_writeback_safety.py:162-199` |
| 离线 fault-injection 已覆盖异常恢复 | PASS | - | shared helper 已有 focused fault-injection tests，覆盖 set_mode / GETCO / rollback / 模式确认异常。 | `src/gas_calibrator/tools/run_v1_corrected_autodelivery.py:781-1013`, `tests/test_v1_writeback_fault_injection.py:108-119`, `tests/test_v1_writeback_fault_injection.py:122-138`, `tests/test_v1_writeback_fault_injection.py:150-165`, `tests/test_v1_writeback_fault_injection.py:168-182`, `tests/test_v1_writeback_fault_injection.py:185-200`, `tests/test_v1_writeback_fault_injection.py:203-215`, `tests/test_v1_writeback_fault_injection.py:218-231` |
| 真实设备异常恢复证据 | ONLINE_EVIDENCE_REQUIRED | - | 代码、离线注入和受双开关保护的 online acceptance 工具已经就位；但现场异常恢复仍缺真机协议证据。 | `src/gas_calibrator/tools/run_v1_corrected_autodelivery.py:781-1013`, `src/gas_calibrator/devices/gas_analyzer.py:555-576`, `src/gas_calibrator/tools/run_v1_online_acceptance.py:611-763`, `src/gas_calibrator/tools/run_v1_online_acceptance.py:528-560` |
| 2026-04-03 以来的改动中，是否存在高风险改动 | PASS | - | 默认真写设备风险、主路径无回读验证、点位无 save_ts、系数 before/after 缺失，已在当前 HEAD 上收口。 | `src/gas_calibrator/workflow/runner.py:4574-4602`, `src/gas_calibrator/workflow/runner.py:4604-4624`, `src/gas_calibrator/workflow/runner.py:16874-17009`, `src/gas_calibrator/workflow/runner.py:17011-17054`, `tests/test_runner_v1_writeback_safety.py:100-110` |

## 重点说明

- 历史高风险 commit 参考: `1ebff243fdcf907d1b254add4d1ab05f9cb9d421`, `248d0ac69942415ac136b17e0aaa24e116e52db4`, `8fa3f3ecd44e1c3a853e095f182da6f9cf70e27e`
- 本文件明确区分“代码已证明/离线已证明”和“现场仍缺证据”。
