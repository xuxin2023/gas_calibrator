# 风险清单

| 检查项 | 结论 | 风险等级 | 触发条件 / 说明 | 证据 |
| --- | --- | --- | --- | --- |
| V1 主流程有明确入口 | PASS | - | - | `run_app.py:1-24`, `src/gas_calibrator/ui/app.py:9760-9808`, `src/gas_calibrator/workflow/runner.py:4540-4619` |
| 流程顺序完整且闭环 | PASS | - | - | `src/gas_calibrator/workflow/runner.py:4540-4619`, `src/gas_calibrator/workflow/runner.py:4621-4628`, `src/gas_calibrator/workflow/runner.py:5165-5256`, `src/gas_calibrator/workflow/runner.py:5600-5642` |
| CO2 零点检查存在 | PASS | - | - | `src/gas_calibrator/workflow/runner.py:11125-11142`, `src/gas_calibrator/workflow/runner.py:11463-11549` |
| CO2 跨度存在 | PASS | - | - | `src/gas_calibrator/workflow/runner.py:5165-5256`, `src/gas_calibrator/workflow/runner.py:10896-11104` |
| H2O 零点存在 | UNKNOWN | - | - | `src/gas_calibrator/workflow/runner.py:11628-11780` |
| H2O 跨度存在 | UNKNOWN | - | - | `src/gas_calibrator/workflow/runner.py:11628-11780` |
| 进入校准模式存在 | PASS | - | - | `src/gas_calibrator/workflow/runner.py:16213-16335`, `src/gas_calibrator/devices/gas_analyzer.py:225-236` |
| 退出校准模式存在 | PASS | - | - | `src/gas_calibrator/workflow/runner.py:16213-16335` |
| 系数写入存在 | PASS | - | - | `src/gas_calibrator/workflow/runner.py:16213-16335`, `src/gas_calibrator/devices/gas_analyzer.py:349-373` |
| 系数写入后回读验证存在 | FAIL | High | 主 runner 的 `_maybe_write_coefficients` 只执行 `MODE=2 -> SENCO -> MODE=1`，没有 `GETCO`/等价比对；如果写入被设备部分接受、截断或被旧值覆盖，本流程自己无法发现。 | `src/gas_calibrator/workflow/runner.py:16213-16335`, `src/gas_calibrator/devices/gas_analyzer.py:390-459`, `src/gas_calibrator/tools/run_v1_corrected_autodelivery.py:661-818` |
| 每个点位有唯一标识 | PASS | - | - | `src/gas_calibrator/workflow/runner.py:13677-13811`, `src/gas_calibrator/workflow/runner.py:5108-5118`, `src/gas_calibrator/workflow/runner.py:5120-5130` |
| 每个点位有原始时间戳 | PASS | - | - | `src/gas_calibrator/workflow/runner.py:14815-15181`, `src/gas_calibrator/logging_utils.py:334-454` |
| 每个点位有保存时间戳 | FAIL | Medium | CSV/XLSX 只保存采样时间与设备时间，没有单独 `save_ts`/`insert_ts`；审查“何时落盘”时无法和采样时间区分。 | `src/gas_calibrator/workflow/runner.py:14815-15181`, `src/gas_calibrator/workflow/runner.py:13677-13811`, `src/gas_calibrator/logging_utils.py:938-940`, `src/gas_calibrator/logging_utils.py:942-960` |
| 点位保存前有稳态/等待/滤波逻辑 | PASS | - | - | `src/gas_calibrator/workflow/runner.py:9212-9483`, `src/gas_calibrator/workflow/runner.py:12624-12811`, `src/gas_calibrator/workflow/runner.py:12006-12125` |
| 点位保存不会覆盖前一点位 | PASS | - | - | `src/gas_calibrator/logging_utils.py:938-940`, `src/gas_calibrator/logging_utils.py:991-1007`, `src/gas_calibrator/logging_utils.py:1720-1756`, `tests/test_audit_v1_trace_check.py:107-156` |
| 点位保存不会混入上一点位过渡态数据 | PASS | - | - | `src/gas_calibrator/workflow/runner.py:12624-12811`, `src/gas_calibrator/workflow/runner.py:12006-12125`, `src/gas_calibrator/workflow/runner.py:13878-13897`, `tests/test_runner_route_handoff.py:76-142` |
| 报表导出/过程表生成存在 | PASS | - | - | `src/gas_calibrator/logging_utils.py:942-960`, `src/gas_calibrator/logging_utils.py:1720-1756`, `src/gas_calibrator/logging_utils.py:1262-1393`, `src/gas_calibrator/tools/run_v1_corrected_autodelivery.py:839-950` |
| 标定前后系数可追溯 | FAIL | Medium | 主 runner 不自动保存 before/after coefficient snapshot；如果只保留本轮主流程产物，无法直接追到写前系数。独立 sidecar 可以做 before/after，但未接入主 runner。 | `src/gas_calibrator/workflow/runner.py:16213-16335`, `src/gas_calibrator/workflow/runner.py:7138-7194`, `src/gas_calibrator/tools/run_v1_corrected_autodelivery.py:839-950` |
| 异常中断后不会把设备留在错误模式 | UNKNOWN | - | - | `src/gas_calibrator/workflow/runner.py:5600-5642`, `src/gas_calibrator/workflow/runner.py:5644-5696`, `src/gas_calibrator/workflow/runner.py:16213-16335` |
| 2026-04-03 以来的改动中，是否存在高风险改动 | FAIL | High | 2026-04-07 起把 postrun corrected delivery 接进主 runner，2026-04-12 又把默认配置改成 `enabled=True`、`write_devices=True`、`verify_short_run.enabled=True`。这会让一次完成的 V1 运行自动进入后处理写回/短验证链路，风险边界明显扩大。 | `src/gas_calibrator/workflow/runner.py:7138-7194`, `src/gas_calibrator/config.py:220-236` |

## 重点说明

- 最近高风险改动关联 commit: `1ebff243fdcf907d1b254add4d1ab05f9cb9d421`, `248d0ac69942415ac136b17e0aaa24e116e52db4`, `8fa3f3ecd44e1c3a853e095f182da6f9cf70e27e`
