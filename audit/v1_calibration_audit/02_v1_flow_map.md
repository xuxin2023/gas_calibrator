# V1 校准主流程图

入口 -> 点表解析/重排 -> 温度分组编排 -> CO2/H2O 路由执行 -> 稳态/门禁 -> 样本采集 -> 点位保存 -> 系数写前快照/写入/回读/回滚 -> 模式恢复 -> 清理/后处理

## 入口

- 默认入口文件: `run_app.py:1-24`
- UI 后台启动: `src/gas_calibrator/ui/app.py:9760-9808`
- 主执行函数: `src/gas_calibrator/workflow/runner.py:4673-4756`

## 步骤编排

- 点表解析: `src/gas_calibrator/data/points.py:98-288`
- 点位重排: `src/gas_calibrator/data/points.py:305-341`
- 点位主调度: `src/gas_calibrator/workflow/runner.py:4758-4765`
- 温度组编排: `src/gas_calibrator/workflow/runner.py:5302-5393`
- CO2 主链: `src/gas_calibrator/workflow/runner.py:11033-11241`
- H2O 主链: `src/gas_calibrator/workflow/runner.py:11765-11917`

## 设备指令

- MODE: `src/gas_calibrator/devices/gas_analyzer.py:225-236`
- SENCO: `src/gas_calibrator/devices/gas_analyzer.py:349-373`
- GETCO: `src/gas_calibrator/devices/gas_analyzer.py:390-459`
- READDATA: `src/gas_calibrator/devices/gas_analyzer.py:461-472`, `src/gas_calibrator/devices/gas_analyzer.py:517-535`, `src/gas_calibrator/devices/gas_analyzer.py:685-694`

## 数据采集与保存

- 稳态判定: `src/gas_calibrator/workflow/runner.py:9349-9620`
- 压力后门禁: `src/gas_calibrator/workflow/runner.py:12761-12948`
- freshness gate: `src/gas_calibrator/workflow/runner.py:12143-12262`
- 样本采集: `src/gas_calibrator/workflow/runner.py:15056-15422`
- 点位采样与导出: `src/gas_calibrator/workflow/runner.py:15527-15796`
- 点位汇总行: `src/gas_calibrator/workflow/runner.py:13814-13968`
- 样本/点位/写回导出: `src/gas_calibrator/logging_utils.py:963-965`, `src/gas_calibrator/logging_utils.py:967-985`, `src/gas_calibrator/logging_utils.py:1045-1061`

## 系数写入闭环

- 主 runner 写回入口: `src/gas_calibrator/workflow/runner.py:16460-16595`
- shared helper: `src/gas_calibrator/tools/run_v1_corrected_autodelivery.py:774-1006`
- 写回持久化: `src/gas_calibrator/workflow/runner.py:16597-16640`
- 模式快照: `src/gas_calibrator/devices/gas_analyzer.py:555-576`

## 结论

- CO2 零点检查: PASS | 证据 `src/gas_calibrator/workflow/runner.py:11262-11279`, `src/gas_calibrator/workflow/runner.py:11600-11686`
- CO2 跨度: PASS | 证据 `src/gas_calibrator/workflow/runner.py:5302-5393`, `src/gas_calibrator/workflow/runner.py:11033-11241`
- H2O 零点: NOT_SUPPORTED | 证据 `src/gas_calibrator/workflow/runner.py:11765-11917`, `src/gas_calibrator/workflow/runner.py:16218-16290`, `src/gas_calibrator/h2o_summary_selection.py:29-42`, `src/gas_calibrator/workflow/runner.py:4646-4659`, `src/gas_calibrator/workflow/runner.py:4661-4671`
- H2O 跨度: NOT_SUPPORTED | 证据 `src/gas_calibrator/workflow/runner.py:11765-11917`, `src/gas_calibrator/workflow/runner.py:16218-16290`, `src/gas_calibrator/h2o_summary_selection.py:29-42`, `src/gas_calibrator/workflow/runner.py:4646-4659`, `src/gas_calibrator/workflow/runner.py:4661-4671`
- MODE=校准模式与恢复正常模式: PASS | 证据 `src/gas_calibrator/workflow/runner.py:16460-16595`, `src/gas_calibrator/tools/run_v1_corrected_autodelivery.py:774-1006`, `src/gas_calibrator/devices/gas_analyzer.py:225-236`
- 系数写入后 GETCO 回读验证: PASS | 证据 `src/gas_calibrator/workflow/runner.py:16460-16595`, `src/gas_calibrator/tools/run_v1_corrected_autodelivery.py:774-1006`, `src/gas_calibrator/devices/gas_analyzer.py:390-459`

## CO2 / H2O 关系

- 结构上是两套并行链路。CO2 执行入口见 `src/gas_calibrator/workflow/runner.py:11033-11241`，H2O 执行入口见 `src/gas_calibrator/workflow/runner.py:11765-11917`。
- 但“存在 H2O 路由/点位”不等于“存在 H2O zero/span 业务闭环”；当前 HEAD 的明确结论是 NOT_SUPPORTED，而不是 UNKNOWN。
