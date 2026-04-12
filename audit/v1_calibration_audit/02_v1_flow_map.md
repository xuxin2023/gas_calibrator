# V1 校准主流程文字图

入口 -> 点表解析/重排 -> 按温度分组编排 -> CO2/H2O 路线执行 -> 稳态/门禁 -> 样本采集 -> 点位导出 -> 系数写入 -> 模式恢复 -> 清理/后处理

## 入口

- 默认入口文件: `run_app.py:1-24`。
- UI 启动后台执行: `src/gas_calibrator/ui/app.py:9760-9808`，其中创建 `RunLogger` 与 `CalibrationRunner`。
- 主执行函数: `src/gas_calibrator/workflow/runner.py:4540-4619`。

## 步骤编排

- 点表解析: `src/gas_calibrator/data/points.py:98-288`；`CalibrationPoint.index` 直接使用 Excel 行号。
- 点位重排: `src/gas_calibrator/data/points.py:305-341`；高温段可先水路后气路。
- 温度分组调度: `src/gas_calibrator/workflow/runner.py:4621-4628`。
- 单温度组编排: `src/gas_calibrator/workflow/runner.py:5165-5256`，把 H2O 组和 CO2 源点编进 `route_plan`。
- CO2 主链路: `src/gas_calibrator/workflow/runner.py:10896-11104`。
- H2O 主链路: `src/gas_calibrator/workflow/runner.py:11628-11780`。

## 设备指令与通信层

- 进入/切换模式 `MODE`: `src/gas_calibrator/devices/gas_analyzer.py:225-236`。
- 系数写入 `SENCO`: `src/gas_calibrator/devices/gas_analyzer.py:349-373`。
- 系数回读 `GETCO`: `src/gas_calibrator/devices/gas_analyzer.py:390-459`。
- 被动读数 `READDATA`: `src/gas_calibrator/devices/gas_analyzer.py:461-472`；主动流读取/解析见 `src/gas_calibrator/devices/gas_analyzer.py:517-535`, `src/gas_calibrator/devices/gas_analyzer.py:662-671`。

## 数据采集与稳态门禁

- 传感器稳态窗口: `src/gas_calibrator/workflow/runner.py:9212-9483`；基于时间窗峰峰值判稳，不是简单“切点后立即取最新值”。
- 压力达标后的二次等待/门禁: `src/gas_calibrator/workflow/runner.py:12624-12811`；包含 post-seal dewpoint gate、CO2 长稳守护、adaptive pressure gate、最小等待时长。
- 采样 freshness gate: `src/gas_calibrator/workflow/runner.py:12006-12125`。
- 样本行组装: `src/gas_calibrator/workflow/runner.py:14815-15181`。
- 点位采样与导出编排: `src/gas_calibrator/workflow/runner.py:15286-15549`。

## 点位保存与报表

- 样本级 CSV 追加: `src/gas_calibrator/logging_utils.py:938-940`, `src/gas_calibrator/logging_utils.py:756-818`。
- 点位汇总 CSV/可读 CSV/XLSX: `src/gas_calibrator/logging_utils.py:942-960`, `src/gas_calibrator/logging_utils.py:991-1007`。
- 单点位样本文件: `src/gas_calibrator/logging_utils.py:1720-1756`，文件名包含 `point_row + phase + tag`。
- 点位汇总 payload 构造: `src/gas_calibrator/workflow/runner.py:13677-13811`。
- 分析仪汇总表: `src/gas_calibrator/logging_utils.py:1262-1393`。

## 系数写入/回读/恢复

- 主 runner 的系数写入路径: `src/gas_calibrator/workflow/runner.py:16213-16335`；会 `MODE=2 -> SENCO -> MODE=1`，但该路径没有调用 `GETCO` 回读比较。
- corrected autodelivery 旁路: `src/gas_calibrator/tools/run_v1_corrected_autodelivery.py:290-333`, `src/gas_calibrator/tools/run_v1_corrected_autodelivery.py:661-818`, `src/gas_calibrator/tools/run_v1_corrected_autodelivery.py:626-651`；这里对 CO2 使用 1/3 组、对 H2O 使用 2/4 组，并执行 `GETCO` 回读匹配。
- 运行结束后自动触发 corrected autodelivery 的 hook: `src/gas_calibrator/workflow/runner.py:7138-7194`。
- 清理与基线恢复: `src/gas_calibrator/workflow/runner.py:5600-5642`, `src/gas_calibrator/workflow/runner.py:5644-5696`。

## 明确结论

- CO2 零点检查: 有。见 `src/gas_calibrator/workflow/runner.py:11125-11142`, `src/gas_calibrator/workflow/runner.py:11463-11549`。
- CO2 标气跨度: 有。`_run_temperature_group` 会按 CO2 源点 ppm 扫描，`_run_co2_point` 负责执行。见 `src/gas_calibrator/workflow/runner.py:5165-5256`, `src/gas_calibrator/workflow/runner.py:10896-11104`。
- H2O 零点/跨度: 只能确认 H2O 路线与多组湿度点存在，未找到明确以“零点/跨度”命名或判定的业务步骤；见 `src/gas_calibrator/workflow/runner.py:11628-11780`。结论: UNKNOWN。
- MODE=校准模式 与恢复正常模式: 有。主 runner 在写 SENCO 时 `MODE=2 -> MODE=1`，见 `src/gas_calibrator/workflow/runner.py:16213-16335`。
- 系数写入后 GETCO 或等价回读验证: 旁路 corrected autodelivery 有，主 runner 当前主路径没有。主路径结论: FAIL；旁路能力见 `src/gas_calibrator/tools/run_v1_corrected_autodelivery.py:661-818`。

## CO2/H2O 链路关系

- 结构上是两套并行链路，不是只有 CO2 完整实现。CO2 执行入口见 `src/gas_calibrator/workflow/runner.py:10896-11104`，H2O 执行入口见 `src/gas_calibrator/workflow/runner.py:11628-11780`。
- 但 H2O 的“零点/跨度”业务语义在当前代码中没有像 CO2 zero-gas 那样被显式建模，因此这部分不能直接判定闭环完成。
