# V1.5 采集后闭环自动化 Runbook

## 目标

本文件固定 V1.5 正式流程中 CO2/H2O 开放流通采集完成后的离线闭环步骤。它解决的是“采集完成以后，如何自动生成候选系数评审、受控写入包、复验计划、归档缺口和总证据状态”，不是执行真机控制。

闭环顺序应为：

```text
CO2/H2O open-flow no-write 采集完成
↓
fit input quality review
↓
post-run coefficient executor
↓
full-flow closure readiness
↓
controlled write review
↓
post-write reverification
↓
final archive / certificates
```

## 物理边界

采集后闭环工具必须保持离线审查属性：

- 不打开 COM。
- 不控制继电器、气路阀、水路阀、湿度发生器或 PACE。
- 不写 SENCO，不清 CLEARSENCO。
- 不运行 V2 real COM。
- 不修改 `run_app.py` 默认入口。
- 不把工程诊断数据提升为正式 acceptance。

其物理意义是把已经采集到的真实开放流通数据、标准气/露点/压力/温度证据、QC 结论和候选系数动作整理为可审计证据链。它不能替代采样时的开放流通、露点稳定、ratio 稳定、状态寄存器正常和每台设备独立判稳。

## 推荐入口

在源码工作区运行时先确保 `PYTHONPATH=src`，然后使用：

```powershell
$env:PYTHONPATH = "src"
python -m gas_calibrator.tools.run_v1_5_full_calibration_chain `
  --config <config.json> `
  --output-dir <run_dir> `
  --run-id <run_id> `
  --reviewed-run-dir <completed_run_dir> `
  --archive-plan-json <formal_plan_snapshot.json> `
  --pressure-reference-json <pressure_reference.json> `
  --full-flow-closure-readiness
```

`--full-flow-closure-readiness` 会自动触发 `post_run_coefficient_executor`。因此采集完成后不需要手工分别运行两个导出器，除非是在工程排查中单独补证据。

## 输出工件

`post_run_coefficient_executor/` 应至少生成：

- `executor_manifest.json`
- `executor_summary.md`
- `executor_stages.csv`
- `device_eligibility.csv`
- `coefficient_execution_plan.csv`
- `controlled_write_package.csv`
- `post_write_reverification_plan.csv`
- `archive_gap_list.csv`

`full_flow_closure_readiness/` 应至少生成：

- `v1_5_full_flow_closure_readiness.json`
- `v1_5_full_flow_closure_readiness.md`
- `v1_5_full_flow_closure_stages.csv`
- `v1_5_full_flow_closure_gaps.csv`
- `v1_5_full_flow_device_closure.csv`
- `v1_5_full_flow_release_domains.csv`

最终 `v1_5_run_evidence_status.json` / `.md` 必须重新刷新，并索引上述两个阶段。

## 证据状态验收

`post_run_coefficient_executor` 阶段通过的最低条件：

- 有 executor manifest。
- 有逐台 device eligibility。
- 有 controlled write package。
- 有 post-write reverification plan。
- 有 archive gap list。

`full_flow_closure_readiness` 阶段通过的最低条件：

- 有 closure readiness 主文件。
- 有 closure gap list。
- 有 per-device closure 表。
- 有 release domain 表。

CLI JSON 输出应显式包含：

- `post_run_coefficient_executor_manifest`
- `post_run_coefficient_executor_controlled_write_package`
- `post_run_coefficient_executor_post_write_reverification_plan`
- `post_run_coefficient_executor_archive_gap_list`
- `full_flow_closure_readiness_stages`
- `full_flow_closure_readiness_release_domains`
- `run_evidence_status_final_json`

## 系数与低端锚点原则

CO2 和 H2O 的低端锚点不能混用：

- CO2 的低端锚点是零气或低 CO2 标气，应围绕 CO2 标准值、CO2 ratio、温度、压力、水汽状态进行建模。
- H2O 的低端锚点是干气/低露点状态，应围绕 H2O ratio、露点、dry/wet ppmv、温度和压力进行建模。

采集后闭环只能检查这些证据是否齐全、是否被正确引用，不能把缺失的低端物理状态用另一个组分的低端点替代。

## 完成标准

一次采集后闭环完成时，应能证明：

- 每台设备是否可进入候选系数评审有明确原因。
- 写入包只是一份受控计划，不是自动写入动作。
- 复验计划明确写后验证点和验收状态。
- 归档缺口清单能提示缺失证据。
- 总证据状态能从同一运行目录重建。
- 所有输出可追溯到本次 run、配置、标准气、参考设备和原始采样证据。

