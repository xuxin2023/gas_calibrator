# V1.5 采集后离线闭环 Runbook

## 目标

本文档固化 V1.5 正式流程中，CO2/H2O 开放流通采集完成后的离线闭环步骤。它解决的是：

```text
采集完成
-> 候选系数评审
-> 受控写入包
-> 写后复验计划
-> 归档/数据库/报告/证书缺口
-> 总证据状态
```

这一步不是运行真机，也不是写入系数。它的作用是把已经采到的标准气、露点、压力、温度、工厂模式信号、QC、候选系数和报告证据整理成可审计的下一步计划。

## 默认入口

在已完成采集的运行目录上，默认使用 full-flow 工具的 post-acquisition closure 模式：

```powershell
$env:PYTHONPATH = "src"
python -m gas_calibrator.tools.run_v1_5_full_calibration_chain `
  --config <config.json> `
  --output-dir <closure_output_dir> `
  --run-id <closure_run_id> `
  --reviewed-run-dir <completed_run_dir> `
  --archive-plan-json <formal_plan_snapshot.json> `
  --pressure-reference-json <pressure_reference.json> `
  --evidence-bundle-json <evidence_bundle.json> `
  --post-acquisition-closure
```

`--post-acquisition-closure` 会在离线边界内串起：

```text
post_run_coefficient_executor
-> full_flow_closure_readiness
-> run_evidence_status refresh
```

除非是在补证据或排查单个 artifact，不建议手工分散运行多个导出器。

## 物理边界

采集后闭环工具必须保持离线审查属性：

- 不打开 COM。
- 不控制继电器、气路阀、水路阀、湿度发生器或 PACE。
- 不写 SENCO，不清 CLEARSENCO。
- 不修改设备 ID。
- 不运行 V2 real COM。
- 不把工程诊断数据提升为正式 acceptance。

物理意义：采集后闭环只判断“证据能否支持下一步”，不能替代采样时的开放流通、露点稳定、ratio 稳定、状态寄存器正常和每台设备独立判稳。

## 输出工件

`post_run_coefficient_executor/` 至少应生成：

- `executor_manifest.json`
- `executor_summary.md`
- `executor_stages.csv`
- `device_eligibility.csv`
- `coefficient_execution_plan.csv`
- `controlled_write_package.csv`
- `post_write_reverification_plan.csv`
- `archive_gap_list.csv`

`full_flow_closure_readiness/` 至少应生成：

- `v1_5_full_flow_closure_readiness.json`
- `v1_5_full_flow_closure_readiness.md`
- `v1_5_full_flow_closure_stages.csv`
- `v1_5_full_flow_closure_gaps.csv`
- `v1_5_full_flow_device_closure.csv`
- `v1_5_full_flow_release_domains.csv`

最终 `v1_5_run_evidence_status.json` / `.md` 应重新刷新，并索引上述两个阶段。

## 中文建议要求

所有 blocked / partial 状态必须同时保留内部 key 和中文解释：

- 内部 key 用于程序复算和数据库索引。
- 中文原因用于操作员、工程师、审核员快速理解问题。
- 中文下一步必须说明是补证据、重跑评审、阻断单台设备、还是进入受控写入评审。

典型解释：

| 内部原因 | 中文解释 |
| --- | --- |
| `needs_senco9_review_or_calibration` | 压力输入量 P 未闭环，应先做 SENCO9 评审/校准/复验。 |
| `needs_senco78_review_or_temperature_gate` | 温度输入量 T 未闭环，应先做温度评审或 SENCO7/SENCO8 修复。 |
| `h2o_blocked:model_matrix_rank_deficient` | H2O 拟合矩阵秩不足，湿度点、干气锚点、温度覆盖或有效样本不足。 |
| `ratio_stable_but_curve_inconsistent_not_window_noise` | ratio 虽稳定，但曲线/光学健康不符合标气响应，需要查 ref_signal、signal、SETCO2、SETPOW 和状态寄存器。 |
| `archive_gap_count=*` | 归档缺口未闭合，需要补齐原始数据、QC、报告、证书、数据库或 hash 证据。 |

## 校准合同

- 使用设备自身 ID 作为身份，不按 COM 或 GA 标签写系数。
- 压力 P 先通过 SENCO9 独立评审/校准，CO2/H2O 当前大气开放流通主拟合不引入压力项。
- 温度 T 先通过 SENCO7/SENCO8 评审/修正，避免温度错误被组分系数吸收。
- CO2 零气低端锚点和 H2O 干气低水锚点物理意义不同，不能混用。
- fit / verification 标签不默认排除样本；只要样本满足稳定、证书、状态寄存器和物理门禁，就可以进入拟合。
- 某台设备异常只阻断该设备，不拖死其它设备。
- 采样窗口必须在气路/水路保持开放流通时取得，采样完成后才允许关阀。
- S5/S6 是输出层线性修正，应在 S1/S3、S2/S4 主链路之后评审。

## 完成标准

一次采集后闭环完成时，应能证明：

- 每台设备是否可进入候选系数评审有明确原因。
- 受控写入包只是一份计划，不是自动写入动作。
- 写后复验计划明确复验点、采样物理状态和验收口径。
- 归档缺口清单能提示缺失证据。
- 总证据状态能从同一运行目录重建。
- 所有输出可追溯到本次 run、配置、标准气、参考设备和原始采样证据。
