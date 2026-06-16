# V1.5 工具入口清理候选报告

生成时间：2026-06-07

本报告用于回答一个很具体的问题：压力校准完成后，哪些旧程序、诊断程序、写入程序或旁路入口容易再次把 V1.5 正式流程搞乱。它不是删除清单，也不改变任何运行逻辑。

## 一句话结论

第一步“正式入口和 V1.5 流程合同梳理”基本已经完成；但“危险入口隔离、旧入口阻断、无用文件删除”还没有完全闭环。

当前项目已经有：

- `docs/v1_5_entrypoint_inventory/v1_5_formal_entrypoints.md`
- `docs/v1_5_entrypoint_inventory/v1_5_file_convergence_report.md`
- `docs/v1_5_flow_contract/v1_5_formal_flow_contract.md`
- `docs/v1_5_flow_contract/v1_5_stage_detail_and_route_optimization.md`

这些文件已经把 V1.5 正式路线、诊断边界、受控写入边界说清楚了。但 `src/gas_calibrator/tools` 里仍有一些入口容易被误用，尤其是压力诊断、封路压力调试、旧 V1 写入/acceptance 工具、SENCO 清除写入工具。

## 本次检查边界

本次只做静态文件和文档检查：

- 不打开 COM。
- 不控制 PACE。
- 不控制气路、水路、阀门、湿度发生器。
- 不写 SENCO。
- 不删除文件。
- 不修改 `run_app.py`。

## 已完成的部分

### 1. 正式入口清单已存在

现有 inventory 已经识别出 V1.5 相关入口、模块和测试共 `191` 个，并划分为：

| 类别 | 数量 | 当前判断 |
|---|---:|---|
| `formal_runner` | 4 | 正式开放流通采样入口，保留 |
| `full_flow_orchestration` | 4 | 正式流程编排/监督入口，保留 |
| `formal_review_evidence` | 59 | 离线评审、报告、证据、候选系数工具，保留 |
| `controlled_write` | 9 | 受控写入入口，保留但必须硬门禁 |
| `diagnostic_only` | 7 | 工程诊断入口，保留但必须隔离 |
| `evidence_database` | 5 | 证据数据库相关，保留 |
| `advanced_qc` | 10 | 高级质控，保留 |
| `ui_review` | 3 | 评审/操作台显示，保留 |
| `test_gate` | 85 | 测试门禁，保留 |

### 2. 正式物理合同已写明

正式 V1.5 路线已经固定为：

1. 设备身份和旧系数快照。
2. 压力通道独立验证/校准。
3. 温度通道评审。
4. 开放流通 CO2 主校准。
5. 开放流通 H2O 主校准。
6. QC 和候选系数评审。
7. 受控写入。
8. 写后复验。
9. 证据归档和报告。

物理边界也已经写明：

- CO2/H2O 主拟合必须用开放流通、稳定、可追溯的样本。
- 压力 P 是独立输入量，不能被 CO2/H2O 系数吸收。
- 封路压力点、VENT-hold、dynamic pressure、short diagnostic 默认不能进入正式 CO2/H2O 拟合。
- S1/S3、S2/S4 是主 ratio/温度响应链路；S5/S6 是最终显示层线性修正，不能混进主拟合。

## 还没有完全闭环的部分

### A. 已分类但仍容易误用的诊断入口

这些工具已经被 inventory 标成 `diagnostic_only`，不应该删除，因为它们有工程排查价值；但建议下一步加显式诊断解锁参数，避免误当正式流程运行。

| 文件 | 当前建议 | 原因 |
|---|---|---|
| `src/gas_calibrator/tools/run_v1_5_open_flow_dynamic_pressure_diagnostic.py` | 保留但隔离 | 动态压力诊断，不能进入正式 CO2/H2O 拟合 |
| `src/gas_calibrator/tools/run_v1_5_sealed_pressure_tune_900.py` | 保留但隔离 | 封路压力调试，不是正式主校准 |
| `src/gas_calibrator/tools/run_v1_5_pace_mode_ingress_diagnostic.py` | 保留但隔离 | PACE 模式排查工具 |
| `src/gas_calibrator/tools/run_v1_5_no_outp_preseal_probe.py` | 保留但隔离 | preseal/NO OUTP 工程探针 |
| `src/gas_calibrator/tools/run_v1_5_dewpoint_gate_extended_hold_after_gate.py` | 保留但隔离 | 水路诊断观察工具，不是主入口 |
| `src/gas_calibrator/tools/probe_v1_5_getco9_protocol.py` | 保留但隔离 | 协议探针，不能当压力校准入口 |
| `src/gas_calibrator/tools/export_v1_5_co2_post_h2o_diagnostic.py` | 保留但标识 | 离线诊断，不是 acceptance |

建议后续统一要求：

```text
--engineering-diagnostic
--not-real-acceptance
--operator-confirmation DIAGNOSTIC_ONLY
```

这不是为了麻烦，而是为了防止某个诊断工具再次绕过正式压力/气路/水路合同。

### B. 清单外但与 V1.5/压力/SENCO 有关的入口

这些文件在本次扫描中没有完全进入当前 V1.5 inventory，建议优先补分类。

| 文件 | 建议分类 | 处理建议 |
|---|---|---|
| `src/gas_calibrator/tools/validate_pressure_only.py` | `formal_pressure_no_write_runner` | 保留；这是压力通道 no-write 评估核心入口，但要在 inventory 中明确“显式控制压力才控压” |
| `src/gas_calibrator/tools/run_v1_5_pressure_senco9_clear_controlled_write.py` | `controlled_write` | 保留；这是 SENCO9 恢复/清除工具，必须受控写入分类 |
| `src/gas_calibrator/tools/build_v1_5_h2o_archive_inputs.py` | `formal_review_evidence` | 保留；只聚合证据，不打开 COM，不控制路线 |
| `src/gas_calibrator/tools/export_v1_5_calibration_capability.py` | `formal_review_evidence` | 保留；能力评估/报告类入口 |
| `src/gas_calibrator/tools/export_single_gas_pressure_curve.py` | `diagnostic_only` 或 `formal_review_evidence` | 建议标为诊断分析，不进入正式拟合 |
| `src/gas_calibrator/tools/run_room_temp_co2_pressure_diagnostic.py` | `diagnostic_only` | 保留但隔离；它是室温 CO2/压力诊断，不是 V1.5 正式入口 |

### C. 旧 V1/历史入口需要阻断或迁移

这些文件不是 V1.5 正式流程主入口，且有写入或 acceptance 语义。建议不要马上删除，因为可能还保存着旧算法或审计线索；但应该从 V1.5 导航中移出，并明确“不要从这里启动 V1.5”。

| 文件 | 风险 | 建议 |
|---|---|---|
| `src/gas_calibrator/tools/run_v1_corrected_autodelivery.py` | 旧 V1 自动交付/写入逻辑，包含 SENCO 写入和压力行处理 | 标为 `legacy_v1_do_not_use_for_v1_5`，后续只作为算法参考 |
| `src/gas_calibrator/tools/run_v1_merged_calibration_sidecar.py` | 旧合并拟合/写入 sidecar，包含温度和气体写入逻辑 | 标为 `legacy_v1_reference_only` |
| `src/gas_calibrator/tools/run_v1_online_acceptance.py` | 旧 online acceptance 入口，可能误导为正式放行 | 标为 `legacy_acceptance_blocked_for_v1_5` |
| `src/gas_calibrator/tools/run_v1_no500_postprocess.py` | 旧 no-500 后处理 | 保留为历史离线后处理，但不能进入 V1.5 主路 |

这些文件的危险不在于“存在”，而在于它们名字像可运行入口，并且含有旧写入/acceptance 语义。

## 建议的下一步清理顺序

### 第 1 步：补齐 inventory 分类

把本报告列出的清单外入口补进 inventory 规则，优先处理：

1. `validate_pressure_only.py`
2. `run_v1_5_pressure_senco9_clear_controlled_write.py`
3. `build_v1_5_h2o_archive_inputs.py`
4. `export_v1_5_calibration_capability.py`
5. `run_room_temp_co2_pressure_diagnostic.py`
6. 旧 V1 写入/acceptance 文件

完成标准：

- inventory 中没有 `classification_required`。
- V1.5 相关真实 COM、PACE、阀、SENCO 入口都明确风险。

### 第 2 步：给诊断入口加硬门禁

对 `diagnostic_only` 且有真实硬件风险的工具，加统一诊断解锁：

```text
--engineering-diagnostic
--not-real-acceptance
--operator-confirmation DIAGNOSTIC_ONLY
```

完成标准：

- 不带诊断解锁时，诊断工具直接拒绝运行。
- 报告和归档不会自动调用这些工具。

### 第 3 步：给旧 V1 写入/acceptance 工具加 V1.5 阻断说明

旧 V1 文件先不删，建议先加：

```text
legacy_v1_reference_only
not_v1_5_formal_entrypoint
do_not_use_for_v1_5
```

如果将来确认没有测试或算法引用，再迁移到 legacy 目录或删除。

### 第 4 步：增加防回归测试

建议新增或扩展测试，验证：

- `full_flow` 不调用 dynamic pressure diagnostic。
- `full_flow` 不调用 sealed pressure tune。
- `full_flow` 不调用旧 V1 online acceptance。
- 受控写入工具不能被 archive/report/review 自动触发。
- `validate_pressure_only.py` 是压力 no-write/controlled pressure 入口，不是 CO2/H2O 拟合入口。

### 第 5 步：最后再删文件

只有满足以下条件才建议删除：

1. 文件不在正式路径。
2. 不在诊断路径。
3. 不被测试引用。
4. 不含仍有价值的旧算法。
5. 已有迁移记录或替代入口。

否则更安全的处理是“隔离 + 硬门禁 + 明确标识”，不是马上删除。

## 当前风险判断

| 风险 | 等级 | 说明 |
|---|---|---|
| 误用旧 V1 写入/acceptance 工具 | P1 | 文件还在 tools 中，名字像入口，且含写入语义 |
| 误用诊断压力工具替代正式压力校准 | P1 | dynamic/sealed/ingress 等诊断工具仍可见 |
| 删除过早导致丢失旧算法参考 | P1 | V1/V2 旧算法仍可能用于核对拟合物理意义 |
| 直接清理破坏气路/水路 | P0 | 本报告没有执行删除或代码修改，因此未触发 |

## 建议结论

现在不建议大规模删除。正确顺序是：

```text
先补 inventory -> 再加硬门禁 -> 再阻断旧 V1 入口 -> 再跑测试 -> 最后才删除确认无用文件
```

这符合 V1.5 的物理和计量逻辑：正式流程依靠稳定的开放流通、独立压力/温度通道、可追溯证据和受控写入；诊断工具可以保留，但不能有机会伪装成正式 acceptance。
