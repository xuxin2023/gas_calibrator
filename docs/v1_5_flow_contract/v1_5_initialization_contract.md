# V1.5 初始化合同

## 目标

V1.5 初始化不是普通的串口准备，而是正式校准证据链的起点。它必须先冻结每台气体分析仪的真实设备身份和旧系数状态，再清除或中性化会污染主拟合的辅助系数层，最后才允许进入压力、温度、气路和水路采样。

## 物理意义

气体分析仪输出的 CO2/H2O、压力和温度不是单纯原始信号，而是固件内部系数链计算后的结果。若初始化阶段没有处理旧系数，后续开放流通采样即使气体稳定，也可能采到被旧输出层、温度层或压力层污染过的显示值。

因此初始化必须区分两类数据：

- 原始/工厂模式比值和信号：用于主拟合和诊断。
- 固件显示输出：会受到已有 SENCO 系数影响，必须在证据中记录系数状态。

## 固定顺序

1. 加载校准计划、证书、运行配置和 run_id。
2. 读取每台分析仪 MODE2 身份，按设备自身 ID 绑定，不按 COM 口或 GA 别名作为身份。
3. 读取并备份 GETCO1 到 GETCO9，形成 `epoch0` 旧系数证据。
4. 在旧系数备份完成后，才允许受控处理中性化/清除辅助系数：
   - SENCO5：CO2 最终显示浓度线性修正，目标中性层。
   - SENCO6：H2O 最终显示水值线性修正，目标中性层。
   - SENCO7/SENCO8：腔体/壳体温度输入层，异常时必须中性化或形成温度校准证据。
   - SENCO9：压力输入层，压力快检/压力校准前必须清除、中性化或形成已校准证据。
5. 中性化/清除后必须读回，形成辅助系数新 epoch 证据。
6. 设置采样通信合同：MODE2、1 Hz 主动上传、AVERAGE1/2 滤波参数，且只允许 `SETCOMWAY`、`MODE`、`FTD`、`AVERAGE` 这类采样配置命令。
7. 禁止初始化阶段写入 ID、SENCO1/2/3/4、SENCO5/6/7/8/9 候选、SETPOW、SETILLUM、SETCO2 等高风险参数，除非进入对应受控写入工具并生成审批和读回证据。
8. 压力通道快检/校准、温度通道评审/校准通过后，才进入 CO2/H2O 正式开放流通采样。

## 工具归口

初始化相关工具不删除、不合并成一大段脚本，而是按职责归口：

| 工具 | 角色 | 允许用途 | 禁止事项 |
|---|---|---|---|
| `run_v1_5_formal_initialization_runner.py` | 统一初始化入口 | 生成初始化计划、命令包、证据清单、数据库 bundle 和 readiness 门禁 | 不直接打开 COM、不写 SENCO、不控制气路/水路/PACE |
| `probe_v1_5_getco_component_snapshot.py` | 身份和旧系数读取工具 | 在用户授权真实 V1.5 设备后，只读绑定设备 ID、读取 GETCO1-9、生成 epoch0 快照 | 不写设备 ID、不写 SENCO、不作为正式流程顶层入口 |
| `run_v1_5_*_controlled_write.py` | 受控写入工具 | 只在旧快照、候选评审、操作确认、读回计划齐全时执行对应 S5/S6/S7/S8/S9 或主系数写入 | 不被 report/readiness/archive 自动调用，不绕过身份绑定 |
| `export_v1_5_initialization_readiness.py` | 初始化审核工具 | 离线检查 run 目录是否满足初始化 ready 条件，指出缺失证据 | 不打开 COM、不补写系数、不替代真实读回 |
| 历史 logs/report/artifact | 溯源证据 | 保留用于复核旧运行和问题追踪 | 不作为默认入口，不删改以免破坏证据链 |

这样做的原因是：初始化阶段既有真实串口读取，也有高风险受控写入，还有离线审核和数据库索引。如果没有统一入口，后续很容易直接运行某个探针或历史脚本，导致跳过 GETCO1-9 旧系数冻结、S5/S6 输出层处理、S7/S8 温度输入处理或 S9 压力输入处理。

## 正式证据

正式初始化至少需要这些证据：

- `old_component_coefficients_snapshot.json`
- `runtime_identity_bound_config.json`
- `getco_component_snapshot_identity.csv`
- `senco5_neutral_write_events.csv`
- `senco6_neutral_write_events.csv`
- `senco78_neutral_write_events.csv`
- `senco9_clear_write_events.csv`
- 辅助系数读回快照或等效新 epoch 证据
- runtime config 中的禁写配置和 startup command allow/deny 列表

## 续跑例外

如果当前 run 是恢复/续跑，而不是从零开始的正式初始化，可以允许缺少部分初始化证据，但必须标记为 `continuation_requires_review`。这类数据不能静默进入正式候选系数评审，必须由工程师和审核员确认旧证据链是否仍然适用。

## 离线检查器

新增离线检查入口：

```text
python -m gas_calibrator.tools.export_v1_5_initialization_readiness ^
  --run-dir <run_dir> ^
  --config <runtime_config.json> ^
  --output-dir <output_dir>
```

该工具只读现有文件：

- 不打开 COM
- 不控制 PACE
- 不控制水路/气路
- 不写 SENCO
- 不写设备 ID

输出：

- `v1_5_initialization_readiness.json`
- `v1_5_initialization_readiness.md`

## 完成标准

初始化阶段只有在以下条件满足时才可判定为 `initialization_ready`：

1. 每台有效分析仪都有设备 ID 绑定证据。
2. 每台有效分析仪都有 GETCO1-9 epoch0 备份。
3. 每台有效分析仪都有 S5/S6/S7/S8/S9 中性化或清除读回证据。
4. runtime config 明确禁止初始化阶段写 SENCO、ID、SETPOW、SETILLUM、SETCO2。
5. 采样通信配置只包含 MODE2、1 Hz 主动上传、滤波/平均等低风险采样准备命令。
