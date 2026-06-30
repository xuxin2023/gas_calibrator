# V1.5 新旧算法气路/水路配置整理 - 2026-06-30

## 总原则

V1.5 的气路和水路物理流程不因为新算法重写。两种算法共用成熟 V1.5 路线：

- 气路 runner：`gas_calibrator.tools.run_v1_5_formal_co2_open_flow_queue`
- 水路 runner：`gas_calibrator.tools.run_v1_5_formal_h2o_open_flow_queue`
- 压力仍然先走 `SENCO9`
- 初始化阶段新旧算法都默认保持温度系数中性
- 分析仪运行模式保持 `MODE2`、`1 Hz` 主动上传、串口命令间隔不小于 `1 s`

两种算法真正的差别在拟合输入和锚点证据：

- 旧算法：直接使用比值 `R`
- 新算法：先计算 `A=-ln(R/R0(T))/(P_kPa/100)`，再沿用原浓度-温度拟合结构

## 旧算法配置：`legacy_ratio_production`

这是当前生产默认配置。

### 气路

使用 canonical CO2 队列，共 45 点：

| 温度 | CO2 点 |
| --- | --- |
| -20C | 0 / 400 / 1000 ppm |
| -10C | 0 / 400 / 1000 ppm |
| 0C | 0 / 400 / 1000 ppm |
| 10C | 0 / 100 / 200 / 300 / 400 / 500 / 600 / 700 / 800 / 900 / 1000 ppm |
| 20C | 0 / 100 / 200 / 300 / 400 / 500 / 600 / 700 / 800 / 900 / 1000 ppm |
| 30C | 0 / 100 / 200 / 300 / 400 / 500 / 600 / 700 / 800 / 900 / 1000 ppm |
| 40C | 0 / 400 / 1000 ppm |

拟合输入为 `R_CO2`，主要系数为 `SENCO1/SENCO3`，`SENCO5` 作为最终线性层单独评审。

### 水路

使用 canonical H2O 队列，共 13 个湿点：

| 温度 | 水点 |
| --- | --- |
| 0C | HGEN0C 50RH |
| 10C | HGEN10C 30RH / 50RH / 70RH |
| 20C | HGEN20C 30RH / 50RH / 70RH |
| 30C | HGEN20C 30RH / 50RH / 70RH / 90RH |
| 40C | HGEN30C 50RH / 70RH |

拟合输入为 `R_H2O`，主要系数为 `SENCO2/SENCO4`，`SENCO6` 作为最终线性层单独评审或保持中性。

旧算法不强制把气路低水点纳入正式水路拟合；低水点可作为低端 QC 或专项评审证据。

## 新算法配置：`absorption_ratio_shadow`

这是新算法生产候选配置，不是默认旧算法生产配置。它仍走成熟 V1.5 runner，但在点位和锚点证据上比旧算法多几项强制证据。

### 气路

气路仍以旧算法同一套 45 点 canonical 队列为主体。新算法不默认把低温段扩成 11 点。

新算法 CO2 的 `R0_CO2(T)` 只来自 CO2 零气点。当前 canonical 队列已经在 `-20/-10/0/10/20/30/40C` 都有 0 ppm 锚点，所以 R0 温度覆盖充足。

根据 SN01260607 的实测残差，新算法生产候选气路在 45 点基础上增加 2 个低温曲率约束气点：

- -20C 600ppm
- -10C 600ppm

所以新算法 CO2 生产候选总气点数为 `47`。这些点物理上仍然就是气点，走同一套气路动作、采样和存储。新增 `600ppm` 的目的不是替代已有 `400ppm/1000ppm`，而是约束低温吸收曲线的中间曲率。

### 水路

水路仍以旧算法同一套 13 个 H2O 湿点为主体，但新算法增加 1 个高温中水约束点：

- `40C / HGEN30C / 30RH`

所以新算法 H2O 生产候选湿点数为 `14`。这个点的作用是约束高温下 `12-15 mmol/mol` 左右的中水曲线形状，不是为了制造极低湿。

新算法额外要求 `R0_H2O(T)` 低端锚点。低端锚点来自 **气路 CO2 0 气点** 的水汽证据，而不是要求湿度发生器产生极低湿：

- 必须有 `R_H2O` 或等价 H2O 比值
- 必须有露点/压力反算的残余水汽目标
- 必须有采样压力
- 必须有分析仪腔体温度 `T1`
- 残余水汽不能强制写成 0
- CO2 0 气点不能直接等同 H2O=0，只能作为带残余水目标的低水锚点

低端锚点使用的物理方程是：

```text
ln(R_H2O)=ln(R0_H2O(T))-k(T)*H2O_residual*(P_kPa/100)
```

离线锚点提取工具为：

```text
gas_calibrator.tools.export_v1_5_h2o_low_anchor_from_co2_zero
```

现有历史低水锚点已覆盖 `-20/-10/0/10/20/30C`。新算法生产候选需要补齐或保留 `40C` 的 CO2 0 气低水锚点，用来关闭 `R0_H2O(T)` 高温外推风险。

### 新算法额外证据

每个温度组在所有 active 分析仪腔体温度判稳后，应读取并记录：

- `CHECK,YGAS,FFF` 原始响应
- 两路恒温芯片电压
- 欠温/恒温/过温状态

这部分是新算法硬件状态证据，不改变气路/水路 runner 的物理动作。

## SN01260607 实测拟合收敛

这台新算法设备的 SN 为 `01260607`，设备 ID 为 `001`。这轮实测说明，新算法未来点位不应简单扩成低温全量 11 点，而应先围绕实际冲突点做复核。

### CO2

最终写入使用 `old7_absorption_A_TK_zero1ppm_exclude4_physical_lowtemp_guard`。保留点最大非零相对误差约 `0.589%`，但把 4 个冲突点一起回放时最大非零相对误差约 `1.757%`。实际评审点数为 44 点，缺少 canonical 中的 `20C / 200ppm`，后续正式运行应把缺点登记清楚。

4 个冲突点是 SN01260607 / ID001 这台设备在本轮拟合里观察到的历史冲突点：

- `-20C / 400ppm`
- `-10C / 1000ppm`
- `20C / 100ppm`
- `0C / 400ppm`

因此，新算法 CO2 后续最稳策略是：保持 45 点 canonical 气路不变；新增 `-20C / 600ppm` 和 `-10C / 600ppm` 作为所有新算法设备的低温曲线形状约束，不默认扩成完整低温 11 点。上述 4 个已有气点只作为 SN01260607 的设备特异诊断复查点，不应被理解为所有新算法设备固定 release 前必查点。其它新算法设备应先跑完整候选点集，再根据本设备残差自动生成自己的诊断复查点。

CO2 写入合同需要和点位合同分开看：

- 旧算法：`old_ratio_temperature`，主链写入为 `SENCO1/SENCO3`
- 新算法：`old7_absorption_A_TK_zero1ppm`，仍使用旧七槽固件容量，但输入变量由 `R` 改为 `A=-ln(R/R0(T))/(P_kPa/100)`
- 主链受控写入工具：`gas_calibrator.tools.run_v1_5_co2_senco13_controlled_write`
- 写入前必须有 `candidate_senco_mapping_review.csv` 和公式/SENCO5 分层检查
- `SENCO5` 是 CO2 最终输出线性层，不能折进 `SENCO1/SENCO3`
- `SENCO5` 如需中性化必须走 `CLEARSENCO5,YGAS,FFF`，如需线性修正必须走独立 `SENCO5` 评审和三位小数十进制写入合同
- 新算法的 `R0_CO2(T)` 写入/读回依赖 `SENCOA/GETCOA` 合同；当前只列为离线 blocker，尚无受控 writer

### H2O

最终 H2O 使用 `old7_absorption_A_T_full13_relative_floor_5`，13 个湿点全部参与。最大相对误差约 `2.193%`，平均相对误差约 `0.672%`。

最高残差点是：

- `T30 / HG20C 50RH`，约 `2.193%`
- `T40 / HG30C 50RH`，约 `2.172%`

这说明当前 13 个湿点已经足够建立首版候选，但裕量刚好卡在 2% 附近。未来新算法水路不应优先新增湿度发生器极低湿点；低端锚点优先从 CO2 0 气点的 `R_H2O + 露点 + 压力 + T1` 计算获得。若要把 H2O 稳定压进 2%，应新增 `40C/HGEN30C/30RH`，同时保留一个 `40C` CO2 0 气低水锚点来减少高温 `R0_H2O(T)` 外推风险。`T30/HGEN20C/50RH` 和 `T40/HGEN30C/50RH` 是 SN01260607 的设备特异高残差诊断点，不是所有新算法设备固定复核点。

H2O 写入合同也需要和点位合同分开看：

- 旧算法：`old_ratio_temperature`，主链写入为 `SENCO2/SENCO4`
- `SENCO6` 是 H2O 最终输出线性层，不能折进 `SENCO2/SENCO4`
- `SENCO6` 如需中性化必须走 `CLEARSENCO6,YGAS,FFF`，如需线性修正必须走独立 `SENCO6` 评审和三位小数十进制写入合同
- 新算法 H2O 目前是 `old7_absorption_A_T_pending_firmware_scale`，状态为 `blocked_pending_firmware_input_scale_confirmation`
- `new_absorption_R0_A_k` 的 `SENCO2=lnR0(T1)`、`SENCO4=k(T1)` 合同只保留为诊断分支，不作为默认生产写入合同
- 新算法的 `R0_H2O(T)` 写入/读回依赖 `SENCOB/GETCOB` 合同；当前只列为离线 blocker，尚无受控 writer

## 配置文件

机器可读配置落在：

`configs/v1_5_algorithm_route_profiles.json`

当前只作为配置合同和后续自动化接入依据，不会自动改变生产入口。

## 新算法候选点位计划导出

新算法额外点、复核点、低水锚点现在由离线计划工具统一展开：

```text
gas_calibrator.tools.export_v1_5_new_algorithm_test_point_plan
```

这个工具只读取 `configs/v1_5_algorithm_route_profiles.json`，输出 no-write CSV/JSON 评审表，不打开 COM、不控制气路/水路、不写系数。导出的点位分三类：

- 新算法额外候选点：`-20C/600ppm`、`-10C/600ppm`、`40C/HGEN30C/30RH`
- 设备特异诊断复查点：SN01260607 的 CO2 四个冲突点、H2O 两个最高残差湿点
- H2O 低水锚点：来自 `40C CO2 0ppm` 的 `R_H2O + 露点 + 压力 + T1` 证据

这些点只属于新算法候选合同，不改变旧算法默认 `45` 个 CO2 气点和 `13` 个 H2O 湿点。其中 `-20C/600ppm`、`-10C/600ppm`、`40C/HGEN30C/30RH` 是所有新算法设备的候选补点；SN01260607 的冲突/高残差点只是历史诊断复查参考，不能外推成其它设备的固定流程。

## 新旧算法写入合同导出

新旧算法写入合同由离线工具导出：

```text
gas_calibrator.tools.export_v1_5_algorithm_write_contract_review
```

这个工具只读取 profile JSON，输出 no-write CSV/JSON，不打开 COM、不写系数。它的作用是把：

- CO2 `SENCO1/SENCO3` 主链和 `SENCO5` 最终线性层
- H2O `SENCO2/SENCO4` 主链和 `SENCO6` 最终线性层
- 新算法 `R -> A` 输入替换
- `SENCOA/SENCOB` 的 `R0(T)` 写入/读回 blocker
- 所需 review checks

明确成机器可读合同。当前新算法不能宣称完整生产闭环完成，因为 `SENCOA/SENCOB` 仍缺受控 writer/读回/回滚合同，H2O 新算法主链还需要固件输入变量和缩放确认。

## SENCOA/SENCOB 离线 writer 设计评审

`SENCOA/SENCOB` 的真实受控 writer 仍不存在；当前只新增离线设计评审工具：

```text
gas_calibrator.tools.export_v1_5_sencoa_sencob_writer_design_review
```

这个工具只读取 profile JSON 并导出 no-write 设计证据，不打开 COM、不写系数、不控制气路/水路。它固定约束：

- `SENCOA` 对应 `R0_CO2(T)`，未来必须通过 `GETCOA` 读回验证
- `SENCOB` 对应 `R0_H2O(T)`，未来必须通过 `GETCOB` 读回验证
- payload 为 4 个有限浮点系数
- 未来真实写入必须在 `MODE2` 下执行，串口命令间隔必须 `>=1.0s`
- 写入前必须有旧值快照，写入失败或 readback 不一致必须可按快照 rollback
- 当前状态仍为 `design_only_no_real_writer` / `blocked`，不能作为生产写入入口

## SENCOA/SENCOB 受控 writer no-write preflight

未来真实 writer 的解锁门禁由离线工具先行定义：

```text
gas_calibrator.tools.export_v1_5_sencoa_sencob_controlled_writer_preflight
```

这个工具只导出 preflight 证据和真实写入边界，不打开 COM、不导入 `GasAnalyzer`、不写系数。它可以检查候选 payload CSV 和旧 `GETCOA/GETCOB` 快照 JSON 的形状，但即使这些材料齐全，真实写入仍保持 `blocked_pending_real_writer_implementation`，直到单独实现并评审真实 controlled writer。

preflight 约束：

- payload 必须同时包含 `SENCOA` 与 `SENCOB`
- 每组 payload 必须是 4 个有限浮点系数
- 默认不接受 `FFF` 广播 target
- 旧值快照必须包含 `GETCOA_before` 与 `GETCOB_before`
- future command gap 必须 `>=1.0s`
- 真实写入后必须单独做 CO2/H2O no-write 复验，不能只凭 readback 宣称生产合格
