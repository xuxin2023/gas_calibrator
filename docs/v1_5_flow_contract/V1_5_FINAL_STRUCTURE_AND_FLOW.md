# V1.5 最终结构与流程总说明

- 状态：V1.5 结构整理索引，文档层收口。
- 日期：2026-06-30。
- 适用区域：`D:\gas_calibrator\_worktrees\v1_5_fixed_wait_window_gate_1aee26d_clean`。
- 边界：本文档不打开 COM、不控制气路/水路/压力/温箱、不写 SN、不写 SENCO、不产生 real acceptance 结论。

## 1. 当前判断

V1.5 现在已经不是“找不到入口”的状态了。正式路径可以整理成一条主干：

1. 初始化和身份入库。
2. 压力通道。
3. 温度通道评审。
4. CO2 开流气路采样。
5. H2O 开流水路采样。
6. QC 和拟合输入评审。
7. 候选系数计算。
8. 写入前评审。
9. 受控写入。
10. 写后复验。
11. 归档、报告、数据库闭环。

后面不应再从 V1、V2、根目录草稿、单个诊断脚本或 `_handoff` 历史证据里临时挑入口。V1.5 正式流程应以本 worktree 中的正式入口、配置合同和受控写入工具为准。

## 2. 工作区边界

| 区域 | 用途 | 当前规则 |
|---|---|---|
| `D:\gas_calibrator\_worktrees\v1_5_fixed_wait_window_gate_1aee26d_clean` | V1.5 成熟路径和最终整理区域 | 当前 source of truth。正式整理、提交、评审都应发生在这里。 |
| `D:\gas_calibrator` 根目录 | 历史主目录和近期草稿区 | 当前仍有较大 dirty diff，不能作为正式 V1.5 流程来源。只允许从里面挑单个功能，经审查后迁入 clean worktree。 |
| `_handoff` | 运行证据、阶段报告、临时分析 | 只作为证据和追溯材料，不应整体 staged，不应当作源码主入口。 |
| V1 / V2 相关入口 | 历史 fallback 或未来架构参考 | 不作为当前生产 V1.5 主入口；V2 不替代 V1.5 成熟生产路径。 |

## 3. 正式层级

| 层级 | 责任 | 正式入口或核心工具 | 注意事项 |
|---|---|---|---|
| 全流程守门 | 规定顺序、生成离线计划、阻止越级 | `src/gas_calibrator/tools/run_v1_5_full_calibration_chain.py` | 这是 planner/gate，不是隐藏真机 runner。 |
| 初始化 | SN/device_code、设备 ID、MODE2、GETCO1-9、运行配置、数据库预检 | `src/gas_calibrator/tools/run_v1_5_formal_initialization_runner.py` | 单一正式初始化 owner。子工具只服务这一层。 |
| SN 身份 | 首次发现设备时分配/写入 8 位数字 SN | `src/gas_calibrator/tools/run_v1_5_sn_identity_initialization.py` | 只写 SN/device_code；不写 SENCO、不采样、不拟合。 |
| 运行配置 | MODE2、1 Hz 主动上传、滤波/启动设置、CHECK 记录 | `src/gas_calibrator/tools/run_v1_5_analyzer_runtime_setup.py` | 串口命令最小间隔必须 `>=1.0s`。 |
| 初始化数据库 | 把身份、run_device、GETCO 快照、runtime setup 入库 | `src/gas_calibrator/tools/run_v1_5_initialization_db_preflight.py` | 正式库目标为 PostgreSQL 18；支持 SN/device_code 和设备 ID 兼容查询。 |
| 压力通道 | 压力 P 独立评审和 SENCO9 | `src/gas_calibrator/tools/export_v1_5_pressure_channel_validation.py`、`src/gas_calibrator/tools/run_v1_5_pressure_senco9_controlled_write.py` | 压力必须先处理，不能让 CO2/H2O 主拟合吸收压力误差。 |
| 温度通道 | 温度证据评审和 S7/S8 策略 | `src/gas_calibrator/tools/export_v1_5_temperature_channel_review.py`、`src/gas_calibrator/tools/run_v1_5_temperature_senco78_neutral_controlled_write.py` | 当前新旧算法默认不做温度校准，S7/S8 保持中性。 |
| CO2 气路 | 成熟 V1.5 开流气路队列 | `src/gas_calibrator/tools/run_v1_5_formal_co2_open_flow_queue.py` | 真实运行需要操作授权；成熟物理动作和点序不能被新算法污染。 |
| CO2 采样 worker | 单点采样、判稳、质量等级、采样存储 | `src/gas_calibrator/tools/run_v1_5_formal_open_flow_sampling.py` | 由 CO2 queue 调用，不作为人工起点。 |
| H2O 水路 | 成熟 V1.5 开流水路队列 | `src/gas_calibrator/tools/run_v1_5_formal_h2o_open_flow_queue.py` | 真实运行需要操作授权；保持上一轮成熟水路时序。 |
| H2O 采样 worker | 水点采样、判稳、质量等级、采样存储 | `src/gas_calibrator/tools/run_v1_5_formal_h2o_open_flow_sampling.py` | 由 H2O queue 调用，不作为人工起点。 |
| 拟合输入 QC | 判定哪些点能进入主拟合 | `src/gas_calibrator/tools/export_v1_5_fit_input_quality.py`、`src/gas_calibrator/tools/export_v1_5_open_flow_canonical_points.py` | A/B/C 级和 reject 原因必须保留。 |
| 候选系数 | 只读计算 S1/S3、S2/S4、S5/S6、S9、R0(T) 候选 | `src/gas_calibrator/tools/export_v1_5_candidate_coefficients.py` 及专项 review exporter | 只产生 no-write 评审，不直接写设备。 |
| 写入评审 | 比较旧值、候选值、残差、风险和授权状态 | `src/gas_calibrator/tools/export_v1_5_candidate_write_review.py` | 评审通过不等于已经授权写入。 |
| 受控写入 | 明确授权、旧值快照、清除/写入、读回、回滚 | `run_v1_5_*_controlled_write.py` | 每类 SENCO 独立工具，不能被报告或队列隐式调用。 |
| 写后复验 | 不控温箱的成熟复验口径、独立样本验证 | `src/gas_calibrator/tools/export_v1_5_post_write_reverification.py` | 写入成功和复验合格要分开报告。 |
| 归档报告 | evidence sidecar、hash、中文报告、数据库索引 | `src/gas_calibrator/tools/run_v1_5_formal_archive_closure.py` | 归档不改变设备状态，不能隐藏失败点。 |

## 4. 初始化层的正式过程

初始化是进入气路/水路前的身份、状态和证据闭环，不是采样流程。

1. 读取当前配置和计划，限定本轮 active 设备数量，支持 1 至 6 台。
2. 打开每台 active 分析仪的串口，串行发送设备命令，命令间隔不低于 1 秒。
3. 读取 MODE2 设备身份，得到设备内部 3 位协议 ID。该 ID 仍然长期保留，用作兼容查询和指令识别，但不是未来唯一主键。
4. 读取或分配 8 位 SN/device_code。SN 只能是数字，当前策略为 `硬件版本2位 + YYMM 4位 + 序号2位`，例如硬件版本 `01`。
5. 对需要首次绑定的设备，先进入 MODE2，再按 `SN,YGAS,<target>,<8位SN>` 写入，并用 `SN,YGAS,FFF` 连续读回确认。
6. 记录 SN、device_code、协议 ID、slot、COM 端口、run_id。COM 端口只作为 transport，不作为设备身份。
7. 读取 GETCO1 到 GETCO9 epoch-0 快照，作为后续清除、写入、回滚和溯源基础。
8. 将 S5/S6/S7/S8/S9 等辅助层的状态纳入 readiness。当前新旧算法都默认温度系数 S7/S8 中性。
9. 设置 MODE2、1 Hz 主动上传、滤波和启动运行配置。
10. 新算法设备在腔体温度判稳后读取 `CHECK,YGAS,FFF`，记录两路电压和锁温监控状态；该动作只读。
11. 生成初始化 bundle、runtime setup result、readiness/evidence index。
12. 正式数据库写入前先做 preflight。生产目标为 PostgreSQL 18，`sn_code/device_code` 是主身份，协议 ID 是兼容 alias。
13. 初始化 ready 后才允许进入压力、温度、气路、水路。

初始化阶段禁止做这些事：

- 禁止写 CO2/H2O/压力/温度校准系数。
- 禁止跑气点、水点或拟合。
- 禁止把 COM 号当设备身份。
- 禁止把 3 位协议 ID 当唯一生产主键。
- 禁止未读回就认为 SN 写入成功。

## 5. 物理流程顺序

正式 V1.5 从物理意义上应按这个顺序执行：

1. `LOAD_PLAN`：冻结 run_id、配置、证书、点位计划和目标设备。
2. `PRECHECK`：绑定设备身份，完成 SN/设备 ID/端口映射，读取 GETCO 快照。
3. `PRESSURE`：用压力控制器/参考压力验证分析仪压力 P；必要时独立处理 SENCO9。
4. `TEMPERATURE_REVIEW`：评审腔体/机壳/环境温度证据；当前默认 S7/S8 中性，不用外部温度箱温度强行校准腔体。
5. `CO2_OPEN_FLOW`：连续开流通标准气，按成熟 V1.5 点序、判稳和采样窗口采集 CO2。
6. `H2O_OPEN_FLOW`：连续开流水汽路线，按成熟 V1.5 点序、判稳和采样窗口采集 H2O。
7. `QC`：保留 raw frames、reject frames、质量等级、采样间隔、判稳证据。
8. `CANDIDATE_REVIEW`：只用符合角色和质量要求的点生成候选系数。
9. `CONTROLLED_WRITE`：明确授权后，用专用工具清除/写入/读回/快照。
10. `POST_WRITE_REVERIFY`：按成熟 V1.5 复验方式验证输出误差。
11. `ARCHIVE_REPORT_DATABASE`：归档证据、报告、数据库索引和 release 状态。

## 6. 旧算法与新算法边界

| 项目 | 旧算法 / legacy ratio | 新算法 / absorption |
|---|---|---|
| 主拟合输入 | 比值 `R` | 吸收率 `A=-ln(R/R0(T))/(P_kPa/100)` |
| 压力顺序 | SENCO9 first | SENCO9 first，不能跳过 |
| 温度系数 | 默认中性，除非有腔体温度证据证明需要写 | 默认中性，不能用温箱外部温度替代腔体真值 |
| CO2/H2O 跑点 | 旧算法成熟 45/13 默认队列 | 不改旧默认队列；额外点由 algorithm profile 生成 |
| R0(T) | 不依赖 SENCOA/SENCOB | 依赖 R0_CO2(T)/R0_H2O(T)，对应 SENCOA/SENCOB 合同、预检、写入和读回 |
| 代码落点 | 成熟 queue/worker | profile、离线评审、写入合同、补点计划 |

关键原则：新算法差异应该放在拟合输入、R0(T) 合同、额外点 profile 和写入合同里，不应重写成熟 CO2/H2O runner 的物理动作。

## 7. 锚点政策

CO2 和 H2O 的低端锚点不能混成一个概念：

- CO2 zero gas 是低 CO2 标气点，用于 CO2 主拟合或低端检查。
- H2O dry-gas / low-water anchor 是低水汽锚点，必须由露点、压力和水汽比值证据证明水含量。
- 气路 0 气点里的 H2O ratio 可以成为 H2O 低水锚点候选，但前提是有露点/压力可追溯，且角色明确写成 H2O low-water anchor，不是简单把 CO2 zero gas 当成 H2O 0 点。

## 8. 写入边界

| 系数组 | 物理含义 | 工具边界 |
|---|---|---|
| S1/S3 | CO2 主链拟合 | CO2 controlled write，写前必须有候选评审、旧值快照、读回计划。 |
| S5 | CO2 最终线性显示修正 | 必须考虑设备当前已有 S5，先清再写目标十进制系数，不能叠加误写。 |
| S2/S4 | H2O 主链拟合 | H2O controlled write，不能和 S6 混用不配套模型。 |
| S6 | H2O 最终线性显示修正 | 只做输出层线性修正；主模型不合格时不能靠 S6 掩盖。 |
| S7/S8 | 温度输入/修正 | 当前新旧算法默认中性；candidate write 保持阻断，除非后续有正式腔体温度证据。 |
| S9 | 压力输入/修正 | 压力通道独立处理，必须在 CO2/H2O 主拟合前完成或明确保持 no-write。 |
| SENCOA/SENCOB | 新算法 R0(T) | 目前已有设计评审和 preflight 合同，真实 writer 必须另走受控写入、读回和 rollback。 |

## 9. 不应作为正式入口的内容

这些东西可以保留，但不能当正式流程起点：

- `src/gas_calibrator/tools/run_v1_corrected_autodelivery.py` 等 V1 历史入口。
- V2 runner、V2 storage 里的未评审流程入口。
- `probe_*`、`diagnostic_*`、`*_review.py` 这类诊断/评审脚本。
- 单点 sampling worker，除非由正式 CO2/H2O queue 调用。
- 所有 controlled write 工具，除非已经完成写入授权、旧值快照和 readback 计划。
- `_handoff` 里的历史脚本、报告、临时 CSV。
- 根目录 dirty diff 里的未迁入改动。

## 10. 现在还没等于“全项目清爽”

当前结论应该精确表述为：

- V1.5 的正式主干、入口分层、新旧算法边界已经清楚。
- 近期已经把 SN/数据库、CHECK/15 字段协议、串口 1 秒节拍、新旧算法 profile、R0(T) 设计评审等拆成独立小包。
- 成熟 CO2/H2O runner 没有被新算法 profile 包污染。
- 根目录仍是草稿/污染区，不应直接用于正式生产。
- `_handoff` 仍有大量历史证据，不应整体合入。
- 自动化仍需按正式 runner 和受控授权一步步执行；不能因为有 planner 就跳过真实设备 readiness、压力、温度、采样 QC、写入评审和复验。

## 11. 下一次正式运行前检查

1. 只在 clean V1.5 worktree 选择入口。
2. 确认 active 设备数量为 1 至 6 台，不默认必须 6 台。
3. 确认每台设备 SN/device_code、协议 ID、COM transport 映射正确。
4. 确认初始化 bundle 和 runtime setup result ready，并能进入 PostgreSQL 18 preflight。
5. 确认 GETCO1-9 epoch-0 快照完整。
6. 确认 S7/S8 中性，S5/S6/S9 状态已经快照或明确处理。
7. 确认压力通道先完成或明确 no-write 风险。
8. 确认新算法设备 CHECK 只读记录存在。
9. 气路和水路使用成熟 V1.5 queue，不临时复制/迁移 runner。
10. 拟合时按算法 profile 选择 legacy ratio 或 absorption 输入。
11. 写入前必须有 no-write 评审、旧值快照、授权、读回和 rollback 计划。
12. 写后必须复验，再归档、入库、出报告。
