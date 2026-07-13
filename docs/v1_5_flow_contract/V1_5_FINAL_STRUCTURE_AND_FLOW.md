# V1.5 最终结构与流程总说明

- 状态：V1.5 结构整理收尾验收说明。
- 日期：2026-07-01。
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

后面不应再从 V1、V2、根目录草稿、单个诊断脚本或 `_handoff` 历史证据里临时挑入口。V1.5 正式流程应以本 worktree 中的正式入口、配置合同和受控写入工具为准。最终状态判断统一看 `export_v1_5_formal_run_status.py` 生成的只读 `formal_run_status` rollup。

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
| 初始化 executor dry-run | 在真实执行前分类初始化 plan 的 offline / read-only COM / controlled-write 步骤 | `src/gas_calibrator/tools/export_v1_5_formal_initialization_executor_dry_run.py` | 只读 sidecar；不传 `--execute`，不打开 COM、不写 SN/设备 ID、不写 SENCO、不连接 PostgreSQL。 |
| 初始化 blocked executor | 运行未来初始化 executor 的受阻 stub，证明 live 初始化仍被锁住 | `src/gas_calibrator/tools/run_v1_5_formal_initialization_blocked_executor.py` | 只读 stub；不支持 `--execute`，拒绝 real-COM unlock / controlled-write unlock，不打开 COM、不写 SN/设备 ID、不写 SENCO、不连接 PostgreSQL、不控压力/气路/水路。 |
| 初始化 read-only COM preflight 设计 | 固化未来只读真机 COM 预检的端口、节拍、身份、GETCO、CHECK 和 hold 合同 | `src/gas_calibrator/tools/export_v1_5_formal_initialization_readonly_com_preflight_design.py` | 只读设计评审；不实现 `--execute-read-only-real-com`，不打开 COM、不写 SN/设备 ID、不写 SENCO、不连接 PostgreSQL、不控压力/气路/水路。 |
| 初始化 read-only COM preflight blocked executor | 运行未来只读真机 COM 预检的受阻 stub，证明 analyzer contact 仍被锁住 | `src/gas_calibrator/tools/run_v1_5_formal_initialization_readonly_com_preflight_blocked_executor.py` | 只读 stub；拒绝 `--execute-read-only-real-com`、`--allow-real-com`、授权字段和端口清单输入，不打开 COM、不写 SN/设备 ID、不写 SENCO、不连接 PostgreSQL、不控压力/气路/水路。 |
| 初始化 read-only COM preflight controlled executor 设计 | 固化未来受控只读真机 COM 预检 executor 的授权、端口、读序、证据和 hold 合同 | `src/gas_calibrator/tools/export_v1_5_formal_initialization_readonly_com_preflight_controlled_executor_design.py` | 只读设计评审；消费 blocked executor 证据，但仍不实现 `--execute-read-only-real-com`，不打开 COM、不写 SN/设备 ID、不写 SENCO、不连接 PostgreSQL、不控压力/气路/水路。 |
| Read-only COM execution packet validator | 离线校验未来只读 COM 执行授权包、端口清单、active analyzer list、1s 节拍和新旧算法 CHECK 规则 | `src/gas_calibrator/tools/export_v1_5_formal_readonly_com_execution_packet_validator.py` | 只读 packet validator；full-flow 默认不注入授权包，不打开 COM、不读分析仪、不写 SN/设备 ID、不写 SENCO、不连接 PostgreSQL、不控压力/气路/水路。 |
| SN 身份 | 首次发现设备时分配/写入 8 位数字 SN | `src/gas_calibrator/tools/run_v1_5_sn_identity_initialization.py` | 只写 SN/device_code；不写 SENCO、不采样、不拟合。 |
| 运行配置 | MODE2、1 Hz 主动上传、滤波/启动设置、CHECK 记录 | `src/gas_calibrator/tools/run_v1_5_analyzer_runtime_setup.py` | 串口命令最小间隔必须 `>=1.0s`。 |
| 初始化数据库 | 把身份、run_device、GETCO 快照、runtime setup 入库 | `src/gas_calibrator/tools/run_v1_5_initialization_db_preflight.py` | 正式库目标为 PostgreSQL 18；支持 SN/device_code 和设备 ID 兼容查询。 |
| 正式数据库 dry-run | 预览 PostgreSQL 18 schema、唯一键、SN/device_code 主身份、协议 ID alias、insert roles | `src/gas_calibrator/tools/export_v1_5_formal_database_dry_run.py` | 只读 contract，不连接 PostgreSQL、不入库、不授权 release。 |
| 正式数据库 import preflight | 检查 DSN env、迁移锁、dry-run 合同和入库边界 | `src/gas_calibrator/tools/export_v1_5_formal_database_import_preflight.py` | 不连接 PostgreSQL；只生成入库前置评审证据。 |
| 正式数据库 import 授权 | 检查 archive release、preflight、operator/reviewer/approver 授权记录 | `src/gas_calibrator/tools/export_v1_5_formal_database_import_authorization.py` | 不连接 PostgreSQL；授权记录本身仍不执行入库。 |
| 正式数据库 import 命令合同 | 固化未来真实 import 命令必须消费的 authorization/preflight/archive/evidence/DSN 输入 | `src/gas_calibrator/tools/export_v1_5_formal_database_import_command_contract.py` | 不连接 PostgreSQL；只定义命令输入合同。 |
| 正式数据库 blocked executor | 运行未来 import 命令的受阻 stub，证明当前仍拒绝连接、迁移和写库 | `src/gas_calibrator/tools/import_v1_5_evidence_package.py` | 默认无真实执行路径；legacy bundle 仅 dry-run。 |
| 正式数据库 controlled executor 设计 | 固化未来真实 import executor 的双重授权、事务、readback、rollback 和 post-commit hold 合同 | `src/gas_calibrator/tools/export_v1_5_formal_database_import_controlled_executor_design.py` | 只读设计评审；不连接 PostgreSQL、不写库、不启用真实执行。 |
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
| 正式状态汇总 | 汇总当前阶段、下一步、物理流程可继续性、正式放行和数据库导入状态 | `src/gas_calibrator/tools/export_v1_5_formal_run_status.py` | 只读 rollup，不打开 COM、不连接 PostgreSQL、不控制路由、不写系数。 |

### Initialization controlled executor design addendum

`src/gas_calibrator/tools/export_v1_5_formal_initialization_controlled_executor_design.py` is an offline design review for the future live initialization executor. It freezes the authorization, read-only real-COM, controlled-write, readback, CHECK, and hold/rollback contract while keeping live initialization locked. It does not implement `--execute-controlled-initialization`, open COM, write SN/device_code, write SENCO, connect PostgreSQL, or control pressure/routes.

### Initialization read-only COM preflight design addendum

`src/gas_calibrator/tools/export_v1_5_formal_initialization_readonly_com_preflight_design.py` is an offline design review for the future read-only real-COM initialization preflight. It freezes the reviewed active-port inventory, `>=1.0s` command/retry spacing, protocol ID plus 8-digit SN/device_code reads, GETCO1-9 epoch-0 snapshot, CHECK-capable/new-algorithm monitor reads after all active chambers are stable, old-algorithm CHECK skip behavior, and hold policy for serial, identity, GETCO, CHECK, or pacing failures. It does not implement `--execute-read-only-real-com`, open COM, write SN/device_code, write SENCO, connect PostgreSQL, or control pressure/routes.

### Initialization read-only COM preflight blocked executor addendum

`src/gas_calibrator/tools/run_v1_5_formal_initialization_readonly_com_preflight_blocked_executor.py` is the no-COM/no-write stub that must sit after the read-only COM preflight design and before any future analyzer contact. It consumes the reviewed design sidecar, writes an evidence artifact that live read-only COM preflight remains blocked, and rejects `--execute`, `--execute-read-only-real-com`, `--execute-controlled-writes`, `--allow-real-com`, authorization labels, approver/reviewer labels, and reviewed port inventory input. It does not open COM, write SN/device_code, write SENCO, connect PostgreSQL, control pressure, or control gas/water routes.

### Initialization read-only COM preflight controlled executor design addendum

`src/gas_calibrator/tools/export_v1_5_formal_initialization_readonly_com_preflight_controlled_executor_design.py` is the offline design review for a future controlled read-only COM preflight executor. It consumes the blocked preflight stub evidence and freezes the future `--execute-read-only-real-com` requirements: operator confirmation, distinct reviewer/approver, reviewed 1 to 6 active analyzers, reviewed COM/GA transport inventory, `>=1.0s` command/retry/cross-device spacing, protocol ID compatibility alias, 8-digit SN/device_code read, GETCO1-9 epoch-0 snapshot, runtime evidence, CHECK only for CHECK-capable/new-algorithm analyzers, old-algorithm CHECK skip behavior, and hold policy for serial, identity, pacing, GETCO, and CHECK failures. It still does not implement real COM execution, write SN/device_code, write SENCO, connect PostgreSQL, control pressure, or control gas/water routes.

### Initialization read-only COM preflight controlled blocked executor addendum

`src/gas_calibrator/tools/run_v1_5_formal_initialization_readonly_com_preflight_controlled_blocked_executor.py` is the no-COM/no-write stub that must sit after the controlled read-only COM preflight executor design and before any future controlled analyzer contact. It consumes the reviewed controlled design sidecar, writes an evidence artifact that the controlled `--execute-read-only-real-com` path remains blocked, and rejects execute flags, real-COM flags, controlled-write flags, operator confirmation, reviewer/approver labels, authorization id, reviewed port inventory, and active-analyzer list inputs. It does not open COM, write SN/device_code, write SENCO, connect PostgreSQL, control pressure, or control gas/water routes.

### Read-only COM execution packet contract addendum

`src/gas_calibrator/tools/export_v1_5_formal_readonly_com_execution_contract.py` is the offline contract layer after the controlled blocked executor. It defines the future real read-only COM execution packet fields: explicit `--execute-read-only-real-com`, authorization id, operator confirmation, reviewer, approver, reviewed port inventory, active analyzer list, 1s serial pacing, read order, old-algorithm CHECK skip behavior, and denied write/database/route actions. It does not accept those future authorization fields as unlocks, does not open COM, and does not produce real acceptance evidence.

### Read-only COM execution packet validator addendum

`src/gas_calibrator/tools/export_v1_5_formal_readonly_com_execution_packet_validator.py` is the offline validator between the blocked read-only COM executor and initialization readiness. It accepts only JSON packet inputs when run manually for review, validates operator authorization through the structured `v1_5_readonly_com_no_write_reviewed_ports_v1` confirmation template or a legacy English-token fallback, reviewed COM/GA inventory, 1 to 6 active analyzers, 8-digit SN/device_code, unique protocol/transport mapping, `>=1.0s` command and retry pacing, new-algorithm CHECK-capable requirements, and old-algorithm CHECK skip behavior. The full-flow planner deliberately calls it without authorization packet inputs, so it proves the validator is wired while keeping real COM execution blocked. It does not implement `--execute-read-only-real-com`, open COM, read analyzers, write SN/device_code, write SENCO, connect PostgreSQL, control pressure, or control gas/water routes.

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
12. 正式数据库写入前必须依次经过 dry-run、preflight、manual authorization、command contract、blocked executor stub、controlled executor design review、deterministic transaction plan、transaction blocked executor。生产目标为 PostgreSQL 18，`sn_code/device_code` 是主身份，协议 ID 是兼容 alias；当前仍不允许真实 import。
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
| CO2/H2O 跑点 | 旧算法成熟 45/13 默认队列，保持 0620 成熟修复效果 | 不改旧默认队列；额外点由 algorithm profile 生成 |
| R0(T) | 不依赖 SENCOA/SENCOB | 依赖 R0_CO2(T)/R0_H2O(T)，对应 SENCOA/SENCOB 合同、预检、写入和读回 |
| 代码落点 | 成熟 queue/worker | profile、离线评审、写入合同、补点计划 |

关键原则：新算法差异应该放在拟合输入、R0(T) 合同、额外点 profile 和写入合同里，不应重写成熟 CO2/H2O runner 的物理动作。`run_v1_5_formal_co2_open_flow_queue.py`、`run_v1_5_formal_h2o_open_flow_queue.py`、`run_v1_5_formal_open_flow_sampling.py`、`src/gas_calibrator/workflow/runner.py`、`src/gas_calibrator/devices/gas_analyzer.py`、`configs/default_config.json` 是最终边界核查重点。

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
- 0620 成熟气路/水路的点序、物理动作、判稳/QC 口径只能由正式 runner 小包单独评审，不应被 profile、报告或归档包顺手改动。
- `export_v1_5_mature_route_contract.py` 是当前成熟路径合同 guard；它把 legacy CO2 45 点、legacy H2O 13 湿点、0620 route_behavior、新旧算法 runner 共用、补点只在 profile 层、SENCOA/SENCOB blocker、worker 不能顶层启动这些规则变成可测试证据。
- `export_v1_5_algorithm_formal_point_plan_guard.py` 是新旧算法正式点序 guard；它从 `configs/v1_5_algorithm_route_profiles.json` 生成只读预览，锁定 legacy 仍为 CO2 45 点 / H2O 13 湿点，新算法正式候选为 CO2 47 点 / H2O 14 湿点，并确认 `-20C 600ppm`、`-10C 600ppm`、`40C HGEN30C 30RH` 是随对应温度段一起跑的正式必跑点，不是 historical missing-point audit 的补跑标签。
- `export_v1_5_algorithm_formal_runlist_preview.py` 是新算法正式 runlist 预览生成器；它把点序 guard 转成 mature queue-compatible 的 CO2/H2O CSV，输出新算法 CO2 47 点和 H2O 14 湿点的正式 runlist preview，但仍标记为 `preview_only_not_runner_wired`，不修改正式 runner、不打开 COM、不控制气路水路。
- `export_v1_5_algorithm_runlist_readiness.py` 是新算法 runlist readiness gate；它只读 runlist preview manifest/CSV，缺少 `-20C 600ppm`、`-10C 600ppm` 或 `40C HGEN30C 30RH` 会直接 blocked，完整 47/14 才能进入后续 runner integration review。这个 gate 仍不授权真机、气路水路、写系数、归档 release 或数据库入库。
- `export_v1_5_algorithm_runner_integration_dry_run.py` 是新算法 runner integration dry-run 计划器；它只读 runlist readiness 和 runlist preview，输出未来应如何把 CO2 47 / H2O 14 CSV 交给成熟 CO2/H2O queue 的 `--dry-run --no-prompt` 命令预览，不执行命令、不接 COM、不控气水路、不修改正式 runner。
- `export_v1_5_algorithm_profile_runner_dry_run.py` 是新算法 profile runner dry-run 组合入口；它从 `configs/v1_5_algorithm_route_profiles.json` 一次性生成 runlist preview、runlist readiness 和 runner integration dry-run 证据包，仍然不执行正式队列、不接 COM、不控气水路、不修改成熟 runner。
- `export_v1_5_algorithm_queue_handoff_preflight.py` 是新算法 queue handoff preflight；它只读 profile runner dry-run 证据，确认 CO2/H2O 成熟 queue 只允许进入 `--dry-run --no-prompt` 评审，明确 `live_queue_execution_allowed=false`，不授权真机、气路水路、写系数、归档 release 或数据库入库。
- `export_v1_5_formal_database_dry_run.py` 是正式数据库 dry-run contract；它只读代码中的 storage model 和 evidence registry 合同，输出 PostgreSQL 18 schema、identity 唯一键、insert preview、planned-device 唯一性检查和 release/import 边界，不连接 PostgreSQL，也不导入生产数据。
- `import_v1_5_evidence_package.py` 当前是 PostgreSQL 18 blocked executor stub 和 legacy bundle dry-run 入口；它不再执行真实 import、不应用 migration、不写生产数据库。未来如果要真实入库，必须另做受控 executor 设计并追加双重授权、readback/import 证据。
- `export_v1_5_formal_database_import_controlled_executor_design.py` 是未来真实 PostgreSQL 18 import executor 的离线设计评审；它定义 `--execute-controlled-import`、operator/reviewer/approver 授权、DSN secret、事务、pre-commit readback、rollback 和 post-commit hold 合同，但当前不连接 PostgreSQL、不写库。
- `export_v1_5_formal_database_import_transaction_plan.py` 把 schema dry-run 和 controlled executor design 收敛为确定性的事务阶段、目标表、自然键、1-6 台 SN/device_code 身份、pre-commit readback 与 rollback 计划；所有 operation 固定为 `would_execute=false`，不生成可执行 SQL、不读取 DSN 值。
- `run_v1_5_formal_database_import_transaction_blocked_executor.py` 是事务形状的默认拒绝执行层；它拒绝 execute、DSN、authorization、migration、archive/evidence 等真实解锁参数，只证明 PostgreSQL 18 仍未连接和未写入。
- `export_v1_5_historical_replay_contract.py` 是历史数据 replay 合同 guard；它只做离线程序级回放解释检查，确认 0620/后续 legacy 数据仍按 R、45/13、QC/拟合/复验/归档角色解释，新算法数据只按 `A=-ln(R/R0(T))/(P_kPa/100)` 和 R0 证据做 shadow 评审，且 replay 通过不能释放归档或 PostgreSQL 18 入库。
- `export_v1_5_historical_replay_evidence.py` 是历史证据读取/回放绑定器；它只读历史 CSV/JSON 点级证据，识别 CO2/H2O 点序、QC 等级、fit eligibility、reject reason、fit input profile 和 replay 状态。旧算法仍按成熟 45/13 点检查；新算法候选必须按 profile 中的 47 CO2 点 / 14 H2O 点检查，缺 `-20C 600ppm`、`-10C 600ppm` 或 `40C HGEN30C 30RH` 时只能进入 review_required，不修改成熟 runner，也不能授权 release 或入库。
- `export_v1_5_historical_replay_missing_point_audit.py` 是历史 replay 缺点审计器；它只读 replay evidence 和历史分段/补跑目录，区分可审查绑定的 segmented/retry 质量候选、raw-only 候选、新算法 supplemental 未跑点和需要定点补跑的物理点，不会把缺失物理点提升为拟合合格点。
- `export_v1_5_historical_replay_qc_gap_audit.py` 是历史 replay 缺 QC 点审计器；它只读 replay evidence、同轮 queue manifest、raw IO 和跨轮参考质量文件，区分同轮 `C_reject` 可补追溯证据、跨轮质量只能参考、raw-only 仍需 QC 派生或定点补跑，不会把缺 QC 点提升为拟合合格点。
- `export_v1_5_legacy_historical_evidence_catalog.py` 是旧格式历史点证据目录；它对 segmented/retry/direct-recovery、accepted composite manifest、sidecar、samples 和 QC 做只读哈希与来源分类，但固定禁止把这些零散点提升为连续 0613/0620/0621 route attestation、正式拟合、release 或数据库入库证据。CO2 zero gas 与 H2O dry-gas anchor 继续保持物理口径分离。
- `export_v1_5_legacy_evidence_gap_task_plan.py` 是旧格式证据缺口任务规划器；它重新核验 catalog 中每个文件的大小和 SHA-256，把核心文件缺失、组件 QC 缺失、accepted warning、已被同物理点替代的旧尝试和 0624 禁区拆成离线人工任务，但不修改源文件、不自动生成 QC、不跨轮直接绑定质量，也不授权拟合或连续 route promotion。
- `export_v1_5_p1_evidence_lineage_audit.py` 是 P1 核心证据同轮审计器；它只检查 point 所属 lineage root 下的直属 run/point/manifest，区分真实 retry 与 dry-run reference，允许把同轮 retry 标成后续 P2 QC 候选，但不复制文件、不借跨轮样本、不自动生成 QC，也不改变原失败点状态。
- `export_v1_5_p2_qc_derivation_design.py` 是 P2 组件 QC 派生设计分类器；它只读校验同点 samples、frame QC、runtime 和 route-specific 证据是否具备未来派生输入，并记录 alignment、purge 和 accepted-warning 人工门禁。它不会生成组件 QC，也不会采用 0624/migration QC 阈值；在 0613/0620/0621 权威 writer 合同完成前，拟合、release 和入库继续阻塞。
- `export_v1_5_component_qc_authority_audit.py` 是组件 QC 权威来源审计器；它明确分离成熟采样前 ratio 判稳门禁与采样后 per-analyzer QC，证明当前 git 历史没有 0613/0620/0621 组件 QC writer，现有 43 个组件 QC 文件全部属于禁止的 0624/migration CO2 证据，根目录未跟踪 writer 只能作为 schema/诊断参考。因此 QC 生成、历史回填和拟合继续阻塞。
- `export_v1_5_new_algorithm_mature_queue_live_handoff.py` 固化新算法 47/14 点位到 0620/0621 成熟 CO2/H2O runner 的不可变离线交接合同：旧算法继续保持默认 45/13，新增 `-20C/600ppm`、`-10C/600ppm`、`40C/HGEN30C/30RH` 只进入新算法 profile；拟合输入为 `A=-ln(R/R0(T))/(P_kPa/100)`，S9 先行、S7/S8 中性，CO2 zero gas 与 H2O dry/low-water anchor 保持分离。配套 blocked executor 拒绝所有 live、COM、设备清单、授权、route、write 和数据库参数；当前只完成程序合同，真实 live handoff 仍需独立授权、现场 readiness 和 SENCOA/SENCOB 生产 writer。
- `export_v1_5_component_qc_generator_contract.py` 是 design-only 组件 QC 合同评审器；它固定 per-analyzer 独立分级、CO2 `0.0005/0.001`、H2O A=`0.001`、公共物理失败才整点拒绝、raw usable ratio 不得被 summary outlier filter 掩盖，以及 A/B/C 的 fit/diagnostic 语义。当前只允许人工合同评审，不代表 writer 已实现，也不允许回填 125 个历史点。
- `export_v1_5_component_qc_reference_evaluator.py` 是 synthetic-only 组件 QC 参考评估器；它只接受 `evidence_source=simulated` 的内存/fixture 数据，用于验证上述 per-analyzer 分级合同。它不会读取或写回历史点目录，不会把点级最差设备扩散成其它设备的拟合拒绝，也不授权历史 QC 生成、拟合、release、入库、COM 或任何设备动作。
- `export_v1_5_historical_component_qc_generator_preflight.py` 是历史组件 QC 生成前的 no-write 输入门禁；它只按 P2 已登记清单重验点目录内源工件角色、大小、SHA256、P2 inventory 路径绑定和目标覆盖风险。当前 125 个候选的 697 个源工件均通过完整性检查，但 125 点仍全部保留人工 gate，真正 QC 生成、历史写回、拟合、release、入库和设备动作继续锁定。
- `export_v1_5_historical_component_qc_blocked_generator_plan.py` 是历史组件 QC 的 blocked plan / would-write preview；它消费并重验上述 preflight，逐点固定目标路径和源包聚合哈希，但 `would_evaluate=false`、`would_write=false`、`overwrite_allowed=false`，不计算 A/B/C、不创建正式 QC 文件。任何上游/源文件漂移或目标已存在都会阻断整份 operation plan，未来 writer 仍需独立授权、原子 create-only、读回和回滚合同。
- `export_v1_5_historical_component_qc_controlled_writer_design.py` 是历史组件 QC 的受控 writer 设计评审；它重新计算 blocked plan，并固定未来一次性授权、全量 staging、OS 级 exclusive-create、逐文件 SHA/schema 读回和仅限本事务新建文件的补偿回滚合同。当前 evaluator、authorization validator、writer、readback 和 rollback executor 均不存在，生成、写回、拟合、release、入库及设备动作继续锁定。
- 根目录仍是草稿/污染区，不应直接用于正式生产。
- `_handoff` 仍有大量历史证据，不应整体合入。
- 自动化仍需按正式 runner 和受控授权一步步执行；不能因为有 planner 就跳过真实设备 readiness、压力、温度、采样 QC、写入评审和复验。

## 11. 收尾验收包要求

V1.5 结构整理基本完成前，必须保留一个只读收尾验收包：

1. 最终结构说明：本文件作为人工导航入口，必须明确正式入口、禁止入口、新旧算法边界、0620 成熟路径保护和污染区策略。
2. mature route contract：生成 `docs/v1_5_flow_contract/mature_route_contract/`，确保 0620 成熟 CO2/H2O 路径 `pass` 且 `blocker_count=0`。
3. algorithm formal point-plan guard：生成 `docs/v1_5_flow_contract/algorithm_formal_point_plan_guard/`，确保 legacy 仍为 45/13，新算法正式候选为 47/14，且新算法额外点被标记为正式必跑点而不是历史补跑点。
4. algorithm formal runlist preview：生成 `docs/v1_5_flow_contract/algorithm_formal_runlist_preview/`，输出 queue-compatible 的新算法 CO2 47 / H2O 14 CSV，并明确它仍是离线预览，不是正式 runner 自动调度。
5. algorithm runlist readiness：生成 `docs/v1_5_flow_contract/algorithm_runlist_readiness/`，只读检查 runlist preview 47/14、关键补点、字段和 preview-only 边界，缺点时必须 blocked。
6. algorithm runner integration dry-run：生成 `docs/v1_5_flow_contract/algorithm_runner_integration_dry_run/`，只读输出成熟 CO2/H2O queue 的 dry-run/no-prompt 调用计划，确认仍不执行命令、不接 COM、不修改 runner。
7. algorithm profile runner dry-run：生成 `docs/v1_5_flow_contract/algorithm_profile_runner_dry_run/`，从 profile 一次性产出 runlist preview、runlist readiness 和 runner dry-run 证据包，确认它仍是离线组合器而不是正式 runner。
8. algorithm queue handoff preflight：生成 `docs/v1_5_flow_contract/algorithm_queue_handoff_preflight/`，只读检查 profile-generated CO2/H2O runlist handoff 是否仍停留在 `--dry-run --no-prompt`，并明确 live queue execution 仍不允许。
9. formal database dry-run：生成 `docs/v1_5_flow_contract/formal_database_dry_run/`，只读检查 PostgreSQL 18 schema、SN/device_code 唯一主身份、protocol ID alias、COM/GA transport、insert preview 和 import/release 边界。
10. formal initialization read-only COM preflight controlled executor design：生成 `docs/v1_5_flow_contract/formal_initialization_readonly_com_preflight_controlled_executor_design/`，只读确认未来 read-only COM executor 的授权、端口、读序、证据和 hold 合同，仍不打开 COM、不写 SN/设备 ID、不写 SENCO、不连接 PostgreSQL、不控压力/气路/水路。
11. formal database import preflight / authorization / command contract / blocked executor / controlled executor design：生成 `docs/v1_5_flow_contract/formal_database_import_*` 和 `docs/v1_5_flow_contract/formal_database_import_controlled_executor_design/`，只读确认 DSN env、archive release、授权记录、命令输入、受阻执行器、未来事务/readback/rollback 合同，仍不连接 PostgreSQL、不写库。
12. historical replay contract：生成 `docs/v1_5_flow_contract/historical_replay_contract/`，确保历史 replay 只作为程序级 regression evidence，不改变成熟点序、不洗掉 QC reject、不授权归档/入库。
13. historical replay evidence：生成 `docs/v1_5_flow_contract/historical_replay_evidence/`，只读绑定 0620/后续历史 CSV/JSON，识别点序、QC、fit eligibility、reject reason 和 replay 状态。
14. historical replay missing point audit：生成 `docs/v1_5_flow_contract/historical_replay_missing_point_audit/`，只读审计缺点是否存在分段/补跑证据，并明确新算法 supplemental 缺点不能被成熟 45/13 replay 掩盖。
15. historical replay QC gap audit：生成 `docs/v1_5_flow_contract/historical_replay_qc_gap_audit/`，只读审计缺 QC 点是否存在同轮 reject-only 质量证据、retry/同点证据、跨轮参考或 raw-only 缺口。
16. focused pytest stdout：至少覆盖 canonical entrypoint、mature route contract、algorithm formal point-plan/runlist/readiness/dry-run/handoff guard、formal database dry-run、historical replay contract/evidence/missing-point audit/QC gap audit、initialization readiness、dirty zone audit、formal run status、archive/report/console。
17. 成熟路径边界核查：确认本次收尾包不改 `run_v1_5_formal_co2_open_flow_queue.py`、`run_v1_5_formal_h2o_open_flow_queue.py`、`run_v1_5_formal_open_flow_sampling.py`、`src/gas_calibrator/workflow/runner.py`、`src/gas_calibrator/devices/gas_analyzer.py`、`configs/default_config.json`。
18. 污染区策略：`_handoff` 是证据和草稿区，不进入正式小包；根目录 `D:\gas_calibrator` 冻结为污染区，正式 V1.5 只认 clean worktree。
19. 只读 full-flow status rollup：生成 `docs/v1_5_flow_contract/final_acceptance_status/`，用现有 JSON/CSV 证据判断能否继续物理流程、能否归档、能否入库、还缺什么证据。

这个验收包仍然不是 real acceptance：它不开 COM、不控气路/水路、不连 PostgreSQL、不写 SN/SENCO。

## 12. 下一次正式运行前检查

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

## 13. PostgreSQL 18 staging-to-production promotion addendum (2026-07-13)

- `run_v1_5_formal_database_import_staging_executor.py` is the only current database writer. It is limited to a dedicated staging/test database and `v1_5_core_staging*` / `v1_5_evidence_staging*` schemas.
- `export_v1_5_formal_database_import_production_promotion_preflight.py` revalidates the exact staging transaction, transaction plan, evidence bundle, production authorization, archive closure, command contract, executor design, identities, and table counts before a future production executor may be reviewed.
- A passing promotion preflight does not connect to the production database and does not authorize import. `production_import_execution_allowed`, `database_import_allowed`, and `formal_release_allowed` remain false.
- Production database execution must remain a separate, explicitly authorized implementation with transaction readback, rollback, conflict hold, and commit-uncertain handling.

## 14. PostgreSQL 18 production controlled executor addendum (2026-07-13)

- `run_v1_5_formal_database_import_production_controlled_executor.py` is the separate manual production executor. Default invocation is a no-DSN/no-connect preview; real execution requires `--execute-production-import` and a fresh three-party authorization packet bound to the exact promotion preflight, transaction plan, and evidence bundle hashes.
- The target is not configurable: PostgreSQL 18 database `gas_calibrator`, core schema `public`, evidence schema `v1_5_evidence`, and DSN environment `V1_5_POSTGRES_DSN`.
- The executor never creates schemas or applies migrations. Migration `002_v1_5_production_import_ledger` must already exist, otherwise the transaction rolls back and holds.
- A production transaction writes identity aliases and the evidence bundle atomically, performs precommit identity/table-count readback, records four immutable input hashes, supports exact idempotent replay, and holds conflicts or uncertain commits.
- Database import does not open COM, write SN/SENCO, control pressure/routes, modify 0613/0620/0621 mature calibration paths, or grant formal calibration release.

## 15. PostgreSQL 18 DBA migration readiness addendum (2026-07-14)

- `export_v1_5_formal_database_migration_dba_readiness.py` creates the no-connect DBA handoff for migrations `001` and `002`.
- The packet fixes PostgreSQL 18 database `gas_calibrator`, schema `v1_5_evidence`, source migration order, SHA256 checksums, read-only precheck/postcheck SQL, transactional migration 002 SQL, and rollback/hold boundaries.
- The three SQL artifacts have their own SHA256 bindings, and a template-only execution record reserves operator/reviewer/approver plus precheck/apply/postcheck output hashes; the blank template is not execution evidence.
- The exporter rejects DSN, connection, execution, apply-migration, and production-import arguments. It does not read `V1_5_POSTGRES_DSN`, connect PostgreSQL, apply a migration, write a row, or grant import/release authority.
- A DBA must separately review the packet, execute with `ON_ERROR_STOP`, retain the pre/post-check output, and record operator/reviewer/approver approval. The production importer still refuses to create schemas or apply migrations itself.

## 16. PostgreSQL 18 migration 002 controlled executor addendum (2026-07-14)

- `run_v1_5_formal_database_migration_production_controlled_executor.py` is the only code path allowed to execute migration `002_v1_5_production_import_ledger`; it is not a generic migration runner.
- Default invocation is a no-DSN/no-connect preview. Real execution requires `--execute-postgresql18-migration`, a fresh three-party authorization, and exact SHA256 bindings for the DBA readiness JSON plus precheck/apply/postcheck SQL.
- The target is fixed to PostgreSQL 18 database `gas_calibrator`, schema `v1_5_evidence`, and DSN environment `V1_5_POSTGRES_DSN`; target, schema, migration-version, import, and release overrides are rejected.
- Immediately before execution, the executor re-reads and re-hashes every bound artifact, rebuilds the readiness packet from repository migrations, then checks database/version/migration-001/migration-002/table state before starting the transaction.
- A successful apply must also pass postcheck readback for the exact ledger columns, primary key, unique constraints, foreign key, and index. SQL failure rolls back when possible; connection-loss ambiguity is held as `commit_uncertain` and is never represented as confirmed no-write.
- This executor never imports calibration evidence, opens COM, writes SN/device identity/SENCO coefficients, controls pressure/gas/water routes, modifies the 0613/0620/0621 mature calibration paths, grants database import, or grants formal release.

## 17. Production import migration-evidence gate addendum (2026-07-14)

- The production import preview now requires a confirmed migration-002 controlled-executor artifact before the import package can become ready.
- The migration artifact must prove the fixed PostgreSQL 18 target, repository migration 001/002 checksums, exact production-import ledger columns/constraints/indexes, committed or exact idempotent state, and a distinct three-party migration authorization record.
- The production-import authorization packet must bind the exact migration artifact path and SHA256 together with the promotion preflight, transaction plan, and evidence bundle. Replacing or changing any one of these four inputs holds before the CLI reads `V1_5_POSTGRES_DSN`.
- The importer still never applies migrations. This gate only allows a separately confirmed migration to become a prerequisite for a later separately authorized evidence import; it does not connect PostgreSQL, import evidence, or grant formal release by itself.
