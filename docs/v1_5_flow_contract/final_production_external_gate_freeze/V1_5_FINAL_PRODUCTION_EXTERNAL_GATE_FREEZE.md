# V1.5 最终生产外部门禁冻结清单

- source origin/main: `dbbed56689f2d48bd79339fa9af8bea58775fed4`
- overall_status: `program_automation_complete_real_production_evidence_pending`
- program capability count: `7`
- remaining external gate count: `6`
- recommended next gate: `production_postgresql18_migration_002_authorization_and_execution`
- mature fitting baseline: `0613 V1.5 fitting path`
- mature route baseline: `0620/0621 clean-worktree mature physical route path`

本清单取代 2026-07-13 的七项程序缺口清单。程序结构与离线自动化已经闭合；以下未完成项都需要真实硬件、当前批次证据或独立三方授权。

## 已完成的程序能力

| capability_id | 状态 | 能力 | 生产含义 |
|---|---|---|---|
| `mature_route_and_entrypoint_protection` | `implemented_and_tested` | 0613 拟合与 0620/0621 成熟物理路径保护 | 正式流程只认成熟 V1.5 路径，迁移版、0624、diagnostic、worker、V1/V2 均不得升格。 |
| `legacy_full_flow_orchestrator_offline_replay` | `implemented_and_tested_real_evidence_held` | 旧算法 45/13 全流程 orchestrator 离线 replay | 程序可解释初始化到归档的完整顺序，但不会把离线 replay 冒充真机 acceptance。 |
| `production_component_qc_and_0613_fit_matrix` | `implemented_and_tested_current_batch_continuity_required` | 生产 component-QC 与 0613 多策略拟合矩阵 | 程序已具备逐设备、逐组件 QC 和 no-write 策略评审；真实拟合仍需连续批次证据。 |
| `unified_controlled_write_readback_reverify` | `implemented_contract_ready_candidate_required` | 统一系数写入、GETCO 读回、回滚和短复验状态机 | 写入成功与复验成功已分离建模；没有获批候选时绝不允许写设备。 |
| `new_algorithm_47_14_mature_queue_handoff` | `implemented_contract_ready_live_locked` | 新算法 47/14 成熟队列 handoff 合同 | 新旧算法共用 0620/0621 点内物理路径；新算法 live 执行仍需独立授权与真机 smoke。 |
| `postgresql18_staging_migration_import_chain` | `implemented_and_real_staging_verified_production_locked` | PostgreSQL 18 staging、migration 002 与 production import 链 | 真实隔离 staging 已验证原子性、幂等、查询和回滚；生产迁移与入库仍分别授权。 |
| `final_offline_acceptance_suite` | `passed_real_acceptance_locked` | 最终 simulation/replay/parity/resilience 离线验收 | 程序级验收已通过，但所有离线证据继续标记为非 real acceptance。 |

## 剩余真实生产外部门禁

| 优先级 | gate_id | 状态 | 工作 | 完成标准 |
|---:|---|---|---|---|
| 1 | `production_postgresql18_migration_002_authorization_and_execution` | `external_authorization_required` | 生产 PostgreSQL 18 migration 002 三方授权与受控执行 | 受控 executor 在固定 gas_calibrator/v1_5_evidence 目标执行 migration 002，保留 pre/apply/post、cluster system_identifier 和确认 artifact。 |
| 2 | `current_batch_continuous_mature_route_evidence` | `hardware_required` | 当前 1-6 台真实批次连续成熟路径证据 | 旧算法批次完成连续 45 CO2/13 H2O；新算法批次在获批后完成 47/14，且点内路径均为 0620/0621。 |
| 3 | `current_batch_fit_candidate_approval` | `blocked_by_current_batch_evidence` | 当前批次 QC、0613 多策略拟合与候选批准 | 每台设备的 CO2/H2O 输入、剔除/替代原因、低端锚点和最大误差均可追溯，并产生 no-write approved candidate。 |
| 4 | `device_controlled_write_readback_short_reverify` | `blocked_by_fit_candidate_and_hardware` | 真实设备受控写入、读回与短复验 | 按槽位快照旧值、节拍写入、GETCO 读回、必要时回滚，并将 write success 与 validation success 分开记录。 |
| 5 | `new_algorithm_47_14_live_smoke_and_batch_acceptance` | `hardware_and_separate_authorization_required` | 新算法 47/14 live handoff smoke 与批次 acceptance | 先以最小 smoke 证明 profile 只改变点表/拟合/R0/write contract，再完成真实 47/14 批次。 |
| 6 | `production_evidence_import_archive_and_release` | `blocked_by_migration_real_archive_and_separate_authorization` | 生产证据入库、读回、归档和正式 release | production importer 绑定 promotion/plan/bundle/migration 四个哈希和同一 cluster system_identifier，原子入库并查询读回后再刷新归档/状态。 |

## 当前结论

- V1.5 程序结构和离线自动化能力已经完成，不再把已实现的小包列为待开发。
- 生产 PostgreSQL 18 staging 已真实验证；生产 migration 002 和 production import 从未执行。
- 当前没有真机批次证据时，不允许拟合候选、写系数、live queue、入库或 release。
- 下一项是收集真实 operator/reviewer/approver 身份并审核 migration 002 授权包；不得由程序虚构身份。
- `full_production_auto_allowed=false`、`formal_release_allowed=false`、`database_import_allowed=false`。
- 本包不开 COM、不控压力/气水路、不写设备、不连接生产 PostgreSQL。
- `not_real_acceptance_evidence=true`。
