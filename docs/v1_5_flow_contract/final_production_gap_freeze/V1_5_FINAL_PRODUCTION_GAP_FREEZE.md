# V1.5 最终生产缺口冻结清单

- source origin/main: `2dca3bcd67e35268110abd50cf4c3819b8ef330d`
- overall_status: `production_gap_scope_frozen_offline_replay_next`
- scope_frozen: `true`
- critical_gap_count: `7`
- recommended_next_gap_id: `legacy_full_flow_orchestrator_offline_replay`
- mature fitting baseline: `0613 V1.5 fitting path`
- mature route baseline: `0620/0621 clean-worktree mature physical route path`

这份清单取代 2026-07-10 的旧 closure/next-action 快照。新增生产缺口必须得到用户明确批准。

## 冻结的生产关键缺口

| 优先级 | 状态 | gap_id | 工作 | 完成标准 |
|---:|---|---|---|---|
| 1 | `next` | `legacy_full_flow_orchestrator_offline_replay` | 旧算法全流程 orchestrator 离线 replay | A single replay explains every stage, exact evidence consumed, hold reason, and next action without promoting replay to real acceptance. |
| 2 | `pending` | `production_component_qc_and_0613_fit_matrix` | 生产 component-QC evaluator 与 0613 拟合策略矩阵 | Every fit input has component-matched QC, physical supersede/reject reasons, anchor roles, and a no-write candidate decision. |
| 3 | `pending` | `unified_controlled_write_readback_reverify` | 统一系数受控写入、读回与短复验闭环 | Write success and validation success are reported separately for S1-S9 and SENCOA/B as applicable. |
| 4 | `pending` | `new_algorithm_47_14_live_mature_queue_handoff` | 新算法 47/14 点 live mature-queue 接入 | 47 CO2 and 14 H2O points run through the same 0620/0621 runners; only fit input/R0/write contracts differ. |
| 5 | `pending` | `postgresql18_controlled_import` | PostgreSQL 18 真实受控入库 | Schema, uniqueness, preview, transaction, readback, rollback, and import lineage pass before database_import_allowed becomes true. |
| 6 | `pending` | `final_offline_acceptance_suite` | 最终 simulation/replay/parity/resilience 验收套件 | All offline suites pass and every artifact retains not_real_acceptance_evidence=true. |
| 7 | `hardware_deferred` | `real_batch_acceptance_when_hardware_available` | 真机批次 acceptance（设备可用时） | Continuous mature routes, fitting, controlled writes, reverify, archive, and PostgreSQL import close with real evidence. |

## 延后项

- `historical_component_qc_backfill_writer`: Historical 125-point repair is useful for archaeology but is not required to finish the current production orchestrator. Only resume by explicit user approval or when a frozen replay blocker cannot be resolved from canonical evidence.
- `root_pollution_cleanup_and_v1_v2_deletion`: The root worktree remains quarantined; deleting historical folders is not needed for V1.5 production closure. Handle as a separate retention/cleanup review after production automation closes.
- `noncritical_ui_report_polish`: Cosmetic expansion must not displace orchestration, fitting, writing, database, or acceptance work. Resume after the final offline suite or for a concrete production usability defect.

## 当前锁

- 旧算法生产物理基准仅认 `0613` 拟合与 `0620/0621` clean mature route。
- 旧算法保持 `45 CO2 / 13 H2O`；新算法候选保持 `47 / 14`。
- `full_production_auto_allowed=false`。
- `live_queue_execution_allowed=false`。
- `formal_release_allowed=false`。
- `database_import_allowed=false`。
- 本包不开 COM、不控压力/气水路、不写系数、不连 PostgreSQL。
- `not_real_acceptance_evidence=true`。
