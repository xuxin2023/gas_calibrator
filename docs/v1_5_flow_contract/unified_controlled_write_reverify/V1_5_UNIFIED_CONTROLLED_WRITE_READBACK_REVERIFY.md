# V1.5 统一系数受控写入、读回与短复验合同

- overall_status: `blocked_no_fit_approved_candidate`
- fit baseline: `0613 V1.5 fitting path`
- mature route baseline: `0620/0621 clean-worktree mature physical route path`
- production_fit_allowed: `false`
- fit_ready_strategy_count: `0`
- operation_plan_count: `0`
- write_transaction_status: `not_authorized`
- getco_readback_status: `not_authorized`
- physical_short_reverify_status: `not_attempted`

本工件只统一证据和状态机，不执行串口命令。当前历史数据没有合格拟合候选，因此操作计划为空，写入、读回、复验继续锁定。

## 物理边界

- S1/S3 与 S2/S4 必须成对评审，使用科学计数法写入合同。
- S5/S6 是最终仿射层，必须读取当前 GETCO5/6 并按层叠关系计算绝对目标；清除、读回中性、写入、再读回缺一不可。
- S7/S8 不做温度校准，只允许保持 `[0,1,0,0]` 中性状态。
- S9 默认 offset-only；linear 只允许有明确特例证据的设备。
- SENCOA/B 只属于新算法 R0(T)，真实 writer 仍是 blocked-design-only。
- 写入成功、GETCO 读回成功和独立物理复验成功是三个不同结论。

## 当前 blockers

- `approved_candidate_packet_missing`
- `authorization_missing`
- `current_getco_snapshot_missing_or_invalid`
- `fit_ready_candidate_missing`
- `production_fit_not_allowed`
