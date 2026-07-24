# ADR: V1/V1.5 全局 no-write 策略

- 状态：已接受
- 日期：2026-07-24
- 决策：不在 V1/V1.5 生产运行器中隐式启用全局 no-write guard
- 机器可读策略：`docs/architecture/v1_5_global_no_write_policy.json`

## 背景

V1 是当前生产 fallback，V1.5 已存在按具体动作划分的 no-write 评审、受控写入、写后读回和复验链。若在通用 `CalibrationRunner` 中默认安装全局 no-write guard，会改变既有受控写入语义，也可能让已授权的生产动作静默转为拦截。这不是本次发布集成可以隐式引入的兼容性变化。

另一方面，simulation、replay、离线评审以及 V2 Step 3A 工程探针必须保持 no-write。两类需求不能折叠为同一个全局布尔开关。

## 决策

1. 不向 V1/V1.5 通用生产运行器增加默认开启的全局 no-write guard。
2. simulation、replay 和离线评审继续强制 no-write。
3. V2 默认仍禁止真实 COM；Step 3A 例外仍必须双重解锁、操作员确认、no-write，并标记 `engineering_probe_only`、`promotion_state=blocked`、`not_real_acceptance_evidence=true`。
4. V1/V1.5 写入继续采用“按入口、按动作授权”的受控链：写前旧值快照、明确授权、写后读回、rollback 计划和独立复验缺一不可。
5. 本决策不授权任何真机操作、正式数据库写入、设备系数写入或 `real_primary_latest` 刷新。

## 影响

- 保留 V1 fallback 和现有生产行为。
- 不修改 `run_app.py`，不切换默认入口。
- 旧的“全局 guard 默认开启”假设不再作为有效测试契约；测试改为锁定本 ADR 的分层策略。
- 若未来确需全局 guard，必须另立 ADR，完成 V1 生产影响评审、parity、写回故障注入验证，并取得明确授权。

## 验证

- 策略 JSON 与本 ADR 保持一致。
- `CalibrationRunner` 不隐式安装全局 no-write 状态。
- 现有 V1/V1.5 受控写安全测试继续通过。
- V2 no-write/Step 3A 既有门禁继续独立验证。
