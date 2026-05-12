# B2 No-Write Stage Gate and Evidence Registry Seed

**生成时间**: 2026-05-12  
**仓库**: `D:/gas_calibrator`  
**分支**: `codex/v2-golden-recovery-cdb82111`  
**HEAD**: `8966ab6d`  
**上游**: `B1_EVIDENCE_CLOSURE_AND_NEXT_STAGE_DECISION.md`（B1_CLOSURE_PASS）  
**报告类型**: B2 阶段 no-write stage gate 清单与证据注册种子  
**角色**: `engineering_probe_only` / `not_real_acceptance_evidence=true` / `promotion_state=blocked`

---

## 1. Purpose

1. B2 不是 production acceptance。
2. B2 不是 controlled write。
3. B2 不是 formal switch。
4. B2 不是 V2 替代 V1。
5. B2 是下一阶段 **no-write 证据注册、门禁清单、阶段准入准备**。
6. B2 不是跑真机，不是写参数。
7. 本文件是 B2 的初始种子清单（seed），后续 B2 各子任务会填充和更新本清单。

---

## 2. B2 Entry Preconditions

| # | 条件 | 状态 |
|---|------|:--:|
| 1 | H2O R4/R5 PASS（vent/valve 顺序正确，sealed vent=0，no-write 完整） | ✅ 已满足 |
| 2 | CO2 protection gate（route_text guard 隔离确认） | ✅ 已满足 |
| 3 | B1-R1 host-contract fix（orchestrator.py + conditioning_service.py 补齐） | ✅ 已满足 |
| 4 | CO2 A2 protected baseline audit（51 PASS，保护链 intact） | ✅ 已满足 |
| 5 | V1 fallback retained（默认入口未切换，V1 生产代码未修改） | ✅ 已满足 |
| 6 | no controlled write | ✅ 已满足 |
| 7 | no production acceptance | ✅ 已满足 |
| 8 | stash@{0} not restored | ✅ 需保持 |
| 9 | worktree clean（tracked） | ✅ 已确认 |
| 10 | B1 closure decision = PASS | ✅ 已确认 |

**结论**：B2 准入条件全部满足。

---

## 3. Evidence Registry Seed

B2 初始证据注册表（后续 B2 子任务补充）：

| Evidence ID | Route | Type | Source File | Decision | Notes |
|---|---|---|---|---|---|
| `H2O-D29-R4` | H2O | no-write engineering probe | `_handoff/d29_h2o_vent_order_5bc4fa2c_r4_evidence_pack/` | **PASS** | vent=OFF → vent_closed_verified → 1.5s wait → set_h2o_path(False) → seal gate |
| `H2O-D29-R5` | H2O | repeatability | `_handoff/d29_h2o_vent_order_034b2d6b_r5_evidence_pack/` | **PASS** | repeatability 复现；同顺序；sealed vent=0；no-write；七压力点完成 |
| `CO2-GATE-A3` | CO2 | protection gate | `A3_ENTRY_REVIEW_FROM_D29_R4_R5_WITH_CO2_GATE.md` | **PASS** | route_text guard 隔离；CO2 路径未受 H2O D29 影响 |
| `CO2-B1-R1-ROOTCAUSE` | CO2 | root cause | `CO2_GOLDEN_PATH_B1_R1_ROOT_CAUSE_REPORT.md` | **PASS** | orchestrator-host contract gap；非 H2O 回归；分类 B |
| `CO2-B1-R1-FIX` | CO2 | host-contract fix | `CO2_GOLDEN_PATH_B1_R1_FIX_AND_SIM_RERUN.md` | **PASS** | `c62093c4`；contract tests 47/47；sim MATCH；sealed vent=0；write=0 |
| `CO2-A2-BASELINE` | CO2 | protected baseline audit | `CO2_A2_SEVEN_PRESSURE_PROTECTED_BASELINE_AUDIT_AFTER_B1_R1.md` | **PASS** | A2 51 PASS；七压力点 `[1100..500]` intact；保护链完整 |

---

## 4. B2 Known Gaps

以下 gap 已在 B1 closure 中识别，在 B2 阶段需要逐一处理或明确推迟：

| # | Gap | 严重等级 | B2 处理建议 |
|---|-----|:--:|------|
| 1 | `test_a2_no_write_pressure_sweep.py` 114 个预存失败（mock fixture 缺口） | **P1** | B2 做专项设计（测试 mock fixture 修复方案），不动 runtime 代码；是否修复 fixture 需单独确认 |
| 2 | B1-R1 simulation profile 没有显式 `no_write_guard_active=true`（当前仅 `collect_only=true` + `simulation_mode=true`） | **P2** | B2 确认是否需要收紧 profile，或留到后续阶段 |
| 3 | 真实 A2 probe 不自动执行 | **P0 guard** | B2 不自动执行真机 probe；如需执行需单独 operator confirmation + CLI unlock + no-write |
| 4 | controlled write 仍无准入 | **P0 guard** | B2 不建立 controlled write 准入 |
| 5 | V1/V2 正式对照验收未开始 | **P1** | B2 可整理对照验收 checklist，但不执行对照 |
| 6 | A4/A5/A6/A7/A8/A9 均未进入 | **P2** | B2 不跨阶段，仅整理未来阶段骨架名称 |
| 7 | shared service 高风险改动（valve safe-stop、pressure_control_service 重构） | **P0 guard** | B2 不触碰；标记为 B3+ 或独立专项 |

---

## 5. B2 Allowed Actions

| # | 允许事项 |
|---|---------|
| 1 | 整理 evidence registry（盘点已有证据、补全缺失条目） |
| 2 | 整理 acceptance checklist（按 gas type / route / pressure / temperature / write/no-write 分类） |
| 3 | 整理 no-write boundary checklist（明确哪些操作是 no-write、哪些触发 write） |
| 4 | 修复测试 fixture/mock 的**后置专项设计**（仅设计文档，不修 runtime） |
| 5 | 生成 handoff 文档 |
| 6 | 生成 stage gate checklist |
| 7 | 做离线 pytest（不跑真实 COM） |
| 8 | 补齐 B1 证据注册中的遗漏条目 |
| 9 | 设计 B2 → B3 的门禁条件清单 |
| 10 | 审计现有 profile 中 no-write 相关字段的一致性 |

---

## 6. B2 Forbidden Actions

| # | 禁止事项 | 原因 |
|---|---------|------|
| 1 | 真实 COM | 当前不允许 |
| 2 | `--execute-probe` | 真实探针执行禁止 |
| 3 | 写 ID / SENCO / zero / span / coefficient | no-write 约束 |
| 4 | controlled write | 未通过准入 |
| 5 | production acceptance | 未进入该阶段 |
| 6 | formal switch | 未进入该阶段 |
| 7 | 恢复 stash@{0} | 含高风险改动（flow_lpm removal + test contract updates） |
| 8 | 改 V1 | 硬约束 |
| 9 | 改 H2O runtime | H2O D29 已停止补丁 |
| 10 | 改 CO2 runner | CO2 主流程不修改 |
| 11 | 改 pressure_control_service | shared service 不修改 |
| 12 | 改 valve_routing_service | shared service 不修改 |
| 13 | 新增 simulation profile | 不扩 profile |
| 14 | 新增 points 文件 | 不扩 points |
| 15 | 把 B1-R1 6-point simulation 说成七压力点 | 事实错误 |
| 16 | 把 CO2 A2 baseline audit 说成真机重跑 | 事实错误 |
| 17 | 重复做 H2O/CO2 对照审计 | 重复工作 |

---

## 7. B2 Stage Gate Checklist (Initial Seed)

初始门禁清单骨架（后续 B2 子任务填充细节）：

### 7.1 No-Write Boundary Gate

- [ ] 盘点全部 V2 runtime 中写入类操作的调用点
- [ ] 确认每个写入调用点有对应的 no-write guard
- [ ] 确认 `collect_only` flag 对所有写入点的阻断覆盖
- [ ] 确认 `apply_device_id=false` 对 ID 写入的阻断
- [ ] 确认 `allow_write_coefficients/zero/span/calibration_parameters=false` 的一致性

### 7.2 Evidence Completeness Gate

- [ ] H2O 证据条目数 ≥ 2（R4 + R5）
- [ ] CO2 证据条目数 ≥ 3（protection gate + B1-R1 + A2 baseline）
- [ ] 每条证据都有明确文件路径
- [ ] 每条证据都有 decision 和 notes

### 7.3 Protection Chain Gate

- [ ] A2 seven-pressure no-write probe config 未被修改
- [ ] A2 seven-pressure points file 未被修改
- [ ] A2 seven-pressure protection tests (51) 仍通过
- [ ] CO2 contract tests (47) 仍通过
- [ ] V1 fallback 仍保留

### 7.4 Compliance Gate

- [ ] worktree clean（tracked）
- [ ] stash@{0} 未恢复
- [ ] 无真实 COM 记录
- [ ] 无参数写入记录
- [ ] 无 controlled write 记录
- [ ] 无 production acceptance 记录

---

## 8. Recommended B2 First Task

**`B2-P1: evidence registry formalization + acceptance checklist`**

内容：
1. 基于 Section 3 的种子表，补全所有证据条目的详细元数据（timestamp、commit、test results、trace 链接）。
2. 整理 B2 门禁清单（Section 7）的详细执行步骤。
3. 输出 `B2_EVIDENCE_REGISTRY_V1.md` 与 `B2_ACCEPTANCE_CHECKLIST_V1.md`。

**注意**：下一轮仍不跑真机、不写参数。

---

## 9. B2 Exit Criteria (Preliminary)

B2 完成条件（初步，后续 B2 子任务可细化）：

1. Evidence registry 已建立且条目 ≥ 9（覆盖 H2O + CO2 全部已收集证据）。
2. Acceptance checklist 已建立（按 gas type / route / stage-gate 分类）。
3. No-write boundary 已在代码级 doc 中盘点完毕。
4. Protection chain gate checklist 通过。
5. Compliance gate checklist 通过。
6. 所有 B2 文档均为 handoff 报告，不涉及 runtime 修改。

---

*B2 No-Write Stage Gate and Evidence Registry Seed 结束。*
