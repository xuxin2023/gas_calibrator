# B2 Evidence Registry V1

**生成时间**: 2026-05-12  
**仓库**: `D:/gas_calibrator`  
**分支**: `codex/v2-golden-recovery-cdb82111`  
**HEAD**: `02972ea5`  
**报告类型**: B2 阶段证据注册表 V1（正式版）  
**上游**: `B1_EVIDENCE_CLOSURE_AND_NEXT_STAGE_DECISION.md`（B1_CLOSURE_PASS）、`B2_NO_WRITE_STAGE_GATE_AND_EVIDENCE_REGISTRY_SEED.md`（B2 seed）  
**角色**: `engineering_probe_only` / `not_real_acceptance_evidence=true` / `promotion_state=blocked`

---

## 1. Scope

1. 这是 **B2 evidence registry V1**，将 B1/B2 已收集的全部工程证据正式登记。
2. 不是 production acceptance。
3. 不是 controlled write。
4. 不是 formal switch。
5. 不是 V2 替代 V1。
6. 仅登记 no-write engineering evidence。
7. 不包含任何运行时修改或 real COM 记录。

---

## 2. Evidence Registry

| Evidence ID | Route | Stage | Type | Commit | Source File/Folder | Decision | Acceptance Meaning | Not Meaning |
|---|---|---|---|---|---|---|---|---|
| `H2O-D29-R4` | H2O | A3 | no-write engineering probe | `5bc4fa2c` | `_handoff/d29_h2o_vent_order_5bc4fa2c_r4_evidence_pack/` | **PASS** | vent/valve 顺序修复验证通过；sealed vent=0；no-write；七压力点完成 | 非 production acceptance；非 controlled write；非 formal switch |
| `H2O-D29-R5` | H2O | A3 | repeatability | `034b2d6b` | `_handoff/d29_h2o_vent_order_034b2d6b_r5_evidence_pack/` | **PASS** | R4 的 repeatability 复现；sealed vent=0；no-write；no-flow；七压力点完成 | 非 production acceptance；非 controlled write；非 formal switch |
| `CO2-GATE-A3` | CO2 | A3 | protection gate | `eb605517` | `_handoff/d29_h2o_vent_order_034b2d6b_r5_evidence_pack/A3_ENTRY_REVIEW_FROM_D29_R4_R5_WITH_CO2_GATE.md` | **PASS** | route_text guard 隔离确认；CO2 路径未受 H2O D29 修改影响 | 非 CO2 真机测试；非 CO2 route 修改 |
| `CO2-B1-R1-ROOTCAUSE` | CO2 | B1 | root cause analysis | `eb605517` | `_handoff/a3_after_co2_golden_path_regression/CO2_GOLDEN_PATH_B1_R1_ROOT_CAUSE_REPORT.md` | **ALLOW_MINIMAL_FIX** | 根因确认：orchestrator-host contract gap（batch-7 遗留）；非 H2O 回归；分类 B | 非 runtime bug；非 H2O 缺陷 |
| `CO2-B1-R1-FIX` | CO2 | B1 | host-contract fix + sim rerun | `c62093c4` | `_handoff/a3_after_co2_golden_path_regression/CO2_GOLDEN_PATH_B1_R1_FIX_AND_SIM_RERUN.md` | **PASS** | orchestrator +2 wrapper + conditioning_service +115 行；CO2 contract tests 47/47；simulation MATCH；sealed vent=ON count=0；write=0 | 非七压力点回归；非真机重跑；非 production acceptance |
| `CO2-A2-BASELINE` | CO2 | B1 | protected baseline audit | `8966ab6d` | `_handoff/a3_after_co2_golden_path_regression/CO2_A2_SEVEN_PRESSURE_PROTECTED_BASELINE_AUDIT_AFTER_B1_R1.md` | **PASS** | A2 51 PASS；七压力点 `[1100..500]` 保护链 intact；CO2 contracts 47 PASS | 非真机重跑；非 controlled write ready；非 production acceptance |
| `B1-CLOSURE` | H2O+CO2 | B1 | evidence closure | `02972ea5` | `_handoff/a3_after_co2_golden_path_regression/B1_EVIDENCE_CLOSURE_AND_NEXT_STAGE_DECISION.md` | **B1_CLOSURE_PASS** | 12 条准入条件全部满足；B1 证据阶段可关闭 | 非 B2 穿透；非 production acceptance |
| `B2-SEED` | H2O+CO2 | B2 | stage gate seed | `02972ea5` | `_handoff/a3_after_co2_golden_path_regression/B2_NO_WRITE_STAGE_GATE_AND_EVIDENCE_REGISTRY_SEED.md` | **PASS** | B2 准入条件全部满足；seed registry + checklist 骨架已建立 | 非 B2 完成；非 production acceptance |
| `CO2-PROTECTION-PLAN` | CO2 | A3 | protection & regression plan | `171e530c` | `_handoff/a3_entry_review_from_d29_r4_r5/CO2_GOLDEN_PATH_PROTECTION_AND_REGRESSION_PLAN.md` | **PASS** | CO2 物理阶段和 guard 已定义；CO2 路径被 H2O D29 修改完全隔离 | 非 regression execution；非真机测试 |

---

## 3. Evidence Summary by Route

### 3.1 H2O Route

| Evidence ID | no-write | real COM | production acceptance | controlled write | formal switch | 可用于下一阶段准入 |
|---|---|---|---|---|---|---|
| `H2O-D29-R4` | ✅ | ❌（no-write engineering probe，但涉及真实设备） | ❌ | ❌ | ❌ | ✅（工程证据） |
| `H2O-D29-R5` | ✅ | ❌（同上） | ❌ | ❌ | ❌ | ✅（repeatability 证据） |

**注意**：H2O R4/R5 虽然 no-write（未写参数），但执行了真实 COM 设备操作（vent/valve/pressure control）。与 CO2 simulation-only 证据有本质区别，但两者均为 engineering probe，不构成 production acceptance。

### 3.2 CO2 Route

| Evidence ID | no-write | real COM | production acceptance | controlled write | formal switch | 可用于下一阶段准入 |
|---|---|---|---|---|---|---|
| `CO2-GATE-A3` | N/A（静态分析） | ❌ | ❌ | ❌ | ❌ | ✅（静态门禁证据） |
| `CO2-B1-R1-ROOTCAUSE` | N/A（审计） | ❌ | ❌ | ❌ | ❌ | ✅（根因证据） |
| `CO2-B1-R1-FIX` | ✅ | ❌（simulation only） | ❌ | ❌ | ❌ | ✅（fix + sim 证据） |
| `CO2-A2-BASELINE` | ✅ | ❌（test only） | ❌ | ❌ | ❌ | ✅（保护链证据） |
| `CO2-PROTECTION-PLAN` | N/A（计划文档） | ❌ | ❌ | ❌ | ❌ | ✅（计划证据） |

### 3.3 Cross-Route

| Evidence ID | no-write | real COM | production acceptance | controlled write | formal switch | 可用于下一阶段准入 |
|---|---|---|---|---|---|---|
| `B1-CLOSURE` | N/A（收口文档） | ❌ | ❌ | ❌ | ❌ | ✅（阶段准入证据） |
| `B2-SEED` | N/A（种子文档） | ❌ | ❌ | ❌ | ❌ | ✅（阶段计划证据） |

---

## 4. Key Facts Confirmed

1. **H2O R4/R5 PASS**：vent/valve 顺序正确，sealed vent=ON count=0，no-write，七压力点完成，构成 repeatability 工程证据。

2. **H2O 正确物理顺序**：
   ```
   ambient_sample_complete
   → vent=OFF
   → vent_closed_verified
   → wait 1.5s
   → set_h2o_path(False)
   → seal gate
   → sealed pressure control
   ```

3. **CO2 protected baseline**：Run-001/A2 CO2-only seven-pressure no-write probe。

4. **CO2 七压力点**：`[1100, 1000, 900, 800, 700, 600, 500]`，不含常压点。

5. **B1-R1 6-point simulation**：简化集成 simulation（Group A: 1100/1000/600 + Group B: 1100/1000/600），**不是七压力点**，不得混淆。

6. **A2 保护测试**：51 passed。

7. **CO2 contract tests**：47 passed。

8. **B1_CLOSURE_PASS**：已成立（`02972ea5`）。

---

## 5. Registry Gaps

以下 gap 已识别，需后置 B2 子任务或更高阶段处理：

| # | Gap | 当前状态 | 处理阶段 |
|---|-----|---------|---------|
| 1 | `test_a2_no_write_pressure_sweep.py` 114 个预存失败（~70 mocking condition_service + ~35 mocking a2_hooks） | 未修复 | B2-P2 或更高 |
| 2 | B1-R1 simulation profile 未显式 `no_write_guard_active=true`（仅 `collect_only=true` + `simulation_mode=true`） | 未收紧 | B2 或后续 |
| 3 | no-write boundary 尚未逐调用点登记 | 未执行 | B2-P3+ |
| 4 | V1/V2 正式对照验收未开始 | 未启动 | B3+ |
| 5 | 真实 A2 probe 未执行 | 未执行 | 未来（需 operator confirmation + CLI unlock + no-write） |
| 6 | A4/A5/A6/A7/A8/A9 均未进入 | 未计划 | 未来 |
| 7 | shared service 高风险改动（valve safe-stop、pressure_control_service 重构） | 禁止 | 独立专项 |
| 8 | stash@{0} 高风险内容（flow_lpm removal + test contract updates） | 禁止恢复 | 独立专项 |

---

## 6. Registry Validity

本 registry 在以下条件下有效：

1. tracked worktree clean（当前验证通过）。
2. stash@{0} 未恢复（当前验证通过）。
3. 无 runtime/config/test/profile/points 修改（当前验证通过）。
4. 无 real COM 写入记录（当前验证通过）。
5. 无 controlled write / production acceptance / formal switch（当前验证通过）。

若上述任何条件被打破，本 registry 需重新审计。

---

*B2 Evidence Registry V1 结束。*
