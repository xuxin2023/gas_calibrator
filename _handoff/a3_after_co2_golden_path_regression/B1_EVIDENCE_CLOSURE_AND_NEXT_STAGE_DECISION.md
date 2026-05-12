# B1 Evidence Closure and Next-Stage Decision

**生成时间**: 2026-05-12  
**仓库**: `D:/gas_calibrator`  
**分支**: `codex/v2-golden-recovery-cdb82111`  
**HEAD**: `8966ab6d`  
**报告类型**: B1 阶段证据收口与下一阶段准入决策  
**角色**: `engineering_probe_only` / `not_real_acceptance_evidence=true` / `promotion_state=blocked`

---

## 1. Scope

1. 这是 **B1 阶段证据收口**，用于确认 B1 证据阶段已完成并关闭。
2. 不是 runtime 修改。
3. 不是重复 H2O/CO2 对照审计。
4. 不是真实 COM。
5. 不是 controlled write。
6. 不是 production acceptance。
7. 不是 formal switch。
8. 不是 V2 替代 V1 宣告。

本轮仅新增 2 个 handoff 文档（本文档 + B2 seed），不修改任何 runtime/config/test/profile/points。

---

## 2. Current Git State

| 项目 | 值 |
|------|-----|
| 当前 branch | `codex/v2-golden-recovery-cdb82111` |
| 当前 HEAD | `8966ab6d` |
| 远端 | `origin/codex/v2-golden-recovery-cdb82111`（已同步） |
| tracked worktree | clean（无 tracked modified / staged） |
| stash@{0} | 保留未恢复：`D29-R4 unrelated: flow_lpm removal + test contract updates NOT part of 5bc4fa2c` |
| runtime/config/test/profile/points 修改 | 无 |
| real COM execution | 无 |
| parameter write | 无 |

最近关键 commits：

| Hash | Message |
|------|---------|
| `5bc4fa2c` | fix(v2): enforce H2O ambient-to-sealed vent valve order |
| `034b2d6b` | test(v2): align H2O probe and contracts with D29-R4 baseline |
| `171e530c` | docs(handoff): D29-R5 evidence pack with A3 CO2 gate review |
| `eb605517` | docs(handoff): A3-H2O entry PASS decision and CO2 gate check |
| `c62093c4` | fix(v2): close CO2 preseal host contract gap |
| `8966ab6d` | docs(handoff): audit CO2 A2 seven-pressure protected baseline |

---

## 3. Evidence Already Completed

| 证据 | 文件 | 结论 |
|---|---|---|
| A3-H2O 准入索引 | `A3_ENTRY_INDEX.md` | A3-H2O 材料已成组，入口清晰 |
| A3-H2O + CO2 gate | `A3_ENTRY_REVIEW_FROM_D29_R4_R5_WITH_CO2_GATE.md` | H2O R4/R5 PASS，CO2 gate 已做，A3-H2O 准入通过 |
| CO2 protection plan | `CO2_GOLDEN_PATH_PROTECTION_AND_REGRESSION_PLAN.md` | CO2 物理阶段和 guard 已定义；CO2 路径被 H2O D29 修改完全隔离 |
| B1-R1 root cause | `CO2_GOLDEN_PATH_B1_R1_ROOT_CAUSE_REPORT.md` | orchestrator-host interface contract gap（batch-7 遗留），不是 H2O D29 回归 |
| B1-R1 fix | `CO2_GOLDEN_PATH_B1_R1_FIX_AND_SIM_RERUN.md` | `c62093c4` 修复；CO2 contract tests 47/47 PASS；simulation MATCH；sealed vent=ON count=0；write=0 |
| CO2 A2 baseline audit | `CO2_A2_SEVEN_PRESSURE_PROTECTED_BASELINE_AUDIT_AFTER_B1_R1.md` | A2 seven-pressure protected baseline intact；A2 51 PASS；CO2 contracts 47 PASS；PASS 仅表示保护链完整 |

---

## 4. H2O Status

- **H2O D29 R4/R5 已 PASS**，构成 repeatability 工程证据。
- 物理顺序已正确：
  ```
  ambient_sample_complete
  → vent=OFF
  → vent_closed_verified
  → wait 1.5s
  → set_h2o_path(False)
  → seal gate
  → sealed pressure control
  ```
- sealed 阶段 vent=ON count = **0**。
- no-write 证据完整（`attempted_write_count=0`、`identity_write_command_sent=false`）。
- 七压力点（ambient + 1100/1000/900/800/700/600/500）两次均全部完成。
- **当前不继续做 D29/D3 runtime 补丁**。
- 当前不是回到水路继续修。

---

## 5. CO2 Status

- CO2 protected baseline 是 **Run-001/A2 CO2-only seven-pressure no-write probe**。
- 七压力点是 `[1100, 1000, 900, 800, 700, 600, 500]`。
- 不含常压点（ambient/open-conditioning 点不属于密封压力扫点）。
- 该保护链 **没有被 H2O A3 或 B1-R1 破坏**。
- A2 seven-pressure protected baseline tests = **51 passed**。
- CO2 contract tests = **47 passed**（含 route_runner / no-vent guard / artifact contract / golden master / shadow state trace / pressure_control_service）。
- `c62093c4` 修复了 orchestrator-host contract gap，CO2 全栈 simulation 恢复 MATCH。
- B1-R1 的 6 points simulation（Group A: 1100/1000/600 + Group B: 1100/1000/600）是 **simplified integration simulation**，不是 CO2 七压力点。
- **不需要重新建立七压力点主线**。
- **不需要新增 seven-pressure simulation profile**。

---

## 6. B1-R1 Status

- B1-R1 root cause 是 orchestrator-host interface contract gap：batch-7（`2ec4682f`）将 conditioning 逻辑移入 `conditioning_service.py`，但 `orchestrator.py` 未同步补齐 `_verify_co2_preseal_atmosphere_hold_pressure` 和 `_refresh_live_analyzer_snapshots` 方法。
- `c62093c4` 修复该缺口：orchestrator.py +2 wrapper methods，conditioning_service.py +115 行实现。
- `8966ab6d` 固化 CO2 A2 protected baseline audit，确认保护链完整。
- B1-R1 完成了 host-contract 修复与保护链审计。
- **不是真机七压力点重跑**。
- **不代表 production acceptance**。

---

## 7. Do We Need Another H2O/CO2 Audit?

**不需要重复做同类 H2O/CO2 保护门审计。**

原因：
- A3-H2O + CO2 gate 已做（`A3_ENTRY_REVIEW_FROM_D29_R4_R5_WITH_CO2_GATE.md`），结论为 H2O PASS + CO2 gate PASS。
- CO2 protection plan 已存在（`CO2_GOLDEN_PATH_PROTECTION_AND_REGRESSION_PLAN.md`），物理阶段和 guard 已明确定义。
- B1-R1 后又做了 CO2 A2 seven-pressure protected baseline audit（`CO2_A2_SEVEN_PRESSURE_PROTECTED_BASELINE_AUDIT_AFTER_B1_R1.md`），确认保护链完整。
- 再做同类审计属于重复工作，容易制造报告噪声。

但必须强调：
- 这不等于 production acceptance。
- 这不等于 controlled write。
- 这不等于 formal switch。
- 这不等于可以关闭 V1 fallback。

---

## 8. Remaining Gaps

| # | Gap | 处理阶段 |
|---|-----|---------|
| 1 | B2 evidence registry / acceptance checklist 尚未建立 | **B2 本轮启动**（见 B2 seed 文档） |
| 2 | `test_a2_no_write_pressure_sweep.py` 114 个预存失败需要后置专项（~70 个 mocking condition_service + ~35 个 mocking a2_hooks） | B2-P2 或更高 |
| 3 | 真实 A2 probe 如未来需要，必须单独 operator confirmation + CLI unlock + no-write | 未来（非当前阶段） |
| 4 | controlled write 仍禁止 | 持续 |
| 5 | production acceptance 仍禁止 | 持续 |
| 6 | formal switch 仍禁止 | 持续 |
| 7 | stash@{0} 高风险内容仍不恢复 | 持续 |
| 8 | shared service 高风险改动（valve safe-stop、pressure_control_service 重构等）需单独阶段 | 未来 |
| 9 | V1 fallback 必须保留 | 持续 |
| 10 | B1-R1 simulation profile 没有显式 `no_write_guard_active=true`（当前仅 `collect_only=true` + `simulation_mode=true`） | B2 或后续阶段确认是否收紧 |

---

## 9. Decision

**final_decision**: **`B1_CLOSURE_PASS`**

满足以下全部条件：

| # | 条件 | 状态 |
|---|------|:--:|
| 1 | 当前 HEAD 为 `8966ab6d` | ✅ |
| 2 | tracked worktree clean | ✅ |
| 3 | A3-H2O 证据存在 | ✅ |
| 4 | CO2 protection gate 证据存在 | ✅ |
| 5 | B1-R1 root cause / fix 证据存在 | ✅ |
| 6 | CO2 A2 seven-pressure protected baseline audit 存在 | ✅ |
| 7 | 无 runtime/config/test/profile/points 修改 | ✅ |
| 8 | 无真实 COM | ✅ |
| 9 | 无参数写入 | ✅ |
| 10 | 无 controlled write | ✅ |
| 11 | 无 production acceptance | ✅ |
| 12 | 无 formal switch | ✅ |

**含义限定**：
> B1 证据阶段已完成工程证据收集、host-contract 修复、保护链审计。B1 可以关闭，进入 B2 no-write stage-gate 阶段。这不表示 production acceptance、controlled write、formal switch 或 V2 替代 V1。

---

## 10. Recommended Next Stage

1. **当前关闭 B1-R1 / B1 evidence closure**。
2. **下一步进入 B2**：no-write evidence registry / acceptance checklist / stage gate checklist。
3. B2 仍是 **no-write / engineering evidence**。
4. B2 第一阶段只整理证据和门禁，不跑真机、不写参数。
5. 是否执行真实 A2 probe 需要用户另行单独确认。
6. V1 fallback 继续保留。

---

*B1 Evidence Closure and Next-Stage Decision 结束。*
