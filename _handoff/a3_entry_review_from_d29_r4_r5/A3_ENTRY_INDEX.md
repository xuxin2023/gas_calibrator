# A3 准入评审索引

**基线**: `171e530c`  
**分支**: `codex/v2-golden-recovery-cdb82111`  
**生成时间**: 2026-05-12  
**本文档角色**: A3-H2O 准入评审材料总索引  

---

## 1. 仓库状态

| 项目 | 值 |
|------|-----|
| 当前 HEAD | `171e530c` |
| 分支 | `codex/v2-golden-recovery-cdb82111` |
| 远端 | `origin/codex/v2-golden-recovery-cdb82111` (已同步) |
| tracked modified | 0 |
| staged | 0 |
| stash@{0} | 保留 (未恢复) |

## 2. 关键 Commits

| Hash | Message |
|------|---------|
| `171e530c` | docs(handoff): D29-R5 evidence pack with A3 CO2 gate review |
| `034b2d6b` | test(v2): align H2O probe and contracts with D29-R4 baseline |
| `5bc4fa2c` | fix(v2): enforce H2O ambient-to-sealed vent valve order |
| `54f4b2df` | fix(v2): add humidity generator flow_lpm 1.5 L/min aligned with V1 |

## 3. Evidence Pack 路径

| 包 | 路径 |
|----|------|
| D29-R4 evidence pack | `_handoff/d29_h2o_vent_order_5bc4fa2c_r4_evidence_pack/` |
| D29-R5 evidence pack | `_handoff/d29_h2o_vent_order_034b2d6b_r5_evidence_pack/` |

## 4. A3 准入评审材料路径

| 文档 | 路径 |
|------|------|
| **A3-H2O 准入评审执行单** | `_handoff/a3_entry_review_from_d29_r4_r5/A3_H2O_ENTRY_REVIEW_EXECUTION_PLAN.md` |
| **CO2 golden path 保护与回归计划** | `_handoff/a3_entry_review_from_d29_r4_r5/CO2_GOLDEN_PATH_PROTECTION_AND_REGRESSION_PLAN.md` |
| **Stash 后置专项拆分计划** | `_handoff/a3_entry_review_from_d29_r4_r5/STASH_DEFERRED_WORK_SPLIT_PLAN.md` |
| **A3 准入评审索引** (本文档) | `_handoff/a3_entry_review_from_d29_r4_r5/A3_ENTRY_INDEX.md` |

还有两个证据包内的重要引用文件：
| 文档 | 路径 |
|------|------|
| A3 with CO2 gate review (R5 包内) | `_handoff/d29_h2o_vent_order_034b2d6b_r5_evidence_pack/A3_ENTRY_REVIEW_FROM_D29_R4_R5_WITH_CO2_GATE.md` |
| D29-R5 repeatability evidence (R5 包内) | `_handoff/d29_h2o_vent_order_034b2d6b_r5_evidence_pack/D29_R5_H2O_REPEATABILITY_EVIDENCE.md` |

## 5. 关键判断

| 问题 | 回答 |
|------|------|
| 是否建议进入 A3-H2O 准入评审 | **YES** ✅ |
| 是否建议继续 D29/D3 runtime 补丁 | **NO** — R4/R5 已充分验证 |
| 是否建议停止 D29 runtime 补丁 | **YES** ✅ |
| 是否已 push 到远端 | **YES** — 171e530c 已推送 |
| 是否建议继续 push 当前 A3 材料 | **NO** — 等待用户确认 |
| 是否建议恢复 stash@{0} | **NO** — 按拆分计划分阶段推进 |

## 6. 下一步建议

1. **立即**：用户确认 A3 准入材料后，提交 `_handoff/a3_entry_review_from_d29_r4_r5/` 到 git 并 push
2. **A3 后 Phase 1**：确认 test contract 文件 (B1/B2) 与 stash 一致，diff 对齐
3. **A3 后 Phase 2**：config/probe cleanup (A1/A2)
4. **A3 后 Phase 3**：flow_lpm 行为收缩专项 (C1/C2/E1/E2)
5. **A3 后优先**：CO2 golden path no-write simulation 回归
6. **更高阶段前**：shared valve safe-stop 高风险专项 (D1/D2)

## 7. 硬约束状态

| 约束 | 状态 |
|------|:--:|
| 不改 V1 | ✅ |
| 不改 CO2 主链 | ✅ |
| 不改 runtime | ✅ |
| 不恢复 stash@{0} | ✅ |
| 不写 ID/SENCO/zero/span/coefficient | ✅ |
| 不默认切 V2 | ✅ |
| 不做 production acceptance | ✅ |
| 不做 formal switch | ✅ |
| 不做 controlled write | ✅ |
| 不跑真实 COM | ✅ |
| V1 fallback retained | ✅ |

---

**A3 准入评审材料索引结束。等待用户确认后提交。**
