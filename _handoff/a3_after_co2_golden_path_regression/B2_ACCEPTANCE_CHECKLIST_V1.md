# B2 Acceptance Checklist V1

**生成时间**: 2026-05-12  
**仓库**: `D:/gas_calibrator`  
**分支**: `codex/v2-golden-recovery-cdb82111`  
**HEAD**: `02972ea5`  
**报告类型**: B2 阶段 no-write 验收清单 V1  
**上游**: `B2_EVIDENCE_REGISTRY_V1.md`、`B2_NO_WRITE_STAGE_GATE_AND_EVIDENCE_REGISTRY_SEED.md`  
**角色**: `engineering_probe_only` / `not_real_acceptance_evidence=true` / `promotion_state=blocked`

---

## 1. Scope

1. 这是 **B2 no-write acceptance checklist V1**。
2. 只做证据和门禁检查，不做 runtime 修改。
3. 不跑真机，不写参数，不进入 production。
4. 非 controlled write，非 formal switch，非 V2 替代 V1。
5. 本 checklist 用于 B2 阶段内 self-check 和下一阶段准入准备。

---

## 2. Checklist

### A. Git / Worktree Gate

| # | 检查项 | 预期 | 当前状态 |
|---|--------|------|:--:|
| A1 | branch 为 `codex/v2-golden-recovery-cdb82111` | 是 | ✅ |
| A2 | HEAD 为 `02972ea5` 或更新 | 是 | ✅ |
| A3 | tracked worktree clean | 是 | ✅ |
| A4 | staged 为空 | 是 | ✅ |
| A5 | stash@{0} 未恢复 | 是 | ✅ |
| A6 | 仅新增 handoff 文档，无 runtime/config/test/profile/points 修改 | 是 | ✅ |

### B. Evidence Completeness Gate

| # | 检查项 | 证据文件 | 当前状态 |
|---|--------|---------|:--:|
| B1 | H2O R4 evidence 存在 | `_handoff/d29_h2o_vent_order_5bc4fa2c_r4_evidence_pack/` | ✅ |
| B2 | H2O R5 evidence 存在 | `_handoff/d29_h2o_vent_order_034b2d6b_r5_evidence_pack/` | ✅ |
| B3 | CO2 gate evidence 存在 | `A3_ENTRY_REVIEW_FROM_D29_R4_R5_WITH_CO2_GATE.md` | ✅ |
| B4 | CO2 protection plan 存在 | `CO2_GOLDEN_PATH_PROTECTION_AND_REGRESSION_PLAN.md` | ✅ |
| B5 | B1-R1 root cause evidence 存在 | `CO2_GOLDEN_PATH_B1_R1_ROOT_CAUSE_REPORT.md` | ✅ |
| B6 | B1-R1 fix evidence 存在 | `CO2_GOLDEN_PATH_B1_R1_FIX_AND_SIM_RERUN.md` | ✅ |
| B7 | CO2 A2 baseline audit 存在 | `CO2_A2_SEVEN_PRESSURE_PROTECTED_BASELINE_AUDIT_AFTER_B1_R1.md` | ✅ |
| B8 | B1 closure evidence 存在 | `B1_EVIDENCE_CLOSURE_AND_NEXT_STAGE_DECISION.md` | ✅ |
| B9 | B2 seed evidence 存在 | `B2_NO_WRITE_STAGE_GATE_AND_EVIDENCE_REGISTRY_SEED.md` | ✅ |
| B10 | B2 evidence registry V1 存在 | `B2_EVIDENCE_REGISTRY_V1.md` | ✅ |
| B11 | B2 acceptance checklist V1 存在 | 本文档 | ✅ |

### C. Physical Meaning Gate

| # | 检查项 | 要求 | 当前状态 |
|---|--------|------|:--:|
| C1 | H2O vent/valve 顺序正确 | vent=OFF → vent_closed_verified → wait 1.5s → set_h2o_path(False) → seal gate | ✅ |
| C2 | H2O sealed 阶段 vent=ON count = 0 | 必须为 0 | ✅ |
| C3 | CO2 sealed pressure control vent=ON count = 0 | 必须为 0（blocked attempts 不计入） | ✅ |
| C4 | CO2 七压力点不含常压点 | `[1100..500]`，不含 ambient | ✅ |
| C5 | B1-R1 6 points simulation 不冒充七压力点 | 明确区分，不混淆 | ✅ |
| C6 | 任何通大气动作不得发生在 sealed pressure control 内 | sealed 内 vent=ON 必须被 BLOCKED 或=0 | ✅ |
| C7 | CO2 物理三阶段存在且 guard 有效 | open conditioning → seal transition → sealed pressure control | ✅ |
| C8 | H2O D29 runtime 补丁已停止 | 不再回水路修补 | ✅ |

### D. No-Write Gate

| # | 检查项 | 要求 | 当前状态 |
|---|--------|------|:--:|
| D1 | no ID write | 无 analyzer ID 写入 | ✅ |
| D2 | no SENCO write | 无 SENCO 写入 | ✅ |
| D3 | no zero write | 无零点校准写入 | ✅ |
| D4 | no span write | 无跨度校准写入 | ✅ |
| D5 | no coefficient write | 无校准系数写入 | ✅ |
| D6 | no calibration parameter write | 无标定参数写入 | ✅ |
| D7 | no chamber SV write | 无温箱 SV 写入 | ✅ |
| D8 | `apply_device_id=false` | A2 probe config 中明确 | ✅ |
| D9 | `collect_only=true` | 全局 workflow config 中明确 | ✅ |
| D10 | `allow_write_coefficients=false` | A2 probe config 中明确 | ✅ |
| D11 | `allow_write_zero=false` | A2 probe config 中明确 | ✅ |
| D12 | `allow_write_span=false` | A2 probe config 中明确 | ✅ |
| D13 | `allow_write_calibration_parameters=false` | A2 probe config 中明确 | ✅ |
| D14 | `senco_write_enabled=false` | A2 probe config 中明确 | ✅ |
| D15 | `calibration_write_enabled=false` | A2 probe config 中明确 | ✅ |
| D16 | `analyzer_id_write_enabled=false` | A2 probe config 中明确 | ✅ |
| D17 | B1-R1 fix 未引入任何写入操作 | c62093c4 审计通过；wrapper 为 no-op | ✅ |

### E. V1 Fallback Gate

| # | 检查项 | 要求 | 当前状态 |
|---|--------|------|:--:|
| E1 | V1 生产代码未修改 | 所有 V1 文件 untouched | ✅ |
| E2 | `run_app.py` 默认入口不切 V2 | 保持 V1 为默认 | ✅ |
| E3 | `disable_v1=false` | A2 probe config 中明确 | ✅ |
| E4 | `default_cutover_to_v2=false` | A2 probe config 中明确 | ✅ |
| E5 | V1 fallback 完整保留 | 任何时刻可回退 | ✅ |

### F. Blocked Higher-Stage Gate

| # | 检查项 | 要求 | 当前状态 |
|---|--------|------|:--:|
| F1 | no controlled write | 禁止 | ✅ |
| F2 | no production acceptance | 禁止 | ✅ |
| F3 | no formal switch | 禁止 | ✅ |
| F4 | no A4/A5/A6/A7/A8/A9 | 未进入 | ✅ |
| F5 | no real A2 probe execution | 除非未来 operator confirmation + CLI unlock + no-write | ✅ |
| F6 | no V2 替代 V1 宣告 | 禁止 | ✅ |

### G. Known Gaps Gate

| # | Gap | 严重等级 | 当前处理 | 状态 |
|---|-----|:--:|------|:--:|
| G1 | `test_a2_no_write_pressure_sweep.py` 114 个预存失败 | P1 | B2-P2 或更高专项设计 | ⚠️ 待处理 |
| G2 | B1-R1 simulation profile 未显式 `no_write_guard_active=true` | P2 | 后续阶段确认是否收紧 | ⚠️ 待确认 |
| G3 | no-write boundary 未逐调用点审计 | P1 | B2-P3+ 执行 | ⚠️ 待处理 |
| G4 | shared service 高风险改动后置 | P0 guard | 持续禁止 | ✅ 已控制 |
| G5 | stash@{0} 继续禁止恢复 | P0 guard | 持续禁止 | ✅ 已控制 |
| G6 | V1/V2 正式对照验收未开始 | P1 | B3+ 启动 | ⚠️ 待启动 |
| G7 | 真实 A2 probe 未执行 | P1 | 未来单独确认 | ⚠️ 待确认 |

---

## 3. B2-P1 Final Decision

**`B2_P1_DOCS_PASS`**

判定依据：

| # | 条件 | 状态 |
|---|------|:--:|
| 1 | Git / Worktree Gate（A1-A6）全部通过 | ✅ |
| 2 | Evidence Completeness Gate（B1-B11）全部通过 | ✅ |
| 3 | Physical Meaning Gate（C1-C8）全部通过 | ✅ |
| 4 | No-Write Gate（D1-D17）全部通过 | ✅ |
| 5 | V1 Fallback Gate（E1-E5）全部通过 | ✅ |
| 6 | Blocked Higher-Stage Gate（F1-F6）全部通过 | ✅ |
| 7 | Known Gaps Gate（G1-G7）中 G4/G5 已控制，G1/G2/G3/G6/G7 明确待后续 | ✅ |
| 8 | 无 runtime/config/test/profile/points 修改 | ✅ |
| 9 | 无 stash 恢复 | ✅ |
| 10 | 无 real COM | ✅ |
| 11 | 无参数写入 | ✅ |

**含义限定**：
> B2-P1 evidence registry V1 和 acceptance checklist V1 已完成。这不表示 B2 阶段结束，不表示可以进入 controlled write / production acceptance / formal switch。G1-G3/G6-G7 仍需后续 B2 子任务处理。

---

*B2 Acceptance Checklist V1 结束。*
