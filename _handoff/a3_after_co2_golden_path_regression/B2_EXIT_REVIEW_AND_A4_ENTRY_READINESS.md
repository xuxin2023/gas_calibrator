# B2 Exit Review and A4 Entry Readiness

**生成时间**: 2026-05-12  
**仓库**: `D:/gas_calibrator`  
**分支**: `codex/v2-golden-recovery-cdb82111`  
**起始 HEAD**: `3c7ef787`  
**角色**: `engineering_probe_only` / `not_real_acceptance_evidence=true` / `promotion_state=blocked`

---

## 1. Scope

本轮是 B2 exit review + A4 entry readiness，不是 runtime 修改，不是真实 COM，不是 production acceptance，不是 controlled write，不是 formal switch，不是 V2 替代 V1 宣告。

---

## 2. Current Stage

当前处于 B2 exit review。A1/A2/A3 已完成相应 no-write 工程证据；B1/B2 已完成 evidence registry、acceptance checklist、no-write boundary callsite audit、analyzer setup write classification、B2-P4A guard contract、B2-P4B V1/V2 gap audit、B2-P6 profile marker。下一阶段只允许进入 A4 准入准备，不能直接跑 production。

---

## 3. B2 Evidence Summary

| Evidence | Commit | Decision | Meaning | Not Meaning |
|---|---|---|---|---|
| B2 Evidence Registry V1 | `02972ea5` | PASS | 9 条证据正式登记、H2O R4/R5 PASS、CO2 protected baseline intact | 非 production acceptance、非 controlled write |
| B2 Acceptance Checklist V1 | `02972ea5` | B2_P1_DOCS_PASS | 全部 6 类 gate（Git/Evidence/Physical/NoWrite/V1Fallback/BlockedHigher）通过 | 非 B2 阶段结束、非 formal switch |
| B2 No-Write Boundary Callsite Audit V1 | `0885a1a7` | PASS | 7 类逐调用点审计；Cal/Identity 全部覆盖；Category C gap 已识别 | 非 runtime 修改、非 guard 部署 |
| B2 Analyzer Setup Write Classification V1 | `0885a1a7` | PASS | 7 条 analyzer 命令全量分类；runtime setup 建议合同已设计 | 非 guard 代码修改 |
| B2-P4A Guard Runtime Setup Contract | `3188aa73` | B2_P4A_NO_WRITE_GUARD_CONTRACT_PASS | RUNTIME_SETUP_METHODS 固化；identity/cal write 仍阻断；runtime setup 允许且记录 | 非生产 write、非 controlled write |
| B2-P4B V1/V2 Required Action Gap Audit | `3188aa73` | PASS | 18 项 V1 必要动作 100% 无缺口；V1 不合理动作拒绝照搬 | 非 runtime 修改、非 V2 替代 V1 |
| B2-P6 Profile No-Write Marker | `3c7ef787` | B2_P6_PROFILE_MARKER_PASS | simulation profile 显式 `no_write_guard_active: true`；marker test 1 PASS | 非七压力点、非 runtime 修改 |
| B2 Test Fixture & Profile Gap Plan V1 | `0885a1a7` | PASS（审计文档） | 114 P5 失败根因分析；profile 显式性 gap 分析 | 非测试修复、非 profile 修改 |

---

## 4. B2 Exit Gate

| Gate | 状态 |
|---|---|
| no calibration/identity/persistent write protected | ✅ |
| analyzer runtime setup recorded (B2-P4A contract) | ✅ |
| profile explicit `no_write_guard_active: true` (B2-P6) | ✅ |
| V1 fallback retained (`run_app.py` 未修改、`disable_v1=false`) | ✅ |
| no controlled write | ✅ |
| no production acceptance | ✅ |
| no formal switch | ✅ |
| no runtime changes in P6 | ✅ |
| no config/profile/points changes beyond P6 marker | ✅ |
| P5 114 fixture debt deferred, not blocking | ✅ |
| 3 test files all PASS (37/37) | ✅ |

**B2_EXIT_DECISION = PASS_WITH_DEFERRED_TEST_FIXTURE_DEBT**

---

## 5. Deferred Items

1. **P5 test_a2_no_write_pressure_sweep.py 114 fixture failures**：后置独立治理，不阻塞 B2 exit。根因已确认（~70 conditioning_service + ~35 a2_hooks mock 缺口），见 B2_TEST_FIXTURE_AND_PROFILE_GAP_PLAN_V1.md。

2. **V2 气路常压点缺口**：当前 CO2 七压力点 baseline 为 `[1100,1000,900,800,700,600,500]`，不含常压点。已记录为 A4 风险项，本轮不加。

3. **A2/A4 真实 no-write probe 仍需用户另行确认**：当前 simulation-only gate 不构成 real COM 授权。

4. **controlled write 仍禁止**：B2 exit 不放开 controlled write gate。

5. **V1/V2 正式对照验收尚未开始**：需 A4+ 专项启动。

6. **A5/A6/A7/A8/A9 未进入**：当前阶段只允许 A4 准入准备 planning，不可跨阶段。

7. **stash@{0} 恢复仍禁止**：包含高风险 flow_lpm removal，需独立专项。

---

## 6. A4 Entry Readiness

**A4 目标**（只能写成）：
> single-temperature H2O+CO2 no-write group readiness/planning

不能写成 production、controlled write、formal acceptance。

### A4 Entry Preconditions

| Precondition | 状态 |
|---|---|
| B2 exit PASS | ✅ |
| H2O R4/R5 physical order PASS (vent/valve/sealed vent=0) | ✅ |
| CO2 protected baseline intact (A2 51/51, contracts 47/47) | ✅ |
| no-write guard contract in place (B2-P4A) | ✅ |
| profile marker in place (B2-P6) | ✅ |
| V1 fallback retained | ✅ |
| user explicitly approves any real no-write probe | ⚠️ 待确认 |
| no parameter write | ✅ |

### A4 Allowed Actions

- 编写 A4 no-write plan
- 编写 A4 preflight checklist
- 离线测试
- 不跑真实 COM
- 不写参数

### A4 Forbidden Actions

- production run
- controlled write
- formal switch
- restore stash
- 修改 V1
- 跳过 user confirmation
- 补气路常压点 runtime
- 多温/多标气/七压力生产流程

---

## 7. Physical Meaning Notes

- sealed pressure control 内 vent=ON 必须为 0（CO2=0, H2O=0 已审计）
- H2O 采样后必须先 vent=OFF、确认后等待 1.5s，再关阀封闭（D29 R4/R5 已验证）
- CO2 七压力点 protected baseline 仍是 `[1100,1000,900,800,700,600,500]`，不含常压点
- 气路常压点缺口已记录，不在本轮补
- V1 不合理动作不得照搬（sealed 内通大气、自动写设备 ID、无证据参数写入）

---

## 8. Test Results

| Test File | Result |
|---|---|
| `tests/v2/test_simulation_profile_no_write_marker.py` | 1/1 PASS |
| `tests/v2/test_no_write_guard.py` | 15/15 PASS |
| `tests/v2/test_analyzer_fleet_service.py` | 21/21 PASS |
| **总计** | **37/37 PASS** |

`test_a2_no_write_pressure_sweep.py` 未作为 B2 exit gate 执行。其 114 个失败是 P5 独立 fixture debt，不阻塞主线。

---

## 9. Final Decision

**B2_EXIT_PASS_A4_ENTRY_READY_FOR_PLANNING**

判定依据：
- 8 份 B2 证据文件全部存在且可读
- 6 类 gate 全部通过
- 3 条测试线全部 PASS（37/37）
- diff 只新增 1 个 handoff 文档
- 无 runtime/config/profile/points/test/V1 修改
- 无真实 COM / 无参数写入
- P5 fixture debt 已后置记录

---

## 10. 气路常压点缺口

**已记录但不修改**。当前 CO2 A2 protected baseline 七压力点 `[1100..500]` 不含常压点；V2 气路在常压点识别上尚需与 V1 对齐。此为 A4 风险项，需在 A4 planning 中评估优先级。

*B2 Exit Review and A4 Entry Readiness 结束。*
