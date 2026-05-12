# A4-P2 Offline Route/Profile Readiness

**生成时间**: 2026-05-12
**仓库**: `D:/gas_calibrator`
**分支**: `codex/v2-golden-recovery-cdb82111`
**起始 HEAD**: `a3eb8e58`
**角色**: `engineering_probe_only` / `not_real_acceptance_evidence=true` / `promotion_state=blocked`

---

## 1. Scope

A4-P2 是 offline readiness audit，不改代码，不跑真机，不创建 runtime profile，不补气路常压点，不进入 production。

---

## 2. Current Inputs

| Input | Source | Status |
|---|---|---|
| A4-P1 plan | `A4_P1_SINGLE_TEMPERATURE_H2O_CO2_NO_WRITE_PLAN.md` | READY |
| A4-P1 checklist | `A4_P1_PREFLIGHT_CHECKLIST.md` | READY |
| B2 exit review | `B2_EXIT_REVIEW_AND_A4_ENTRY_READINESS.md` | PASS |
| H2O R4/R5 evidence | `d29_h2o_vent_order_*` | PASS |
| CO2 A2 protected baseline | `CO2_A2_SEVEN_PRESSURE_PROTECTED_BASELINE_AUDIT_AFTER_B1_R1.md` | PASS (51/51) |
| V1/V2 gap audit | `B2_P4B_V1_V2_REQUIRED_ACTION_GAP_AUDIT.md` | PASS (18/18) |

---

## 3. Existing V2 Capability Map

| Capability | V2 Owner | Status | A4 Relevance | Risk |
|---|---|---|---|---|
| temperature group | `runners/temperature_group_runner.py` | ✅ 已实现 | 单温编组调度 | P2 (config flag 阻断多温可行) |
| H2O route | `runners/h2o_route_runner.py` | ✅ 已实现 | 水路全流程 | P1 (需确认 A4 config scope) |
| CO2 route | `runners/co2_route_runner.py` | ✅ 已实现 | 气路全流程 | P1 (需确认常压点处理) |
| conditioning/preseal | `services/conditioning_service.py` | ✅ 已实现 | 封闭前过渡 | P2 (B1-R1 已修复) |
| pressure control | `services/pressure_control_service.py` | ✅ 已实现 | 封闭控压 | P0 (shared, 不改) |
| valve routing | `services/valve_routing_service.py` | ✅ 已实现 | 阀切换 | P0 (shared, 不改) |
| sampling | `services/sampling_service.py` | ✅ 已实现 | 样本采集 | P2 (稳定计数/闸门) |
| no-write guard | `core/no_write_guard.py` | ✅ 已实现 (B2-P4A) | 写入阻断+记录 | P0 (contract 已固化) |
| artifact/export | `services/artifact_service.py` | ✅ 已实现 | 工件输出 | P2 (compute-only) |
| safe stop | `runners/finalization_runner.py` | ✅ 已实现 | 复位+安全停止 | P0 (必须可达) |
| V1 fallback | `run_app.py` unchanged | ✅ 保留 | 回退路径 | P0 (不能破坏) |

---

## 4. A4 Profile Readiness

### Existing Profiles

| Profile | Type | A4-Ready? |
|---|---|---|
| `simulated/replacement_skip0_co2_only_simulated.json` | Simulation (CO2 only) | ❌ (sim only, 无 H2O) |
| `run001_a2_co2_only_7_pressure_no_write_real_machine.json` | Real-machine (CO2 only, 7 pressure) | ❌ (CO2 only, 无 H2O, 含多压) |
| `run001_h2o_only_1_point_no_write_real_machine.json` | Real-machine (H2O only) | ❌ (H2O only, 无 CO2) |
| `simulated/replacement_full_route_simulated.json` | Simulation (full route) | ❌ (sim only, 多温) |
| **A4 single-temp H2O+CO2 group profile** | **NOT YET CREATED** | ❌ |

### Decision

**A4 profile not yet created.** 后续 A4-P3 可做 profile draft (H2O+CO2 single-temperature group, no-write, simulation-only)，但仍不跑真机。

---

## 5. Known Gaps

1. **气路常压点缺口**: 已记录，不在本轮补。CO2 baseline [1100..500] 不含常压点
2. **A4 H2O+CO2 group profile 尚未确认/创建**: 需 A4-P3 起草
3. **真实 A4 no-write probe 需用户确认**: User Confirmation Gate 已定义
4. **P5 test fixture debt**: 后置独立治理
5. **controlled write**: 仍禁止
6. **production acceptance**: 仍禁止
7. **A5-A9**: 未进入
8. **B1-R1 6-points simulation != 七压力点**: 不得混淆

---

## 6. Architecture Boundary

不将 A4 新流程堆到单个大文件。持续遵守：

| 职责 | 归口 |
|---|---|
| profile/config | `configs/validation/` |
| flow orchestration | `runners/` (co2/h2o/temperature_group/finalization) |
| device IO | `services/` (pressure/valve/sampling/humidity/temperature) |
| evidence/artifact | `services/artifact_service.py` + `services/coefficient_service.py` |
| no-write guard | `core/no_write_guard.py` |
| safe stop | `runners/finalization_runner.py` |

---

## 7. Decision

**A4_P2_OFFLINE_READINESS_PASS**

判定: 11 项核心 capability 全部就绪, architecture boundary 清晰, gaps 已登记, 不预设真机。A4 profile 待 A4-P3 起草。

---

*结束*
