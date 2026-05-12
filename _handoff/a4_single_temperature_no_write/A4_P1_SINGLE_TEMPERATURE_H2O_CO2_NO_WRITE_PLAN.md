# A4-P1 Single-Temperature H2O+CO2 No-Write Plan

**生成时间**: 2026-05-12
**仓库**: `D:/gas_calibrator`
**分支**: `codex/v2-golden-recovery-cdb82111`
**起始 HEAD**: `d3df0a6e`
**角色**: `engineering_probe_only` / `not_real_acceptance_evidence=true` / `promotion_state=blocked`

---

## 1. Scope

A4-P1 是 planning，不是 runtime 修改。单温（single temperature），H2O+CO2 group，no-write，不是真机，不是 production，不是 controlled write，不是 formal switch。

---

## 2. Stage Boundary

A1/A2/A3/B1/B2 已完成。A4 只进入 plan/checklist，不跳 A5/A6/A7/A8/A9。V1 fallback 保留。

上游证据链：
- H2O D29 R4/R5: vent/valve order PASS, sealed vent=0 (034b2d6b / 5bc4fa2c)
- CO2 A2 protected baseline: 51/51 PASS, contracts 47/47 (8966ab6d)
- B2-P4A: no-write guard runtime setup contract (3188aa73)
- B2-P4B: V1/V2 action gap audit 18/18 no gap (3188aa73)
- B2-P6: profile no_write_guard_active=true marker (3c7ef787)

---

## 3. Proposed A4 Flow

按物理流程写，不写代码：

1. **preflight**: Git/stage gate, no-write gate, physical gate, route gate
2. **analyzer runtime setup evidence**: MODE2 init → recorded in runtime_setup_events
3. **H2O route readiness**: valve path open, humidity generator stable, dewpoint check
4. **CO2 route readiness**: valve path open, gas source stable, atmosphere phase
5. **single temperature group**: one temperature point, no multi-temp expansion
6. **ambient/open phase**: atmosphere flush → analyzer reads ambient
7. **preseal vent=OFF**: vent=OFF → vent_closed_verified
8. **wait 1.5s**: post-vent-closed handoff timing
9. **close valve / seal**: set route path False → seal gate
10. **sealed pressure control**: PACE pressurize_and_hold per pressure point
11. **sealed vent=0**: sealed vent=ON count must be 0
12. **sampling**: sample window per pressure point
13. **compute/export only**: coefficient calc → artifact export; no device write
14. **safe stop**: valve restore, vent=ON, PACE stop, chamber stop
15. **V1 fallback remains**: run_app.py unchanged, disable_v1=false

---

## 4. Physical Meaning Constraints

- sealed pressure control 内 vent=ON 必须为 0（CO2=0, H2O=0 已审计）
- H2O 采样后必须先 vent=OFF，确认后等待 1.5s，再关阀封闭（R4/R5 验证）
- CO2 protected baseline 七压力点仍是 [1100, 1000, 900, 800, 700, 600, 500]，不含常压点
- 气路常压点缺口已记录，本轮不补 runtime
- V1 不合理动作不得照搬：sealed 内通大气 / 自动写设备 ID / 无证据参数写入

---

## 5. Architecture Boundary

A4 不把新功能堆入单个大文件。后续修改必须遵守：

| 职责 | 归口 |
|---|---|
| route orchestration | runners/ (co2_route_runner.py, h2o_route_runner.py) |
| device interaction | services/ (pressure_control, valve_routing, sampling, etc.) |
| evidence/export | services/artifact_service.py + services/coefficient_service.py |
| no-write contract | core/no_write_guard.py |
| checklist/profile | configs/validation/ + _handoff/ |
| finalization/safe stop | runners/finalization_runner.py |
| temperature group | runners/temperature_group_runner.py |

本轮不做代码拆分，但后续修改必须遵守边界。

---

## 6. Known Deferred Items

1. P5 114 fixture failures (test_a2_no_write_pressure_sweep.py) — 后置独立治理
2. 气路常压点缺口 — A4 risk item，本轮不补 runtime
3. 真实 A4 no-write probe — 需用户明确确认
4. controlled write 仍禁止
5. production acceptance 仍禁止
6. V1/V2 formal comparison 未开始
7. A5-A9 未进入
8. stash@{0} 恢复仍禁止
9. B1-R1 6 points simulation 不是七压力点，不得混淆

---

## 7. A4-P1 Decision

**A4_P1_PLAN_READY**

判定依据：B2 exit PASS, evidence chain intact, architecture boundaries clear, deferred items explicitly listed, no runtime/modification planned.

---

*结束*
