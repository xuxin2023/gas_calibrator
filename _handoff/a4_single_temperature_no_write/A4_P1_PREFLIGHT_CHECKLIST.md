# A4-P1 Preflight Checklist

**生成时间**: 2026-05-12
**仓库**: `D:/gas_calibrator`
**分支**: `codex/v2-golden-recovery-cdb82111`
**起始 HEAD**: `d3df0a6e`
**角色**: `engineering_probe_only` / `not_real_acceptance_evidence=true` / `promotion_state=blocked`

---

## 1. Git / Stage Gate

| # | 检查项 | 预期 | A4 执行时确认 |
|---|--------|------|:--:|
| G1 | HEAD = d3df0a6e 或更新 | 是 | ⬜ |
| G2 | worktree clean | 是 | ⬜ |
| G3 | staged empty | 是 | ⬜ |
| G4 | stash 未恢复 | 是 | ⬜ |
| G5 | V1 fallback retained (run_app.py unchanged) | 是 | ⬜ |
| G6 | no runtime diff | 是 | ⬜ |
| G7 | no config/profile/points diff beyond plan | 是 | ⬜ |

---

## 2. No-Write Gate

| # | 检查项 | 预期 | A4 执行时确认 |
|---|--------|------|:--:|
| N1 | no ID write (set_device_id/write_device_id/assign_device_id) | 阻断 | ⬜ |
| N2 | no SENCO write | 阻断 | ⬜ |
| N3 | no zero write | 阻断 | ⬜ |
| N4 | no span write | 阻断 | ⬜ |
| N5 | no coefficient write | 阻断 | ⬜ |
| N6 | no calibration parameter write | 阻断 | ⬜ |
| N7 | analyzer runtime setup recorded in runtime_setup_events | 已记录 | ⬜ |
| N8 | attempted_write_count = 0 | 0 | ⬜ |
| N9 | identity_write_command_sent = false | false | ⬜ |
| N10 | persistent_write_command_sent = false | false | ⬜ |
| N11 | profile no_write_guard_active = true | true | ⬜ |
| N12 | collect_only = true | true | ⬜ |

---

## 3. Physical Gate

| # | 检查项 | 预期 | A4 执行时确认 |
|---|--------|------|:--:|
| P1 | H2O vent/valve order: vent=OFF → verified → 1.5s → close valve | 顺序正确 | ⬜ |
| P2 | CO2 sealed vent=ON count = 0 | 0 | ⬜ |
| P3 | H2O sealed vent=ON count = 0 | 0 | ⬜ |
| P4 | pressure readback valid (digital gauge + PACE) | 有效 | ⬜ |
| P5 | sample count > 0 per pressure point | >0 | ⬜ |
| P6 | safe stop: valves restored, vent=ON, PACE stopped | 完成 | ⬜ |
| P7 | no sealed vent ON during pressure control | 0 | ⬜ |
| P8 | CO2 protected baseline 七压力点 intact | [1100..500] | ⬜ |

---

## 4. Route Gate

| # | 检查项 | 预期 | A4 执行时确认 |
|---|--------|------|:--:|
| R1 | H2O route enabled only in A4 plan | 是 | ⬜ |
| R2 | CO2 route enabled only in A4 plan | 是 | ⬜ |
| R3 | single temperature only | 是 | ⬜ |
| R4 | no multi-temperature expansion | 是 | ⬜ |
| R5 | no production pressure sweep expansion | 是 | ⬜ |
| R6 | no gas ambient runtime addition this round | 是 | ⬜ |
| R7 | CO2 route runner intact | 是 | ⬜ |

---

## 5. User Confirmation Gate

未来如需真实 no-write probe，必须用户逐项确认：

| # | 确认项 | 内容 |
|---|--------|------|
| U1 | exact config | 指定使用哪个 validation config JSON |
| U2 | exact route | H2O-only / CO2-only / H2O+CO2 group |
| U3 | exact temperature | 单一温度值 (°C) |
| U4 | no-write | attempted_write_count 预期=0 |
| U5 | no parameter write | 不写 ID/SENCO/zero/span/coefficient/calibration |
| U6 | V1 fallback | run_app.py 不变，disable_v1=false |
| U7 | abort/safe-stop path | 异常时 safe stop 可达 |

---

## 6. Blockers

任一 blocker 触发 = A4 real probe 立即中止：

| # | Blocker | 判定 |
|---|---------|------|
| B1 | user not approved real run | ⛔ BLOCK |
| B2 | unexpected runtime diff | ⛔ BLOCK |
| B3 | any calibration/identity write attempt | ⛔ BLOCK |
| B4 | sealed vent ON during pressure control | ⛔ BLOCK |
| B5 | pressure not stable within timeout | ⛔ BLOCK |
| B6 | sample_count = 0 | ⛔ BLOCK |
| B7 | V1 fallback unavailable | ⛔ BLOCK |
| B8 | no_write_guard_active != true | ⛔ BLOCK |

---

## 7. Checklist Decision

**A4_PREFLIGHT_CHECKLIST_READY**

判定依据：7 类 gate 全覆盖 (Git/NoWrite/Physical/Route/UserConfirmation/Blockers)，与 A4-P1 plan 配套，不假设 runtime 修改，不预设真机执行。

---

*结束*
