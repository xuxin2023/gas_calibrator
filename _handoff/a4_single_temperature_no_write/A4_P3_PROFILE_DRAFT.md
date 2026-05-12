# A4-P3 Profile Draft

**生成时间**: 2026-05-12
**仓库**: `D:/gas_calibrator`
**分支**: `codex/v2-golden-recovery-cdb82111`
**起始 HEAD**: `86b288f7`
**角色**: `engineering_probe_only` / `not_real_acceptance_evidence=true` / `promotion_state=blocked`

---

## 1. Scope

A4-P3 是 simulation-only profile draft，不改 runtime，不跑真机，不补气路常压点 runtime，不写参数。

---

## 2. Profile Path

`src/gas_calibrator/v2/configs/validation/simulated/a4_single_temp_h2o_co2_no_write_20c_simulated.json`

---

## 3. Schema Source

| 字段来源 | 参考文件 |
|---|---|
| devices/valves/workflow structure | `replacement_full_route_simulated.json` |
| `no_write_guard_active` + device list | `replacement_skip0_co2_only_simulated.json` |
| H2O route contract (timeout policy) | `run001_h2o_only_1_point_no_write_real_machine.json` |
| A2 no-write guard pattern | `run001_a2_co2_only_7_pressure_no_write_real_machine.json` |

---

## 4. Key Fields

| 字段 | 值 | 来源 |
|---|---|---|
| `features.simulation_mode` | true | simulation-only constraint |
| `workflow.collect_only` | true | A4-P1 plan §3 |
| `workflow.no_write_guard_active` | true | B2-P6 marker convention |
| `workflow.route_mode` | h2o_then_co2 | existing schema (replacement_full_route) |
| `workflow.selected_temps_c` | [20.0] | single-temperature constraint |
| `workflow.humidity_generator.ensure_run` | true | H2O route requires humidity |
| `workflow.a4_notes.gas_ambient_point_gap` | recorded, not addressed | A4-P2 audit #11 |
| `workflow.production.enabled` | false | not production-ready |

---

## 5. H2O+CO2 Group Intent

`route_mode: "h2o_then_co2"` 已在 V2 架构中支持水路由先、气路由后的顺序编排。`selected_temps_c: [20.0]` 确认为单温。

---

## 6. Gas Ambient Gap

气路常压点缺口已在 `a4_notes.gas_ambient_point_gap` 中记录为 "recorded, NOT addressed"。CO2 seven-pressure baseline [1100..500] 保持完整。points_excel 当前复用 `full_route_points_simulated.json`，标记 NEED_USER_DECISION。

---

## 7. Not Modified

V1, runtime, no_write_guard.py, analyzer_fleet_service.py, pressure_control_service.py, valve_routing_service.py, h2o_route_runner, co2_route_runner, A2 seven-pressure config/points, test_a2_no_write_pressure_sweep.py。

---

## 8. Architecture Boundary

Profile 归 `configs/validation/simulated/`，测试归 `tests/v2/`，handoff 归 `_handoff/a4_single_temperature_no_write/`。未将功能堆入单文件。

---

## 9. Profile Draft Decision

**A4_P3_PROFILE_DRAFT_READY**

判定: simulation-only ✓, no_write_guard_active ✓, single temp 20°C ✓, H2O+CO2 group route_mode ✓, gas ambient gap recorded ✓, production not declared ✓, NEED_USER_DECISION points file ✓, no runtime/V1/A2 baseline modified ✓。

---

*结束*
