# A4-P12 Operator Confirmation Package

## 1. Scope

这是 operator confirmation package（用户确认包），**不是**：
- 真实运行
- 真机配置 (real-machine config)
- production
- controlled write
- formal switch

A4 当前仍为 simulation-only。任何真实 COM 运行必须用户逐项确认并输入批准码后方可进入 A4-P13 (draft real-machine config)。

## 2. Current A4 Readiness

| Phase | Decision | 说明 |
|-------|----------|------|
| A4-P1 Plan | PASS | 单温 H2O+CO2 plan |
| A4-P2 V1/V2 diff audit | PASS | 19/19 steps mapped |
| A4-P3 B2 context snapshot | PASS | |
| A4-P4 No-write points initial | PASS | |
| A4-P5 15-point matrix | PASS | H2O ambient + 7 sealed, CO2 7 sealed |
| A4-P6 PointParser alignment | PASS | ambient/sealed detection |
| A4-P7 Route-scoped refs | PASS | H2O ambient not leaked to CO2 |
| A4-P8 Static contract | PASS | P9 corrected cleanup doc |
| A4-P9 Dynamic verification | PASS | fake-service transition |
| A4-P10 Simulation smoke | PASS | profile/planner readiness |
| A4-P11 Workflow adapter | PASS | A4ExecutionSummary |

- H2O: 8 points (1 ambient_open + 7 sealed_pressure)
- CO2: 7 points (7 sealed_pressure only)
- Total: 15 points, all 20℃
- no-write / no-real-COM / no-parameter-write 证据齐全

## 3. Operator Must Confirm

| # | 确认项 | 当前值 | 状态 |
|---|--------|--------|------|
| 1 | **Exact config** | `a4_single_temp_h2o_co2_no_write_20c_simulated.json` (simulation-only) | ⬜ PENDING_USER_CONFIRMATION |
| 2 | **Real-machine config** | NOT CREATED — A4-P13 may draft after approval | ⬜ PENDING_USER_CONFIRMATION |
| 3 | **Route** | H2O+CO2 group, h2o_then_co2, H2O first | ⬜ PENDING_USER_CONFIRMATION |
| 4 | **Temperature** | 20.0℃, single temperature only | ⬜ PENDING_USER_CONFIRMATION |
| 5 | **H2O points** | 1 ambient_open + 7 sealed (1100→500 hPa) | ⬜ PENDING_USER_CONFIRMATION |
| 6 | **CO2 points** | 7 sealed only (1100→500 hPa), no ambient_open | ⬜ PENDING_USER_CONFIRMATION |
| 7 | **CO2 ppm** | **NEED_USER_DECISION_CO2_PPM** — current placeholder=1000.0 ppm | ⬜ PENDING_USER_CONFIRMATION |
| 8 | **no-write** | collect_only=true, no_write_guard_active=true, attempted_write_count=0 | ⬜ PENDING_USER_CONFIRMATION |
| 9 | **No parameter write** | no ID / no SENCO / no zero / no span / no coefficient / no calibration | ⬜ PENDING_USER_CONFIRMATION |
| 10 | **V1 fallback** | run_app.py unchanged, disable_v1=false, V1 remains production fallback | ⬜ PENDING_USER_CONFIRMATION |
| 11 | **Abort/safe-stop** | valves baseline/off, vent=ON, PACE safe, chamber/humidity safe, no sealed vent ON | ⬜ PENDING_USER_CONFIRMATION |

## 4. Explicit Approval Phrase

以下批准码必须由用户输入，不得由他人代签：

```
批准码: APPROVE_A4_SINGLE_TEMP_H2O_CO2_NO_WRITE_PREFLIGHT
签名:   ____________________
日期:   ____________________
备注:   ____________________
```

**未输入批准码 = 禁止进入 A4-P13 real-machine no-write config 起草。**

## 5. Deferred Items

| # | 延期项 | 说明 |
|---|--------|------|
| 1 | CO2 ambient_open runtime gap | 不在本轮范围，标记 deferred |
| 2 | P5 test_a2_no_write_pressure_sweep fixture debt | 已后置 |
| 3 | Real-machine config | NOT CREATED |
| 4 | Real COM | NOT APPROVED |
| 5 | A5-A9 (multi-temp / full group) | NOT ENTERED |
| 6 | Production acceptance | FORBIDDEN |
| 7 | Formal switch to V2 | FORBIDDEN |

## 6. Decision

**A4_P12_OPERATOR_CONFIRMATION_PACKAGE_READY**
