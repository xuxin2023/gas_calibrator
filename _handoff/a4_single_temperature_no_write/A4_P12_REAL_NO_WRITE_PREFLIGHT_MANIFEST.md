# A4-P12 Real No-Write Preflight Manifest

## 1. Scope

这是 real no-write preflight manifest（真机 no-write 预检清单），**不是**：
- real-machine config
- 真实 COM 运行授权
- production acceptance
- controlled write
- formal switch

只列出未来真实 no-write probe 前必须满足的 gate（关卡）。本 manifest 本身不授权任何真实运行。

## 2. Preflight Gates

### A. Git Gate

| 检查项 | 标准 | 状态 |
|--------|------|------|
| branch | `codex/v2-golden-recovery-cdb82111` | ⬜ |
| worktree | clean (no uncommitted changes) | ⬜ |
| exact commit | P12 commit hash verified | ⬜ |
| V1 fallback | `run_app.py` unchanged, `disable_v1=false` | ⬜ |

### B. Config Gate

| 检查项 | 标准 | 状态 |
|--------|------|------|
| real-machine config | NOT CREATED YET — must be created by A4-P13 after user approval | ⬜ |
| simulation config | `a4_single_temp_h2o_co2_no_write_20c_simulated.json` MUST NOT be used as real config | ⬜ |
| user approval | APPROVE_A4_SINGLE_TEMP_H2O_CO2_NO_WRITE_PREFLIGHT phrase entered | ⬜ |

### C. Route Gate

| 检查项 | 标准 | 状态 |
|--------|------|------|
| route mode | h2o_then_co2 | ⬜ |
| H2O first | TemperatureGroupRunner executes H2O before CO2 | ⬜ |
| CO2 second | Co2RouteRunner after H2O cleanup | ⬜ |
| CO2 ambient_open | deferred — not included in this round | ⬜ |

### D. Physical Gate

| 检查项 | 标准 | 状态 |
|--------|------|------|
| H2O ambient open | not sealed, vent=ON | ⬜ |
| 1000 hPa | sealed controlled point, NOT ambient | ⬜ |
| sealed vent=0 | P7 audit confirmed CO2=0, H2O=0 | ⬜ |
| H2O cleanup before CO2 baseline | P9 dynamic test verified | ⬜ |
| CO2 baseline before route open | set_co2_route_baseline before set_valves_for_co2 | ⬜ |
| CO2 preseal before sealed sweep | dewpoint gate before pressurize_and_hold | ⬜ |

### E. No-Write Gate

| 检查项 | 标准 | 状态 |
|--------|------|------|
| no ID write | blocked | ⬜ |
| no SENCO write | blocked | ⬜ |
| no zero write | blocked | ⬜ |
| no span write | blocked | ⬜ |
| no coefficient write | blocked | ⬜ |
| no calibration parameter write | blocked | ⬜ |
| runtime setup events | recorded only, not written | ⬜ |
| attempted_write_count | MUST remain 0 | ⬜ |

### F. Safe-Stop Gate

| 检查项 | 标准 | 状态 |
|--------|------|------|
| abort path | WorkflowInterruptedError → cleanup | ⬜ |
| valve baseline | apply_valve_states([]) | ⬜ |
| vent ON | _set_pressure_controller_vent(True) after stop | ⬜ |
| pressure control stop | PACE/pressure controller safe | ⬜ |
| chamber/humidity safe | temperature/humidity generator safe stop | ⬜ |

### G. User Confirmation Gate

| 检查项 | 标准 | 状态 |
|--------|------|------|
| exact config | approved by operator | ⬜ |
| route | h2o_then_co2 approved | ⬜ |
| temperature | 20℃ approved | ⬜ |
| CO2 ppm | confirmed by user (NOT placeholder 1000) | ⬜ |
| no-write | confirmed | ⬜ |
| V1 fallback | confirmed | ⬜ |
| safe-stop | confirmed | ⬜ |
| explicit approval phrase | entered by user | ⬜ |

## 3. Must Not Proceed If

- ❌ no user approval phrase (APPROVE_A4_SINGLE_TEMP_H2O_CO2_NO_WRITE_PREFLIGHT)
- ❌ CO2 ppm not confirmed by user
- ❌ real-machine config missing
- ❌ V1 fallback unavailable
- ❌ any write gate unclear
- ❌ any sealed vent risk unverified
- ❌ any COM mapping uncertainty
- ❌ any pressure/valve ambiguity
- ❌ simulation profile used as real config

## 4. Next Allowed Stage

A4-P13 may draft real-machine no-write config **only after**:
1. All gates above checked ✓
2. User approval phrase entered
3. CO2 ppm confirmed by user

DO NOT execute real COM. DO NOT execute --execute-probe. DO NOT write parameters.

## 5. Decision

**A4_P12_REAL_PREFLIGHT_MANIFEST_READY**
