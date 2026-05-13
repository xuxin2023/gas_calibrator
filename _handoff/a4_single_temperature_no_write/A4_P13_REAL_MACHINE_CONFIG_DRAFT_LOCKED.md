# A4-P13 Real-Machine Config Draft (Locked)

## 1. scope
起草 A4 real-machine no-write config 草案，保持完全锁定状态。禁止真实 COM、禁止 execute-probe、禁止写参数。等用户输入批准码后进入 A4-P14 解锁。

## 2. 为什么 P13 只是 locked draft
1. 用户尚未输入批准码 `APPROVE_A4_SINGLE_TEMP_H2O_CO2_NO_WRITE_PREFLIGHT`
2. CO2 ppm 尚未确认（当前 placeholder=1000.0）
3. COM 端口映射尚未确认
4. 阀门映射尚未确认
5. 压力控制器 readiness 尚未确认

在以上 5 项全部确认前，config 必须保持 `draft_locked=true`、`operator_approval_phrase_entered=false`。

## 3. config 路径
`src/gas_calibrator/v2/configs/validation/a4_single_temp_h2o_co2_no_write_20c_real_machine_DRAFT_LOCKED.json`

## 4. locked gates

| Gate | 状态 |
|------|------|
| `draft_locked` | true |
| `operator_approval_phrase_entered` | false |
| `execute_probe` | false |
| `real_com_enabled` | false |
| `real_machine_probe_enabled` | false |
| `production_enabled` | false |
| `controlled_write` | false |
| `formal_switch` | false |

## 5. still needing user decisions

| # | 待确认项 | 当前值 |
|---|---------|--------|
| 1 | CO2 ppm | `NEED_USER_DECISION_CO2_PPM` |
| 2 | COM mapping | `NEED_USER_DECISION_COM_MAPPING` |
| 3 | Valve mapping | `NEED_USER_DECISION` |
| 4 | Pressure controller | `NEED_USER_DECISION` |
| 5 | Operator approval phrase | not entered |

## 6. no-write protections

| 保护项 | 值 |
|--------|-----|
| `collect_only` | true |
| `no_write_guard_active` | true |
| `no_write` | true |
| `allow_write_coefficients` | false |
| `allow_write_zero` | false |
| `allow_write_span` | false |
| `allow_write_calibration_parameters` | false |
| `apply_device_id` | false |

## 7. V1 fallback
- `v1_fallback_retained = true`
- `run_app.py` unchanged
- `disable_v1 = false`

## 8. safe-stop
- `valves_baseline_on_abort = true`
- `vent_on_abort = true`
- `pressure_control_stop_on_abort = true`

## 9. CO2 ambient_open still deferred
`workflow.co2_ambient_open = "deferred"`

## 10. 未改 runtime/V1/A2 baseline/A4 simulation profile/points
零修改。simulation config 声明 `simulation_config_must_not_be_used_as_real_config = true`。

## 11. 未跑真实 COM、未写参数
config 本身禁止执行。

## 12. next allowed step
**A4-P14: user-filled approval values validation** — 用户输入 CO2 ppm / COM mapping / valve mapping / pressure controller readiness / 批准码后，验证并解锁。

## 13. decision
**A4_P13_REAL_MACHINE_CONFIG_DRAFT_LOCKED_READY**
