# A4-P16 Real No-Write Preflight Ready (Not Executable)

## 1. scope
A4-P15 已关闭所有待确认项。P16 生成"预检就绪"real-machine config，draft unlocked 但所有执行 gate 仍 OFF。P17 才允许实际执行。

## 2. why P16
P15 关闭 valve mapping 后，所有映射已完整。需要一笔可见的"软件侧 preflight 完成"记录，明确区分"配置就绪"和"允许执行"。

## 3. config path
`src/gas_calibrator/v2/configs/validation/a4_single_temp_h2o_co2_no_write_20c_real_machine_PREFLIGHT_READY.json`

## 4. what changed from locked draft

| 属性 | DRAFT_LOCKED (P15) | PREFLIGHT_READY (P16) |
|------|-------------------|----------------------|
| `draft_locked` | true | **false** |
| `preflight_ready` | 无 | **true** |
| `profile_stage` | A4_P15 | A4_P16 |
| 设备块 | 仅在 operator_decisions | **完整 devices 块** |
| 阀门块 | 仅在 operator_decisions | **完整 valves 块** |
| 命令预览 | 无 | **preflight_command_preview** |
| 解锁要求 | 无 | **real_run_unlock_requirements** |

## 5. still not executable

| Gate | 状态 |
|------|------|
| `execute_probe` | **false** |
| `real_com_enabled` | **false** |
| `real_machine_probe_enabled` | **false** |
| `production_enabled` | **false** |
| `controlled_write` | **false** |
| `formal_switch` | **false** |

## 6. all confirmed values (summary)

- CO2 ppm: 1000.0 ✅
- Temperature: 20℃ ✅
- Route: h2o_then_co2 ✅
- Points: H2O 1+7, CO2 0+7 ✅
- No-write: all parameters blocked ✅
- V1 fallback: retained ✅
- Safe-stop: all flags true ✅
- Approval phrase: entered ✅

## 7. COM mapping (all confirmed)

| 设备 | COM | baud |
|------|-----|------|
| ga01/ID001 | COM35 | 115200 |
| ga02/ID029 | COM37 | 115200 |
| ga03/ID003 | COM41 | 115200 |
| ga04/ID004 | COM42 | 115200 |
| humidity_generator | COM16 | 9600 |
| dewpoint_meter | COM17 | 9600 |
| thermometer | COM18 | 2400 |
| temperature_chamber | COM19 | 9600 |
| relay | COM20 | 38400 |
| relay_8 | COM21 | 38400 |
| pressure_gauge | COM22 | 9600 |
| pressure_controller | COM23 | 9600 |

## 8. valve mapping (fully repo-backed)

CO2 1000ppm: co2_map[1000]=6 → relay channel 12
H2O path: logical 8 → relay_8 channel 8

## 9. no-write protections

`collect_only=true`, `no_write_guard_active=true`, all `allow_write_*=false`, `apply_device_id=false`

## 10. command preview status

`command_type = "preview_only"` — CLI entry point marked `NEED_CLI_ENTRYPOINT_REVIEW`
`execute_allowed_in_this_commit = false`

## 11. A4-P17 required for actual execution

必须：operator physically present、emergency stop available、V1 fallback verified、all devices powered、pressure/gas/water line checked。

## 12. V1 fallback

`v1_fallback_retained = true`，`run_app.py` unchanged。

## 13. safe-stop

`valves_baseline_on_abort=true`, `vent_on_abort=true`, `pressure_control_stop_on_abort=true`

## 14. CO2 ambient_open still deferred

## 15. tests

`test_a4_real_machine_preflight_ready_config.py`: 35 tests covering stage, locking, no-write, confirmed values, devices, valves, command preview, unlock requirements, payload safety

## 16. decision

**A4_P16_PREFLIGHT_READY_CONFIG_PASS**
