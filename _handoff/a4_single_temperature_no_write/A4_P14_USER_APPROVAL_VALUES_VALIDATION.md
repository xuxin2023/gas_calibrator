# A4-P14 User Approval Values Validation (Locked)

## 1. scope
用户已明确确认 A4 单温 H2O+CO2 no-write preflight 关键值。本轮将确认值写入 `operator_decisions`，config 仍保持 locked 和 non-executable。

## 2. 用户已确认值

| # | 项目 | 确认值 |
|---|------|--------|
| 1 | CO2 标气浓度 | 1000.0 ppm |
| 2 | 温度 | 20℃ |
| 3 | 路线 | h2o_then_co2 |
| 4 | H2O 点位 | ambient_open + 7 sealed pressure |
| 5 | CO2 点位 | 7 sealed pressure only |
| 6 | no-write | 不写 ID/SENCO/zero/span/coefficient/calibration |
| 7 | V1 fallback | 保留 |
| 8 | safe-stop | 确认 |
| 9 | 批准码 | APPROVE_A4_SINGLE_TEMP_H2O_CO2_NO_WRITE_PREFLIGHT |

## 3. CO2 ppm：NEED_USER_DECISION → 1000.0 confirmed

`operator_decisions.co2_ppm_confirmed = true`
`operator_decisions.co2_ppm_value = 1000.0`

## 4. route / temperature / points / no-write / V1 fallback / safe-stop

全部 `*_confirmed = true`。

## 5. Analyzer COM mapping

已按已知冻结映射写入 `operator_decisions.device_ports.analyzer_ports`：

| port | device_id |
|------|-----------|
| COM35 | ID001 |
| COM37 | ID029 |
| COM41 | ID003 |
| COM42 | ID004 |

`analyzer_mode = MODE2`, `active_send = true`, `duplicate_device_id = false`

## 6. 其他设备 COM 映射（仓库 evidence）

| 设备 | COM | 置信度 |
|------|-----|--------|
| humidity_generator | COM16 | REPO_EVIDENCE_A2_REAL_RUN_VERIFIED |
| dewpoint_meter | COM17 | REPO_EVIDENCE_A2_REAL_RUN_VERIFIED |
| thermometer | COM18 | REPO_EVIDENCE_A2_REAL_RUN_VERIFIED |
| temperature_chamber | COM19 | REPO_EVIDENCE_A2_REAL_RUN_VERIFIED |
| relay | COM20 | REPO_EVIDENCE_A2_REAL_RUN_VERIFIED |
| relay_8 | COM21 | REPO_EVIDENCE_A2_REAL_RUN_VERIFIED |
| pressure_gauge | COM22 | REPO_EVIDENCE_A2_REAL_RUN_VERIFIED |
| pressure_controller | COM23 | REPO_EVIDENCE_A2_REAL_RUN_VERIFIED |

证据源：`run001_a2_co2_only_7_pressure_no_write_real_machine.json` (a2_real_probe_config_ports_after_mapping) + `run001_h2o_only_1_point_no_write_real_machine.json` (devices)
Advantech COM 曾从 COM24-31 → COM16-23 重映射 (delta=-8)。

## 7. Pressure controller readiness

`pressure_controller_ready = "CANDIDATE_CONFIRMED_BY_REPO_EVIDENCE"` — A2 + H2O real probes 均确认 COM23。

## 8. Valve mapping

`valve_mapping_confirmed = false`, `valve_mapping_status = "NEED_REPO_EVIDENCE_OR_OPERATOR_CONFIRMATION"` — relay port 已知 (COM20/COM21) 但阀门编号映射仍需确认。

## 9. Config 仍 locked / non-executable

- `draft_locked = true`
- `execute_probe = false`
- `real_com_enabled = false`
- `real_machine_probe_enabled = false`
- `production_enabled = false`
- `controlled_write = false`
- `formal_switch = false`
- all `allow_write_* = false`

## 10. 不跑真实 COM、不写参数

## 11. 不改 runtime / V1 / A2 baseline / A4 simulation profile/points

## 12. next allowed step

**A4-P15: repo-backed COM/valve/pressure readiness closure** — 当 valve mapping 等剩余待确认项关闭后，进入 A4-P16 unlock + real no-write probe preparation。

## 13. decision

**A4_P14_USER_VALUES_VALIDATED_LOCKED**
