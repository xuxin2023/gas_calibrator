# A4-P15 Repo-Backed Valve Mapping Closure (Locked)

## 1. scope
A4-P14 已写入用户确认值但 valve_mapping 仍未关闭。本轮从仓库已有的 Run-001/A2 real-machine config 中抽取完整阀门映射，关闭 valve mapping 待确认项。config 仍 locked、non-executable。

## 2. why P15 needed
A4-P14 标注 `valve_mapping_status: NEED_REPO_EVIDENCE_OR_OPERATOR_CONFIRMATION` — 仓库已有 A2 real config（`run001_a2_co2_only_7_pressure_no_write_real_machine.json`）包含完整 `valves` 配置，可直接抽取。

## 3. repo evidence source
`src/gas_calibrator/v2/configs/validation/run001_a2_co2_only_7_pressure_no_write_real_machine.json`
交叉核对：`valve_routing_service.py` 使用相同 logical valve ID 读取 `valves.*` 配置。

## 4. relay ports

| device | COM |
|--------|-----|
| relay (relay_a) | COM20 |
| relay_8 (relay_b) | COM21 |

## 5. logical valves

| name | logical ID |
|------|-----------|
| co2_path | 7 |
| co2_path_group2 | 16 |
| gas_main | 11 |
| h2o_path | 8 |
| flow_switch | 10 |
| hold | 9 |

## 6. relay_map (key logical → physical)

| logical | device | channel |
|---------|--------|---------|
| 1 | relay | 7 |
| 2 | relay | 8 |
| 3 | relay | 9 |
| 4 | relay | 10 |
| 5 | relay | 11 |
| 6 | relay | 12 |
| 7 | relay | 15 |
| 8 | relay_8 | 8 |
| 9 | relay_8 | 1 |
| 10 | relay_8 | 2 |
| 11 | relay_8 | 3 |
| 16 | relay | 16 |
| 21 | relay | 6 |
| 22 | relay | 5 |
| 23 | relay | 4 |
| 24 | relay | 3 |
| 25 | relay | 2 |
| 26 | relay | 1 |

## 7. co2_map / co2_map_group2

`co2_map`: 0→1, 200→2, 400→3, 600→4, 800→5, 1000→6

`co2_map_group2`: 0→21, 100→22, 300→23, 500→24, 700→25, 900→26

## 8. CO2 1000 ppm valve path

`co2_map["1000"] = 6` → `relay_map["6"] = relay channel 12`

## 9. H2O path valve path

`logical_valves["h2o_path"] = 8` → `relay_map["8"] = relay_8 channel 8`

## 10. config remains locked / non-executable

- `draft_locked = true`
- `execute_probe = false`
- `real_com_enabled = false`
- `real_machine_probe_enabled = false`
- all `allow_write_* = false`

## 11. no runtime / V1 / A2 baseline / A4 simulation changes

零修改。

## 12. no real COM / no write

## 13. tests

- `test_a4_valve_mapping_repo_backed.py`: 25 tests — valve mapping completeness
- `test_a4_real_machine_config_draft_locked.py`: updated valve test
- `test_a4_user_filled_approval_values.py`: unchanged (valve not in scope)

## 14. decision

**A4_P15_VALVE_MAPPING_CLOSED_LOCKED**
