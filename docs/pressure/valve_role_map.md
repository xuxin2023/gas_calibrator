# Valve Role Map

当前 V1/OLD_PACE5000 formal flow 采用以下阀位语义。

关键修正：
- `valve 8` 不是 H2O 专用阀，而是当前仓库配置中的总阀门 / 总路阀。
- `h2o_path` 只是 `valve 8` 的遗留配置键名，不代表它只能用于水路。
- CO2 dry route 不允许移除 `valve 8`。

| valve_id | role | legacy_config_key | route_name | meaning | connects_from | connects_to | can_introduce_pressure_source | can_vent_to_atmosphere | can_connect_analyzer | can_connect_pace | required_for_co2_route | required_for_h2o_route | requires_active_atmosphere_flush | risk_amplifier | risk_evidence |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 4 | `co2_source_600ppm_group1` | `co2_map` | `co2_group1_source` | A组 600 ppm 源阀 | group1 600 ppm source manifold | co2_path_group1 | true | false | true | true | true | false | true | false | source valve can introduce upstream gas pressure after route seal |
| 7 | `co2_path_group1` | `co2_path` | `co2_group1` | A组总气路阀 | group1 source selector | gas main | false | false | true | true | true | false | true | false |  |
| 8 | `common_total_valve` | `h2o_path` | `common_total` | 总阀门 / 总路阀 | common route manifold | gas main / analyzer / PACE manifold | false | false | true | true | true | true | true | false | open:8 can expose pressurized/common volume if PACE vent is not synchronized |
| 9 | `hold` | `hold` | `h2o_hold` | 水路保压阀 | humidity branch | hold volume | false | false | false | false | false | true | true | false |  |
| 10 | `flow_switch` | `flow_switch` | `h2o_flow_switch` | 水路切换阀 | humidity branch | selected downstream path | false | false | false | false | false | true | true | false |  |
| 11 | `gas_main` | `gas_main` | `common_gas` | 总气路阀 | selected gas path | analyzer / downstream manifold | false | false | true | true | true | false | true | true | 8|11 amplifies pressure rise |
| 16 | `co2_path_group2` | `co2_path_group2` | `co2_group2` | B组总气路阀 | group2 source selector | gas main | false | false | true | true | true | false | true | false |  |
| 24 | `co2_source_500ppm_group2` | `co2_map_group2` | `co2_group2_source` | B组 500 ppm 源阀 | group2 500 ppm source manifold | co2_path_group2 | true | false | true | true | true | false | true | false | source valve can introduce upstream gas pressure after route seal |

## Route Combinations

- CO2 A group: `8 -> 11 -> 7 -> source`
- CO2 B group: `8 -> 11 -> 16 -> source`
- H2O route: `8 -> 9 -> 10`

## Pressure Interpretation

- `OLD_PACE5000` 上 `VENT?=2` 只能表示本次 fresh vent completed。
- route flush 阶段必须在 route 打开后继续执行受控 fresh vent pulse，确认压力仍接近 ambient。
- `PressureSetpointHold` 阶段禁止 periodic vent refresh，也不能再把 `VENT?=2` 当作持续通大气证据。
