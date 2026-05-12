# A4-P8 H2O→CO2 Transition Smoothness Contract

## 1. scope
离线静态验证 A4 20℃ H2O+CO2 no-write simulation profile 中"水路→气路"切换动作顺序、
cleanup contract、baseline/vent/valve/pressure 物理约束的正确性。不跑真机、不写参数。

## 2. H2O→CO2 transition sequence（TemperatureGroupRunner）
```
route_sequence = ["h2o", "co2"]  ← route_mode=h2o_then_co2
1. H2oRouteRunner.execute(h2o_group, h2o_pressure_points)
2. → 恢复 temperature_group route_context
3. Co2RouteRunner.execute(co2_source, co2_pressure_points)
```
> 源码：TemperatureGroupRunner.execute 遍历 route_sequence，先 for route_name=="h2o" 后 route_name=="co2"。

## 3. 水路结束动作
```
H2oRouteRunner.execute 结束时：
- _stop_h2o_vent_keepalive()        # finally 块，确保 vent keepalive 线程终止
- route_context.clear()
# 注意：正常成功路径不调用 cleanup_h2o_route；
# cleanup_h2o_route 只在异常路径调用（chamber timeout / humidity timeout / route timeout / dewpoint timeout / pressure-seal failure）
```
cleanup_h2o_route（ValveRoutingService）:
- `_set_pressure_controller_vent(True)`     → vent ON
- `apply_valve_states([])`                   → 所有阀门 off
- `record_route_trace(action="cleanup", route="h2o")`
- `_set_preseal_dewpoint_snapshot(None)`

## 4. 气路开始动作
```
Co2RouteRunner.execute 开始时：
1. set_temperature_for_point(point, phase="co2")    → 温度 stability
2. set_co2_route_baseline()                          → apply_valve_states([]) + vent ON
3. "Pressure controller kept at atmosphere for CO2 route conditioning"
4. set_valves_for_co2(point)                         → 打开 CO2 path + source valve
5. _wait_route_soak_before_seal(point)               → 路 soak
6. pressurize_and_hold(point, route="co2")           → 封路控压
7. sealed pressure sweep (500..1100 hPa, 7 点)
```

## 5. vent/valve/pressure physical constraints
- H2O ambient_open 期间 vent=ON（seal_deferred=True）
- H2O ambient→sealed transition：_stop_h2o_vent_keepalive() → pressurize_and_hold(prefer_direct_vent_close=True)
- H2O cleanup：vent ON + 阀门全部 off
- CO2 baseline：vent ON + 阀门全部 off（大气压 conditioning）
- CO2 set_valves：打开 CO2 path + hold + h2o_path（gas route）→ vent OFF → seal
- CO2 sealed pressure sweep：vent=0 硬约束

## 6. route-scoped pressure refs 结果
- H2O pressure_points = ambient_open(1点) + 7 sealed (500..1100 hPa)
- CO2 pressure_points = 7 sealed only (500..1100 hPa)，0 ambient
- 自动检测：RoutePlanner 在 H2O ambient + CO2 共存时启用 route-scoped 过滤

## 7. CO2 ambient_open 仍 deferred
A4 profile notes 明确记录：
> "co2_ambient_open_point": "not included in this round; deferred risk item"

## 8. 是否发现水路到气路切换风险
无。静态 contract 检查确认：
- TemperatureGroupRunner 先 H2O 后 CO2
- H2O runner has _stop_h2o_vent_keepalive in finally
- CO2 runner has set_co2_route_baseline before set_valves_for_co2
- cleanup_h2o_route sets vent ON + empty valve states
- set_co2_route_baseline sets vent ON + empty valve states

## 9. 是否做代码 trace 补充
无。现有 trace action 已覆盖全部切换关键动作（h2o_ambient_sample_complete, h2o_seal_transition_start, h2o_vent_keepalive_stopped, cleanup, route_baseline, set_co2_valves, wait_route_soak, pressure_skip 等）。不需要补 trace。

## 10. 未改 V1/A2 baseline/no_write/pressure service
未修改任何 V1 代码、PointParser、RoutePlanner、pressure_selection、no_write_guard、analyzer_fleet_service、pressure_control_service、A2 baseline。

## 11. 测试结果
```
tests/v2/test_a4_h2o_to_co2_transition_contract.py ... 24 passed
tests/v2/test_a4_single_temp_profile.py ......... passed
tests/v2/test_simulation_profile_no_write_marker.py .. passed
tests/v2/test_no_write_guard.py ......... passed
tests/v2/test_analyzer_fleet_service.py ........ passed
tests/v2/test_route_planner.py ......... passed
tests/v2/test_temperature_group_runner.py ......... passed
tests/v2/test_compare_v1_v2_control_flow.py .. passed
────────────────────────────────────────────────────
Total: 134 passed, 0 failed
```

## 12. decision
**A4_P8_TRANSITION_CONTRACT_PASS**
