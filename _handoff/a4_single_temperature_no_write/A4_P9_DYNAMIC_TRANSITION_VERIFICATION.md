# A4-P9 Dynamic Transition Verification

## 1. scope
修正 A4-P8 handoff 中 cleanup 正常路径表述错误，并新增轻量 fake-service 动态测试验证 H2O→CO2 完整调用序列。

## 2. 为什么需要 P9
P8 静态 contract handoff 第 3 节写"正常成功路径不调用 cleanup_h2o_route；cleanup_h2o_route 只在异常路径调用"。实际代码 h2o_route_runner.py 在正常 H2O group 完成后会调用 `cleanup_h2o_route(lead, reason="after H2O group complete")`（pressure loop 之后、return RouteRunResult 之前）。必须修正文档并通过动态测试验证完整序列。

## 3. 修正内容

**P8 handoff 第 3 节原表述：**
> 注意：正常成功路径不调用 cleanup_h2o_route；
> cleanup_h2o_route 只在异常路径调用

**已修正为：**
> cleanup_h2o_route(lead, reason="after H2O group complete")  # 正常成功路径也调用
> 当前 h2o_route_runner.py 正常 H2O group 完成后会调用 cleanup_h2o_route；异常路径也会 cleanup

## 4. dynamic/fake transition 结果

TemperatureGroupRunner.execute() 动态序列：
```
h2o_execute_start
  → h2o_cleanup:after H2O group complete
h2o_execute_end
co2_execute_start
  → co2_baseline:before CO2 route conditioning
  → co2_set_valves
  → co2_sealed_sweep
```

测试 `test_h2o_cleanup_order_in_tgr_for_h2o_co2` 用 monkeypatch 替换 H2O/CO2 runner 为 Recording 版本，通过 `calls.index()` 严格验证顺序。

## 5. V1/V2 transition equivalence 判断

A4-P2 audit 19 步全部在 V2 有对应实现。9 key steps 测试确认：
- H2O open/ambient → h2o_route_runner vent ON + path open
- H2O vent=OFF → pressure_control_service prefer_direct_vent_close=True
- H2O close valve → valve_routing_service cleanup (vent ON + empty valves)
- CO2 valve select → set_valves_for_co2 + co2_open_valves
- CO2 flush/stability → atmosphere conditioning + route soak
- CO2 preseal vent=OFF → dewpoint gate before pressurize_and_hold
- sealed pressure control → 7 sealed points (500..1100 hPa)
- sealed vent=0 → audit confirmed CO2=0, H2O=0
- safe stop → finalization_runner valve restore + vent ON

## 6. V2 不照搬 V1 不合理动作
- 自动写设备 ID → V2 双重阻断
- legacy PACE vent3 状态机 → V2 简化
- sealed pressure control 内通大气 → V2 sealed vent=0

## 7. CO2 ambient_open 仍 deferred
A4 profile notes 明确记录 CO2 ambient_open 不在本轮范围。测试确认 transition 序列中不含任何 CO2 ambient。

## 8. 未改 runtime/V1/A2 baseline/A4 profile/points
零 runtime/V1 修改。

## 9. 未跑真实 COM、未写参数
simulation-only，no-write。

## 10. 测试结果
```
tests/v2/test_a4_h2o_to_co2_transition_contract.py ... 30 passed  (P8:24 + P9:6)
tests/v2/test_a4_single_temp_profile.py ......... passed
tests/v2/test_simulation_profile_no_write_marker.py .. passed
tests/v2/test_no_write_guard.py ......... passed
tests/v2/test_analyzer_fleet_service.py ........ passed
tests/v2/test_route_planner.py ......... passed
tests/v2/test_temperature_group_runner.py ......... passed
tests/v2/test_compare_v1_v2_control_flow.py .. passed
────────────────────────────────────────────────────
Total: 140 passed, 0 failed
```

## 11. decision
**A4_P9_DYNAMIC_TRANSITION_VERIFICATION_PASS**
