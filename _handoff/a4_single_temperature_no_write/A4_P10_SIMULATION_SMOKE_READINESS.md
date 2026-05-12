# A4-P10 Simulation Smoke Readiness

## 1. scope
检查 A4 simulation profile 是否具备安全离线 smoke 能力，验证 profile/points 加载、route planner 展开、transition contract 覆盖，不跑真实 COM、不写参数。

## 2. simulation entrypoint 搜索结果

搜索 `simulation_mode|collect_only|SimulatedDevice` 发现：
- `src/gas_calibrator/v2/core/simulated_devices.py`：`SimulationPlantState` 类提供共享虚拟设备状态
- `src/gas_calibrator/v2/scripts/test_v2_safe.py`：安全测试入口，强制 `simulation_mode=True`
- 现有 test 文件已覆盖 profile 加载、points 解析、route planner 展开

**结论**：存在安全 simulation 基础设施，但完整 workflow 执行需要更多 mock。本轮只做 readiness smoke，不执行完整 workflow。

## 3. 是否执行 full offline simulation entrypoint
否。原因：
1. 完整 workflow 需要 device_manager、sampling_service、pressure_control_service 等真实服务 mock
2. 现有 test_v2_safe.py 针对通用配置，非 A4 专用
3. P9 已验证 transition contract，P10 只需验证 profile/points/planner readiness

## 4. A4 profile/points smoke 结果

Profile 安全标记：
- `simulation_mode = true`
- `collect_only = true`
- `no_write_guard_active = true`
- `production.enabled = false`
- 无 `execute_probe`
- 无 `real_machine_dry_run`
- 无 `allow_write_* = true`

Points 矩阵：
- H2O: 1 ambient_open + 7 sealed = 8 points
- CO2: 7 sealed = 7 points
- Total: 15 points
- All temperature: 20℃

## 5. expected matrix
- H2O sample targets: 8
- CO2 sample targets: 7
- Total points: 15
- no_write: true
- real_com: false (profile declares simulation_only)

## 6. H2O→CO2 transition smoke 结果
- `test_a4_h2o_to_co2_transition_contract.py` 存在且通过 30 tests
- route_sequence = ["h2o", "co2"]
- H2O pressure refs = ambient + 7 sealed
- CO2 pressure refs = 7 sealed only, 0 ambient

## 7. CO2 ambient_open 仍 deferred
Profile notes 明确记录 `co2_ambient_open_point: "not included in this round; deferred risk item"`。测试确认 CO2 pressure refs 无 ambient。

## 8. 未改 runtime/V1/A2 baseline/A4 profile/points
零修改。

## 9. 未跑真实 COM、未写参数
simulation-only，no-write。

## 10. 是否需要下一步新增最小 simulation adapter
可选。如需执行完整 workflow smoke，需要：
- FakeDeviceManager（基于 SimulationPlantState）
- FakeSamplingService
- FakePressureControlService

但当前 readiness smoke 已足够验证 profile/planner 正确性。

## 11. 测试结果
```
tests/v2/test_a4_simulation_smoke.py ................. 17 passed
tests/v2/test_a4_h2o_to_co2_transition_contract.py ... 30 passed
tests/v2/test_a4_single_temp_profile.py ............. passed
tests/v2/test_simulation_profile_no_write_marker.py .. passed
tests/v2/test_no_write_guard.py ..................... passed
tests/v2/test_analyzer_fleet_service.py ............. passed
tests/v2/test_route_planner.py ...................... passed
tests/v2/test_temperature_group_runner.py ........... passed
tests/v2/test_compare_v1_v2_control_flow.py ......... passed
────────────────────────────────────────────────────
Total: 157 passed, 0 failed
```

## 12. decision
**A4_P10_SIMULATION_SMOKE_READY**
