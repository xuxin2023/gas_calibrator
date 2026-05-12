# A4-P11 Workflow Simulation Adapter

## 1. scope
在 tests 内新增最小 A4 workflow simulation adapter，基于 profile/points/planner 生成 execution summary，验证 A4 H2O+CO2 15 点矩阵的完整 no-write simulation 计划。不跑真实 COM、不写参数、不改 runtime。

## 2. 为什么 P11 需要 adapter
P10 验证了 profile/points/planner readiness，但没有生成结构化 execution summary。adapter 将 profile 加载、points 解析、route planner 展开、transition sequence 整合为一个可审计的 summary 对象，为未来真实 acceptance 做治理框架准备。

## 3. adapter 是 test-only，不是 runtime
- `tests/v2/a4_simulation_adapter.py`：仅被 test import
- 不被生产 runtime import
- 不打开 serial/COM
- 不调用真实 DeviceManager
- 不写任何 analyzer 参数
- 只生成 execution summary，不生成生产 artifact

## 4. execution summary

```
A4ExecutionSummary:
  profile_path: a4_single_temp_h2o_co2_no_write_20c_simulated.json
  points_path: a4_20c_h2o_co2_points_simulated.json
  simulation_only: True
  no_write: True
  real_com: False
  route_sequence: ["h2o", "co2"]
  h2o_points_total: 8
  h2o_ambient_open_count: 1
  h2o_sealed_pressure_count: 7
  h2o_sealed_pressures: [500, 600, 700, 800, 900, 1000, 1100]
  co2_points_total: 7
  co2_ambient_open_count: 0
  co2_sealed_pressure_count: 7
  co2_sealed_pressures: [500, 600, 700, 800, 900, 1000, 1100]
  total_sample_targets: 15
  transition_sequence: [h2o_route_start, h2o_ambient_open_sample,
    h2o_sealed_pressure_sweep, h2o_cleanup, co2_route_baseline,
    co2_route_open, co2_route_soak, co2_preseal,
    co2_sealed_pressure_sweep, safe_stop]
  attempted_write_count: 0
  identity_write_command_sent: False
  calibration_write_command_sent: False
  production_acceptance: False
  controlled_write: False
  formal_switch: False
  deferred: [co2_ambient_open, real_machine_probe, p5_fixture_debt]
```

## 5. H2O 8 / CO2 7 / total 15
- H2O: 1 ambient_open + 7 sealed = 8
- CO2: 7 sealed = 7
- Total: 15 points, all 20℃

## 6. H2O cleanup → CO2 baseline 顺序
transition_sequence 中 h2o_cleanup(index=3) 在 co2_route_baseline(index=4) 前。co2_preseal(index=7) 在 co2_sealed_pressure_sweep(index=8) 前。safe_stop(index=9) 最后。

## 7. no-write evidence
- attempted_write_count = 0
- identity_write_command_sent = False
- calibration_write_command_sent = False
- production_acceptance = False

## 8. CO2 ambient_open still deferred
- co2_ambient_open_count = 0
- deferred 列表包含 "co2_ambient_open"

## 9. 未改 runtime/V1/A2 baseline/A4 profile/points
零修改。adapter 是 test-only helper。

## 10. 未跑真实 COM、未写参数
simulation-only，no-write。

## 11. 是否需要下一步 operator confirmation
否。当前 adapter 只生成 summary，不执行任何真实操作。如需真实 acceptance，需要 operator confirmation + engineering probe unlock。

## 12. decision
**A4_P11_WORKFLOW_SIM_ADAPTER_PASS**
