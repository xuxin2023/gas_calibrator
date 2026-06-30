# V1.5 新算法测试点与低水锚点补丁评审清单 - 2026-06-30

## 小包 A：新旧算法 profile 与新算法候选点计划

目标：把新算法差异外置成配置合同和离线点位计划，不改成熟 V1.5 气路/水路 runner。

文件：

- `configs/v1_5_algorithm_route_profiles.json`
- `src/gas_calibrator/validation/v1_5_algorithm_route_profiles.py`
- `src/gas_calibrator/tools/export_v1_5_new_algorithm_test_point_plan.py`
- `src/gas_calibrator/tools/export_v1_5_algorithm_write_contract_review.py`
- `tests/test_v1_5_algorithm_route_profiles.py`
- `tests/test_v1_5_new_algorithm_test_point_plan.py`
- `tests/test_v1_5_algorithm_write_contract_review.py`
- `_handoff/v1_5_algorithm_route_profiles_20260630/V1_5_ALGORITHM_ROUTE_PROFILES_20260630.md`
- `_handoff/v1_5_algorithm_route_profiles_20260630/new_algorithm_test_point_plan/*`
- `_handoff/v1_5_algorithm_route_profiles_20260630/algorithm_write_contract_review/*`

边界：

- 旧算法默认仍是 `legacy_ratio_production`
- 旧算法 CO2 默认仍是 45 个气点
- 旧算法 H2O 默认仍是 13 个湿点
- 新算法额外点只属于 `absorption_ratio_shadow` 生产候选
- 新算法 CO2 补点为 `-20C/600ppm`、`-10C/600ppm`
- 新算法 H2O 补点为 `40C/HGEN30C/30RH`
- SN01260607 设备特异诊断复查点不算新增物理点，也不是其它新算法设备的固定 release gate
- 其它新算法设备应先跑完整 CO2 47 点、H2O 14 点候选集，再按本设备残差自动生成诊断复查点
- CO2 旧算法写入合同为 `old_ratio_temperature`
- CO2 新算法写入合同为 `old7_absorption_A_TK_zero1ppm`
- CO2 新旧算法主链都通过受控 `SENCO1/SENCO3` 成对写入评审
- `SENCO5` 是独立最终线性层，不能折进 `SENCO1/SENCO3`
- `SENCO5` 中性化必须使用 `CLEARSENCO5,YGAS,FFF`

## 小包 B：H2O 低端锚点来自 CO2 0 气证据

目标：把水路低端锚点从 CO2 0 气点的 `R_H2O + 露点 + 压力 + T1` 离线提取出来，并明确残余水汽不能强制为 0。

文件：

- `src/gas_calibrator/validation/h2o_low_anchor_from_co2_zero.py`
- `src/gas_calibrator/tools/export_v1_5_h2o_low_anchor_from_co2_zero.py`
- `tests/test_v1_5_h2o_low_anchor_from_co2_zero.py`

边界：

- 只读历史 CO2 0 气证据
- 不打开 COM
- 不控制气路或水路
- 不写 SENCO 系数
- CO2 0 气点不能直接等同于 H2O=0

## 验证命令

```powershell
python -m pytest tests\test_v1_5_algorithm_route_profiles.py tests\test_v1_5_new_algorithm_test_point_plan.py tests\test_v1_5_algorithm_write_contract_review.py tests\test_v1_5_h2o_low_anchor_from_co2_zero.py tests\test_v1_5_h2o_dry_anchor_bridge_review.py -q
python -m pytest tests\test_v1_5_entrypoint_inventory.py -q
```

## 不应纳入本小包的内容

- `run_v1_5_formal_co2_open_flow_queue.py`
- `run_v1_5_formal_h2o_open_flow_queue.py`
- 任意真实 COM 运行产物
- 任意 coefficient writeback 产物
- 根目录 `D:\gas_calibrator` 的大 diff
- clean worktree 中与 CHECK、初始化、压力、数据库或 V2 相关的其它 dirty diff
