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

## 小包 C：H2O 写入合同与 R0(T) SENCOA/SENCOB blocker

目标：把 H2O `SENCO2/SENCO4 + SENCO6` 和新算法 `SENCOA/SENCOB R0(T)` 写入/读回要求整理成机器可读离线合同，不宣称新算法生产闭环完成。

文件：

- `configs/v1_5_algorithm_route_profiles.json`
- `src/gas_calibrator/validation/v1_5_algorithm_route_profiles.py`
- `tests/test_v1_5_algorithm_route_profiles.py`
- `tests/test_v1_5_algorithm_write_contract_review.py`
- `_handoff/v1_5_algorithm_route_profiles_20260630/V1_5_ALGORITHM_ROUTE_PROFILES_20260630.md`
- `_handoff/v1_5_algorithm_route_profiles_20260630/algorithm_write_contract_review/*`

边界：

- 旧算法 H2O 写入合同仍是 `old_ratio_temperature`
- H2O 主链仍为 `SENCO2/SENCO4`
- `SENCO6` 是独立最终线性层，不能折进 `SENCO2/SENCO4`
- `SENCO6` 中性化必须使用 `CLEARSENCO6,YGAS,FFF`
- 新算法 H2O 合同状态为 `blocked_pending_firmware_input_scale_confirmation`
- `new_absorption_R0_A_k` 只作为诊断分支，不作为默认生产写入合同
- `SENCOA/GETCOA` 与 `SENCOB/GETCOB` 是新算法 R0(T) 生产 blocker
- 当前没有新增 `SENCOA/SENCOB` 真实 writer，不打开 COM，不写系数

## 小包 D：SENCOA/SENCOB 离线 writer 设计评审

目标：先把未来 `SENCOA/SENCOB` 受控 writer 的 payload、读回、旧值快照、回滚、串口节拍和 no-write preflight 设计成机器可读证据；仍然不实现真实 writer，不打开 COM，不写系数。

文件：

- `configs/v1_5_algorithm_route_profiles.json`
- `src/gas_calibrator/validation/v1_5_sencoa_sencob_writer_design.py`
- `src/gas_calibrator/tools/export_v1_5_sencoa_sencob_writer_design_review.py`
- `tests/test_v1_5_sencoa_sencob_writer_design_review.py`
- `_handoff/v1_5_algorithm_route_profiles_20260630/sencoa_sencob_writer_design_review/*`

边界：

- `SENCOA` 只用于新算法 `R0_CO2(T)`，读回指令为 `GETCOA,YGAS,<target>`
- `SENCOB` 只用于新算法 `R0_H2O(T)`，读回指令为 `GETCOB,YGAS,<target>`
- payload 固定为 4 个有限浮点系数，写入模板为 `SENCOA/SENCOB,YGAS,<target>,c0,c1,c2,c3`
- 未来真实写入必须在 `MODE2` 下执行，串口命令间隔必须 `>=1.0s`
- 写入前必须有设备身份绑定、旧 `GETCOA/GETCOB` 快照、相关主链系数快照和候选 payload review
- 写入后必须 readback 对比；任一组失败必须按旧值快照回滚，不能接受半写入状态
- 当前状态仍为 `design_only_no_real_writer` / `blocked`，不是生产可用 writer

## 小包 E：SENCOA/SENCOB 受控 writer no-write preflight

目标：把未来真实 `SENCOA/SENCOB` writer 的解锁门禁、候选 payload 检查、旧值快照检查和真实写入边界导出成 no-write 证据；即使 payload 与快照齐全，也仍然保持真实 writer blocker。

文件：

- `configs/v1_5_algorithm_route_profiles.json`
- `src/gas_calibrator/validation/v1_5_sencoa_sencob_controlled_writer_preflight.py`
- `src/gas_calibrator/tools/export_v1_5_sencoa_sencob_controlled_writer_preflight.py`
- `tests/test_v1_5_sencoa_sencob_controlled_writer_preflight.py`
- `_handoff/v1_5_algorithm_route_profiles_20260630/sencoa_sencob_controlled_writer_preflight/*`

边界：

- 仍然不打开 COM，不导入 `GasAnalyzer`，不写 `SENCOA/SENCOB`
- `payload_review` 必须同时包含 `SENCOA` 与 `SENCOB`，每组 4 个有限浮点系数
- 默认拒绝 `FFF` 广播 target，未来真实写入应优先使用身份绑定后的单设备 target
- `old_snapshot_json` 必须包含 `GETCOA_before` 与 `GETCOB_before`
- future command gap 小于 `1.0s` 会被拒绝
- 真实写入步骤必须是：payload review -> identity binding -> old snapshot -> MODE2/1s pacing -> SENCOA write/readback -> SENCOB write/readback -> rollback on mismatch -> independent CO2/H2O no-write reverification
- 当前 `real_write_unlock_status` 仍为 `blocked_pending_real_writer_implementation`

## 验证命令

```powershell
python -m pytest tests\test_v1_5_algorithm_route_profiles.py tests\test_v1_5_new_algorithm_test_point_plan.py tests\test_v1_5_algorithm_write_contract_review.py tests\test_v1_5_h2o_low_anchor_from_co2_zero.py tests\test_v1_5_h2o_dry_anchor_bridge_review.py -q
python -m pytest tests\test_v1_5_sencoa_sencob_writer_design_review.py -q
python -m pytest tests\test_v1_5_sencoa_sencob_controlled_writer_preflight.py -q
python -m pytest tests\test_v1_5_entrypoint_inventory.py -q
```

## 不应纳入本小包的内容

- `run_v1_5_formal_co2_open_flow_queue.py`
- `run_v1_5_formal_h2o_open_flow_queue.py`
- 任意真实 COM 运行产物
- 任意 coefficient writeback 产物
- 根目录 `D:\gas_calibrator` 的大 diff
- clean worktree 中与 CHECK、初始化、压力、数据库或 V2 相关的其它 dirty diff
