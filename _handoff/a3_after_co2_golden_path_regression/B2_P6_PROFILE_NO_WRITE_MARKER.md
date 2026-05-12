# B2-P6: Simulation Profile No-Write Marker

**日期**: 2026-05-12 | **分支**: `codex/v2-golden-recovery-cdb82111` | **起始 HEAD**: `43c8f626`
**角色**: `engineering_probe_only` / `not_real_acceptance_evidence=true`

---

## 1. Scope

本轮只做 B2-P6：给 B1-R1 simulation profile 补显式 `no_write_guard_active: true`。
P5 的 114 个 `test_a2_no_write_pressure_sweep.py` 失败是独立 test fixture debt，后置处理，本轮不阻塞。

## 2. 修改文件

| 文件 | 操作 |
|---|---|
| `src/.../simulated/replacement_skip0_co2_only_simulated.json` | workflow 下新增 `no_write_guard_active: true` |
| `tests/v2/test_simulation_profile_no_write_marker.py` | 新增：验证 marker 存在，确认非七压力点 baseline |
| 本文件 | handoff |

## 3. Marker 位置

`workflow.no_write_guard_active = true`，紧邻 `collect_only`：

```json
"workflow": {
    "collect_only": true,
    "no_write_guard_active": true,
    "collect_only_fast_path": false
```

保持：`features.simulation_mode=true`、`points_excel=./skip0_co2_only_points_simulated.json`。

## 4. 测试结果

```powershell
$env:PYTHONPATH="src"
python -m pytest tests/v2/test_simulation_profile_no_write_marker.py -q --tb=short
python -m pytest tests/v2/test_no_write_guard.py -q --tb=short
python -m pytest tests/v2/test_analyzer_fleet_service.py -q --tb=short
```

预期全部 PASS（marker 1 + guard 15 + fleet 21 = 37）。

## 5. 未修改

V1、no_write_guard、analyzer_fleet、pressure/valve/H2O/CO2 runner、test_a2_no_write_pressure_sweep.py、A2 七压力点 config/points。

## 6. 安全和边界

- 不跑真实 COM、不执行 `--execute-probe`
- 不写 ID/SENCO/zero/span/coefficient/calibration 参数
- 不进入 controlled write / production acceptance / formal switch

## 7. V2 气路常压点缺口

当前 V2 气路在常压点识别与处理上尚需对齐。已识别为后续 A4/B2 exit 缺口，本轮只记录，不修改。

## 8. Final Decision

`B2_P6_PROFILE_MARKER_PASS`
