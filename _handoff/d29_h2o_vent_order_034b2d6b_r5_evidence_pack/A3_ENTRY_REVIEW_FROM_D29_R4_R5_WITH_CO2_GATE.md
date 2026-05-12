# A3-H2O 准入评审报告（含 CO2 气路保护门）

**基线**: `034b2d6bf3321ebc40b078b6c8bd9f97eb6ab2ef`  
**分支**: `codex/v2-golden-recovery-cdb82111`  
**生成时间**: 2026-05-12  
**报告类型**: A3 准入评审（H2O 水路，含 CO2 保护门）  
**证据来源**: D29-R4 + D29-R5 no-write 工程探针  
**分类**: `engineering_probe_only` / `promotion_state=blocked` / `not_real_acceptance_evidence=true`

---

## 一、A3 定位

1. 这是 **A3-H2O 准入评审**，不是 V2 整体 cutover。
2. D29-R4/R5 主证据对象是 **H2O 水路**。
3. A3 准入不能只看 H2O，必须包含 **CO2 气路保护门**。
4. 这不是 real acceptance，不是 production acceptance，不是 V2 replacement，不是 formal switch。
5. V1 fallback 完整保留，不做任何默认入口切换。

---

## 二、H2O 准入证据

### 2.1 D29-R4 PASS 摘要

| 项目 | 结果 |
|------|------|
| Run ID | `run_20260512_073103` |
| 提交 | `5bc4fa2c` fix(v2): enforce H2O ambient-to-sealed vent valve order |
| 决断 | PASS |
| vent/valve 顺序 | vent=OFF → vent_closed_verified → 1.5s wait → set_h2o_path(False) → seal gate |
| sealed 阶段 vent=ON count | 0 |
| sealed 阶段 keepalive count | 0 |
| no-write | PASS |
| 七压力点 | 全部完成 |

### 2.2 D29-R5 PASS 摘要（修正版）

| 项目 | 结果 |
|------|------|
| 主运行 ID | **`run_20260512_085322`**（启动 08:53 CST） |
| 提交 | `034b2d6b` test(v2): align H2O probe and contracts with D29-R4 baseline |
| 决断 | **PASS** |
| vent/valve 顺序 | vent=OFF → vent_closed_verified → 1.5s wait → set_h2o_path(False) → seal gate → 1100 hPa → ... → 500 hPa |
| sealed 阶段 vent=ON count | **0**（2次 blocked cleanup 不计入） |
| sealed 阶段 keepalive count | **0** |
| no-write | **PASS** (`attempted_write_count=0`, `identity_write_command_sent=false`) |
| no-flow | **PASS** (`flow=NoneL/min`, 无 `set_flow_target`) |
| 七压力点 | ambient + 1100/1000/900/800/700/600/500 全部完成 |

### 2.3 Transition Timeline（085322 主运行）

| # | Event | Time (UTC) | Time (CST) | Δ from previous | Result |
|---|---|---|---|---|---|
| 1 | ambient_sample_complete | 01:25:13.081 | 09:25:13 | - | ok |
| 2 | h2o_seal_transition_start | 01:25:13.082 | 09:25:13 | 0.001s | ok |
| 3 | h2o_vent_keepalive_stopped | 01:25:13.082 | 09:25:13 | 0.000s | ok |
| 4 | vent=OFF command sent | 01:25:24.162 | 09:25:24 | 11.080s | ok |
| 5 | vent_closed_verified | 01:25:25.345 | 09:25:25 | 1.183s | ok |
| 6 | post_vent_closed_wait_started | 01:25:25.346 | 09:25:25 | 0.001s | ok |
| 7 | post_vent_closed_wait_completed | 01:25:26.846 | 09:25:26 | 1.500s | ok |
| 8 | h2o_path_close_command_sent | 01:25:29.311 | 09:25:29 | 2.465s | ok |
| 9 | set_h2o_path(False) | 01:25:29.851 | 09:25:29 | 0.540s | ok |
| 10 | h2o_path_closed_after_vent_closed | 01:25:29.852 | 09:25:29 | 0.001s | ok |
| 11 | seal_transition gate | 01:25:29.852 | 09:25:29 | 0.000s | ok |
| 12 | seal_route (P=1348.398 hPa) | 01:25:35.715 | 09:25:35 | 5.863s | ok |
| 13 | 1100 hPa in-limits | 01:25:58.225 | 09:25:58 | 22.510s | ok |

### 2.4 顺序判断

- ✅ vent=OFF 早于 set_h2o_path(False)（01:25:24 vs 01:25:29）
- ✅ vent_closed_verified 早于 set_h2o_path(False)（01:25:25 vs 01:25:29）
- ✅ 1.5s wait 在关阀前完成（01:25:26.846 < 01:25:29.851）
- ✅ 没有重复 vent=OFF
- ✅ 没有重复 set_h2o_path(False)

### 2.5 压力行为

| 阶段 | 压力 (hPa) |
|------|-----------|
| Ambient（采样时） | ~1009.9 |
| vent=OFF 后密闭 | → 开始自然升压 |
| 密闭后最高 | **1348.398** |
| 1100 hPa 稳定 | 1099.92 |
| 1000 hPa 稳定 | 1000.00 |
| 900 hPa 稳定 | 900.01 |
| 800 hPa 稳定 | 800.01 |
| 700 hPa 稳定 | ~700 (稳定) |
| 600 hPa 稳定 | ~600 (稳定) |
| 500 hPa 稳定 | 500.05 |

### 2.6 七压力点结果

| # | Target (hPa) | Actual (hPa) | CO₂ mean (ppm) | H₂O mean (mmol/mol) | Result |
|---|---|---|---|---|---|
| 1 | Ambient | 1009.926 | 904.677 | 15.586 | ok |
| 2 | 1100 | 1098.58 | 986.256 | 15.932 | ok |
| 3 | 1000 | 999.502 | 888.705 | 15.517 | ok |
| 4 | 900 | 899.6 | 783.807 | 14.984 | ok |
| 5 | 800 | 799.695 | 677.463 | 14.335 | ok |
| 6 | 700 | ~700 | 479.135 | 13.059 | ok |
| 7 | 600 | 599.887 | ~560 | ~13.6 | ok |
| 8 | 500 | 500.051 | 387.882 | 12.327 | ok |

### 2.7 H2O 准入小结

- ✅ R4 PASS + R5 PASS 构成 repeatability 证据
- ✅ vent/valve 顺序在 R4 和 R5 均复现
- ✅ sealed 阶段 vent=ON = 0，keepalive = 0
- ✅ no-write 约束完整保持
- ✅ no-flow 约束完整保持
- ✅ 七压力点两次均全部完成
- ✅ R4/R5 可作为 A3-H2O 准入工程证据

---

## 三、CO2 Protection Gate / 气路保护门

### 3.1 本轮是否修改 CO2 runner

| 检查项 | 结果 |
|--------|------|
| 本轮是否修改 CO2 runner | **NO** — `co2_route_runner.py` 在 `034b2d6b` 与 `5bc4fa2c` 之间未变更 |
| 本轮是否修改 CO2 route 主链 | **NO** — CO2 路由逻辑未被本次任何 commit 触碰 |
| 本轮是否修改 valve_routing_service | **NO** — `valve_routing_service.py` 未被本次任何 commit 触碰 |

**证据**：

```bash
git diff 5bc4fa2c..034b2d6b -- src/gas_calibrator/v2/core/runners/co2_route_runner.py
# → 无输出（文件未被修改）
```

### 3.2 本轮是否恢复 flow_lpm/UI/shared-service stash

| 检查项 | 结果 |
|--------|------|
| stash@{0} 是否仍存在 | **YES** |
| stash@{0} 内容 | `D29-R4 unrelated: flow_lpm removal + test contract updates NOT part of 5bc4fa2c` |
| 本轮是否恢复 stash@{0} | **NO** |
| 本轮是否恢复 stash 中任何高风险改动 | **NO** |

```bash
$ git stash list -n 3
stash@{0}: On codex/v2-golden-recovery-cdb82111: D29-R4 unrelated: flow_lpm removal + test contract updates NOT part of 5bc4fa2c
stash@{1}: WIP on codex/run001-a1-no-write-dry-run: ...
```

### 3.3 5bc4fa2c 的 pressure_control_service.py 修改是否影响 CO2 路径

**详细分析**：

`5bc4fa2c` 在 `pressure_control_service.py` 的 `pressurize_and_hold()` 方法中新增了一段逻辑，其激活条件是：

```python
if route_text == "h2o" and prefer_direct_vent_close:
    # H2O vent closed verification + post-vent-closed 1.5s wait + set_h2o_path(False)
```

关键保护门分析：

| 维度 | 分析 |
|------|------|
| route_text guard | `== "h2o"` — 仅 H2O 路由进入 |
| prefer_direct_vent_close guard | 必须为 truthy 才进入新增逻辑 |
| CO2 runner 调用方式 | `pressurize_and_hold(point, route=phase)` — **不传** `prefer_direct_vent_close` |
| H2O runner 调用方式 | `pressurize_and_hold(lead, route=phase, prefer_direct_vent_close=True)` — 传入 True |
| CO2 是否会进入新增逻辑 | **NO** — 双重 guard 均不满足 |

**结论**：5bc4fa2c 的修改 **仅影响 H2O + prefer_direct_vent_close 路径**，CO2 路径完整隔离。

### 3.4 CO2 sealed no-vent guard 是否被放松

| 检查项 | 结果 |
|--------|------|
| CO2 sealed 阶段 vent=ON guard 是否仍有效 | **YES** — CO2 不在本次修改范围内 |
| 5bc4fa2c 是否增加/删除任何 CO2 相关 guard | **NO** — 修改仅触及 H2O 分支 |
| CO2 原有的 sealed phase vent gate 是否被放松 | **NO** — 未修改任何 CO2 路径代码 |

### 3.5 CO2 golden path 是否需要在 A3 后做 no-write 回归

| 维度 | 结论 |
|------|------|
| 当前 CO2 路径是否被修改 | NO |
| CO2 是否受 H2O vent/valve 修改影响 | NO（双重 guard 隔离） |
| 但 CO2 runner 是否调用同一 `pressurize_and_hold` | YES |
| 是否存在间接影响风险 | **低**（route_text guard 明确隔离） |
| 建议 A3 后 CO2 no-write 回归 | **YES** — 保守起见，作为 A3 后优先项 |

### 3.6 A3 准入是否允许跳过 CO2 保护门

| 判定 | 结论 |
|------|------|
| A3 准入是否允许跳过 CO2 保护门 | **NO** |
| CO2 保护门是否通过 | **YES**（CO2 路径未受影响，有明确的代码级隔离证据） |
| CO2 保护门是否还需要额外验证 | A3 准入评审期间**不需要**；A3 后建议跑一次 CO2 golden path no-write 回归 |

---

## 四、关于 091713 重复启动 fail-closed 附注

### 4.1 事实确认

- Run `run_20260512_091713` 在 **09:17:13 CST** 启动
- Run `run_20260512_085322` 在 **08:53:22 CST** 启动，在 09:17 时正在运行中（湿度 settling 阶段）
- 091713 启动后 **3 秒内**（09:17:16 CST）即 fail-closed
- 失败根因：

```
Calibration failed: Critical device initialization failed
critical_devices_failed=['dewpoint_meter', 'gas_analyzer_0', 'gas_analyzer_1',
  'gas_analyzer_2', 'gas_analyzer_3', 'humidity_generator', 'pressure_controller',
  'pressure_meter', 'relay_a', 'relay_b', 'temperature_chamber']
```

所有 11 个关键设备全部初始化失败，原因是 085322 已占用全部 COM 口（COM16/17/20/22/23/35/37/41/42）。

### 4.2 091713 定性

| 命题 | 判定 |
|------|------|
| 091713 是否为 D29-R5 主运行 | **NO** |
| 091713 是否为 duplicate-start fail-closed 附注 | **YES** |
| 091713 是否由 COM 口被 085322 占用导致 | **YES**（全部设备 serial not open） |
| 091713 是否不是 vent/valve 顺序回归 | **YES**（不是 — 设备初始化前就已失败） |
| 091713 是否不是 H2O 物理流程失败 | **YES**（不是 — 未到达 H2O 流程） |
| 091713 是否不是需要当前修 runtime 的理由 | **YES**（不是 — 环境/运维问题，非代码缺陷） |

### 4.3 工程建议

- 当前不为 091713 修改 runtime
- A3 后可考虑单实例 PID/lock guard 作为后置工程增强，但当前阶段不改 runtime

---

## 五、A3 准入结论

### 5.1 关键问题回答

| 问题 | 回答 |
|------|------|
| 是否建议停止 D3/D29 runtime 补丁 | **YES** — D29-R4/R5 已充分验证 vent/valve 顺序修复 |
| 是否建议进入 A3-H2O 准入评审 | **YES** ✅ |
| 是否允许进入 controlled write | **NO** — 当前仍是 no-write only |
| 是否允许 V2 replacement | **NO** — 当前仍是 V1 fallback + V2 engineering probe |
| 是否允许 default cutover | **NO** — 默认入口仍为 V1 |
| 是否保留 V1 fallback | **YES** — 完整保留 |
| H2O 准入证据是否充分 | **YES** — R4+R5 构成 repeatability |
| CO2 保护门是否通过 | **YES** — CO2 路径未受影响 |
| 是否为 real acceptance | **NO** — `not_real_acceptance_evidence=true` |
| 是否为 production acceptance | **NO** — 仍为 engineering probe |
| 是否为 formal switch | **NO** — 无任何入口切换 |

### 5.2 A3 后下一步建议

1. **CO2 golden path 保护/回归** — 跑一次 CO2 no-write sealed route 验证（优先级最高）
2. **R4/R5 evidence registry** — 将 D29-R4 和 D29-R5 证据正式登记入 registry
3. **VentManager / RoutePressureStateMachine shadow 设计** — 为未来 vent/valve 管理提供更结构化的框架，但**不立即改 runtime**
4. **high-risk stash 后置专项** — flow_lpm/UI/shared-service 高风险改动不得混入 A3 baseline，需独立后置处理
5. **单实例 PID/lock guard** — 列为后置工程增强，防止重复启动 COM 口占用，但当前不改 runtime

---

## 六、证据清单

| 文件 | 路径 |
|------|------|
| 主运行 run.log | `output/run001_h2o/1_point_no_write/run_20260512_085322/run.log` |
| route_trace | `output/run001_h2o/1_point_no_write/run_20260512_085322/route_trace.jsonl` |
| summary.json | `output/run001_h2o/1_point_no_write/run_20260512_085322/summary.json` |
| no_write_guard.json | `output/run001_h2o/1_point_no_write/run_20260512_085322/no_write_guard.json` |
| manifest.json | `output/run001_h2o/1_point_no_write/run_20260512_085322/manifest.json` |
| io_log.csv | `output/run001_h2o/1_point_no_write/run_20260512_085322/io_log.csv` |
| points.csv | `output/run001_h2o/1_point_no_write/run_20260512_085322/points.csv` |
| config | `_handoff/d29_h2o_vent_order_034b2d6b_r5_evidence_pack/config.json` |
| 091713 附注 | `output/run001_h2o/1_point_no_write/run_20260512_091713/run.log` |
| D29-R4 evidence | `_handoff/d29_h2o_vent_order_034b2d6b_r4_evidence_pack/` |

---

**结论**: D29-R4 与 D29-R5 构成 H2O 水路 vent/valve 顺序修复的 repeatability 工程证据。CO2 气路保护门经过代码级分析确认未受影响。**建议进入 A3-H2O 准入评审**，同时按上述建议推进 CO2 回归和证据治理。这仍然不是 real acceptance，不是 production acceptance，不是 V2 replacement，不是 formal switch。
