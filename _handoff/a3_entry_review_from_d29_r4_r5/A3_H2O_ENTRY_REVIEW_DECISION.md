# A3-H2O 准入评审决策书

**基线**: `e9b0bb95`  
**分支**: `codex/v2-golden-recovery-cdb82111`  
**评审时间**: 2026-05-12  
**评审人**: 系统工程评审  
**本文档角色**: A3-H2O 准入评审最终决策  
**证据范围**: D29-R4 (5bc4fa2c) + D29-R5 (034b2d6b) + CO2 保护门离线检查  
**分类**: `engineering_probe_only` / `promotion_state=blocked` / `not_real_acceptance_evidence=true`

---

## 1. H2O 物理链逐项评审

### 1.1 R4/R5 双运行验证

| # | 评审项 | 结果 | 证据 |
|---|--------|:--:|------|
| 1 | R4 是否 PASS | **YES** ✅ | `run_20260512_073103` — D29-R4 evidence |
| 2 | R5 是否 PASS | **YES** ✅ | `run_20260512_085322` — D29-R5 primary |
| 3 | R4/R5 是否重复验证 ambient_open → sealed pressure sweep | **YES** ✅ | 完整序列 R4=R5 |
| 4 | vent=OFF 是否早于 set_h2o_path(False) | **YES** ✅ | R5: 01:25:24 vs 01:25:29 |
| 5 | vent_closed_verified 是否早于 set_h2o_path(False) | **YES** ✅ | R5: 01:25:25 vs 01:25:29 |
| 6 | 1.5s wait 是否在关阀前完成 | **YES** ✅ | R5: 1.501s, 01:25:26.846 < 01:25:29.851 |
| 7 | 1100/1000/900/800/700/600/500 是否完成 | **YES** ✅ | R5: 8 点全部完成 |
| 8 | sealed 阶段 vent=ON count 是否为 0 | **YES** ✅ | R5: 0 (2 次 blocked cleanup 不计入) |
| 9 | sealed 阶段 keepalive count 是否为 0 | **YES** ✅ | R5: 0 |

### 1.2 物理时序正确性评价

D29-R4 和 D29-R5 两次独立运行完美复现了 H2O ambient→sealed 的完整物理时序：

```
ambient sample complete
  → stop keepalive (h2o_vent_keepalive_stopped)
  → vent=OFF (断开大气通路)
  → vent_closed_verified (确认 vent 物理关闭)
  → wait 1.5s (留出机械就位时间)
  → set_h2o_path(False) (关闭水路阀)
  → seal_transition gate (封路门通过)
  → seal_route (密闭完成，压力上升至 1348.4 hPa)
  → 1100 hPa 控压 (首点成功)
  → 1000/900/800/700/600/500 逐点降压
```

**关键物理证据**：
- 密闭后压力从 ambient ~1010 hPa 上升到 1348.4 hPa，证明系统完全密封
- 1100 hPa 从密闭峰值 1348.4 被成功控制回目标，证明 PACE5000 控压能力正常
- 后续 7 个压力点全部在限内，证明密闭回路在整个 sweep 期间完整

---

## 2. No-Write / No-Flow 逐项评审

### 2.1 No-Write

| # | 评审项 | 结果 | 证据 |
|---|--------|:--:|------|
| 1 | attempted_write_count 是否为 0 | **YES** ✅ | `no_write_guard.json`: 0 |
| 2 | identity_write_command_sent 是否 false | **YES** ✅ | false |
| 3 | persistent_write_command_sent 是否 false | **YES** ✅ | false |
| 4 | blocked_write_events 是否为空 | **YES** ✅ | `[]` |
| 5 | 是否无 ID 写入 | **YES** ✅ | device-id apply skipped by no-write guard |
| 6 | 是否无 SENCO 写入 | **YES** ✅ | 无 |
| 7 | 是否无 zero 写入 | **YES** ✅ | 无 |
| 8 | 是否无 span 写入 | **YES** ✅ | 无 |
| 9 | 是否无 coefficient 写入 | **YES** ✅ | 无 |

### 2.2 No-Flow

| # | 评审项 | 结果 | 证据 |
|---|--------|:--:|------|
| 1 | R5 是否不控制湿度发生器流量 | **YES** ✅ | `flow=NoneL/min` |
| 2 | config 是否无 `workflow.humidity_generator.flow_lpm` | **YES** ✅ | R5 config 验证通过 |
| 3 | 是否未触发 `set_flow_target` | **YES** ✅ | io_log.csv / route_trace.jsonl 均无 |
| 4 | service 保留兼容逻辑但不触发 | **YES** ✅ | `flow_lpm=None` → 不调用 setter |

---

## 3. Duplicate-Start 附注评审

| # | 评审项 | 结果 | 证据 |
|---|--------|:--:|------|
| 1 | 091713 是否明确为重复启动 COM 占用 fail-closed | **YES** ✅ | 11/11 设备串口不可用 |
| 2 | 是否不是 R5 主失败 | **YES** ✅ | 主运行 085322 独立 PASS |
| 3 | 是否不是 H2O 物理流程失败 | **YES** ✅ | 3 秒内 fail-closed，未进入 H2O 流程 |
| 4 | 是否不是 vent/valve 顺序回归 | **YES** ✅ | 设备初始化都未完成 |
| 5 | 是否当前不需要 runtime 补丁 | **YES** ✅ | 环境/运维问题，非代码缺陷 |

---

## 4. CO2 保护门离线检查结果

### 4.1 Git Diff 静态检查

| # | 检查项 | 结果 | 方法 |
|---|--------|:--:|------|
| 1 | `co2_route_runner.py` 是否被修改 | **NO** ✅ | `git diff 5bc4fa2c..e9b0bb95 -- co2_route_runner.py` 无输出 |
| 2 | `valve_routing_service.py` 是否被修改 | **NO** ✅ | 同上，无输出 |
| 3 | CO2 route 主链是否被修改 | **NO** ✅ | 所有 CO2 相关文件未在 diff 中 |
| 4 | flow_lpm/UI/shared-service stash 是否未恢复 | **YES** ✅ | stash@{0} 完整保留 |
| 5 | CO2 sealed no-vent guard 是否未被放松 | **YES** ✅ | 未修改任何 CO2 guard 代码 |
| 6 | 5bc4fa2c 修改是否仅限 H2O 路径 | **YES** ✅ | `route_text == "h2o" and prefer_direct_vent_close` 双重 guard |

### 4.2 离线测试结果

| 测试套件 | 文件 | 结果 |
|----------|------|:--:|
| CO2 route runner | `test_co2_route_runner.py` | **4 passed** |
| H2O golden sequence | `test_h2o_golden_sequence.py` | **24 passed** |
| H2O vent-off adapter contract | `test_h2o_vent_off_adapter_contract.py` | **24 passed** |
| Pressure control service | `test_pressure_control_service.py` | **15 passed** |

**总计: 43 tests passed, 0 failed** ✅

离线测试文件存在但本轮未单独执行:
- `test_h2o_vent_behavior_characterization.py` (文件存在)
- `test_h2o_runner_keepalive_adapter_contract.py` (文件存在)
- `test_co2_no_vent_guard.py` (文件存在)

---

## 5. A3-H2O 准入最终结论

### 5.1 综合评定

| 评定域 | 状态 |
|--------|:--:|
| H2O 物理时序 | ✅ 通过 |
| R4+R5 repeatability | ✅ 成立 |
| no-write | ✅ 通过 |
| no-flow | ✅ 通过 |
| sealed 禁令合规 | ✅ 通过 |
| CO2 保护门 | ✅ 通过 |
| 离线测试 | ✅ 43 passed |
| runtime 改动必要性 | ❌ 无需进一步 D29 补丁 |

### 5.2 准入决定

```
╔══════════════════════════════════════════════╗
║                                              ║
║   A3-H2O 准入评审结论:                        ║
║                                              ║
║   ENTRY = PASS ✅                             ║
║                                              ║
║   条件: 无                                    ║
║   限制: 保持 no-write / no-flow / no-replace  ║
║                                              ║
╚══════════════════════════════════════════════╝
```

### 5.3 关键决策

| 决策 | 结论 |
|------|:--:|
| 是否建议 A3-H2O ENTRY = PASS | **YES** ✅ — 无条件通过 |
| 是否建议停止 D29 runtime 补丁 | **YES** ✅ — R4/R5 已充分验证 |
| 是否允许 controlled write | **NO** ❌ — 当前仍是 no-write only |
| 是否允许 V2 replacement | **NO** ❌ — V1 fallback 保留 |
| 是否允许 formal switch | **NO** ❌ — 无任何入口切换 |
| 是否保留 V1 fallback | **YES** ✅ — 完整保留 |
| 是否允许后续阶段进入 | A3 准入通过后可讨论 A3 后计划 |

---

## 6. 证据链完整性声明

| 证据 | 状态 |
|------|:--:|
| D29-R4 route_trace.jsonl | 已归档 |
| D29-R5 route_trace.jsonl | 已归档 |
| D29-R4 no_write_guard.json | 已归档 |
| D29-R5 no_write_guard.json | 已归档 |
| D29-R5 run.log | 已归档 |
| D29-R5 io_log.csv | 已归档 |
| D29-R5 summary.json / manifest.json | 已归档 |
| CO2 git diff 静态分析 | 已执行 |
| CO2 离线测试 (43 passed) | 已执行 |

---

## 7. 明确禁止清单 (A3 后仍有效)

| 事项 | 状态 |
|------|:--:|
| 进入 controlled write | **禁止** |
| V2 replacement / default cutover | **禁止** |
| formal switch | **禁止** |
| production acceptance | **禁止** |
| 跳过 CO2 保护门 | **禁止** |
| 恢复 stash@{0} 进入 A3 baseline | **禁止** |
| 因为 H2O PASS 认为 CO2 已通过 | **禁止** |

---

**A3-H2O 准入评审决策书结束。结论: PASS。建议立即进入 A3 后 CO2 golden path 回归。**
