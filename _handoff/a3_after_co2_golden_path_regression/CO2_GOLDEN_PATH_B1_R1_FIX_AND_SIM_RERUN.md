# CO2 Golden Path B1-R1 修复与 Simulation Rerun 报告

**生成时间**: 2026-05-12  
**HEAD**: eb605517（+修复）  
**分支**: codex/v2-golden-recovery-cdb82111  
**上游**: CO2_GOLDEN_PATH_B1_R1_ROOT_CAUSE_REPORT.md  
**审计**: B1-R1 Pre-Commit Audit ✅  
**结论**: **B1-R1 PASS** ✅（注意：本轮是 B1-R1 orchestrator host-contract fix，不是完整 B1 七压力点最终回归）

---

## 1. ROOT_CAUSE_REPORT 摘要

根因分类：**B — 历史 CO2 成功入口未覆盖 orchestrator integration，当前暴露 host interface 缺口。**

- `2ec4682f` (batch-7: migrate 6 conditioning methods to conditioning_service) 将 CO2 conditioning 逻辑移入 `conditioning_service.py`，包括 3 处 `self.host._verify_co2_preseal_atmosphere_hold_pressure(point)` 调用和 1 处 `self.host._refresh_live_analyzer_snapshots()` 调用
- `2ec4682f` 是 HEAD 的祖先（存在于当前分支）
- 但 orchestrator 从未定义这些方法
- 修复已存在于 `8f411f34`（`codex/run001-a1-no-write-dry-run` 分支），未合并到当前分支
- **非 H2O D29 引入** — 调用点早于 `5bc4fa2c`
- **CO2 主流程未受破坏** — route_runner / valve_routing / pressure_control 均未受影响
- **测试 mock 掩盖了缺口** — `test_a2_no_write_pressure_sweep.py` 通过 monkeypatch 注入 mock

## 2. 为什么允许修改代码

分类 B：orchestrator-host 接口合同缺口，非错误入口。修复范围最小、隔离。

## 3. 修改文件清单

| 文件 | 变更 | 行数 |
|------|------|------|
| `src/gas_calibrator/v2/core/orchestrator.py` | 新增 2 个 wrapper 方法 | +6 |
| `src/gas_calibrator/v2/core/services/conditioning_service.py` | 新增 `_verify_co2_preseal_atmosphere_hold_pressure` 实现 | +115 |
| **总计** | | **+121** |

### 3.1 orchestrator.py 变更

```python
def _verify_co2_preseal_atmosphere_hold_pressure(self, point):
    return self.conditioning_service._verify_co2_preseal_atmosphere_hold_pressure(point)

def _refresh_live_analyzer_snapshots(self, *, force: bool = False, reason: str = "") -> bool:
    return True
```

### 3.2 `_refresh_live_analyzer_snapshots` wrapper 合理性说明

| 审计项 | 结果 |
|--------|------|
| 调用链 | `conditioning_service.py:2146` → `self.host._refresh_live_analyzer_snapshots(reason="co2_route_preseal_soak")` |
| 同类缺口 | 是 — 与 `_verify_co2_preseal_atmosphere_hold_pressure` 同属 batch-7 迁移（`2ec4682f`）留下的 orchestrator-host interface 缺口 |
| 旧实现参考 | V1: `runner.py:14507` 有完整实现；V2 旧分支 `8f411f34` 有相同 wrapper |
| 改变 CO2 物理流程 | ❌ 否 — wrapper 返回 `True`（no-op） |
| 触发真实 analyzer 写入 | ❌ 否 — `return True` 是纯 no-op，无任何 I/O |
| H2O 路径 | H2O services 通过 `getattr(self.host, "_refresh_live_analyzer_snapshots", None)` fallback 调用；H2O 当前不可用（`h2o_route_available=false`） |
| TDD mock pattern | `test_a2_no_write_pressure_sweep.py` 中多处 mock 为 `lambda **kwargs: True` — 与 wrapper 行为一致 |
| 是否触碰 V1 | ❌ 否 |
| 是否触碰 H2O runtime | ❌ 否 |
| 是否触碰 CO2 route_runner | ❌ 否 |
| 是否触碰 pressure_control_service | ❌ 否 |
| 是否触碰 valve_routing_service | ❌ 否 |
| 是否属最小修复范围 | ✅ 是 — conditioning service 代码中已有调用点（`2ec4682f` 引入），orchestrator 必须提供该方法才能正常运行。不是新增功能，是补齐接口合同 |

### 3.3 conditioning_service.py 变更

实现 `_verify_co2_preseal_atmosphere_hold_pressure(point)` 方法（约 115 行）：
- 读取 `ready_pressure_hpa` / `urgent_seal_threshold_hpa` / `hard_abort_pressure_hpa` 配置阈值
- 通过 `pressure_control_service._current_high_pressure_first_point_sample` 获取实时压力
- 压力不可用时返回 `"ok"`（safe-continue）
- 压力超过 hard_abort 阈值时 `raise WorkflowValidationError`（fail-closed）
- 压力超过 urgent_seal 阈值时返回 `"positive_preseal_arm_handoff"`
- 压力超过 ready 阈值时返回 `"positive_preseal_ready_handoff"`
- 否则返回 `"ok"`（继续 monitoring）

## 4. 每个文件为什么必须改

| 文件 | 必须改原因 |
|------|----------|
| orchestrator.py | conditioning_service.py 通过 `self.host._verify_co2_preseal_atmosphere_hold_pressure(point)` 和 `self.host._refresh_live_analyzer_snapshots()` 调用 host 方法；orchestrator 作为 host 必须提供这些方法 |
| conditioning_service.py | 方法体需要压力采样、阈值判定、fail-closed 异常处理逻辑，必须与 pressure_control_service 和 a2_hooks 交互 |

## 5. 合规检查

| 检查项 | 状态 |
|--------|------|
| 是否触碰 V1 | ❌ 否 |
| 是否触碰 H2O runtime | ❌ 否 |
| 是否触碰 CO2 route_runner | ❌ 否 |
| 是否触碰 pressure_control_service（shared 行为） | ❌ 否（仅读取已有 API） |
| 是否触碰 valve_routing_service | ❌ 否 |
| 是否恢复 stash | ❌ 否 |
| 是否跑真实 COM | ❌ 否 |
| 是否写任何参数 | ❌ 否 |
| 是否新增 CO2 常压采样点 | ❌ 否 |
| 是否改变 CO2 物理流程 | ❌ 否 |
| 是否影响 H2O | ❌ 否 |
| 是否影响 V1 | ❌ 否 |

## 6. 测试结果

### 6.1 CO2 离线合同测试（47/47）

```
47 passed in 28.65s ✅
```

| 测试文件 | 结果 |
|----------|------|
| test_co2_route_runner.py | PASS ✅ |
| test_co2_no_vent_guard.py | PASS ✅ |
| test_co2_artifact_contract.py | PASS ✅ |
| test_co2_route_golden_master.py | PASS ✅ |
| test_co2_shadow_state_trace.py | PASS ✅ |
| test_pressure_control_service.py | PASS ✅ |

### 6.2 test_a2_no_write_pressure_sweep.py（预存失败，非本轮引入）

**114 failed, 30 passed** — 修改前 = 修改后，修复未引入新失败。

失败原因：
- 约 70 个 `'WorkflowOrchestrator' object has no attribute 'conditioning_service'` — 测试通过 monkeypatch 注入 mock orchestrator，未构建完整 service graph
- 约 35 个 `'SimpleNamespace' object has no attribute 'a2_hooks'` — 同上，mock 未提供 a2_hooks 属性
- 这些测试文件在 HEAD eb605517 上已是 114 failed，非本轮修复引入

**建议**：B2 阶段单独开 P2 专项审计修复 test_a2_no_write_pressure_sweep.py 的 mock fixture。

## 7. Simulation Rerun 结果

### 7.1 执行命令

```bash
$env:PYTHONPATH = "src"
python -m gas_calibrator.v2.scripts.run_simulated_compare \
  --profile replacement_skip0_co2_only_simulated \
  --scenario co2_only_skip0_success_single_temp \
  --report-root "_handoff\a3_after_co2_golden_path_regression" \
  --run-name co2_golden_no_write_b1_r1_rcfix_v2 \
  --no-publish-latest
```

### 7.2 总体结果

| 指标 | 值 |
|------|-----|
| Compare status | **MATCH** ✅ |
| Overall status | **MATCH** ✅ |
| V1 | ok=True, phase=completed |
| V2 | ok=True, phase=**completed** |
| first_failure_phase | **-** (无失败) |
| points_total | **6** |
| points_completed | **6** ✅ |
| Group A 完成 | 1100/1000/600 ✅ |
| Group B 完成 | 1100/1000/600 ✅ |

**说明**：本轮 simulation 覆盖 6 个压力点（Group A: 1100/1000/600 + Group B: 1100/1000/600），skip0（0 ppm）排除，单温度 20°C，仅 CO2 route。**完整 7-pressure CO2 no-write regression 留到 B1-R2。**

### 7.3 路由轨迹关键顺序

```
Group A (100ppm):
  1. set_vent(ON)           — "before CO2 route conditioning"          [open_conditioning]
  2. set_co2_valves         — CO2 route open [8,11,16,22]
  3. set_vent(ON)           — "CO2 route pre-seal atmosphere hold"     [preseal_atmosphere_hold]
  4. wait_route_soak        — pre-seal soak
  5. set_vent(OFF)          — "before CO2 pressure seal"               ← SEAL BEGINS
  6. preseal_final_atmosphere_exit
  7. seal_transition
  8. seal_route             — "CO2 route sealed for pressure control"
  9. pressure_control_ready_gate → set_pressure → sample_start → sample_end
     × 3 points (1100/1000/600 hPa)                                     [sealed_pressure_control]
 10. cleanup

Group B (1000ppm):
  11. set_vent(ON)          — "CO2 route pre-seal atmosphere hold"     [preseal_atmosphere_hold]
  12-19. 同上顺序
      × 3 points (1100/1000/600 hPa)                                    [sealed_pressure_control]
  20. restore_baseline
  21. final_safe_stop
```

### 7.4 Sealed Vent=ON 精确审计

从 `v2_route_trace.jsonl` 全文提取的全部 `set_vent` 事件（`target.vent_on=true`）：

| # | point | target | result | message | 阶段 |
|---|-------|--------|--------|---------|------|
| 1 | 2 (A) | ON | ok | `before CO2 route conditioning` | open_conditioning |
| 2 | 2 (A) | ON | ok | `CO2 route pre-seal atmosphere hold` | preseal_atmosphere_hold |
| 3 | 3 | ON | **blocked** | `route_conditioning_phase_not_flush` | sealed_pressure_control（A） |
| 4 | 3 | ON | **blocked** | `route_conditioning_phase_not_flush` | sealed_pressure_control（A） |
| 5 | 3 (B) | ON | ok | `CO2 route pre-seal atmosphere hold` | preseal_atmosphere_hold（B） |
| 6 | 3 | ON | **blocked** | `route_conditioning_phase_not_flush` | sealed_pressure_control（B） |
| 7 | null | ON | **blocked** | `post_seal_pressure_control` safe stop | cleanup/restore |

**Sealed Pressure Control 边界判定**：
- **Group A sealed period**: event #5 set_vent(OFF) "before CO2 pressure seal" (Group A trace line) → event #11 set_vent(ON) for Group B transition
  - 期间 event #3 尝试 vent ON → **BLOCKED**（`route_conditioning_phase_not_flush`）
  - 期间 event #4 尝试 vent ON → **BLOCKED**（同上）
- **Group B sealed period**: event #15 set_vent(OFF) "before CO2 pressure seal" (Group B trace line) → trace end
  - 期间 event #6 尝试 vent ON → **BLOCKED**
  - event #7 final safe stop → **BLOCKED**（`post_seal_pressure_control`, 原因: `seal_command_sent`）

| 指标 | 数值 | 结论 |
|---|---:|---|
| total_vent_on_count（所有 target.vent_on=true） | **7** | 信息项，包含 blocked attempts |
| open_conditioning_vent_on_count | **1** | 合理（CO2 route 初始开通风） |
| preseal_atmosphere_hold_vent_on_count | **2** | 合理（Group A + Group B 各一次 pre-seal atmosphere hold） |
| **sealed_pressure_control_vent_on_count** | **0** | ✅ **必须为 0 — PASS** |
| transition_or_cleanup_vent_on_count | **0** | 可接受 |
| cleanup_or_restore_vent_on_count | **0** | 可接受 |

**关键说明**：
1. sealed pressure control 期间 vent=ON 尝试 **均被 BLOCKED**（`route_conditioning_phase_not_flush` 或 `seal_command_sent`）
2. blocked set_vent ON **不是实际通大气成功**；物理上 vent 保持 OFF
3. **物理红线是 sealed pressure control 内不得实际 VENT=ON**，本轮该红线 **满足** ✅
4. 原报告错误地将 preseal_atmosphere_hold 的 2 个 vent ON 归入 "sealed vent=ON"，已修正

### 7.5 No-write 证据

| 指标 | 值 |
|------|-----|
| write-related events | **0** ✅ |
| attempted_write_count | 0 |
| SENCO write | 无 |
| zero/span write | 无 |
| coefficient write | 无 |
| calibration write | 无 |
| mode_switch write | 无 |
| identity_write_command_sent | N/A（simulation 内部虚拟 ID 分配） |
| analyzer_device_id_assignment | 8（simulation 协议虚拟 ID 分配，非真实 COM 写入） |

**No-write 判定依据**：
1. `collect_only: true` — profile 明确定义为只收不发
2. `simulation_mode: true` — simulation 模式
3. 所有 8 个 `analyzer_device_id_assignment` 均在 simulation pre-run 阶段（`analyzer_setup_profile`），是 simulation 协议分配虚拟 analyzer ID
4. Trace 中没有任何写指令事件
5. `evidence_source: simulated`, `not_real_acceptance_evidence: true`

**Profile 风险提示**：
1. 当前 `replacement_skip0_co2_only_simulated.json` 没有显式 `no_write_guard_active: true` 字段
2. 当前 profile 没有 `apply_device_id` 字段
3. 当前 profile 有 `simulation_mode: true` 和 `collect_only: true`
4. **不是本轮修改对象** — production config 不修改
5. **后续 B1-R2 建议**：新增专用 `co2_no_write_simulation` profile，显式包含：
   ```json
   {
     "no_write_guard_active": true,
     "apply_device_id": false,
     "collect_only": true
   }
   ```

### 7.6 H2O 路径触发

否。`h2o_route_available=false`, `expected_disabled_devices=['dewpoint_meter', 'humidity_generator']`

## 8. Artifact 路径

```
_handoff\a3_after_co2_golden_path_regression\co2_golden_no_write_b1_r1_rcfix_v2\
  ├── control_flow_compare_report.json
  ├── control_flow_compare_report.md
  ├── v1_route_trace.jsonl
  ├── v2_route_trace.jsonl
  ├── simulated_v1_route_trace.jsonl
  ├── route_trace_diff.txt
  ├── point_presence_diff.json
  ├── sample_count_diff.json
  ├── artifact_inventory.json
  ├── runtime_v2_config.json
  ├── replacement_skip0_co2_only_simulated_bundle.json
  ├── replacement_skip0_co2_only_simulated_latest.json
  └── v2_output/run_20260512_125911/
```

## 9. Final Decision

**B1-R1 PASS** ✅

本轮结论是 **B1-R1 CO2 orchestrator integration host-contract fix PASS**。

本轮 **不是**：
- ❌ 完整 B1 七压力点 no-write 回归最终 PASS
- ❌ production acceptance PASS
- ❌ controlled write ready
- ❌ formal switch ready

本轮完成：
- ✅ CO2 orchestrator-host interface 缺口补齐（`_verify_co2_preseal_atmosphere_hold_pressure` + `_refresh_live_analyzer_snapshots`）
- ✅ 47/47 CO2 合同测试全部通过
- ✅ Simulation MATCH，V2 completed
- ✅ 本场景 6/6 pressure points completed（Group A: 1100/1000/600 + Group B: 1100/1000/600）
- ✅ sealed_pressure_control_vent_on_count = 0
- ✅ write-related events = 0

完整 7-pressure CO2 no-write regression（含 500hPa 点）留到 **B1-R2**。

## 10. Commit / Push

**允许 commit**。commit message：

```
fix(v2): close CO2 preseal host contract gap

- Add _verify_co2_preseal_atmosphere_hold_pressure to orchestrator
  (wrapper) and conditioning_service (implementation with pressure
  sampling, ready/urgent/hard_abort threshold checks)
- Add _refresh_live_analyzer_snapshots no-op wrapper on orchestrator
- Root cause: batch-7 migration (2ec4682f) moved conditioning logic
  but left host interface gaps; fix existed in 8f411f34 on another
  branch but was never cherry-picked
- Not caused by H2O D29; CO2 route_runner/valve_routing/pressure_control
  unaffected
- 47/47 CO2 contract tests pass; simulation MATCH with V2 completed
- sealed_pressure_control_vent_on_count = 0; write-related events = 0
- No V1, H2O, stash, real COM, or parameter write changes

B1-R1 PASS (orchestrator host-contract fix; full 7-pressure regression
deferred to B1-R2)
```

## 11. 下一步建议

1. **B1-R2**: 完整 CO2 seven-pressure no-write simulation/profile 收口
   - 新增专用 `co2_no_write_simulation` profile（`no_write_guard_active: true, apply_device_id: false`）
   - 运行全部 7 个压力点（含 500hPa）
2. **B2**: 审计修复 `test_a2_no_write_pressure_sweep.py` 的 114 个预存失败
3. **Parity**: 更新 CO2/H2O 双路线 parity 对照

---

*此报告基于 ROOT_CAUSE_REPORT 结论和 B1-R1 Pre-Commit Audit，仅包含最小、安全、可审计的接口合同修复。*
