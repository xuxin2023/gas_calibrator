# CO2 保护门离线检查报告

**基线**: `e9b0bb95`  
**分支**: `codex/v2-golden-recovery-cdb82111`  
**检查时间**: 2026-05-12  
**检查方式**: 静态 git diff 分析 + 离线 pytest 测试  
**分类**: `engineering_probe_only` / `not_real_acceptance_evidence=true`

---

## 1. 检查方法

不跑真机，只做两件事：
1. **静态 git diff 分析**：确认 D29 阶段所有 commit 是否触碰了 CO2 相关文件
2. **离线 pytest 测试**：运行已有测试套件，确认 H2O 修改未破坏现有合同

检查范围：`5bc4fa2c` → `e9b0bb95` 之间的所有 commit (含 034b2d6b, 171e530c, e9b0bb95)

---

## 2. CO2 Runner 触碰检查

### 2.1 `co2_route_runner.py`

```bash
$ git diff 5bc4fa2c..e9b0bb95 -- src/gas_calibrator/v2/core/runners/co2_route_runner.py
(无输出)
```

| 检查项 | 结果 |
|--------|:--:|
| 是否触碰 CO2 runner | **NO** ✅ |
| 文件是否在 diff 中 | **NO** — 零改动 |

### 2.2 CO2 主链相关文件

```bash
$ git diff 5bc4fa2c..e9b0bb95 -- src/gas_calibrator/v2/core/runners/ | findstr co2
(无输出 — 唯一被修改的文件只有 h2o_route_runner.py)
```

`034b2d6b` 实际修改的 runner 文件：仅 `h2o_route_runner.py`

| 检查项 | 结果 |
|--------|:--:|
| 是否触碰 CO2 route 主链 | **NO** ✅ |
| 唯一被修改的 runner | `h2o_route_runner.py` |

---

## 3. Valve Routing Service 触碰检查

```bash
$ git diff 5bc4fa2c..e9b0bb95 -- src/gas_calibrator/v2/core/services/valve_routing_service.py
(无输出)
```

| 检查项 | 结果 |
|--------|:--:|
| 是否触碰 valve_routing_service | **NO** ✅ |
| stash@{0} 中的 valve_routing_service 改动是否恢复 | **NO** — stash 完整保留 |

---

## 4. Flow LPM / UI / Shared-Service Stash 状态

```bash
$ git stash list -n 3
stash@{0}: On codex/v2-golden-recovery-cdb82111: D29-R4 unrelated: flow_lpm removal + test contract updates NOT part of 5bc4fa2c
stash@{1}: WIP on codex/run001-a1-no-write-dry-run: ...
```

| 检查项 | 结果 |
|--------|:--:|
| stash@{0} 是否仍存在 | **YES** ✅ |
| 本轮是否恢复 stash | **NO** ✅ |
| stash 中的 flow_lpm/UI/shared-service 改动是否进入 baseline | **NO** ✅ |

---

## 5. Pressure Control Service 修改范围确认

### 5.1 5bc4fa2c 修改的精确定位

```bash
$ git show 5bc4fa2c -- src/gas_calibrator/v2/core/services/pressure_control_service.py | findstr "route_text h2o"
```

所有 `5bc4fa2c` 在 `pressure_control_service.py` 中的新增代码均以以下 guard 开始：

```python
if route_text == "h2o" and prefer_direct_vent_close:
    # H2O vent closed verification + 1.5s wait + set_h2o_path(False)
```

以及 `route_text == "h2o"` 的 `_set_h2o_path(False)` 调用。

### 5.2 隔离分析

| 隔离层 | 机制 | CO2 是否满足条件 |
|--------|------|:--:|
| 路由类型 guard | `route_text == "h2o"` | **NO** — CO2 的 route_text 是 `"co2"` |
| 参数 guard | `prefer_direct_vent_close` 必须 truthy | **NO** — CO2 runner 不传此参数 |
| 函数调用 guard | `_set_h2o_path(False)` | **NO** — CO2 使用 `set_valves_for_co2` |

### 5.3 代码调用路径对比

| 路径 | H2O | CO2 |
|------|:--:|:--:|
| `pressurize_and_hold()` 调用 | `pressurize_and_hold(lead, route=phase, prefer_direct_vent_close=True)` | `pressurize_and_hold(point, route=phase)` |
| 是否会进入新增 vent 验证逻辑 | ✅ 会 | ❌ 不会 |
| 是否会触发 `set_h2o_path(False)` | ✅ 会 | ❌ 不会 |

**结论**：5bc4fa2c 修改的激活条件与 CO2 路径**完全正交**。

| 检查项 | 结果 |
|--------|:--:|
| 是否放松 CO2 sealed no-vent guard | **NO** ✅ |
| 修改是否限定在 H2O + prefer_direct_vent_close 路径 | **YES** ✅ |

---

## 6. 离线测试结果

| 测试套件 | 命令 | 结果 |
|----------|------|:--:|
| CO2 route runner | `pytest tests/v2/test_co2_route_runner.py -q` | **4 passed** ✅ |
| H2O golden sequence | `pytest tests/v2/test_h2o_golden_sequence.py -q` | 合并执行 |
| H2O vent-off adapter contract | `pytest tests/v2/test_h2o_vent_off_adapter_contract.py -q` | 合并执行 |
| H2O golden + vent-off | `pytest tests/v2/test_h2o_golden_sequence.py tests/v2/test_h2o_vent_off_adapter_contract.py -q` | **24 passed** ✅ |
| Pressure control service | `pytest tests/v2/test_pressure_control_service.py -q` | **15 passed** ✅ |

**总计**: **43 tests passed, 0 failed**

存在的测试文件（本轮未单独执行，文件已确认存在）：
- `tests/v2/test_h2o_vent_behavior_characterization.py` ✅ 存在
- `tests/v2/test_h2o_runner_keepalive_adapter_contract.py` ✅ 存在
- `tests/v2/test_co2_no_vent_guard.py` ✅ 存在
- `tests/v2/test_h2o_pressure_direct_vent.py` ✅ 存在

---

## 7. CO2 保护门综合判断

| # | 判定 | 结果 |
|---|------|:--:|
| 1 | 是否触碰 CO2 runner | **NO** ✅ |
| 2 | 是否触碰 CO2 主链 | **NO** ✅ |
| 3 | 是否触碰 valve_routing_service | **NO** ✅ |
| 4 | 是否恢复 high-risk stash | **NO** ✅ |
| 5 | 是否放松 CO2 sealed no-vent guard | **NO** ✅ |
| 6 | 5bc4fa2c 修改是否仅限 H2O 路径 | **YES** ✅ |
| 7 | 离线测试是否全部通过 | **YES** ✅ (43/43) |
| 8 | 是否建议 A3 后做 CO2 no-write 单路回归 | **YES** ✅ — 保守锁门 |
| 9 | 是否允许跳过 CO2 保护门 | **NO** ❌ |

---

## 8. CO2 保护门结论

```
╔══════════════════════════════════════════════╗
║                                              ║
║   CO2 保护门离线检查:                          ║
║                                              ║
║   GATE = PASS ✅                              ║
║                                              ║
║   CO2 路径未受 D29 修改影响                    ║
║   43 离线测试全部通过                          ║
║   A3 后建议跑 CO2 no-write 单路回归            ║
║                                              ║
╚══════════════════════════════════════════════╝
```

---

**CO2 保护门离线检查报告结束。结论: PASS。CO2 路径不受 H2O D29 修改影响，建议 A3 后进行 CO2 golden path no-write 回归作为最终锁门。**
