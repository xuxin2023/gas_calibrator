# B2 Test Fixture and Profile Gap Plan V1

**生成时间**: 2026-05-12  
**仓库**: `D:/gas_calibrator`  
**分支**: `codex/v2-golden-recovery-cdb82111`  
**HEAD**: `0885a1a7`  
**上游**: `B2_NO_WRITE_BOUNDARY_CALLSITE_AUDIT_V1.md`、`B2_ANALYZER_SETUP_WRITE_CLASSIFICATION_V1.md`  
**角色**: `engineering_probe_only` / `not_real_acceptance_evidence=true` / `promotion_state=blocked`

---

## 1. Scope

1. 这是 **B2 test fixture 与 profile gap plan V1**。
2. 不做 runtime 修改，不做 pytest 修改。
3. 只做根因分析 + 修复方案设计 + 下一步任务拆分。
4. 覆盖 3 个 gap：
   - test_a2_no_write_pressure_sweep.py 114 个预存失败
   - B1-R1 simulation profile 未显式 `no_write_guard_active=true`
   - profile 显式性审计建议

---

## 2. test_a2_no_write_pressure_sweep.py: 114 预存失败根因分析

### 2.1 测试文件创建方式

```python
# test_a2_no_write_pressure_sweep.py 中 mock orchestrator 的创建方式
orchestrator = WorkflowOrchestrator.__new__(WorkflowOrchestrator)
# 然后 monkeypatch 注入各种 mock 属性和 service
```

**关键问题**：`__new__` 只创建对象但不调用 `__init__`。因此 orchestrator 缺少 `__init__` 中创建的关键属性。

### 2.2 根因拆解

#### Root Cause 1: Mock orchestrator 缺少 `conditioning_service`（约 70 个失败）

真实 `WorkflowOrchestrator.__init__` 中：

```python
# orchestrator.py:112-113
self.a2_hooks = A2Hooks()
self.conditioning_service = ConditioningService(host=self)
```

但 `__new__` + 手动 monkeypatch 的 mock orchestrator **没有 `conditioning_service` 属性**。

当测试调用 `orchestrator._begin_a2_co2_route_conditioning_at_atmosphere()` 等方法时（这些方法是 orchestrator 上 conditioning 方法的 wrapper），它们会转发到 `self.conditioning_service`：

```python
# orchestrator.py:1709
def _begin_a2_co2_route_conditioning_at_atmosphere(self, point, pressure_points):
    return self.conditioning_service._begin_a2_co2_route_conditioning_at_atmosphere(point, pressure_points)
    #     ^^^^^^^^^^^^^^^^^^^^^^^^^^ → AttributeError: 'WorkflowOrchestrator' object has no attribute 'conditioning_service'
```

**影响的测试**：所有经过 `orchestrator → conditioning_service` 转发链的测试（约 70 个），包括：
- `_begin_a2_co2_route_conditioning_at_atmosphere`
- `_wait_co2_route_soak_before_seal`
- `_a2_conditioning_pressure_source_mode`
- `_a2_conditioning_vent_heartbeat_interval_s`
- 以及其他约 60 个 wrapper 方法

#### Root Cause 2: Mock SimpleNamespace 缺少 `a2_hooks`（约 35 个失败）

```python
# test_a2_no_write_pressure_sweep.py:348
orchestrator.pressure_control_service = SimpleNamespace(
    _current_high_pressure_first_point_sample=lambda **kwargs: {...},
    _remember_ambient_reference_pressure=lambda *args, **kwargs: remembered.append({...}),
)
```

`SimpleNamespace` 被用作 mock service 但没有 `a2_hooks` 属性。当 orchestrator 方法尝试访问 `self.a2_hooks.co2_route_conditioning_at_atmosphere_context` 等时失败：

```python
# orchestrator.py:2687
context = self.a2_hooks.co2_route_conditioning_at_atmosphere_context
#         ^^^^^^^^^^^^ → AttributeError: 'WorkflowOrchestrator' object has no attribute 'a2_hooks'
```

**影响的测试**：所有直接访问 `a2_hooks` 属性但不经过 `conditioning_service` 转发链的测试（约 35 个）。

### 2.3 修改前 = 修改后确认

| 时间点 | 失败数 | 通过数 |
|--------|:--:|:--:|
| `eb605517`（B1-R1 修复前） | 114 | 30 |
| `c62093c4`（B1-R1 修复后） | 114 | 30 |

结论：B1-R1 修复（orchestrator + conditioning_service）**未引入新失败，也未修复这 114 个预存失败**。这 114 个失败是测试 fixture 设计问题，与 B1-R1 runtime 修改无关。

### 2.4 是否阻塞 B2/B3

| 问题 | 答案 |
|------|------|
| 是否阻塞 B2 evidence registry | ❌ 否（registry 只看证据存在性，不跑测试） |
| 是否阻塞 B2 acceptance checklist | ❌ 否（checklist 是文档，不依赖测试通过） |
| 是否阻塞 real A2 probe | ❌ 否（real probe 走真实 orchestrator，不走 mock） |
| 是否阻塞 simulation | ❌ 否（simulation 走真实 orchestrator，不走 mock） |
| 是否阻塞 contract tests | ❌ 否（contract tests 在独立测试文件中，47/47 PASS） |
| 是否影响保护链 | ❌ 否（A2 protection tests 51/51 PASS，在 `test_a2_co2_only_7_pressure_no_write_probe.py` 中） |

**判决**：这 114 个失败是测试质量 debt，不是功能缺陷，CURRENTLY NOT BLOCKING。

---

## 3. test_a2_no_write_pressure_sweep.py: 修复方案设计（仅设计，不修改代码）

### 3.1 修复策略

| 策略 | 说明 | 推荐度 |
|------|------|:--:|
| **策略 1**: 创建 `_conditioning_guard_orchestrator` 时添加 `conditioning_service` mock | 最小改动，匹配现有 monkeypatch 模式 | ⭐⭐⭐ 推荐 |
| 策略 2: 重写为 `create_autospec(WorkflowOrchestrator)` | 自动处理缺失属性 | ⭐⭐ 工作量大 |
| 策略 3: 单独修复每个测试的 monkeypatch | 逐个补丁 | ⭐ 不可维护 |

### 3.2 策略 1 详细设计

#### 修复 Root Cause 1: 添加 `conditioning_service`

```python
# 在 _high_pressure_orchestrator 和 _conditioning_guard_orchestrator 中添加
from gas_calibrator.v2.core.services.conditioning_service import ConditioningService

# 或者创建 Mock conditioning_service
conditioning_service = SimpleNamespace()
conditioning_service._begin_a2_co2_route_conditioning_at_atmosphere = lambda point, pressure_points: ...
conditioning_service._a2_conditioning_pressure_source_mode = lambda: "digital_gauge"
# ... 其他 wrapper 方法

orchestrator.conditioning_service = conditioning_service
```

**推荐方式**：因为 orchestrator 有约 60 个 wrapper 方法转发到 `conditioning_service`，逐个 mock 不可行。更好的方式是：

```python
from gas_calibrator.v2.core.services.conditioning_service import ConditioningService

# 在 orchestrator 上直接设置，但需要 mock 其内部的 host 依赖
conditioning_service = ConditioningService.__new__(ConditioningService)
conditioning_service.host = orchestrator  # host 引用
orchestrator.conditioning_service = conditioning_service
```

但 `conditioning_service.__init__` 中又会设置 `self._a2_pressure_policy` 等属性，需要进一步 mock。

**更实际的方式**：为 mock orchestrator 补充需要的 `conditioning_service` 方法的最小 mock：

```python
conditioning_mock = SimpleNamespace()
# 只 mock 当前测试实际调用的方法
conditioning_mock._verify_co2_preseal_atmosphere_hold_pressure = lambda point: "ok"
conditioning_mock._begin_a2_co2_route_conditioning_at_atmosphere = lambda *args, **kwargs: None
# ... 按需添加
orchestrator.conditioning_service = conditioning_mock
```

但这意味着需要为 114 个测试中涉及的约 60 个 wrapper 分别提供 mock，工作量大且脆弱。

#### 修复 Root Cause 2: 添加 `a2_hooks`

```python
from gas_calibrator.v2.core.a2_hooks import A2Hooks

orchestrator.a2_hooks = A2Hooks()
orchestrator._populate_a2_hooks_callbacks()
```

这会设置 `a2_hooks` 及其 callbacks，但 callbacks 中的方法（如 `_mark_a2_co2_route_open_command_write_started`）也需要在 orchestrator 上可用。

### 3.3 工作量估算

| 修复项 | 影响测试数 | 估计工作量 |
|--------|:--:|:--:|
| Root Cause 1 (conditioning_service) | ~70 | 高 — 需要 mock ~60 wrapper 方法或重构测试入口 |
| Root Cause 2 (a2_hooks) | ~35 | 中 — 添加 `A2Hooks()` + callbacks |
| 总计 | 114 | 独立任务（B2-P5），不建议与 runtime 修改混在一起 |

### 3.4 建议

**将 test fixture 修复作为独立后置任务（B2-P5），不在 B2-P2/P3 中执行。**

原因：
1. 114 个失败是测试 fixture mock 设计问题，不是功能缺陷。
2. 修复可能需要大量 mock 补充或测试架构调整，应单独做设计评审。
3. 不阻塞 B2 evidence/checklist/audit。
4. A2 protection tests (51 PASS) + CO2 contract tests (47 PASS) 已经覆盖了关键保护链。
5. 这些测试的 purpose 是验证 no-write pressure sweep 的各路径行为，mock 缺口修复后可以恢复为关键 regression gate。

---

## 4. B1-R1 Simulation Profile `no_write_guard_active` Gap

### 4.1 当前 Profile 状态

B1-R1 simulation 使用的 profile: `replacement_skip0_co2_only_simulated.json`

| 字段 | 值 | 说明 |
|------|-----|------|
| `collect_only` | `true` | 全局 collect only |
| `simulation_mode` | `true` | simulation 模式 |
| `no_write_guard_active` | **不存在** | ⚠️ 未显式声明 |

### 4.2 对比：A2 Real Machine Config

`run001_a2_co2_only_7_pressure_no_write_real_machine.json`:

| 字段 | 值 | 说明 |
|------|-----|------|
| `run001_a2.no_write` | `true` | 全局 no-write |
| `run001_a2.allow_write_coefficients` | `false` | 明确禁止 |
| `run001_a2.allow_write_zero` | `false` | 明确禁止 |
| `run001_a2.allow_write_span` | `false` | 明确禁止 |
| `a2_co2_7_pressure_no_write_probe.no_write` | `true` | probe 级 no-write |
| `a2_co2_7_pressure_no_write_probe.analyzer_id_write_enabled` | `false` | 明确禁止 |
| `a2_co2_7_pressure_no_write_probe.senco_write_enabled` | `false` | 明确禁止 |
| `a2_co2_7_pressure_no_write_probe.calibration_write_enabled` | `false` | 明确禁止 |
| `workflow.collect_only` | `true` | 全局 collect only |
| `workflow.analyzer_setup.apply_device_id` | `false` | ID 写入禁止 |

### 4.3 Gap 分析

| 维度 | simulation profile | real machine config | 差异 |
|------|:--:|:--:|:--:|
| `collect_only` | ✅ `true` | ✅ `true` | 一致 |
| `simulation_mode` | ✅ `true` | N/A | simulation 独有 |
| `no_write` | ⚠️ 未显式 | ✅ `true`（多级） | simulation profile 缺少 |
| `allow_write_coefficients` | ⚠️ 未显式 | ✅ `false` | 缺少 |
| `allow_write_zero/span` | ⚠️ 未显式 | ✅ `false` | 缺少 |
| `apply_device_id` | ⚠️ 未显式 | ✅ `false` | 缺少 |
| `no_write_guard_active` | ⚠️ 字段不存在 | 由 `no_write` 推导 | 缺少显式字段 |

### 4.4 是否需要修复

| 问题 | 答案 |
|------|------|
| 当前 simulation 中是否有实际写入 | ❌ 否（simulation 无真实设备，所有 write 都是 no-op） |
| `collect_only=true` 是否足够 | ⚠️ 语义上足够（collect_only 含义就是只收不发），但缺少多级显式声明 |
| 是否有证据被误解的风险 | ⚠️ 低风险 — simulation 天然不带真实设备，但 profile 文档性不够 |
| 是否需要立即修复 | **P2** — 可以在 B2-P6 阶段做 profile 治理，不紧急 |

### 4.5 建议

**B2-P6 阶段为所有 simulation profile 新增显式 `no_write_guard_active: true` 标记。**

具体建议：
```json
{
  "no_write_guard_active": true,
  "allow_write_coefficients": false,
  "allow_write_zero": false,
  "allow_write_span": false,
  "apply_device_id": false
}
```

**不在本轮修改，只做审计标志。**

---

## 5. Profile No-Write 显式性审计总结

### 5.1 已审计 Profile

| Profile | `collect_only` | `no_write` 显式 | `allow_write_*` 显式 | `no_write_guard_active` 显式 | 评估 |
|---------|:--:|:--:|:--:|:--:|:--:|
| `run001_a2_co2_only_7_pressure_no_write_real_machine.json` | ✅ `true` | ✅ 多级 `true` | ✅ 全 `false` | ⚠️ 由 `no_write` 推导 | **完备** — 多级声明覆盖率最高 |
| `run001_h2o_only_1_point_no_write_real_machine.json` | ✅ `true` | ✅ 多级 `true` | ✅ `apply_device_id=false` | ✅ 由 config 读取 | **完备** |
| `replacement_skip0_co2_only_simulated.json` | ✅ `true` | ⚠️ 未显式 | ⚠️ 未显式 | ⚠️ 未显式 | **需补充** — P2 后置 |
| 其他 simulation profiles | ⚠️ 未逐份审计 | — | — | — | **B2-P6 逐份审计** |

### 5.2 建议合同

```
Profile 显式性合同：
1. 任何 no-write probe（simulation 或 real machine）必须在 profile 中显式声明：
   - "no_write": true
   - "allow_write_coefficients": false
   - "allow_write_zero": false
   - "allow_write_span": false
   - "allow_write_calibration_parameters": false
   - "apply_device_id": false
2. simulation profile 额外声明：
   - "no_write_guard_active": true（显式冗余，增强文档性）
   - "evidence_source": "simulated"
3. real machine profile 额外声明：
   - "mode": "real_machine_dry_run"
   - "not_real_acceptance_evidence": true
```

---

## 6. 任务拆分建议

| Task | Scope | 类型 | 何时执行 |
|------|-------|:--:|------|
| **B2-P4** | analyzer runtime setup guard contract | 设计/可能小代码修改 | B2 后续（需用户授权） |
| **B2-P5** | test fixture repair only（`test_a2_no_write_pressure_sweep.py` 114 失败修复） | 纯测试修改 | B2 后续（需用户授权） |
| **B2-P6** | profile explicit no-write marker | 纯 config/profile 修改 | B2 后续（需用户授权） |

**注意**：
- B2-P4 涉及 `no_write_guard.py` 修改，需要 code change 授权。
- B2-P5 不涉及 runtime，但涉及 114 个测试，工作量大。
- B2-P6 只改 profile JSON，最小风险。
- 所有 3 个任务都不需要在 B2-P2/P3 本轮中执行。

---

*B2 Test Fixture and Profile Gap Plan V1 结束。*
