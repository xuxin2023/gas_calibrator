# B2 Analyzer Setup Write Classification V1

**生成时间**: 2026-05-12  
**仓库**: `D:/gas_calibrator`  
**分支**: `codex/v2-golden-recovery-cdb82111`  
**HEAD**: `0885a1a7`  
**审计对象**: `src/gas_calibrator/v2/core/services/analyzer_fleet_service.py`  
**上游**: `B2_NO_WRITE_BOUNDARY_CALLSITE_AUDIT_V1.md`  
**角色**: `engineering_probe_only` / `not_real_acceptance_evidence=true` / `promotion_state=blocked`

---

## 1. Scope

1. 这是 **B2 analyzer setup write classification V1**，对 `AnalyzerFleetService` 的全部设备写入命令进行分类。
2. 不是 runtime 修改，不是参数写入，不是真机执行。
3. 只做分类和建议合同，不改代码。
4. 后续 B2-P4 基于本分类做 guard contract 设计。

---

## 2. AnalyzerFleetService 写入命令全量分类

### 2.1 `apply_analyzer_setup()` 入口

```
apply_analyzer_setup()
├── ① _apply_device_id_to_analyzer()     → set_device_id/set_id/write_device_id/assign_device_id
├── ② _run_mode2_init_sequence()          → set_comm_way, set_mode, set_average_filter_channel, set_average_filter
└── ③ _apply_basic_gas_analyzer_settings() → set_mode, set_comm_way, set_active_freq, set_average_filter, set_average
```

### 2.2 逐命令分类

| # | 命令 | 调用位置 | 当前 guard 状态 | 分类 | 在 no-write probe 中是否被发送 | 说明 |
|---|------|---------|:--:|:--:|:--:|------|
| 1 | `set_device_id` / `set_id` / `write_device_id` / `assign_device_id` | `_apply_device_id_to_analyzer()` | ✅ `EXACT_BLOCKED_METHODS` + `no_write_guard_active` → `apply_device_id = False` | **B: forbidden identity write** | ❌ 被阻断（config skip + proxy block） | 双重保护生效 |
| 2 | `set_comm_way` | `_run_mode2_init_sequence()` + `_apply_basic_gas_analyzer_settings()` | ❌ 不在 blocked 列表 | **C: analyzer runtime setup** | ✅ 会被发送 | 切换分析仪被动/主动上传模式 |
| 3 | `set_mode` | `_run_mode2_init_sequence()` + `_apply_basic_gas_analyzer_settings()` | ❌ 不在 blocked 列表 | **C: analyzer runtime setup** | ✅ 会被发送 | 设置分析仪工作模式（如 MODE2） |
| 4 | `set_average_filter_channel` | `_run_mode2_init_sequence()` | ❌ 不在 blocked 列表 | **C: analyzer runtime setup** | ✅ 会被发送 | 设置指定通道的平均滤波次数 |
| 5 | `set_average_filter` | `_run_mode2_init_sequence()` + `_apply_basic_gas_analyzer_settings()` | ❌ 不在 blocked 列表 | **C: analyzer runtime setup** | ✅ 会被发送 | 设置平均滤波次数 |
| 6 | `set_active_freq` | `_apply_basic_gas_analyzer_settings()` | ❌ 不在 blocked 列表 | **C: analyzer runtime setup** | ✅ 会被发送 | 设置 FTD 主动上传频率 |
| 7 | `set_average` | `_apply_basic_gas_analyzer_settings()` | ❌ 不在 blocked 列表 | **C: analyzer runtime setup** | ✅ 会被发送 | 设置 CO2/H2O 的平均次数 |

---

## 3. 分类判决

### 3.1 已正确阻止的写入（Category B）

| 命令 | 阻止机制 | 有效性 |
|------|---------|:--:|
| `set_device_id` / `set_id` / `write_device_id` / `assign_device_id` | `no_write_guard_active` → `apply_device_id = False`（config 层面 skip）+ `EXACT_BLOCKED_METHODS`（proxy 层面 block） | ✅ 双重保护 |

判决：**identity write 已被正确阻止。** `apply_analyzer_setup()` 中 `no_write_guard_active` 为 True 时，`apply_device_id` 被设为 False，`_apply_device_id_to_analyzer()` 不会被调用。

### 3.2 未被阻止的写入（Category C）

| 命令组 | 包含方法 | 当前 guard 状态 |
|--------|---------|:--:|
| MODE2 init sequence | `set_comm_way`, `set_mode`, `set_average_filter_channel`, `set_average_filter` | ❌ **未阻止** |
| basic settings | `set_mode`, `set_comm_way`, `set_active_freq`, `set_average_filter`, `set_average` | ❌ **未阻止** |

**为什么未被阻止**：
1. 这些方法名不含 `WRITE_VERBS`（set/write/apply/commit/save/store/update）+ `CALIBRATION_TERMS`（coeff/senco/zero/span/calibration）组合。
2. 方法名 `set_mode` 中 `mode` 不是 calibration term；`set_comm_way` 中 `comm_way` 不是 calibration term。
3. 这些方法不在 `EXACT_BLOCKED_METHODS` 集合中。
4. 通过 `_call_with_optional_ack()` 间接调用，不会触发 `NoWriteDeviceProxy.__getattr__` 的 block 逻辑（因为 `_call_with_optional_ack` 本身不在 blocked 列表中，且 `_first_method` 返回的是底层 analyzer 设备的方法引用，绕过了 proxy 的属性拦截——需要进一步确认 proxy 是否包装了 analyzer device）。

**关键问题**：`_first_method(analyzer, ("set_mode_with_ack", "set_mode"))` 返回的 `method` 是通过 `getattr` 从 analyzer 对象获取的。如果 analyzer 被 `NoWriteDeviceProxy` 包装，那么 `getattr` 会经过 `__getattr__`，其中会调用 `is_blocked_write_method("set_mode")`。`set_mode` 不含任意 `WRITE_VERB` 作为前缀（`set` 是动词但 `mode` 不是 `CALIBRATION_TERMS`），所以 `is_blocked_write_method` 返回 `False`。

**判决**：
- `set_mode`, `set_comm_way`, `set_active_freq`, `set_average_filter`, `set_average` **会被发送到分析仪**。
- 这些是 **analyzer runtime setup commands**，不等同于校准参数写入（zero/span/coefficient/SENCO）。
- 但它们是**真实设备写入**，应与校准参数写入区分记录。

---

## 4. Analyzer Runtime Setup 是否应在 no-write probe 中允许

### 4.1 当前事实

| 问题 | 答案 |
|------|------|
| MODE2 init 是否写校准参数 | ❌ 否（只设置通信模式、工作模式、滤波参数） |
| MODE2 init 是否改变分析仪行为 | ✅ 是（从被动模式切换到 MODE2 主动上传或改变上传频率） |
| MODE2 init 是否持久化存储 | ⚠️ 取决于分析仪固件（大部分分析仪 MODE2 设置是运行时配置，断电后恢复默认） |
| MODE2 init 是否影响后续采样数据 | ✅ 是（avg_filter/avg 设置直接影响采样输出的平滑度和数据频率） |
| 允许 MODE2 init 是否会误导 no-write 含义 | ⚠️ 是 — trace 中无 `blocked_write_events` 记录，但设备命令已发送 |

### 4.2 分类结论

| 命令 | 写入类型 | 是否校准参数 | 是否持久化 | 建议处理 |
|------|:--:|:--:|:--:|------|
| `set_mode` | runtime config | ❌ | ⚠️ 不确定 | **允许但必须 evidence 标记** |
| `set_comm_way` | runtime config | ❌ | ⚠️ 不确定 | **允许但必须 evidence 标记** |
| `set_active_freq` | runtime config | ❌ | ⚠️ 不确定 | **允许但必须 evidence 标记** |
| `set_average_filter` | runtime config | ❌ | ⚠️ 不确定 | **允许但必须 evidence 标记** |
| `set_average` | runtime config | ❌ | ⚠️ 不确定 | **允许但必须 evidence 标记** |
| `set_device_id` | identity write | ❌ | ✅ 大概率持久 | **禁止**（已 guard） |

---

## 5. Suggested Guard Contract（建议合同，非本轮修改）

### 5.1 方案 A：允许 analyzer runtime setup，显式 evidence 标记

```
if no_write_guard_active and engineering_probe:
    allow: set_mode, set_comm_way, set_active_freq, set_average_filter, set_average
    block: set_device_id, set_senco, set_zero, set_span, set_coefficient, ...
    evidence: record each runtime_setup_command_sent with action + timestamp
```

**优点**：保持当前行为，不破坏 analyzer 正常初始化流程。  
**缺点**：no-write guard 含义需要从 "zero device commands sent" 修正为 "no calibration/identity write; runtime setup permitted with evidence"。

### 5.2 方案 B：no-write probe 中禁用 analyzer runtime setup，改成 preflight check only

```
if no_write_guard_active:
    skip: _run_mode2_init_sequence, _apply_basic_gas_analyzer_settings
    replace with: preflight read-only check (verify analyzer is already in correct mode/comm_way/freq/filter)
```

**优点**：真正的 "no device write at all"。  
**缺点**：可能改变现有 simulation/real probe 行为；需要 readback + verify 逻辑。

### 5.3 建议

**推荐方案 A**（允许 runtime setup，显式标记），原因：
1. analyzer runtime setup 不是校准参数写入，不影响分析仪的校准系数。
2. 这些命令在每次校准运行开始时执行，是 analyzer 正常工作的前提。
3. 完全禁用可能导致 MODE2 数据无法正确读取。
4. 关键是 **用 evidence 区分**：`runtime_setup_command_sent` vs `calibration_write_command_sent`。

### 5.4 Evidence 合同草案

```python
# 在 no_write_guard 中新增 event category
RUNTIME_SETUP_METHODS = {
    "set_mode", "set_mode_with_ack",
    "set_comm_way", "set_comm_way_with_ack",
    "set_active_freq", "set_active_freq_with_ack",
    "set_average_filter", "set_average_filter_with_ack",
    "set_average", "set_average_with_ack",
    "set_average_filter_channel", "set_average_filter_channel_with_ack",
}

# 在 NoWriteGuard.record_runtime_setup() 中记录而非阻止
def record_runtime_setup(self, ..., success: bool):
    self.runtime_setup_events.append({...})
```

---

## 6. 与 H2O Probe 的关系

H2O D29 R4/R5 probe 使用了真实 COM，分析仪 setup 命令已被发送（因为 no-write guard 只阻止 calibration/identity write）。当前 H2O evidence 中 `attempted_write_count=0` 是正确的，因为 analyzer runtime setup 未被计入 blocked write events。

但这不意味着 setup 命令没发送——它们确实发送了，只是不被视为"校准写入"。

---

## 7. Next Step

**`B2-P4: analyzer runtime setup guard contract`**

目标：
1. 明确 `set_mode/set_comm_way/set_active_freq/set_average_filter/set_average` 在 no-write probe 中的允许/禁止合同。
2. 如果允许：新增 `runtime_setup_command_sent` event category 到 `NoWriteGuard`。
3. 如果禁止：实现 preflight check 或 config flag 跳过。
4. 更新 `no_write_guard.json` artifact 输出结构。
5. 更新 B1/B2 evidence registry 中 no-write 含义的说明。

**注意**：当前是 B2 审计阶段，B2-P4 的代码修改需要用户单独授权。

---

*B2 Analyzer Setup Write Classification V1 结束。*
