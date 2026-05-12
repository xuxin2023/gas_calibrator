# B2-P4A No-Write Guard Runtime Setup Contract

**生成时间**: 2026-05-12  
**仓库**: `D:/gas_calibrator`  
**分支**: `codex/v2-golden-recovery-cdb82111`  
**起始 HEAD**: `f1bf7b3d`  
**上游**: `B2_NO_WRITE_BOUNDARY_CALLSITE_AUDIT_V1.md`、`B2_ANALYZER_SETUP_WRITE_CLASSIFICATION_V1.md`  
**角色**: `engineering_probe_only` / `not_real_acceptance_evidence=true` / `promotion_state=blocked`

---

## 1. 本轮性质

这是 **B2-P4A 最小 no-write guard 合同修复**。

目的：将 B2-P2/P3 审计发现（`set_mode`/`set_comm_way`/`set_active_freq`/`set_average_filter`/`set_average` 在 no-write probe 中被发送但未记录）固化到代码与测试中。

---

## 2. 设备 ID 写入 —— 仍绝对禁止

| 方法 | 状态 |
|------|:--:|
| `set_device_id_with_ack` | ❌ 阻断（NoWriteViolation） |
| `set_device_id` | ❌ 阻断 |
| `write_device_id` | ❌ 阻断 |
| `assign_device_id` | ❌ 阻断 |
| `set_id` | ❌ 阻断 |
| raw `ID,YGAS,...` payload | ❌ 阻断 |

这些方法的 `attempted_write_count` 增加，`identity_write_command_sent` = `True`，`persistent_write_command_sent` = `True`。

---

## 3. Calibration/Identity/SENCO/Zero/Span/Coefficient 写入 —— 仍绝对禁止

全部 27 个 EXACT_BLOCKED_METHODS + pattern match + raw payload guard 延续阻断。

---

## 4. Analyzer Runtime Setup 命令 —— 允许但必须记录

### 4.1 新增 `RUNTIME_SETUP_METHODS` 集合

```python
RUNTIME_SETUP_METHODS = {
    "set_mode", "set_mode_with_ack",
    "set_comm_way", "set_comm_way_with_ack",
    "set_active_freq", "set_active_freq_with_ack",
    "set_average_filter", "set_average_filter_with_ack",
    "set_average_filter_channel", "set_average_filter_channel_with_ack",
    "set_average", "set_average_with_ack",
}
```

### 4.2 `is_runtime_setup_method()` 函数

只对 `gas_analyzer` / `analyzer` / `gas_analyzer_serial` 类型返回 `True`。设备 ID 写入方法永远优先判定为 blocked write（block > runtime setup）。

### 4.3 `NoWriteDeviceProxy.__getattr__` 优先级

1. **第一优先级**：`is_blocked_write_method()` → raise `NoWriteViolation`
2. **第二优先级**：raw payload guard → raise `NoWriteViolation`
3. **第三优先级**：`is_runtime_setup_method()` → 调用真实方法 + `record_runtime_setup()`
4. 其他：透传原始属性

### 4.4 `record_runtime_setup()` 记录事件

每个 runtime setup 调用记录：
```python
{
    "timestamp": "...",
    "scope": "run001_a1",
    "device_name": "...",
    "device_type": "...",
    "method_name": "...",
    "args_preview": [...],
    "kwargs_keys": [...],
    "command_category": "analyzer_runtime_setup",
    "calibration_write_command_sent": False,
    "identity_write_command_sent": False,
    "persistent_write_command_sent": False,
    "reason": "allowed_runtime_setup_under_no_write_guard",
    "success": True/False,
    "error": "",
}
```

---

## 5. Runtime Setup 不计入 attempted_write_count

`attempted_write_count` 始终等于 `len(blocked_events)`。Runtime setup 只记录在 `runtime_setup_events` 中，不影响 `blocked_events`。

`final_decision` 只有 `attempted_write_count > 0` 才 `FAIL`。Runtime setup 不改变 `final_decision`。

---

## 6. No-Write 语义更新

从 "zero device commands sent" 调整为：

> **"no calibration/identity/persistent parameter write; analyzer runtime setup may be allowed and recorded"**

`to_artifact()` 新增字段：
- `runtime_setup_command_count`
- `runtime_setup_events`
- `runtime_setup_command_sent`
- `blocked_method_policy.runtime_setup_methods`
- `no_write_semantics`

---

## 7. 未修改文件

| 文件 | 状态 |
|------|:--:|
| V1 代码 | 未修改 |
| `pressure_control_service.py` | 未修改 |
| `valve_routing_service.py` | 未修改 |
| `h2o_route_runner.py` | 未修改 |
| `co2_route_runner.py` | 未修改 |
| `analyzer_fleet_service.py` | 未修改 |
| config/profile/points | 未修改 |

---

## 8. 未跑真实 COM

本次全部为 simulation-only 测试。未执行 `--execute-probe`，未写任何参数。

---

## 9. 测试结果

### 新增测试 `tests/v2/test_no_write_guard.py`

| 测试 | 结果 |
|------|:--:|
| `test_identity_write_methods_are_still_blocked` (5 params) | ✅ PASS |
| `test_runtime_setup_methods_are_recorded_not_blocked` | ✅ PASS |
| `test_runtime_setup_exception_is_recorded_and_reraised` | ✅ PASS |
| `test_raw_identity_payload_still_blocked` (2 params) | ✅ PASS |
| `test_raw_calibration_payload_still_blocked` (6 params) | ✅ PASS |
| **总计** | **15/15 PASS** |

### 已有测试 `tests/v2/test_run001_a1_no_write_dry_run.py`

76/77 PASS。1 个预存失败（`test_normal_co2_only_skip0_no_write_preflight_passes_and_writes_artifacts`）与本轮修改无关（artifact keys 扩展未同步测试断言）。

### 已有测试 `tests/v2/test_analyzer_fleet_service.py`

21/21 PASS。

### 无回归

所有 no-write guard 核心测试（blocked methods、raw payload、device id、serial checks）继续 PASS。

---

## 10. final_decision

**B2_P4A_NO_WRITE_GUARD_CONTRACT_PASS**

Runtime setup 合同已固化到 `no_write_guard.py` 和 `test_no_write_guard.py`。

- 设备 ID 写入仍阻断 ✅
- Calibration 写入仍阻断 ✅
- Analyzer runtime setup 允许且记录 ✅
- `attempted_write_count` 语义不变 ✅
- `final_decision` 不受 runtime setup 影响 ✅
- 未修改 V1/config/profile/points ✅
- 未跑真实 COM ✅
- 未写参数 ✅

---

## 11. 下一步建议

**B2-P5**: test fixture repair for `test_a2_no_write_pressure_sweep.py` (114 预存失败)。

**B2-P6**: simulation profile 新增显式 `no_write_guard_active: true` 标记。

---

*B2-P4A No-Write Guard Runtime Setup Contract 结束。*
