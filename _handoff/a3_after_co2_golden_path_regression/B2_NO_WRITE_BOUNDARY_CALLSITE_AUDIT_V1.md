# B2 No-Write Boundary Callsite Audit V1

**生成时间**: 2026-05-12  
**仓库**: `D:/gas_calibrator`  
**分支**: `codex/v2-golden-recovery-cdb82111`  
**HEAD**: `0885a1a7`  
**审计方式**: 只读代码扫描（`git grep` + 人工逐文件核查）  
**角色**: `engineering_probe_only` / `not_real_acceptance_evidence=true` / `promotion_state=blocked`

---

## 1. Scope

1. 这是 **B2 no-write boundary callsite audit V1**，逐文件登记 V2 中所有疑似写入调用点。
2. 不是 runtime 修改，不是真机执行，不是参数写入。
3. 分类只基于代码静态分析 + 已有 no-write guard 机制。
4. 后续 B2 子任务（B2-P4+）将基于本审计做 guard contract 修复设计。

---

## 2. Callsite Registry

### 图例

| 列 | 含义 |
|---|------|
| File | 源文件路径 |
| Function | 方法/函数名 |
| Callsite | 实际调用代码 |
| Category | A-F 分类（见下） |
| Current Guard | 当前是否有 no-write guard 阻断 |
| Risk | 风险等级（P0/P1/P2） |
| Decision | 当前判决 |
| Next Action | 后续行动 |

### Category 定义

| Cat | 含义 | 是否允许在 no-write probe 中 |
|:--:|------|:--:|
| **A** | forbidden calibration write（zero/span/coefficient/SENCO） | ❌ 绝对禁止 |
| **B** | forbidden identity/SENCO write（set_device_id/write_device_id/assign_device_id/set_id） | ❌ 绝对禁止 |
| **C** | analyzer runtime setup command（set_mode/set_comm_way/set_active_freq/set_average_filter/set_average） | ⚠️ 当前未显式 guard；需后置合同 |
| **D** | pressure/valve physical control command（set_vent/set_pressure_target/open_valves/close_valves） | ✅ 允许（物理控制，非参数写入） |
| **E** | chamber/humidity device control command（set_temperature/start/stop） | ⚠️ 允许但需 evidence 标记 |
| **F** | artifact/report write only（文件导出、JSON 写入） | ✅ 允许（非设备写入） |
| **G** | test/mock only（pytest fixture mock 方法） | N/A（仅测试） |

---

### 2.1 Category A: Forbidden Calibration Write

| File | Function | Callsite | Current Guard | Risk | Decision | Next Action |
|------|---------|---------|:--:|:--:|------|------|
| `no_write_guard.py` | `EXACT_BLOCKED_METHODS` | `set_senco / set_coefficients / write_coefficients / set_coefficient / write_coefficient / write_zero / set_zero / zero_calibration / write_span / set_span / span_calibration / apply_calibration / commit_calibration / save_calibration / save_parameters / write_parameters / write_calibration_parameters / set_calibration_parameters / writeback / write_to_eeprom / write_eeprom / write_flash / write_nvm / commit_to_nvm / store_parameters / parameter_store_write` | ✅ `NoWriteDeviceProxy.__getattr__` 拦截 | **P0** | **已覆盖** — 全部 27 个精确方法名被阻断 | 无需修改 |

| File | Function | Callsite | Current Guard | Risk | Decision | Next Action |
|------|---------|---------|:--:|:--:|------|------|
| `no_write_guard.py` | pattern match | `WRITE_VERBS` + `CALIBRATION_TERMS` 组合匹配 | ✅ `is_blocked_write_method()` | **P0** | **已覆盖** — 变体方法名被阻断 | 无需修改 |

| File | Function | Callsite | Current Guard | Risk | Decision | Next Action |
|------|---------|---------|:--:|:--:|------|------|
| `no_write_guard.py` | raw payload guard | `RAW_CALIBRATION_COMMAND_TOKENS`（SENCO/COEFF/WRITECOEFF/WRITEZERO/WRITESPAN/APPLYCAL/CALIBRATION 等 28 个 token） | ✅ `is_blocked_raw_write_payload()` 拦截 `write()/query()/_send_config()` 等 raw 方法的 payload | **P0** | **已覆盖** | 无需修改 |

| File | Function | Callsite | Current Guard | Risk | Decision | Next Action |
|------|---------|---------|:--:|:--:|------|------|
| `coefficient_service.py` | `export_coefficient_report` | 调用 `export_ratio_poly_report()` 写文件 | N/A | **P2** | **非设备写入** — 仅写 report 文件到磁盘（Category F artifact） | 无需修改 |

**Category A 判决**: 所有已知校准参数写入点已被 `NoWriteDeviceProxy` + `EXACT_BLOCKED_METHODS` + pattern match + raw payload guard 覆盖。**无遗漏。**

---

### 2.2 Category B: Forbidden Identity/SENCO Write

| File | Function | Callsite | Current Guard | Risk | Decision | Next Action |
|------|---------|---------|:--:|:--:|------|------|
| `no_write_guard.py` | `IDENTITY_WRITE_METHODS` | `set_device_id_with_ack / set_device_id / write_device_id / assign_device_id / set_id` | ✅ 包含在 `EXACT_BLOCKED_METHODS` 中，被 `NoWriteDeviceProxy.__getattr__` 拦截 | **P0** | **已覆盖** | 无需修改 |
| `analyzer_fleet_service.py:279` | `_apply_device_id_to_analyzer` | 调用 `set_device_id/set_id/write_device_id/assign_device_id` | ✅ `NoWriteDeviceProxy.__getattr__` 会阻断（方法名在 `EXACT_BLOCKED_METHODS` 中） | **P0** | **已覆盖** | 无需修改 |
| `analyzer_fleet_service.py:331` | `apply_analyzer_setup` | `no_write_guard_active` 时强制 `apply_device_id = False` | ✅ `self._no_write_guard_active()` 检查 | **P0** | **已覆盖** — 双重保护：config 层面 skip + proxy 层面 block | 无需修改 |
| `no_write_guard.py` | raw identity payload guard | `RAW_IDENTITY_COMMAND_PREFIXES`（`ID,YGAS,`） | ✅ `is_blocked_raw_identity_write_payload()` | **P0** | **已覆盖** | 无需修改 |

**Category B 判决**: 所有 identity/SENCO 写入点已被三层覆盖（config skip → exact match block → raw payload guard）。**无遗漏。**

---

### 2.3 Category C: Analyzer Runtime Setup Commands

| File | Function | Callsite | Current Guard | Risk | Decision | Next Action |
|------|---------|---------|:--:|:--:|------|------|
| `analyzer_fleet_service.py:494-497` | `_run_mode2_init_sequence` | `set_comm_way(False)` → `set_mode(settings["mode"])` → `set_average_filter_channel(ch, settings["avg_filter"])` → `set_comm_way(True)` | ❌ **不在 `EXACT_BLOCKED_METHODS` 中** | **P1** | **未阻止** — set_mode/set_comm_way/set_average_filter_channel/set_average_filter 均不匹配校准 term，也不在精确 blocked 列表 | **B2-P4: analyzer runtime setup guard contract** |
| `analyzer_fleet_service.py:602-635` | `_apply_basic_gas_analyzer_settings` | `set_mode(settings["mode"])` → `set_comm_way(settings["active_send"])` → `set_active_freq(settings["ftd_hz"])` → `set_average_filter(settings["avg_filter"])` → `set_average(settings["avg_co2"], settings["avg_h2o"])` | ❌ **不在 `EXACT_BLOCKED_METHODS` 中** | **P1** | **未阻止** — 同上 | **B2-P4: analyzer runtime setup guard contract** |
| `analyzer_fleet_service.py:649` | `configure_gas_analyzer` | 调用 `_run_mode2_init_sequence` + `_apply_basic_gas_analyzer_settings` | ❌ 继承上述缺口 | **P1** | **入口未阻止** | **B2-P4** |
| `pressure_control_service.py:5656-5659` | `pressure_control_service` | `controller.set_output_mode_active()` | N/A | **P2** | **PACE 内部配置** — 非 analyzer，属于 pressure controller 设备操作 | 非本次范围 |
| `pressure_control_service.py:6051-6053` | `pressure_control_service` | `controller.set_slew_mode_max()` | N/A | **P2** | **PACE 内部配置** — 同上 | 非本次范围 |

**Category C 判决**:
- `set_mode` / `set_comm_way` / `set_active_freq` / `set_average_filter` / `set_average` **均不被当前 no-write guard 阻止**。
- 这些方法不匹配 `WRITE_VERBS + CALIBRATION_TERMS`（不含 coeff/zero/span 等关键词）。
- 这些方法不在 `EXACT_BLOCKED_METHODS` 中。
- **在 no-write real-COM engineering probe 中，这些命令会被实际发送到分析仪**。
- **这是已知 gap**：当前 no-write guard 覆盖了校准参数写入和 ID 写入，但未覆盖 analyzer runtime setup commands。

---

### 2.4 Category D: Pressure/Valve Physical Control

| File | Function | Callsite | Current Guard | Risk | Decision | Next Action |
|------|---------|---------|:--:|:--:|------|------|
| `pressure_control_service.py` | `set_vent` / `_set_vent` | 物理 vent ON/OFF | N/A（物理控制） | **P0** | **允许** — 物理控制，非参数写入。但必须在 sealed pressure control 内 vent=ON count 审计中=0 | 已审计（CO2=0, H2O=0） |
| `valve_routing_service.py` | `set_valves_for_co2` / `_apply_valve_states` | 物理阀切换 | N/A（物理控制） | **P0** | **允许** — 物理控制，非参数写入 | 已审计 |
| `pressure_control_service.py` | `set_pressure_target` / `pressurize_and_hold` | 设定 PACE5000 压力目标 | N/A（PACE 控制） | **P0** | **允许** — 物理控制 | 已审计 |

**Category D 判决**: pressure/valve 物理控制不是"参数写入"，且已在 sealed vent=0 审计中通过。

---

### 2.5 Category E: Chamber/Humidity Device Control

| File | Function | Callsite | Current Guard | Risk | Decision | Next Action |
|------|---------|---------|:--:|:--:|------|------|
| `temperature_control_service.py` | `set_temperature` / `start` / `stop` | 温箱 SV 控制 | N/A（设备控制） | **P1** | **当前 CO2 probe config 中 `chamber_set_temperature_enabled=false` / `chamber_start_enabled=false` / `chamber_stop_enabled=false`** | config 级阻断已生效 |
| `humidity_generator_service.py` | `set_flow` / `set_dewpoint` | 湿度发生器控制 | N/A（设备控制） | **P1** | **H2O probe 中可用，CO2 probe 中 `h2o_enabled=false`** | config 级阻断已生效 |

**Category E 判决**: chamber/humidity 控制在各自 config 中已通过 scope flag 阻断。CO2 probe 中不触发。

---

### 2.6 Category F: Artifact/Report Write

| File | Function | Callsite | Current Guard | Risk | Decision | Next Action |
|------|---------|---------|:--:|:--:|------|------|
| `artifact_service.py` | 各 export 方法 | 写 JSON/CSV/TXT 到 `output/` | N/A（文件 I/O） | **P2** | **允许** — 文件写入，非设备参数写入 | 无需修改 |
| `coefficient_service.py` | `export_coefficient_report` | 写 ratio-poly report 文件 | N/A（文件 I/O） | **P2** | **允许** — 同上。注意：这是 `export`（读 + 计算 + 写报告），不是 `apply`（写入设备） | 无需修改 |
| `orchestrator.py` | trace / state 持久化 | 写 `route_trace.jsonl` / `summary.json` 等 | N/A（文件 I/O） | **P2** | **允许** — artifact 文件写入 | 无需修改 |

**Category F 判决**: 所有 artifact 文件写入是正常的工程工件产出，不含设备参数修改。

---

### 2.7 Category G: Test/Mock Only

| File | Function | Callsite | Current Guard | Risk | Decision | Next Action |
|------|---------|---------|:--:|:--:|------|------|
| `test_a2_no_write_pressure_sweep.py` | `monkeypatch` | 大量 `monkeypatch.setattr` 注入 mock | N/A（测试） | **P1** | **mock 缺口导致 114 预存失败**（非真实 guard 失效） | B2-P5: test fixture repair only |

---

## 3. Summary

| Category | 总数 | 已 guard | 未 guard（已知 gap） | 非本次范围 |
|:--:|:--:|:--:|:--:|:--:|
| A: calibration write | 全部（27 exact + pattern + raw） | ✅ 全部 | 0 | 0 |
| B: identity/SENCO write | 5 exact + raw | ✅ 全部 | 0 | 0 |
| C: analyzer runtime setup | 5 methods（set_mode/set_comm_way/set_active_freq/set_average_filter/set_average） | ❌ | **5** | 0 |
| D: pressure/valve control | 3+ | N/A（物理控制） | 0（已审计） | 0 |
| E: chamber/humidity | 2+ | N/A（config flag 阻断） | 0 | 0 |
| F: artifact write | 3+ | N/A（文件 I/O） | 0 | 0 |
| G: test/mock | N/A | N/A | 0 | 0 |

**关键发现**：
1. Calibration + Identity 写入已被 no-write guard 全面覆盖（Category A+B）。✅
2. Analyzer runtime setup commands（Category C）**未被 guard 覆盖**。⚠️
3. Pressure/valve 物理控制不是参数写入，已有 sealed vent=0 审计。✅
4. Chamber/humidity 控制已被 config flag 阻断。✅
5. Artifact 文件写入不涉及设备。✅

**P1 gap**：在 no-write real-COM engineering probe 中，`configure_gas_analyzer()` 会向分析仪发送 `set_mode/set_comm_way/set_active_freq/set_average_filter/set_average` 命令。虽然这些不是校准参数写入，但它们是设备写入。当前 trace 中这些命令会被执行但**不会被记录为 blocked_write_events**。

---

*B2 No-Write Boundary Callsite Audit V1 结束。*
