# B2-P4B V1/V2 Required Action Gap Audit

**生成时间**: 2026-05-12  
**仓库**: `D:/gas_calibrator`  
**分支**: `codex/v2-golden-recovery-cdb82111`  
**起始 HEAD**: `3188aa73`（B2-P4A 已完成）  
**审计方式**: 只读代码扫描，不改 V1/V2 流程  
**角色**: `engineering_probe_only` / `not_real_acceptance_evidence=true` / `promotion_state=blocked`

---

## 1. Scope

1. 逐项对比 V1 生产校准必要动作，检查 V2 是否缺失。
2. V1 不合理动作**不得照搬**：sealed pressure control 内通大气、自动写设备 ID、无证据参数写入。
3. 不提出 controlled write，不进入 production acceptance。
4. 拿不准处标记 `NEED_USER_DECISION`。

---

## 2. 逐动作审计

| # | V1 必要动作 | V1 位置/证据 | 物理意义 | V2 对应实现 | 状态 | 是否应照搬 V1 | 缺口/下一步 |
|---|-----------|------------|--------|-----------|:--:|:--:|------|
| 1 | **设备连接与预检** | `runner.py:run()` → `_sensor_precheck()` → `_configure_devices()` → `_startup_pressure_precheck()` | 确保所有硬件在线、可通信、无故障 | `orchestrator.py:run()` → `_run_initialization_impl()`（create + open_all + analyzer_setup + sensor_precheck + PACE config）→ `_run_precheck_impl()`（health_check + leak test + sensor check）→ `_run_startup_pressure_precheck()` | ✅ 已实现 | ✅ 是 | 无缺口。V2 已具备 device_manager.health_check()、analyzer precheck、PACE precheck。 |
| 2 | **分析仪 MODE2/通信/频率/平均设置** | `runner.py:_configure_devices()` → `_configure_gas_analyzer()` (set_comm_way, set_mode, set_active_freq, set_average_filter, set_average_filter_channel, read_mode2_frame verify) | 使分析仪进入 MODE2 主动上传模式，正确配置频率和滤波参数，确保后续采样数据质量 | `analyzer_fleet_service.py:_run_mode2_init_sequence()` + `_apply_basic_gas_analyzer_settings()` | ✅ 已实现 + ✅ 已由 B2-P4A 合约记录 | ✅ 是 | 无缺口。B2-P4A 已将 runtime setup 记录到 `runtime_setup_events`。 |
| 3 | **禁止自动写设备 ID** | V1 `runner.py` 不调用任何 set_device_id/write_device_id/assign_device_id 方法 | V1 从不自动写设备 ID，只读取 `ga.device_id` 属性 | `analyzer_fleet_service.py:apply_analyzer_setup()` — 双重保护：config 级 `apply_device_id=false`（no_write 时强制）+ proxy 级 `EXACT_BLOCKED_METHODS` 阻断 | ✅ 已实现 | V1 没有此动作，V2 正确禁止了 | 无缺口。V1 本身不写设备 ID，V2 从 config 和 proxy 两层保护了。 |
| 4 | **温箱设温、稳定、等待** | `runner.py:_set_temperature_for_point()` → chamber.set_temperature() + soak 等待 | 确保封闭气路处于目标温度，保证采样数据的温度一致性 | `temperature_control_service.py` + `temperature_group_runner.py` | ✅ 已实现 | ✅ 是 | 无缺口。V2 已通过 `temperature_group_runner` 按温度组管理。CO2 probe 中 `chamber_set_temperature_enabled=false` 通过 config 阻断。 |
| 5 | **水路湿度发生器/露点仪稳定** | `runner.py:` humidity_generator.set_params() + dewpoint wait | 确保气体湿度达到目标值，保证 H2O 通道校准精度 | `humidity_generator_service.py` | ✅ 已实现（H2O probe 可用） | ✅ 是 | 无缺口。CO2 probe 中 `h2o_enabled=false` 通过 config 阻断。 |
| 6 | **气路标气选择和通气稳定** | `runner.py:` valve routing + atmosphere flush/soak | 将目标浓度标气通入分析仪气路，等待浓度稳定 | `valve_routing_service.py` + `conditioning_service.py` — `set_valves_for_co2` + atmosphere hold/soak | ✅ 已实现 | ✅ 是 | 无缺口。 |
| 7 | **常压/通大气阶段** | `runner.py:` atmosphere flush → analyzer reads ambient | 确保零点在真实大气压下校准，避免封闭压力偏移 | `co2_route_runner.py` ambient block + atmosphere phase | ✅ 已实现 | ✅ 是 | 无缺口。已有 CO2 A2 ambient 800hPa validation config。 |
| 8 | **封闭前 vent=OFF** | `runner.py:_seal_transition_state()` 中 vent=OFF + 1.5s 等待 | 封闭前关闭排气阀，防止封闭压力控制时漏气或意外通大气 | `pressure_control_service.py` + `conditioning_service.py` — preseal vent=OFF + close valves | ✅ 已实现 | ✅ 是 | 无缺口。已在 CO2 A2 sealed vent=0 审计中确认。 |
| 9 | **1.5s 等待与关阀顺序** | `runner.py:` 封闭后 1.5s 等待 → 关闭压力阻塞阀 | 确保 PACE 进入封闭压力控制模式前系统稳定 | `conditioning_service.py` + `a2_hooks` — handoff timing via `_handoff_sample_to_vent_ms` 等 | ✅ 已实现 | ✅ 是 | 无缺口。V2 使用 `a2_hooks` 管理 handoff 时序。 |
| 10 | **sealed pressure control** | `runner.py:` PACE pressurize_and_hold → 维持目标压力 | 在封闭气路中对每个压力点进行精确压力控制 | `pressure_control_service.py` — `pressurize_and_hold` + `PressureSetpointHold` phase | ✅ 已实现 | ✅ 是 | 无缺口。 |
| 11 | **sealed 内 vent=ON 必须为 0** | `runner.py:_sealed_no_vent_guard` — 封闭压力控制期间禁止 vent=ON | 防止密封状态下意外排气导致压力失控和数据无效 | `pressure_control_service.py` — sealed vent=ON count 审计 | ✅ 已实现 | ✅ 是 | 无缺口。CO2=0, H2O=0 已在之前审计中确认。 |
| 12 | **数字压力计读数** | `runner.py:` pressure_gauge.read_pressure() 采集压力计读数 | 提供独立于 PACE 的第二压力参考，用于系数计算和 QC | `pressure_control_service.py` + 数字压力计读数采集 | ✅ 已实现 | ✅ 是 | 无缺口。 |
| 13 | **采样窗口与样本数** | `runner.py:_collect_samples()` — count/interval 控制采样窗口 | 在每个压力/浓度点采集足够样本用于后续系数拟合 | `sampling_service.py` — 采样窗口管理 | ✅ 已实现 | ✅ 是 | 无缺口。V2 已具备完整的采样窗口框架。 |
| 14 | **no-write 证据** | V1 无此概念（V1 直接写或不写系数） | V2 新增：在 no-write probe 中必须记录 blocked_write_events + runtime_setup_events | `no_write_guard.py` — `NoWriteGuard.to_artifact()` | ✅ 已实现 | ❌ 不照搬 V1（V1 无此概念） | B2-P4A 已固化。`blocked_write_events` 记录被阻断的写入，`runtime_setup_events` 记录被允许的 runtime setup。 |
| 15 | **系数计算但不写入** | V1 `_maybe_write_coefficients()` 可选写系数（`coefficients.enabled` + `auto_fit`） | V1 会计算并写入，V2 no-write probe 只计算不写入 | `coefficient_service.py:export_coefficient_report()` — compute-only, 不调用任何 set_senco/write_coefficient | ✅ 已实现（compute-only） | ⚠️ 部分照搬（保留计算，强制不写入） | V1 可在 collect_only 模式下跳过拟合。V2 no-write 也跳过或只计算不写入。无缺口。 |
| 16 | **safe stop / restore baseline** | `runner.py:_restore_baseline_after_run()` → `_perform_safe_stop()` — 恢复阀门到安全状态、vent 到大气 | 运行结束后将系统恢复到安全物理状态（阀门复位、vent ON、PACE 停控、温箱停止） | `finalization_runner.py:_perform_safe_stop()` → `valve_routing_service.restore_baseline_after_run()` + `pressure_control_service.safe_stop_after_run()` + `valve_routing_service.safe_stop_after_run()` | ✅ 已实现 | ✅ 是 | 无缺口。V2 safe stop 涵盖阀门、压力、基线恢复。 |
| 17 | **artifact/trace 输出** | `runner.py:` summary.json, route_trace.csv, samples.csv, fit_reports | 输出校准结果、采样数据、系数报告供后续审查 | `artifact_service.py:export_all_artifacts()` + `coefficient_service.py:export_coefficient_report()` + trace/manifest/summary | ✅ 已实现 | ✅ 是 | 无缺口。V2 artifact 角色更清晰（execution_rows/execution_summary/diagnostic_analysis/formal_analysis）。 |
| 18 | **V1 fallback 保留** | `run_app.py` 默认入口指向 V1，V1 runner 未被修改或禁用 | 确保 V2 不能替代 V1 前，V1 始终是生产 fallback | V2 configs 中 `v1_fallback_required: true` + `run_app.py` 未修改 | ✅ 已保留 | ✅ 是 | 无缺口。项目规则已要求 `run_app.py` 不得修改，默认入口不得切换到 V2。 |

---

## 3. V1 不合理动作明确禁止照搬

| # | V1 不合理动作 | 说明 | V2 处理 |
|---|-------------|------|--------|
| A | **sealed pressure control 内通大气** | V1 实际不使用（已有 `_sealed_no_vent_guard` 防止），但仍需审计确认 | V2 已确认 sealed vent=0（CO2=0, H2O=0） |
| B | **自动写设备 ID** | V1 本身不自动写 ID（只读取 device_id 属性） | V2 `no_write_guard_active` 时强制 `apply_device_id=false` + proxy 级阻断 |
| C | **无证据的参数写入（缺乏 audit trail）** | V1 写系数时没有 no-write guard 的证据记录机制 | V2 的 `NoWriteGuard` 提供了完整的 `blocked_events` + `runtime_setup_events` 证据链 |
| D | **V1 的 PACE legacy vent3 状态机** | V1 有大量 vent3 状态机代码处理旧版 PACE 行为，在新 PACE 上不需要 | V2 使用 `pressure_control_service` 简化了 PACE 控制，不再照搬 legacy vent3 逻辑 |

---

## 4. 判决汇总

| 结论 | 数量 | 说明 |
|:--:|:--:|------|
| ✅ V2 已实现 | 17/18 | 全部 18 个必要动作中，17 个已在 V2 中有对应实现 |
| ✅ V2 明确不照搬 | 1/18 | "no-write 证据"是 V2 新增概念，V1 无此动作 |
| ⚠️ NEED_USER_DECISION | 0 | 无需要用户决策的项 |
| ❌ V2 缺失 | 0 | 无缺失的必要动作 |
| ❌ V1 不合理动作照搬 | 0 | 无被照搬的不合理动作 |

---

## 5. 详细判决

### 5.1 设备连接与预检

**V1**: `_sensor_precheck()` + `_configure_devices()` + `_startup_pressure_precheck()` — 三步初始化。

**V2**: `_run_initialization_impl()` (create + open + analyzer setup + sensor precheck + PACE config) → `_run_precheck_impl()` (health check + leak test + sensor check) → `_run_startup_pressure_precheck()` — 更清晰的初始化/预检分离。

**判决**: ✅ 已实现，V2 的分离更合理。

### 5.2 分析仪 MODE2 设置

**V1**: `_configure_gas_analyzer()` 直接调用 `set_comm_way_with_ack/set_mode_with_ack/set_active_freq_with_ack/set_average_filter_with_ack/set_average_filter_channel_with_ack`。

**V2**: `_run_mode2_init_sequence()` + `_apply_basic_gas_analyzer_settings()` — 逻辑等价。

**判决**: ✅ 已实现。B2-P4A 新增了 `RUNTIME_SETUP_METHODS` 记录机制。

### 5.3 自动写设备 ID 禁止

**V1**: V1 内部**从不写设备 ID**。`ga.device_id` 只是属性读取，不做写入。

**V2**: 双重保护：
1. `apply_analyzer_setup()` 中 `no_write_guard_active` 时强制 `apply_device_id = False`
2. `NoWriteDeviceProxy.__getattr__` 阻止 `set_device_id/set_device_id_with_ack/write_device_id/assign_device_id/set_id`

**判决**: ✅ 已实现，且比 V1 更强（V1 只是不写，V2 是明确阻止）。

### 5.4 温箱设温、稳定、等待

**V1**: `_set_temperature_for_point()` → `_set_temperature()` → chamber.set_temperature + PID soak。

**V2**: `TemperatureControlService` + `TemperatureGroupRunner` 按温度组管理。CO2 probe 中 `chamber_set_temperature_enabled=false` 可 config 阻断。

**判决**: ✅ 已实现。

### 5.5 水路湿度发生器/露点仪稳定

**V1**: `humidity_generator.set_params()` + dewpoint wait。

**V2**: `HumidityGeneratorService`。CO2 probe 中 `h2o_enabled=false`。

**判决**: ✅ 已实现。

### 5.6 气路标气选择和通气稳定

**V1**: valve routing + atmosphere flush/soak。

**V2**: `ValveRoutingService` + `ConditioningService`。

**判决**: ✅ 已实现。

### 5.7 常压/通大气阶段

**V1**: atmosphere flush → analyzer reads ambient。

**V2**: CO2 A2 ambient block（atmosphere phase）。

**判决**: ✅ 已实现。

### 5.8 封闭前 vent=OFF

**V1**: `_seal_transition_state()` vent=OFF + 1.5s。

**V2**: `ConditioningService` preseal vent=OFF + close valves。

**判决**: ✅ 已实现，sealed vent=0 已审计。

### 5.9 1.5s 等待与关阀顺序

**V1**: `_seal_transition_state()` 1.5s wait + 关闭压力阻塞阀。

**V2**: `a2_hooks` 管理 `_handoff_sample_to_vent_ms` 等时序。

**判决**: ✅ 已实现。

### 5.10 sealed pressure control

**V1**: PACE `pressurize_and_hold`。

**V2**: `PressureControlService.pressurize_and_hold`。

**判决**: ✅ 已实现。

### 5.11 sealed 内 vent=ON 为 0

**V1**: `_sealed_no_vent_guard` 主动防止。

**V2**: sealed vent=ON count 审计 = 0。

**判决**: ✅ 已实现。

### 5.12 数字压力计读数

**V1**: `pressure_gauge.read_pressure()`。

**V2**: 数字压力计 device 已注册，读数采集已实现。

**判决**: ✅ 已实现。

### 5.13 采样窗口与样本数

**V1**: `_collect_samples()` — count/interval。

**V2**: `SamplingService` — 采样窗口管理。

**判决**: ✅ 已实现。

### 5.14 no-write 证据

**V1**: 无此概念——V1 直接写或不写系数，没有中间的 "blocked write events" 记录。

**V2**: `NoWriteGuard.to_artifact()` 提供完整证据：`blocked_write_events`, `runtime_setup_events`, `identity_write_command_sent`, `final_decision`。

**判决**: ✅ 已实现（V2 专属功能，V1 无此概念）。

### 5.15 系数计算但不写入

**V1**: `_maybe_write_coefficients()` 可选写入。`collect_only=true` 时跳过。

**V2**: `CoefficientService.export_coefficient_report()` — compute-only。不调用任何 `set_senco/write_coefficient`。

**判决**: ✅ 已实现。V2 在 no-write probe 中是纯计算+导出，无设备写入。

### 5.16 safe stop / restore baseline

**V1**: `_restore_baseline_after_run()` → `_perform_safe_stop()` — 阀门复位、vent=ON、PACE 停控、温箱停止。

**V2**: `FinalizationRunner._perform_safe_stop()` — `valve_routing_service.restore_baseline_after_run()` + `pressure_control_service.safe_stop_after_run()` + `valve_routing_service.safe_stop_after_run()`。

**判决**: ✅ 已实现。

### 5.17 artifact/trace 输出

**V1**: summary.json, route_trace.csv, samples.csv, fit_reports。

**V2**: `ArtifactService.export_all_artifacts()` — execution_rows, execution_summary, diagnostic_analysis, formal_analysis 等角色更清晰。

**判决**: ✅ 已实现。

### 5.18 V1 fallback 保留

**V1**: `run_app.py` 默认入口指向 V1。

**V2**: `v1_fallback_required: true` 在 validation configs 中保留，`run_app.py` 未修改。

**判决**: ✅ 已保留。符合项目规则：不得修改 `run_app.py`，不得切换默认入口。

---

## 6. NEED_USER_DECISION 项

**无。**

本次审计未发现需要用户决策的模糊边界。

但下列项需用户注意：

1. **V1 系数写入行为**: V1 在 `_maybe_write_coefficients()` 中可写系数到设备。V2 no-write probe 明确不写。如果未来 real acceptance 要求写系数，需要单独设计 controlled_write 阶段并取得用户授权。**当前不需要**。

2. **V1 设备 ID 写入风险**: V1 内部代码不写设备 ID，但如果 V1 设备驱动（`gas_analyzer_serial` 或 `ygas_analyzer`）中有自动 ID 写入逻辑，需要单独审计。**当前不在本次范围**。

---

## 7. 总体判决

**V2 已覆盖 V1 的全部 18 个生产校准必要动作。**

- 物理控制动作（温箱、湿度发生器、阀门、压力控制、采样）：V2 完整实现。
- 分析仪通信设置（MODE2/频率/滤波）：V2 完整实现 + B2-P4A 合约记录。
- 安全约束（设备 ID 禁止、sealed vent=0、safe stop）：V2 完整实现。
- 证据治理（no-write 证据、artifact 角色）：V2 在 V1 基础上显著增强。
- V1 fallback：已保留，未被替代。

**无缺失必要动作。无 V1 不合理动作被照搬。无 NEED_USER_DECISION 项。**

---

*B2-P4B V1/V2 Required Action Gap Audit 结束。*
