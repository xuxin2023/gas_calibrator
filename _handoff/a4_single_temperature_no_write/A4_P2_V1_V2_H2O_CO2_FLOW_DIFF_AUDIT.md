# A4-P2 V1/V2 H2O+CO2 Flow Difference Audit

**生成时间**: 2026-05-12
**仓库**: `D:/gas_calibrator`
**分支**: `codex/v2-golden-recovery-cdb82111`
**起始 HEAD**: `a3eb8e58`
**角色**: `engineering_probe_only` / `not_real_acceptance_evidence=true` / `promotion_state=blocked`

---

## 1. Scope

只读对比 V1 (`workflow/runner.py`) 与 V2 (`v2/core/`) 的水路+气路必要动作，不改代码。上游: B2-P4B V1/V2 gap audit + A4-P1 plan + CO2 A2 baseline audit + H2O R4/R5 evidence。

---

## 2. V1 vs V2 Flow Table

| # | Step | V1 Behavior / Source | Physical Meaning | V2 Owner / Source | Match | Copy V1? | Gap/Decision |
|---|------|----------------------|------------------|-------------------|:-----:|:--------:|-------------|
| 1 | device connect/precheck | `runner.py:_configure_devices()` + `_startup_pressure_precheck()` | 硬件在线、可通信 | `orchestrator.py:_run_initialization_impl()` + `_run_precheck_impl()` | ✅ | 是 | 无缺口。V2 更清晰分离 init/precheck |
| 2 | analyzer MODE2/runtime setup | `set_comm_way/set_mode/set_active_freq/set_average_filter/set_average` | MODE2 主动上传、滤波参数 | `analyzer_fleet_service.py` + B2-P4A `runtime_setup_events` | ✅ | 是 | 无缺口。V2 新增 evidence 记录 |
| 3 | 禁止自动写设备 ID | V1 不调用任何 set_device_id | V1 本身不写 ID | `no_write_guard.py` EXACT_BLOCKED + config `apply_device_id=false` | ✅ | 不照搬(V1 无此概念) | V2 双重保护更强 |
| 4 | temperature chamber set/soak | `runner.py:_set_temperature()` → chamber.set_temp_c + PID soak | 封闭气路目标温度 | `temperature_control_service.py` + `temperature_group_runner.py` | ✅ | 是 | 无缺口。config flag 可阻断 CO2 probe |
| 5 | H2O humidity generator/dewpoint | `runner.py:` hgen.set_params + dewpoint wait | 气体湿度达标 | `humidity_generator_service.py` + `dewpoint_alignment_service.py` | ✅ | 是 | 无缺口。CO2 probe 中 `h2o_enabled=false` 阻断 |
| 6 | H2O open/ambient phase | `runner.py:` vent ON + H2O path open + soak | 水路开式通气稳定 | `h2o_route_runner.py` open-route + pre-seal soak | ✅ | 是 | 无缺口。R4/R5 验证通过 |
| 7 | H2O vent=OFF before sealing | `runner.py:_seal_transition_state()` vent=OFF | 封闭前关排气 | `conditioning_service.py` preseal vent=OFF | ✅ | 是 | 无缺口。R4/R5 顺序已验证 |
| 8 | H2O wait 1.5s then close valve | `runner.py:` 1.5s wait → close pressure block valve | 关阀前系统稳定 | `conditioning_service.py` + `a2_hooks` `_handoff_sample_to_vent_ms` | ✅ | 是 | 无缺口。V2 用 a2_hooks 管理时序 |
| 9 | CO2 gas source/valve select | `runner.py:` valve routing → `_set_valves_for_co2()` | 标气选择+阀切换 | `valve_routing_service.py:set_valves_for_co2()` + `co2_open_valves()` | ✅ | 是 | 无缺口。V2 阀映射完整 |
| 10 | CO2 gas flush/stability | `runner.py:` atmosphere flush → stability wait | 标气通气后浓度稳定 | `conditioning_service.py` atmosphere hold/soak + `co2_route_runner.py` | ✅ | 是 | 无缺口。dewpoint gate 已实现 |
| 11 | CO2 ambient/normal-pressure point | `runner.py:` atmosphere flush → analyzer reads ambient | 真实大气压下零点校准 | `co2_route_runner.py` ambient block + atmosphere phase | ⚠️ | 是 | **GAP: 气路常压点缺口** — 当前 CO2 baseline [1100..500] 不含常压点; A2 ambient 800hPa config 存在但未集成到七压力 baseline |
| 12 | CO2 preseal vent=OFF | `runner.py:` vent=OFF before sealed control | 封闭控压前关排气 | `conditioning_service.py:_verify_co2_preseal_atmosphere_hold_pressure` | ✅ | 是 | 无缺口。B1-R1 已修复 |
| 13 | sealed pressure control | `runner.py:_pressurize_and_hold()` PACE 控压 | 封闭气路精确控压 | `pressure_control_service.py:pressurize_and_hold()` | ✅ | 是 | 无缺口。shared service, 不改 |
| 14 | sealed vent=0 | `runner.py:_sealed_no_vent_guard` 禁止 vent=ON | 密封状态不排气 | `pressure_control_service.py` sealed vent=ON count audit | ✅ | 是 | 无缺口。CO2=0, H2O=0 已确认 |
| 15 | pressure gauge readback | `runner.py:` pressure_gauge.read_pressure() | 独立压力参考 | `pressure_control_service.py` 数字压力计读数 | ✅ | 是 | 无缺口。PACE+digital gauge 双通道 |
| 16 | sample window/sample_count | `runner.py:_collect_samples()` count/interval | 每压力点采集足够样本 | `sampling_service.py` + sample window management | ✅ | 是 | 无缺口。V2 已具备完整框架 |
| 17 | coefficient compute/export only | `runner.py:_maybe_write_coefficients()` (V1 可选写) | no-write 下仅计算不写 | `coefficient_service.py:export_coefficient_report()` compute-only | ✅ | 部分(V1 可写,V2 no-write 禁写) | 无缺口。V2 在 no-write probe 中纯计算+导出 |
| 18 | safe stop | `runner.py:_perform_safe_stop()` → valve restore, vent=ON, PACE stop | 运行结束系统复位 | `finalization_runner.py:_perform_safe_stop()` | ✅ | 是 | 无缺口。valve/pressure/baseline 全覆盖 |
| 19 | V1 fallback | `run_app.py` 默认入口指向 V1 | 生产回退路径 | `run_app.py` 未修改, `disable_v1=false` | ✅ | 是 | 无缺口。AGENTS.md 硬约束保护 |

---

## 3. Must Not Copy From V1

| # | V1 不合理动作 | V2 处理 |
|---|-------------|--------|
| A | sealed pressure control 内通大气 | V2 sealed vent=0 已审计 (CO2=0, H2O=0) |
| B | 自动写设备 ID | V2 双重阻断: `apply_device_id=false` + proxy block |
| C | 无证据参数写入 | V2 NoWriteGuard 提供 blocked_events + runtime_setup_events |
| D | legacy PACE vent3 状态机 | V2 pressure_control_service 简化, 不照搬 |
| E | V1 中不符合物理链路的顺序 | 以物理意义为准, V2 H2O vent/valve order 已修正 |

---

## 4. Gas Ambient (Normal-Pressure Point) Gap

**V2 气路常压点缺口存在。** 当前状态:
- CO2 A2 protected baseline 七压力点: `[1100, 1000, 900, 800, 700, 600, 500]`，不含常压点
- A2 ambient 800hPa config 文件存在 (`run001_a2_co2_0ppm_ambient_800hpa_no_write.json`) 但未集成到 baseline
- V1 在大气压环境采集零点，V2 目前缺少对应常压点处理

**本轮只记录，不补。** 后续应作为 A4-P3 / A4 risk item 设计。

---

## 5. Need User Decision

**NONE** — 全部 19 步均有明确对应关系和判决。

---

## 6. Decision

**A4_P2_V1_V2_DIFF_AUDIT_PASS**

判定: 19/19 步 V2 全部有对应实现; 18/19 Match (仅 #11 常压点缺口标记为 ⚠️ GAP); 5 项 V1 不合理动作拒绝照搬; 常压点缺口已登记不补。

---

*结束*
