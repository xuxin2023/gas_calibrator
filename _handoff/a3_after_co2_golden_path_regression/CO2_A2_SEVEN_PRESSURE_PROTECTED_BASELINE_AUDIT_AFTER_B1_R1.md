# CO2 A2 Seven-Pressure Protected Baseline Audit After B1-R1

**生成时间**: 2026-05-12  
**仓库**: `D:/gas_calibrator`  
**分支**: `codex/v2-golden-recovery-cdb82111`  
**HEAD**: `c62093c4b662c7436216c330dcc1c73f3d0f1b3a`  
**审计对象**: Run-001/A2 CO2-only seven-pressure no-write protected baseline  
**final_decision**: **PASS** — 仅表示保护链未被 H2O A3 与 B1-R1 破坏；不表示重新跑了真机、不表示 controlled write ready、不表示 production acceptance。

---

## 1. 任务边界

本轮审计不是重新建立 CO2 七压力点主线，也不把 B1-R1 的 6 points simulation 解释为 CO2 七压力点完成。

本轮只确认：

1. `c62093c4` 是否破坏 Run-001/A2 CO2-only seven-pressure no-write protected baseline；
2. A2 七压力点保护测试是否仍通过；
3. CO2 合同测试是否仍通过；
4. B1-R1 的 6 points simulation 与 A2 七压力点保护链边界是否清楚。

本轮未执行：

- 真实 COM；
- `--execute-probe`；
- ID / SENCO / zero / span / coefficient / calibration 参数写入；
- controlled write；
- production acceptance；
- formal switch；
- V1 修改；
- H2O runtime 修改。

---

## 2. `c62093c4` 修改范围审计

执行命令：

```powershell
git show --name-only c62093c4
```

结果显示 `c62093c4` 只包含以下 4 个文件：

| 文件 | 说明 |
|---|---|
| `_handoff/a3_after_co2_golden_path_regression/CO2_GOLDEN_PATH_B1_R1_FIX_AND_SIM_RERUN.md` | B1-R1 修复与 simulation rerun 报告 |
| `_handoff/a3_after_co2_golden_path_regression/CO2_GOLDEN_PATH_B1_R1_ROOT_CAUSE_REPORT.md` | B1-R1 根因报告 |
| `src/gas_calibrator/v2/core/orchestrator.py` | 补齐 CO2 preseal host wrapper 与 no-op snapshot wrapper |
| `src/gas_calibrator/v2/core/services/conditioning_service.py` | 补齐 `_verify_co2_preseal_atmosphere_hold_pressure` 实现 |

`c62093c4` 未修改以下保护链文件：

| 保护链文件 | 是否被 `c62093c4` 修改 |
|---|---|
| `src/gas_calibrator/v2/core/run001_a2_co2_only_7_pressure_no_write_probe.py` | 否 |
| `src/gas_calibrator/v2/configs/validation/run001_a2_co2_only_7_pressure_no_write_real_machine.json` | 否 |
| `src/gas_calibrator/v2/configs/validation/run001_a2_co2_only_7_pressure_points.json` | 否 |
| `src/gas_calibrator/v2/tests/test_a2_co2_only_7_pressure_no_write_probe.py` | 否 |
| V1 生产代码 | 否 |
| H2O runtime | 否 |
| pressure_control_service shared 行为 | 否 |
| valve_routing_service | 否 |
| co2_route_runner | 否 |

---

## 3. CO2 七压力点保护链确认

### 3.1 Core probe identity

文件：`src/gas_calibrator/v2/core/run001_a2_co2_only_7_pressure_no_write_probe.py`

关键保护项：

```python
A2_ALLOWED_PRESSURE_POINTS_HPA = (1100.0, 1000.0, 900.0, 800.0, 700.0, 600.0, 500.0)
```

Evidence marker：

```python
"probe_identity": "A2.12R CO2-only seven-pressure no-write engineering probe"
"acceptance_level": "engineering_probe_only"
"not_real_acceptance_evidence": True
"promotion_state": "blocked"
```

结论：七压力点保护基线仍锁定为：

`[1100, 1000, 900, 800, 700, 600, 500]`

### 3.2 Real-machine no-write probe config

文件：`src/gas_calibrator/v2/configs/validation/run001_a2_co2_only_7_pressure_no_write_real_machine.json`

关键保护项：

| 字段 | 值 |
|---|---|
| `run001_a2.mode` | `real_machine_dry_run` |
| `run001_a2.no_write` | `true` |
| `run001_a2.authorized_pressure_points_hpa` | `[1100,1000,900,800,700,600,500]` |
| `run001_a2.allow_write_coefficients` | `false` |
| `run001_a2.allow_write_zero` | `false` |
| `run001_a2.allow_write_span` | `false` |
| `run001_a2.allow_write_calibration_parameters` | `false` |
| `run001_a2.default_cutover_to_v2` | `false` |
| `run001_a2.disable_v1` | `false` |
| `a2_co2_7_pressure_no_write_probe.no_write` | `true` |
| `a2_co2_7_pressure_no_write_probe.pressure_points_hpa` | `[1100,1000,900,800,700,600,500]` |
| `a2_co2_7_pressure_no_write_probe.analyzer_id_write_enabled` | `false` |
| `a2_co2_7_pressure_no_write_probe.senco_write_enabled` | `false` |
| `a2_co2_7_pressure_no_write_probe.calibration_write_enabled` | `false` |
| `workflow.collect_only` | `true` |
| `workflow.analyzer_setup.apply_device_id` | `false` |

结论：该 config 仍是 no-write / collect-only / no cutover / no V1 disable 的受控工程探针配置。本轮未执行该 config，也未打开真实 COM。

### 3.3 Seven-pressure points file

文件：`src/gas_calibrator/v2/configs/validation/run001_a2_co2_only_7_pressure_points.json`

内容为 7 个 CO2 pressure points：

| index | route | pressure_hpa | co2_ppm |
|---:|---|---:|---:|
| 1 | co2 | 1100 | 100 |
| 2 | co2 | 1000 | 100 |
| 3 | co2 | 900 | 100 |
| 4 | co2 | 800 | 100 |
| 5 | co2 | 700 | 100 |
| 6 | co2 | 600 | 100 |
| 7 | co2 | 500 | 100 |

结论：不含常压点，不把 1000 hPa ambient-open 当成七压力点之一。

### 3.4 Protection tests

文件：`src/gas_calibrator/v2/tests/test_a2_co2_only_7_pressure_no_write_probe.py`

测试保护项包括：

- `A2_ALLOWED_PRESSURE_POINTS_HPA`；
- `pressure_points_hpa` 与 `authorized_pressure_points_hpa`；
- no-write flags；
- route 为 CO2-only；
- `default_cutover_to_v2 = false`；
- `disable_v1 = false`；
- points rows 与 `[1100,1000,900,800,700,600,500]` 对齐。

---

## 4. 测试结果

### 4.1 A2 CO2 seven-pressure no-write protected baseline tests

执行命令：

```powershell
$env:PYTHONPATH = "src"
python -m pytest src/gas_calibrator/v2/tests/test_a2_co2_only_7_pressure_no_write_probe.py -q --tb=short
```

结果：

```text
51 passed in 7.33s
```

结论：A2 七压力点 no-write protected baseline 测试全部通过。

### 4.2 CO2 contract tests

执行命令：

```powershell
$env:PYTHONPATH = "src"
python -m pytest tests/v2/test_co2_route_runner.py tests/v2/test_co2_no_vent_guard.py tests/v2/test_co2_artifact_contract.py tests/v2/test_co2_route_golden_master.py tests/v2/test_co2_shadow_state_trace.py tests/v2/test_pressure_control_service.py -q --tb=short
```

结果：

```text
47 passed in 25.21s
```

结论：CO2 route_runner / no-vent guard / artifact contract / golden master / shadow state trace / pressure_control_service 合同测试全部通过。

---

## 5. B1-R1 与 A2 七压力点边界

B1-R1 提交 `c62093c4` 解决的是 CO2 orchestrator integration host-contract gap：

- `_verify_co2_preseal_atmosphere_hold_pressure` host method 缺失；
- `_refresh_live_analyzer_snapshots` wrapper 缺失；
- `run_simulated_compare --profile replacement_skip0_co2_only_simulated --scenario co2_only_skip0_success_single_temp` 从 V2 error 恢复为 MATCH。

B1-R1 simulation 是简化 integration simulation：

| 项目 | B1-R1 simulation |
|---|---|
| scenario | `co2_only_skip0_success_single_temp` |
| points_total | 6 |
| points_completed | 6 |
| Group A | 1100 / 1000 / 600 |
| Group B | 1100 / 1000 / 600 |
| sealed_pressure_control_vent_on_count | 0 |
| write-related events | 0 |

明确结论：

- B1-R1 的 6 points simulation **不是** CO2 七压力点基线；
- 不允许把 B1-R1 的 6 points simulation 写成七压力点通过；
- CO2 七压力点保护链仍然是 Run-001/A2 CO2-only seven-pressure no-write probe；
- 本报告的 PASS 仅表示该保护链在 H2O A3 与 B1-R1 后仍未被破坏。

---

## 6. Compliance

| 检查项 | 结果 |
|---|---|
| 不回滚 `c62093c4` | PASS |
| 不 force push | PASS |
| 不修改 V1 | PASS |
| 不修改 H2O runtime | PASS |
| 不恢复 stash | PASS |
| 不跑真实 COM | PASS |
| 不执行 `--execute-probe` | PASS |
| 不写 ID / SENCO / zero / span / coefficient / calibration 参数 | PASS |
| 不进入 controlled write | PASS |
| 不进入 production acceptance | PASS |
| 不进入 formal switch | PASS |
| 不新增七压力点主线 | PASS |
| 不把 B1-R1 6 points simulation 说成七压力点 | PASS |

---

## 7. Final Decision

**PASS**

含义仅限于：

> CO2 seven-pressure protected baseline remains intact after H2O A3 and B1-R1.

不表示：

- 重新跑了真机；
- 执行了 `--execute-probe`；
- controlled write ready；
- production acceptance PASS；
- formal switch ready；
- B1-R1 的 6 points simulation 等同于七压力点。

---

## 8. 下一步建议

1. 继续把 Run-001/A2 CO2-only seven-pressure no-write probe 作为唯一受保护七压力点基线；
2. 后续若需要真实 A2 探针，必须按 Step 3A 双重解锁、operator confirmation、no-write evidence 和 engineering_probe_only 规则执行；
3. B1/B2 后续报告中继续明确区分：
   - B1-R1 simplified integration simulation（6 points）；
   - A2 protected seven-pressure engineering probe baseline（7 pressure points）。
