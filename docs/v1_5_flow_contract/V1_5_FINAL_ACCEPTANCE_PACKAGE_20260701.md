# V1.5 收尾验收包

- 时间：2026-07-01 15:22:29 +08:00
- 范围：V1.5 clean worktree 结构整理收尾验收。
- 工作区：`D:\gas_calibrator\_worktrees\v1_5_fixed_wait_window_gate_1aee26d_clean`
- 边界：本包不开 COM、不连接 PostgreSQL、不控制压力/气路/水路、不写 SN、不写 SENCO、不产生 real acceptance 结论。

## 1. focused pytest stdout

命令：

```powershell
& 'C:\Users\A\AppData\Local\Programs\Python\Python313\python.exe' -m pytest tests\test_v1_5_entrypoint_inventory.py tests\test_v1_5_formal_flow_contract.py tests\test_v1_5_formal_initialization_runner.py tests\test_v1_5_initialization_readiness.py tests\test_v1_5_dirty_zone_audit.py tests\test_v1_5_formal_run_status.py tests\test_v1_5_formal_archive_closure.py tests\test_v1_5_calibration_reports.py tests\test_v1_5_operation_console.py tests\test_v1_5_run_evidence_status.py tests\test_v1_5_evidence_registry.py -q
```

stdout：

```text
........................................................................ [ 45%]
........................................................................ [ 90%]
...............                                                          [100%]
159 passed in 110.43s (0:01:50)
```

覆盖范围：

- canonical entrypoint 和正式入口防误用。
- formal flow contract 和 full-flow 阶段顺序。
- initialization runner / readiness / PostgreSQL 18 / SN-device_code 合同。
- dirty-zone audit。
- formal run status。
- archive closure、calibration reports、operation console、run evidence status、evidence registry。

## 2. 只读 full-flow status rollup stdout

命令：

```powershell
$env:PYTHONPATH = (Resolve-Path src).Path
& 'C:\Users\A\AppData\Local\Programs\Python\Python313\python.exe' -m gas_calibrator.tools.export_v1_5_formal_run_status --run-dir docs\v1_5_flow_contract --output-dir docs\v1_5_flow_contract\final_acceptance_status
```

stdout：

```json
{
  "overall_status": "review_required",
  "current_stage": "initialization_readiness",
  "next_action": "Generate or refresh initialization readiness before any open-flow step.",
  "formal_release_allowed": false,
  "database_import_allowed": false,
  "can_continue_physical_flow": false,
  "outputs": {
    "json_path": "D:\\gas_calibrator\\_worktrees\\v1_5_fixed_wait_window_gate_1aee26d_clean\\docs\\v1_5_flow_contract\\final_acceptance_status\\v1_5_formal_run_status.json",
    "markdown_path": "D:\\gas_calibrator\\_worktrees\\v1_5_fixed_wait_window_gate_1aee26d_clean\\docs\\v1_5_flow_contract\\final_acceptance_status\\v1_5_formal_run_status.md",
    "gates_csv_path": "D:\\gas_calibrator\\_worktrees\\v1_5_fixed_wait_window_gate_1aee26d_clean\\docs\\v1_5_flow_contract\\final_acceptance_status\\v1_5_formal_run_status_gates.csv",
    "gaps_csv_path": "D:\\gas_calibrator\\_worktrees\\v1_5_fixed_wait_window_gate_1aee26d_clean\\docs\\v1_5_flow_contract\\final_acceptance_status\\v1_5_formal_run_status_gaps.csv"
  },
  "physical_boundaries": {
    "offline_status_only": true,
    "opens_com_ports": false,
    "connects_postgresql": false,
    "controls_pressure": false,
    "controls_water_or_gas_routes": false,
    "writes_coefficients": false,
    "writes_device_id": false,
    "not_real_acceptance_evidence": true
  }
}
```

结论：

- 当前只读参考目录不是一轮完整真实校准 run，所以 status 正确落在 `review_required`。
- `can_continue_physical_flow=false`，因为缺少当前 run 的 initialization readiness、GETCO/SN traceability、pre-gas readiness 和压力证据。
- `formal_release_allowed=false`、`database_import_allowed=false`，因为没有完整归档/数据库释放证据。
- 该 rollup 只读现有 JSON/CSV/Markdown sidecar，不触碰设备。

## 3. dirty-zone audit stdout

提交后已重新刷新 dirty-zone audit，下面是刷新后的当前状态证据。

命令：

```powershell
$env:PYTHONPATH = (Resolve-Path src).Path
& 'C:\Users\A\AppData\Local\Programs\Python\Python313\python.exe' -m gas_calibrator.tools.export_v1_5_dirty_zone_audit --clean-worktree . --root-workspace D:\gas_calibrator --output-dir docs\v1_5_flow_contract\final_acceptance_status\dirty_zone_audit
```

stdout：

```json
{
  "json": "D:\\gas_calibrator\\_worktrees\\v1_5_fixed_wait_window_gate_1aee26d_clean\\docs\\v1_5_flow_contract\\final_acceptance_status\\dirty_zone_audit\\v1_5_dirty_zone_audit.json",
  "markdown": "D:\\gas_calibrator\\_worktrees\\v1_5_fixed_wait_window_gate_1aee26d_clean\\docs\\v1_5_flow_contract\\final_acceptance_status\\dirty_zone_audit\\v1_5_dirty_zone_audit.md",
  "csv": "D:\\gas_calibrator\\_worktrees\\v1_5_fixed_wait_window_gate_1aee26d_clean\\docs\\v1_5_flow_contract\\final_acceptance_status\\dirty_zone_audit\\v1_5_dirty_zone_entries.csv",
  "status": "review_required",
  "blocker_count": 0
}
```

结论：

- `blocker_count=0`，没有误 staged 的根目录污染区或 `_handoff` 证据。
- 提交后 clean worktree 的 review 项已经归零；clean worktree 只剩 `_handoff` 未跟踪历史证据区，不进入正式代码小包。
- 根目录 `D:\gas_calibrator` 仍有 dirty 内容，继续作为污染/草稿区隔离，不作为正式 V1.5 来源。

## 4. 成熟路径边界核查

保护文件：

- `src/gas_calibrator/tools/run_v1_5_formal_co2_open_flow_queue.py`
- `src/gas_calibrator/tools/run_v1_5_formal_h2o_open_flow_queue.py`
- `src/gas_calibrator/tools/run_v1_5_formal_open_flow_sampling.py`
- `src/gas_calibrator/workflow/runner.py`
- `src/gas_calibrator/devices/gas_analyzer.py`
- `configs/default_config.json`

命令：

```powershell
git diff --name-only -- src\gas_calibrator\tools\run_v1_5_formal_co2_open_flow_queue.py src\gas_calibrator\tools\run_v1_5_formal_h2o_open_flow_queue.py src\gas_calibrator\tools\run_v1_5_formal_open_flow_sampling.py src\gas_calibrator\workflow\runner.py src\gas_calibrator\devices\gas_analyzer.py configs\default_config.json
git diff --cached --name-status
```

stdout：

```text

```

结论：本收尾包没有修改成熟 CO2/H2O queue、shared sampling、workflow runner、analyzer protocol 或默认配置。

## 5. patch hygiene

命令：

```powershell
git diff --check
```

stdout：

```text
warning: in the working copy of 'docs/v1_5_flow_contract/V1_5_FINAL_STRUCTURE_AND_FLOW.md', LF will be replaced by CRLF the next time Git touches it
warning: in the working copy of 'tests/test_v1_5_entrypoint_inventory.py', LF will be replaced by CRLF the next time Git touches it
```

结论：只有 Windows 换行提示，没有 whitespace error。

## 6. Done when

本收尾包完成标准：

- 最终结构说明已明确正式入口、禁止入口、新旧算法边界、0620 成熟路径保护和污染区策略。
- focused pytest 已通过并保留 stdout。
- 只读 `formal_run_status` 已生成，且明确当前不是 real acceptance。
- dirty-zone audit 已生成，根目录污染区和 `_handoff` 证据区继续隔离。
- 成熟 CO2/H2O runner、sampling、`runner.py`、`gas_analyzer.py`、`default_config.json` 未被本包修改。
