# V2 运行与验证指南

## 1. 适用范围

本文只覆盖 Step 2 允许的运行方式：

- simulation
- replay
- suite regression
- parity
- resilience
- offline review
- simulation-only workbench

本文不覆盖：

- 真实串口 / COM 打开
- V2 真机测试
- real compare / real verify
- real acceptance 放行

## 2. 执行前约定

从仓库根目录 `D:\gas_calibrator` 执行：

```powershell
$env:PYTHONPATH = "src"
```

PowerShell 下建议把 `PYTHONPATH` 单独设置一行，再执行命令。这样比把环境变量和命令写在一行里更稳定，也更适合复制到值班手册里。

## 3. 常用命令

### 3.1 安全自检

```powershell
python -m gas_calibrator.v2.scripts.test_v2_safe
```

用途：

- 验证 V2 默认安全配置能否在 simulation-only 模式闭环
- 检查连接、单点流程、QC、算法引擎
- 输出 `output/test_v2_safe/test_report.json`

适用场景：

- 新环境首次验证
- 改动 `entry.py`、`config/`、`core/` 运行主链路后做 smoke

### 3.2 Headless 仿真运行

```powershell
python -m gas_calibrator.v2.scripts.run_v2 `
  --config src/gas_calibrator/v2/configs/smoke_v2_minimal.json `
  --simulation `
  --headless
```

用途：

- 走正式入口执行一次最小仿真 run
- 生成 `summary.json`、`manifest.json` 与离线评审工件

注意：

- `--headless` 模式必须显式提供 `--config`
- `--allow-unsafe-step2-config` 只用于受控的 Step 2 配置研究，不是默认路径

### 3.3 UI 启动

```powershell
python -m gas_calibrator.v2.ui_v2.app `
  --config src/gas_calibrator/v2/configs/smoke_v2_minimal.json `
  --simulation
```

用途：

- 验证中文默认 UI
- 查看 reports、results、review center、simulation-only workbench

适用场景：

- UI 合同验证
- 1920x1080 布局回归
- 评审工件可见性检查

### 3.4 simulation-only 设备工作台辅助脚本

```powershell
python -m gas_calibrator.v2.scripts.test_v2_device connection
python -m gas_calibrator.v2.scripts.test_v2_device single
python -m gas_calibrator.v2.scripts.test_v2_device full
```

用途：

- `connection`：检查默认设备矩阵是否全部走 simulated 设备
- `single`：执行单点仿真链路
- `full`：执行更完整的 simulation-only 工作台辅助流程

边界：

- 默认是 simulation-only
- `--bench` / `--allow-real-bench` 代表未来保留接口，不是当前阶段允许能力

### 3.5 仿真 compare

列出 profile / scenario：

```powershell
python -m gas_calibrator.v2.scripts.run_simulated_compare --list-profiles
python -m gas_calibrator.v2.scripts.run_simulated_compare --list-scenarios
```

运行一次 compare：

```powershell
python -m gas_calibrator.v2.scripts.run_simulated_compare `
  --profile replacement_skip0_co2_only_simulated `
  --scenario co2_only_skip0_success_single_temp
```

用途：

- 在协议仿真或 fixture 回放基础上生成 V1/V2 对比结果
- 输出 compare 报告与离线工件

注意：

- 脚本支持 `--publish-latest`，但 Step 2 当前不应把它用于真实证据链

### 3.6 replay

列出回放场景：

```powershell
python -m gas_calibrator.v2.scripts.run_validation_replay --list-scenarios
```

运行回放：

```powershell
python -m gas_calibrator.v2.scripts.run_validation_replay `
  --scenario primary_latest_missing
```

用途：

- 从 `tests/v2/fixtures/replay` 或显式 fixture 构建离线回放工件
- 验证历史证据、latest 缺失、snapshot-only 等非真实流程语义

### 3.7 suite

```powershell
python -m gas_calibrator.v2.scripts.run_simulation_suite --suite smoke
python -m gas_calibrator.v2.scripts.run_simulation_suite --suite regression
python -m gas_calibrator.v2.scripts.run_simulation_suite --suite nightly
python -m gas_calibrator.v2.scripts.run_simulation_suite --suite parity
```

suite 定义来自 `sim/scenarios/suites.py`：

| suite | 作用 | 典型覆盖 |
| --- | --- | --- |
| `smoke` | 核心离线烟测 | 主成功路径、路由异常、参考降级、`summary_parity` |
| `regression` | 日常回归 | 主路径、关键故障、回放 latest 语义 |
| `nightly` | 扩展覆盖 | regression + `export_resilience` + `summary_parity` |
| `parity` | 口径门禁 | 只跑 `summary_parity` |

### 3.8 工件补建与历史扫描

补建当前 run / suite 的治理工件：

```powershell
python -m gas_calibrator.v2.scripts.build_offline_governance_artifacts --run-dir <run_dir>
python -m gas_calibrator.v2.scripts.build_offline_governance_artifacts --suite-dir <suite_dir>
```

历史工件扫描 / 重建：

```powershell
python -m gas_calibrator.v2.scripts.historical_artifacts scan --root-dir src/gas_calibrator/v2/output
python -m gas_calibrator.v2.scripts.historical_artifacts reindex --root-dir src/gas_calibrator/v2/output
```

用途：

- 补齐 analytics / acceptance / lineage / registry 工件
- 做历史 run 的兼容性扫描与 sidecar 重建

## 4. 常用配置文件

| 配置 | 作用 |
| --- | --- |
| `src/gas_calibrator/v2/configs/smoke_v2_minimal.json` | 最小 headless 仿真运行配置 |
| `src/gas_calibrator/v2/configs/smoke_v2_measurement_trace.json` | 带测量过程证据的仿真配置 |
| `src/gas_calibrator/v2/configs/test_v2_safe.json` | `test_v2_safe` 专用安全配置 |
| `src/gas_calibrator/v2/configs/test_v2_config.json` | `test_v2_device` 常用配置 |
| `src/gas_calibrator/v2/configs/storage_config.json` | 存储侧附加配置 |
| `src/gas_calibrator/v2/configs/ai_config.json` | AI 能力附加配置 |

配置原则：

- 所有相对路径最终会由 `entry.py` 归一化为绝对路径
- 非默认 Step 2 配置需要双重解锁时，必须显式说明原因和风险
- 仿真运行应优先使用仓库内隔离输出目录

## 5. 变更类型与推荐验证

| 改动类型 | 最低验证 | 补充验证 |
| --- | --- | --- |
| 文档 / 注释 | 链接检查、`git diff --check` | 无需跑 suite |
| `entry.py` / `config/` / `core` 主链路 | `test_v2_safe` | 最小 headless run |
| `sim/` / compare / replay | `run_simulation_suite --suite smoke` | `regression` 或目标 replay |
| parity 口径 | `run_simulation_suite --suite parity` | 需要时补 `regression` |
| 导出 / 工件 / registry | `run_simulation_suite --suite nightly` 或 resilience | `build_offline_governance_artifacts` |
| `ui_v2/` | 相关 UI pytest + 手工仿真 UI 检查 | 中文 i18n 与 1080p 布局回归 |

## 6. 与测试目录的对应关系

V2 自动化测试主要位于 `tests/v2/`。常见映射：

- 入口 / 配置：`test_config_models.py`、`test_entry_runtime_hooks.py`
- 主运行链路：`test_calibration_service.py`、`test_orchestrator.py`
- 工件治理：`test_offline_artifacts.py`、`test_result_store.py`、`test_acceptance_governance.py`
- parity / resilience：`test_export_resilience.py`、`test_compare_v1_v2_control_flow.py`
- UI / 中文化：`test_ui_v2_*.py`、`test_closeout_readiness_ui_parity.py`

如果任务触及这些合同，文档里就不应只写“建议验证”，而应明确指出对应测试类别。

## 7. 当前阶段禁止事项

- 不把 replay / suite 结果写成真实 acceptance 结论
- 不用 `--publish-latest` 去覆盖真实主证据链
- 不在 V2 上做真机 short run
- 不把 future real bench 预留参数写成日常操作步骤
