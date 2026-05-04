# V2 工件与证据治理

## 1. 目标

V2 的输出不只是“跑完生成若干文件”，而是要把一次仿真运行、一次 suite、一次 replay 变成可追溯、可评审、可比对的离线证据包。当前阶段的治理目标有三项：

- 守住 artifact role / export status 合同
- 守住 simulated evidence 与 real acceptance evidence 的边界
- 让 operator / reviewer / approver 都能从同一套工件看到一致结论

## 2. 主要输出位置

常见输出根目录：

- `src/gas_calibrator/v2/output/test_v2_safe/`
- `src/gas_calibrator/v2/output/test_v2/`
- `src/gas_calibrator/v2/output/v1_v2_compare/`
- `src/gas_calibrator/v2/output/smoke_v2_measurement_trace/`

单次运行通常会沉淀到 `run_<timestamp>/` 子目录，suite 会沉淀到 `suite_<name>_<timestamp>/` 或显式 `run-name/` 目录。

## 3. artifact role 合同

V2 对外治理时必须把工件分到以下四类：

| artifact role | 含义 | 常见内容 |
| --- | --- | --- |
| `execution_rows` | 逐行、逐点、逐样本的原始执行产物 | `results.json`、`samples.csv`、`points_readable.csv` |
| `execution_summary` | 运行摘要与主索引 | `summary.json`、`manifest.json`、`point_summaries.json`、`suite_summary.json` |
| `diagnostic_analysis` | 用于定位问题、解释状态的诊断工件 | parity / resilience 报告、`measurement_phase_coverage`、`multi_source_stability`、`state_transition_evidence` |
| `formal_analysis` | 面向治理、审核、交接的结构化工件 | acceptance plan、evidence registry、lineage summary、registry indexes、review packs |

要求：

- 同一工件只能有一个主角色，不要同时归到多个 role。
- 新增工件时，必须先决定 role，再决定落点与 UI 展示位置。
- review center 与 reports 页面应按 role 展示，不直接按文件名堆叠。

## 4. export status 合同

统一导出状态必须保持以下四个值：

| status | 含义 |
| --- | --- |
| `ok` | 工件已成功生成且可读取 |
| `skipped` | 工件按当前配置或流程被跳过 |
| `missing` | 工件理论应存在，但当前缺失 |
| `error` | 工件生成或读取失败 |

建议用法：

- `skipped` 用于设计上不应出现的工件
- `missing` 用于应该存在但没有落盘的工件
- `error` 用于生成失败、格式损坏、解析失败等异常

## 5. 证据边界字段

用户可见摘要层至少应暴露以下字段：

| 字段 | 作用 |
| --- | --- |
| `evidence_source` | 当前证据来源，如 simulated/offline/replay 类证据 |
| `evidence_state` | 证据当前状态，如 `replay`、`simulated_workbench`、`collected` |
| `acceptance_level` | 当前只能到离线回归、诊断或 real probe 级别，不能越界宣称真实验收 |
| `promotion_state` | 如 `dry_run_only`、`blocked`、`ready`，描述证据距离正式放行还有多远 |
| `not_real_acceptance_evidence` | 必须明确标记离线证据不是 real acceptance evidence |

说明：

- 当前代码里会细分 `simulated_protocol`、`replay` 等内部枚举；对外治理语义上都属于 simulated / offline 证据。
- 任何 suite、replay、workbench、offline review 结果都必须保留 `not_real_acceptance_evidence = true` 的边界。

## 6. 基础工件与治理工件

### 6.1 基础工件

由 `ResultStore` 主导写出：

- `results.json`
- `point_summaries.json`
- `summary.json`
- `manifest.json`
- `samples.csv`
- `points_readable.csv`

这些工件负责把一次 run 的“执行事实”记录下来。

### 6.2 治理工件

由 `core/offline_artifacts.py` 与相关 gateway/builders 生成，常见包括：

- analytics summary
- suite analytics summary
- acceptance plan
- evidence registry
- lineage summary
- point taxonomy handoff
- measurement phase coverage
- multi-source stability evidence
- state transition evidence
- review digest / compact summary / closeout packs

这些工件负责把“执行事实”整理成适合评审和追溯的视图。

## 7. UI 与治理的对应关系

| 视图 | 依赖重点 |
| --- | --- |
| Results | `execution_rows` + `execution_summary` |
| Reports | `diagnostic_analysis` + `formal_analysis` |
| Review Center | role、boundary、phase、fragment、artifact availability |
| Device Workbench | simulation-only 行为记录 + workbench evidence summary |

因此，改工件合同往往不只是改导出逻辑，也会影响：

- `adapters/results_gateway.py`
- `ui_v2/controllers/app_facade.py`
- `ui_v2/pages/reports_page.py`
- review center 相关扫描合同与渲染逻辑

## 8. 历史工件治理

当前提供两类辅助工具：

- `build_offline_governance_artifacts.py`
  - 对单个 run 或 suite 补建 acceptance / analytics / lineage / registry 工件
- `historical_artifacts.py`
  - 扫描历史目录、重建兼容 sidecar、输出重建索引

使用原则：

- 优先补 sidecar，不重写 primary evidence
- 补建行为必须是离线治理，不改变运行事实
- 历史工件再索引不等于真实验收刷新

## 9. 当前阶段的治理红线

- 不把 `real_primary_latest` 当成文档中的默认刷新目标
- 不把 Step 3 dossier / admission / closeout 工件写成“当前已放行”
- 不让 UI 把 simulated workbench 呈现为真实设备控制界面
- 不让 artifact role、status、boundary 字段在不同页面出现不同语义
