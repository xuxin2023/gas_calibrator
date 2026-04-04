# Step 2 V1 -> V2 Sync Matrix

本矩阵基于最新代码、最新测试与本轮命令结果重审，不依赖旧 markdown 收口。

边界声明：
- 不改 `run_app.py`
- 不回接 V1 UI
- 不引入真实 COM / 真机路径
- 只接受 simulation / replay / suite / parity / resilience / offline review / UI contract / headless 证据
- 以下所有结论都不是 real acceptance evidence

| V1/Shared change | V1/shared source files | Current V2 status | Why it matters | Safe Step-2 sync target | Risk to V1 | Suggested tests | Should enter default workflow? |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `dewpoint_flush_gate` | `src/gas_calibrator/validation/dewpoint_flush_gate.py` | `partially synced` | V2 已有气路露点 gate；pressure-scaled dewpoint 预测也已被离线 QC 复用，但没有把 V1 的完整 runtime gate 直接搬进 V2 主链 | `src/gas_calibrator/v2/core/orchestrator.py`, `src/gas_calibrator/v2/core/services/qc_service.py`, `src/gas_calibrator/v2/core/offline_artifacts.py` | 低；只要维持 offline/review，不会影响 V1 默认流程 | `python -m pytest tests/test_dewpoint_flush_gate.py tests/v2/test_co2_route_runner.py tests/v2/test_qc_service.py -q` | no |
| `runner_co2_quality_guards` | `src/gas_calibrator/workflow/runner.py`, `src/gas_calibrator/validation/dewpoint_flush_gate.py` | `already synced` | `rebound veto`、pressure-scaled expected dewpoint physical QC、`pressure_gauge_stale_*` 已进入 V2 QC / artifacts / review，且默认受 config gate 控制 | `src/gas_calibrator/v2/core/services/qc_service.py`, `src/gas_calibrator/v2/core/result_store.py`, `src/gas_calibrator/v2/core/offline_artifacts.py` | 低；当前仅 simulation/offline/review，未改真实执行默认行为 | `python -m pytest tests/test_runner_co2_quality_guards.py tests/v2/test_qc_service.py tests/v2/test_analytics_service.py tests/v2/test_measurement_analytics_service.py -q` | no |
| `pressure_point_selection` (`selected_pressure_points`, `ambient`, `ambient_open`) | `src/gas_calibrator/workflow/runner.py`, `tests/test_pressure_point_selection.py` | `already synced` | 这是 V1/V2 压力点语义对齐的核心合同，直接影响 plan compile、preview、workbench/results/review handoff 的口径 | `src/gas_calibrator/v2/domain/pressure_selection.py`, `src/gas_calibrator/v2/config/models.py`, `src/gas_calibrator/v2/domain/plan_models.py`, `src/gas_calibrator/v2/core/models.py`, `src/gas_calibrator/v2/core/point_parser.py`, `src/gas_calibrator/v2/core/route_planner.py`, `src/gas_calibrator/v2/core/plan_compiler.py`, `src/gas_calibrator/v2/ui_v2/controllers/plan_gateway.py`, `src/gas_calibrator/v2/ui_v2/controllers/app_facade.py`, `src/gas_calibrator/v2/core/result_store.py` | 低；只同步语义和数据合同，未复制 V1 Tk UI | `python -m pytest tests/test_pressure_point_selection.py tests/v2/test_config_models.py tests/v2/test_plan_compiler.py tests/v2/test_ui_v2_plan_editor_page.py tests/v2/test_ui_v2_app_facade.py tests/v2/test_results_gateway.py -q` | yes |
| `room_temp_co2_pressure_diagnostic / plots / analyzer chain isolation` | `src/gas_calibrator/validation/room_temp_co2_pressure_diagnostic.py`, `src/gas_calibrator/validation/room_temp_co2_pressure_plots.py`, `tests/test_room_temp_co2_pressure_diagnostic.py` | `missing` | V1/shared 已具备完整离线工程诊断与 analyzer-chain isolation 能力，但当前 V2 代码里没有 dedicated adapter；这类能力适合进入 offline analytics / reports / review，而不该进入默认执行链 | 后续仅建议落到现有 `v2/core/offline_artifacts.py`, `v2/adapters/results_gateway.py`, `v2/ui_v2/controllers/app_facade.py`, `v2/ui_v2/pages/reports_page.py` 的离线工件识别与 review adapter | 低到中；若误接默认执行链会拉宽 Step 2 范围 | `python -m pytest tests/test_room_temp_co2_pressure_diagnostic.py tests/v2/test_results_gateway.py tests/v2/test_ui_v2_reports_page.py tests/v2/test_ui_v2_review_center.py -q` | no |
| `validate_verification_doc` | `src/gas_calibrator/tools/validate_verification_doc.py`, `tests/test_validate_verification_doc.py` | `partially synced` | 这类文档工具适合作为 standalone/offline validator；不该接 V2 默认运行入口。本轮已确认失败根因是测试硬编码桌面路径，而不是解析逻辑本身 | 仅保留 standalone/offline helper；不要接 `run_v2`、不要接 V1 UI | 低；只修可移植性不会影响 V1/V2 主链 | `python -m pytest tests/test_validate_verification_doc.py -q` | no |
| gas analyzer coefficient quiet I/O | `src/gas_calibrator/devices/gas_analyzer.py`, `tests/test_gas_analyzer_mode2.py` | `already synced` | V2 coefficient downloader 直接复用 shared `GasAnalyzer`，因此 `_prepare_coefficient_io()`、`COEFFICIENT_COMM_QUIET_DELAY_S` 已天然继承 | `src/gas_calibrator/v2/adapters/analyzer_coefficient_downloader.py` | 低；仍是独立下载路径，不改变 V2 默认运行链 | `python -m pytest tests/v2/test_analyzer_coefficient_downloader.py tests/v2/test_ratio_poly_report.py -q` | no |
| legacy `YGAS` mode1 parse | `src/gas_calibrator/devices/gas_analyzer.py`, `tests/test_validate_verification_doc.py` | `already synced` | V2 仍通过 shared `GasAnalyzer.parse_line()` 继承 legacy frame parse，兼容旧帧格式 | shared parser through V2 analyzer adapters | 低；不需单独复制实现 | `python -m pytest tests/test_validate_verification_doc.py tests/test_gas_analyzer_mode2.py -q` | yes |
| `logging_utils` 新增字段与空导出裁剪 | `src/gas_calibrator/logging_utils.py`, `tests/test_logging_utils.py` | `partially synced` | V1 新增的是字段 taxonomy 与空导出治理，不是 logger 类本体。当前 V2 已同步 `pressure_mode` / `pressure_target_label` / post-seal / stale gauge 关键字段到 results/artifacts/review，但未做 V1 logger 级别逐项 parity | `src/gas_calibrator/v2/core/result_store.py`, `src/gas_calibrator/v2/core/offline_artifacts.py`, `src/gas_calibrator/v2/adapters/results_gateway.py` | 低；只同步字段语义，不搬 V1 logger 实现 | `python -m pytest tests/test_logging_utils.py tests/v2/test_results_gateway.py tests/v2/test_ui_v2_results_page.py tests/v2/test_ui_v2_reports_page.py -q` | yes |

## Current Step-2 Classification

- `already synced`
  - pressure point selection ambient semantics
  - CO2 post-seal quality guards
  - coefficient quiet I/O inheritance
  - legacy `YGAS` mode1 parse inheritance
- `safe to keep in current Step 2 path`
  - ambient semantics in config / compiler / handoff
  - post-seal guard summaries in QC / analytics / artifacts / review
- `sync later / offline only`
  - room-temp diagnostic / analyzer-chain isolation adapters
  - deeper logging/export parity beyond current field taxonomy
- `do not sync into default path`
  - any V1 Tk UI behavior
  - any real-COM / engineering-only config path
  - verification-doc tooling as default workflow step

## Verification Doc Portability Audit

- `tests/test_validate_verification_doc.py` 的失败根因是硬编码桌面绝对路径，不是 `load_template_spec()` 本身不可用。
- 当前可移植修法是使用最小 `.docx` fixture，并保持 helper 为 standalone/offline。
- 该 helper 仍不应接入 V2 默认入口，也不应被解释为 real acceptance evidence。
