# V1.5 活跃工作面与隔离清单

本报告由 `python -m gas_calibrator.tools.export_v1_5_entrypoint_inventory` 自动生成。
它不是删除清单，而是给 V1.5 正式版本建立一层导航边界：哪些文件属于当前活跃面，哪些只能作为历史参考、工程诊断或归档候选。

## 原则

- V1.5 是当前正式校准方向；V1/V2 只保留为历史算法、审计和对照资料。
- 诊断工具可以保留，但不能作为正式 acceptance、正式 CO2/H2O 拟合或默认入口。
- 临时配置、现场 observed/generated 配置和根目录运行日志不得作为默认配置直接复用。
- 本报告只做离线文件识别；不打开 COM、不控制气路/水路/压力、不写 SENCO。

## 隔离/归档候选

| Surface | Status | Action | Count | Path | Reason | Examples |
|---|---|---|---:|---|---|---|
| `legacy_v2_source_tree` | `legacy_reference_only` | `exclude_from_v1_5_active_surface` | 337 | `src/gas_calibrator/v2` | V1.5 is the final production direction; V2 source stays as archived reference unless explicitly revived. | `src/gas_calibrator/v2/__init__.py`<br>`src/gas_calibrator/v2/adapters/__init__.py`<br>`src/gas_calibrator/v2/adapters/analyzer_coefficient_downloader.py`<br>`src/gas_calibrator/v2/adapters/legacy_runner.py`<br>`src/gas_calibrator/v2/adapters/method_confirmation_gateway.py`<br>`src/gas_calibrator/v2/adapters/offline_refit_runner.py`<br>`src/gas_calibrator/v2/adapters/recognition_scope_gateway.py`<br>`src/gas_calibrator/v2/adapters/results_gateway.py` |
| `legacy_v2_tests` | `legacy_reference_only` | `exclude_from_v1_5_active_surface` | 240 | `tests/v2` | V2 tests protect old simulation/replay work only; they are not V1.5 formal-flow gates. | `tests/v2/fixtures/replay/co2_only_skip0_success_single_temp.json`<br>`tests/v2/fixtures/replay/co2_route_entered_but_sample_count_mismatch.json`<br>`tests/v2/fixtures/replay/co2_route_entered_sample_mismatch.json`<br>`tests/v2/fixtures/replay/compare_generates_partial_artifacts_on_failure.json`<br>`tests/v2/fixtures/replay/full_route_success_all_temps_all_sources.json`<br>`tests/v2/fixtures/replay/gauge_no_response.json`<br>`tests/v2/fixtures/replay/h2o_route_success_single_temp.json`<br>`tests/v2/fixtures/replay/humidity_generator_timeout.json` |
| `legacy_v2_docs` | `legacy_reference_only` | `exclude_from_v1_5_active_surface` | 2 | `docs/architecture/*v2*` | V2 cutover/replay documents are historical context and must not steer V1.5 formal operations. | `docs/architecture/v1_to_v2_behavior_contract.md`<br>`docs/architecture/v2_cutover_checklist.md` |
| `legacy_v1_reference_tools` | `legacy_reference_only` | `do_not_start_v1_5_here` | 4 | `src/gas_calibrator/tools/run_v1_*` | Old V1 write/acceptance tools may preserve algorithm history but must not launch V1.5. | `src/gas_calibrator/tools/run_v1_corrected_autodelivery.py`<br>`src/gas_calibrator/tools/run_v1_merged_calibration_sidecar.py`<br>`src/gas_calibrator/tools/run_v1_no500_postprocess.py`<br>`src/gas_calibrator/tools/run_v1_online_acceptance.py` |
| `v1_5_diagnostic_tools` | `diagnostic_only` | `guarded_engineering_use_only` | 11 | `src/gas_calibrator/tools/*diagnostic*` | Diagnostics are useful for root cause work but cannot enter formal acceptance or CO2/H2O fitting by default. | `src/gas_calibrator/tools/export_single_gas_pressure_curve.py`<br>`src/gas_calibrator/tools/export_v1_5_co2_post_h2o_diagnostic.py`<br>`src/gas_calibrator/tools/export_v1_5_h2o_special_diagnostic_queue.py`<br>`src/gas_calibrator/tools/export_v1_5_h2o_state_transfer_diagnostic.py`<br>`src/gas_calibrator/tools/probe_v1_5_getco9_protocol.py`<br>`src/gas_calibrator/tools/run_room_temp_co2_pressure_diagnostic.py`<br>`src/gas_calibrator/tools/run_v1_5_dewpoint_gate_extended_hold_after_gate.py`<br>`src/gas_calibrator/tools/run_v1_5_no_outp_preseal_probe.py` |
| `temporary_or_observed_v1_5_configs` | `review_before_use` | `do_not_use_as_default_config` | 11 | `configs/site_v1_5_*current* / *observed* / *generated*` | Observed/generated/current configs may reflect a bench snapshot; formal runs should use an explicit reviewed site config. | `configs/default_config_corrected_autodelivery.json`<br>`configs/default_config_corrected_autodelivery_real_smoke_no500_20260407.json`<br>`configs/points_v1_5_co2_20c_100ppm_limited_ambient_2sealed_nowait.xlsx`<br>`configs/site_v1_5_no_write_current_hardware_co2_20c.json`<br>`configs/site_v1_5_no_write_current_hardware_co2_20c_1000ppm_full_pressure_controlled_outp_skip_tempwait.json`<br>`configs/site_v1_5_no_write_current_hardware_co2_20c_1000ppm_full_pressure_no_outp_skip_tempwait.json`<br>`configs/site_v1_5_no_write_current_hardware_co2_20c_1000ppm_full_pressure_no_tempwait.json`<br>`configs/site_v1_5_no_write_current_hardware_co2_20c_1000ppm_full_pressure_no_tempwait_no_outp.json` |
| `root_temporary_run_artifacts` | `archive_candidate` | `move_to_logs_or_archive_after_review` | 7 | `repo-root logs/csv/md` | Root-level run artifacts slow navigation and can look like active inputs; keep evidence, but outside the code entrypoint surface. | `post_route_close_delay_root_cause.md`<br>`post_route_close_fast_control_replay.csv`<br>`post_route_close_to_outp1_timeline.csv`<br>`v1_5_7ea_no_write_limited_0ppm_ambient_2sealed_20260520_093150.stderr.log`<br>`v1_5_7ea_no_write_limited_0ppm_ambient_2sealed_20260520_093150.stdout.log`<br>`v1_5_f2fa_no_write_limited_0ppm_ambient_2sealed_20260520_103644.stdout.log`<br>`v1_5_vs_v2_post_route_close_control_diff.csv` |

## 默认入口策略校验

- blocker：`0`
- review：`0`

| Severity | Rule | Path | Message |
|---|---|---|---|
| `ready` | `none` | `` | V1.5 canonical path is not pointing at isolated surfaces. |

## 建议执行顺序

1. 先使用正式入口清单中的 canonical V1.5 主线查流程。
2. 遇到 legacy V1/V2 文件，只作为算法或审计参考，不从那里启动正式流程。
3. 遇到 diagnostic 工具，必须保留 `diagnostic_only` 语义，不进入正式拟合。
4. 遇到 observed/generated/current 配置，先人工复核设备 ID、串口映射、S5-S9 中性化、压力/温度前置状态，再决定是否复制为正式 site config。
5. 根目录临时日志和 CSV 先归档到 `logs/` 或证据包，不直接删除。
