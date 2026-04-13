# 点位存储链路

## 每个点位的数据链路

- 点位编号从哪里来: `CalibrationPoint.index` 直接取 Excel 行号，见 `src/gas_calibrator/data/points.py:21-38`, `src/gas_calibrator/data/points.py:98-288`。
- 目标标准值从哪里来: CO2 目标来自 `point.co2_ppm`，H2O 目标来自 `point.h2o_mmol/hgen_*`，压力目标来自 `point.target_pressure_hpa`，样本组装见 `src/gas_calibrator/workflow/runner.py:15470-15836`。
- 原始数据从哪里读: 分析仪数据由 `_read_sensor_parsed`/MODE2 帧缓存进入样本；露点/压力/温湿度来自 fast signal 与 slow aux cache，见 `src/gas_calibrator/devices/gas_analyzer.py:461-472`, `src/gas_calibrator/devices/gas_analyzer.py:517-535`, `src/gas_calibrator/workflow/runner.py:15470-15836`。
- 是否有稳态等待/冲洗/延时: 有。CO2 路线预冲洗和零气特殊 flush 见 `src/gas_calibrator/workflow/runner.py:11034-11243`, `src/gas_calibrator/workflow/runner.py:11928-12024`；压力达标后采样前门禁见 `src/gas_calibrator/workflow/runner.py:13171-13358`；采样 freshness gate 见 `src/gas_calibrator/workflow/runner.py:12553-12672`。
- 是否用窗口平均/标准差: 样本采集前的稳态判定使用时间窗峰峰值；点位汇总时再计算 mean/std，见 `src/gas_calibrator/workflow/runner.py:9350-9621`, `src/gas_calibrator/workflow/runner.py:14228-14382`。
- 何时触发保存: `_sample_and_log` 在采集结束、完成质量与完整性汇总后，触发 light/heavy export，见 `src/gas_calibrator/workflow/runner.py:15941-16210`, `src/gas_calibrator/workflow/runner.py:14509-14531`, `src/gas_calibrator/workflow/runner.py:14463-14507`。
- 保存到哪里: `samples_*.csv`、`point_XXXX*_samples.csv`、`points_*.csv`、`points_readable_*.csv/.xlsx`、`分析仪汇总_*.csv/.xlsx`、`coefficient_writeback_*.csv`，见 `src/gas_calibrator/logging_utils.py:776-843`, `src/gas_calibrator/logging_utils.py:963-965`, `src/gas_calibrator/logging_utils.py:967-985`, `src/gas_calibrator/logging_utils.py:1778-1814`, `src/gas_calibrator/logging_utils.py:1045-1061`, `src/gas_calibrator/logging_utils.py:1320-1451`。

## 保存 payload

- 样本级 payload 关键字段: `run_id/session_id/device_id/gas_type/step/point_no/target_value/measured_value/sample_ts/save_ts/window_start_ts/window_end_ts/sample_count/stable_flag/...`，见 `src/gas_calibrator/workflow/runner.py:15470-15836`, `src/gas_calibrator/workflow/runner.py:14431-14461`, `src/gas_calibrator/logging_utils.py:109-306`。
- 点位汇总 payload 关键字段: `run_id/session_id/device_id/gas_type/step/point_no/target_value/measured_value/sample_ts/save_ts/window_start_ts/window_end_ts/sample_count/stable_flag/targets/mean/std/valid_count/quality/timing`，见 `src/gas_calibrator/workflow/runner.py:14228-14382`, `src/gas_calibrator/logging_utils.py:967-985`。

## 必答问题

- 每个点位是否有唯一标识: PASS。当前实现依赖 `point_row + point_phase + point_tag` 组合，而不是单独 UUID；构造点与 tag 见 `src/gas_calibrator/workflow/runner.py:5204-5223`, `src/gas_calibrator/workflow/runner.py:5225-5244`, `src/gas_calibrator/workflow/runner.py:5246-5256`, `src/gas_calibrator/workflow/runner.py:5258-5268`, `src/gas_calibrator/workflow/runner.py:14228-14382`。
- 是否保存 raw timestamp 和 save timestamp: PASS。样本与点位导出现在同时保留 `sample_ts` 和独立 `save_ts`，并补了窗口时间字段；见 `src/gas_calibrator/workflow/runner.py:15470-15836`, `src/gas_calibrator/workflow/runner.py:14431-14461`, `src/gas_calibrator/workflow/runner.py:14228-14382`, `src/gas_calibrator/logging_utils.py:963-965`, `src/gas_calibrator/logging_utils.py:967-985`。
- 是否区分采样时间与入库时间: PASS。`sample_ts` 保留采样时间，`save_ts` 在真正导出前写入，证据见 `src/gas_calibrator/workflow/runner.py:14509-14531`, `src/gas_calibrator/workflow/runner.py:14463-14507`, `src/gas_calibrator/workflow/runner.py:14228-14382`。
- 是否可能把“最新一条高频数据”误存成当前点位: 当前代码有 freshness gate 与压力后门禁，结论 PASS，但仅限静态审计；证据见 `src/gas_calibrator/workflow/runner.py:12553-12672`, `src/gas_calibrator/workflow/runner.py:13171-13358`, `tests/test_audit_v1_trace_check.py:159-176`。
- 是否可能上一点位/过渡态/未稳定数据被保存到下一点位: 代码有 route handoff + deferred export 保护，结论 PASS；见 `src/gas_calibrator/workflow/runner.py:14533-14552`, `src/gas_calibrator/workflow/runner.py:14554-14567`, `src/gas_calibrator/workflow/runner.py:14569-14592`, `src/gas_calibrator/workflow/runner.py:14594-14609`, `tests/test_runner_route_handoff.py:76-142`。
- 是否可能覆盖前一个点位，而不是新增一条: 对“不同点位”结论 PASS。`samples.csv/points.csv` 追加写入，单点样本文件按 `point_row + phase + tag` 分文件，离线测试见 `src/gas_calibrator/logging_utils.py:963-965`, `src/gas_calibrator/logging_utils.py:1016-1032`, `src/gas_calibrator/logging_utils.py:1778-1814`, `tests/test_audit_v1_trace_check.py:107-156`。但如果同一 `point_row + phase + tag` 被重复导出，单点样本 CSV 会覆盖同名文件，这属于同标识重写，不是不同点位覆盖。
- 是否能追溯标定前系数、标定后系数、上一次系数: 主 runner 结论 PASS。当前主路径已把 `coeff_before/coeff_target/coeff_readback/coeff_rollback_*` 持久化到 `coefficient_writeback_*.csv`，见 `src/gas_calibrator/workflow/runner.py:16874-17009`, `src/gas_calibrator/workflow/runner.py:17011-17054`, `src/gas_calibrator/logging_utils.py:1045-1061`。
- CO2 和 H2O 两套点位表结构是否一致: 样本主结构大体一致，共用 `COMMON_SHEET_FIELDS`，但目标字段和 H2O 预封压露点快照有差异；见 `src/gas_calibrator/logging_utils.py:354-474`, `src/gas_calibrator/workflow/runner.py:15470-15836`, `tests/test_runner_collect_only.py:1381-1464`。

## 点位存储风险表

| 项目 | 结论 | 证据 |
| --- | --- | --- |
| 点位唯一标识 | PASS | `src/gas_calibrator/workflow/runner.py:14228-14382`, `src/gas_calibrator/workflow/runner.py:5246-5256`, `src/gas_calibrator/workflow/runner.py:5258-5268` |
| 样本原始时间戳 | PASS | `src/gas_calibrator/workflow/runner.py:15470-15836`, `src/gas_calibrator/logging_utils.py:354-474` |
| 保存时间戳 | PASS | `src/gas_calibrator/workflow/runner.py:14509-14531`, `src/gas_calibrator/workflow/runner.py:14463-14507`, `src/gas_calibrator/logging_utils.py:963-965`, `src/gas_calibrator/logging_utils.py:967-985` |
| 采样/入库时间区分 | PASS | `src/gas_calibrator/workflow/runner.py:14228-14382`, `src/gas_calibrator/workflow/runner.py:14509-14531`, `src/gas_calibrator/workflow/runner.py:14463-14507` |
| 切点后立即取最新值 | PASS | `src/gas_calibrator/workflow/runner.py:13171-13358`, `src/gas_calibrator/workflow/runner.py:12553-12672`, `tests/test_audit_v1_trace_check.py:159-176` |
| 过渡态混入下一点位 | PASS | `src/gas_calibrator/workflow/runner.py:14533-14552`, `src/gas_calibrator/workflow/runner.py:14554-14567`, `tests/test_runner_route_handoff.py:76-142` |
| 不同点位互相覆盖 | PASS | `src/gas_calibrator/logging_utils.py:1778-1814`, `tests/test_audit_v1_trace_check.py:107-156` |
| 系数 before/after 追溯 | PASS | `src/gas_calibrator/workflow/runner.py:16874-17009`, `src/gas_calibrator/workflow/runner.py:17011-17054`, `src/gas_calibrator/logging_utils.py:1045-1061` |
| CO2/H2O 点位表结构完全一致 | UNKNOWN | `src/gas_calibrator/logging_utils.py:354-474`, `src/gas_calibrator/workflow/runner.py:15470-15836` |
