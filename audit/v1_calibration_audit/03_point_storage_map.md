# 点位存储链路

## 每个点位的数据链路

- 点位编号从哪里来: `CalibrationPoint.index` 直接取 Excel 行号，见 `src/gas_calibrator/data/points.py:21-38`, `src/gas_calibrator/data/points.py:98-288`。
- 目标标准值从哪里来: CO2 目标来自 `point.co2_ppm`，H2O 目标来自 `point.h2o_mmol/hgen_*`，压力目标来自 `point.target_pressure_hpa`，样本组装见 `src/gas_calibrator/workflow/runner.py:14815-15181`。
- 原始数据从哪里读: 分析仪数据由 `_read_sensor_parsed`/MODE2 帧缓存进入样本；露点/压力/温湿度来自 fast signal 与 slow aux cache，见 `src/gas_calibrator/devices/gas_analyzer.py:461-472`, `src/gas_calibrator/devices/gas_analyzer.py:517-535`, `src/gas_calibrator/workflow/runner.py:14815-15181`。
- 是否有稳态等待/冲洗/延时: 有。CO2 路线预冲洗和零气特殊 flush 见 `src/gas_calibrator/workflow/runner.py:10896-11104`, `src/gas_calibrator/workflow/runner.py:11463-11549`；压力达标后采样前门禁见 `src/gas_calibrator/workflow/runner.py:12624-12811`；采样 freshness gate 见 `src/gas_calibrator/workflow/runner.py:12006-12125`。
- 是否用窗口平均/标准差: 样本采集前的稳态判定使用时间窗峰峰值；点位汇总时再计算 mean/std，见 `src/gas_calibrator/workflow/runner.py:9212-9483`, `src/gas_calibrator/workflow/runner.py:13677-13811`。
- 何时触发保存: `_sample_and_log` 在采集结束、完成质量与完整性汇总后，触发 light/heavy export，见 `src/gas_calibrator/workflow/runner.py:15286-15549`, `src/gas_calibrator/workflow/runner.py:13858-13876`, `src/gas_calibrator/workflow/runner.py:13813-13856`。
- 保存到哪里: `samples_*.csv`、`point_XXXX*_samples.csv`、`points_*.csv`、`points_readable_*.csv/.xlsx`、`分析仪汇总_*.csv/.xlsx`，见 `src/gas_calibrator/logging_utils.py:756-818`, `src/gas_calibrator/logging_utils.py:938-940`, `src/gas_calibrator/logging_utils.py:942-960`, `src/gas_calibrator/logging_utils.py:1720-1756`, `src/gas_calibrator/logging_utils.py:1262-1393`。

## 保存 payload

- 样本级 payload 关键字段: `point_title/sample_ts/sample_start_ts/sample_end_ts/point_phase/point_tag/point_row/co2_ppm_target/h2o_mmol_target/pressure_target_hpa/co2_ppm/h2o_mmol/pressure_hpa/pressure_gauge_hpa/dewpoint_sample_ts/...`，见 `src/gas_calibrator/workflow/runner.py:14815-15181`, `src/gas_calibrator/logging_utils.py:334-454`, `src/gas_calibrator/logging_utils.py:109-286`。
- 点位汇总 payload 关键字段: `point_row/point_phase/point_tag/targets/mean/std/valid_count/quality/timing`，见 `src/gas_calibrator/workflow/runner.py:13677-13811`。

## 必答问题

- 每个点位是否有唯一标识: PASS。当前实现依赖 `point_row + point_phase + point_tag` 组合，而不是单独 UUID；构造点与 tag 见 `src/gas_calibrator/workflow/runner.py:5066-5085`, `src/gas_calibrator/workflow/runner.py:5087-5106`, `src/gas_calibrator/workflow/runner.py:5108-5118`, `src/gas_calibrator/workflow/runner.py:5120-5130`, `src/gas_calibrator/workflow/runner.py:13677-13811`。
- 是否保存 raw timestamp 和 save timestamp: 部分 FAIL。样本有 `sample_ts`、设备采样时间戳和 `sample_end_ts`，见 `src/gas_calibrator/workflow/runner.py:14815-15181`, `src/gas_calibrator/logging_utils.py:334-454`；但点位/样本导出没有单独的 `save_ts`/`insert_ts` 字段，见 `src/gas_calibrator/logging_utils.py:938-940`, `src/gas_calibrator/logging_utils.py:942-960`, `src/gas_calibrator/workflow/runner.py:13677-13811`。
- 是否区分采样时间与入库时间: FAIL。当前只持久化采样相关时间，没有单独入库时间戳，见 `src/gas_calibrator/workflow/runner.py:14815-15181`, `src/gas_calibrator/workflow/runner.py:13677-13811`。
- 是否可能把“最新一条高频数据”误存成当前点位: 当前代码有 freshness gate 与压力后门禁，结论 PASS，但仅限静态审计；证据见 `src/gas_calibrator/workflow/runner.py:12006-12125`, `src/gas_calibrator/workflow/runner.py:12624-12811`, `tests/test_audit_v1_trace_check.py:159-176`。
- 是否可能上一点位/过渡态/未稳定数据被保存到下一点位: 代码有 route handoff + deferred export 保护，结论 PASS；见 `src/gas_calibrator/workflow/runner.py:13878-13897`, `src/gas_calibrator/workflow/runner.py:13899-13912`, `src/gas_calibrator/workflow/runner.py:13914-13937`, `src/gas_calibrator/workflow/runner.py:13939-13954`, `tests/test_runner_route_handoff.py:76-142`。
- 是否可能覆盖前一个点位，而不是新增一条: 对“不同点位”结论 PASS。`samples.csv/points.csv` 追加写入，单点样本文件按 `point_row + phase + tag` 分文件，离线测试见 `src/gas_calibrator/logging_utils.py:938-940`, `src/gas_calibrator/logging_utils.py:991-1007`, `src/gas_calibrator/logging_utils.py:1720-1756`, `tests/test_audit_v1_trace_check.py:107-156`。但如果同一 `point_row + phase + tag` 被重复导出，单点样本 CSV 会覆盖同名文件，这属于同标识重写，不是不同点位覆盖。
- 是否能追溯标定前系数、标定后系数、上一次系数: 主 runner 结论 FAIL。主路径只写 `SENCO`，没有在本流程里保存 before/after snapshot；见 `src/gas_calibrator/workflow/runner.py:16213-16335`。旁路工具 `run_v1_merged_calibration_sidecar.py` 可以单独读 before/after，但不是主 runner 自动路径。
- CO2 和 H2O 两套点位表结构是否一致: 样本主结构大体一致，共用 `COMMON_SHEET_FIELDS`，但目标字段和 H2O 预封压露点快照有差异；见 `src/gas_calibrator/logging_utils.py:334-454`, `src/gas_calibrator/workflow/runner.py:14815-15181`, `tests/test_runner_collect_only.py:1343-1426`。

## 点位存储风险表

| 项目 | 结论 | 证据 |
| --- | --- | --- |
| 点位唯一标识 | PASS | `src/gas_calibrator/workflow/runner.py:13677-13811`, `src/gas_calibrator/workflow/runner.py:5108-5118`, `src/gas_calibrator/workflow/runner.py:5120-5130` |
| 样本原始时间戳 | PASS | `src/gas_calibrator/workflow/runner.py:14815-15181`, `src/gas_calibrator/logging_utils.py:334-454` |
| 保存时间戳 | FAIL | `src/gas_calibrator/logging_utils.py:938-940`, `src/gas_calibrator/logging_utils.py:942-960`, `src/gas_calibrator/workflow/runner.py:13677-13811` |
| 采样/入库时间区分 | FAIL | `src/gas_calibrator/workflow/runner.py:14815-15181`, `src/gas_calibrator/workflow/runner.py:13677-13811` |
| 切点后立即取最新值 | PASS | `src/gas_calibrator/workflow/runner.py:12624-12811`, `src/gas_calibrator/workflow/runner.py:12006-12125`, `tests/test_audit_v1_trace_check.py:159-176` |
| 过渡态混入下一点位 | PASS | `src/gas_calibrator/workflow/runner.py:13878-13897`, `src/gas_calibrator/workflow/runner.py:13899-13912`, `tests/test_runner_route_handoff.py:76-142` |
| 不同点位互相覆盖 | PASS | `src/gas_calibrator/logging_utils.py:1720-1756`, `tests/test_audit_v1_trace_check.py:107-156` |
| 系数 before/after 追溯 | FAIL | `src/gas_calibrator/workflow/runner.py:16213-16335`, `src/gas_calibrator/tools/run_v1_corrected_autodelivery.py:839-950` |
| CO2/H2O 点位表结构完全一致 | UNKNOWN | `src/gas_calibrator/logging_utils.py:334-454`, `src/gas_calibrator/workflow/runner.py:14815-15181` |
