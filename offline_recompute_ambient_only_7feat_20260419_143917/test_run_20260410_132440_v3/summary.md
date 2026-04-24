# corrected-entry no-500 summary

- run_dir: D:\gas_calibrator\offline_recompute_ambient_only_7feat_20260419_143917\test_run_20260410_132440_v3\_staging_run
- output_dir: D:\gas_calibrator\offline_recompute_ambient_only_7feat_20260419_143917\test_run_20260410_132440_v3
- pressure_row_source: current_ambient
- h2o_zero_span_status: NOT_SUPPORTED
- h2o_zero_span_note: Current HEAD V1 only supports the CO2 main chain; H2O zero/span is NOT_SUPPORTED.

## filter summary
- 分析仪汇总_气路_run_20260410_132440.xlsx: original=152 removed=0 kept=152
- 分析仪汇总_水路_run_20260410_132440.xlsx: original=48 removed=0 kept=48

## run structure hints
- [info] 当前自动后处理只覆盖本轮 run: 如果一次校准分成多轮完成，建议改用 merged calibration sidecar，并按 ActualDeviceId 合并后再计算最终系数。
- [warn] 当前 run 仅包含 ambient 压力工况: 不修改本轮结果，但若想更接近 2026-04-03 的约束力，建议后续至少保留 1 个 sealed 压力点。
- [warn] H2O 0ppm 气路锚点覆盖不完整: 期望温组 [10.0]°C，当前仅匹配到 []°C，缺少 [10.0]°C。
- [warn] 本轮有 4 个 H2O 气路锚点被质量门禁剔除: 命中 PointRow=14, 14, 14, 14。这不会改变原始点位记录，但会从 H2O 拟合中排除这些锚点。
