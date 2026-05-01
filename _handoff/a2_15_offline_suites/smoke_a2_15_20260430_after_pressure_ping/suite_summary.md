# 仿真套件摘要

- 套件: smoke
- 说明: 核心离线烟测：主成功路径、路由门禁、参考仪器门禁。
- 报告目录: D:\gas_calibrator\_handoff\a2_15_offline_suites\smoke_a2_15_20260430_after_pressure_ping
- 总用例数: 5
- 通过: 5
- 失败: 0
- 验收就绪度: offline_regression | dry_run_only | missing 5 gates
- 分析摘要: 5/5 passed | top failures none
- 操作员视角: run health ready
- 审阅人视角: evidence complete
- 批准人视角: promotion blocked

## 证据工件
- 套件摘要 JSON: D:\gas_calibrator\_handoff\a2_15_offline_suites\smoke_a2_15_20260430_after_pressure_ping\suite_summary.json
- 套件分析摘要: D:\gas_calibrator\_handoff\a2_15_offline_suites\smoke_a2_15_20260430_after_pressure_ping\suite_analytics_summary.json
- 套件验收计划: D:\gas_calibrator\_handoff\a2_15_offline_suites\smoke_a2_15_20260430_after_pressure_ping\suite_acceptance_plan.json
- 套件证据索引: D:\gas_calibrator\_handoff\a2_15_offline_suites\smoke_a2_15_20260430_after_pressure_ping\suite_evidence_registry.json

## 失败用例
- 无

## 失败复盘
- 本套件无失败用例。

## 用例结果
- full_route_success_with_relay_and_thermometer: 一致（期望 一致）[工件: D:\gas_calibrator\_handoff\a2_15_offline_suites\smoke_a2_15_20260430_after_pressure_ping\full_route_success_with_relay_and_thermometer]
  证据=协议仿真 状态=已收集 风险=低 失败类型=无 失败阶段=full_route_success_with_relay_and_thermometer
  审阅工件=json:D:\gas_calibrator\_handoff\a2_15_offline_suites\smoke_a2_15_20260430_after_pressure_ping\full_route_success_with_relay_and_thermometer\control_flow_compare_report.json md:D:\gas_calibrator\_handoff\a2_15_offline_suites\smoke_a2_15_20260430_after_pressure_ping\full_route_success_with_relay_and_thermometer\control_flow_compare_report.md
- relay_stuck_channel_causes_route_mismatch: 不一致（期望 不一致）[工件: D:\gas_calibrator\_handoff\a2_15_offline_suites\smoke_a2_15_20260430_after_pressure_ping\relay_stuck_channel_causes_route_mismatch]
  证据=协议仿真 状态=已收集 风险=中 失败类型=路由物理不一致 失败阶段=relay_stuck_channel_causes_route_mismatch
  审阅工件=json:D:\gas_calibrator\_handoff\a2_15_offline_suites\smoke_a2_15_20260430_after_pressure_ping\relay_stuck_channel_causes_route_mismatch\control_flow_compare_report.json md:D:\gas_calibrator\_handoff\a2_15_offline_suites\smoke_a2_15_20260430_after_pressure_ping\relay_stuck_channel_causes_route_mismatch\control_flow_compare_report.md
- thermometer_stale_reference: 一致（期望 一致）[工件: D:\gas_calibrator\_handoff\a2_15_offline_suites\smoke_a2_15_20260430_after_pressure_ping\thermometer_stale_reference]
  证据=协议仿真 状态=已收集 风险=中 失败类型=参考质量 失败阶段=thermometer_stale_reference
  审阅工件=json:D:\gas_calibrator\_handoff\a2_15_offline_suites\smoke_a2_15_20260430_after_pressure_ping\thermometer_stale_reference\control_flow_compare_report.json md:D:\gas_calibrator\_handoff\a2_15_offline_suites\smoke_a2_15_20260430_after_pressure_ping\thermometer_stale_reference\control_flow_compare_report.md
- pressure_reference_degraded: 一致（期望 一致）[工件: D:\gas_calibrator\_handoff\a2_15_offline_suites\smoke_a2_15_20260430_after_pressure_ping\pressure_reference_degraded]
  证据=协议仿真 状态=已收集 风险=中 失败类型=参考质量 失败阶段=pressure_reference_degraded
  审阅工件=json:D:\gas_calibrator\_handoff\a2_15_offline_suites\smoke_a2_15_20260430_after_pressure_ping\pressure_reference_degraded\control_flow_compare_report.json md:D:\gas_calibrator\_handoff\a2_15_offline_suites\smoke_a2_15_20260430_after_pressure_ping\pressure_reference_degraded\control_flow_compare_report.md
- summary_parity: 一致（期望 一致）[工件: D:\gas_calibrator\_handoff\a2_15_offline_suites\smoke_a2_15_20260430_after_pressure_ping\summary_parity]
  证据=诊断 状态=已收集 风险=低 失败类型=摘要口径一致性 失败阶段=summary_parity
  审阅工件=json:D:\gas_calibrator\_handoff\a2_15_offline_suites\smoke_a2_15_20260430_after_pressure_ping\summary_parity\summary_parity_report.json md:D:\gas_calibrator\_handoff\a2_15_offline_suites\smoke_a2_15_20260430_after_pressure_ping\summary_parity\summary_parity_report.md
