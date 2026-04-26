# A2 no-write 启动前准备单

- run_id: run_20260425_231142
- source_stage: A1 no-write dry-run closeout
- artifact_root: D:\gas_calibrator\src\gas_calibrator\v2\output\run001_a1\co2_only_skip0_no_write\run_20260425_231142
- a1_audit_report: D:\gas_calibrator\src\gas_calibrator\v2\output\run001_a1\co2_only_skip0_no_write\run_20260425_231142\a1_audit_report.md
- document_scope: A2 启动前准备单，仅用于人工授权前评审
- execution_status: not_executed
- authorization_status: explicit_human_authorization_required
- real_acceptance_claim: false
- v2_replaces_v1_claim: false

## 1. A2 目标

A2 目标为 CO2 单路线全压力点 no-write 验证。

本准备单仅记录 A2 启动前必须确认的范围、条件、停止准则、工件清单与 PASS 条件草案；不执行 A2，不替代 A2 artifact，不构成真实 acceptance 结论。

## 2. A2 边界

- no-write。
- 不写校准参数。
- 不发送 ID 写入。
- 不修改 device_id。
- 不进入 H2O。
- 不进入 full group。
- 不替代 V1。
- V1 仍为生产保底。
- 不修改 V1 生产逻辑。
- 不修改 run_app.py 默认入口。
- 不默认切换到 V2。
- A1 green 与 A1 audit PASS 只代表可以申请 A2 授权，不代表已经进入 A2。

## 3. A2 前置条件

- 必须获得人工显式授权。
- A1 audit PASS 已被人工接受。
- analyzer mapping 无 silent fallback。
- COM35/COM37/COM41/COM42 MODE2 ready。
- no-write guard active。
- 阀门初始安全状态已确认。
- PACE 初始安全状态已确认。
- pressure atmosphere vent gate active。
- chamber temperature tolerance=0.08C / window=60s。
- abort criteria 已定义并在执行前可见。
- analyzer effective list 必须保持：
  - ga01 COM35/001。
  - ga02 COM37/029。
  - ga03 COM41/003。
  - ga04 COM42/004。
- 不允许以 preflight PASS 代替 A2 execute PASS。
- 不允许以 A1 artifact 代替 A2 artifact。

## 4. A2 压力点范围建议

仅记录建议，不执行。最终压力点、顺序、保持时间、采样策略与执行窗口必须以人工显式授权为准。

建议压力点：

- 1100 hPa。
- 1000 hPa。
- 900 hPa。
- 800 hPa。
- 700 hPa。
- 600 hPa。
- 500 hPa。

## 5. A2 abort criteria

出现以下任一情况，应立即中止 A2，并保留已有 artifact 与日志：

- 任意写参尝试。
- identity write attempt。
- device_id mismatch。
- analyzer active-send 中断。
- pressure atmosphere vent 失败。
- route 未按顺序执行。
- pressure ready gate hard fail。
- wait gate timeout。
- sample_count 不足。
- artifact 生成失败。
- 阀门状态异常。
- PACE 状态异常。
- analyzer mapping 出现 silent fallback。
- COM35/COM37/COM41/COM42 任一目标 analyzer 未处于 MODE2 ready。
- no-write guard 未激活或状态不明确。

## 6. A2 必须生成的 artifact

A2 execute PASS 必须有独立于 A1 的新 artifact；不得复用 A1 artifact 作为 A2 证据。

- summary.json。
- no_write_guard.json。
- run_manifest.json。
- human_readable_report.md。
- effective_analyzer_fleet.json。
- temperature_stability_evidence.json。
- pressure_gate_evidence。
- route_trace.jsonl。
- points.csv。
- io_log.csv。
- run.log。

建议同时保留：

- route_pressure_sample_trace.json。
- samples.csv 或等效采样明细。
- temperature_stability_samples.csv。
- readiness/preflight 记录。

## 7. A2 PASS 条件草案

A2 PASS 草案必须至少满足：

- all planned pressure points completed。
- sample_count > 0。
- route / pressure / wait / sample 全完成。
- attempted_write_count=0。
- identity_write_command_sent=false。
- persistent_write_command_sent=false。
- final_decision=PASS。
- a2_final_decision=PASS 或等效 A2 decision 字段。
- artifact 完整。
- analyzer mapping 与人工授权的 effective list 一致。
- no-write guard.json 与 summary.json 结论一致。
- route_trace、points、io_log、run.log 之间无矛盾。

## 8. A2 不允许事项

- 不允许 H2O。
- 不允许 full group。
- 不允许写参。
- 不允许发送 ID 写入。
- 不允许修改 device_id。
- 不允许修改 V1。
- 不允许修改 run_app.py 默认入口。
- 不允许默认切换 V2。
- 不允许用 A1 artifact 代替 A2 artifact。
- 不允许 preflight PASS 代替 A2 execute PASS。
- 不允许用 A2 no-write 结论宣称 V2 可以替代 V1。
- 不允许 real compare / real verify / real manual operation，除非后续另有明确授权且范围重新定义。

## 9. A1 收口引用状态

以下只作为申请 A2 授权的输入条件，不是 A2 证据：

- A1 green: true。
- A1 audit conclusion: PASS。
- no-write: PASS。
- attempted_write_count=0。
- identity_write_command_sent=false。
- persistent_write_command_sent=false。
- points_completed=4。
- sample_count=160。
- route_completed=true。
- pressure_completed=true。
- wait_gate_completed=true。
- sample_completed=true。
- temperature stability tolerance=0.08C / window=60s。
- observed temperature span=0.0200C。
- pressure atmosphere vent 在 CO2 route 打开前完成。

## 10. 启动前人工确认清单

执行 A2 前，人工授权记录应至少确认：

- 授权人。
- 授权时间。
- 授权范围为 A2 CO2 单路线全压力点 no-write。
- 授权压力点列表。
- 明确不进入 H2O。
- 明确不进入 full group。
- 明确不写校准参数。
- 明确不发送 ID 写入。
- 明确不修改 device_id。
- 明确 V1 仍为生产保底。
- 明确 A2 结果不直接构成 V2 替代 V1 结论。

## 11. 当前准备单结论

A1 green 与 A1 audit PASS 支持进入“申请 A2 人工显式授权”的状态。

截至本准备单生成时：

- A2 未执行。
- 未进入 H2O。
- 未进入 full group。
- 未写校准参数。
- 未发送 ID 写入。
- 未修改 device_id。
- 未修改 V1。
- 未修改 run_app.py。
- 未默认切换 V2。
- A2 仍必须等待人工显式授权。
