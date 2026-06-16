# V1.5 采样后系数执行器设计与输入输出合同

本文固定 V1.5 正式校准中“采样完成之后”的执行器边界。它不替代正在运行的 CO2/H2O open-flow runner，也不在运行中热插逻辑；它只在一轮 no-write 采样完整落盘后，把证据、QC、候选系数、受控写入、写后复验、数据库和证书串成可追溯闭环。

## 目标

采样后执行器回答五个问题：

1. 哪些设备具备正式校准资格？
2. 哪些点位可以进入 CO2/H2O 候选系数拟合？
3. 每台设备应写入哪些 SENCO 系数，哪些系数必须阻断或降级？
4. 写入后是否通过独立复验？
5. 最终证据能否生成每台设备的校准证书、检定/验证证书和数据库归档？

执行器必须围绕“有效测量结果”设计，而不是围绕“流程跑完”设计。

## 不做的事

- 不修改 `run_app.py`。
- 不接 V2 真实 COM。
- 不在当前 CO2/H2O 采样运行中修改阀、PACE、湿度发生器或温箱逻辑。
- 不把封路压力点、动态压力探针、VENT-hold 或 short smoke 作为 CO2/H2O 正式拟合样本。
- 不用 COM 号、GA 别名或串口顺序作为设备身份。
- 不把温度异常、压力异常、湿度异常用 CO2/H2O 主系数硬吸收。
- 不静默删除坏帧、拒绝点或异常设备。

## 执行位置

```text
LOAD_PLAN
  -> PRECHECK / GETCO epoch0 snapshot
  -> PRESSURE_QUICK_CHECK
  -> optional PRESSURE_SENCO9_NO_WRITE_ACQUISITION
  -> optional PRESSURE_SENCO9_WRITE_AND_REVERIFY
  -> TEMPERATURE_CHANNEL_REVIEW
  -> CO2_OPEN_FLOW_NO_WRITE
  -> H2O_OPEN_FLOW_NO_WRITE
  -> POST_RUN_COEFFICIENT_EXECUTOR
       evidence discovery
       device eligibility
       point QC and fit input review
       candidate coefficient generation
       candidate write review
       controlled SENCO writes
       post-write reverification
       archive / database / reports / certificates
```

当前正在运行的 CO2/H2O 链路应先完整结束。执行器从已完成 run directory 读取证据，不抢占或改变正在运行的物理流程。

## 设备身份合同

设备身份以分析仪自身 MODE2/设备 ID 为主键：

```text
identity_key = analyzer_device_id
transport_key = serial_port
```

执行器输入必须包含运行开始时冻结的：

- `runtime_identity_bound_config.json`
- `old_component_coefficients_snapshot.json`
- `getco_component_snapshot_identity.csv`
- 设备 ID 到串口的映射证据

如果用户更换了设备或工控机串口号整体漂移，执行器不得沿用旧串口身份。必须重新生成设备 ID 绑定和 GETCO 快照后，才允许后续写入。

## 压力通道决策

压力是分析仪内部 CO2/H2O 计算模型的输入量，不能用 CO2/H2O 误差反推压力。

### 压力快检输出分类

压力快检后，每台设备进入以下状态之一：

| 状态 | 含义 | 后续动作 |
|---|---|---|
| `pressure_pass` | 分析仪内部 P 与 COM22/PACE 一致 | 允许进入 CO2/H2O 候选写入门禁 |
| `pressure_warn` | 偏差接近阈值或短时不稳 | 可继续采样，但写入前必须复核不确定度 |
| `pressure_calibration_required` | 内部 P 有响应但超差 | 跑多压力点 no-write，计算并写入 SENCO9 后复验 |
| `pressure_nonresponsive` | 例如固定 200 kPa、固定异常、无随压力变化响应 | 提示更换、维修或剔除；不能用 SENCO9 拟合硬修 |
| `pressure_excluded_by_operator` | 用户确认剔除 | 保留证据，报告中列为未校准或诊断设备 |

类似 `090` 这种内部压力明显异常的设备，执行器应给出“更换/剔除/仅诊断”的明确提示；不能因为一台异常阻断其它设备，也不能让它污染其它设备的拟合。

### 多压力点与 SENCO9

当设备属于 `pressure_calibration_required` 时，执行器应使用独立压力链路：

```text
1100, 1000, 900, 800, 700, 600, 500 hPa
```

多压力点只用于压力通道。合格后：

1. 生成 SENCO9 候选。
2. 写入 SENCO9。
3. 读回 GETCO9。
4. 复验压力点。
5. 生成压力通道证据与数据库事件。

若 500 hPa 受微漏气影响而无法稳定，应显式标记该点为 `pressure_point_leak_suspect`，由评审决定是否剔除；不得静默丢点。

## 温度通道评审

温度通道评审当前默认是“异常识别与风险提示”，不是默认完整温度校准。它应判断：

- 腔体温度是否明显异常。
- 壳体温度是否明显异常。
- 数字测温仪与分析仪温度是否存在系统偏差。
- 是否存在类似温度系数过大导致温度输出异常的设备。
- 是否需要进入 SENCO7/SENCO8 专项温度校准。

没有足够温度校准模型证据时，执行器只能输出：

```text
temperature_pass
temperature_warn
temperature_calibration_required
temperature_nonresponsive
```

不得把温度异常用 CO2/H2O 的 S1/3 或 S2/4 主系数吸收。

## CO2/H2O 拟合物理合同

### CO2

CO2 主拟合使用工厂模式底层信号和 traceable CO2 标准气证据：

```text
CO2 reference
CO2 ratio / raw ratio
CO2 signal / ref signal
T
P, already pressure-verified
dewpoint / H2O state evidence
```

当前大气压开放流通路线下：

- SENCO1/SENCO3 是主 ratio/温度响应链路。
- 压力项冻结，不进入 CO2 主拟合。
- SENCO5 是最终显示层线性修正，必须在主拟合之后单独评审。
- CO2 zero-gas anchor 是 CO2 低端锚点；不能把 H2O dry anchor 当作同一个概念。

### H2O

H2O 主拟合使用露点/湿度参考和 H2O ratio 证据：

```text
dewpoint reference
H2O mmol/mol or equivalent reference
H2O ratio / raw ratio
H2O signal / ref signal
T
P, already pressure-verified
```

- SENCO2/SENCO4 是主 ratio/温度响应链路。
- SENCO6 是最终显示层线性修正，必须在主拟合之后单独评审。
- H2O dry-gas low-water anchor 只有在露点/参考证据证明低水且稳定时才能进入 H2O 拟合。
- 不能只因为 CO2 是零气，就默认该点也是 H2O 零点。

## 输入合同

执行器输入是一个只读 manifest：

```json
{
  "schema_version": "v1_5_post_run_executor_input_v1",
  "run_id": "v15_6ch_co2h2o_r4",
  "run_dir": "<completed_run_dir>",
  "runtime_config": "<runtime_identity_bound_config.json>",
  "formal_plan_snapshot": "<formal_plan_snapshot.json>",
  "standard_gas_snapshot": "<standard_gas_snapshot.json>",
  "pressure_reference_snapshot": "<com22_pressure_reference.json>",
  "old_getco_snapshot": "<old_component_coefficients_snapshot.json>",
  "pressure_quick_check": "<pressure_quick_check_dir_or_csv>",
  "pressure_senco9_no_write": "<optional_pressure_senco9_no_write_dir>",
  "temperature_review_inputs": "<digital_thermometer_and_analyzer_temperature_evidence>",
  "co2_open_flow_dir": "<co2_open_flow>",
  "h2o_open_flow_dir": "<h2o_open_flow>",
  "database_dsn_name": "V1_5_EVIDENCE_DSN",
  "write_policy": {
    "enabled": true,
    "scope": "formal_calibration_run",
    "actor": "<operator>",
    "reviewer": "<reviewer>",
    "approver": "<approver>",
    "allow_senco_groups": [1, 2, 3, 4, 5, 6, 9],
    "forbid_device_id_write": true,
    "forbid_clear_senco_by_default": true
  }
}
```

正式自动校准可以在运行计划中一次性冻结写入授权，不需要每台设备弹窗。但执行器仍必须记录 actor、reviewer、approver、写入范围、写入前旧系数、写入后读回和失败回滚证据。

## 输出合同

执行器输出目录建议：

```text
post_run_executor/
  executor_manifest.json
  device_eligibility.csv
  pressure_decisions.csv
  temperature_decisions.csv
  fit_input_quality/
  candidate_coefficients/
  write_packages/
  write_events/
  post_write_reverification/
  certificates/
  formal_reports/
  database_import/
  executor_summary.json
  executor_summary.md
```

### `device_eligibility.csv`

每台设备一行：

```text
analyzer_device_id
serial_port_at_epoch0
identity_status
pressure_status
temperature_status
co2_sample_status
h2o_sample_status
write_eligibility
blocked_reasons
recommended_action
```

### `candidate_coefficients/`

每台设备、每个组分独立输出：

```text
analyzer_device_id
component
primary_senco
secondary_senco
optional_linear_senco
fit_points_used
fit_points_rejected
rejected_reasons
old_coefficients
candidate_coefficients
replay_predicted_error_table
fit_residual_summary
pressure_terms_frozen
temperature_terms_policy
zero_or_dry_anchor_policy
candidate_status
```

### `write_events/`

每个写入事件必须记录：

```text
event_id
analyzer_device_id
component
senco_group
old_values
target_values
write_command
readback_command
readback_values
verify_status
rollback_attempted
rollback_status
actor
reviewer
approver
timestamp
source_candidate_id
coefficient_epoch_before
coefficient_epoch_after
```

### `post_write_reverification/`

写后复验应输出：

```text
verification_point_id
component
analyzer_device_id
reference_value
measured_value
error_abs
error_relative_pct
expanded_uncertainty
qc_grade
pass_fail
evidence_path
```

短复验可以用于工程判断，但正式证书必须标明复验点、吹扫条件、稳定门禁和限制范围。

### `certificates/`

每台设备至少生成：

```text
<device_id>_calibration_certificate.md
<device_id>_verification_certificate.md
<device_id>_coefficient_table.csv
<device_id>_point_error_table.csv
<device_id>_traceability_index.json
```

校准证书说明“如何校准、写入了哪些系数、写后复验是否通过”。检定/验证证书说明“当前设备在指定范围内的误差、不确定度、限制条件和是否合格”。

## 状态机

```text
WAIT_FOR_ACQUISITION_COMPLETE
  -> DISCOVER_EVIDENCE
  -> CLASSIFY_DEVICE_ELIGIBILITY
  -> PRESSURE_DECISION
  -> OPTIONAL_PRESSURE_SENCO9_WRITE_AND_REVERIFY
  -> TEMPERATURE_REVIEW
  -> COMPONENT_QC_AND_FIT_INPUT_REVIEW
  -> CANDIDATE_COEFFICIENT_GENERATION
  -> CANDIDATE_WRITE_REVIEW
  -> CONTROLLED_WRITE
  -> POST_WRITE_REVERIFY
  -> ARCHIVE_DATABASE_REPORT_CERTIFICATE
  -> COMPLETE
```

任何阶段失败都不得丢失已产生证据。失败输出必须包含：

```text
failed_stage
failed_device_id
failure_reason
safe_next_action
artifacts_written
whether_any_senco_was_written
rollback_status_if_applicable
```

## 写入顺序

推荐正式顺序：

1. 压力异常设备先处理 SENCO9，并复验。
2. 温度异常设备先降级或进入 SENCO7/8 专项，不直接写 CO2/H2O。
3. CO2 写 SENCO1/SENCO3。
4. H2O 写 SENCO2/SENCO4。
5. 需要时再写 SENCO5/SENCO6 显示层线性修正。
6. 统一跑写后复验。

SENCO5/SENCO6 不能替代主拟合，也不能混进 S1/3、S2/4 主系数计算；它们只修正最终显示层的残余线性偏差。

## 数据库归档合同

数据库保存索引、状态、审核和溯源关系；原始帧、CSV、JSON、报告和图表仍保存在证据包文件系统中，并以 sha256 绑定。

至少归档：

- run
- devices
- standard_gases
- reference_devices
- calibration_points
- sample_files
- qc_results
- coefficient_snapshots
- coefficient_candidates
- coefficient_write_events
- post_write_verification_results
- reports
- certificates
- audit_events

## 完成标准

执行器完成后应能做到：

- 从一个完成的 no-write run directory 自动发现 CO2/H2O/压力/温度证据。
- 按设备 ID 判断设备是否可校准、应剔除、应维修或应进入专项校准。
- 对压力异常设备给出 SENCO9 决策，并在写入后复验。
- 对温度异常设备给出风险结论，不把温度异常吸收到 CO2/H2O 主系数。
- 对每台设备分别生成 CO2/H2O 候选系数和误差重放。
- 按设备 ID 写入，不按 COM 或 GA 别名写入。
- 写入后读回，并生成 coefficient epoch。
- 复验后生成误差表、中文报告、每台设备校准证书和检定/验证证书。
- 数据库中可追溯到证书、原始帧、QC、候选系数、写入事件、读回值和报告 hash。

## 当前接入建议

本轮 `v15_6ch_co2h2o_r4` 正在运行时，不接入执行器。等 CO2/H2O 采样全部完成后：

1. 先以 no-write/offline 模式运行执行器到 `CANDIDATE_WRITE_REVIEW`。
2. 检查 `device_eligibility.csv`、候选系数、预测误差和拒绝原因。
3. 再进入受控写入与复验。
4. 最后生成证据包、数据库归档、总报告和每台设备证书。
