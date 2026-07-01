# V1.5 文件收敛报告

本报告由 `python -m gas_calibrator.tools.export_v1_5_entrypoint_inventory` 自动生成。
它不是第二套入口清单，而是从同一份 inventory 数据压缩出来的中文导航页。

## 一句话结论

- 当前识别到 V1.5 相关入口/模块/测试共 `334` 个。
- 正式主路以 `12` 个 canonical 阶段为准。
- 真实路线 runner 只有 `2` 个分类入口，其他多数文件是评审、证据、测试或诊断支撑。
- 需要防误用收纳的入口共 `33` 个。
- 本次导出不打开 COM、不控制气路/水路/压力、不写 SENCO。

## 先从这里开始

| 顺序 | 阶段 | 入口 | 用途 |
|---:|---|---|---|
| 1 | `00_full_flow_guard` | `src/gas_calibrator/tools/run_v1_5_full_calibration_chain.py` | Orders pressure, temperature, open-flow CO2, open-flow H2O, QC, review, write gate, reverify, and archive without opening COM by default. |
| 2 | `01_formal_initialization` | `src/gas_calibrator/tools/run_v1_5_formal_initialization_runner.py` | Owns the formal initialization contract: device-ID binding, GETCO1-9 epoch-0 snapshot, S5/S6/S7/S8/S9 gates, startup acquisition settings, readiness, pre-gas gap list, and database evidence indexing. |
| 3 | `02_pressure_channel` | `src/gas_calibrator/tools/export_v1_5_pressure_channel_validation.py` | Verifies analyzer pressure P against COM22/PACE before CO2/H2O fitting so pressure error is not absorbed into gas coefficients. |
| 4 | `03_temperature_channel` | `src/gas_calibrator/tools/export_v1_5_temperature_channel_review.py` | Reviews chamber/case temperature behavior against temperature evidence before interpreting multi-temperature gas response. |
| 5 | `04_co2_open_flow_sampling` | `src/gas_calibrator/tools/run_v1_5_formal_co2_open_flow_queue.py` | Collects clean open-flow CO2 data while standard gas continuously refreshes the analyzer cavity and downstream line. |
| 6 | `05_h2o_open_flow_sampling` | `src/gas_calibrator/tools/run_v1_5_formal_h2o_open_flow_queue.py` | Collects open-flow H2O data with dewpoint/reference evidence so dry-gas and wet-gas anchors remain physically interpretable. |
| 7 | `06_candidate_coefficients` | `src/gas_calibrator/tools/export_v1_5_candidate_coefficients.py` | Builds candidate coefficients from eligible A-grade open-flow samples and preserves rejected points with reasons. |
| 8 | `07_write_review_gate` | `src/gas_calibrator/tools/export_v1_5_candidate_write_review.py` | Checks old coefficients, candidate coefficients, residuals, blockers, and reviewer evidence before any SENCO write. |
| 9 | `08_controlled_write` | `src/gas_calibrator/tools/run_v1_5_co2_senco13_controlled_write.py` | Writes CO2 main-chain coefficients only after identity, old-coefficient snapshot, candidate review, and readback plan are available. |
| 10 | `09_post_write_reverify` | `src/gas_calibrator/tools/export_v1_5_post_write_reverification.py` | Confirms written coefficients against independent reverify samples before release. |
| 11 | `10_archive_report_database` | `src/gas_calibrator/tools/run_v1_5_formal_archive_closure.py` | Closes the run evidence chain with artifact hashes, reports, database index inputs, and traceability status. |
| 12 | `11_formal_run_status_dashboard` | `src/gas_calibrator/tools/export_v1_5_formal_run_status.py` | Summarizes current stage, next action, physical-flow continuation, and formal archive/database release readiness from existing sidecars. |

## 文件分组

| 分组 | 数量 | 使用原则 |
|---|---:|---|
| 正式路线 runner (`formal_runner`) | 2 | 只用于开放流通 CO2/H2O 等真实物理采样入口；运行前必须确认设备、气路、水路和授权。 |
| 正式流程编排 (`full_flow_orchestration`) | 5 | 用于确认 V1.5 顺序和证据合同；默认不打开 COM，不直接控制设备。 |
| 压力 no-write 跑器 (`formal_pressure_no_write_runner`) | 1 | 只用于压力通道验证/校准证据；压力 P 独立处理，不能混入 CO2/H2O 主拟合。 |
| 离线证据和评审 (`formal_review_evidence`) | 109 | 用于候选系数、报告、归档、数据库和复核；不能替代真实采样。 |
| 受控写入 (`controlled_write`) | 14 | 只在评审通过后手动授权执行，必须有旧值、候选值、读回和复验。 |
| 工程诊断 (`diagnostic_only`) | 11 | 可以保留证据，但默认不进入 formal acceptance，也不进入 CO2/H2O 正式拟合。 |
| 旧 V1 参考入口 (`legacy_v1_reference`) | 4 | 只保留历史算法和审计参考；不要用于启动 V1.5 正式校准。 |
| 证据数据库 (`evidence_database`) | 5 | 保存索引、hash、追溯关系和查询能力；原始文件仍是证据包的一部分。 |
| 高级质控 (`advanced_qc`) | 10 | 解释稳定性、湿度、压力、工厂信号和根因；阈值调整必须留痕。 |
| 只读/评审界面 (`ui_review`) | 3 | 显示流程状态和证据，不应暗示可直接真实控制设备。 |
| 测试门禁 (`test_gate`) | 156 | 保护 V1.5 合同、证据口径和防误用规则。 |

## 不要从这里启动正式流程

| 防误用规则 | 数量 | 含义 |
|---|---:|---|
| `authorized_write_only` | 14 | 只能在候选系数评审和授权后写入，不能被报告或归档自动调用。 |
| `diagnostic_not_acceptance` | 11 | 只能用于工程诊断，默认不能作为正式验收或正式拟合数据。 |
| `review_before_formal_use` | 0 | 有真实路线能力但不在 canonical 主路，使用前必须人工复核。 |
| `pressure_no_write_only` | 1 | 只用于压力通道 no-write 验证或校准证据，不能作为 CO2/H2O 拟合入口。 |
| `legacy_v1_reference_only` | 4 | 旧 V1 入口只作为历史参考，不用于 V1.5 正式流程。 |
| `archive_housekeeping_only` | 1 | 只做整理或归档辅助，不是校准数据生成入口。 |
| `classification_required` | 0 | 若出现，说明新增文件还没有明确身份，不能投入使用。 |

## 校准物理边界

- CO2 主拟合和 H2O 主拟合都必须使用开放流通的干净、稳定、可追溯数据。
- 压力 P 是分析仪内部补偿输入，应独立验证和处理，不能让 CO2/H2O 系数吸收压力错误。
- CO2 零气锚点与 H2O 干气低水锚点不是同一个物理概念，不能因为都在低端就混用。
- 诊断压力点、封路压力点、VENT-hold、dynamic pressure 只能作为工程诊断，默认不能进入正式 CO2/H2O 拟合。
- 写入 SENCO 前必须有旧系数快照、候选系数、写入命令、读回值、写后复验和报告证据。

## 后续修改规则

1. 新增 V1.5 文件时，先判断它属于 canonical 主路、离线证据、受控写入、诊断、UI、数据库还是测试。
2. 如果它能打开 COM、控制阀、控制 PACE、或写 SENCO，必须在 inventory 中显式体现风险。
3. 如果它只是诊断或历史脚本，保留证据但不要接入正式主流程。
4. 如果它会影响 CO2/H2O 拟合，必须说明所用数据的物理意义、锚点来源和 QC 资格。
5. 更新入口分类后，重新导出本报告并运行 V1.5 离线测试。
