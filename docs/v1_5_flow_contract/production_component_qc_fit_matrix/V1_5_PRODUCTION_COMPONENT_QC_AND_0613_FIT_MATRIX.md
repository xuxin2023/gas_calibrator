# V1.5 生产 Component-QC 与 0613 拟合策略矩阵

- overall_status: `production_component_qc_evaluated_fit_matrix_blocked_by_continuity`
- point_count: `125`
- analyzer_qc_row_count: `460`
- grade_counts: `{"A_calibration_eligible": 312, "B_diagnostic_model_only": 2, "C_reject": 146}`
- fit_strategy_row_count: `420`
- fit_ready_strategy_count: `0`
- continuous_mature_route_attestation_ready: `false`
- production_fit_allowed: `false`
- evidence_source: `historical_replay`
- not_real_acceptance_evidence: `true`

## 物理口径

- 逐分析仪独立分级；单台不稳不取消其它已合格分析仪的采样资格。
- 旧算法主输入是滤波后比值 R，温度使用每台分析仪自己的腔体温度 T1。
- CO2 零气只承担 CO2 低端锚点角色，不替代 H2O 干气锚点。
- H2O 干气锚点必须单独绑定露点与压力证据。
- S5/S6 是主链写入并独立复验后的最终输出层，不提前吸收主模型问题。

## 当前结论

- Bind one continuous 45/13 mature root and a separate traceable H2O dry-gas anchor, then rerun the no-write matrix.
- 本包只在中央 review 目录生成证据，不回写历史点目录，不计算可写系数。
