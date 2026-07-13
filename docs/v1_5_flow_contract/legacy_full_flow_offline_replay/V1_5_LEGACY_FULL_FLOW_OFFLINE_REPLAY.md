# V1.5 旧算法全流程 Orchestrator 离线 Replay

- source origin/main: `2a9c8c8b85820b1cc9f3fd88ea55f73fba7dca6b`
- overall_status: `legacy_full_flow_replay_complete_production_evidence_incomplete`
- orchestrator_replay_complete: `true`
- production_flow_complete: `false`
- current_stage_id: `initialization_identity_runtime`
- evidence_source: `historical_replay`
- not_real_acceptance_evidence: `true`

本 replay 只验证一个旧算法状态机能否按 0613/0620/0621 口径解释现有证据。它不会把分段、retry、diagnostic composite 或缺 QC 的历史数据提升为连续生产 run。

## Stage Replay

| 顺序 | stage | evidence | effective | observed | blockers |
|---:|---|---|---|---|---|
| 1 | `initialization_identity_runtime` | `hold` | `hold` | overall_status=review_required; device_count=0 | active_device_count=0_outside_1_to_6; active_device_count_not_1_to_6; one_or_more_device_closeout_rows_not_ready; pressure_s9_evidence_missing; readonly_com_executor_evidence_missing; route_readiness_evidence_missing |
| 2 | `pressure_s9_readiness` | `hold` | `blocked_by_previous_stage` | overall_status=review_required; device_count=0 | active_pressure_s9_device_count=0_outside_1_to_6; one_or_more_devices_missing_no_write_fit_basis; one_or_more_devices_missing_post_write_pressure_reverify; one_or_more_devices_missing_senco9_readback; pressure_device_count_not_1_to_6 |
| 3 | `mature_route_readiness` | `hold` | `blocked_by_previous_stage` | entrypoint=pass; mature_contract=pass; root_discovery=blocked_no_complete_mature_root; complete_roots=0 | no_complete_continuous_mature_route_root |
| 4 | `legacy_co2_45` | `hold` | `blocked_by_previous_stage` | accepted_composite_members=45; co2_points=145; missing_component_qc=102 | co2_component_qc_missing=102; co2_composite_not_continuous_route_attestation; co2_historical_fit_not_allowed |
| 5 | `legacy_h2o_13` | `hold` | `blocked_by_previous_stage` | cataloged_h2o_points=36; missing_component_qc=36; complete_mature_roots=0 | h2o_component_qc_missing=36; h2o_historical_fit_not_allowed; legacy_h2o_continuous_13_point_root_missing |
| 6 | `component_qc_and_0613_fit_review` | `hold` | `blocked_by_previous_stage` | catalog_fit_allowed=False; component_qc_evaluator_available=True; strategy_matrix_available=True; evaluated_qc_rows=460; fit_ready_strategies=0 | catalog_not_fit_eligible; production_fit_input_not_eligible |
| 7 | `controlled_write_readback` | `hold` | `blocked_by_previous_stage` | unified_status=blocked_no_fit_approved_candidate; operation_plan_count=0; write=not_authorized; readback=not_authorized | post_run_write_package=not_attempted; unified_getco_readback=not_authorized; unified_operation_plan=blocked_no_fit_approved_candidate; unified_write_transaction=not_authorized |
| 8 | `post_write_short_reverify` | `hold` | `blocked_by_previous_stage` | physical_short_reverify=not_attempted; formal_gate=not_attempted | controlled_write_and_reverification=not_attempted; unified_physical_short_reverify=not_attempted |
| 9 | `archive_release_postgresql18` | `hold` | `blocked_by_previous_stage` | archive_gate=missing; db_execution_supported=False; database_import_allowed=False | formal_archive_database_release=missing; postgresql18_database_import_allowed=false; postgresql18_execution_supported=false; postgresql18_real_import_execution_allowed=false |

## Conclusion

- 程序级 replay 已遍历初始化到归档/数据库的全部 9 个阶段。
- 当前生产流在初始化批次证据处 hold；后续阶段仍被只读检查并列出自身证据缺口。
- 45 点 CO2 composite 只可诊断，不能证明一条连续 mature route。
- H2O 分段历史点不能替代一条连续 13 点 mature route，且 CO2 zero gas 与 H2O dry-gas anchor 不可互换。
- 下一冻结缺口仍是生产 component-QC evaluator 与 0613 多策略拟合矩阵；真机未连接时不做 live acceptance。
- 本包不开 COM、不控压力/气水路、不写系数、不连 PostgreSQL、不授权 release/import。
