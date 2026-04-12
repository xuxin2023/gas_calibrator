# 外部比对导入器

- summary: Step 2 离线导入器: 仅支持本地文件, 不联网
- 支持格式: json, csv, markdown, artifact_sidecar
- 仅从本地 artifact / fixture / 用户文件导入
- 不连接任何真实第三方系统
- importer_id: run_20260412_185117-comparison-importer
- import_mode: local_file_only
- network_access: false
- 所有导入结果默认 evidence_source=simulated
- 缺字段/旧schema/legacy payload: 保守降级 + placeholder

## Readiness Linkage

- anchor_id: external-comparison-importer
- readiness_status: external_comparison_importer_readiness_stub
- linked_artifact_refs: --
- linked_measurement_phases: --
- linked_measurement_gaps: --
- linked_method_confirmation_items: --
- linked_uncertainty_inputs: --
- linked_traceability_nodes: --
- preseal_partial_gap: --
- gap_reason: --
- missing_evidence: importer only supports local file sources, no network access | all imported comparison data is marked simulated
- blockers: importer only supports local file sources, no network access | all imported comparison data is marked simulated
- next_required_artifacts: comparison_evidence_pack | scope_comparison_view
- reviewer_next_step: --
- boundary_digest: Step 2 reviewer readiness only | simulation / offline / headless only | file-artifact-first reviewer evidence | not real acceptance | not compliance claim | not accreditation claim | cannot replace real metrology validation
- non_claim_digest: --
