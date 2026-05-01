# External Comparison Importer

- summary: offline-only importer for local JSON/CSV external comparison files
- supported formats: json, csv
- schema normalize: enabled
- trace fields: source_file / import_mode / evidence_source
- network access: disabled
- importer_id: run_20260430_154401-comparison-importer
- local-file-only: true
- conservative defaults: reviewer-only / readiness-mapping-only / simulated
- no connection to third-party PT/ILC systems is allowed in Step 2

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
