# V1.5 Legacy Evidence Gap Task Plan

- overall_status: `review_required_manual_offline_evidence_tasks`
- task_count: `181`
- artifact_integrity_mismatch_count: `0`
- recoverable_manual_review_task_count: `127`
- forbidden_reference_task_count: `54`
- priority_counts: `{"P1_core_evidence": 2, "P2_quality_traceability": 125, "P3_forbidden_reference": 47, "P3_superseded_reference": 7}`
- automatic_repair_allowed: `false`
- historical_fit_allowed: `false`
- formal_release_allowed: `false`
- database_import_allowed: `false`
- offline_only: `true`

Tasks describe manual offline evidence work. They do not modify source files, derive QC, or authorize fitting.
Cross-run quality remains reference-only, and CO2 zero gas remains distinct from an H2O dry-gas anchor.

## Highest-Priority Tasks

| Priority | Route | Point | Gaps |
| --- | --- | --- | --- |
| `P1_core_evidence` | `co2` | `p017_T20_200ppm_fit` | `component_qc_missing,frame_qc_missing,point_samples_missing,segmented_lineage_not_continuous` |
| `P1_core_evidence` | `co2` | `p006_Tm20_1000ppm_fit` | `component_qc_missing,frame_qc_missing,point_samples_missing,segmented_lineage_not_continuous` |
| `P2_quality_traceability` | `co2` | `p019_Tm20_400ppm_fit_retry1` | `component_qc_missing,retry_or_recovery_lineage_not_continuous` |
| `P2_quality_traceability` | `co2` | `p020_Tm20_1000ppm_fit` | `component_qc_missing,retry_or_recovery_lineage_not_continuous` |
| `P2_quality_traceability` | `co2` | `p001_T40_0ppm_fit` | `accepted_composite_lineage_not_continuous,component_qc_missing` |
| `P2_quality_traceability` | `co2` | `p002_T40_400ppm_fit_retry1` | `component_qc_missing,retry_or_recovery_lineage_not_continuous` |
| `P2_quality_traceability` | `co2` | `p003_T40_1000ppm_fit` | `accepted_composite_lineage_not_continuous,accepted_manifest_warning_requires_review,component_qc_missing` |
| `P2_quality_traceability` | `co2` | `p001_T30_0ppm_fit` | `accepted_composite_lineage_not_continuous,accepted_manifest_warning_requires_review,component_qc_missing` |
| `P2_quality_traceability` | `co2` | `p002_T30_100ppm_fit` | `accepted_composite_lineage_not_continuous,component_qc_missing` |
| `P2_quality_traceability` | `co2` | `p003_T30_200ppm_fit` | `accepted_composite_lineage_not_continuous,component_qc_missing` |
| `P2_quality_traceability` | `co2` | `p004_T30_300ppm_fit` | `accepted_composite_lineage_not_continuous,component_qc_missing` |
| `P2_quality_traceability` | `co2` | `p005_T30_400ppm_fit` | `accepted_composite_lineage_not_continuous,component_qc_missing` |
| `P2_quality_traceability` | `co2` | `p006_T30_500ppm_fit` | `accepted_composite_lineage_not_continuous,component_qc_missing` |
| `P2_quality_traceability` | `co2` | `p007_T30_600ppm_fit` | `accepted_composite_lineage_not_continuous,component_qc_missing` |
| `P2_quality_traceability` | `co2` | `p008_T30_700ppm_fit` | `accepted_composite_lineage_not_continuous,component_qc_missing` |
| `P2_quality_traceability` | `co2` | `p009_T30_800ppm_fit` | `accepted_composite_lineage_not_continuous,component_qc_missing` |
| `P2_quality_traceability` | `co2` | `p010_T30_900ppm_fit` | `accepted_composite_lineage_not_continuous,component_qc_missing` |
| `P2_quality_traceability` | `co2` | `p011_T30_1000ppm_fit` | `accepted_composite_lineage_not_continuous,component_qc_missing` |
| `P2_quality_traceability` | `co2` | `p012_T20_0ppm_fit` | `accepted_composite_lineage_not_continuous,component_qc_missing` |
| `P2_quality_traceability` | `co2` | `p013_T20_100ppm_fit` | `accepted_composite_lineage_not_continuous,component_qc_missing` |
| `P2_quality_traceability` | `co2` | `p014_T20_200ppm_fit` | `accepted_composite_lineage_not_continuous,component_qc_missing` |
| `P2_quality_traceability` | `co2` | `p015_T20_300ppm_fit_retry1` | `component_qc_missing,retry_or_recovery_lineage_not_continuous` |
| `P2_quality_traceability` | `co2` | `p016_T20_400ppm_fit_retry1` | `component_qc_missing,retry_or_recovery_lineage_not_continuous` |
| `P2_quality_traceability` | `co2` | `p017_T20_500ppm_fit_retry1` | `component_qc_missing,retry_or_recovery_lineage_not_continuous` |
| `P2_quality_traceability` | `co2` | `p018_T20_600ppm_fit_retry1` | `component_qc_missing,retry_or_recovery_lineage_not_continuous` |
| `P2_quality_traceability` | `co2` | `p019_T20_700ppm_fit_retry1` | `component_qc_missing,retry_or_recovery_lineage_not_continuous` |
| `P2_quality_traceability` | `co2` | `p020_T20_800ppm_fit_retry1` | `component_qc_missing,retry_or_recovery_lineage_not_continuous` |
| `P2_quality_traceability` | `co2` | `p021_T20_900ppm_fit_retry1` | `component_qc_missing,retry_or_recovery_lineage_not_continuous` |
| `P2_quality_traceability` | `co2` | `p022_T20_1000ppm_fit_retry1` | `component_qc_missing,retry_or_recovery_lineage_not_continuous` |
| `P2_quality_traceability` | `co2` | `p001_T10_0ppm_fit` | `accepted_composite_lineage_not_continuous,component_qc_missing` |
| `P2_quality_traceability` | `co2` | `p002_T10_100ppm_fit` | `accepted_composite_lineage_not_continuous,component_qc_missing` |
| `P2_quality_traceability` | `co2` | `p003_T10_200ppm_fit` | `accepted_composite_lineage_not_continuous,component_qc_missing` |
| `P2_quality_traceability` | `co2` | `p004_T10_300ppm_fit` | `accepted_composite_lineage_not_continuous,component_qc_missing` |
| `P2_quality_traceability` | `co2` | `p005_T10_400ppm_fit` | `accepted_composite_lineage_not_continuous,component_qc_missing` |
| `P2_quality_traceability` | `co2` | `p006_T10_500ppm_fit` | `accepted_composite_lineage_not_continuous,component_qc_missing` |
| `P2_quality_traceability` | `co2` | `p007_T10_600ppm_fit` | `accepted_composite_lineage_not_continuous,component_qc_missing` |
| `P2_quality_traceability` | `co2` | `p008_T10_700ppm_fit` | `accepted_composite_lineage_not_continuous,component_qc_missing` |
| `P2_quality_traceability` | `co2` | `p009_T10_800ppm_fit` | `accepted_composite_lineage_not_continuous,component_qc_missing` |
| `P2_quality_traceability` | `co2` | `p010_T10_900ppm_fit` | `accepted_composite_lineage_not_continuous,component_qc_missing` |
| `P2_quality_traceability` | `co2` | `p011_T10_1000ppm_fit` | `accepted_composite_lineage_not_continuous,component_qc_missing` |
