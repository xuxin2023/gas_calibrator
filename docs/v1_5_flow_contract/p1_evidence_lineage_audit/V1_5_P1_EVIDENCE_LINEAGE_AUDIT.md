# V1.5 P1 Evidence Lineage Audit

- overall_status: `review_required_p1_lineage_audit_complete`
- point_count: `2`
- recoverable_reference_count: `1`
- unrecoverable_count: `1`
- same_lineage_only: `true`
- cross_run_direct_bind_allowed: `false`
- automatic_file_copy_allowed: `false`
- automatic_qc_derivation_allowed: `false`
- historical_fit_allowed: `false`
- offline_only: `true`

| Point | Conclusion | Next action |
| --- | --- | --- |
| `p017_T20_200ppm_fit` | `unrecoverable_from_reviewed_lineage` | `retain_failed_attempt_as_raw_diagnostic_only_do_not_borrow_cross_run_samples` |
| `p006_Tm20_1000ppm_fit` | `core_gap_resolved_by_same_lineage_retry_reference` | `use_retry_as_explicit_diagnostic_candidate_then_run_separate_component_qc_review` |
