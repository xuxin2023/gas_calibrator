# V1.5 Route Physical Recovery Evidence Packet Template

- schema: `v1_5_route_physical_recovery_evidence_packet_template_v1`
- status: `template_ready`
- packet_schema: `v1_5_route_physical_recovery_evidence_packet_v1`
- boundary: offline template only; no COM, no pressure/route control, no writes, no PostgreSQL.

## Collection Steps

| step | packet field | pass condition | hardware |
|---|---|---|---|
| `dry_gas_dewpoint_recovery` | `dry_gas_dewpoint_recovery` | status=pass; dewpoint_c <= -28 C; tail_span_c <= 0.5 C; tail_slope_abs_c_per_s <= 0.01; route_or_dryer_checked=true. | yes |
| `pace_vent_recovery` | `pace_vent_recovery` | status=pass; vent_on_off_roundtrip_pass=true; no_response_absent=true. | yes |
| `pressure_gauge_recovery` | `pressure_gauge_recovery` | status=pass; readback_status=pass; absolute_pressure_source=inl; no_response_absent=true. | yes |
| `fresh_canonical_queue_policy` | `next_run_policy` | fresh_canonical_queue=true; baseline includes 0613/0620/0621; forbidden_surfaces_absent=true; canonical queue entrypoints named. | no |
| `accepted_manifest_review` | `accepted_manifest_review` | status=pass; accepted_manifest_path is non-empty; supersedence_review_id is non-empty. | no |

## Usage

1. Collect or review the physical evidence listed above.
2. Replace the pending fields in `v1_5_route_physical_recovery_evidence_packet_template.json`.
3. Run `export_v1_5_route_physical_recovery_evidence_packet.py` on the reviewed packet.
4. Feed `v1_5_validated_route_physical_recovery_evidence.json` into route physical recovery readiness.

This template is not real acceptance evidence and does not unlock a continuous run by itself.
