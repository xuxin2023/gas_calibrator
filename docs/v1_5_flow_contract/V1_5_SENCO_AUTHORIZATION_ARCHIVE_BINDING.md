# V1.5 SENCO Authorization Archive Binding

This gate closes the traceability gap between reviewed final coefficient artifacts and actual controlled writes.
It is an offline archive check. It does not open COM ports, write coefficients, connect PostgreSQL, or control gas/water routes.

## Scope

The archive scans every evidence directory for these main writer families:

- `co2_senco13_pair`
- `h2o_senco24_pair`
- `co2_senco5_linear`
- `h2o_senco6_linear`

When no main SENCO controlled-write evidence exists, the gate is
`not_applicable_no_main_senco_write_evidence` and does not penalize a no-write historical archive.

When controlled-write evidence exists, every evidence set must contain:

- writer metadata;
- device-level write rows;
- successful post-write readback status for every row;
- `artifact_hash_status=pass`;
- `artifact_authorization_status=pass`;
- the same authorization ID as the reviewed authorization JSON;
- reviewer and distinct approver matching the authorization;
- writer scope and final device set matching the authorization exactly;
- an unchanged artifact manifest path and SHA-256 validated by the authorization contract.

Any missing, malformed, unauthorized, failed-readback, or mismatched evidence set makes the binding `blocked`.
The blocked state prevents formal archive release and PostgreSQL import. It does not block mature CO2/H2O sampling and does not change analyzer state.

## Outputs

Formal archive closure writes:

- `senco_authorization_write_traceability/v1_5_senco_authorization_archive_binding.json`
- `senco_authorization_write_traceability/v1_5_senco_authorization_archive_binding.csv`
- `senco_authorization_write_traceability/V1_5_SENCO_AUTHORIZATION_ARCHIVE_BINDING.md`

The final archive index and traceability summary expose
`senco_authorization_write_traceability_ready`. Formal run status consumes the same binding before release/import decisions.
