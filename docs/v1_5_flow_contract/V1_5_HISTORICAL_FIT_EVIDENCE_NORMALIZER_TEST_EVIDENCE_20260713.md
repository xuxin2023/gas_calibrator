# V1.5 Historical Fit Evidence Normalizer Test Evidence

## Focused

```text
11 passed in 10.31s
```

Coverage includes legacy R, absorption A/R0(T1), complete 45/13 and 47/14
normalization, profile-parity consumption, separate CO2/H2O roots, reviewed
route attestation, missing component QC, stale/missing R0 files, source hashes,
sample-count mismatches, duplicate QC rows, CLI fail-closed behavior, and
offline entrypoint classification.

## Compatibility

```text
104 passed, 1 warning in 25.11s
```

Coverage includes historical replay contract/evidence/gap audits, historical
profile parity, algorithm profile lineage, route profiles, mature route
contract, fit-input quality, and entrypoint inventory. The warning is the
existing unregistered `v1_5_formal_gate` pytest marker.

Ruff and Python compilation passed. No hardware, COM port, pressure/gas/water
route, coefficient writer, PostgreSQL importer, or release path was used.
