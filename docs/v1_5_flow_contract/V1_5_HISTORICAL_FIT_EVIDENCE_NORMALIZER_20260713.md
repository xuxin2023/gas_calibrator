# V1.5 Historical Fit Evidence Normalizer

## Purpose

This offline tool converts point-level historical V1.5 artifacts into the
normalized `fit_points.csv` contract consumed by the historical profile parity
gate.

It reads only:

- `samples_machine_readable.csv`
- `formal_open_flow_data_quality_by_analyzer.csv`
- historical replay point/root metadata
- the selected algorithm-profile lineage gate
- reviewed R0(T1) model files for the absorption profile

Displayed CO2/H2O concentration is never used as a fitting variable. The
normalizer uses each analyzer's filtered ratio, chamber T1, and analyzer
pressure. The absorption profile additionally requires a current reviewed
R0(T1) evaluation and computes `A=-ln(R/R0(T))/(P_kPa/100)`.

## Quality Contract

A row is fit-eligible only when component-matched formal quality evidence is A
grade and explicitly allows calibration fitting. Missing H2O quality cannot be
borrowed from a CO2 quality row. Duplicate component-quality rows or a mismatch
between quality sample counts and machine-readable samples fail closed. Frame
and usable-ratio counts are required on every accepted formal quality row.

## Anchor Contract

CO2 zero gas and H2O dry-gas anchors remain separate roles. A dry-gas anchor is
emitted from a CO2 0ppm point only when the H2O ratio, dewpoint, analyzer
pressure, chamber T1, and H2O-specific formal quality are present. Residual
water is not forced to zero.

## Mature Baseline Attestation

Directory names and family labels are not sufficient to claim a mature route.
Every evidence root requires a separate reviewed attestation with this shape:

```json
{
  "schema": "v1_5_historical_route_baseline_attestation_v1",
  "families": [
    {
      "family_id": "example_family",
      "route_kind": "co2",
      "root_path": "D:/evidence/example_family/co2",
      "route_baseline": "0620",
      "fitting_baseline": "0613",
      "status": "reviewed",
      "reviewer": "reviewer-id",
      "reviewed_at": "2026-07-13T00:00:00+08:00",
      "not_0624_or_migration_source": true,
      "mature_contract": "0613_fit_0620_0621_route"
    }
  ]
}
```

The root path is matched exactly, and CO2/H2O roots sharing one family remain
separately bound by `family_id + route_kind`. Duplicate root keys are rejected.
The output records SHA-256 for the lineage, replay evidence, attestation, sample
files, and formal quality files.

## Safety Boundary

The normalizer does not open COM ports, control pressure or routes, write SN or
SENCO coefficients, connect PostgreSQL, fit coefficients, authorize release,
or authorize database import. Historical replay remains non-acceptance
evidence.
