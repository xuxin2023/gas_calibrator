# V1.5 Historical Fit Profile Parity

## Purpose

This offline gate validates normalized historical fit-point evidence before it
is reused by a V1.5 coefficient review. It preserves the following production
contracts:

- fitting-method baseline: `0613`
- physical route baselines: `0620` or `0621`
- legacy profile: fit variable is ratio `R`
- absorption profile: fit variable is
  `A=-ln(R/R0(T))/(P_kPa/100)`
- temperature source: each analyzer's chamber `T1`
- legacy formal coverage: CO2 `45`, H2O wet `13`
- absorption candidate coverage: CO2 `47`, H2O wet `14`

The gate rejects `0624`, root migration, V1/V2 reinterpretation, profile/hash
substitution, missing formal points, duplicate point rows, non-A quality rows,
and insufficient H2O dry-gas anchor temperature coverage.

## R0(T) Binding

The absorption profile requires both reviewed R0 model files. Every fit row
must bind to the current model file SHA-256 and to an explicit reviewed
`temperature_c -> r0_value` evaluation. The gate then recomputes A and compares
it with the supplied fit-input value. A model name or status alone is not
sufficient.

The legacy profile rejects any R0 field or absorption fit variable. It must use
the measured ratio R directly.

## Anchor Separation

`CO2 zero gas` and `H2O dry-gas anchor` are separate evidence roles. An H2O
dry-gas anchor may originate from a CO2 zero-gas physical point only when it
also carries water-ratio, dewpoint, pressure, and chamber-T1 evidence. It is not
treated as zero water and is not counted as one of the 13/14 H2O wet points.

## Safety Boundary

The exporter is offline and no-write. It does not open COM ports, control
pressure or routes, write identity or SENCO coefficients, connect PostgreSQL,
authorize release, or authorize database import. A passing replay proves only
that a normalized historical fit package follows the selected profile's
mathematical and physical-input contract.
