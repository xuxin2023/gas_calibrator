# V1.5 Historical Fit Evidence Current Audit

This is a read-only audit of the historical roots currently referenced by
`historical_replay_evidence`. It is not real acceptance evidence.

## Legacy Historical Root

- historical point directories: `45`
- normalized analyzer/anchor rows: `294`
- formal A-grade fit-eligible rows: `94`
- blocked fit rows: `200`
- structural blockers: `3`
- fit-review gaps: `200`

Observed blockers/gaps:

- the root has no reviewed 0613/0620/0621 baseline attestation;
- two point directories have no usable `samples_machine_readable.csv`;
- many analyzer rows are formally non-A for CO2;
- dry-gas water anchors lack component-matched formal H2O quality.

The historical family label says `mature_0620`, but the evidence path is dated
`20260624`. The normalizer therefore refuses to promote that label into a
mature baseline without an exact-root reviewed attestation.

## New-Algorithm Historical Roots

- historical point directories: `52` (`39` CO2 + `13` H2O)
- normalized analyzer/anchor rows: `56`
- formal fit-eligible rows: `0`
- structural blockers: `5`
- fit-review gaps: `168`

Observed blockers/gaps:

- CO2 and H2O roots have no reviewed baseline attestations;
- reviewed `R0_CO2(T1)` and `R0_H2O(T1)` model files/evaluations are missing;
- one point has no usable machine-readable samples;
- formal component-matched CO2/H2O quality is missing;
- point coverage remains below the required new-algorithm `47/14` contract.

## Conclusion

The normalizer can read and preserve the historical evidence without guessing,
but neither current historical family is safe to feed into a formal coefficient
fit package. The next data action is evidence closure, not hardware rerun:
identify the actual 0613/0620/0621 roots, issue exact-root reviewed
attestations, restore missing machine-readable/QC evidence where it exists, and
provide reviewed R0(T1) evaluations for the absorption profile.
