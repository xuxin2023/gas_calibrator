# V1.5 component-QC authority review

## 1. Decision

No reviewed 0613/0620/0621 post-sample component-QC writer exists in the tracked
repository or its git history. Component-QC generation and historical backfill
must remain blocked.

This is not evidence that the mature physical route is missing. The mature CO2
and H2O sampling entries contain the pre-sample ratio stability gates used to
decide when sampling may begin. They do not create
`formal_open_flow_data_quality_by_analyzer.csv` and must not be treated as a
post-sample per-analyzer grading algorithm.

## 2. Source findings

### Mature tracked samplers

The tracked mature CO2 and H2O sampling sources both contain:

- route-specific filtered-ratio preseal tolerance configuration;
- an A-grade preseal tolerance configuration;
- minimum preseal sample count configuration;
- no import of `apply_open_flow_quality_grades`;
- no component-QC output filename.

The 125 P2 evidence directories record these runtime pre-sample settings:

| Route | Point count | Hard tolerance | A tolerance | Policy | Minimum samples |
|---|---:|---:|---:|---|---:|
| CO2 | 89 | 0.001 | 0.0005 | reject | 10 |
| H2O | 36 | 0.001 | 0.0005 | warn | 10 |

These values describe sample-readiness behavior in those runs. They do not, by
themselves, authorize assigning A/B/C grades after sampling.

### Polluted-root migration writer

The only concrete writer found is:

`D:\gas_calibrator\src\gas_calibrator\validation\v1_5_open_flow_quality.py`

It is untracked in the frozen root worktree and absent from git history. Its
SHA-256 is:

`07a63eb31ef5e6701d0b0257c699ecc3049e09cd981e1a6e512f52b17833ef0e`

The polluted-root CO2 and H2O sampling files import this writer. It implements
per-analyzer A/B/C grading, cadence checks, and the expected component-QC CSV.
This explains the 0624 output schema, but does not give the implementation
0613/0620/0621 authority.

### Historical component-QC artifacts

The legacy evidence catalog contains 43 component-QC files:

- CO2: 43
- H2O: 0
- classified `forbidden_0624_or_migration`: 43
- artifact hash mismatches: 0

The files are internally traceable and useful as schema/diagnostic references.
They cannot establish a mature production threshold contract because they all
come from the forbidden migration lineage and provide no H2O precedent.

## 3. Physical and mathematical separation

Preseal stability and post-sample component quality answer different questions:

1. **Preseal gate:** Is each active analyzer sufficiently stable now to begin a
   synchronized sample window under the intended physical condition?
2. **Post-sample component QC:** Given the complete sample window, which rows and
   analyzers are usable for calibration, diagnostic modeling, or rejection, and
   why?

Reusing `0.0005/0.001` across these layers without an explicit reviewed contract
would hide decisions about cadence, frame usability, alignment failures,
point-level physical warnings, relative tolerances, and whether B-grade data may
enter fitting.

CO2 zero-gas evidence and H2O dry/low-water evidence also remain different
physical roles. A future component-QC writer must not manufacture or exchange
those anchors.

## 4. Remaining authority gaps

The audit records four blockers:

1. `tracked_mature_component_qc_writer_missing`
2. `component_qc_writer_absent_from_git_history`
3. `all_observed_component_qc_artifacts_are_0624_or_migration`
4. `h2o_historical_component_qc_examples_missing`

All 125 P2 candidate directories additionally retain manual review conditions;
therefore a future writer must not bulk-promote them merely because their input
files are structurally complete.

## 5. Next allowed package

The next package may define a **reviewed component-QC generator contract** only.
It must explicitly specify:

- route-specific ratio and relative-tolerance rules;
- frame/data availability and per-analyzer minimum counts;
- sample cadence and alignment handling;
- route physical warning and purge handling;
- A/B/C grade meanings and exact fit/diagnostic flags;
- treatment of CO2 and H2O as separate physical components;
- immutable input hashes and idempotent output behavior.

That package must remain design-only. It must not generate the 125 missing QC
files until the contract has independent review and tests.

## 6. Safety boundary

This audit is offline only. It opens no COM ports, controls no equipment, writes
no device identity or coefficients, connects no PostgreSQL instance, generates
no QC backfill, authorizes no fitting, and changes no release/import state.

## 7. Verification

Focused authority/P2/entrypoint tests:

```text
47 passed, 1 warning
```

Legacy evidence-chain and entrypoint regression:

```text
71 passed, 2 warnings
```

Expanded historical replay, mature-route, fit-input, and CO2/H2O sampling
configuration regression:

```text
166 passed, 2 warnings
```

The warnings are the existing unregistered `v1_5_formal_gate` marker and do not
represent a functional failure in this package.
