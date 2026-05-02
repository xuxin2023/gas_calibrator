# V1 Runtime Parity Gate

## Background

V1 has long had strong evidence for three offline-side steps:

1. fit quality can be inspected from corrected/autodelivery outputs
2. download plans can be generated in a controlled way
3. SENCO write/readback can prove the intended coefficients were written

What it has not proven is the missing fourth step:

4. the device runtime `co2_ppm` / `h2o` output is actually executing with the written coefficients

That gap explains why "fit looked good" and "readback matched" could still produce field behavior that was wrong.

## Why Offline Coefficients Kept Going Wrong

### Summary fit is not device execution

The corrected report summarizes an offline fit surface. The runtime device only matters if its live execution path uses the same visible inputs, coefficient bank, temperature semantics, and post-processing steps.

### Corrected autodelivery is a report/download/write helper, not a runtime simulator

`run_v1_corrected_autodelivery.py` is useful for:

- no-500 filtering
- pressure-point overrides
- per-gas temperature key selection
- simplification guard rails
- SENCO command generation
- write/readback verification summaries

It does not simulate the full runtime execution chain.

### Offline and runtime semantics can drift apart

Several known drift sources have existed in practice:

- summary fit columns do not necessarily equal runtime execution inputs
- merged multi-run data can blur the runtime chain if rows are concatenated without alignment
- temperature fallback columns can change offline interpretation without proving device runtime uses the same temperature source
- ratio semantics are layered: filtered ratio, raw ratio, signal/ref, and legacy signal fields are not interchangeable
- writeback proves coefficient storage, not displayed runtime behavior

## New Standard

A coefficient set is only `final_write_ready = true` when all three gates pass:

1. `fit_quality`
2. `writeback_quality`
3. `runtime_parity_quality`

If any gate is missing, inconclusive, partial, or failed, the result is still engineering evidence only and must not be treated as write-ready.

## Gate Definitions

### 1. fit_quality

This gate stays in corrected/offline space. It answers:

- was the fit input quality acceptable
- was delivery recommendation still allowed
- did simplification stay within guard rails

### 2. writeback_quality

This gate stays in SENCO delivery space. It answers:

- were the intended groups written
- did readback match
- were any groups partial, skipped, or mismatched

### 3. runtime_parity_quality

This is the missing gate. It answers:

- does the live runtime output match what the visible main-chain would predict
- are the visible runtime inputs sufficient to test that claim
- is the stream using direct ratio semantics, signal/ref semantics, or only legacy fallback semantics

## Why Legacy-Only Stream Cannot Be `final_write_ready`

Legacy-only `YGAS` typically exposes only:

- `co2_ppm`
- `h2o_mmol`
- `co2_sig`
- `h2o_sig`
- `temp_c`
- `pressure_kpa`

It does not expose the direct runtime inputs needed for a trustworthy parity gate:

- `co2_ratio_f` / `h2o_ratio_f`
- `co2_ratio_raw` / `h2o_ratio_raw`
- `ref_signal`
- `co2_signal` / `h2o_signal`
- `chamber_temp_c`
- `case_temp_c`

Therefore legacy-only stream is not a parity fail. It is an evidence gap:

- `runtime_parity_quality = parity_inconclusive_missing_runtime_inputs`
- `final_write_ready = false`

## Observation Rule

Only when enough runtime input/output is visible may parity be allowed to pass.

At minimum the audit must explicitly classify:

- visible runtime inputs available
- visible runtime inputs missing
- parity candidates actually testable
- parity verdict

If the stream is missing direct ratio fields, or missing the temperature channels needed to evaluate visible temperature compensation, the verdict must remain inconclusive.

## Tool Responsibilities

### Single-run corrected toolchain

Responsibilities:

- generate offline fit/report artifacts
- produce download plans
- summarize fit quality
- summarize coefficient source selection

Non-responsibilities:

- does not certify runtime execution parity
- does not by itself make a bundle write-ready

### Merged-run toolchain

Responsibilities:

- merge/alignment preparation across runs
- normalize the evidence scope before parity work

Non-responsibilities for this phase:

- do not assume raw concatenation is valid execution evidence
- do not let corrected autodelivery solve merged runtime parity by itself

### Write-ready gate

Responsibilities:

- combine fit quality
- combine writeback quality
- combine runtime parity quality
- emit a conservative readiness decision

## Practical Rule Going Forward

From now on:

- `fit_good + readback_good` is not enough
- runtime parity must pass before `final_write_ready = true`
