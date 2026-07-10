# V1.5 Candidate Coefficient Fit-Input Gate Evidence

- Date: 2026-07-10
- Scope: candidate coefficient review artifacts only
- Boundary: offline/no-write; no COM ports; no pressure, gas, or water route control; no PostgreSQL connection; no SENCO write
- Protected mature paths changed: no

## Change

`export_v1_5_candidate_coefficients` now supports an explicit fit-input quality gate:

- `--fit-input-quality-summary-csv`
- `--fit-input-quality-devices-csv`
- `--require-fit-input-quality`

When the gate is enabled, candidate coefficients are blocked unless:

- the fit-input quality summary has `run_status=pass`
- the mature-route continuity gate has `fit_input_continuity_gate_status=pass`
- the summary remains no-COM/no-route/no-write
- the device/component row has `fit_input_grade=A`

If the gate blocks a device/component, no coefficient rows are emitted for that candidate and `allowed_for_review=false`.

## Physical Meaning

Candidate coefficients must not become write-review evidence just because a curve can be fit mathematically. The upstream fit-input review must first prove that the data came from acceptable mature-route evidence and that rejected, segmented, migration, retry, or direct-recovery rows are not being silently reused as writeable production evidence.

## Validation

Focused:

```text
python -m pytest tests\test_v1_5_candidate_coefficients.py -q
26 passed in 43.03s
```

Compatibility chain:

```text
python -m pytest tests\test_v1_5_fit_input_quality.py tests\test_v1_5_candidate_coefficients.py tests\test_v1_5_post_run_coefficient_executor.py -q
58 passed, 1 warning in 51.70s
```

The warning is the existing unregistered `v1_5_formal_gate` pytest marker.

