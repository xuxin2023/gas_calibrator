## Summary

- What changed?
- Why was it needed?

## Scope

- Affected areas:
- Does this touch V1 production logic?
- Does this change `run_app.py`?

## Risk Boundary

- Confirm V1 production flow is not broken.
- Confirm no real-device or COM access was introduced for V2.
- Confirm this change does not claim or imply V2 replaces V1.

## Validation

- Tests or suites run:
- Commands used:
- Evidence type: `simulation` / `replay` / `suite regression` / `parity` / `resilience` / `offline review`

## Artifact and Evidence Notes

- If artifacts changed, confirm role clarity for:
  - `execution_rows`
  - `execution_summary`
  - `diagnostic_analysis`
  - `formal_analysis`
- If export behavior changed, confirm status vocabulary remains:
  - `ok`
  - `skipped`
  - `missing`
  - `error`
- If evidence is simulated, confirm:
  - `evidence_source = simulated`
  - `not_real_acceptance_evidence = true`

## UI Checklist

- Chinese remains the default user-facing language.
- New user-facing copy uses i18n keys.
- 1920x1080 layout remains usable, with scrolling where needed.

## Remaining Risks

- P0:
- P1:
