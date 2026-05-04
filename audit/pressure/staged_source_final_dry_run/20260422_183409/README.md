# CO2 A Staged Dry-Run Blocked Evidence

This evidence records a blocked CO2 A staged source/final dry-run precheck.

This commit exposes sanitized audit/review evidence only. It does not rerun hardware and it does not add any release path.

- The staged valve-4 dry-run did not run.
- CO2 valve 4 did not open.
- CO2 valve 24 did not open.
- H2O valve 10 did not open.
- Source/final stage did not open.
- `_route_final_stage_seal_safety[key]` was not updated.
- verifier, candidate evaluator, and explicit apply were not called.
- The run was blocked because pressure protection was not confirmed:
- `analyzer_pressure_protection_active=false`
- `mechanical_pressure_protection_confirmed=false`
- Final `SYST:ERR` was clear.
- No `VENT 2` TX was observed.
- Cleanup completed.
- This is not real-device sealed pressure acceptance.
- This is not full V1 production acceptance.
- A retry must first provide same-source approved pressure-protection evidence or configuration.

Only a sanitized public summary is committed here. No raw hardware logs, full PACE command traces, valve event streams, or machine-local artifact paths are included in this evidence surface.
