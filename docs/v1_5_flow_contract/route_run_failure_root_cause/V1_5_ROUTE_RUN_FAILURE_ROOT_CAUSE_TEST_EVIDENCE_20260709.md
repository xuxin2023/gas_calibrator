# V1.5 Route Run Failure Root-Cause Test Evidence

Command:

```powershell
python -m pytest tests\test_v1_5_route_run_failure_root_cause.py tests\test_v1_5_formal_run_continuity_gate.py tests\test_v1_5_production_entrypoint_gate.py tests\test_v1_5_mature_route_contract.py -q
```

Result:

```text
26 passed in 5.22s
```

Coverage:

- CO2 40C dewpoint rebound is classified as dry-gas dewpoint instability.
- Stale `running` manifest rows with completed point artifacts are blockers.
- PACE vent `NO_RESPONSE` is classified for CO2 and H2O.
- Truncated manifest failure reasons are completed by reading point logs.
- H2O aborted runs without `queue_manifest.csv` are not treated as success.
- Direct/retry point folders without closed queue manifests require supersedence review.
- Pressure gauge `NO_RESPONSE` is separated from PACE vent `NO_RESPONSE`.
- The audit remains offline/no-COM/no-write/no-DB/no-route.
