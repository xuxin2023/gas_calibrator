# V1.5 Authoritative Resume Offline State Advance Atomic Writer

This package advances the authoritative V1.5 resume state by exactly one
already verified offline step. It consumes the exact #107 authorization
validation and does not accept direct state-target or candidate arguments.

## Execution boundary

- The CLI requires `--execute-controlled-state-advance`, the exact
  authorization id, and the #107 confirmation template.
- Authorization is independently recomputed before locking and again while
  holding the shared authoritative-state writer lock.
- The current state SHA256 and candidate preview SHA256 are rechecked under the
  lock immediately before replacement.
- The old state is fsynced to a rollback snapshot before a same-directory
  temporary file is atomically replaced into the authoritative path.
- Write readback failure restores the exact previous bytes and verifies the
  rollback SHA256.

## Prohibited actions

This writer does not open COM, control pressure or gas/water routes, write
SN/device ids or SENCO coefficients, connect PostgreSQL, release a calibration,
or authorize database import. It changes only the canonical V1.5 resume-state
JSON after the complete authorization and CAS chain succeeds.

The mature 0613 fitting and 0620/0621 CO2/H2O execution paths are unchanged.

## Verification

- State-advance writer/authorization/preflight, existing atomic writer, and
  entrypoint inventory: `73 passed`.
- Formal run status and flow contract compatibility: `103 passed`.
- Mature-route and full-flow compatibility: `38 passed`.
- Existing controlled-write preflight, atomic writer, and post-write verifier:
  `34 passed`.
- `py_compile`, Ruff, and `git diff --check` pass.
