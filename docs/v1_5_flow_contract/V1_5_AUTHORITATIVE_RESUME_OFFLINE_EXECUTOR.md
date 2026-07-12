# V1.5 Authoritative Resume Offline Executor

This executor may run exactly one canonical offline V1.5 step after explicit authorization. It cannot execute physical, device-write, coefficient-write, state-write, or database steps.

## Unlock

- explicit `--execute-offline-step`;
- exact attempt ID from the fresh offline candidate gate;
- exact confirmation text `execute_v1_5_offline_canonical_step_only`;
- candidate gate independently revalidated immediately before process start.

## Command Boundary

- Python module invocation only: `python -m gas_calibrator.tools.*`;
- module must be classified as offline and no-COM;
- `shell=false` with no shell metacharacters;
- generic execute, COM, pressure, route, write, state-replace, and database-import flags are forbidden;
- canonical `--output-dir` must remain inside the current run root;
- command, module, step ID, and attempt ID cannot be replaced by caller input.

## Postcondition

Process return code zero is insufficient. Every canonical expected output must be newly created or have a new SHA256. Otherwise execution is held.

The execution artifact stores each expected output's before and after SHA256. A later verifier must reject any output whose current hash no longer matches the recorded after hash.

Successful execution is `offline_step_executed_pending_verification`. It does not advance authoritative state. A separate post-execution verifier must bind outputs before any state transition.

## Protected Boundary

No COM, pressure, gas/water route, SN/device ID, SENCO/coefficient, PostgreSQL, release, or import action is allowed. The 0613 fitting baseline and 0620/0621 mature route implementations remain unchanged.
