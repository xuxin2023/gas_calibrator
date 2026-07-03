# V1.5 Initialization Readiness Contract

- Date: 2026-06-30
- Scope: offline readiness/report/database-sidecar contract for the formal V1.5 initialization stage.
- Boundary: this document does not authorize COM access, PostgreSQL mutation, SN writes, SENCO writes, pressure control, gas routing, or water routing.

## Purpose

The formal initialization runner owns the pre-route evidence plan. The readiness exporter summarizes existing evidence after the operator or controlled tools have produced it. The readiness exporter must remain offline: it reads files, emits JSON/Markdown/CSV/sidecar artifacts, and never repairs missing hardware evidence by itself.

## Contract

| Area | Contract |
|---|---|
| Active analyzers | Supports 1 to 6 active analyzers. A six-analyzer batch is not assumed. |
| Primary identity | `sn_code/device_code` is the production identity key. |
| Compatibility identity | The protocol device ID remains a query alias and command identity, but is not the only unique production key. |
| Transport | COM port and GA slot are transport mapping only. |
| Database | Production preflight targets PostgreSQL 18 with `run_v1_5_initialization_db_preflight --require-postgresql-18`. |
| Runtime | MODE2, 1 Hz active upload, `AVERAGE1/2`, and analyzer command spacing `>=1.0s`. |
| Temperature | SENCO7/SENCO8 are neutralized for both classic and new algorithm devices. Temperature calibration is disabled until chamber-representative evidence says otherwise. |
| CHECK monitor | `CHECK,YGAS,FFF` is read-only point-level evidence after all active analyzer chamber temperatures are stable and before point sampling. |
| Legacy CHECK support | A legacy device that does not support CHECK does not block the initialization mainline; normal chamber-temperature stability remains the gate. |

## Ordering

1. Plan SN/device_code identity and active analyzer list.
2. Freeze MODE2 identity and GETCO1-9 epoch-0 snapshots.
3. Handle S5/S6/S7/S8/S9 through dedicated controlled tools or archive evidence.
4. Complete pressure-channel evidence or explicitly block before open-flow.
5. Run PostgreSQL 18 initialization DB preflight for traceability.
6. Prove formal route readiness without running CO2/H2O sampling.
7. During formal point execution, after all active analyzer chamber temperatures stabilize, record CHECK monitor evidence when supported.
8. Export initialization readiness JSON, Markdown, evidence index, and database sidecar.

## Non-Execution Rule

The readiness exporter reports the contract; it does not execute the contract. In particular:

- It does not connect to PostgreSQL.
- It does not open COM ports.
- It does not write SN/device_code.
- It does not write SENCO coefficients.
- It does not control PACE, valves, CO2 routes, or H2O routes.

Any real action still belongs to the dedicated V1.5 controlled tool or mature CO2/H2O queue, with explicit operator authorization where required.
