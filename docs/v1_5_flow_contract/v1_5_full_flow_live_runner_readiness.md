# V1.5 全流程 Live Runner Readiness

- schema: `v1_5_full_flow_live_runner_readiness_v1`
- run_id: `v1_5_contract_reference`
- one_button_live_runner_ready: `False`
- automation_level: `supervised_tool_chain_with_controlled_live_gates`
- summary: V1.5 can generate an auditable supervised plan and offline review chain, but it is not yet a one-button unattended live runner because identity, pressure, CO2/H2O route control, and SENCO writes remain explicit controlled gates.

## Required Authorizations

- `real_com`
- `pressure_control`
- `route_control`
- `coefficient_write`

## Domain Readiness

| Domain | Status | Required authorization | Physical risk | Next action |
| --- | --- | --- | --- | --- |
| `offline_planning` | `ready_offline_supervised` | `none` | none during generation; artifacts remain review evidence, not real acceptance | keep these stages as offline prerequisites before any controlled live step |
| `initialization_contract` | `ready_offline_supervised` | `none` | none during generation; missing live GETCO/SENCO/CHECK evidence remains a gate rather than being repaired automatically | review the generated initialization plan, then run only the dedicated V1.5 identity/SN/auxiliary tools that have explicit authorization |
| `identity_and_epoch0` | `requires_real_com_authorization` | `real_com` | wrong ID/COM binding corrupts traceability and may write coefficients to the wrong analyzer | run the validated identity/GETCO snapshot with 1s or longer command spacing, then verify the no-write epoch-0 evidence offline |
| `auxiliary_coefficients` | `blocked_controlled_write` | `real_com, coefficient_write` | old output trims or bad T/P input coefficients can be silently absorbed into CO2/H2O candidate fits | backup old coefficients; neutralize S5/S6 when appropriate; review S7/S8/S9 before component fitting |
| `pressure_channel` | `requires_pressure_authorization` | `real_com, pressure_control` | bad internal pressure contaminates component outputs and makes pressure compensation impossible to interpret | run pressure SENCO9 acquisition/review with the restored closed-volume PACE control contract before component sampling |
| `temperature_channel` | `offline_review_waiting_for_temperature_evidence` | `real_com` | bad chamber/case temperature input can be absorbed into CO2/H2O coefficients across temperature groups | use digital thermometer evidence to review S7/S8; do single-point repair only as documented recovery, not final full-range proof |
| `co2_open_flow` | `requires_route_authorization` | `real_com, route_control` | sampling after valve closure, dirty/damp line state, or unstable ratio creates non-representative CO2 fit data | run validated CO2 open-flow runner; sample only while the gas route remains open and preserve per-device reject reasons |
| `h2o_open_flow` | `requires_route_authorization` | `real_com, route_control` | HGEN cycling, wet line memory, or unstable dewpoint can produce water evidence that is not a stable humidity state | run validated H2O open-flow runner with continuous HGEN strategy during the water route and per-device grading |
| `candidate_fit_and_qc` | `offline_review_waiting_for_run_artifacts` | `none` | optical saturation, invalid frames, wrong low-end anchors, or unmodeled trims can make offline residuals disagree with real output | use all traceable stable fit-eligible points while keeping CO2 zero gas and H2O dry-gas anchors conceptually separate |
| `controlled_write_and_reverify` | `blocked_controlled_write` | `real_com, coefficient_write, route_control` | wrong coefficient epoch or missing post-write proof can create a calibrated-looking but invalid analyzer | write only reviewed per-device candidates, then run independent open-flow reverification before release |
| `archive_and_release` | `offline_review_waiting_for_run_artifacts` | `none` | if evidence status is stale, the operator may believe a run is complete while audit artifacts remain incomplete | refresh evidence status after acquisition/write/report steps; generate per-device certificates from the evidence package |

## Not Ready Reasons

- identity_and_epoch0: formal calibration must bind every COM transport to analyzer device ID, freeze GETCO1-9, and pass the offline identity/GETCO readiness sidecar before sampling
- auxiliary_coefficients: S5/S6 display trims and S7/S8/S9 input channels must be backed up, neutralized, repaired, or explicitly modeled
- pressure_channel: analyzer pressure P is an input to CO2/H2O firmware calculations and must be verified or calibrated first
- temperature_channel: temperature evidence can be reviewed offline, but abnormal S7/S8 must be repaired before component acceptance
- co2_open_flow: CO2 sampling controls gas valves and must prove clean open-flow gas, dewpoint condition, ratio stability, and per-device sample eligibility
- h2o_open_flow: H2O sampling controls water route/HGEN and must prove dewpoint, H2O ratio, dry/wet ppmv, and reference evidence stability
- candidate_fit_and_qc: candidate fitting is offline-ready after raw open-flow samples, factory-signal health, and fit-input QC exist
- controlled_write_and_reverify: SENCO writes change the analyzer measurement model and require per-device review, readback, rollback plan, and independent reverification
- archive_and_release: database import, evidence status refresh, and reports are offline-ready once final run artifacts exist

## Guardrail

- This readiness artifact is generated offline from the V1.5 full-flow plan.
- It does not open COM ports, control PACE, control gas/water routes, or write SENCO.
- `one_button_live_runner_ready=false` is intentional until controlled live gates are implemented end to end.
