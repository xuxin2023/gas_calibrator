# V1.5 Completion Gap Assessment

Date: 2026-05-25

This assessment describes the V1.5 formal calibration program after the
open-flow route decision. It is intentionally scoped to V1.5. V2 compatibility,
sealed-pressure formal fitting, and real acceptance promotion are outside this
assessment.

## Current Formal Route

The V1.5 formal route is:

```text
设备预检
压力通道快速验证
开放流通 CO2/H2O 主校准采样
QC 与报告
候选系数评审
```

The formal route does not include sealed multi-pressure CO2/H2O fitting,
long-term open-flow PACE OUTPUT dynamic control, PACE ACT + sink bias as formal
pressure-point generation, or VENT-hold pressure control. Those paths can remain
engineering diagnostics only.

## Current Completion Snapshot

Engineering estimate:

| Area | Status | Notes |
| --- | --- | --- |
| V1.5 water/gas production route guardrails | Mostly restored | Open-flow atmosphere handling and no-write guardrails are documented and covered by focused tests. Do not refactor route control casually. |
| MODE2 data preservation | Implemented in software contract | Normal and factory fields are carried through sample/QC/report contracts. Real analyzer coverage still depends on clean real run evidence. |
| Formal evidence contracts | Implemented offline | Plan, standard gas, COM22 reference, pressure quick-check, sample artifacts, and no-write config are validated. |
| Pressure-channel quick-check model | Implemented offline | Requires continuous atmosphere-hold evidence and COM22 traceability. Real check is blocked when COM22/PACE are physically unavailable. |
| Evidence package and registry | Implemented offline | Evidence bundle, hashes, artifact roles, database import/query/migration scaffolding exist. Needs database operational hardening. |
| QC and advanced QC | Implemented as first offline version | Root-cause, humidity diagnostics, factory-signal health, control charts, uncertainty budget scaffolds exist. Needs real data tuning. |
| Reports | Implemented as draft/review artifacts | Markdown/DOCX/PDF/model outputs exist. Formal release remains blocked until released uncertainty and approval workflow are complete. |
| Operation console / review surface | Implemented as static sidecar UI | Read-only/no-write surfaces exist. Not connected to the production runner or real-time UI. |
| Parameter governance | Implemented as no-write policy model | A/B/C/D/E parameter classification exists. Device writes remain blocked in this version. |
| Test gates | Implemented | `v1_5_formal_gate` is separated from diagnostic/legacy sealed-pressure tests. |
| Real V1.5 formal no-write run | Pending hardware | Needs pressure controller, COM22, analyzer communication, standard gas, and explicit operator authorization. |

## Verified Offline Gate

Formal gate command:

```powershell
python -m pytest -m v1_5_formal_gate -q
```

Current result:

```text
96 passed
```

Diagnostic/legacy pressure tests are intentionally separate:

```powershell
python -m pytest -m v1_5_diagnostic_gate -q
```

Those tests protect engineering probes and legacy sealed-pressure behavior. They
are not the formal CO2/H2O release gate.

## Work That Can Continue Now Without Hardware

### P0: Freeze the Current V1.5 Formal Gate

Create a focused commit for the V1.5 formal gate, evidence contracts, report
sidecars, operation console, parameter governance, advanced QC, and test gate
policy.

Acceptance:

- `python -m pytest -m v1_5_formal_gate -q` passes.
- Diagnostic files remain outside the formal gate.
- No COM ports are opened.
- No route/valve/PACE control is executed.
- No SENCOx/CLEARSENCOx/coefficient write path is enabled.

### P0: Make a Formal Readiness Fixture Package

Create one canonical simulated evidence package that represents a complete
V1.5 no-write formal run:

- `formal_plan_snapshot.json`
- `standard_gas_snapshot.json`
- `com22_pressure_reference.json`
- `runtime_config_snapshot.json`
- `samples_*.csv`
- `pressure_channel_quick_check_*.csv`
- `evidence_bundle.json`
- reports and review surface outputs

Acceptance:

- The package can be regenerated from source inputs.
- All artifacts have SHA256.
- The report can be rebuilt from the evidence bundle.
- Sealed diagnostic rows, if present, are excluded from formal fit eligibility.

Current offline generator:

```powershell
python -m gas_calibrator.tools.prepare_v1_5_canonical_evidence_package `
  --output-dir <canonical_package_dir>
```

Physical meaning of the canonical package:

- the simulated CO2 source is a 900 ppm traceability snapshot
- the simulated COM22 record is the primary pressure reference
- analyzer pressure P is checked against COM22 under verified continuous
  atmosphere hold
- CO2/H2O MODE2 and factory signal fields are retained in raw samples
- one sealed diagnostic row is retained as evidence but excluded from formal
  fit eligibility
- all generated outputs are sidecar-only and marked as not real acceptance
  evidence

### P1: Harden the PostgreSQL Evidence Registry

The registry exists conceptually and as code scaffolding, but the next useful
offline step is operational hardening:

- migrations are idempotent
- import is repeatable
- duplicate run import is deterministic
- artifact hashes are queryable
- report regeneration can be traced from database rows back to files

Acceptance:

- A simulated V1.5 package imports into PostgreSQL.
- Query tool returns run, devices, gases, points, QC, reports, and artifacts.
- Bad/rejected rows are retained with reasons.

### P1: Improve Report Release Logic

Current report generation supports draft/review/formal statuses. The remaining
offline work is to formalize release criteria:

- released uncertainty inputs
- reviewer/approver signatures
- no-write statement
- pressure coverage limitation
- sealed diagnostic exclusion statement

Acceptance:

- Draft report cannot be mistaken for formal certificate.
- Formal release requires released uncertainty and approval metadata.
- Report states that pressure compensation coverage is limited unless explicitly
  verified.

### P1: Build a Stable No-Write Formal Runner Wrapper

The production runner can produce samples, but V1.5 still needs a narrow wrapper
that packages a run into the formal evidence chain without changing water/gas
route behavior.

Acceptance:

- Wrapper reads an existing completed run directory.
- It does not open COM ports.
- It does not call route, valve, PACE, or SENCO commands.
- It generates the evidence bundle, QC, reports, workbench, and review surface.

### P2: Advanced QC Calibration Tuning

Advanced QC algorithms are present, but thresholds still need tuning against
real open-flow data:

- steady-state selector windows
- humidity pressure normalization thresholds
- factory signal drift limits
- pressure trend limits
- uncertainty contribution defaults
- root-cause wording

Acceptance:

- Tuning is performed on replay/simulated packages first.
- Threshold changes are versioned.
- No rejected point is removed without an explicit reason.

### P2: Static UI Refinement

The current UI sidecars are useful for review, but not yet a live operator UI.
Continue improving read-only surfaces first:

- Chinese-first labels
- blocked reasons
- visible physical status
- role-specific review cards
- report and evidence links

Acceptance:

- The UI does not imply real device control.
- Device writes and SENCO actions remain hidden/blocked.
- Formal-vs-diagnostic boundary is visible.

## Work Blocked Until Hardware Is Available

### Real Pressure-Channel Quick Check

Blocked while COM22/PACE are unavailable.

Required physical meaning:

- analyzer internal pressure P
- COM22 pressure reference
- optional PACE pressure
- same open-atmosphere/continuous-atmosphere pressure state

This is not CO2/H2O fitting and must not use CO2/H2O data to tune pressure.

### Real Open-Flow CO2/H2O Formal No-Write Run

Requires:

- analyzer communication stable
- standard gas certificate snapshot
- water/humidity reference snapshot
- COM22 certificate snapshot
- pressure quick check completed or formally blocked
- operator authorization

Output is evidence for review, not automatic coefficient write.

### Candidate Coefficient Review Against Real Data

Blocked until A-grade real open-flow samples exist.

Candidate coefficients must be based only on A-grade formal samples. Diagnostic
sealed-pressure rows, B/C rows, and rejected rows cannot enter the formal fit.

## Work Not Recommended Yet

Do not start these until the V1.5 no-write formal evidence path is stable:

- automatic SENCO write
- CLEARSENCO workflows
- real acceptance promotion
- sealed-pressure CO2/H2O formal fitting
- live UI controlling route/valve/PACE
- replacing the default application entrypoint

## Next Recommended Sequence

1. Freeze the V1.5 formal gate and evidence sidecar files.
2. Produce one canonical simulated formal evidence package and import it into
   the registry.
3. Harden PostgreSQL import/query/report-regeneration.
4. Tune advanced QC with replay packages.
5. When pressure hardware returns, run pressure-channel quick check first.
6. Only after pressure quick check passes, run open-flow CO2/H2O formal no-write
   sampling.
7. Review candidate coefficients from A-grade samples only.

This sequence keeps the physical meaning clean: pressure P is validated as an
input quantity before CO2/H2O formal sampling, and only stable open-flow
component data can support candidate coefficient review.
