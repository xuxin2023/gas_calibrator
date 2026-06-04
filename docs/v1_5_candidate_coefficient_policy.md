# V1.5 Candidate Coefficient Policy

This note defines the offline/no-write policy for V1.5 CO2/H2O candidate
coefficient generation. It protects the open-flow gas and water routes by
keeping coefficient preparation separate from device control, route control,
PACE control, and SENCO writes.

## Formal Scope

V1.5 formal CO2/H2O calibration uses open-flow, current-atmosphere A-grade
samples only. The physical basis is continuous clean standard gas refresh:

```text
standard gas or humidity source
-> open-flow route
-> analyzer optical chamber
-> stable CO2/H2O, dewpoint, pressure, temperature, ratio/signal window
-> A-grade sample set
```

Sealed-pressure samples, dynamic pressure probes, PACE continuous sink, and
VENT-hold samples remain engineering diagnostics. They must not enter the
formal CO2/H2O fit or real acceptance path.

## Current-Atmosphere Model Boundary

When the run only covers current atmospheric pressure, pressure terms are not
identifiable from the data. The candidate fit freezes these terms by default:

```text
P
R*P
R*T*P
```

When the run only covers one temperature condition, temperature terms are not
identifiable from the data. The candidate fit freezes these terms by default:

```text
T
T*T
R*T
```

For the current V1.5 scope, pressure P is a validated input and QC condition,
not a CO2/H2O pressure-compensation fitting dimension.

## Fit Eligibility

A sample can enter a candidate fit only when all of the following are true:

```text
formal_sample_grade = A
sample_role = fit
pressure_mode is open/ambient/open_flow
analyzer identity is bound by device ID
pressure quick check is traceable and passes
standard gas or humidity reference target is available
ratio, pressure, and temperature fields are available
```

## Per-Analyzer Reuse Boundary

Multi-analyzer evidence is judged by analyzer device ID, not by the serial
port label or by the fleet as one undifferentiated batch. A failing analyzer
does not invalidate other analyzers that saw the same open-flow gas condition.

For example, if `ID001` or `ID027` reports zero/suspect MODE2 frames, those
device IDs are rejected independently. Other analyzer device IDs in the same
run can still be reused when their own evidence satisfies:

```text
device identity bound by analyzer MODE2 ID
pressure quick check bound to the same analyzer device ID
MODE2 contract/QC pass
A-grade open-flow sample count sufficient
no route/pressure-mode contamination
```

This rule protects the physical meaning of the run: every analyzer is an
independent measurement chain sampling the same clean flowing gas. A sensor or
serial failure on one chain is evidence against that chain, not against the
gas state seen by the other chains.

The default policy requires at least two distinct fit targets. A single
900 ppm point is useful for smoke testing or verification, but it is not enough
to generate a formal CO2 calibration curve.

When a run contains one A-grade target only, the candidate review must classify
it as:

```text
a_grade_single_target_review_only
```

It may support a single-point bias review or a later independent verification
role, but it must not be promoted into a full coefficient fit.

## Verification Requirement

Candidate coefficients are not review-ready until an independent verification
point passes. Verification samples must:

```text
formal_sample_grade = A
sample_role = verification
come from open-flow data
use a point identity not reused by the fit samples
remain outside diagnostic or sealed-pressure roles
```

The verification step answers a different physical question from fitting:

```text
fit: which coefficients best explain the selected A-grade calibration points?
verify: do those coefficients predict an independent clean-gas point?
```

If verification is missing, reused, or outside the error limit, the candidate
stays blocked or failed. It must not be written to the analyzer.

## No-Write Boundary

The candidate coefficient exporter is an evidence generator only. It must
report:

```text
auto_write_allowed = false
opens_com_ports = false
controls_water_or_gas_routes = false
writes_coefficients = false
```

Any SENCO write remains a separate controlled workflow with backup, approval,
write-back read verification, post-write validation, and rollback evidence.

## Water Route Evidence

H2O candidate evidence must preserve the water-route physical quantities:

```text
dewpoint
H2O ratio
H2O signal
H2O mmol/mol or equivalent target
pressure-normalized humidity indicators when available
```

Water-route data must not be reduced to a final H2O number only. The evidence
must keep enough raw and normalized information to distinguish real moisture
release from pressure or compensation effects.

Before a new H2O open-flow sample window, the runner must separately check:

```text
dewpoint tail stability
H2O ratio stability
analyzer internal pressure P stability
```

The analyzer pressure gate does not control PACE and does not create a sealed
pressure point. It only proves that the pressure value used internally by the
analyzer algorithm is no longer moving during the H2O sample window. If H2O
frames are B-grade because `analyzer_pressure_hpa_span` is excessive, those
frames remain review evidence and must not be silently upgraded to A-grade.
