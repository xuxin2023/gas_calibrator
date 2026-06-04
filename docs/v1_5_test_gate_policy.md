# V1.5 Test Gate Policy

V1.5 formal calibration development uses separated pytest gates so engineering
diagnostics cannot be mistaken for formal CO2/H2O calibration acceptance.

## Formal Gate

Run:

```powershell
python -m pytest -m v1_5_formal_gate -q
```

This gate covers the no-write formal path:

- open-flow CO2/H2O evidence contracts
- MODE2/factory-signal data preservation through artifacts
- pressure-channel quick-check contracts
- QC classification and review surfaces
- evidence registry, report generation, and release decision logic
- parameter governance and no-write safety

The formal gate is offline unless a test explicitly says otherwise. It must not
open COM ports, control water or gas routes, command PACE/VENT/OUTP, write
SENCOx, clear CLEARSENCOx, or refresh real acceptance evidence.

## Diagnostic Gate

Run:

```powershell
python -m pytest -m v1_5_diagnostic_gate -q
```

This gate keeps sealed-pressure, VENT-hold, OUTP, PACE ingress, and pressure
tuning diagnostics visible. These tests may protect engineering tools, but they
are not the V1.5 formal CO2/H2O release gate and must not be treated as real
acceptance evidence.

The same tests are also marked:

```powershell
python -m pytest -m v1_5_legacy_pressure_diagnostic -q
```

Use this marker when reviewing legacy sealed-pressure behavior separately from
the open-flow formal route.

## Current Physical Boundary

The V1.5 formal route is:

```text
device precheck
pressure-channel quick verification
open-flow CO2/H2O sampling
QC and report
candidate coefficient review
```

The formal route excludes by default:

- sealed multi-pressure CO2/H2O fitting
- long-term open-flow dynamic PACE OUTPUT pressure control
- PACE ACT + sink bias as formal pressure-point generation
- VENT-hold as a formal pressure-point control method

These excluded behaviors can remain as engineering diagnostics only. They cannot
enter the formal CO2/H2O fit or be used as real acceptance unless a future
approved scope explicitly promotes them.
