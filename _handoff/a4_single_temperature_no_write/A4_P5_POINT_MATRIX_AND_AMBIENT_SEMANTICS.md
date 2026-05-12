# A4-P5：A4 20℃ H2O+CO2 Simulation Point Matrix + Ambient Semantics

## 1. Scope
Upgrade A4 simulation points from 5-point draft to 15-point matrix with ambient_open vs sealed_pressure semantics. H2O gets ambient_open + 7 sealed pressure points; CO2 gets 7 sealed pressure points only. CO2 ambient delegate deferred.

## 2. H2O = ambient_open (1) + sealed_pressure (7)
| idx | kind | pressure_hpa | sealed | vent | route |
|-----|------|-------------|--------|------|-------|
| 1 | ambient_open | 1013.25 | false | open | h2o |
| 2-8 | sealed_pressure | 1100..500 | true | closed | h2o |

- ambient_open: `pressure_control_active=false`, `vent_expected="open"`, `pressure_hpa_role="ambient_open_placeholder_measured_at_runtime"`
- sealed_pressure: `pressure_control_active=true`, `vent_expected="closed"`, 7 pressure steps descending 1100→500 hPa

## 3. CO2 = sealed_pressure (7) only
| idx | kind | pressure_hpa | sealed | vent | route |
|-----|------|-------------|--------|------|-------|
| 9-15 | sealed_pressure | 1100..500 | true | closed | co2 |

- CO2 ambient_open: 0 points. Deferred risk item.
- CO2 ppm: 1000.0 simulation placeholder. `NEED_USER_DECISION_CO2_PPM` on index 9.

## 4. Ambient Open vs 1000 hPa Sealed
- **ambient_open** (index 1, 1013.25 hPa): current atmospheric open point, NOT pressure-controlled. Vent expected open.
- **1000 hPa** (index 3 H2O, index 10 CO2): sealed pressure-control point, vent closed.
- Profile notes: `ambient_open_vs_1000hpa` field documents this distinction.

## 5. CO2 Ambient Open Gap
CO2 ambient open point NOT included. Recorded as `co2_ambient_open_point = "not included in this round; deferred risk item"` in profile notes.

## 6. Future Flexible Matrix Goal
Profile records: `future_matrix_goal = "future V2 should support flexible temperature/gas/pressure point combinations like V1, with no-write gates and physical constraints"`

## 7. Not Modified
- Runtime: untouched
- V1: untouched
- A2 baseline config/points: untouched
- `full_route_points_simulated.json`: untouched
- Core services (no_write_guard, analyzer_fleet, pressure_control, valve_routing, route runners): untouched

## 8. No Real COM / No Parameter Write
No real COM opened. No ID/SENCO/zero/span/coefficient/calibration written. No `--execute-probe`.

## 9. Test Results
```
tests/v2/test_a4_single_temp_profile.py ...............  35 passed
tests/v2/test_simulation_profile_no_write_marker.py ..   6 passed
tests/v2/test_no_write_guard.py .....................    19 passed
tests/v2/test_analyzer_fleet_service.py .............    12 passed
                                                         72 passed total
```

## 10. Decision
**A4_P5_POINT_MATRIX_READY**

15-point matrix with ambient_open/sealed_pressure semantics defined. H2O ambient open included; CO2 ambient open deferred. No-write/simulation-only/production=false boundaries intact. Ready for operator review.
