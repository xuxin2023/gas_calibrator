# A4-P7：Route-Scoped Pressure Reference Guard

## 1. Scope
Prevent H2O ambient_open pressure reference from leaking into CO2 route when both H2O ambient and CO2 sealed points coexist in the same points list. Add route-scoped pressure filtering to RoutePlanner with backward-compatible auto-detection.

## 2. Problem Found
`RoutePlanner._pressure_reference_points(points)` collects ambient refs from ALL points before numeric refs. In A4's 15-point matrix (1 H2O ambient + 7 H2O sealed + 7 CO2 sealed), `h2o_pressure_points()` correctly returns ambient+7 sealed, but `co2_pressure_points()` was also returning the H2O ambient ref — meaning CO2 route would get an ambient pressure point it shouldn't have.

## 3. Code Fix
**`route_planner.py`** — three changes:

| Change | Detail |
|--------|--------|
| `_route_scoped_pressure_references(points)` | Auto-detect: returns True when points contain both H2O ambient + CO2 points |
| `_points_for_pressure_references(points, route)` | New helper: when scoping active, filter to route-specific points (H2O-only or CO2-only) |
| `h2o_pressure_points` / `co2_pressure_points` | Call `_points_for_pressure_references` before `_pressure_reference_points` |

## 4. Why Auto-Detection (Not Config Flag)
`WorkflowConfig` is a strict dataclass — unknown fields like `route_scoped_pressure_references` are dropped by `from_dict`. Auto-detection avoids modifying `config/models.py` (outside allowed files) while still achieving the guard. When H2O ambient + CO2 coexist in the same points list, scoping is automatically enabled.

## 5. Backward Compatibility
- Profiles without H2O ambient + CO2 mix: no behavior change
- Profiles with H2O ambient + CO2 mix: H2O ambient correctly excluded from CO2 route
- All existing tests pass unchanged

## 6. H2O Route Result (A4 15-point matrix)
```
h2o_pressure_points → [ambient_open, 1100hPa, 1000hPa, 900hPa, 800hPa, 700hPa, 600hPa, 500hPa]
```
1 ambient + 7 sealed, ambient first, sorted descending.

## 7. CO2 Route Result (A4 15-point matrix)
```
co2_pressure_points → [1100hPa, 1000hPa, 900hPa, 800hPa, 700hPa, 600hPa, 500hPa]
```
7 sealed only. 0 ambient.

## 8. CO2 Ambient Open Still Deferred
Not included in this round.

## 9. Not Modified
- V1: untouched
- PointParser: untouched
- pressure_selection.py: untouched
- H2O/CO2 route runners: untouched
- A2 baseline: untouched
- Full route points: untouched
- A4 points file: untouched
- config/models.py: untouched

## 10. No Real COM / No Parameter Write

## 11. Test Results
```
test_a4_single_temp_profile.py ...............  45 passed
test_simulation_profile_no_write_marker.py ..   6 passed
test_no_write_guard.py .....................    19 passed
test_analyzer_fleet_service.py .............    12 passed
test_route_planner.py ......................     9 passed
test_temperature_group_runner.py ..........     11 passed
test_compare_v1_v2_control_flow.py ........     8 passed
                                              110 passed total
```

## 12. Decision
**A4_P7_ROUTE_SCOPED_PRESSURE_REFS_PASS**
