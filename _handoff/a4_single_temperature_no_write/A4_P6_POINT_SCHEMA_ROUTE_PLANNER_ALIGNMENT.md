# A4-P6：Point Schema & Route-Planner Alignment

## 1. Scope
Align A4 simulation point matrix with V2 PointParser/RoutePlanner schema. A4-P5 added `point_kind`/`sealed`/`pressure_control_active`/`vent_expected` fields, but PointParser only recognizes `pressure_mode` and `pressure_selection_token`. This round adds those parser-recognized fields without changing runtime.

## 2. Problem Found
`point_kind`, `sealed`, `pressure_control_active`, `vent_expected` are human-readable semantic markers. PointParser (`_row_to_point`) and RoutePlanner (`_pressure_reference_points`) actually use:

| Concept | Parser field | Resolution |
|---------|-------------|------------|
| Ambient open | `pressure_mode: "ambient_open"` or `pressure_selection_token: "ambient"` | `effective_pressure_mode` → `"ambient_open"`, `pressure_hpa` → `None` |
| Sealed controlled | `pressure_mode: "sealed_controlled"` (or auto-detected from numeric `pressure_hpa`) | `effective_pressure_mode` → `"sealed_controlled"` |

## 3. Fix Applied
**H2O ambient_open point (index 1):**
- `pressure_hpa`: `null` (was 1013.25; parser nullifies it when `pressure_mode=ambient_open`)
- Added `"pressure_mode": "ambient_open"`
- Added `"pressure_selection_token": "ambient"`
- Added `"pressure_target_label": "当前大气压"` (also derived by parser)
- `pressure_hpa_role`: `"ambient_open_measured_at_runtime"`

**All 14 sealed_pressure points (indices 2-15):**
- Added `"pressure_mode": "sealed_controlled"` to each
- `pressure_hpa` kept numeric (1100..500)

## 4. 1000hPa Confirmed Sealed
Both H2O index 3 and CO2 index 10 have `pressure_hpa: 1000.0` with `pressure_mode: "sealed_controlled"`. Parser resolves to `is_ambient_pressure_point=False`.

## 5. CO2 Ambient Open Still Not Included
0 CO2 ambient_open points. Deferred risk item.

## 6. Future Flexible Matrix Goal Preserved
`future_matrix_goal` in profile notes unchanged.

## 7. Not Modified
- Runtime: untouched
- PointParser: untouched
- RoutePlanner: untouched
- pressure_selection.py: untouched
- V1: untouched
- A2 baseline: untouched
- full_route_points_simulated.json: untouched

## 8. Test Results
```
test_a4_single_temp_profile.py ...............  41 passed  (incl. 7 new parser tests)
test_simulation_profile_no_write_marker.py ..   6 passed
test_no_write_guard.py .....................    19 passed
test_analyzer_fleet_service.py .............    12 passed
                                               78 passed total
```

**New parser tests:**
| Test | What it verifies |
|------|-----------------|
| `test_point_parser_recognizes_h2o_ambient_open` | H2O ambient point: `is_ambient_pressure_point=True`, `pressure_hpa=None`, `pressure_target_label="当前大气压"` |
| `test_point_parser_treats_1000hpa_as_sealed` | 1000hPa points: `is_ambient_pressure_point=False`, `pressure_mode="sealed_controlled"` |
| `test_point_parser_h2o_pressure_refs_ambient_plus_7_numeric` | 1 ambient + 7 sealed = 8 H2O points |
| `test_point_parser_co2_has_7_sealed_no_ambient` | 7 CO2 sealed, 0 ambient |
| `test_all_points_have_pressure_mode_field` | Every parsed point has `pressure_mode` in {ambient_open, sealed_controlled} |

## 9. Decision
**A4_P6_POINT_SCHEMA_ALIGNMENT_PASS**
