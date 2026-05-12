# A4-P4：Dedicated 20℃ H2O+CO2 Simulation Points Draft + Profile Points Alignment

## 1. Scope
Dedicated A4 20℃ simulation points draft. Extracts only 20℃ H2O + CO2 points from `full_route_points_simulated.json`, renumbers from 1, and aligns the A4 profile `points_excel` to point to this dedicated file. No 10℃ points, no gas ambient point, no seven-pressure baseline.

## 2. New Points Path
```
src/gas_calibrator/v2/configs/validation/simulated/a4_20c_h2o_co2_points_simulated.json
```

## 3. Profile points_excel Switched
```
"points_excel": "./a4_20c_h2o_co2_points_simulated.json"
```
(was `./full_route_points_simulated.json`)

## 4. 20℃ Single-Temperature Confirmed
All 5 points have `temperature_c: 20.0`:
| index | route | pressure_hpa | humidity/ppm |
|-------|-------|-------------|--------------|
| 1 | h2o | 1000 | 40% |
| 2 | h2o | 800 | 70% |
| 3 | co2 | 1100 | 0 ppm |
| 4 | co2 | 1000 | 400 ppm |
| 5 | co2 | 600 | 1000 ppm |

## 5. H2O+CO2 Route Confirmed
Routes = {h2o, co2} only. No other routes present.

## 6. No New Gas Ambient Point
Gas ambient point NOT added to dedicated points. The gap between CO2 1100 hPa and the next lower pressure (as baseline 1100→500) is recorded but NOT addressed in this draft.

## 7. No Seven-Pressure Baseline
CO2 pressures = {1100, 1000, 600}. Three unique pressures, not the seven-pressure [500..1100] baseline.

## 8. Gas Ambient Point Gap Still Deferred
Profile `a4_notes.gas_ambient_point_gap` = `"recorded, NOT addressed in this profile; CO2 baseline [1100..500] intact"`. `NEED_USER_DECISION_ambient_point` preserved.

## 9. Not Modified
- Runtime: untouched
- V1: untouched
- A2 baseline config/points: untouched
- `full_route_points_simulated.json`: untouched
- 10℃ points: excluded (not deleted from source)
- `no_write_guard.py`, `analyzer_fleet_service.py`, `pressure_control_service.py`, `valve_routing_service.py`, `h2o_route_runner`, `co2_route_runner`: untouched

## 10. Test Results
```
tests/v2/test_a4_single_temp_profile.py ...............  19 passed
tests/v2/test_simulation_profile_no_write_marker.py ..   6 passed
tests/v2/test_no_write_guard.py .....................    19 passed
tests/v2/test_analyzer_fleet_service.py .............    12 passed
                                                         56 passed total
```

## 11. Decision
**A4_P4_DEDICATED_POINTS_READY**

The dedicated 20℃ points file is extracted and aligned. Profile points_excel correctly points to it. All A4 no-write/simulation-only/production=false boundaries intact. Gas ambient point gap recorded but deferred per A4 scope.
