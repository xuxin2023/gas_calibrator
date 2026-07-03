# V1.5 Historical Replay Evidence

- schema: `v1_5_historical_replay_evidence_v1`
- status: `blocked`
- blocker_count: `1`
- review_required_count: `1`
- profile_path: `D:\gas_calibrator\_worktrees\v1_5_fixed_wait_window_gate_1aee26d_clean\configs\v1_5_algorithm_route_profiles.json`

## Physical Boundaries

- opens_com_ports: `False`
- connects_postgresql: `False`
- controls_pressure: `False`
- controls_water_or_gas_routes: `False`
- writes_coefficients: `False`
- writes_device_id: `False`
- formal_release_allowed: `False`
- database_import_allowed: `False`
- not_real_acceptance_evidence: `True`

## Route Summaries

| Family | Route | Status | Observed | Expected | Matched | Missing | Unexpected | Rejected rows |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| `mature_0620_legacy_ratio` | `co2` | `review_required` | 45 | 45 | 45 | 0 | 0 | 164 |
| `new_algorithm_shadow_run` | `co2` | `review_required` | 39 | 47 | 39 | 8 | 0 | 0 |
| `new_algorithm_shadow_run` | `h2o` | `review_required` | 13 | 14 | 13 | 1 | 0 | 0 |

## Checks

| Check | Status | Reason | Physical meaning |
|---|---|---|---|
| `evidence_roots_exist` | `pass` | every requested evidence root must be present before replay binding can be trusted | Replay binding must point at real historical evidence directories, not guessed locations. |
| `point_directories_discovered` | `pass` | each route root must contain parseable pNNN point directories | The replay binder operates on point-level physical evidence, not only top-level notes. |
| `point_sequence_matches_profile_or_requires_review` | `review_required` | missing or unexpected points are preserved as review gaps instead of being silently filled | This is where split runs and retry segments are surfaced for human review before fitting. |
| `quality_evidence_present` | `blocker` | replay binding must see QC/quality evidence for every discovered point | A point without quality evidence cannot be safely promoted into fit input review. |
| `rejected_rows_preserved` | `pass` | the binder records rejected rows and exclusion reasons without changing eligibility | Historical replay must keep C/B/rejected analyzer rows visible instead of washing them into fit-ready data. |
| `fit_input_profile_bound` | `pass` | legacy and new-algorithm fit inputs are read from the profile contract | This prevents a historical directory name from switching legacy data into absorption A or vice versa. |
| `replay_release_blocked` | `pass` | this binder is no-write and cannot authorize archive release or PostgreSQL import | A historical replay can validate interpretation logic, not today's production release state. |
