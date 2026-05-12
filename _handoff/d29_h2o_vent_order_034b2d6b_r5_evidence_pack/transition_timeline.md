# Transition Timeline - D29-R5 Primary (085322)

All times UTC (CST = UTC+8h)

| # | Event | Time (UTC) | Pressure (hPa) | Result |
|---|---|---|---|---|
| 1 | ambient_sample_complete | 01:25:13.081 | ~1009.9 | ok |
| 2 | h2o_seal_transition_start | 01:25:13.082 | ~1009.9 | ok |
| 3 | h2o_vent_keepalive_stopped | 01:25:13.082 | ~1009.9 | ok |
| 4 | vent=OFF command sent | 01:25:24.163 | ~1009.9 | ok |
| 5 | vent_closed_verified | 01:25:25.345 | ~1009.9 | ok |
| 6 | post_vent_closed_wait_started | 01:25:25.346 | ~1009.9 | ok |
| 7 | post_vent_closed_wait_completed | 01:25:26.846 | ~1009.9 | ok |
| 8 | h2o_path_close_command_sent | 01:25:29.311 | ~1009.9 | ok |
| 9 | set_h2o_path(False) | 01:25:29.851 | ~1009.9 | ok |
| 10 | h2o_path_closed_after_vent_closed | 01:25:29.852 | ~1009.9 | ok |
| 11 | seal_transition gate | 01:25:29.852 | ~1009.9 | ok |
| 12 | seal_route | 01:25:35.715 | **1348.398** | ok |
| 13 | 1100 hPa in-limits | 01:25:58 | ~1100.0 | ok |

## Key intervals
| Interval | Duration |
|---|---|
| vent_closed_verified → wait_completed | 1.501s (~1.5s) |
| vent_closed_verified → h2o_path_close | 3.967s |
| wait_completed → h2o_path_close | 2.466s |

## Sealed phase
- vent=ON during sealed sweep: 0
- h2o-vent-keepalive during sealed sweep: 0
