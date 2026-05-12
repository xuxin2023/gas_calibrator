# Vent/Valve Timeline - D29-R5 Primary (085322)

| Step | Event | Time (UTC) |
|---|---|---|
| K1 | h2o_vent_keepalive_stopped | 01:25:13.082 |
| V1 | vent=OFF command | 01:25:24.163 |
| V2 | vent_closed_verified | 01:25:25.345 |
| W1 | post_vent_closed_wait_started | 01:25:25.346 |
| W2 | post_vent_closed_wait_completed (~1.5s) | 01:25:26.846 |
| C1 | h2o_path_close_command_sent | 01:25:29.311 |
| C2 | set_h2o_path(False) | 01:25:29.851 |
| C3 | h2o_path_closed_after_vent_closed | 01:25:29.852 |
| S1 | seal_transition gate | 01:25:29.852 |
| S2 | seal_route (pressure=1348.4 hPa) | 01:25:35.715 |

## Order verdict
- vent=OFF before set_h2o_path(False): YES
- vent_closed_verified before set_h2o_path(False): YES
- 1.5s wait completed before h2o_path close: YES
- Duplicate vent=OFF: NO
- Duplicate set_h2o_path(False): NO
