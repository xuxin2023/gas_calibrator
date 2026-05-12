# D29-R5 H2O Repeatability Evidence (Corrected)

## 1. Correction Notice
The handoff wrapper previously misclassified `run_20260512_091713` (fail-closed) as the primary D29-R5 result. This report corrects that:

- **D29-R5 primary run**: `run_20260512_085322` (started 08:53 CST)
- **D29-R5 duplicate-start fail-closed**: `run_20260512_091713` (started 09:17 CST, COM ports occupied by 085322)

---

## 2. 阶段定位
V2 水路重构阶段 D / D29-R5，按原计划连续推进。

## 3. 当前 commit
`034b2d6bf3321ebc40b078b6c8bd9f97eb6ab2ef`

## 4. R5 目标
复现 D29-R4 H2O ambient_open → sealed pressure sweep 的 vent/valve 顺序。

## 5. no-flow 说明
- 当前 real-machine config 无 `workflow.humidity_generator.flow_lpm`
- `humidity_generator_service.py` 仍保留兼容逻辑，但当前 config 不触发 `set_flow_target`
- `run.log` 确认: `flow=NoneL/min`
- 本轮不控制湿度发生器流量

---

## 6. Primary Run (085322) Transition Timeline

所有时间 UTC (CST = UTC+8h)

| # | Event | Time (UTC) | Result |
|---|---|---|---|
| 1 | ambient_sample_complete | 01:25:13.081 | ok |
| 2 | h2o_seal_transition_start | 01:25:13.082 | ok |
| 3 | h2o_vent_keepalive_stopped | 01:25:13.082 | ok |
| 4 | vent=OFF command | 01:25:24.163 | ok |
| 5 | vent_closed_verified | 01:25:25.345 | ok |
| 6 | post_vent_closed_wait_started | 01:25:25.346 | ok |
| 7 | post_vent_closed_wait_completed | 01:25:26.846 (~1.5s) | ok |
| 8 | h2o_path_close_command_sent | 01:25:29.311 | ok |
| 9 | set_h2o_path(False) | 01:25:29.851 | ok |
| 10 | h2o_path_closed_after_vent_closed | 01:25:29.852 | ok |
| 11 | seal_transition gate | 01:25:29.852 | ok |
| 12 | seal_route (1348.4 hPa) | 01:25:35.715 | ok |
| 13 | 1100 hPa in-limits | 01:25:58 | ok |

---

## 7. Vent/Valve Order

| Check | Result |
|---|---|
| vent=OFF before set_h2o_path(False)? | **YES** |
| vent_closed_verified before set_h2o_path(False)? | **YES** |
| 1.5s wait completed before h2o_path close? | **YES** (1.501s) |
| Duplicate vent=OFF? | **NO** |
| Duplicate set_h2o_path(False)? | **NO** |

---

## 8. Pressure Behavior

| Phase | Pressure (hPa) |
|---|---|
| Ambient | ~1009.9 |
| vent=OFF | ~1009.9 |
| Post-seal peak | **1348.398** |
| 1100 hPa control | ~1100.0 |

---

## 9. Seven Pressure Points (085322)

| # | Target (hPa) | Actual (hPa) | Status |
|---|---|---|---|
| ambient | atmospheric | 1009.926 | ok |
| 1 | 1100 | 1098.58 | ok |
| 2 | 1000 | 999.502 | ok |
| 3 | 900 | 899.6 | ok |
| 4 | 800 | 799.695 | ok |
| 5 | 700 | 699.798 | ok |
| 6 | 600 | 599.887 | ok |
| 7 | 500 | 500.051 | ok |

**All 7 points completed**

---

## 10. Sealed Phase Prohibited Items

| Check | Count |
|---|---|
| vent=ON during sealed sweep | **0** |
| h2o-vent-keepalive during sealed sweep | **0** |
| Any atmosphere-open action during pressure control | **0** |

---

## 11. No-Write (085322)

| Check | Value |
|---|---|
| attempted_write_count | **0** |
| identity_write_command_sent | **false** |
| persistent_write_command_sent | **false** |
| blocked_write_events | **[]** |
| ID write | none |
| SENCO write | none |
| Zero write | none |
| Span write | none |
| Coefficient write | none |

---

## 12. Duplicate-Start Run (091713) - Appendix

### Summary
`run_20260512_091713` was started while `run_20260512_085322` was still holding all COM ports. All devices failed initialization because ports were occupied.

### Evidence
```
Calibration failed: Critical device initialization failed
serial not open (all devices)
Modbus Error: Failed to connect[ModbusSerialClient COM20:0]
Pressure controller vent command failed: serial not open
```

### Classification
- **duplicate-start COM port contention**
- NOT physical-process failure
- NOT vent/valve regression
- NOT humidity generator issue
- NOT reason to modify runtime

---

## 13. Final Decision

**D29-R5 PASS**

- 085322 复现了 D29-R4 的 vent/valve 顺序
- 七压力点全部完成
- no-write 通过
- no-flow 通过
- sealed 阶段无违反项
- 091713 是重复启动导致的 fail-closed，与物理流程无关

---

## 14. Declarations

- NOT real acceptance
- NOT production acceptance
- NOT V2 replacement
- NOT formal switch
- V1 fallback retained
- engineering_probe_only
- promotion_state = blocked
- not_real_acceptance_evidence = true
