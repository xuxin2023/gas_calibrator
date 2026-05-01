# State Transition Evidence

- title: State Transition Evidence
- role: diagnostic_analysis
- reviewer_note: Controlled state machine trace, fixed canonical states, transition policy profile, and compiled route graph for Step 2 offline replay only.

## Boundary

- Step 2 tail / Stage 3 bridge
- simulation / offline / headless only
- not real acceptance
- cannot replace real metrology validation
- shadow evaluation only
- does not modify live sampling gate by default

## Transition Policy Profile

- policy_version: transition_policy_profile_v2
- feature_set_version: controlled_state_machine.step2_offline_v2
- summary: retry 5 | routes 1 | diagnostic 3

## Compiled Route Graph

- summary: routes 1 | nodes 19 | edges 56 | profile transition_policy_profile_v2
- gas: TEMP_SOAK -> ROUTE_FLUSH -> SEAL -> PRESSURE_HANDOFF -> PRESSURE_STABLE -> RAW_SIGNAL_STABLE -> OUTPUT_STABLE -> SAMPLE_WINDOW -> POINT_COMPLETE -> RUN_COMPLETE

## Transition Trace

- #1: -- -> INIT | allowed True | point 0 | route system
- #2: INIT -> DEVICE_READY | allowed True | point 0 | route system
- #3: DEVICE_READY -> PLAN_COMPILED | allowed True | point 0 | route system
- #4: PLAN_COMPILED -> TEMP_SOAK | allowed True | point 0 | route gas
- #5: TEMP_SOAK -> ROUTE_FLUSH | allowed True | point 0 | route gas
- #6: ROUTE_FLUSH -> SEAL | allowed True | point 0 | route gas
- #7: SEAL -> PRESSURE_HANDOFF | allowed True | point 0 | route gas
- #8: PRESSURE_HANDOFF -> PRESSURE_STABLE | allowed True | point 0 | route gas
- #9: PRESSURE_STABLE -> RAW_SIGNAL_STABLE | allowed True | point 0 | route gas
- #10: RAW_SIGNAL_STABLE -> OUTPUT_STABLE | allowed True | point 0 | route gas
- #11: OUTPUT_STABLE -> SAMPLE_WINDOW | allowed True | point 0 | route gas
- #12: SAMPLE_WINDOW -> POINT_COMPLETE | allowed True | point 0 | route gas
- #13: POINT_COMPLETE -> NEXT_POINT | allowed True | point 0 | route gas
- #14: NEXT_POINT -> TEMP_SOAK | allowed True | point 0 | route gas
- #15: TEMP_SOAK -> ROUTE_FLUSH | allowed True | point 0 | route gas
- #16: ROUTE_FLUSH -> SEAL | allowed True | point 0 | route gas
- #17: SEAL -> PRESSURE_HANDOFF | allowed True | point 0 | route gas
- #18: PRESSURE_HANDOFF -> PRESSURE_STABLE | allowed True | point 0 | route gas
- #19: PRESSURE_STABLE -> RAW_SIGNAL_STABLE | allowed True | point 0 | route gas
- #20: RAW_SIGNAL_STABLE -> OUTPUT_STABLE | allowed True | point 0 | route gas
- #21: OUTPUT_STABLE -> SAMPLE_WINDOW | allowed True | point 0 | route gas
- #22: SAMPLE_WINDOW -> POINT_COMPLETE | allowed True | point 0 | route gas
- #23: POINT_COMPLETE -> NEXT_POINT | allowed True | point 0 | route gas
- #24: NEXT_POINT -> TEMP_SOAK | allowed True | point 0 | route gas
- #25: TEMP_SOAK -> ROUTE_FLUSH | allowed True | point 0 | route gas
- #26: ROUTE_FLUSH -> SEAL | allowed True | point 0 | route gas
- #27: SEAL -> PRESSURE_HANDOFF | allowed True | point 0 | route gas
- #28: PRESSURE_HANDOFF -> PRESSURE_STABLE | allowed True | point 0 | route gas
- #29: PRESSURE_STABLE -> SAMPLE_WINDOW | allowed False | point 0 | route gas
- #30: SAMPLE_WINDOW -> POINT_COMPLETE | allowed True | point 0 | route gas
- #31: POINT_COMPLETE -> FAULT_CAPTURE | allowed True | point 0 | route gas
- #32: FAULT_CAPTURE -> SAFE_RECOVERY | allowed True | point 0 | route gas
- #33: SAFE_RECOVERY -> NEXT_POINT | allowed False | point 0 | route gas
- #34: NEXT_POINT -> TEMP_SOAK | allowed True | point 0 | route gas
- #35: TEMP_SOAK -> ROUTE_FLUSH | allowed True | point 0 | route gas
- #36: ROUTE_FLUSH -> SEAL | allowed True | point 0 | route gas
- #37: SEAL -> PRESSURE_HANDOFF | allowed True | point 0 | route gas
- #38: PRESSURE_HANDOFF -> PRESSURE_STABLE | allowed True | point 0 | route gas
- #39: PRESSURE_STABLE -> RAW_SIGNAL_STABLE | allowed True | point 0 | route gas
- #40: RAW_SIGNAL_STABLE -> OUTPUT_STABLE | allowed True | point 0 | route gas
- #41: OUTPUT_STABLE -> SAMPLE_WINDOW | allowed True | point 0 | route gas
- #42: SAMPLE_WINDOW -> POINT_COMPLETE | allowed True | point 0 | route gas
- #43: POINT_COMPLETE -> NEXT_POINT | allowed True | point 0 | route gas
- #44: NEXT_POINT -> TEMP_SOAK | allowed True | point 0 | route gas
- #45: TEMP_SOAK -> ROUTE_FLUSH | allowed True | point 0 | route gas
- #46: ROUTE_FLUSH -> SEAL | allowed True | point 0 | route gas
- #47: SEAL -> PRESSURE_HANDOFF | allowed True | point 0 | route gas
- #48: PRESSURE_HANDOFF -> PRESSURE_STABLE | allowed True | point 0 | route gas
- #49: PRESSURE_STABLE -> RAW_SIGNAL_STABLE | allowed True | point 0 | route gas
- #50: RAW_SIGNAL_STABLE -> OUTPUT_STABLE | allowed True | point 0 | route gas
- #51: OUTPUT_STABLE -> SAMPLE_WINDOW | allowed True | point 0 | route gas
- #52: SAMPLE_WINDOW -> POINT_COMPLETE | allowed True | point 0 | route gas
- #53: POINT_COMPLETE -> NEXT_POINT | allowed True | point 0 | route gas
- #54: NEXT_POINT -> TEMP_SOAK | allowed True | point 0 | route gas
- #55: TEMP_SOAK -> ROUTE_FLUSH | allowed True | point 0 | route gas
- #56: ROUTE_FLUSH -> SEAL | allowed True | point 0 | route gas
- #57: SEAL -> PRESSURE_HANDOFF | allowed True | point 0 | route gas
- #58: PRESSURE_HANDOFF -> PRESSURE_STABLE | allowed True | point 0 | route gas
- #59: PRESSURE_STABLE -> SAMPLE_WINDOW | allowed False | point 0 | route gas
- #60: SAMPLE_WINDOW -> POINT_COMPLETE | allowed True | point 0 | route gas
- #61: POINT_COMPLETE -> FAULT_CAPTURE | allowed True | point 0 | route gas
- #62: FAULT_CAPTURE -> SAFE_RECOVERY | allowed True | point 0 | route gas
- #63: SAFE_RECOVERY -> NEXT_POINT | allowed False | point 0 | route gas
- #64: NEXT_POINT -> TEMP_SOAK | allowed True | point 0 | route gas
- #65: TEMP_SOAK -> ROUTE_FLUSH | allowed True | point 0 | route gas
- #66: ROUTE_FLUSH -> SEAL | allowed True | point 0 | route gas
- #67: SEAL -> PRESSURE_HANDOFF | allowed True | point 0 | route gas
- #68: PRESSURE_HANDOFF -> PRESSURE_STABLE | allowed True | point 0 | route gas
- #69: PRESSURE_STABLE -> RAW_SIGNAL_STABLE | allowed True | point 0 | route gas
- #70: RAW_SIGNAL_STABLE -> OUTPUT_STABLE | allowed True | point 0 | route gas
- #71: OUTPUT_STABLE -> SAMPLE_WINDOW | allowed True | point 0 | route gas
- #72: SAMPLE_WINDOW -> POINT_COMPLETE | allowed True | point 0 | route gas
- #73: POINT_COMPLETE -> NEXT_POINT | allowed True | point 0 | route gas
- #74: NEXT_POINT -> TEMP_SOAK | allowed True | point 0 | route gas
- #75: TEMP_SOAK -> ROUTE_FLUSH | allowed True | point 0 | route gas
- #76: ROUTE_FLUSH -> SEAL | allowed True | point 0 | route gas
- #77: SEAL -> PRESSURE_HANDOFF | allowed True | point 0 | route gas
- #78: PRESSURE_HANDOFF -> PRESSURE_STABLE | allowed True | point 0 | route gas
- #79: PRESSURE_STABLE -> RAW_SIGNAL_STABLE | allowed True | point 0 | route gas
- #80: RAW_SIGNAL_STABLE -> OUTPUT_STABLE | allowed True | point 0 | route gas
- #81: OUTPUT_STABLE -> SAMPLE_WINDOW | allowed True | point 0 | route gas
- #82: SAMPLE_WINDOW -> POINT_COMPLETE | allowed True | point 0 | route gas
- #83: POINT_COMPLETE -> NEXT_POINT | allowed True | point 0 | route gas
- #84: NEXT_POINT -> TEMP_SOAK | allowed True | point 0 | route gas
- #85: TEMP_SOAK -> ROUTE_FLUSH | allowed True | point 0 | route gas
- #86: ROUTE_FLUSH -> SEAL | allowed True | point 0 | route gas
- #87: SEAL -> PRESSURE_HANDOFF | allowed True | point 0 | route gas
- #88: PRESSURE_HANDOFF -> PRESSURE_STABLE | allowed True | point 0 | route gas
- #89: PRESSURE_STABLE -> SAMPLE_WINDOW | allowed False | point 0 | route gas
- #90: SAMPLE_WINDOW -> POINT_COMPLETE | allowed True | point 0 | route gas
- #91: POINT_COMPLETE -> FAULT_CAPTURE | allowed True | point 0 | route gas
- #92: FAULT_CAPTURE -> SAFE_RECOVERY | allowed True | point 0 | route gas
- #93: SAFE_RECOVERY -> NEXT_POINT | allowed False | point 0 | route gas
- #94: NEXT_POINT -> TEMP_SOAK | allowed True | point 0 | route gas
- #95: TEMP_SOAK -> ROUTE_FLUSH | allowed True | point 0 | route gas
- #96: ROUTE_FLUSH -> SEAL | allowed True | point 0 | route gas
- #97: SEAL -> PRESSURE_HANDOFF | allowed True | point 0 | route gas
- #98: PRESSURE_HANDOFF -> PRESSURE_STABLE | allowed True | point 0 | route gas
- #99: PRESSURE_STABLE -> SAMPLE_WINDOW | allowed False | point 0 | route gas
- #100: SAMPLE_WINDOW -> POINT_COMPLETE | allowed True | point 0 | route gas
- #101: POINT_COMPLETE -> NEXT_POINT | allowed True | point 0 | route gas
- #102: NEXT_POINT -> TEMP_SOAK | allowed True | point 0 | route gas
- #103: TEMP_SOAK -> ROUTE_FLUSH | allowed True | point 0 | route gas
- #104: ROUTE_FLUSH -> SEAL | allowed True | point 0 | route gas
- #105: SEAL -> PRESSURE_HANDOFF | allowed True | point 0 | route gas
- #106: PRESSURE_HANDOFF -> PRESSURE_STABLE | allowed True | point 0 | route gas
- #107: PRESSURE_STABLE -> RAW_SIGNAL_STABLE | allowed True | point 0 | route gas
- #108: RAW_SIGNAL_STABLE -> OUTPUT_STABLE | allowed True | point 0 | route gas
- #109: OUTPUT_STABLE -> SAMPLE_WINDOW | allowed True | point 0 | route gas
- #110: SAMPLE_WINDOW -> POINT_COMPLETE | allowed True | point 0 | route gas
- #111: POINT_COMPLETE -> NEXT_POINT | allowed True | point 0 | route gas
- #112: NEXT_POINT -> TEMP_SOAK | allowed True | point 0 | route gas
- #113: TEMP_SOAK -> ROUTE_FLUSH | allowed True | point 0 | route gas
- #114: ROUTE_FLUSH -> SEAL | allowed True | point 0 | route gas
- #115: SEAL -> PRESSURE_HANDOFF | allowed True | point 0 | route gas
- #116: PRESSURE_HANDOFF -> PRESSURE_STABLE | allowed True | point 0 | route gas
- #117: PRESSURE_STABLE -> RAW_SIGNAL_STABLE | allowed True | point 0 | route gas
- #118: RAW_SIGNAL_STABLE -> OUTPUT_STABLE | allowed True | point 0 | route gas
- #119: OUTPUT_STABLE -> SAMPLE_WINDOW | allowed True | point 0 | route gas
- #120: SAMPLE_WINDOW -> POINT_COMPLETE | allowed True | point 0 | route gas
- #121: POINT_COMPLETE -> NEXT_POINT | allowed True | point 0 | route gas
- #122: NEXT_POINT -> TEMP_SOAK | allowed True | point 0 | route gas
- #123: TEMP_SOAK -> ROUTE_FLUSH | allowed True | point 0 | route gas
- #124: ROUTE_FLUSH -> SEAL | allowed True | point 0 | route gas
- #125: SEAL -> PRESSURE_HANDOFF | allowed True | point 0 | route gas
- #126: PRESSURE_HANDOFF -> PRESSURE_STABLE | allowed True | point 0 | route gas
- #127: PRESSURE_STABLE -> SAMPLE_WINDOW | allowed False | point 0 | route gas
- #128: SAMPLE_WINDOW -> POINT_COMPLETE | allowed True | point 0 | route gas
- #129: POINT_COMPLETE -> FAULT_CAPTURE | allowed True | point 0 | route gas
- #130: FAULT_CAPTURE -> SAFE_RECOVERY | allowed True | point 0 | route gas
- #131: SAFE_RECOVERY -> RUN_COMPLETE | allowed True | point 0 | route gas

## Compiled vs Observed

- observed 18 | unexpected 2 | compiled_not_visited 40

## Artifact Paths

- json: D:\gas_calibrator\_handoff\a2_17_suite_reports\a2_17_smoke\pressure_reference_degraded\v2_output\run_20260430_233136\state_transition_evidence.json
- markdown: D:\gas_calibrator\_handoff\a2_17_suite_reports\a2_17_smoke\pressure_reference_degraded\v2_output\run_20260430_233136\state_transition_evidence.md
