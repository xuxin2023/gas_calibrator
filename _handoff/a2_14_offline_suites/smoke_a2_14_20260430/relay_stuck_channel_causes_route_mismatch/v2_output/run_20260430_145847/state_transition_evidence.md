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

- summary: routes 1 | nodes 20 | edges 60 | profile transition_policy_profile_v2
- water: TEMP_SOAK -> ROUTE_FLUSH -> PRESEAL_STABILITY -> SEAL -> PRESSURE_HANDOFF -> PRESSURE_STABLE -> RAW_SIGNAL_STABLE -> OUTPUT_STABLE -> SAMPLE_WINDOW -> POINT_COMPLETE -> RUN_COMPLETE

## Transition Trace

- #1: -- -> INIT | allowed True | point 0 | route system
- #2: INIT -> DEVICE_READY | allowed True | point 0 | route system
- #3: DEVICE_READY -> PLAN_COMPILED | allowed True | point 0 | route system
- #4: PLAN_COMPILED -> TEMP_SOAK | allowed True | point 0 | route water
- #5: TEMP_SOAK -> ROUTE_FLUSH | allowed True | point 0 | route water
- #6: ROUTE_FLUSH -> PRESEAL_STABILITY | allowed True | point 0 | route water
- #7: PRESEAL_STABILITY -> SEAL | allowed True | point 0 | route water
- #8: SEAL -> PRESSURE_HANDOFF | allowed True | point 0 | route water
- #9: PRESSURE_HANDOFF -> PRESSURE_STABLE | allowed True | point 0 | route water
- #10: PRESSURE_STABLE -> RAW_SIGNAL_STABLE | allowed True | point 0 | route water
- #11: RAW_SIGNAL_STABLE -> OUTPUT_STABLE | allowed True | point 0 | route water
- #12: OUTPUT_STABLE -> SAMPLE_WINDOW | allowed True | point 0 | route water
- #13: SAMPLE_WINDOW -> POINT_COMPLETE | allowed True | point 0 | route water
- #14: POINT_COMPLETE -> NEXT_POINT | allowed True | point 0 | route water
- #15: NEXT_POINT -> TEMP_SOAK | allowed True | point 0 | route water
- #16: TEMP_SOAK -> ROUTE_FLUSH | allowed True | point 0 | route water
- #17: ROUTE_FLUSH -> PRESEAL_STABILITY | allowed True | point 0 | route water
- #18: PRESEAL_STABILITY -> SEAL | allowed True | point 0 | route water
- #19: SEAL -> PRESSURE_HANDOFF | allowed True | point 0 | route water
- #20: PRESSURE_HANDOFF -> PRESSURE_STABLE | allowed True | point 0 | route water
- #21: PRESSURE_STABLE -> RAW_SIGNAL_STABLE | allowed True | point 0 | route water
- #22: RAW_SIGNAL_STABLE -> OUTPUT_STABLE | allowed True | point 0 | route water
- #23: OUTPUT_STABLE -> SAMPLE_WINDOW | allowed True | point 0 | route water
- #24: SAMPLE_WINDOW -> POINT_COMPLETE | allowed True | point 0 | route water
- #25: POINT_COMPLETE -> NEXT_POINT | allowed True | point 0 | route water
- #26: NEXT_POINT -> TEMP_SOAK | allowed True | point 0 | route water
- #27: TEMP_SOAK -> ROUTE_FLUSH | allowed True | point 0 | route water
- #28: ROUTE_FLUSH -> SEAL | allowed True | point 0 | route water
- #29: SEAL -> PRESSURE_HANDOFF | allowed True | point 0 | route water
- #30: PRESSURE_HANDOFF -> PRESSURE_STABLE | allowed True | point 0 | route water
- #31: PRESSURE_STABLE -> SAMPLE_WINDOW | allowed False | point 0 | route water
- #32: SAMPLE_WINDOW -> POINT_COMPLETE | allowed True | point 0 | route water
- #33: POINT_COMPLETE -> FAULT_CAPTURE | allowed True | point 0 | route water
- #34: FAULT_CAPTURE -> SAFE_RECOVERY | allowed True | point 0 | route water
- #35: SAFE_RECOVERY -> NEXT_POINT | allowed False | point 0 | route water
- #36: NEXT_POINT -> TEMP_SOAK | allowed True | point 0 | route water
- #37: TEMP_SOAK -> ROUTE_FLUSH | allowed True | point 0 | route water
- #38: ROUTE_FLUSH -> PRESEAL_STABILITY | allowed True | point 0 | route water
- #39: PRESEAL_STABILITY -> SEAL | allowed True | point 0 | route water
- #40: SEAL -> PRESSURE_HANDOFF | allowed True | point 0 | route water
- #41: PRESSURE_HANDOFF -> PRESSURE_STABLE | allowed True | point 0 | route water
- #42: PRESSURE_STABLE -> RAW_SIGNAL_STABLE | allowed True | point 0 | route water
- #43: RAW_SIGNAL_STABLE -> OUTPUT_STABLE | allowed True | point 0 | route water
- #44: OUTPUT_STABLE -> SAMPLE_WINDOW | allowed True | point 0 | route water
- #45: SAMPLE_WINDOW -> POINT_COMPLETE | allowed True | point 0 | route water
- #46: POINT_COMPLETE -> NEXT_POINT | allowed True | point 0 | route water
- #47: NEXT_POINT -> TEMP_SOAK | allowed True | point 0 | route water
- #48: TEMP_SOAK -> ROUTE_FLUSH | allowed True | point 0 | route water
- #49: ROUTE_FLUSH -> PRESEAL_STABILITY | allowed True | point 0 | route water
- #50: PRESEAL_STABILITY -> SEAL | allowed True | point 0 | route water
- #51: SEAL -> PRESSURE_HANDOFF | allowed True | point 0 | route water
- #52: PRESSURE_HANDOFF -> PRESSURE_STABLE | allowed True | point 0 | route water
- #53: PRESSURE_STABLE -> RAW_SIGNAL_STABLE | allowed True | point 0 | route water
- #54: RAW_SIGNAL_STABLE -> OUTPUT_STABLE | allowed True | point 0 | route water
- #55: OUTPUT_STABLE -> SAMPLE_WINDOW | allowed True | point 0 | route water
- #56: SAMPLE_WINDOW -> POINT_COMPLETE | allowed True | point 0 | route water
- #57: POINT_COMPLETE -> NEXT_POINT | allowed True | point 0 | route water
- #58: NEXT_POINT -> TEMP_SOAK | allowed True | point 0 | route water
- #59: TEMP_SOAK -> ROUTE_FLUSH | allowed True | point 0 | route water
- #60: ROUTE_FLUSH -> SEAL | allowed True | point 0 | route water
- #61: SEAL -> PRESSURE_HANDOFF | allowed True | point 0 | route water
- #62: PRESSURE_HANDOFF -> PRESSURE_STABLE | allowed True | point 0 | route water
- #63: PRESSURE_STABLE -> SAMPLE_WINDOW | allowed False | point 0 | route water
- #64: SAMPLE_WINDOW -> POINT_COMPLETE | allowed True | point 0 | route water
- #65: POINT_COMPLETE -> FAULT_CAPTURE | allowed True | point 0 | route water
- #66: FAULT_CAPTURE -> SAFE_RECOVERY | allowed True | point 0 | route water
- #67: SAFE_RECOVERY -> RUN_COMPLETE | allowed True | point 0 | route water

## Compiled vs Observed

- observed 20 | unexpected 2 | compiled_not_visited 42

## Artifact Paths

- json: D:\gas_calibrator\_handoff\a2_14_offline_suites\smoke_a2_14_20260430\relay_stuck_channel_causes_route_mismatch\v2_output\run_20260430_145847\state_transition_evidence.json
- markdown: D:\gas_calibrator\_handoff\a2_14_offline_suites\smoke_a2_14_20260430\relay_stuck_channel_causes_route_mismatch\v2_output\run_20260430_145847\state_transition_evidence.md
