# State Transition Evidence

- title: State Transition Evidence
- role: diagnostic_analysis
- reviewer_note: Controlled-flex state machine trace with fixed canonical states and allowed transitions only. This is reviewer evidence and not a runtime control surface.

## Boundary

- Step 2 tail / Stage 3 bridge
- simulation / offline / headless only
- not real acceptance
- cannot replace real metrology validation
- shadow evaluation only
- does not modify live sampling gate by default

## Transition Trace

- #1: -- -> INIT | allowed True | point 0 | route system
- #2: INIT -> DEVICE_READY | allowed True | point 0 | route system
- #3: DEVICE_READY -> PLAN_COMPILED | allowed True | point 0 | route system
- #4: PLAN_COMPILED -> TEMP_SOAK | allowed True | point 2 | route gas
- #5: TEMP_SOAK -> ROUTE_FLUSH | allowed True | point 2 | route gas
- #6: ROUTE_FLUSH -> SEAL | allowed True | point 2 | route gas
- #7: SEAL -> PRESSURE_HANDOFF | allowed True | point 2 | route gas
- #8: PRESSURE_HANDOFF -> PRESSURE_STABLE | allowed True | point 2 | route gas
- #9: PRESSURE_STABLE -> RAW_SIGNAL_STABLE | allowed True | point 2 | route gas
- #10: RAW_SIGNAL_STABLE -> OUTPUT_STABLE | allowed True | point 2 | route gas
- #11: OUTPUT_STABLE -> SAMPLE_WINDOW | allowed True | point 2 | route gas
- #12: SAMPLE_WINDOW -> POINT_COMPLETE | allowed True | point 2 | route gas
- #13: POINT_COMPLETE -> TEMP_SOAK | allowed False | point 2 | route gas
- #14: TEMP_SOAK -> ROUTE_FLUSH | allowed True | point 2 | route gas
- #15: ROUTE_FLUSH -> SEAL | allowed True | point 2 | route gas
- #16: SEAL -> PRESSURE_HANDOFF | allowed True | point 2 | route gas
- #17: PRESSURE_HANDOFF -> PRESSURE_STABLE | allowed True | point 2 | route gas
- #18: PRESSURE_STABLE -> SAMPLE_WINDOW | allowed False | point 2 | route gas
- #19: SAMPLE_WINDOW -> POINT_COMPLETE | allowed True | point 2 | route gas
- #20: POINT_COMPLETE -> TEMP_SOAK | allowed False | point 2 | route gas
- #21: TEMP_SOAK -> ROUTE_FLUSH | allowed True | point 2 | route gas
- #22: ROUTE_FLUSH -> SEAL | allowed True | point 2 | route gas
- #23: SEAL -> PRESSURE_HANDOFF | allowed True | point 2 | route gas
- #24: PRESSURE_HANDOFF -> PRESSURE_STABLE | allowed True | point 2 | route gas
- #25: PRESSURE_STABLE -> SAMPLE_WINDOW | allowed False | point 2 | route gas
- #26: SAMPLE_WINDOW -> POINT_COMPLETE | allowed True | point 2 | route gas
- #27: POINT_COMPLETE -> FAULT_CAPTURE | allowed True | point 2 | route gas
- #28: FAULT_CAPTURE -> SAFE_RECOVERY | allowed True | point 2 | route gas
- #29: SAFE_RECOVERY -> RUN_COMPLETE | allowed True | point 2 | route gas

## Artifact Paths

- json: d:\gas_calibrator\_debug_test\run_20260412_185117\state_transition_evidence.json
- markdown: d:\gas_calibrator\_debug_test\run_20260412_185117\state_transition_evidence.md
