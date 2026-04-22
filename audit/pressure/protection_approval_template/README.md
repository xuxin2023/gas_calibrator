This template is not approval and must not allow retry.
It exists so CO2 A valve 4 staged dry-run can use a machine-readable same-source approval file after explicit approval is granted.
It is limited to `CO2_A_VALVE_4_STAGED_DRY_RUN_ONLY`.
It does not open valves.
It does not run live hardware.
It does not release `SealPressureStageNotVerified`.
It does not update `_route_final_stage_seal_safety[key]`.
It does not modify V1 route.
It does not modify PACE/VENT semantics.
It does not send `VENT 2`.
Use this template to create a real approval JSON only after same-source analyzer or mechanical pressure protection has been explicitly approved.