from __future__ import annotations

from gas_calibrator.v2.core.run_state import RunState


def test_run_state_starts_with_empty_segmented_runtime_state() -> None:
    state = RunState()

    assert state.artifacts.output_files == []
    assert state.qc.cleaned_point_samples == {}
    assert state.qc.point_qc_inputs == []
    assert state.qc.point_validations == []
    assert state.qc.run_quality_score is None
    assert state.qc.qc_report is None
    assert state.analyzers.disabled == set()
    assert state.analyzers.disabled_reasons == {}
    assert state.analyzers.disabled_last_reprobe_ts == {}
    assert state.analyzers.last_live_snapshot_ts == 0.0
    assert state.humidity.preseal_dewpoint_snapshot is None
    assert state.humidity.h2o_pressure_prepared_target is None
    assert state.humidity.post_h2o_co2_zero_flush_pending is False
    assert state.humidity.initial_co2_zero_flush_pending is False
    assert state.humidity.first_co2_route_soak_pending is True
    assert state.humidity.last_hgen_target == (None, None)
    assert state.temperature.snapshot_keys == set()
    assert state.temperature.snapshots == []
    assert state.temperature.ready_target_c is None
    assert state.timing.point_contexts == {}


def test_run_state_reset_clears_mutable_state_and_restores_defaults() -> None:
    state = RunState()
    state.artifacts.output_files.append("summary.json")
    state.qc.cleaned_point_samples[1] = []
    state.qc.point_qc_inputs.append(("point", []))
    state.qc.point_validations.append("validation")
    state.qc.run_quality_score = 0.8
    state.qc.qc_report = {"status": "ok"}
    state.analyzers.disabled.add("GA01")
    state.analyzers.disabled_reasons["GA01"] = "fault"
    state.analyzers.disabled_last_reprobe_ts["GA01"] = 1.0
    state.analyzers.last_live_snapshot_ts = 12.0
    state.humidity.preseal_dewpoint_snapshot = {"dewpoint_c": 4.0}
    state.humidity.h2o_pressure_prepared_target = 800.0
    state.humidity.post_h2o_co2_zero_flush_pending = True
    state.humidity.initial_co2_zero_flush_pending = False
    state.humidity.first_co2_route_soak_pending = False
    state.humidity.active_post_h2o_co2_zero_flush = True
    state.humidity.last_hgen_target = (10.0, 50.0)
    state.humidity.last_hgen_setpoint_ready = True
    state.temperature.snapshot_keys.add((25.0, "co2"))
    state.temperature.snapshots.append({"point_index": 1})
    state.temperature.ready_target_c = 25.0
    state.temperature.last_wait_result = object()
    state.timing.point_contexts["co2:1"] = {"started_at": 1.0}

    state.reset(initial_co2_zero_flush_pending=True)

    assert state.artifacts.output_files == []
    assert state.qc.cleaned_point_samples == {}
    assert state.qc.point_qc_inputs == []
    assert state.qc.point_validations == []
    assert state.qc.run_quality_score is None
    assert state.qc.qc_report is None
    assert state.analyzers.disabled == set()
    assert state.analyzers.disabled_reasons == {}
    assert state.analyzers.disabled_last_reprobe_ts == {}
    assert state.analyzers.last_live_snapshot_ts == 0.0
    assert state.humidity.preseal_dewpoint_snapshot is None
    assert state.humidity.h2o_pressure_prepared_target is None
    assert state.humidity.post_h2o_co2_zero_flush_pending is False
    assert state.humidity.initial_co2_zero_flush_pending is True
    assert state.humidity.first_co2_route_soak_pending is True
    assert state.humidity.active_post_h2o_co2_zero_flush is False
    assert state.humidity.last_hgen_target == (None, None)
    assert state.humidity.last_hgen_setpoint_ready is False
    assert state.temperature.snapshot_keys == set()
    assert state.temperature.snapshots == []
    assert state.temperature.ready_target_c is None
    assert state.temperature.last_wait_result is None
    assert state.timing.point_contexts == {}
