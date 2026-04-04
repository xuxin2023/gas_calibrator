from gas_calibrator.v2.ui_v2.runtime.crash_recovery import CrashRecovery
from gas_calibrator.v2.ui_v2.runtime.recovery_store import RecoveryStore


def test_crash_recovery_detects_pending_snapshot_and_builds_prompt(tmp_path) -> None:
    recovery = CrashRecovery(RecoveryStore(tmp_path / "runtime" / "crash_snapshot.json"))

    recovery.save_ui_snapshot(
        current_page="devices",
        ui_snapshot={
            "run": {
                "run_id": "run_123",
                "phase": "co2_route",
                "message": "sampling",
                "current_point": "#1",
                "route": "co2",
                "progress_pct": 50.0,
            }
        },
        logs=["line1", "line2"],
    )
    snapshot = recovery.load_pending_snapshot()
    prompt = recovery.build_prompt(snapshot)

    assert recovery.has_pending_recovery() is True
    assert snapshot["current_page"] == "devices"
    assert "run_123" in prompt
    assert "设备" in prompt
    assert "气路执行" in prompt

    recovery.clear()
    assert recovery.has_pending_recovery() is False
