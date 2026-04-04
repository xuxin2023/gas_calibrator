from pathlib import Path

from gas_calibrator.v2.ui_v2.runtime.recovery_store import RecoveryStore


def test_recovery_store_round_trips_snapshot_and_clears(tmp_path: Path) -> None:
    store = RecoveryStore(tmp_path / "runtime" / "crash_snapshot.json")

    payload = store.save({"run_id": "run_001", "phase": "co2_route", "current_page": "reports"})

    assert store.exists() is True
    assert payload["run_id"] == "run_001"
    assert store.load()["current_page"] == "reports"

    store.clear()

    assert store.exists() is False
