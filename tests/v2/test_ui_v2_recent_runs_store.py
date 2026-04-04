from pathlib import Path

from gas_calibrator.v2.ui_v2.utils.recent_runs_store import RecentRunsStore


def test_recent_runs_store_deduplicates_and_limits_rows(tmp_path: Path) -> None:
    store = RecentRunsStore(tmp_path / "recent_runs.json", limit=2)

    store.add("run_a")
    store.add("run_b")
    rows = store.add("run_a")

    assert len(rows) == 2
    assert rows[0]["path"] == "run_a"
    assert rows[1]["path"] == "run_b"
    assert "opened_at" in rows[0]
