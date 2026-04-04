from pathlib import Path

from gas_calibrator.v2.ui_v2.utils.route_memory import RouteMemory


def test_route_memory_saves_and_loads_last_page(tmp_path: Path) -> None:
    memory = RouteMemory(tmp_path / "route_memory.json")

    saved = memory.save("reports")
    loaded = memory.load()

    assert saved == "reports"
    assert loaded == "reports"
