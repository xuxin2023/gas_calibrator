from __future__ import annotations

import json
from pathlib import Path


def test_replacement_skip0_co2_only_simulated_has_explicit_no_write_marker() -> None:
    """B1-R1 simulation profile marker test, NOT A2 seven-pressure baseline."""
    profile_path = (
        Path(__file__).resolve().parents[2]
        / "src"
        / "gas_calibrator"
        / "v2"
        / "configs"
        / "validation"
        / "simulated"
        / "replacement_skip0_co2_only_simulated.json"
    )
    payload = json.loads(profile_path.read_text(encoding="utf-8"))

    assert payload["features"]["simulation_mode"] is True
    assert payload["workflow"]["collect_only"] is True
    assert payload["workflow"]["no_write_guard_active"] is True
    assert payload["paths"]["points_excel"] == "./skip0_co2_only_points_simulated.json"

    fname = profile_path.name.lower()
    assert "seven_pressure" not in fname
    assert "seven-pressure" not in fname
    assert "7_pressure" not in fname
