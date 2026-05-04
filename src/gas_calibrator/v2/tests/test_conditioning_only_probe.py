from __future__ import annotations

import csv
import json
from pathlib import Path

from gas_calibrator.v2.core.run001_conditioning_only_probe import (
    CONDITIONING_ONLY_FINAL_DECISION,
    write_conditioning_only_probe_artifacts,
)


def test_conditioning_only_probe_writes_no_com_no_write_artifacts(tmp_path: Path) -> None:
    config_path = (
        Path(__file__).resolve().parents[1]
        / "configs"
        / "validation"
        / "run001_a2_co2_only_7_pressure_no_write_real_machine.json"
    )
    run_dir = tmp_path / "conditioning_only"

    summary = write_conditioning_only_probe_artifacts(
        config_path,
        output_dir=run_dir,
        run_timestamp="20260427_170000",
    )

    artifact_paths = summary["artifact_paths"]
    evidence = json.loads(Path(artifact_paths["co2_route_conditioning_evidence"]).read_text(encoding="utf-8"))
    no_write = json.loads(Path(artifact_paths["no_write_guard"]).read_text(encoding="utf-8"))
    route_rows = [
        json.loads(line)
        for line in Path(artifact_paths["route_trace"]).read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    with Path(artifact_paths["pressure_read_latency_samples"]).open(encoding="utf-8", newline="") as handle:
        latency_rows = list(csv.DictReader(handle))

    assert summary["final_decision"] == CONDITIONING_ONLY_FINAL_DECISION
    assert summary["execution_mode"] == "simulated_no_com"
    assert summary["evidence_source"] == "simulated_no_com"
    assert summary["not_real_acceptance_evidence"] is True
    assert summary["not_a1_pass"] is True
    assert summary["not_a2_pass"] is True
    assert summary["not_v2_replacement"] is True
    assert summary["not_real_primary_latest"] is True
    assert summary["real_primary_latest_refreshed"] is False
    assert "not real acceptance" in summary["governance_statement"]
    assert "not A1 PASS" in summary["governance_statement"]
    assert "not A2 PASS" in summary["governance_statement"]
    assert "not V2 replacement" in summary["governance_statement"]
    assert "not real_primary_latest" in summary["governance_statement"]
    assert summary["real_probe_executed"] is False
    assert summary["real_com_opened"] is False
    assert summary["co2_only"] is True
    assert summary["skip0"] is True
    assert summary["h2o_enabled"] is False
    assert summary["full_group_enabled"] is False
    assert summary["refresh_real_primary_latest"] is False

    assert summary["vent_tick_max_gap_s"] <= 3.0
    assert summary["route_open_to_first_vent_s"] <= 1.0
    assert summary["digital_gauge_latest_age_s"] <= 3.0
    assert summary["digital_gauge_sequence_progress"] is True
    assert summary["pressure_overlimit_seen"] is False
    assert summary["conditioning_pressure_max_hpa"] < 1150.0
    assert summary["stream_stale_seen"] is False

    assert evidence["vent_command_before_route_open"] is True
    assert evidence["vent_command_after_route_open"] is True
    assert evidence["evidence_source"] == "simulated_no_com"
    assert evidence["not_real_acceptance_evidence"] is True
    assert evidence["not_a1_pass"] is True
    assert evidence["not_a2_pass"] is True
    assert evidence["not_v2_replacement"] is True
    assert evidence["not_real_primary_latest"] is True
    assert evidence["vent_tick_max_gap_s"] == summary["vent_tick_max_gap_s"]
    assert evidence["route_open_to_first_vent_s"] == summary["route_open_to_first_vent_s"]
    assert evidence["pressure_overlimit_seen"] == summary["pressure_overlimit_seen"]
    assert evidence["conditioning_pressure_max_hpa"] == summary["conditioning_pressure_max_hpa"]
    assert evidence["stream_stale_seen"] == summary["stream_stale_seen"]

    assert no_write["attempted_write_count"] == 0
    assert no_write["identity_write_command_sent"] is False
    assert no_write["calibration_write_command_sent"] is False
    assert no_write["senco_write_command_sent"] is False

    actions = [row["action"] for row in route_rows]
    assert "set_co2_valves" in actions
    assert not any(row["action"] == "set_vent" and row.get("target", {}).get("vent_on") is False for row in route_rows)
    assert "seal_route" not in actions
    assert "high_pressure_first_point_mode_enabled" not in actions
    assert "set_pressure" not in actions
    assert "sample_start" not in actions

    assert latency_rows
    assert all(row["pressure_overlimit_seen"] == "False" for row in latency_rows)
    assert all(row["vent_heartbeat_gap_exceeded"] == "False" for row in latency_rows)
    assert all(row["digital_gauge_sequence_progress"] == "True" for row in latency_rows)
    assert all(row["stream_stale"] == "False" for row in latency_rows)
    assert all(row["fail_closed_before_vent_off"] == "False" for row in latency_rows)
    assert max(float(row["pressure_hpa"]) for row in latency_rows) == summary["conditioning_pressure_max_hpa"]
    assert summary["attempted_write_count"] == 0
    assert summary["identity_write_command_sent"] is False
    assert summary["calibration_write_command_sent"] is False
    assert summary["senco_write_command_sent"] is False
