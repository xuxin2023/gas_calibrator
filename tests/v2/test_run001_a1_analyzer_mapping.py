from __future__ import annotations

import json
from pathlib import Path

from gas_calibrator.v2.core.run001_a1_analyzer_mapping import (
    build_analyzer_mapping_candidate_payload,
    build_mode2_setup_target_plan_payload,
    write_analyzer_mapping_artifacts,
)


def _diagnostics_payload() -> dict:
    rows = [
        ("COM35", "091", 1, True, "mode_mismatch"),
        ("COM36", "", None, False, "no_data"),
        ("COM37", "003", 2, True, "ok"),
        ("COM38", "", None, False, "no_data"),
        ("COM39", "", None, False, "no_data"),
        ("COM40", "", None, False, "no_data"),
        ("COM41", "023", 1, True, "mode_mismatch"),
        ("COM42", "012", 1, True, "mode_mismatch"),
    ]
    analyzers = []
    for port, device_id, mode, active_send, error_type in rows:
        analyzers.append(
            {
                "logical_id": f"port_discovery_{port}",
                "configured_port": port,
                "observed_device_id": device_id,
                "observed_mode": mode,
                "mode1_detected": mode == 1,
                "mode2_detected": mode == 2,
                "frame_parse_success": bool(mode),
                "active_send_detected": active_send,
                "error_type": error_type,
            }
        )
    return {
        "generated_at": "2026-04-25T06:54:02+00:00",
        "read_only": True,
        "requested_ports": ["COM35", "COM36", "COM37", "COM38", "COM39", "COM40", "COM41", "COM42"],
        "analyzers": analyzers,
    }


def test_mapping_candidate_includes_four_detected_analyzers_and_no_data_ports() -> None:
    payload = build_analyzer_mapping_candidate_payload(_diagnostics_payload())
    candidates = payload["detected_analyzers"]

    assert [item["port"] for item in candidates] == ["COM35", "COM37", "COM41", "COM42"]
    assert [item["detected_device_id"] for item in candidates] == ["091", "003", "023", "012"]
    assert payload["no_data_ports"] == ["COM36", "COM38", "COM39", "COM40"]
    assert candidates[0]["physical_label"] == "待现场确认"
    assert candidates[0]["suggested_action"] == "set MODE2 + active-send"
    assert candidates[1]["suggested_action"] == "keep"
    assert payload["old_ga01_ga04_id_assumption_valid"] is False
    assert payload["not_auto_apply_to_formal_config"] is True
    assert candidates[0]["do_not_force_old_configured_device_id"] is True


def test_setup_target_plan_only_targets_mode1_and_keeps_com37() -> None:
    mapping = build_analyzer_mapping_candidate_payload(_diagnostics_payload())
    plan = build_mode2_setup_target_plan_payload(mapping)

    assert [item["port"] for item in plan["setup_targets"]] == ["COM35", "COM41", "COM42"]
    assert [item["expected_current_device_id"] for item in plan["setup_targets"]] == ["091", "023", "012"]
    assert [item["port"] for item in plan["already_mode2_keep"]] == ["COM37"]
    assert plan["already_mode2_keep"][0]["detected_device_id"] == "003"
    assert plan["already_mode2_keep"][0]["repeat_setup_recommended"] is False
    assert plan["command_target_id"] == "FFF"
    assert plan["command_whitelist"] == ["MODE,YGAS,FFF,2", "SETCOMWAY,YGAS,FFF,1"]
    assert "SENCO" in plan["forbidden_tokens"]
    assert plan["commands_sent"] == []
    assert plan["persistent_write_command_sent"] is False
    assert plan["calibration_write_command_sent"] is False


def test_mapping_artifact_writer_does_not_modify_formal_config(tmp_path: Path) -> None:
    mapping = build_analyzer_mapping_candidate_payload(_diagnostics_payload())
    plan = build_mode2_setup_target_plan_payload(mapping)

    written = write_analyzer_mapping_artifacts(tmp_path, mapping, plan)
    saved_mapping = json.loads(Path(written["analyzer_mapping_candidate_json"]).read_text(encoding="utf-8"))
    saved_plan = json.loads(Path(written["setup_target_plan_json"]).read_text(encoding="utf-8"))

    assert Path(written["analyzer_mapping_candidate_md"]).exists()
    assert Path(written["setup_target_plan_md"]).exists()
    assert saved_mapping["summary"]["formal_config_updated"] is False
    assert saved_plan["summary"]["formal_config_updated"] is False
