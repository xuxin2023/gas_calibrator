from __future__ import annotations

import json
from pathlib import Path

from gas_calibrator.v2.core.run001_a1_dry_run import (
    RUN001_NOT_EXECUTED,
    RUN001_PASS,
    build_run001_a1_evidence_payload,
    load_point_rows,
    summarize_enabled_analyzers,
    write_run001_a1_artifacts,
)
from gas_calibrator.v2.entry import load_config_bundle


CONFIG_PATH = (
    Path(__file__).resolve().parents[2]
    / "src"
    / "gas_calibrator"
    / "v2"
    / "configs"
    / "validation"
    / "run001_a1_co2_only_skip0_no_write_detected_4_analyzers.json"
)


def _raw_config() -> dict:
    return json.loads(CONFIG_PATH.read_text(encoding="utf-8"))


def test_detected_analyzer_config_uses_only_measured_four_enabled_analyzers() -> None:
    raw = _raw_config()
    analyzers = summarize_enabled_analyzers(raw)

    assert [item["name"] for item in analyzers] == ["analyzer_0", "analyzer_1", "analyzer_2", "analyzer_3"]
    assert [item["port"] for item in analyzers] == ["COM35", "COM37", "COM41", "COM42"]
    assert [item["device_id"] for item in analyzers] == ["091", "003", "023", "012"]
    assert all(item["mode"] == 2 for item in analyzers)
    assert all(item["active_send"] is True for item in analyzers)
    assert {"COM36", "COM38", "COM39", "COM40"}.isdisjoint({item["port"] for item in analyzers})
    assert {"001", "002", "004"}.isdisjoint({item["device_id"] for item in analyzers})


def test_detected_analyzer_config_preflight_payload_records_enabled_list(tmp_path: Path) -> None:
    raw = _raw_config()
    points = load_point_rows(CONFIG_PATH, raw)
    payload = build_run001_a1_evidence_payload(raw, config_path=CONFIG_PATH, point_rows=points, run_dir=tmp_path)
    written = write_run001_a1_artifacts(tmp_path, payload)
    summary = json.loads(Path(written["summary"]).read_text(encoding="utf-8"))
    manifest = json.loads(Path(written["manifest"]).read_text(encoding="utf-8"))

    assert payload["final_decision"] == RUN001_PASS
    assert payload["readiness_result"] == RUN001_PASS
    assert payload["a1_final_decision"] == RUN001_NOT_EXECUTED
    assert payload["no_write"] is True
    assert payload["co2_only"] is True
    assert payload["skip_co2_ppm"] == [0]
    assert payload["single_route"] is True
    assert payload["single_temperature_group"] is True
    assert payload["h2o_single_route_readiness"] == "yellow"
    assert payload["full_single_temperature_h2o_co2_group_readiness"] == "yellow"
    assert payload["default_cutover_to_v2"] is False
    assert payload["disable_v1"] is False
    assert payload["analyzer_ports"] == ["COM35", "COM37", "COM41", "COM42"]
    assert payload["analyzer_device_ids"] == ["091", "003", "023", "012"]
    assert summary["enabled_analyzers"] == payload["enabled_analyzers"]
    assert manifest["enabled_analyzers"] == payload["enabled_analyzers"]
    assert manifest["analyzer_ports"] == ["COM35", "COM37", "COM41", "COM42"]
    assert manifest["analyzer_device_ids"] == ["091", "003", "023", "012"]


def test_detected_analyzer_config_loads_without_unsafe_step2_bypass() -> None:
    resolved_config_path, raw, app_config = load_config_bundle(
        str(CONFIG_PATH),
        simulation_mode=False,
        allow_unsafe_step2_config=False,
        enforce_step2_execution_gate=False,
    )

    assert Path(resolved_config_path) == CONFIG_PATH
    assert raw["run001_a1"]["no_write"] is True
    assert raw["run001_a1"]["full_h2o_co2_group"] is False
    assert app_config.workflow.route_mode == "co2_only"
    assert app_config.workflow.skip_co2_ppm == [0]
    assert [item.port for item in app_config.devices.gas_analyzers] == ["COM35", "COM37", "COM41", "COM42"]
    assert [item.device_id for item in app_config.devices.gas_analyzers] == ["091", "003", "023", "012"]
