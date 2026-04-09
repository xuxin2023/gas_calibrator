import json
from pathlib import Path
from types import SimpleNamespace
import sys

from gas_calibrator.v2.config import summarize_step2_config_safety
from gas_calibrator.v2.core.measurement_phase_coverage import MEASUREMENT_PHASE_COVERAGE_REPORT_FILENAME
from gas_calibrator.v2.core.multi_source_stability import MULTI_SOURCE_STABILITY_EVIDENCE_FILENAME
from gas_calibrator.v2.ui_v2.i18n import t

SUPPORT_DIR = Path(__file__).resolve().parent
if str(SUPPORT_DIR) not in sys.path:
    sys.path.insert(0, str(SUPPORT_DIR))

from ui_v2_support import build_fake_facade


def _inject_point_taxonomy_summary(run_dir: Path) -> None:
    summary_path = run_dir / "summary.json"
    payload = json.loads(summary_path.read_text(encoding="utf-8"))
    stats = dict(payload.get("stats", {}) or {})
    stats["point_summaries"] = [
        {
            "point": {
                "index": 1,
                "pressure_target_label": "ambient",
                "pressure_mode": "ambient",
            },
            "stats": {
                "flush_gate_status": "pass",
                "preseal_dewpoint_c": 6.1,
                "preseal_trigger_overshoot_hpa": 4.2,
                "preseal_vent_off_begin_to_route_sealed_ms": 1200,
                "pressure_gauge_stale_ratio": 0.25,
                "pressure_gauge_stale_count": 1,
                "pressure_gauge_total_count": 4,
            },
        },
        {
            "point": {
                "index": 2,
                "pressure_target_label": "ambient_open",
                "pressure_mode": "ambient_open",
            },
            "stats": {
                "flush_gate_status": "veto",
                "postseal_timeout_blocked": True,
                "dewpoint_rebound_detected": True,
            },
        },
    ]
    stats["point_taxonomy_summary"] = {
        "pressure_summary": "ambient 1 | ambient_open 1",
        "pressure_mode_summary": "ambient_open 2",
        "pressure_target_label_summary": "ambient 1 | ambient_open 1",
        "flush_gate_summary": "pass 1 | veto 1 | rebound 1",
        "preseal_summary": "points 1 | max overshoot 4.2 hPa | max sealed wait 1200 ms",
        "postseal_summary": "timeout blocked 1 | late rebound 1",
        "stale_gauge_summary": "points 1 | worst 25%",
    }
    payload["stats"] = stats
    summary_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _inject_stored_point_taxonomy_summary(run_dir: Path, summary: dict[str, str]) -> None:
    summary_path = run_dir / "summary.json"
    payload = json.loads(summary_path.read_text(encoding="utf-8"))
    stats = dict(payload.get("stats", {}) or {})
    stats["point_taxonomy_summary"] = dict(summary)
    payload["stats"] = stats
    summary_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def test_workbench_snapshot_is_exposed_from_devices_payload(tmp_path: Path) -> None:
    facade = build_fake_facade(tmp_path)
    _inject_point_taxonomy_summary(Path(facade.result_store.run_dir))

    devices = facade.get_devices_snapshot()
    workbench = devices["workbench"]

    assert workbench["meta"]["simulated"] is True
    assert workbench["meta"]["simulation_mode_label"] == t("pages.devices.workbench.banner.simulation_mode")
    assert workbench["workbench"]["view_mode"] == "operator_view"
    assert workbench["workbench"]["config_safety"]["classification"] == "simulation_real_port_inventory_risk"
    assert workbench["workbench"]["config_safety_review"]["status"] == "blocked"
    assert workbench["workbench"]["config_safety_review"]["execution_gate"]["status"] == "blocked"
    assert workbench["workbench"]["live_snapshot_evidence"]["evidence_source"] == "simulated_protocol"
    assert workbench["workbench"]["live_snapshot_evidence"]["acceptance_level"] == "offline_regression"
    assert workbench["workbench"]["live_snapshot_evidence"]["config_governance_handoff"]["execution_gate"]["status"] == "blocked"
    assert workbench["workbench"]["live_snapshot_evidence"]["qc_evidence_section"]["lines"]
    assert workbench["workbench"]["live_snapshot_evidence"]["qc_evidence_section"]["cards"]
    assert workbench["workbench"]["live_snapshot_evidence"]["qc_review_cards"]
    assert workbench["workbench"]["live_snapshot_evidence"]["point_taxonomy_summary"]["pressure_summary"] == (
        "ambient 1 | ambient_open 1"
    )
    assert workbench["workbench"]["live_snapshot_evidence"]["measurement_core_evidence"]["available"] is True
    assert workbench["workbench"]["live_snapshot_evidence"]["measurement_core_evidence"]["artifact_paths"][
        "multi_source_stability_evidence"
    ].endswith(MULTI_SOURCE_STABILITY_EVIDENCE_FILENAME)
    assert workbench["workbench"]["live_snapshot_evidence"]["measurement_core_evidence"]["artifact_paths"][
        "measurement_phase_coverage_report"
    ].endswith(MEASUREMENT_PHASE_COVERAGE_REPORT_FILENAME)
    assert workbench["workbench"]["live_snapshot_evidence"]["recognition_readiness_evidence"]["available"] is True
    assert (
        "scope_readiness_summary"
        in workbench["workbench"]["live_snapshot_evidence"]["recognition_readiness_evidence"]["artifact_paths"]
    )
    assert any(
        "payload" in str(line).lower()
        for line in list(workbench["workbench"]["live_snapshot_evidence"]["measurement_core_evidence"]["summary_lines"] or [])
    )
    assert workbench["workbench"]["live_snapshot_evidence"]["point_taxonomy_summary"]["pressure_mode_summary"] == (
        "ambient_open 2"
    )
    assert workbench["evidence"]["point_taxonomy_summary"]["flush_gate_summary"] == "pass 1 | veto 1 | rebound 1"
    assert workbench["evidence"]["measurement_core_evidence"]["available"] is True
    assert workbench["evidence"]["recognition_readiness_evidence"]["available"] is True
    assert any(
        section["id"] == "recognition_readiness"
        for section in list(workbench["engineer_summary"]["sections"] or [])
    )
    assert any(
        "scope package + decision rule profile" in str(section.get("summary") or "")
        for section in list(workbench["engineer_summary"]["sections"] or [])
        if section.get("id") == "recognition_readiness"
    )
    assert workbench["history"]["items"] == []
    assert workbench["workbench"]["preset_center"]["groups"]
    assert workbench["workbench"]["preset_center"]["manager"]["supports_import_export"] is True
    assert workbench["workbench"]["preset_center"]["manager"]["directory_index"]["builtin"]["count"] > 0
    assert "sharing_scope" in list(workbench["workbench"]["preset_center"]["manager"]["sharing_reserved_fields"] or [])
    assert str(workbench["workbench"]["preset_center"]["manager"]["selected_preset_metadata_summary"] or "").strip()
    assert str(workbench["workbench"]["preset_center"]["manager"]["selected_preset_capability_summary"] or "").strip()
    assert str(workbench["workbench"]["preset_center"]["manager"]["directory_summary"] or "").strip()
    assert workbench["engineer_summary"]["sections"]
    assert set(workbench) >= {
        "analyzer",
        "pace",
        "grz",
        "chamber",
        "relay",
        "thermometer",
        "pressure_gauge",
        "evidence",
        "operator_summary",
        "engineer_summary",
        "workbench",
        "history",
    }


def test_workbench_live_snapshot_prefers_runtime_config_governance_override(tmp_path: Path) -> None:
    facade = build_fake_facade(tmp_path)
    runtime_config_safety = summarize_step2_config_safety(
        facade.config,
        allow_unsafe_step2_config=True,
        unsafe_config_env_enabled=True,
    )
    for config_obj in (facade.config, facade.service.config):
        setattr(config_obj, "_config_safety", dict(runtime_config_safety))
        setattr(config_obj, "_step2_execution_gate", dict(runtime_config_safety.get("execution_gate") or {}))

    workbench = facade.get_devices_snapshot()["workbench"]["workbench"]
    live_snapshot = workbench["live_snapshot_evidence"]

    assert workbench["config_safety_review"]["status"] == "unlocked_override"
    assert workbench["config_safety_review"]["execution_gate"]["status"] == "unlocked_override"
    assert live_snapshot["config_governance_handoff"]["execution_gate"]["status"] == "unlocked_override"
    assert live_snapshot["config_safety_review"]["execution_gate"]["allow_unsafe_step2_config_flag"] is True
    assert live_snapshot["config_safety_review"]["execution_gate"]["allow_unsafe_step2_config_env"] is True


def test_workbench_prefers_stored_point_taxonomy_summary_handoff(tmp_path: Path) -> None:
    facade = build_fake_facade(tmp_path)
    run_dir = Path(facade.result_store.run_dir)
    _inject_point_taxonomy_summary(run_dir)
    _inject_stored_point_taxonomy_summary(
        run_dir,
        {
            "pressure_summary": "stored pressure taxonomy",
            "pressure_mode_summary": "stored pressure mode taxonomy",
            "pressure_target_label_summary": "stored pressure target taxonomy",
            "flush_gate_summary": "stored flush taxonomy",
            "preseal_summary": "stored preseal taxonomy",
            "postseal_summary": "stored postseal taxonomy",
            "stale_gauge_summary": "stored stale taxonomy",
        },
    )

    workbench = facade.get_devices_snapshot()["workbench"]

    assert workbench["workbench"]["live_snapshot_evidence"]["point_taxonomy_summary"]["pressure_summary"] == (
        "stored pressure taxonomy"
    )
    assert workbench["workbench"]["live_snapshot_evidence"]["point_taxonomy_summary"]["pressure_mode_summary"] == (
        "stored pressure mode taxonomy"
    )
    assert workbench["evidence"]["point_taxonomy_summary"]["flush_gate_summary"] == "stored flush taxonomy"
    assert workbench["engineer_summary"]["diagnostics"]["point_taxonomy_summary"]["preseal_summary"] == (
        "stored preseal taxonomy"
    )


def test_analyzer_workbench_supports_eight_devices_and_fault_injection(tmp_path: Path) -> None:
    facade = build_fake_facade(tmp_path)

    facade.execute_device_workbench_action("analyzer", "select", analyzer_index=7)
    facade.execute_device_workbench_action("analyzer", "set_mode", analyzer_index=7, mode=3)
    facade.execute_device_workbench_action("analyzer", "set_active_state", analyzer_index=7, active=False)
    facade.execute_device_workbench_action("analyzer", "inject_fault", analyzer_index=7, fault="partial_frame")
    facade.execute_device_workbench_action("analyzer", "read_frame", analyzer_index=7)

    snapshot = facade.get_device_workbench_snapshot()["analyzer"]
    panel = snapshot["panel_status"]
    overrides = facade.get_device_workbench_snapshot()["evidence"]["simulation_context"]["device_matrix"]["device_overrides"]

    assert panel["selected_analyzer"] == 8
    assert panel["mode"] == 3
    assert panel["active_send"] is False
    assert panel["recent_frames"]
    assert overrides["gas_analyzer_7"]["mode2_stream"] == "partial_frame"


def test_pace_workbench_updates_pressure_and_error_faults(tmp_path: Path) -> None:
    facade = build_fake_facade(tmp_path)

    facade.execute_device_workbench_action("pace", "set_vent", enabled=False)
    facade.execute_device_workbench_action("pace", "set_output", enabled=True)
    facade.execute_device_workbench_action("pace", "set_isolation", enabled=True)
    facade.execute_device_workbench_action("pace", "set_pressure", pressure_hpa=955.0)
    facade.execute_device_workbench_action("pace", "inject_fault", fault="unsupported_header")
    facade.get_device_workbench_snapshot()
    result = facade.execute_device_workbench_action("pace", "query_error")

    panel = facade.get_device_workbench_snapshot()["pace"]["panel_status"]

    assert panel["target_pressure_display"] == "955 hPa"
    assert panel["output_on"] is True
    assert panel["isolation_on"] is True
    assert panel["error_queue"]
    assert "PACE" in result["message"]


def test_grz_chamber_relay_thermometer_and_pressure_workbench_feed_evidence(tmp_path: Path) -> None:
    facade = build_fake_facade(tmp_path)

    facade.execute_device_workbench_action("grz", "set_target_temp", temperature_c=30.0)
    facade.execute_device_workbench_action("grz", "inject_fault", fault="timeout")
    facade.execute_device_workbench_action("chamber", "run")
    facade.execute_device_workbench_action("chamber", "set_mode", mode="stalled")
    facade.execute_device_workbench_action("relay", "inject_fault", relay_name="relay_8", fault="stuck_channel", stuck_channels=[1])
    facade.execute_device_workbench_action("relay", "write_channel", relay_name="relay_8", channel=1, enabled=True)
    facade.execute_device_workbench_action("thermometer", "set_mode", mode="stale")
    facade.execute_device_workbench_action("pressure_gauge", "set_measurement_mode", measurement_mode="sample_hold")
    facade.execute_device_workbench_action("pressure_gauge", "inject_fault", fault="wrong_unit_configuration")

    snapshot = facade.get_device_workbench_snapshot()
    relay_evidence = snapshot["evidence"]["route_physical_validation"]
    reference_quality = snapshot["evidence"]["reference_quality"]
    simulation_context = snapshot["evidence"]["simulation_context"]

    assert snapshot["evidence"]["evidence_source"] == "simulated_protocol"
    assert snapshot["evidence"]["evidence_state"] == "simulated_workbench"
    assert snapshot["evidence"]["not_real_acceptance_evidence"] is True
    assert snapshot["evidence"]["acceptance_level"] == "offline_regression"
    assert snapshot["evidence"]["promotion_state"] == "dry_run_only"
    assert snapshot["evidence"]["qc_review_summary"]["lines"]
    assert snapshot["evidence"]["qc_reviewer_card"]["lines"]
    assert snapshot["evidence"]["qc_evidence_section"]["lines"]
    assert snapshot["evidence"]["qc_review_cards"]
    assert snapshot["evidence"]["qc_review_summary"]["evidence_source"] == "simulated_protocol"
    assert snapshot["evidence"]["qc_review_summary"]["run_gate"]["status"] == "warn"
    assert snapshot["evidence"]["config_safety"]["classification"] == "simulation_real_port_inventory_risk"
    assert snapshot["evidence"]["config_safety_review"]["status"] == "blocked"
    assert snapshot["evidence"]["config_safety_review"]["execution_gate"]["status"] == "blocked"
    assert snapshot["evidence"]["config_governance_handoff"]["blocked_reason_details"]
    assert snapshot["grz"]["injection_state"]["mode"] == "timeout"
    assert snapshot["chamber"]["injection_state"]["mode"] == "stalled"
    assert relay_evidence["relay_physical_mismatch"] is True
    assert relay_evidence["mismatched_channels"][0]["relay"] == "relay_8"
    assert reference_quality["thermometer_reference_status"] == "stale"
    assert reference_quality["pressure_reference_status"] == "wrong_unit_configuration"
    assert simulation_context["workbench_route_trace"]["relay_physical_mismatch"] is True
    assert len(simulation_context["workbench_actions"]) >= 8


def test_workbench_does_not_reuse_non_simulated_service_device_manager(tmp_path: Path) -> None:
    facade = build_fake_facade(tmp_path)
    calls = {"create": 0, "register": 0}

    def _unexpected_create(*_args, **_kwargs):
        calls["create"] += 1
        raise AssertionError("non-simulated device_manager.create_device should not be used by workbench")

    def _unexpected_register(*_args, **_kwargs):
        calls["register"] += 1
        raise AssertionError("non-simulated device_manager.register_device should not be used by workbench")

    facade.service.device_manager = SimpleNamespace(
        device_factory=SimpleNamespace(simulation_mode=False),
        get_device=lambda _name: None,
        _devices={},
        create_device=_unexpected_create,
        register_device=_unexpected_register,
    )
    facade.device_workbench._device_cache.clear()

    result = facade.execute_device_workbench_action("pace", "read_pressure")

    assert result["ok"] is True
    assert calls == {"create": 0, "register": 0}
    assert facade.get_device_workbench_snapshot()["pace"]["simulated"] is True


def test_workbench_supports_view_layering_history_and_quick_scenarios(tmp_path: Path) -> None:
    facade = build_fake_facade(tmp_path)
    _inject_point_taxonomy_summary(Path(facade.result_store.run_dir))

    facade.execute_device_workbench_action("workbench", "set_view_mode", view_mode="engineer_view")
    facade.execute_device_workbench_action("relay", "run_preset", preset_id="route_h2o", relay_name="relay_8")
    facade.execute_device_workbench_action("workbench", "run_quick_scenario", scenario_id="relay_stuck", relay_name="relay_8", channel=1)

    snapshot = facade.get_device_workbench_snapshot()
    history_items = snapshot["history"]["items"]

    assert snapshot["meta"]["view_mode"] == "engineer_view"
    assert snapshot["workbench"]["view_mode_display"]
    assert snapshot["workbench"]["preset_center"]["recent_presets"]
    assert snapshot["engineer_summary"]["cards"]
    assert snapshot["engineer_summary"]["simulation_context_text"]
    assert any(
        str(item.get("title") or "") == t("pages.devices.workbench.engineer_card.suite_analytics")
        for item in snapshot["engineer_summary"]["cards"]
    )
    assert any(
        str(item.get("title") or "") == t("shell.nav.qc")
        for item in snapshot["engineer_summary"]["cards"]
    )
    assert any(
        str(item.get("id") or "") == "qc_review"
        and "质控" in str(item.get("summary") or "")
        for item in snapshot["engineer_summary"]["sections"]
    )
    assert any(
        str(item.get("id") or "") == "config_safety"
        and "配置安全" in str(item.get("summary") or "")
        for item in snapshot["engineer_summary"]["sections"]
    )
    assert snapshot["engineer_summary"]["diagnostics"]["point_taxonomy_summary"]["preseal_summary"] == (
        "points 1 | max overshoot 4.2 hPa | max sealed wait 1200 ms"
    )
    assert any(
        str(item.get("title") or "") == t("pages.devices.workbench.engineer_card.point_taxonomy")
        and "压力语义" in str(item.get("summary") or "")
        for item in snapshot["engineer_summary"]["cards"]
    )
    assert any(
        str(item.get("id") or "") == "point_taxonomy"
        and "压力语义" in str(item.get("body_text") or "")
        for item in snapshot["engineer_summary"]["sections"]
    )
    assert any(
        str(item.get("title") or "")
        == t("pages.devices.workbench.engineer_card.measurement_core", default="measurement-core readiness")
        and "shadow" in str(item.get("summary") or "").lower()
        for item in snapshot["engineer_summary"]["cards"]
    )
    assert any(
        str(item.get("id") or "") == "measurement_core"
        and MULTI_SOURCE_STABILITY_EVIDENCE_FILENAME in str(item.get("body_text") or "")
        for item in snapshot["engineer_summary"]["sections"]
    )
    assert any(
        str(item.get("id") or "") == "measurement_core"
        and MEASUREMENT_PHASE_COVERAGE_REPORT_FILENAME in str(item.get("body_text") or "")
        for item in snapshot["engineer_summary"]["sections"]
    )
    assert "workbench_route_trace" in snapshot["evidence"]["simulation_context"]
    assert history_items[0]["device"] == "workbench"
    assert history_items[0]["action"] == "run_quick_scenario"
    assert history_items[0]["is_fault_injection"] is True
    assert history_items[1]["sequence"] > 0
