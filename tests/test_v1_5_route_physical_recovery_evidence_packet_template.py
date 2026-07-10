import json
from pathlib import Path

from gas_calibrator.tools.export_v1_5_route_physical_recovery_evidence_packet_template import main as cli_main
from gas_calibrator.validation.v1_5_entrypoint_inventory import classify_v1_5_entrypoint
from gas_calibrator.validation.v1_5_route_physical_recovery_evidence_packet import (
    build_v1_5_route_physical_recovery_evidence_packet,
    write_v1_5_route_physical_recovery_evidence_packet,
)
from gas_calibrator.validation.v1_5_route_physical_recovery_evidence_packet_template import (
    build_v1_5_route_physical_recovery_evidence_packet_template,
    write_v1_5_route_physical_recovery_evidence_packet_template,
)
from gas_calibrator.validation.v1_5_route_physical_recovery_readiness import (
    build_v1_5_route_physical_recovery_readiness,
)


def _write_json(path: Path, payload: dict) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def _root_cause_payload() -> dict:
    categories = {
        "dry_gas_dewpoint_rebound_or_not_dry_enough": 1,
        "pressure_controller_vent_no_response": 1,
        "pressure_gauge_no_response": 1,
        "queue_aborted_before_sampling_no_manifest": 1,
    }
    return {
        "schema": "v1_5_route_run_failure_root_cause_audit_v1",
        "manifest": {
            "status": "blocked",
            "blocker_count": sum(categories.values()),
            "review_required_count": 0,
            "category_counts": categories,
            "opens_com_ports": False,
            "controls_pressure": False,
            "controls_water_or_gas_routes": False,
            "connects_postgresql": False,
            "writes_coefficients": False,
            "writes_sn_or_device_code": False,
        },
        "findings": [{"category": key, "severity": "blocker"} for key in categories],
    }


def _fill_reviewed_packet(template: dict) -> dict:
    packet = json.loads(json.dumps(template))
    packet["dry_gas_dewpoint_recovery"].update(
        {
            "status": "pass",
            "dewpoint_c": -30.2,
            "tail_span_c": 0.22,
            "tail_slope_abs_c_per_s": 0.002,
            "route_or_dryer_checked": True,
            "evidence": {"dewpoint_trace": "dry_gas_dewpoint_trace.csv", "route_or_dryer_check": "dryer_checked"},
        }
    )
    packet["pace_vent_recovery"].update(
        {
            "status": "pass",
            "vent_on_off_roundtrip_pass": True,
            "no_response_absent": True,
            "evidence": {"pace_vent_roundtrip_trace": "pace_vent_roundtrip.csv"},
        }
    )
    packet["pressure_gauge_recovery"].update(
        {
            "status": "pass",
            "readback_status": "pass",
            "absolute_pressure_source": "inl",
            "no_response_absent": True,
            "evidence": {"pressure_gauge_inl_trace": "com22_inl_readback.csv"},
        }
    )
    packet["accepted_manifest_review"].update(
        {
            "status": "pass",
            "accepted_manifest_path": "accepted_manifest.csv",
            "supersedence_review_id": "reviewed_supersedence_20260710",
        }
    )
    return packet


def test_template_contains_required_physical_collection_steps(tmp_path: Path) -> None:
    root_cause = _write_json(tmp_path / "root_cause.json", _root_cause_payload())
    model = build_v1_5_route_physical_recovery_evidence_packet_template(
        root_cause_audit_path=root_cause,
    )
    steps = {row["step_id"]: row for row in model["collection_plan"]}

    assert model["manifest"]["status"] == "template_ready"
    assert model["manifest"]["ready_for_validator"] is False
    assert model["manifest"]["opens_com_ports"] is False
    assert set(steps) == {
        "dry_gas_dewpoint_recovery",
        "pace_vent_recovery",
        "pressure_gauge_recovery",
        "fresh_canonical_queue_policy",
        "accepted_manifest_review",
    }
    assert steps["dry_gas_dewpoint_recovery"]["collection_requires_real_hardware"] is True
    assert "0613/0620/0621" in model["recovery_evidence_packet_template"]["next_run_policy"]["mature_physical_baseline"]
    assert model["manifest"]["root_cause_category_counts"]["pressure_gauge_no_response"] == 1


def test_unfilled_template_is_blocked_by_packet_validator(tmp_path: Path) -> None:
    outputs = write_v1_5_route_physical_recovery_evidence_packet_template(output_dir=tmp_path / "template")
    model = build_v1_5_route_physical_recovery_evidence_packet(
        recovery_evidence_packet_path=outputs["packet_template"],
    )
    blocker_requirements = {row["requirement"] for row in model["findings"] if row["severity"] == "blocker"}

    assert model["manifest"]["status"] == "blocked"
    assert "dry_gas_dewpoint_recovery" in blocker_requirements
    assert "pace_vent_recovery" in blocker_requirements
    assert "pressure_gauge_recovery" in blocker_requirements
    assert "side_effect_boundary" not in blocker_requirements


def test_filled_template_can_feed_packet_validator_and_readiness(tmp_path: Path) -> None:
    template_outputs = write_v1_5_route_physical_recovery_evidence_packet_template(output_dir=tmp_path / "template")
    template_packet = json.loads(template_outputs["packet_template"].read_text(encoding="utf-8"))
    reviewed_packet = _write_json(tmp_path / "reviewed_packet.json", _fill_reviewed_packet(template_packet))
    root_cause = _write_json(tmp_path / "root_cause.json", _root_cause_payload())
    packet_outputs = write_v1_5_route_physical_recovery_evidence_packet(
        recovery_evidence_packet_path=reviewed_packet,
        output_dir=tmp_path / "packet_review",
    )

    readiness = build_v1_5_route_physical_recovery_readiness(
        root_cause_audit_path=root_cause,
        recovery_evidence_path=packet_outputs["validated_recovery_evidence"],
    )

    assert readiness["manifest"]["status"] == "pass"
    assert readiness["manifest"]["next_continuous_run_allowed"] is True
    assert readiness["manifest"]["segmented_evidence_fit_use_allowed"] is True


def test_writer_cli_and_entrypoint_classification(tmp_path: Path) -> None:
    root_cause = _write_json(tmp_path / "root_cause.json", _root_cause_payload())
    outputs = write_v1_5_route_physical_recovery_evidence_packet_template(
        output_dir=tmp_path / "out",
        root_cause_audit_path=root_cause,
    )
    manifest = json.loads(outputs["manifest"].read_text(encoding="utf-8"))
    collection_plan = outputs["collection_plan"].read_text(encoding="utf-8")
    entrypoint = classify_v1_5_entrypoint(
        Path("src/gas_calibrator/tools/export_v1_5_route_physical_recovery_evidence_packet_template.py")
    )

    assert manifest["manifest"]["collection_step_count"] == 5
    assert "dry_gas_dewpoint_recovery" in collection_plan
    assert (
        cli_main(
            [
                "--root-cause-audit-path",
                str(root_cause),
                "--output-dir",
                str(tmp_path / "cli"),
            ]
        )
        == 0
    )
    assert entrypoint.category == "formal_review_evidence"
    assert entrypoint.risk_level == "offline"
    assert entrypoint.opens_com_ports is False
