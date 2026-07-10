import json
from pathlib import Path

from gas_calibrator.tools.export_v1_5_route_physical_recovery_evidence_packet import main as cli_main
from gas_calibrator.validation.v1_5_entrypoint_inventory import classify_v1_5_entrypoint
from gas_calibrator.validation.v1_5_route_physical_recovery_evidence_packet import (
    build_v1_5_route_physical_recovery_evidence_packet,
    write_v1_5_route_physical_recovery_evidence_packet,
)
from gas_calibrator.validation.v1_5_route_physical_recovery_readiness import (
    build_v1_5_route_physical_recovery_readiness,
)


def _write_json(path: Path, payload: dict) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def _packet(**overrides: object) -> dict:
    payload = {
        "schema": "v1_5_route_physical_recovery_evidence_packet_v1",
        "dry_gas_dewpoint_recovery": {
            "status": "pass",
            "dewpoint_c": -30.4,
            "dry_enough_threshold_c": -28.0,
            "tail_span_c": 0.18,
            "tail_slope_abs_c_per_s": 0.002,
            "route_or_dryer_checked": True,
            "evidence": {
                "dewpoint_trace": "D:/gas_calibrator/_p9_20260710/dry_gas_dewpoint_trace.csv",
                "route_check": "dryer_checked_before_smoke",
            },
        },
        "pace_vent_recovery": {
            "status": "pass",
            "vent_on_off_roundtrip_pass": True,
            "no_response_absent": True,
            "evidence": {"trace": "D:/gas_calibrator/_p9_20260710/pace_vent_roundtrip.csv"},
        },
        "pressure_gauge_recovery": {
            "status": "pass",
            "readback_status": "pass",
            "absolute_pressure_source": "inl",
            "no_response_absent": True,
            "evidence": {"trace": "D:/gas_calibrator/_p9_20260710/com22_inl_readback.csv"},
        },
        "accepted_manifest_review": {
            "status": "pass",
            "accepted_manifest_path": "D:/gas_calibrator/_p9_20260710/accepted_manifest.csv",
            "supersedence_review_id": "reviewed_supersedence_20260710",
        },
        "next_run_policy": {
            "fresh_canonical_queue": True,
            "mature_physical_baseline": "0613/0620/0621",
            "forbidden_surfaces_absent": True,
            "co2_entrypoint": "src/gas_calibrator/tools/run_v1_5_formal_co2_open_flow_queue.py",
            "h2o_entrypoint": "src/gas_calibrator/tools/run_v1_5_formal_h2o_open_flow_queue.py",
        },
        "opens_com_ports": False,
        "controls_pressure": False,
        "controls_water_or_gas_routes": False,
        "connects_postgresql": False,
        "writes_coefficients": False,
        "writes_sn_or_device_code": False,
        "formal_release_allowed": False,
        "database_import_allowed": False,
        "not_real_acceptance_evidence": True,
    }
    payload.update(overrides)
    return payload


def _root_cause_payload() -> dict:
    categories = {
        "dry_gas_dewpoint_rebound_or_not_dry_enough": 1,
        "pressure_controller_vent_no_response": 1,
        "pressure_gauge_no_response": 1,
        "queue_aborted_before_sampling_no_manifest": 1,
        "direct_or_retry_point_without_queue_manifest": 1,
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


def test_valid_packet_is_ready_input_for_route_recovery_readiness(tmp_path: Path) -> None:
    packet = _write_json(tmp_path / "packet.json", _packet())
    model = build_v1_5_route_physical_recovery_evidence_packet(recovery_evidence_packet_path=packet)

    assert model["manifest"]["status"] == "pass"
    assert model["manifest"]["readiness_input_ready"] is True
    assert model["manifest"]["segmented_evidence_review_ready"] is True
    assert model["manifest"]["opens_com_ports"] is False
    assert model["manifest"]["not_real_acceptance_evidence"] is True


def test_validated_recovery_evidence_can_unlock_recovery_gate_with_root_cause(tmp_path: Path) -> None:
    packet = _write_json(tmp_path / "packet.json", _packet())
    outputs = write_v1_5_route_physical_recovery_evidence_packet(
        recovery_evidence_packet_path=packet,
        output_dir=tmp_path / "packet_out",
    )
    root_cause = _write_json(tmp_path / "root_cause.json", _root_cause_payload())

    readiness = build_v1_5_route_physical_recovery_readiness(
        root_cause_audit_path=root_cause,
        recovery_evidence_path=outputs["validated_recovery_evidence"],
    )

    assert readiness["manifest"]["status"] == "pass"
    assert readiness["manifest"]["next_continuous_run_allowed"] is True
    assert readiness["manifest"]["segmented_evidence_fit_use_allowed"] is True


def test_bad_dewpoint_pressure_source_and_side_effects_block_packet(tmp_path: Path) -> None:
    packet = _write_json(
        tmp_path / "packet.json",
        _packet(
            dry_gas_dewpoint_recovery={
                "status": "pass",
                "dewpoint_c": -26.0,
                "dry_enough_threshold_c": -28.0,
                "tail_span_c": 0.1,
                "tail_slope_abs_c_per_s": 0.001,
                "route_or_dryer_checked": True,
                "evidence": {"dewpoint_trace": "trace.csv"},
            },
            pressure_gauge_recovery={
                "status": "pass",
                "readback_status": "pass",
                "absolute_pressure_source": "gauge",
                "no_response_absent": True,
                "evidence": {"trace": "pressure.csv"},
            },
            controls_pressure=True,
        ),
    )

    model = build_v1_5_route_physical_recovery_evidence_packet(recovery_evidence_packet_path=packet)
    requirements = {row["requirement"] for row in model["findings"] if row["severity"] == "blocker"}

    assert model["manifest"]["status"] == "blocked"
    assert model["manifest"]["readiness_input_ready"] is False
    assert "dry_gas_dewpoint_recovery" in requirements
    assert "pressure_gauge_recovery" in requirements
    assert "side_effect_boundary.controls_pressure" in requirements


def test_next_run_policy_blocks_handoff_0624_worker_and_missing_canonical_queue(tmp_path: Path) -> None:
    packet = _write_json(
        tmp_path / "packet.json",
        _packet(
            next_run_policy={
                "fresh_canonical_queue": True,
                "mature_physical_baseline": "0624",
                "forbidden_surfaces_absent": True,
                "co2_entrypoint": "D:/gas_calibrator/_handoff/v1_5_formal_queue_migration_20260624/run_v1_5_formal_open_flow_sampling.py",
            }
        ),
    )

    model = build_v1_5_route_physical_recovery_evidence_packet(recovery_evidence_packet_path=packet)
    row = next(row for row in model["findings"] if row["requirement"] == "next_run_policy")

    assert model["manifest"]["status"] == "blocked"
    assert row["severity"] == "blocker"
    assert "Forbidden references" in row["reason"]


def test_next_run_policy_blocks_root_migration_absolute_path(tmp_path: Path) -> None:
    packet = _write_json(
        tmp_path / "packet.json",
        _packet(
            next_run_policy={
                "fresh_canonical_queue": True,
                "mature_physical_baseline": "0613/0620/0621",
                "forbidden_surfaces_absent": True,
                "co2_entrypoint": "D:/gas_calibrator/src/gas_calibrator/tools/run_v1_5_formal_co2_open_flow_queue.py",
                "h2o_entrypoint": "src/gas_calibrator/tools/run_v1_5_formal_h2o_open_flow_queue.py",
            }
        ),
    )

    model = build_v1_5_route_physical_recovery_evidence_packet(recovery_evidence_packet_path=packet)
    row = next(row for row in model["findings"] if row["requirement"] == "next_run_policy")

    assert model["manifest"]["status"] == "blocked"
    assert "d:/gas_calibrator/src/" in row["reason"]


def test_missing_accepted_manifest_is_review_not_next_run_blocker(tmp_path: Path) -> None:
    packet_payload = _packet()
    packet_payload.pop("accepted_manifest_review")
    packet = _write_json(tmp_path / "packet.json", packet_payload)

    model = build_v1_5_route_physical_recovery_evidence_packet(recovery_evidence_packet_path=packet)

    assert model["manifest"]["status"] == "review_required"
    assert model["manifest"]["readiness_input_ready"] is True
    assert model["manifest"]["segmented_evidence_review_ready"] is False
    assert any(row["requirement"] == "accepted_manifest_review" for row in model["findings"])


def test_writer_cli_and_entrypoint_classification(tmp_path: Path) -> None:
    packet = _write_json(tmp_path / "packet.json", _packet(controls_water_or_gas_routes=True))
    outputs = write_v1_5_route_physical_recovery_evidence_packet(
        recovery_evidence_packet_path=packet,
        output_dir=tmp_path / "out",
    )
    model = json.loads(outputs["manifest"].read_text(encoding="utf-8"))
    markdown = outputs["markdown"].read_text(encoding="utf-8")
    entrypoint = classify_v1_5_entrypoint(
        Path("src/gas_calibrator/tools/export_v1_5_route_physical_recovery_evidence_packet.py")
    )

    assert model["manifest"]["status"] == "blocked"
    assert "V1.5 Route Physical Recovery Evidence Packet" in markdown
    assert cli_main(["--recovery-evidence-packet-path", str(packet), "--output-dir", str(tmp_path / "cli")]) == 0
    assert (
        cli_main(
            [
                "--recovery-evidence-packet-path",
                str(packet),
                "--output-dir",
                str(tmp_path / "cli_block"),
                "--fail-on-blocker",
            ]
        )
        == 2
    )
    assert entrypoint.category == "formal_review_evidence"
    assert entrypoint.risk_level == "offline"
