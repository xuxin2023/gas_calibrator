import csv
import json
from pathlib import Path

from gas_calibrator.tools.export_v1_5_route_physical_recovery_evidence_binder import main as cli_main
from gas_calibrator.validation.v1_5_entrypoint_inventory import classify_v1_5_entrypoint
from gas_calibrator.validation.v1_5_route_physical_recovery_evidence_binder import (
    build_v1_5_route_physical_recovery_evidence_binder,
    write_v1_5_route_physical_recovery_evidence_binder,
)
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


def _write_csv(path: Path, rows: list[dict[str, object]]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = sorted({key for row in rows for key in row})
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    return path


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


def _good_trace_paths(tmp_path: Path) -> dict[str, Path]:
    return {
        "dewpoint": _write_csv(
            tmp_path / "dry_gas_dewpoint_trace.csv",
            [
                {"elapsed_s": 0, "dewpoint_c": -29.9},
                {"elapsed_s": 30, "dewpoint_c": -30.1},
                {"elapsed_s": 60, "dewpoint_c": -30.2},
                {"elapsed_s": 90, "dewpoint_c": -30.2},
                {"elapsed_s": 120, "dewpoint_c": -30.3},
            ],
        ),
        "pace": _write_csv(
            tmp_path / "pace_vent_roundtrip.csv",
            [
                {"elapsed_s": 0, "command": "VENT 1", "response": "OK", "vent_state": "on"},
                {"elapsed_s": 2, "command": "VENT 0", "response": "OK", "vent_state": "off"},
            ],
        ),
        "pressure": _write_csv(
            tmp_path / "com22_inl_readback.csv",
            [
                {"elapsed_s": 0, "query": ":SENS:PRES:INL?", "pressure_hpa": 1000.5},
                {"elapsed_s": 1, "query": ":SENS:PRES:INL?", "pressure_hpa": 1000.6},
            ],
        ),
        "route_check": _write_json(tmp_path / "route_or_dryer_check.json", {"status": "pass"}),
        "accepted_manifest": _write_csv(
            tmp_path / "accepted_manifest.csv",
            [{"point_id": "fresh_canonical_queue_required", "status": "superseded"}],
        ),
    }


def test_binder_builds_packet_that_can_unlock_readiness(tmp_path: Path) -> None:
    paths = _good_trace_paths(tmp_path)
    binder_outputs = write_v1_5_route_physical_recovery_evidence_binder(
        output_dir=tmp_path / "binder",
        dewpoint_trace_path=paths["dewpoint"],
        pace_vent_trace_path=paths["pace"],
        pressure_gauge_trace_path=paths["pressure"],
        route_or_dryer_check_path=paths["route_check"],
        accepted_manifest_path=paths["accepted_manifest"],
        supersedence_review_id="reviewed_supersedence_20260710",
    )
    binder_model = json.loads(binder_outputs["manifest"].read_text(encoding="utf-8"))

    packet_model = build_v1_5_route_physical_recovery_evidence_packet(
        recovery_evidence_packet_path=binder_outputs["recovery_evidence_packet"],
    )
    packet_outputs = write_v1_5_route_physical_recovery_evidence_packet(
        recovery_evidence_packet_path=binder_outputs["recovery_evidence_packet"],
        output_dir=tmp_path / "packet",
    )
    readiness = build_v1_5_route_physical_recovery_readiness(
        root_cause_audit_path=_write_json(tmp_path / "root_cause.json", _root_cause_payload()),
        recovery_evidence_path=packet_outputs["validated_recovery_evidence"],
    )

    assert binder_model["manifest"]["status"] == "packet_ready_for_validator"
    assert binder_model["manifest"]["ready_for_validator"] is True
    assert binder_model["manifest"]["opens_com_ports"] is False
    assert packet_model["manifest"]["status"] == "pass"
    assert readiness["manifest"]["status"] == "pass"
    assert readiness["manifest"]["next_continuous_run_allowed"] is True
    assert readiness["manifest"]["segmented_evidence_fit_use_allowed"] is True


def test_binder_blocks_bad_dewpoint_no_response_and_bad_pressure_source(tmp_path: Path) -> None:
    dewpoint = _write_csv(
        tmp_path / "bad_dewpoint.csv",
        [
            {"elapsed_s": 0, "dewpoint_c": -27.5},
            {"elapsed_s": 30, "dewpoint_c": -27.3},
            {"elapsed_s": 60, "dewpoint_c": -27.2},
        ],
    )
    pace = _write_csv(
        tmp_path / "bad_pace.csv",
        [{"command": "VENT 1", "response": "NO_RESPONSE", "vent_state": "on"}],
    )
    pressure = _write_csv(
        tmp_path / "bad_pressure.csv",
        [{"query": ":SENS:PRES?", "pressure_hpa": 1000.0, "pressure_source": "gauge"}],
    )

    model = build_v1_5_route_physical_recovery_evidence_binder(
        dewpoint_trace_path=dewpoint,
        pace_vent_trace_path=pace,
        pressure_gauge_trace_path=pressure,
        route_or_dryer_check_note="dryer checked",
    )
    blocker_requirements = {row["requirement"] for row in model["findings"] if row["severity"] == "blocker"}

    assert model["manifest"]["status"] == "blocked"
    assert model["manifest"]["ready_for_validator"] is False
    assert blocker_requirements == {
        "dry_gas_dewpoint_recovery",
        "pace_vent_recovery",
        "pressure_gauge_recovery",
    }


def test_missing_accepted_manifest_remains_packet_review_not_binder_blocker(tmp_path: Path) -> None:
    paths = _good_trace_paths(tmp_path)
    binder_outputs = write_v1_5_route_physical_recovery_evidence_binder(
        output_dir=tmp_path / "binder",
        dewpoint_trace_path=paths["dewpoint"],
        pace_vent_trace_path=paths["pace"],
        pressure_gauge_trace_path=paths["pressure"],
        route_or_dryer_check_note="dryer checked before smoke",
    )
    packet_model = build_v1_5_route_physical_recovery_evidence_packet(
        recovery_evidence_packet_path=binder_outputs["recovery_evidence_packet"],
    )

    assert json.loads(binder_outputs["manifest"].read_text(encoding="utf-8"))["manifest"]["status"] == (
        "packet_ready_for_validator"
    )
    assert packet_model["manifest"]["status"] == "review_required"
    assert packet_model["manifest"]["readiness_input_ready"] is True
    assert packet_model["manifest"]["segmented_evidence_review_ready"] is False


def test_cli_and_entrypoint_classification(tmp_path: Path) -> None:
    paths = _good_trace_paths(tmp_path)
    output_dir = tmp_path / "cli"
    exit_code = cli_main(
        [
            "--dewpoint-trace-path",
            str(paths["dewpoint"]),
            "--pace-vent-trace-path",
            str(paths["pace"]),
            "--pressure-gauge-trace-path",
            str(paths["pressure"]),
            "--route-or-dryer-check-path",
            str(paths["route_check"]),
            "--accepted-manifest-path",
            str(paths["accepted_manifest"]),
            "--supersedence-review-id",
            "reviewed_supersedence_20260710",
            "--output-dir",
            str(output_dir),
            "--fail-on-blocker",
        ]
    )
    manifest = json.loads((output_dir / "v1_5_route_physical_recovery_evidence_binder.json").read_text(encoding="utf-8"))
    markdown = (output_dir / "V1_5_ROUTE_PHYSICAL_RECOVERY_EVIDENCE_BINDER.md").read_text(encoding="utf-8")
    entrypoint = classify_v1_5_entrypoint(
        Path("src/gas_calibrator/tools/export_v1_5_route_physical_recovery_evidence_binder.py")
    )

    assert exit_code == 0
    assert manifest["manifest"]["status"] == "packet_ready_for_validator"
    assert "offline trace binding only" in markdown
    assert entrypoint.category == "formal_review_evidence"
    assert entrypoint.risk_level == "offline"
    assert entrypoint.opens_com_ports is False
