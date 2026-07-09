import json
from pathlib import Path

from gas_calibrator.tools.export_v1_5_route_physical_recovery_readiness import main as cli_main
from gas_calibrator.validation.v1_5_entrypoint_inventory import classify_v1_5_entrypoint
from gas_calibrator.validation.v1_5_route_physical_recovery_readiness import (
    build_v1_5_route_physical_recovery_readiness,
    write_v1_5_route_physical_recovery_readiness,
)


def _write_json(path: Path, payload: dict) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def _audit_payload(*, categories: dict[str, int]) -> dict:
    return {
        "schema": "v1_5_route_run_failure_root_cause_audit_v1",
        "manifest": {
            "status": "blocked" if categories else "pass",
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


def _recovery_payload(**overrides: object) -> dict:
    payload = {
        "schema": "v1_5_route_physical_recovery_evidence_v1",
        "dry_gas_dewpoint_recovery": {
            "status": "pass",
            "dewpoint_c": -30.2,
            "dry_enough_threshold_c": -28.0,
            "tail_span_c": 0.2,
            "tail_slope_abs_c_per_s": 0.002,
            "route_or_dryer_checked": True,
        },
        "pace_vent_recovery": {
            "status": "pass",
            "vent_on_off_roundtrip_pass": True,
            "no_response_absent": True,
        },
        "pressure_gauge_recovery": {
            "status": "pass",
            "readback_status": "pass",
            "absolute_pressure_source": "inl",
            "no_response_absent": True,
        },
        "accepted_manifest_review": {
            "status": "pass",
            "accepted_manifest_path": "D:/gas_calibrator/_p9_20260709/accepted_manifest.csv",
            "supersedence_review_id": "reviewed_supersedence_001",
        },
        "next_run_policy": {
            "fresh_canonical_queue": True,
            "mature_physical_baseline": "0613/0620/0621",
            "forbidden_surfaces_absent": True,
        },
    }
    payload.update(overrides)
    return payload


def test_physical_blockers_without_recovery_block_next_continuous_run(tmp_path: Path) -> None:
    audit = _write_json(
        tmp_path / "root_cause.json",
        _audit_payload(
            categories={
                "dry_gas_dewpoint_rebound_or_not_dry_enough": 1,
                "pressure_controller_vent_no_response": 1,
                "pressure_gauge_no_response": 1,
                "queue_aborted_before_sampling_no_manifest": 1,
            }
        ),
    )

    model = build_v1_5_route_physical_recovery_readiness(root_cause_audit_path=audit)
    requirements = {row["requirement"] for row in model["findings"] if row["severity"] == "blocker"}

    assert model["manifest"]["status"] == "blocked"
    assert model["manifest"]["next_continuous_run_allowed"] is False
    assert "dry_gas_dewpoint_recovery" in requirements
    assert "pace_vent_recovery" in requirements
    assert "pressure_gauge_recovery" in requirements
    assert "next_run_policy" in requirements
    assert model["manifest"]["opens_com_ports"] is False
    assert model["manifest"]["controls_pressure"] is False


def test_recovered_physical_state_and_fresh_canonical_queue_allow_next_run(tmp_path: Path) -> None:
    audit = _write_json(
        tmp_path / "root_cause.json",
        _audit_payload(
            categories={
                "dry_gas_dewpoint_rebound_or_not_dry_enough": 1,
                "pressure_controller_vent_no_response": 1,
                "pressure_gauge_no_response": 1,
                "queue_aborted_before_sampling_no_manifest": 1,
            }
        ),
    )
    recovery = _write_json(tmp_path / "recovery.json", _recovery_payload())

    model = build_v1_5_route_physical_recovery_readiness(
        root_cause_audit_path=audit,
        recovery_evidence_path=recovery,
    )

    assert model["manifest"]["status"] == "pass"
    assert model["manifest"]["physical_recovery_passed"] is True
    assert model["manifest"]["fresh_canonical_queue_policy_passed"] is True
    assert model["manifest"]["next_continuous_run_allowed"] is True
    assert model["manifest"]["segmented_evidence_fit_use_allowed"] is True


def test_bad_dewpoint_or_pressure_source_keeps_gate_blocked(tmp_path: Path) -> None:
    audit = _write_json(
        tmp_path / "root_cause.json",
        _audit_payload(
            categories={
                "dry_gas_dewpoint_rebound_or_not_dry_enough": 1,
                "pressure_gauge_no_response": 1,
            }
        ),
    )
    recovery = _write_json(
        tmp_path / "recovery.json",
        _recovery_payload(
            dry_gas_dewpoint_recovery={
                "status": "pass",
                "dewpoint_c": -25.0,
                "dry_enough_threshold_c": -28.0,
                "tail_span_c": 0.1,
                "tail_slope_abs_c_per_s": 0.001,
                "route_or_dryer_checked": True,
            },
            pressure_gauge_recovery={
                "status": "pass",
                "readback_status": "pass",
                "absolute_pressure_source": "gauge",
                "no_response_absent": True,
            },
        ),
    )

    model = build_v1_5_route_physical_recovery_readiness(
        root_cause_audit_path=audit,
        recovery_evidence_path=recovery,
    )

    assert model["manifest"]["status"] == "blocked"
    assert model["manifest"]["next_continuous_run_allowed"] is False
    blocker_requirements = {row["requirement"] for row in model["findings"] if row["severity"] == "blocker"}
    assert blocker_requirements == {"dry_gas_dewpoint_recovery", "pressure_gauge_recovery"}


def test_any_prior_route_root_cause_requires_fresh_canonical_next_queue(tmp_path: Path) -> None:
    audit = _write_json(
        tmp_path / "root_cause.json",
        _audit_payload(categories={"dry_gas_dewpoint_rebound_or_not_dry_enough": 1}),
    )
    recovery = _write_json(
        tmp_path / "recovery.json",
        _recovery_payload(
            next_run_policy={
                "fresh_canonical_queue": False,
                "mature_physical_baseline": "0613/0620/0621",
                "forbidden_surfaces_absent": True,
            }
        ),
    )

    model = build_v1_5_route_physical_recovery_readiness(
        root_cause_audit_path=audit,
        recovery_evidence_path=recovery,
    )

    assert model["manifest"]["status"] == "blocked"
    assert model["manifest"]["physical_recovery_passed"] is True
    assert model["manifest"]["fresh_canonical_queue_policy_passed"] is False
    assert any(row["requirement"] == "next_run_policy" for row in model["findings"])


def test_segmented_evidence_review_is_separate_from_new_clean_queue(tmp_path: Path) -> None:
    audit = _write_json(
        tmp_path / "root_cause.json",
        _audit_payload(
            categories={
                "direct_or_retry_point_without_queue_manifest": 2,
                "manual_parameter_or_execution_mode_change": 1,
            }
        ),
    )
    recovery = _write_json(
        tmp_path / "recovery.json",
        _recovery_payload(
            accepted_manifest_review={
                "status": "missing",
            }
        ),
    )

    model = build_v1_5_route_physical_recovery_readiness(
        root_cause_audit_path=audit,
        recovery_evidence_path=recovery,
    )

    assert model["manifest"]["status"] == "review_required"
    assert model["manifest"]["next_continuous_run_allowed"] is True
    assert model["manifest"]["segmented_evidence_fit_use_allowed"] is False
    assert any(row["requirement"] == "accepted_manifest_review" for row in model["findings"])


def test_writer_cli_and_entrypoint_classification(tmp_path: Path) -> None:
    audit = _write_json(
        tmp_path / "root_cause.json",
        _audit_payload(categories={"pressure_controller_vent_no_response": 1}),
    )
    outputs = write_v1_5_route_physical_recovery_readiness(
        root_cause_audit_path=audit,
        output_dir=tmp_path / "out",
    )
    model = json.loads(outputs["manifest"].read_text(encoding="utf-8"))
    markdown = outputs["markdown"].read_text(encoding="utf-8")
    entrypoint = classify_v1_5_entrypoint(
        Path("src/gas_calibrator/tools/export_v1_5_route_physical_recovery_readiness.py")
    )

    assert model["manifest"]["status"] == "blocked"
    assert "V1.5 Route Physical Recovery Readiness" in markdown
    assert cli_main(["--root-cause-audit-path", str(audit), "--output-dir", str(tmp_path / "cli")]) == 0
    assert (
        cli_main(
            [
                "--root-cause-audit-path",
                str(audit),
                "--output-dir",
                str(tmp_path / "cli_block"),
                "--fail-on-blocker",
            ]
        )
        == 2
    )
    assert entrypoint.category == "formal_review_evidence"
    assert entrypoint.risk_level == "offline"
