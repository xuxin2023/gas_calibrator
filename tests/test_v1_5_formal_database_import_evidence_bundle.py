from copy import deepcopy
from pathlib import Path

from gas_calibrator.validation.v1_5_formal_database_import_evidence_bundle import (
    validate_v1_5_formal_database_import_evidence_bundle,
)


def _valid_bundle(tmp_path: Path) -> dict:
    run_id = "formal-run-001"
    run_db_id = "run-db-001"
    roles = (
        "raw_samples",
        "formal_plan_snapshot",
        "pressure_reference_snapshot",
        "pressure_channel_completion",
        "run_evidence_status",
        "formal_run_status",
        "formal_calibration_report",
    )
    sample_files = [
        {
            "id": f"artifact-{index}",
            "artifact_role": role,
            "path": str((tmp_path / "evidence" / f"{role}.json").resolve()),
            "sha256": f"{index + 1:x}" * 64,
            "required": index < 4,
        }
        for index, role in enumerate(roles)
    ]
    return {
        "schema": "v1_5_evidence_registry",
        "schema_version": "001",
        "created_at": "2026-07-11T00:00:00+00:00",
        "run_id": run_id,
        "run_db_id": run_db_id,
        "tables": {
            "runs": [
                {
                    "id": run_db_id,
                    "run_id": run_id,
                    "evidence_status": "ready_for_reviewer",
                    "package_status": "ready_for_reviewer",
                }
            ],
            "devices": [{"id": "device-001"}],
            "run_devices": [{"run_db_id": run_db_id, "device_id": "device-001"}],
            "standard_gases": [{"id": "gas-001"}],
            "reference_certificates": [{"id": "reference-001"}],
            "calibration_points": [{"id": "point-001"}],
            "sample_files": sample_files,
            "qc_results": [{"id": "qc-001", "status": "pass"}],
            "coefficient_snapshots": [],
            "coefficient_candidates": [],
            "coefficient_write_events": [],
            "reports": [{"id": "report-001"}],
            "audit_events": [{"id": "audit-001"}],
            "evidence_integrity_checks": [
                {"check_name": "required_artifacts_hashed", "status": "pass", "severity": "error"}
            ],
        },
    }


def test_database_import_evidence_bundle_accepts_registry_schema_and_required_roles(
    tmp_path: Path,
) -> None:
    ready, reasons, details = validate_v1_5_formal_database_import_evidence_bundle(
        _valid_bundle(tmp_path)
    )

    assert ready is True
    assert reasons == []
    assert details["schema"] == "v1_5_evidence_registry"
    assert details["schema_version"] == "001"
    assert details["missing_artifact_roles"] == []
    assert details["role_group_matches"]["pressure_channel_evidence"] == [
        "pressure_channel_completion"
    ]


def test_database_import_evidence_bundle_rejects_missing_table_and_role(tmp_path: Path) -> None:
    payload = deepcopy(_valid_bundle(tmp_path))
    del payload["tables"]["qc_results"]
    payload["tables"]["sample_files"] = [
        row
        for row in payload["tables"]["sample_files"]
        if row["artifact_role"] != "formal_run_status"
    ]

    ready, reasons, _details = validate_v1_5_formal_database_import_evidence_bundle(payload)

    assert ready is False
    assert "evidence_bundle_table_missing:qc_results" in reasons
    assert "evidence_bundle_required_artifact_role_missing:formal_run_status" in reasons


def test_database_import_evidence_bundle_rejects_malformed_required_artifact(tmp_path: Path) -> None:
    payload = deepcopy(_valid_bundle(tmp_path))
    raw = next(
        row for row in payload["tables"]["sample_files"] if row["artifact_role"] == "raw_samples"
    )
    raw["sha256"] = "not-a-sha"

    ready, reasons, _details = validate_v1_5_formal_database_import_evidence_bundle(payload)

    assert ready is False
    assert "artifact_role:raw_samples:0_sha256_invalid" in reasons
    assert "evidence_bundle_required_artifact_role_invalid:raw_samples" in reasons


def test_database_import_evidence_bundle_rejects_run_identity_mismatch(tmp_path: Path) -> None:
    payload = deepcopy(_valid_bundle(tmp_path))
    payload["tables"]["runs"][0]["run_id"] = "different-run"

    ready, reasons, _details = validate_v1_5_formal_database_import_evidence_bundle(payload)

    assert ready is False
    assert "evidence_bundle_run_row_run_id_mismatch" in reasons


def test_database_import_evidence_bundle_rejects_failed_error_integrity_check(tmp_path: Path) -> None:
    payload = deepcopy(_valid_bundle(tmp_path))
    payload["tables"]["evidence_integrity_checks"][0]["status"] = "fail"

    ready, reasons, details = validate_v1_5_formal_database_import_evidence_bundle(payload)

    assert ready is False
    assert "evidence_bundle_integrity_check_not_pass:required_artifacts_hashed" in reasons
    assert details["failing_integrity_checks"] == ["required_artifacts_hashed"]
