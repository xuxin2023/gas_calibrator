import csv
import hashlib
import json
from pathlib import Path

from gas_calibrator.tools.export_v1_5_legacy_evidence_gap_task_plan import main
from gas_calibrator.validation.v1_5_entrypoint_inventory import classify_v1_5_entrypoint
from gas_calibrator.validation.v1_5_legacy_evidence_gap_task_plan import (
    build_v1_5_legacy_evidence_gap_task_plan,
    write_v1_5_legacy_evidence_gap_task_plan,
)
from gas_calibrator.validation.v1_5_legacy_historical_evidence_catalog import SCHEMA


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _catalog(tmp_path: Path, *, lineage: str = "segmented_point_evidence") -> tuple[Path, Path]:
    point = tmp_path / "run" / "p001_T40_0ppm_fit"
    point.mkdir(parents=True)
    samples = point / "samples_machine_readable.csv"
    samples.write_text("label,ratio\nga01,0.5\n", encoding="utf-8")
    sidecar = point / "formal_open_flow_sidecar_metadata.json"
    sidecar.write_text(json.dumps({"run_id": point.name}), encoding="utf-8")
    frame = point / "frame_quality_summary.csv"
    frame.write_text("label,grade\nga01,A\n", encoding="utf-8")
    artifacts = {
        path.name: {
            "path": str(path),
            "size_bytes": path.stat().st_size,
            "sha256": _sha256(path),
        }
        for path in (samples, sidecar, frame)
    }
    payload = {
        "schema": SCHEMA,
        "overall_status": "catalog_complete_diagnostic_only",
        "point_count": 1,
        "points": [
            {
                "point_name": point.name,
                "point_dir": str(point),
                "route_kind": "co2",
                "root_classification": "segmented_v1_5_point_evidence",
                "lineage_classification": lineage,
                "accepted_manifest_member": False,
                "accepted_manifest_status": "",
                "accepted_manifest_warning": "",
                "has_sidecar": True,
                "has_samples": True,
                "has_component_qc": False,
                "has_frame_qc": True,
                "artifacts": artifacts,
            }
        ],
        "interpretation_contract": {
            "co2_zero_and_h2o_dry_anchor_are_interchangeable": False,
            "anchor_role_inference_allowed": False,
        },
        "opens_com_ports": False,
        "controls_pressure": False,
        "controls_water_or_gas_routes": False,
        "writes_coefficients": False,
        "writes_sn_or_device_code": False,
        "connects_postgresql": False,
        "continuous_route_attestation_allowed": False,
        "historical_fit_allowed": False,
        "formal_release_allowed": False,
        "database_import_allowed": False,
        "not_real_acceptance_evidence": True,
    }
    catalog = tmp_path / "catalog.json"
    catalog.write_text(json.dumps(payload), encoding="utf-8")
    return catalog, samples


def test_missing_component_qc_generates_manual_same_point_task(tmp_path: Path) -> None:
    catalog, _ = _catalog(tmp_path)
    model = build_v1_5_legacy_evidence_gap_task_plan(catalog_json_path=catalog)
    task = model["tasks"][0]
    assert model["overall_status"] == "review_required_manual_offline_evidence_tasks"
    assert model["artifact_integrity_mismatch_count"] == 0
    assert task["priority"] == "P2_quality_traceability"
    assert "component_qc_missing" in task["gap_codes"]
    assert "review_same_point_samples_and_frame_qc_for_component_qc_backfill" in task["recommended_actions"]
    assert task["cross_run_qc_direct_bind_allowed"] is False
    assert task["automatic_qc_derivation_allowed"] is False
    assert task["formal_fit_allowed"] is False


def test_artifact_drift_is_p0_and_blocks_plan_status(tmp_path: Path) -> None:
    catalog, samples = _catalog(tmp_path)
    samples.write_text("label,ratio\nga01,changed\n", encoding="utf-8")
    model = build_v1_5_legacy_evidence_gap_task_plan(catalog_json_path=catalog)
    task = model["tasks"][0]
    assert model["overall_status"] == "blocked_cataloged_artifact_integrity_mismatch"
    assert model["artifact_integrity_mismatch_count"] == 1
    assert task["priority"] == "P0_integrity"
    assert set(task["gap_codes"]) & {
        "artifact_size_mismatch_since_catalog",
        "artifact_hash_mismatch_since_catalog",
    }
    assert task["offline_evidence_recovery_review_allowed"] is True


def test_0624_source_is_retain_only_and_never_repair_promoted(tmp_path: Path) -> None:
    catalog, _ = _catalog(tmp_path, lineage="forbidden_0624_or_migration")
    model = build_v1_5_legacy_evidence_gap_task_plan(catalog_json_path=catalog)
    task = model["tasks"][0]
    assert task["task_status"] == "forbidden_source_retain_diagnostic_only"
    assert task["priority"] == "P3_forbidden_reference"
    assert task["offline_evidence_recovery_review_allowed"] is False
    assert "retain_for_diagnostic_reference_only_never_promote" in task["recommended_actions"]
    assert not any("component_qc" in action for action in task["recommended_actions"])
    assert task["continuous_route_attestation_allowed"] is False


def test_forbidden_source_artifact_drift_is_still_p0_integrity(tmp_path: Path) -> None:
    catalog, samples = _catalog(tmp_path, lineage="forbidden_0624_or_migration")
    samples.write_text("label,ratio\nga01,drifted\n", encoding="utf-8")
    model = build_v1_5_legacy_evidence_gap_task_plan(catalog_json_path=catalog)
    task = model["tasks"][0]
    assert model["overall_status"] == "blocked_cataloged_artifact_integrity_mismatch"
    assert task["priority"] == "P0_integrity"
    assert task["task_status"] == "artifact_integrity_blocker_manual_review_required"
    assert task["offline_evidence_recovery_review_allowed"] is False


def test_same_root_accepted_alternative_makes_old_attempt_reference_only(tmp_path: Path) -> None:
    catalog, _ = _catalog(tmp_path)
    payload = json.loads(catalog.read_text(encoding="utf-8"))
    original = payload["points"][0]
    original["root_path"] = str(tmp_path / "root")
    accepted = dict(original)
    accepted["point_name"] = "p009_T40_0ppm_fit_retry1"
    accepted["point_dir"] = str(tmp_path / "root" / accepted["point_name"])
    accepted["accepted_manifest_member"] = True
    accepted["lineage_classification"] = "segmented_retry_or_recovery"
    payload["points"].append(accepted)
    payload["point_count"] = 2
    catalog.write_text(json.dumps(payload), encoding="utf-8")

    model = build_v1_5_legacy_evidence_gap_task_plan(catalog_json_path=catalog)
    original_task = next(row for row in model["tasks"] if row["point_name"] == original["point_name"])
    assert original_task["priority"] == "P3_superseded_reference"
    assert original_task["task_status"] == "superseded_attempt_retain_reference_only"
    assert original_task["offline_evidence_recovery_review_allowed"] is False
    assert original_task["cross_run_qc_direct_bind_allowed"] is False
    assert not any("component_qc" in action for action in original_task["recommended_actions"])


def test_invalid_catalog_locks_fail_closed_without_tasks(tmp_path: Path) -> None:
    catalog, _ = _catalog(tmp_path)
    payload = json.loads(catalog.read_text(encoding="utf-8"))
    payload["historical_fit_allowed"] = True
    catalog.write_text(json.dumps(payload), encoding="utf-8")
    model = build_v1_5_legacy_evidence_gap_task_plan(catalog_json_path=catalog)
    assert model["overall_status"] == "blocked_invalid_catalog"
    assert model["task_count"] == 0
    assert "catalog_historical_fit_allowed_not_false" in model["catalog_blocker_codes"]
    assert model["historical_fit_allowed"] is False


def test_writer_cli_and_entrypoint_remain_offline(tmp_path: Path) -> None:
    catalog, _ = _catalog(tmp_path)
    model = build_v1_5_legacy_evidence_gap_task_plan(catalog_json_path=catalog)
    outputs = write_v1_5_legacy_evidence_gap_task_plan(model, tmp_path / "direct")
    rc = main(
        [
            "--catalog-json-path",
            str(catalog),
            "--output-dir",
            str(tmp_path / "cli"),
            "--fail-on-integrity-mismatch",
        ]
    )
    entry = classify_v1_5_entrypoint(
        Path("src/gas_calibrator/tools/export_v1_5_legacy_evidence_gap_task_plan.py"),
        root=Path.cwd(),
    )
    assert rc == 0
    assert outputs["tasks_csv"].is_file()
    assert outputs["integrity_csv"].is_file()
    saved = json.loads(outputs["json"].read_text(encoding="utf-8"))
    assert "artifact_integrity_rows" not in saved
    assert saved["artifact_integrity_rows_omitted_from_json"] is True
    assert saved["artifact_integrity_csv"] == outputs["integrity_csv"].name
    assert entry.category == "formal_review_evidence"
    assert entry.risk_level == "offline"
    assert entry.opens_com_ports is False
    assert entry.controls_routes is False
    assert entry.writes_coefficients is False


def test_summary_csv_is_machine_readable(tmp_path: Path) -> None:
    catalog, _ = _catalog(tmp_path)
    model = build_v1_5_legacy_evidence_gap_task_plan(catalog_json_path=catalog)
    outputs = write_v1_5_legacy_evidence_gap_task_plan(model, tmp_path / "out")
    with outputs["summary_csv"].open("r", encoding="utf-8-sig", newline="") as handle:
        rows = list(csv.DictReader(handle))
    assert any(row["metric"] == "gap:component_qc_missing" for row in rows)
