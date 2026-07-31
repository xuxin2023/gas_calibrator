import csv
import json

import pytest

from gas_calibrator.tools.run_v1_5_formal_archive_closure import main as closure_main
from gas_calibrator.v1_5.orchestration.operator_workstation import (
    load_v1_5_decision_authorities,
)
from gas_calibrator.validation.v1_5_canonical_evidence import write_canonical_v1_5_evidence_package
from gas_calibrator.validation.v1_5_formal_archive_closure import build_v1_5_formal_archive_closure
from gas_calibrator.validation.v1_5_formal_database_import_evidence_bundle import (
    validate_v1_5_formal_database_import_evidence_bundle,
)
from gas_calibrator.validation.v1_5_senco_artifact_authorization import (
    write_senco_artifact_authorization,
)


def _write_contract(path):
    payload = {
        "schema": "v1_5_formal_flow_contract_v1",
        "status": "pass",
        "physical_boundaries": {
            "offline_audit_only": True,
            "opens_com_ports": False,
            "controls_water_or_gas_routes": False,
            "controls_valves_or_pace": False,
            "writes_coefficients": False,
            "not_real_acceptance_evidence": True,
        },
    }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def _write_identity_getco_readiness(run_dir, *, review_required=False):
    path = run_dir / "coefficient_epoch_0_getco_snapshot" / "v1_5_getco_identity_readiness.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "schema": "v1_5_getco_identity_readiness_v1",
        "overall_status": "identity_getco_ready_for_auxiliary_neutralization",
        "active_analyzer_count": 1,
        "analyzer_device_ids": ["001"],
        "traceability_review_required": review_required,
        "checks": [
            {
                "check": "sn_device_code_traceability_preserved",
                "status": "review_required" if review_required else "ready",
                "reasons": ["COM35_runtime_sn_code_missing"] if review_required else [],
            }
        ],
    }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def _write_s13_controlled_write_evidence(run_dir, *, authorization_id="AUTH-MISSING"):
    output = run_dir / "controlled_write" / "s13"
    output.mkdir(parents=True, exist_ok=True)
    (output / "co2_senco13_pair_write_meta.json").write_text(
        json.dumps(
            {
                "config_summary": {
                    "reviewer": "reviewer-a",
                    "approver": "approver-b",
                    "artifact_hash_status": "pass",
                    "artifact_authorization_status": "pass",
                    "artifact_authorization_id": authorization_id,
                }
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    with (output / "co2_senco13_pair_write_summary.csv").open(
        "w", encoding="utf-8-sig", newline=""
    ) as handle:
        writer = csv.DictWriter(handle, fieldnames=["analyzer_device_id", "status"])
        writer.writeheader()
        writer.writerow({"analyzer_device_id": "001", "status": "written_readback_verified"})


def _write_s13_authorization(run_dir, *, authorization_id="AUTH-ARCHIVE-001"):
    precheck = run_dir / "main_senco_write_precheck"
    precheck.mkdir(parents=True, exist_ok=True)
    manifest = precheck / "main_senco_artifact_hash_manifest.json"
    manifest.write_text(json.dumps({"files": []}, ensure_ascii=False), encoding="utf-8")
    authorization = precheck / "main_senco_artifact_authorization.json"
    write_senco_artifact_authorization(
        authorization,
        manifest_path=manifest,
        reviewer="reviewer-a",
        approver="approver-b",
        authorization_id=authorization_id,
        authorized_writer_scopes=["co2_senco13_pair"],
        authorized_device_ids=["001"],
    )
    return authorization, manifest


def _reviewed_standard_gases():
    return {
        "standard_gases": [
            {
                "component": "co2",
                "cylinder_id": "CO2-900",
                "certificate_id": "CO2-CERT-REVIEWED",
                "certificate_value": 900.0,
                "certificate_uncertainty": 0.9,
                "valid_until": "2027-01-01",
                "supplier": "reviewed-standard-lab",
                "certificate_hash": "reviewed-co2-cert-hash",
            },
            {
                "component": "h2o",
                "cylinder_id": "H2O-GEN-REVIEWED",
                "certificate_id": "H2O-CERT-REVIEWED",
                "certificate_value": 0.5,
                "certificate_uncertainty": 0.01,
                "valid_until": "2027-01-01",
                "supplier": "reviewed-standard-lab",
                "certificate_hash": "reviewed-h2o-cert-hash",
            },
        ],
    }


def test_formal_archive_closure_generates_reports_bundle_traceability_and_dry_run_db(tmp_path):
    canonical = write_canonical_v1_5_evidence_package(
        tmp_path / "canonical",
        include_reports=False,
    )
    run_dir = canonical["root"] / "run"
    closure_dir = run_dir / "formal_archive_closure"
    contract_path = _write_contract(run_dir / "v1_5_formal_flow_contract.json")
    identity_getco_path = _write_identity_getco_readiness(run_dir)

    result = build_v1_5_formal_archive_closure(
        run_dir=run_dir,
        plan_json=canonical["plan"],
        pressure_reference_json=canonical["pressure_reference"],
        contract_json=contract_path,
        output_dir=closure_dir,
        today="2026-05-24",
        report_no="RPT-CLOSURE-001",
        reviewer="reviewer-a",
        approver="approver-a",
        location="lab-a",
        calibration_date="2026-05-24",
        db_mode="dry_run",
    )

    index = result["index"]
    paths = result["paths"]
    assert index["schema"] == "v1_5_formal_archive_closure_v1"
    assert index["physical_boundaries"]["opens_com_ports"] is False
    assert index["physical_boundaries"]["controls_water_or_gas_routes"] is False
    assert index["physical_boundaries"]["controls_valves_or_pace"] is False
    assert index["physical_boundaries"]["writes_coefficients"] is False
    assert index["database"]["mode"] == "dry_run"
    assert index["database"]["database_imported"] is False
    assert index["calibration_capability"]["method_backbone_ready"] is True
    assert index["calibration_capability"]["formal_release_ready"] is False
    assert index["traceability_checks"]["has_raw_samples"] is True
    assert index["traceability_checks"]["has_run_evidence_status"] is True
    assert index["traceability_checks"]["has_formal_run_status"] is True
    assert index["traceability_checks"]["has_water_route_traceability"] is True
    assert index["traceability_checks"]["identity_getco_sn_device_code_traceability_ready"] is True
    assert index["traceability_checks"]["senco_authorization_write_traceability_ready"] is True
    assert index["identity_getco_traceability"]["status"] == "ready"
    assert index["identity_getco_traceability"]["evidence_path"] == str(identity_getco_path.resolve())
    assert (
        index["senco_authorization_write_traceability"]["overall_status"]
        == "not_applicable_no_main_senco_write_evidence"
    )
    assert index["formal_run_status"]["overall_status"] in {"in_progress", "review_required", "formal_release_ready"}
    assert isinstance(index["formal_run_status"]["can_continue_physical_flow"], bool)
    assert index["formal_run_status"]["formal_release_allowed"] is False
    assert index["formal_run_status"]["database_import_allowed"] is False

    final_bundle = json.loads(paths["evidence_bundle"].read_text(encoding="utf-8"))
    bundle_ready, bundle_reasons, bundle_details = (
        validate_v1_5_formal_database_import_evidence_bundle(final_bundle)
    )
    assert bundle_ready is True, bundle_reasons
    assert bundle_details["schema"] == "v1_5_evidence_registry"
    assert bundle_details["missing_artifact_roles"] == []
    report_types = {row["report_type"] for row in final_bundle["tables"]["reports"]}
    assert {"run_report", "technical_report", "formal_calibration_report", "report_model"}.issubset(
        report_types
    )
    report_roles = {
        row["artifact_role"]
        for row in final_bundle["tables"]["sample_files"]
        if str(row.get("path") or "").endswith((".md", ".docx", ".pdf", ".json"))
    }
    all_artifact_roles = {row["artifact_role"] for row in final_bundle["tables"]["sample_files"]}
    assert {"run_report", "technical_report", "formal_calibration_report", "report_model"}.issubset(
        report_roles
    )
    assert {"run_evidence_status", "calibration_capability", "calibration_capability_report"}.issubset(
        report_roles
    )
    assert {"formal_run_status", "formal_run_status_report"}.issubset(report_roles)
    assert {"formal_run_status_gates", "formal_run_status_gaps"}.issubset(all_artifact_roles)

    traceability = json.loads(paths["traceability_summary"].read_text(encoding="utf-8"))
    assert traceability["traceability_checks"]["all_required_artifacts_have_sha256"] is True
    assert traceability["traceability_checks"]["has_formal_run_status"] is True
    assert any(row["report_type"] == "formal_calibration_report" for row in traceability["reports"])
    assert paths["archive_index_json"].exists()
    assert paths["archive_index_markdown"].exists()
    assert paths["calibration_capability_json"].exists()
    assert paths["calibration_capability_markdown"].exists()
    assert paths["formal_run_status_json"].exists()
    assert paths["formal_run_status_markdown"].exists()
    assert paths["formal_run_status_gates"].exists()
    assert paths["formal_run_status_gaps"].exists()
    assert paths["senco_authorization_write_traceability_json"].exists()
    assert paths["senco_authorization_write_traceability_csv"].exists()
    assert paths["senco_authorization_write_traceability_markdown"].exists()
    capability = json.loads(paths["calibration_capability_json"].read_text(encoding="utf-8"))
    assert capability["physical_boundaries"]["opens_com_ports"] is False
    index_text = paths["archive_index_markdown"].read_text(encoding="utf-8")
    formal_report_text = paths["report_formal_calibration_report_markdown"].read_text(encoding="utf-8")
    assert "V1.5 正式归档闭环索引" in index_text
    assert "正式运行状态" in index_text
    assert "formal_release_allowed" in index_text
    assert "正式运行状态" in formal_report_text
    assert "允许正式放行：否" in formal_report_text
    assert "控制气路/水路" not in index_text
    assert "controls_water_or_gas_routes" in index_text


def test_formal_archive_closure_blocks_release_when_write_evidence_lacks_authorization(tmp_path):
    canonical = write_canonical_v1_5_evidence_package(
        tmp_path / "canonical_senco_binding_blocked",
        include_reports=False,
    )
    run_dir = canonical["root"] / "run"
    contract_path = _write_contract(run_dir / "v1_5_formal_flow_contract.json")
    _write_identity_getco_readiness(run_dir)
    _write_s13_controlled_write_evidence(run_dir)

    result = build_v1_5_formal_archive_closure(
        run_dir=run_dir,
        plan_json=canonical["plan"],
        pressure_reference_json=canonical["pressure_reference"],
        contract_json=contract_path,
        output_dir=run_dir / "formal_archive_closure_senco_binding_blocked",
        today="2026-05-24",
        report_no="RPT-CLOSURE-SENCO-BLOCKED",
        db_mode="dry_run",
    )

    index = result["index"]
    binding = index["senco_authorization_write_traceability"]
    assert binding["overall_status"] == "blocked"
    assert binding["ready_for_archive_release"] is False
    assert index["traceability_checks"]["senco_authorization_write_traceability_ready"] is False
    formal_status = json.loads(result["paths"]["formal_run_status_json"].read_text(encoding="utf-8"))
    archive_gate = next(
        gate for gate in formal_status["gates"] if gate["gate_id"] == "formal_archive_database_release"
    )
    assert archive_gate["status"] == "blocked"
    assert formal_status["formal_release_allowed"] is False
    assert formal_status["database_import_allowed"] is False


def test_formal_archive_closure_indexes_authorization_manifest_and_write_readback_sources(tmp_path):
    canonical = write_canonical_v1_5_evidence_package(
        tmp_path / "canonical_senco_binding_ready",
        include_reports=False,
    )
    run_dir = canonical["root"] / "run"
    contract_path = _write_contract(run_dir / "v1_5_formal_flow_contract.json")
    _write_identity_getco_readiness(run_dir)
    authorization, manifest = _write_s13_authorization(run_dir)
    _write_s13_controlled_write_evidence(run_dir, authorization_id="AUTH-ARCHIVE-001")

    result = build_v1_5_formal_archive_closure(
        run_dir=run_dir,
        plan_json=canonical["plan"],
        pressure_reference_json=canonical["pressure_reference"],
        contract_json=contract_path,
        output_dir=run_dir / "formal_archive_closure_senco_binding_ready",
        senco_artifact_authorization_json=authorization,
        today="2026-05-24",
        report_no="RPT-CLOSURE-SENCO-READY",
        db_mode="dry_run",
    )

    index = result["index"]
    binding = index["senco_authorization_write_traceability"]
    assert binding["overall_status"] == "ready_for_archive_release"
    assert binding["authorization_path"] == str(authorization.resolve())
    assert binding["manifest_path"] == str(manifest.resolve())
    assert len(binding["authorization_sha256"]) == 64
    assert len(binding["manifest_sha256"]) == 64
    artifact_roles = {row["role"] for row in index["artifacts"]}
    assert "senco_artifact_authorization" in artifact_roles
    assert "senco_artifact_hash_manifest" in artifact_roles
    assert "senco_write_001_co2_senco13_pair_metadata" in artifact_roles
    assert "senco_write_001_co2_senco13_pair_readback_rows" in artifact_roles
    evidence_bundle = json.loads(
        result["paths"]["evidence_bundle"].read_text(encoding="utf-8")
    )
    run_row = evidence_bundle["tables"]["runs"][0]
    loaded = load_v1_5_decision_authorities(
        result["paths"]["archive_index_json"],
        expected_run_id=index["run_id"],
        expected_device_ids="001",
        expected_runtime_config_sha256=run_row["config_hash"],
    )
    assert loaded["status"] == "ready", loaded["blockers"]
    assert loaded["identity_binding"]["status"] == "ready"
    assert all(loaded["identity_binding"]["checks"].values())


def test_formal_archive_closure_refuses_database_import_when_senco_binding_is_blocked(tmp_path):
    canonical = write_canonical_v1_5_evidence_package(
        tmp_path / "canonical_senco_binding_import_blocked",
        include_reports=False,
    )
    run_dir = canonical["root"] / "run"
    contract_path = _write_contract(run_dir / "v1_5_formal_flow_contract.json")
    _write_identity_getco_readiness(run_dir)
    _write_s13_controlled_write_evidence(run_dir)

    with pytest.raises(ValueError, match="authorization/readback traceability"):
        build_v1_5_formal_archive_closure(
            run_dir=run_dir,
            plan_json=canonical["plan"],
            pressure_reference_json=canonical["pressure_reference"],
            contract_json=contract_path,
            output_dir=run_dir / "formal_archive_closure_senco_import_blocked",
            today="2026-05-24",
            report_no="RPT-CLOSURE-SENCO-IMPORT-BLOCKED",
            db_mode="import",
            dsn="postgresql://user:password@localhost:5432/gas_calibrator",
        )


def test_formal_archive_closure_marks_sn_traceability_review_before_release(tmp_path):
    canonical = write_canonical_v1_5_evidence_package(
        tmp_path / "canonical_sn_review",
        include_reports=False,
    )
    run_dir = canonical["root"] / "run"
    closure_dir = run_dir / "formal_archive_closure_sn_review"
    contract_path = _write_contract(run_dir / "v1_5_formal_flow_contract.json")
    _write_identity_getco_readiness(run_dir, review_required=True)

    result = build_v1_5_formal_archive_closure(
        run_dir=run_dir,
        plan_json=canonical["plan"],
        pressure_reference_json=canonical["pressure_reference"],
        contract_json=contract_path,
        output_dir=closure_dir,
        today="2026-05-24",
        report_no="RPT-CLOSURE-SN-REVIEW",
        db_mode="dry_run",
    )

    index = result["index"]
    assert index["identity_getco_traceability"]["status"] == "review_required"
    assert index["identity_getco_traceability"]["traceability_review_required"] is True
    assert index["traceability_checks"]["identity_getco_sn_device_code_traceability_ready"] is False
    assert "COM35_runtime_sn_code_missing" in index["identity_getco_traceability"]["reasons"]


def test_formal_archive_closure_refuses_database_import_when_sn_traceability_needs_review(tmp_path):
    canonical = write_canonical_v1_5_evidence_package(
        tmp_path / "canonical_sn_review_import",
        include_reports=False,
    )
    run_dir = canonical["root"] / "run"
    contract_path = _write_contract(run_dir / "v1_5_formal_flow_contract.json")
    _write_identity_getco_readiness(run_dir, review_required=True)

    with pytest.raises(ValueError, match="SN/device_code traceability"):
        build_v1_5_formal_archive_closure(
            run_dir=run_dir,
            plan_json=canonical["plan"],
            pressure_reference_json=canonical["pressure_reference"],
            contract_json=contract_path,
            output_dir=run_dir / "formal_archive_closure_sn_import_blocked",
            today="2026-05-24",
            report_no="RPT-CLOSURE-SN-IMPORT",
            db_mode="import",
            dsn="postgresql://user:password@localhost:5432/gas_calibrator",
        )


def test_formal_archive_closure_can_bind_reviewed_standard_gas_snapshot(tmp_path):
    canonical = write_canonical_v1_5_evidence_package(
        tmp_path / "canonical_reviewed_gases",
        include_reports=False,
    )
    run_dir = canonical["root"] / "run"
    closure_dir = run_dir / "formal_archive_closure_reviewed_gases"
    contract_path = _write_contract(run_dir / "v1_5_formal_flow_contract.json")
    source_plan = json.loads(canonical["plan"].read_text(encoding="utf-8"))
    source_plan.pop("standard_gases", None)
    plan_without_gases = run_dir / "formal_plan_without_standard_gases.json"
    plan_without_gases.write_text(json.dumps(source_plan, ensure_ascii=False, indent=2), encoding="utf-8")
    gases_path = run_dir / "standard_gases_reviewed.json"
    gases_path.write_text(json.dumps(_reviewed_standard_gases(), ensure_ascii=False), encoding="utf-8")

    result = build_v1_5_formal_archive_closure(
        run_dir=run_dir,
        plan_json=plan_without_gases,
        pressure_reference_json=canonical["pressure_reference"],
        standard_gases_json=gases_path,
        contract_json=contract_path,
        output_dir=closure_dir,
        today="2026-05-24",
        report_no="RPT-CLOSURE-GASES",
        db_mode="dry_run",
    )

    index = result["index"]
    paths = result["paths"]
    traceability = json.loads(paths["traceability_summary"].read_text(encoding="utf-8"))
    final_bundle = json.loads(paths["evidence_bundle"].read_text(encoding="utf-8"))
    gases = {row["component"]: row for row in final_bundle["tables"]["standard_gases"]}

    assert index["source_plan_json"] == str(plan_without_gases.resolve())
    assert index["standard_gases_json"] == str(gases_path.resolve())
    assert paths["standard_gases_reviewed_snapshot"].exists()
    assert paths["formal_plan_with_standard_gases"].exists()
    assert traceability["traceability_checks"]["has_standard_gas_traceability"] is True
    assert traceability["traceability_checks"]["has_water_route_traceability"] is True
    assert gases["co2"]["certificate_hash"] == "reviewed-co2-cert-hash"
    assert gases["h2o"]["cylinder_id"] == "H2O-GEN-REVIEWED"


def test_formal_archive_closure_cli_keeps_output_inside_run_dir(tmp_path):
    canonical = write_canonical_v1_5_evidence_package(
        tmp_path / "canonical_cli",
        include_reports=False,
    )
    run_dir = canonical["root"] / "run"
    closure_dir = run_dir / "formal_archive_closure_cli"
    contract_path = _write_contract(run_dir / "v1_5_formal_flow_contract.json")

    rc = closure_main(
        [
            "--run-dir",
            str(run_dir),
            "--plan-json",
            str(canonical["plan"]),
            "--pressure-reference-json",
            str(canonical["pressure_reference"]),
            "--contract-json",
            str(contract_path),
            "--output-dir",
            str(closure_dir),
            "--today",
            "2026-05-24",
            "--db-mode",
            "dry-run",
        ]
    )

    assert rc == 0
    index_path = closure_dir / "v1_5_formal_archive_closure_index.json"
    bundle_path = closure_dir / "evidence_bundle.json"
    assert index_path.exists()
    assert bundle_path.exists()
    index = json.loads(index_path.read_text(encoding="utf-8"))
    assert index["database"]["database_imported"] is False
    assert index["reports"]["formal_calibration_report_markdown"].endswith(
        "formal_calibration_report.md"
    )
    assert index["calibration_capability"]["method_backbone_ready"] is True


def test_formal_archive_closure_blocks_too_long_per_device_certificate_paths(tmp_path):
    canonical = write_canonical_v1_5_evidence_package(
        tmp_path / "canonical_long_path",
        include_reports=False,
    )
    run_dir = canonical["root"] / "run"
    contract_path = _write_contract(run_dir / "v1_5_formal_flow_contract.json")
    closure_dir = run_dir / ("formal_archive_closure_" + ("x" * 220))

    with pytest.raises(ValueError, match="output_dir path is too long"):
        build_v1_5_formal_archive_closure(
            run_dir=run_dir,
            plan_json=canonical["plan"],
            pressure_reference_json=canonical["pressure_reference"],
            contract_json=contract_path,
            output_dir=closure_dir,
            today="2026-05-24",
            db_mode="dry_run",
        )


def test_formal_archive_closure_excludes_previous_archive_closure_dirs(tmp_path):
    canonical = write_canonical_v1_5_evidence_package(
        tmp_path / "canonical_repeat",
        include_reports=False,
    )
    run_dir = canonical["root"] / "run"
    contract_path = _write_contract(run_dir / "v1_5_formal_flow_contract.json")

    first_dir = run_dir / "archive_first"
    build_v1_5_formal_archive_closure(
        run_dir=run_dir,
        plan_json=canonical["plan"],
        pressure_reference_json=canonical["pressure_reference"],
        contract_json=contract_path,
        output_dir=first_dir,
        today="2026-05-24",
        report_no="RPT-CLOSURE-FIRST",
        db_mode="dry_run",
    )

    second_dir = run_dir / "archive_second"
    result = build_v1_5_formal_archive_closure(
        run_dir=run_dir,
        plan_json=canonical["plan"],
        pressure_reference_json=canonical["pressure_reference"],
        contract_json=contract_path,
        output_dir=second_dir,
        today="2026-05-24",
        report_no="RPT-CLOSURE-SECOND",
        db_mode="dry_run",
    )

    final_bundle = json.loads(result["paths"]["evidence_bundle"].read_text(encoding="utf-8"))
    indexed_paths = [
        str(row.get("path") or "")
        for table in ("sample_files", "reports")
        for row in final_bundle["tables"][table]
    ]
    assert not any(str(first_dir) in path for path in indexed_paths)
    assert any(str(second_dir / "reports") in path for path in indexed_paths)
