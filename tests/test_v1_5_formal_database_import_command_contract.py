import json
from pathlib import Path

from gas_calibrator.tools.export_v1_5_formal_database_import_command_contract import main as cli_main
from gas_calibrator.validation.v1_5_artifact_hash_binding import sha256_file
from gas_calibrator.validation.v1_5_formal_database_dry_run import (
    build_v1_5_formal_database_dry_run_contract,
    write_v1_5_formal_database_dry_run_outputs,
)
from gas_calibrator.validation.v1_5_formal_database_import_authorization import (
    build_v1_5_formal_database_import_authorization,
    write_v1_5_formal_database_import_authorization_outputs,
)
from gas_calibrator.validation.v1_5_formal_database_import_command_contract import (
    build_v1_5_formal_database_import_command_contract,
    write_v1_5_formal_database_import_command_contract_outputs,
)
from gas_calibrator.validation.v1_5_formal_database_import_preflight import (
    build_v1_5_formal_database_import_preflight,
    write_v1_5_formal_database_import_preflight_outputs,
)


def _write_json(path: Path, payload: dict) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def _preflight_json(tmp_path: Path, *, dsn: str = "postgresql://user:secret@localhost:5432/v15") -> Path:
    dry_run_model = build_v1_5_formal_database_dry_run_contract()
    dry_run_outputs = write_v1_5_formal_database_dry_run_outputs(dry_run_model, tmp_path / "dry_run")
    preflight_model = build_v1_5_formal_database_import_preflight(
        formal_database_dry_run_json=dry_run_outputs["json"],
        dsn=dsn,
    )
    preflight_outputs = write_v1_5_formal_database_import_preflight_outputs(
        preflight_model,
        tmp_path / "preflight",
    )
    return preflight_outputs["json"]


def _archive_json(tmp_path: Path, *, ready: bool = True) -> Path:
    binding = {
        "schema": "v1_5_senco_authorization_archive_binding_v1",
        "overall_status": "not_applicable_no_main_senco_write_evidence",
        "ready_for_archive_release": True,
        "write_evidence_present": False,
        "authorization_path": "",
        "authorization_sha256": "",
        "manifest_path": "",
        "manifest_sha256": "",
        "writer_evidence": [],
    }
    binding_path = _write_json(
        tmp_path / "archive" / "binding" / "v1_5_senco_authorization_archive_binding.json",
        binding,
    )
    payload = {
        "schema": "v1_5_formal_archive_closure_v1",
        "overall_status": "ready" if ready else "review_required",
        "package_status": "ready" if ready else "review_required",
        "identity_getco_traceability": {
            "status": "ready" if ready else "review_required",
            "ready_for_archive_release": ready,
            "traceability_review_required": not ready,
        },
        "senco_authorization_write_traceability": binding,
        "artifacts": [
            {
                "role": "senco_authorization_write_traceability_json",
                "path": str(binding_path.resolve()),
                "sha256": sha256_file(binding_path),
            }
        ],
    }
    return _write_json(tmp_path / "archive" / "v1_5_formal_archive_closure_index.json", payload)


def _evidence_bundle_json(tmp_path: Path) -> Path:
    payload = {
        "schema": "v1_5_formal_evidence_bundle_v1",
        "tables": {"devices": [], "samples": [], "reports": []},
    }
    return _write_json(tmp_path / "archive" / "evidence_bundle.json", payload)


def _authorization_json(tmp_path: Path) -> tuple[Path, Path, Path, Path]:
    preflight_json = _preflight_json(tmp_path)
    archive_json = _archive_json(tmp_path)
    bundle_json = _evidence_bundle_json(tmp_path)
    model = build_v1_5_formal_database_import_authorization(
        formal_database_import_preflight_json=preflight_json,
        archive_closure_json=archive_json,
        operator="operator-a",
        reviewer="reviewer-a",
        approver="approver-b",
        authorization_id="db-import-001",
    )
    outputs = write_v1_5_formal_database_import_authorization_outputs(model, tmp_path / "authorization")
    return outputs["json"], preflight_json, archive_json, bundle_json


def _check(model: dict, name: str) -> dict:
    return {row["check"]: row for row in model["checks"]}[name]


def test_formal_database_import_command_contract_ready_without_connecting(tmp_path: Path) -> None:
    authorization_json, preflight_json, archive_json, bundle_json = _authorization_json(tmp_path)

    model = build_v1_5_formal_database_import_command_contract(
        formal_database_import_authorization_json=authorization_json,
        formal_database_import_preflight_json=preflight_json,
        archive_closure_json=archive_json,
        evidence_bundle_json=bundle_json,
    )

    assert model["schema"] == "v1_5_formal_database_import_command_contract_v1"
    assert model["overall_status"] == "ready_for_controlled_postgresql18_import_command_review"
    assert model["blocker_count"] == 0
    assert model["review_required_count"] == 0
    assert model["authorization_ready"] is True
    assert model["database_import_authorization_binding_ready"] is True
    assert model["formal_database_import_authorization_sha256"] == sha256_file(authorization_json)
    assert model["preflight_ready"] is True
    assert model["archive_release_ready"] is True
    assert model["archive_closure_index_binding_ready"] is True
    assert model["archive_closure_sha256"] == sha256_file(archive_json)
    assert model["senco_authorization_archive_binding_ready"] is True
    assert model["evidence_bundle_ready"] is True
    assert model["command_contract_ready"] is True
    assert model["real_import_execution_allowed"] is False
    assert model["database_import_allowed"] is False
    assert model["connects_postgresql"] is False
    assert model["applies_migrations"] is False
    assert model["database_import_attempted"] is False
    assert model["database_written"] is False
    assert _check(model, "formal_database_import_authorization_ready")["status"] == "ready"
    assert _check(model, "formal_database_import_authorization_hash_bound")["status"] == "ready"
    assert _check(model, "formal_database_import_preflight_ready")["status"] == "ready"
    assert _check(model, "formal_archive_closure_ready")["status"] == "ready"
    assert _check(model, "formal_archive_index_bound_to_authorization")["status"] == "ready"
    assert _check(model, "senco_authorization_archive_binding_ready")["status"] == "ready"
    assert _check(model, "formal_evidence_bundle_ready")["status"] == "ready"


def test_formal_database_import_command_contract_blocks_missing_archive_binding(tmp_path: Path) -> None:
    authorization_json, preflight_json, _archive_json_path, _bundle_json_path = _authorization_json(tmp_path)

    model = build_v1_5_formal_database_import_command_contract(
        formal_database_import_authorization_json=authorization_json,
        formal_database_import_preflight_json=preflight_json,
    )

    assert model["overall_status"] == "blocked"
    assert model["blocker_count"] == 2
    assert model["review_required_count"] == 2
    assert model["authorization_ready"] is True
    assert model["preflight_ready"] is True
    assert model["archive_release_ready"] is False
    assert model["senco_authorization_archive_binding_ready"] is False
    assert model["senco_authorization_archive_binding_ready"] is False
    assert model["evidence_bundle_ready"] is False
    assert model["command_contract_ready"] is False
    assert _check(model, "formal_archive_closure_ready")["status"] == "review_required"
    assert "archive_closure_missing" in _check(model, "formal_archive_closure_ready")["reasons"]
    assert _check(model, "senco_authorization_archive_binding_ready")["status"] == "blocker"
    assert _check(model, "formal_evidence_bundle_ready")["status"] == "review_required"
    assert "evidence_bundle_missing" in _check(model, "formal_evidence_bundle_ready")["reasons"]


def test_formal_database_import_command_contract_blocks_missing_authorization(tmp_path: Path) -> None:
    preflight_json = _preflight_json(tmp_path)
    archive_json = _archive_json(tmp_path)
    bundle_json = _evidence_bundle_json(tmp_path)

    model = build_v1_5_formal_database_import_command_contract(
        formal_database_import_authorization_json=tmp_path / "missing_authorization.json",
        formal_database_import_preflight_json=preflight_json,
        archive_closure_json=archive_json,
        evidence_bundle_json=bundle_json,
    )

    assert model["overall_status"] == "blocked"
    assert model["blocker_count"] == 3
    assert model["command_contract_ready"] is False
    assert model["real_import_execution_allowed"] is False
    assert _check(model, "formal_database_import_authorization_ready")["status"] == "blocker"
    assert "formal_database_import_authorization_missing" in _check(
        model,
        "formal_database_import_authorization_ready",
    )["reasons"]


def test_formal_database_import_command_contract_blocks_binding_changed_after_authorization(
    tmp_path: Path,
) -> None:
    authorization_json, preflight_json, archive_json, bundle_json = _authorization_json(tmp_path)
    archive = json.loads(archive_json.read_text(encoding="utf-8"))
    binding_path = Path(archive["artifacts"][0]["path"])
    binding_path.write_text("{}", encoding="utf-8")

    model = build_v1_5_formal_database_import_command_contract(
        formal_database_import_authorization_json=authorization_json,
        formal_database_import_preflight_json=preflight_json,
        archive_closure_json=archive_json,
        evidence_bundle_json=bundle_json,
    )

    assert model["overall_status"] == "blocked"
    assert model["command_contract_ready"] is False
    binding_check = _check(model, "senco_authorization_archive_binding_ready")
    assert binding_check["status"] == "blocker"
    assert "senco_authorization_archive_binding_sha256_mismatch" in binding_check["reasons"]


def test_formal_database_import_command_contract_blocks_archive_index_changed_after_authorization(
    tmp_path: Path,
) -> None:
    authorization_json, preflight_json, archive_json, bundle_json = _authorization_json(tmp_path)
    archive = json.loads(archive_json.read_text(encoding="utf-8"))
    archive["post_authorization_change"] = True
    archive_json.write_text(json.dumps(archive, ensure_ascii=False, indent=2), encoding="utf-8")

    model = build_v1_5_formal_database_import_command_contract(
        formal_database_import_authorization_json=authorization_json,
        formal_database_import_preflight_json=preflight_json,
        archive_closure_json=archive_json,
        evidence_bundle_json=bundle_json,
    )

    assert model["overall_status"] == "blocked"
    assert model["archive_closure_index_binding_ready"] is False
    assert model["command_contract_ready"] is False
    index_check = _check(model, "formal_archive_index_bound_to_authorization")
    assert index_check["status"] == "blocker"
    assert "authorization_archive_closure_sha256_mismatch" in index_check["reasons"]


def test_formal_database_import_command_contract_rechecks_distinct_authorizers(tmp_path: Path) -> None:
    authorization_json, preflight_json, archive_json, bundle_json = _authorization_json(tmp_path)
    authorization = json.loads(authorization_json.read_text(encoding="utf-8-sig"))
    authorization["approver"] = authorization["reviewer"]
    authorization_json.write_text(
        json.dumps(authorization, ensure_ascii=False, indent=2),
        encoding="utf-8-sig",
    )

    model = build_v1_5_formal_database_import_command_contract(
        formal_database_import_authorization_json=authorization_json,
        formal_database_import_preflight_json=preflight_json,
        archive_closure_json=archive_json,
        evidence_bundle_json=bundle_json,
    )

    assert model["overall_status"] == "blocked"
    assert model["authorization_ready"] is False
    assert model["database_import_authorization_binding_ready"] is False
    authorization_check = _check(model, "formal_database_import_authorization_ready")
    assert authorization_check["status"] == "blocker"
    assert "authorization_reviewer_approver_must_be_distinct" in authorization_check["reasons"]


def test_formal_database_import_command_contract_blocks_archive_index_path_replacement(
    tmp_path: Path,
) -> None:
    authorization_json, preflight_json, archive_json, bundle_json = _authorization_json(tmp_path)
    replacement = tmp_path / "replacement" / archive_json.name
    replacement.parent.mkdir(parents=True, exist_ok=True)
    replacement.write_bytes(archive_json.read_bytes())

    model = build_v1_5_formal_database_import_command_contract(
        formal_database_import_authorization_json=authorization_json,
        formal_database_import_preflight_json=preflight_json,
        archive_closure_json=replacement,
        evidence_bundle_json=bundle_json,
    )

    assert model["overall_status"] == "blocked"
    assert model["archive_closure_index_binding_ready"] is False
    index_check = _check(model, "formal_archive_index_bound_to_authorization")
    assert "authorization_archive_closure_json_mismatch" in index_check["reasons"]


def test_formal_database_import_command_contract_writer_and_cli(tmp_path: Path) -> None:
    authorization_json, preflight_json, archive_json, bundle_json = _authorization_json(tmp_path)
    model = build_v1_5_formal_database_import_command_contract(
        formal_database_import_authorization_json=authorization_json,
        formal_database_import_preflight_json=preflight_json,
        archive_closure_json=archive_json,
        evidence_bundle_json=bundle_json,
    )
    outputs = write_v1_5_formal_database_import_command_contract_outputs(model, tmp_path / "command_contract")

    assert outputs["json"].exists()
    assert outputs["checks_csv"].exists()
    assert outputs["summary_csv"].exists()
    assert outputs["markdown"].exists()
    assert "does not connect PostgreSQL" in outputs["markdown"].read_text(encoding="utf-8")
    payload = json.loads(outputs["json"].read_text(encoding="utf-8-sig"))
    assert payload["command_contract_ready"] is True
    assert payload["database_import_allowed"] is False

    cli_out = tmp_path / "cli_command_contract"
    rc = cli_main(
        [
            "--formal-database-import-authorization-json",
            str(authorization_json),
            "--formal-database-import-preflight-json",
            str(preflight_json),
            "--archive-closure-json",
            str(archive_json),
            "--evidence-bundle-json",
            str(bundle_json),
            "--output-dir",
            str(cli_out),
            "--fail-on-blocker",
            "--fail-on-review-required",
        ]
    )

    assert rc == 0
    assert (cli_out / "v1_5_formal_database_import_command_contract.json").exists()
