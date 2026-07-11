import json
from pathlib import Path

from gas_calibrator.validation.v1_5_artifact_hash_binding import sha256_file
from gas_calibrator.validation.v1_5_formal_database_import_archive_binding import (
    validate_v1_5_database_import_archive_binding,
)


def _write_json(path: Path, payload: dict) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def _ready_archive(tmp_path: Path, *, with_write_sources: bool = False) -> tuple[dict, Path, list[Path]]:
    sources: list[Path] = []
    writer_evidence = []
    authorization_path = ""
    authorization_sha256 = ""
    manifest_path = ""
    manifest_sha256 = ""
    authorization_id = ""
    if with_write_sources:
        authorization = _write_json(tmp_path / "write" / "authorization.json", {"id": "AUTH-001"})
        manifest = _write_json(tmp_path / "write" / "manifest.json", {"files": []})
        metadata = _write_json(tmp_path / "write" / "writer_meta.json", {"status": "pass"})
        rows = tmp_path / "write" / "readback.csv"
        rows.write_text("device_id,status\n001,written_readback_verified\n", encoding="utf-8")
        sources.extend((authorization, manifest, metadata, rows))
        authorization_path = str(authorization.resolve())
        authorization_sha256 = sha256_file(authorization)
        manifest_path = str(manifest.resolve())
        manifest_sha256 = sha256_file(manifest)
        authorization_id = "AUTH-001"
        writer_evidence.append(
            {
                "writer_scope": "co2_senco13_pair",
                "metadata_path": str(metadata.resolve()),
                "metadata_sha256": sha256_file(metadata),
                "write_rows_path": str(rows.resolve()),
                "write_rows_sha256": sha256_file(rows),
                "status": "pass",
            }
        )
    binding = {
        "schema": "v1_5_senco_authorization_archive_binding_v1",
        "overall_status": "ready_for_archive_release" if with_write_sources else "not_applicable_no_main_senco_write_evidence",
        "ready_for_archive_release": True,
        "write_evidence_present": with_write_sources,
        "authorization_id": authorization_id,
        "authorization_path": authorization_path,
        "authorization_sha256": authorization_sha256,
        "manifest_path": manifest_path,
        "manifest_sha256": manifest_sha256,
        "writer_evidence": writer_evidence,
    }
    binding_path = _write_json(
        tmp_path / "archive" / "senco_authorization_write_traceability" / "v1_5_senco_authorization_archive_binding.json",
        binding,
    )
    artifacts = [
        {
            "role": "senco_authorization_write_traceability_json",
            "path": str(binding_path.resolve()),
            "sha256": sha256_file(binding_path),
        }
    ]
    source_roles = (
        "senco_artifact_authorization",
        "senco_artifact_hash_manifest",
        "senco_write_001_co2_senco13_pair_metadata",
        "senco_write_001_co2_senco13_pair_readback_rows",
    )
    for role, source in zip(source_roles, sources):
        artifacts.append(
            {
                "role": role,
                "path": str(source.resolve()),
                "sha256": sha256_file(source),
            }
        )
    return {
        "senco_authorization_write_traceability": binding,
        "artifacts": artifacts,
    }, binding_path, sources


def test_database_import_archive_binding_accepts_frozen_no_write_archive(tmp_path: Path) -> None:
    archive, binding_path, _sources = _ready_archive(tmp_path)

    ready, reasons, detail = validate_v1_5_database_import_archive_binding(archive)

    assert ready is True
    assert reasons == []
    assert detail["binding_path"] == str(binding_path.resolve())
    assert detail["binding_sha256"] == sha256_file(binding_path)
    assert detail["source_artifact_count"] == 0


def test_database_import_archive_binding_accepts_and_rehashes_write_sources(tmp_path: Path) -> None:
    archive, _binding_path, sources = _ready_archive(tmp_path, with_write_sources=True)

    ready, reasons, detail = validate_v1_5_database_import_archive_binding(archive)

    assert ready is True
    assert reasons == []
    assert detail["source_artifact_count"] == 4
    assert detail["verified_source_artifact_count"] == 4
    assert len(sources) == 4


def test_database_import_archive_binding_blocks_tampered_binding_json(tmp_path: Path) -> None:
    archive, binding_path, _sources = _ready_archive(tmp_path)
    binding_path.write_text("{}", encoding="utf-8")

    ready, reasons, _detail = validate_v1_5_database_import_archive_binding(archive)

    assert ready is False
    assert "senco_authorization_archive_binding_sha256_mismatch" in reasons
    assert "senco_authorization_archive_binding_json_invalid" in reasons


def test_database_import_archive_binding_blocks_tampered_write_source(tmp_path: Path) -> None:
    archive, _binding_path, sources = _ready_archive(tmp_path, with_write_sources=True)
    sources[-1].write_text("device_id,status\n001,changed\n", encoding="utf-8")

    ready, reasons, _detail = validate_v1_5_database_import_archive_binding(archive)

    assert ready is False
    assert "senco_authorization_archive_source_sha256_mismatch:writer_1_readback_rows" in reasons


def test_database_import_archive_binding_blocks_missing_binding_artifact(tmp_path: Path) -> None:
    archive, _binding_path, _sources = _ready_archive(tmp_path)
    archive["artifacts"] = []

    ready, reasons, _detail = validate_v1_5_database_import_archive_binding(archive)

    assert ready is False
    assert "senco_authorization_archive_binding_artifact_count=0" in reasons


def test_database_import_archive_binding_rejects_self_declared_ready_write_without_sources(
    tmp_path: Path,
) -> None:
    archive, binding_path, _sources = _ready_archive(tmp_path)
    binding = dict(archive["senco_authorization_write_traceability"])
    binding["overall_status"] = "ready_for_archive_release"
    binding["write_evidence_present"] = True
    archive["senco_authorization_write_traceability"] = binding
    _write_json(binding_path, binding)
    archive["artifacts"][0]["sha256"] = sha256_file(binding_path)

    ready, reasons, _detail = validate_v1_5_database_import_archive_binding(archive)

    assert ready is False
    assert "senco_authorization_archive_authorization_path_missing" in reasons
    assert "senco_authorization_archive_manifest_path_missing" in reasons
    assert "senco_authorization_archive_writer_evidence_missing" in reasons


def test_database_import_archive_binding_rejects_wrong_source_artifact_role(tmp_path: Path) -> None:
    archive, _binding_path, _sources = _ready_archive(tmp_path, with_write_sources=True)
    archive["artifacts"][-1]["role"] = "untrusted_readback_alias"

    ready, reasons, _detail = validate_v1_5_database_import_archive_binding(archive)

    assert ready is False
    assert "senco_authorization_archive_source_role_mismatch:writer_1_readback_rows" in reasons
