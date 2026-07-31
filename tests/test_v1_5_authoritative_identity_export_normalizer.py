from __future__ import annotations

import json
from pathlib import Path

import pytest

from gas_calibrator.tools import normalize_v1_5_authoritative_identity_export as tool
from gas_calibrator.validation.v1_5_entrypoint_inventory import (
    classify_v1_5_entrypoint,
)


def _arguments(source: Path) -> dict[str, object]:
    return {
        "source_path": source,
        "source_format": "auto",
        "source_type": "controlled_asset_registry_readonly_export",
        "source_system": "controlled_fleet_registry",
        "exported_at": "2026-07-31T22:00:00+08:00",
        "exported_by": "asset_registry_operator",
        "candidate_sn": "99000009",
        "candidate_protocol_id": "099",
        "scope_complete": True,
        "includes_powered_devices": True,
        "includes_unpowered_devices": True,
        "includes_silent_ports": True,
    }


def _write_csv(path: Path) -> None:
    path.write_text(
        "资产编号,SN,协议地址,设备状态\n"
        "ASSET-01,99000001,1,active\n"
        "ASSET-02,99000002,2,unpowered\n"
        "ASSET-03,99000003,3,retired\n"
        "ASSET-04,99000004,4,silent\n",
        encoding="utf-8-sig",
    )


def test_csv_normalization_preserves_every_row_and_trace(tmp_path: Path) -> None:
    source = tmp_path / "asset_export.csv"
    _write_csv(source)

    payload, summary = tool.build_normalized_export(**_arguments(source))

    assert summary["status"] == "ready"
    assert summary["input_record_count"] == 4
    assert summary["output_record_count"] == 4
    assert summary["row_mapping_complete"] is True
    assert payload["test_fixture_only"] is False
    assert payload["scope"]["record_count"] == 4
    assert [row["protocol_device_id"] for row in payload["records"]] == [
        "001",
        "002",
        "003",
        "004",
    ]
    assert {row["lifecycle_status"] for row in payload["records"]} == {
        "active",
        "unpowered",
        "retired",
        "silent",
    }
    assert [row["source_trace"]["source_locator"] for row in payload["records"]] == [
        "csv_row:2",
        "csv_row:3",
        "csv_row:4",
        "csv_row:5",
    ]
    assert payload["normalization"]["source_sha256"] == summary["source_sha256"]


def test_json_and_csv_have_equivalent_canonical_identities(tmp_path: Path) -> None:
    csv_source = tmp_path / "asset_export.csv"
    _write_csv(csv_source)
    json_source = tmp_path / "asset_export.json"
    json_source.write_text(
        json.dumps(
            {
                "records": [
                    {
                        "asset_key": f"ASSET-0{index}",
                        "sn_code": f"9900000{index}",
                        "protocol_device_id": index,
                        "lifecycle_status": status,
                    }
                    for index, status in enumerate(
                        ("active", "unpowered", "retired", "silent"), start=1
                    )
                ]
            }
        ),
        encoding="utf-8",
    )

    csv_payload, _ = tool.build_normalized_export(**_arguments(csv_source))
    json_payload, _ = tool.build_normalized_export(**_arguments(json_source))
    def canonical(payload: dict[str, object]) -> list[tuple[object, ...]]:
        return [
            tuple(row[field] for field in tool.CANONICAL_FIELDS)
            for row in payload["records"]
        ]

    assert canonical(csv_payload) == canonical(json_payload)


def test_missing_scope_attestations_fail_closed_in_shared_gate(tmp_path: Path) -> None:
    source = tmp_path / "asset_export.csv"
    _write_csv(source)
    arguments = _arguments(source)
    arguments["scope_complete"] = False
    arguments["includes_unpowered_devices"] = False

    payload, validation = tool.normalize_and_validate(
        **arguments,
        normalized_output=tmp_path / "normalized.json",
        validation_output=tmp_path / "validation.json",
    )

    assert payload["overall_status"] == "blocked_normalization_review_required"
    assert validation["status"] == "blocked"
    assert "global_uniqueness_evidence_scope_incomplete" in validation["blockers"]
    assert (
        "global_uniqueness_evidence_scope_includes_unpowered_devices_missing"
        in validation["blockers"]
    )
    assert validation["not_write_authorization"] is True
    assert all(value is False for value in validation["boundary"].values())


def test_duplicate_sn_and_protocol_id_are_blocked_by_normalizer_and_shared_gate(
    tmp_path: Path,
) -> None:
    source = tmp_path / "duplicates.csv"
    source.write_text(
        "asset_key,sn_code,protocol_device_id,lifecycle_status\n"
        "ASSET-01,99000001,001,active\n"
        "ASSET-02,99000001,001,active\n",
        encoding="utf-8",
    )

    payload, validation = tool.normalize_and_validate(
        **_arguments(source),
        normalized_output=tmp_path / "normalized.json",
        validation_output=tmp_path / "validation.json",
    )

    assert "duplicate_sn_code" in payload["normalization"]["blockers"]
    assert "duplicate_protocol_device_id" in payload["normalization"]["blockers"]
    assert (
        "global_uniqueness_evidence_records_duplicate_sn_code"
        in validation["blockers"]
    )
    assert (
        "global_uniqueness_evidence_records_duplicate_protocol_device_id"
        in validation["blockers"]
    )
    assert validation["status"] == "blocked"


def test_static_test_fixture_cannot_become_ready_authority() -> None:
    fixture = (
        Path(__file__).parent
        / "fixtures"
        / "v1_5_authoritative_identity_asset_export.csv"
    )

    payload, summary = tool.build_normalized_export(**_arguments(fixture))

    assert payload["test_fixture_only"] is True
    assert payload["overall_status"] == "blocked_normalization_review_required"
    assert "test_fixture_source_forbidden" in summary["blockers"]


def test_cli_writes_normalized_and_validation_artifacts(tmp_path: Path) -> None:
    source = tmp_path / "asset_export.csv"
    _write_csv(source)
    normalized = tmp_path / "normalized.json"
    validation = tmp_path / "validation.json"

    rc = tool.main(
        [
            "--source",
            str(source),
            "--source-type",
            "controlled_asset_registry_readonly_export",
            "--source-system",
            "controlled_fleet_registry",
            "--exported-at",
            "2026-07-31T22:00:00+08:00",
            "--exported-by",
            "asset_registry_operator",
            "--candidate-sn",
            "99000009",
            "--candidate-protocol-id",
            "099",
            "--attest-scope-complete",
            "--attest-includes-powered-devices",
            "--attest-includes-unpowered-devices",
            "--attest-includes-silent-ports",
            "--normalized-output",
            str(normalized),
            "--validation-output",
            str(validation),
        ]
    )

    assert rc == 0
    assert json.loads(normalized.read_text(encoding="utf-8"))["scope"][
        "record_count"
    ] == 4
    validation_payload = json.loads(validation.read_text(encoding="utf-8"))
    assert validation_payload["status"] == "ready"
    assert validation_payload["not_write_authorization"] is True


def test_source_and_output_paths_must_be_distinct(tmp_path: Path) -> None:
    source = tmp_path / "asset_export.csv"
    _write_csv(source)

    with pytest.raises(ValueError, match="source_and_output_paths_must_be_distinct"):
        tool.normalize_and_validate(
            **_arguments(source),
            normalized_output=source,
            validation_output=tmp_path / "validation.json",
        )


def test_normalizer_entrypoint_is_offline_read_only() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    path = (
        repo_root
        / "src/gas_calibrator/tools/normalize_v1_5_authoritative_identity_export.py"
    )

    entry = classify_v1_5_entrypoint(path, root=repo_root)

    assert entry.category == "formal_review_evidence"
    assert entry.stage == "identity_and_serial_binding"
    assert entry.formal_status == "read_only_authority_normalization"
    assert entry.risk_level == "offline"
    assert entry.opens_com_ports is False
    assert entry.controls_routes is False
    assert entry.writes_coefficients is False
