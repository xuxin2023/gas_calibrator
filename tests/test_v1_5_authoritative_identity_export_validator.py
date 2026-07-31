from __future__ import annotations

import json
from pathlib import Path

from gas_calibrator.tools import verify_v1_5_authoritative_identity_export as tool
from gas_calibrator.validation.v1_5_entrypoint_inventory import (
    classify_v1_5_entrypoint,
)


FIXTURE = (
    Path(__file__).parent
    / "fixtures"
    / "v1_5_protocol_identity_global_uniqueness_evidence.json"
)


def _authoritative_payload() -> dict[str, object]:
    payload = json.loads(FIXTURE.read_text(encoding="utf-8"))
    payload["test_fixture_only"] = False
    payload.pop("not_real_acceptance_evidence", None)
    return payload


def _authoritative_export(tmp_path: Path) -> Path:
    path = tmp_path / "authoritative_identity_export.json"
    path.write_text(
        json.dumps(_authoritative_payload(), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return path


def test_valid_authoritative_export_is_ready_without_write_authority(tmp_path):
    evidence = _authoritative_export(tmp_path)
    result = tool.build_validation(
        evidence_path=evidence,
        candidate_sn="01260716",
        candidate_protocol_id="016",
    )

    assert result["status"] == "ready"
    assert result["blockers"] == []
    validation = result["global_uniqueness_evidence_validation"]
    assert validation["valid"] is True
    signature = validation["trusted_authority_signature"]
    assert signature["required"] is False
    assert signature["status"] == "not_required_semantic_validation_only"
    assert result["trusted_signature_required_for_controlled_write"] is True
    assert result["not_write_authorization"] is True
    assert all(value is False for value in result["boundary"].values())


def test_candidate_present_and_incomplete_scope_are_blocked(tmp_path):
    payload = _authoritative_payload()
    payload["scope_complete"] = False
    payload["scope"]["scope_complete"] = False
    payload["records"][0]["sn_code"] = "01260716"
    path = tmp_path / "blocked_export.json"
    path.write_text(json.dumps(payload), encoding="utf-8")

    result = tool.build_validation(
        evidence_path=path,
        candidate_sn="01260716",
        candidate_protocol_id="016",
    )

    assert result["status"] == "blocked"
    assert "global_uniqueness_evidence_scope_incomplete" in result["blockers"]
    assert (
        "global_uniqueness_evidence_candidate_sn_present_in_authority_records"
        in result["blockers"]
    )
    assert result["boundary"]["opens_com_ports"] is False
    assert result["boundary"]["connects_postgresql"] is False


def test_cli_writes_offline_review_artifact(tmp_path):
    evidence = _authoritative_export(tmp_path)
    output = tmp_path / "validation.json"

    rc = tool.main(
        [
            "--evidence-json",
            str(evidence),
            "--candidate-sn",
            "01260716",
            "--candidate-protocol-id",
            "016",
            "--output-json",
            str(output),
        ]
    )

    assert rc == 0
    payload = json.loads(output.read_text(encoding="utf-8"))
    assert payload["status"] == "ready"
    assert payload["not_write_authorization"] is True
    assert payload["promotion_state"] == "blocked"


def test_static_test_fixture_is_blocked_as_authoritative_evidence():
    result = tool.build_validation(
        evidence_path=FIXTURE,
        candidate_sn="01260716",
        candidate_protocol_id="016",
    )

    assert result["status"] == "blocked"
    assert "global_uniqueness_evidence_test_fixture_forbidden" in result["blockers"]
    assert (
        "global_uniqueness_evidence_test_fixture_path_forbidden" in result["blockers"]
    )


def test_validator_entrypoint_is_classified_offline_read_only() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    path = (
        repo_root
        / "src/gas_calibrator/tools/verify_v1_5_authoritative_identity_export.py"
    )

    entry = classify_v1_5_entrypoint(path, root=repo_root)

    assert entry.category == "formal_review_evidence"
    assert entry.stage == "identity_and_serial_binding"
    assert entry.formal_status == "read_only_authority_validation"
    assert entry.risk_level == "offline"
    assert entry.opens_com_ports is False
    assert entry.controls_routes is False
    assert entry.writes_coefficients is False
