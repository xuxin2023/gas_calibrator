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


def test_valid_authoritative_export_is_ready_without_write_authority(tmp_path):
    result = tool.build_validation(
        evidence_path=FIXTURE,
        candidate_sn="01260716",
        candidate_protocol_id="016",
    )

    assert result["status"] == "ready"
    assert result["blockers"] == []
    assert result["global_uniqueness_evidence_validation"]["valid"] is True
    assert result["not_write_authorization"] is True
    assert all(value is False for value in result["boundary"].values())


def test_candidate_present_and_incomplete_scope_are_blocked(tmp_path):
    payload = json.loads(FIXTURE.read_text(encoding="utf-8"))
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
    output = tmp_path / "validation.json"

    rc = tool.main(
        [
            "--evidence-json",
            str(FIXTURE),
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
