import json

from gas_calibrator.validation.v1_5_artifact_hash_binding import write_artifact_hash_manifest
from gas_calibrator.validation.v1_5_senco_artifact_authorization import (
    READY_STATUS,
    validate_senco_artifact_authorization,
    write_senco_artifact_authorization,
)


def _authorization(
    tmp_path,
    *,
    reviewer="reviewer-a",
    approver="approver-b",
    scopes=("co2_senco13_pair",),
    device_ids=("091",),
):
    source = tmp_path / "source.csv"
    source.write_text("status\npass\n", encoding="utf-8")
    manifest = write_artifact_hash_manifest(
        tmp_path / "main_senco_artifact_hash_manifest.json",
        artifacts={"source": source},
    )
    authorization = write_senco_artifact_authorization(
        tmp_path / "main_senco_artifact_authorization.json",
        manifest_path=manifest,
        reviewer=reviewer,
        approver=approver,
        authorization_id="AUTH-001",
        authorized_writer_scopes=scopes,
        authorized_device_ids=device_ids,
    )
    return source, manifest, authorization


def test_senco_artifact_authorization_round_trip(tmp_path):
    _, manifest, authorization = _authorization(tmp_path)

    ok, reasons, detail = validate_senco_artifact_authorization(
        authorization,
        manifest_path=manifest,
        reviewer="reviewer-a",
        approver="approver-b",
        writer_scope="co2_senco13_pair",
        device_ids=("091",),
    )

    assert ok is True
    assert reasons == []
    assert detail["authorization_id"] == "AUTH-001"


def test_senco_artifact_authorization_rejects_manifest_replacement(tmp_path):
    _, manifest, authorization = _authorization(tmp_path)
    payload = json.loads(manifest.read_text(encoding="utf-8"))
    payload["generated_at"] = "replaced"
    manifest.write_text(json.dumps(payload), encoding="utf-8")

    ok, reasons, _ = validate_senco_artifact_authorization(
        authorization,
        manifest_path=manifest,
        reviewer="reviewer-a",
        approver="approver-b",
        writer_scope="co2_senco13_pair",
        device_ids=("091",),
    )

    assert ok is False
    assert "senco_artifact_authorization_manifest_sha256_mismatch" in reasons


def test_senco_artifact_authorization_rejects_person_and_scope_mismatch(tmp_path):
    _, manifest, authorization = _authorization(tmp_path)

    ok, reasons, _ = validate_senco_artifact_authorization(
        authorization,
        manifest_path=manifest,
        reviewer="reviewer-other",
        approver="approver-b",
        writer_scope="h2o_senco24_pair",
        device_ids=("092",),
    )

    assert ok is False
    assert "senco_artifact_authorization_reviewer_mismatch" in reasons
    assert "senco_artifact_authorization_writer_scope_not_authorized:h2o_senco24_pair" in reasons
    assert "senco_artifact_authorization_device_not_authorized:092" in reasons


def test_senco_artifact_authorization_writer_rejects_same_reviewer_and_approver(tmp_path):
    _, _, authorization = _authorization(tmp_path, reviewer="same", approver="same")
    payload = json.loads(authorization.read_text(encoding="utf-8"))

    assert payload["overall_status"] != READY_STATUS
    assert "artifact_authorization_reviewer_equals_approver" in payload["blockers"]
