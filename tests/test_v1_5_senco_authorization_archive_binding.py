import csv
import json

import pytest

from gas_calibrator.validation.v1_5_senco_artifact_authorization import (
    write_senco_artifact_authorization,
)
from gas_calibrator.validation.v1_5_senco_authorization_archive_binding import (
    build_v1_5_senco_authorization_archive_binding,
    write_v1_5_senco_authorization_archive_binding_outputs,
)


def _write_json(path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def _write_csv(path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    return path


def _seed_authorization(
    root,
    *,
    device_ids=("001",),
    authorization_id="AUTH-001",
    writer_scopes=("co2_senco13_pair",),
):
    manifest = _write_json(root / "precheck" / "main_senco_artifact_hash_manifest.json", {"files": []})
    authorization = root / "precheck" / "main_senco_artifact_authorization.json"
    write_senco_artifact_authorization(
        authorization,
        manifest_path=manifest,
        reviewer="reviewer-a",
        approver="approver-b",
        authorization_id=authorization_id,
        authorized_writer_scopes=writer_scopes,
        authorized_device_ids=device_ids,
    )
    return authorization


def _seed_s13_write(
    root,
    *,
    evidence_name="s13",
    device_id="001",
    authorization_id="AUTH-001",
    status="written_readback_verified",
):
    output = root / "controlled_write" / evidence_name
    _write_json(
        output / "co2_senco13_pair_write_meta.json",
        {
            "config_summary": {
                "reviewer": "reviewer-a",
                "approver": "approver-b",
                "artifact_hash_status": "pass",
                "artifact_authorization_status": "pass",
                "artifact_authorization_id": authorization_id,
            }
        },
    )
    _write_csv(
        output / "co2_senco13_pair_write_summary.csv",
        [{"analyzer_device_id": device_id, "status": status}],
    )


def test_archive_binding_is_not_applicable_without_main_senco_write_evidence(tmp_path):
    model = build_v1_5_senco_authorization_archive_binding(run_dir=tmp_path)

    assert model["overall_status"] == "not_applicable_no_main_senco_write_evidence"
    assert model["ready_for_archive_release"] is True
    assert model["write_evidence_present"] is False


def test_archive_binding_accepts_exact_authorization_scope_devices_and_readback(tmp_path):
    authorization = _seed_authorization(tmp_path)
    _seed_s13_write(tmp_path)

    model = build_v1_5_senco_authorization_archive_binding(
        run_dir=tmp_path,
        authorization_json=authorization,
    )
    paths = write_v1_5_senco_authorization_archive_binding_outputs(model, tmp_path / "archive")

    assert model["overall_status"] == "ready_for_archive_release"
    assert model["ready_for_archive_release"] is True
    assert model["device_ids"] == ["001"]
    assert model["writer_evidence"][0]["status"] == "pass"
    assert all(path.exists() for path in paths.values())


def test_archive_binding_blocks_device_outside_authorized_set(tmp_path):
    authorization = _seed_authorization(tmp_path, device_ids=("001",))
    _seed_s13_write(tmp_path, device_id="002")

    model = build_v1_5_senco_authorization_archive_binding(
        run_dir=tmp_path,
        authorization_json=authorization,
    )

    assert model["overall_status"] == "blocked"
    assert model["ready_for_archive_release"] is False
    assert any("device_not_authorized:002" in reason for reason in model["blockers"])


def test_archive_binding_blocks_authorization_id_mismatch(tmp_path):
    authorization = _seed_authorization(tmp_path, authorization_id="AUTH-001")
    _seed_s13_write(tmp_path, authorization_id="AUTH-OTHER")

    model = build_v1_5_senco_authorization_archive_binding(
        run_dir=tmp_path,
        authorization_json=authorization,
    )

    assert model["overall_status"] == "blocked"
    assert any("artifact_authorization_id_mismatch" in reason for reason in model["blockers"])


def test_archive_binding_blocks_write_without_verified_readback(tmp_path):
    authorization = _seed_authorization(tmp_path)
    _seed_s13_write(tmp_path, status="failed_readback_mismatch")

    model = build_v1_5_senco_authorization_archive_binding(
        run_dir=tmp_path,
        authorization_json=authorization,
    )

    assert model["overall_status"] == "blocked"
    assert any("write_readback_not_verified" in reason for reason in model["blockers"])


def test_archive_binding_checks_every_write_evidence_directory_not_only_latest(tmp_path):
    authorization = _seed_authorization(tmp_path, device_ids=("001",))
    _seed_s13_write(tmp_path, evidence_name="s13_valid", device_id="001")
    _seed_s13_write(tmp_path, evidence_name="s13_unauthorized", device_id="002")

    model = build_v1_5_senco_authorization_archive_binding(
        run_dir=tmp_path,
        authorization_json=authorization,
    )

    assert model["write_evidence_set_count"] == 2
    assert model["overall_status"] == "blocked"
    assert any("device_not_authorized:002" in reason for reason in model["blockers"])


def test_archive_binding_requires_exact_final_authorized_device_set(tmp_path):
    authorization = _seed_authorization(tmp_path, device_ids=("001", "002"))
    _seed_s13_write(tmp_path, device_id="001")

    model = build_v1_5_senco_authorization_archive_binding(
        run_dir=tmp_path,
        authorization_json=authorization,
    )

    assert model["overall_status"] == "blocked"
    assert any("artifact_authorization_device_set_mismatch" in reason for reason in model["blockers"])


@pytest.mark.parametrize(
    ("writer_scope", "metadata_name", "rows_name", "device_field", "nested_metadata"),
    [
        (
            "h2o_senco24_pair",
            "h2o_senco24_pair_write_meta.json",
            "h2o_senco24_pair_write_summary.csv",
            "analyzer_device_id",
            True,
        ),
        (
            "co2_senco5_linear",
            "senco5_linear_write_meta.json",
            "senco5_linear_write_events.csv",
            "device_id",
            False,
        ),
        (
            "h2o_senco6_linear",
            "senco6_linear_write_meta.json",
            "senco6_linear_write_events.csv",
            "device_id",
            False,
        ),
    ],
)
def test_archive_binding_supports_each_main_writer_evidence_shape(
    tmp_path,
    writer_scope,
    metadata_name,
    rows_name,
    device_field,
    nested_metadata,
):
    authorization = _seed_authorization(tmp_path, writer_scopes=(writer_scope,))
    output = tmp_path / "controlled_write" / writer_scope
    config = {
        "reviewer": "reviewer-a",
        "approver": "approver-b",
        "artifact_hash_status": "pass",
        "artifact_authorization_status": "pass",
        "artifact_authorization_id": "AUTH-001",
    }
    _write_json(output / metadata_name, {"config_summary": config} if nested_metadata else config)
    _write_csv(output / rows_name, [{device_field: "001", "status": "written_readback_verified"}])

    model = build_v1_5_senco_authorization_archive_binding(
        run_dir=tmp_path,
        authorization_json=authorization,
    )

    assert model["overall_status"] == "ready_for_archive_release"
    assert model["writer_evidence"][0]["writer_scope"] == writer_scope
