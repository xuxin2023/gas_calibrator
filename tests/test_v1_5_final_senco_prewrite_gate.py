import csv
import json

from gas_calibrator.validation.v1_5_final_senco_prewrite_gate import validate_final_senco_prewrite_gate
from gas_calibrator.validation.v1_5_artifact_hash_binding import write_artifact_hash_manifest
from gas_calibrator.validation.v1_5_senco_artifact_authorization import write_senco_artifact_authorization


def _write_csv(path, rows):
    fields = []
    for row in rows:
        for key in row:
            if key not in fields:
                fields.append(key)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def _pack(
    tmp_path,
    *,
    package_meta_status="pass",
    package_check_status="pass",
    device_status="pass",
    blockers="",
):
    root = tmp_path / "precheck"
    root.mkdir()
    (root / "main_senco_write_precheck_meta.json").write_text(
        json.dumps(
            {
                "no_write": True,
                "opens_com": False,
                "writes_senco": False,
                "controls_routes": False,
                "fit_input_traceability_required": True,
                "fit_input_traceability_status": package_meta_status,
                "artifact_hash_manifest_required": True,
                "artifact_hash_manifest_path": str((root / "main_senco_artifact_hash_manifest.json").resolve()),
                "artifact_hash_algorithm": "sha256",
                "artifact_authorization_required": True,
                "artifact_authorization_path": str((root / "main_senco_artifact_authorization.json").resolve()),
                "artifact_authorization_schema": "v1_5_senco_artifact_authorization_v1",
            }
        ),
        encoding="utf-8",
    )
    _write_csv(
        root / "candidate_write_review_checks.csv",
        [
            {
                "check": "fit_input_traceability_required_before_final_senco_review",
                "status": package_check_status,
            },
            {"check": "fit_input_traceability_bound:co2:091", "status": device_status},
        ],
    )
    _write_csv(
        root / "main_senco_write_precheck_summary.csv",
        [
            {
                "analyzer_device_id": "091",
                "co2_fit_input_traceability_status": device_status,
                "co2_fit_input_traceability_blockers": blockers,
            }
        ],
    )
    mapping_path = root / "candidate_senco_mapping_review.csv"
    mapping_path.write_text("component,analyzer_device_id\nco2,091\n", encoding="utf-8")
    artifacts = {
        "precheck_summary": root / "main_senco_write_precheck_summary.csv",
        "precheck_checks": root / "candidate_write_review_checks.csv",
        "precheck_co2_mapping": mapping_path,
    }
    for role in (
        "co2_fit_input_quality_summary",
        "co2_fit_input_quality_devices",
        "co2_candidate_run_summary",
        "co2_candidate_policy_summary",
        "co2_model_selection_summary",
    ):
        source = root / f"{role}.csv"
        source.write_text("status\npass\n", encoding="utf-8")
        artifacts[role] = source
    write_artifact_hash_manifest(root / "main_senco_artifact_hash_manifest.json", artifacts=artifacts)
    write_senco_artifact_authorization(
        root / "main_senco_artifact_authorization.json",
        manifest_path=root / "main_senco_artifact_hash_manifest.json",
        reviewer="reviewer-a",
        approver="approver-b",
        authorization_id="AUTH-GATE-001",
        authorized_writer_scopes=("co2_senco13_pair", "co2_senco5_linear"),
        authorized_device_ids=("091",),
    )
    return root


def _validate(root, *, device_ids=("091",), writer_scope="co2_senco13_pair", **kwargs):
    return validate_final_senco_prewrite_gate(
        root,
        component="co2",
        device_ids=device_ids,
        reviewer="reviewer-a",
        approver="approver-b",
        writer_scope=writer_scope,
        **kwargs,
    )


def test_final_senco_prewrite_gate_accepts_bound_device(tmp_path):
    root = _pack(tmp_path)

    ok, reasons, detail = _validate(root, device_ids=("91",))

    assert ok is True
    assert reasons == []
    assert detail["device_ids"] == ["091"]
    assert detail["fit_input_traceability_status"] == "pass"


def test_final_senco_prewrite_gate_allows_selected_component_when_unrelated_package_rows_are_blocked(tmp_path):
    root = _pack(tmp_path, package_meta_status="blocked", package_check_status="block_write")

    ok, reasons, detail = _validate(root)

    assert ok is True
    assert reasons == []
    assert detail["package_fit_input_traceability_status"] == "blocked"
    assert detail["fit_input_traceability_status"] == "pass"


def test_final_senco_prewrite_gate_rejects_missing_device_check(tmp_path):
    root = _pack(tmp_path)

    ok, reasons, _ = _validate(root, device_ids=("092",))

    assert ok is False
    assert "fit_input_traceability_bound:co2:092:missing" in reasons
    assert "main_senco_precheck_summary_device_missing:092" in reasons


def test_final_senco_prewrite_gate_rejects_device_blockers(tmp_path):
    root = _pack(tmp_path, blockers="historical_candidate_without_live_fit_input")

    ok, reasons, _ = _validate(root)

    assert ok is False
    assert any(reason.startswith("co2_fit_input_traceability_blockers:091:") for reason in reasons)


def test_final_senco_prewrite_gate_rejects_missing_safety_boundary_field(tmp_path):
    root = _pack(tmp_path)
    meta_path = root / "main_senco_write_precheck_meta.json"
    meta = json.loads(meta_path.read_text(encoding="utf-8"))
    del meta["writes_senco"]
    meta_path.write_text(json.dumps(meta), encoding="utf-8")

    ok, reasons, _ = _validate(root)

    assert ok is False
    assert "main_senco_precheck_boundary_missing:writes_senco" in reasons


def test_final_senco_prewrite_gate_rejects_same_path_content_replacement(tmp_path):
    root = _pack(tmp_path)
    source = root / "co2_model_selection_summary.csv"
    source.write_text("status\nreplaced_after_review\n", encoding="utf-8")

    ok, reasons, _ = _validate(root)

    assert ok is False
    assert "artifact_hash_manifest_sha256_mismatch:co2_model_selection_summary" in reasons


def test_final_senco_prewrite_gate_requires_writer_specific_candidate_binding(tmp_path):
    root = _pack(tmp_path)
    candidate = root / "senco5_candidates.csv"
    candidate.write_text("device_id,C0,C1\n091,0,1\n", encoding="utf-8")

    ok, reasons, _ = _validate(
        root,
        writer_scope="co2_senco5_linear",
        required_artifact_paths={"co2_senco5_candidate_coefficients": candidate},
    )

    assert ok is False
    assert "artifact_hash_manifest_required_role_missing:co2_senco5_candidate_coefficients" in reasons


def test_final_senco_prewrite_gate_rejects_replaced_authorization_manifest_binding(tmp_path):
    root = _pack(tmp_path)
    authorization_path = root / "main_senco_artifact_authorization.json"
    payload = json.loads(authorization_path.read_text(encoding="utf-8"))
    payload["manifest_sha256"] = "0" * 64
    authorization_path.write_text(json.dumps(payload), encoding="utf-8")

    ok, reasons, _ = _validate(root)

    assert ok is False
    assert "senco_artifact_authorization_manifest_sha256_mismatch" in reasons


def test_final_senco_prewrite_gate_rejects_reviewer_mismatch(tmp_path):
    root = _pack(tmp_path)

    ok, reasons, _ = validate_final_senco_prewrite_gate(
        root,
        component="co2",
        device_ids=["091"],
        reviewer="reviewer-other",
        approver="approver-b",
        writer_scope="co2_senco13_pair",
    )

    assert ok is False
    assert "senco_artifact_authorization_reviewer_mismatch" in reasons


def test_final_senco_prewrite_gate_rejects_device_outside_authorization(tmp_path):
    root = _pack(tmp_path)
    authorization_path = root / "main_senco_artifact_authorization.json"
    payload = json.loads(authorization_path.read_text(encoding="utf-8"))
    payload["authorized_device_ids"] = ["092"]
    authorization_path.write_text(json.dumps(payload), encoding="utf-8")

    ok, reasons, _ = _validate(root)

    assert ok is False
    assert "senco_artifact_authorization_device_not_authorized:091" in reasons
