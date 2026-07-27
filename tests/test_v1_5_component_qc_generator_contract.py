import json
from pathlib import Path

from gas_calibrator.tools.export_v1_5_component_qc_generator_contract import main
from gas_calibrator.validation.v1_5_component_qc_authority_audit import SCHEMA as AUTHORITY_SCHEMA
from gas_calibrator.validation.v1_5_component_qc_generator_contract import (
    build_v1_5_component_qc_generator_contract_review,
    write_v1_5_component_qc_generator_contract_review,
)
from gas_calibrator.validation.v1_5_entrypoint_inventory import classify_v1_5_entrypoint


CONTRACT_PATH = Path("configs/v1_5_component_qc_generator_contract.json")


def _json(path: Path, payload: dict) -> Path:
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def _authority(tmp_path: Path) -> Path:
    return _json(
        tmp_path / "authority.json",
        {
            "schema": AUTHORITY_SCHEMA,
            "overall_status": "blocked_no_reviewed_mature_component_qc_authority",
            "tracked_component_qc_writer_present": False,
            "component_qc_generation_allowed": False,
            "component_qc_backfill_allowed": False,
            "p2_candidate_count": 125,
        },
    )


def _contract_copy(tmp_path: Path) -> Path:
    return _json(tmp_path / "contract.json", json.loads(CONTRACT_PATH.read_text(encoding="utf-8")))


def test_review_accepts_design_contract_but_keeps_execution_locked(tmp_path: Path) -> None:
    model = build_v1_5_component_qc_generator_contract_review(
        authority_audit_json_path=_authority(tmp_path),
        contract_json_path=CONTRACT_PATH,
    )
    assert model["overall_status"] == "ready_for_component_qc_generator_contract_manual_review"
    assert model["blocker_codes"] == []
    assert model["rules"]["grading_scope"] == "per_analyzer_independent"
    assert model["rules"]["one_analyzer_failure_blocks_other_analyzers"] is False
    assert model["rules"]["co2_a_ratio_span_max"] == 0.0005
    assert model["rules"]["co2_b_ratio_span_max"] == 0.001
    assert model["rules"]["h2o_a_ratio_span_max"] == 0.001
    assert model["implementation_available"] is False
    assert model["component_qc_generation_allowed"] is False
    assert model["component_qc_backfill_allowed"] is False


def test_h2o_a_threshold_must_be_0_001_and_ratio_alone_c_is_forbidden(tmp_path: Path) -> None:
    contract = _contract_copy(tmp_path)
    payload = json.loads(contract.read_text(encoding="utf-8"))
    payload["routes"]["h2o"]["A_ratio_span_max"] = 0.0005
    payload["routes"]["h2o"]["ratio_span_alone_can_assign_C"] = True
    contract.write_text(json.dumps(payload), encoding="utf-8")
    model = build_v1_5_component_qc_generator_contract_review(
        authority_audit_json_path=_authority(tmp_path),
        contract_json_path=contract,
    )
    assert model["overall_status"] == "blocked_invalid_component_qc_contract"
    assert "h2o_a_ratio_threshold_must_be_0_001" in model["blocker_codes"]
    assert "h2o_ratio_span_alone_c_reject_not_allowed" in model["blocker_codes"]


def test_global_worst_analyzer_cannot_block_other_analyzers(tmp_path: Path) -> None:
    contract = _contract_copy(tmp_path)
    payload = json.loads(contract.read_text(encoding="utf-8"))
    payload["scope"]["one_analyzer_failure_blocks_other_analyzers"] = True
    contract.write_text(json.dumps(payload), encoding="utf-8")
    model = build_v1_5_component_qc_generator_contract_review(
        authority_audit_json_path=_authority(tmp_path),
        contract_json_path=contract,
    )
    assert "scope_one_analyzer_failure_blocks_other_analyzers_invalid" in model["blocker_codes"]
    assert model["component_qc_generation_allowed"] is False


def test_summary_filter_and_traceability_cannot_be_weakened(tmp_path: Path) -> None:
    contract = _contract_copy(tmp_path)
    payload = json.loads(contract.read_text(encoding="utf-8"))
    payload["scope"]["uses_summary_outlier_filtered_values"] = True
    payload["output_contract"]["required_fields"].remove("source_samples_sha256")
    contract.write_text(json.dumps(payload), encoding="utf-8")
    model = build_v1_5_component_qc_generator_contract_review(
        authority_audit_json_path=_authority(tmp_path),
        contract_json_path=contract,
    )
    assert "scope_uses_summary_outlier_filtered_values_invalid" in model["blocker_codes"]
    assert "output_traceability_field_missing:source_samples_sha256" in model["blocker_codes"]


def test_temporal_thresholds_and_evidence_fields_cannot_be_weakened(
    tmp_path: Path,
) -> None:
    contract = _contract_copy(tmp_path)
    payload = json.loads(contract.read_text(encoding="utf-8"))
    payload["cadence_and_alignment_contract"]["minimum_window_duration_fraction"] = 0.5
    payload["output_contract"]["required_fields"].remove("actual_window_duration_s")
    contract.write_text(json.dumps(payload), encoding="utf-8")

    model = build_v1_5_component_qc_generator_contract_review(
        authority_audit_json_path=_authority(tmp_path),
        contract_json_path=contract,
    )

    assert model["overall_status"] == "blocked_invalid_component_qc_contract"
    assert (
        "cadence_minimum_window_duration_fraction_invalid"
        in model["blocker_codes"]
    )
    assert (
        "output_traceability_field_missing:actual_window_duration_s"
        in model["blocker_codes"]
    )


def test_evidence_identity_contract_cannot_be_weakened(tmp_path: Path) -> None:
    contract = _contract_copy(tmp_path)
    payload = json.loads(contract.read_text(encoding="utf-8"))
    payload["evidence_identity_contract"]["identity_mismatch_grade"] = (
        "B_diagnostic_model_only"
    )
    payload["output_contract"]["required_fields"].remove(
        "evidence_bundle_manifest_verified"
    )
    contract.write_text(json.dumps(payload), encoding="utf-8")

    model = build_v1_5_component_qc_generator_contract_review(
        authority_audit_json_path=_authority(tmp_path),
        contract_json_path=contract,
    )

    assert model["overall_status"] == "blocked_invalid_component_qc_contract"
    assert (
        "evidence_identity_identity_mismatch_grade_invalid"
        in model["blocker_codes"]
    )
    assert (
        "output_traceability_field_missing:evidence_bundle_manifest_verified"
        in model["blocker_codes"]
    )


def test_writer_cli_and_entrypoint_are_offline(tmp_path: Path) -> None:
    authority = _authority(tmp_path)
    model = build_v1_5_component_qc_generator_contract_review(
        authority_audit_json_path=authority,
        contract_json_path=CONTRACT_PATH,
    )
    outputs = write_v1_5_component_qc_generator_contract_review(model, tmp_path / "direct")
    rc = main(
        [
            "--authority-audit-json-path",
            str(authority),
            "--contract-json-path",
            str(CONTRACT_PATH),
            "--output-dir",
            str(tmp_path / "cli"),
        ]
    )
    entry = classify_v1_5_entrypoint(
        Path("src/gas_calibrator/tools/export_v1_5_component_qc_generator_contract.py"),
        root=Path.cwd(),
    )
    assert rc == 0
    assert outputs["json"].is_file()
    assert entry.category == "formal_review_evidence"
    assert entry.risk_level == "offline"
    assert entry.opens_com_ports is False
    assert entry.controls_routes is False
    assert entry.writes_coefficients is False
