from __future__ import annotations

from copy import deepcopy
import csv
from hashlib import sha256
import json
from pathlib import Path

import pytest

from gas_calibrator.validation.certificate_operational_admission import (
    build_locked_fixture_verification,
    evaluate_certificate_operational_admission,
    load_certificate_operational_admission_contract,
    load_owner_attested_certificate_evidence,
    verify_documentary_files,
    write_certificate_operational_admission_artifacts,
)
from gas_calibrator.v2.core.offline_artifacts import build_suite_case_metadata
from gas_calibrator.v2.sim import (
    build_certificate_operational_admission_offline_report,
    get_simulation_suite,
)
from gas_calibrator.v2.ui_v2.i18n import display_suite_failure_type


def _evaluate(
    evidence: dict | None = None,
    contract: dict | None = None,
) -> dict:
    active_evidence = evidence or load_owner_attested_certificate_evidence()
    active_contract = contract or load_certificate_operational_admission_contract()
    return evaluate_certificate_operational_admission(
        active_evidence,
        contract=active_contract,
        source_verification=build_locked_fixture_verification(active_evidence),
    )


def test_owner_attested_operational_gate_passes_without_promoting_strict_gate() -> None:
    result = _evaluate()

    assert result["status"] == "PASSED_WITH_OWNER_ATTESTATION"
    assert result["evidence_source"] == "simulated"
    assert result["suite_contract_self_test"] is True
    assert result["operational_certificate_gate_passed"] is True
    assert result["offline_program_progress_allowed"] is True
    assert result["strict_original_certificate_gate_passed"] is False
    assert result["formal_certificate_dossier_complete"] is False
    assert result["ready_for_real_execution"] is False
    assert result["execution_authorization_status"] == "not_requested_and_not_granted"
    assert result["real_acceptance_status"] == "not_evaluated"
    assert result["not_real_acceptance_evidence"] is True
    assert result["promotion_state"] == "blocked"
    assert result["device_io_status"] == "not_attempted"
    assert result["database_write_status"] == "not_attempted"
    assert result["coefficient_writeback_status"] == "not_attempted"
    assert result["failed_gate_ids"] == []
    assert result["warnings"] == [
        {
            "warning_id": "dewpoint_certificate_near_expiry",
            "severity": "P1",
            "days_until_expiry": 23,
            "valid_until": "2026-08-17",
        }
    ]


def test_ten_owner_attested_co2_values_are_locked() -> None:
    evidence = load_owner_attested_certificate_evidence()
    evidence["co2_standard_gas_series"]["values_by_nominal_ppm"]["900"] = 897.04

    result = _evaluate(evidence)

    assert result["operational_certificate_gate_passed"] is False
    assert "owner_attested_co2_values_locked" in result["failed_gate_ids"]


def test_dry_air_is_not_promoted_to_certified_co2_zero() -> None:
    evidence = load_owner_attested_certificate_evidence()
    evidence["co2_low_end_anchor"]["certified_co2_value_ppm"] = 0.0

    result = _evaluate(evidence)

    assert result["operational_certificate_gate_passed"] is False
    assert "co2_low_end_anchor_truthful" in result["failed_gate_ids"]


def test_co2_low_end_and_h2o_dry_anchor_cannot_be_collapsed() -> None:
    evidence = load_owner_attested_certificate_evidence()
    evidence["anchor_separation"]["h2o_low_end_anchor_role"] = (
        "low_co2_dry_air_process_anchor"
    )

    result = _evaluate(evidence)

    assert result["operational_certificate_gate_passed"] is False
    assert "co2_zero_h2o_dry_separated" in result["failed_gate_ids"]


def test_temperature_truth_must_be_in_chamber_platinum_thermometer() -> None:
    evidence = load_owner_attested_certificate_evidence()
    evidence["temperature_reference"]["placement"] = "outside_temperature_chamber"
    evidence["temperature_chamber"]["setpoint_used_as_temperature_reference"] = True

    result = _evaluate(evidence)

    assert result["operational_certificate_gate_passed"] is False
    assert (
        "in_chamber_platinum_thermometer_is_temperature_truth"
        in result["failed_gate_ids"]
    )


def test_dewpoint_flow_cannot_be_promoted_to_fit_input_or_traceable_standard() -> None:
    evidence = load_owner_attested_certificate_evidence()
    evidence["flow_monitor"]["used_in_concentration_fit_or_correction"] = True
    evidence["flow_monitor"]["claimed_as_traceable_flow_reference"] = True

    result = _evaluate(evidence)

    assert result["operational_certificate_gate_passed"] is False
    assert "dewpoint_output_flow_is_process_monitor" in result["failed_gate_ids"]


def test_static_timebase_cannot_be_reused_for_dynamic_acceptance() -> None:
    evidence = load_owner_attested_certificate_evidence()
    evidence["timebase"]["dynamic_response_or_ec_acceptance_in_scope"] = True

    result = _evaluate(evidence)

    assert result["operational_certificate_gate_passed"] is False
    assert "static_timebase_scope_bounded" in result["failed_gate_ids"]


def test_local_file_verification_reports_match_missing_and_mismatch(
    tmp_path: Path,
) -> None:
    root = tmp_path / "evidence"
    root.mkdir()
    matched = root / "matched.jpg"
    mismatched = root / "mismatched.pdf"
    matched.write_bytes(b"locked")
    mismatched.write_bytes(b"changed")
    evidence = {
        "documentary_files": [
            {
                "filename": matched.name,
                "sha256": sha256(b"locked").hexdigest(),
                "evidence_group": "gas",
            },
            {
                "filename": mismatched.name,
                "sha256": sha256(b"expected").hexdigest(),
                "evidence_group": "pressure",
            },
            {
                "filename": "missing.pdf",
                "sha256": sha256(b"missing").hexdigest(),
                "evidence_group": "temperature",
            },
        ]
    }

    result = verify_documentary_files(evidence, roots=[root])

    assert result["all_files_verified"] is False
    assert result["matched_file_count"] == 1
    assert result["sha256_mismatch_count"] == 1
    assert result["missing_file_count"] == 1
    assert result["source_mutation_status"] == "not_attempted"
    assert [row["status"] for row in result["records"]] == [
        "matched",
        "sha256_mismatch",
        "missing",
    ]


def test_contract_rejects_write_or_promotion_drift(tmp_path: Path) -> None:
    contract = load_certificate_operational_admission_contract()
    unsafe = deepcopy(contract)
    unsafe["evidence_boundary"]["device_io_allowed"] = True
    path = tmp_path / "unsafe.json"
    path.write_text(json.dumps(unsafe, ensure_ascii=False), encoding="utf-8")
    with pytest.raises(ValueError, match="offline, no-write"):
        load_certificate_operational_admission_contract(path)

    unsafe = deepcopy(contract)
    unsafe["interpretation"]["operational_gate_is_real_acceptance"] = True
    path.write_text(json.dumps(unsafe, ensure_ascii=False), encoding="utf-8")
    with pytest.raises(ValueError, match="cannot promote"):
        load_certificate_operational_admission_contract(path)


def test_artifacts_keep_four_roles_and_explicit_boundaries(tmp_path: Path) -> None:
    result = _evaluate()
    artifacts = write_certificate_operational_admission_artifacts(
        result,
        output_dir=tmp_path / "reports",
    )

    with Path(artifacts["execution_rows"]).open(
        encoding="utf-8-sig",
        newline="",
    ) as handle:
        rows = list(csv.DictReader(handle))
    summary = json.loads(
        Path(artifacts["execution_summary"]).read_text(encoding="utf-8")
    )
    diagnostic = json.loads(
        Path(artifacts["diagnostic_analysis"]).read_text(encoding="utf-8")
    )
    formal = json.loads(Path(artifacts["formal_analysis"]).read_text(encoding="utf-8"))
    markdown = Path(artifacts["diagnostic_markdown"]).read_text(encoding="utf-8")

    assert rows
    assert summary["artifact_role"] == "execution_summary"
    assert summary["operational_certificate_gate_passed"] is True
    assert summary["strict_original_certificate_gate_passed"] is False
    assert diagnostic["artifact_role"] == "diagnostic_analysis"
    assert formal["artifact_role"] == "formal_analysis"
    assert formal["formal_certificate_dossier_complete"] is False
    assert formal["ready_for_real_execution"] is False
    assert "温度真值：温箱内铂电阻数字测温仪" in markdown
    assert "未宣称 CO2=0 ppm" in markdown
    assert "H2O 低端" in markdown
    assert Path(artifacts["sha256_manifest"]).exists()


def test_regression_and_nightly_include_ga_d6b_with_chinese_metadata() -> None:
    for suite_name in ("regression", "nightly"):
        matching = [
            case.name
            for case in get_simulation_suite(suite_name).cases
            if case.kind == "ga_certificate_admission"
        ]
        assert matching == ["ga_d6b_owner_attested_certificate_admission"]
    assert not any(
        case.kind == "ga_certificate_admission"
        for case in get_simulation_suite("smoke").cases
    )

    metadata = build_suite_case_metadata(
        {
            "name": "ga_d6b_owner_attested_certificate_admission",
            "kind": "ga_certificate_admission",
            "status": "MATCH",
            "ok": True,
            "artifact_dir": "",
            "details": {},
        },
        suite_name="regression",
    )
    assert metadata["evidence_source"] == "simulated"
    assert metadata["failure_type"] == (
        "gas_analyzer_certificate_operational_admission"
    )
    assert (
        display_suite_failure_type(metadata["failure_type"], locale="zh_CN")
        == "气体分析仪证书运行资料门禁"
    )


def test_suite_contract_self_test_exports_match(tmp_path: Path) -> None:
    result = build_certificate_operational_admission_offline_report(
        report_root=tmp_path,
    )

    assert result["status"] == "MATCH"
    assert result["report"]["evidence_source"] == "simulated"
    assert result["report"]["suite_contract_self_test"] is True
    assert result["report"]["operational_certificate_gate_passed"] is True
    assert result["report"]["strict_original_certificate_gate_passed"] is False
    assert result["report"]["ready_for_real_execution"] is False
    assert Path(result["execution_rows"]).exists()
    assert Path(result["formal_analysis"]).exists()
