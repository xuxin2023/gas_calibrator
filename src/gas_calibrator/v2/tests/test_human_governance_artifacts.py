from __future__ import annotations

import json

from gas_calibrator.v2.core.human_governance_artifacts import (
    HUMAN_GOVERNANCE_FIXTURE_SCHEMA_VERSION,
    build_human_governance_artifacts,
    load_human_governance_fixtures,
)


def test_load_human_governance_fixtures_reads_minimum_file_backed_rows() -> None:
    fixtures = load_human_governance_fixtures()

    assert fixtures["schema_version"] == HUMAN_GOVERNANCE_FIXTURE_SCHEMA_VERSION
    assert set(fixtures["fixture_paths"]) == {
        "operator_roster",
        "authorization_scope",
        "training_records",
        "sop_versions",
        "qc_flag_catalog",
    }
    assert any(row["role"] == "operator" for row in fixtures["operator_roster"])
    assert any(row["person_id"] == "OP-SIM-LI" for row in fixtures["authorization_scopes"])
    assert any(row["module_id"] == "STEP2_SIMULATION_OPERATOR" for row in fixtures["training_records"])
    assert any(row["sop_id"] == "SOP-STEP2-CAL-SIM" for row in fixtures["sop_versions"])
    assert any(bool(row["requires_dual_check"]) for row in fixtures["qc_flag_catalog_rows"])


def test_build_human_governance_artifacts_binds_operator_reviewer_and_sop(sample_run_dir) -> None:
    summary = json.loads((sample_run_dir / "summary.json").read_text(encoding="utf-8"))
    manifest = json.loads((sample_run_dir / "manifest.json").read_text(encoding="utf-8"))

    payloads = build_human_governance_artifacts(
        run_id=summary["run_id"],
        run_dir=sample_run_dir,
        summary=summary,
        manifest=manifest,
        acceptance_plan={},
        workbench_action_report={},
    )

    run_metadata = payloads["run_metadata_profile"]
    operator_authorization = payloads["operator_authorization_profile"]
    training_record = payloads["training_record"]
    sop_binding = payloads["sop_version_binding"]
    dual_check = payloads["reviewer_dual_check_placeholder"]

    assert run_metadata["operator"]["person_id"] == "OP-SIM-LI"
    assert run_metadata["reviewer"]["person_id"] == "RVW-STEP2-CHEN"
    assert "OP-SIM-LI" in run_metadata["summary_line"]
    assert "RVW-STEP2-CHEN" in run_metadata["summary_line"]
    assert operator_authorization["authorization_ready"] is True
    assert training_record["missing_training_modules"] == []
    assert any(row["sop_id"] == "SOP-STEP2-CAL-SIM" for row in sop_binding["bound_sops"])
    assert len(dual_check["required_action_rows"]) >= 1
    assert dual_check["placeholder_mode"] == "reviewer_note_only"
