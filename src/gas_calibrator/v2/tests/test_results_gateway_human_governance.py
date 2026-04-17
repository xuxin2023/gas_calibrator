from __future__ import annotations

from gas_calibrator.v2.adapters.results_gateway import ResultsGateway


def test_results_gateway_surfaces_human_governance_across_bundle_gate_and_compact_pack(sample_run_dir) -> None:
    payload = ResultsGateway(sample_run_dir).read_results_payload()

    human_pack = next(
        pack
        for pack in payload["compact_summary_packs"]
        if pack.get("summary_key") == "human_governance"
    )
    assert "OP-SIM-LI" in human_pack["summary_line"]

    closeout_category = next(
        category
        for category in payload["step2_closeout_bundle"]["evidence_categories"]
        if category.get("category_id") == "human_governance"
    )
    assert closeout_category["present"] is True
    assert closeout_category["required"] is True

    gate_check = next(
        check
        for check in payload["engineering_isolation_gate_result"]["checks"]
        if check.get("check_id") == "personnel_sop_metadata_governance"
    )
    assert gate_check["status"] in {"advisory", "pass"}
    assert "OP-SIM-LI" in gate_check["summary"]
    assert "SOP-STEP2-CAL-SIM" in gate_check["summary"]

    assert payload["run_metadata_profile"]["reviewer_only"] is True
    assert "人员/SOP治理" in payload["result_summary_text"]
