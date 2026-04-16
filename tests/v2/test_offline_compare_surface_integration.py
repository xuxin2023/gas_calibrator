from __future__ import annotations

import json
import sys
from pathlib import Path

from gas_calibrator.v2.adapters.results_gateway import ResultsGateway

SUPPORT_DIR = Path(__file__).resolve().parent
if str(SUPPORT_DIR) not in sys.path:
    sys.path.insert(0, str(SUPPORT_DIR))

from ui_v2_support import build_fake_facade


def _write_compare_only_bundle(run_dir: Path) -> None:
    compare_root = run_dir / "v1_v2_compare"
    compare_dir = compare_root / "compare_fixed"
    compare_dir.mkdir(parents=True, exist_ok=True)
    (compare_dir / "v1_route_trace.jsonl").write_text("{}\n", encoding="utf-8")
    (compare_dir / "v2_route_trace.jsonl").write_text("{}\n", encoding="utf-8")
    (compare_dir / "route_trace_diff.txt").write_text("sample_end mismatch\n", encoding="utf-8")
    (compare_dir / "point_presence_diff.json").write_text("{}", encoding="utf-8")
    (compare_dir / "sample_count_diff.json").write_text("{}", encoding="utf-8")
    (compare_dir / "artifact_inventory.json").write_text(
        json.dumps({"complete": True}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    (compare_dir / "control_flow_compare_report.md").write_text("# compare\n", encoding="utf-8")
    compare_bundle = {
        "generated_at": "2026-04-16T12:00:00",
        "compare_status": "MISMATCH",
        "overall_match": False,
        "evidence_source": "simulated_protocol",
        "evidence_state": "simulated_compare",
        "diagnostic_only": True,
        "acceptance_evidence": False,
        "not_real_acceptance_evidence": True,
        "first_failure_phase": "sample_end",
        "metadata": {
            "validation_profile": "replacement_skip0_co2_only_simulated",
            "run_name": "compare_fixed",
        },
        "route_execution_summary": {
            "target_route": "co2",
            "first_failure_phase": "sample_end",
            "has_physical_route_mismatches": True,
        },
        "presence": {"matches": True},
        "sample_count": {"matches": False},
        "route_sequence": {"matches": False},
        "key_actions": {
            "pressure": {"matches": True},
            "vent": {"matches": False},
        },
        "artifacts": {
            "v1_route_trace": "v1_route_trace.jsonl",
            "v2_route_trace": "v2_route_trace.jsonl",
            "route_trace_diff": "route_trace_diff.txt",
            "point_presence_diff": "point_presence_diff.json",
            "sample_count_diff": "sample_count_diff.json",
            "control_flow_compare_report_json": "control_flow_compare_report.json",
            "control_flow_compare_report_markdown": "control_flow_compare_report.md",
            "artifact_inventory": "artifact_inventory.json",
        },
    }
    (compare_dir / "control_flow_compare_report.json").write_text(
        json.dumps(compare_bundle, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def test_offline_compare_surface_integration_smoke(tmp_path: Path) -> None:
    facade = build_fake_facade(tmp_path)
    run_dir = Path(facade.result_store.run_dir)
    _write_compare_only_bundle(run_dir)

    gateway = ResultsGateway(
        run_dir,
        output_files_provider=facade.service.get_output_files,
    )
    results_payload = gateway.read_results_payload()
    reports_payload = gateway.read_reports_payload()
    snapshot = facade.build_results_snapshot()

    summary = dict(results_payload.get("offline_diagnostic_adapter_summary", {}) or {})
    compare_pack = next(
        pack
        for pack in list(results_payload.get("compact_summary_packs") or [])
        if pack["summary_key"] == "control_flow_compare"
    )
    compare_item = next(
        item
        for item in list(snapshot["review_center"]["evidence_items"] or [])
        if str(item.get("path") or "").endswith("control_flow_compare_report.json")
    )
    report_row = next(
        row
        for row in list(reports_payload["files"] or [])
        if str(row.get("path") or "").endswith("control_flow_compare_report.json")
    )

    assert summary["control_flow_compare_count"] == 1
    assert summary["latest_control_flow_compare"]["sample_count_diff"] == "diff_present"
    assert summary["latest_control_flow_compare"]["route_trace_diff"] == "diff_present"
    assert summary["latest_control_flow_compare"]["key_action_mismatches"] == ["vent"]
    assert compare_pack["compare_status"] == "MISMATCH"
    assert compare_pack["reviewer_only"] is True
    assert results_payload["step2_closeout_package"]["compare_available"] is True
    assert results_payload["step2_freeze_audit"]["compare_available"] is True
    assert results_payload["step2_final_closure_matrix"]["compare_available"] is True
    assert report_row["artifact_key"] == "v1_v2_control_flow_compare_report"
    assert report_row["artifact_role"] == "diagnostic_analysis"
    assert "V1/V2 离线对齐" in compare_item["detail_text"]
    assert "样本数差异: 存在差异" in compare_item["detail_text"]
    assert "物理气路不一致: 是" in compare_item["detail_text"]
