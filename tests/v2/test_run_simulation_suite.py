from __future__ import annotations

import json
from pathlib import Path

from gas_calibrator.v2.scripts.run_simulation_suite import run_suite
from gas_calibrator.v2.sim.parity import build_summary_parity_report


def test_run_simulation_suite_smoke_writes_suite_summary(tmp_path: Path) -> None:
    summary = run_suite(suite_name="smoke", report_root=tmp_path, run_name="suite_smoke")

    summary_path = Path(summary["report_dir"]) / "suite_summary.json"
    markdown_path = Path(summary["report_dir"]) / "suite_summary.md"
    payload = json.loads(summary_path.read_text(encoding="utf-8"))

    assert summary["suite"] == "smoke"
    assert summary["all_passed"] is True
    assert payload["counts"]["passed"] == payload["counts"]["total"]
    assert markdown_path.exists()
    text = markdown_path.read_text(encoding="utf-8")
    assert "失败用例" in text
    assert "验收就绪度" in text
    assert "套件分析摘要" in text
    assert Path(summary["suite_analytics_summary"]).exists()
    assert Path(summary["suite_acceptance_plan"]).exists()
    assert Path(summary["suite_evidence_registry"]).exists()
    assert {case["name"] for case in payload["cases"]}.issuperset(
        {
            "full_route_success_with_relay_and_thermometer",
            "relay_stuck_channel_causes_route_mismatch",
            "pressure_reference_degraded",
            "summary_parity",
        }
    )


def test_run_simulation_suite_parity_runs_artifact_level_parity_report(tmp_path: Path) -> None:
    summary = run_suite(suite_name="parity", report_root=tmp_path, run_name="suite_parity")

    assert summary["all_passed"] is True
    parity_case = summary["cases"][0]
    assert parity_case["name"] == "summary_parity"
    assert parity_case["status"] == "MATCH"
    assert parity_case["details"]["comparison_summary"]["cases_total"] >= 1
    assert parity_case["details"]["tolerance_rules"]["default_float_abs"] > 0
    assert Path(parity_case["details"]["report_json"]).exists()


def test_build_summary_parity_report_writes_json_and_markdown(tmp_path: Path) -> None:
    result = build_summary_parity_report(report_root=tmp_path, run_name="parity_tool")

    payload = json.loads(Path(result["report_json"]).read_text(encoding="utf-8"))

    assert result["status"] == "MATCH"
    assert payload["status"] == "MATCH"
    assert {case["name"] for case in payload["cases"]} == {
        "reference_on_aligned_rows",
        "reference_pool_pressure_expansion",
    }
    assert payload["summary"]["cases_failed"] == 0
    assert payload["tolerance_rules"]["default_float_abs"] > 0
    assert Path(result["report_markdown"]).exists()
    assert "摘要口径一致性" in Path(result["report_markdown"]).read_text(encoding="utf-8")


def test_run_simulation_suite_nightly_writes_summary_and_markdown(tmp_path: Path) -> None:
    summary = run_suite(suite_name="nightly", report_root=tmp_path, run_name="suite_nightly")

    summary_path = Path(summary["summary_json"])
    markdown_path = Path(summary["summary_markdown"])
    payload = json.loads(summary_path.read_text(encoding="utf-8"))

    assert summary["suite"] == "nightly"
    assert payload["counts"]["total"] >= 10
    assert markdown_path.exists()
    text = markdown_path.read_text(encoding="utf-8")
    assert "summary_parity" in text
    assert "export_resilience" in text
    assert "pressure_gauge_wrong_unit_configuration" in text
    assert "风险=" in text
    assert "失败复盘" in text
