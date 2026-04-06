from __future__ import annotations

from datetime import datetime, timedelta
import json
from pathlib import Path
import sys
import time

import gas_calibrator.v2.ui_v2.review_center_artifact_scope as artifact_scope
from gas_calibrator.v2.ui_v2.i18n import t
from gas_calibrator.v2.review_surface_formatter import build_review_scope_payload_reviewer_display
from gas_calibrator.v2.ui_v2.review_center_presenter import (
    build_artifact_scope_view,
    build_review_center_selection_snapshot,
    build_review_center_view,
)
from gas_calibrator.v2.ui_v2.review_scope_export_index import (
    build_review_scope_export_entry,
    build_review_scope_batch_id,
    write_review_scope_export_index,
)
from gas_calibrator.v2.ui_v2.widgets.review_center_panel import ReviewCenterPanel

SUPPORT_DIR = Path(__file__).resolve().parent
if str(SUPPORT_DIR) not in sys.path:
    sys.path.insert(0, str(SUPPORT_DIR))

from ui_v2_support import build_fake_facade, make_root


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def test_review_center_builds_cross_run_index_from_recent_runs(tmp_path: Path) -> None:
    facade = build_fake_facade(tmp_path)
    run_root = Path(facade.result_store.run_dir).parent
    now = datetime.now()

    run_a = run_root / "review_run_a"
    run_b = run_root / "review_run_b"
    facade.add_recent_run(str(run_a))
    facade.add_recent_run(str(run_b))

    _write_json(
        run_a / "suite_summary.json",
        {
            "suite": "smoke",
            "generated_at": now.isoformat(timespec="seconds"),
            "counts": {"passed": 3, "total": 3},
            "all_passed": True,
            "evidence_source": "simulated_protocol",
            "evidence_state": "collected",
            "not_real_acceptance_evidence": True,
        },
    )
    _write_json(
        run_a / "workbench_action_report.json",
        {
            "generated_at": (now - timedelta(hours=2)).isoformat(timespec="seconds"),
            "summary_line": "pressure reference degraded; diagnostic snapshot retained",
            "risk_level": "medium",
            "evidence_source": "simulated",
            "evidence_state": "simulated_workbench",
            "not_real_acceptance_evidence": True,
            "current_device_display": "pressure gauge",
            "current_action_display": "run preset",
        },
    )
    _write_json(
        run_b / "summary_parity_report.json",
        {
            "generated_at": (now - timedelta(days=2)).isoformat(timespec="seconds"),
            "status": "MATCH",
            "evidence_source": "diagnostic",
            "evidence_state": "collected",
            "not_real_acceptance_evidence": True,
            "summary": {"cases_matched": 5, "cases_total": 5, "failed_cases": []},
        },
    )
    _write_json(
        run_b / "export_resilience_report.json",
        {
            "generated_at": (now - timedelta(days=5)).isoformat(timespec="seconds"),
            "status": "MISMATCH",
            "evidence_source": "diagnostic",
            "evidence_state": "collected",
            "not_real_acceptance_evidence": True,
            "cases": [
                {"name": "json_export", "status": "MATCH"},
                {"name": "csv_export", "status": "MISMATCH"},
            ],
        },
    )
    _write_json(
        run_b / "analytics_summary.json",
        {
            "generated_at": (now - timedelta(hours=6)).isoformat(timespec="seconds"),
            "evidence_source": "simulated",
            "evidence_state": "collected",
            "not_real_acceptance_evidence": True,
            "analyzer_coverage": {"coverage_text": "1/1"},
            "reference_quality_statistics": {
                "reference_quality": "degraded",
                "reference_quality_trend": "drift",
            },
            "export_resilience_status": {"overall_status": "degraded"},
            "digest": {
                "summary": "coverage 1/1 | reference degraded | exports degraded | lineage cfg-index",
                "health": "attention",
            },
        },
    )
    _write_json(
        run_b / "lineage_summary.json",
        {
            "generated_at": (now - timedelta(hours=6)).isoformat(timespec="seconds"),
            "config_version": "cfg-index",
            "points_version": "pts-index",
            "profile_version": "profile-index",
        },
    )

    review_center = facade.build_results_snapshot()["review_center"]
    source_labels = {str(item.get("source_label") or "") for item in review_center["evidence_items"]}
    types = {str(item.get("type") or "") for item in review_center["evidence_items"]}

    assert review_center["index_summary"]["recent_runs"] >= 2
    assert review_center["index_summary"]["suite_count"] >= 1
    assert review_center["index_summary"]["parity_count"] >= 1
    assert review_center["index_summary"]["resilience_count"] >= 1
    assert review_center["index_summary"]["workbench_count"] >= 1
    assert review_center["index_summary"]["analytics_count"] >= 1
    assert review_center["index_summary"]["source_kind_counts"]["run"] >= 2
    assert review_center["index_summary"]["source_kind_counts"]["suite"] >= 1
    assert review_center["index_summary"]["source_kind_counts"]["workbench"] >= 1
    assert review_center["index_summary"]["sources"]
    assert review_center["index_summary"]["gapped_sources"] >= 1
    assert review_center["index_summary"]["coverage_gaps_display"]
    assert review_center["index_summary"]["source_kind_summary"]
    assert review_center["index_summary"]["coverage_summary"]
    assert review_center["index_summary"]["diagnostics_summary"]
    assert review_center["filters"]["source_options"]
    assert {"review_run_a", "review_run_b"} <= source_labels
    assert {"suite", "parity", "resilience", "workbench", "analytics"} <= types
    assert any(item["source_label"] == "review_run_a" for item in review_center["index_summary"]["sources"])
    assert all(str(item.get("coverage_display") or "").strip() for item in review_center["index_summary"]["sources"])
    assert all(str(item.get("gaps_display") or "").strip() for item in review_center["index_summary"]["sources"])
    assert all(bool(item.get("not_real_acceptance_evidence", False)) for item in review_center["evidence_items"])
    assert review_center["risk_summary"]["level"] in {"high", "medium"}
    assert "acceptance" in review_center["disclaimer"].lower()


def test_review_center_artifact_discovery_uses_cache_budget_and_recent_run_invalidation(tmp_path: Path) -> None:
    facade = build_fake_facade(tmp_path)
    run_root = Path(facade.result_store.run_dir).parent
    nested_run = run_root / "review_run_nested"
    invalidating_run = run_root / "review_run_invalidation"
    facade.add_recent_run(str(nested_run))

    _write_json(
        nested_run / "nested" / "analytics_summary.json",
        {
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "evidence_source": "simulated",
            "evidence_state": "collected",
            "not_real_acceptance_evidence": True,
            "analyzer_coverage": {"coverage_text": "1/1"},
            "reference_quality_statistics": {"reference_quality": "healthy", "reference_quality_trend": "healthy"},
            "export_resilience_status": {"overall_status": "ok"},
            "digest": {"summary": "coverage 1/1 | healthy", "health": "healthy"},
        },
    )
    _write_json(
        nested_run / "nested" / "lineage_summary.json",
        {
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "config_version": "cfg-cache",
            "points_version": "pts-cache",
            "profile_version": "profile-cache",
        },
    )

    first_review_center = facade.build_results_snapshot()["review_center"]
    second_review_center = facade.build_results_snapshot()["review_center"]
    facade.add_recent_run(str(invalidating_run))
    third_review_center = facade.build_results_snapshot()["review_center"]

    first_diagnostics = dict(first_review_center.get("diagnostics", {}) or {})
    second_diagnostics = dict(second_review_center.get("diagnostics", {}) or {})
    third_diagnostics = dict(third_review_center.get("diagnostics", {}) or {})

    assert first_diagnostics["cache_hit"] is False
    assert first_diagnostics["scanned_root_count"] >= 1
    assert first_diagnostics["scanned_candidate_count"] >= 1
    assert first_diagnostics["scan_budget_used"] > 0
    assert first_diagnostics["elapsed_ms"] >= 0
    assert second_diagnostics["cache_hit"] is True
    assert third_diagnostics["cache_hit"] is False
    assert any(item["type"] == "analytics" for item in first_review_center["evidence_items"])


def test_review_center_source_drilldown_keeps_duplicate_labels_distinct_in_headless_view(tmp_path: Path) -> None:
    facade = build_fake_facade(tmp_path)
    run_root = Path(facade.result_store.run_dir).parent
    duplicate_a = run_root / "branch_a" / "shared_run"
    duplicate_b = run_root / "branch_b" / "shared_run"
    facade.add_recent_run(str(duplicate_a))
    facade.add_recent_run(str(duplicate_b))

    _write_json(
        duplicate_a / "analytics_summary.json",
        {
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "evidence_source": "simulated",
            "evidence_state": "collected",
            "not_real_acceptance_evidence": True,
            "analyzer_coverage": {"coverage_text": "1/1"},
            "reference_quality_statistics": {
                "reference_quality": "healthy",
                "reference_quality_trend": "steady",
            },
            "export_resilience_status": {"overall_status": "ok"},
            "digest": {"summary": "coverage 1/1 | healthy | lineage cfg-a", "health": "healthy"},
        },
    )
    _write_json(
        duplicate_a / "lineage_summary.json",
        {
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "config_version": "cfg-a",
            "points_version": "pts-a",
            "profile_version": "profile-a",
        },
    )
    _write_json(
        duplicate_b / "analytics_summary.json",
        {
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "evidence_source": "simulated",
            "evidence_state": "collected",
            "not_real_acceptance_evidence": True,
            "analyzer_coverage": {"coverage_text": "1/1"},
            "reference_quality_statistics": {
                "reference_quality": "degraded",
                "reference_quality_trend": "drift",
            },
            "export_resilience_status": {"overall_status": "degraded"},
            "digest": {"summary": "coverage 1/1 | degraded | lineage cfg-b", "health": "attention"},
        },
    )
    _write_json(
        duplicate_b / "lineage_summary.json",
        {
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "config_version": "cfg-b",
            "points_version": "pts-b",
            "profile_version": "profile-b",
        },
    )

    review_center = facade.build_results_snapshot()["review_center"]
    duplicate_sources = [
        dict(item)
        for item in list(review_center["index_summary"]["sources"])
        if str(item.get("source_label") or "") == "shared_run"
    ]
    decorated_sources = [
        dict(item)
        for item in list(build_review_center_view(review_center).get("sources", []) or [])
        if str(item.get("source_label") or "") == "shared_run"
    ]

    assert len(duplicate_sources) == 2
    assert len({str(item.get("source_id") or "") for item in duplicate_sources}) == 2
    assert len({str(item.get("source_label_display") or "") for item in decorated_sources}) == 2
    assert all("(" in str(item.get("source_label_display") or "") for item in decorated_sources)

    filtered_view = build_review_center_view(
        review_center,
        selected_source_id=str(duplicate_sources[0]["source_id"]),
    )

    assert filtered_view["source_scope_active"] is True
    assert filtered_view["selected_source_row"]["source_label"] == "shared_run"
    assert all(
        str(item.get("source_id") or "") == str(duplicate_sources[0]["source_id"])
        for item in filtered_view["items"]
    )
    assert "shared_run" in filtered_view["index_text"]
    assert "acceptance" in filtered_view["readiness_summary"].lower()
    assert "offline only" not in filtered_view["readiness_summary"].lower()
    assert "shared_run" in filtered_view["operator_summary"]
    assert filtered_view["analytics_summary"]
    assert filtered_view["lineage_summary"]


def test_review_center_artifact_scope_helper_handles_all_source_and_evidence_without_tk() -> None:
    payload = {
        "index_summary": {
            "sources": [
                {
                    "source_id": "D:/tmp/branch_a/shared_run",
                    "source_label": "shared_run",
                    "source_dir": "D:/tmp/branch_a/shared_run",
                    "source_scope": "run",
                    "source_scope_display": t("results.review_center.source_kind.run"),
                    "latest_display": "03-27 10:00",
                    "coverage_display": "2/5 | suite / analytics",
                    "gaps_display": "missing parity / resilience / workbench",
                    "evidence_count": 2,
                },
                {
                    "source_id": "D:/tmp/branch_b/shared_run",
                    "source_label": "shared_run",
                    "source_dir": "D:/tmp/branch_b/shared_run",
                    "source_scope": "run",
                    "source_scope_display": t("results.review_center.source_kind.run"),
                    "latest_display": "03-27 11:00",
                    "coverage_display": "1/5 | parity",
                    "gaps_display": "missing suite / resilience / workbench / analytics",
                    "evidence_count": 1,
                },
            ]
        },
        "filters": {
            "time_options": [{"id": "all", "label": t("results.review_center.filter.all_time"), "window_seconds": None}]
        },
        "evidence_items": [
            {
                "type": "suite",
                "type_display": t("results.review_center.type.suite"),
                "status": "passed",
                "status_display": t("results.review_center.status.passed"),
                "summary": "suite a",
                "detail_summary": "suite a",
                "generated_at_display": "03-27 10:00",
                "sort_key": time.time(),
                "source_kind": "run",
                "source_scope": "run",
                "source_id": "D:/tmp/branch_a/shared_run",
                "source_label": "shared_run",
                "source_dir": "D:/tmp/branch_a/shared_run",
                "detail_artifact_paths": [
                    "D:/tmp/branch_a/shared_run/suite_summary.json",
                    "D:/tmp/branch_a/shared_run/manifest.json",
                ],
            },
            {
                "type": "analytics",
                "type_display": t("results.review_center.type.analytics"),
                "status": "degraded",
                "status_display": t("results.review_center.status.degraded"),
                "summary": "analytics a",
                "detail_summary": "analytics a",
                "generated_at_display": "03-27 10:05",
                "sort_key": time.time(),
                "source_kind": "run",
                "source_scope": "run",
                "source_id": "D:/tmp/branch_a/shared_run",
                "source_label": "shared_run",
                "source_dir": "D:/tmp/branch_a/shared_run",
                "detail_artifact_paths": ["D:/tmp/branch_a/shared_run/analytics_summary.json"],
            },
            {
                "type": "parity",
                "type_display": t("results.review_center.type.parity"),
                "status": "failed",
                "status_display": t("results.review_center.status.failed"),
                "summary": "parity b",
                "detail_summary": "parity b",
                "generated_at_display": "03-27 11:00",
                "sort_key": time.time(),
                "source_kind": "run",
                "source_scope": "run",
                "source_id": "D:/tmp/branch_b/shared_run",
                "source_label": "shared_run",
                "source_dir": "D:/tmp/branch_b/shared_run",
                "detail_artifact_paths": ["D:/tmp/branch_b/shared_run/summary_parity_report.json"],
            },
        ],
    }
    files = [
        {"name": "suite_summary.json", "present": True, "path": "D:/tmp/branch_a/shared_run/suite_summary.json"},
        {"name": "analytics_summary.json", "present": True, "path": "D:/tmp/branch_a/shared_run/analytics_summary.json"},
        {"name": "summary_parity_report.json", "present": True, "path": "D:/tmp/branch_b/shared_run/summary_parity_report.json"},
    ]

    view_all = build_review_center_view(payload)
    all_scope = build_artifact_scope_view(files, selection=view_all["selection_snapshot"])
    source_scope_snapshot = build_review_center_selection_snapshot(
        build_review_center_view(payload, selected_source_id="D:/tmp/branch_a/shared_run"),
        scope="source",
    )
    source_scope = build_artifact_scope_view(files, selection=source_scope_snapshot)
    evidence_scope_snapshot = build_review_center_selection_snapshot(
        build_review_center_view(payload, selected_source_id="D:/tmp/branch_a/shared_run"),
        scope="evidence",
        selected_item=dict(build_review_center_view(payload, selected_source_id="D:/tmp/branch_a/shared_run")["items"][0]),
    )
    evidence_scope = build_artifact_scope_view(files, selection=evidence_scope_snapshot)

    assert all_scope["scope"] == "all"
    assert len(all_scope["rows"]) == 3
    assert source_scope_snapshot["selected_source_label_display"].startswith("shared_run")
    assert source_scope_snapshot["selected_source_visible_count"] == 2
    assert len(source_scope["rows"]) == 3
    assert "shared_run" in source_scope["summary_text"]
    assert "当前范围" in source_scope["summary_text"]
    assert "存在" in source_scope["summary_text"]
    assert "外部" in source_scope["summary_text"]
    assert "当前运行基线" in source_scope["summary_text"]
    assert "visible " not in source_scope["summary_text"]
    assert "external " not in source_scope["summary_text"]
    assert "catalog " not in source_scope["summary_text"]
    decorated_source = next(
        item for item in view_all["sources"] if str(item.get("source_id") or "") == "D:/tmp/branch_a/shared_run"
    )
    assert "当前筛选" in str(decorated_source.get("scope_count_display") or "")
    assert "filtered " not in str(decorated_source.get("scope_count_display") or "")
    assert evidence_scope["scope"] == "evidence"
    assert len(evidence_scope["rows"]) == 2
    assert evidence_scope["rows"][0]["path"].endswith("suite_summary.json")
    assert evidence_scope["rows"][1]["artifact_origin"] == "missing_reference"
    assert evidence_scope["rows"][1]["present_on_disk"] is False


def test_review_center_artifact_scope_marks_external_present_review_reference_and_source_scan_headless(
    tmp_path: Path,
) -> None:
    current_run = tmp_path / "current_run"
    current_run.mkdir(parents=True, exist_ok=True)
    (current_run / "summary.json").write_text("{}", encoding="utf-8")
    history_dir = tmp_path / "output" / "v2_suite_runs" / "codex_nightly_review_center"
    history_dir.mkdir(parents=True, exist_ok=True)
    suite_summary = history_dir / "suite_summary.json"
    suite_summary.write_text("{}", encoding="utf-8")
    suite_markdown = history_dir / "suite_summary.md"
    suite_markdown.write_text("# suite summary\n", encoding="utf-8")

    view = build_artifact_scope_view(
        [
            {
                "name": "summary.json",
                "present": True,
                "path": str(current_run / "summary.json"),
            }
        ],
        selection={
            "scope": "source",
            "selected_source_label_display": "codex_nightly_review_center",
            "selected_source_dir": str(history_dir),
            "selected_source_artifact_paths": [str(suite_summary)],
        },
    )

    rows_by_path = {str(row.get("path") or ""): dict(row) for row in view["rows"]}

    assert view["catalog_total_count"] == 1
    assert view["catalog_present_count"] == 1
    assert view["scope_visible_count"] == 2
    assert view["scope_present_count"] == 2
    assert view["scope_external_count"] == 2
    assert view["scope_missing_count"] == 0
    assert "1/0" not in view["summary_text"]
    assert rows_by_path[str(suite_summary)]["present_on_disk"] is True
    assert rows_by_path[str(suite_summary)]["listed_in_current_run"] is False
    assert rows_by_path[str(suite_summary)]["artifact_origin"] == "review_reference"
    assert rows_by_path[str(suite_summary)]["artifact_role"] == "execution_summary"
    assert rows_by_path[str(suite_summary)]["scope_match"] == "source"
    assert rows_by_path[str(suite_markdown)]["artifact_origin"] == "source_scan"
    assert rows_by_path[str(suite_markdown)]["artifact_role"] == "execution_summary"
    assert rows_by_path[str(suite_markdown)]["present_on_disk"] is True


def test_review_center_artifact_scope_classifies_parity_review_reference_and_resilience_source_scan_headless(
    tmp_path: Path,
) -> None:
    source_dir = tmp_path / "history" / "review_diagnostics"
    source_dir.mkdir(parents=True, exist_ok=True)
    parity_report = source_dir / "summary_parity_report.json"
    resilience_report = source_dir / "export_resilience_report.json"
    parity_report.write_text("{}", encoding="utf-8")
    resilience_report.write_text("{}", encoding="utf-8")

    view = build_artifact_scope_view(
        [],
        selection={
            "scope": "source",
            "selected_source_label_display": "review_diagnostics",
            "selected_source_dir": str(source_dir),
            "selected_source_artifact_paths": [str(parity_report)],
        },
    )

    rows_by_path = {str(row.get("path") or ""): dict(row) for row in view["rows"]}
    parity_row = rows_by_path[str(parity_report)]
    resilience_row = rows_by_path[str(resilience_report)]
    diagnostic_display = t("enum.artifact_role.diagnostic_analysis")
    unregistered_display = t("widgets.artifact_list.export_status_unregistered")

    assert view["scope_external_count"] == 2
    assert parity_row["artifact_origin"] == "review_reference"
    assert parity_row["artifact_role"] == "diagnostic_analysis"
    assert parity_row["artifact_role_display"] == diagnostic_display
    assert parity_row["export_status"] is None
    assert parity_row["export_status_known"] is False
    assert parity_row["export_status_display"] == unregistered_display
    assert parity_row["role_status_display"] == (
        f"{diagnostic_display} | {unregistered_display} | "
        f"{t('widgets.artifact_list.exportability_review_reference')}"
    )
    assert resilience_row["artifact_origin"] == "source_scan"
    assert resilience_row["artifact_role"] == "diagnostic_analysis"
    assert resilience_row["artifact_role_display"] == diagnostic_display
    assert resilience_row["export_status_known"] is False
    assert resilience_row["export_status_display"] == unregistered_display
    assert resilience_row["role_status_display"] == (
        f"{diagnostic_display} | {unregistered_display} | "
        f"{t('widgets.artifact_list.exportability_source_scan')}"
    )


def test_review_center_artifact_scope_classifies_resilience_review_reference_and_parity_source_scan_headless(
    tmp_path: Path,
) -> None:
    source_dir = tmp_path / "history" / "review_diagnostics"
    source_dir.mkdir(parents=True, exist_ok=True)
    parity_report = source_dir / "summary_parity_report.json"
    resilience_report = source_dir / "export_resilience_report.json"
    parity_report.write_text("{}", encoding="utf-8")
    resilience_report.write_text("{}", encoding="utf-8")

    view = build_artifact_scope_view(
        [],
        selection={
            "scope": "evidence",
            "selected_source_label_display": "review_diagnostics",
            "selected_source_dir": str(source_dir),
            "selected_evidence_summary": "parity resilience review",
            "selected_evidence_artifact_paths": [str(resilience_report)],
        },
    )

    rows_by_path = {str(row.get("path") or ""): dict(row) for row in view["rows"]}
    parity_row = rows_by_path[str(parity_report)]
    resilience_row = rows_by_path[str(resilience_report)]
    diagnostic_display = t("enum.artifact_role.diagnostic_analysis")
    unregistered_display = t("widgets.artifact_list.export_status_unregistered")

    assert view["scope"] == "evidence"
    assert view["scope_external_count"] == 2
    assert resilience_row["artifact_origin"] == "review_reference"
    assert resilience_row["artifact_role"] == "diagnostic_analysis"
    assert resilience_row["artifact_role_display"] == diagnostic_display
    assert resilience_row["export_status_known"] is False
    assert resilience_row["export_status_display"] == unregistered_display
    assert resilience_row["role_status_display"] == (
        f"{diagnostic_display} | {unregistered_display} | "
        f"{t('widgets.artifact_list.exportability_review_reference')}"
    )
    assert parity_row["artifact_origin"] == "source_scan"
    assert parity_row["artifact_role"] == "diagnostic_analysis"
    assert parity_row["artifact_role_display"] == diagnostic_display
    assert parity_row["export_status_known"] is False
    assert parity_row["export_status_display"] == unregistered_display
    assert parity_row["role_status_display"] == (
        f"{diagnostic_display} | {unregistered_display} | "
        f"{t('widgets.artifact_list.exportability_source_scan')}"
    )


def test_review_center_artifact_scope_source_scan_degrades_safely_on_permission_error(
    tmp_path: Path,
    monkeypatch,
) -> None:
    source_dir = tmp_path / "restricted_source"
    source_dir.mkdir(parents=True, exist_ok=True)
    suite_summary = source_dir / "suite_summary.json"
    suite_summary.write_text("{}", encoding="utf-8")
    original_iterdir = artifact_scope.Path.iterdir

    def _guarded_iterdir(self):
        if self == source_dir:
            raise PermissionError("blocked")
        return original_iterdir(self)

    monkeypatch.setattr(artifact_scope.Path, "iterdir", _guarded_iterdir)

    view = build_artifact_scope_view(
        [],
        selection={
            "scope": "source",
            "selected_source_label_display": "restricted_source",
            "selected_source_dir": str(source_dir),
            "selected_source_artifact_paths": [str(suite_summary)],
        },
    )

    assert [row["path"] for row in view["rows"]] == [str(suite_summary)]
    assert view["rows"][0]["artifact_origin"] == "review_reference"
    assert view["rows"][0]["present_on_disk"] is True


def test_review_center_artifact_scope_evidence_orders_direct_then_source_scan_then_missing_headless(
    tmp_path: Path,
) -> None:
    source_dir = tmp_path / "branch_a" / "shared_run"
    source_dir.mkdir(parents=True, exist_ok=True)
    suite_summary = source_dir / "suite_summary.json"
    suite_summary.write_text("{}", encoding="utf-8")
    analytics_summary = source_dir / "analytics_summary.json"
    analytics_summary.write_text("{}", encoding="utf-8")
    missing_manifest = source_dir / "manifest.json"

    view = build_artifact_scope_view(
        [],
        selection={
            "scope": "evidence",
            "selected_source_label_display": "shared_run",
            "selected_source_dir": str(source_dir),
            "selected_evidence_summary": "suite evidence",
            "selected_evidence_artifact_paths": [
                str(suite_summary),
                str(missing_manifest),
            ],
        },
    )

    assert [row["path"] for row in view["rows"]] == [
        str(suite_summary),
        str(analytics_summary),
        str(missing_manifest),
    ]
    assert [row["artifact_origin"] for row in view["rows"]] == [
        "review_reference",
        "source_scan",
        "missing_reference",
    ]
    assert [row["scope_match"] for row in view["rows"]] == ["evidence", "source", "evidence"]


def test_review_center_artifact_scope_keeps_scope_denominator_consistent_headless(tmp_path: Path) -> None:
    current_run = tmp_path / "current_run"
    current_run.mkdir(parents=True, exist_ok=True)
    (current_run / "summary.json").write_text("{}", encoding="utf-8")
    source_dir = tmp_path / "history" / "scope_case"
    source_dir.mkdir(parents=True, exist_ok=True)
    external_suite = source_dir / "suite_summary.json"
    external_suite.write_text("{}", encoding="utf-8")

    view = build_artifact_scope_view(
        [{"name": "summary.json", "present": True, "path": str(current_run / "summary.json")}],
        selection={
            "scope": "source",
            "selected_source_label_display": "scope_case",
            "selected_source_dir": str(source_dir),
            "selected_source_artifact_paths": [str(external_suite)],
        },
    )

    assert view["catalog_total_count"] == 1
    assert view["catalog_present_count"] == 1
    assert view["scope_visible_count"] == 1
    assert view["scope_present_count"] == 1
    assert view["scope_external_count"] == 1
    assert view["scope_missing_count"] == 0
    assert view["total_count"] == view["scope_visible_count"]
    assert "1/1" in view["summary_text"]
    assert "1/0" not in view["summary_text"]
    assert "当前范围" in view["summary_text"]
    assert "存在" in view["summary_text"]
    assert "外部" in view["summary_text"]
    assert "当前运行基线" in view["summary_text"]
    assert "visible " not in view["summary_text"]
    assert "external " not in view["summary_text"]
    assert "catalog " not in view["summary_text"]
    assert "当前运行基线" in view["run_dir_note_text"]
    assert "可见" in view["scope_note_text"]
    assert "外部" in view["scope_note_text"]
    assert "存在" in view["present_note_text"]
    assert view["reviewer_display"]["summary_text"] == view["summary_text"]
    assert view["reviewer_display"]["run_dir_note_text"] == view["run_dir_note_text"]
    assert view["reviewer_display"]["scope_note_text"] == view["scope_note_text"]
    assert view["reviewer_display"]["present_note_text"] == view["present_note_text"]
    assert view["reviewer_display"]["catalog_note_text"] == view["catalog_note_text"]
    assert "catalog " not in view["run_dir_note_text"]
    assert "visible " not in view["scope_note_text"]
    assert "present " not in view["present_note_text"]


def test_review_scope_manifest_payload_marks_reference_only_rows_headless(tmp_path: Path) -> None:
    current_run = tmp_path / "current_run"
    current_run.mkdir(parents=True, exist_ok=True)
    summary_file = current_run / "summary.json"
    summary_file.write_text("{}", encoding="utf-8")
    source_dir = tmp_path / "history" / "manifest_case"
    source_dir.mkdir(parents=True, exist_ok=True)
    suite_file = source_dir / "suite_summary.json"
    suite_file.write_text("{}", encoding="utf-8")
    missing_file = source_dir / "manifest.json"

    payload = artifact_scope.build_review_scope_manifest_payload(
        [
            {
                "name": "summary.json",
                "path": str(summary_file),
                "present": True,
                "listed_in_current_run": True,
                "artifact_role": "execution_summary",
                "export_status": "ok",
                "export_status_known": True,
                "exportable_in_current_run": True,
            }
        ],
        selection={
            "scope": "evidence",
            "selected_source_label_display": "manifest_case",
            "selected_source_dir": str(source_dir),
            "selected_evidence_summary": "suite summary",
            "selected_evidence_artifact_paths": [str(suite_file), str(missing_file)],
        },
        run_dir=str(current_run),
    )

    assert payload["scope_summary"]["scope"] == "evidence"
    assert payload["scope_summary"]["scope_visible_count"] == 2
    assert payload["scope_summary"]["scope_present_count"] == 1
    assert payload["reviewer_display"]["selection_line"] == "范围=evidence | 来源=manifest_case | 证据=suite summary"
    assert payload["reviewer_display"]["counts_line"] == "可见 2 | 存在 1 | 外部 1 | 缺少 1 | 当前运行基线 1/1"
    assert payload["disclaimer"]["not_real_acceptance_evidence"] is True
    assert payload["selection"]["selected_evidence_summary"] == "suite summary"
    assert payload["rows"][0]["artifact_role"] == "execution_summary"
    assert payload["rows"][0]["exportable_in_current_run"] is False
    assert "当前运行 exporter" in payload["rows"][0]["note"] or "current-run exporter" in payload["rows"][0]["note"]
    assert payload["rows"][1]["present_on_disk"] is False
    markdown = artifact_scope.render_review_scope_manifest_markdown(payload)
    assert "范围=evidence" in markdown
    assert "来源=manifest_case" in markdown
    assert "证据=suite summary" in markdown
    assert "可见" in markdown
    assert "存在" in markdown
    assert "外部" in markdown
    assert "当前运行基线" in markdown
    assert "scope=" not in markdown
    assert "source=" not in markdown
    assert "evidence=" not in markdown
    assert "visible " not in markdown
    assert "external " not in markdown
    assert "catalog " not in markdown


def test_review_scope_export_index_keeps_handoff_history_without_overwriting(tmp_path: Path) -> None:
    destination = tmp_path / "ui_exports" / "review_scope"
    destination.mkdir(parents=True, exist_ok=True)
    payload_all = {
        "generated_at": "2026-03-28T14:22:10+00:00",
        "selection": {
            "scope": "all",
            "selected_source_label_display": "",
            "selected_evidence_summary": "",
        },
        "scope_summary": {
            "scope": "all",
            "scope_label": "All",
            "catalog_total_count": 12,
            "catalog_present_count": 8,
            "scope_visible_count": 8,
            "scope_present_count": 8,
            "scope_external_count": 0,
            "scope_missing_count": 0,
        },
        "disclaimer": {
            "offline_review_only": True,
            "simulated_or_replay_context": True,
            "diagnostic_context": True,
            "not_real_acceptance_evidence": True,
        },
    }
    batch_a = build_review_scope_batch_id(destination, scope="all", generated_at=payload_all["generated_at"])
    json_a = destination / f"{batch_a}.json"
    md_a = destination / f"{batch_a}.md"
    json_a.write_text("{}", encoding="utf-8")
    md_a.write_text("# batch a\n", encoding="utf-8")
    first_index = write_review_scope_export_index(
        destination,
        run_dir=tmp_path / "run_a",
        payload=payload_all,
        batch_id=batch_a,
        exported_files=[str(json_a), str(md_a)],
    )

    payload_source = {
        "generated_at": "2026-03-28T14:22:10+00:00",
        "selection": {
            "scope": "source",
            "selected_source_label_display": "history_run",
            "selected_evidence_summary": "",
        },
        "scope_summary": {
            "scope": "source",
            "scope_label": "Source",
            "catalog_total_count": 12,
            "catalog_present_count": 8,
            "scope_visible_count": 3,
            "scope_present_count": 2,
            "scope_external_count": 2,
            "scope_missing_count": 1,
        },
        "disclaimer": {
            "offline_review_only": True,
            "simulated_or_replay_context": True,
            "diagnostic_context": True,
            "not_real_acceptance_evidence": True,
        },
    }
    batch_b = build_review_scope_batch_id(destination, scope="source", generated_at=payload_source["generated_at"])
    json_b = destination / f"{batch_b}.json"
    md_b = destination / f"{batch_b}.md"
    json_b.write_text("{}", encoding="utf-8")
    md_b.write_text("# batch b\n", encoding="utf-8")
    second_index = write_review_scope_export_index(
        destination,
        run_dir=tmp_path / "run_a",
        payload=payload_source,
        batch_id=batch_b,
        exported_files=[str(json_b), str(md_b)],
    )

    assert batch_a == "review_scope_20260328_142210_all"
    assert batch_b == "review_scope_20260328_142210_source"
    assert first_index["entry_count"] == 1
    assert second_index["entry_count"] == 2
    assert second_index["latest"]["batch_id"] == batch_b
    assert second_index["previous"]["batch_id"] == batch_a
    assert second_index["latest"]["selection_snapshot"]["scope"] == "source"
    assert second_index["latest"]["selection_snapshot"]["selected_source_label_display"] == "history_run"
    assert second_index["latest"]["summary_counts"]["scope_visible_count"] == 3
    assert second_index["latest"]["summary_counts"]["scope_missing_count"] == 1
    assert second_index["latest"]["exported_files"] == [str(json_b), str(md_b)]
    assert second_index["latest"]["reviewer_display"]["selection_line"] == "范围=source | 来源=history_run | 证据=无"
    assert second_index["latest"]["reviewer_display"]["counts_line"] == "可见 3 | 存在 2 | 外部 2 | 缺少 1 | 当前运行基线 8/12"
    assert "当前审阅视角" in second_index["latest"]["reviewer_display"]["run_dir_note_text"]
    assert "当前可见 3" in second_index["latest"]["reviewer_display"]["scope_note_text"]
    assert "当前范围 磁盘存在 2/3" in second_index["latest"]["reviewer_display"]["present_note_text"]
    assert second_index["latest"]["disclaimer_flags"]["offline_review_only"] is True
    assert second_index["latest"]["disclaimer_flags"]["not_real_acceptance_evidence"] is True
    assert second_index["latest"]["selection_snapshot"]["scope"] == "source"
    assert second_index["latest"]["summary_counts"]["catalog_present_count"] == 8
    assert json.loads((destination / "index.json").read_text(encoding="utf-8"))["entry_count"] == 2


def test_review_scope_manifest_and_export_index_share_reviewer_display_lines() -> None:
    payload = {
        "generated_at": "2026-03-28T14:22:10+00:00",
        "selection": {
            "scope": "source",
            "selected_source_label_display": "history_run",
            "selected_evidence_summary": "suite summary",
        },
        "scope_summary": {
            "scope": "source",
            "scope_label": "Source",
            "catalog_total_count": 12,
            "catalog_present_count": 8,
            "scope_visible_count": 3,
            "scope_present_count": 2,
            "scope_external_count": 2,
            "scope_missing_count": 1,
        },
        "disclaimer": {
            "text": "offline review only",
            "offline_review_only": True,
            "simulated_or_replay_context": True,
            "diagnostic_context": True,
            "not_real_acceptance_evidence": True,
        },
        "rows": [],
    }
    payload["reviewer_display"] = build_review_scope_payload_reviewer_display(
        selection=payload["selection"],
        scope_summary=payload["scope_summary"],
    )

    markdown = artifact_scope.render_review_scope_manifest_markdown(payload)

    export_entry = build_review_scope_export_entry(
        payload,
        batch_id="review_scope_20260328_142210_source",
        exported_files=["D:/tmp/review_scope_source.json"],
    )

    assert payload["reviewer_display"]["selection_line"] == "范围=source | 来源=history_run | 证据=suite summary"
    assert payload["reviewer_display"]["counts_line"] == "可见 3 | 存在 2 | 外部 2 | 缺少 1 | 当前运行基线 8/12"
    assert "当前审阅视角" in payload["reviewer_display"]["run_dir_note_text"]
    assert "当前可见 3" in payload["reviewer_display"]["scope_note_text"]
    assert "当前范围 磁盘存在 2/3" in payload["reviewer_display"]["present_note_text"]
    assert export_entry["reviewer_display"] == payload["reviewer_display"]
    assert export_entry["reviewer_display"]["selection_line"] in markdown
    assert export_entry["reviewer_display"]["counts_line"] in markdown
    assert "scope=" not in markdown
    assert "source=" not in markdown
    assert "evidence=" not in markdown
    assert payload["selection"]["scope"] == "source"
    assert payload["scope_summary"]["scope_visible_count"] == 3
    assert payload["selection"]["selected_source_label_display"] == "history_run"
    assert export_entry["selection_snapshot"]["scope"] == "source"
    assert export_entry["summary_counts"]["scope_visible_count"] == 3


def test_review_center_panel_exposes_index_summary_and_time_source_filters() -> None:
    root = make_root()
    try:
        panel = ReviewCenterPanel(root, compact=True)
        now = time.time()
        payload = {
            "operator_focus": {"summary": "operator"},
            "reviewer_focus": {"summary": "reviewer"},
            "approver_focus": {"summary": "approver"},
                "risk_summary": {
                    "level": "medium",
                    "level_display": t("results.review_center.risk.medium"),
                    "summary": t(
                        "results.review_center.risk.summary",
                        level=t("results.review_center.risk.medium"),
                        failed=0,
                        degraded=1,
                        diagnostic=0,
                        missing=0,
                        coverage="suite / parity",
                    ),
                },
            "acceptance_readiness": {"summary": "offline readiness only"},
            "analytics_summary": {"summary": "analytics"},
            "lineage_summary": {"summary": "lineage"},
            "index_summary": {
                "summary": "recent sources 2 | suite 1 | parity 1 | resilience 0 | workbench 0",
                "source_kind_summary": "sources by kind | run 1 | suite 1 | workbench 0",
                "coverage_summary": "coverage | complete 0 | gapped 2 | missing parity / resilience",
                "diagnostics_summary": "diagnostics | cache no | roots 2 | candidates 4 | elapsed 12 ms | budget 6",
                "sources": [
                    {
                        "source_label": "review_run_a",
                        "latest_display": "03-27 10:00",
                        "coverage_display": "2/4 | suite / workbench",
                        "gaps_display": "missing parity / resilience",
                    },
                    {
                        "source_label": "review_run_b",
                        "latest_display": "03-20 10:00",
                        "coverage_display": "2/4 | parity / resilience",
                        "gaps_display": "missing suite / workbench",
                    },
                ],
            },
            "filters": {
                "selected_type": "all",
                "selected_status": "all",
                "selected_time": "all",
                "selected_source": "all",
                "type_options": [
                    {"id": "all", "label": t("results.review_center.filter.all_types")},
                    {"id": "suite", "label": t("results.review_center.type.suite")},
                    {"id": "parity", "label": t("results.review_center.type.parity")},
                ],
                "status_options": [
                    {"id": "all", "label": t("results.review_center.filter.all_statuses")},
                    {"id": "passed", "label": t("results.review_center.status.passed")},
                    {"id": "failed", "label": t("results.review_center.status.failed")},
                ],
                "time_options": [
                    {"id": "all", "label": t("results.review_center.filter.all_time"), "window_seconds": None},
                    {"id": "24h", "label": t("results.review_center.filter.time_24h"), "window_seconds": 86400},
                    {"id": "7d", "label": t("results.review_center.filter.time_7d"), "window_seconds": 604800},
                ],
                "source_options": [
                    {"id": "all", "label": t("results.review_center.filter.all_sources")},
                    {"id": "suite", "label": t("results.review_center.source_kind.suite")},
                    {"id": "run", "label": t("results.review_center.source_kind.run")},
                ],
            },
            "evidence_items": [
                {
                    "type": "suite",
                    "type_display": t("results.review_center.type.suite"),
                    "status": "passed",
                    "status_display": t("results.review_center.status.passed"),
                    "generated_at_display": "03-27 10:00",
                    "sort_key": now - 3600,
                    "summary": "smoke passed",
                    "detail_text": "suite detail",
                    "source_kind": "suite",
                    "detail_hint": t(
                        "results.review_center.detail.source_hint",
                        kind=t("results.review_center.source_kind.suite"),
                        source="review_run_a",
                    ),
                },
                {
                    "type": "parity",
                    "type_display": t("results.review_center.type.parity"),
                    "status": "failed",
                    "status_display": t("results.review_center.status.failed"),
                    "generated_at_display": "03-20 10:00",
                    "sort_key": now - (9 * 86400),
                    "summary": "parity failed",
                    "detail_text": "parity detail",
                    "source_kind": "run",
                    "detail_hint": t(
                        "results.review_center.detail.source_hint",
                        kind=t("results.review_center.source_kind.run"),
                        source="review_run_b",
                    ),
                },
            ],
            "detail_hint": "select evidence to inspect details",
            "empty_detail": "no evidence",
            "disclaimer": "offline evidence only; not real acceptance.",
        }

        panel.render(payload)
        assert panel.index_var.get().startswith("recent sources 2")
        assert "sources by kind" in panel.index_var.get()
        assert "\u8986\u76d6 | \u5b8c\u6574" in panel.index_var.get()
        assert "diagnostics | cache no" in panel.index_var.get()
        assert panel.risk_var.get() == payload["risk_summary"]["summary"]
        assert len(panel.source_tree.get_children()) == 2
        assert len(panel.tree.get_children()) == 2

        panel.time_filter_var.set(t("results.review_center.filter.time_7d"))
        panel._apply_filters()
        assert len(panel.tree.get_children()) == 1
        values = panel.tree.item(panel.tree.get_children()[0], "values")
        assert values[1] == t("results.review_center.type.suite")

        panel.time_filter_var.set(t("results.review_center.filter.all_time"))
        panel.source_filter_var.set(t("results.review_center.source_kind.run"))
        panel._apply_filters()
        assert len(panel.tree.get_children()) == 1
        values = panel.tree.item(panel.tree.get_children()[0], "values")
        assert values[1] == t("results.review_center.type.parity")
        source_values = panel.source_tree.item(panel.source_tree.get_children()[0], "values")
        assert source_values[0] == "review_run_a"
    finally:
        root.destroy()


def test_artifact_scope_decorate_source_rows_humanizes_raw_coverage_fragments() -> None:
    rows = [
        {
            "source_id": "D:/tmp/shared_run",
            "source_label": "shared_run",
            "source_scope": "run",
            "coverage_display": "coverage | complete 0 | gapped 2 | missing parity / resilience",
            "gaps_display": "missing suite / analytics",
            "evidence_count": 2,
        }
    ]

    decorated = artifact_scope.decorate_source_rows(
        rows,
        visible_items=[],
        item_matcher=lambda item, row: False,
    )

    assert decorated[0]["coverage_display"] == "\u8986\u76d6 | \u5b8c\u6574 0 | \u7f3a\u53e3 2 | \u7f3a\u5c11 parity / resilience"
    assert decorated[0]["gaps_display"] == "\u7f3a\u5c11 suite / analytics"
    assert "\u8986\u76d6 | \u5b8c\u6574 0 | \u7f3a\u53e3 2 | \u7f3a\u5c11 parity / resilience" in decorated[0]["scope_count_display"]
    assert rows[0]["coverage_display"] == "coverage | complete 0 | gapped 2 | missing parity / resilience"
