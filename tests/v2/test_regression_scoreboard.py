from __future__ import annotations

import json
from pathlib import Path
import sys

from gas_calibrator.v2.core.regression_scoreboard import generate_regression_scoreboard
from gas_calibrator.v2.scripts.build_offline_governance_artifacts import rebuild_run

SUPPORT_DIR = Path(__file__).resolve().parent
if str(SUPPORT_DIR) not in sys.path:
    sys.path.insert(0, str(SUPPORT_DIR))

from ui_v2_support import build_fake_service


def _build_bundle(run_root: Path) -> Path:
    run_root.mkdir(parents=True, exist_ok=True)
    service = build_fake_service(run_root)
    run_dir = Path(service.result_store.run_dir)
    rebuild_run(run_dir)
    (run_dir / "suite_summary.json").write_text(
        json.dumps(
            {
                "suite": "regression",
                "counts": {"total": 4, "passed": 3, "failed": 1},
                "cases": [
                    {"name": "full_route_success_all_temps_all_sources", "ok": True, "status": "MATCH"},
                    {"name": "humidity_generator_timeout", "ok": True, "status": "MISMATCH"},
                    {"name": "relay_stuck_channel_causes_route_mismatch", "ok": True, "status": "MISMATCH"},
                    {"name": "summary_parity", "ok": False, "status": "MISMATCH"},
                ],
            },
            ensure_ascii=False,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    (run_dir / "summary_parity_report.json").write_text(
        json.dumps(
            {"status": "MISMATCH", "not_real_acceptance_evidence": True},
            ensure_ascii=False,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    (run_dir / "export_resilience_report.json").write_text(
        json.dumps(
            {"status": "MATCH", "not_real_acceptance_evidence": True},
            ensure_ascii=False,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    (run_dir / "sidecar_index_summary.json").write_text(
        json.dumps(
            {"summary_line": "sidecar ready", "not_real_acceptance_evidence": True},
            ensure_ascii=False,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    (run_dir / "review_copilot_payload.json").write_text(
        json.dumps(
            {"risk_summary": "offline review only", "reviewer_only": True},
            ensure_ascii=False,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    (run_dir / "model_governance_summary.json").write_text(
        json.dumps(
            {"release_status": "draft", "reviewer_only": True},
            ensure_ascii=False,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    return run_dir


def test_generate_regression_scoreboard_writes_expected_outputs(tmp_path: Path) -> None:
    bundle_dir = _build_bundle(tmp_path / "current")
    output_dir = tmp_path / "scoreboard"

    scoreboard = generate_regression_scoreboard(
        current_bundle_dir=bundle_dir,
        output_dir=output_dir,
    )

    assert scoreboard["schema_version"] == "regression-scoreboard-v1"
    assert scoreboard["validation_counts"]["failed"] == 1
    assert scoreboard["surface_counts"]["total"] >= 6
    assert (output_dir / "golden_dataset_registry.json").exists()
    assert (output_dir / "regression_scoreboard.json").exists()
    assert (output_dir / "regression_scoreboard.md").exists()
    assert (output_dir / "bundle_diff_summary.json").exists()
    assert (output_dir / "artifact_schema_diff.json").exists()
    assert scoreboard["recommendation"]["recommended_bundle_label"] == "current_branch_result"
    assert "review_center_surface" in list(scoreboard.get("missing_surfaces") or []) or scoreboard["degraded_areas"]


def test_schema_diff_detects_added_removed_and_type_changed_fields(tmp_path: Path) -> None:
    baseline_dir = tmp_path / "baseline_bundle"
    current_dir = tmp_path / "current_bundle"
    baseline_dir.mkdir()
    current_dir.mkdir()

    (baseline_dir / "summary.json").write_text(
        json.dumps({"stats": {"value": 1, "note": "baseline"}}, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    (current_dir / "summary.json").write_text(
        json.dumps({"stats": {"value": "1", "extra": True}}, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    for directory in (baseline_dir, current_dir):
        for filename in ("manifest.json", "results.json", "acceptance_plan.json", "analytics_summary.json", "run_artifact_index.json"):
            (directory / filename).write_text("{}\n", encoding="utf-8")
        (directory / "ai_run_summary.md").write_text("# ai\n", encoding="utf-8")

    output_dir = tmp_path / "schema_scoreboard"
    generate_regression_scoreboard(
        current_bundle_dir=current_dir,
        baseline_bundle_dir=baseline_dir,
        output_dir=output_dir,
    )

    diff_payload = json.loads((output_dir / "artifact_schema_diff.json").read_text(encoding="utf-8"))
    changed_fields = list(diff_payload.get("changed_schema_fields") or [])

    assert any(item["field_path"] == "stats.extra" and item["change"] == "added" for item in changed_fields)
    assert any(item["field_path"] == "stats.note" and item["change"] == "removed" for item in changed_fields)
    assert any(item["field_path"] == "stats.value" and item["change"] == "type_changed" for item in changed_fields)


def test_baseline_compare_reports_removed_artifacts_and_prefers_previous_baseline(tmp_path: Path) -> None:
    baseline_dir = _build_bundle(tmp_path / "baseline")
    current_dir = _build_bundle(tmp_path / "current")
    (current_dir / "software_validation_traceability_matrix.json").unlink()
    output_dir = tmp_path / "compare_scoreboard"

    scoreboard = generate_regression_scoreboard(
        current_bundle_dir=current_dir,
        baseline_bundle_dir=baseline_dir,
        output_dir=output_dir,
        current_label="current_candidate",
        baseline_label="step2_baseline_prev",
    )

    regressions = list(scoreboard.get("artifact_regressions") or [])
    assert any(item["kind"] == "artifact_removed" for item in regressions)
    assert scoreboard["recommendation"]["recommended_bundle_label"] == "step2_baseline_prev"
