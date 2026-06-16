import csv
import json

from gas_calibrator.tools.export_v1_5_recommendation_closure import main as export_closure_main
from gas_calibrator.validation.v1_5_recommendation_closure import (
    build_v1_5_recommendation_closure,
    render_v1_5_recommendation_closure_markdown,
)


def _write(root, relative_path, text):
    path = root / relative_path
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    return path


def _seed_repo(root):
    _write(
        root,
        "src/gas_calibrator/config.py",
        "active_frame_recovery_enabled active_frame_recovery_wait_s",
    )
    _write(
        root,
        "src/gas_calibrator/workflow/runner.py",
        (
            "_active_analyzer_anchor_match_with_recovery stale_frame "
            "_assess_status_register_qc status_register_qc"
        ),
    )
    _write(
        root,
        "src/gas_calibrator/logging_utils.py",
        "status_register_qc",
    )
    _write(
        root,
        "src/gas_calibrator/tools/run_v1_5_formal_open_flow_sampling.py",
        (
            "route_open_until_sample_end gas_route_open_until_sample_end "
            "per_analyzer_ratio_stability_required "
            "independent_grade_or_reject_do_not_block_all_when_min_valid_met"
        ),
    )
    _write(
        root,
        "src/gas_calibrator/tools/run_v1_5_formal_h2o_open_flow_sampling.py",
        (
            "route_open_until_sample_end h2o_route_open_until_sample_end "
            "per_analyzer_status_register_qc_required "
            "independent_grade_or_reject_do_not_block_all_when_min_valid_met"
        ),
    )
    _write(
        root,
        "src/gas_calibrator/validation/factory_signal_health_review.py",
        "pass_factory_signal_health",
    )
    _write(
        root,
        "src/gas_calibrator/tools/export_v1_5_factory_signal_health_review.py",
        "factory_signal_health",
    )
    _write(
        root,
        "src/gas_calibrator/v1_5/orchestration/full_flow.py",
        "factory_signal_health_review requires_factory_signal_health_review",
    )
    _write(
        root,
        "src/gas_calibrator/validation/v1_5_run_evidence_status.py",
        "build_v1_5_run_evidence_status diagnostic_analysis optical_root_cause",
    )
    _write(
        root,
        "src/gas_calibrator/storage/v1_5_evidence/bundle.py",
        "diagnostic_analysis optical_root_cause",
    )
    _write(
        root,
        "src/gas_calibrator/validation/v1_5_formal_archive_closure.py",
        "build_v1_5_formal_archive_closure build_v1_5_run_evidence_status",
    )
    _write(
        root,
        "tools/generate_v15_optical_root_cause_docx.py",
        "六台气体分析仪光学根因分析增强报告",
    )
    _write(
        root,
        "tools/generate_v15_optical_root_cause_docx.py",
        "光学根因",
    )
    _write(
        root,
        "tests/test_runner_multi_analyzers.py",
        (
            "test_active_analyzer_anchor_match_recovers_from_stale_frame "
            "test_merge_analyzer_cache_rejects_unrecovered_stale_active_frame "
            "test_status_register_qc_distinguishes_missing_pass_and_fail"
        ),
    )
    _write(
        root,
        "tests/test_v1_5_formal_open_flow.py",
        "route_open_until_sample_end=False",
    )
    _write(
        root,
        "tests/test_v1_5_formal_open_flow_sampling_runner.py",
        (
            "per_analyzer_ratio_stability_required "
            "independent_grade_or_reject_do_not_block_all_when_min_valid_met"
        ),
    )
    _write(
        root,
        "tests/test_v1_5_formal_h2o_open_flow_sampling_runner.py",
        (
            "per_analyzer_h2o_ratio_stability_required "
            "independent_grade_or_reject_do_not_block_all_when_min_valid_met"
        ),
    )
    _write(
        root,
        "tests/test_v1_5_full_flow_orchestration.py",
        "test_full_flow_plan_requires_factory_signal_health_before_fit_review",
    )
    _write(
        root,
        "tests/test_v1_5_formal_archive_closure.py",
        "has_run_evidence_status",
    )


def _items(model):
    return {row["recommendation_id"]: row for row in model["items"]}


def test_recommendation_closure_marks_code_closed_and_live_evidence_gaps_partial(tmp_path):
    _seed_repo(tmp_path)
    model = build_v1_5_recommendation_closure(repo_root=tmp_path, run_dir=tmp_path / "run")
    items = _items(model)

    assert model["schema"] == "v1_5_recommendation_closure_v1"
    assert model["overall_status"] == "partially_closed"
    assert model["summary_counts"]["open"] == 0
    assert model["summary_counts"]["closed"] >= 4
    assert model["summary_counts"]["partial"] >= 2
    assert model["physical_boundaries"]["opens_com_ports"] is False
    assert model["physical_boundaries"]["writes_coefficients"] is False
    assert items["active_frame_recovery"]["status"] == "closed"
    assert items["route_open_until_sample_end"]["status"] == "closed"
    assert items["per_analyzer_independent_grade"]["status"] == "closed"
    assert items["status_register_qc_logic"]["status"] == "partial"
    assert items["optical_root_cause_report"]["status"] == "closed"
    assert "状态寄存器" in render_v1_5_recommendation_closure_markdown(model)


def test_recommendation_closure_cli_writes_json_csv_and_chinese_markdown(tmp_path, capsys):
    repo = tmp_path / "repo"
    _seed_repo(repo)
    output = tmp_path / "out"

    rc = export_closure_main(["--repo-root", str(repo), "--output-dir", str(output), "--fail-on-open"])
    result = json.loads(capsys.readouterr().out)

    assert rc == 0
    assert result["status"] == "partially_closed"
    model = json.loads((output / "v1_5_recommendation_closure.json").read_text(encoding="utf-8"))
    markdown = (output / "v1_5_recommendation_closure.md").read_text(encoding="utf-8-sig")
    rows = list(csv.DictReader((output / "v1_5_recommendation_closure.csv").open(encoding="utf-8-sig")))

    assert model["summary_counts"]["open"] == 0
    assert "V1.5 建议改进闭环表" in markdown
    assert any(row["recommendation_id"] == "factory_signal_health_gate" for row in rows)


def test_recommendation_closure_fail_on_open_returns_two(tmp_path, capsys):
    output = tmp_path / "out"

    rc = export_closure_main(["--repo-root", str(tmp_path / "empty"), "--output-dir", str(output), "--fail-on-open"])
    result = json.loads(capsys.readouterr().out)

    assert rc == 2
    assert result["summary_counts"]["open"] > 0
