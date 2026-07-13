import json
from pathlib import Path

from gas_calibrator.tools.export_v1_5_final_production_gap_freeze import main as cli_main
from gas_calibrator.validation.v1_5_entrypoint_inventory import classify_v1_5_entrypoint
from gas_calibrator.validation.v1_5_final_production_gap_freeze import (
    CRITICAL_GAPS,
    DEFERRED_ITEMS,
    READY_STATUS,
    SCHEMA,
    build_v1_5_final_production_gap_freeze,
    write_v1_5_final_production_gap_freeze,
)


ROOT = Path(__file__).resolve().parents[1]
SOURCE_MAIN = "2dca3bcd67e35268110abd50cf4c3819b8ef330d"


def test_freeze_replaces_stale_status_with_seven_production_gaps() -> None:
    model = build_v1_5_final_production_gap_freeze(
        repository_root=ROOT, source_origin_main_commit=SOURCE_MAIN
    )

    assert model["schema"] == SCHEMA
    assert model["overall_status"] == READY_STATUS
    assert model["scope_frozen"] is True
    assert model["new_gap_requires_user_approval"] is True
    assert model["source_origin_main_commit"] == SOURCE_MAIN
    assert model["critical_gap_count"] == len(CRITICAL_GAPS) == 7
    assert model["deferred_item_count"] == len(DEFERRED_ITEMS) == 3
    assert model["recommended_next_gap_id"] == "legacy_full_flow_orchestrator_offline_replay"
    assert model["mature_fitting_baseline"] == "0613 V1.5 fitting path"
    assert model["mature_route_baseline"] == "0620/0621 clean-worktree mature physical route path"
    assert model["legacy_point_counts"] == {"co2": 45, "h2o": 13}
    assert model["new_algorithm_profile_point_counts"] == {"co2": 47, "h2o": 14}
    assert model["review_reasons"] == []
    assert all(row["status"] == "bound" for row in model["source_evidence"])
    assert "v1_5_full_flow_next_action_plan_v1_generated_20260710" in model["supersedes"]


def test_freeze_keeps_live_release_import_and_writes_locked() -> None:
    model = build_v1_5_final_production_gap_freeze(
        repository_root=ROOT, source_origin_main_commit=SOURCE_MAIN
    )

    for key in (
        "full_production_auto_allowed",
        "live_queue_execution_allowed",
        "formal_release_allowed",
        "database_import_allowed",
        "opens_com_ports",
        "controls_pressure",
        "controls_water_or_gas_routes",
        "writes_coefficients",
        "writes_sn_or_device_code",
        "connects_postgresql",
    ):
        assert model[key] is False
    assert model["not_real_acceptance_evidence"] is True


def test_freeze_defers_noncritical_scope_and_preserves_order() -> None:
    model = build_v1_5_final_production_gap_freeze(
        repository_root=ROOT, source_origin_main_commit=SOURCE_MAIN
    )
    ids = [row["gap_id"] for row in model["critical_gaps"]]
    deferred = {row["item_id"] for row in model["deferred_items"]}

    assert [row["priority"] for row in model["critical_gaps"]] == list(range(1, 8))
    assert ids[:3] == [
        "legacy_full_flow_orchestrator_offline_replay",
        "production_component_qc_and_0613_fit_matrix",
        "unified_controlled_write_readback_reverify",
    ]
    assert "historical_component_qc_backfill_writer" in deferred
    assert "root_pollution_cleanup_and_v1_v2_deletion" in deferred
    assert "noncritical_ui_report_polish" in deferred


def test_freeze_reviews_invalid_commit_or_missing_evidence(tmp_path: Path) -> None:
    model = build_v1_5_final_production_gap_freeze(
        repository_root=tmp_path, source_origin_main_commit="not-a-commit"
    )

    assert model["overall_status"] != READY_STATUS
    assert model["scope_frozen"] is False
    assert "source_origin_main_commit_invalid" in model["review_reasons"]
    assert any(reason.startswith("source_evidence_missing:") for reason in model["review_reasons"])
    assert model["opens_com_ports"] is False


def test_freeze_writer_cli_and_entrypoint_classification(tmp_path: Path) -> None:
    output_dir = tmp_path / "freeze"
    paths = write_v1_5_final_production_gap_freeze(
        output_dir=output_dir,
        repository_root=ROOT,
        source_origin_main_commit=SOURCE_MAIN,
    )
    assert all(path.is_file() for path in paths.values())
    manifest = json.loads(paths["manifest"].read_text(encoding="utf-8"))
    markdown = paths["markdown"].read_text(encoding="utf-8")
    assert manifest["overall_status"] == READY_STATUS
    assert "旧算法全流程 orchestrator 离线 replay" in markdown
    assert "新增生产缺口必须得到用户明确批准" in markdown

    cli_dir = tmp_path / "cli"
    assert cli_main(
        [
            "--repository-root",
            str(ROOT),
            "--source-origin-main-commit",
            SOURCE_MAIN,
            "--output-dir",
            str(cli_dir),
        ]
    ) == 0
    assert (cli_dir / "v1_5_final_production_gap_freeze.json").is_file()

    entry = classify_v1_5_entrypoint(
        ROOT / "src/gas_calibrator/tools/export_v1_5_final_production_gap_freeze.py",
        root=ROOT,
    )
    assert entry.category == "formal_review_evidence"
    assert entry.formal_status == "formal_support"
    assert entry.risk_level == "offline"
    assert entry.opens_com_ports is False
    assert entry.controls_routes is False
    assert entry.writes_coefficients is False
    assert any("production-gap freeze" in note for note in entry.notes)
