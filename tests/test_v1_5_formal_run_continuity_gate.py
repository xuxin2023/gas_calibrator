import json
from pathlib import Path

from gas_calibrator.tools.export_v1_5_formal_run_continuity_gate import main as cli_main
from gas_calibrator.validation.v1_5_entrypoint_inventory import classify_v1_5_entrypoint
from gas_calibrator.validation.v1_5_formal_run_continuity_gate import (
    build_v1_5_formal_run_continuity_gate,
    write_v1_5_formal_run_continuity_gate,
)


ROOT = Path(__file__).resolve().parents[1]


def _write_ledger(tmp_path: Path, payload: dict) -> Path:
    tmp_path.mkdir(parents=True, exist_ok=True)
    path = tmp_path / "segment_ledger.json"
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def _base_segment(**overrides: object) -> dict:
    row = {
        "segment_id": "co2_single",
        "parent_formal_run_id": "co2_formal_001",
        "route_kind": "co2",
        "algorithm_profile": "legacy_ratio_production",
        "runner": "gas_calibrator.tools.run_v1_5_formal_co2_open_flow_queue",
        "queue_csv": "D:/gas_calibrator/_worktrees/v1_5_fixed_wait_window_gate_1aee26d_clean/logs/canonical_open_flow_points/co2_runner_queue.csv",
        "parameter_hash": "hash-final",
        "selected_points": 45,
        "ok_points": 45,
        "failed_points": 0,
        "running_points": 0,
        "status": "ok",
        "fit_eligible": True,
    }
    row.update(overrides)
    return row


def test_continuous_single_segment_co2_passes(tmp_path: Path) -> None:
    ledger = _write_ledger(
        tmp_path,
        {
            "formal_run_id": "co2_formal_001",
            "route_kind": "co2",
            "algorithm_profile": "legacy_ratio_production",
            "segments": [_base_segment()],
        },
    )

    model = build_v1_5_formal_run_continuity_gate(ledger_path=ledger)

    assert model["manifest"]["status"] == "pass"
    assert model["manifest"]["blocker_count"] == 0
    assert model["manifest"]["continuous_formal_run"] is True
    assert model["manifest"]["expected_point_count"] == 45
    assert model["manifest"]["opens_com_ports"] is False
    assert model["manifest"]["writes_coefficients"] is False
    assert model["manifest"]["not_real_acceptance_evidence"] is True


def test_segmented_run_without_accepted_manifest_is_blocked(tmp_path: Path) -> None:
    ledger = _write_ledger(
        tmp_path,
        {
            "formal_run_id": "co2_formal_002",
            "route_kind": "co2",
            "algorithm_profile": "legacy_ratio_production",
            "segments": [
                _base_segment(
                    segment_id="g3",
                    parent_formal_run_id="co2_formal_002",
                    selected_points=3,
                    ok_points=2,
                    failed_points=1,
                    fit_eligible=False,
                    segment_reason="dewpoint rebound interrupted 40C segment",
                ),
                _base_segment(
                    segment_id="g4",
                    parent_formal_run_id="co2_formal_002",
                    selected_points=42,
                    ok_points=42,
                    segment_reason="resume after dry source recovery",
                    supersedes_points="g3:p002",
                ),
            ],
        },
    )

    model = build_v1_5_formal_run_continuity_gate(ledger_path=ledger)
    policies = {row["policy"] for row in model["findings"]}

    assert model["manifest"]["status"] == "blocked"
    assert "segmented_run_missing_accepted_manifest" in policies
    assert model["manifest"]["continuous_formal_run"] is False


def test_segmented_run_with_complete_ledger_requires_review_not_pass(tmp_path: Path) -> None:
    ledger = _write_ledger(
        tmp_path,
        {
            "formal_run_id": "co2_formal_003",
            "route_kind": "co2",
            "algorithm_profile": "legacy_ratio_production",
            "accepted_manifest_path": "D:/gas_calibrator/_p9_20260705/co2_acceptance/accepted_co2_45_point_manifest.csv",
            "accepted_point_count": 45,
            "queue_source_review_id": "reviewed_source_supersedence_001",
            "parameter_change_review_id": "reviewed_finalparams_001",
            "segments": [
                _base_segment(
                    segment_id="g3",
                    parent_formal_run_id="co2_formal_003",
                    selected_points=3,
                    ok_points=2,
                    failed_points=1,
                    fit_eligible=False,
                    segment_reason="dewpoint rebound interrupted 40C/400; point superseded by retry",
                    supersedes_points="none",
                    parameter_hash="hash-a",
                    parameter_change_review_id="reviewed_finalparams_001",
                ),
                _base_segment(
                    segment_id="g10_retry",
                    parent_formal_run_id="co2_formal_003",
                    selected_points=1,
                    ok_points=1,
                    fit_eligible=True,
                    source_kind="retry_or_direct_recovery",
                    segment_reason="accepted manifest selected this retry for the failed 40C/400 point",
                    supersedes_points="g3:p002_T40_400ppm_fit",
                    parameter_hash="hash-b",
                    parameter_change_review_id="reviewed_finalparams_001",
                ),
                _base_segment(
                    segment_id="g4_remaining",
                    parent_formal_run_id="co2_formal_003",
                    selected_points=42,
                    ok_points=42,
                    fit_eligible=True,
                    segment_reason="resume remaining mature queue after interruption",
                    supersedes_points="none",
                    parameter_hash="hash-b",
                    parameter_change_review_id="reviewed_finalparams_001",
                ),
            ],
        },
    )

    model = build_v1_5_formal_run_continuity_gate(ledger_path=ledger)

    assert model["manifest"]["status"] == "review_required"
    assert model["manifest"]["blocker_count"] == 0
    assert model["manifest"]["review_required_count"] >= 1
    assert model["manifest"]["continuous_formal_run"] is False
    assert model["manifest"]["segmented_run_requires_ledger_review"] is True


def test_parameter_changes_without_review_are_blocked(tmp_path: Path) -> None:
    ledger = _write_ledger(
        tmp_path,
        {
            "formal_run_id": "co2_formal_004",
            "route_kind": "co2",
            "algorithm_profile": "legacy_ratio_production",
            "accepted_manifest_path": "D:/gas_calibrator/_p9_20260705/accepted_co2_45_point_manifest.csv",
            "accepted_point_count": 45,
            "segments": [
                _base_segment(
                    segment_id="g3",
                    parent_formal_run_id="co2_formal_004",
                    selected_points=3,
                    ok_points=3,
                    segment_reason="first segment",
                    parameter_hash="hash-a",
                ),
                _base_segment(
                    segment_id="g4",
                    parent_formal_run_id="co2_formal_004",
                    selected_points=42,
                    ok_points=42,
                    segment_reason="continued after manual restart",
                    parameter_hash="hash-b",
                ),
            ],
        },
    )

    model = build_v1_5_formal_run_continuity_gate(ledger_path=ledger)
    policies = {row["policy"] for row in model["findings"]}

    assert model["manifest"]["status"] == "blocked"
    assert "parameter_hash_changed_without_review" in policies


def test_gate_blocks_worker_handoff_0624_and_root_migration_references(tmp_path: Path) -> None:
    ledger = _write_ledger(
        tmp_path,
        {
            "formal_run_id": "co2_formal_005",
            "route_kind": "co2",
            "algorithm_profile": "legacy_ratio_production",
            "segments": [
                _base_segment(
                    runner="src/gas_calibrator/tools/run_v1_5_formal_open_flow_sampling.py",
                    queue_csv="D:/gas_calibrator/_handoff/v1_5_formal_queue_migration_20260624/co2_runner_queue.csv",
                    selected_points=45,
                    ok_points=45,
                )
            ],
        },
    )

    model = build_v1_5_formal_run_continuity_gate(ledger_path=ledger)
    policies = {row["policy"] for row in model["findings"]}

    assert model["manifest"]["status"] == "blocked"
    assert "sampling_worker_not_formal_queue_segment" in policies
    assert "handoff_0624_or_scratch_reference" in policies


def test_h2o_remaining_segments_need_accepted_manifest_and_reason(tmp_path: Path) -> None:
    ledger = _write_ledger(
        tmp_path,
        {
            "formal_run_id": "h2o_formal_001",
            "route_kind": "h2o",
            "algorithm_profile": "legacy_ratio_production",
            "segments": [
                {
                    **_base_segment(
                        segment_id="h2o_g4",
                        parent_formal_run_id="h2o_formal_001",
                        route_kind="h2o",
                        runner="gas_calibrator.tools.run_v1_5_formal_h2o_open_flow_queue",
                        queue_csv="D:/gas_calibrator/_worktrees/v1_5_fixed_wait_window_gate_1aee26d_clean/logs/canonical_open_flow_points/h2o_runner_queue.csv",
                        selected_points=2,
                        ok_points=1,
                        failed_points=1,
                        fit_eligible=False,
                        segment_reason="PACE vent NO_RESPONSE stopped 10C/30RH",
                    ),
                    "algorithm_profile": "legacy_ratio_production",
                },
                {
                    **_base_segment(
                        segment_id="h2o_g5_remaining",
                        parent_formal_run_id="h2o_formal_001",
                        route_kind="h2o",
                        runner="gas_calibrator.tools.run_v1_5_formal_h2o_open_flow_queue",
                        queue_csv="D:/gas_calibrator/_p9_20260706/h2o_remaining_p003_p013_queue.csv",
                        selected_points=11,
                        ok_points=11,
                        fit_eligible=True,
                        segment_reason="remaining p003-p013 after h2o_g4 interruption",
                        supersedes_points="h2o_g4:p002",
                    ),
                    "algorithm_profile": "legacy_ratio_production",
                },
            ],
        },
    )

    model = build_v1_5_formal_run_continuity_gate(ledger_path=ledger)
    policies = {row["policy"] for row in model["findings"]}

    assert model["manifest"]["status"] == "blocked"
    assert model["manifest"]["expected_point_count"] == 13
    assert "segmented_run_missing_accepted_manifest" in policies


def test_writer_and_cli(tmp_path: Path) -> None:
    ledger = _write_ledger(
        tmp_path,
        {
            "formal_run_id": "co2_formal_006",
            "route_kind": "co2",
            "algorithm_profile": "legacy_ratio_production",
            "segments": [_base_segment(parent_formal_run_id="co2_formal_006")],
        },
    )

    outputs = write_v1_5_formal_run_continuity_gate(ledger_path=ledger, output_dir=tmp_path / "out")
    model = json.loads(outputs["manifest"].read_text(encoding="utf-8"))
    markdown = outputs["markdown"].read_text(encoding="utf-8")

    assert model["manifest"]["status"] == "pass"
    assert "V1.5 Formal Run Continuity Gate" in markdown
    assert "continuous_formal_run" in markdown

    assert cli_main(["--ledger-path", str(ledger), "--output-dir", str(tmp_path / "cli")]) == 0

    bad = _write_ledger(tmp_path / "bad", {"formal_run_id": "bad", "route_kind": "co2", "segments": []})
    assert cli_main(["--ledger-path", str(bad), "--output-dir", str(tmp_path / "cli_bad"), "--fail-on-blocker"]) == 2


def test_exporter_is_offline_review_evidence() -> None:
    entry = classify_v1_5_entrypoint(
        ROOT / "src/gas_calibrator/tools/export_v1_5_formal_run_continuity_gate.py",
        root=ROOT,
    )

    assert entry.category == "formal_review_evidence"
    assert entry.formal_status == "formal_support"
    assert entry.risk_level == "offline"
    assert entry.opens_com_ports is False
    assert entry.controls_routes is False
    assert entry.writes_coefficients is False
