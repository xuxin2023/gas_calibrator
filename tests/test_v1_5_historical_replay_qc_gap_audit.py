import csv
import json
from pathlib import Path

from gas_calibrator.tools.export_v1_5_historical_replay_qc_gap_audit import main as cli_main
from gas_calibrator.validation.v1_5_historical_replay_qc_gap_audit import (
    build_v1_5_historical_replay_qc_gap_audit,
    write_v1_5_historical_replay_qc_gap_audit,
)


def _write_csv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def _write_replay_evidence(tmp_path: Path) -> Path:
    same_run = tmp_path / "same_run"
    p040 = same_run / "p040_Tm10_0ppm_fit"
    p017 = tmp_path / "new_algo" / "p017_T20_200ppm_fit"
    p099 = tmp_path / "raw_only" / "p099_T20_900ppm_fit"
    for point_dir in (p040, p017, p099):
        point_dir.mkdir(parents=True, exist_ok=True)
        (point_dir / "io_20260701.csv").write_text("timestamp,value\n1,ok\n", encoding="utf-8")
    manifest = same_run / "quality_backfill_20260701" / "queue_manifest_with_quality.csv"
    _write_csv(
        manifest,
        [
            {
                "point_run_id": "p040_Tm10_0ppm_fit",
                "status": "failed",
                "failure_category": "dewpoint_unstable",
                "failure_reason": "dewpoint_tail_slope_too_large",
                "quality_grade": "C_reject",
                "sample_can_enter_calibration_fit": "False",
                "sample_can_enter_diagnostic_model": "False",
                "quality_reason": "dewpoint_tail_slope_too_large",
            }
        ],
    )
    cross_run = tmp_path / "cross_run" / "legacy" / "p017_T20_200ppm_fit"
    cross_run.mkdir(parents=True, exist_ok=True)
    _write_csv(
        cross_run / "formal_open_flow_data_quality_by_analyzer.csv",
        [
            {
                "label": "ga01",
                "prefix": "ga01",
                "grade": "A_calibration_eligible",
                "ratio_key": "ga01_co2_ratio_f",
                "reason": "",
                "sample_can_enter_calibration_fit": "True",
                "sample_can_enter_diagnostic_model": "True",
            }
        ],
    )
    payload = {
        "manifest": {"schema": "fixture"},
        "points": [
            {
                "family_id": "mature_0620_legacy_ratio",
                "route_kind": "co2",
                "point_id": "p040_Tm10_0ppm_fit",
                "temp_c": -10.0,
                "co2_ppm": 0.0,
                "hgen_c": None,
                "rh_pct": None,
                "point_path": str(p040),
                "quality_source": "",
            },
            {
                "family_id": "new_algorithm_shadow_run",
                "route_kind": "co2",
                "point_id": "p017_T20_200ppm_fit",
                "temp_c": 20.0,
                "co2_ppm": 200.0,
                "hgen_c": None,
                "rh_pct": None,
                "point_path": str(p017),
                "quality_source": "",
            },
            {
                "family_id": "mature_0620_legacy_ratio",
                "route_kind": "co2",
                "point_id": "p099_T20_900ppm_fit",
                "temp_c": 20.0,
                "co2_ppm": 900.0,
                "hgen_c": None,
                "rh_pct": None,
                "point_path": str(p099),
                "quality_source": "",
            },
        ],
    }
    replay = tmp_path / "v1_5_historical_replay_evidence.json"
    replay.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return replay


def _summary_by_point(model: dict) -> dict[str, dict]:
    return {row["point_id"]: row for row in model["missing_points"]}


def test_qc_gap_audit_finds_reject_only_cross_run_and_raw_only(tmp_path: Path) -> None:
    replay = _write_replay_evidence(tmp_path)

    model = build_v1_5_historical_replay_qc_gap_audit(
        replay_evidence_path=replay,
        search_roots=[tmp_path / "cross_run"],
    )
    summaries = _summary_by_point(model)

    assert model["manifest"]["status"] == "review_required"
    assert model["manifest"]["missing_qc_point_count"] == 3
    assert summaries["p040_Tm10_0ppm_fit"]["recommendation"] == "bind_same_run_reject_only_quality"
    assert summaries["p017_T20_200ppm_fit"]["recommendation"] == "cross_run_reference_only_find_same_run_qc_or_retry"
    assert summaries["p099_T20_900ppm_fit"]["recommendation"] == "raw_only_generate_qc_or_rerun_targeted_point"
    assert any(
        row["bind_decision"] == "cross_run_reference_not_direct_bind"
        and row["point_id"] == "p017_T20_200ppm_fit"
        for row in model["candidate_evidence"]
    )
    assert not any(
        row["bind_decision"] == "bindable_point_quality"
        and row["point_id"] == "p017_T20_200ppm_fit"
        for row in model["candidate_evidence"]
    )


def test_qc_gap_audit_writer_and_cli(tmp_path: Path) -> None:
    replay = _write_replay_evidence(tmp_path)
    output = tmp_path / "out"

    outputs = write_v1_5_historical_replay_qc_gap_audit(
        replay_evidence_path=replay,
        search_roots=[tmp_path / "cross_run"],
        output_dir=output,
    )
    manifest = json.loads(Path(outputs["manifest"]).read_text(encoding="utf-8"))
    assert manifest["manifest"]["status"] == "review_required"
    assert Path(outputs["markdown"]).read_text(encoding="utf-8").startswith("# V1.5 Historical Replay QC Gap Audit")
    assert (output / "v1_5_historical_replay_qc_candidate_evidence.csv").exists()

    cli_output = tmp_path / "cli"
    rc = cli_main(
        [
            "--replay-evidence-path",
            str(replay),
            "--output-dir",
            str(cli_output),
            "--search-root",
            str(tmp_path / "cross_run"),
            "--fail-on-blocker",
        ]
    )

    assert rc == 0
    assert (cli_output / "v1_5_historical_replay_qc_gap_audit.json").exists()
