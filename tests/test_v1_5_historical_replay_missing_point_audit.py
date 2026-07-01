import csv
import json
from pathlib import Path

from gas_calibrator.tools.export_v1_5_historical_replay_missing_point_audit import main as cli_main
from gas_calibrator.validation.v1_5_historical_replay_missing_point_audit import (
    build_v1_5_historical_replay_missing_point_audit,
    write_v1_5_historical_replay_missing_point_audit,
)


ROOT = Path(__file__).resolve().parents[1]
PROFILE_PATH = ROOT / "configs" / "v1_5_algorithm_route_profiles.json"


def _write_csv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def _write_replay_evidence(tmp_path: Path) -> Path:
    payload = {
        "manifest": {"schema": "fixture"},
        "route_summaries": [
            {
                "family_id": "new_algorithm_shadow_run",
                "route_kind": "co2",
                "algorithm_profile_id": "absorption_ratio_shadow",
                "status": "review_required",
                "missing_expected_points": ["-10/0", "-10/600", "-20/1000"],
            },
            {
                "family_id": "new_algorithm_shadow_run",
                "route_kind": "h2o",
                "algorithm_profile_id": "absorption_ratio_shadow",
                "status": "review_required",
                "missing_expected_points": ["40/30/30"],
            },
        ],
    }
    replay = tmp_path / "v1_5_historical_replay_evidence.json"
    replay.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return replay


def _write_segmented_candidates(tmp_path: Path) -> Path:
    search = tmp_path / "new_algo_segmented"
    quality_candidate = search / "co2_resume" / "p001_Tm10_0ppm_fit"
    quality_candidate.mkdir(parents=True, exist_ok=True)
    _write_csv(
        quality_candidate / "frame_quality_summary.csv",
        [
            {
                "mode": "current",
                "Analyzer": "GA01",
                "TotalFrames": "30",
                "ValidFrames": "30",
                "ValidRatio": "1.0",
                "UnusableReasonTopN": "",
            }
        ],
    )
    raw_candidate = search / "co2_resume" / "p006_Tm20_1000ppm_fit"
    raw_candidate.mkdir(parents=True, exist_ok=True)
    (raw_candidate / "io_20260701.csv").write_text("timestamp,value\n1,ok\n", encoding="utf-8")
    return search


def _summary_by_key(model: dict) -> dict[str, dict]:
    return {row["point_key"]: row for row in model["missing_points"]}


def test_missing_point_audit_separates_segmented_supplemental_and_raw_candidates(tmp_path: Path) -> None:
    replay = _write_replay_evidence(tmp_path)
    search = _write_segmented_candidates(tmp_path)
    legacy_reference = tmp_path / "legacy_reference" / "p007_Tm10_0ppm_fit"
    legacy_reference.mkdir(parents=True, exist_ok=True)
    _write_csv(
        legacy_reference / "formal_open_flow_data_quality_by_analyzer.csv",
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

    model = build_v1_5_historical_replay_missing_point_audit(
        replay_evidence_path=replay,
        profile_path=PROFILE_PATH,
        search_roots=[search, tmp_path / "legacy_reference"],
    )
    summaries = _summary_by_key(model)

    assert model["manifest"]["status"] == "review_required"
    assert model["manifest"]["missing_point_count"] == 4
    assert summaries["-10/0"]["recommendation"] == "review_bind_segmented_quality_candidate"
    assert summaries["-20/1000"]["recommendation"] == "derive_qc_from_segmented_raw_candidate"
    assert summaries["-10/600"]["is_new_algorithm_supplemental"] is True
    assert summaries["-10/600"]["recommendation"] == "targeted_supplemental_resampling_candidate"
    assert summaries["40/30/30"]["is_new_algorithm_supplemental"] is True
    assert summaries["40/30/30"]["recommendation"] == "targeted_supplemental_resampling_candidate"
    assert any(
        row["point_key"] == "-10/0"
        and row["bind_decision"] == "segmented_quality_candidate_review_bind"
        for row in model["candidate_evidence"]
    )
    assert any(
        row["point_key"] == "-10/0"
        and row["bind_decision"] == "cross_family_reference_not_direct_bind"
        for row in model["candidate_evidence"]
    )
    assert any(
        row["point_key"] == "-20/1000"
        and row["bind_decision"] == "segmented_raw_only_qc_derivation_required"
        for row in model["candidate_evidence"]
    )


def test_missing_point_audit_writer_and_cli(tmp_path: Path) -> None:
    replay = _write_replay_evidence(tmp_path)
    search = _write_segmented_candidates(tmp_path)
    output = tmp_path / "out"

    outputs = write_v1_5_historical_replay_missing_point_audit(
        replay_evidence_path=replay,
        profile_path=PROFILE_PATH,
        search_roots=[search],
        output_dir=output,
    )
    manifest = json.loads(Path(outputs["manifest"]).read_text(encoding="utf-8"))
    assert manifest["manifest"]["status"] == "review_required"
    assert Path(outputs["markdown"]).read_text(encoding="utf-8").startswith(
        "# V1.5 Historical Replay Missing Point Audit"
    )

    cli_output = tmp_path / "cli"
    rc = cli_main(
        [
            "--replay-evidence-path",
            str(replay),
            "--profile-path",
            str(PROFILE_PATH),
            "--output-dir",
            str(cli_output),
            "--search-root",
            str(search),
            "--fail-on-blocker",
        ]
    )

    assert rc == 0
    assert (cli_output / "v1_5_historical_replay_missing_point_audit.json").exists()
