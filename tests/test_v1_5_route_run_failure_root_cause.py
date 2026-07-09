import csv
import json
from pathlib import Path

from gas_calibrator.tools.export_v1_5_route_run_failure_root_cause import main as cli_main
from gas_calibrator.validation.v1_5_route_run_failure_root_cause import (
    audit_v1_5_route_run_failure_root_causes,
    write_v1_5_route_run_failure_root_cause_audit,
)


def _write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = sorted({key for row in rows for key in row})
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _completed_point(root: Path, point_run_id: str) -> None:
    point = root / point_run_id
    point.mkdir(parents=True, exist_ok=True)
    _write_csv(point / "conclusion_summary.csv", [{"risk_level": "ok"}])


def test_co2_g3_dewpoint_rebound_and_stale_running_manifest_are_blocked(tmp_path: Path) -> None:
    run = tmp_path / "co2_6old_0620clean_mature45_g3_finalparams"
    _write_csv(
        run / "queue" / "queue_manifest.csv",
        [
            {
                "point_run_id": "p002_T40_400ppm_fit",
                "status": "failed",
                "failure_reason": "dewpoint_rebound_detected;max_total_wait_exceeded",
            },
            {
                "point_run_id": "p003_T40_1000ppm_fit",
                "status": "running",
            },
        ],
    )
    _completed_point(run, "p003_T40_1000ppm_fit")

    model = audit_v1_5_route_run_failure_root_causes(run_dirs=[run])
    categories = {row["category"] for row in model["findings"]}

    assert model["manifest"]["status"] == "blocked"
    assert "dry_gas_dewpoint_rebound_or_not_dry_enough" in categories
    assert "stale_running_manifest_with_completed_point_artifacts" in categories


def test_pace_vent_no_response_is_classified_for_co2_and_h2o(tmp_path: Path) -> None:
    co2 = tmp_path / "co2_6old_0620clean_mature45_g4_T30_to_Tm20_finalparams"
    h2o = tmp_path / "h2o_6old_0620clean_mature13_g4"
    _write_csv(
        co2 / "queue_manifest.csv",
        [
            {
                "point_run_id": "p015_T20_300ppm_fit",
                "status": "failed",
                "failure_reason": "Pressure controller vent command failed (ON): NO_RESPONSE",
            }
        ],
    )
    _write_csv(
        h2o / "queue_manifest.csv",
        [
            {
                "point_run_id": "p002_T10_HG10C_30RH_h2o",
                "point_id": "h2o_T10_HGEN10C_30RH_ambient",
                "status": "failed",
                "failure_reason": "Pressure controller vent command failed (ON): NO_RESPONSE",
            }
        ],
    )

    model = audit_v1_5_route_run_failure_root_causes(run_dirs=[co2, h2o])
    findings = [row for row in model["findings"] if row["category"] == "pressure_controller_vent_no_response"]

    assert len(findings) == 2
    assert {row["route_kind"] for row in findings} == {"co2", "h2o"}


def test_truncated_manifest_failure_uses_point_log_for_no_response_classification(tmp_path: Path) -> None:
    run = tmp_path / "co2_6old_0620clean_mature45_g4_T30_to_Tm20_finalparams"
    log = run / "point_logs" / "p015_T20_300ppm_fit.log"
    log.parent.mkdir(parents=True, exist_ok=True)
    log.write_text(
        "WARNING: pressure controller atmosphere hold strategy fallback -> legacy hold thread\n"
        "Pressure controller vent command failed (ON): NO_RESPONSE\n"
        "Formal open-flow sampling failed: Pressure controller vent ON failed: NO_RESPONSE\n",
        encoding="utf-8",
    )
    _write_csv(
        run / "queue_manifest.csv",
        [
            {
                "point_run_id": "p015_T20_300ppm_fit",
                "status": "failed",
                "failure_reason": "Pressure controller vent comm...",
                "point_log": str(log),
            }
        ],
    )

    model = audit_v1_5_route_run_failure_root_causes(run_dirs=[run])
    categories = {row["category"] for row in model["findings"]}

    assert "pressure_controller_vent_no_response" in categories
    assert "unclassified_failed_point" not in categories


def test_h2o_aborted_before_queue_manifest_is_not_success(tmp_path: Path) -> None:
    run = tmp_path / "h2o_6old_0620clean_mature13_g1"
    _write_csv(
        run / "queue_abort_exclusion.csv",
        [
            {
                "source_status": "aborted",
            }
        ],
    )

    model = audit_v1_5_route_run_failure_root_causes(run_dirs=[run])
    categories = {row["category"] for row in model["findings"]}

    assert "queue_aborted_before_sampling_no_manifest" in categories
    assert model["manifest"]["blocker_count"] == 1


def test_direct_retry_points_without_queue_manifest_require_supersedence_review(tmp_path: Path) -> None:
    run = tmp_path / "co2_6old_0620clean_mature45_g8c_T20_500_direct_240purge_finalparams"
    _completed_point(run, "p017_T20_500ppm_fit_retry1")

    model = audit_v1_5_route_run_failure_root_causes(run_dirs=[run])
    categories = {row["category"] for row in model["findings"]}

    assert model["manifest"]["status"] == "review_required"
    assert "direct_or_retry_point_without_queue_manifest" in categories
    assert "manual_parameter_or_execution_mode_change" in categories


def test_pressure_gauge_no_response_is_separate_from_vent_no_response(tmp_path: Path) -> None:
    run = tmp_path / "co2_6old_0620clean_mature45_g9_T10_to_Tm20_finalparams"
    _write_csv(
        run / "queue_manifest.csv",
        [
            {
                "point_run_id": "p019_Tm20_400ppm_fit",
                "status": "failed",
                "failure_reason": "Pre-seal pressure-gauge fast read failed: NO_RESPONSE; Formal open-flow sampling failed: NO_RESPONSE",
            }
        ],
    )

    model = audit_v1_5_route_run_failure_root_causes(run_dirs=[run])
    categories = {row["category"] for row in model["findings"]}

    assert "pressure_gauge_no_response" in categories


def test_writer_and_cli(tmp_path: Path) -> None:
    run = tmp_path / "h2o_6old_0620clean_mature13_g2"
    _write_csv(run / "queue_abort_exclusion.csv", [{"source_status": "aborted"}])

    outputs = write_v1_5_route_run_failure_root_cause_audit(run_dirs=[run], output_dir=tmp_path / "out")
    model = json.loads(outputs["manifest"].read_text(encoding="utf-8"))
    markdown = outputs["markdown"].read_text(encoding="utf-8")

    assert model["manifest"]["status"] == "blocked"
    assert "V1.5 Route Run Failure Root-Cause Audit" in markdown
    assert cli_main(["--run-dir", str(run), "--output-dir", str(tmp_path / "cli")]) == 0
    assert cli_main(["--run-dir", str(run), "--output-dir", str(tmp_path / "cli_block"), "--fail-on-blocker"]) == 2
