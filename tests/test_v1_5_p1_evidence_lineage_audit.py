import csv
import json
from pathlib import Path

from gas_calibrator.tools.export_v1_5_p1_evidence_lineage_audit import main
from gas_calibrator.validation.v1_5_entrypoint_inventory import classify_v1_5_entrypoint
from gas_calibrator.validation.v1_5_legacy_evidence_gap_task_plan import SCHEMA as TASK_SCHEMA
from gas_calibrator.validation.v1_5_p1_evidence_lineage_audit import (
    build_v1_5_p1_evidence_lineage_audit,
    write_v1_5_p1_evidence_lineage_audit,
)


def _json(path: Path, payload: dict) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def _csv(path: Path, rows: list[dict]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    return path


def _task_plan(tmp_path: Path, point_dirs: list[Path]) -> Path:
    payload = {
        "schema": TASK_SCHEMA,
        "overall_status": "review_required_manual_offline_evidence_tasks",
        "artifact_integrity_mismatch_count": 0,
        "tasks": [
            {
                "task_id": f"task_{index}",
                "priority": "P1_core_evidence",
                "route_kind": "co2",
                "point_name": point.name,
                "point_dir": str(point),
            }
            for index, point in enumerate(point_dirs, start=1)
        ],
        "opens_com_ports": False,
        "controls_pressure": False,
        "controls_water_or_gas_routes": False,
        "writes_coefficients": False,
        "writes_sn_or_device_code": False,
        "connects_postgresql": False,
        "automatic_repair_allowed": False,
        "continuous_route_attestation_allowed": False,
        "historical_fit_allowed": False,
        "formal_release_allowed": False,
        "database_import_allowed": False,
        "not_real_acceptance_evidence": True,
    }
    return _json(tmp_path / "task_plan.json", payload)


def _failed_point(run_dir: Path, name: str, *, failure: str = "dewpoint_rebound") -> Path:
    point = run_dir / name
    _json(point / "formal_open_flow_sidecar_metadata.json", {"run_id": name})
    _json(
        point / "formal_open_flow_route_timing.json",
        {
            "run_id": name,
            "sample_window_started_at": None,
            "sample_window_ended_at": None,
            "sampling_before_route_close": False,
        },
    )
    (point / "io_20260713.csv").write_text("timestamp,value\n1,raw\n", encoding="utf-8")
    _csv(
        run_dir / "queue" / "queue_manifest.csv",
        [
            {
                "point_run_id": name,
                "status": "failed",
                "returncode": "1",
                "failure_category": failure,
                "failure_reason": f"{failure};no_sample_window",
                "quality_grade": "",
            }
        ],
    )
    return point


def _retry(run_dir: Path, name: str, *, completed: bool = True) -> Path:
    point = run_dir / name
    _json(point / "formal_open_flow_sidecar_metadata.json", {"run_id": name})
    _json(
        point / "formal_open_flow_route_timing.json",
        {
            "run_id": name,
            "sample_window_started_at": "2026-07-13T01:00:00" if completed else None,
            "sample_window_ended_at": "2026-07-13T01:00:10" if completed else None,
            "sampling_before_route_close": completed,
        },
    )
    _csv(point / "samples_machine_readable.csv", [{"label": "ga01", "ratio": "0.5"}])
    _csv(point / "frame_quality_summary.csv", [{"label": "ga01", "grade": "A"}])
    return point


def test_same_lineage_retry_resolves_core_gap_but_not_qc_or_fit(tmp_path: Path) -> None:
    lineage = tmp_path / "lineage"
    run = lineage / "run_a"
    original = _failed_point(run, "p006_Tm20_1000ppm_fit", failure="pressure_hard_limit")
    retry = _retry(run, "p006_Tm20_1000ppm_fit_retry1")
    plan = _task_plan(tmp_path, [original])

    model = build_v1_5_p1_evidence_lineage_audit(task_plan_json_path=plan)
    point = model["points"][0]
    candidate = next(row for row in model["candidates"] if row["candidate_point_name"] == retry.name)

    assert model["recoverable_reference_count"] == 1
    assert model["unrecoverable_count"] == 0
    assert point["audit_conclusion"] == "core_gap_resolved_by_same_lineage_retry_reference"
    assert candidate["core_evidence_recovery_candidate"] is True
    assert candidate["component_qc_still_required"] is True
    assert candidate["formal_fit_allowed"] is False
    assert model["automatic_file_copy_allowed"] is False
    assert model["automatic_qc_derivation_allowed"] is False


def test_failed_point_without_same_lineage_retry_is_unrecoverable(tmp_path: Path) -> None:
    original = _failed_point(
        tmp_path / "lineage" / "run_a",
        "p017_T20_200ppm_fit",
        failure="dewpoint_rebound",
    )
    plan = _task_plan(tmp_path, [original])
    model = build_v1_5_p1_evidence_lineage_audit(task_plan_json_path=plan)
    point = model["points"][0]
    assert model["recoverable_reference_count"] == 0
    assert model["unrecoverable_count"] == 1
    assert point["audit_conclusion"] == "unrecoverable_from_reviewed_lineage"
    assert point["original_attempt_io_file_count"] == 1
    assert point["manifest_failure_categories"] == ["dewpoint_rebound"]
    assert any(row["artifact_role"] == "raw_io_diagnostic_only" for row in model["artifact_inventory"])


def test_samples_without_retry_marker_or_completed_window_do_not_resolve(tmp_path: Path) -> None:
    run = tmp_path / "lineage" / "run_a"
    original = _failed_point(run, "p017_T20_200ppm_fit")
    _retry(run, "p099_T20_200ppm_fit", completed=True)
    _retry(run, "p017_T20_200ppm_fit_retry1", completed=False)
    plan = _task_plan(tmp_path, [original])
    model = build_v1_5_p1_evidence_lineage_audit(task_plan_json_path=plan)
    assert model["unrecoverable_count"] == 1
    assert not any(row["core_evidence_recovery_candidate"] for row in model["candidates"])


def test_dry_run_retry_samples_never_resolve_real_core_gap(tmp_path: Path) -> None:
    lineage = tmp_path / "lineage"
    original = _failed_point(lineage / "run_a", "p017_T20_200ppm_fit")
    _retry(lineage / "dry_run", "p017_T20_200ppm_fit_retry1", completed=True)
    plan = _task_plan(tmp_path, [original])
    model = build_v1_5_p1_evidence_lineage_audit(task_plan_json_path=plan)
    candidate = next(
        row for row in model["candidates"] if row["candidate_point_name"].endswith("retry1")
    )
    assert model["unrecoverable_count"] == 1
    assert candidate["source_run_classification"] == "dry_run_reference_only"
    assert candidate["core_evidence_recovery_candidate"] is False


def test_invalid_task_plan_fails_closed(tmp_path: Path) -> None:
    original = _failed_point(tmp_path / "lineage" / "run_a", "p017_T20_200ppm_fit")
    plan = _task_plan(tmp_path, [original])
    payload = json.loads(plan.read_text(encoding="utf-8"))
    payload["historical_fit_allowed"] = True
    plan.write_text(json.dumps(payload), encoding="utf-8")
    model = build_v1_5_p1_evidence_lineage_audit(task_plan_json_path=plan)
    assert model["overall_status"] == "blocked_invalid_task_plan"
    assert model["point_count"] == 0
    assert "task_plan_historical_fit_allowed_not_false" in model["task_plan_blocker_codes"]
    assert model["historical_fit_allowed"] is False


def test_writer_cli_and_entrypoint_are_offline(tmp_path: Path) -> None:
    original = _failed_point(tmp_path / "lineage" / "run_a", "p017_T20_200ppm_fit")
    plan = _task_plan(tmp_path, [original])
    model = build_v1_5_p1_evidence_lineage_audit(task_plan_json_path=plan)
    outputs = write_v1_5_p1_evidence_lineage_audit(model, tmp_path / "direct")
    rc = main(
        [
            "--task-plan-json-path",
            str(plan),
            "--output-dir",
            str(tmp_path / "cli"),
        ]
    )
    entry = classify_v1_5_entrypoint(
        Path("src/gas_calibrator/tools/export_v1_5_p1_evidence_lineage_audit.py"),
        root=Path.cwd(),
    )
    assert rc == 0
    assert outputs["points_csv"].is_file()
    assert outputs["artifacts_csv"].is_file()
    assert entry.category == "formal_review_evidence"
    assert entry.risk_level == "offline"
    assert entry.opens_com_ports is False
    assert entry.controls_routes is False
    assert entry.writes_coefficients is False
