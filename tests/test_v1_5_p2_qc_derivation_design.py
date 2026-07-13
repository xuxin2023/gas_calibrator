import csv
import json
from pathlib import Path

from gas_calibrator.tools.export_v1_5_p2_qc_derivation_design import main
from gas_calibrator.validation.v1_5_entrypoint_inventory import classify_v1_5_entrypoint
from gas_calibrator.validation.v1_5_legacy_evidence_gap_task_plan import SCHEMA as TASK_SCHEMA
from gas_calibrator.validation.v1_5_p1_evidence_lineage_audit import SCHEMA as P1_SCHEMA
from gas_calibrator.validation.v1_5_p2_qc_derivation_design import (
    build_v1_5_p2_qc_derivation_design,
    write_v1_5_p2_qc_derivation_design,
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


def _point(tmp_path: Path, name: str, *, route: str = "co2", aligned: bool = True) -> Path:
    point = tmp_path / name
    ratio = "ga01_co2_ratio_f" if route == "co2" else "ga01_h2o_ratio_f"
    _csv(
        point / "samples_machine_readable.csv",
        [
            {
                "sample_index": "1",
                "point_phase": route,
                "sample_alignment_ok": str(aligned),
                "point_quality_blocked": "False",
                "ga01_analyzer_device_id": "001",
                "ga01_frame_usable": "True",
                ratio: "1.0001",
            }
        ],
    )
    _csv(
        point / "frame_quality_summary.csv",
        [
            {
                "Analyzer": "GA01",
                "AnalyzerId": "001",
                "TotalFrames": "10",
                "ValidFrames": "10",
                "ValidRatio": "1.0",
            }
        ],
    )
    _json(point / "runtime_config_snapshot.json", {"devices": {"gas_analyzer": {"device_id": "001"}}})
    if route == "co2":
        _json(
            point / "formal_open_flow_sidecar_metadata.json",
            {
                "writes_senco": False,
                "writes_device_id": False,
                "route_open_until_sample_end": True,
                "formal_sample_anchor_interval_s": 1.0,
            },
        )
        _json(point / "formal_open_flow_route_timing.json", {"sampling_before_route_close": True})
    else:
        _json(
            point / "formal_h2o_open_flow_sidecar_metadata.json",
            {
                "writes_senco": False,
                "writes_device_id": False,
                "route_open_until_sample_end": True,
                "formal_sample_anchor_interval_s": 1.0,
                "actual_purge_s": 720,
                "minimum_purge_s": 720,
            },
        )
        _json(point / "formal_h2o_open_flow_hgen_flow_set.json", {"flow": "reviewed"})
        _json(point / "h2o_humidity_reference_review.json", {"status": "reviewed"})
        _csv(point / "point_timing_summary.csv", [{"point": name, "status": "complete"}])
    return point


def _upstream(tmp_path: Path, p2_points: list[tuple[Path, str]], retry: Path | None = None) -> tuple[Path, Path]:
    task = {
        "schema": TASK_SCHEMA,
        "overall_status": "review_required_manual_offline_evidence_tasks",
        "artifact_integrity_mismatch_count": 0,
        "tasks": [
            {
                "priority": "P2_quality_traceability",
                "route_kind": route,
                "point_name": point.name,
                "point_dir": str(point),
                "accepted_manifest_warning": "",
            }
            for point, route in p2_points
        ],
        "opens_com_ports": False,
        "controls_pressure": False,
        "controls_water_or_gas_routes": False,
        "writes_coefficients": False,
        "writes_sn_or_device_code": False,
        "connects_postgresql": False,
        "automatic_repair_allowed": False,
        "historical_fit_allowed": False,
        "formal_release_allowed": False,
        "database_import_allowed": False,
        "not_real_acceptance_evidence": True,
    }
    audit = {
        "schema": P1_SCHEMA,
        "overall_status": "review_required_p1_lineage_audit_complete",
        "points": [],
        "candidates": [],
        "opens_com_ports": False,
        "controls_pressure": False,
        "controls_water_or_gas_routes": False,
        "writes_coefficients": False,
        "writes_sn_or_device_code": False,
        "connects_postgresql": False,
        "automatic_file_copy_allowed": False,
        "automatic_qc_derivation_allowed": False,
        "historical_fit_allowed": False,
        "formal_release_allowed": False,
        "database_import_allowed": False,
        "not_real_acceptance_evidence": True,
    }
    if retry:
        audit["points"] = [{"point_name": "original", "route_kind": "co2"}]
        audit["candidates"] = [
            {
                "original_point_name": "original",
                "candidate_point_name": retry.name,
                "candidate_point_dir": str(retry),
                "core_evidence_recovery_candidate": True,
                "component_qc_still_required": True,
            }
        ]
    return _json(tmp_path / "task.json", task), _json(tmp_path / "p1.json", audit)


def test_complete_co2_h2o_and_p1_retry_are_design_candidates_but_execution_locked(tmp_path: Path) -> None:
    co2 = _point(tmp_path, "p001_T40_0ppm_fit", route="co2")
    h2o = _point(tmp_path, "p001_T20_HG20C_30RH_h2o", route="h2o")
    retry = _point(tmp_path, "p006_Tm20_1000ppm_fit_retry1", route="co2")
    task, audit = _upstream(tmp_path, [(co2, "co2"), (h2o, "h2o")], retry=retry)
    model = build_v1_5_p2_qc_derivation_design(
        task_plan_json_path=task, p1_audit_json_path=audit
    )
    assert model["candidate_count"] == 3
    assert model["input_complete_count"] == 3
    assert model["input_incomplete_count"] == 0
    assert model["reviewed_generator_available"] is False
    assert model["qc_derivation_execution_allowed"] is False
    assert model["historical_fit_allowed"] is False
    assert any(row["source_role"] == "p1_same_lineage_retry_reference" for row in model["candidates"])


def test_p1_retry_reference_deduplicates_same_p2_directory_and_preserves_stronger_role(
    tmp_path: Path,
) -> None:
    retry = _point(tmp_path, "p006_Tm20_1000ppm_fit_retry1", route="co2")
    task, audit = _upstream(tmp_path, [(retry, "co2")], retry=retry)
    model = build_v1_5_p2_qc_derivation_design(
        task_plan_json_path=task, p1_audit_json_path=audit
    )
    assert model["p2_source_reference_count"] == 1
    assert model["p1_recovery_source_reference_count"] == 1
    assert model["source_reference_count"] == 2
    assert model["candidate_count"] == 1
    assert model["duplicate_source_reference_count"] == 1
    assert model["source_role_counts"] == {"p1_same_lineage_retry_reference": 1}
    assert model["candidates"][0]["source_role"] == "p1_same_lineage_retry_reference"


def test_alignment_failure_and_warning_remain_manual_review(tmp_path: Path) -> None:
    point = _point(tmp_path, "p001_T40_0ppm_fit", aligned=False)
    task, audit = _upstream(tmp_path, [(point, "co2")])
    payload = json.loads(task.read_text(encoding="utf-8"))
    payload["tasks"][0]["accepted_manifest_warning"] = "parent_queue_stopped"
    task.write_text(json.dumps(payload), encoding="utf-8")
    model = build_v1_5_p2_qc_derivation_design(
        task_plan_json_path=task, p1_audit_json_path=audit
    )
    row = model["candidates"][0]
    assert row["input_complete"] is True
    assert row["manual_gate_review_required"] is True
    assert row["sample_alignment_false_count"] == 1
    assert row["classification"] == "input_complete_manual_gate_review_generator_missing"


def test_analyzer_prefix_mismatch_is_input_incomplete(tmp_path: Path) -> None:
    point = _point(tmp_path, "p001_T40_0ppm_fit")
    frame = point / "frame_quality_summary.csv"
    _csv(
        frame,
        [
            {
                "Analyzer": "GA02",
                "AnalyzerId": "002",
                "TotalFrames": "10",
                "ValidFrames": "10",
                "ValidRatio": "1.0",
            }
        ],
    )
    task, audit = _upstream(tmp_path, [(point, "co2")])
    model = build_v1_5_p2_qc_derivation_design(
        task_plan_json_path=task, p1_audit_json_path=audit
    )
    row = model["candidates"][0]
    assert row["input_complete"] is False
    assert "sample_frame_analyzer_prefix_mismatch" in row["input_gap_codes"]
    assert row["derivation_design_review_candidate"] is False


def test_invalid_upstream_lock_fails_closed(tmp_path: Path) -> None:
    point = _point(tmp_path, "p001_T40_0ppm_fit")
    task, audit = _upstream(tmp_path, [(point, "co2")])
    payload = json.loads(audit.read_text(encoding="utf-8"))
    payload["automatic_qc_derivation_allowed"] = True
    audit.write_text(json.dumps(payload), encoding="utf-8")
    model = build_v1_5_p2_qc_derivation_design(
        task_plan_json_path=task, p1_audit_json_path=audit
    )
    assert model["overall_status"] == "blocked_invalid_upstream_evidence"
    assert model["candidate_count"] == 0
    assert "p1_audit_automatic_qc_derivation_allowed_not_false" in model["upstream_blocker_codes"]


def test_writer_cli_and_entrypoint_are_offline(tmp_path: Path) -> None:
    point = _point(tmp_path, "p001_T40_0ppm_fit")
    task, audit = _upstream(tmp_path, [(point, "co2")])
    model = build_v1_5_p2_qc_derivation_design(
        task_plan_json_path=task, p1_audit_json_path=audit
    )
    outputs = write_v1_5_p2_qc_derivation_design(model, tmp_path / "direct")
    rc = main(
        [
            "--task-plan-json-path",
            str(task),
            "--p1-audit-json-path",
            str(audit),
            "--output-dir",
            str(tmp_path / "cli"),
        ]
    )
    entry = classify_v1_5_entrypoint(
        Path("src/gas_calibrator/tools/export_v1_5_p2_qc_derivation_design.py"),
        root=Path.cwd(),
    )
    assert rc == 0
    assert outputs["candidates_csv"].is_file()
    assert outputs["artifacts_csv"].is_file()
    assert entry.category == "formal_review_evidence"
    assert entry.risk_level == "offline"
    assert entry.opens_com_ports is False
    assert entry.controls_routes is False
    assert entry.writes_coefficients is False
