import hashlib
import json
from pathlib import Path

from gas_calibrator.tools.export_v1_5_historical_component_qc_blocked_generator_plan import (
    main,
)
from gas_calibrator.validation.v1_5_entrypoint_inventory import classify_v1_5_entrypoint
from gas_calibrator.validation.v1_5_historical_component_qc_blocked_generator_plan import (
    BLOCKED_STATUS,
    READY_STATUS,
    build_v1_5_historical_component_qc_blocked_generator_plan,
    write_v1_5_historical_component_qc_blocked_generator_plan,
)
from gas_calibrator.validation.v1_5_historical_component_qc_generator_preflight import (
    SCHEMA as PREFLIGHT_SCHEMA,
)


REVIEW_SUFFIX = Path(
    "docs/v1_5_flow_contract/historical_component_qc_blocked_generator_plan"
)


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _write_json(path: Path, payload: dict) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def _locks() -> dict:
    return {
        "preflight_available": True,
        "production_component_qc_generator_available": False,
        "historical_component_qc_generation_allowed": False,
        "historical_component_qc_write_allowed": False,
        "component_qc_backfill_allowed": False,
        "historical_fit_allowed": False,
        "formal_release_allowed": False,
        "database_import_allowed": False,
        "opens_com_ports": False,
        "controls_pressure": False,
        "controls_water_or_gas_routes": False,
        "writes_coefficients": False,
        "writes_sn_or_device_code": False,
        "connects_postgresql": False,
    }


def _bundle(tmp_path: Path) -> dict:
    point_dir = tmp_path / "historical_replay" / "p001_T40_0ppm_fit"
    point_dir.mkdir(parents=True)
    artifacts = []
    for role, filename in (
        ("samples", "samples_machine_readable.csv"),
        ("frame_qc", "frame_quality_summary.csv"),
    ):
        path = point_dir / filename
        path.write_text("field,value\nsynthetic,1\n", encoding="utf-8")
        artifacts.append(
            {
                "point_dir": str(point_dir.resolve()),
                "role": role,
                "path": str(path.resolve()),
                "recorded_sha256": _sha(path),
                "actual_sha256": _sha(path),
                "recorded_size_bytes": path.stat().st_size,
                "actual_size_bytes": path.stat().st_size,
                "status": "pass",
                "blocker_codes": [],
            }
        )
    source_paths = {}
    for role, suffix in (
        ("p2_design_json", ".json"),
        ("p2_artifact_inventory_csv", ".csv"),
        ("contract_json", ".json"),
        ("reference_evaluation_json", ".json"),
    ):
        path = tmp_path / "upstream" / f"{role}{suffix}"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(f"source={role}\n", encoding="utf-8")
        source_paths[role] = str(path.resolve())
        source_paths[
            {
                "p2_design_json": "p2_design_sha256",
                "p2_artifact_inventory_csv": "p2_artifact_inventory_sha256",
                "contract_json": "contract_file_sha256",
                "reference_evaluation_json": "reference_evaluation_sha256",
            }[role]
        ] = _sha(path)
    candidate = {
        "source_role": "p2_catalog_point",
        "route_kind": "co2",
        "point_name": point_dir.name,
        "point_dir": str(point_dir.resolve()),
        "preflight_status": "input_packet_ready_for_manual_review",
        "preflight_ready": True,
        "blocker_codes": [],
        "manual_gate_review_required": True,
        "sample_alignment_false_count": 2,
        "point_quality_blocked_count": 0,
        "purge_below_declared_minimum": False,
        "planned_output_path": str(
            (point_dir / "formal_open_flow_data_quality_by_analyzer.csv").resolve()
        ),
        "planned_output_exists": False,
        "historical_component_qc_generation_allowed": False,
        "historical_component_qc_write_allowed": False,
        "formal_fit_allowed": False,
    }
    preflight = {
        "schema": PREFLIGHT_SCHEMA,
        "overall_status": (
            "ready_for_historical_component_qc_generator_preflight_manual_review"
        ),
        "production_state": "preflight_only_generator_and_writer_blocked",
        "global_blocker_codes": [],
        "candidate_count": 1,
        "candidate_preflight_ready_count": 1,
        "candidate_blocked_count": 0,
        "manual_gate_review_count": 1,
        "artifact_check_count": len(artifacts),
        "artifact_check_blocked_count": 0,
        "source_paths": source_paths,
        "candidates": [candidate],
        "artifact_checks": artifacts,
        "locks": _locks(),
        "evidence_source": "historical_replay",
        "not_real_acceptance_evidence": True,
    }
    return {
        "point_dir": point_dir,
        "artifacts": artifacts,
        "source_paths": source_paths,
        "preflight": _write_json(tmp_path / "preflight.json", preflight),
    }


def _build(bundle: dict) -> dict:
    return build_v1_5_historical_component_qc_blocked_generator_plan(
        preflight_json_path=bundle["preflight"]
    )


def test_ready_preflight_produces_deterministic_blocked_plan_without_grades_or_writes(
    tmp_path: Path,
) -> None:
    bundle = _bundle(tmp_path)
    first = _build(bundle)
    second = _build(bundle)
    assert first == second
    assert first["overall_status"] == READY_STATUS
    assert first["blocked_generator_plan_ready"] is True
    assert first["candidate_plan_ready_count"] == 1
    assert first["candidate_blocked_count"] == 0
    assert first["source_evidence_check_count"] == 4
    assert first["source_evidence_check_blocked_count"] == 0
    assert len(first["operation_plan"]) == 1
    row = first["operation_plan"][0]
    assert row["operation_role"] == "review_only_would_write_preview"
    assert row["would_evaluate"] is False
    assert row["would_derive_grades"] is False
    assert row["would_write"] is False
    assert row["overwrite_allowed"] is False
    assert row["requires_distinct_authorization"] is True
    assert row["preflight_json_sha256"] == first["preflight_json_sha256"]
    assert "grade" not in row
    assert first["locks"]["component_qc_grade_derivation_allowed"] is False
    assert first["locks"]["historical_component_qc_write_allowed"] is False
    assert not (bundle["point_dir"] / "formal_open_flow_data_quality_by_analyzer.csv").exists()
    assert first["evidence_source"] == "historical_replay"
    assert first["not_real_acceptance_evidence"] is True


def test_artifact_drift_after_preflight_blocks_entire_operation_plan(tmp_path: Path) -> None:
    bundle = _bundle(tmp_path)
    sample = Path(bundle["artifacts"][0]["path"])
    sample.write_text(sample.read_text(encoding="utf-8") + "drift,1\n", encoding="utf-8")
    model = _build(bundle)
    assert model["overall_status"] == BLOCKED_STATUS
    assert model["operation_plan"] == []
    codes = model["candidate_reviews"][0]["blocker_codes"]
    assert "samples:artifact_size_drift_after_preflight" in codes
    assert "samples:artifact_sha256_drift_after_preflight" in codes


def test_upstream_source_hash_drift_blocks_plan(tmp_path: Path) -> None:
    bundle = _bundle(tmp_path)
    source = Path(bundle["source_paths"]["p2_design_json"])
    source.write_text("changed\n", encoding="utf-8")
    model = _build(bundle)
    assert model["overall_status"] == BLOCKED_STATUS
    assert model["operation_plan"] == []
    assert "p2_design_json:source_sha256_mismatch" in model["global_blocker_codes"]


def test_target_created_after_preflight_blocks_overwrite_and_is_unchanged(tmp_path: Path) -> None:
    bundle = _bundle(tmp_path)
    target = bundle["point_dir"] / "formal_open_flow_data_quality_by_analyzer.csv"
    target.write_text("existing-do-not-touch\n", encoding="utf-8")
    before = target.read_bytes()
    model = _build(bundle)
    assert model["overall_status"] == BLOCKED_STATUS
    assert model["operation_plan"] == []
    assert "component_qc_output_target_now_exists" in model["candidate_reviews"][0][
        "blocker_codes"
    ]
    assert target.read_bytes() == before


def test_tampered_preflight_lock_or_duplicate_candidate_fails_closed(tmp_path: Path) -> None:
    bundle = _bundle(tmp_path)
    payload = json.loads(bundle["preflight"].read_text(encoding="utf-8"))
    payload["locks"]["historical_component_qc_write_allowed"] = True
    payload["candidates"].append(dict(payload["candidates"][0]))
    payload["candidate_count"] = 2
    payload["candidate_preflight_ready_count"] = 2
    payload["manual_gate_review_count"] = 2
    _write_json(bundle["preflight"], payload)
    model = _build(bundle)
    assert model["overall_status"] == BLOCKED_STATUS
    assert model["operation_plan"] == []
    assert "preflight_lock_not_false:historical_component_qc_write_allowed" in model[
        "global_blocker_codes"
    ]
    assert any(
        code.startswith("duplicate_preflight_candidate_point_dir:")
        for code in model["global_blocker_codes"]
    )


def test_malformed_candidate_or_artifact_row_is_blocker_not_exception(tmp_path: Path) -> None:
    bundle = _bundle(tmp_path)
    payload = json.loads(bundle["preflight"].read_text(encoding="utf-8"))
    payload["candidates"] = ["not-an-object"]
    payload["artifact_checks"] = ["not-an-object"]
    payload["artifact_check_count"] = 1
    _write_json(bundle["preflight"], payload)
    model = _build(bundle)
    assert model["overall_status"] == BLOCKED_STATUS
    assert model["operation_plan"] == []
    assert "preflight_candidate_not_object:row_1" in model["global_blocker_codes"]
    assert "preflight_artifact_check_not_object:row_1" in model["global_blocker_codes"]


def test_cli_writes_review_only_artifacts_and_entrypoint_is_offline(tmp_path: Path) -> None:
    bundle = _bundle(tmp_path)
    direct_dir = tmp_path / "direct" / REVIEW_SUFFIX
    cli_dir = tmp_path / "cli" / REVIEW_SUFFIX
    outputs = write_v1_5_historical_component_qc_blocked_generator_plan(
        _build(bundle), direct_dir
    )
    rc = main(
        [
            "--preflight-json-path",
            str(bundle["preflight"]),
            "--output-dir",
            str(cli_dir),
            "--fail-on-blocker",
        ]
    )
    entry = classify_v1_5_entrypoint(
        Path(
            "src/gas_calibrator/tools/"
            "export_v1_5_historical_component_qc_blocked_generator_plan.py"
        ),
        root=Path.cwd(),
    )
    assert rc == 0
    assert set(outputs) == {
        "json",
        "operation_plan_csv",
        "candidate_reviews_csv",
        "source_checks_csv",
        "markdown",
    }
    assert entry.category == "formal_review_evidence"
    assert entry.risk_level == "offline"
    assert entry.opens_com_ports is False
    assert entry.controls_routes is False
    assert entry.writes_coefficients is False
    assert not (bundle["point_dir"] / "formal_open_flow_data_quality_by_analyzer.csv").exists()


def test_cli_rejects_arbitrary_output_directory_before_writing(tmp_path: Path) -> None:
    bundle = _bundle(tmp_path)
    output_dir = tmp_path / "historical_point"
    rc = main(
        [
            "--preflight-json-path",
            str(bundle["preflight"]),
            "--output-dir",
            str(output_dir),
        ]
    )
    assert rc == 2
    assert not output_dir.exists()


def test_fail_on_blocker_returns_two_and_never_touches_existing_target(tmp_path: Path) -> None:
    bundle = _bundle(tmp_path)
    target = bundle["point_dir"] / "formal_open_flow_data_quality_by_analyzer.csv"
    target.write_text("existing\n", encoding="utf-8")
    output_dir = tmp_path / REVIEW_SUFFIX
    rc = main(
        [
            "--preflight-json-path",
            str(bundle["preflight"]),
            "--output-dir",
            str(output_dir),
            "--fail-on-blocker",
        ]
    )
    assert rc == 2
    assert (output_dir / "v1_5_historical_component_qc_blocked_generator_plan.json").is_file()
    assert target.read_text(encoding="utf-8") == "existing\n"
