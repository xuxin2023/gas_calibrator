import hashlib
import json
from pathlib import Path

import pytest

from gas_calibrator.tools.export_v1_5_historical_component_qc_controlled_writer_design import (
    main,
)
from gas_calibrator.validation.v1_5_entrypoint_inventory import classify_v1_5_entrypoint
from gas_calibrator.validation.v1_5_historical_component_qc_blocked_generator_plan import (
    build_v1_5_historical_component_qc_blocked_generator_plan,
)
from gas_calibrator.validation.v1_5_historical_component_qc_controlled_writer_design import (
    BLOCKED_STATUS,
    READY_STATUS,
    build_v1_5_historical_component_qc_controlled_writer_design,
    write_v1_5_historical_component_qc_controlled_writer_design,
)
from gas_calibrator.validation.v1_5_historical_component_qc_generator_preflight import (
    SCHEMA as PREFLIGHT_SCHEMA,
)


REVIEW_SUFFIX = Path(
    "docs/v1_5_flow_contract/historical_component_qc_controlled_writer_design"
)


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _write_json(path: Path, payload: dict) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def _preflight_locks() -> dict:
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
    artifact_checks = []
    for role, filename in (
        ("samples", "samples_machine_readable.csv"),
        ("frame_qc", "frame_quality_summary.csv"),
    ):
        path = point_dir / filename
        path.write_text("field,value\nsynthetic,1\n", encoding="utf-8")
        artifact_checks.append(
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
    hash_fields = {
        "p2_design_json": "p2_design_sha256",
        "p2_artifact_inventory_csv": "p2_artifact_inventory_sha256",
        "contract_json": "contract_file_sha256",
        "reference_evaluation_json": "reference_evaluation_sha256",
    }
    for role, hash_field in hash_fields.items():
        suffix = ".csv" if role.endswith("csv") else ".json"
        path = tmp_path / "upstream" / f"{role}{suffix}"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(f"source={role}\n", encoding="utf-8")
        source_paths[role] = str(path.resolve())
        source_paths[hash_field] = _sha(path)
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
        "artifact_check_count": len(artifact_checks),
        "artifact_check_blocked_count": 0,
        "source_paths": source_paths,
        "candidates": [candidate],
        "artifact_checks": artifact_checks,
        "locks": _preflight_locks(),
        "evidence_source": "historical_replay",
        "not_real_acceptance_evidence": True,
    }
    preflight_path = _write_json(tmp_path / "preflight.json", preflight)
    plan = build_v1_5_historical_component_qc_blocked_generator_plan(
        preflight_json_path=preflight_path
    )
    plan_path = _write_json(tmp_path / "blocked_plan.json", plan)
    return {
        "point_dir": point_dir,
        "artifact_checks": artifact_checks,
        "source_paths": source_paths,
        "preflight": preflight_path,
        "plan": plan_path,
    }


def _build(bundle: dict) -> dict:
    return build_v1_5_historical_component_qc_controlled_writer_design(
        blocked_generator_plan_json_path=bundle["plan"]
    )


def test_ready_blocked_plan_produces_design_only_contract_with_all_actions_locked(
    tmp_path: Path,
) -> None:
    bundle = _bundle(tmp_path)
    first = _build(bundle)
    second = _build(bundle)
    assert first == second
    assert first["overall_status"] == READY_STATUS
    assert first["controlled_writer_design_ready"] is True
    assert first["candidate_binding_count"] == 1
    assert first["candidate_binding_blocked_count"] == 0
    assert len(first["authorization_contract"]) >= 8
    assert len(first["atomic_write_contract"]) >= 7
    assert len(first["readback_rollback_contract"]) >= 7
    binding = first["candidate_bindings"][0]
    assert binding["binding_ready_for_design_review"] is True
    assert binding["target_create_mode"] == "exclusive_create_only_future_contract"
    assert binding["payload_derivation_supported"] is False
    assert binding["write_execution_supported"] is False
    assert first["locks"]["authorization_validator_available"] is False
    assert first["locks"]["component_qc_payload_evaluator_available"] is False
    assert first["locks"]["atomic_create_only_writer_available"] is False
    assert first["locks"]["historical_component_qc_write_allowed"] is False
    assert first["locks"]["compensating_rollback_execution_allowed"] is False
    assert first["evidence_source"] == "historical_replay"
    assert first["not_real_acceptance_evidence"] is True
    assert not (bundle["point_dir"] / "formal_open_flow_data_quality_by_analyzer.csv").exists()


def test_plan_ready_flag_or_operation_lock_tamper_is_recomputed_and_blocked(
    tmp_path: Path,
) -> None:
    bundle = _bundle(tmp_path)
    payload = json.loads(bundle["plan"].read_text(encoding="utf-8"))
    payload["locks"]["historical_component_qc_write_allowed"] = True
    payload["operation_plan"][0]["would_write"] = True
    _write_json(bundle["plan"], payload)
    model = _build(bundle)
    assert model["overall_status"] == BLOCKED_STATUS
    assert model["controlled_writer_design_ready"] is False
    assert "blocked_plan_lock_not_false:historical_component_qc_write_allowed" in model[
        "review_reasons"
    ]
    assert "blocked_plan_recompute_mismatch" in model["review_reasons"]
    assert model["candidate_bindings"][0]["binding_ready_for_design_review"] is False


def test_source_drift_after_plan_blocks_design_via_recompute(tmp_path: Path) -> None:
    bundle = _bundle(tmp_path)
    sample = Path(bundle["artifact_checks"][0]["path"])
    sample.write_text(sample.read_text(encoding="utf-8") + "drift,1\n", encoding="utf-8")
    model = _build(bundle)
    assert model["overall_status"] == BLOCKED_STATUS
    assert "blocked_plan_recompute_mismatch" in model["review_reasons"]


def test_target_created_after_plan_blocks_design_and_is_never_overwritten(tmp_path: Path) -> None:
    bundle = _bundle(tmp_path)
    target = bundle["point_dir"] / "formal_open_flow_data_quality_by_analyzer.csv"
    target.write_text("existing-do-not-touch\n", encoding="utf-8")
    before = target.read_bytes()
    model = _build(bundle)
    assert model["overall_status"] == BLOCKED_STATUS
    assert model["candidate_bindings"][0]["binding_ready_for_design_review"] is False
    assert "target_now_exists" in model["candidate_bindings"][0]["blocker_codes"]
    assert target.read_bytes() == before


def test_duplicate_target_or_missing_authorization_requirement_blocks_binding(
    tmp_path: Path,
) -> None:
    bundle = _bundle(tmp_path)
    payload = json.loads(bundle["plan"].read_text(encoding="utf-8"))
    duplicate = dict(payload["operation_plan"][0])
    duplicate["requires_distinct_authorization"] = False
    payload["operation_plan"].append(duplicate)
    payload["candidate_count"] = 2
    payload["candidate_plan_ready_count"] = 2
    _write_json(bundle["plan"], payload)
    model = _build(bundle)
    assert model["overall_status"] == BLOCKED_STATUS
    assert model["candidate_binding_blocked_count"] >= 1
    codes = model["candidate_bindings"][1]["blocker_codes"]
    assert "duplicate_point_dir" in codes
    assert "duplicate_target_path" in codes
    assert "distinct_authorization_requirement_missing" in codes


def test_cli_writes_design_review_only_and_entrypoint_is_offline(tmp_path: Path) -> None:
    bundle = _bundle(tmp_path)
    direct_dir = tmp_path / "direct" / REVIEW_SUFFIX
    cli_dir = tmp_path / "cli" / REVIEW_SUFFIX
    outputs = write_v1_5_historical_component_qc_controlled_writer_design(
        _build(bundle), direct_dir
    )
    rc = main(
        [
            "--blocked-generator-plan-json-path",
            str(bundle["plan"]),
            "--output-dir",
            str(cli_dir),
            "--fail-on-blocker",
        ]
    )
    entry = classify_v1_5_entrypoint(
        Path(
            "src/gas_calibrator/tools/"
            "export_v1_5_historical_component_qc_controlled_writer_design.py"
        ),
        root=Path.cwd(),
    )
    assert rc == 0
    assert set(outputs) == {
        "json",
        "candidate_bindings_csv",
        "authorization_contract_csv",
        "atomic_write_contract_csv",
        "readback_rollback_contract_csv",
        "markdown",
    }
    assert entry.category == "formal_review_evidence"
    assert entry.risk_level == "offline"
    assert entry.opens_com_ports is False
    assert entry.controls_routes is False
    assert entry.writes_coefficients is False
    assert not (bundle["point_dir"] / "formal_open_flow_data_quality_by_analyzer.csv").exists()


def test_cli_rejects_arbitrary_output_and_execute_flags(tmp_path: Path) -> None:
    bundle = _bundle(tmp_path)
    output_dir = tmp_path / "historical_point"
    rc = main(
        [
            "--blocked-generator-plan-json-path",
            str(bundle["plan"]),
            "--output-dir",
            str(output_dir),
        ]
    )
    assert rc == 2
    assert not output_dir.exists()

    review_output = tmp_path / REVIEW_SUFFIX
    with pytest.raises(SystemExit) as exc:
        main(
            [
                "--blocked-generator-plan-json-path",
                str(bundle["plan"]),
                "--output-dir",
                str(review_output),
                "--execute",
            ]
        )
    assert exc.value.code == 2
    assert not review_output.exists()


def test_fail_on_blocker_writes_only_review_evidence(tmp_path: Path) -> None:
    bundle = _bundle(tmp_path)
    target = bundle["point_dir"] / "formal_open_flow_data_quality_by_analyzer.csv"
    target.write_text("existing\n", encoding="utf-8")
    output_dir = tmp_path / REVIEW_SUFFIX
    rc = main(
        [
            "--blocked-generator-plan-json-path",
            str(bundle["plan"]),
            "--output-dir",
            str(output_dir),
            "--fail-on-blocker",
        ]
    )
    assert rc == 2
    assert (output_dir / "v1_5_historical_component_qc_controlled_writer_design.json").is_file()
    assert target.read_text(encoding="utf-8") == "existing\n"
