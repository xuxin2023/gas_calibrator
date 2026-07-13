import csv
import hashlib
import json
from pathlib import Path

from gas_calibrator.tools.export_v1_5_historical_component_qc_generator_preflight import main
from gas_calibrator.validation.v1_5_entrypoint_inventory import classify_v1_5_entrypoint
from gas_calibrator.validation.v1_5_historical_component_qc_generator_preflight import (
    build_v1_5_historical_component_qc_generator_preflight,
    write_v1_5_historical_component_qc_generator_preflight,
)
from gas_calibrator.validation.v1_5_p2_qc_derivation_design import SCHEMA as P2_SCHEMA


CONTRACT_PATH = Path("configs/v1_5_component_qc_generator_contract.json")
REFERENCE_PATH = Path(
    "docs/v1_5_flow_contract/component_qc_reference_evaluator/"
    "v1_5_component_qc_reference_evaluation.json"
)
REVIEW_SUFFIX = Path(
    "docs/v1_5_flow_contract/historical_component_qc_generator_preflight"
)


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _json(path: Path, payload: dict) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def _csv(path: Path, rows: list[dict]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    return path


def _p2_locks() -> dict:
    return {
        "opens_com_ports": False,
        "controls_pressure": False,
        "controls_water_or_gas_routes": False,
        "writes_coefficients": False,
        "writes_sn_or_device_code": False,
        "connects_postgresql": False,
        "reviewed_generator_available": False,
        "qc_derivation_execution_allowed": False,
        "generated_qc_write_allowed": False,
        "cross_run_qc_direct_bind_allowed": False,
        "historical_fit_allowed": False,
        "formal_release_allowed": False,
        "database_import_allowed": False,
        "not_real_acceptance_evidence": True,
    }


def _bundle(tmp_path: Path, route_kind: str = "co2") -> dict:
    point_name = "p001_T40_0ppm_fit" if route_kind == "co2" else "p001_T0_HG0C_50RH_h2o"
    point_dir = tmp_path / "historical_replay" / point_name
    role_names = {
        "samples": "samples_machine_readable.csv",
        "frame_qc": "frame_quality_summary.csv",
        "runtime_config": "runtime_config_snapshot.json",
        "sidecar": (
            "formal_open_flow_sidecar_metadata.json"
            if route_kind == "co2"
            else "formal_h2o_open_flow_sidecar_metadata.json"
        ),
    }
    if route_kind == "co2":
        role_names["route_timing"] = "formal_open_flow_route_timing.json"
    else:
        role_names.update(
            {
                "hgen_flow_set": "formal_h2o_open_flow_hgen_flow_set.json",
                "humidity_reference_review": "h2o_humidity_reference_review.json",
                "point_timing_summary": "point_timing_summary.csv",
            }
        )
    artifacts: list[dict] = []
    for role, filename in role_names.items():
        path = point_dir / filename
        path.parent.mkdir(parents=True, exist_ok=True)
        if path.suffix == ".csv":
            path.write_text("field,value\nsynthetic,1\n", encoding="utf-8")
        else:
            path.write_text(json.dumps({"synthetic": True, "role": role}), encoding="utf-8")
        artifacts.append(
            {
                "point_dir": str(point_dir.resolve()),
                "artifact_role": role,
                "artifact_path": str(path.resolve()),
                "size_bytes": path.stat().st_size,
                "sha256": _sha(path),
            }
        )
    candidate = {
        "source_role": "p2_catalog_point",
        "route_kind": route_kind,
        "point_name": point_name,
        "point_dir": str(point_dir.resolve()),
        "input_complete": True,
        "derivation_design_review_candidate": True,
        "manual_gate_review_required": True,
        "sample_alignment_false_count": 3,
        "point_quality_blocked_count": 0,
        "purge_below_declared_minimum": route_kind == "h2o",
        "qc_derivation_execution_allowed": False,
        "generated_qc_write_allowed": False,
        "formal_fit_allowed": False,
    }
    p2 = {
        "schema": P2_SCHEMA,
        "overall_status": "blocked_missing_reviewed_qc_generator_contract",
        "candidate_count": 1,
        "candidates": [candidate],
        "artifact_inventory_csv": "inventory.csv",
        **_p2_locks(),
    }
    return {
        "point_dir": point_dir,
        "candidate": candidate,
        "artifacts": artifacts,
        "p2": _json(tmp_path / "p2.json", p2),
        "inventory": _csv(tmp_path / "inventory.csv", artifacts),
    }


def _build(bundle: dict) -> dict:
    return build_v1_5_historical_component_qc_generator_preflight(
        p2_design_json_path=bundle["p2"],
        p2_artifact_inventory_csv_path=bundle["inventory"],
        contract_json_path=CONTRACT_PATH,
        reference_evaluation_json_path=REFERENCE_PATH,
    )


def test_complete_source_packet_is_preflight_ready_but_all_execution_stays_locked(
    tmp_path: Path,
) -> None:
    model = _build(_bundle(tmp_path))
    assert model["overall_status"] == (
        "ready_for_historical_component_qc_generator_preflight_manual_review"
    )
    assert model["candidate_preflight_ready_count"] == 1
    assert model["candidate_blocked_count"] == 0
    assert model["manual_gate_review_count"] == 1
    assert model["artifact_check_blocked_count"] == 0
    assert model["candidates"][0]["preflight_ready"] is True
    assert model["candidates"][0]["planned_output_exists"] is False
    assert model["locks"]["preflight_available"] is True
    assert model["locks"]["production_component_qc_generator_available"] is False
    assert model["locks"]["historical_component_qc_generation_allowed"] is False
    assert model["locks"]["historical_component_qc_write_allowed"] is False
    assert model["locks"]["historical_fit_allowed"] is False
    assert model["locks"]["opens_com_ports"] is False
    assert model["evidence_source"] == "historical_replay"
    assert model["not_real_acceptance_evidence"] is True


def test_h2o_source_packet_uses_route_specific_roles_without_promoting_fit(tmp_path: Path) -> None:
    model = _build(_bundle(tmp_path, "h2o"))
    roles = {row["role"] for row in model["artifact_checks"]}
    assert {
        "samples",
        "frame_qc",
        "runtime_config",
        "sidecar",
        "hgen_flow_set",
        "humidity_reference_review",
        "point_timing_summary",
    } == roles
    assert model["candidate_preflight_ready_count"] == 1
    assert model["candidates"][0]["purge_below_declared_minimum"] is True
    assert model["candidates"][0]["formal_fit_allowed"] is False


def test_source_hash_mismatch_blocks_candidate(tmp_path: Path) -> None:
    bundle = _bundle(tmp_path)
    samples = bundle["point_dir"] / "samples_machine_readable.csv"
    samples.write_text(samples.read_text(encoding="utf-8") + "tampered,1\n", encoding="utf-8")
    model = _build(bundle)
    assert model["candidate_preflight_ready_count"] == 0
    assert model["candidate_blocked_count"] == 1
    assert "samples:artifact_size_mismatch" in model["candidates"][0]["blocker_codes"]
    assert "samples:artifact_sha256_mismatch" in model["candidates"][0]["blocker_codes"]


def test_missing_role_and_artifact_outside_point_dir_are_blocked(tmp_path: Path) -> None:
    bundle = _bundle(tmp_path)
    rows = bundle["artifacts"]
    rows[:] = [row for row in rows if row["artifact_role"] != "route_timing"]
    outside = tmp_path / "outside.json"
    outside.write_text("{}", encoding="utf-8")
    next(row for row in rows if row["artifact_role"] == "sidecar").update(
        {
            "artifact_path": str(outside.resolve()),
            "size_bytes": outside.stat().st_size,
            "sha256": _sha(outside),
        }
    )
    _csv(bundle["inventory"], rows)
    model = _build(bundle)
    codes = model["candidates"][0]["blocker_codes"]
    assert "artifact_role_missing:route_timing" in codes
    assert "sidecar:artifact_path_outside_point_dir" in codes


def test_existing_component_qc_target_blocks_overwrite_without_changing_file(tmp_path: Path) -> None:
    bundle = _bundle(tmp_path)
    target = bundle["point_dir"] / "formal_open_flow_data_quality_by_analyzer.csv"
    target.write_text("do-not-overwrite\n", encoding="utf-8")
    before = target.read_bytes()
    model = _build(bundle)
    assert "component_qc_output_target_already_exists" in model["candidates"][0]["blocker_codes"]
    assert target.read_bytes() == before
    assert model["locks"]["historical_component_qc_write_allowed"] is False


def test_upstream_or_reference_lock_tamper_blocks_entire_preflight(tmp_path: Path) -> None:
    bundle = _bundle(tmp_path)
    p2 = json.loads(bundle["p2"].read_text(encoding="utf-8"))
    p2["generated_qc_write_allowed"] = True
    _json(bundle["p2"], p2)
    reference = json.loads(REFERENCE_PATH.read_text(encoding="utf-8"))
    reference["locks"]["historical_component_qc_write_allowed"] = True
    reference_path = _json(tmp_path / "reference.json", reference)
    model = build_v1_5_historical_component_qc_generator_preflight(
        p2_design_json_path=bundle["p2"],
        p2_artifact_inventory_csv_path=bundle["inventory"],
        contract_json_path=CONTRACT_PATH,
        reference_evaluation_json_path=reference_path,
    )
    assert model["overall_status"] == "blocked_historical_component_qc_generator_preflight"
    assert "p2_lock_not_false:generated_qc_write_allowed" in model["global_blocker_codes"]
    assert "reference_lock_not_false:historical_component_qc_write_allowed" in model[
        "global_blocker_codes"
    ]


def test_inventory_must_be_the_file_declared_by_p2_design(tmp_path: Path) -> None:
    bundle = _bundle(tmp_path)
    replacement = _csv(tmp_path / "replacement.csv", bundle["artifacts"])
    model = build_v1_5_historical_component_qc_generator_preflight(
        p2_design_json_path=bundle["p2"],
        p2_artifact_inventory_csv_path=replacement,
        contract_json_path=CONTRACT_PATH,
        reference_evaluation_json_path=REFERENCE_PATH,
    )
    assert model["overall_status"] == "blocked_historical_component_qc_generator_preflight"
    assert "p2_artifact_inventory_path_mismatch" in model["global_blocker_codes"]


def test_duplicate_p2_candidate_point_directory_blocks_preflight(tmp_path: Path) -> None:
    bundle = _bundle(tmp_path)
    p2 = json.loads(bundle["p2"].read_text(encoding="utf-8"))
    p2["candidates"].append(dict(p2["candidates"][0]))
    p2["candidate_count"] = 2
    _json(bundle["p2"], p2)
    model = _build(bundle)
    assert model["overall_status"] == "blocked_historical_component_qc_generator_preflight"
    assert any(
        code.startswith("duplicate_p2_candidate_point_dir:")
        for code in model["global_blocker_codes"]
    )


def test_preflight_model_is_deterministic_for_unchanged_sources(tmp_path: Path) -> None:
    bundle = _bundle(tmp_path)
    assert _build(bundle) == _build(bundle)


def test_cli_and_entrypoint_are_offline_and_output_is_review_only(tmp_path: Path) -> None:
    bundle = _bundle(tmp_path)
    direct_dir = tmp_path / "direct" / REVIEW_SUFFIX
    cli_dir = tmp_path / "cli" / REVIEW_SUFFIX
    direct = write_v1_5_historical_component_qc_generator_preflight(
        _build(bundle), direct_dir
    )
    rc = main(
        [
            "--p2-design-json-path",
            str(bundle["p2"]),
            "--p2-artifact-inventory-csv-path",
            str(bundle["inventory"]),
            "--contract-json-path",
            str(CONTRACT_PATH),
            "--reference-evaluation-json-path",
            str(REFERENCE_PATH),
            "--output-dir",
            str(cli_dir),
            "--fail-on-blocker",
        ]
    )
    entry = classify_v1_5_entrypoint(
        Path(
            "src/gas_calibrator/tools/"
            "export_v1_5_historical_component_qc_generator_preflight.py"
        ),
        root=Path.cwd(),
    )
    assert rc == 0
    assert set(direct) == {"json", "candidates_csv", "artifact_checks_csv", "markdown"}
    assert entry.category == "formal_review_evidence"
    assert entry.risk_level == "offline"
    assert entry.opens_com_ports is False
    assert entry.controls_routes is False
    assert entry.writes_coefficients is False


def test_cli_rejects_arbitrary_output_directory_before_writing(tmp_path: Path) -> None:
    bundle = _bundle(tmp_path)
    output_dir = tmp_path / "historical_point"
    rc = main(
        [
            "--p2-design-json-path",
            str(bundle["p2"]),
            "--p2-artifact-inventory-csv-path",
            str(bundle["inventory"]),
            "--contract-json-path",
            str(CONTRACT_PATH),
            "--reference-evaluation-json-path",
            str(REFERENCE_PATH),
            "--output-dir",
            str(output_dir),
        ]
    )
    assert rc == 2
    assert not output_dir.exists()


def test_fail_on_blocker_returns_two_but_writes_only_review_evidence(tmp_path: Path) -> None:
    bundle = _bundle(tmp_path)
    (bundle["point_dir"] / "formal_open_flow_data_quality_by_analyzer.csv").write_text(
        "existing\n", encoding="utf-8"
    )
    output_dir = tmp_path / REVIEW_SUFFIX
    rc = main(
        [
            "--p2-design-json-path",
            str(bundle["p2"]),
            "--p2-artifact-inventory-csv-path",
            str(bundle["inventory"]),
            "--contract-json-path",
            str(CONTRACT_PATH),
            "--reference-evaluation-json-path",
            str(REFERENCE_PATH),
            "--output-dir",
            str(output_dir),
            "--fail-on-blocker",
        ]
    )
    assert rc == 2
    assert (output_dir / "v1_5_historical_component_qc_generator_preflight.json").is_file()
    assert (bundle["point_dir"] / "formal_open_flow_data_quality_by_analyzer.csv").read_text(
        encoding="utf-8"
    ) == "existing\n"
