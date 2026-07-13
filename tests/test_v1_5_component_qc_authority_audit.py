import csv
import hashlib
import json
import subprocess
from pathlib import Path

from gas_calibrator.tools.export_v1_5_component_qc_authority_audit import main
from gas_calibrator.validation.v1_5_component_qc_authority_audit import (
    build_v1_5_component_qc_authority_audit,
    write_v1_5_component_qc_authority_audit,
)
from gas_calibrator.validation.v1_5_entrypoint_inventory import classify_v1_5_entrypoint
from gas_calibrator.validation.v1_5_legacy_historical_evidence_catalog import (
    SCHEMA as CATALOG_SCHEMA,
)
from gas_calibrator.validation.v1_5_p2_qc_derivation_design import SCHEMA as P2_SCHEMA


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


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _run(*args: str, cwd: Path) -> None:
    subprocess.run(args, cwd=cwd, check=True, capture_output=True)


def _git_repo(path: Path) -> Path:
    path.mkdir(parents=True)
    _run("git", "init", cwd=path)
    _run("git", "config", "user.email", "test@example.com", cwd=path)
    _run("git", "config", "user.name", "Test", cwd=path)
    co2 = path / "src/gas_calibrator/tools/run_v1_5_formal_open_flow_sampling.py"
    h2o = path / "src/gas_calibrator/tools/run_v1_5_formal_h2o_open_flow_sampling.py"
    co2.parent.mkdir(parents=True)
    co2.write_text(
        "co2_ratio_f_preseal_tol co2_ratio_f_preseal_a_grade_tol "
        "co2_ratio_f_preseal_min_samples\n",
        encoding="utf-8",
    )
    h2o.write_text(
        "h2o_ratio_f_preseal_tol h2o_ratio_f_preseal_a_grade_tol "
        "h2o_ratio_f_preseal_min_samples\n",
        encoding="utf-8",
    )
    _run("git", "add", ".", cwd=path)
    _run("git", "commit", "-m", "mature samplers", cwd=path)
    return path


def _polluted_repo(path: Path) -> Path:
    path.mkdir(parents=True)
    _run("git", "init", cwd=path)
    _run("git", "config", "user.email", "test@example.com", cwd=path)
    _run("git", "config", "user.name", "Test", cwd=path)
    marker = path / "README.md"
    marker.write_text("polluted root\n", encoding="utf-8")
    _run("git", "add", "README.md", cwd=path)
    _run("git", "commit", "-m", "root marker", cwd=path)
    writer = path / "src/gas_calibrator/validation/v1_5_open_flow_quality.py"
    writer.parent.mkdir(parents=True)
    writer.write_text(
        "A_calibration_eligible B_diagnostic_model_only C_reject ratio_tol ratio_a_tol "
        "formal_open_flow_data_quality_by_analyzer.csv\n",
        encoding="utf-8",
    )
    for name in (
        "run_v1_5_formal_open_flow_sampling.py",
        "run_v1_5_formal_h2o_open_flow_sampling.py",
    ):
        sampler = path / "src/gas_calibrator/tools" / name
        sampler.parent.mkdir(parents=True, exist_ok=True)
        sampler.write_text("apply_open_flow_quality_grades\n", encoding="utf-8")
    return path


def _upstream(tmp_path: Path) -> tuple[Path, Path]:
    point = tmp_path / "point"
    _json(
        point / "runtime_config_snapshot.json",
        {
            "workflow": {
                "stability": {
                    "sensor": {
                        "co2_ratio_f_preseal_tol": 0.001,
                        "co2_ratio_f_preseal_a_grade_tol": 0.0005,
                        "co2_ratio_f_preseal_policy": "reject",
                        "co2_ratio_f_preseal_min_samples": 10,
                    }
                }
            }
        },
    )
    p2 = {
        "schema": P2_SCHEMA,
        "overall_status": "blocked_missing_reviewed_qc_generator_contract",
        "candidate_count": 1,
        "manual_gate_review_count": 1,
        "candidates": [{"route_kind": "co2", "point_dir": str(point)}],
        "opens_com_ports": False,
        "controls_pressure": False,
        "controls_water_or_gas_routes": False,
        "writes_coefficients": False,
        "writes_sn_or_device_code": False,
        "connects_postgresql": False,
        "qc_derivation_execution_allowed": False,
        "generated_qc_write_allowed": False,
        "historical_fit_allowed": False,
        "formal_release_allowed": False,
        "database_import_allowed": False,
        "not_real_acceptance_evidence": True,
    }
    quality = _csv(
        tmp_path / "0624" / "formal_open_flow_data_quality_by_analyzer.csv",
        [
            {
                "label": "ga01",
                "prefix": "ga01",
                "grade": "A_calibration_eligible",
                "ratio_key": "ga01_co2_ratio_f",
                "ratio_span": "0.0004",
                "ratio_tol": "0.001",
                "ratio_a_tol": "0.0005",
                "frame_count": "10",
                "usable_ratio_count": "10",
                "reason": "",
                "sample_can_enter_calibration_fit": "True",
                "sample_can_enter_diagnostic_model": "True",
            }
        ],
    )
    catalog = {
        "schema": CATALOG_SCHEMA,
        "overall_status": "catalog_complete_diagnostic_only",
        "points": [
            {
                "point_name": "p001_T40_0ppm_fit",
                "route_kind": "co2",
                "root_classification": "forbidden_0624_or_migration",
                "lineage_classification": "forbidden_0624_or_migration",
                "has_component_qc": True,
                "artifacts": {
                    "formal_open_flow_data_quality_by_analyzer.csv": {
                        "path": str(quality),
                        "sha256": _sha256(quality),
                    }
                },
            }
        ],
        "opens_com_ports": False,
        "controls_pressure": False,
        "controls_water_or_gas_routes": False,
        "writes_coefficients": False,
        "writes_sn_or_device_code": False,
        "connects_postgresql": False,
        "historical_fit_allowed": False,
        "formal_release_allowed": False,
        "database_import_allowed": False,
        "not_real_acceptance_evidence": True,
    }
    return _json(tmp_path / "p2.json", p2), _json(tmp_path / "catalog.json", catalog)


def test_authority_audit_separates_mature_preseal_from_untracked_0624_writer(
    tmp_path: Path,
) -> None:
    repo = _git_repo(tmp_path / "repo")
    polluted = _polluted_repo(tmp_path / "polluted")
    p2, catalog = _upstream(tmp_path)
    model = build_v1_5_component_qc_authority_audit(
        repo_root=repo,
        polluted_root=polluted,
        p2_design_json_path=p2,
        legacy_catalog_json_path=catalog,
    )
    assert model["overall_status"] == "blocked_no_reviewed_mature_component_qc_authority"
    assert model["tracked_component_qc_writer_present"] is False
    assert model["tracked_component_qc_writer_history_commits"] == []
    assert model["polluted_writer_review"]["exists"] is True
    assert model["polluted_writer_review"]["tracked_in_polluted_root"] is False
    assert model["historical_component_qc_artifact_count"] == 1
    assert model["historical_component_qc_route_counts"] == {"co2": 1}
    assert "h2o_historical_component_qc_examples_missing" in model["authority_gap_codes"]
    assert all(row["is_component_qc_writer_authority"] is False for row in model["mature_sampler_reviews"])
    assert model["component_qc_generation_allowed"] is False
    assert model["component_qc_backfill_allowed"] is False


def test_runtime_thresholds_are_recorded_as_preseal_not_component_qc_authority(
    tmp_path: Path,
) -> None:
    repo = _git_repo(tmp_path / "repo")
    polluted = _polluted_repo(tmp_path / "polluted")
    p2, catalog = _upstream(tmp_path)
    model = build_v1_5_component_qc_authority_audit(
        repo_root=repo,
        polluted_root=polluted,
        p2_design_json_path=p2,
        legacy_catalog_json_path=catalog,
    )
    row = model["runtime_preseal_threshold_inventory"][0]
    assert row["preseal_hard_tol"] == 0.001
    assert row["preseal_a_grade_tol"] == 0.0005
    assert row["preseal_min_samples"] == 10
    assert row["authority_role"].endswith("not_post_sample_component_qc")
    assert model["interpretation_contract"][
        "preseal_stability_thresholds_are_component_qc_threshold_authority"
    ] is False


def test_tracked_writer_still_requires_separate_review_and_does_not_unlock_generation(
    tmp_path: Path,
) -> None:
    repo = _git_repo(tmp_path / "repo")
    writer = repo / "src/gas_calibrator/validation/v1_5_open_flow_quality.py"
    writer.parent.mkdir(parents=True)
    writer.write_text("review candidate\n", encoding="utf-8")
    _run("git", "add", ".", cwd=repo)
    _run("git", "commit", "-m", "candidate writer", cwd=repo)
    polluted = _polluted_repo(tmp_path / "polluted")
    p2, catalog = _upstream(tmp_path)
    model = build_v1_5_component_qc_authority_audit(
        repo_root=repo,
        polluted_root=polluted,
        p2_design_json_path=p2,
        legacy_catalog_json_path=catalog,
    )
    assert model["tracked_component_qc_writer_present"] is True
    assert len(model["tracked_component_qc_writer_history_commits"]) == 1
    assert "tracked_component_qc_writer_requires_separate_review" in model["authority_gap_codes"]
    assert model["component_qc_generation_allowed"] is False


def test_invalid_upstream_lock_fails_closed(tmp_path: Path) -> None:
    repo = _git_repo(tmp_path / "repo")
    polluted = _polluted_repo(tmp_path / "polluted")
    p2, catalog = _upstream(tmp_path)
    payload = json.loads(p2.read_text(encoding="utf-8"))
    payload["generated_qc_write_allowed"] = True
    p2.write_text(json.dumps(payload), encoding="utf-8")
    model = build_v1_5_component_qc_authority_audit(
        repo_root=repo,
        polluted_root=polluted,
        p2_design_json_path=p2,
        legacy_catalog_json_path=catalog,
    )
    assert model["overall_status"] == "blocked_invalid_upstream_evidence"
    assert "p2_generated_qc_write_not_locked" in model["upstream_blocker_codes"]
    assert model["component_qc_generation_allowed"] is False


def test_writer_cli_and_entrypoint_are_offline(tmp_path: Path) -> None:
    repo = _git_repo(tmp_path / "repo")
    polluted = _polluted_repo(tmp_path / "polluted")
    p2, catalog = _upstream(tmp_path)
    model = build_v1_5_component_qc_authority_audit(
        repo_root=repo,
        polluted_root=polluted,
        p2_design_json_path=p2,
        legacy_catalog_json_path=catalog,
    )
    outputs = write_v1_5_component_qc_authority_audit(model, tmp_path / "direct")
    rc = main(
        [
            "--repo-root",
            str(repo),
            "--polluted-root",
            str(polluted),
            "--p2-design-json-path",
            str(p2),
            "--legacy-catalog-json-path",
            str(catalog),
            "--output-dir",
            str(tmp_path / "cli"),
        ]
    )
    entry = classify_v1_5_entrypoint(
        Path("src/gas_calibrator/tools/export_v1_5_component_qc_authority_audit.py"),
        root=Path.cwd(),
    )
    assert rc == 0
    assert outputs["sources_csv"].is_file()
    assert outputs["artifacts_csv"].is_file()
    assert entry.category == "formal_review_evidence"
    assert entry.risk_level == "offline"
    assert entry.opens_com_ports is False
    assert entry.controls_routes is False
    assert entry.writes_coefficients is False
