import json
from pathlib import Path

from gas_calibrator.v2.core.artifact_compatibility import (
    ARTIFACT_COMPATIBILITY_INDEX_SCHEMA_VERSION,
    ARTIFACT_CONTRACT_CATALOG_FILENAME,
    COMPATIBILITY_SCAN_SUMMARY_FILENAME,
    REINDEX_MANIFEST_FILENAME,
    RUN_ARTIFACT_INDEX_FILENAME,
    regenerate_artifact_compatibility_sidecars,
)
from gas_calibrator.v2.core.multi_source_stability import MULTI_SOURCE_STABILITY_EVIDENCE_FILENAME
from gas_calibrator.v2.scripts import historical_artifacts


def _write_run(run_dir: Path, *, run_id: str, canonical_surface: bool = False) -> dict[str, str]:
    run_dir.mkdir(parents=True, exist_ok=True)
    summary_payload = {
        "run_id": run_id,
        "stats": {
            "sample_count": 0,
            "point_summaries": [],
        },
    }
    manifest_payload = {
        "run_id": run_id,
        "artifacts": {
            "role_catalog": {
                "execution_summary": ["manifest", "run_summary"],
                "execution_rows": ["results_json"],
                "diagnostic_analysis": ["multi_source_stability_evidence"],
            }
        },
    }
    results_payload = {
        "run_id": run_id,
        "samples": [],
        "point_summaries": [],
    }
    summary_text = json.dumps(summary_payload, ensure_ascii=False, indent=2) + "\n"
    manifest_text = json.dumps(manifest_payload, ensure_ascii=False, indent=2) + "\n"
    results_text = json.dumps(results_payload, ensure_ascii=False, indent=2) + "\n"
    (run_dir / "summary.json").write_text(summary_text, encoding="utf-8")
    (run_dir / "manifest.json").write_text(manifest_text, encoding="utf-8")
    (run_dir / "results.json").write_text(results_text, encoding="utf-8")
    if canonical_surface:
        (run_dir / MULTI_SOURCE_STABILITY_EVIDENCE_FILENAME).write_text(
            json.dumps(
                {
                    "artifact_type": "multi_source_stability_evidence",
                    "schema_version": "measurement-core-rich-trace-v1",
                    "summary": "canonical surface ready",
                },
                ensure_ascii=False,
                indent=2,
            )
            + "\n",
            encoding="utf-8",
        )
    return {
        "summary": summary_text,
        "manifest": manifest_text,
        "results": results_text,
    }


def _parse_last_json(stdout: str) -> dict:
    json_lines = [line.strip() for line in stdout.splitlines() if line.strip().startswith("{")]
    assert json_lines
    return json.loads(json_lines[-1])


def test_historical_scan_supports_single_run_dir_and_root_dir(tmp_path: Path, capsys) -> None:
    root_dir = tmp_path / "historical"
    legacy_run = root_dir / "legacy_run"
    canonical_run = root_dir / "canonical_run"
    _write_run(legacy_run, run_id="legacy-run")
    _write_run(canonical_run, run_id="canonical-run", canonical_surface=True)
    regenerate_artifact_compatibility_sidecars(canonical_run)

    assert historical_artifacts.main(["scan", "--run-dir", str(legacy_run)]) == 0
    single_report = _parse_last_json(capsys.readouterr().out)

    assert single_report["run_count"] == 1
    assert single_report["index_schema_version"] == ARTIFACT_COMPATIBILITY_INDEX_SCHEMA_VERSION
    assert single_report["rollup_scope"] == "run-dir"
    assert single_report["compatibility_rollup"]["parent_run_count"] == 1
    assert single_report["compatibility_rollup"]["artifact_count"] >= 3
    assert single_report["primary_evidence_rewritten"] is False
    assert single_report["runs"][0]["current_reader_mode"] == "compatibility_adapter"
    assert single_report["runs"][0]["regenerate_recommended"] is True
    assert single_report["runs"][0]["primary_evidence_rewritten"] is False
    assert single_report["runs"][0]["index_schema_version"] == ARTIFACT_COMPATIBILITY_INDEX_SCHEMA_VERSION
    assert single_report["runs"][0]["compatibility_rollup"]["rollup_scope"] == "run-dir"
    assert single_report["runs"][0]["compatibility_rollup"]["primary_evidence_rewritten"] is False
    assert single_report["runs"][0]["scope_overview"]
    assert single_report["runs"][0]["decision_rule_overview"]
    assert single_report["runs"][0]["conformity_boundary"]
    assert single_report["runs"][0]["asset_readiness_overview"]
    assert single_report["runs"][0]["certificate_lifecycle_overview"]
    assert single_report["runs"][0]["pre_run_gate_status"] in {
        "--",
        "ok_for_reviewer_mapping",
        "warning_reviewer_attention",
        "blocked_for_formal_claim",
    }
    assert single_report["runs"][0]["pre_run_gate_summary"]
    assert single_report["runs"][0]["blocking_digest"]
    assert single_report["runs"][0]["warning_digest"]
    assert single_report["runs"][0]["reviewer_only_boundary"]
    assert single_report["runs"][0]["ready_for_readiness_mapping"] is True
    assert single_report["runs"][0]["not_ready_for_formal_claim"] is True
    assert single_report["runs"][0]["uncertainty_overview"]
    assert single_report["runs"][0]["uncertainty_budget_completeness"]
    assert single_report["runs"][0]["uncertainty_top_contributors"]
    assert single_report["runs"][0]["uncertainty_data_completeness"]
    assert single_report["runs"][0]["uncertainty_readiness_status"] == "ready_for_readiness_mapping"
    assert single_report["runs"][0]["uncertainty_non_claim_note"]
    assert single_report["runs"][0]["uncertainty_ready_for_readiness_mapping"] is True
    assert single_report["runs"][0]["uncertainty_not_ready_for_formal_claim"] is True
    assert single_report["runs"][0]["uncertainty_not_real_acceptance_evidence"] is True
    assert single_report["runs"][0]["uncertainty_primary_evidence_rewritten"] is False
    assert single_report["runs"][0]["not_real_acceptance_evidence"] is True
    assert single_report["runs"][0]["recognition_scope_rollup"]["repository_mode"] == "file_artifact_first"

    assert historical_artifacts.main(["scan", "--root-dir", str(root_dir)]) == 0
    batch_stdout = capsys.readouterr().out
    batch_report = _parse_last_json(batch_stdout)

    assert "[Step2 safety]" in batch_stdout
    assert "real-COM 0" in batch_stdout
    assert batch_report["run_count"] == 2
    assert batch_report["target_mode"] == "batch"
    assert batch_report["rollup_scope"] == "root-dir"
    assert batch_report["compatibility_rollup"]["parent_run_count"] == 2
    assert batch_report["compatibility_rollup"]["compatible_run_count"] == 1
    assert batch_report["compatibility_rollup"]["legacy_run_count"] == 1
    assert batch_report["compatibility_rollup"]["regenerate_recommended_count"] == 2
    assert batch_report["recognition_scope_rollup"]["rollup_scope"] == "root-dir"
    assert batch_report["recognition_scope_rollup"]["readiness_status_counts"]
    assert batch_report["recognition_scope_rollup"]["pre_run_gate_status_counts"]
    assert batch_report["pre_run_gate_status_counts"]
    assert batch_report["ready_for_readiness_mapping_count"] == 2
    reader_modes = {row["run_dir"]: row["current_reader_mode"] for row in batch_report["runs"]}
    assert reader_modes[str(legacy_run.resolve())] == "compatibility_adapter"
    assert reader_modes[str(canonical_run.resolve())] == "canonical_direct"


def test_historical_export_summary_writes_json_report(tmp_path: Path, capsys) -> None:
    run_dir = tmp_path / "export_run"
    _write_run(run_dir, run_id="export-run")
    output_path = tmp_path / "compatibility-summary.json"

    assert historical_artifacts.main(
        ["export-summary", "--run-dir", str(run_dir), "--output", str(output_path)]
    ) == 0
    stdout = capsys.readouterr().out
    payload = json.loads(output_path.read_text(encoding="utf-8"))

    assert "[historical-artifacts] summary_path=" in stdout
    assert payload["run_count"] == 1
    assert payload["index_schema_version"] == ARTIFACT_COMPATIBILITY_INDEX_SCHEMA_VERSION
    assert payload["compatibility_rollup"]["rollup_scope"] == "run-dir"
    assert payload["runs"][0]["schema_contract_summary"]
    assert payload["runs"][0]["observed_contract_version_summary"]
    assert payload["runs"][0]["primary_evidence_rewritten"] is False
    assert payload["runs"][0]["recognition_scope_rollup"]["index_schema_version"]
    assert payload["runs"][0]["scope_non_claim_note"]
    assert payload["runs"][0]["asset_readiness_overview"]
    assert payload["runs"][0]["pre_run_gate_status"]
    assert payload["runs"][0]["uncertainty_overview"]
    assert payload["runs"][0]["uncertainty_rollup"]["repository_mode"] == "file_artifact_first"


def test_historical_regenerate_dry_run_does_not_write_sidecars_or_rewrite_primary_evidence(
    tmp_path: Path,
    capsys,
) -> None:
    run_dir = tmp_path / "dry_run"
    primary_text = _write_run(run_dir, run_id="dry-run")

    assert historical_artifacts.main(["regenerate", "--run-dir", str(run_dir), "--dry-run"]) == 0
    report = _parse_last_json(capsys.readouterr().out)

    assert report["dry_run"] is True
    assert report["primary_evidence_rewritten"] is False
    assert not (run_dir / RUN_ARTIFACT_INDEX_FILENAME).exists()
    assert not (run_dir / ARTIFACT_CONTRACT_CATALOG_FILENAME).exists()
    assert not (run_dir / COMPATIBILITY_SCAN_SUMMARY_FILENAME).exists()
    assert not (run_dir / REINDEX_MANIFEST_FILENAME).exists()
    assert (run_dir / "summary.json").read_text(encoding="utf-8") == primary_text["summary"]
    assert (run_dir / "manifest.json").read_text(encoding="utf-8") == primary_text["manifest"]
    assert (run_dir / "results.json").read_text(encoding="utf-8") == primary_text["results"]


def test_historical_reindex_writes_sidecars_only_and_keeps_primary_evidence(tmp_path: Path, capsys) -> None:
    run_dir = tmp_path / "reindex_run"
    primary_text = _write_run(run_dir, run_id="reindex-run")

    assert historical_artifacts.main(["reindex", "--run-dir", str(run_dir)]) == 0
    report = _parse_last_json(capsys.readouterr().out)
    run_report = report["runs"][0]

    assert report["primary_evidence_rewritten"] is False
    assert run_report["primary_evidence_rewritten"] is False
    assert report["index_schema_version"] == ARTIFACT_COMPATIBILITY_INDEX_SCHEMA_VERSION
    assert report["compatibility_rollup"]["regenerate_recommended_count"] == 1
    assert run_report["regenerate_scope"] == "reviewer_index_sidecar_only"
    assert run_report["boundary_digest"]
    assert run_report["non_claim_digest"]
    assert run_report["compatibility_rollup"]["primary_evidence_rewritten"] is False
    assert run_report["compatibility_rollup"]["linked_surface_visibility"] == [
        "results",
        "review_center",
        "workbench",
    ]
    assert run_report["recognition_scope_rollup"]["primary_evidence_rewritten"] is False
    assert run_report["written_paths"]["run_artifact_index"]["json_path"].endswith(RUN_ARTIFACT_INDEX_FILENAME)
    assert (run_dir / RUN_ARTIFACT_INDEX_FILENAME).exists()
    assert (run_dir / ARTIFACT_CONTRACT_CATALOG_FILENAME).exists()
    assert (run_dir / COMPATIBILITY_SCAN_SUMMARY_FILENAME).exists()
    assert (run_dir / REINDEX_MANIFEST_FILENAME).exists()
    assert (run_dir / "summary.json").read_text(encoding="utf-8") == primary_text["summary"]
    assert (run_dir / "manifest.json").read_text(encoding="utf-8") == primary_text["manifest"]
    assert (run_dir / "results.json").read_text(encoding="utf-8") == primary_text["results"]
