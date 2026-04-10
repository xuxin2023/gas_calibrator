"""WP6 contract tests: PT/ILC importer + comparison evidence pack + reviewer navigation.

Step 2 only — reviewer-facing / readiness-mapping-only.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from gas_calibrator.v2.adapters.wp6_gateway import Wp6Gateway
from gas_calibrator.v2.core.wp6_builder import (
    COMPARISON_TYPES,
    IMPORT_MODES,
    WP6_BUILDER_SCHEMA_VERSION,
    WP6_COMPARISON_VERSION,
    build_wp6_artifacts,
    import_comparison_from_csv,
    import_comparison_from_json,
)
from gas_calibrator.v2.core.wp6_repository import (
    WP6_DB_READY_MODE,
    WP6_GATEWAY_MODE,
    WP6_REPOSITORY_MODE,
    WP6_REPOSITORY_SCHEMA_VERSION,
    DatabaseReadyWp6RepositoryStub,
    FileBackedWp6Repository,
)


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _make_minimal_run_dir(tmp_path: Path) -> Path:
    """Create a minimal run_dir with summary.json for repository tests."""
    run_dir = tmp_path / "run-001"
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "summary.json").write_text(
        json.dumps({"run_id": "run-001", "status": "completed"}), encoding="utf-8"
    )
    return run_dir


def _build_gateway(tmp_path: Path) -> tuple[Path, Wp6Gateway]:
    run_dir = _make_minimal_run_dir(tmp_path)
    gateway = Wp6Gateway(run_dir)
    return run_dir, gateway


# ---------------------------------------------------------------------------
# 1) WP6 object model contract
# ---------------------------------------------------------------------------

class TestWp6ObjectModelContract:
    def test_wp6_builder_returns_all_artifact_keys(self, tmp_path: Path) -> None:
        artifacts = build_wp6_artifacts(
            run_id="test-run",
            scope_definition_pack={"raw": {"scope_id": "s1"}, "digest": {}, "review_surface": {}},
            decision_rule_profile={"raw": {"decision_rule_id": "dr1"}, "digest": {}, "review_surface": {}},
            reference_asset_registry={"raw": {}, "digest": {}, "review_surface": {}},
            certificate_lifecycle_summary={"raw": {}, "digest": {}, "review_surface": {}},
            pre_run_readiness_gate={"raw": {}, "digest": {}, "review_surface": {}},
            uncertainty_report_pack={"raw": {}, "digest": {}, "review_surface": {}},
            uncertainty_rollup={"raw": {}, "digest": {}, "review_surface": {}},
            method_confirmation_protocol={"raw": {}, "digest": {}, "review_surface": {}},
            verification_digest={"raw": {}, "digest": {}, "review_surface": {}},
            software_validation_rollup={"raw": {}, "digest": {}, "review_surface": {}},
            path_map={},
            filenames={},
            boundary_statements=["readiness mapping only"],
        )
        expected_keys = {
            "pt_ilc_registry",
            "external_comparison_importer",
            "comparison_evidence_pack",
            "scope_comparison_view",
            "comparison_digest",
            "comparison_rollup",
        }
        assert set(artifacts.keys()) == expected_keys

    def test_wp6_builder_all_artifacts_are_bundles(self, tmp_path: Path) -> None:
        artifacts = build_wp6_artifacts(
            run_id="test-run",
            scope_definition_pack={"raw": {}, "digest": {}, "review_surface": {}},
            decision_rule_profile={"raw": {}, "digest": {}, "review_surface": {}},
            reference_asset_registry={"raw": {}, "digest": {}, "review_surface": {}},
            certificate_lifecycle_summary={"raw": {}, "digest": {}, "review_surface": {}},
            pre_run_readiness_gate={"raw": {}, "digest": {}, "review_surface": {}},
            uncertainty_report_pack={"raw": {}, "digest": {}, "review_surface": {}},
            uncertainty_rollup={"raw": {}, "digest": {}, "review_surface": {}},
            method_confirmation_protocol={"raw": {}, "digest": {}, "review_surface": {}},
            verification_digest={"raw": {}, "digest": {}, "review_surface": {}},
            software_validation_rollup={"raw": {}, "digest": {}, "review_surface": {}},
            path_map={},
            filenames={},
            boundary_statements=["readiness mapping only"],
        )
        for key, artifact in artifacts.items():
            assert "raw" in artifact, f"{key} missing 'raw'"
            assert "digest" in artifact, f"{key} missing 'digest'"
            assert "available" in artifact, f"{key} missing 'available'"
            raw = artifact["raw"]
            assert raw.get("not_real_acceptance_evidence") is True, f"{key} not_real_acceptance_evidence"
            assert raw.get("primary_evidence_rewritten") is False, f"{key} primary_evidence_rewritten"
            assert raw.get("ready_for_readiness_mapping") is True, f"{key} ready_for_readiness_mapping"
            assert raw.get("not_ready_for_formal_claim") is True, f"{key} not_ready_for_formal_claim"
            assert raw.get("evidence_source") == "simulated", f"{key} evidence_source"
            assert "review_surface" in raw, f"{key} missing review_surface"

    def test_wp6_pt_ilc_registry_has_required_fields(self, tmp_path: Path) -> None:
        artifacts = build_wp6_artifacts(
            run_id="test-run",
            scope_definition_pack={"raw": {"scope_id": "s1"}, "digest": {}, "review_surface": {}},
            decision_rule_profile={"raw": {"decision_rule_id": "dr1"}, "digest": {}, "review_surface": {}},
            reference_asset_registry={"raw": {}, "digest": {}, "review_surface": {}},
            certificate_lifecycle_summary={"raw": {}, "digest": {}, "review_surface": {}},
            pre_run_readiness_gate={"raw": {}, "digest": {}, "review_surface": {}},
            uncertainty_report_pack={"raw": {}, "digest": {}, "review_surface": {}},
            uncertainty_rollup={"raw": {}, "digest": {}, "review_surface": {}},
            method_confirmation_protocol={"raw": {}, "digest": {}, "review_surface": {}},
            verification_digest={"raw": {}, "digest": {}, "review_surface": {}},
            software_validation_rollup={"raw": {}, "digest": {}, "review_surface": {}},
            path_map={},
            filenames={},
            boundary_statements=["readiness mapping only"],
        )
        registry = artifacts["pt_ilc_registry"]
        raw = registry["raw"]
        # Required fields per spec
        for field in (
            "comparison_id", "comparison_version",
            "scope_id", "decision_rule_id",
            "linked_uncertainty_case_ids", "linked_method_confirmation_protocol_ids",
            "linked_software_validation_release_ids",
            "reference_asset_refs", "certificate_lifecycle_refs",
            "source_files", "import_mode",
            "reviewer_only", "readiness_mapping_only",
            "not_real_acceptance_evidence", "not_ready_for_formal_claim",
            "primary_evidence_rewritten",
            "limitation_note", "non_claim_note",
        ):
            assert field in raw, f"pt_ilc_registry missing field: {field}"
        # Rows should have PT and ILC demo entries
        rows = raw.get("rows", [])
        assert len(rows) >= 2
        types = {r["comparison_type"] for r in rows}
        assert "PT" in types
        assert "ILC" in types

    def test_wp6_comparison_types(self) -> None:
        assert "PT" in COMPARISON_TYPES
        assert "ILC" in COMPARISON_TYPES
        assert "external_comparison" in COMPARISON_TYPES
        assert "readiness_demo" in COMPARISON_TYPES

    def test_wp6_import_modes(self) -> None:
        for mode in ("local_json", "local_csv", "local_markdown", "artifact_sidecar", "manual_fixture"):
            assert mode in IMPORT_MODES


# ---------------------------------------------------------------------------
# 2) importer schema normalization / legacy compatibility
# ---------------------------------------------------------------------------

class TestWp6ImporterContract:
    def test_import_json_file_not_found(self, tmp_path: Path) -> None:
        result = import_comparison_from_json(tmp_path / "nonexistent.json")
        assert result["status"] == "warning"
        assert result["warning_type"] == "file_not_found"
        assert result["not_real_acceptance_evidence"] is True

    def test_import_json_invalid_json(self, tmp_path: Path) -> None:
        bad_file = tmp_path / "bad.json"
        bad_file.write_text("not json{{{", encoding="utf-8")
        result = import_comparison_from_json(bad_file)
        assert result["status"] == "warning"
        assert result["warning_type"] == "json_parse_error"

    def test_import_json_non_dict_root(self, tmp_path: Path) -> None:
        arr_file = tmp_path / "array.json"
        arr_file.write_text("[1, 2, 3]", encoding="utf-8")
        result = import_comparison_from_json(arr_file)
        assert result["status"] == "warning"
        assert result["warning_type"] == "invalid_schema"

    def test_import_json_valid_payload(self, tmp_path: Path) -> None:
        payload = {
            "comparison_id": "pt-001",
            "comparison_type": "PT",
            "scope_id": "scope-1",
            "decision_rule_id": "dr-1",
        }
        json_file = tmp_path / "comparison.json"
        json_file.write_text(json.dumps(payload), encoding="utf-8")
        result = import_comparison_from_json(json_file)
        assert result["status"] == "ok"
        assert result["evidence_source"] == "simulated"
        assert result["import_mode"] == "local_json"
        assert result["not_real_acceptance_evidence"] is True
        assert result["primary_evidence_rewritten"] is False
        imported = result["imported_data"]
        assert imported["comparison_id"] == "pt-001"
        assert imported["comparison_type"] == "PT"
        assert imported["evidence_source"] == "simulated"
        assert imported["reviewer_only"] is True
        assert imported["readiness_mapping_only"] is True

    def test_import_json_missing_fields_get_defaults(self, tmp_path: Path) -> None:
        payload = {"comparison_id": "minimal"}
        json_file = tmp_path / "minimal.json"
        json_file.write_text(json.dumps(payload), encoding="utf-8")
        result = import_comparison_from_json(json_file)
        imported = result["imported_data"]
        assert imported["comparison_type"] == "readiness_demo"
        assert imported["evidence_source"] == "simulated"
        assert imported["reviewer_only"] is True
        assert imported["not_real_acceptance_evidence"] is True

    def test_import_json_unknown_comparison_type_normalized(self, tmp_path: Path) -> None:
        payload = {"comparison_id": "x", "comparison_type": "UNKNOWN_TYPE"}
        json_file = tmp_path / "unknown.json"
        json_file.write_text(json.dumps(payload), encoding="utf-8")
        result = import_comparison_from_json(json_file)
        imported = result["imported_data"]
        assert imported["comparison_type"] == "readiness_demo"

    def test_import_csv_file_not_found(self, tmp_path: Path) -> None:
        result = import_comparison_from_csv(tmp_path / "nonexistent.csv")
        assert result["status"] == "warning"
        assert result["warning_type"] == "file_not_found"

    def test_import_csv_valid(self, tmp_path: Path) -> None:
        csv_file = tmp_path / "comparisons.csv"
        csv_file.write_text(
            "comparison_id,comparison_type,scope_id\npt-001,PT,scope-1\nilc-001,ILC,scope-1\n",
            encoding="utf-8",
        )
        result = import_comparison_from_csv(csv_file)
        assert result["status"] == "ok"
        assert result["evidence_source"] == "simulated"
        assert result["import_mode"] == "local_csv"
        assert result["imported_data"]["row_count"] == 2

    def test_import_preserves_extra_fields(self, tmp_path: Path) -> None:
        payload = {"comparison_id": "x", "custom_field": "custom_value"}
        json_file = tmp_path / "extra.json"
        json_file.write_text(json.dumps(payload), encoding="utf-8")
        result = import_comparison_from_json(json_file)
        imported = result["imported_data"]
        assert "extra_fields" in imported
        assert imported["extra_fields"]["custom_field"] == "custom_value"


# ---------------------------------------------------------------------------
# 3) repository / gateway contract
# ---------------------------------------------------------------------------

class TestWp6RepositoryGatewayContract:
    def test_file_backed_repository_load_snapshot_keys(self, tmp_path: Path) -> None:
        run_dir = _make_minimal_run_dir(tmp_path)
        repo = FileBackedWp6Repository(run_dir)
        snapshot = repo.load_snapshot()
        expected_keys = {
            "pt_ilc_registry",
            "external_comparison_importer",
            "comparison_evidence_pack",
            "scope_comparison_view",
            "comparison_digest",
            "comparison_rollup",
        }
        assert expected_keys.issubset(set(snapshot.keys()))

    def test_file_backed_repository_step2_boundary(self, tmp_path: Path) -> None:
        run_dir = _make_minimal_run_dir(tmp_path)
        repo = FileBackedWp6Repository(run_dir)
        snapshot = repo.load_snapshot()
        for key in ("pt_ilc_registry", "external_comparison_importer", "comparison_evidence_pack",
                     "scope_comparison_view", "comparison_digest", "comparison_rollup"):
            payload = snapshot[key]
            assert payload.get("not_real_acceptance_evidence") is True, f"{key}"
            assert payload.get("primary_evidence_rewritten") is False, f"{key}"
            assert payload.get("evidence_source") == "simulated", f"{key}"

    def test_file_backed_repository_rollup(self, tmp_path: Path) -> None:
        run_dir = _make_minimal_run_dir(tmp_path)
        repo = FileBackedWp6Repository(run_dir)
        snapshot = repo.load_snapshot()
        rollup = snapshot["comparison_rollup"]
        # schema_version should be set by repository (may be overridden by builder fallback)
        assert "schema_version" in rollup
        assert rollup.get("repository_mode") == WP6_REPOSITORY_MODE
        assert rollup.get("gateway_mode") == WP6_GATEWAY_MODE
        assert rollup.get("primary_evidence_rewritten") is False
        assert rollup.get("not_real_acceptance_evidence") is True

    def test_db_ready_stub(self, tmp_path: Path) -> None:
        run_dir = _make_minimal_run_dir(tmp_path)
        stub = DatabaseReadyWp6RepositoryStub(run_dir)
        snapshot = stub.load_snapshot()
        rollup = snapshot["comparison_rollup"]
        assert rollup["repository_mode"] == WP6_DB_READY_MODE
        assert rollup["gateway_mode"] == "not_active"
        db_stub = rollup["db_ready_stub"]
        assert db_stub["enabled"] is False
        assert db_stub["not_in_default_chain"] is True
        assert db_stub["requires_explicit_injection"] is True
        assert rollup["primary_evidence_rewritten"] is False
        assert rollup["not_real_acceptance_evidence"] is True

    def test_gateway_read_payload_keys(self, tmp_path: Path) -> None:
        _, gateway = _build_gateway(tmp_path)
        payload = gateway.read_payload()
        expected_keys = {
            "pt_ilc_registry",
            "external_comparison_importer",
            "comparison_evidence_pack",
            "scope_comparison_view",
            "comparison_digest",
            "comparison_rollup",
        }
        assert set(payload.keys()) == expected_keys

    def test_gateway_step2_boundary(self, tmp_path: Path) -> None:
        _, gateway = _build_gateway(tmp_path)
        payload = gateway.read_payload()
        for key, value in payload.items():
            assert value.get("not_real_acceptance_evidence") is True, f"{key}"
            assert value.get("primary_evidence_rewritten") is False, f"{key}"
            assert value.get("evidence_source") == "simulated", f"{key}"


# ---------------------------------------------------------------------------
# 4) results_gateway reviewer payload exposure
# ---------------------------------------------------------------------------

class TestWp6ResultsGatewayVisibility:
    def test_results_gateway_exposes_wp6_keys(self, tmp_path: Path) -> None:
        from gas_calibrator.v2.adapters.results_gateway import ResultsGateway
        run_dir = _make_minimal_run_dir(tmp_path)
        gateway = ResultsGateway(run_dir)
        payload = gateway.read_results_payload()
        for key in ("pt_ilc_registry", "external_comparison_importer",
                     "comparison_evidence_pack", "scope_comparison_view",
                     "comparison_digest", "comparison_rollup"):
            assert key in payload, f"results_gateway missing WP6 key: {key}"
            value = payload[key]
            assert isinstance(value, dict), f"results_gateway WP6 key {key} not dict"
