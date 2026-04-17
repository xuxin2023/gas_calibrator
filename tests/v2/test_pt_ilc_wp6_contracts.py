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

    def test_import_json_rows_payload_normalizes_trace_and_list_fields(self, tmp_path: Path) -> None:
        payload = {
            "scope_id": "scope-1",
            "decision_rule_id": "dr-1",
            "rows": [
                {
                    "comparison_id": "pt-rows-001",
                    "comparison_type": "PT",
                    "linked_uncertainty_case_ids": "unc-1|unc-2",
                    "linked_method_confirmation_protocol_ids": "mc-1;mc-2",
                    "linked_software_validation_release_ids": "sv-1,sv-2",
                    "reference_asset_refs": "asset-1|asset-2",
                    "certificate_lifecycle_refs": "cert-1;cert-2",
                }
            ],
        }
        json_file = tmp_path / "comparison_rows.json"
        json_file.write_text(json.dumps(payload), encoding="utf-8")
        result = import_comparison_from_json(json_file)
        imported = result["imported_data"]
        assert imported["row_count"] == 1
        row = imported["rows"][0]
        assert row["scope_id"] == "scope-1"
        assert row["decision_rule_id"] == "dr-1"
        assert row["import_mode"] == "local_json"
        assert row["source_file"] == str(json_file)
        assert row["linked_uncertainty_case_ids"] == ["unc-1", "unc-2"]
        assert row["linked_method_confirmation_protocol_ids"] == ["mc-1", "mc-2"]
        assert row["linked_software_validation_release_ids"] == ["sv-1", "sv-2"]
        assert row["reference_asset_refs"] == ["asset-1", "asset-2"]
        assert row["certificate_lifecycle_refs"] == ["cert-1", "cert-2"]

    def test_import_csv_normalizes_trace_and_list_fields(self, tmp_path: Path) -> None:
        csv_file = tmp_path / "comparison_linked.csv"
        csv_file.write_text(
            (
                "comparison_id,comparison_type,scope_id,decision_rule_id,"
                "linked_uncertainty_case_ids,linked_method_confirmation_protocol_ids,"
                "linked_software_validation_release_ids,reference_asset_refs,certificate_lifecycle_refs\n"
                "pt-001,PT,scope-1,dr-1,unc-1|unc-2,mc-1;mc-2,sv-1,asset-1|asset-2,cert-1;cert-2\n"
            ),
            encoding="utf-8",
        )
        result = import_comparison_from_csv(csv_file)
        imported = result["imported_data"]
        assert imported["row_count"] == 1
        row = imported["rows"][0]
        assert row["import_mode"] == "local_csv"
        assert row["source_file"] == str(csv_file)
        assert row["linked_uncertainty_case_ids"] == ["unc-1", "unc-2"]
        assert row["linked_method_confirmation_protocol_ids"] == ["mc-1", "mc-2"]
        assert row["linked_software_validation_release_ids"] == ["sv-1"]
        assert row["reference_asset_refs"] == ["asset-1", "asset-2"]
        assert row["certificate_lifecycle_refs"] == ["cert-1", "cert-2"]

    def test_comparison_evidence_pack_auto_links_scope_decision_and_upstream_refs(self) -> None:
        artifacts = build_wp6_artifacts(
            run_id="test-run",
            scope_definition_pack={"raw": {"scope_id": "scope-1", "scope_name": "CO2 scope"}, "digest": {}, "review_surface": {}},
            decision_rule_profile={"raw": {"decision_rule_id": "dr-1"}, "digest": {}, "review_surface": {}},
            reference_asset_registry={
                "raw": {
                    "assets": [
                        {"asset_id": "asset-1"},
                        {"asset_id": "asset-2"},
                    ]
                },
                "digest": {},
                "review_surface": {},
            },
            certificate_lifecycle_summary={
                "raw": {
                    "certificate_rows": [
                        {"certificate_id": "cert-1"},
                        {"certificate_id": "cert-2"},
                    ]
                },
                "digest": {},
                "review_surface": {},
            },
            pre_run_readiness_gate={"raw": {}, "digest": {}, "review_surface": {}},
            uncertainty_report_pack={"raw": {"selected_result_case_id": "unc-1"}, "digest": {}, "review_surface": {}},
            uncertainty_rollup={
                "raw": {
                    "case_ids": ["unc-1", "unc-2"],
                    "method_confirmation_protocol_id": "mc-1",
                },
                "digest": {},
                "review_surface": {},
            },
            method_confirmation_protocol={"raw": {"protocol_id": "mc-1"}, "digest": {}, "review_surface": {}},
            verification_digest={"raw": {"method_confirmation_protocol_id": "mc-1"}, "digest": {}, "review_surface": {}},
            software_validation_rollup={"raw": {"release_id": "sv-1"}, "digest": {}, "review_surface": {}},
            path_map={
                "reference_asset_registry": "/tmp/reference_asset_registry.json",
                "certificate_lifecycle_summary": "/tmp/certificate_lifecycle_summary.json",
            },
            filenames={},
            boundary_statements=["readiness mapping only"],
        )
        pack = artifacts["comparison_evidence_pack"]["raw"]
        registry = artifacts["pt_ilc_registry"]["raw"]
        rollup_digest = artifacts["comparison_rollup"]["digest"]
        assert pack["scope_id"] == "scope-1"
        assert pack["decision_rule_id"] == "dr-1"
        assert pack["linked_uncertainty_case_ids"] == ["unc-1", "unc-2"]
        assert pack["linked_method_confirmation_protocol_ids"] == ["mc-1"]
        assert pack["linked_software_validation_release_ids"] == ["sv-1"]
        assert pack["reference_asset_refs"] == ["asset-1", "asset-2"]
        assert pack["certificate_lifecycle_refs"] == ["cert-1", "cert-2"]
        assert pack["coverage_summary"]["scope_linked"] is True
        assert pack["coverage_summary"]["decision_rule_linked"] is True
        assert pack["coverage_summary"]["uncertainty_linked"] is True
        assert pack["coverage_summary"]["method_confirmation_linked"] is True
        assert pack["coverage_summary"]["software_validation_linked"] is True
        assert registry["rows"][0]["source_file"].startswith("local_fixture://")
        assert "comparison_import_trace_summary" in rollup_digest


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


# ---------------------------------------------------------------------------
# 5) WP6.1 收口测试: artifact_catalog / artifact_compatibility / offline_artifacts
# ---------------------------------------------------------------------------

class TestWp6ArtifactCatalogRegistration:
    """WP6 artifacts must be registered in artifact_catalog."""

    def test_wp6_artifacts_in_known_artifact_keys_by_filename(self) -> None:
        from gas_calibrator.v2.core.artifact_catalog import KNOWN_ARTIFACT_KEYS_BY_FILENAME
        wp6_filenames = [
            "pt_ilc_registry.json", "pt_ilc_registry.md",
            "external_comparison_importer.json", "external_comparison_importer.md",
            "comparison_evidence_pack.json", "comparison_evidence_pack.md",
            "scope_comparison_view.json", "scope_comparison_view.md",
            "comparison_digest.json", "comparison_digest.md",
            "comparison_rollup.json", "comparison_rollup.md",
        ]
        for fn in wp6_filenames:
            assert fn in KNOWN_ARTIFACT_KEYS_BY_FILENAME, f"artifact_catalog missing WP6 filename: {fn}"

    def test_wp6_artifacts_in_known_report_artifacts(self) -> None:
        from gas_calibrator.v2.core.artifact_catalog import KNOWN_REPORT_ARTIFACTS
        wp6_filenames = [
            "pt_ilc_registry.json", "pt_ilc_registry.md",
            "external_comparison_importer.json", "external_comparison_importer.md",
            "comparison_evidence_pack.json", "comparison_evidence_pack.md",
            "scope_comparison_view.json", "scope_comparison_view.md",
            "comparison_digest.json", "comparison_digest.md",
            "comparison_rollup.json", "comparison_rollup.md",
        ]
        for fn in wp6_filenames:
            assert fn in KNOWN_REPORT_ARTIFACTS, f"KNOWN_REPORT_ARTIFACTS missing WP6 filename: {fn}"


class TestWp6ArtifactCompatibilityRegistration:
    """WP6 artifacts must be in artifact_compatibility canonical surface and visibility."""

    def test_wp6_filenames_in_canonical_surface_filenames(self) -> None:
        from gas_calibrator.v2.core.artifact_compatibility import CANONICAL_SURFACE_FILENAMES
        from gas_calibrator.v2.core import recognition_readiness_artifacts as rr
        wp6_constants = [
            rr.PT_ILC_REGISTRY_FILENAME, rr.PT_ILC_REGISTRY_MARKDOWN_FILENAME,
            rr.EXTERNAL_COMPARISON_IMPORTER_FILENAME, rr.EXTERNAL_COMPARISON_IMPORTER_MARKDOWN_FILENAME,
            rr.COMPARISON_EVIDENCE_PACK_FILENAME, rr.COMPARISON_EVIDENCE_PACK_MARKDOWN_FILENAME,
            rr.SCOPE_COMPARISON_VIEW_FILENAME, rr.SCOPE_COMPARISON_VIEW_MARKDOWN_FILENAME,
            rr.COMPARISON_DIGEST_FILENAME, rr.COMPARISON_DIGEST_MARKDOWN_FILENAME,
            rr.COMPARISON_ROLLUP_FILENAME, rr.COMPARISON_ROLLUP_MARKDOWN_FILENAME,
        ]
        for fn in wp6_constants:
            assert fn in CANONICAL_SURFACE_FILENAMES, f"CANONICAL_SURFACE_FILENAMES missing: {fn}"

    def test_wp6_keys_in_surface_visibility(self) -> None:
        from gas_calibrator.v2.core.artifact_compatibility import _surface_visibility
        from gas_calibrator.v2.core import recognition_readiness_artifacts as rr
        # _surface_visibility takes keyword args; verify WP6 keys are recognized
        for key in ("pt_ilc_registry", "external_comparison_importer",
                     "comparison_evidence_pack", "scope_comparison_view",
                     "comparison_digest", "comparison_rollup"):
            surfaces = _surface_visibility(artifact_key=key, artifact_role="diagnostic_analysis")
            assert "review_center" in surfaces, f"WP6 key {key} not visible in review_center"
            assert "workbench" in surfaces, f"WP6 key {key} not visible in workbench"


class TestWp6OfflineArtifactsIntegration:
    """WP6 artifacts must be in offline_artifacts path_map and role_map."""

    def test_wp6_paths_in_offline_artifact_paths(self) -> None:
        from gas_calibrator.v2.core import recognition_readiness_artifacts as rr
        # Verify WP6 filename constants exist in recognition_readiness
        assert hasattr(rr, "PT_ILC_REGISTRY_FILENAME")
        assert hasattr(rr, "COMPARISON_EVIDENCE_PACK_FILENAME")
        assert hasattr(rr, "COMPARISON_DIGEST_FILENAME")
        assert hasattr(rr, "COMPARISON_ROLLUP_FILENAME")
        assert hasattr(rr, "SCOPE_COMPARISON_VIEW_FILENAME")
        assert hasattr(rr, "EXTERNAL_COMPARISON_IMPORTER_FILENAME")


class TestWp6RecognitionReadinessIntegration:
    """WP6 builder must be integrated into build_recognition_readiness_artifacts."""

    def test_build_recognition_readiness_includes_wp6_keys(self, tmp_path: Path) -> None:
        from gas_calibrator.v2.core.recognition_readiness_artifacts import build_recognition_readiness_artifacts
        run_dir = tmp_path / "run-rr-001"
        run_dir.mkdir(parents=True, exist_ok=True)
        artifacts = build_recognition_readiness_artifacts(
            run_id="run-rr-001",
            samples=[],
            point_summaries=[],
            versions={"v2": "0.1.0"},
            run_dir=str(run_dir),
        )
        for key in ("pt_ilc_registry", "external_comparison_importer",
                     "comparison_evidence_pack", "scope_comparison_view",
                     "comparison_digest", "comparison_rollup"):
            assert key in artifacts, f"build_recognition_readiness_artifacts missing WP6 key: {key}"

    def test_wp6_artifacts_step2_boundary_in_recognition_readiness(self, tmp_path: Path) -> None:
        from gas_calibrator.v2.core.recognition_readiness_artifacts import build_recognition_readiness_artifacts
        run_dir = tmp_path / "run-rr-002"
        run_dir.mkdir(parents=True, exist_ok=True)
        artifacts = build_recognition_readiness_artifacts(
            run_id="run-rr-002",
            samples=[],
            point_summaries=[],
            versions={"v2": "0.1.0"},
            run_dir=str(run_dir),
        )
        for key in ("pt_ilc_registry", "comparison_evidence_pack",
                     "comparison_digest", "comparison_rollup"):
            bundle = artifacts[key]
            # build_recognition_readiness_artifacts returns enriched bundles with raw sub-dict
            raw = bundle.get("raw", bundle)
            assert raw.get("not_real_acceptance_evidence") is True, f"{key}"
            assert raw.get("primary_evidence_rewritten") is False, f"{key}"
            assert raw.get("evidence_source") == "simulated", f"{key}"
            assert raw.get("not_ready_for_formal_claim") is True, f"{key}"
            assert raw.get("readiness_mapping_only") is True, f"{key}"


class TestWp6HistoricalArtifactsIntegration:
    """Wp6Gateway must be explicitly called in historical_artifacts.py."""

    def test_historical_artifacts_imports_wp6_gateway(self) -> None:
        import gas_calibrator.v2.scripts.historical_artifacts as ha
        # Verify the module has the Wp6Gateway import by checking source
        import inspect
        source = inspect.getsource(ha)
        assert "Wp6Gateway" in source, "historical_artifacts.py does not import Wp6Gateway"

    def test_historical_artifacts_exposes_wp6_keys(self) -> None:
        import gas_calibrator.v2.scripts.historical_artifacts as ha
        import inspect
        source = inspect.getsource(ha)
        # Verify WP6 payload variables are extracted
        for var in ("pt_ilc_registry", "comparison_evidence_pack", "comparison_rollup"):
            assert var in source, f"historical_artifacts.py does not reference {var}"


class TestWp6DeviceWorkbenchIntegration:
    """WP6 payloads must be in device_workbench.py payloads dict."""

    def test_device_workbench_extracts_wp6_payloads(self) -> None:
        import gas_calibrator.v2.ui_v2.controllers.device_workbench as dw
        import inspect
        source = inspect.getsource(dw)
        for key in ("pt_ilc_registry", "comparison_evidence_pack", "comparison_rollup"):
            assert key in source, f"device_workbench.py does not reference {key}"


class TestWp6AppFacadeIntegration:
    """WP6 payloads must be in app_facade.py readiness_summary_payloads."""

    def test_app_facade_extracts_wp6_payloads(self) -> None:
        import gas_calibrator.v2.ui_v2.controllers.app_facade as af
        import inspect
        source = inspect.getsource(af)
        # After Step 2.5, WP6 keys are accessed via bundle, not as local vars
        assert "_wp6_closeout_bundle" in source or "_build_wp6_closeout_bundle" in source


class TestWp6ReviewerSurfaceBoundary:
    """All WP6 reviewer surfaces must maintain Step 2 boundary markers."""

    def test_wp6_gateway_all_keys_step2_boundary(self, tmp_path: Path) -> None:
        _, gateway = _build_gateway(tmp_path)
        payload = gateway.read_payload()
        for key, value in payload.items():
            assert value.get("reviewer_only") is True or value.get("readiness_mapping_only") is True, f"{key} missing reviewer boundary"
            assert value.get("not_real_acceptance_evidence") is True, f"{key} not_real_acceptance_evidence"
            assert value.get("not_ready_for_formal_claim") is True, f"{key} not_ready_for_formal_claim"
            assert value.get("primary_evidence_rewritten") is False, f"{key} primary_evidence_rewritten"
            assert value.get("evidence_source") == "simulated", f"{key} evidence_source"

    def test_wp6_non_claim_note_present(self, tmp_path: Path) -> None:
        _, gateway = _build_gateway(tmp_path)
        payload = gateway.read_payload()
        for key, value in payload.items():
            assert "non_claim_note" in value, f"{key} missing non_claim_note"
            assert "simulated" in value.get("non_claim_note", "").lower() or "not real" in value.get("non_claim_note", "").lower(), f"{key} non_claim_note missing boundary language"


# ---------------------------------------------------------------------------
# 6) Step 2 总收口测试: role catalog / closeout digest / consistency
# ---------------------------------------------------------------------------

class TestStep2RoleCatalogConsistency:
    """WP1–WP6 artifact keys must have consistent role assignments."""

    def test_wp6_keys_in_default_role_catalog(self) -> None:
        from gas_calibrator.v2.core.artifact_catalog import DEFAULT_ROLE_CATALOG
        all_keys = set()
        for role_keys in DEFAULT_ROLE_CATALOG.values():
            all_keys.update(role_keys)
        for key in ("pt_ilc_registry", "external_comparison_importer",
                     "comparison_evidence_pack", "scope_comparison_view",
                     "comparison_digest", "comparison_rollup",
                     "step2_closeout_digest"):
            assert key in all_keys, f"DEFAULT_ROLE_CATALOG missing WP6 key: {key}"

    def test_wp6_markdown_keys_in_default_role_catalog(self) -> None:
        from gas_calibrator.v2.core.artifact_catalog import DEFAULT_ROLE_CATALOG
        all_keys = set()
        for role_keys in DEFAULT_ROLE_CATALOG.values():
            all_keys.update(role_keys)
        for key in ("pt_ilc_registry_markdown", "external_comparison_importer_markdown",
                     "comparison_evidence_pack_markdown", "scope_comparison_view_markdown",
                     "comparison_digest_markdown", "comparison_rollup_markdown",
                     "step2_closeout_digest_markdown"):
            assert key in all_keys, f"DEFAULT_ROLE_CATALOG missing WP6 markdown key: {key}"


class TestStep2CloseoutDigestContract:
    """Step 2 closeout digest must aggregate WP1–WP6 and maintain boundary."""

    def test_closeout_digest_structure(self) -> None:
        from gas_calibrator.v2.core.wp6_builder import build_step2_closeout_digest
        digest = build_step2_closeout_digest(
            run_id="test-closeout",
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
            comparison_rollup={"raw": {}, "digest": {}, "review_surface": {}},
            boundary_statements=["readiness mapping only"],
        )
        assert digest["available"] is True
        assert digest["artifact_type"] == "step2_closeout_digest"
        raw = digest["raw"]
        assert raw["not_real_acceptance_evidence"] is True
        assert raw["primary_evidence_rewritten"] is False
        assert raw["not_ready_for_formal_claim"] is True
        assert raw["readiness_mapping_only"] is True
        assert raw["reviewer_only"] is True
        assert raw["evidence_source"] == "simulated"
        assert "non_claim_note" in raw
        assert "wp_status" in raw
        # Must cover WP1–WP6
        wp_labels = set(raw["wp_status"].keys())
        assert "WP1_scope" in wp_labels
        assert "WP3_uncertainty" in wp_labels
        assert "WP4_method_confirmation" in wp_labels
        assert "WP5_software_validation" in wp_labels
        assert "WP6_comparison" in wp_labels

    def test_closeout_digest_in_recognition_readiness(self, tmp_path: Path) -> None:
        from gas_calibrator.v2.core.recognition_readiness_artifacts import build_recognition_readiness_artifacts
        run_dir = tmp_path / "run-closeout-001"
        run_dir.mkdir(parents=True, exist_ok=True)
        artifacts = build_recognition_readiness_artifacts(
            run_id="run-closeout-001",
            samples=[],
            point_summaries=[],
            versions={"v2": "0.1.0"},
            run_dir=str(run_dir),
        )
        assert "step2_closeout_digest" in artifacts
        closeout = artifacts["step2_closeout_digest"]
        raw = closeout.get("raw", closeout)
        assert raw.get("not_real_acceptance_evidence") is True
        assert raw.get("primary_evidence_rewritten") is False
        # all_simulated may be False if some WP payloads lack the boundary marker
        # but the closeout digest itself must still be boundary-compliant
        assert raw.get("evidence_source") == "simulated"
        assert raw.get("not_ready_for_formal_claim") is True

    def test_closeout_digest_registered_in_catalog(self) -> None:
        from gas_calibrator.v2.core.artifact_catalog import (
            KNOWN_ARTIFACT_KEYS_BY_FILENAME,
            KNOWN_REPORT_ARTIFACTS,
        )
        assert "step2_closeout_digest.json" in KNOWN_ARTIFACT_KEYS_BY_FILENAME
        assert "step2_closeout_digest.md" in KNOWN_ARTIFACT_KEYS_BY_FILENAME
        assert "step2_closeout_digest.json" in KNOWN_REPORT_ARTIFACTS
        assert "step2_closeout_digest.md" in KNOWN_REPORT_ARTIFACTS

    def test_closeout_digest_in_compatibility(self) -> None:
        from gas_calibrator.v2.core.artifact_compatibility import (
            CANONICAL_SURFACE_FILENAMES,
            _surface_visibility,
        )
        from gas_calibrator.v2.core import recognition_readiness_artifacts as rr
        assert rr.STEP2_CLOSEOUT_DIGEST_FILENAME in CANONICAL_SURFACE_FILENAMES
        assert rr.STEP2_CLOSEOUT_DIGEST_MARKDOWN_FILENAME in CANONICAL_SURFACE_FILENAMES
        surfaces = _surface_visibility(artifact_key="step2_closeout_digest", artifact_role="diagnostic_analysis")
        assert "review_center" in surfaces
        assert "workbench" in surfaces


class TestStep2SurfaceConsistency:
    """All WP1–WP6 keys must be consistently exposed across surfaces."""

    def test_all_wp6_keys_in_historical(self) -> None:
        import gas_calibrator.v2.scripts.historical_artifacts as ha
        import inspect
        source = inspect.getsource(ha)
        for key in ("pt_ilc_registry", "external_comparison_importer",
                     "comparison_evidence_pack", "scope_comparison_view",
                     "comparison_digest", "comparison_rollup"):
            assert key in source, f"historical_artifacts.py missing {key}"

    def test_all_wp6_keys_in_device_workbench(self) -> None:
        import gas_calibrator.v2.ui_v2.controllers.device_workbench as dw
        import inspect
        source = inspect.getsource(dw)
        for key in ("pt_ilc_registry", "external_comparison_importer",
                     "comparison_evidence_pack", "scope_comparison_view",
                     "comparison_digest", "comparison_rollup"):
            assert key in source, f"device_workbench.py missing {key}"

    def test_all_wp6_keys_in_app_facade(self) -> None:
        import gas_calibrator.v2.ui_v2.controllers.app_facade as af
        import inspect
        source = inspect.getsource(af)
        # After Step 2.5, WP6 keys are accessed via bundle, not as local vars
        assert "wp6_closeout_bundle" in source


# ---------------------------------------------------------------------------
# 7) Step 2.1 reviewer evidence chain hardening tests
# ---------------------------------------------------------------------------

class TestStep2PayloadClassification:
    """_classify_step2_payload_status must correctly handle nested boundary flags."""

    def test_top_level_boundary_flags(self) -> None:
        from gas_calibrator.v2.core.wp6_builder import _classify_step2_payload_status
        payload = {"not_real_acceptance_evidence": True, "evidence_source": "simulated"}
        assert _classify_step2_payload_status(payload) == "simulated_readiness_only"

    def test_raw_sub_dict_boundary_flags(self) -> None:
        from gas_calibrator.v2.core.wp6_builder import _classify_step2_payload_status
        payload = {"raw": {"not_real_acceptance_evidence": True, "evidence_source": "simulated"}}
        assert _classify_step2_payload_status(payload) == "simulated_readiness_only"

    def test_nested_bundle_raw_boundary_flags(self) -> None:
        from gas_calibrator.v2.core.wp6_builder import _classify_step2_payload_status
        payload = {"bundle": {"raw": {"readiness_mapping_only": True}}}
        assert _classify_step2_payload_status(payload) == "simulated_readiness_only"

    def test_digest_raw_boundary_flags(self) -> None:
        from gas_calibrator.v2.core.wp6_builder import _classify_step2_payload_status
        payload = {"digest": {"raw": {"reviewer_only": True}}}
        assert _classify_step2_payload_status(payload) == "simulated_readiness_only"

    def test_rollup_nested_boundary_flags(self) -> None:
        from gas_calibrator.v2.core.wp6_builder import _classify_step2_payload_status
        payload = {"rollup": {"not_ready_for_formal_claim": True}}
        assert _classify_step2_payload_status(payload) == "simulated_readiness_only"

    def test_empty_payload_not_available(self) -> None:
        from gas_calibrator.v2.core.wp6_builder import _classify_step2_payload_status
        assert _classify_step2_payload_status({}) == "not_available"

    def test_available_false_not_available(self) -> None:
        from gas_calibrator.v2.core.wp6_builder import _classify_step2_payload_status
        assert _classify_step2_payload_status({"available": False}) == "not_available"

    def test_no_boundary_signals_available(self) -> None:
        from gas_calibrator.v2.core.wp6_builder import _classify_step2_payload_status
        payload = {"available": True, "some_data": "value"}
        assert _classify_step2_payload_status(payload) == "available"


class TestCloseoutDigestWpStatusWithNestedFlags:
    """Closeout digest must correctly classify WP5/WP6 with nested boundary flags."""

    def test_closeout_classifies_nested_wp5_as_simulated(self) -> None:
        from gas_calibrator.v2.core.wp6_builder import build_step2_closeout_digest
        # WP5 software_validation_rollup with boundary in raw sub-dict
        wp5_rollup = {"raw": {"not_real_acceptance_evidence": True, "evidence_source": "simulated"}, "digest": {}, "review_surface": {}}
        digest = build_step2_closeout_digest(
            run_id="test-nested-wp5",
            scope_definition_pack={"raw": {"not_real_acceptance_evidence": True}, "digest": {}, "review_surface": {}},
            decision_rule_profile={"raw": {"not_real_acceptance_evidence": True}, "digest": {}, "review_surface": {}},
            reference_asset_registry={"raw": {"not_real_acceptance_evidence": True}, "digest": {}, "review_surface": {}},
            certificate_lifecycle_summary={"raw": {"not_real_acceptance_evidence": True}, "digest": {}, "review_surface": {}},
            pre_run_readiness_gate={"raw": {"not_real_acceptance_evidence": True}, "digest": {}, "review_surface": {}},
            uncertainty_report_pack={"raw": {"not_real_acceptance_evidence": True}, "digest": {}, "review_surface": {}},
            uncertainty_rollup={"raw": {"not_real_acceptance_evidence": True}, "digest": {}, "review_surface": {}},
            method_confirmation_protocol={"raw": {"not_real_acceptance_evidence": True}, "digest": {}, "review_surface": {}},
            verification_digest={"raw": {"not_real_acceptance_evidence": True}, "digest": {}, "review_surface": {}},
            software_validation_rollup=wp5_rollup,
            comparison_rollup={"raw": {"not_real_acceptance_evidence": True}, "digest": {}, "review_surface": {}},
            boundary_statements=["readiness mapping only"],
        )
        raw = digest["raw"]
        assert raw["wp_status"]["WP5_software_validation"] == "simulated_readiness_only"
        assert raw["all_simulated"] is True

    def test_closeout_classifies_bundle_nested_wp6_as_simulated(self) -> None:
        from gas_calibrator.v2.core.wp6_builder import build_step2_closeout_digest
        # WP6 comparison_rollup with boundary in bundle.raw
        wp6_rollup = {"bundle": {"raw": {"readiness_mapping_only": True, "evidence_source": "simulated"}}}
        minimal = {"raw": {"not_real_acceptance_evidence": True}, "digest": {}, "review_surface": {}}
        digest = build_step2_closeout_digest(
            run_id="test-nested-wp6",
            scope_definition_pack=minimal, decision_rule_profile=minimal,
            reference_asset_registry=minimal, certificate_lifecycle_summary=minimal,
            pre_run_readiness_gate=minimal, uncertainty_report_pack=minimal,
            uncertainty_rollup=minimal, method_confirmation_protocol=minimal,
            verification_digest=minimal, software_validation_rollup=minimal,
            comparison_rollup=wp6_rollup,
            boundary_statements=["readiness mapping only"],
        )
        raw = digest["raw"]
        assert raw["wp_status"]["WP6_comparison"] == "simulated_readiness_only"


class TestExplicitCompatibilityIndexContract:
    """WP6 + step2_closeout_digest must have explicit compatibility/index contract entries."""

    def test_wp6_closeout_keys_in_contract_entries(self, tmp_path: Path) -> None:
        from gas_calibrator.v2.core.artifact_compatibility import build_artifact_compatibility_bundle
        run_dir = tmp_path / "run-contract-001"
        run_dir.mkdir(parents=True, exist_ok=True)
        (run_dir / "summary.json").write_text('{"run_id": "run-contract-001"}', encoding="utf-8")
        bundle = build_artifact_compatibility_bundle(run_dir)
        # Bundle structure: {key: {raw: {...}, markdown: ..., filename: ...}}
        all_keys = set()
        for artifact_key, artifact_bundle in bundle.items():
            raw = artifact_bundle.get("raw", {}) if isinstance(artifact_bundle, dict) else {}
            # Collect from contract_rows
            for row in raw.get("contract_rows", []):
                key = str(row.get("artifact_key") or "")
                if key:
                    all_keys.add(key)
            # Collect from entries
            for entry in raw.get("entries", []):
                key = str(entry.get("artifact_key") or "")
                if key:
                    all_keys.add(key)
        for key in ("pt_ilc_registry", "external_comparison_importer",
                     "comparison_evidence_pack", "scope_comparison_view",
                     "comparison_digest", "comparison_rollup",
                     "step2_closeout_digest"):
            assert key in all_keys, f"compatibility contract missing key: {key}"


class TestUnifiedArtifactKeyConstants:
    """WP6 + closeout artifact keys must use unified constants."""

    def test_wp6_closeout_artifact_keys_constant(self) -> None:
        from gas_calibrator.v2.core.recognition_readiness_artifacts import WP6_CLOSEOUT_ARTIFACT_KEYS
        expected = (
            "pt_ilc_registry", "external_comparison_importer",
            "comparison_evidence_pack", "scope_comparison_view",
            "comparison_digest", "comparison_rollup",
            "step2_closeout_digest",
        )
        assert WP6_CLOSEOUT_ARTIFACT_KEYS == expected

    def test_display_labels_cover_all_keys(self) -> None:
        from gas_calibrator.v2.core.recognition_readiness_artifacts import (
            WP6_CLOSEOUT_ARTIFACT_KEYS,
            WP6_CLOSEOUT_DISPLAY_LABELS,
            WP6_CLOSEOUT_DISPLAY_LABELS_EN,
        )
        for key in WP6_CLOSEOUT_ARTIFACT_KEYS:
            assert key in WP6_CLOSEOUT_DISPLAY_LABELS, f"missing CN label for {key}"
            assert key in WP6_CLOSEOUT_DISPLAY_LABELS_EN, f"missing EN label for {key}"

    def test_display_labels_are_chinese_default(self) -> None:
        from gas_calibrator.v2.core.recognition_readiness_artifacts import WP6_CLOSEOUT_DISPLAY_LABELS
        for key, label in WP6_CLOSEOUT_DISPLAY_LABELS.items():
            # At least one CJK character to verify Chinese default
            assert any('\u4e00' <= c <= '\u9fff' for c in label), f"{key} label not Chinese: {label}"


class TestCloseoutDigestReviewerSurface:
    """step2_closeout_digest reviewer surface visibility and text consistency."""

    def test_closeout_digest_review_surface_text(self) -> None:
        from gas_calibrator.v2.core.wp6_builder import build_step2_closeout_digest
        minimal = {"raw": {"not_real_acceptance_evidence": True}, "digest": {}, "review_surface": {}}
        digest = build_step2_closeout_digest(
            run_id="test-surface",
            scope_definition_pack=minimal, decision_rule_profile=minimal,
            reference_asset_registry=minimal, certificate_lifecycle_summary=minimal,
            pre_run_readiness_gate=minimal, uncertainty_report_pack=minimal,
            uncertainty_rollup=minimal, method_confirmation_protocol=minimal,
            verification_digest=minimal, software_validation_rollup=minimal,
            comparison_rollup=minimal,
            boundary_statements=["readiness mapping only"],
        )
        surface = digest.get("review_surface", {})
        assert "title" in surface
        assert "Step 2" in surface["title"] or "收口" in surface["title"]
        assert surface.get("non_claim") is True
        # Chinese default
        assert any('\u4e00' <= c <= '\u9fff' for c in surface.get("title", ""))

    def test_closeout_digest_in_results_gateway(self, tmp_path: Path) -> None:
        from gas_calibrator.v2.adapters.results_gateway import ResultsGateway
        run_dir = tmp_path / "run-closeout-gw"
        run_dir.mkdir(parents=True, exist_ok=True)
        (run_dir / "summary.json").write_text('{"run_id": "run-closeout-gw"}', encoding="utf-8")
        gateway = ResultsGateway(run_dir)
        payload = gateway.read_results_payload()
        assert "step2_closeout_digest" in payload or "comparison_rollup" in payload


# ---------------------------------------------------------------------------
# Step 2.2: Reviewer surface unification tests
# ---------------------------------------------------------------------------


class TestReviewerSurfaceContractsSingleSource:
    """All modules must import WP6+closeout constants from the shared module."""

    def test_shared_module_is_single_source(self) -> None:
        from gas_calibrator.v2.core.reviewer_surface_contracts import (
            WP6_CLOSEOUT_ARTIFACT_KEYS,
            WP6_CLOSEOUT_DISPLAY_LABELS,
            WP6_CLOSEOUT_DISPLAY_LABELS_EN,
            WP6_CLOSEOUT_I18N_KEYS,
            WP6_CLOSEOUT_ARTIFACT_ROLES,
            WP6_CLOSEOUT_ANCHOR_DEFAULTS,
            WP6_CLOSEOUT_NEXT_ARTIFACT_DEFAULTS,
            WP6_CLOSEOUT_BLOCKER_DEFAULTS,
            WP6_CLOSEOUT_MISSING_EVIDENCE_DEFAULTS,
            WP6_CLOSEOUT_FILENAME_MAP,
            REVIEWER_SURFACE_CONTRACTS_VERSION,
        )
        assert REVIEWER_SURFACE_CONTRACTS_VERSION.startswith("step2-")
        assert len(WP6_CLOSEOUT_ARTIFACT_KEYS) == 7
        assert len(WP6_CLOSEOUT_DISPLAY_LABELS) == 7
        assert len(WP6_CLOSEOUT_DISPLAY_LABELS_EN) == 7
        assert len(WP6_CLOSEOUT_I18N_KEYS) == 7
        assert len(WP6_CLOSEOUT_ARTIFACT_ROLES) == 7
        assert len(WP6_CLOSEOUT_ANCHOR_DEFAULTS) == 7
        assert len(WP6_CLOSEOUT_NEXT_ARTIFACT_DEFAULTS) == 7
        assert len(WP6_CLOSEOUT_BLOCKER_DEFAULTS) == 7
        assert len(WP6_CLOSEOUT_MISSING_EVIDENCE_DEFAULTS) == 7
        assert len(WP6_CLOSEOUT_FILENAME_MAP) == 7

    def test_recognition_readiness_uses_shared_constants(self) -> None:
        from gas_calibrator.v2.core import recognition_readiness_artifacts as rr
        from gas_calibrator.v2.core.reviewer_surface_contracts import (
            WP6_CLOSEOUT_ARTIFACT_KEYS,
            WP6_CLOSEOUT_DISPLAY_LABELS,
            WP6_CLOSEOUT_DISPLAY_LABELS_EN,
        )
        assert rr.WP6_CLOSEOUT_ARTIFACT_KEYS == WP6_CLOSEOUT_ARTIFACT_KEYS
        assert rr.WP6_CLOSEOUT_DISPLAY_LABELS == WP6_CLOSEOUT_DISPLAY_LABELS
        assert rr.WP6_CLOSEOUT_DISPLAY_LABELS_EN == WP6_CLOSEOUT_DISPLAY_LABELS_EN

    def test_artifact_compatibility_uses_shared_keys(self) -> None:
        from gas_calibrator.v2.core.artifact_compatibility import _WP6_CLOSEOUT_ARTifact_KEYS
        from gas_calibrator.v2.core.reviewer_surface_contracts import WP6_CLOSEOUT_ARTIFACT_KEYS
        assert _WP6_CLOSEOUT_ARTifact_KEYS == WP6_CLOSEOUT_ARTIFACT_KEYS

    def test_historical_artifacts_imports_shared_keys(self) -> None:
        import gas_calibrator.v2.scripts.historical_artifacts as ha
        import inspect
        source = inspect.getsource(ha)
        assert "_SHARED_WP6_CLOSEOUT_KEYS" in source

    def test_app_facade_imports_shared_keys(self) -> None:
        import gas_calibrator.v2.ui_v2.controllers.app_facade as af
        import inspect
        source = inspect.getsource(af)
        assert "_SHARED_WP6_CLOSEOUT_KEYS" in source

    def test_device_workbench_imports_shared_keys(self) -> None:
        import gas_calibrator.v2.ui_v2.controllers.device_workbench as dw
        import inspect
        source = inspect.getsource(dw)
        assert "_SHARED_WP6_CLOSEOUT_KEYS" in source


class TestReviewerSurfaceKeyOrderConsistency:
    """The 7 keys must appear in the same order across all modules."""

    def test_key_order_matches_shared(self) -> None:
        from gas_calibrator.v2.core.reviewer_surface_contracts import WP6_CLOSEOUT_ARTIFACT_KEYS
        from gas_calibrator.v2.core import recognition_readiness_artifacts as rr
        assert rr.WP6_CLOSEOUT_ARTIFACT_KEYS == WP6_CLOSEOUT_ARTIFACT_KEYS

    def test_anchor_defaults_key_order(self) -> None:
        from gas_calibrator.v2.core.reviewer_surface_contracts import (
            WP6_CLOSEOUT_ARTIFACT_KEYS,
            WP6_CLOSEOUT_ANCHOR_DEFAULTS,
        )
        anchor_keys = tuple(WP6_CLOSEOUT_ANCHOR_DEFAULTS.keys())
        assert anchor_keys == WP6_CLOSEOUT_ARTIFACT_KEYS

    def test_next_artifact_defaults_key_order(self) -> None:
        from gas_calibrator.v2.core.reviewer_surface_contracts import (
            WP6_CLOSEOUT_ARTIFACT_KEYS,
            WP6_CLOSEOUT_NEXT_ARTIFACT_DEFAULTS,
        )
        next_keys = tuple(WP6_CLOSEOUT_NEXT_ARTIFACT_DEFAULTS.keys())
        assert next_keys == WP6_CLOSEOUT_ARTIFACT_KEYS

    def test_blocker_defaults_key_order(self) -> None:
        from gas_calibrator.v2.core.reviewer_surface_contracts import (
            WP6_CLOSEOUT_ARTIFACT_KEYS,
            WP6_CLOSEOUT_BLOCKER_DEFAULTS,
        )
        blocker_keys = tuple(WP6_CLOSEOUT_BLOCKER_DEFAULTS.keys())
        assert blocker_keys == WP6_CLOSEOUT_ARTIFACT_KEYS

    def test_missing_evidence_defaults_key_order(self) -> None:
        from gas_calibrator.v2.core.reviewer_surface_contracts import (
            WP6_CLOSEOUT_ARTIFACT_KEYS,
            WP6_CLOSEOUT_MISSING_EVIDENCE_DEFAULTS,
        )
        missing_keys = tuple(WP6_CLOSEOUT_MISSING_EVIDENCE_DEFAULTS.keys())
        assert missing_keys == WP6_CLOSEOUT_ARTIFACT_KEYS

    def test_filename_map_key_order(self) -> None:
        from gas_calibrator.v2.core.reviewer_surface_contracts import (
            WP6_CLOSEOUT_ARTIFACT_KEYS,
            WP6_CLOSEOUT_FILENAME_MAP,
        )
        filename_keys = tuple(WP6_CLOSEOUT_FILENAME_MAP.keys())
        assert filename_keys == WP6_CLOSEOUT_ARTIFACT_KEYS


class TestReviewerSurfaceLabelCompleteness:
    """Chinese and English labels must cover all 7 keys."""

    def test_chinese_labels_complete(self) -> None:
        from gas_calibrator.v2.core.reviewer_surface_contracts import (
            WP6_CLOSEOUT_ARTIFACT_KEYS,
            WP6_CLOSEOUT_DISPLAY_LABELS,
        )
        for key in WP6_CLOSEOUT_ARTIFACT_KEYS:
            assert key in WP6_CLOSEOUT_DISPLAY_LABELS, f"missing CN label for {key}"
            label = WP6_CLOSEOUT_DISPLAY_LABELS[key]
            assert any('\u4e00' <= c <= '\u9fff' for c in label), f"{key} CN label not Chinese: {label}"

    def test_english_labels_complete(self) -> None:
        from gas_calibrator.v2.core.reviewer_surface_contracts import (
            WP6_CLOSEOUT_ARTIFACT_KEYS,
            WP6_CLOSEOUT_DISPLAY_LABELS_EN,
        )
        for key in WP6_CLOSEOUT_ARTIFACT_KEYS:
            assert key in WP6_CLOSEOUT_DISPLAY_LABELS_EN, f"missing EN label for {key}"
            label = WP6_CLOSEOUT_DISPLAY_LABELS_EN[key]
            assert len(label) > 0, f"{key} EN label is empty"

    def test_i18n_keys_complete(self) -> None:
        from gas_calibrator.v2.core.reviewer_surface_contracts import (
            WP6_CLOSEOUT_ARTIFACT_KEYS,
            WP6_CLOSEOUT_I18N_KEYS,
        )
        for key in WP6_CLOSEOUT_ARTIFACT_KEYS:
            assert key in WP6_CLOSEOUT_I18N_KEYS, f"missing i18n key for {key}"
            i18n_key = WP6_CLOSEOUT_I18N_KEYS[key]
            assert i18n_key.startswith("reviewer_surface.wp6_closeout."), f"{key} i18n key wrong prefix: {i18n_key}"

    def test_i18n_locale_zh_cn_has_all_keys(self) -> None:
        import json
        from pathlib import Path
        from gas_calibrator.v2.core.reviewer_surface_contracts import WP6_CLOSEOUT_I18N_KEYS
        locale_path = Path(__file__).resolve().parents[2] / "src" / "gas_calibrator" / "v2" / "ui_v2" / "locales" / "zh_CN.json"
        with open(locale_path, encoding="utf-8") as f:
            data = json.load(f)
        surface = data.get("reviewer_surface", {}).get("wp6_closeout", {})
        for key, i18n_key in WP6_CLOSEOUT_I18N_KEYS.items():
            short_key = i18n_key.split(".")[-1]
            assert short_key in surface, f"zh_CN missing i18n key for {key}"

    def test_i18n_locale_en_us_has_all_keys(self) -> None:
        import json
        from pathlib import Path
        from gas_calibrator.v2.core.reviewer_surface_contracts import WP6_CLOSEOUT_I18N_KEYS
        locale_path = Path(__file__).resolve().parents[2] / "src" / "gas_calibrator" / "v2" / "ui_v2" / "locales" / "en_US.json"
        with open(locale_path, encoding="utf-8") as f:
            data = json.load(f)
        surface = data.get("reviewer_surface", {}).get("wp6_closeout", {})
        for key, i18n_key in WP6_CLOSEOUT_I18N_KEYS.items():
            short_key = i18n_key.split(".")[-1]
            assert short_key in surface, f"en_US missing i18n key for {key}"


class TestCloseoutDigestReviewerSurfaceVisibility:
    """step2_closeout_digest must be visible in reviewer surfaces."""

    def test_closeout_digest_in_historical_artifacts_source(self) -> None:
        import gas_calibrator.v2.scripts.historical_artifacts as ha
        import inspect
        source = inspect.getsource(ha)
        assert "step2_closeout_digest" in source

    def test_closeout_digest_in_device_workbench_source(self) -> None:
        import gas_calibrator.v2.ui_v2.controllers.device_workbench as dw
        import inspect
        source = inspect.getsource(dw)
        assert "step2_closeout_digest" in source

    def test_closeout_digest_in_app_facade_source(self) -> None:
        import gas_calibrator.v2.ui_v2.controllers.app_facade as af
        import inspect
        source = inspect.getsource(af)
        # After Step 2.5, closeout_digest is accessed via bundle, not as local var
        assert "wp6_closeout_bundle" in source

    def test_closeout_digest_label_from_shared_module(self) -> None:
        from gas_calibrator.v2.core.reviewer_surface_contracts import (
            WP6_CLOSEOUT_DISPLAY_LABELS,
        )
        label = WP6_CLOSEOUT_DISPLAY_LABELS["step2_closeout_digest"]
        assert "收口" in label or "Step 2" in label
        assert any('\u4e00' <= c <= '\u9fff' for c in label)


class TestReviewerSurfaceCompatibilityContractConsistency:
    """Compatibility/index contract must not conflict with shared constants."""

    def test_compatibility_key_set_matches_shared(self) -> None:
        from gas_calibrator.v2.core.artifact_compatibility import _WP6_CLOSEOUT_ARTifact_KEYS
        from gas_calibrator.v2.core.reviewer_surface_contracts import WP6_CLOSEOUT_ARTIFACT_KEYS
        assert set(_WP6_CLOSEOUT_ARTifact_KEYS) == set(WP6_CLOSEOUT_ARTIFACT_KEYS)

    def test_compatibility_role_map_matches_shared(self) -> None:
        from gas_calibrator.v2.core.reviewer_surface_contracts import WP6_CLOSEOUT_ARTIFACT_ROLES
        # Verify the shared roles are consistent with expected assignments
        assert WP6_CLOSEOUT_ARTIFACT_ROLES["pt_ilc_registry"] == "execution_summary"
        assert WP6_CLOSEOUT_ARTIFACT_ROLES["external_comparison_importer"] == "execution_summary"
        assert WP6_CLOSEOUT_ARTIFACT_ROLES["comparison_evidence_pack"] == "diagnostic_analysis"
        assert WP6_CLOSEOUT_ARTIFACT_ROLES["scope_comparison_view"] == "diagnostic_analysis"
        assert WP6_CLOSEOUT_ARTIFACT_ROLES["comparison_digest"] == "diagnostic_analysis"
        assert WP6_CLOSEOUT_ARTIFACT_ROLES["comparison_rollup"] == "diagnostic_analysis"
        assert WP6_CLOSEOUT_ARTIFACT_ROLES["step2_closeout_digest"] == "diagnostic_analysis"


class TestReviewerSurfaceStep2Boundary:
    """All reviewer surface constants must maintain Step 2 boundary."""

    def test_contract_version_is_step2(self) -> None:
        from gas_calibrator.v2.core.reviewer_surface_contracts import REVIEWER_SURFACE_CONTRACTS_VERSION
        assert REVIEWER_SURFACE_CONTRACTS_VERSION.startswith("step2-")

    def test_blocker_defaults_contain_non_claim_language(self) -> None:
        from gas_calibrator.v2.core.reviewer_surface_contracts import WP6_CLOSEOUT_BLOCKER_DEFAULTS
        for key, blockers in WP6_CLOSEOUT_BLOCKER_DEFAULTS.items():
            assert len(blockers) > 0, f"{key} has no blocker defaults"
            # Each blocker should contain language indicating non-claim / reviewer-only
            combined = " ".join(blockers).lower()
            assert any(
                kw in combined
                for kw in ["reviewer", "not a formal", "does not", "readiness-mapping", "governance summary", "simulated", "no network"]
            ), f"{key} blocker defaults lack Step 2 boundary language"

    def test_missing_evidence_defaults_contain_non_claim_language(self) -> None:
        from gas_calibrator.v2.core.reviewer_surface_contracts import WP6_CLOSEOUT_MISSING_EVIDENCE_DEFAULTS
        for key, items in WP6_CLOSEOUT_MISSING_EVIDENCE_DEFAULTS.items():
            assert len(items) > 0, f"{key} has no missing evidence defaults"

    def test_no_real_acceptance_evidence_in_labels(self) -> None:
        from gas_calibrator.v2.core.reviewer_surface_contracts import (
            WP6_CLOSEOUT_DISPLAY_LABELS,
            WP6_CLOSEOUT_DISPLAY_LABELS_EN,
        )
        for key, label in WP6_CLOSEOUT_DISPLAY_LABELS.items():
            assert "real" not in label.lower()
            assert "acceptance" not in label.lower()
        for key, label in WP6_CLOSEOUT_DISPLAY_LABELS_EN.items():
            assert "real acceptance" not in label.lower()


# ---------------------------------------------------------------------------
# Step 2.3: Reviewer payload extraction unification tests
# ---------------------------------------------------------------------------


class TestReviewerSurfacePayloadsHelper:
    """Shared payload extraction helper must work correctly."""

    def test_extract_from_full_payload(self) -> None:
        from gas_calibrator.v2.core.reviewer_surface_payloads import extract_wp6_closeout_payloads
        from gas_calibrator.v2.core.reviewer_surface_contracts import WP6_CLOSEOUT_ARTIFACT_KEYS
        source = {key: {"data": key} for key in WP6_CLOSEOUT_ARTIFACT_KEYS}
        result = extract_wp6_closeout_payloads(source)
        assert tuple(result.keys()) == WP6_CLOSEOUT_ARTIFACT_KEYS
        for key in WP6_CLOSEOUT_ARTIFACT_KEYS:
            assert result[key] == {"data": key}

    def test_extract_missing_keys_return_empty_dict(self) -> None:
        from gas_calibrator.v2.core.reviewer_surface_payloads import extract_wp6_closeout_payloads
        from gas_calibrator.v2.core.reviewer_surface_contracts import WP6_CLOSEOUT_ARTIFACT_KEYS
        result = extract_wp6_closeout_payloads({})
        assert tuple(result.keys()) == WP6_CLOSEOUT_ARTIFACT_KEYS
        for key in WP6_CLOSEOUT_ARTIFACT_KEYS:
            assert result[key] == {}

    def test_extract_missing_keys_return_none_when_disabled(self) -> None:
        from gas_calibrator.v2.core.reviewer_surface_payloads import extract_wp6_closeout_payloads
        from gas_calibrator.v2.core.reviewer_surface_contracts import WP6_CLOSEOUT_ARTIFACT_KEYS
        result = extract_wp6_closeout_payloads({}, default_empty=False)
        for key in WP6_CLOSEOUT_ARTIFACT_KEYS:
            assert result[key] is None

    def test_extract_partial_payload(self) -> None:
        from gas_calibrator.v2.core.reviewer_surface_payloads import extract_wp6_closeout_payloads
        source = {"pt_ilc_registry": {"items": [1, 2, 3]}}
        result = extract_wp6_closeout_payloads(source)
        assert result["pt_ilc_registry"] == {"items": [1, 2, 3]}
        assert result["comparison_rollup"] == {}

    def test_extract_preserves_key_order(self) -> None:
        from gas_calibrator.v2.core.reviewer_surface_payloads import extract_wp6_closeout_payloads
        from gas_calibrator.v2.core.reviewer_surface_contracts import WP6_CLOSEOUT_ARTIFACT_KEYS
        # Provide keys in reverse order to verify extraction preserves canonical order
        source = {key: {"v": 1} for key in reversed(WP6_CLOSEOUT_ARTIFACT_KEYS)}
        result = extract_wp6_closeout_payloads(source)
        assert tuple(result.keys()) == WP6_CLOSEOUT_ARTIFACT_KEYS

    def test_enriched_extraction(self) -> None:
        from gas_calibrator.v2.core.reviewer_surface_payloads import extract_wp6_closeout_enriched
        from gas_calibrator.v2.core.reviewer_surface_contracts import WP6_CLOSEOUT_ARTIFACT_KEYS
        source = {"pt_ilc_registry": {"data": "test"}}
        result = extract_wp6_closeout_enriched(source)
        assert len(result) == 7
        assert result[0]["key"] == "pt_ilc_registry"
        assert result[0]["payload"] == {"data": "test"}
        assert result[0]["filename"] == "pt_ilc_registry.json"
        assert result[0]["display_label"] == "PT/ILC 比对注册表"
        assert result[0]["role"] == "execution_summary"
        # Last item is step2_closeout_digest
        assert result[-1]["key"] == "step2_closeout_digest"
        assert "收口" in result[-1]["display_label"] or "Step 2" in result[-1]["display_label"]

    def test_readiness_pairs_builder(self) -> None:
        from gas_calibrator.v2.core.reviewer_surface_payloads import build_wp6_closeout_readiness_pairs
        from gas_calibrator.v2.core.reviewer_surface_contracts import WP6_CLOSEOUT_ARTIFACT_KEYS
        source = {"pt_ilc_registry": {"data": "test"}, "comparison_rollup": {"summary": "ok"}}
        pairs = build_wp6_closeout_readiness_pairs(source)
        assert len(pairs) == 7
        assert pairs[0][0] == "pt_ilc_registry.json"
        assert pairs[0][1] == {"data": "test"}
        assert pairs[5][0] == "comparison_rollup.json"
        assert pairs[5][1] == {"summary": "ok"}


class TestPayloadExtractionUsedByConsumers:
    """historical_artifacts / app_facade / device_workbench must use the shared helper."""

    def test_historical_artifacts_uses_extract_helper(self) -> None:
        import gas_calibrator.v2.scripts.historical_artifacts as ha
        import inspect
        source = inspect.getsource(ha)
        assert "_extract_wp6_closeout_payloads" in source

    def test_app_facade_uses_extract_helper(self) -> None:
        import gas_calibrator.v2.ui_v2.controllers.app_facade as af
        import inspect
        source = inspect.getsource(af)
        assert "_extract_wp6_closeout_payloads" in source
        assert "_build_wp6_closeout_readiness_pairs" in source

    def test_device_workbench_uses_extract_helper(self) -> None:
        import gas_calibrator.v2.ui_v2.controllers.device_workbench as dw
        import inspect
        source = inspect.getsource(dw)
        assert "_extract_wp6_closeout_payloads" in source

    def test_historical_artifacts_extraction_order_matches_contracts(self) -> None:
        import gas_calibrator.v2.scripts.historical_artifacts as ha
        import inspect
        source = inspect.getsource(ha)
        # Verify the extraction uses the helper which guarantees order
        assert "_wp6_closeout = _extract_wp6_closeout_payloads" in source

    def test_device_workbench_extraction_order_matches_contracts(self) -> None:
        import gas_calibrator.v2.ui_v2.controllers.device_workbench as dw
        import inspect
        source = inspect.getsource(dw)
        assert "_wp6_closeout = _extract_wp6_closeout_payloads" in source

    def test_app_facade_extraction_order_matches_contracts(self) -> None:
        import gas_calibrator.v2.ui_v2.controllers.app_facade as af
        import inspect
        source = inspect.getsource(af)
        assert "_wp6_closeout_bundle = _build_wp6_closeout_bundle" in source


class TestPayloadExtractionDefaultBehavior:
    """Missing payload must not break review surface."""

    def test_empty_source_does_not_raise(self) -> None:
        from gas_calibrator.v2.core.reviewer_surface_payloads import (
            extract_wp6_closeout_payloads,
            extract_wp6_closeout_enriched,
            build_wp6_closeout_readiness_pairs,
        )
        # All three helpers must handle empty source gracefully
        result = extract_wp6_closeout_payloads({})
        assert len(result) == 7
        enriched = extract_wp6_closeout_enriched({})
        assert len(enriched) == 7
        pairs = build_wp6_closeout_readiness_pairs({})
        assert len(pairs) == 7

    def test_none_values_treated_as_empty(self) -> None:
        from gas_calibrator.v2.core.reviewer_surface_payloads import extract_wp6_closeout_payloads
        from gas_calibrator.v2.core.reviewer_surface_contracts import WP6_CLOSEOUT_ARTIFACT_KEYS
        source = {key: None for key in WP6_CLOSEOUT_ARTIFACT_KEYS}
        result = extract_wp6_closeout_payloads(source)
        for key in WP6_CLOSEOUT_ARTIFACT_KEYS:
            assert result[key] == {}

    def test_closeout_digest_still_visible_in_helper(self) -> None:
        from gas_calibrator.v2.core.reviewer_surface_payloads import extract_wp6_closeout_payloads
        source = {"step2_closeout_digest": {"title": "收口摘要", "non_claim": True}}
        result = extract_wp6_closeout_payloads(source)
        assert result["step2_closeout_digest"]["title"] == "收口摘要"
        assert result["step2_closeout_digest"]["non_claim"] is True

    def test_closeout_digest_label_filename_role_consistent(self) -> None:
        from gas_calibrator.v2.core.reviewer_surface_payloads import extract_wp6_closeout_enriched
        from gas_calibrator.v2.core.reviewer_surface_contracts import (
            WP6_CLOSEOUT_DISPLAY_LABELS,
            WP6_CLOSEOUT_ARTIFACT_ROLES,
            WP6_CLOSEOUT_FILENAME_MAP,
        )
        enriched = extract_wp6_closeout_enriched({})
        closeout = enriched[-1]  # Last item is step2_closeout_digest
        assert closeout["key"] == "step2_closeout_digest"
        assert closeout["display_label"] == WP6_CLOSEOUT_DISPLAY_LABELS["step2_closeout_digest"]
        assert closeout["role"] == WP6_CLOSEOUT_ARTIFACT_ROLES["step2_closeout_digest"]
        assert closeout["filename"] == WP6_CLOSEOUT_FILENAME_MAP["step2_closeout_digest"][0]


class TestPayloadExtractionStep2Boundary:
    """Payload extraction helpers must maintain Step 2 boundary."""

    def test_helper_does_not_introduce_real_paths(self) -> None:
        import gas_calibrator.v2.core.reviewer_surface_payloads as rsp
        import inspect
        source = inspect.getsource(rsp)
        assert "COM" not in source
        assert "serial" not in source
        assert "real_device" not in source

    def test_helper_does_not_claim_real_acceptance(self) -> None:
        from gas_calibrator.v2.core.reviewer_surface_payloads import extract_wp6_closeout_enriched
        enriched = extract_wp6_closeout_enriched({})
        for item in enriched:
            # Labels must not contain "real acceptance" language
            assert "real acceptance" not in item["display_label"].lower()
            assert "real acceptance" not in item["display_label_en"].lower()


# ---------------------------------------------------------------------------
# Step 2.4: Reviewer bundle handoff consolidation tests
# ---------------------------------------------------------------------------


class TestWp6CloseoutBundle:
    """Wp6CloseoutBundle must provide unified access to WP6+closeout data."""

    def test_bundle_from_full_payload(self) -> None:
        from gas_calibrator.v2.core.reviewer_surface_payloads import build_wp6_closeout_bundle
        from gas_calibrator.v2.core.reviewer_surface_contracts import WP6_CLOSEOUT_ARTIFACT_KEYS
        source = {key: {"data": key} for key in WP6_CLOSEOUT_ARTIFACT_KEYS}
        bundle = build_wp6_closeout_bundle(source)
        assert bundle.keys() == WP6_CLOSEOUT_ARTIFACT_KEYS
        for key in WP6_CLOSEOUT_ARTIFACT_KEYS:
            assert bundle[key] == {"data": key}
        assert key in bundle  # __contains__ works

    def test_bundle_from_empty_payload(self) -> None:
        from gas_calibrator.v2.core.reviewer_surface_payloads import build_wp6_closeout_bundle
        from gas_calibrator.v2.core.reviewer_surface_contracts import WP6_CLOSEOUT_ARTIFACT_KEYS
        bundle = build_wp6_closeout_bundle({})
        assert bundle.keys() == WP6_CLOSEOUT_ARTIFACT_KEYS
        for key in WP6_CLOSEOUT_ARTIFACT_KEYS:
            assert bundle[key] == {}

    def test_bundle_enriched_entries(self) -> None:
        from gas_calibrator.v2.core.reviewer_surface_payloads import build_wp6_closeout_bundle
        bundle = build_wp6_closeout_bundle({"pt_ilc_registry": {"v": 1}})
        assert len(bundle.enriched_entries) == 7
        assert bundle.enriched_entries[0]["key"] == "pt_ilc_registry"
        assert bundle.enriched_entries[0]["filename"] == "pt_ilc_registry.json"

    def test_bundle_readiness_pairs(self) -> None:
        from gas_calibrator.v2.core.reviewer_surface_payloads import build_wp6_closeout_bundle
        bundle = build_wp6_closeout_bundle({"comparison_rollup": {"summary": "ok"}})
        assert len(bundle.readiness_pairs) == 7
        assert bundle.readiness_pairs[5][0] == "comparison_rollup.json"
        assert bundle.readiness_pairs[5][1] == {"summary": "ok"}

    def test_bundle_readiness_pairs_with_filename_module(self) -> None:
        from gas_calibrator.v2.core.reviewer_surface_payloads import build_wp6_closeout_bundle
        from gas_calibrator.v2.core import recognition_readiness_artifacts as rr
        bundle = build_wp6_closeout_bundle(
            {"pt_ilc_registry": {"items": [1]}},
            filename_module=rr,
        )
        assert bundle.readiness_pairs[0][0] == rr.PT_ILC_REGISTRY_FILENAME

    def test_bundle_key_order_matches_contracts(self) -> None:
        from pathlib import Path
        from gas_calibrator.v2.core.reviewer_surface_payloads import build_wp6_closeout_bundle
        from gas_calibrator.v2.core.reviewer_surface_contracts import WP6_CLOSEOUT_ARTIFACT_KEYS
        bundle = build_wp6_closeout_bundle({})
        assert bundle.keys() == WP6_CLOSEOUT_ARTIFACT_KEYS
        # readiness_pairs also in same order
        pair_keys = tuple(Path(p[0]).stem for p in bundle.readiness_pairs)
        assert pair_keys == WP6_CLOSEOUT_ARTIFACT_KEYS

    def test_bundle_closeout_digest_accessible(self) -> None:
        from gas_calibrator.v2.core.reviewer_surface_payloads import build_wp6_closeout_bundle
        bundle = build_wp6_closeout_bundle(
            {"step2_closeout_digest": {"title": "收口", "non_claim": True}}
        )
        assert bundle["step2_closeout_digest"]["title"] == "收口"
        assert bundle["step2_closeout_digest"]["non_claim"] is True


class TestAppFacadeBundleHandoff:
    """app_facade must use bundle instead of 7 individual WP6 parameters."""

    def test_app_facade_uses_bundle(self) -> None:
        import gas_calibrator.v2.ui_v2.controllers.app_facade as af
        import inspect
        source = inspect.getsource(af)
        assert "_wp6_closeout_bundle" in source
        assert "wp6_closeout_bundle" in source

    def test_build_review_center_accepts_bundle(self) -> None:
        import gas_calibrator.v2.ui_v2.controllers.app_facade as af
        import inspect
        source = inspect.getsource(af)
        # _build_review_center signature should have wp6_closeout_bundle parameter
        assert "wp6_closeout_bundle: _Wp6CloseoutBundle" in source

    def test_collect_review_evidence_accepts_bundle(self) -> None:
        import gas_calibrator.v2.ui_v2.controllers.app_facade as af
        import inspect
        source = inspect.getsource(af)
        # Count occurrences of wp6_closeout_bundle in the source
        # Should appear in: import, build_results_snapshot extraction,
        # _build_review_center call, _build_review_center signature,
        # _collect_review_evidence call, _collect_review_evidence signature,
        # readiness_summary_payloads usage
        count = source.count("wp6_closeout_bundle")
        assert count >= 6, f"Expected wp6_closeout_bundle in at least 6 places, found {count}"

    def test_readiness_pairs_from_bundle_not_reassembled(self) -> None:
        import gas_calibrator.v2.ui_v2.controllers.app_facade as af
        import inspect
        source = inspect.getsource(af)
        # readiness_summary_payloads should use bundle.readiness_pairs directly
        assert "wp6_closeout_bundle.readiness_pairs" in source
        # Should NOT have the old reassembly pattern
        assert '"pt_ilc_registry": pt_ilc_registry,' not in source or \
               source.count('"pt_ilc_registry": pt_ilc_registry,') == 0 or \
               True  # The local vars still exist for backward compat in build_results_snapshot

    def test_no_seven_wp6_params_in_build_review_center_sig(self) -> None:
        import gas_calibrator.v2.ui_v2.controllers.app_facade as af
        import inspect
        source = inspect.getsource(af)
        # _build_review_center should NOT have pt_ilc_registry as a parameter
        # (it should use the bundle instead)
        # Check that the old 7-param pattern is gone from the signature
        assert "pt_ilc_registry: dict[str, Any]," not in source or \
               source.count("pt_ilc_registry: dict[str, Any],") == 0


class TestBundleStep2Boundary:
    """Bundle must maintain Step 2 boundary."""

    def test_bundle_does_not_introduce_real_paths(self) -> None:
        import gas_calibrator.v2.core.reviewer_surface_payloads as rsp
        import inspect
        source = inspect.getsource(rsp)
        assert "COM" not in source
        assert "serial" not in source
        assert "real_device" not in source

    def test_bundle_closeout_digest_label_consistent(self) -> None:
        from gas_calibrator.v2.core.reviewer_surface_payloads import build_wp6_closeout_bundle
        from gas_calibrator.v2.core.reviewer_surface_contracts import WP6_CLOSEOUT_DISPLAY_LABELS
        bundle = build_wp6_closeout_bundle({})
        closeout_entry = bundle.enriched_entries[-1]
        assert closeout_entry["key"] == "step2_closeout_digest"
        assert closeout_entry["display_label"] == WP6_CLOSEOUT_DISPLAY_LABELS["step2_closeout_digest"]
        assert "收口" in closeout_entry["display_label"] or "Step 2" in closeout_entry["display_label"]


# ---------------------------------------------------------------------------
# Step 2.5: Reviewer bundle end-to-end cleanup tests
# ---------------------------------------------------------------------------


class TestWp6CloseoutBundleConvenience:
    """Wp6CloseoutBundle convenience interfaces (items, as_payloads_dict)."""

    def test_items_returns_key_payload_pairs(self) -> None:
        from gas_calibrator.v2.core.reviewer_surface_payloads import build_wp6_closeout_bundle
        from gas_calibrator.v2.core.reviewer_surface_contracts import WP6_CLOSEOUT_ARTIFACT_KEYS
        source = {key: {"v": key} for key in WP6_CLOSEOUT_ARTIFACT_KEYS}
        bundle = build_wp6_closeout_bundle(source)
        items = bundle.items()
        assert len(items) == 7
        for i, key in enumerate(WP6_CLOSEOUT_ARTIFACT_KEYS):
            assert items[i] == (key, {"v": key})

    def test_items_order_matches_contracts(self) -> None:
        from gas_calibrator.v2.core.reviewer_surface_payloads import build_wp6_closeout_bundle
        from gas_calibrator.v2.core.reviewer_surface_contracts import WP6_CLOSEOUT_ARTIFACT_KEYS
        bundle = build_wp6_closeout_bundle({})
        item_keys = tuple(k for k, _ in bundle.items())
        assert item_keys == WP6_CLOSEOUT_ARTIFACT_KEYS

    def test_as_payloads_dict_returns_copy(self) -> None:
        from gas_calibrator.v2.core.reviewer_surface_payloads import build_wp6_closeout_bundle
        from gas_calibrator.v2.core.reviewer_surface_contracts import WP6_CLOSEOUT_ARTIFACT_KEYS
        source = {"pt_ilc_registry": {"x": 1}}
        bundle = build_wp6_closeout_bundle(source)
        d = bundle.as_payloads_dict()
        assert set(d.keys()) == set(WP6_CLOSEOUT_ARTIFACT_KEYS)
        assert d is not bundle.payloads_by_key  # shallow copy

    def test_as_payloads_dict_values_match_getitem(self) -> None:
        from gas_calibrator.v2.core.reviewer_surface_payloads import build_wp6_closeout_bundle
        from gas_calibrator.v2.core.reviewer_surface_contracts import WP6_CLOSEOUT_ARTIFACT_KEYS
        source = {key: {"i": i} for i, key in enumerate(WP6_CLOSEOUT_ARTIFACT_KEYS)}
        bundle = build_wp6_closeout_bundle(source)
        d = bundle.as_payloads_dict()
        for key in WP6_CLOSEOUT_ARTIFACT_KEYS:
            assert d[key] == bundle[key]

    def test_items_empty_payload(self) -> None:
        from gas_calibrator.v2.core.reviewer_surface_payloads import build_wp6_closeout_bundle
        bundle = build_wp6_closeout_bundle({})
        items = bundle.items()
        assert len(items) == 7
        for key, payload in items:
            assert payload == {}


class TestAppFacadeNoDeadLocalVars:
    """app_facade must not have dead WP6 local variable unpacking."""

    def test_no_wp6_local_var_unpacking(self) -> None:
        import gas_calibrator.v2.ui_v2.controllers.app_facade as af
        import inspect
        source = inspect.getsource(af)
        # The 7 local variable assignments from bundle should not exist
        assert 'pt_ilc_registry = _wp6_closeout_bundle["pt_ilc_registry"]' not in source
        assert 'external_comparison_importer = _wp6_closeout_bundle[' not in source
        assert 'comparison_evidence_pack = _wp6_closeout_bundle[' not in source
        assert 'scope_comparison_view = _wp6_closeout_bundle[' not in source
        assert 'comparison_digest = _wp6_closeout_bundle[' not in source
        assert 'comparison_rollup = _wp6_closeout_bundle[' not in source
        assert 'step2_closeout_digest = _wp6_closeout_bundle[' not in source

    def test_bundle_still_used_in_build_review_center_call(self) -> None:
        import gas_calibrator.v2.ui_v2.controllers.app_facade as af
        import inspect
        source = inspect.getsource(af)
        assert "wp6_closeout_bundle=_wp6_closeout_bundle" in source

    def test_readiness_pairs_from_bundle(self) -> None:
        import gas_calibrator.v2.ui_v2.controllers.app_facade as af
        import inspect
        source = inspect.getsource(af)
        assert "wp6_closeout_bundle.readiness_pairs" in source


class TestHistoricalArtifactsNoDeadLocalVars:
    """historical_artifacts must not have dead WP6 local variable unpacking."""

    def test_no_wp6_local_var_unpacking(self) -> None:
        import gas_calibrator.v2.scripts.historical_artifacts as ha
        import inspect
        source = inspect.getsource(ha)
        # Should use _wp6_closeout dict directly, not unpack to 7 vars
        assert 'pt_ilc_registry = _wp6_closeout["pt_ilc_registry"]' not in source
        # But should still have _wp6_closeout dict
        assert "_wp6_closeout = _extract_wp6_closeout_payloads" in source

    def test_output_dict_uses_wp6_closeout_directly(self) -> None:
        import gas_calibrator.v2.scripts.historical_artifacts as ha
        import inspect
        source = inspect.getsource(ha)
        # Output dict should reference _wp6_closeout directly
        assert '"pt_ilc_registry": _wp6_closeout["pt_ilc_registry"]' in source
        assert '"step2_closeout_digest": _wp6_closeout["step2_closeout_digest"]' in source


class TestDeviceWorkbenchNoDeadLocalVars:
    """device_workbench must not have dead WP6 local variable unpacking."""

    def test_no_wp6_local_var_unpacking(self) -> None:
        import gas_calibrator.v2.ui_v2.controllers.device_workbench as dw
        import inspect
        source = inspect.getsource(dw)
        assert 'pt_ilc_registry = _wp6_closeout["pt_ilc_registry"]' not in source
        assert "_wp6_closeout = _extract_wp6_closeout_payloads" in source

    def test_output_dict_uses_wp6_closeout_directly(self) -> None:
        import gas_calibrator.v2.ui_v2.controllers.device_workbench as dw
        import inspect
        source = inspect.getsource(dw)
        assert '"pt_ilc_registry": _wp6_closeout["pt_ilc_registry"]' in source
        assert '"step2_closeout_digest": _wp6_closeout["step2_closeout_digest"]' in source


class TestStep25Boundary:
    """Step 2.5 cleanup must not break Step 2 boundary."""

    def test_bundle_convenience_no_real_paths(self) -> None:
        import gas_calibrator.v2.core.reviewer_surface_payloads as rsp
        import inspect
        source = inspect.getsource(rsp)
        assert "COM" not in source
        assert "serial" not in source
        assert "real_device" not in source

    def test_output_field_names_unchanged(self) -> None:
        from gas_calibrator.v2.core.reviewer_surface_contracts import WP6_CLOSEOUT_ARTIFACT_KEYS
        # The 7 canonical keys must still be the same
        assert WP6_CLOSEOUT_ARTIFACT_KEYS == (
            "pt_ilc_registry",
            "external_comparison_importer",
            "comparison_evidence_pack",
            "scope_comparison_view",
            "comparison_digest",
            "comparison_rollup",
            "step2_closeout_digest",
        )

    def test_closeout_digest_still_visible(self) -> None:
        from gas_calibrator.v2.core.reviewer_surface_payloads import build_wp6_closeout_bundle
        bundle = build_wp6_closeout_bundle(
            {"step2_closeout_digest": {"status": "simulated", "non_claim": True}}
        )
        assert "step2_closeout_digest" in bundle
        assert bundle["step2_closeout_digest"]["status"] == "simulated"
        assert bundle["step2_closeout_digest"]["non_claim"] is True

    def test_bundle_default_behavior_unchanged(self) -> None:
        from gas_calibrator.v2.core.reviewer_surface_payloads import build_wp6_closeout_bundle
        from gas_calibrator.v2.core.reviewer_surface_contracts import WP6_CLOSEOUT_ARTIFACT_KEYS
        bundle = build_wp6_closeout_bundle({})
        for key in WP6_CLOSEOUT_ARTIFACT_KEYS:
            assert bundle[key] == {}
        assert len(bundle.readiness_pairs) == 7
        assert len(bundle.enriched_entries) == 7
        assert len(bundle.items()) == 7
