from __future__ import annotations

from pathlib import Path
from typing import Any, Protocol

from .step2_closeout_bundle_builder import (
    STEP2_CLOSEOUT_BUNDLE_FILENAME,
    STEP2_CLOSEOUT_EVIDENCE_INDEX_FILENAME,
    STEP2_CLOSEOUT_SUMMARY_FILENAME,
    build_step2_closeout_bundle,
)


STEP2_CLOSEOUT_REPOSITORY_SCHEMA_VERSION = "step2-closeout-repository-v1"
STEP2_CLOSEOUT_REPOSITORY_MODE = "file_artifact_first"
STEP2_CLOSEOUT_GATEWAY_MODE = "file_backed_default"


class Step2CloseoutRepository(Protocol):
    def load_snapshot(self) -> dict[str, Any]:
        """Return Step 2 closeout bundle payloads."""


class FileBackedStep2CloseoutRepository:
    def __init__(
        self,
        run_dir: Path,
        *,
        run_id: str = "",
        scope_definition_pack: dict[str, Any] | None = None,
        decision_rule_profile: dict[str, Any] | None = None,
        conformity_statement_profile: dict[str, Any] | None = None,
        reference_asset_registry: dict[str, Any] | None = None,
        certificate_lifecycle_summary: dict[str, Any] | None = None,
        pre_run_readiness_gate: dict[str, Any] | None = None,
        uncertainty_report_pack: dict[str, Any] | None = None,
        uncertainty_rollup: dict[str, Any] | None = None,
        method_confirmation_protocol: dict[str, Any] | None = None,
        verification_rollup: dict[str, Any] | None = None,
        software_validation_traceability_matrix: dict[str, Any] | None = None,
        requirement_design_code_test_links: dict[str, Any] | None = None,
        validation_evidence_index: dict[str, Any] | None = None,
        change_impact_summary: dict[str, Any] | None = None,
        rollback_readiness_summary: dict[str, Any] | None = None,
        release_manifest: dict[str, Any] | None = None,
        release_scope_summary: dict[str, Any] | None = None,
        release_boundary_digest: dict[str, Any] | None = None,
        release_evidence_pack_index: dict[str, Any] | None = None,
        release_validation_manifest: dict[str, Any] | None = None,
        software_validation_rollup: dict[str, Any] | None = None,
        audit_readiness_digest: dict[str, Any] | None = None,
        comparison_evidence_pack: dict[str, Any] | None = None,
        scope_comparison_view: dict[str, Any] | None = None,
        comparison_digest: dict[str, Any] | None = None,
        comparison_rollup: dict[str, Any] | None = None,
        step2_closeout_digest: dict[str, Any] | None = None,
        sidecar_index_summary: dict[str, Any] | None = None,
        review_copilot_payload: dict[str, Any] | None = None,
        model_governance_summary: dict[str, Any] | None = None,
        run_metadata_profile: dict[str, Any] | None = None,
        operator_authorization_profile: dict[str, Any] | None = None,
        training_record: dict[str, Any] | None = None,
        sop_version_binding: dict[str, Any] | None = None,
        qc_flag_catalog: dict[str, Any] | None = None,
        recovery_action_log: dict[str, Any] | None = None,
        reviewer_dual_check_placeholder: dict[str, Any] | None = None,
    ) -> None:
        self.run_dir = Path(run_dir)
        self.builder_kwargs = {
            "run_id": str(run_id or ""),
            "run_dir": str(self.run_dir),
            "scope_definition_pack": dict(scope_definition_pack or {}),
            "decision_rule_profile": dict(decision_rule_profile or {}),
            "conformity_statement_profile": dict(conformity_statement_profile or {}),
            "reference_asset_registry": dict(reference_asset_registry or {}),
            "certificate_lifecycle_summary": dict(certificate_lifecycle_summary or {}),
            "pre_run_readiness_gate": dict(pre_run_readiness_gate or {}),
            "uncertainty_report_pack": dict(uncertainty_report_pack or {}),
            "uncertainty_rollup": dict(uncertainty_rollup or {}),
            "method_confirmation_protocol": dict(method_confirmation_protocol or {}),
            "verification_rollup": dict(verification_rollup or {}),
            "software_validation_traceability_matrix": dict(software_validation_traceability_matrix or {}),
            "requirement_design_code_test_links": dict(requirement_design_code_test_links or {}),
            "validation_evidence_index": dict(validation_evidence_index or {}),
            "change_impact_summary": dict(change_impact_summary or {}),
            "rollback_readiness_summary": dict(rollback_readiness_summary or {}),
            "release_manifest": dict(release_manifest or {}),
            "release_scope_summary": dict(release_scope_summary or {}),
            "release_boundary_digest": dict(release_boundary_digest or {}),
            "release_evidence_pack_index": dict(release_evidence_pack_index or {}),
            "release_validation_manifest": dict(release_validation_manifest or {}),
            "software_validation_rollup": dict(software_validation_rollup or {}),
            "audit_readiness_digest": dict(audit_readiness_digest or {}),
            "comparison_evidence_pack": dict(comparison_evidence_pack or {}),
            "scope_comparison_view": dict(scope_comparison_view or {}),
            "comparison_digest": dict(comparison_digest or {}),
            "comparison_rollup": dict(comparison_rollup or {}),
            "step2_closeout_digest": dict(step2_closeout_digest or {}),
            "sidecar_index_summary": dict(sidecar_index_summary or {}),
            "review_copilot_payload": dict(review_copilot_payload or {}),
            "model_governance_summary": dict(model_governance_summary or {}),
            "run_metadata_profile": dict(run_metadata_profile or {}),
            "operator_authorization_profile": dict(operator_authorization_profile or {}),
            "training_record": dict(training_record or {}),
            "sop_version_binding": dict(sop_version_binding or {}),
            "qc_flag_catalog": dict(qc_flag_catalog or {}),
            "recovery_action_log": dict(recovery_action_log or {}),
            "reviewer_dual_check_placeholder": dict(reviewer_dual_check_placeholder or {}),
        }

    def load_snapshot(self) -> dict[str, Any]:
        built = build_step2_closeout_bundle(**self.builder_kwargs)
        bundle = {
            **dict(built.get("step2_closeout_bundle") or {}),
            **self._load_json(STEP2_CLOSEOUT_BUNDLE_FILENAME),
        }
        evidence_index = {
            **dict(built.get("step2_closeout_evidence_index") or {}),
            **self._load_json(STEP2_CLOSEOUT_EVIDENCE_INDEX_FILENAME),
        }
        summary_markdown = self._load_markdown(STEP2_CLOSEOUT_SUMMARY_FILENAME) or str(
            built.get("step2_closeout_summary_markdown") or ""
        )
        compact_section = {
            **dict(built.get("step2_closeout_compact_section") or {}),
            **dict(bundle.get("compact_section") or {}),
        }
        bundle.setdefault("schema_version", STEP2_CLOSEOUT_REPOSITORY_SCHEMA_VERSION)
        bundle["repository_mode"] = STEP2_CLOSEOUT_REPOSITORY_MODE
        bundle["gateway_mode"] = STEP2_CLOSEOUT_GATEWAY_MODE
        bundle["artifact_present_on_disk"] = bool((self.run_dir / STEP2_CLOSEOUT_BUNDLE_FILENAME).exists())
        bundle["summary_markdown_filename"] = STEP2_CLOSEOUT_SUMMARY_FILENAME
        bundle["file_artifact_first_preserved"] = True
        bundle["main_chain_dependency"] = False
        evidence_index.setdefault("schema_version", STEP2_CLOSEOUT_REPOSITORY_SCHEMA_VERSION)
        evidence_index["repository_mode"] = STEP2_CLOSEOUT_REPOSITORY_MODE
        evidence_index["gateway_mode"] = STEP2_CLOSEOUT_GATEWAY_MODE
        evidence_index["artifact_present_on_disk"] = bool((self.run_dir / STEP2_CLOSEOUT_EVIDENCE_INDEX_FILENAME).exists())
        evidence_index["file_artifact_first_preserved"] = True
        evidence_index["main_chain_dependency"] = False
        compact_section["repository_mode"] = STEP2_CLOSEOUT_REPOSITORY_MODE
        compact_section["gateway_mode"] = STEP2_CLOSEOUT_GATEWAY_MODE
        compact_section["file_artifact_first_preserved"] = True
        compact_section["main_chain_dependency"] = False
        return {
            "step2_closeout_bundle": bundle,
            "step2_closeout_digest": dict(self.builder_kwargs.get("step2_closeout_digest") or {}),
            "step2_closeout_evidence_index": evidence_index,
            "step2_closeout_summary_markdown": summary_markdown,
            "step2_closeout_compact_section": compact_section,
        }

    def _load_json(self, filename: str) -> dict[str, Any]:
        path = self.run_dir / filename
        if not path.exists():
            return {}
        try:
            import json

            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return {}
        return dict(payload) if isinstance(payload, dict) else {}

    def _load_markdown(self, filename: str) -> str:
        path = self.run_dir / filename
        if not path.exists():
            return ""
        try:
            return path.read_text(encoding="utf-8")
        except Exception:
            return ""
