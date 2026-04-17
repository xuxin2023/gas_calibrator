from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Protocol

from . import recognition_readiness_artifacts as recognition_readiness
from .method_confirmation_repository import FileBackedMethodConfirmationRepository
from .recognition_scope_repository import FileBackedRecognitionScopeRepository
from .software_validation_builder import build_software_validation_wp5_artifacts
from .uncertainty_repository import FileBackedUncertaintyRepository

SOFTWARE_VALIDATION_REPOSITORY_SCHEMA_VERSION = "step2-software-validation-repository-v1"
SOFTWARE_VALIDATION_REPOSITORY_MODE = "file_artifact_first"
SOFTWARE_VALIDATION_GATEWAY_MODE = "file_backed_default"
SOFTWARE_VALIDATION_DB_READY_MODE = "db_ready_stub"
SOFTWARE_VALIDATION_REPOSITORY_TOOL = "gas_calibrator.v2.adapters.software_validation_gateway"

_LINKED_SURFACES = ["results", "review_center", "workbench", "historical_artifacts"]
_ARTIFACT_KEYS = (
    "software_validation_traceability_matrix",
    "requirement_design_code_test_links",
    "validation_evidence_index",
    "change_impact_summary",
    "rollback_readiness_summary",
    "artifact_hash_registry",
    "audit_event_store",
    "environment_fingerprint",
    "config_fingerprint",
    "release_input_digest",
    "release_manifest",
    "release_scope_summary",
    "release_boundary_digest",
    "release_evidence_pack_index",
    "release_validation_manifest",
    "audit_readiness_digest",
)


class SoftwareValidationRepository(Protocol):
    def load_snapshot(self) -> dict[str, Any]:
        """Return Step 2 reviewer-facing software validation payloads."""


class DatabaseReadySoftwareValidationRepositoryStub:
    def __init__(self, run_dir: Path) -> None:
        self.run_dir = Path(run_dir)

    def load_snapshot(self) -> dict[str, Any]:
        empty = {key: {} for key in _ARTIFACT_KEYS}
        return {
            **empty,
            "software_validation_rollup": {
                "schema_version": SOFTWARE_VALIDATION_REPOSITORY_SCHEMA_VERSION,
                "run_dir": str(self.run_dir),
                "repository_mode": SOFTWARE_VALIDATION_DB_READY_MODE,
                "gateway_mode": "not_active",
                "db_ready_stub": {
                    "enabled": False,
                    "mode": SOFTWARE_VALIDATION_DB_READY_MODE,
                    "default_path": False,
                    "requires_explicit_injection": True,
                    "not_in_default_chain": True,
                },
                "summary_lines": ["DB-ready software validation stub reserved; Step 2 default remains file-backed."],
                "detail_lines": ["No database connection is opened or required in the current software validation path."],
                "primary_evidence_rewritten": False,
                "not_real_acceptance_evidence": True,
            },
        }


class FileBackedSoftwareValidationRepository:
    def __init__(
        self,
        run_dir: Path,
        *,
        summary: dict[str, Any] | None = None,
        analytics_summary: dict[str, Any] | None = None,
        evidence_registry: dict[str, Any] | None = None,
        workbench_action_report: dict[str, Any] | None = None,
        workbench_action_snapshot: dict[str, Any] | None = None,
        scope_readiness_summary: dict[str, Any] | None = None,
        compatibility_scan_summary: dict[str, Any] | None = None,
    ) -> None:
        self.run_dir = Path(run_dir)
        self.summary = dict(summary or {})
        self.analytics_summary = dict(analytics_summary or {})
        self.evidence_registry = dict(evidence_registry or {})
        self.workbench_action_report = dict(workbench_action_report or {})
        self.workbench_action_snapshot = dict(workbench_action_snapshot or {})
        self.scope_readiness_summary = dict(scope_readiness_summary or {})
        self.compatibility_scan_summary = dict(compatibility_scan_summary or {})

    def load_snapshot(self) -> dict[str, Any]:
        recognition_snapshot = FileBackedRecognitionScopeRepository(
            self.run_dir,
            summary=self.summary,
            analytics_summary=self.analytics_summary,
            evidence_registry=self.evidence_registry,
            workbench_action_report=self.workbench_action_report,
            workbench_action_snapshot=self.workbench_action_snapshot,
            scope_readiness_summary=self.scope_readiness_summary,
            compatibility_scan_summary=self.compatibility_scan_summary,
        ).load_snapshot()
        uncertainty_snapshot = FileBackedUncertaintyRepository(
            self.run_dir,
            summary=self.summary,
            analytics_summary=self.analytics_summary,
            evidence_registry=self.evidence_registry,
            workbench_action_report=self.workbench_action_report,
            workbench_action_snapshot=self.workbench_action_snapshot,
            scope_readiness_summary=self.scope_readiness_summary,
            compatibility_scan_summary=self.compatibility_scan_summary,
        ).load_snapshot()
        method_snapshot = FileBackedMethodConfirmationRepository(
            self.run_dir,
            summary=self.summary,
            analytics_summary=self.analytics_summary,
            evidence_registry=self.evidence_registry,
            workbench_action_report=self.workbench_action_report,
            workbench_action_snapshot=self.workbench_action_snapshot,
            scope_readiness_summary=self.scope_readiness_summary,
            compatibility_scan_summary=self.compatibility_scan_summary,
        ).load_snapshot()
        fallback = build_software_validation_wp5_artifacts(
            run_id=str(self.summary.get("run_id") or self.run_dir.name),
            run_dir=self.run_dir,
            path_map=self._artifact_paths(),
            filenames=self._artifact_filenames(),
            boundary_statements=list(recognition_readiness.RECOGNITION_READINESS_BOUNDARY_STATEMENTS),
            scope_definition_pack=self._as_bundle(recognition_snapshot.get("scope_definition_pack")),
            decision_rule_profile=self._as_bundle(recognition_snapshot.get("decision_rule_profile")),
            reference_asset_registry=self._as_bundle(recognition_snapshot.get("reference_asset_registry")),
            certificate_lifecycle_summary=self._as_bundle(recognition_snapshot.get("certificate_lifecycle_summary")),
            pre_run_readiness_gate=self._as_bundle(recognition_snapshot.get("pre_run_readiness_gate")),
            uncertainty_report_pack=self._as_bundle(uncertainty_snapshot.get("uncertainty_report_pack")),
            uncertainty_rollup=self._as_bundle(uncertainty_snapshot.get("uncertainty_rollup")),
            method_confirmation_protocol=self._as_bundle(method_snapshot.get("method_confirmation_protocol")),
            route_specific_validation_matrix=self._as_bundle(method_snapshot.get("route_specific_validation_matrix")),
            validation_run_set=self._as_bundle(method_snapshot.get("validation_run_set")),
            verification_digest=self._as_bundle(method_snapshot.get("verification_digest")),
            verification_rollup=self._as_bundle(method_snapshot.get("verification_rollup")),
            version_payload=dict(self.summary.get("versions") or {}),
            lineage_payload=self._load_json("lineage_summary.json"),
            analytics_payload=self.analytics_summary or self._load_json("analytics_summary.json"),
        )
        snapshot: dict[str, Any] = {}
        for artifact_key in _ARTIFACT_KEYS:
            snapshot[artifact_key] = self._load_payload(
                artifact_key,
                self._artifact_filenames()[artifact_key],
                fallback_bundle=dict(fallback.get(artifact_key) or {}),
            )
        snapshot["software_validation_rollup"] = self._build_rollup(
            traceability_payload=dict(snapshot.get("software_validation_traceability_matrix") or {}),
            change_impact_payload=dict(snapshot.get("change_impact_summary") or {}),
            rollback_payload=dict(snapshot.get("rollback_readiness_summary") or {}),
            hash_registry_payload=dict(snapshot.get("artifact_hash_registry") or {}),
            audit_event_payload=dict(snapshot.get("audit_event_store") or {}),
            environment_payload=dict(snapshot.get("environment_fingerprint") or {}),
            config_payload=dict(snapshot.get("config_fingerprint") or {}),
            release_input_payload=dict(snapshot.get("release_input_digest") or {}),
            release_manifest_payload=dict(snapshot.get("release_manifest") or {}),
            audit_payload=dict(snapshot.get("audit_readiness_digest") or {}),
        )
        return snapshot

    @staticmethod
    def _as_bundle(payload: Any) -> dict[str, Any]:
        raw = dict(payload or {})
        return {
            "raw": raw,
            "digest": dict(raw.get("digest") or {}),
            "review_surface": dict(raw.get("review_surface") or {}),
        }

    def _load_payload(self, artifact_key: str, filename: str, *, fallback_bundle: dict[str, Any]) -> dict[str, Any]:
        payload = self._load_json(filename) or self._read_summary_section(artifact_key)
        fallback_raw = dict(fallback_bundle.get("raw") or {})
        payload = {**fallback_raw, **dict(payload or {})}
        payload["artifact_type"] = str(payload.get("artifact_type") or artifact_key)
        payload["schema_version"] = str(payload.get("schema_version") or SOFTWARE_VALIDATION_REPOSITORY_SCHEMA_VERSION)
        payload["repository_mode"] = SOFTWARE_VALIDATION_REPOSITORY_MODE
        payload["gateway_mode"] = SOFTWARE_VALIDATION_GATEWAY_MODE
        payload["reviewer_only"] = bool(payload.get("reviewer_only", True))
        payload["readiness_mapping_only"] = bool(payload.get("readiness_mapping_only", True))
        payload["not_ready_for_formal_claim"] = bool(payload.get("not_ready_for_formal_claim", True))
        payload["not_real_acceptance_evidence"] = bool(payload.get("not_real_acceptance_evidence", True))
        payload["primary_evidence_rewritten"] = False
        payload["artifact_present_on_disk"] = bool((self.run_dir / filename).exists())
        payload["artifact_paths"] = {
            **dict(fallback_raw.get("artifact_paths") or {}),
            **dict(payload.get("artifact_paths") or {}),
        }
        payload["digest"] = {
            **dict(fallback_bundle.get("digest") or {}),
            **dict(fallback_raw.get("digest") or {}),
            **dict(payload.get("digest") or {}),
        }
        payload["review_surface"] = {
            **dict(fallback_raw.get("review_surface") or {}),
            **dict(payload.get("review_surface") or {}),
        }
        return payload

    def _build_rollup(
        self,
        *,
        traceability_payload: dict[str, Any],
        change_impact_payload: dict[str, Any],
        rollback_payload: dict[str, Any],
        hash_registry_payload: dict[str, Any],
        audit_event_payload: dict[str, Any],
        environment_payload: dict[str, Any],
        config_payload: dict[str, Any],
        release_input_payload: dict[str, Any],
        release_manifest_payload: dict[str, Any],
        audit_payload: dict[str, Any],
    ) -> dict[str, Any]:
        traceability_digest = dict(traceability_payload.get("digest") or {})
        change_impact_digest = dict(change_impact_payload.get("digest") or {})
        rollback_digest = dict(rollback_payload.get("digest") or {})
        hash_digest = dict(hash_registry_payload.get("digest") or {})
        audit_event_digest = dict(audit_event_payload.get("digest") or {})
        config_digest = dict(config_payload.get("digest") or {})
        release_input_digest = dict(release_input_payload.get("digest") or {})
        release_digest = dict(release_manifest_payload.get("digest") or {})
        audit_digest = dict(audit_payload.get("digest") or {})
        summary_lines = [
            str(traceability_digest.get("summary") or "software validation traceability"),
            str(change_impact_digest.get("summary") or "change impact"),
            str(rollback_digest.get("summary") or "rollback readiness"),
            str(hash_digest.get("summary") or "artifact hash registry"),
            str(config_digest.get("summary") or "config fingerprint"),
            str(release_input_digest.get("summary") or "release input digest"),
            str(release_digest.get("summary") or "release manifest"),
            str(audit_digest.get("summary") or "audit readiness"),
        ]
        linked_surface_visibility = [
            *list(_LINKED_SURFACES),
            *[
                str(item)
                for item in list(change_impact_payload.get("linked_surface_visibility") or [])
                if str(item).strip()
            ],
        ]
        linked_surface_visibility = list(dict.fromkeys(linked_surface_visibility))
        return {
            "schema_version": SOFTWARE_VALIDATION_REPOSITORY_SCHEMA_VERSION,
            "index_schema_version": SOFTWARE_VALIDATION_REPOSITORY_SCHEMA_VERSION,
            "run_dir": str(self.run_dir),
            "generated_at": str(
                release_manifest_payload.get("generated_at")
                or traceability_payload.get("generated_at")
                or datetime.now(timezone.utc).replace(microsecond=0).isoformat()
            ),
            "generated_by_tool": SOFTWARE_VALIDATION_REPOSITORY_TOOL,
            "repository_mode": SOFTWARE_VALIDATION_REPOSITORY_MODE,
            "gateway_mode": SOFTWARE_VALIDATION_GATEWAY_MODE,
            "db_ready_stub": {
                "enabled": False,
                "mode": SOFTWARE_VALIDATION_DB_READY_MODE,
                "default_path": False,
                "requires_explicit_injection": True,
                "not_in_default_chain": True,
            },
            "linked_surface_visibility": linked_surface_visibility,
            "traceability_summary": str(traceability_digest.get("summary") or "--"),
            "traceability_completeness_summary": str(
                traceability_payload.get("traceability_completeness")
                or traceability_digest.get("current_coverage_summary")
                or "--"
            ),
            "change_impact_summary": str(change_impact_digest.get("summary") or "--"),
            "changed_modules_summary": str(
                change_impact_payload.get("changed_modules_summary")
                or change_impact_digest.get("current_coverage_summary")
                or "--"
            ),
            "main_execution_chain_impacted": bool(change_impact_payload.get("impacts_main_execution_chain", False)),
            "main_execution_chain_impact_summary": str(
                change_impact_payload.get("main_execution_chain_impact_summary") or "--"
            ),
            "artifact_schema_impacted": bool(change_impact_payload.get("impacts_artifact_schema", False)),
            "artifact_schema_impact_summary": str(
                change_impact_payload.get("artifact_schema_impact_summary") or "--"
            ),
            "results_surface_impacted": bool(change_impact_payload.get("impacts_results_surface", False)),
            "review_center_surface_impacted": bool(change_impact_payload.get("impacts_review_center_surface", False)),
            "workbench_surface_impacted": bool(change_impact_payload.get("impacts_workbench_surface", False)),
            "rollback_summary": str(rollback_digest.get("summary") or "--"),
            "rollback_mode": str(rollback_payload.get("rollback_mode") or "--"),
            "rollback_scope_summary": str(
                rollback_payload.get("rollback_scope_summary")
                or rollback_digest.get("current_coverage_summary")
                or "--"
            ),
            "file_artifact_first": bool(rollback_payload.get("file_artifact_first", False)),
            "sidecar_revocable": bool(rollback_payload.get("sidecar_revocable", False)),
            "primary_evidence_preserved": bool(rollback_payload.get("primary_evidence_preserved", True)),
            "hash_registry_summary": str(hash_digest.get("summary") or "--"),
            "audit_event_summary": str(audit_event_digest.get("summary") or "--"),
            "environment_summary": str(environment_payload.get("environment_summary") or "--"),
            "config_fingerprint_summary": str(config_digest.get("summary") or "--"),
            "release_input_summary": str(release_input_digest.get("summary") or "--"),
            "release_manifest_summary": str(release_digest.get("summary") or "--"),
            "parity_status": str(release_manifest_payload.get("parity_status") or "--"),
            "resilience_status": str(release_manifest_payload.get("resilience_status") or "--"),
            "smoke_status": str(release_manifest_payload.get("smoke_status") or "--"),
            "reviewer_only": True,
            "readiness_mapping_only": True,
            "not_ready_for_formal_claim": True,
            "not_real_acceptance_evidence": True,
            "primary_evidence_rewritten": False,
            "rollup_summary_display": " | ".join(line for line in summary_lines if str(line).strip()),
            "summary_lines": summary_lines,
            "detail_lines": [
                f"repository/gateway: {SOFTWARE_VALIDATION_REPOSITORY_MODE} / {SOFTWARE_VALIDATION_GATEWAY_MODE}",
                f"environment: {str(environment_payload.get('environment_summary') or '--')}",
                f"change impact: {str(change_impact_digest.get('summary') or '--')}",
                f"rollback: {str(rollback_digest.get('summary') or '--')}",
                f"config / release input: {str(config_digest.get('summary') or '--')} | {str(release_input_digest.get('summary') or '--')}",
                "non-claim: reviewer-only / simulation-only / not real acceptance evidence",
                "primary evidence rewritten: false",
            ],
        }

    def _load_json(self, relative_name: str) -> dict[str, Any]:
        path = self.run_dir / relative_name
        if not path.exists():
            return {}
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return {}
        return dict(payload) if isinstance(payload, dict) else {}

    def _read_summary_section(self, key: str) -> dict[str, Any]:
        for payload in (
            self.summary,
            self.evidence_registry,
            self.analytics_summary,
            self.workbench_action_report,
            self.workbench_action_snapshot,
        ):
            stats = dict(payload.get("stats") or {})
            section = stats.get(key)
            if isinstance(section, dict) and section:
                return dict(section)
            direct = payload.get(key)
            if isinstance(direct, dict) and direct:
                return dict(direct)
        return {}

    def _artifact_paths(self) -> dict[str, str]:
        return {key: str(self.run_dir / filename) for key, filename in self._artifact_filenames().items()}

    @staticmethod
    def _artifact_filenames() -> dict[str, str]:
        return {
            "software_validation_traceability_matrix": recognition_readiness.SOFTWARE_VALIDATION_TRACEABILITY_MATRIX_FILENAME,
            "software_validation_traceability_matrix_markdown": recognition_readiness.SOFTWARE_VALIDATION_TRACEABILITY_MATRIX_MARKDOWN_FILENAME,
            "requirement_design_code_test_links": recognition_readiness.REQUIREMENT_DESIGN_CODE_TEST_LINKS_FILENAME,
            "requirement_design_code_test_links_markdown": recognition_readiness.REQUIREMENT_DESIGN_CODE_TEST_LINKS_MARKDOWN_FILENAME,
            "validation_evidence_index": recognition_readiness.VALIDATION_EVIDENCE_INDEX_FILENAME,
            "validation_evidence_index_markdown": recognition_readiness.VALIDATION_EVIDENCE_INDEX_MARKDOWN_FILENAME,
            "change_impact_summary": recognition_readiness.CHANGE_IMPACT_SUMMARY_FILENAME,
            "change_impact_summary_markdown": recognition_readiness.CHANGE_IMPACT_SUMMARY_MARKDOWN_FILENAME,
            "rollback_readiness_summary": recognition_readiness.ROLLBACK_READINESS_SUMMARY_FILENAME,
            "rollback_readiness_summary_markdown": recognition_readiness.ROLLBACK_READINESS_SUMMARY_MARKDOWN_FILENAME,
            "artifact_hash_registry": recognition_readiness.ARTIFACT_HASH_REGISTRY_FILENAME,
            "artifact_hash_registry_markdown": recognition_readiness.ARTIFACT_HASH_REGISTRY_MARKDOWN_FILENAME,
            "audit_event_store": recognition_readiness.AUDIT_EVENT_STORE_FILENAME,
            "audit_event_store_markdown": recognition_readiness.AUDIT_EVENT_STORE_MARKDOWN_FILENAME,
            "environment_fingerprint": recognition_readiness.ENVIRONMENT_FINGERPRINT_FILENAME,
            "environment_fingerprint_markdown": recognition_readiness.ENVIRONMENT_FINGERPRINT_MARKDOWN_FILENAME,
            "config_fingerprint": recognition_readiness.CONFIG_FINGERPRINT_FILENAME,
            "config_fingerprint_markdown": recognition_readiness.CONFIG_FINGERPRINT_MARKDOWN_FILENAME,
            "release_input_digest": recognition_readiness.RELEASE_INPUT_DIGEST_FILENAME,
            "release_input_digest_markdown": recognition_readiness.RELEASE_INPUT_DIGEST_MARKDOWN_FILENAME,
            "release_manifest": recognition_readiness.RELEASE_MANIFEST_FILENAME,
            "release_manifest_markdown": recognition_readiness.RELEASE_MANIFEST_MARKDOWN_FILENAME,
            "release_scope_summary": recognition_readiness.RELEASE_SCOPE_SUMMARY_FILENAME,
            "release_scope_summary_markdown": recognition_readiness.RELEASE_SCOPE_SUMMARY_MARKDOWN_FILENAME,
            "release_boundary_digest": recognition_readiness.RELEASE_BOUNDARY_DIGEST_FILENAME,
            "release_boundary_digest_markdown": recognition_readiness.RELEASE_BOUNDARY_DIGEST_MARKDOWN_FILENAME,
            "release_evidence_pack_index": recognition_readiness.RELEASE_EVIDENCE_PACK_INDEX_FILENAME,
            "release_evidence_pack_index_markdown": recognition_readiness.RELEASE_EVIDENCE_PACK_INDEX_MARKDOWN_FILENAME,
            "release_validation_manifest": recognition_readiness.RELEASE_VALIDATION_MANIFEST_FILENAME,
            "release_validation_manifest_markdown": recognition_readiness.RELEASE_VALIDATION_MANIFEST_MARKDOWN_FILENAME,
            "audit_readiness_digest": recognition_readiness.AUDIT_READINESS_DIGEST_FILENAME,
            "audit_readiness_digest_markdown": recognition_readiness.AUDIT_READINESS_DIGEST_MARKDOWN_FILENAME,
        }
