"""WP6 repository: PT/ILC + comparison evidence pack.

Step 2 only — file-artifact-first, reviewer-facing.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Protocol

from . import recognition_readiness_artifacts as recognition_readiness
from .recognition_scope_repository import FileBackedRecognitionScopeRepository
from .uncertainty_repository import FileBackedUncertaintyRepository
from .method_confirmation_repository import FileBackedMethodConfirmationRepository
from .software_validation_repository import FileBackedSoftwareValidationRepository
from .wp6_builder import build_wp6_artifacts

WP6_REPOSITORY_SCHEMA_VERSION = "step2-wp6-repository-v1"
WP6_REPOSITORY_MODE = "file_artifact_first"
WP6_GATEWAY_MODE = "file_backed_default"
WP6_DB_READY_MODE = "db_ready_stub"
WP6_REPOSITORY_TOOL = "gas_calibrator.v2.adapters.wp6_gateway"

_LINKED_SURFACES = ["results", "review_center", "workbench", "historical_artifacts"]
_ARTIFACT_KEYS = (
    "pt_ilc_registry",
    "external_comparison_importer",
    "comparison_evidence_pack",
    "scope_comparison_view",
    "comparison_digest",
    "comparison_rollup",
)


class Wp6Repository(Protocol):
    def load_snapshot(self) -> dict[str, Any]:
        """Return Step 2 reviewer-facing WP6 payloads."""


class DatabaseReadyWp6RepositoryStub:
    def __init__(self, run_dir: Path) -> None:
        self.run_dir = Path(run_dir)

    def load_snapshot(self) -> dict[str, Any]:
        empty = {key: {} for key in _ARTIFACT_KEYS}
        return {
            **empty,
            "comparison_rollup": {
                "schema_version": WP6_REPOSITORY_SCHEMA_VERSION,
                "run_dir": str(self.run_dir),
                "repository_mode": WP6_DB_READY_MODE,
                "gateway_mode": "not_active",
                "db_ready_stub": {
                    "enabled": False,
                    "mode": WP6_DB_READY_MODE,
                    "default_path": False,
                    "requires_explicit_injection": True,
                    "not_in_default_chain": True,
                },
                "summary_lines": ["DB-ready WP6 stub reserved; Step 2 default remains file-backed."],
                "detail_lines": ["No database connection is opened or required in the current WP6 path."],
                "primary_evidence_rewritten": False,
                "not_real_acceptance_evidence": True,
            },
        }


class FileBackedWp6Repository:
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
        # Load upstream WP snapshots
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
        mc_snapshot = FileBackedMethodConfirmationRepository(
            self.run_dir,
            summary=self.summary,
            analytics_summary=self.analytics_summary,
            evidence_registry=self.evidence_registry,
            workbench_action_report=self.workbench_action_report,
            workbench_action_snapshot=self.workbench_action_snapshot,
            scope_readiness_summary=self.scope_readiness_summary,
            compatibility_scan_summary=self.compatibility_scan_summary,
        ).load_snapshot()
        sv_snapshot = FileBackedSoftwareValidationRepository(
            self.run_dir,
            summary=self.summary,
            analytics_summary=self.analytics_summary,
            evidence_registry=self.evidence_registry,
            workbench_action_report=self.workbench_action_report,
            workbench_action_snapshot=self.workbench_action_snapshot,
            scope_readiness_summary=self.scope_readiness_summary,
            compatibility_scan_summary=self.compatibility_scan_summary,
        ).load_snapshot()

        # Build fallback artifacts from builder
        fallback_artifacts = build_wp6_artifacts(
            run_id=str(
                self.summary.get("run_id")
                or recognition_snapshot.get("scope_definition_pack", {}).get("run_id")
                or self.run_dir.name
            ),
            scope_definition_pack=self._as_bundle(recognition_snapshot.get("scope_definition_pack")),
            decision_rule_profile=self._as_bundle(recognition_snapshot.get("decision_rule_profile")),
            reference_asset_registry=self._as_bundle(recognition_snapshot.get("reference_asset_registry")),
            certificate_lifecycle_summary=self._as_bundle(recognition_snapshot.get("certificate_lifecycle_summary")),
            pre_run_readiness_gate=self._as_bundle(recognition_snapshot.get("pre_run_readiness_gate")),
            uncertainty_report_pack=self._as_bundle(uncertainty_snapshot.get("uncertainty_report_pack")),
            uncertainty_rollup=self._as_bundle(uncertainty_snapshot.get("uncertainty_rollup")),
            method_confirmation_protocol=self._as_bundle(mc_snapshot.get("method_confirmation_protocol")),
            verification_digest=self._as_bundle(mc_snapshot.get("verification_digest")),
            software_validation_rollup=self._as_bundle(sv_snapshot.get("software_validation_rollup")),
            path_map=self._artifact_paths(),
            filenames=self._artifact_filenames(),
            boundary_statements=[
                "readiness mapping only",
                "reviewer-only PT/ILC comparison skeleton",
                "not ready for formal claim",
                "not real acceptance evidence",
            ],
        )

        snapshot: dict[str, Any] = {}
        missing_artifacts: list[str] = []
        for artifact_key in _ARTIFACT_KEYS:
            payload = self._load_payload(
                artifact_key,
                self._artifact_filenames()[artifact_key],
                fallback_bundle=dict(fallback_artifacts.get(artifact_key) or {}),
            )
            if not bool(payload.get("artifact_present_on_disk", False)):
                missing_artifacts.append(artifact_key)
            snapshot[artifact_key] = payload

        snapshot["comparison_rollup"] = self._build_rollup(
            rollup_payload=dict(snapshot.get("comparison_rollup") or {}),
            comparison_evidence_pack=dict(snapshot.get("comparison_evidence_pack") or {}),
            comparison_digest=dict(snapshot.get("comparison_digest") or {}),
            missing_artifacts=missing_artifacts,
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
        payload["schema_version"] = str(payload.get("schema_version") or "1.0")
        payload["repository_mode"] = WP6_REPOSITORY_MODE
        payload["gateway_mode"] = WP6_GATEWAY_MODE
        payload["ready_for_readiness_mapping"] = bool(payload.get("ready_for_readiness_mapping", True))
        payload["not_ready_for_formal_claim"] = bool(payload.get("not_ready_for_formal_claim", True))
        payload["not_real_acceptance_evidence"] = bool(payload.get("not_real_acceptance_evidence", True))
        payload["primary_evidence_rewritten"] = False
        payload["not_in_default_chain"] = False
        payload["evidence_source"] = str(payload.get("evidence_source") or "simulated")
        payload["artifact_present_on_disk"] = bool((self.run_dir / filename).exists())
        payload["reviewer_placeholder"] = not payload["artifact_present_on_disk"]
        if payload["reviewer_placeholder"]:
            payload["placeholder_origin"] = "file_backed_repository_fallback"
            payload["reviewer_only"] = True
            payload["readiness_mapping_only"] = True
            payload["non_claim"] = True
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
        review_surface = dict(payload.get("review_surface") or {})
        review_surface["artifact_paths"] = {
            **dict(fallback_raw.get("artifact_paths") or {}),
            **dict(review_surface.get("artifact_paths") or {}),
        }
        reviewer_note = str(review_surface.get("reviewer_note") or payload.get("reviewer_note") or "").strip()
        if payload["reviewer_placeholder"]:
            placeholder_note = (
                "No WP6 artifact exists on disk for this run; repository fallback stays reviewer-only and "
                "readiness-mapping only."
            )
            review_surface["reviewer_note"] = (
                f"{reviewer_note} | {placeholder_note}" if reviewer_note else placeholder_note
            )
        payload["review_surface"] = review_surface
        return payload

    def _build_rollup(
        self,
        *,
        rollup_payload: dict[str, Any],
        comparison_evidence_pack: dict[str, Any],
        comparison_digest: dict[str, Any],
        missing_artifacts: list[str],
    ) -> dict[str, Any]:
        payload = dict(rollup_payload or {})
        digest = dict(payload.get("digest") or {})
        ep_digest = dict(comparison_evidence_pack.get("digest") or {})
        cd_digest = dict(comparison_digest.get("digest") or {})
        summary_lines = list(payload.get("summary_lines") or [])
        detail_lines = list(payload.get("detail_lines") or [])
        repository_line = f"repository/gateway: {WP6_REPOSITORY_MODE} / {WP6_GATEWAY_MODE}"
        if repository_line not in detail_lines:
            detail_lines.insert(0, repository_line)
        if missing_artifacts:
            detail_lines.append(
                "placeholder fallback: " + " | ".join(sorted({str(item).strip() for item in missing_artifacts if str(item).strip()}))
            )
        summary_display = str(
            payload.get("rollup_summary_display")
            or payload.get("overview_display")
            or digest.get("comparison_overview_summary")
            or ep_digest.get("comparison_overview_summary")
            or cd_digest.get("comparison_overview_summary")
            or "PT/ILC comparison reviewer placeholder"
        )
        if summary_display and summary_display not in summary_lines:
            summary_lines.insert(0, summary_display)
        payload["schema_version"] = str(payload.get("schema_version") or WP6_REPOSITORY_SCHEMA_VERSION)
        payload["index_schema_version"] = WP6_REPOSITORY_SCHEMA_VERSION
        payload["run_dir"] = str(self.run_dir)
        payload["generated_at"] = str(
            payload.get("generated_at") or datetime.now(timezone.utc).replace(microsecond=0).isoformat()
        )
        payload["generated_by_tool"] = str(payload.get("generated_by_tool") or WP6_REPOSITORY_TOOL)
        payload["repository_mode"] = WP6_REPOSITORY_MODE
        payload["gateway_mode"] = WP6_GATEWAY_MODE
        payload["db_ready_stub"] = {
            "enabled": False,
            "mode": WP6_DB_READY_MODE,
            "default_path": False,
            "requires_explicit_injection": True,
            "not_in_default_chain": True,
        }
        payload["linked_surface_visibility"] = list(payload.get("linked_surface_visibility") or _LINKED_SURFACES)
        payload["rollup_scope"] = str(payload.get("rollup_scope") or "run-dir")
        payload["artifact_count"] = len(_ARTIFACT_KEYS)
        payload["legacy_placeholder_used"] = bool(missing_artifacts)
        payload["missing_artifact_types"] = [str(item) for item in missing_artifacts if str(item).strip()]
        payload["rollup_summary_display"] = summary_display
        payload["summary_lines"] = summary_lines
        payload["detail_lines"] = detail_lines
        payload["ready_for_readiness_mapping"] = bool(payload.get("ready_for_readiness_mapping", True))
        payload["not_ready_for_formal_claim"] = bool(payload.get("not_ready_for_formal_claim", True))
        payload["not_real_acceptance_evidence"] = bool(payload.get("not_real_acceptance_evidence", True))
        payload["primary_evidence_rewritten"] = False
        payload["non_claim_note"] = str(
            payload.get("non_claim_note")
            or comparison_evidence_pack.get("non_claim_note")
            or comparison_digest.get("non_claim_note")
            or "reviewer-only / readiness mapping only / non-claim"
        )
        payload["limitation_note"] = str(
            payload.get("limitation_note")
            or comparison_evidence_pack.get("limitation_note")
            or "WP6 artifacts remain skeleton / placeholder packs in Step 2"
        )
        payload["digest"] = {
            **ep_digest,
            **cd_digest,
            **dict(payload.get("digest") or {}),
        }
        return payload

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
            "pt_ilc_registry": recognition_readiness.PT_ILC_REGISTRY_FILENAME,
            "pt_ilc_registry_markdown": recognition_readiness.PT_ILC_REGISTRY_MARKDOWN_FILENAME,
            "external_comparison_importer": recognition_readiness.EXTERNAL_COMPARISON_IMPORTER_FILENAME,
            "external_comparison_importer_markdown": recognition_readiness.EXTERNAL_COMPARISON_IMPORTER_MARKDOWN_FILENAME,
            "comparison_evidence_pack": recognition_readiness.COMPARISON_EVIDENCE_PACK_FILENAME,
            "comparison_evidence_pack_markdown": recognition_readiness.COMPARISON_EVIDENCE_PACK_MARKDOWN_FILENAME,
            "scope_comparison_view": recognition_readiness.SCOPE_COMPARISON_VIEW_FILENAME,
            "scope_comparison_view_markdown": recognition_readiness.SCOPE_COMPARISON_VIEW_MARKDOWN_FILENAME,
            "comparison_digest": recognition_readiness.COMPARISON_DIGEST_FILENAME,
            "comparison_digest_markdown": recognition_readiness.COMPARISON_DIGEST_MARKDOWN_FILENAME,
            "comparison_rollup": recognition_readiness.COMPARISON_ROLLUP_FILENAME,
            "comparison_rollup_markdown": recognition_readiness.COMPARISON_ROLLUP_MARKDOWN_FILENAME,
            # upstream WP filenames for path resolution
            "scope_definition_pack": recognition_readiness.SCOPE_DEFINITION_PACK_FILENAME,
            "decision_rule_profile": recognition_readiness.DECISION_RULE_PROFILE_FILENAME,
            "reference_asset_registry": recognition_readiness.REFERENCE_ASSET_REGISTRY_FILENAME,
            "certificate_lifecycle_summary": recognition_readiness.CERTIFICATE_LIFECYCLE_SUMMARY_FILENAME,
            "pre_run_readiness_gate": recognition_readiness.PRE_RUN_READINESS_GATE_FILENAME,
            "uncertainty_report_pack": recognition_readiness.UNCERTAINTY_REPORT_PACK_FILENAME,
            "uncertainty_rollup": recognition_readiness.UNCERTAINTY_ROLLUP_FILENAME,
            "method_confirmation_protocol": recognition_readiness.METHOD_CONFIRMATION_PROTOCOL_FILENAME,
            "verification_digest": recognition_readiness.VERIFICATION_DIGEST_FILENAME,
            "software_validation_rollup": recognition_readiness.AUDIT_READINESS_DIGEST_FILENAME,
        }
