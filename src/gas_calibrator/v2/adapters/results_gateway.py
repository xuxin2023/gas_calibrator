from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Callable, Iterable, Optional

from ..config import build_step2_config_governance_handoff
from ..core.acceptance_model import normalize_evidence_source
from ..core.artifact_catalog import KNOWN_REPORT_ARTIFACTS
from ..core.engineering_isolation_admission_checklist import (
    ENGINEERING_ISOLATION_ADMISSION_CHECKLIST_FILENAME,
    ENGINEERING_ISOLATION_ADMISSION_CHECKLIST_REVIEWER_FILENAME,
)
from ..core.engineering_isolation_admission_checklist_artifact_entry import (
    ENGINEERING_ISOLATION_ADMISSION_CHECKLIST_ARTIFACT_KEY,
    ENGINEERING_ISOLATION_ADMISSION_CHECKLIST_REVIEWER_ARTIFACT_KEY,
    build_engineering_isolation_admission_checklist_artifact_entry,
)
from ..core.controlled_state_machine_profile import (
    STATE_TRANSITION_EVIDENCE_FILENAME,
    STATE_TRANSITION_EVIDENCE_MARKDOWN_FILENAME,
)
from ..core.multi_source_stability import (
    MULTI_SOURCE_STABILITY_EVIDENCE_FILENAME,
    MULTI_SOURCE_STABILITY_EVIDENCE_MARKDOWN_FILENAME,
    SIMULATION_EVIDENCE_SIDECAR_BUNDLE_FILENAME,
)
from ..core.measurement_phase_coverage import (
    MEASUREMENT_PHASE_COVERAGE_REPORT_FILENAME,
    MEASUREMENT_PHASE_COVERAGE_REPORT_MARKDOWN_FILENAME,
)
from ..core import recognition_readiness_artifacts as recognition_readiness
from ..core.offline_artifacts import build_point_taxonomy_handoff, summarize_offline_diagnostic_adapters
from ..core.phase_transition_bridge_reviewer_artifact_entry import (
    PHASE_TRANSITION_BRIDGE_REVIEWER_ARTIFACT_KEY,
    build_phase_transition_bridge_reviewer_artifact_entry,
)
from ..core.phase_transition_bridge_reviewer_artifact import PHASE_TRANSITION_BRIDGE_REVIEWER_FILENAME
from ..core.stage_admission_review_pack import (
    STAGE_ADMISSION_REVIEW_PACK_FILENAME,
    STAGE_ADMISSION_REVIEW_PACK_REVIEWER_FILENAME,
)
from ..core.stage_admission_review_pack_artifact_entry import (
    STAGE_ADMISSION_REVIEW_PACK_ARTIFACT_KEY,
    STAGE_ADMISSION_REVIEW_PACK_REVIEWER_ARTIFACT_KEY,
    build_stage_admission_review_pack_artifact_entry,
)
from ..core.stage3_real_validation_plan import (
    STAGE3_REAL_VALIDATION_PLAN_FILENAME,
    STAGE3_REAL_VALIDATION_PLAN_REVIEWER_FILENAME,
)
from ..core.stage3_real_validation_plan_artifact_entry import (
    STAGE3_REAL_VALIDATION_PLAN_ARTIFACT_KEY,
    STAGE3_REAL_VALIDATION_PLAN_REVIEWER_ARTIFACT_KEY,
    build_stage3_real_validation_plan_artifact_entry,
)
from ..core.stage3_standards_alignment_matrix import (
    STAGE3_STANDARDS_ALIGNMENT_MATRIX_FILENAME,
    STAGE3_STANDARDS_ALIGNMENT_MATRIX_REVIEWER_FILENAME,
)
from ..core.stage3_standards_alignment_matrix_artifact_entry import (
    STAGE3_STANDARDS_ALIGNMENT_MATRIX_ARTIFACT_KEY,
    STAGE3_STANDARDS_ALIGNMENT_MATRIX_REVIEWER_ARTIFACT_KEY,
    build_stage3_standards_alignment_matrix_artifact_entry,
)
from ..review_surface_formatter import (
    build_measurement_review_digest_lines,
    build_readiness_review_digest_lines,
    build_offline_diagnostic_detail_item_line,
    build_offline_diagnostic_scope_line,
    collect_offline_diagnostic_detail_lines,
    humanize_review_surface_text,
    humanize_offline_diagnostic_summary_value,
    normalize_offline_diagnostic_line,
    offline_diagnostic_scope_label,
)
from ..ui_v2.artifact_registry_governance import build_current_run_governance
from ..ui_v2.i18n import t


class ResultsGateway:
    """Read-only access layer for run artifacts and derived result payloads."""

    def __init__(
        self,
        run_dir: Path,
        *,
        output_files_provider: Optional[Callable[[], Iterable[str]]] = None,
    ) -> None:
        self.run_dir = Path(run_dir)
        self.output_files_provider = output_files_provider

    def read_results_payload(self) -> dict[str, Any]:
        summary = self.load_json("summary.json")
        analytics_summary = self.load_json("analytics_summary.json")
        evidence_registry = self.load_json("evidence_registry.json")
        workbench_action_report = self.load_json("workbench_action_report.json")
        workbench_action_snapshot = self.load_json("workbench_action_snapshot.json")
        config_safety = self._read_summary_section(
            "config_safety",
            summary,
            evidence_registry,
            analytics_summary,
            workbench_action_report,
            workbench_action_snapshot,
        )
        config_safety_review = self._read_summary_section(
            "config_safety_review",
            summary,
            evidence_registry,
            analytics_summary,
            workbench_action_report,
            workbench_action_snapshot,
        )
        offline_diagnostic_adapter_summary = self._read_summary_section(
            "offline_diagnostic_adapter_summary",
            summary,
            evidence_registry,
            analytics_summary,
            workbench_action_report,
            workbench_action_snapshot,
        )
        if not offline_diagnostic_adapter_summary:
            offline_diagnostic_adapter_summary = summarize_offline_diagnostic_adapters(self.run_dir)
        point_taxonomy_summary = self._read_summary_section(
            "point_taxonomy_summary",
            summary,
            evidence_registry,
            analytics_summary,
            workbench_action_report,
            workbench_action_snapshot,
        )
        if not point_taxonomy_summary:
            point_taxonomy_summary = (
                build_point_taxonomy_handoff(list(summary.get("stats", {}).get("point_summaries", []) or []))
                if isinstance(summary, dict)
                else {}
            )
        artifact_role_summary = self._read_summary_section(
            "artifact_role_summary",
            summary,
            evidence_registry,
            analytics_summary,
            workbench_action_report,
            workbench_action_snapshot,
        )
        workbench_evidence_summary = self._read_summary_section(
            "workbench_evidence_summary",
            summary,
            evidence_registry,
            analytics_summary,
            workbench_action_report,
            workbench_action_snapshot,
        )
        multi_source_stability_evidence = self.load_json(MULTI_SOURCE_STABILITY_EVIDENCE_FILENAME)
        if not multi_source_stability_evidence:
            multi_source_stability_evidence = self._read_summary_section(
                "multi_source_stability_evidence",
                summary,
                evidence_registry,
                analytics_summary,
                workbench_action_report,
                workbench_action_snapshot,
            )
        state_transition_evidence = self.load_json(STATE_TRANSITION_EVIDENCE_FILENAME)
        if not state_transition_evidence:
            state_transition_evidence = self._read_summary_section(
                "state_transition_evidence",
                summary,
                evidence_registry,
                analytics_summary,
                workbench_action_report,
                workbench_action_snapshot,
            )
        simulation_evidence_sidecar_bundle = self.load_json(SIMULATION_EVIDENCE_SIDECAR_BUNDLE_FILENAME)
        if not simulation_evidence_sidecar_bundle:
            simulation_evidence_sidecar_bundle = self._read_summary_section(
                "simulation_evidence_sidecar_bundle",
                summary,
                evidence_registry,
                analytics_summary,
                workbench_action_report,
                workbench_action_snapshot,
            )
        measurement_phase_coverage_report = self.load_json(MEASUREMENT_PHASE_COVERAGE_REPORT_FILENAME)
        if not measurement_phase_coverage_report:
            measurement_phase_coverage_report = self._read_summary_section(
                "measurement_phase_coverage_report",
                summary,
                evidence_registry,
                analytics_summary,
                workbench_action_report,
                workbench_action_snapshot,
            )
        scope_readiness_summary = self.load_json(recognition_readiness.SCOPE_READINESS_SUMMARY_FILENAME)
        if not scope_readiness_summary:
            scope_readiness_summary = self._read_summary_section(
                "scope_readiness_summary",
                summary,
                evidence_registry,
                analytics_summary,
                workbench_action_report,
                workbench_action_snapshot,
            )
        certificate_readiness_summary = self.load_json(recognition_readiness.CERTIFICATE_READINESS_SUMMARY_FILENAME)
        if not certificate_readiness_summary:
            certificate_readiness_summary = self._read_summary_section(
                "certificate_readiness_summary",
                summary,
                evidence_registry,
                analytics_summary,
                workbench_action_report,
                workbench_action_snapshot,
            )
        uncertainty_method_readiness_summary = self.load_json(
            recognition_readiness.UNCERTAINTY_METHOD_READINESS_SUMMARY_FILENAME
        )
        if not uncertainty_method_readiness_summary:
            uncertainty_method_readiness_summary = self._read_summary_section(
                "uncertainty_method_readiness_summary",
                summary,
                evidence_registry,
                analytics_summary,
                workbench_action_report,
                workbench_action_snapshot,
            )
        audit_readiness_digest = self.load_json(recognition_readiness.AUDIT_READINESS_DIGEST_FILENAME)
        if not audit_readiness_digest:
            audit_readiness_digest = self._read_summary_section(
                "audit_readiness_digest",
                summary,
                evidence_registry,
                analytics_summary,
                workbench_action_report,
                workbench_action_snapshot,
            )
        evidence_source = self._resolve_current_run_evidence_source(workbench_evidence_summary, workbench_action_report)
        evidence_state = str(
            workbench_evidence_summary.get("evidence_state")
            or dict(workbench_action_report or {}).get("evidence_state")
            or "collected"
        )
        not_real_acceptance_evidence = bool(
            workbench_evidence_summary.get(
                "not_real_acceptance_evidence",
                dict(workbench_action_report or {}).get("not_real_acceptance_evidence", True),
            )
        )
        acceptance_level = str(
            workbench_evidence_summary.get("acceptance_level")
            or dict(workbench_action_report or {}).get("acceptance_level")
            or "offline_regression"
        )
        promotion_state = str(
            workbench_evidence_summary.get("promotion_state")
            or dict(workbench_action_report or {}).get("promotion_state")
            or "dry_run_only"
        )
        result_summary_text = self._build_result_summary_text(
            summary=summary,
            artifact_role_summary=artifact_role_summary,
            config_safety=config_safety,
            config_safety_review=config_safety_review,
            offline_diagnostic_adapter_summary=offline_diagnostic_adapter_summary,
            point_taxonomy_summary=point_taxonomy_summary,
            workbench_evidence_summary=workbench_evidence_summary,
            evidence_source=evidence_source,
            multi_source_stability_evidence=multi_source_stability_evidence,
            state_transition_evidence=state_transition_evidence,
            simulation_evidence_sidecar_bundle=simulation_evidence_sidecar_bundle,
            measurement_phase_coverage_report=measurement_phase_coverage_report,
            scope_readiness_summary=scope_readiness_summary,
            certificate_readiness_summary=certificate_readiness_summary,
            uncertainty_method_readiness_summary=uncertainty_method_readiness_summary,
            audit_readiness_digest=audit_readiness_digest,
        )
        return {
            "summary": summary,
            "manifest": self.load_json("manifest.json"),
            "results": self.load_json("results.json"),
            "acceptance_plan": self.load_json("acceptance_plan.json"),
            "analytics_summary": analytics_summary,
            "spectral_quality_summary": self.load_json("spectral_quality_summary.json"),
            "trend_registry": self.load_json("trend_registry.json"),
            "lineage_summary": self.load_json("lineage_summary.json"),
            "evidence_registry": evidence_registry,
            "coefficient_registry": self.load_json("coefficient_registry.json"),
            "suite_summary": self.load_json("suite_summary.json"),
            "suite_analytics_summary": self.load_json("suite_analytics_summary.json"),
            "suite_acceptance_plan": self.load_json("suite_acceptance_plan.json"),
            "suite_evidence_registry": self.load_json("suite_evidence_registry.json"),
            "workbench_action_report": workbench_action_report,
            "workbench_action_snapshot": workbench_action_snapshot,
            "ai_summary_text": self.load_text("ai_run_summary.md") or self.load_text("run_summary.txt"),
            "output_files": self.list_output_files(),
            "reporting": dict(summary.get("reporting", {}) or {}) if isinstance(summary, dict) else {},
            "config_safety": config_safety,
            "config_safety_review": config_safety_review,
            "config_governance_handoff": self._read_config_governance_handoff(
                config_safety,
                config_safety_review,
                summary,
                evidence_registry,
                analytics_summary,
                workbench_action_report,
                workbench_action_snapshot,
            ),
            "artifact_exports": dict(summary.get("stats", {}).get("artifact_exports", {}) or {}) if isinstance(summary, dict) else {},
            "artifact_role_summary": artifact_role_summary,
            "workbench_evidence_summary": workbench_evidence_summary,
            "offline_diagnostic_adapter_summary": offline_diagnostic_adapter_summary,
            "point_taxonomy_summary": point_taxonomy_summary,
            "multi_source_stability_evidence": multi_source_stability_evidence,
            "state_transition_evidence": state_transition_evidence,
            "simulation_evidence_sidecar_bundle": simulation_evidence_sidecar_bundle,
            "measurement_phase_coverage_report": measurement_phase_coverage_report,
            "scope_readiness_summary": scope_readiness_summary,
            "certificate_readiness_summary": certificate_readiness_summary,
            "uncertainty_method_readiness_summary": uncertainty_method_readiness_summary,
            "audit_readiness_digest": audit_readiness_digest,
            "result_summary_text": result_summary_text,
            "evidence_source": evidence_source,
            "evidence_state": evidence_state,
            "not_real_acceptance_evidence": not_real_acceptance_evidence,
            "acceptance_level": acceptance_level,
            "promotion_state": promotion_state,
        }

    def read_reports_payload(self) -> dict[str, Any]:
        payload = self.read_results_payload()
        manifest = dict(payload.get("manifest", {}) or {})
        role_catalog = dict(manifest.get("artifacts", {}) or {}).get("role_catalog", {})
        artifact_exports = dict(payload.get("artifact_exports", {}) or {})
        offline_diagnostic_adapter_summary = dict(payload.get("offline_diagnostic_adapter_summary", {}) or {})
        analytics_summary = dict(payload.get("analytics_summary", {}) or {})
        summary_stats = dict(dict(payload.get("summary", {}) or {}).get("stats", {}) or {})
        multi_source_stability_evidence = dict(payload.get("multi_source_stability_evidence", {}) or {})
        state_transition_evidence = dict(payload.get("state_transition_evidence", {}) or {})
        simulation_evidence_sidecar_bundle = dict(payload.get("simulation_evidence_sidecar_bundle", {}) or {})
        measurement_phase_coverage_report = dict(payload.get("measurement_phase_coverage_report", {}) or {})
        scope_readiness_summary = dict(payload.get("scope_readiness_summary", {}) or {})
        certificate_readiness_summary = dict(payload.get("certificate_readiness_summary", {}) or {})
        uncertainty_method_readiness_summary = dict(payload.get("uncertainty_method_readiness_summary", {}) or {})
        audit_readiness_digest = dict(payload.get("audit_readiness_digest", {}) or {})

        def _artifact_path(value: Any) -> Path:
            candidate = Path(str(value or "").strip())
            if candidate.is_absolute():
                return candidate
            return self.run_dir / candidate

        reviewer_artifact_section = dict(manifest.get(PHASE_TRANSITION_BRIDGE_REVIEWER_ARTIFACT_KEY) or {})
        reviewer_surface_section = (
            dict(manifest.get("phase_transition_bridge_reviewer_section") or {})
            or dict(analytics_summary.get("phase_transition_bridge_reviewer_section") or {})
        )
        reviewer_artifact_path = str(reviewer_artifact_section.get("path") or "").strip()
        if not reviewer_artifact_path:
            fallback_path = self.run_dir / PHASE_TRANSITION_BRIDGE_REVIEWER_FILENAME
            if fallback_path.exists():
                reviewer_artifact_path = str(fallback_path)
        reviewer_artifact_entry = build_phase_transition_bridge_reviewer_artifact_entry(
            artifact_path=reviewer_artifact_path,
            manifest_section=reviewer_artifact_section,
            reviewer_section=reviewer_surface_section,
        )
        if not bool(reviewer_artifact_entry.get("available", False)):
            reviewer_artifact_entry = {}
        stage_admission_review_pack_section = dict(manifest.get(STAGE_ADMISSION_REVIEW_PACK_ARTIFACT_KEY) or {})
        stage_admission_review_pack_reviewer_section = dict(
            manifest.get(STAGE_ADMISSION_REVIEW_PACK_REVIEWER_ARTIFACT_KEY) or {}
        )
        stage_admission_review_pack_path = str(stage_admission_review_pack_section.get("path") or "").strip()
        if not stage_admission_review_pack_path:
            fallback_path = self.run_dir / STAGE_ADMISSION_REVIEW_PACK_FILENAME
            if fallback_path.exists():
                stage_admission_review_pack_path = str(fallback_path)
        stage_admission_review_pack_reviewer_path = str(
            stage_admission_review_pack_reviewer_section.get("path") or ""
        ).strip()
        if not stage_admission_review_pack_reviewer_path:
            fallback_path = self.run_dir / STAGE_ADMISSION_REVIEW_PACK_REVIEWER_FILENAME
            if fallback_path.exists():
                stage_admission_review_pack_reviewer_path = str(fallback_path)
        stage_admission_review_pack_entry = build_stage_admission_review_pack_artifact_entry(
            artifact_path=stage_admission_review_pack_path,
            reviewer_artifact_path=stage_admission_review_pack_reviewer_path,
            manifest_section=stage_admission_review_pack_section,
            reviewer_manifest_section=stage_admission_review_pack_reviewer_section,
        )
        if not bool(stage_admission_review_pack_entry.get("available", False)):
            stage_admission_review_pack_entry = {}
        engineering_isolation_admission_checklist_section = dict(
            manifest.get(ENGINEERING_ISOLATION_ADMISSION_CHECKLIST_ARTIFACT_KEY) or {}
        )
        engineering_isolation_admission_checklist_reviewer_section = dict(
            manifest.get(ENGINEERING_ISOLATION_ADMISSION_CHECKLIST_REVIEWER_ARTIFACT_KEY) or {}
        )
        engineering_isolation_admission_checklist_path = str(
            engineering_isolation_admission_checklist_section.get("path") or ""
        ).strip()
        if not engineering_isolation_admission_checklist_path:
            fallback_path = self.run_dir / ENGINEERING_ISOLATION_ADMISSION_CHECKLIST_FILENAME
            if fallback_path.exists():
                engineering_isolation_admission_checklist_path = str(fallback_path)
        engineering_isolation_admission_checklist_reviewer_path = str(
            engineering_isolation_admission_checklist_reviewer_section.get("path") or ""
        ).strip()
        if not engineering_isolation_admission_checklist_reviewer_path:
            fallback_path = self.run_dir / ENGINEERING_ISOLATION_ADMISSION_CHECKLIST_REVIEWER_FILENAME
            if fallback_path.exists():
                engineering_isolation_admission_checklist_reviewer_path = str(fallback_path)
        engineering_isolation_admission_checklist_entry = (
            build_engineering_isolation_admission_checklist_artifact_entry(
                artifact_path=engineering_isolation_admission_checklist_path,
                reviewer_artifact_path=engineering_isolation_admission_checklist_reviewer_path,
                manifest_section=engineering_isolation_admission_checklist_section,
                reviewer_manifest_section=engineering_isolation_admission_checklist_reviewer_section,
            )
        )
        if not bool(engineering_isolation_admission_checklist_entry.get("available", False)):
            engineering_isolation_admission_checklist_entry = {}
        stage3_real_validation_plan_section = dict(manifest.get(STAGE3_REAL_VALIDATION_PLAN_ARTIFACT_KEY) or {})
        stage3_real_validation_plan_reviewer_section = dict(
            manifest.get(STAGE3_REAL_VALIDATION_PLAN_REVIEWER_ARTIFACT_KEY) or {}
        )
        stage3_real_validation_plan_path = str(stage3_real_validation_plan_section.get("path") or "").strip()
        if not stage3_real_validation_plan_path:
            fallback_path = self.run_dir / STAGE3_REAL_VALIDATION_PLAN_FILENAME
            if fallback_path.exists():
                stage3_real_validation_plan_path = str(fallback_path)
        stage3_real_validation_plan_reviewer_path = str(
            stage3_real_validation_plan_reviewer_section.get("path") or ""
        ).strip()
        if not stage3_real_validation_plan_reviewer_path:
            fallback_path = self.run_dir / STAGE3_REAL_VALIDATION_PLAN_REVIEWER_FILENAME
            if fallback_path.exists():
                stage3_real_validation_plan_reviewer_path = str(fallback_path)
        stage3_reviewer_markdown_text = ""
        if stage3_real_validation_plan_reviewer_path:
            try:
                stage3_reviewer_markdown_text = _artifact_path(
                    stage3_real_validation_plan_reviewer_path
                ).read_text(encoding="utf-8")
            except Exception:
                stage3_reviewer_markdown_text = ""
        stage3_real_validation_plan_entry = build_stage3_real_validation_plan_artifact_entry(
            artifact_path=stage3_real_validation_plan_path,
            reviewer_artifact_path=stage3_real_validation_plan_reviewer_path,
            manifest_section=stage3_real_validation_plan_section,
            reviewer_manifest_section=stage3_real_validation_plan_reviewer_section,
            digest_section=dict(summary_stats.get("stage3_real_validation_plan_digest") or {}),
            reviewer_markdown_text=stage3_reviewer_markdown_text,
        )
        if not bool(stage3_real_validation_plan_entry.get("available", False)):
            stage3_real_validation_plan_entry = {}
        stage3_standards_alignment_matrix_section = dict(
            manifest.get(STAGE3_STANDARDS_ALIGNMENT_MATRIX_ARTIFACT_KEY) or {}
        )
        stage3_standards_alignment_matrix_reviewer_section = dict(
            manifest.get(STAGE3_STANDARDS_ALIGNMENT_MATRIX_REVIEWER_ARTIFACT_KEY) or {}
        )
        stage3_standards_alignment_matrix_path = str(
            stage3_standards_alignment_matrix_section.get("path") or ""
        ).strip()
        if not stage3_standards_alignment_matrix_path:
            fallback_path = self.run_dir / STAGE3_STANDARDS_ALIGNMENT_MATRIX_FILENAME
            if fallback_path.exists():
                stage3_standards_alignment_matrix_path = str(fallback_path)
        stage3_standards_alignment_matrix_reviewer_path = str(
            stage3_standards_alignment_matrix_reviewer_section.get("path") or ""
        ).strip()
        if not stage3_standards_alignment_matrix_reviewer_path:
            fallback_path = self.run_dir / STAGE3_STANDARDS_ALIGNMENT_MATRIX_REVIEWER_FILENAME
            if fallback_path.exists():
                stage3_standards_alignment_matrix_reviewer_path = str(fallback_path)
        stage3_standards_alignment_matrix_markdown_text = ""
        if stage3_standards_alignment_matrix_reviewer_path:
            try:
                stage3_standards_alignment_matrix_markdown_text = _artifact_path(
                    stage3_standards_alignment_matrix_reviewer_path
                ).read_text(encoding="utf-8")
            except Exception:
                stage3_standards_alignment_matrix_markdown_text = ""
        stage3_standards_alignment_matrix_entry = build_stage3_standards_alignment_matrix_artifact_entry(
            artifact_path=stage3_standards_alignment_matrix_path,
            reviewer_artifact_path=stage3_standards_alignment_matrix_reviewer_path,
            manifest_section=stage3_standards_alignment_matrix_section,
            reviewer_manifest_section=stage3_standards_alignment_matrix_reviewer_section,
            digest_section=dict(summary_stats.get("stage3_standards_alignment_matrix_digest") or {}),
            reviewer_markdown_text=stage3_standards_alignment_matrix_markdown_text,
        )
        if not bool(stage3_standards_alignment_matrix_entry.get("available", False)):
            stage3_standards_alignment_matrix_entry = {}
        files = []
        seen: set[str] = set()

        candidate_paths = [self.run_dir / item for item in KNOWN_REPORT_ARTIFACTS]
        candidate_paths.extend(_artifact_path(item) for item in payload["output_files"] if str(item or "").strip())
        candidate_paths.extend(
            _artifact_path(item)
            for item in list(offline_diagnostic_adapter_summary.get("artifact_paths") or [])
            if str(item or "").strip()
        )
        for path in candidate_paths:
            key = str(path)
            if key in seen:
                continue
            seen.add(key)
            present_on_disk = path.exists()
            governance = build_current_run_governance(
                path,
                artifact_exports=artifact_exports,
                role_catalog=role_catalog if isinstance(role_catalog, dict) else None,
                present_on_disk=present_on_disk,
            )
            row = {
                "name": path.name,
                "path": str(path),
                "present": present_on_disk,
                "present_on_disk": present_on_disk,
                "listed_in_current_run": True,
                "artifact_origin": "current_run",
                "scope_match": "all",
                **governance,
            }
            row = self._decorate_phase_transition_bridge_reviewer_artifact_row(
                row,
                reviewer_artifact_entry=reviewer_artifact_entry,
            )
            row = self._decorate_stage_admission_review_pack_row(
                row,
                stage_admission_review_pack_entry=stage_admission_review_pack_entry,
            )
            row = self._decorate_engineering_isolation_admission_checklist_row(
                row,
                engineering_isolation_admission_checklist_entry=engineering_isolation_admission_checklist_entry,
            )
            row = self._decorate_stage3_real_validation_plan_row(
                row,
                stage3_real_validation_plan_entry=stage3_real_validation_plan_entry,
            )
            row = self._decorate_stage3_standards_alignment_matrix_row(
                row,
                stage3_standards_alignment_matrix_entry=stage3_standards_alignment_matrix_entry,
            )
            row = self._decorate_multi_source_stability_row(
                row,
                multi_source_stability_evidence=multi_source_stability_evidence,
            )
            row = self._decorate_state_transition_evidence_row(
                row,
                state_transition_evidence=state_transition_evidence,
            )
            row = self._decorate_simulation_sidecar_bundle_row(
                row,
                simulation_evidence_sidecar_bundle=simulation_evidence_sidecar_bundle,
            )
            row = self._decorate_measurement_phase_coverage_row(
                row,
                measurement_phase_coverage_report=measurement_phase_coverage_report,
            )
            row = self._decorate_scope_readiness_summary_row(
                row,
                scope_readiness_summary=scope_readiness_summary,
            )
            row = self._decorate_certificate_readiness_summary_row(
                row,
                certificate_readiness_summary=certificate_readiness_summary,
            )
            row = self._decorate_uncertainty_method_readiness_summary_row(
                row,
                uncertainty_method_readiness_summary=uncertainty_method_readiness_summary,
            )
            row = self._decorate_audit_readiness_digest_row(
                row,
                audit_readiness_digest=audit_readiness_digest,
            )
            files.append(row)
        return {
            "run_dir": str(self.run_dir),
            "files": files,
            "ai_summary_text": str(payload.get("ai_summary_text", "") or ""),
            "result_summary_text": str(payload.get("result_summary_text", "") or ""),
            "output_files": list(payload["output_files"]),
            "reporting": dict(payload.get("reporting", {}) or {}),
            "config_safety": dict(payload.get("config_safety", {}) or {}),
            "config_safety_review": dict(payload.get("config_safety_review", {}) or {}),
            "config_governance_handoff": dict(payload.get("config_governance_handoff", {}) or {}),
            "artifact_exports": dict(payload.get("artifact_exports", {}) or {}),
            "artifact_role_summary": dict(payload.get("artifact_role_summary", {}) or {}),
            "workbench_evidence_summary": dict(payload.get("workbench_evidence_summary", {}) or {}),
            "offline_diagnostic_adapter_summary": offline_diagnostic_adapter_summary,
            "point_taxonomy_summary": dict(payload.get("point_taxonomy_summary", {}) or {}),
            "multi_source_stability_evidence": multi_source_stability_evidence,
            "state_transition_evidence": state_transition_evidence,
            "simulation_evidence_sidecar_bundle": simulation_evidence_sidecar_bundle,
            "measurement_phase_coverage_report": measurement_phase_coverage_report,
            "scope_readiness_summary": scope_readiness_summary,
            "certificate_readiness_summary": certificate_readiness_summary,
            "uncertainty_method_readiness_summary": uncertainty_method_readiness_summary,
            "audit_readiness_digest": audit_readiness_digest,
            "phase_transition_bridge_reviewer_artifact_entry": dict(reviewer_artifact_entry),
            "stage_admission_review_pack_artifact_entry": dict(stage_admission_review_pack_entry),
            "engineering_isolation_admission_checklist_artifact_entry": dict(
                engineering_isolation_admission_checklist_entry
            ),
            "stage3_real_validation_plan_artifact_entry": dict(stage3_real_validation_plan_entry),
            "stage3_standards_alignment_matrix_artifact_entry": dict(stage3_standards_alignment_matrix_entry),
            "evidence_source": str(payload.get("evidence_source", "") or "simulated_protocol"),
            "evidence_state": str(payload.get("evidence_state", "") or "collected"),
            "not_real_acceptance_evidence": bool(payload.get("not_real_acceptance_evidence", True)),
            "acceptance_level": str(payload.get("acceptance_level", "") or "offline_regression"),
            "promotion_state": str(payload.get("promotion_state", "") or "dry_run_only"),
        }

    def list_output_files(self) -> list[str]:
        files = set()
        if self.output_files_provider is not None:
            try:
                files.update(str(item) for item in self.output_files_provider() or [])
            except Exception:
                pass
        if self.run_dir.exists():
            for path in self.run_dir.iterdir():
                files.add(str(path))
        return sorted(files)

    def load_json(self, relative_name: str) -> dict[str, Any] | None:
        path = self.run_dir / relative_name
        if not path.exists():
            return None
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return None
        return payload if isinstance(payload, dict) else None

    def load_text(self, relative_name: str) -> str:
        path = self.run_dir / relative_name
        if not path.exists():
            return ""
        try:
            return path.read_text(encoding="utf-8")
        except Exception:
            return ""

    @staticmethod
    def _read_section_from_payload(payload: dict[str, Any] | None, key: str) -> dict[str, Any]:
        if not isinstance(payload, dict):
            return {}
        stats = payload.get("stats")
        if isinstance(stats, dict):
            legacy = stats.get(key)
            if isinstance(legacy, dict):
                return dict(legacy)
        direct = payload.get(key)
        return dict(direct) if isinstance(direct, dict) else {}

    @classmethod
    def _read_summary_section(cls, key: str, *payloads: dict[str, Any] | None) -> dict[str, Any]:
        for payload in payloads:
            section = cls._read_section_from_payload(payload, key)
            if section:
                return section
        return {}

    @classmethod
    def _read_config_governance_handoff(
        cls,
        config_safety: dict[str, Any] | None,
        config_safety_review: dict[str, Any] | None,
        *payloads: dict[str, Any] | None,
    ) -> dict[str, Any]:
        for payload in payloads:
            section = cls._read_section_from_payload(payload, "config_governance_handoff")
            if section:
                return section
        if config_safety_review:
            return build_step2_config_governance_handoff(config_safety_review)
        if config_safety:
            return build_step2_config_governance_handoff(config_safety)
        return {}

    @staticmethod
    def _build_result_summary_text(
        *,
        summary: dict[str, Any] | None,
        artifact_role_summary: dict[str, Any] | None,
        config_safety: dict[str, Any] | None,
        config_safety_review: dict[str, Any] | None,
        offline_diagnostic_adapter_summary: dict[str, Any] | None,
        point_taxonomy_summary: dict[str, Any] | None,
        workbench_evidence_summary: dict[str, Any] | None,
        evidence_source: str,
        multi_source_stability_evidence: dict[str, Any] | None,
        state_transition_evidence: dict[str, Any] | None,
        simulation_evidence_sidecar_bundle: dict[str, Any] | None,
        measurement_phase_coverage_report: dict[str, Any] | None,
        scope_readiness_summary: dict[str, Any] | None,
        certificate_readiness_summary: dict[str, Any] | None,
        uncertainty_method_readiness_summary: dict[str, Any] | None,
        audit_readiness_digest: dict[str, Any] | None,
    ) -> str:
        summary_payload = dict(summary or {})
        stats = dict(summary_payload.get("stats", {}) or {})
        role_summary = dict(artifact_role_summary or {})
        safety = dict(config_safety or {})
        safety_review = dict(config_safety_review or {})
        offline_summary = dict(offline_diagnostic_adapter_summary or {})
        taxonomy_summary = dict(point_taxonomy_summary or {})
        workbench_summary = dict(workbench_evidence_summary or {})
        stability_summary = dict(multi_source_stability_evidence or {})
        transition_summary = dict(state_transition_evidence or {})
        sidecar_summary = dict(simulation_evidence_sidecar_bundle or {})
        phase_coverage_summary = dict(measurement_phase_coverage_report or {})
        scope_readiness_payload = dict(scope_readiness_summary or {})
        certificate_readiness_payload = dict(certificate_readiness_summary or {})
        uncertainty_method_payload = dict(uncertainty_method_readiness_summary or {})
        audit_readiness_payload = dict(audit_readiness_digest or {})

        role_parts: list[str] = []
        for role in ("execution_summary", "execution_rows", "diagnostic_analysis", "formal_analysis"):
            payload = dict(role_summary.get(role) or {})
            count = int(payload.get("count", 0) or 0)
            if count > 0:
                role_parts.append(f"{role} {count}")
        artifact_role_text = " | ".join(role_parts) if role_parts else "--"

        sample_count = int(stats.get("sample_count", 0) or 0)
        point_summary_count = len(list(stats.get("point_summaries", []) or []))
        lines = [
            f"结果文件: {'已生成' if isinstance(summary, dict) else '缺失'}",
            f"样本数: {sample_count}",
            f"点摘要数: {point_summary_count}",
            f"工件角色: {artifact_role_text}",
            f"配置安全: {str(safety_review.get('summary') or safety.get('summary') or '--')}",
        ]

        lines.insert(4, f"证据来源: {evidence_source}")
        if offline_summary:
            lines.append(
                "离线诊断: "
                + str(
                    offline_summary.get("summary")
                    or (
                        f"room-temp {int(offline_summary.get('room_temp_count', 0) or 0)} | "
                        f"analyzer-chain {int(offline_summary.get('analyzer_chain_count', 0) or 0)}"
                    )
                )
            )
        if str(offline_summary.get("coverage_summary") or "").strip():
            coverage_summary = humanize_offline_diagnostic_summary_value(str(offline_summary.get("coverage_summary") or ""))
            lines.append(
                t(
                    "facade.results.result_summary.offline_diagnostic_coverage",
                    value=coverage_summary,
                    default=f"离线诊断覆盖：{coverage_summary}",
                )
            )
        if str(offline_summary.get("review_scope_summary") or "").strip():
            review_scope_summary = humanize_offline_diagnostic_summary_value(
                str(offline_summary.get("review_scope_summary") or "")
            )
            lines.append(
                t(
                    "facade.results.result_summary.offline_diagnostic_scope",
                    value=review_scope_summary,
                    default="离线诊断工件范围：" + review_scope_summary,
                )
            )
        if str(offline_summary.get("next_check_summary") or "").strip():
            lines.append(
                t(
                    "facade.results.result_summary.offline_diagnostic_next_checks",
                    value=str(offline_summary.get("next_check_summary") or ""),
                    default=f"离线诊断下一步：{str(offline_summary.get('next_check_summary') or '')}",
                )
            )

        for detail_line in ResultsGateway._offline_diagnostic_detail_lines(offline_summary):
            lines.append(
                t(
                    "facade.results.result_summary.offline_diagnostic_detail",
                    value=detail_line,
                    default=f"离线诊断补充: {detail_line}",
                )
            )
        point_taxonomy_summary = taxonomy_summary
        pressure_summary = str(taxonomy_summary.get("pressure_summary") or "").strip()
        pressure_mode_summary = str(taxonomy_summary.get("pressure_mode_summary") or "").strip()
        pressure_target_label_summary = str(taxonomy_summary.get("pressure_target_label_summary") or "").strip()
        if pressure_summary:
            lines.append(
                t(
                    "facade.results.result_summary.taxonomy_pressure",
                    value=pressure_summary,
                    default=f"压力语义：{pressure_summary}",
                )
            )
        if pressure_mode_summary and pressure_mode_summary != pressure_summary:
            lines.append(
                t(
                    "facade.results.result_summary.taxonomy_pressure_mode",
                    value=pressure_mode_summary,
                    default=f"压力模式：{pressure_mode_summary}",
                )
            )
        if pressure_target_label_summary and pressure_target_label_summary != pressure_summary:
            lines.append(
                t(
                    "facade.results.result_summary.taxonomy_pressure_target_label",
                    value=pressure_target_label_summary,
                    default=f"压力目标标签：{pressure_target_label_summary}",
                )
            )
        if str(point_taxonomy_summary.get("flush_gate_summary") or "").strip():
            lines.append(
                t(
                    "facade.results.result_summary.taxonomy_flush",
                    value=str(point_taxonomy_summary.get("flush_gate_summary") or ""),
                    default=f"冲洗门禁：{str(point_taxonomy_summary.get('flush_gate_summary') or '')}",
                )
            )
        if str(point_taxonomy_summary.get("preseal_summary") or "").strip():
            lines.append(
                t(
                    "facade.results.result_summary.taxonomy_preseal",
                    value=str(point_taxonomy_summary.get("preseal_summary") or ""),
                    default=f"前封气：{str(point_taxonomy_summary.get('preseal_summary') or '')}",
                )
            )
        if str(point_taxonomy_summary.get("postseal_summary") or "").strip():
            lines.append(
                t(
                    "facade.results.result_summary.taxonomy_postseal",
                    value=str(point_taxonomy_summary.get("postseal_summary") or ""),
                    default=f"后封气：{str(point_taxonomy_summary.get('postseal_summary') or '')}",
                )
            )
        if str(point_taxonomy_summary.get("stale_gauge_summary") or "").strip():
            lines.append(
                t(
                    "facade.results.result_summary.taxonomy_stale_gauge",
                    value=str(point_taxonomy_summary.get("stale_gauge_summary") or ""),
                    default=f"压力参考陈旧：{str(point_taxonomy_summary.get('stale_gauge_summary') or '')}",
                )
            )

        workbench_text = str(
            workbench_summary.get("summary_line")
            or workbench_summary.get("summary")
            or workbench_summary.get("review_summary")
            or "--"
        )
        lines.append(f"工作台诊断证据: {workbench_text}")

        stability_digest = dict(stability_summary.get("digest") or {})
        transition_digest = dict(transition_summary.get("digest") or {})
        phase_coverage_digest = dict(phase_coverage_summary.get("digest") or {})
        measurement_core_stability_text = (
            str(
                stability_digest.get("summary")
                or stability_summary.get("summary")
                or stability_summary.get("coverage_status")
                or "--"
            )
            if stability_summary
            else ""
        )
        measurement_core_transition_text = (
            str(
                transition_digest.get("summary")
                or transition_summary.get("summary")
                or transition_summary.get("overall_status")
                or "--"
            )
            if transition_summary
            else ""
        )
        measurement_core_phase_coverage_text = (
            str(
                phase_coverage_digest.get("summary")
                or phase_coverage_summary.get("summary")
                or phase_coverage_summary.get("overall_status")
                or "--"
            )
            if phase_coverage_summary
            else ""
        )
        measurement_core_payload_phase_text = (
            str(phase_coverage_digest.get("payload_phase_summary") or "").strip()
            if phase_coverage_summary
            else ""
        )
        measurement_core_payload_complete_text = (
            str(phase_coverage_digest.get("payload_complete_phase_summary") or "").strip()
            if phase_coverage_summary
            else ""
        )
        measurement_core_payload_partial_text = (
            str(phase_coverage_digest.get("payload_partial_phase_summary") or "").strip()
            if phase_coverage_summary
            else ""
        )
        measurement_core_trace_only_text = (
            str(phase_coverage_digest.get("trace_only_phase_summary") or "").strip()
            if phase_coverage_summary
            else ""
        )
        measurement_core_payload_completeness_text = (
            str(phase_coverage_digest.get("payload_completeness_summary") or "").strip()
            if phase_coverage_summary
            else ""
        )
        measurement_core_next_artifacts_text = (
            str(phase_coverage_digest.get("next_required_artifacts_summary") or "").strip()
            if phase_coverage_summary
            else ""
        )
        measurement_core_sidecar_text = (
            " | ".join(
                f"{key} {len(list(value or []))}"
                for key, value in dict(sidecar_summary.get("stores") or {}).items()
            )
            if sidecar_summary
            else ""
        )
        measurement_review_lines = build_measurement_review_digest_lines(phase_coverage_summary)

        if measurement_core_stability_text:
            lines.append(
                humanize_review_surface_text(
                    t(
                        "facade.results.result_summary.measurement_core_stability",
                        value=measurement_core_stability_text,
                        default=f"multi-source stability shadow: {measurement_core_stability_text}",
                    )
                )
            )
        if measurement_core_transition_text:
            lines.append(
                humanize_review_surface_text(
                    t(
                        "facade.results.result_summary.measurement_core_transition",
                        value=measurement_core_transition_text,
                        default=f"controlled state trace: {measurement_core_transition_text}",
                    )
                )
            )
        if measurement_core_phase_coverage_text:
            lines.extend(measurement_review_lines.get("summary_lines") or [])
            lines.extend((measurement_review_lines.get("detail_lines") or [])[:4])
        if measurement_core_sidecar_text or dict(simulation_evidence_sidecar_bundle or {}):
            sidecar_contract_text = str(sidecar_summary.get("reviewer_note") or "").strip()
            lines.append(
                humanize_review_surface_text(
                    "sidecar-ready contract: "
                    + (
                        measurement_core_sidecar_text
                        or sidecar_contract_text
                        or "future database intake / sidecar-ready"
                    )
                )
            )
        for readiness_payload in (
            scope_readiness_payload,
            certificate_readiness_payload,
            uncertainty_method_payload,
            audit_readiness_payload,
        ):
            localized_lines = build_readiness_review_digest_lines(readiness_payload)
            lines.extend(localized_lines.get("summary_lines") or [])
            lines.extend((localized_lines.get("detail_lines") or [])[:3])

        return "\n".join(line for line in lines if str(line).strip())

    @staticmethod
    def _resolve_current_run_evidence_source(
        workbench_evidence_summary: dict[str, Any] | None,
        workbench_action_report: dict[str, Any] | None,
    ) -> str:
        source = (
            dict(workbench_evidence_summary or {}).get("evidence_source")
            or dict(workbench_action_report or {}).get("evidence_source")
            or "simulated_protocol"
        )
        return normalize_evidence_source(source)

    @staticmethod
    def _offline_diagnostic_detail_lines(
        offline_diagnostic_adapter_summary: dict[str, Any] | None,
        *,
        limit: int = 3,
    ) -> list[str]:
        return collect_offline_diagnostic_detail_lines(offline_diagnostic_adapter_summary, limit=limit)

    @staticmethod
    def _offline_diagnostic_detail_item_line(item: Any) -> str:
        return build_offline_diagnostic_detail_item_line(item)

    @staticmethod
    def _offline_diagnostic_scope_line(scope_summary: str) -> str:
        return build_offline_diagnostic_scope_line(scope_summary)

    @staticmethod
    def _offline_diagnostic_scope_label() -> str:
        return offline_diagnostic_scope_label()

    @staticmethod
    def _normalize_offline_diagnostic_line(line: str) -> str:
        return normalize_offline_diagnostic_line(line)

    @staticmethod
    def _decorate_phase_transition_bridge_reviewer_artifact_row(
        row: dict[str, Any],
        *,
        reviewer_artifact_entry: dict[str, Any],
    ) -> dict[str, Any]:
        payload = dict(row or {})
        if str(payload.get("artifact_key") or "") != PHASE_TRANSITION_BRIDGE_REVIEWER_ARTIFACT_KEY:
            return payload
        entry = dict(reviewer_artifact_entry or {})
        if not entry:
            return payload
        return {
            **payload,
            "name": str(entry.get("name_text") or payload.get("name") or ""),
            "note": str(entry.get("note_text") or payload.get("note") or ""),
            "role_status_display": str(entry.get("role_status_display") or payload.get("role_status_display") or ""),
            "phase_transition_bridge_reviewer_artifact_entry": entry,
        }

    @staticmethod
    def _decorate_stage_admission_review_pack_row(
        row: dict[str, Any],
        *,
        stage_admission_review_pack_entry: dict[str, Any],
    ) -> dict[str, Any]:
        payload = dict(row or {})
        artifact_key = str(payload.get("artifact_key") or "")
        if artifact_key not in {
            STAGE_ADMISSION_REVIEW_PACK_ARTIFACT_KEY,
            STAGE_ADMISSION_REVIEW_PACK_REVIEWER_ARTIFACT_KEY,
        }:
            return payload
        entry = dict(stage_admission_review_pack_entry or {})
        if not entry:
            return payload
        is_reviewer_artifact = artifact_key == STAGE_ADMISSION_REVIEW_PACK_REVIEWER_ARTIFACT_KEY
        existing_role_status = str(payload.get("role_status_display") or "").strip()
        entry_role_status = str(entry.get("role_status_display") or "").strip()
        role_status_display = " | ".join(
            part
            for part in (existing_role_status, entry_role_status)
            if str(part).strip()
        )
        return {
            **payload,
            "name": str(
                entry.get("name_text")
                or payload.get("name")
                or ""
            )
            + (" (Markdown)" if is_reviewer_artifact else " (JSON)"),
            "note": str(entry.get("note_text") or payload.get("note") or ""),
            "role_status_display": role_status_display or existing_role_status,
            "stage_admission_review_pack_artifact_entry": entry,
        }

    @staticmethod
    def _decorate_engineering_isolation_admission_checklist_row(
        row: dict[str, Any],
        *,
        engineering_isolation_admission_checklist_entry: dict[str, Any],
    ) -> dict[str, Any]:
        payload = dict(row or {})
        artifact_key = str(payload.get("artifact_key") or "")
        if artifact_key not in {
            ENGINEERING_ISOLATION_ADMISSION_CHECKLIST_ARTIFACT_KEY,
            ENGINEERING_ISOLATION_ADMISSION_CHECKLIST_REVIEWER_ARTIFACT_KEY,
        }:
            return payload
        entry = dict(engineering_isolation_admission_checklist_entry or {})
        if not entry:
            return payload
        is_reviewer_artifact = artifact_key == ENGINEERING_ISOLATION_ADMISSION_CHECKLIST_REVIEWER_ARTIFACT_KEY
        existing_role_status = str(payload.get("role_status_display") or "").strip()
        entry_role_status = str(entry.get("role_status_display") or "").strip()
        role_status_display = " | ".join(
            part
            for part in (existing_role_status, entry_role_status)
            if str(part).strip()
        )
        return {
            **payload,
            "name": str(
                entry.get("name_text")
                or payload.get("name")
                or ""
            )
            + (" (Markdown)" if is_reviewer_artifact else " (JSON)"),
            "note": str(entry.get("note_text") or payload.get("note") or ""),
            "role_status_display": role_status_display or existing_role_status,
            "engineering_isolation_admission_checklist_artifact_entry": entry,
        }

    @staticmethod
    def _decorate_stage3_real_validation_plan_row(
        row: dict[str, Any],
        *,
        stage3_real_validation_plan_entry: dict[str, Any],
    ) -> dict[str, Any]:
        payload = dict(row or {})
        artifact_key = str(payload.get("artifact_key") or "")
        if artifact_key not in {
            STAGE3_REAL_VALIDATION_PLAN_ARTIFACT_KEY,
            STAGE3_REAL_VALIDATION_PLAN_REVIEWER_ARTIFACT_KEY,
        }:
            return payload
        entry = dict(stage3_real_validation_plan_entry or {})
        if not entry:
            return payload
        is_reviewer_artifact = artifact_key == STAGE3_REAL_VALIDATION_PLAN_REVIEWER_ARTIFACT_KEY
        existing_role_status = str(payload.get("role_status_display") or "").strip()
        entry_role_status = str(entry.get("role_status_display") or "").strip()
        role_status_display = " | ".join(
            part
            for part in (existing_role_status, entry_role_status)
            if str(part).strip()
        )
        return {
            **payload,
            "name": str(
                entry.get("name_text")
                or payload.get("name")
                or ""
            )
            + (" (Markdown)" if is_reviewer_artifact else " (JSON)"),
            "note": str(entry.get("summary_text") or payload.get("note") or ""),
            "role_status_display": role_status_display or existing_role_status,
            "stage3_real_validation_plan_artifact_entry": entry,
        }

    @staticmethod
    def _decorate_stage3_standards_alignment_matrix_row(
        row: dict[str, Any],
        *,
        stage3_standards_alignment_matrix_entry: dict[str, Any],
    ) -> dict[str, Any]:
        payload = dict(row or {})
        artifact_key = str(payload.get("artifact_key") or "")
        if artifact_key not in {
            STAGE3_STANDARDS_ALIGNMENT_MATRIX_ARTIFACT_KEY,
            STAGE3_STANDARDS_ALIGNMENT_MATRIX_REVIEWER_ARTIFACT_KEY,
        }:
            return payload
        entry = dict(stage3_standards_alignment_matrix_entry or {})
        if not entry:
            return payload
        is_reviewer_artifact = artifact_key == STAGE3_STANDARDS_ALIGNMENT_MATRIX_REVIEWER_ARTIFACT_KEY
        existing_role_status = str(payload.get("role_status_display") or "").strip()
        entry_role_status = str(entry.get("role_status_display") or "").strip()
        role_status_display = " | ".join(
            part
            for part in (existing_role_status, entry_role_status)
            if str(part).strip()
        )
        return {
            **payload,
            "name": str(entry.get("name_text") or payload.get("name") or "")
            + (" (Markdown)" if is_reviewer_artifact else " (JSON)"),
            "note": str(entry.get("summary_text") or payload.get("note") or ""),
            "role_status_display": role_status_display or existing_role_status,
            "stage3_standards_alignment_matrix_artifact_entry": entry,
        }

    @classmethod
    def _decorate_multi_source_stability_row(
        cls,
        row: dict[str, Any],
        *,
        multi_source_stability_evidence: dict[str, Any],
    ) -> dict[str, Any]:
        return cls._decorate_measurement_core_row(
            row,
            payload=multi_source_stability_evidence,
            json_filename=MULTI_SOURCE_STABILITY_EVIDENCE_FILENAME,
            markdown_filename=MULTI_SOURCE_STABILITY_EVIDENCE_MARKDOWN_FILENAME,
            entry_key="multi_source_stability_evidence_entry",
        )

    @classmethod
    def _decorate_state_transition_evidence_row(
        cls,
        row: dict[str, Any],
        *,
        state_transition_evidence: dict[str, Any],
    ) -> dict[str, Any]:
        return cls._decorate_measurement_core_row(
            row,
            payload=state_transition_evidence,
            json_filename=STATE_TRANSITION_EVIDENCE_FILENAME,
            markdown_filename=STATE_TRANSITION_EVIDENCE_MARKDOWN_FILENAME,
            entry_key="state_transition_evidence_entry",
        )

    @classmethod
    def _decorate_measurement_phase_coverage_row(
        cls,
        row: dict[str, Any],
        *,
        measurement_phase_coverage_report: dict[str, Any],
    ) -> dict[str, Any]:
        return cls._decorate_measurement_core_row(
            row,
            payload=measurement_phase_coverage_report,
            json_filename=MEASUREMENT_PHASE_COVERAGE_REPORT_FILENAME,
            markdown_filename=MEASUREMENT_PHASE_COVERAGE_REPORT_MARKDOWN_FILENAME,
            entry_key="measurement_phase_coverage_report_entry",
        )

    @classmethod
    def _decorate_scope_readiness_summary_row(
        cls,
        row: dict[str, Any],
        *,
        scope_readiness_summary: dict[str, Any],
    ) -> dict[str, Any]:
        return cls._decorate_measurement_core_row(
            row,
            payload=scope_readiness_summary,
            json_filename=recognition_readiness.SCOPE_READINESS_SUMMARY_FILENAME,
            markdown_filename=recognition_readiness.SCOPE_READINESS_SUMMARY_MARKDOWN_FILENAME,
            entry_key="scope_readiness_summary_entry",
        )

    @classmethod
    def _decorate_certificate_readiness_summary_row(
        cls,
        row: dict[str, Any],
        *,
        certificate_readiness_summary: dict[str, Any],
    ) -> dict[str, Any]:
        return cls._decorate_measurement_core_row(
            row,
            payload=certificate_readiness_summary,
            json_filename=recognition_readiness.CERTIFICATE_READINESS_SUMMARY_FILENAME,
            markdown_filename=recognition_readiness.CERTIFICATE_READINESS_SUMMARY_MARKDOWN_FILENAME,
            entry_key="certificate_readiness_summary_entry",
        )

    @classmethod
    def _decorate_uncertainty_method_readiness_summary_row(
        cls,
        row: dict[str, Any],
        *,
        uncertainty_method_readiness_summary: dict[str, Any],
    ) -> dict[str, Any]:
        return cls._decorate_measurement_core_row(
            row,
            payload=uncertainty_method_readiness_summary,
            json_filename=recognition_readiness.UNCERTAINTY_METHOD_READINESS_SUMMARY_FILENAME,
            markdown_filename=recognition_readiness.UNCERTAINTY_METHOD_READINESS_SUMMARY_MARKDOWN_FILENAME,
            entry_key="uncertainty_method_readiness_summary_entry",
        )

    @classmethod
    def _decorate_audit_readiness_digest_row(
        cls,
        row: dict[str, Any],
        *,
        audit_readiness_digest: dict[str, Any],
    ) -> dict[str, Any]:
        return cls._decorate_measurement_core_row(
            row,
            payload=audit_readiness_digest,
            json_filename=recognition_readiness.AUDIT_READINESS_DIGEST_FILENAME,
            markdown_filename=recognition_readiness.AUDIT_READINESS_DIGEST_MARKDOWN_FILENAME,
            entry_key="audit_readiness_digest_entry",
        )

    @staticmethod
    def _decorate_simulation_sidecar_bundle_row(
        row: dict[str, Any],
        *,
        simulation_evidence_sidecar_bundle: dict[str, Any],
    ) -> dict[str, Any]:
        payload = dict(row or {})
        if Path(str(payload.get("path") or "")).name != SIMULATION_EVIDENCE_SIDECAR_BUNDLE_FILENAME:
            return payload
        sidecar_payload = dict(simulation_evidence_sidecar_bundle or {})
        if not sidecar_payload:
            return payload
        note = str(sidecar_payload.get("reviewer_note") or payload.get("note") or "").strip()
        boundary_summary = " | ".join(str(item).strip() for item in list(sidecar_payload.get("boundary_statements") or []) if str(item).strip())
        role_status_display = " | ".join(
            part
            for part in (
                str(payload.get("role_status_display") or "").strip(),
                boundary_summary,
            )
            if str(part).strip()
        )
        return {
            **payload,
            "name": str(sidecar_payload.get("title_text") or payload.get("name") or ""),
            "note": note,
            "role_status_display": role_status_display or str(payload.get("role_status_display") or ""),
            "simulation_evidence_sidecar_bundle_entry": sidecar_payload,
        }

    @staticmethod
    def _decorate_measurement_core_row(
        row: dict[str, Any],
        *,
        payload: dict[str, Any],
        json_filename: str,
        markdown_filename: str,
        entry_key: str,
    ) -> dict[str, Any]:
        artifact_row = dict(row or {})
        path_name = Path(str(artifact_row.get("path") or "")).name
        if path_name not in {json_filename, markdown_filename}:
            return artifact_row
        evidence_payload = dict(payload or {})
        review_surface = dict(evidence_payload.get("review_surface") or {})
        digest = dict(evidence_payload.get("digest") or {})
        if not review_surface and not digest:
            return artifact_row
        is_markdown = path_name == markdown_filename
        boundary_summary = " | ".join(
            str(item).strip()
            for item in list(review_surface.get("boundary_filters") or evidence_payload.get("boundary_statements") or [])
            if str(item).strip()
        )
        role_status_display = " | ".join(
            part
            for part in (
                str(artifact_row.get("role_status_display") or "").strip(),
                str(evidence_payload.get("overall_status") or "").strip(),
                boundary_summary,
            )
            if str(part).strip()
        )
        return {
            **artifact_row,
            "name": str(review_surface.get("title_text") or artifact_row.get("name") or "")
            + (" (Markdown)" if is_markdown else " (JSON)"),
            "note": str(
                review_surface.get("reviewer_note")
                or digest.get("summary")
                or artifact_row.get("note")
                or ""
            ),
            "role_status_display": role_status_display or str(artifact_row.get("role_status_display") or ""),
            entry_key: {
                "review_surface": review_surface,
                "digest": digest,
                "artifact_paths": dict(evidence_payload.get("artifact_paths") or {}),
                "overall_status": str(evidence_payload.get("overall_status") or ""),
                "anchor_id": str(evidence_payload.get("anchor_id") or review_surface.get("anchor_id") or ""),
                "anchor_label": str(evidence_payload.get("anchor_label") or review_surface.get("anchor_label") or ""),
                "readiness_status": str(evidence_payload.get("readiness_status") or digest.get("readiness_status") or ""),
                "linked_artifact_refs": [dict(item) for item in list(evidence_payload.get("linked_artifact_refs") or []) if isinstance(item, dict)],
                "linked_measurement_phase_artifacts": [
                    dict(item) for item in list(evidence_payload.get("linked_measurement_phase_artifacts") or []) if isinstance(item, dict)
                ],
                "linked_measurement_phases": list(evidence_payload.get("linked_measurement_phases") or []),
                "linked_measurement_gaps": [
                    dict(item) for item in list(evidence_payload.get("linked_measurement_gaps") or []) if isinstance(item, dict)
                ],
                "linked_method_confirmation_item_keys": list(
                    evidence_payload.get("linked_method_confirmation_item_keys") or []
                ),
                "linked_method_confirmation_items": list(evidence_payload.get("linked_method_confirmation_items") or []),
                "linked_uncertainty_input_keys": list(evidence_payload.get("linked_uncertainty_input_keys") or []),
                "linked_uncertainty_inputs": list(evidence_payload.get("linked_uncertainty_inputs") or []),
                "linked_traceability_node_keys": list(evidence_payload.get("linked_traceability_node_keys") or []),
                "linked_traceability_nodes": list(evidence_payload.get("linked_traceability_nodes") or []),
                "linked_gap_classification_keys": list(evidence_payload.get("linked_gap_classification_keys") or []),
                "linked_gap_severity_keys": list(evidence_payload.get("linked_gap_severity_keys") or []),
                "next_required_artifacts": list(evidence_payload.get("next_required_artifacts") or []),
                "blockers": list(evidence_payload.get("blockers") or []),
                "gap_reason": str(evidence_payload.get("gap_reason") or digest.get("gap_reason") or ""),
                "gap_classification_label": str(
                    evidence_payload.get("gap_classification_label") or digest.get("gap_classification_label") or ""
                ),
                "gap_severity_label": str(
                    evidence_payload.get("gap_severity_label") or digest.get("gap_severity_label") or ""
                ),
                "reviewer_next_step_digest": str(
                    evidence_payload.get("reviewer_next_step_digest") or digest.get("reviewer_next_step_digest") or ""
                ),
                "reviewer_next_step_template_key": str(
                    evidence_payload.get("reviewer_next_step_template_key")
                    or digest.get("reviewer_next_step_template_key")
                    or ""
                ),
                "boundary_digest": str(evidence_payload.get("boundary_digest") or digest.get("boundary_digest") or ""),
                "non_claim_digest": str(evidence_payload.get("non_claim_digest") or digest.get("non_claim_digest") or ""),
            },
        }
