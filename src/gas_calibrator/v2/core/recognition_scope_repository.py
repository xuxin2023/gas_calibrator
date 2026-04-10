from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Protocol

from . import recognition_readiness_artifacts as recognition_readiness

RECOGNITION_SCOPE_REPOSITORY_SCHEMA_VERSION = "step2-recognition-scope-repository-v1"
RECOGNITION_SCOPE_REPOSITORY_MODE = "file_artifact_first"
RECOGNITION_SCOPE_GATEWAY_MODE = "file_backed_default"
RECOGNITION_SCOPE_DB_READY_MODE = "db_ready_stub"
RECOGNITION_SCOPE_REPOSITORY_TOOL = "recognition_scope_gateway"

_LINKED_SURFACES = ["results", "review_center", "workbench", "historical_artifacts"]


class RecognitionScopeRepository(Protocol):
    def load_snapshot(self) -> dict[str, Any]:
        """Return scope package / decision rule / rollup using the default file-backed path."""


class DatabaseReadyRecognitionScopeRepositoryStub:
    def __init__(self, run_dir: Path) -> None:
        self.run_dir = Path(run_dir)

    def load_snapshot(self) -> dict[str, Any]:
        return {
            "scope_definition_pack": {},
            "decision_rule_profile": {},
            "reference_asset_registry": {},
            "certificate_lifecycle_summary": {},
            "pre_run_readiness_gate": {},
            "recognition_scope_rollup": {
                "schema_version": RECOGNITION_SCOPE_REPOSITORY_SCHEMA_VERSION,
                "run_dir": str(self.run_dir),
                "repository_mode": RECOGNITION_SCOPE_DB_READY_MODE,
                "gateway_mode": "not_active",
                "db_ready_stub": {
                    "enabled": False,
                    "mode": RECOGNITION_SCOPE_DB_READY_MODE,
                    "default_path": False,
                    "requires_explicit_injection": True,
                    "not_in_default_chain": True,
                },
                "summary_lines": ["DB-ready stub reserved; Step 2 default remains file-backed."],
                "detail_lines": ["No database connection is opened or required in the current path."],
                "primary_evidence_rewritten": False,
                "not_real_acceptance_evidence": True,
            },
        }


class FileBackedRecognitionScopeRepository:
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
        scope_payload = self._load_payload(
            recognition_readiness.SCOPE_DEFINITION_PACK_FILENAME,
            "scope_definition_pack",
            artifact_type="scope_definition_pack",
            title_text="Scope Definition Pack",
        )
        decision_payload = self._load_payload(
            recognition_readiness.DECISION_RULE_PROFILE_FILENAME,
            "decision_rule_profile",
            artifact_type="decision_rule_profile",
            title_text="Decision Rule Profile",
        )
        reference_asset_registry = self._load_payload(
            recognition_readiness.REFERENCE_ASSET_REGISTRY_FILENAME,
            "reference_asset_registry",
            artifact_type="reference_asset_registry",
            title_text="Reference Asset Registry",
        )
        certificate_lifecycle_summary = self._load_payload(
            recognition_readiness.CERTIFICATE_LIFECYCLE_SUMMARY_FILENAME,
            "certificate_lifecycle_summary",
            artifact_type="certificate_lifecycle_summary",
            title_text="Certificate Lifecycle Summary",
        )
        pre_run_readiness_gate = self._load_payload(
            recognition_readiness.PRE_RUN_READINESS_GATE_FILENAME,
            "pre_run_readiness_gate",
            artifact_type="pre_run_readiness_gate",
            title_text="Pre-run Readiness Gate",
        )
        rollup = self._build_rollup(
            scope_payload,
            decision_payload,
            reference_asset_registry,
            certificate_lifecycle_summary,
            pre_run_readiness_gate,
        )
        return {
            "scope_definition_pack": scope_payload,
            "decision_rule_profile": decision_payload,
            "reference_asset_registry": reference_asset_registry,
            "certificate_lifecycle_summary": certificate_lifecycle_summary,
            "pre_run_readiness_gate": pre_run_readiness_gate,
            "recognition_scope_rollup": rollup,
        }

    def _load_payload(self, filename: str, summary_key: str, *, artifact_type: str, title_text: str) -> dict[str, Any]:
        payload = self._load_json(filename) or self._read_summary_section(summary_key)
        artifact_paths = self._artifact_paths()
        digest = dict(payload.get("digest") or {})
        review_surface = dict(payload.get("review_surface") or {})
        readiness_payload = dict(self.scope_readiness_summary or self._read_summary_section("scope_readiness_summary"))
        readiness_digest = dict(readiness_payload.get("digest") or {})
        if not payload:
            payload = {
                "artifact_type": artifact_type,
                "schema_version": f"compatibility-adapter-{artifact_type}-v1",
                "run_id": str(self.summary.get("run_id") or self.run_dir.name),
                "evidence_source": "simulated_protocol",
                "evidence_state": "reviewer_readiness_only",
                "not_real_acceptance_evidence": True,
                "readiness_status": str(readiness_payload.get("readiness_status") or "ready_for_readiness_mapping"),
                "non_claim_note": str(
                    readiness_payload.get("non_claim_note")
                    or readiness_digest.get("non_claim_digest")
                    or "simulation/offline/shadow outputs remain reviewer-only and cannot become formal claims."
                ),
                "limitation_note": str(
                    readiness_payload.get("limitation_note")
                    or "Current payload is compatibility-adapter reviewer output; formal claim remains out of scope."
                ),
                "gap_note": str(
                    readiness_payload.get("gap_note")
                    or readiness_digest.get("missing_evidence_summary")
                    or "Formal scope approval, released certificates, and real metrology evidence remain outside Step 2."
                ),
                "artifact_paths": artifact_paths,
            }
            digest = {
                "summary": str(readiness_digest.get("summary") or f"{artifact_type} reviewer digest"),
                "scope_overview_summary": str(readiness_digest.get("scope_overview_summary") or "reviewer scope mapping"),
                "decision_rule_summary": str(readiness_digest.get("decision_rule_summary") or "reviewer rule only"),
                "conformity_boundary_summary": str(
                    readiness_digest.get("non_claim_digest") or payload["non_claim_note"]
                ),
                "current_coverage_summary": str(readiness_digest.get("current_coverage_summary") or "--"),
                "missing_evidence_summary": payload["gap_note"],
                "reviewer_next_step_digest": str(
                    readiness_digest.get("reviewer_next_step_digest") or "Only rebuild reviewer/index sidecars."
                ),
                "non_claim_digest": payload["non_claim_note"],
            }
            review_surface = {
                "title_text": title_text,
                "reviewer_note": "Compatibility adapter produced reviewer-facing payload; primary evidence stays unchanged.",
                "summary_text": str(digest.get("summary") or "--"),
                "summary_lines": [
                    f"scope overview: {str(digest.get('scope_overview_summary') or '--')}",
                    f"decision rule: {str(digest.get('decision_rule_summary') or '--')}",
                    f"non-claim: {str(digest.get('conformity_boundary_summary') or '--')}",
                ],
                "detail_lines": [
                    f"current coverage: {str(digest.get('current_coverage_summary') or '--')}",
                    f"gap note: {payload['gap_note']}",
                ],
                "artifact_paths": artifact_paths,
                "phase_filters": ["step2_tail_recognition_ready"],
            }
        payload["artifact_type"] = str(payload.get("artifact_type") or artifact_type)
        payload["schema_version"] = str(payload.get("schema_version") or "1.0")
        payload["artifact_paths"] = {**artifact_paths, **dict(payload.get("artifact_paths") or {})}
        payload["digest"] = {**digest, **dict(payload.get("digest") or {})}
        payload["review_surface"] = {**review_surface, **dict(payload.get("review_surface") or {})}
        payload["repository_mode"] = RECOGNITION_SCOPE_REPOSITORY_MODE
        payload["gateway_mode"] = RECOGNITION_SCOPE_GATEWAY_MODE
        payload["scope_export_pack"] = dict(
            payload.get("scope_export_pack")
            or payload.get("scope_package")
            or {
                "scope_id": str(payload.get("scope_id") or ""),
                "scope_name": str(payload.get("scope_name") or title_text),
                "scope_version": str(payload.get("scope_version") or payload.get("schema_version") or "1.0"),
                "ready_for_readiness_mapping": bool(
                    payload.get("ready_for_readiness_mapping", str(payload.get("readiness_status") or "") == "ready_for_readiness_mapping")
                ),
                "not_ready_for_formal_claim": bool(payload.get("not_ready_for_formal_claim", True)),
                "gap_note": str(payload.get("gap_note") or dict(payload.get("digest") or {}).get("missing_evidence_summary") or ""),
                "limitation_note": str(payload.get("limitation_note") or ""),
                "non_claim_note": str(payload.get("non_claim_note") or dict(payload.get("digest") or {}).get("non_claim_digest") or ""),
            }
        )
        payload["scope_overview"] = dict(
            payload.get("scope_overview")
            or {
                "summary": str(
                    dict(payload.get("digest") or {}).get("scope_overview_summary")
                    or dict(payload.get("digest") or {}).get("summary")
                    or "--"
                ),
                "readiness_status": str(payload.get("readiness_status") or "ready_for_readiness_mapping"),
                "decision_rule_id": str(payload.get("decision_rule_id") or ""),
            }
        )
        payload["decision_rule_overview"] = dict(
            payload.get("decision_rule_overview")
            or {
                "summary": str(
                    dict(payload.get("digest") or {}).get("decision_rule_summary")
                    or payload.get("decision_rule_id")
                    or "--"
                ),
                "readiness_status": str(payload.get("readiness_status") or "ready_for_readiness_mapping"),
            }
        )
        payload["conformity_boundary"] = dict(
            payload.get("conformity_boundary")
            or {
                "summary": str(
                    dict(payload.get("digest") or {}).get("conformity_boundary_summary")
                    or payload.get("non_claim_note")
                    or "--"
                ),
                "reviewer_gate": dict(payload.get("reviewer_gate") or {}),
                "not_ready_for_formal_claim": bool(payload.get("not_ready_for_formal_claim", True)),
            }
        )
        default_digest = {
            "summary": str(
                dict(payload.get("digest") or {}).get("summary")
                or dict(payload.get("scope_overview") or {}).get("summary")
                or dict(payload.get("decision_rule_overview") or {}).get("summary")
                or f"{artifact_type} reviewer digest"
            ),
            "scope_overview_summary": str(
                dict(payload.get("scope_overview") or {}).get("summary")
                or payload.get("scope_name")
                or dict(payload.get("digest") or {}).get("scope_overview_summary")
                or "reviewer scope mapping"
            ),
            "decision_rule_summary": str(
                dict(payload.get("decision_rule_overview") or {}).get("summary")
                or payload.get("decision_rule_id")
                or dict(payload.get("digest") or {}).get("decision_rule_summary")
                or "reviewer rule only"
            ),
            "conformity_boundary_summary": str(
                dict(payload.get("conformity_boundary") or {}).get("summary")
                or payload.get("non_claim_note")
                or dict(payload.get("digest") or {}).get("conformity_boundary_summary")
                or "simulation/offline/shadow outputs remain reviewer-only"
            ),
            "current_coverage_summary": " | ".join(
                str(item).strip()
                for item in list(payload.get("current_evidence_coverage") or [])
                if str(item).strip()
            )
            or str(dict(payload.get("digest") or {}).get("current_coverage_summary") or "--"),
            "missing_evidence_summary": str(
                payload.get("gap_note")
                or dict(payload.get("digest") or {}).get("missing_evidence_summary")
                or "--"
            ),
            "reviewer_next_step_digest": str(
                dict(payload.get("digest") or {}).get("reviewer_next_step_digest")
                or "Keep reviewer mapping explicit and rebuild sidecars/indexes only when needed."
            ),
            "non_claim_digest": str(
                payload.get("non_claim_note")
                or dict(payload.get("digest") or {}).get("non_claim_digest")
                or "--"
            ),
            "standard_family_summary": " | ".join(
                str(item).strip() for item in list(payload.get("standard_family") or []) if str(item).strip()
            )
            or str(dict(payload.get("digest") or {}).get("standard_family_summary") or "--"),
            "required_evidence_categories_summary": " | ".join(
                str(item).strip()
                for item in list(payload.get("required_evidence_categories") or [])
                if str(item).strip()
            )
            or str(dict(payload.get("digest") or {}).get("required_evidence_categories_summary") or "--"),
        }
        payload["digest"] = {**default_digest, **dict(payload.get("digest") or {})}
        default_review_surface = {
            "title_text": title_text,
            "reviewer_note": "File-backed reviewer scope payload only; primary evidence stays unchanged and no formal claim is created.",
            "summary_text": str(payload["digest"].get("summary") or "--"),
            "summary_lines": [
                f"scope overview: {str(payload['digest'].get('scope_overview_summary') or '--')}",
                f"decision rule: {str(payload['digest'].get('decision_rule_summary') or '--')}",
                f"non-claim: {str(payload['digest'].get('conformity_boundary_summary') or '--')}",
            ],
            "detail_lines": [
                f"current coverage: {str(payload['digest'].get('current_coverage_summary') or '--')}",
                f"required evidence categories: {str(payload['digest'].get('required_evidence_categories_summary') or '--')}",
            ],
            "artifact_paths": dict(payload.get("artifact_paths") or {}),
            "phase_filters": ["step2_tail_recognition_ready"],
        }
        payload["review_surface"] = {**default_review_surface, **dict(payload.get("review_surface") or {})}
        payload["primary_evidence_rewritten"] = False
        payload["not_real_acceptance_evidence"] = bool(payload.get("not_real_acceptance_evidence", True))
        return payload

    def _build_rollup(
        self,
        scope_payload: dict[str, Any],
        decision_payload: dict[str, Any],
        reference_asset_registry: dict[str, Any],
        certificate_lifecycle_summary: dict[str, Any],
        pre_run_readiness_gate: dict[str, Any],
    ) -> dict[str, Any]:
        compatibility_overview = dict(self.compatibility_scan_summary.get("compatibility_overview") or {})
        reader_mode = str(
            compatibility_overview.get("current_reader_mode")
            or self.compatibility_scan_summary.get("current_reader_mode")
            or ("canonical_direct" if (self.run_dir / recognition_readiness.SCOPE_DEFINITION_PACK_FILENAME).exists() else "compatibility_adapter")
        )
        reader_mode_display = str(
            compatibility_overview.get("current_reader_mode_display")
            or self.compatibility_scan_summary.get("current_reader_mode_display")
            or {"canonical_direct": "canonical 直读", "compatibility_adapter": "兼容适配读取"}.get(reader_mode, reader_mode)
        )
        scope_digest = dict(scope_payload.get("digest") or {})
        decision_digest = dict(decision_payload.get("digest") or {})
        registry_digest = dict(reference_asset_registry.get("digest") or {})
        lifecycle_digest = dict(certificate_lifecycle_summary.get("digest") or {})
        gate_digest = dict(pre_run_readiness_gate.get("digest") or {})
        generated_at = str(
            scope_payload.get("generated_at")
            or decision_payload.get("generated_at")
            or reference_asset_registry.get("generated_at")
            or certificate_lifecycle_summary.get("generated_at")
            or pre_run_readiness_gate.get("generated_at")
            or datetime.now(timezone.utc).isoformat()
        )
        readiness_status = str(
            pre_run_readiness_gate.get("readiness_status")
            or scope_payload.get("readiness_status")
            or decision_payload.get("readiness_status")
            or "ready_for_readiness_mapping"
        )
        asset_readiness_overview = str(
            registry_digest.get("asset_readiness_overview")
            or registry_digest.get("summary")
            or "--"
        )
        certificate_lifecycle_overview = str(
            lifecycle_digest.get("certificate_lifecycle_overview")
            or lifecycle_digest.get("summary")
            or "--"
        )
        pre_run_gate_status = str(
            gate_digest.get("pre_run_gate_status")
            or pre_run_readiness_gate.get("gate_status")
            or "--"
        )
        blocking_digest = str(gate_digest.get("blocker_summary") or "--")
        warning_digest = str(gate_digest.get("warning_summary") or "--")
        scope_reference_assets_summary = str(
            registry_digest.get("scope_reference_assets_summary")
            or gate_digest.get("scope_reference_assets_summary")
            or "--"
        )
        decision_rule_dependency_summary = str(
            registry_digest.get("decision_rule_dependency_summary")
            or gate_digest.get("decision_rule_dependency_summary")
            or "--"
        )
        regenerate_recommended = bool(
            compatibility_overview.get("regenerate_recommended")
            or self.compatibility_scan_summary.get("regenerate_recommended", False)
        )
        summary_lines = [
            f"认可范围包：{str(scope_digest.get('scope_overview_summary') or scope_digest.get('summary') or '--')}",
            f"决策规则：{str(decision_digest.get('decision_rule_summary') or decision_payload.get('decision_rule_id') or '--')}",
            f"符合性边界：{str(decision_digest.get('conformity_boundary_summary') or scope_payload.get('non_claim_note') or '--')}",
            f"读取路径：{reader_mode_display}",
            f"就绪状态：{readiness_status}",
        ]
        return {
            "schema_version": RECOGNITION_SCOPE_REPOSITORY_SCHEMA_VERSION,
            "index_schema_version": RECOGNITION_SCOPE_REPOSITORY_SCHEMA_VERSION,
            "run_id": str(scope_payload.get("run_id") or decision_payload.get("run_id") or self.run_dir.name),
            "run_dir": str(self.run_dir),
            "generated_at": generated_at,
            "generated_by_tool": RECOGNITION_SCOPE_REPOSITORY_TOOL,
            "rollup_scope": "run-dir",
            "parent_run_count": 1,
            "artifact_count": 2,
            "compatible_run_count": 1 if reader_mode == "canonical_direct" else 0,
            "legacy_run_count": 1 if reader_mode == "compatibility_adapter" else 0,
            "regenerate_recommended_count": 1 if regenerate_recommended else 0,
            "repository_mode": RECOGNITION_SCOPE_REPOSITORY_MODE,
            "gateway_mode": RECOGNITION_SCOPE_GATEWAY_MODE,
            "db_ready_stub": {
                "enabled": False,
                "mode": RECOGNITION_SCOPE_DB_READY_MODE,
                "default_path": False,
                "requires_explicit_injection": True,
                "not_in_default_chain": True,
            },
            "reader_mode": reader_mode,
            "reader_mode_display": reader_mode_display,
            "canonical_direct": reader_mode == "canonical_direct",
            "compatibility_adapter": reader_mode == "compatibility_adapter",
            "scope_overview_display": str(scope_digest.get("scope_overview_summary") or scope_digest.get("summary") or "--"),
            "decision_rule_display": str(decision_digest.get("decision_rule_summary") or decision_payload.get("decision_rule_id") or "--"),
            "conformity_boundary_display": str(
                decision_digest.get("conformity_boundary_summary")
                or scope_payload.get("non_claim_note")
                or "--"
            ),
            "non_claim_note": str(decision_payload.get("non_claim_note") or scope_payload.get("non_claim_note") or "--"),
            "standard_family": list(scope_payload.get("standard_family") or decision_payload.get("standard_family") or []),
            "required_evidence_categories": list(
                scope_payload.get("required_evidence_categories")
                or decision_payload.get("required_evidence_categories")
                or []
            ),
            "linked_existing_artifacts": dict(scope_payload.get("linked_artifacts") or decision_payload.get("artifact_paths") or {}),
            "current_evidence_coverage": list(
                scope_payload.get("current_evidence_coverage")
                or decision_payload.get("current_evidence_coverage")
                or []
            ),
            "readiness_status": readiness_status,
            "linked_surface_visibility": list(_LINKED_SURFACES),
            "regenerate_recommended": regenerate_recommended,
            "primary_evidence_rewritten": False,
            "not_real_acceptance_evidence": True,
            "rollup_summary_display": " | ".join(part for part in summary_lines[:4] if str(part).strip()),
            "summary_lines": summary_lines,
            "detail_lines": [
                f"repository/gateway：{RECOGNITION_SCOPE_REPOSITORY_MODE} / {RECOGNITION_SCOPE_GATEWAY_MODE}",
                "non-claim：simulation/offline/shadow 只用于 reviewer digest / readiness mapping",
                "主证据改写：false",
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
        return {
            "scope_definition_pack": str(self.run_dir / recognition_readiness.SCOPE_DEFINITION_PACK_FILENAME),
            "scope_definition_pack_markdown": str(self.run_dir / recognition_readiness.SCOPE_DEFINITION_PACK_MARKDOWN_FILENAME),
            "decision_rule_profile": str(self.run_dir / recognition_readiness.DECISION_RULE_PROFILE_FILENAME),
            "decision_rule_profile_markdown": str(self.run_dir / recognition_readiness.DECISION_RULE_PROFILE_MARKDOWN_FILENAME),
            "scope_readiness_summary": str(self.run_dir / recognition_readiness.SCOPE_READINESS_SUMMARY_FILENAME),
            "reference_asset_registry": str(self.run_dir / recognition_readiness.REFERENCE_ASSET_REGISTRY_FILENAME),
            "reference_asset_registry_markdown": str(self.run_dir / recognition_readiness.REFERENCE_ASSET_REGISTRY_MARKDOWN_FILENAME),
            "certificate_lifecycle_summary": str(self.run_dir / recognition_readiness.CERTIFICATE_LIFECYCLE_SUMMARY_FILENAME),
            "certificate_lifecycle_summary_markdown": str(self.run_dir / recognition_readiness.CERTIFICATE_LIFECYCLE_SUMMARY_MARKDOWN_FILENAME),
            "pre_run_readiness_gate": str(self.run_dir / recognition_readiness.PRE_RUN_READINESS_GATE_FILENAME),
            "pre_run_readiness_gate_markdown": str(self.run_dir / recognition_readiness.PRE_RUN_READINESS_GATE_MARKDOWN_FILENAME),
        }
