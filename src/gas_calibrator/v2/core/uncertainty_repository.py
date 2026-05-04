from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Protocol

from . import recognition_readiness_artifacts as recognition_readiness
from .measurement_phase_coverage import MEASUREMENT_PHASE_COVERAGE_REPORT_FILENAME
from .recognition_scope_repository import FileBackedRecognitionScopeRepository
from .uncertainty_builder import build_uncertainty_wp3_artifacts

UNCERTAINTY_REPOSITORY_SCHEMA_VERSION = "step2-uncertainty-repository-v1"
UNCERTAINTY_REPOSITORY_MODE = "file_artifact_first"
UNCERTAINTY_GATEWAY_MODE = "file_backed_default"
UNCERTAINTY_DB_READY_MODE = "db_ready_stub"
UNCERTAINTY_REPOSITORY_TOOL = "gas_calibrator.v2.adapters.uncertainty_gateway"
UNCERTAINTY_INPUT_FIXTURE_FILENAME = "uncertainty_budget_inputs.json"

_LINKED_SURFACES = ["results", "review_center", "workbench", "historical_artifacts"]
_ARTIFACT_KEYS = (
    "uncertainty_model",
    "uncertainty_input_set",
    "sensitivity_coefficient_set",
    "budget_case",
    "uncertainty_golden_cases",
    "uncertainty_report_pack",
    "uncertainty_digest",
    "uncertainty_rollup",
)


class UncertaintyRepository(Protocol):
    def load_snapshot(self) -> dict[str, Any]:
        """Return Step 2 reviewer-facing uncertainty payloads."""


class DatabaseReadyUncertaintyRepositoryStub:
    def __init__(self, run_dir: Path) -> None:
        self.run_dir = Path(run_dir)

    def load_snapshot(self) -> dict[str, Any]:
        empty = {key: {} for key in _ARTIFACT_KEYS}
        return {
            **empty,
            "uncertainty_rollup": {
                "schema_version": UNCERTAINTY_REPOSITORY_SCHEMA_VERSION,
                "run_dir": str(self.run_dir),
                "repository_mode": UNCERTAINTY_DB_READY_MODE,
                "gateway_mode": "not_active",
                "db_ready_stub": {
                    "enabled": False,
                    "mode": UNCERTAINTY_DB_READY_MODE,
                    "default_path": False,
                    "requires_explicit_injection": True,
                    "not_in_default_chain": True,
                },
                "summary_lines": ["DB-ready uncertainty stub reserved; Step 2 default remains file-backed."],
                "detail_lines": ["No database connection is opened or required in the current uncertainty path."],
                "primary_evidence_rewritten": False,
                "not_real_acceptance_evidence": True,
            },
        }


class FileBackedUncertaintyRepository:
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
        uncertainty_fixture, fixture_artifact_paths = self._load_uncertainty_fixture_bundle()
        fallback_artifacts = build_uncertainty_wp3_artifacts(
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
            path_map=self._artifact_paths(),
            uncertainty_fixture=uncertainty_fixture,
            fixture_artifact_paths=fixture_artifact_paths,
            filenames=self._artifact_filenames(),
            boundary_statements=[
                "readiness mapping only",
                "reviewer-only uncertainty skeleton",
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
        snapshot["uncertainty_rollup"] = self._build_rollup(
            rollup_payload=dict(snapshot.get("uncertainty_rollup") or {}),
            uncertainty_report_pack=dict(snapshot.get("uncertainty_report_pack") or {}),
            uncertainty_digest=dict(snapshot.get("uncertainty_digest") or {}),
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

    def _load_uncertainty_fixture_bundle(self) -> tuple[dict[str, Any], dict[str, str]]:
        run_fixture_path = self.run_dir / UNCERTAINTY_INPUT_FIXTURE_FILENAME
        run_fixture_payload = self._load_json(UNCERTAINTY_INPUT_FIXTURE_FILENAME)
        if run_fixture_payload:
            return (
                dict(run_fixture_payload),
                {
                    "uncertainty_fixture_file": str(run_fixture_path),
                    "uncertainty_fixture_source": "run_dir_override",
                },
            )
        metrology_fixtures = recognition_readiness.load_metrology_registry_fixtures()
        fixture_paths = dict(recognition_readiness._fixture_artifact_paths(metrology_fixtures))
        fixture_paths["uncertainty_fixture_source"] = "readiness_fixture_root"
        return (
            dict(metrology_fixtures.get("uncertainty_budget_inputs") or {}),
            fixture_paths,
        )

    def _load_payload(self, artifact_key: str, filename: str, *, fallback_bundle: dict[str, Any]) -> dict[str, Any]:
        payload = self._load_json(filename) or self._read_summary_section(artifact_key)
        fallback_raw = dict(fallback_bundle.get("raw") or {})
        payload = {**fallback_raw, **dict(payload or {})}
        payload["artifact_type"] = str(payload.get("artifact_type") or artifact_key)
        payload["schema_version"] = str(payload.get("schema_version") or "1.0")
        payload["repository_mode"] = UNCERTAINTY_REPOSITORY_MODE
        payload["gateway_mode"] = UNCERTAINTY_GATEWAY_MODE
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
                "No uncertainty artifact exists on disk for this run; repository fallback stays reviewer-only and "
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
        uncertainty_report_pack: dict[str, Any],
        uncertainty_digest: dict[str, Any],
        missing_artifacts: list[str],
    ) -> dict[str, Any]:
        payload = dict(rollup_payload or {})
        digest = dict(payload.get("digest") or {})
        report_digest = dict(uncertainty_report_pack.get("digest") or {})
        uncertainty_digest_payload = dict(uncertainty_digest.get("digest") or {})
        summary_lines = list(payload.get("summary_lines") or [])
        detail_lines = list(payload.get("detail_lines") or [])
        repository_line = f"repository/gateway: {UNCERTAINTY_REPOSITORY_MODE} / {UNCERTAINTY_GATEWAY_MODE}"
        if repository_line not in detail_lines:
            detail_lines.insert(0, repository_line)
        if missing_artifacts:
            detail_lines.append(
                "placeholder fallback: " + " | ".join(sorted({str(item).strip() for item in missing_artifacts if str(item).strip()}))
            )
        summary_display = str(
            payload.get("rollup_summary_display")
            or payload.get("overview_display")
            or digest.get("uncertainty_overview_summary")
            or report_digest.get("uncertainty_overview_summary")
            or uncertainty_digest_payload.get("uncertainty_overview_summary")
            or "uncertainty reviewer placeholder"
        )
        if summary_display and summary_display not in summary_lines:
            summary_lines.insert(0, summary_display)
        payload["schema_version"] = str(payload.get("schema_version") or UNCERTAINTY_REPOSITORY_SCHEMA_VERSION)
        payload["index_schema_version"] = UNCERTAINTY_REPOSITORY_SCHEMA_VERSION
        payload["run_dir"] = str(self.run_dir)
        payload["generated_at"] = str(
            payload.get("generated_at") or datetime.now(timezone.utc).replace(microsecond=0).isoformat()
        )
        payload["generated_by_tool"] = str(payload.get("generated_by_tool") or UNCERTAINTY_REPOSITORY_TOOL)
        payload["repository_mode"] = UNCERTAINTY_REPOSITORY_MODE
        payload["gateway_mode"] = UNCERTAINTY_GATEWAY_MODE
        payload["db_ready_stub"] = {
            "enabled": False,
            "mode": UNCERTAINTY_DB_READY_MODE,
            "default_path": False,
            "requires_explicit_injection": True,
            "not_in_default_chain": True,
        }
        payload["linked_surface_visibility"] = list(payload.get("linked_surface_visibility") or _LINKED_SURFACES)
        payload["rollup_scope"] = str(payload.get("rollup_scope") or "run-dir")
        payload["artifact_count"] = len(_ARTIFACT_KEYS)
        payload["report_pack_available"] = bool(uncertainty_report_pack)
        payload["report_pack_available_on_disk"] = bool(uncertainty_report_pack.get("artifact_present_on_disk", False))
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
            or uncertainty_report_pack.get("non_claim_note")
            or uncertainty_digest.get("non_claim_note")
            or "reviewer-only / readiness mapping only / non-claim"
        )
        payload["limitation_note"] = str(
            payload.get("limitation_note")
            or uncertainty_report_pack.get("limitation_note")
            or "uncertainty artifacts remain skeleton / placeholder packs in Step 2"
        )
        payload["digest"] = {
            **report_digest,
            **uncertainty_digest_payload,
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
            "uncertainty_model": recognition_readiness.UNCERTAINTY_MODEL_FILENAME,
            "uncertainty_model_markdown": recognition_readiness.UNCERTAINTY_MODEL_MARKDOWN_FILENAME,
            "uncertainty_input_set": recognition_readiness.UNCERTAINTY_INPUT_SET_FILENAME,
            "uncertainty_input_set_markdown": recognition_readiness.UNCERTAINTY_INPUT_SET_MARKDOWN_FILENAME,
            "sensitivity_coefficient_set": recognition_readiness.SENSITIVITY_COEFFICIENT_SET_FILENAME,
            "sensitivity_coefficient_set_markdown": recognition_readiness.SENSITIVITY_COEFFICIENT_SET_MARKDOWN_FILENAME,
            "budget_case": recognition_readiness.BUDGET_CASE_FILENAME,
            "budget_case_markdown": recognition_readiness.BUDGET_CASE_MARKDOWN_FILENAME,
            "uncertainty_golden_cases": recognition_readiness.UNCERTAINTY_GOLDEN_CASES_FILENAME,
            "uncertainty_golden_cases_markdown": recognition_readiness.UNCERTAINTY_GOLDEN_CASES_MARKDOWN_FILENAME,
            "uncertainty_report_pack": recognition_readiness.UNCERTAINTY_REPORT_PACK_FILENAME,
            "uncertainty_report_pack_markdown": recognition_readiness.UNCERTAINTY_REPORT_PACK_MARKDOWN_FILENAME,
            "uncertainty_digest": recognition_readiness.UNCERTAINTY_DIGEST_FILENAME,
            "uncertainty_digest_markdown": recognition_readiness.UNCERTAINTY_DIGEST_MARKDOWN_FILENAME,
            "uncertainty_rollup": recognition_readiness.UNCERTAINTY_ROLLUP_FILENAME,
            "uncertainty_rollup_markdown": recognition_readiness.UNCERTAINTY_ROLLUP_MARKDOWN_FILENAME,
            "scope_definition_pack": recognition_readiness.SCOPE_DEFINITION_PACK_FILENAME,
            "decision_rule_profile": recognition_readiness.DECISION_RULE_PROFILE_FILENAME,
            "reference_asset_registry": recognition_readiness.REFERENCE_ASSET_REGISTRY_FILENAME,
            "certificate_lifecycle_summary": recognition_readiness.CERTIFICATE_LIFECYCLE_SUMMARY_FILENAME,
            "pre_run_readiness_gate": recognition_readiness.PRE_RUN_READINESS_GATE_FILENAME,
            "measurement_phase_coverage_report": MEASUREMENT_PHASE_COVERAGE_REPORT_FILENAME,
            "uncertainty_budget_stub": recognition_readiness.UNCERTAINTY_BUDGET_STUB_FILENAME,
            "uncertainty_budget_stub_markdown": recognition_readiness.UNCERTAINTY_BUDGET_STUB_MARKDOWN_FILENAME,
        }
