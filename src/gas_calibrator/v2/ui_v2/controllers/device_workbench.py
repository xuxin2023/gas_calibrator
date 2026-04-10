from __future__ import annotations

import copy
from datetime import datetime
import json
from collections import Counter, deque
from dataclasses import asdict, is_dataclass
from pathlib import Path
from typing import Any, Optional

from ...config import (
    build_step2_config_governance_handoff,
    build_step2_config_safety_review,
    hydrate_step2_config_safety_summary,
    summarize_step2_config_safety,
)
from ...core.controlled_state_machine_profile import STATE_TRANSITION_EVIDENCE_FILENAME
from ...core.multi_source_stability import (
    MULTI_SOURCE_STABILITY_EVIDENCE_FILENAME,
    SIMULATION_EVIDENCE_SIDECAR_BUNDLE_FILENAME,
)
from ...core.measurement_phase_coverage import MEASUREMENT_PHASE_COVERAGE_REPORT_FILENAME
from ...core import recognition_readiness_artifacts as recognition_readiness
from ...core.offline_artifacts import build_point_taxonomy_handoff
from ...core.device_factory import DeviceFactory, DeviceType
from ...qc.qc_report import build_qc_evidence_section, build_qc_reviewer_card
from ...review_surface_formatter import (
    build_measurement_review_digest_lines,
    build_readiness_review_digest_lines,
    collect_boundary_digest_lines,
    humanize_review_surface_text,
)
from ...sim.devices import SimulatedDeviceMatrix
from ..i18n import (
    display_artifact_role,
    display_bool,
    display_evidence_source,
    display_evidence_state,
    display_reference_quality,
    display_risk_level,
    format_pressure_hpa,
    format_temperature_c,
    t,
)


def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    merged = copy.deepcopy(base)
    for key, value in dict(override or {}).items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge(dict(merged[key]), value)
        else:
            merged[key] = copy.deepcopy(value)
    return merged


def _copy_payload(value: Any) -> Any:
    if is_dataclass(value):
        return asdict(value)
    if isinstance(value, dict):
        return copy.deepcopy(value)
    return copy.deepcopy(value)


class DeviceWorkbenchController:
    """Simulation-only device workbench controller for the Devices page."""

    ANALYZER_COUNT = 8
    LAYOUT_MODES = ("compact", "standard")
    RELAY_NAMES = ("relay", "relay_8")
    VIEW_MODES = ("operator_view", "engineer_view")
    DISPLAY_PROFILES = ("auto", "dense_1080p", "standard_display")
    DISPLAY_PROFILE_STRATEGY_VERSION = "display_profile_v2"
    HISTORY_RESULT_FILTERS = ("all", "success", "failed", "fault_injection")
    MAX_RECENT_PRESETS = 8
    MAX_FAVORITE_PRESETS = 8
    MAX_PINNED_PRESETS = 6
    MAX_CUSTOM_PRESET_STEPS = 3
    PRESET_DEFINITION_SCHEMA = "preset_definition_v2"
    PRESET_IMPORT_CONFLICT_POLICIES = ("rename", "overwrite")
    PRESET_BUNDLE_PROFILE = "simulation_only_local_exchange"
    PRESET_GROUP_DEVICE_MAP = {
        "analyzer": "analyzer",
        "pace": "pace",
        "grz": "grz",
        "chamber": "chamber",
        "relay": "relay",
        "thermometer": "thermometer",
        "pressure": "pressure_gauge",
    }
    FEATURED_PRESETS = {
        "analyzer": ("mode2_active_read", "partial_frame"),
        "pace": ("set_pressure", "unsupported_header"),
        "grz": ("stable", "humidity_static_fault"),
        "chamber": ("reach_target", "stalled"),
        "relay": ("route_h2o", "route_co2", "stuck_channel"),
        "thermometer": ("stable", "stale"),
        "pressure_gauge": ("stable", "wrong_unit"),
    }
    WORKBENCH_EXPORTS = {
        "workbench_action_report_json": "workbench_action_report.json",
        "workbench_action_report_markdown": "workbench_action_report.md",
        "workbench_action_snapshot": "workbench_action_snapshot.json",
    }
    WORKBENCH_EVIDENCE_SOURCE = "simulated_protocol"
    WORKBENCH_EVIDENCE_STATE = "simulated_workbench"
    WORKBENCH_ACCEPTANCE_LEVEL = "offline_regression"
    WORKBENCH_PROMOTION_STATE = "dry_run_only"
    QUICK_SCENARIOS = (
        "analyzer_partial_frame",
        "pace_cleanup_no_response",
        "relay_stuck",
        "thermometer_stale",
        "pressure_wrong_unit",
    )
    DISPLAY_PROFILE_LAYOUT_HINTS = {
        "auto": "compact",
        "dense_1080p": "compact",
        "standard_display": "standard",
    }
    DISPLAY_PROFILE_FAMILIES = {
        "auto": "1080p_compact",
        "dense_1080p": "1080p_compact",
        "standard_display": "1080p_standard",
    }
    DISPLAY_PROFILE_DEFAULT_RESOLUTION = "1920x1080"
    DISPLAY_PROFILE_DEFAULT_CONTEXT = {
        "screen_width": 1920,
        "screen_height": 1080,
        "window_width": 1720,
        "window_height": 940,
    }
    PRESET_BUNDLE_SCHEMA = "simulation_preset_bundle_v1"
    DISPLAY_PROFILE_WIDE_ASPECT_RATIO = 2.10

    def __init__(self, facade: Any) -> None:
        self.facade = facade
        self._selected_analyzer_index = 0
        self._view_mode = "operator_view"
        self._layout_mode = "compact"
        self._display_profile = "auto"
        self._display_profile_context = self._default_display_profile_context()
        self._action_sequence = 0
        self._recent_frames: dict[str, deque[str]] = {
            f"gas_analyzer_{index}": deque(maxlen=5)
            for index in range(self.ANALYZER_COUNT)
        }
        self._recent_ascii_streams: dict[str, deque[str]] = {
            "thermometer": deque(maxlen=6),
            "pressure_gauge": deque(maxlen=6),
        }
        self._preset_usage: Counter[str] = Counter()
        self._recent_presets: deque[dict[str, Any]] = deque(maxlen=self.MAX_RECENT_PRESETS)
        self._favorite_presets: list[str] = []
        self._pinned_presets: list[str] = []
        self._custom_presets: list[dict[str, Any]] = []
        self._preset_import_conflict_policy = "rename"
        self._action_log: deque[dict[str, Any]] = deque(maxlen=40)
        self._snapshot_log: deque[dict[str, Any]] = deque(maxlen=16)
        self._desired_relay_states: dict[str, dict[int, bool]] = {
            relay_name: {}
            for relay_name in self.RELAY_NAMES
        }
        self._history_device_filter = "all"
        self._history_result_filter = "all"
        self._selected_history_sequence: int | None = None
        self._selected_snapshot_left: int | None = None
        self._selected_snapshot_right: int | None = None
        self._simulation_context = self._bind_simulation_context()
        self._factory = self._resolve_factory()
        self._device_cache: dict[str, Any] = {}
        self._last_evidence_report: dict[str, Any] = {}
        self._load_persistent_state()
        self._seed_simulation_context()

    def _load_persistent_state(self) -> None:
        preferences = {}
        loader = getattr(self.facade, "get_preferences", None)
        if callable(loader):
            try:
                preferences = dict(loader() or {})
            except Exception:
                preferences = {}
        workbench_preferences = dict(preferences.get("workbench", {}) or {})
        preset_preferences = dict(workbench_preferences.get("preset_preferences", {}) or {})

        requested_view = str(workbench_preferences.get("view_mode") or "operator_view").strip().lower()
        self._view_mode = requested_view if requested_view in self.VIEW_MODES else "operator_view"

        requested_layout = str(workbench_preferences.get("layout_mode") or "compact").strip().lower()
        self._layout_mode = requested_layout if requested_layout in self.LAYOUT_MODES else "compact"

        display_profile_context = self._normalize_display_profile_context(
            dict(workbench_preferences.get("display_profile_context", {}) or {})
        )
        requested_profile = str(
            workbench_preferences.get("display_profile")
            or display_profile_context.get("selected")
            or "auto"
        ).strip().lower()
        self._display_profile = requested_profile if requested_profile in self.DISPLAY_PROFILES else "auto"
        self._display_profile_context = self._normalize_display_profile_context(
            {
                **display_profile_context,
                "selected": self._display_profile,
            }
        )

        usage_payload = dict(preset_preferences.get("usage", {}) or {})
        self._preset_usage = Counter(
            {
                str(key): max(0, int(value or 0))
                for key, value in usage_payload.items()
                if str(key).strip()
            }
        )

        self._favorite_presets = self._normalize_preset_keys(
            preset_preferences.get("favorites"),
            limit=self.MAX_FAVORITE_PRESETS,
        )
        self._pinned_presets = self._normalize_preset_keys(
            preset_preferences.get("pinned"),
            limit=self.MAX_PINNED_PRESETS,
        )
        self._custom_presets = [
            dict(item)
            for item in list(preset_preferences.get("custom_presets", []) or [])
            if isinstance(item, dict)
        ]
        self._custom_presets = self._normalize_custom_presets(self._custom_presets)
        self._preset_import_conflict_policy = self._normalize_preset_import_conflict_policy(
            preset_preferences.get("import_conflict_policy")
        )

        recent_rows: list[dict[str, Any]] = []
        for item in list(preset_preferences.get("recent_presets", []) or []):
            if not isinstance(item, dict):
                continue
            device_kind = self._normalize_device_kind(item.get("device_kind"))
            preset_id = str(item.get("id") or "").strip()
            if not device_kind or not preset_id:
                continue
            group_id = self._preset_group_id(device_kind)
            recent_rows.append(
                {
                    "id": preset_id,
                    "label": self._preset_label(device_kind, preset_id),
                    "device_kind": device_kind,
                    "group_id": group_id,
                    "group_display": self._preset_group_display(group_id),
                    "usage_count": int(item.get("usage_count", self._preset_usage.get(self._preset_key(device_kind, preset_id), 0)) or 0),
                    "used_at": str(item.get("used_at") or ""),
                }
            )
        self._recent_presets = deque(recent_rows[: self.MAX_RECENT_PRESETS], maxlen=self.MAX_RECENT_PRESETS)

    def _save_persistent_state(self) -> None:
        saver = getattr(self.facade, "save_ui_layout_preferences", None)
        if not callable(saver):
            return
        saver(
            {
                "workbench": {
                    "view_mode": self._view_mode,
                    "layout_mode": self._layout_mode,
                    "display_profile": self._display_profile,
                    "display_profile_context": self._display_profile_payload(self._display_profile),
                    "preset_preferences": {
                        "favorites": list(self._favorite_presets),
                        "pinned": list(self._pinned_presets),
                        "recent_presets": [
                            {
                                "id": str(item.get("id") or ""),
                                "device_kind": self._normalize_device_kind(item.get("device_kind")),
                                "usage_count": int(item.get("usage_count", 0) or 0),
                                "used_at": str(item.get("used_at") or ""),
                            }
                            for item in list(self._recent_presets)
                        ],
                        "usage": {
                            str(key): int(value or 0)
                            for key, value in dict(self._preset_usage).items()
                            if str(key).strip()
                        },
                        "import_conflict_policy": self._preset_import_conflict_policy,
                        "custom_presets": [dict(item) for item in self._custom_presets],
                    },
                }
            }
        )

    def _workbench_evidence_boundary(self) -> dict[str, Any]:
        return {
            "evidence_source": self.WORKBENCH_EVIDENCE_SOURCE,
            "evidence_source_display": display_evidence_source(self.WORKBENCH_EVIDENCE_SOURCE),
            "evidence_state": self.WORKBENCH_EVIDENCE_STATE,
            "evidence_state_display": display_evidence_state(self.WORKBENCH_EVIDENCE_STATE),
            "acceptance_evidence": False,
            "not_real_acceptance_evidence": True,
            "acceptance_level": self.WORKBENCH_ACCEPTANCE_LEVEL,
            "promotion_state": self.WORKBENCH_PROMOTION_STATE,
        }

    def _load_measurement_core_evidence(self) -> dict[str, Any]:
        gateway = getattr(self.facade, "results_gateway", None)
        if gateway is None:
            return {}
        payload = dict(gateway.read_results_payload() or {})
        stability = dict(payload.get("multi_source_stability_evidence") or {})
        transition = dict(payload.get("state_transition_evidence") or {})
        sidecar = dict(payload.get("simulation_evidence_sidecar_bundle") or {})
        phase_coverage = dict(payload.get("measurement_phase_coverage_report") or {})
        compatibility_summary = dict(payload.get("compatibility_scan_summary") or {})
        compatibility_overview = dict(compatibility_summary.get("compatibility_overview") or {})
        compatibility_rollup = dict(
            compatibility_summary.get("compatibility_rollup")
            or compatibility_overview.get("compatibility_rollup")
            or {}
        )
        run_artifact_index = dict(payload.get("run_artifact_index") or {})
        if not stability and not transition and not sidecar and not phase_coverage:
            return {}
        stability_digest = dict(stability.get("digest") or {})
        transition_digest = dict(transition.get("digest") or {})
        phase_coverage_digest = dict(phase_coverage.get("digest") or {})
        localized_measurement_lines = build_measurement_review_digest_lines(phase_coverage)
        summary_lines = [
            humanize_review_surface_text(str(stability_digest.get("summary") or "").strip()),
            humanize_review_surface_text(str(transition_digest.get("summary") or "").strip()),
            *[str(item).strip() for item in list(localized_measurement_lines.get("summary_lines") or []) if str(item).strip()],
            str(sidecar.get("reviewer_note") or "").strip(),
        ]
        compatibility_reader_mode = str(
            compatibility_overview.get("current_reader_mode_display")
            or compatibility_summary.get("current_reader_mode_display")
            or compatibility_overview.get("current_reader_mode")
            or compatibility_summary.get("current_reader_mode")
            or ""
        ).strip()
        compatibility_status = str(
            compatibility_overview.get("compatibility_status_display")
            or compatibility_summary.get("compatibility_status_display")
            or compatibility_overview.get("compatibility_status")
            or compatibility_summary.get("compatibility_status")
            or ""
        ).strip()
        if compatibility_reader_mode or compatibility_status:
            summary_lines.append(
                "工件兼容: "
                + " | ".join(part for part in (compatibility_reader_mode, compatibility_status) if part)
            )
        if str(compatibility_overview.get("schema_contract_summary_display") or "").strip():
            summary_lines.append(str(compatibility_overview.get("schema_contract_summary_display") or "").strip())
        if str(compatibility_rollup.get("rollup_summary_display") or "").strip():
            summary_lines.append(
                t(
                    "facade.results.result_summary.artifact_compatibility_rollup",
                    value=str(compatibility_rollup.get("rollup_summary_display") or "").strip(),
                    default="兼容性 rollup：{value}",
                )
            )
        summary_lines = [line for line in summary_lines if line]
        detail_lines = [str(item).strip() for item in list(localized_measurement_lines.get("detail_lines") or []) if str(item).strip()]
        if compatibility_summary:
            compatibility_detail_lines = [
                str(item).strip()
                for item in list(compatibility_summary.get("detail_lines") or [])[:3]
                if str(item).strip()
            ]
            detail_lines.extend(compatibility_detail_lines)
            detail_lines.extend(
                str(item).strip()
                for item in list(compatibility_overview.get("detail_lines") or [])[:2]
                if str(item).strip()
            )
            if bool(compatibility_summary.get("regenerate_recommended", False)):
                detail_lines.append("建议轻量 regenerate/reindex，仅重建 reviewer/index sidecar")
        if str(compatibility_rollup.get("rollup_summary_display") or "").strip():
            detail_lines.append(
                t(
                    "facade.results.result_summary.artifact_compatibility_rollup",
                    value=str(compatibility_rollup.get("rollup_summary_display") or "").strip(),
                    default="兼容性 rollup：{value}",
                )
            )
        detail_lines = [line for line in detail_lines if line]
        boundary_lines = collect_boundary_digest_lines(
            phase_coverage,
            stability,
            transition,
            sidecar,
        )
        for item in list(compatibility_summary.get("boundary_statements") or []):
            text = str(item).strip()
            if text and text not in boundary_lines:
                boundary_lines.append(text)
        extra_boundary = str(compatibility_overview.get("non_primary_boundary_display") or "").strip()
        if extra_boundary and extra_boundary not in boundary_lines:
            boundary_lines.append(extra_boundary)
        compatibility_artifact_paths = dict(compatibility_summary.get("artifact_paths") or {})
        compatibility_entries = {
            str(entry.get("artifact_name") or ""): dict(entry)
            for entry in list(run_artifact_index.get("entries") or [])
            if isinstance(entry, dict)
        }
        return {
            "available": True,
            "summary_line": " | ".join(summary_lines) if summary_lines else t("common.none"),
            "summary_lines": summary_lines,
            "detail_lines": detail_lines,
            "boundary_lines": boundary_lines,
            "multi_source_stability_evidence": stability,
            "state_transition_evidence": transition,
            "simulation_evidence_sidecar_bundle": sidecar,
            "measurement_phase_coverage_report": phase_coverage,
            "compatibility_scan_summary": compatibility_summary,
            "compatibility_rollup": compatibility_rollup,
            "run_artifact_index": run_artifact_index,
            "compatibility_entries": compatibility_entries,
            "artifact_paths": {
                "multi_source_stability_evidence": str(
                    dict(stability.get("artifact_paths") or {}).get("multi_source_stability_evidence")
                    or gateway.run_dir / MULTI_SOURCE_STABILITY_EVIDENCE_FILENAME
                ),
                "state_transition_evidence": str(
                    dict(transition.get("artifact_paths") or {}).get("state_transition_evidence")
                    or gateway.run_dir / STATE_TRANSITION_EVIDENCE_FILENAME
                ),
                "simulation_evidence_sidecar_bundle": str(
                    dict(sidecar.get("artifact_paths") or {}).get("simulation_evidence_sidecar_bundle")
                    or gateway.run_dir / SIMULATION_EVIDENCE_SIDECAR_BUNDLE_FILENAME
                ),
                "measurement_phase_coverage_report": str(
                    dict(phase_coverage.get("artifact_paths") or {}).get("measurement_phase_coverage_report")
                    or gateway.run_dir / MEASUREMENT_PHASE_COVERAGE_REPORT_FILENAME
                ),
                "compatibility_scan_summary": str(
                    compatibility_artifact_paths.get("compatibility_scan_summary")
                    or gateway.run_dir / "compatibility_scan_summary.json"
                ),
                "run_artifact_index": str(
                    dict(run_artifact_index.get("artifact_paths") or {}).get("run_artifact_index")
                    or gateway.run_dir / "run_artifact_index.json"
                ),
            },
        }

    def _load_recognition_readiness_evidence(self) -> dict[str, Any]:
        gateway = getattr(self.facade, "results_gateway", None)
        if gateway is None:
            return {}
        payload = dict(gateway.read_results_payload() or {})
        scope_definition_pack = dict(payload.get("scope_definition_pack") or {})
        decision_rule_profile = dict(payload.get("decision_rule_profile") or {})
        reference_asset_registry = dict(payload.get("reference_asset_registry") or {})
        certificate_lifecycle_summary = dict(payload.get("certificate_lifecycle_summary") or {})
        scope_summary = dict(payload.get("scope_readiness_summary") or {})
        certificate_summary = dict(payload.get("certificate_readiness_summary") or {})
        pre_run_readiness_gate = dict(payload.get("pre_run_readiness_gate") or {})
        method_confirmation_protocol = dict(payload.get("method_confirmation_protocol") or {})
        method_confirmation_matrix = dict(payload.get("method_confirmation_matrix") or {})
        route_specific_validation_matrix = dict(payload.get("route_specific_validation_matrix") or {})
        validation_run_set = dict(payload.get("validation_run_set") or {})
        verification_digest = dict(payload.get("verification_digest") or {})
        verification_rollup = dict(payload.get("verification_rollup") or {})
        software_validation_traceability_matrix = dict(
            payload.get("software_validation_traceability_matrix") or {}
        )
        artifact_hash_registry = dict(payload.get("artifact_hash_registry") or {})
        environment_fingerprint = dict(payload.get("environment_fingerprint") or {})
        release_manifest = dict(payload.get("release_manifest") or {})
        release_scope_summary = dict(payload.get("release_scope_summary") or {})
        release_boundary_digest = dict(payload.get("release_boundary_digest") or {})
        release_evidence_pack_index = dict(payload.get("release_evidence_pack_index") or {})
        software_validation_rollup = dict(payload.get("software_validation_rollup") or {})
        uncertainty_model = dict(payload.get("uncertainty_model") or {})
        uncertainty_input_set = dict(payload.get("uncertainty_input_set") or {})
        sensitivity_coefficient_set = dict(payload.get("sensitivity_coefficient_set") or {})
        budget_case = dict(payload.get("budget_case") or {})
        uncertainty_golden_cases = dict(payload.get("uncertainty_golden_cases") or {})
        uncertainty_report_pack = dict(payload.get("uncertainty_report_pack") or {})
        uncertainty_digest = dict(payload.get("uncertainty_digest") or {})
        uncertainty_rollup = dict(payload.get("uncertainty_rollup") or {})
        uncertainty_summary = dict(payload.get("uncertainty_method_readiness_summary") or {})
        audit_summary = dict(payload.get("audit_readiness_digest") or {})
        recognition_scope_rollup = dict(payload.get("recognition_scope_rollup") or {})
        compatibility_summary = dict(payload.get("compatibility_scan_summary") or {})
        compatibility_overview = dict(compatibility_summary.get("compatibility_overview") or {})
        compatibility_rollup = dict(
            compatibility_summary.get("compatibility_rollup")
            or compatibility_overview.get("compatibility_rollup")
            or {}
        )
        payloads = {
            "scope_definition_pack": scope_definition_pack,
            "decision_rule_profile": decision_rule_profile,
            "reference_asset_registry": reference_asset_registry,
            "certificate_lifecycle_summary": certificate_lifecycle_summary,
            "scope_readiness_summary": scope_summary,
            "certificate_readiness_summary": certificate_summary,
            "pre_run_readiness_gate": pre_run_readiness_gate,
            "method_confirmation_protocol": method_confirmation_protocol,
            "method_confirmation_matrix": method_confirmation_matrix,
            "route_specific_validation_matrix": route_specific_validation_matrix,
            "validation_run_set": validation_run_set,
            "verification_digest": verification_digest,
            "verification_rollup": verification_rollup,
            "software_validation_traceability_matrix": software_validation_traceability_matrix,
            "artifact_hash_registry": artifact_hash_registry,
            "environment_fingerprint": environment_fingerprint,
            "release_manifest": release_manifest,
            "release_scope_summary": release_scope_summary,
            "release_boundary_digest": release_boundary_digest,
            "release_evidence_pack_index": release_evidence_pack_index,
            "uncertainty_model": uncertainty_model,
            "uncertainty_input_set": uncertainty_input_set,
            "sensitivity_coefficient_set": sensitivity_coefficient_set,
            "budget_case": budget_case,
            "uncertainty_golden_cases": uncertainty_golden_cases,
            "uncertainty_report_pack": uncertainty_report_pack,
            "uncertainty_digest": uncertainty_digest,
            "uncertainty_rollup": uncertainty_rollup,
            "uncertainty_method_readiness_summary": uncertainty_summary,
            "audit_readiness_digest": audit_summary,
        }
        if not any(payloads.values()):
            return {}

        summary_lines: list[str] = []
        detail_lines: list[str] = []
        boundary_lines: list[str] = []
        artifact_paths: dict[str, str] = {}
        for payload in payloads.values():
            if not payload:
                continue
            localized_lines = build_readiness_review_digest_lines(payload)
            for line in list(localized_lines.get("summary_lines") or []):
                text = str(line).strip()
                if text and text not in summary_lines:
                    summary_lines.append(text)
            for line in list(localized_lines.get("detail_lines") or []):
                text = str(line).strip()
                if text and text not in detail_lines:
                    detail_lines.append(text)
            for item in collect_boundary_digest_lines(payload):
                text = str(item).strip()
                if text and text not in boundary_lines:
                    boundary_lines.append(text)
            for label, path in dict(payload.get("artifact_paths") or {}).items():
                path_text = str(path or "").strip()
                if path_text:
                    artifact_paths[str(label)] = path_text
        software_validation_overview = str(
            software_validation_rollup.get("rollup_summary_display")
            or software_validation_rollup.get("release_manifest_summary")
            or dict(release_manifest.get("digest") or {}).get("summary")
            or ""
        ).strip()
        if software_validation_overview and software_validation_overview not in summary_lines:
            summary_lines.append(
                t(
                    "facade.results.result_summary.software_validation_overview",
                    value=software_validation_overview,
                    default=f"软件验证总览：{software_validation_overview}",
                )
            )
        traceability_completeness = str(
            software_validation_rollup.get("traceability_completeness_summary")
            or dict(software_validation_traceability_matrix.get("digest") or {}).get("current_coverage_summary")
            or ""
        ).strip()
        if traceability_completeness and traceability_completeness not in summary_lines:
            summary_lines.append(
                t(
                    "facade.results.result_summary.traceability_completeness",
                    value=traceability_completeness,
                    default=f"追溯完整度：{traceability_completeness}",
                )
            )
        audit_hash_summary = str(
            software_validation_rollup.get("hash_registry_summary")
            or dict(artifact_hash_registry.get("digest") or {}).get("summary")
            or ""
        ).strip()
        if audit_hash_summary and audit_hash_summary not in summary_lines:
            summary_lines.append(
                t(
                    "facade.results.result_summary.audit_hash_summary",
                    value=audit_hash_summary,
                    default=f"审计哈希：{audit_hash_summary}",
                )
            )
        environment_summary = str(
            software_validation_rollup.get("environment_summary")
            or environment_fingerprint.get("environment_summary")
            or dict(environment_fingerprint.get("digest") or {}).get("summary")
            or ""
        ).strip()
        if environment_summary and environment_summary not in summary_lines:
            summary_lines.append(
                t(
                    "facade.results.result_summary.environment_fingerprint_summary",
                    value=environment_summary,
                    default=f"环境指纹：{environment_summary}",
                )
            )
        release_manifest_overview = str(
            software_validation_rollup.get("release_manifest_summary")
            or dict(release_manifest.get("digest") or {}).get("summary")
            or ""
        ).strip()
        if release_manifest_overview and release_manifest_overview not in summary_lines:
            summary_lines.append(
                t(
                    "facade.results.result_summary.release_manifest_overview",
                    value=release_manifest_overview,
                    default=f"Release manifest：{release_manifest_overview}",
                )
            )
        release_linkage = " | ".join(
            [
                f"parity {str(software_validation_rollup.get('parity_status') or release_manifest.get('parity_status') or '--')}",
                f"resilience {str(software_validation_rollup.get('resilience_status') or release_manifest.get('resilience_status') or '--')}",
                f"smoke {str(software_validation_rollup.get('smoke_status') or release_manifest.get('smoke_status') or '--')}",
            ]
        ).strip()
        if release_linkage and release_linkage not in detail_lines:
            detail_lines.append(
                t(
                    "facade.results.result_summary.release_test_linkage",
                    value=release_linkage,
                    default=f"验证联动：{release_linkage}",
                )
            )
        for extra_payload in (
            software_validation_traceability_matrix,
            artifact_hash_registry,
            environment_fingerprint,
            release_manifest,
            release_scope_summary,
            release_boundary_digest,
            release_evidence_pack_index,
        ):
            for item in collect_boundary_digest_lines(extra_payload):
                text = str(item).strip()
                if text and text not in boundary_lines:
                    boundary_lines.append(text)
            for label, path in dict(extra_payload.get("artifact_paths") or {}).items():
                path_text = str(path or "").strip()
                if path_text:
                    artifact_paths[str(label)] = path_text
        if software_validation_rollup or release_manifest:
            software_validation_boundary = t(
                "facade.results.result_summary.software_validation_boundary",
                default="软件验证边界：仅供审阅 / 仅限仿真 / 不是真实验收证据 / 非 formal claim",
            )
            if software_validation_boundary not in boundary_lines:
                boundary_lines.append(software_validation_boundary)
        if compatibility_summary:
            compatibility_reader_mode = str(
                compatibility_overview.get("current_reader_mode_display")
                or compatibility_summary.get("current_reader_mode_display")
                or compatibility_overview.get("current_reader_mode")
                or compatibility_summary.get("current_reader_mode")
                or ""
            ).strip()
            compatibility_status = str(
                compatibility_overview.get("compatibility_status_display")
                or compatibility_summary.get("compatibility_status_display")
                or compatibility_overview.get("compatibility_status")
                or compatibility_summary.get("compatibility_status")
                or ""
            ).strip()
            if compatibility_reader_mode or compatibility_status:
                summary_lines.append(
                    "工件兼容: "
                    + " | ".join(part for part in (compatibility_reader_mode, compatibility_status) if part)
                )
            if str(compatibility_overview.get("schema_contract_summary_display") or "").strip():
                summary_lines.append(str(compatibility_overview.get("schema_contract_summary_display") or "").strip())
            if str(compatibility_rollup.get("rollup_summary_display") or "").strip():
                summary_lines.append(
                    t(
                        "facade.results.result_summary.artifact_compatibility_rollup",
                        value=str(compatibility_rollup.get("rollup_summary_display") or "").strip(),
                        default="兼容性 rollup：{value}",
                    )
                )
            for line in list(compatibility_summary.get("detail_lines") or [])[:2]:
                text = str(line).strip()
                if text and text not in detail_lines:
                    detail_lines.append(text)
            for line in list(compatibility_overview.get("detail_lines") or [])[:1]:
                text = str(line).strip()
                if text and text not in detail_lines:
                    detail_lines.append(text)
            if str(compatibility_rollup.get("rollup_summary_display") or "").strip():
                detail_lines.append(
                    t(
                        "facade.results.result_summary.artifact_compatibility_rollup",
                        value=str(compatibility_rollup.get("rollup_summary_display") or "").strip(),
                        default="兼容性 rollup：{value}",
                    )
                )
            for item in list(compatibility_summary.get("boundary_statements") or []):
                text = str(item).strip()
                if text and text not in boundary_lines:
                    boundary_lines.append(text)
            extra_boundary = str(compatibility_overview.get("non_primary_boundary_display") or "").strip()
            if extra_boundary and extra_boundary not in boundary_lines:
                boundary_lines.append(extra_boundary)
            for label, path in dict(compatibility_summary.get("artifact_paths") or {}).items():
                path_text = str(path or "").strip()
                if path_text:
                    artifact_paths[str(label)] = path_text
        if recognition_scope_rollup:
            labeled_rollup = str(recognition_scope_rollup.get("rollup_summary_display") or "").strip()
            if labeled_rollup:
                summary_lines.append(
                    t(
                        "facade.results.result_summary.recognition_scope_rollup",
                        value=labeled_rollup,
                        default="范围/规则 rollup：{value}",
                    )
                )
            for line in list(recognition_scope_rollup.get("summary_lines") or []):
                text = str(line).strip()
                if text and text not in summary_lines:
                    summary_lines.append(text)
            for line in list(recognition_scope_rollup.get("detail_lines") or []):
                text = str(line).strip()
                if text and text not in detail_lines:
                    detail_lines.append(text)
            extra_boundary = str(recognition_scope_rollup.get("conformity_boundary_display") or "").strip()
            if extra_boundary and extra_boundary not in boundary_lines:
                boundary_lines.append(extra_boundary)

        return {
            "available": True,
            "summary_line": " | ".join(summary_lines) if summary_lines else t("common.none"),
            "summary_lines": summary_lines,
            "detail_lines": detail_lines,
            "boundary_lines": boundary_lines,
            "artifact_paths": artifact_paths,
            "recognition_scope_rollup": recognition_scope_rollup,
            "compatibility_rollup": compatibility_rollup,
            "verification_rollup": verification_rollup,
            "software_validation_rollup": software_validation_rollup,
            "compatibility_scan_summary": compatibility_summary,
            **payloads,
        }

    @staticmethod
    def _review_lines(value: Any) -> list[str]:
        if isinstance(value, (list, tuple)):
            return [str(item).strip() for item in value if str(item).strip()]
        text = str(value or "").strip()
        return [text] if text else []

    def _build_qc_review_summary(self, analytics_summary_payload: dict[str, Any]) -> dict[str, Any]:
        unified_review_summary = dict(analytics_summary_payload.get("unified_review_summary", {}) or {})
        qc_summary = dict(unified_review_summary.get("qc_summary", {}) or {})
        qc_overview = dict(analytics_summary_payload.get("qc_overview", {}) or {})
        reviewer_digest = dict(qc_overview.get("reviewer_digest", {}) or {})
        run_gate = dict(qc_overview.get("run_gate", {}) or {})
        point_gate = dict(qc_overview.get("point_gate_summary", {}) or {})
        decision_counts = dict(qc_overview.get("decision_counts", {}) or {})
        route_decision_breakdown = dict(qc_overview.get("route_decision_breakdown", {}) or {})
        reject_reason_taxonomy = list(qc_overview.get("reject_reason_taxonomy") or [])
        failed_check_taxonomy = list(qc_overview.get("failed_check_taxonomy") or [])
        reviewer_card = dict(qc_overview.get("reviewer_card") or qc_summary.get("reviewer_card") or {})
        if not reviewer_card or not self._review_lines(reviewer_card.get("lines")):
            reviewer_card = build_qc_reviewer_card(
                reviewer_digest=reviewer_digest,
                run_gate=run_gate,
                point_gate_summary=point_gate,
                decision_counts=decision_counts,
                route_decision_breakdown=route_decision_breakdown,
                reject_reason_taxonomy=reject_reason_taxonomy,
                failed_check_taxonomy=failed_check_taxonomy,
                boundary_note=t(
                    "results.review_center.detail.qc_boundary",
                    default="证据边界: 仅供 simulation/offline/headless 审阅，不代表 real acceptance evidence。",
                ),
            )
        review_sections = [
            dict(item)
            for item in list(qc_overview.get("review_sections") or qc_summary.get("review_sections") or reviewer_card.get("sections") or [])
            if isinstance(item, dict)
        ]
        lines = self._review_lines(qc_summary.get("lines") or reviewer_card.get("lines"))
        summary = str(qc_summary.get("summary") or reviewer_card.get("summary") or "").strip()
        if not lines:
            lines = self._review_lines(reviewer_digest.get("lines"))
            if not lines:
                fallback_summary = str(reviewer_digest.get("summary") or "").strip()
                if fallback_summary:
                    lines = [fallback_summary]
        if not lines:
            lines = [
                t(
                    "results.review_center.detail.workbench_qc_note",
                    default="当前工作台证据未生成独立质控门禁，仅提供仿真/离线诊断摘要。",
                )
            ]
        if not summary:
            summary = str(reviewer_card.get("summary") or "").strip() or lines[0]
        boundary_note = t("results.review_center.disclaimer")
        if boundary_note not in lines:
            lines.append(boundary_note)
        flagged_routes = ", ".join(str(item) for item in list(point_gate.get("flagged_routes") or []) if str(item).strip()) or "--"
        top_reject_reason = str((reject_reason_taxonomy[0] or {}).get("code") or "--") if reject_reason_taxonomy else "--"
        top_failed_check = str((failed_check_taxonomy[0] or {}).get("code") or "--") if failed_check_taxonomy else "--"
        review_card_lines = self._review_lines(reviewer_card.get("lines")) or [
            f"{t('results.review_center.detail.qc_summary', default='质控摘要')}: {summary}",
            f"{t('results.review_center.detail.qc_reviewer_card', default='审阅卡片')}: {summary}",
            (
                f"{t('results.review_center.detail.qc_run_gate', default='运行门禁')}: "
                f"{str(run_gate.get('status') or '--')} | "
                f"{t('results.review_center.detail.qc_gate_reason', default='原因')}: {str(run_gate.get('reason') or '--')}"
            ),
            (
                f"{t('results.review_center.detail.qc_point_gate', default='点级门禁')}: "
                f"{str(point_gate.get('status') or '--')} | "
                f"{t('results.review_center.detail.qc_flagged_routes', default='关注路由')}: {flagged_routes}"
            ),
            (
                f"{t('results.review_center.detail.qc_decision_breakdown', default='结果分级')}: "
                f"{t('pages.qc.level.pass', default='通过')} {int(decision_counts.get('pass', 0) or 0)} / "
                f"{t('pages.qc.level.warn', default='预警')} {int(decision_counts.get('warn', 0) or 0)} / "
                f"{t('pages.qc.level.reject', default='拒绝')} {int(decision_counts.get('reject', 0) or 0)} / "
                f"{t('pages.qc.level.skipped', default='跳过')} {int(decision_counts.get('skipped', 0) or 0)}"
            ),
            (
                f"{t('results.review_center.detail.qc_top_reject_reason', default='主要拒绝原因')}: {top_reject_reason} | "
                f"{t('results.review_center.detail.qc_top_failed_check', default='失败检查')}: {top_failed_check}"
            ),
            t(
                "results.review_center.detail.qc_boundary",
                default="证据边界: 仅供 simulation/offline 审阅，不代表 real acceptance evidence。",
            ),
        ]
        evidence_source = str(analytics_summary_payload.get("evidence_source") or self.WORKBENCH_EVIDENCE_SOURCE).strip()
        if evidence_source.lower() in {"simulated", "simulated_protocol"}:
            evidence_source = self.WORKBENCH_EVIDENCE_SOURCE
        qc_evidence_section = build_qc_evidence_section(
            reviewer_digest=reviewer_digest,
            reviewer_card=reviewer_card,
            run_gate=run_gate,
            point_gate_summary=point_gate,
            decision_counts=decision_counts,
            route_decision_breakdown=route_decision_breakdown,
            reject_reason_taxonomy=reject_reason_taxonomy,
            failed_check_taxonomy=failed_check_taxonomy,
            review_sections=review_sections,
            summary_override=summary,
            lines_override=[str(item).strip() for item in review_card_lines if str(item).strip()],
            evidence_source=evidence_source,
            evidence_state=self.WORKBENCH_EVIDENCE_STATE,
            not_real_acceptance_evidence=bool(analytics_summary_payload.get("not_real_acceptance_evidence", True)),
            acceptance_level=self.WORKBENCH_ACCEPTANCE_LEVEL,
            promotion_state=self.WORKBENCH_PROMOTION_STATE,
        )
        return {
            "summary": str(qc_evidence_section.get("summary") or summary),
            "lines": [str(item).strip() for item in lines if str(item).strip()],
            "review_card_lines": [
                str(item).strip() for item in list(qc_evidence_section.get("review_card_lines") or review_card_lines) if str(item).strip()
            ],
            "reviewer_card": dict(qc_evidence_section.get("reviewer_card") or reviewer_card),
            "review_sections": [
                dict(item) for item in list(qc_evidence_section.get("sections") or review_sections) if isinstance(item, dict)
            ],
            "cards": [dict(item) for item in list(qc_evidence_section.get("cards") or []) if isinstance(item, dict)],
            "evidence_section": {
                "title": t("results.review_center.detail.qc_summary", default="质控摘要"),
                "summary": summary,
                "lines": [str(item).strip() for item in review_card_lines if str(item).strip()],
                "sections": review_sections,
            },
            "evidence_section": dict(qc_evidence_section),
            "run_gate": run_gate,
            "point_gate_summary": point_gate,
            "decision_counts": decision_counts,
            "route_decision_breakdown": route_decision_breakdown,
            "reject_reason_taxonomy": [dict(item) for item in reject_reason_taxonomy if isinstance(item, dict)],
            "failed_check_taxonomy": [dict(item) for item in failed_check_taxonomy if isinstance(item, dict)],
            "evidence_source": evidence_source,
            "evidence_state": self.WORKBENCH_EVIDENCE_STATE,
            "not_real_acceptance_evidence": bool(analytics_summary_payload.get("not_real_acceptance_evidence", True)),
            "acceptance_level": self.WORKBENCH_ACCEPTANCE_LEVEL,
            "promotion_state": self.WORKBENCH_PROMOTION_STATE,
        }

    def _config_safety_snapshot(self) -> tuple[dict[str, Any], dict[str, Any]]:
        cached_config_safety = dict(getattr(self.facade.config, "_config_safety", {}) or {})
        if cached_config_safety:
            config_safety = hydrate_step2_config_safety_summary(cached_config_safety)
        else:
            config_safety = summarize_step2_config_safety(self.facade.config)
        execution_gate_override = dict(getattr(self.facade.config, "_step2_execution_gate", {}) or {})
        if execution_gate_override:
            review_payload = copy.deepcopy(config_safety)
            review_payload["execution_gate"] = {
                **dict(config_safety.get("execution_gate") or {}),
                **execution_gate_override,
            }
            config_safety_review = build_step2_config_safety_review(review_payload)
        else:
            config_safety_review = build_step2_config_safety_review(config_safety)
        return config_safety, config_safety_review

    @staticmethod
    def _config_governance_payload(
        config_safety: dict[str, Any],
        config_safety_review: dict[str, Any],
    ) -> dict[str, Any]:
        handoff = build_step2_config_governance_handoff(config_safety_review or config_safety)
        return {
            "config_governance_handoff": handoff,
            "config_classification": str(handoff.get("classification") or config_safety.get("classification") or ""),
            "config_badge_ids": list(handoff.get("badge_ids") or config_safety.get("badge_ids") or []),
            "config_inventory_summary": str(
                handoff.get("inventory_summary") or config_safety_review.get("inventory_summary") or "--"
            ),
            "blocked_reasons": [str(item).strip() for item in list(handoff.get("blocked_reasons") or []) if str(item).strip()],
            "blocked_reason_details": [
                dict(item) for item in list(handoff.get("blocked_reason_details") or []) if isinstance(item, dict)
            ],
            "devices_with_real_ports": [
                dict(item) for item in list(handoff.get("devices_with_real_ports") or []) if isinstance(item, dict)
            ],
            "enabled_engineering_flags": [
                dict(item) for item in list(handoff.get("enabled_engineering_flags") or []) if isinstance(item, dict)
            ],
            "execution_gate": dict(handoff.get("execution_gate") or {}),
        }

    def _point_taxonomy_snapshot(self) -> dict[str, Any]:
        summary_path = Path(self.facade.results_gateway.run_dir) / "summary.json"
        summary_payload = self._load_json_dict(summary_path)
        stats = dict(summary_payload.get("stats", {}) or {})
        point_taxonomy_summary = dict(stats.get("point_taxonomy_summary") or {})
        if point_taxonomy_summary:
            return point_taxonomy_summary
        point_taxonomy_summary = dict(summary_payload.get("point_taxonomy_summary") or {})
        if point_taxonomy_summary:
            return point_taxonomy_summary
        point_summaries = [
            dict(item)
            for item in list(stats.get("point_summaries") or [])
            if isinstance(item, dict)
        ]
        return build_point_taxonomy_handoff(point_summaries)

    @staticmethod
    def _point_taxonomy_lines(point_taxonomy_summary: dict[str, Any]) -> list[str]:
        payload = dict(point_taxonomy_summary or {})
        rendered: list[str] = []
        taxonomy_rows = (
            ("pressure_summary", "facade.results.result_summary.taxonomy_pressure", "压力语义：{value}"),
            ("pressure_mode_summary", "facade.results.result_summary.taxonomy_pressure_mode", "压力模式：{value}"),
            (
                "pressure_target_label_summary",
                "facade.results.result_summary.taxonomy_pressure_target_label",
                "压力目标标签：{value}",
            ),
            ("flush_gate_summary", "facade.results.result_summary.taxonomy_flush", "冲洗门禁：{value}"),
            ("preseal_summary", "facade.results.result_summary.taxonomy_preseal", "前封气：{value}"),
            ("postseal_summary", "facade.results.result_summary.taxonomy_postseal", "后封气：{value}"),
            ("stale_gauge_summary", "facade.results.result_summary.taxonomy_stale_gauge", "压力参考陈旧：{value}"),
        )
        for field_name, key, default_template in taxonomy_rows:
            value = str(payload.get(field_name) or "").strip()
            if not value:
                continue
            if field_name == "pressure_mode_summary" and value == str(payload.get("pressure_summary") or "").strip():
                continue
            if field_name == "pressure_target_label_summary" and value == str(payload.get("pressure_summary") or "").strip():
                continue
            rendered.append(t(key, value=value, default=default_template.format(value=value)))
        return rendered

    def _default_display_profile_context(self) -> dict[str, Any]:
        return self._normalize_display_profile_context({})

    def _display_monitor_class(self, screen_width: int, screen_height: int) -> str:
        if screen_width >= 2560 or screen_height >= 1440:
            return "wide_monitor"
        if screen_width <= 1600 or screen_height <= 900:
            return "compact_monitor"
        return "standard_monitor"

    @staticmethod
    def _display_resolution_class(screen_width: int, screen_height: int) -> str:
        aspect_ratio = float(screen_width) / max(1.0, float(screen_height))
        if screen_height >= 2160 or (screen_width >= 3840 and aspect_ratio < DeviceWorkbenchController.DISPLAY_PROFILE_WIDE_ASPECT_RATIO):
            return "ultra_hd"
        if screen_width >= 3440 or aspect_ratio >= DeviceWorkbenchController.DISPLAY_PROFILE_WIDE_ASPECT_RATIO:
            return "ultrawide_resolution"
        if screen_width >= 2560 or screen_height >= 1440:
            return "wide_resolution"
        if screen_width >= 1920 or screen_height >= 1080:
            return "full_hd"
        if screen_width >= 1600 or screen_height >= 900:
            return "mid_resolution"
        return "compact_resolution"

    @staticmethod
    def _display_window_class(window_width: int, window_height: int) -> str:
        if window_width >= 1880 and window_height >= 980:
            return "wide_window"
        if window_width >= 1600 and window_height >= 900:
            return "standard_window"
        return "compact_window"

    def _resolve_auto_display_family(
        self,
        *,
        screen_width: int,
        screen_height: int,
        window_width: int,
        window_height: int,
    ) -> str:
        resolution_class = self._display_resolution_class(screen_width, screen_height)
        window_class = self._display_window_class(window_width, window_height)
        monitor_class = self._display_monitor_class(screen_width, screen_height)
        if resolution_class == "ultra_hd":
            return "4k_standard" if window_class in {"standard_window", "wide_window"} else "4k_compact"
        if resolution_class == "ultrawide_resolution":
            return "ultrawide_standard" if window_class in {"standard_window", "wide_window"} else "ultrawide_compact"
        if resolution_class == "wide_resolution":
            return "1440p_standard" if window_class in {"standard_window", "wide_window"} else "1440p_compact"
        if resolution_class == "full_hd" and window_class in {"standard_window", "wide_window"}:
            return "1080p_standard"
        if window_class == "wide_window":
            return "1080p_standard"
        if monitor_class == "wide_monitor":
            return "1080p_standard"
        return "1080p_compact"

    @staticmethod
    def _display_resolution_bucket(resolved: str) -> str:
        family = str(resolved or "1080p_standard").split("_", 1)[0] or "1080p"
        return family if family in {"1080p", "1440p", "4k", "ultrawide"} else "1080p"

    @staticmethod
    def _display_multi_monitor_ready_hint(family: str) -> str:
        return "future_multi_monitor_ready" if str(family or "") in {"1440p", "4k", "ultrawide"} else "single_monitor_baseline"

    def _display_profile_auto_reason(
        self,
        *,
        selected: str,
        resolution_class: str,
        window_class: str,
        monitor_class: str,
        resolved: str,
    ) -> str:
        if selected == "dense_1080p":
            return "manual_dense_1080p"
        if selected == "standard_display":
            return "manual_standard_display"
        if str(resolved or "").startswith("4k_"):
            return "simulated_4k_canvas"
        if str(resolved or "").startswith("ultrawide_"):
            return "simulated_ultrawide_canvas"
        if str(resolved or "").startswith("1440p_"):
            return "simulated_1440p_canvas"
        if resolved == "1080p_standard":
            if resolution_class == "wide_resolution":
                return "wide_resolution"
            if resolution_class == "full_hd" and window_class in {"standard_window", "wide_window"}:
                return "default_1080p"
            if window_class == "wide_window":
                return "wide_or_large_window"
            if monitor_class == "wide_monitor":
                return "wide_or_large_window"
            return "standard_density"
        if monitor_class == "compact_monitor":
            return "compact_monitor"
        if window_class == "compact_window":
            return "compact_window"
        return "balanced_default"

    def _normalize_display_profile_context(self, payload: dict[str, Any]) -> dict[str, Any]:
        context = dict(self.DISPLAY_PROFILE_DEFAULT_CONTEXT)
        for key in ("screen_width", "screen_height", "window_width", "window_height"):
            try:
                parsed = int(dict(payload or {}).get(key, context[key]) or context[key])
            except Exception:
                parsed = int(context[key])
            context[key] = max(1, parsed)
        selected = str(dict(payload or {}).get("selected") or self._display_profile or "auto").strip().lower()
        if selected not in self.DISPLAY_PROFILES:
            selected = "auto"
        resolved = (
            self._resolve_auto_display_family(
                screen_width=int(context["screen_width"]),
                screen_height=int(context["screen_height"]),
                window_width=int(context["window_width"]),
                window_height=int(context["window_height"]),
            )
            if selected == "auto"
            else self.DISPLAY_PROFILE_FAMILIES.get(selected, "1080p_compact")
        )
        monitor_class = self._display_monitor_class(
            int(context["screen_width"]),
            int(context["screen_height"]),
        )
        resolution_class = self._display_resolution_class(
            int(context["screen_width"]),
            int(context["screen_height"]),
        )
        window_class = self._display_window_class(
            int(context["window_width"]),
            int(context["window_height"]),
        )
        family = self._display_resolution_bucket(resolved)
        layout_hint = (
            "compact" if str(resolved or "").endswith("_compact") else "standard"
            if selected == "auto"
            else self.DISPLAY_PROFILE_LAYOUT_HINTS.get(selected, "compact" if str(resolved or "").endswith("_compact") else "standard")
        )
        auto_reason = self._display_profile_auto_reason(
            selected=selected,
            resolution_class=resolution_class,
            window_class=window_class,
            monitor_class=monitor_class,
            resolved=resolved,
        )
        resolution_bucket = self._display_resolution_bucket(resolved)
        multi_monitor_ready_hint = self._display_multi_monitor_ready_hint(resolution_bucket)
        aspect_ratio = f"{int(context['screen_width']) / max(1, int(context['screen_height'])):.2f}"
        screen_area = int(context["screen_width"]) * int(context["screen_height"])
        window_area = int(context["window_width"]) * int(context["window_height"])
        mapping_summary = " | ".join(
            [
                f"{selected}->{resolved}",
                resolution_bucket,
                f"{context['screen_width']}x{context['screen_height']}",
                monitor_class,
                window_class,
                multi_monitor_ready_hint,
            ]
        )
        profile_summary = t(
            "pages.devices.workbench.display_profile_hint",
            profile=t(f"pages.devices.workbench.display_profile_profile.{resolved}", default=resolved),
            family=t(f"pages.devices.workbench.display_profile_family.{family}", default=family),
            resolution=f"{context['screen_width']}x{context['screen_height']}",
            layout=t(f"pages.devices.workbench.layout.{layout_hint}", default=layout_hint),
            monitor=t(f"pages.devices.workbench.display_profile_monitor.{monitor_class}", default=monitor_class),
            default=f"{resolved} | {family} | {context['screen_width']}x{context['screen_height']} | {layout_hint} | {monitor_class}",
        )
        return {
            "strategy_version": self.DISPLAY_PROFILE_STRATEGY_VERSION,
            "selected": selected,
            "resolved": resolved,
            "family": family,
            "resolution_bucket": resolution_bucket,
            "resolution": f"{context['screen_width']}x{context['screen_height']}",
            "resolution_class": resolution_class,
            "window_class": window_class,
            "layout_hint": layout_hint,
            "monitor_class": monitor_class,
            "auto_reason": auto_reason,
            "multi_monitor_ready_hint": multi_monitor_ready_hint,
            "aspect_ratio": aspect_ratio,
            "screen_area": screen_area,
            "window_area": window_area,
            "mapping_summary": mapping_summary,
            "profile_summary": profile_summary,
            "screen_width": int(context["screen_width"]),
            "screen_height": int(context["screen_height"]),
            "window_width": int(context["window_width"]),
            "window_height": int(context["window_height"]),
        }

    @classmethod
    def _normalize_preset_import_conflict_policy(cls, value: Any) -> str:
        candidate = str(value or "rename").strip().lower()
        return candidate if candidate in cls.PRESET_IMPORT_CONFLICT_POLICIES else "rename"

    @staticmethod
    def _normalize_preset_keys(values: Any, *, limit: int) -> list[str]:
        normalized: list[str] = []
        seen: set[str] = set()
        for item in list(values or []):
            key = str(item or "").strip().lower()
            if not key or key in seen:
                continue
            seen.add(key)
            normalized.append(key)
            if len(normalized) >= max(1, int(limit)):
                break
        return normalized

    @staticmethod
    def _normalize_device_kind(device_kind: Any) -> str:
        return "pressure_gauge" if str(device_kind or "").strip().lower() in {"pressure", "pressure_gauge"} else str(device_kind or "").strip().lower()

    def _preset_key(self, device_kind: Any, preset_id: Any) -> str:
        normalized_kind = self._normalize_device_kind(device_kind)
        return f"{normalized_kind}:{str(preset_id or '').strip().lower()}"

    def _builtin_preset_definitions(self, device_kind: str) -> tuple[tuple[str, str, str, bool], ...]:
        normalized_kind = self._normalize_device_kind(device_kind)
        definitions = {
            "analyzer": (
                ("mode2_active_read", "切到 MODE2 并读一帧", "切到 MODE2、主动发送并读取最近一帧", False),
                ("partial_frame", "半帧故障", "注入半帧故障并读取最近一帧", True),
            ),
            "pace": (
                ("vent_on", "打开 vent", "打开 vent 状态", False),
                ("vent_off", "关闭 vent", "关闭 vent 状态", False),
                ("set_pressure", "设置目标压力", "设置目标压力并读取当前压力", False),
                ("unsupported_header", "协议头不支持", "注入 unsupported header 并查询错误", True),
            ),
            "grz": (
                ("stable", "正常稳定", "恢复稳定状态并抓取快照", False),
                ("humidity_static_fault", "湿度不变", "注入湿度静止故障并抓取快照", True),
                ("timeout", "超时", "注入 timeout 并抓取快照", True),
            ),
            "chamber": (
                ("reach_target", "到温", "恢复稳定到温并启动运行", False),
                ("stalled", "卡滞", "注入 stalled 并启动运行", True),
                ("alarm", "报警", "注入 alarm 状态", True),
            ),
            "relay": (
                ("all_off", "全关", "关闭全部继电器通道", False),
                ("route_h2o", "H2O 路", "切到 H2O 路预置继电器状态", False),
                ("route_co2", "CO2 路", "切到 CO2 路预置继电器状态", False),
                ("stuck_channel", "通道卡住", "注入 stuck_channel 并写入目标通道", True),
            ),
            "thermometer": (
                ("stable", "稳定参考", "恢复稳定参考状态", False),
                ("stale", "参考陈旧", "注入 stale 状态", True),
                ("drift", "参考漂移", "注入 drift 状态", True),
            ),
            "pressure_gauge": (
                ("stable", "稳定参考", "恢复稳定压力参考状态", False),
                ("wrong_unit", "单位错误", "注入 wrong_unit_configuration", True),
                ("no_response", "无响应", "注入 no_response", True),
            ),
        }
        return tuple(definitions.get(normalized_kind, ()))

    def _builtin_preset_payloads(self, device_kind: str) -> list[dict[str, Any]]:
        normalized_kind = self._normalize_device_kind(device_kind)
        group_id = self._preset_group_id(normalized_kind)
        group_display = self._preset_group_display(group_id)
        payloads: list[dict[str, Any]] = []
        for preset_id, label, description, is_fault_injection in self._builtin_preset_definitions(normalized_kind):
            steps = [
                {
                    "device_kind": normalized_kind,
                    "preset_id": preset_id,
                    "label": t(
                        f"pages.devices.workbench.preset.{normalized_kind}.{preset_id}.label",
                        default=label,
                    ),
                }
            ]
            payloads.append(
                {
                    "id": preset_id,
                    "label": t(
                        f"pages.devices.workbench.preset.{normalized_kind}.{preset_id}.label",
                        default=label,
                    ),
                    "description": t(
                        f"pages.devices.workbench.preset.{normalized_kind}.{preset_id}.description",
                        default=description,
                    ),
                    "device_kind": normalized_kind,
                    "group_id": group_id,
                    "group_display": group_display,
                    "is_fault_injection": bool(is_fault_injection),
                    "is_featured": str(preset_id) in set(self.FEATURED_PRESETS.get(normalized_kind, ())),
                    "is_custom": False,
                    "source_kind": "built_in",
                    "source_display": t("pages.devices.workbench.preset_center.source_builtin", default="内置"),
                    "step_count": 1,
                    "steps": steps,
                    **self._preset_fake_capability_payload(
                        device_kind=normalized_kind,
                        steps=steps,
                        is_fault_injection=bool(is_fault_injection),
                    ),
                }
            )
        return payloads

    def _builtin_preset_payload(self, device_kind: str, preset_id: str) -> dict[str, Any]:
        normalized_kind = self._normalize_device_kind(device_kind)
        for item in self._builtin_preset_payloads(normalized_kind):
            if str(item.get("id") or "") == str(preset_id):
                return dict(item)
        return {}

    def _is_builtin_preset(self, device_kind: str, preset_id: str) -> bool:
        normalized_kind = self._normalize_device_kind(device_kind)
        return any(str(item[0]) == str(preset_id) for item in self._builtin_preset_definitions(normalized_kind))

    def _normalize_custom_presets(self, items: list[dict[str, Any]]) -> list[dict[str, Any]]:
        normalized: list[dict[str, Any]] = []
        seen: set[str] = set()
        for raw in list(items or []):
            payload = self._normalize_custom_preset_payload(raw)
            if not payload:
                continue
            preset_id = str(payload.get("id") or "")
            if preset_id in seen:
                continue
            seen.add(preset_id)
            normalized.append(payload)
        return normalized

    @staticmethod
    def _normalize_positive_int(value: Any, *, default: int = 1) -> int:
        try:
            parsed = int(value or default)
        except Exception:
            parsed = int(default)
        return max(1, parsed)

    def _preset_origin_display(self, origin: Any) -> str:
        origin_id = str(origin or "local_editor").strip().lower() or "local_editor"
        return t(
            f"pages.devices.workbench.preset_center.origin.{origin_id}",
            default=origin_id,
        )

    def _preset_import_conflict_policy_display(self, policy: Any) -> str:
        policy_id = self._normalize_preset_import_conflict_policy(policy)
        return t(
            f"pages.devices.workbench.preset_center.manager.conflict_policy.{policy_id}",
            default=policy_id,
        )

    @staticmethod
    def _preset_label_key(label: Any) -> str:
        return " ".join(str(label or "").strip().lower().split())

    def _preset_import_source_label(self, payload: dict[str, Any]) -> str:
        exported_at = str(payload.get("exported_at") or "").strip()
        scope = str(payload.get("scope") or "selected").strip().lower() or "selected"
        if exported_at:
            return t(
                "pages.devices.workbench.preset_center.manager.import_source_timestamp",
                exported_at=exported_at,
                scope=scope,
                default=f"{scope}:{exported_at}",
            )
        return t(
            "pages.devices.workbench.preset_center.manager.import_source_scope",
            scope=scope,
            default=f"scope:{scope}",
        )

    def _preset_import_renamed_label(self, label: str, index: int) -> str:
        return t(
            "pages.devices.workbench.preset_center.manager.import_renamed_label",
            label=label,
            index=index,
            default=f"{label} Imported {index}",
        )

    def _ensure_unique_preset_label(
        self,
        label: str,
        *,
        device_kind: str,
        existing_items: list[dict[str, Any]],
        ignore_id: str = "",
    ) -> str:
        base_label = str(label or "").strip() or "custom_preset"
        existing_keys = {
            self._preset_label_key(item.get("label"))
            for item in existing_items
            if self._normalize_device_kind(item.get("device_kind")) == self._normalize_device_kind(device_kind)
            and str(item.get("id") or "") != str(ignore_id or "")
        }
        candidate = base_label
        index = 2
        while self._preset_label_key(candidate) in existing_keys:
            candidate = self._preset_import_renamed_label(base_label, index)
            index += 1
        return candidate

    def _preset_conflict_target(
        self,
        item: dict[str, Any],
        existing_items: list[dict[str, Any]],
    ) -> dict[str, Any]:
        candidate_id = str(item.get("id") or "")
        candidate_device_kind = self._normalize_device_kind(item.get("device_kind"))
        candidate_label_key = self._preset_label_key(item.get("label"))
        for existing in existing_items:
            if str(existing.get("id") or "") == candidate_id:
                return dict(existing)
        for existing in existing_items:
            if self._normalize_device_kind(existing.get("device_kind")) != candidate_device_kind:
                continue
            if self._preset_label_key(existing.get("label")) == candidate_label_key:
                return dict(existing)
        return {}

    def _preset_metadata_payload(self, payload: dict[str, Any]) -> dict[str, Any]:
        metadata = dict(payload.get("metadata", {}) or {})
        schema_version = str(
            payload.get("schema_version")
            or metadata.get("schema_version")
            or self.PRESET_DEFINITION_SCHEMA
        ).strip() or self.PRESET_DEFINITION_SCHEMA
        preset_version = self._normalize_positive_int(
            payload.get("preset_version") or metadata.get("preset_version"),
            default=1,
        )
        origin = str(
            payload.get("origin")
            or metadata.get("origin")
            or "local_editor"
        ).strip().lower() or "local_editor"
        source_ref = str(
            payload.get("source_ref")
            or metadata.get("source_ref")
            or ""
        ).strip()
        imported_from = str(
            payload.get("imported_from")
            or metadata.get("imported_from")
            or ""
        ).strip()
        created_at = str(
            payload.get("created_at")
            or metadata.get("created_at")
            or ""
        ).strip()
        updated_at = str(
            payload.get("updated_at")
            or metadata.get("updated_at")
            or ""
        ).strip()
        sharing_scope = str(
            payload.get("sharing_scope")
            or metadata.get("sharing_scope")
            or "local_reserved"
        ).strip().lower() or "local_reserved"
        return {
            "schema_version": schema_version,
            "preset_version": preset_version,
            "origin": origin,
            "origin_display": self._preset_origin_display(origin),
            "source_ref": source_ref,
            "imported_from": imported_from,
            "sharing_scope": sharing_scope,
            "created_at": created_at,
            "updated_at": updated_at,
        }

    def _preset_metadata_summary(self, payload: dict[str, Any]) -> str:
        metadata = self._preset_metadata_payload(payload)
        return t(
            "pages.devices.workbench.preset_center.metadata_line",
            version=metadata.get("preset_version", 1),
            schema=metadata.get("schema_version", self.PRESET_DEFINITION_SCHEMA),
            origin=metadata.get("origin_display", self._preset_origin_display("local_editor")),
            created=metadata.get("created_at") or t("common.none"),
            updated=metadata.get("updated_at") or t("common.none"),
            default=(
                f"v{metadata.get('preset_version', 1)} | {metadata.get('schema_version', self.PRESET_DEFINITION_SCHEMA)}"
                f" | {metadata.get('origin_display', self._preset_origin_display('local_editor'))}"
                f" | {metadata.get('created_at') or t('common.none')}"
                f" | {metadata.get('updated_at') or t('common.none')}"
            ),
        )

    def _preset_fake_capability_display(self, capability_id: str) -> str:
        return t(
            f"pages.devices.workbench.preset_center.capability.{str(capability_id or '').strip().lower()}",
            default=str(capability_id or "").strip().lower() or t("common.none"),
        )

    def _preset_fake_capability_payload(
        self,
        *,
        device_kind: str,
        steps: list[dict[str, Any]],
        is_fault_injection: bool,
    ) -> dict[str, Any]:
        capability_ids: list[str] = []
        seen: set[str] = set()

        def _remember(capability_id: str) -> None:
            normalized = str(capability_id or "").strip().lower()
            if not normalized or normalized in seen:
                return
            seen.add(normalized)
            capability_ids.append(normalized)

        normalized_kind = self._normalize_device_kind(device_kind)
        if normalized_kind:
            _remember(normalized_kind)
        for step in list(steps or []):
            step_device = self._normalize_device_kind(step.get("device_kind"))
            if step_device:
                _remember(step_device)
            if bool(step.get("is_fault_injection", False)):
                _remember("fault_injection")
        if bool(is_fault_injection):
            _remember("fault_injection")
        if len(list(steps or [])) > 1:
            _remember("multi_step")
        capability_labels = [self._preset_fake_capability_display(item) for item in capability_ids]
        return {
            "fake_capabilities": capability_ids,
            "fake_capability_labels": capability_labels,
            "fake_capability_summary": t(
                "pages.devices.workbench.preset_center.capability.summary",
                capabilities=" / ".join(capability_labels) or t("common.none"),
                default=" / ".join(capability_labels) or t("common.none"),
            ),
        }

    def _preset_bundle_error(self, key: str, **kwargs: Any) -> str:
        return t(
            f"pages.devices.workbench.preset_center.manager.error.{key}",
            **kwargs,
        )

    def _normalize_custom_preset_payload(self, payload: dict[str, Any]) -> dict[str, Any] | None:
        group_id = str(payload.get("group_id") or "").strip().lower()
        device_kind = self._normalize_device_kind(payload.get("device_kind"))
        if not group_id and device_kind:
            group_id = self._preset_group_id(device_kind)
        if not device_kind and group_id:
            device_kind = self._normalize_device_kind(self.PRESET_GROUP_DEVICE_MAP.get(group_id))
        if not group_id or not device_kind:
            return None

        preset_id = str(payload.get("id") or "").strip().lower()
        label = str(payload.get("label") or payload.get("name") or "").strip()
        description = str(payload.get("description") or "").strip()
        if not preset_id or not label:
            return None

        parameters = self._normalize_custom_preset_parameters(dict(payload.get("parameters", {}) or {}))
        steps: list[dict[str, Any]] = []
        for raw_step in list(payload.get("steps") or [])[: self.MAX_CUSTOM_PRESET_STEPS]:
            if not isinstance(raw_step, dict):
                continue
            step_device = self._normalize_device_kind(raw_step.get("device_kind"))
            step_preset = str(raw_step.get("preset_id") or "").strip().lower()
            if not step_device or not step_preset or not self._is_builtin_preset(step_device, step_preset):
                continue
            preset_payload = self._builtin_preset_payload(step_device, step_preset)
            steps.append(
                {
                    "device_kind": step_device,
                    "preset_id": step_preset,
                    "label": str(preset_payload.get("label") or step_preset),
                    "description": str(preset_payload.get("description") or t("common.none")),
                    "is_fault_injection": bool(preset_payload.get("is_fault_injection", False)),
                }
            )
        if not steps:
            return None
        metadata = self._preset_metadata_payload(payload)
        return {
            "id": preset_id,
            "label": label,
            "name": label,
            "description": description,
            "group_id": group_id,
            "group_display": self._preset_group_display(group_id),
            "device_kind": device_kind,
            "parameters": parameters,
            "steps": steps,
            "step_count": len(steps),
            "is_custom": True,
            "source_kind": "custom",
            "source_display": t("pages.devices.workbench.preset_center.source_custom", default="自定义"),
            "schema_version": str(metadata.get("schema_version") or self.PRESET_DEFINITION_SCHEMA),
            "preset_version": int(metadata.get("preset_version", 1) or 1),
            "origin": str(metadata.get("origin") or "local_editor"),
            "origin_display": str(metadata.get("origin_display") or self._preset_origin_display("local_editor")),
            "source_ref": str(metadata.get("source_ref") or ""),
            "imported_from": str(metadata.get("imported_from") or ""),
            "sharing_scope": str(metadata.get("sharing_scope") or "local_reserved"),
            "created_at": str(metadata.get("created_at") or ""),
            "updated_at": str(metadata.get("updated_at") or ""),
            "metadata": metadata,
            "metadata_summary": self._preset_metadata_summary({**dict(payload), **metadata}),
            **self._preset_fake_capability_payload(
                device_kind=device_kind,
                steps=steps,
                is_fault_injection=any(bool(step.get("is_fault_injection", False)) for step in steps),
            ),
        }

    def _normalize_custom_preset_parameters(self, payload: dict[str, Any]) -> dict[str, Any]:
        try:
            analyzer_index = int(payload.get("analyzer_index", 1) or 1)
        except Exception:
            analyzer_index = 1
        try:
            pressure_hpa = float(payload.get("pressure_hpa", 1000.0) or 1000.0)
        except Exception:
            pressure_hpa = 1000.0
        try:
            channel = int(payload.get("channel", 1) or 1)
        except Exception:
            channel = 1
        relay_name = str(payload.get("relay_name") or "relay").strip().lower() or "relay"
        if relay_name not in self.RELAY_NAMES:
            relay_name = "relay"
        return {
            "analyzer_index": max(1, min(self.ANALYZER_COUNT, analyzer_index)),
            "pressure_hpa": pressure_hpa,
            "relay_name": relay_name,
            "channel": max(1, channel),
        }

    def _custom_preset_payloads(self, device_kind: str) -> list[dict[str, Any]]:
        normalized_kind = self._normalize_device_kind(device_kind)
        rows: list[dict[str, Any]] = []
        for item in self._normalize_custom_presets(self._custom_presets):
            if self._normalize_device_kind(item.get("device_kind")) != normalized_kind:
                continue
            steps = [dict(step) for step in list(item.get("steps") or [])]
            rows.append(
                {
                    "id": str(item.get("id") or ""),
                    "label": str(item.get("label") or ""),
                    "description": str(item.get("description") or ""),
                    "device_kind": normalized_kind,
                    "group_id": str(item.get("group_id") or self._preset_group_id(normalized_kind)),
                    "group_display": self._preset_group_display(str(item.get("group_id") or self._preset_group_id(normalized_kind))),
                    "is_fault_injection": any(bool(step.get("is_fault_injection", False)) for step in list(item.get("steps") or [])),
                    "is_featured": False,
                    "is_custom": True,
                    "source_kind": "custom",
                    "source_display": t("pages.devices.workbench.preset_center.source_custom", default="自定义"),
                    "step_count": int(item.get("step_count", len(steps)) or 0),
                    "steps": steps,
                    "parameters": dict(item.get("parameters", {}) or {}),
                    "schema_version": str(item.get("schema_version") or self.PRESET_DEFINITION_SCHEMA),
                    "preset_version": int(item.get("preset_version", 1) or 1),
                    "origin": str(item.get("origin") or "local_editor"),
                    "origin_display": str(item.get("origin_display") or self._preset_origin_display(item.get("origin"))),
                    "source_ref": str(item.get("source_ref") or ""),
                    "imported_from": str(item.get("imported_from") or ""),
                    "sharing_scope": str(item.get("sharing_scope") or "local_reserved"),
                    "created_at": str(item.get("created_at") or ""),
                    "updated_at": str(item.get("updated_at") or ""),
                    "metadata": dict(item.get("metadata", {}) or {}),
                    "metadata_summary": str(item.get("metadata_summary") or self._preset_metadata_summary(item)),
                    **self._preset_fake_capability_payload(
                        device_kind=normalized_kind,
                        steps=steps,
                        is_fault_injection=any(bool(step.get("is_fault_injection", False)) for step in steps),
                    ),
                }
            )
        return rows

    def _preset_description(self, device_kind: str, preset_id: str) -> str:
        normalized_kind = self._normalize_device_kind(device_kind)
        for item in self._builtin_preset_payloads(normalized_kind) + self._custom_preset_payloads(normalized_kind):
            if str(item.get("id") or "") == str(preset_id):
                return str(item.get("description") or t("common.none"))
        return t("common.none")

    def _preset_fault_flag(self, device_kind: str, preset_id: str) -> bool:
        normalized_kind = self._normalize_device_kind(device_kind)
        for item in self._builtin_preset_payloads(normalized_kind) + self._custom_preset_payloads(normalized_kind):
            if str(item.get("id") or "") == str(preset_id):
                return bool(item.get("is_fault_injection", False))
        return False

    def _find_custom_preset(self, preset_id: str) -> dict[str, Any]:
        for item in self._normalize_custom_presets(self._custom_presets):
            if str(item.get("id") or "") == str(preset_id or "").strip().lower():
                return dict(item)
        return {}

    def _next_custom_preset_id(self, group_id: str) -> str:
        prefix = f"custom_{str(group_id or 'preset').strip().lower()}_"
        numbers = []
        for item in self._custom_presets:
            current = str(dict(item).get("id") or "")
            if not current.startswith(prefix):
                continue
            tail = current[len(prefix) :]
            try:
                numbers.append(int(tail))
            except Exception:
                continue
        return f"{prefix}{(max(numbers) + 1) if numbers else 1:02d}"

    def _custom_editor_payload(self) -> dict[str, Any]:
        device_options = [
            {
                "id": mapped_kind,
                "label": self._device_display(mapped_kind),
            }
            for mapped_kind in self.PRESET_GROUP_DEVICE_MAP.values()
        ]
        step_catalog = {
            str(item.get("id") or ""): [
                {
                    "id": str(preset.get("id") or ""),
                    "label": str(preset.get("label") or ""),
                }
                for preset in self._builtin_preset_payloads(str(item.get("id") or ""))
            ]
            for item in device_options
        }
        return {
            "max_steps": self.MAX_CUSTOM_PRESET_STEPS,
            "group_options": [
                {"id": group_id, "label": self._preset_group_display(group_id)}
                for group_id in self.PRESET_GROUP_DEVICE_MAP
            ],
            "device_options": device_options,
            "step_catalog": step_catalog,
        }

    def _preset_bundle_item(self, device_kind: str, preset_id: str) -> dict[str, Any]:
        custom_preset = self._find_custom_preset(str(preset_id or "").strip().lower())
        if custom_preset:
            steps = [
                {
                    "device_kind": str(step.get("device_kind") or ""),
                    "preset_id": str(step.get("preset_id") or ""),
                }
                for step in list(custom_preset.get("steps") or [])
                if str(step.get("device_kind") or "").strip() and str(step.get("preset_id") or "").strip()
            ]
            return {
                "id": str(custom_preset.get("id") or ""),
                "label": str(custom_preset.get("label") or ""),
                "description": str(custom_preset.get("description") or ""),
                "group_id": str(custom_preset.get("group_id") or self._preset_group_id(device_kind)),
                "device_kind": self._normalize_device_kind(custom_preset.get("device_kind") or device_kind),
                "parameters": dict(custom_preset.get("parameters", {}) or {}),
                "steps": steps,
                "schema_version": str(custom_preset.get("schema_version") or self.PRESET_DEFINITION_SCHEMA),
                "preset_version": int(custom_preset.get("preset_version", 1) or 1),
                "origin": str(custom_preset.get("origin") or "local_editor"),
                "source_ref": str(custom_preset.get("source_ref") or ""),
                "imported_from": str(custom_preset.get("imported_from") or ""),
                "sharing_scope": str(custom_preset.get("sharing_scope") or "local_reserved"),
                "created_at": str(custom_preset.get("created_at") or ""),
                "updated_at": str(custom_preset.get("updated_at") or ""),
                "metadata": dict(custom_preset.get("metadata", {}) or {}),
                **self._preset_fake_capability_payload(
                    device_kind=self._normalize_device_kind(custom_preset.get("device_kind") or device_kind),
                    steps=steps,
                    is_fault_injection=any(bool(step.get("is_fault_injection", False)) for step in list(custom_preset.get("steps") or [])),
                ),
            }
        builtin = self._builtin_preset_payload(device_kind, str(preset_id or "").strip().lower())
        normalized_kind = self._normalize_device_kind(device_kind)
        group_id = self._preset_group_id(normalized_kind)
        steps = [{"device_kind": normalized_kind, "preset_id": str(preset_id or "").strip().lower()}]
        return {
            "id": self._next_custom_preset_id(group_id),
            "label": str(builtin.get("label") or preset_id),
            "description": str(builtin.get("description") or ""),
            "group_id": group_id,
            "device_kind": normalized_kind,
            "parameters": self._normalize_custom_preset_parameters({}),
            "steps": steps,
            "schema_version": self.PRESET_DEFINITION_SCHEMA,
            "preset_version": 1,
            "origin": "builtin_export",
            "source_ref": self._preset_key(normalized_kind, preset_id),
            "imported_from": "",
            "sharing_scope": "local_reserved",
            "created_at": "",
            "updated_at": "",
            **self._preset_fake_capability_payload(
                device_kind=normalized_kind,
                steps=steps,
                is_fault_injection=bool(builtin.get("is_fault_injection", False)),
            ),
        }

    def _ensure_unique_bundle_item_id(
        self,
        item: dict[str, Any],
        *,
        used_ids: set[str],
        reserved_ids: set[str],
        device_kind: str,
        preserve_existing: bool = False,
    ) -> dict[str, Any]:
        payload = dict(item)
        group_id = str(payload.get("group_id") or self._preset_group_id(device_kind)).strip().lower()
        candidate = str(payload.get("id") or "").strip().lower()
        if preserve_existing and candidate:
            used_ids.add(candidate)
            payload["id"] = candidate
            return payload
        if candidate and candidate not in used_ids and candidate not in reserved_ids:
            used_ids.add(candidate)
            payload["id"] = candidate
            return payload
        prefix = f"custom_{group_id or self._preset_group_id(device_kind)}_"
        counter = 1
        while f"{prefix}{counter:02d}" in used_ids:
            counter += 1
        payload["id"] = f"{prefix}{counter:02d}"
        used_ids.add(str(payload["id"]))
        return payload

    def _preset_bundle_payload(self, presets: list[dict[str, Any]], *, scope: str) -> dict[str, Any]:
        normalized_policy = self._normalize_preset_import_conflict_policy(self._preset_import_conflict_policy)
        contract = self._preset_manager_contract(normalized_policy)
        return {
            "schema": self.PRESET_BUNDLE_SCHEMA,
            "schema_version": 2,
            "preset_schema_version": self.PRESET_DEFINITION_SCHEMA,
            **contract,
            "scope": str(scope or "selected"),
            "exported_at": datetime.now().isoformat(timespec="seconds"),
            "sharing_interface": {
                "mode": "reserved_for_future_collaboration",
                "supports_import_export_only": True,
            },
            "bundle_format_summary": self._preset_bundle_format_summary(),
            "conflict_policy_summary": self._preset_conflict_policy_summary(normalized_policy),
            "conflict_strategy_summary": self._preset_conflict_strategy_summary(normalized_policy),
            "sharing_reserved_fields_summary": self._preset_sharing_reserved_fields_summary(contract["sharing_reserved_fields"]),
            "bundle_profile_summary": self._preset_bundle_profile_summary(contract["bundle_profile"]),
            "sharing_ready_summary": self._preset_sharing_ready_summary(),
            "preset_count": len(presets),
            "presets": presets,
        }

    def _preset_bundle_format_summary(self) -> str:
        return t(
            "pages.devices.workbench.preset_center.manager.bundle_format_summary",
            schema=self.PRESET_BUNDLE_SCHEMA,
            schema_version=2,
            preset_schema=self.PRESET_DEFINITION_SCHEMA,
            default=(
                f"Bundle format | {self.PRESET_BUNDLE_SCHEMA} v2 | "
                f"preset schema {self.PRESET_DEFINITION_SCHEMA} | simulation-only"
            ),
        )

    def _preset_conflict_policy_summary(self, policy: Any) -> str:
        normalized_policy = self._normalize_preset_import_conflict_policy(policy)
        return t(
            "pages.devices.workbench.preset_center.manager.conflict_policy_summary",
            policy=self._preset_import_conflict_policy_display(normalized_policy),
            default=f"Conflict policy | {self._preset_import_conflict_policy_display(normalized_policy)}",
        )

    def _preset_conflict_strategy_summary(self, policy: str) -> str:
        normalized_policy = self._normalize_preset_import_conflict_policy(policy)
        supported = " / ".join(
            self._preset_import_conflict_policy_display(policy_id)
            for policy_id in self.PRESET_IMPORT_CONFLICT_POLICIES
        )
        return t(
            "pages.devices.workbench.preset_center.manager.conflict_strategy_summary",
            policy=self._preset_import_conflict_policy_display(normalized_policy),
            supported=supported,
            default=f"Conflict strategy | {self._preset_import_conflict_policy_display(normalized_policy)} | {supported}",
        )

    @classmethod
    def _preset_sharing_reserved_field_list(cls) -> list[str]:
        return [
            "sharing_scope",
            "source_ref",
            "imported_from",
            "sharing_interface",
        ]

    @classmethod
    def _preset_sharing_reserved_fields(cls) -> str:
        return " / ".join(cls._preset_sharing_reserved_field_list())

    def _preset_sharing_ready_summary(self) -> str:
        return t(
            "pages.devices.workbench.preset_center.manager.sharing_ready_summary",
            fields=self._preset_sharing_reserved_fields(),
            default=f"Sharing preparation | local JSON only | reserved fields {self._preset_sharing_reserved_fields()}",
        )

    def _preset_sharing_reserved_fields_summary(self, fields: Optional[list[str]] = None) -> str:
        field_text = " / ".join(
            str(item).strip()
            for item in list(fields or self._preset_sharing_reserved_field_list())
            if str(item).strip()
        ) or self._preset_sharing_reserved_fields()
        return t(
            "pages.devices.workbench.preset_center.manager.sharing_reserved_fields_summary",
            fields=field_text,
            default=f"Sharing reserved fields | {field_text}",
        )

    def _preset_bundle_profile_summary(self, bundle_profile: Any = None) -> str:
        profile = str(bundle_profile or self.PRESET_BUNDLE_PROFILE).strip() or self.PRESET_BUNDLE_PROFILE
        return t(
            "pages.devices.workbench.preset_center.manager.bundle_profile_summary",
            profile=profile,
            default=f"Bundle profile | {profile}",
        )

    def _preset_manager_contract(self, policy: Any = None) -> dict[str, Any]:
        normalized_policy = self._normalize_preset_import_conflict_policy(policy)
        sharing_reserved_fields = self._preset_sharing_reserved_field_list()
        bundle_profile = self.PRESET_BUNDLE_PROFILE
        return {
            "simulation_only": True,
            "evidence_source": self.WORKBENCH_EVIDENCE_SOURCE,
            "not_real_acceptance_evidence": True,
            "conflict_policy": normalized_policy,
            "sharing_reserved_fields": sharing_reserved_fields,
            "bundle_profile": bundle_profile,
            "conflict_policy_summary": self._preset_conflict_policy_summary(normalized_policy),
            "sharing_reserved_fields_summary": self._preset_sharing_reserved_fields_summary(sharing_reserved_fields),
            "bundle_profile_summary": self._preset_bundle_profile_summary(bundle_profile),
        }

    def _validate_preset_bundle_payload(self, payload: dict[str, Any]) -> list[dict[str, Any]]:
        if str(payload.get("schema") or "") != self.PRESET_BUNDLE_SCHEMA:
            raise ValueError(
                self._preset_bundle_error(
                    "schema_invalid",
                    expected=self.PRESET_BUNDLE_SCHEMA,
                    actual=str(payload.get("schema") or "--"),
                    default=f"preset bundle schema is invalid: expected {self.PRESET_BUNDLE_SCHEMA}, got {payload.get('schema') or '--'}",
                )
            )
        if not bool(payload.get("simulation_only", False)):
            raise ValueError(
                self._preset_bundle_error(
                    "simulation_only_required",
                    default="preset bundle must stay simulation-only",
                )
            )
        presets = list(payload.get("presets") or [])
        if not presets:
            raise ValueError(
                self._preset_bundle_error(
                    "bundle_empty",
                    default="preset bundle is empty",
                )
            )
        normalized: list[dict[str, Any]] = []
        for index, item in enumerate(presets, start=1):
            if not isinstance(item, dict):
                continue
            normalized_payload = self._normalize_custom_preset_payload(dict(item))
            if normalized_payload:
                normalized.append(normalized_payload)
                continue
            raise ValueError(
                self._preset_bundle_error(
                    "item_invalid",
                    index=index,
                    default=f"preset item #{index} does not match the simulation preset schema",
                )
            )
        if not normalized:
            raise ValueError(
                self._preset_bundle_error(
                    "bundle_no_valid_presets",
                    default="preset bundle does not contain valid simulation presets",
                )
            )
        return normalized

    def _duplicate_preset(self, device_kind: str, preset_id: str) -> tuple[str, dict[str, Any]]:
        normalized_kind = self._normalize_device_kind(device_kind)
        if not normalized_kind or not str(preset_id or "").strip():
            raise ValueError("preset selection is required")
        bundle_item = self._preset_bundle_item(normalized_kind, preset_id)
        bundle_item["id"] = self._next_custom_preset_id(str(bundle_item.get("group_id") or self._preset_group_id(normalized_kind)))
        label = str(bundle_item.get("label") or preset_id).strip()
        bundle_item["label"] = t(
            "pages.devices.workbench.preset_center.manager.duplicate_label",
            label=label,
            default=f"{label} 副本",
        )
        bundle_item["name"] = str(bundle_item.get("label") or "")
        now_text = datetime.now().isoformat(timespec="seconds")
        bundle_item["preset_version"] = 1
        bundle_item["origin"] = "duplicated"
        bundle_item["source_ref"] = self._preset_key(normalized_kind, preset_id)
        bundle_item["imported_from"] = ""
        bundle_item["sharing_scope"] = "local_reserved"
        bundle_item["created_at"] = now_text
        bundle_item["updated_at"] = now_text
        payload = self._normalize_custom_preset_payload(bundle_item)
        if not payload:
            raise ValueError("unable to duplicate the selected preset")
        self._custom_presets = self._normalize_custom_presets([payload] + [dict(item) for item in self._custom_presets])
        self._save_persistent_state()
        return (
            t(
                "pages.devices.workbench.message.custom_preset_saved",
                preset=str(payload.get("label") or ""),
            ),
            {
                "custom_preset": dict(payload),
                "created": True,
                "duplicated_from": str(preset_id or ""),
            },
        )

    def _export_preset_bundle(self, **params: Any) -> tuple[str, dict[str, Any]]:
        scope = str(params.get("scope") or "selected").strip().lower()
        group_id = str(params.get("group_id") or "").strip().lower()
        device_kind = self._normalize_device_kind(params.get("device_kind"))
        preset_id = str(params.get("preset_id") or "").strip().lower()
        preset_items: list[dict[str, Any]] = []
        reserved_ids = {
            str(item.get("id") or "").strip().lower()
            for item in self._normalize_custom_presets(self._custom_presets)
            if str(item.get("id") or "").strip()
        }
        used_ids: set[str] = set()
        if scope == "selected":
            if not device_kind or not preset_id:
                raise ValueError("preset selection is required for export")
            is_custom = bool(self._find_custom_preset(preset_id))
            preset_items = [
                self._ensure_unique_bundle_item_id(
                    self._preset_bundle_item(device_kind, preset_id),
                    used_ids=used_ids,
                    reserved_ids=reserved_ids,
                    device_kind=device_kind,
                    preserve_existing=is_custom,
                )
            ]
        elif scope == "group":
            if not group_id:
                raise ValueError("preset group is required for export")
            target_device_kind = self._normalize_device_kind(self.PRESET_GROUP_DEVICE_MAP.get(group_id))
            grouped_presets = sorted(
                self._build_presets(target_device_kind),
                key=lambda item: (
                    not bool(item.get("is_custom", False)),
                    str(item.get("id") or ""),
                ),
            )
            for item in grouped_presets:
                current_preset_id = str(item.get("id") or "")
                current_device_kind = self._normalize_device_kind(item.get("device_kind") or target_device_kind)
                preset_items.append(
                    self._ensure_unique_bundle_item_id(
                        self._preset_bundle_item(
                            current_device_kind,
                            current_preset_id,
                        ),
                        used_ids=used_ids,
                        reserved_ids=reserved_ids,
                        device_kind=target_device_kind,
                        preserve_existing=bool(self._find_custom_preset(current_preset_id)),
                    )
                )
        else:
            for target_group_id, target_device_kind in self.PRESET_GROUP_DEVICE_MAP.items():
                normalized_target = self._normalize_device_kind(target_device_kind)
                all_presets = sorted(
                    self._build_presets(normalized_target),
                    key=lambda item: (
                        not bool(item.get("is_custom", False)),
                        str(item.get("id") or ""),
                    ),
                )
                for item in all_presets:
                    current_preset_id = str(item.get("id") or "")
                    current_device_kind = self._normalize_device_kind(item.get("device_kind") or normalized_target)
                    preset_items.append(
                        self._ensure_unique_bundle_item_id(
                            self._preset_bundle_item(
                                current_device_kind,
                                current_preset_id,
                            ),
                            used_ids=used_ids,
                            reserved_ids=reserved_ids,
                            device_kind=self._normalize_device_kind(
                                item.get("device_kind") or self.PRESET_GROUP_DEVICE_MAP.get(target_group_id)
                            ),
                            preserve_existing=bool(self._find_custom_preset(current_preset_id)),
                        )
                    )
        if not preset_items:
            raise ValueError("no simulation presets are available for export")
        bundle = self._preset_bundle_payload(preset_items, scope=scope)
        bundle_text = json.dumps(bundle, ensure_ascii=False, indent=2, sort_keys=True)
        return (
            t(
                "pages.devices.workbench.preset_center.manager.export_summary",
                count=len(preset_items),
                default=f"已导出 {len(preset_items)} 个 simulation-only 预置",
            ),
            {
                "bundle": bundle,
                "bundle_text": bundle_text,
                "preset_count": len(preset_items),
                "conflict_policy_summary": str(bundle.get("conflict_policy_summary") or self._preset_conflict_policy_summary(self._preset_import_conflict_policy)),
                "bundle_format_summary": str(bundle.get("bundle_format_summary") or self._preset_bundle_format_summary()),
                "conflict_strategy_summary": str(
                    bundle.get("conflict_strategy_summary")
                    or self._preset_conflict_strategy_summary(self._preset_import_conflict_policy)
                ),
                "sharing_reserved_fields_summary": str(
                    bundle.get("sharing_reserved_fields_summary")
                    or self._preset_sharing_reserved_fields_summary(bundle.get("sharing_reserved_fields"))
                ),
                "bundle_profile_summary": str(
                    bundle.get("bundle_profile_summary")
                    or self._preset_bundle_profile_summary(bundle.get("bundle_profile"))
                ),
                "sharing_ready_summary": str(bundle.get("sharing_ready_summary") or self._preset_sharing_ready_summary()),
            },
        )

    def _import_preset_bundle(
        self,
        bundle_text: str,
        *,
        conflict_policy: str | None = None,
    ) -> tuple[str, dict[str, Any]]:
        raw_text = str(bundle_text or "").strip()
        if not raw_text:
            raise ValueError(
                self._preset_bundle_error(
                    "json_required",
                    default="preset bundle JSON is required",
                )
            )
        try:
            payload = json.loads(raw_text)
        except Exception as exc:
            raise ValueError(
                self._preset_bundle_error(
                    "json_invalid",
                    error=exc,
                    default=f"preset bundle JSON is invalid: {exc}",
                )
            ) from exc
        if not isinstance(payload, dict):
            raise ValueError(
                self._preset_bundle_error(
                    "object_required",
                    default="preset bundle must be a JSON object",
                )
            )
        imported = self._validate_preset_bundle_payload(payload)
        policy = self._normalize_preset_import_conflict_policy(conflict_policy)
        self._preset_import_conflict_policy = policy
        imported_from = self._preset_import_source_label(payload)
        existing_items = [dict(item) for item in self._normalize_custom_presets(self._custom_presets)]
        used_ids = {
            str(item.get("id") or "")
            for item in existing_items
            if str(item.get("id") or "").strip()
        }
        now_text = datetime.now().isoformat(timespec="seconds")
        imported_rows: list[dict[str, Any]] = []
        renamed_count = 0
        overwritten_count = 0
        created_count = 0
        for raw_item in imported:
            candidate = dict(raw_item)
            candidate["origin"] = "import_bundle"
            candidate["imported_from"] = imported_from
            candidate["sharing_scope"] = "local_reserved"
            target = self._preset_conflict_target(candidate, existing_items)
            if policy == "overwrite" and target:
                candidate["id"] = str(target.get("id") or candidate.get("id") or "")
                candidate["label"] = str(target.get("label") or candidate.get("label") or "")
                candidate["name"] = str(candidate.get("label") or "")
                candidate["created_at"] = str(target.get("created_at") or candidate.get("created_at") or now_text)
                candidate["updated_at"] = now_text
                candidate["preset_version"] = max(
                    self._normalize_positive_int(target.get("preset_version"), default=1),
                    self._normalize_positive_int(candidate.get("preset_version"), default=1),
                ) + 1
                candidate["source_ref"] = str(
                    candidate.get("source_ref")
                    or target.get("source_ref")
                    or self._preset_key(candidate.get("device_kind"), target.get("id"))
                )
                normalized = self._normalize_custom_preset_payload(candidate)
                if not normalized:
                    continue
                existing_items = [
                    dict(normalized) if str(item.get("id") or "") == str(normalized.get("id") or "") else dict(item)
                    for item in existing_items
                ]
                imported_rows.append(dict(normalized))
                overwritten_count += 1
                continue

            if target:
                renamed_count += 1
                candidate = self._ensure_unique_bundle_item_id(
                    candidate,
                    used_ids=used_ids,
                    reserved_ids=set(),
                    device_kind=self._normalize_device_kind(candidate.get("device_kind")),
                    preserve_existing=False,
                )
                candidate["label"] = self._ensure_unique_preset_label(
                    str(candidate.get("label") or ""),
                    device_kind=self._normalize_device_kind(candidate.get("device_kind")),
                    existing_items=existing_items,
                )
                candidate["name"] = str(candidate.get("label") or "")
            else:
                used_ids.add(str(candidate.get("id") or ""))
            candidate["created_at"] = str(candidate.get("created_at") or now_text)
            candidate["updated_at"] = now_text
            candidate["preset_version"] = self._normalize_positive_int(candidate.get("preset_version"), default=1)
            normalized = self._normalize_custom_preset_payload(candidate)
            if not normalized:
                continue
            existing_items.insert(0, dict(normalized))
            imported_rows.append(dict(normalized))
            created_count += 1
        self._custom_presets = self._normalize_custom_presets(existing_items)
        self._save_persistent_state()
        conflict_summary = t(
            "pages.devices.workbench.preset_center.manager.import_conflict_summary",
            policy=self._preset_import_conflict_policy_display(policy),
            created=created_count,
            renamed=renamed_count,
            overwritten=overwritten_count,
            default=(
                f"Import conflicts | {self._preset_import_conflict_policy_display(policy)}"
                f" | created {created_count} | renamed {renamed_count} | overwritten {overwritten_count}"
            ),
        )
        return (
            t(
                "pages.devices.workbench.preset_center.manager.import_summary",
                count=len(imported_rows),
                renamed=renamed_count,
                overwritten=overwritten_count,
                policy=self._preset_import_conflict_policy_display(policy),
                default=(
                    f"Imported {len(imported_rows)} simulation-only presets"
                    f" | renamed {renamed_count} | overwritten {overwritten_count}"
                ),
            ),
            {
                "imported_count": len(imported_rows),
                "created_count": created_count,
                "renamed_count": renamed_count,
                "overwritten_count": overwritten_count,
                "conflict_policy": policy,
                "custom_presets": [dict(item) for item in imported_rows],
                "conflict_summary": conflict_summary,
                "conflict_policy_summary": str(
                    payload.get("conflict_policy_summary")
                    or self._preset_conflict_policy_summary(policy)
                ),
                "bundle_format_summary": str(
                    payload.get("bundle_format_summary") or self._preset_bundle_format_summary()
                ),
                "sharing_reserved_fields_summary": str(
                    payload.get("sharing_reserved_fields_summary")
                    or self._preset_sharing_reserved_fields_summary(list(payload.get("sharing_reserved_fields") or []))
                ),
                "bundle_profile_summary": str(
                    payload.get("bundle_profile_summary")
                    or self._preset_bundle_profile_summary(payload.get("bundle_profile"))
                ),
                "sharing_ready_summary": str(
                    payload.get("sharing_ready_summary") or self._preset_sharing_ready_summary()
                ),
            },
        )

    def _save_custom_preset(self, **params: Any) -> tuple[str, dict[str, Any]]:
        group_id = str(params.get("group_id") or "").strip().lower()
        if group_id not in self.PRESET_GROUP_DEVICE_MAP:
            raise ValueError("custom preset group is required")
        label = str(params.get("label") or "").strip()
        if not label:
            raise ValueError("custom preset name is required")
        preset_id = str(params.get("preset_id") or "").strip().lower() or self._next_custom_preset_id(group_id)
        existing = self._find_custom_preset(preset_id)
        now_text = datetime.now().isoformat(timespec="seconds")
        payload = self._normalize_custom_preset_payload(
            {
                "id": preset_id,
                "label": label,
                "description": str(params.get("description") or "").strip(),
                "group_id": group_id,
                "device_kind": self.PRESET_GROUP_DEVICE_MAP[group_id],
                "parameters": {
                    "analyzer_index": params.get("analyzer_index"),
                    "pressure_hpa": params.get("pressure_hpa"),
                    "relay_name": params.get("relay_name"),
                    "channel": params.get("channel"),
                },
                "steps": list(params.get("steps") or []),
                "schema_version": self.PRESET_DEFINITION_SCHEMA,
                "preset_version": (
                    self._normalize_positive_int(existing.get("preset_version"), default=1) + 1
                    if existing
                    else 1
                ),
                "origin": str(existing.get("origin") or "local_editor") if existing else "local_editor",
                "source_ref": str(existing.get("source_ref") or "") if existing else "",
                "imported_from": str(existing.get("imported_from") or "") if existing else "",
                "sharing_scope": str(existing.get("sharing_scope") or "local_reserved") if existing else "local_reserved",
                "created_at": str(existing.get("created_at") or now_text),
                "updated_at": now_text,
            }
        )
        if not payload:
            raise ValueError("at least one valid simulation preset step is required")
        remaining = [
            dict(item)
            for item in self._custom_presets
            if str(dict(item).get("id") or "") != preset_id
        ]
        remaining.insert(0, payload)
        self._custom_presets = self._normalize_custom_presets(remaining)
        self._save_persistent_state()
        return (
            t(
                "pages.devices.workbench.message.custom_preset_saved",
                preset=str(payload.get("label") or ""),
            ),
            {
                "custom_preset": dict(payload),
                "created": not bool(existing),
            },
        )

    def _delete_custom_preset(self, preset_id: str) -> tuple[str, dict[str, Any]]:
        normalized_id = str(preset_id or "").strip().lower()
        existing = self._find_custom_preset(normalized_id)
        if not existing:
            raise ValueError("custom preset not found")
        self._custom_presets = [
            dict(item)
            for item in self._custom_presets
            if str(dict(item).get("id") or "") != normalized_id
        ]
        self._favorite_presets = [item for item in self._favorite_presets if str(item or "") != self._preset_key(existing.get("device_kind"), normalized_id)]
        self._pinned_presets = [item for item in self._pinned_presets if str(item or "") != self._preset_key(existing.get("device_kind"), normalized_id)]
        self._recent_presets = deque(
            [
                dict(item)
                for item in self._recent_presets
                if not (
                    self._normalize_device_kind(item.get("device_kind")) == self._normalize_device_kind(existing.get("device_kind"))
                    and str(item.get("id") or "") == normalized_id
                )
            ],
            maxlen=self.MAX_RECENT_PRESETS,
        )
        self._save_persistent_state()
        return (
            t(
                "pages.devices.workbench.message.custom_preset_deleted",
                preset=str(existing.get("label") or normalized_id),
            ),
            {
                "deleted_preset_id": normalized_id,
            },
        )

    def _display_profile_payload(self, profile_id: str) -> dict[str, Any]:
        selected = str(profile_id or "auto").strip().lower()
        if selected not in self.DISPLAY_PROFILES:
            selected = "auto"
        context = self._normalize_display_profile_context(
            {
                **dict(self._display_profile_context or {}),
                "selected": selected,
            }
        )
        self._display_profile_context = dict(context)
        resolved = str(context.get("resolved") or "1080p_standard")
        family = str(context.get("family") or "1080p")
        monitor_class = str(context.get("monitor_class") or "standard_monitor")
        resolution = str(context.get("resolution") or self.DISPLAY_PROFILE_DEFAULT_RESOLUTION)
        selection_mode = "manual" if selected != "auto" else "auto"
        aspect_ratio = str(context.get("aspect_ratio") or "1.78")
        screen_area = int(context.get("screen_area", 0) or 0)
        window_area = int(context.get("window_area", 0) or 0)
        resolution_bucket = str(context.get("resolution_bucket") or family or "1080p")
        multi_monitor_ready_hint = str(context.get("multi_monitor_ready_hint") or "single_monitor_baseline")
        profile_summary = str(context.get("profile_summary") or t("common.none"))
        mapping_summary = str(context.get("mapping_summary") or profile_summary)
        return {
            "selected": selected,
            "selected_label": t(f"pages.devices.workbench.display_profile.{selected}", default=selected),
            "resolved": resolved,
            "resolved_label": t(f"pages.devices.workbench.display_profile_profile.{resolved}", default=resolved),
            "strategy_version": str(context.get("strategy_version") or self.DISPLAY_PROFILE_STRATEGY_VERSION),
            "family": family,
            "profile_family": family,
            "profile_family_label": t(
                f"pages.devices.workbench.display_profile_family.{family}",
                default=family,
            ),
            "resolution_bucket": resolution_bucket,
            "resolution_bucket_label": t(
                f"pages.devices.workbench.display_profile_family.{resolution_bucket}",
                default=resolution_bucket,
            ),
            "resolution": resolution,
            "resolution_class": str(context.get("resolution_class") or "full_hd"),
            "resolution_class_label": t(
                f"pages.devices.workbench.display_profile_resolution.{str(context.get('resolution_class') or 'full_hd')}",
                default=str(context.get("resolution_class") or "full_hd"),
            ),
            "window_class": str(context.get("window_class") or "standard_window"),
            "window_class_label": t(
                f"pages.devices.workbench.display_profile_window.{str(context.get('window_class') or 'standard_window')}",
                default=str(context.get("window_class") or "standard_window"),
            ),
            "layout_hint": str(context.get("layout_hint") or self.DISPLAY_PROFILE_LAYOUT_HINTS.get(selected, "compact")),
            "monitor_class": monitor_class,
            "monitor_label": t(
                f"pages.devices.workbench.display_profile_monitor.{monitor_class}",
                default=monitor_class,
            ),
            "auto_reason": str(context.get("auto_reason") or "balanced_default"),
            "auto_reason_label": t(
                f"pages.devices.workbench.display_profile_reason.{str(context.get('auto_reason') or 'balanced_default')}",
                default=str(context.get("auto_reason") or "balanced_default"),
            ),
            "multi_monitor_ready_hint": multi_monitor_ready_hint,
            "multi_monitor_ready_hint_label": t(
                f"pages.devices.workbench.display_profile_multi_monitor.{multi_monitor_ready_hint}",
                default=multi_monitor_ready_hint,
            ),
            "selection_mode": selection_mode,
            "aspect_ratio": aspect_ratio,
            "screen_area": screen_area,
            "window_area": window_area,
            "mapping_summary": mapping_summary,
            "profile_summary": profile_summary,
            "metadata": {
                "strategy_version": str(context.get("strategy_version") or self.DISPLAY_PROFILE_STRATEGY_VERSION),
                "selected_profile": selected,
                "resolved_profile": resolved,
                "profile_family": family,
                "resolution_bucket": resolution_bucket,
                "resolution": resolution,
                "resolution_class": str(context.get("resolution_class") or "full_hd"),
                "window_class": str(context.get("window_class") or "standard_window"),
                "monitor_class": monitor_class,
                "layout_hint": str(context.get("layout_hint") or self.DISPLAY_PROFILE_LAYOUT_HINTS.get(selected, "compact")),
                "auto_reason": str(context.get("auto_reason") or "balanced_default"),
                "multi_monitor_ready_hint": multi_monitor_ready_hint,
                "selection_mode": selection_mode,
                "aspect_ratio": aspect_ratio,
                "screen_area": screen_area,
                "window_area": window_area,
                "profile_summary": profile_summary,
                "mapping_summary": mapping_summary,
            },
            "screen_width": int(context.get("screen_width", self.DISPLAY_PROFILE_DEFAULT_CONTEXT["screen_width"]) or 0),
            "screen_height": int(context.get("screen_height", self.DISPLAY_PROFILE_DEFAULT_CONTEXT["screen_height"]) or 0),
            "window_width": int(context.get("window_width", self.DISPLAY_PROFILE_DEFAULT_CONTEXT["window_width"]) or 0),
            "window_height": int(context.get("window_height", self.DISPLAY_PROFILE_DEFAULT_CONTEXT["window_height"]) or 0),
        }

    def build_snapshot(self) -> dict[str, Any]:
        reference_quality = self._build_reference_quality()
        route_validation = self._build_route_validation()
        analyzer_snapshot = self._build_analyzer_snapshot()
        pace_snapshot = self._build_pace_snapshot()
        grz_snapshot = self._build_grz_snapshot()
        chamber_snapshot = self._build_chamber_snapshot()
        relay_snapshot = self._build_relay_snapshot(route_validation=route_validation)
        thermometer_snapshot = self._build_thermometer_snapshot(reference_quality=reference_quality)
        pressure_snapshot = self._build_pressure_gauge_snapshot(reference_quality=reference_quality)
        history_payload = self._build_history_payload()
        history_items = list(history_payload.get("items", []) or [])
        quick_scenarios = self._build_quick_scenarios()
        snapshot_compare = self._build_snapshot_compare()
        preset_center = self._build_preset_center()
        active_faults = self._collect_active_faults(
            analyzer_snapshot=analyzer_snapshot,
            pace_snapshot=pace_snapshot,
            grz_snapshot=grz_snapshot,
            chamber_snapshot=chamber_snapshot,
            relay_snapshot=relay_snapshot,
            thermometer_snapshot=thermometer_snapshot,
            pressure_snapshot=pressure_snapshot,
            reference_quality=reference_quality,
            route_validation=route_validation,
        )
        operator_summary = self._build_operator_summary(
            active_faults=active_faults,
            reference_quality=reference_quality,
            route_validation=route_validation,
            history_items=history_items,
            history_payload=history_payload,
        )
        display_profile_payload = self._display_profile_payload(self._display_profile)
        analytics_summary_payload = dict(self.facade.results_gateway.load_json("analytics_summary.json") or {})
        qc_review_summary = self._build_qc_review_summary(analytics_summary_payload)
        config_safety, config_safety_review = self._config_safety_snapshot()
        config_governance_payload = self._config_governance_payload(config_safety, config_safety_review)
        point_taxonomy_summary = self._point_taxonomy_snapshot()
        measurement_core_evidence = self._load_measurement_core_evidence()
        recognition_readiness_evidence = self._load_recognition_readiness_evidence()
        engineer_summary = self._build_engineer_summary(
            analyzer_snapshot=analyzer_snapshot,
            pace_snapshot=pace_snapshot,
            grz_snapshot=grz_snapshot,
            chamber_snapshot=chamber_snapshot,
            relay_snapshot=relay_snapshot,
            thermometer_snapshot=thermometer_snapshot,
            pressure_snapshot=pressure_snapshot,
            reference_quality=reference_quality,
            route_validation=route_validation,
            history_items=history_items,
            history_payload=history_payload,
            snapshot_compare=snapshot_compare,
            analytics_summary_payload=analytics_summary_payload,
            qc_review_summary=qc_review_summary,
            config_safety=config_safety,
            config_safety_review=config_safety_review,
            point_taxonomy_summary=point_taxonomy_summary,
            measurement_core_evidence=measurement_core_evidence,
            recognition_readiness_evidence=recognition_readiness_evidence,
        )
        return {
            "meta": {
                "simulated": True,
                "simulation_mode_label": t("pages.devices.workbench.banner.simulation_mode"),
                "safety_notice": [
                    t("pages.devices.workbench.banner.offline_only"),
                    t("pages.devices.workbench.banner.no_real_hardware"),
                    t("pages.devices.workbench.banner.validation_only"),
                ],
                "selected_analyzer_index": self._selected_analyzer_index + 1,
                "action_count": len(self._action_log),
                "last_action": dict(self._action_log[-1]) if self._action_log else {},
                "view_mode": self._view_mode,
                "view_mode_display": t(f"pages.devices.workbench.view.{self._view_mode}", default=self._view_mode),
                "layout_mode": self._layout_mode,
                "layout_mode_display": t(f"pages.devices.workbench.layout.{self._layout_mode}", default=self._layout_mode),
                "display_profile": self._display_profile,
                "display_profile_display": t(
                    f"pages.devices.workbench.display_profile.{self._display_profile}",
                    default=self._display_profile,
                ),
                "display_profile_meta": display_profile_payload,
                "has_fault_injection": bool(active_faults),
                "fault_count": len(active_faults),
                "config_classification": str(config_safety.get("classification") or ""),
                "config_badge_ids": list(config_safety.get("badge_ids") or []),
                "config_inventory_summary": str(config_governance_payload.get("config_inventory_summary") or "--"),
                "last_evidence_report": dict(self._last_evidence_report),
                "simulation_context": {
                    "scenario": str(self._simulation_context.get("scenario") or "device_workbench"),
                    "backend": str(self._simulation_context.get("simulation_backend") or "ui_workbench"),
                    "action_count": len(list(self._simulation_context.get("workbench_actions") or [])),
                    "device_overrides": dict((self._device_matrix().get("device_overrides") or {})),
                },
            },
            "workbench": {
                "title": t("pages.devices.workbench.title"),
                "simulated": True,
                "actions": self._actions_for(
                    "workbench",
                    "set_view_mode",
                    "set_layout_mode",
                    "set_display_profile",
                    "refresh_display_profile_context",
                    "set_history_filters",
                    "select_history_detail",
                    "set_snapshot_compare",
                    "save_custom_preset",
                    "delete_custom_preset",
                    "duplicate_preset",
                    "export_preset_bundle",
                    "import_preset_bundle",
                    "run_quick_scenario",
                    "generate_diagnostic_evidence",
                ),
                "view_mode": self._view_mode,
                "view_mode_display": t(f"pages.devices.workbench.view.{self._view_mode}", default=self._view_mode),
                "layout_mode": self._layout_mode,
                "layout_mode_display": t(f"pages.devices.workbench.layout.{self._layout_mode}", default=self._layout_mode),
                "display_profile": self._display_profile,
                "display_profile_display": t(
                    f"pages.devices.workbench.display_profile.{self._display_profile}",
                    default=self._display_profile,
                ),
                "display_profile_meta": display_profile_payload,
                "view_modes": [
                    {
                        "id": view_mode,
                        "label": t(f"pages.devices.workbench.view.{view_mode}", default=view_mode),
                    }
                    for view_mode in self.VIEW_MODES
                ],
                "layout_modes": [
                    {
                        "id": layout_mode,
                        "label": t(f"pages.devices.workbench.layout.{layout_mode}", default=layout_mode),
                    }
                    for layout_mode in self.LAYOUT_MODES
                ],
                "display_profiles": [
                    {
                        "id": profile,
                        "label": t(f"pages.devices.workbench.display_profile.{profile}", default=profile),
                    }
                    for profile in self.DISPLAY_PROFILES
                ],
                "quick_scenarios": quick_scenarios,
                "preset_center": preset_center,
                "last_evidence_report": dict(self._last_evidence_report),
                "history_filters": dict(history_payload.get("filters", {}) or {}),
                "snapshot_compare": snapshot_compare,
                "live_snapshot_evidence": {
                    **self._workbench_evidence_boundary(),
                    **dict(config_governance_payload),
                    "qc_review_summary": dict(qc_review_summary),
                    "qc_reviewer_card": dict(qc_review_summary.get("reviewer_card") or {}),
                    "qc_evidence_section": dict(qc_review_summary.get("evidence_section") or {}),
                    "qc_review_cards": [dict(item) for item in list(qc_review_summary.get("cards") or []) if isinstance(item, dict)],
                    "config_safety": config_safety,
                    "config_safety_review": config_safety_review,
                    "point_taxonomy_summary": point_taxonomy_summary,
                    "measurement_core_evidence": measurement_core_evidence,
                    "recognition_readiness_evidence": recognition_readiness_evidence,
                },
                "qc_review_summary": dict(qc_review_summary),
                "qc_reviewer_card": dict(qc_review_summary.get("reviewer_card") or {}),
                "qc_evidence_section": dict(qc_review_summary.get("evidence_section") or {}),
                "qc_review_cards": [dict(item) for item in list(qc_review_summary.get("cards") or []) if isinstance(item, dict)],
                "config_safety": config_safety,
                "config_safety_review": config_safety_review,
                "config_governance_handoff": dict(config_governance_payload.get("config_governance_handoff") or {}),
                "point_taxonomy_summary": point_taxonomy_summary,
            },
            "evidence": {
                **self._workbench_evidence_boundary(),
                **dict(config_governance_payload),
                "reference_quality": reference_quality,
                "reference_quality_display": display_reference_quality(reference_quality.get("reference_quality")),
                "route_physical_validation": route_validation,
                "simulation_context": dict(self._simulation_context),
                "qc_review_summary": dict(qc_review_summary),
                "qc_reviewer_card": dict(qc_review_summary.get("reviewer_card") or {}),
                "qc_evidence_section": dict(qc_review_summary.get("evidence_section") or {}),
                "qc_review_cards": [dict(item) for item in list(qc_review_summary.get("cards") or []) if isinstance(item, dict)],
                "config_safety": config_safety,
                "config_safety_review": config_safety_review,
                "point_taxonomy_summary": point_taxonomy_summary,
                "measurement_core_evidence": measurement_core_evidence,
                "recognition_readiness_evidence": recognition_readiness_evidence,
            },
            "history": history_payload,
            "operator_summary": operator_summary,
            "engineer_summary": engineer_summary,
            "point_taxonomy_summary": point_taxonomy_summary,
            "analyzer": analyzer_snapshot,
            "pace": pace_snapshot,
            "grz": grz_snapshot,
            "chamber": chamber_snapshot,
            "relay": relay_snapshot,
            "thermometer": thermometer_snapshot,
            "pressure_gauge": pressure_snapshot,
        }

    def execute_action(self, target_device: str, action: str, **params: Any) -> dict[str, Any]:
        normalized_kind = str(target_device or "").strip().lower()
        normalized_action = str(action or "").strip().lower()
        extras: dict[str, Any] = {}
        result_code = "ok"
        ok = True
        try:
            if normalized_action == "run_preset" and normalized_kind != "workbench":
                message, extras = self._execute_device_preset(normalized_kind, **params)
            elif normalized_kind == "analyzer":
                message = self._execute_analyzer_action(normalized_action, **params)
            elif normalized_kind == "pace":
                message = self._execute_pace_action(normalized_action, **params)
            elif normalized_kind == "grz":
                message = self._execute_grz_action(normalized_action, **params)
            elif normalized_kind == "chamber":
                message = self._execute_chamber_action(normalized_action, **params)
            elif normalized_kind == "relay":
                message = self._execute_relay_action(normalized_action, **params)
            elif normalized_kind == "thermometer":
                message = self._execute_thermometer_action(normalized_action, **params)
            elif normalized_kind in {"pressure_gauge", "pressure"}:
                message = self._execute_pressure_gauge_action(normalized_action, **params)
            elif normalized_kind == "workbench":
                message, extras = self._execute_workbench_action(normalized_action, **params)
            else:
                raise ValueError(f"unsupported workbench device kind: {target_device}")
        except Exception as exc:
            ok = False
            result_code = "failed"
            message = t(
                "pages.devices.workbench.message.action_failed",
                device=self._device_display(normalized_kind or "workbench"),
                action=self._action_display(normalized_kind or "workbench", normalized_action or "unknown"),
                error=exc,
                default=f"{self._device_display(normalized_kind or 'workbench')}操作失败: {exc}",
            )

        entry = None
        if self._should_record_action(normalized_kind, normalized_action):
            record_params = {key: value for key, value in dict(params or {}).items()}
            for extra_key in (
                "preset_source",
                "custom_preset_id",
                "executed_steps",
                "created",
                "preset_fake_capabilities",
                "preset_fake_capability_summary",
                "preset_metadata_summary",
            ):
                if extra_key in extras:
                    record_params[extra_key] = copy.deepcopy(extras[extra_key])
            entry = self._record_action(
                normalized_kind,
                normalized_action,
                message,
                record_params,
                is_fault_injection=bool(extras.get("is_fault_injection", self._is_fault_injection_action(normalized_kind, normalized_action, params))),
                result=result_code,
            )
        if ok or self._should_log_failed_action(normalized_kind, normalized_action):
            self.facade.log_ui(message)
        snapshot = self.build_snapshot()
        if entry is not None and self._should_capture_snapshot(normalized_kind, normalized_action):
            self._capture_snapshot(entry=entry, snapshot=snapshot)
            snapshot = self.build_snapshot()
        if ok and normalized_action == "run_preset":
            current_device = normalized_kind
            if normalized_kind == "workbench":
                current_device = self._normalize_device_kind(params.get("device_kind") or params.get("current_device") or "workbench")
            auto_evidence = self._generate_diagnostic_evidence(
                current_device=current_device,
                current_action=normalized_action,
            )
            extras["evidence_report"] = dict(auto_evidence)
            snapshot = self.build_snapshot()
        response = {
            "ok": ok,
            "message": message,
            "snapshot": snapshot,
        }
        response.update({key: value for key, value in extras.items() if key != "is_fault_injection"})
        if entry is not None:
            response["entry"] = entry
        return response

    def _record_action(
        self,
        device_kind: str,
        action: str,
        message: str,
        params: dict[str, Any],
        *,
        is_fault_injection: bool,
        result: str = "ok",
    ) -> dict[str, Any]:
        self._action_sequence += 1
        entry = {
            "sequence": self._action_sequence,
            "timestamp": datetime.now().isoformat(timespec="seconds"),
            "device": str(device_kind),
            "device_display": self._device_display(device_kind),
            "action": str(action),
            "action_display": self._action_display(device_kind, action),
            "message": str(message),
            "params": {key: value for key, value in dict(params or {}).items()},
            "result": str(result),
            "result_display": t(
                f"pages.devices.workbench.enum.action_result.{result}",
                default={"ok": "成功", "failed": "失败"}.get(str(result), str(result)),
            ),
            "is_fault_injection": bool(is_fault_injection),
            "fault_injection_display": display_bool(bool(is_fault_injection)),
        }
        self._action_log.append(entry)
        self._append_simulation_action(entry)
        return dict(entry)

    def _device_display(self, device_kind: str) -> str:
        if str(device_kind) == "workbench":
            return t("pages.devices.workbench.title")
        if str(device_kind) == "pressure":
            device_kind = "pressure_gauge"
        return t(f"pages.devices.workbench.device.{device_kind}", default=str(device_kind))

    def _action_display(self, device_kind: str, action: str) -> str:
        return t(
            f"pages.devices.workbench.action.{device_kind}.{action}",
            default=t(f"pages.devices.workbench.button.{device_kind}.{action}", default=str(action)),
        )

    def _history_items(self) -> list[dict[str, Any]]:
        return [dict(item) for item in reversed(list(self._action_log))]

    def _build_history_payload(self) -> dict[str, Any]:
        all_items = self._history_items()
        filtered_items = self._apply_history_filters(all_items)
        selected_detail = self._resolve_history_detail(filtered_items, all_items)
        detail_payload = {
            **selected_detail,
            "related_snapshot": self._history_related_snapshot(selected_detail),
            "related_evidence": self._history_related_evidence(selected_detail),
        }
        return {
            "items": filtered_items,
            "all_items": all_items,
            "detail": detail_payload,
            "detail_text": self._history_detail_text(detail_payload),
            "detail_json": self._json_text(detail_payload) if detail_payload else self._json_text({}),
            "limit": int(getattr(self._action_log, "maxlen", len(all_items)) or len(all_items)),
            "filters": {
                "device": self._history_device_filter,
                "result": self._history_result_filter,
                "device_options": self._history_device_options(all_items),
                "result_options": self._history_result_options(),
                "filtered_count": len(filtered_items),
                "total_count": len(all_items),
            },
        }

    def _apply_history_filters(self, items: list[dict[str, Any]]) -> list[dict[str, Any]]:
        filtered: list[dict[str, Any]] = []
        for item in items:
            if self._history_device_filter not in {"", "all"} and str(item.get("device") or "") != self._history_device_filter:
                continue
            if self._history_result_filter == "success" and str(item.get("result") or "") != "ok":
                continue
            if self._history_result_filter == "failed" and str(item.get("result") or "") != "failed":
                continue
            if self._history_result_filter == "fault_injection" and not bool(item.get("is_fault_injection", False)):
                continue
            filtered.append(dict(item))
        return filtered

    def _history_device_options(self, items: list[dict[str, Any]]) -> list[dict[str, str]]:
        seen = {"all": t("pages.devices.workbench.history.filter.all_devices", default="全部设备")}
        for item in items:
            device = str(item.get("device") or "").strip()
            if not device:
                continue
            seen.setdefault(device, str(item.get("device_display") or self._device_display(device)))
        return [{"id": key, "label": value} for key, value in seen.items()]

    def _history_result_options(self) -> list[dict[str, str]]:
        return [
            {"id": "all", "label": t("pages.devices.workbench.history.filter.all_results", default="全部结果")},
            {"id": "success", "label": t("pages.devices.workbench.history.filter.success", default="成功")},
            {"id": "failed", "label": t("pages.devices.workbench.history.filter.failed", default="失败")},
            {"id": "fault_injection", "label": t("pages.devices.workbench.history.filter.fault_injection", default="故障注入")},
        ]

    def _resolve_history_detail(
        self,
        filtered_items: list[dict[str, Any]],
        all_items: list[dict[str, Any]],
    ) -> dict[str, Any]:
        valid_sequences = {int(item.get("sequence", 0) or 0) for item in all_items}
        if self._selected_history_sequence not in valid_sequences:
            self._selected_history_sequence = None
        if self._selected_history_sequence is None and filtered_items:
            self._selected_history_sequence = int(filtered_items[0].get("sequence", 0) or 0)
        selected = next(
            (item for item in filtered_items if int(item.get("sequence", 0) or 0) == self._selected_history_sequence),
            None,
        )
        if selected is None and filtered_items:
            selected = dict(filtered_items[0])
            self._selected_history_sequence = int(selected.get("sequence", 0) or 0)
        return dict(selected or {})

    def _history_detail_text(self, item: dict[str, Any]) -> str:
        if not item:
            return t("pages.devices.workbench.history.no_detail", default="暂无动作详情")
        param_parts = []
        for key, value in dict(item.get("params") or {}).items():
            if value in ("", None, [], {}):
                continue
            param_parts.append(f"{key}={value}")
        param_text = " | ".join(param_parts) or t("common.none")
        lines = [
            t(
                "pages.devices.workbench.history.detail_line",
                seq=item.get("sequence", "--"),
                device=item.get("device_display", "--"),
                action=item.get("action_display", "--"),
                result=item.get("result_display", "--"),
                params=param_text,
                message=item.get("message", "--"),
                default=f"#{item.get('sequence', '--')} | {item.get('device_display', '--')} | {item.get('action_display', '--')} | {item.get('result_display', '--')} | {item.get('message', '--')}",
            )
        ]
        related_snapshot = dict(item.get("related_snapshot", {}) or {})
        if bool(related_snapshot.get("available", False)):
            lines.append(
                t(
                    "pages.devices.workbench.history.related_snapshot.detail",
                    label=related_snapshot.get("label", "--"),
                    compare=related_snapshot.get("compare_label", "--"),
                    default=f"关联快照：{related_snapshot.get('label', '--')} -> {related_snapshot.get('compare_label', '--')}",
                )
            )
        related_evidence = dict(item.get("related_evidence", {}) or {})
        if bool(related_evidence.get("available", False)):
            lines.append(
                t(
                    "pages.devices.workbench.history.related_evidence.detail",
                    summary=related_evidence.get("summary", t("common.none")),
                    path=related_evidence.get("path", "--"),
                    default=f"关联证据：{related_evidence.get('summary', t('common.none'))} | {related_evidence.get('path', '--')}",
                )
            )
        return "\n".join(lines)

    def _history_related_snapshot(self, item: dict[str, Any]) -> dict[str, Any]:
        if not item:
            return {"available": False}
        try:
            sequence = int(item.get("sequence", 0) or 0)
        except Exception:
            sequence = 0
        if sequence <= 0:
            return {"available": False}
        options = self._snapshot_options()
        option_by_sequence = {
            int(option.get("sequence", 0) or 0): dict(option)
            for option in options
        }
        current = option_by_sequence.get(sequence)
        compare_target = next(
            (
                dict(option)
                for option in options
                if int(option.get("sequence", 0) or 0) != sequence
            ),
            {},
        )
        return {
            "available": current is not None,
            "sequence": sequence if current is not None else None,
            "label": str(dict(current or {}).get("label", "") or ""),
            "compare_sequence": int(dict(compare_target).get("sequence", 0) or 0) or None,
            "compare_label": str(dict(compare_target).get("label", "") or ""),
        }

    def _history_related_evidence(self, item: dict[str, Any]) -> dict[str, Any]:
        _ = item
        if not self._last_evidence_report:
            return {"available": False}
        paths = dict(self._last_evidence_report.get("paths", {}) or {})
        report_path = str(paths.get("report_json") or "")
        return {
            "available": bool(report_path),
            "generated_at": str(self._last_evidence_report.get("generated_at", "") or ""),
            "summary": str(self._last_evidence_report.get("summary_line") or t("pages.devices.workbench.summary.no_evidence")),
            "path": report_path,
        }

    def _should_record_action(self, device_kind: str, action: str) -> bool:
        if str(device_kind) != "workbench":
            return True
        return str(action) in {"run_quick_scenario", "generate_diagnostic_evidence", "run_preset"}

    def _should_capture_snapshot(self, device_kind: str, action: str) -> bool:
        if not self._should_record_action(device_kind, action):
            return False
        return str(action) != "generate_diagnostic_evidence"

    @staticmethod
    def _should_log_failed_action(device_kind: str, action: str) -> bool:
        return not (
            str(device_kind) == "workbench"
            and str(action) in {
                "set_view_mode",
                "set_layout_mode",
                "set_display_profile",
                "set_history_filters",
                "select_history_detail",
                "set_snapshot_compare",
            }
        )

    def _build_quick_scenarios(self) -> list[dict[str, str]]:
        return [
            {
                "id": scenario_id,
                "label": t(f"pages.devices.workbench.quick_scenario.{scenario_id}.label", default=scenario_id),
                "description": t(f"pages.devices.workbench.quick_scenario.{scenario_id}.description", default=""),
            }
            for scenario_id in self.QUICK_SCENARIOS
        ]

    def _build_presets(self, device_kind: str) -> list[dict[str, Any]]:
        normalized_kind = self._normalize_device_kind(device_kind)
        presets = []
        for base_payload in self._builtin_preset_payloads(normalized_kind) + self._custom_preset_payloads(normalized_kind):
            preset_id = str(base_payload.get("id") or "")
            preset_key = self._preset_key(normalized_kind, preset_id)
            usage_count = self._preset_usage_count(normalized_kind, preset_id)
            recent_rank = self._preset_recent_rank(normalized_kind, preset_id)
            pinned_rank = self._preset_pinned_rank(normalized_kind, preset_id)
            presets.append(
                {
                    **dict(base_payload),
                    "key": preset_key,
                    "is_favorite": preset_key in set(self._favorite_presets),
                    "is_pinned": pinned_rank < 99,
                    "usage_count": usage_count,
                    "is_recent": recent_rank < 99,
                    "_recent_rank": recent_rank,
                    "_pinned_rank": pinned_rank,
                }
            )
        presets.sort(
            key=lambda item: (
                int(item.get("_pinned_rank", 99) or 99),
                0 if bool(item.get("is_favorite", False)) else 1,
                0 if bool(item.get("is_featured", False)) else 1,
                int(item.get("_recent_rank", 99) or 99),
                -int(item.get("usage_count", 0) or 0),
                0 if not bool(item.get("is_custom", False)) else 1,
                0 if not bool(item.get("is_fault_injection", False)) else 1,
                str(item.get("label") or ""),
            )
        )
        for item in presets:
            item.pop("_recent_rank", None)
            item.pop("_pinned_rank", None)
        return presets

    def _build_preset_center(self) -> dict[str, Any]:
        groups = []
        for group_id, device_kind in self.PRESET_GROUP_DEVICE_MAP.items():
            presets = self._build_presets(device_kind)
            pinned_presets = [dict(item) for item in presets if bool(item.get("is_pinned", False))][: self.MAX_PINNED_PRESETS]
            favorite_presets = [dict(item) for item in presets if bool(item.get("is_favorite", False))][: self.MAX_FAVORITE_PRESETS]
            custom_presets = [dict(item) for item in presets if bool(item.get("is_custom", False))]
            frequent_presets = list(pinned_presets)
            if len(frequent_presets) < 3:
                frequent_presets.extend(
                    dict(item)
                    for item in presets
                    if not bool(item.get("is_pinned", False)) and bool(item.get("is_featured", False))
                )
            if len(frequent_presets) < 3:
                seen_keys = {str(item.get("key") or "") for item in frequent_presets}
                frequent_presets.extend(
                    dict(item)
                    for item in presets
                    if str(item.get("key") or "") not in seen_keys
                )
            groups.append(
                {
                    "id": group_id,
                    "label": self._preset_group_display(group_id),
                    "device_kind": device_kind,
                    "presets": presets,
                    "pinned_presets": pinned_presets,
                    "favorite_presets": favorite_presets,
                    "custom_presets": custom_presets,
                    "frequent_presets": frequent_presets[:3],
                }
            )
        recent_presets = []
        for item in list(self._recent_presets):
            recent_presets.append(
                {
                    **dict(item),
                    "run_label": t(
                        "pages.devices.workbench.preset_center.recent_item",
                        group=item.get("group_display", "--"),
                        preset=item.get("label", "--"),
                        default=f"{item.get('group_display', '--')} / {item.get('label', '--')}",
                    ),
                }
            )
        selected_group = dict(groups[0]) if groups else {}
        selected_preset = dict((selected_group.get("presets") or [None])[0] or {})
        selected_policy = self._normalize_preset_import_conflict_policy(self._preset_import_conflict_policy)
        directory_index = self._preset_directory_index(groups)
        contract = self._preset_manager_contract(selected_policy)
        manager = {
            "schema": self.PRESET_BUNDLE_SCHEMA,
            "schema_version": 2,
            "preset_schema_version": self.PRESET_DEFINITION_SCHEMA,
            **contract,
            "supports_duplicate": True,
            "supports_import_export": True,
            "bundle_format_summary": self._preset_bundle_format_summary(),
            "conflict_policy_summary": self._preset_conflict_policy_summary(selected_policy),
            "conflict_strategy_summary": self._preset_conflict_strategy_summary(selected_policy),
            "sharing_reserved_fields_summary": self._preset_sharing_reserved_fields_summary(contract["sharing_reserved_fields"]),
            "bundle_profile_summary": self._preset_bundle_profile_summary(contract["bundle_profile"]),
            "sharing_ready_summary": self._preset_sharing_ready_summary(),
            "sharing_interface": {
                "mode": "reserved_for_future_collaboration",
                "supports_import_export_only": True,
                "hint": t(
                    "pages.devices.workbench.preset_center.manager.future_sharing_hint",
                    default="Local JSON collaboration only for now.",
                ),
            },
            "selected_import_conflict_policy": selected_policy,
            "import_conflict_policy_options": [
                {
                    "id": policy_id,
                    "label": self._preset_import_conflict_policy_display(policy_id),
                }
                for policy_id in self.PRESET_IMPORT_CONFLICT_POLICIES
            ],
            "selected_group_id": str(selected_group.get("id") or ""),
            "selected_preset_id": str(selected_preset.get("id") or ""),
            "selected_preset_is_custom": bool(selected_preset.get("is_custom", False)),
            "selected_preset_source": str(selected_preset.get("source_display") or t("common.none")),
            "selected_preset_capability_summary": str(selected_preset.get("fake_capability_summary") or t("common.none")),
            "selected_preset_metadata_summary": str(selected_preset.get("metadata_summary") or t("common.none")),
            "directory_index": directory_index,
            "directory_summary": self._preset_directory_summary(directory_index),
            "summary": t(
                "pages.devices.workbench.preset_center.manager.summary",
                custom=len(self._custom_presets),
                recent=len(recent_presets),
                policy=self._preset_import_conflict_policy_display(selected_policy),
                default=f"自定义 {len(self._custom_presets)} / 最近 {len(recent_presets)}",
            ),
        }
        return {
            "groups": groups,
            "recent_presets": recent_presets,
            "favorite_presets": self._preset_payloads_from_keys(self._favorite_presets, limit=self.MAX_FAVORITE_PRESETS),
            "pinned_presets": self._preset_payloads_from_keys(self._pinned_presets, limit=self.MAX_PINNED_PRESETS),
            "supports_custom_presets": True,
            "custom_preset_schema": {
                "version": self.PRESET_DEFINITION_SCHEMA,
                "fields": [
                    "group_id",
                    "label",
                    "description",
                    "parameters",
                    "steps",
                    "schema_version",
                    "preset_version",
                    "origin",
                    "created_at",
                    "updated_at",
                ],
            },
            "custom_presets": [dict(item) for item in self._normalize_custom_presets(self._custom_presets)],
            "editor": self._custom_editor_payload(),
            "manager": manager,
            "selected_group_id": str(selected_group.get("id") or ""),
            "selected_preset": selected_preset,
            "summary": t(
                "pages.devices.workbench.preset_center.summary",
                groups=len(groups),
                pinned=len(self._pinned_presets),
                favorites=len(self._favorite_presets),
                recent=len(recent_presets),
                custom=len(self._custom_presets),
                default=f"{len(groups)} 个预置组 / 自定义 {len(self._custom_presets)} / 最近使用 {len(recent_presets)} 条",
            ),
        }

    def _preset_directory_index(self, groups: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
        builtin_count = 0
        for group in groups:
            builtin_count += sum(1 for item in list(group.get("presets", []) or []) if not bool(item.get("is_custom", False)))
        local_custom_count = sum(
            1
            for item in self._normalize_custom_presets(self._custom_presets)
            if str(item.get("origin") or "local_editor") != "import_bundle"
        )
        imported_count = sum(
            1
            for item in self._normalize_custom_presets(self._custom_presets)
            if str(item.get("origin") or "") == "import_bundle"
        )
        rows = {
            "builtin": {
                "id": "builtin",
                "label": t("pages.devices.workbench.preset_center.manager.directory.builtin"),
                "count": builtin_count,
            },
            "local_custom": {
                "id": "local_custom",
                "label": t("pages.devices.workbench.preset_center.manager.directory.local_custom"),
                "count": local_custom_count,
            },
            "imported": {
                "id": "imported",
                "label": t("pages.devices.workbench.preset_center.manager.directory.imported"),
                "count": imported_count,
            },
        }
        for item in rows.values():
            item["summary"] = t(
                "pages.devices.workbench.preset_center.manager.directory.entry",
                label=str(item.get("label") or "--"),
                count=int(item.get("count", 0) or 0),
                default=f"{item.get('label', '--')} {int(item.get('count', 0) or 0)}",
            )
        return rows

    def _preset_directory_summary(self, directory_index: dict[str, dict[str, Any]]) -> str:
        builtin = int(dict(directory_index.get("builtin") or {}).get("count", 0) or 0)
        local_custom = int(dict(directory_index.get("local_custom") or {}).get("count", 0) or 0)
        imported = int(dict(directory_index.get("imported") or {}).get("count", 0) or 0)
        return t(
            "pages.devices.workbench.preset_center.manager.directory.summary",
            builtin=builtin,
            local_custom=local_custom,
            imported=imported,
            default=f"内置 {builtin} / 本地自定义 {local_custom} / 导入 {imported}",
        )

    def _preset_group_id(self, device_kind: str) -> str:
        normalized_kind = self._normalize_device_kind(device_kind)
        for group_id, mapped_kind in self.PRESET_GROUP_DEVICE_MAP.items():
            if str(mapped_kind) == normalized_kind:
                return group_id
        return normalized_kind

    def _preset_group_display(self, group_id: str) -> str:
        return t(f"pages.devices.workbench.preset_group.{group_id}", default=str(group_id))

    def _preset_usage_count(self, device_kind: str, preset_id: str) -> int:
        return int(self._preset_usage.get(self._preset_key(device_kind, preset_id), 0) or 0)

    def _preset_recent_rank(self, device_kind: str, preset_id: str) -> int:
        for index, item in enumerate(self._recent_presets):
            if self._normalize_device_kind(item.get("device_kind")) == self._normalize_device_kind(device_kind) and str(item.get("id") or "") == str(preset_id):
                return index
        return 99

    def _preset_pinned_rank(self, device_kind: str, preset_id: str) -> int:
        key = self._preset_key(device_kind, preset_id)
        for index, item in enumerate(self._pinned_presets):
            if str(item or "") == key:
                return index
        return 99

    def _record_preset_use(self, device_kind: str, preset_id: str) -> None:
        normalized_kind = self._normalize_device_kind(device_kind)
        usage_key = self._preset_key(normalized_kind, preset_id)
        self._preset_usage[usage_key] += 1
        deduped = [
            dict(item)
            for item in self._recent_presets
            if not (
                str(item.get("device_kind") or "") == normalized_kind
                and str(item.get("id") or "") == str(preset_id)
            )
        ]
        self._recent_presets = deque(deduped, maxlen=self.MAX_RECENT_PRESETS)
        group_id = self._preset_group_id(normalized_kind)
        self._recent_presets.appendleft(
            {
                "id": str(preset_id),
                "label": self._preset_label(normalized_kind, preset_id),
                "device_kind": normalized_kind,
                "group_id": group_id,
                "group_display": self._preset_group_display(group_id),
                "usage_count": int(self._preset_usage.get(usage_key, 0) or 0),
                "used_at": datetime.now().isoformat(timespec="seconds"),
            }
        )
        self._save_persistent_state()

    def _preset_payloads_from_keys(self, keys: list[str], *, limit: int) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for key in list(keys)[: max(1, int(limit))]:
            device_kind, _, preset_id = str(key or "").partition(":")
            if not device_kind or not preset_id:
                continue
            payload = self._preset_payload(device_kind, preset_id)
            if payload:
                rows.append(payload)
        return rows

    def _preset_payload(self, device_kind: str, preset_id: str) -> dict[str, Any]:
        normalized_kind = self._normalize_device_kind(device_kind)
        for item in self._build_presets(normalized_kind):
            if str(item.get("id") or "") == str(preset_id):
                return dict(item)
        return {}

    def _toggle_preset_favorite(self, device_kind: str, preset_id: str) -> tuple[str, bool]:
        key = self._preset_key(device_kind, preset_id)
        favorites = [item for item in self._favorite_presets if str(item or "") != key]
        enabled = key not in set(self._favorite_presets)
        if enabled:
            favorites.insert(0, key)
        self._favorite_presets = self._normalize_preset_keys(favorites, limit=self.MAX_FAVORITE_PRESETS)
        self._save_persistent_state()
        return (
            t(
                "pages.devices.workbench.message.preset_favorite_set",
                preset=self._preset_label(device_kind, preset_id),
                state=t(
                    "pages.devices.workbench.preset_center.favorite_enabled"
                    if enabled
                    else "pages.devices.workbench.preset_center.favorite_disabled"
                ),
            ),
            enabled,
        )

    def _toggle_preset_pin(self, device_kind: str, preset_id: str) -> tuple[str, bool]:
        key = self._preset_key(device_kind, preset_id)
        pinned = [item for item in self._pinned_presets if str(item or "") != key]
        enabled = key not in set(self._pinned_presets)
        if enabled:
            pinned.insert(0, key)
        self._pinned_presets = self._normalize_preset_keys(pinned, limit=self.MAX_PINNED_PRESETS)
        self._save_persistent_state()
        return (
            t(
                "pages.devices.workbench.message.preset_pin_set",
                preset=self._preset_label(device_kind, preset_id),
                state=t(
                    "pages.devices.workbench.preset_center.pin_enabled"
                    if enabled
                    else "pages.devices.workbench.preset_center.pin_disabled"
                ),
            ),
            enabled,
        )

    def _capture_snapshot(self, *, entry: dict[str, Any], snapshot: dict[str, Any]) -> None:
        compact = {
            "sequence": int(entry.get("sequence", 0) or 0),
            "timestamp": str(entry.get("timestamp", "") or ""),
            "device": str(entry.get("device") or ""),
            "device_display": str(entry.get("device_display") or ""),
            "action": str(entry.get("action") or ""),
            "action_display": str(entry.get("action_display") or ""),
            "label": t(
                "pages.devices.workbench.snapshot.item_label",
                seq=entry.get("sequence", "--"),
                device=entry.get("device_display", "--"),
                action=entry.get("action_display", "--"),
                default=f"#{entry.get('sequence', '--')} {entry.get('device_display', '--')} / {entry.get('action_display', '--')}",
            ),
            "snapshot": {
                "evidence": copy.deepcopy(dict(snapshot.get("evidence", {}) or {})),
                "analyzer": {
                    "panel_status": copy.deepcopy(dict(snapshot.get("analyzer", {}).get("panel_status", {}) or {})),
                    "injection_state": copy.deepcopy(dict(snapshot.get("analyzer", {}).get("injection_state", {}) or {})),
                },
                "pace": {
                    "panel_status": copy.deepcopy(dict(snapshot.get("pace", {}).get("panel_status", {}) or {})),
                    "injection_state": copy.deepcopy(dict(snapshot.get("pace", {}).get("injection_state", {}) or {})),
                },
                "grz": {
                    "panel_status": copy.deepcopy(dict(snapshot.get("grz", {}).get("panel_status", {}) or {})),
                    "injection_state": copy.deepcopy(dict(snapshot.get("grz", {}).get("injection_state", {}) or {})),
                },
                "chamber": {
                    "panel_status": copy.deepcopy(dict(snapshot.get("chamber", {}).get("panel_status", {}) or {})),
                    "injection_state": copy.deepcopy(dict(snapshot.get("chamber", {}).get("injection_state", {}) or {})),
                },
                "relay": {
                    "panel_status": copy.deepcopy(dict(snapshot.get("relay", {}).get("panel_status", {}) or {})),
                    "injection_state": copy.deepcopy(dict(snapshot.get("relay", {}).get("injection_state", {}) or {})),
                },
                "thermometer": {
                    "panel_status": copy.deepcopy(dict(snapshot.get("thermometer", {}).get("panel_status", {}) or {})),
                    "injection_state": copy.deepcopy(dict(snapshot.get("thermometer", {}).get("injection_state", {}) or {})),
                },
                "pressure_gauge": {
                    "panel_status": copy.deepcopy(dict(snapshot.get("pressure_gauge", {}).get("panel_status", {}) or {})),
                    "injection_state": copy.deepcopy(dict(snapshot.get("pressure_gauge", {}).get("injection_state", {}) or {})),
                },
            },
        }
        self._snapshot_log.append(compact)
        sequences = {int(item.get("sequence", 0) or 0) for item in self._snapshot_log}
        if self._selected_snapshot_left not in sequences:
            self._selected_snapshot_left = None
        if self._selected_snapshot_right not in sequences:
            self._selected_snapshot_right = None

    def _snapshot_options(self) -> list[dict[str, Any]]:
        return [
            {
                "sequence": int(item.get("sequence", 0) or 0),
                "label": str(item.get("label") or ""),
                "timestamp": str(item.get("timestamp") or ""),
            }
            for item in reversed(list(self._snapshot_log))
        ]

    def _build_snapshot_compare(self) -> dict[str, Any]:
        options = self._snapshot_options()
        if len(options) < 2:
            return {
                "available": False,
                "options": options,
                "left_sequence": None,
                "right_sequence": None,
                "summary": t("pages.devices.workbench.snapshot.no_compare", default="暂无可对比快照"),
                "details_text": t("pages.devices.workbench.snapshot.no_compare", default="暂无可对比快照"),
                "changes": [],
            }
        available_by_sequence = {
            int(item.get("sequence", 0) or 0): item
            for item in self._snapshot_log
        }
        ordered = list(reversed(options))
        default_left = int(ordered[1].get("sequence", 0) or 0)
        default_right = int(ordered[0].get("sequence", 0) or 0)
        left_sequence = self._selected_snapshot_left if self._selected_snapshot_left in available_by_sequence else default_left
        right_sequence = self._selected_snapshot_right if self._selected_snapshot_right in available_by_sequence else default_right
        if left_sequence == right_sequence:
            left_sequence = default_left
            right_sequence = default_right
        left_entry = available_by_sequence.get(int(left_sequence or 0))
        right_entry = available_by_sequence.get(int(right_sequence or 0))
        if left_entry is None or right_entry is None:
            return {
                "available": False,
                "options": options,
                "left_sequence": None,
                "right_sequence": None,
                "summary": t("pages.devices.workbench.snapshot.no_compare", default="暂无可对比快照"),
                "details_text": t("pages.devices.workbench.snapshot.no_compare", default="暂无可对比快照"),
                "changes": [],
            }
        changes = self._snapshot_compare_changes(
            dict(left_entry.get("snapshot", {}) or {}),
            dict(right_entry.get("snapshot", {}) or {}),
        )
        summary = t(
            "pages.devices.workbench.snapshot.compare_summary",
            left=left_entry.get("label", "--"),
            right=right_entry.get("label", "--"),
            change_count=len(changes),
            default=f"{left_entry.get('label', '--')} -> {right_entry.get('label', '--')}，变化 {len(changes)} 项",
        )
        detail_lines = [f"- {item['section']}: {item['left']} -> {item['right']}" for item in changes]
        if not detail_lines:
            detail_lines = [t("pages.devices.workbench.snapshot.no_change", default="未发现关键状态变化")]
        return {
            "available": True,
            "options": options,
            "left_sequence": left_sequence,
            "right_sequence": right_sequence,
            "left_label": left_entry.get("label", "--"),
            "right_label": right_entry.get("label", "--"),
            "summary": summary,
            "details_text": "\n".join([summary, ""] + detail_lines).strip(),
            "changes": changes,
        }

    def _snapshot_compare_changes(self, left: dict[str, Any], right: dict[str, Any]) -> list[dict[str, str]]:
        changes: list[dict[str, str]] = []
        pairs = (
            (
                t("pages.devices.workbench.snapshot.compare.reference", default="参考质量"),
                display_reference_quality(dict(left.get("evidence", {}).get("reference_quality", {}) or {}).get("reference_quality")),
                display_reference_quality(dict(right.get("evidence", {}).get("reference_quality", {}) or {}).get("reference_quality")),
            ),
            (
                t("pages.devices.workbench.snapshot.compare.route", default="路由/继电器"),
                str(dict(left.get("evidence", {}).get("route_physical_validation", {}) or {}).get("summary_line") or t("common.none")),
                str(dict(right.get("evidence", {}).get("route_physical_validation", {}) or {}).get("summary_line") or t("common.none")),
            ),
            (
                t("pages.devices.workbench.snapshot.compare.analyzer", default="分析仪"),
                str(dict(left.get("analyzer", {}).get("panel_status", {}) or {}).get("last_frame") or t("common.none")),
                str(dict(right.get("analyzer", {}).get("panel_status", {}) or {}).get("last_frame") or t("common.none")),
            ),
            (
                t("pages.devices.workbench.snapshot.compare.pace", default="PACE 错误队列"),
                " | ".join(str(item) for item in list(dict(left.get("pace", {}).get("panel_status", {}) or {}).get("error_queue", []) or [])) or t("common.none"),
                " | ".join(str(item) for item in list(dict(right.get("pace", {}).get("panel_status", {}) or {}).get("error_queue", []) or [])) or t("common.none"),
            ),
            (
                t("pages.devices.workbench.snapshot.compare.thermometer", default="测温仪输出"),
                " | ".join(str(item) for item in list(dict(left.get("thermometer", {}).get("panel_status", {}) or {}).get("ascii_preview", []) or [])) or t("common.none"),
                " | ".join(str(item) for item in list(dict(right.get("thermometer", {}).get("panel_status", {}) or {}).get("ascii_preview", []) or [])) or t("common.none"),
            ),
            (
                t("pages.devices.workbench.snapshot.compare.pressure", default="压力计输出"),
                " | ".join(str(item) for item in list(dict(left.get("pressure_gauge", {}).get("panel_status", {}) or {}).get("stream_preview", []) or [])) or t("common.none"),
                " | ".join(str(item) for item in list(dict(right.get("pressure_gauge", {}).get("panel_status", {}) or {}).get("stream_preview", []) or [])) or t("common.none"),
            ),
        )
        for section, left_value, right_value in pairs:
            if str(left_value) == str(right_value):
                continue
            changes.append({"section": section, "left": str(left_value), "right": str(right_value)})
        return changes

    def _is_fault_injection_action(self, device_kind: str, action: str, params: dict[str, Any]) -> bool:
        if str(device_kind) == "workbench":
            if str(action) == "run_quick_scenario":
                return True
            return False
        if str(action) == "inject_fault":
            return True
        if str(device_kind) == "thermometer" and str(action) == "set_mode":
            return str(params.get("mode") or "stable").strip().lower() not in {"stable", "plus_200_mode"}
        return False

    def _execute_workbench_action(self, action: str, **params: Any) -> tuple[str, dict[str, Any]]:
        if action == "set_view_mode":
            requested = str(params.get("view_mode") or "operator_view").strip().lower()
            self._view_mode = requested if requested in self.VIEW_MODES else "operator_view"
            self._save_persistent_state()
            return (
                t(
                    "pages.devices.workbench.message.view_mode_set",
                    view=t(f"pages.devices.workbench.view.{self._view_mode}", default=self._view_mode),
                ),
                {},
            )
        if action == "set_layout_mode":
            requested = str(params.get("layout_mode") or "compact").strip().lower()
            self._layout_mode = requested if requested in self.LAYOUT_MODES else "compact"
            self._save_persistent_state()
            return (
                t(
                    "pages.devices.workbench.message.layout_mode_set",
                    mode=t(f"pages.devices.workbench.layout.{self._layout_mode}", default=self._layout_mode),
                ),
                {},
            )
        if action == "set_display_profile":
            requested = str(params.get("display_profile") or "auto").strip().lower()
            self._display_profile = requested if requested in self.DISPLAY_PROFILES else "auto"
            self._save_persistent_state()
            return (
                t(
                    "pages.devices.workbench.message.display_profile_set",
                    profile=t(
                        f"pages.devices.workbench.display_profile.{self._display_profile}",
                        default=self._display_profile,
                    ),
                ),
                {},
            )
        if action == "refresh_display_profile_context":
            self._display_profile_context = self._normalize_display_profile_context(
                {
                    **dict(self._display_profile_context or {}),
                    "selected": self._display_profile,
                    "screen_width": params.get("screen_width"),
                    "screen_height": params.get("screen_height"),
                    "window_width": params.get("window_width"),
                    "window_height": params.get("window_height"),
                }
            )
            self._save_persistent_state()
            return (
                t(
                    "pages.devices.workbench.message.display_profile_context_refreshed",
                    profile=t(
                        f"pages.devices.workbench.display_profile_profile.{self._display_profile_payload(self._display_profile).get('resolved', '1080p_compact')}",
                        default=self._display_profile_payload(self._display_profile).get("resolved", "1080p_compact"),
                    ),
                    default="显示档位上下文已刷新",
                ),
                {"display_profile_meta": self._display_profile_payload(self._display_profile)},
            )
        if action == "set_history_filters":
            requested_device = str(params.get("device_filter") or "all").strip().lower() or "all"
            requested_result = str(params.get("result_filter") or "all").strip().lower() or "all"
            self._history_device_filter = requested_device
            self._history_result_filter = (
                requested_result
                if requested_result in self.HISTORY_RESULT_FILTERS
                else "all"
            )
            self._selected_history_sequence = None
            return (
                t(
                    "pages.devices.workbench.message.history_filter_set",
                    device=next(
                        (
                            str(item.get("label") or "")
                            for item in self._history_device_options(self._history_items())
                            if str(item.get("id") or "") == self._history_device_filter
                        ),
                        t("pages.devices.workbench.history.filter.all_devices", default="全部设备"),
                    ),
                    result=next(
                        (
                            str(item.get("label") or "")
                            for item in self._history_result_options()
                            if str(item.get("id") or "") == self._history_result_filter
                        ),
                        t("pages.devices.workbench.history.filter.all_results", default="全部结果"),
                    ),
                    default="动作历史筛选已更新",
                ),
                {},
            )
        if action == "select_history_detail":
            try:
                sequence = int(params.get("sequence", 0) or 0)
            except Exception:
                sequence = 0
            self._selected_history_sequence = sequence if sequence > 0 else None
            return (
                t("pages.devices.workbench.message.history_detail_selected", default="已切换动作详情"),
                {},
            )
        if action == "set_snapshot_compare":
            self._selected_snapshot_left = self._normalize_optional_int(params.get("left_sequence"))
            self._selected_snapshot_right = self._normalize_optional_int(params.get("right_sequence"))
            return (
                t("pages.devices.workbench.message.snapshot_compare_set", default="已更新快照对比"),
                {},
            )
        if action == "run_quick_scenario":
            scenario_id = str(params.get("scenario_id") or "analyzer_partial_frame").strip().lower()
            scenario_params = dict(params)
            scenario_params.pop("scenario_id", None)
            message = self._run_quick_scenario(scenario_id, **scenario_params)
            return message, {"is_fault_injection": True}
        if action == "run_preset":
            device_kind = str(params.get("device_kind") or params.get("current_device") or "workbench").strip().lower()
            message, extras = self._execute_device_preset(device_kind, **params)
            return message, extras
        if action == "toggle_preset_favorite":
            device_kind = self._normalize_device_kind(params.get("device_kind"))
            preset_id = str(params.get("preset_id") or "").strip().lower()
            if not device_kind or not preset_id:
                raise ValueError("preset selection is required")
            message, enabled = self._toggle_preset_favorite(device_kind, preset_id)
            return message, {"favorite_enabled": enabled}
        if action == "toggle_preset_pin":
            device_kind = self._normalize_device_kind(params.get("device_kind"))
            preset_id = str(params.get("preset_id") or "").strip().lower()
            if not device_kind or not preset_id:
                raise ValueError("preset selection is required")
            message, enabled = self._toggle_preset_pin(device_kind, preset_id)
            return message, {"pin_enabled": enabled}
        if action == "save_custom_preset":
            message, payload = self._save_custom_preset(**params)
            return message, payload
        if action == "delete_custom_preset":
            preset_id = str(params.get("preset_id") or "").strip().lower()
            if not preset_id:
                raise ValueError("custom preset selection is required")
            message, payload = self._delete_custom_preset(preset_id)
            return message, payload
        if action == "duplicate_preset":
            device_kind = self._normalize_device_kind(params.get("device_kind"))
            preset_id = str(params.get("preset_id") or "").strip().lower()
            message, payload = self._duplicate_preset(device_kind, preset_id)
            return message, payload
        if action == "export_preset_bundle":
            message, payload = self._export_preset_bundle(**params)
            return message, payload
        if action == "import_preset_bundle":
            message, payload = self._import_preset_bundle(
                str(params.get("bundle_text") or ""),
                conflict_policy=str(params.get("conflict_policy") or ""),
            )
            return message, payload
        if action == "generate_diagnostic_evidence":
            summary = self._generate_diagnostic_evidence(
                current_device=str(params.get("current_device") or "").strip().lower(),
                current_action=str(params.get("current_action") or "").strip().lower(),
            )
            return summary["message"], {
                "evidence_report": dict(summary),
                "is_fault_injection": False,
            }
        raise ValueError(f"unsupported workbench action: {action}")

    @staticmethod
    def _normalize_optional_int(value: Any) -> int | None:
        try:
            parsed = int(value)
        except Exception:
            return None
        return parsed if parsed > 0 else None

    def _bind_simulation_context(self) -> dict[str, Any]:
        raw_cfg = getattr(self.facade.service, "_raw_cfg", None)
        if not isinstance(raw_cfg, dict):
            raw_cfg = {}
            self.facade.service._raw_cfg = raw_cfg
        simulation_context = raw_cfg.get("simulation_context")
        if not isinstance(simulation_context, dict):
            simulation_context = {}
            raw_cfg["simulation_context"] = simulation_context

        for config in self.facade._mutable_configs():
            config_raw = getattr(config, "_raw_cfg", None)
            if not isinstance(config_raw, dict):
                config_raw = {}
                setattr(config, "_raw_cfg", config_raw)
            config_raw["simulation_context"] = simulation_context
        facade_config_raw = getattr(self.facade.config, "_raw_cfg", None)
        if isinstance(facade_config_raw, dict):
            facade_config_raw["simulation_context"] = simulation_context
        return simulation_context

    def _resolve_factory(self) -> DeviceFactory:
        service_factory = getattr(self.facade.service, "device_factory", None)
        if isinstance(service_factory, DeviceFactory) and bool(getattr(service_factory, "simulation_mode", False)):
            service_factory.simulation_context = self._simulation_context
            return service_factory
        return DeviceFactory(simulation_mode=True, simulation_context=self._simulation_context)

    def _service_device_manager_is_simulated(self) -> bool:
        device_manager = getattr(self.facade.service, "device_manager", None)
        service_factory = getattr(device_manager, "device_factory", None)
        return bool(getattr(service_factory, "simulation_mode", False))

    def _seed_simulation_context(self) -> None:
        defaults = SimulatedDeviceMatrix().to_dict()
        merged = _deep_merge(defaults, dict(self._simulation_context.get("device_matrix") or {}))
        analyzers = merged.setdefault("analyzers", {})
        analyzers["count"] = max(self.ANALYZER_COUNT, int(analyzers.get("count", self.ANALYZER_COUNT) or self.ANALYZER_COUNT))
        merged.setdefault("device_overrides", {})
        self._simulation_context["device_matrix"] = merged
        self._simulation_context.setdefault("scenario", "device_workbench")
        self._simulation_context.setdefault("description", t("pages.devices.workbench.summary.description"))
        self._simulation_context.setdefault("simulation_backend", "ui_workbench")
        self._simulation_context.setdefault(
            "protocol_devices",
            {
                "analyzer": "ygas",
                "pressure_controller": "pace_scpi",
                "humidity_generator": "grz5013",
                "temperature_chamber": "modbus",
                "relay": "modbus_rtu",
                "relay_8": "modbus_rtu",
                "thermometer": "ascii_stream",
                "pressure_gauge": "paroscientific_735_745",
            },
        )
        self._simulation_context.setdefault("workbench_actions", [])
        self._simulation_context.setdefault("workbench_route_trace", {})
        self._simulation_context.setdefault("workbench_reference_quality", {})
        self._simulation_context.setdefault("workbench_reports", [])
        self._factory.simulation_context = self._simulation_context

    def _device_matrix(self) -> dict[str, Any]:
        matrix = self._simulation_context.setdefault("device_matrix", {})
        if not isinstance(matrix, dict):
            matrix = {}
            self._simulation_context["device_matrix"] = matrix
        return matrix

    def _device_overrides(self) -> dict[str, Any]:
        matrix = self._device_matrix()
        overrides = matrix.setdefault("device_overrides", {})
        if not isinstance(overrides, dict):
            overrides = {}
            matrix["device_overrides"] = overrides
        return overrides

    def _set_override(self, device_name: str, **updates: Any) -> None:
        overrides = self._device_overrides().setdefault(str(device_name), {})
        if not isinstance(overrides, dict):
            overrides = {}
            self._device_overrides()[str(device_name)] = overrides
        for key, value in updates.items():
            overrides[str(key)] = copy.deepcopy(value)

    def _update_spec(self, spec_key: str, **updates: Any) -> None:
        spec = self._device_matrix().setdefault(spec_key, {})
        if not isinstance(spec, dict):
            spec = {}
            self._device_matrix()[spec_key] = spec
        for key, value in updates.items():
            spec[str(key)] = copy.deepcopy(value)

    def _append_simulation_action(self, entry: dict[str, Any]) -> None:
        actions = self._simulation_context.setdefault("workbench_actions", [])
        if not isinstance(actions, list):
            actions = []
            self._simulation_context["workbench_actions"] = actions
        actions.append(copy.deepcopy(entry))

    def _collect_active_faults(
        self,
        *,
        analyzer_snapshot: dict[str, Any],
        pace_snapshot: dict[str, Any],
        grz_snapshot: dict[str, Any],
        chamber_snapshot: dict[str, Any],
        relay_snapshot: dict[str, Any],
        thermometer_snapshot: dict[str, Any],
        pressure_snapshot: dict[str, Any],
        reference_quality: dict[str, Any],
        route_validation: dict[str, Any],
    ) -> list[str]:
        faults: list[str] = []
        analyzer_fault = str(dict(analyzer_snapshot.get("injection_state", {}) or {}).get("mode2_stream") or "stable")
        if analyzer_fault != "stable":
            faults.append(
                t(
                    "pages.devices.workbench.summary.fault_item",
                    device=analyzer_snapshot.get("title", self._device_display("analyzer")),
                    fault=t(f"pages.devices.workbench.enum.analyzer_fault.{analyzer_fault}", default=analyzer_fault),
                )
            )
        if str(dict(pace_snapshot.get("injection_state", {}) or {}).get("mode") or "stable") != "stable":
            pace_fault = str(dict(pace_snapshot.get("injection_state", {}) or {}).get("mode") or "stable")
            faults.append(
                t(
                    "pages.devices.workbench.summary.fault_item",
                    device=pace_snapshot.get("title", self._device_display("pace")),
                    fault=t(f"pages.devices.workbench.enum.pace_fault.{pace_fault}", default=pace_fault),
                )
            )
        if bool(dict(pace_snapshot.get("injection_state", {}) or {}).get("wrong_unit_configuration", False)):
            faults.append(
                t(
                    "pages.devices.workbench.summary.fault_item",
                    device=pace_snapshot.get("title", self._device_display("pace")),
                    fault=t("pages.devices.workbench.enum.pace_fault.wrong_unit_configuration"),
                )
            )
        if str(dict(grz_snapshot.get("injection_state", {}) or {}).get("mode") or "stable") != "stable":
            grz_fault = str(dict(grz_snapshot.get("injection_state", {}) or {}).get("mode") or "stable")
            faults.append(
                t(
                    "pages.devices.workbench.summary.fault_item",
                    device=grz_snapshot.get("title", self._device_display("grz")),
                    fault=t(f"pages.devices.workbench.enum.grz_fault.{grz_fault}", default=grz_fault),
                )
            )
        chamber_mode = str(dict(chamber_snapshot.get("injection_state", {}) or {}).get("mode") or "stable")
        if chamber_mode not in {"stable", "on_target"}:
            faults.append(
                t(
                    "pages.devices.workbench.summary.fault_item",
                    device=chamber_snapshot.get("title", self._device_display("chamber")),
                    fault=t(f"pages.devices.workbench.enum.chamber_mode.{chamber_mode}", default=chamber_mode),
                )
            )
        for relay_name, relay_payload in dict(relay_snapshot.get("injection_state", {}) or {}).items():
            relay_mode = str(dict(relay_payload or {}).get("mode") or "stable")
            if relay_mode != "stable":
                faults.append(
                    t(
                        "pages.devices.workbench.summary.fault_item",
                        device=t(f"pages.devices.workbench.device.{relay_name}", default=relay_name),
                        fault=t(f"pages.devices.workbench.enum.relay_fault.{relay_mode}", default=relay_mode),
                    )
                )
        thermometer_mode = str(dict(thermometer_snapshot.get("injection_state", {}) or {}).get("mode") or "stable")
        if thermometer_mode not in {"stable", "plus_200_mode"}:
            faults.append(
                t(
                    "pages.devices.workbench.summary.fault_item",
                    device=thermometer_snapshot.get("title", self._device_display("thermometer")),
                    fault=t(f"pages.devices.workbench.enum.thermometer_mode.{thermometer_mode}", default=thermometer_mode),
                )
            )
        pressure_mode = str(dict(pressure_snapshot.get("injection_state", {}) or {}).get("mode") or "stable")
        if pressure_mode not in {"stable", "continuous_stream", "sample_hold"}:
            faults.append(
                t(
                    "pages.devices.workbench.summary.fault_item",
                    device=pressure_snapshot.get("title", self._device_display("pressure_gauge")),
                    fault=t(f"pages.devices.workbench.enum.pressure_fault.{pressure_mode}", default=pressure_mode),
                )
            )
        if bool(route_validation.get("relay_physical_mismatch", False)):
            faults.append(t("pages.devices.workbench.summary.route_mismatch_active"))
        if str(reference_quality.get("reference_quality") or "healthy") != "healthy":
            faults.append(
                t(
                    "pages.devices.workbench.summary.reference_fault_item",
                    quality=display_reference_quality(reference_quality.get("reference_quality")),
                )
            )
        return faults

    def _build_operator_summary(
        self,
        *,
        active_faults: list[str],
        reference_quality: dict[str, Any],
        route_validation: dict[str, Any],
        history_items: list[dict[str, Any]],
        history_payload: dict[str, Any],
    ) -> dict[str, Any]:
        recent_history = history_items[:3]
        recent_history_text = " | ".join(
            t(
                "pages.devices.workbench.summary.history_item",
                seq=item.get("sequence", "--"),
                device=item.get("device_display", "--"),
                action=item.get("action_display", "--"),
                result=item.get("result_display", "--"),
            )
            for item in recent_history
        ) or t("common.none")
        history_filters = dict(history_payload.get("filters", {}) or {})
        filtered_count = int(history_filters.get("filtered_count", len(history_items)) or len(history_items))
        total_count = int(history_filters.get("total_count", filtered_count) or filtered_count)
        history_count_text = (
            f"{filtered_count}/{total_count}"
            if filtered_count != total_count
            else str(total_count)
        )
        return {
            "health_summary": t(
                "pages.devices.workbench.summary.health",
                status=t(
                    "pages.devices.workbench.summary.health_stable"
                    if not active_faults
                    else "pages.devices.workbench.summary.health_attention"
                ),
            ),
            "fault_summary": " | ".join(active_faults) or t("pages.devices.workbench.summary.no_faults"),
            "reference_summary": t(
                "pages.devices.workbench.summary.reference",
                quality=display_reference_quality(reference_quality.get("reference_quality")),
                thermometer=t(
                    f"pages.devices.workbench.enum.reference_status.{str(reference_quality.get('thermometer_reference_status') or 'not_assessed')}",
                    default=str(reference_quality.get("thermometer_reference_status") or "not_assessed"),
                ),
                pressure=t(
                    f"pages.devices.workbench.enum.reference_status.{str(reference_quality.get('pressure_reference_status') or 'not_assessed')}",
                    default=str(reference_quality.get("pressure_reference_status") or "not_assessed"),
                ),
            ),
            "route_summary": t(
                "pages.devices.workbench.summary.route",
                status=t(
                    "pages.devices.workbench.enum.route_match.match"
                    if bool(route_validation.get("route_physical_state_match", True))
                    else "pages.devices.workbench.enum.route_match.mismatch"
                ),
                detail=str(route_validation.get("summary_line") or t("common.none")),
            ),
            "history_summary": t(
                "pages.devices.workbench.summary.history",
                count=history_count_text,
                detail=recent_history_text,
            ),
            "risk_summary": t(
                "pages.devices.workbench.summary.risk_attention"
                if active_faults
                else "pages.devices.workbench.summary.risk_safe"
            ),
            "last_evidence_summary": (
                str(self._last_evidence_report.get("summary_line") or "")
                if self._last_evidence_report
                else t("pages.devices.workbench.summary.no_evidence")
            ),
        }

    @staticmethod
    def _normalize_engineer_data_state_token(value: Any) -> str:
        return str(value or "").strip().lower()

    def _engineer_data_state(
        self,
        payload: dict[str, Any],
        path: Path,
        *,
        failure_tokens: Optional[list[Any]] = None,
    ) -> str:
        if not payload and not path.exists():
            return "no_data"
        if bool(payload.get("diagnostic_only", False)):
            return "diagnostic_only"
        tokens = {
            self._normalize_engineer_data_state_token(item)
            for item in list(failure_tokens or [])
            if self._normalize_engineer_data_state_token(item)
        }
        if tokens.intersection({"failed", "error", "critical", "mismatch"}):
            return "failed"
        return "ready"

    @staticmethod
    def _engineer_data_state_display(state: str) -> str:
        return t(f"pages.devices.workbench.engineer_data_state.{state}", default=state)

    def _build_engineer_summary(
        self,
        *,
        analyzer_snapshot: dict[str, Any],
        pace_snapshot: dict[str, Any],
        grz_snapshot: dict[str, Any],
        chamber_snapshot: dict[str, Any],
        relay_snapshot: dict[str, Any],
        thermometer_snapshot: dict[str, Any],
        pressure_snapshot: dict[str, Any],
        reference_quality: dict[str, Any],
        route_validation: dict[str, Any],
        history_items: list[dict[str, Any]],
        history_payload: dict[str, Any],
        snapshot_compare: dict[str, Any],
        analytics_summary_payload: dict[str, Any],
        qc_review_summary: dict[str, Any],
        config_safety: dict[str, Any],
        config_safety_review: dict[str, Any],
        point_taxonomy_summary: dict[str, Any],
        measurement_core_evidence: dict[str, Any],
        recognition_readiness_evidence: dict[str, Any],
    ) -> dict[str, Any]:
        diagnostics = {
            "reference_quality": reference_quality,
            "route_physical_validation": route_validation,
            "recent_action_history": history_items,
            "analyzer": {
                "panel_status": dict(analyzer_snapshot.get("panel_status", {}) or {}),
                "injection_state": dict(analyzer_snapshot.get("injection_state", {}) or {}),
            },
            "pace": {
                "panel_status": dict(pace_snapshot.get("panel_status", {}) or {}),
                "injection_state": dict(pace_snapshot.get("injection_state", {}) or {}),
            },
            "grz": {
                "panel_status": dict(grz_snapshot.get("panel_status", {}) or {}),
                "injection_state": dict(grz_snapshot.get("injection_state", {}) or {}),
            },
            "chamber": {
                "panel_status": dict(chamber_snapshot.get("panel_status", {}) or {}),
                "injection_state": dict(chamber_snapshot.get("injection_state", {}) or {}),
            },
            "relay": {
                "panel_status": dict(relay_snapshot.get("panel_status", {}) or {}),
                "injection_state": dict(relay_snapshot.get("injection_state", {}) or {}),
            },
            "thermometer": {
                "panel_status": dict(thermometer_snapshot.get("panel_status", {}) or {}),
                "injection_state": dict(thermometer_snapshot.get("injection_state", {}) or {}),
            },
            "pressure_gauge": {
                "panel_status": dict(pressure_snapshot.get("panel_status", {}) or {}),
                "injection_state": dict(pressure_snapshot.get("injection_state", {}) or {}),
            },
        }
        analyzer_panel = dict(analyzer_snapshot.get("panel_status", {}) or {})
        pace_panel = dict(pace_snapshot.get("panel_status", {}) or {})
        grz_panel = dict(grz_snapshot.get("panel_status", {}) or {})
        chamber_panel = dict(chamber_snapshot.get("panel_status", {}) or {})
        relay_panel = dict(relay_snapshot.get("panel_status", {}) or {})
        thermometer_panel = dict(thermometer_snapshot.get("panel_status", {}) or {})
        pressure_panel = dict(pressure_snapshot.get("panel_status", {}) or {})
        recent_history = " | ".join(
            t(
                "pages.devices.workbench.summary.history_item",
                seq=item.get("sequence", "--"),
                device=item.get("device_display", "--"),
                action=item.get("action_display", "--"),
                result=item.get("result_display", "--"),
            )
            for item in history_items[:3]
        ) or t("common.none")
        reference_summary = t(
            "pages.devices.workbench.summary.reference",
            quality=display_reference_quality(reference_quality.get("reference_quality")),
            thermometer=t(
                f"pages.devices.workbench.enum.reference_status.{str(reference_quality.get('thermometer_reference_status') or 'not_assessed')}",
                default=str(reference_quality.get("thermometer_reference_status") or "not_assessed"),
            ),
            pressure=t(
                f"pages.devices.workbench.enum.reference_status.{str(reference_quality.get('pressure_reference_status') or 'not_assessed')}",
                default=str(reference_quality.get("pressure_reference_status") or "not_assessed"),
            ),
        )
        route_summary = t(
            "pages.devices.workbench.summary.route",
            status=t(
                "pages.devices.workbench.enum.route_match.match"
                if bool(route_validation.get("route_physical_state_match", True))
                else "pages.devices.workbench.enum.route_match.mismatch"
            ),
            detail=str(route_validation.get("summary_line") or t("common.none")),
        )
        evidence_summary = str(self._last_evidence_report.get("summary_line") or t("pages.devices.workbench.summary.no_evidence"))
        last_evidence_paths = dict(self._last_evidence_report.get("paths", {}) or {})
        artifact_role_display = display_artifact_role(self._last_evidence_report.get("artifact_role"))
        evidence_source_display = display_evidence_source(
            self._last_evidence_report.get("evidence_source"),
            default=str(self._last_evidence_report.get("evidence_source") or self.WORKBENCH_EVIDENCE_SOURCE),
        )
        evidence_state_display = display_evidence_state(
            self._last_evidence_report.get("evidence_state"),
            default=t("common.none"),
        )
        artifact_lineage_summary = t(
            "pages.devices.workbench.engineer_card.artifact_lineage_summary",
            role=artifact_role_display,
            source=evidence_source_display,
            state=evidence_state_display,
            report=str(last_evidence_paths.get("report_json") or t("common.none")),
            default=f"{artifact_role_display} | {evidence_source_display} | {evidence_state_display}",
        )
        config_safety_summary = str(
            config_safety_review.get("summary")
            or config_safety.get("summary")
            or "当前配置为 simulation-only，未发现真实串口风险。"
        )
        if "配置安全" not in config_safety_summary:
            config_safety_summary = f"配置安全: {config_safety_summary}"
        config_inventory_summary = str(
            config_safety_review.get("inventory_summary")
            or config_safety.get("inventory_summary")
            or "--"
        )
        config_safety_lines = [
            f"配置安全: {config_safety_summary}",
            f"库存分类: {str(config_safety.get('classification_display') or config_safety.get('classification') or '--')}",
            f"库存摘要: {config_inventory_summary}",
            "治理标记: "
            + (" / ".join(str(item.get("label") or "--") for item in list(config_safety.get("badges") or [])) or "--"),
        ]
        blocked_reason_lines = [
            f"- {str(item.get('title') or '--')}: {str(item.get('summary') or '--')}"
            for item in list(config_safety_review.get("blocked_reason_details") or [])[:2]
        ]
        point_taxonomy_lines = self._point_taxonomy_lines(point_taxonomy_summary)
        point_taxonomy_summary_text = " | ".join(point_taxonomy_lines) or t(
            "pages.devices.workbench.engineer_section.point_taxonomy.empty",
            default="当前 run 没有可汇总的点位语义摘要。",
        )
        suite_summary_payload = dict(self.facade.results_gateway.load_json("suite_summary.json") or {})
        suite_analytics_payload = dict(self.facade.results_gateway.load_json("suite_analytics_summary.json") or {})
        lineage_summary_payload = dict(self.facade.results_gateway.load_json("lineage_summary.json") or {})
        suite_summary_path = Path(self.facade.results_gateway.run_dir) / "suite_summary.json"
        suite_analytics_path = Path(self.facade.results_gateway.run_dir) / "suite_analytics_summary.json"
        analytics_summary_path = Path(self.facade.results_gateway.run_dir) / "analytics_summary.json"
        lineage_summary_path = Path(self.facade.results_gateway.run_dir) / "lineage_summary.json"
        suite_counts = dict(suite_summary_payload.get("counts", {}) or {})
        analytics_digest = dict(analytics_summary_payload.get("digest", {}) or suite_analytics_payload.get("digest", {}) or {})
        analytics_reference_stats = dict(analytics_summary_payload.get("reference_quality_statistics", {}) or {})
        analytics_export_status = dict(analytics_summary_payload.get("export_resilience_status", {}) or {})
        suite_state = self._engineer_data_state(
            suite_summary_payload,
            suite_summary_path,
            failure_tokens=[
                "mismatch" if suite_summary_payload and not bool(suite_summary_payload.get("all_passed", False)) else "",
            ],
        )
        analytics_state = self._engineer_data_state(
            analytics_summary_payload,
            analytics_summary_path,
            failure_tokens=[
                analytics_summary_payload.get("status"),
                analytics_digest.get("health"),
                analytics_export_status.get("overall_status"),
            ],
        )
        lineage_state = self._engineer_data_state(
            lineage_summary_payload,
            lineage_summary_path,
            failure_tokens=[
                lineage_summary_payload.get("status"),
                lineage_summary_payload.get("overall_status"),
            ],
        )
        diagnostics["suite_analytics_state"] = {
            "suite": suite_state,
            "analytics": analytics_state,
            "lineage": lineage_state,
        }
        diagnostics["qc_review_summary"] = dict(qc_review_summary)
        diagnostics["config_safety"] = dict(config_safety)
        diagnostics["config_safety_review"] = dict(config_safety_review)
        diagnostics["point_taxonomy_summary"] = dict(point_taxonomy_summary)
        diagnostics["measurement_core_evidence"] = dict(measurement_core_evidence)
        diagnostics["recognition_readiness_evidence"] = dict(recognition_readiness_evidence)
        suite_state_display = self._engineer_data_state_display(suite_state)
        analytics_state_display = self._engineer_data_state_display(analytics_state)
        lineage_state_display = self._engineer_data_state_display(lineage_state)
        suite_name = str(suite_summary_payload.get("suite") or suite_state_display)
        suite_status_display = (
            suite_state_display
            if suite_state != "ready"
            else t(
                "results.review_center.status.passed"
                if bool(suite_summary_payload.get("all_passed", False))
                else "results.review_center.status.failed"
            )
        )
        analytics_coverage_text = (
            str(dict(analytics_summary_payload.get("analyzer_coverage", {}) or {}).get("coverage_text") or "--")
            if analytics_state == "ready"
            else analytics_state_display
        )
        lineage_summary_line = (
            t(
                "results.review_center.lineage.summary_line",
                config=str(lineage_summary_payload.get("config_version") or "--"),
                points=str(lineage_summary_payload.get("points_version") or "--"),
                profile=str(lineage_summary_payload.get("profile_version") or "--"),
            )
            if lineage_state == "ready"
            else lineage_state_display
        )
        analytics_health = str(
            analytics_digest.get("health")
            or analytics_export_status.get("overall_status")
            or analytics_reference_stats.get("reference_quality_trend")
            or ""
        ).strip().lower()
        analytics_health_display = (
            self.facade._humanize_ui_summary(str(analytics_digest.get("summary") or analytics_health or t("common.none")))
            if analytics_state == "ready"
            else analytics_state_display
        )
        analytics_reference_quality_display = (
            display_reference_quality(analytics_reference_stats.get("reference_quality"))
            if analytics_state == "ready"
            else analytics_state_display
        )
        analytics_reference_trend = (
            str(analytics_reference_stats.get("reference_quality_trend") or "--")
            if analytics_state == "ready"
            else analytics_state_display
        )
        analytics_export_display = (
            str(analytics_export_status.get("overall_status") or "--")
            if analytics_state == "ready"
            else analytics_state_display
        )
        suite_analytics_card_summary = t(
            "pages.devices.workbench.engineer_card.suite_analytics_summary",
            suite=suite_name,
            status=suite_status_display,
            analytics=analytics_health_display,
            lineage=str(lineage_summary_payload.get("config_version") or lineage_state_display),
            default=f"{suite_name} | {suite_status_display} | {analytics_health_display} | {lineage_summary_payload.get('config_version') or lineage_state_display}",
        )
        suite_analytics_card_summary = " | ".join(
            [
                str(suite_analytics_card_summary or "").strip(),
                str(analytics_coverage_text or "").strip(),
                str(analytics_export_display or "").strip(),
            ]
        )
        reference_reasons = [str(item) for item in list(reference_quality.get("reasons") or []) if str(item).strip()]
        reference_quality_trend = str(reference_quality.get("reference_quality") or "healthy")
        reference_note = t(
            "pages.devices.workbench.engineer_block.reference_note",
            thermometer=thermometer_panel.get("reference_status_display", "--"),
            pressure=pressure_panel.get("reference_status_display", "--"),
            default=f"温度参考 {thermometer_panel.get('reference_status_display', '--')} | 压力参考 {pressure_panel.get('reference_status_display', '--')}",
        )
        measurement_core_summary = str(measurement_core_evidence.get("summary_line") or t("common.none"))
        measurement_core_lines = [
            str(item)
            for item in list(measurement_core_evidence.get("summary_lines") or [])
            if str(item).strip()
        ]
        measurement_core_boundaries = [
            str(item)
            for item in list(measurement_core_evidence.get("boundary_lines") or [])
            if str(item).strip()
        ]
        recognition_readiness_summary = str(recognition_readiness_evidence.get("summary_line") or t("common.none"))
        recognition_readiness_lines = [
            str(item)
            for item in list(recognition_readiness_evidence.get("summary_lines") or [])
            if str(item).strip()
        ]
        recognition_readiness_detail_lines = [
            str(item)
            for item in list(recognition_readiness_evidence.get("detail_lines") or [])
            if str(item).strip()
        ]
        recognition_readiness_boundaries = [
            str(item)
            for item in list(recognition_readiness_evidence.get("boundary_lines") or [])
            if str(item).strip()
        ]
        cards = [
            {
                "title": t("pages.devices.workbench.engineer_card.reference"),
                "summary": reference_summary,
            },
            {
                "title": t("pages.devices.workbench.engineer_card.route"),
                "summary": route_summary,
            },
            {
                "title": t("pages.devices.workbench.engineer_card.history"),
                "summary": recent_history,
            },
            {
                "title": t("pages.devices.workbench.engineer_card.evidence"),
                "summary": evidence_summary,
            },
            {
                "title": t("pages.devices.workbench.engineer_card.statistics"),
                "summary": t(
                    "pages.devices.workbench.engineer_card.statistics_summary",
                    success=sum(1 for item in history_items[:8] if str(item.get("result") or "") == "ok"),
                    failed=sum(1 for item in history_items[:8] if str(item.get("result") or "") == "failed"),
                    devices=len(
                        {
                            str(item.get("device") or "").strip()
                            for item in history_items[:8]
                            if str(item.get("device") or "").strip()
                        }
                    ),
                    default="Recent workbench statistics",
                ),
            },
            {
                "title": t("pages.devices.workbench.engineer_card.suite_analytics"),
                "summary": suite_analytics_card_summary,
            },
            {
                "title": t("pages.devices.workbench.engineer_card.artifact_lineage"),
                "summary": artifact_lineage_summary,
            },
            {
                "title": t("shell.nav.qc"),
                "summary": str(qc_review_summary.get("summary") or t("common.none")),
            },
            {
                "title": t("pages.devices.workbench.engineer_card.config_safety", default="配置安全"),
                "summary": config_safety_summary,
            },
            {
                "title": t("pages.devices.workbench.engineer_card.point_taxonomy", default="点位语义"),
                "summary": point_taxonomy_summary_text,
            },
            {
                "title": t("pages.devices.workbench.engineer_card.measurement_core", default="measurement-core readiness"),
                "summary": measurement_core_summary,
            },
            {
                "title": t(
                    "pages.devices.workbench.engineer_card.recognition_readiness",
                    default="认可就绪治理骨架",
                ),
                "summary": recognition_readiness_summary,
            },
        ]
        device_lines = [
            f"{t('pages.devices.workbench.device.analyzer')}: MODE={analyzer_panel.get('mode_display', '--')} | Active={analyzer_panel.get('active_send_display', '--')} | 最近帧={analyzer_panel.get('last_frame') or t('common.none')}",
            f"{t('pages.devices.workbench.device.pace')}: 当前={pace_panel.get('pressure_display', '--')} | 目标={pace_panel.get('target_pressure_display', '--')} | 错误={', '.join(str(item) for item in list(pace_panel.get('error_queue', []) or [])) or t('common.none')}",
            f"{t('pages.devices.workbench.device.grz')}: 温度={grz_panel.get('current_temp_display', '--')} | 湿度={grz_panel.get('current_rh_pct', '--')} | 露点={grz_panel.get('dewpoint_display', '--')}",
            f"{t('pages.devices.workbench.device.chamber')}: 温度={chamber_panel.get('temperature_display', '--')} | 湿度={chamber_panel.get('humidity_pct', '--')} | Soak={chamber_panel.get('soak_state_display', '--')}",
            f"{t('pages.devices.workbench.device.relay')}: {relay_panel.get('summary_line') or t('common.none')}",
            f"{t('pages.devices.workbench.device.thermometer')}: {thermometer_panel.get('temperature_display', '--')} | 参考={thermometer_panel.get('reference_status_display', '--')}",
            f"{t('pages.devices.workbench.device.pressure_gauge')}: {pressure_panel.get('pressure_display', '--')} | 单位={pressure_panel.get('unit', '--')} | 模式={pressure_panel.get('measurement_mode_display', '--')}",
        ]
        injection_lines = []
        for device_name, device_payload in (
            (t("pages.devices.workbench.device.analyzer"), dict(analyzer_snapshot.get("injection_state", {}) or {})),
            (t("pages.devices.workbench.device.pace"), dict(pace_snapshot.get("injection_state", {}) or {})),
            (t("pages.devices.workbench.device.grz"), dict(grz_snapshot.get("injection_state", {}) or {})),
            (t("pages.devices.workbench.device.chamber"), dict(chamber_snapshot.get("injection_state", {}) or {})),
            (t("pages.devices.workbench.device.relay"), dict(relay_snapshot.get("injection_state", {}) or {})),
            (t("pages.devices.workbench.device.thermometer"), dict(thermometer_snapshot.get("injection_state", {}) or {})),
            (t("pages.devices.workbench.device.pressure_gauge"), dict(pressure_snapshot.get("injection_state", {}) or {})),
        ):
            rendered = " | ".join(
                f"{key}={value}"
                for key, value in device_payload.items()
                if value not in ("", None, [], {})
            )
            if rendered:
                injection_lines.append(f"{device_name}: {rendered}")
        recent_action_window = history_items[:5]
        success_count = sum(1 for item in recent_action_window if str(item.get("result") or "") == "ok")
        failed_count = sum(1 for item in recent_action_window if str(item.get("result") or "") == "failed")
        fault_action_count = sum(1 for item in recent_action_window if bool(item.get("is_fault_injection", False)))
        recent_device_count = len(
            {
                str(item.get("device") or "").strip()
                for item in recent_action_window
                if str(item.get("device") or "").strip()
            }
        )
        compare_change_count = len(list(snapshot_compare.get("changes", []) or []))
        last_evidence_text = t("pages.devices.workbench.summary.no_evidence")
        if self._last_evidence_report:
            evidence_paths = dict(self._last_evidence_report.get("paths", {}) or {})
            last_evidence_text = "\n".join(
                [
                    t(
                        "pages.devices.workbench.summary.last_evidence",
                        generated_at=self._last_evidence_report.get("generated_at", "--"),
                        role=display_artifact_role(self._last_evidence_report.get("artifact_role")),
                        state=display_evidence_state(self._last_evidence_report.get("evidence_state")),
                    ),
                    str(self._last_evidence_report.get("summary_line") or t("common.none")),
                    str(evidence_paths.get("report_json") or t("common.none")),
                ]
            )
        anomaly_lines: list[str] = []
        if not bool(route_validation.get("route_physical_state_match", True)):
            anomaly_lines.append(str(route_validation.get("summary_line") or t("common.none")))
        if str(reference_quality.get("reference_quality") or "healthy") != "healthy":
            anomaly_lines.append(reference_note)
        if injection_lines:
            anomaly_lines.extend(injection_lines[:2])
        if self._last_evidence_report:
            anomaly_lines.append(str(self._last_evidence_report.get("summary_line") or t("common.none")))
        anomaly_lines = [str(item) for item in anomaly_lines if str(item).strip()]
        status_blocks = [
            {
                "title": t("pages.devices.workbench.engineer_block.route"),
                "value": t(
                    "pages.devices.workbench.enum.route_match.match"
                    if bool(route_validation.get("route_physical_state_match", True))
                    else "pages.devices.workbench.enum.route_match.mismatch"
                ),
                "note": str(route_validation.get("summary_line") or t("common.none")),
                "severity": "ok" if bool(route_validation.get("route_physical_state_match", True)) else "warning",
                "severity_display": t(
                    "pages.devices.workbench.engineer_severity.ok"
                    if bool(route_validation.get("route_physical_state_match", True))
                    else "pages.devices.workbench.engineer_severity.warning"
                ),
            },
            {
                "title": t("pages.devices.workbench.engineer_block.reference"),
                "value": display_reference_quality(reference_quality.get("reference_quality")),
                "note": reference_note,
                "severity": "ok" if str(reference_quality.get("reference_quality") or "healthy") == "healthy" else "warning",
                "severity_display": t(
                    "pages.devices.workbench.engineer_severity.ok"
                    if str(reference_quality.get("reference_quality") or "healthy") == "healthy"
                    else "pages.devices.workbench.engineer_severity.warning"
                ),
            },
            {
                "title": t("pages.devices.workbench.engineer_block.faults"),
                "value": str(len(injection_lines)),
                "note": injection_lines[0] if injection_lines else t("pages.devices.workbench.engineer_section.injection_state.empty"),
                "severity": "warning" if injection_lines else "ok",
                "severity_display": t(
                    "pages.devices.workbench.engineer_severity.warning"
                    if injection_lines
                    else "pages.devices.workbench.engineer_severity.ok"
                ),
            },
            {
                "title": t("pages.devices.workbench.engineer_block.evidence"),
                "value": display_evidence_state(self._last_evidence_report.get("evidence_state"), default=t("common.none")),
                "note": str(self._last_evidence_report.get("summary_line") or t("pages.devices.workbench.summary.no_evidence")),
                "severity": "info",
                "severity_display": t("pages.devices.workbench.engineer_severity.info"),
            },
        ]
        trend_blocks = [
            {
                "title": t("pages.devices.workbench.engineer_trend.actions"),
                "value": t(
                    "pages.devices.workbench.engineer_trend.actions_value",
                    success=success_count,
                    failed=failed_count,
                    default=f"成功 {success_count} / 失败 {failed_count}",
                ),
                "note": recent_history,
            },
            {
                "title": t("pages.devices.workbench.engineer_trend.faults"),
                "value": t(
                    "pages.devices.workbench.engineer_trend.faults_value",
                    count=fault_action_count,
                    default=f"最近故障动作 {fault_action_count}",
                ),
                "note": t(
                    "pages.devices.workbench.engineer_trend.faults_note",
                    recent=len(recent_action_window),
                    default=f"最近 {len(recent_action_window)} 条动作窗口",
                ),
            },
            {
                "title": t("pages.devices.workbench.engineer_trend.snapshots"),
                "value": t(
                    "pages.devices.workbench.engineer_trend.snapshots_value",
                    count=len(list(snapshot_compare.get("options", []) or [])),
                    default=f"快照 {len(list(snapshot_compare.get('options', []) or []))}",
                ),
                "note": str(snapshot_compare.get("summary") or t("pages.devices.workbench.snapshot.no_compare")),
            },
            {
                "title": t("pages.devices.workbench.engineer_trend.presets"),
                "value": t(
                    "pages.devices.workbench.engineer_trend.presets_value",
                    recent=len(self._recent_presets),
                    favorites=len(self._favorite_presets),
                    default=f"最近 {len(self._recent_presets)} / 收藏 {len(self._favorite_presets)}",
                ),
                "note": str((list(self._recent_presets)[0].get("label") if self._recent_presets else t("common.none"))),
            },
            {
                "title": t("pages.devices.workbench.engineer_trend.evidence"),
                "value": t(
                    "pages.devices.workbench.engineer_trend.evidence_value",
                    state=display_evidence_state(self._last_evidence_report.get("evidence_state"), default=t("common.none")),
                    changes=compare_change_count,
                    default=f"{display_evidence_state(self._last_evidence_report.get('evidence_state'), default=t('common.none'))} / 变更 {compare_change_count}",
                ),
                "note": str(self._last_evidence_report.get("summary_line") or t("pages.devices.workbench.summary.no_evidence")),
            },
            {
                "title": t("pages.devices.workbench.engineer_trend.devices"),
                "value": t(
                    "pages.devices.workbench.engineer_trend.devices_value",
                    count=recent_device_count,
                    default=f"设备 {recent_device_count}",
                ),
                "note": t(
                    "pages.devices.workbench.engineer_trend.devices_note",
                    recent=len(recent_action_window),
                    default=f"最近 {len(recent_action_window)} 条动作覆盖",
                ),
            },
            {
                "title": t("pages.devices.workbench.engineer_trend.reference_quality"),
                "value": t(
                    "pages.devices.workbench.engineer_trend.reference_quality_value",
                    quality=display_reference_quality(reference_quality_trend),
                    trend=t(
                        f"pages.devices.workbench.enum.reference_status.{reference_quality_trend}",
                        default=reference_quality_trend,
                    ),
                    default=f"{display_reference_quality(reference_quality_trend)} / {reference_quality_trend}",
                ),
                "note": t(
                    "pages.devices.workbench.engineer_trend.reference_quality_note",
                    reasons=", ".join(reference_reasons) or t("common.none"),
                    default=", ".join(reference_reasons) or t("common.none"),
                ),
            },
            {
                "title": t("pages.devices.workbench.engineer_trend.artifact_lineage"),
                "value": t(
                    "pages.devices.workbench.engineer_trend.artifact_lineage_value",
                    role=artifact_role_display,
                    source=evidence_source_display,
                    state=evidence_state_display,
                    default=f"{artifact_role_display} / {evidence_source_display} / {evidence_state_display}",
                ),
                "note": t(
                    "pages.devices.workbench.engineer_trend.artifact_lineage_note",
                    report=str(last_evidence_paths.get("report_json") or t("common.none")),
                    snapshot=str(last_evidence_paths.get("snapshot_json") or t("common.none")),
                    default=(
                        f"{last_evidence_paths.get('report_json') or t('common.none')} | "
                        f"{last_evidence_paths.get('snapshot_json') or t('common.none')}"
                    ),
                ),
            },
            {
                "title": t("pages.devices.workbench.engineer_trend.suite_analytics"),
                "value": t(
                    "pages.devices.workbench.engineer_trend.suite_analytics_value",
                    suite=suite_name,
                    passed=suite_counts.get("passed", 0),
                    total=suite_counts.get("total", 0),
                    analytics=analytics_health_display,
                    default=f"{suite_name} {suite_counts.get('passed', 0)}/{suite_counts.get('total', 0)} | {analytics_health_display}",
                ),
                "note": " | ".join(
                    [
                        t(
                            "pages.devices.workbench.engineer_trend.suite_analytics_note",
                            lineage=str(lineage_summary_line or lineage_state_display),
                            coverage=str(analytics_coverage_text or analytics_state_display),
                            default=f"{lineage_summary_line or lineage_state_display} | {analytics_coverage_text or analytics_state_display}",
                        ),
                        str(analytics_export_display or "--"),
                        str(analytics_reference_trend or "--"),
                    ]
                ),
            },
        ]
        sections = [
            {
                "id": "hot_state",
                "title": t("pages.devices.workbench.engineer_section.hot_state.title"),
                "summary": t(
                    "pages.devices.workbench.engineer_section.hot_state.summary",
                    count=4,
                    default="4 项高频状态",
                ),
                "body_text": "\n".join(
                    [
                        f"{t('pages.devices.workbench.engineer_section.field.reference')}: {reference_summary}",
                        f"{t('pages.devices.workbench.engineer_section.field.route')}: {route_summary}",
                        f"{t('pages.devices.workbench.engineer_section.field.history')}: {recent_history}",
                        f"{t('pages.devices.workbench.engineer_section.field.compare')}: {snapshot_compare.get('summary') or t('pages.devices.workbench.snapshot.no_compare')}",
                    ]
                ),
                "expanded": True,
            },
            {
                "id": "device_groups",
                "title": t("pages.devices.workbench.engineer_section.device_groups.title"),
                "summary": t(
                    "pages.devices.workbench.engineer_section.device_groups.summary",
                    count=len(device_lines),
                    default=f"{len(device_lines)} 个设备摘要",
                ),
                "body_text": "\n".join(device_lines),
                "expanded": True,
            },
            {
                "id": "injection_state",
                "title": t("pages.devices.workbench.engineer_section.injection_state.title"),
                "summary": t(
                    "pages.devices.workbench.engineer_section.injection_state.summary",
                    count=len(injection_lines),
                    default=f"{len(injection_lines)} 条注入状态",
                ),
                "body_text": "\n".join(injection_lines) or t("pages.devices.workbench.engineer_section.injection_state.empty"),
                "expanded": bool(injection_lines),
            },
            {
                "id": "trend_focus",
                "title": t("pages.devices.workbench.engineer_section.trend_focus.title"),
                "summary": t(
                    "pages.devices.workbench.engineer_section.trend_focus.summary",
                    count=len(trend_blocks),
                    default=f"{len(trend_blocks)} 个趋势块",
                ),
                "body_text": "\n".join(
                    f"{block.get('title', '--')}: {block.get('value', '--')} | {block.get('note', t('common.none'))}"
                    for block in trend_blocks
                ),
                "expanded": True,
            },
            {
                "id": "exception_focus",
                "title": t("pages.devices.workbench.engineer_section.exception_focus.title"),
                "summary": t(
                    "pages.devices.workbench.engineer_section.exception_focus.summary",
                    count=len(anomaly_lines),
                    default=f"{len(anomaly_lines)} 条重点异常",
                ),
                "body_text": "\n".join(anomaly_lines) or t("pages.devices.workbench.engineer_section.exception_focus.empty"),
                "expanded": bool(anomaly_lines),
            },
            {
                "id": "artifact_lineage",
                "title": t("pages.devices.workbench.engineer_section.artifact_lineage.title"),
                "summary": t("pages.devices.workbench.engineer_section.artifact_lineage.summary"),
                "body_text": "\n".join(
                    [
                        f"{t('pages.devices.workbench.engineer_section.field.artifact_role')}: {artifact_role_display}",
                        f"{t('pages.devices.workbench.engineer_section.field.evidence_source')}: {evidence_source_display}",
                        f"{t('pages.devices.workbench.engineer_section.field.evidence_state')}: {evidence_state_display}",
                        f"{t('pages.devices.workbench.engineer_section.field.report_path')}: {str(last_evidence_paths.get('report_json') or t('common.none'))}",
                        f"{t('pages.devices.workbench.engineer_section.field.snapshot_path')}: {str(last_evidence_paths.get('snapshot_json') or t('common.none'))}",
                        f"{t('pages.devices.workbench.engineer_section.field.lineage')}: {artifact_lineage_summary}",
                    ]
                ),
                "expanded": bool(self._last_evidence_report),
            },
            {
                "id": "qc_review",
                "title": t("shell.nav.qc"),
                "summary": t("results.review_center.detail.qc_summary", default="质控摘要"),
                "body_text": "\n".join(self._review_lines(qc_review_summary.get("lines")))
                or t(
                    "results.review_center.detail.workbench_qc_note",
                    default="当前工作台证据未生成独立质控门禁，仅提供仿真/离线诊断摘要。",
                ),
                "expanded": True,
            },
            {
                "id": "config_safety",
                "title": t("pages.devices.workbench.engineer_section.config_safety.title", default="配置安全"),
                "summary": config_safety_summary,
                "body_text": "\n".join([*config_safety_lines, *blocked_reason_lines]),
                "expanded": True,
            },
            {
                "id": "point_taxonomy",
                "title": t("pages.devices.workbench.engineer_section.point_taxonomy.title", default="点位语义 / 门禁"),
                "summary": t(
                    "pages.devices.workbench.engineer_section.point_taxonomy.summary",
                    count=len(point_taxonomy_lines),
                    default=f"{len(point_taxonomy_lines)} 条点位口径摘要",
                ),
                "body_text": "\n".join(point_taxonomy_lines)
                or t(
                    "pages.devices.workbench.engineer_section.point_taxonomy.empty",
                    default="当前 run 没有可汇总的点位语义摘要。",
                ),
                "expanded": bool(point_taxonomy_lines),
            },
            {
                "id": "measurement_core",
                "title": t(
                    "pages.devices.workbench.engineer_section.measurement_core.title",
                    default="measurement-core readiness",
                ),
                "summary": measurement_core_summary,
                "body_text": "\n".join(
                    [
                        *measurement_core_lines,
                        *measurement_core_boundaries,
                        *[
                            f"{label}: {path}"
                            for label, path in dict(measurement_core_evidence.get("artifact_paths") or {}).items()
                            if str(path or "").strip()
                        ],
                    ]
                )
                or t("common.none"),
                "expanded": bool(measurement_core_evidence.get("available", False)),
            },
            {
                "id": "recognition_readiness",
                "title": t(
                    "pages.devices.workbench.engineer_section.recognition_readiness.title",
                    default="认可就绪治理骨架",
                ),
                "summary": recognition_readiness_summary,
                "body_text": "\n".join(
                    [
                        *recognition_readiness_lines,
                        *recognition_readiness_detail_lines,
                        *recognition_readiness_boundaries,
                        *[
                            f"{label}: {path}"
                            for label, path in dict(recognition_readiness_evidence.get("artifact_paths") or {}).items()
                            if str(path or "").strip()
                        ],
                    ]
                )
                or t("common.none"),
                "expanded": bool(recognition_readiness_evidence.get("available", False)),
            },
            {
                "id": "suite_analytics",
                "title": t("pages.devices.workbench.engineer_section.suite_analytics.title"),
                "summary": t("pages.devices.workbench.engineer_section.suite_analytics.summary"),
                "body_text": "\n".join(
                    [
                        f"{t('pages.devices.workbench.engineer_card.suite_analytics')}: {suite_analytics_card_summary}",
                        f"{t('pages.devices.workbench.engineer_card.statistics')}: {suite_counts.get('passed', 0)}/{suite_counts.get('total', 0)}",
                        f"{t('results.review_center.section.analytics')}: {analytics_health_display}",
                        f"{t('results.review_center.detail.analytics_reference_quality')}: {analytics_reference_quality_display}",
                        f"{t('results.review_center.detail.analytics_reference_trend')}: {analytics_reference_trend}",
                        f"{t('results.review_center.detail.analytics_export_status')}: {analytics_export_display}",
                        f"{t('results.review_center.table.coverage')}: {analytics_coverage_text}",
                        f"{t('results.review_center.section.lineage')}: {lineage_summary_line}",
                        f"{t('pages.devices.workbench.engineer_section.field.report_path')}: {str(suite_summary_path if suite_summary_path.exists() else t('common.none'))}",
                        f"{t('results.review_center.detail.path')}: {str(analytics_summary_path if analytics_summary_path.exists() else t('common.none'))}",
                        f"{t('results.review_center.detail.analytics_lineage')}: {str(lineage_summary_path if lineage_summary_path.exists() else t('common.none'))}",
                    ]
                ),
                "expanded": bool(suite_summary_payload or analytics_summary_payload or lineage_summary_payload),
            },
            {
                "id": "history_detail",
                "title": t("pages.devices.workbench.engineer_section.history_detail.title"),
                "summary": t("pages.devices.workbench.engineer_section.history_detail.summary"),
                "body_text": str(history_payload.get("detail_text") or t("pages.devices.workbench.history.no_detail", default="暂无动作详情")),
                "expanded": True,
            },
            {
                "id": "simulation_context",
                "title": t("pages.devices.workbench.engineer_section.simulation_context.title"),
                "summary": t("pages.devices.workbench.engineer_section.simulation_context.summary"),
                "body_text": self._json_text(self._simulation_context),
                "expanded": False,
            },
            {
                "id": "raw_diagnostics",
                "title": t("pages.devices.workbench.engineer_section.raw_diagnostics.title"),
                "summary": t("pages.devices.workbench.engineer_section.raw_diagnostics.summary"),
                "body_text": self._json_text(diagnostics),
                "expanded": False,
            },
        ]
        return {
            "diagnostic_summary": t(
                "pages.devices.workbench.summary.engineer",
                context=self._simulation_context.get("scenario", "device_workbench"),
                actions=len(history_items),
                view=t(f"pages.devices.workbench.view.{self._view_mode}", default=self._view_mode),
            ),
            "cards": cards,
            "status_blocks": status_blocks,
            "trend_blocks": trend_blocks,
            "sections": sections,
            "diagnostics": diagnostics,
            "simulation_context_text": self._json_text(self._simulation_context),
            "diagnostic_text": self._json_text(diagnostics),
            "history_detail_text": str(history_payload.get("detail_text") or t("pages.devices.workbench.history.no_detail", default="暂无动作详情")),
            "history_detail_json": str(history_payload.get("detail_json") or self._json_text({})),
            "snapshot_compare_text": str(snapshot_compare.get("details_text") or t("pages.devices.workbench.snapshot.no_compare", default="暂无可对比快照")),
            "last_evidence_text": last_evidence_text,
            "last_evidence_json": self._json_text(self._last_evidence_report or {}),
            "qc_review_summary": dict(qc_review_summary),
        }

    @staticmethod
    def _json_text(payload: Any) -> str:
        return json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True)

    def _run_quick_scenario(self, scenario_id: str, **params: Any) -> str:
        normalized = str(scenario_id or "analyzer_partial_frame").strip().lower()
        if normalized == "analyzer_partial_frame":
            analyzer_index = int(params.get("analyzer_index", self._selected_analyzer_index) or self._selected_analyzer_index)
            self.execute_action("analyzer", "inject_fault", analyzer_index=analyzer_index, fault="partial_frame")
            self.execute_action("analyzer", "read_frame", analyzer_index=analyzer_index)
        elif normalized == "pace_cleanup_no_response":
            self.execute_action("pace", "inject_fault", fault="cleanup_no_response")
            self.execute_action("pace", "query_error")
        elif normalized == "relay_stuck":
            relay_name = str(params.get("relay_name") or "relay").strip().lower() or "relay"
            channel = int(params.get("channel", 1) or 1)
            self.execute_action("relay", "inject_fault", relay_name=relay_name, fault="stuck_channel", stuck_channels=[channel])
            self.execute_action("relay", "write_channel", relay_name=relay_name, channel=channel, enabled=True)
        elif normalized == "thermometer_stale":
            self.execute_action("thermometer", "set_mode", mode="stale")
        elif normalized == "pressure_wrong_unit":
            self.execute_action("pressure_gauge", "inject_fault", fault="wrong_unit_configuration")
        else:
            raise ValueError(f"unsupported quick scenario: {scenario_id}")
        return t(
            "pages.devices.workbench.message.quick_scenario_done",
            scenario=t(f"pages.devices.workbench.quick_scenario.{normalized}.label", default=normalized),
        )

    def _apply_builtin_preset(self, device_kind: str, preset_id: str, **params: Any) -> bool:
        normalized_kind = self._normalize_device_kind(device_kind)
        is_fault_injection = False
        if normalized_kind == "analyzer":
            if preset_id == "mode2_active_read":
                analyzer_index = int(params.get("analyzer_index", self._selected_analyzer_index) or self._selected_analyzer_index)
                self._run_step("analyzer", "set_mode", analyzer_index=analyzer_index, mode=2)
                self._run_step("analyzer", "set_active_state", analyzer_index=analyzer_index, active=True)
                self._run_step("analyzer", "read_frame", analyzer_index=analyzer_index)
            elif preset_id == "partial_frame":
                analyzer_index = int(params.get("analyzer_index", self._selected_analyzer_index) or self._selected_analyzer_index)
                self._run_step("analyzer", "inject_fault", analyzer_index=analyzer_index, fault="partial_frame")
                self._run_step("analyzer", "read_frame", analyzer_index=analyzer_index)
                is_fault_injection = True
            else:
                raise ValueError(f"unsupported analyzer preset: {preset_id}")
        elif normalized_kind == "pace":
            pressure_value = float(params.get("pressure_hpa", 1000.0) or 1000.0)
            if preset_id == "vent_on":
                self._run_step("pace", "set_vent", enabled=True)
            elif preset_id == "vent_off":
                self._run_step("pace", "set_vent", enabled=False)
            elif preset_id == "set_pressure":
                self._run_step("pace", "set_pressure", pressure_hpa=pressure_value)
                self._run_step("pace", "read_pressure")
            elif preset_id == "unsupported_header":
                self._run_step("pace", "inject_fault", fault="unsupported_header")
                self._run_step("pace", "query_error")
                is_fault_injection = True
            else:
                raise ValueError(f"unsupported pace preset: {preset_id}")
        elif normalized_kind == "grz":
            if preset_id == "stable":
                self._run_step("grz", "inject_fault", fault="stable")
                self._run_step("grz", "fetch_all")
            elif preset_id == "humidity_static_fault":
                self._run_step("grz", "inject_fault", fault="humidity_static_fault")
                self._run_step("grz", "fetch_all")
                is_fault_injection = True
            elif preset_id == "timeout":
                self._run_step("grz", "inject_fault", fault="timeout")
                self._run_step("grz", "fetch_all")
                is_fault_injection = True
            else:
                raise ValueError(f"unsupported grz preset: {preset_id}")
        elif normalized_kind == "chamber":
            if preset_id == "reach_target":
                self._run_step("chamber", "set_mode", mode="stable")
                self._run_step("chamber", "run")
            elif preset_id == "stalled":
                self._run_step("chamber", "set_mode", mode="stalled")
                self._run_step("chamber", "run")
                is_fault_injection = True
            elif preset_id == "alarm":
                self._run_step("chamber", "set_mode", mode="alarm")
                is_fault_injection = True
            else:
                raise ValueError(f"unsupported chamber preset: {preset_id}")
        elif normalized_kind == "relay":
            relay_name = str(params.get("relay_name") or "relay").strip().lower() or "relay"
            channel = max(1, int(params.get("channel", 1) or 1))
            if preset_id == "all_off":
                self._run_step("relay", "all_off", relay_name="relay")
                self._run_step("relay", "all_off", relay_name="relay_8")
            elif preset_id == "route_h2o":
                route_relay, route_channel = self._preset_route_target("h2o", fallback_relay=relay_name, fallback_channel=1)
                self._run_step("relay", "all_off", relay_name="relay")
                self._run_step("relay", "all_off", relay_name="relay_8")
                self._run_step("relay", "write_channel", relay_name=route_relay, channel=route_channel, enabled=True)
            elif preset_id == "route_co2":
                route_relay, route_channel = self._preset_route_target("co2", fallback_relay=relay_name, fallback_channel=2)
                self._run_step("relay", "all_off", relay_name="relay")
                self._run_step("relay", "all_off", relay_name="relay_8")
                self._run_step("relay", "write_channel", relay_name=route_relay, channel=route_channel, enabled=True)
            elif preset_id == "stuck_channel":
                self._run_step("relay", "inject_fault", relay_name=relay_name, fault="stuck_channel", stuck_channels=[channel])
                self._run_step("relay", "write_channel", relay_name=relay_name, channel=channel, enabled=True)
                is_fault_injection = True
            else:
                raise ValueError(f"unsupported relay preset: {preset_id}")
        elif normalized_kind == "thermometer":
            if preset_id in {"stable", "stale", "drift"}:
                self._run_step("thermometer", "set_mode", mode=preset_id)
                is_fault_injection = preset_id != "stable"
            else:
                raise ValueError(f"unsupported thermometer preset: {preset_id}")
        elif normalized_kind in {"pressure_gauge", "pressure"}:
            if preset_id == "stable":
                self._run_step("pressure_gauge", "inject_fault", fault="stable")
                self._run_step("pressure_gauge", "set_unit", unit="HPA")
            elif preset_id == "wrong_unit":
                self._run_step("pressure_gauge", "inject_fault", fault="wrong_unit_configuration")
                is_fault_injection = True
            elif preset_id == "no_response":
                self._run_step("pressure_gauge", "inject_fault", fault="no_response")
                is_fault_injection = True
            else:
                raise ValueError(f"unsupported pressure preset: {preset_id}")
        else:
            raise ValueError(f"unsupported preset device kind: {device_kind}")
        return is_fault_injection

    def _execute_device_preset(self, device_kind: str, **params: Any) -> tuple[str, dict[str, Any]]:
        normalized_kind = str(device_kind or "").strip().lower()
        preset_id = str(params.get("preset_id") or "").strip().lower()
        runtime_params = {
            key: value
            for key, value in dict(params or {}).items()
            if str(key) not in {"preset_id", "device_kind", "current_device"}
        }
        custom_preset = self._find_custom_preset(preset_id)
        extras: dict[str, Any] = {}
        if custom_preset:
            actual_kind = self._normalize_device_kind(custom_preset.get("device_kind"))
            selected_preset_payload = dict(custom_preset)
            merged_params = {
                **dict(custom_preset.get("parameters", {}) or {}),
                **runtime_params,
            }
            is_fault_injection = False
            executed_steps = []
            for step in list(custom_preset.get("steps") or []):
                step_device = self._normalize_device_kind(step.get("device_kind"))
                step_preset = str(step.get("preset_id") or "").strip().lower()
                if not step_device or not step_preset:
                    continue
                step_fault = self._apply_builtin_preset(step_device, step_preset, **merged_params)
                is_fault_injection = is_fault_injection or step_fault
                executed_steps.append(
                    {
                        "device_kind": step_device,
                        "preset_id": step_preset,
                        "label": self._preset_label(step_device, step_preset),
                    }
                )
            self._record_preset_use(actual_kind, preset_id)
            extras = {
                "is_fault_injection": is_fault_injection,
                "preset_source": "custom",
                "custom_preset_id": preset_id,
                "executed_steps": executed_steps,
                "preset_fake_capabilities": list(selected_preset_payload.get("fake_capabilities") or []),
                "preset_fake_capability_summary": str(selected_preset_payload.get("fake_capability_summary") or t("common.none")),
                "preset_metadata_summary": str(selected_preset_payload.get("metadata_summary") or t("common.none")),
            }
        else:
            is_fault_injection = self._apply_builtin_preset(normalized_kind, preset_id, **runtime_params)
            actual_kind = "pressure_gauge" if normalized_kind in {"pressure", "pressure_gauge"} else normalized_kind
            selected_preset_payload = self._builtin_preset_payload(actual_kind, preset_id)
            self._record_preset_use(actual_kind, preset_id)
            extras = {
                "is_fault_injection": is_fault_injection,
                "preset_source": "built_in",
                "preset_fake_capabilities": list(selected_preset_payload.get("fake_capabilities") or []),
                "preset_fake_capability_summary": str(selected_preset_payload.get("fake_capability_summary") or t("common.none")),
            }
        return (
            t(
                "pages.devices.workbench.message.preset_done",
                device=self._device_display(actual_kind),
                preset=self._preset_label(actual_kind, preset_id),
                default=f"{self._device_display(actual_kind)}预置已执行",
            ),
            extras,
        )

    def _run_step(self, device_kind: str, action: str, **params: Any) -> None:
        result = self.execute_action(device_kind, action, **params)
        if not bool(result.get("ok", False)):
            raise RuntimeError(str(result.get("message") or f"{device_kind}:{action} failed"))

    def _preset_route_target(
        self,
        route_name: str,
        *,
        fallback_relay: str,
        fallback_channel: int,
    ) -> tuple[str, int]:
        route_token = str(route_name or "").strip().lower()
        for relay_name in self.RELAY_NAMES:
            mapping = self._valve_mapping_for_relay(relay_name)
            for channel, label in mapping.items():
                text = str(label or "").strip().lower()
                if route_token and route_token in text:
                    return relay_name, int(channel)
        return fallback_relay, int(fallback_channel)

    def _preset_label(self, device_kind: str, preset_id: str) -> str:
        normalized_kind = self._normalize_device_kind(device_kind)
        for item in self._build_presets(normalized_kind):
            if str(item.get("id") or "") == str(preset_id):
                return str(item.get("label") or preset_id)
        return t(f"pages.devices.workbench.preset.{normalized_kind}.{preset_id}.label", default=preset_id)

    def _generate_diagnostic_evidence(
        self,
        *,
        current_device: str,
        current_action: str,
    ) -> dict[str, Any]:
        snapshot = self.build_snapshot()
        history_payload = dict(snapshot.get("history", {}) or {})
        workbench_payload = dict(snapshot.get("workbench", {}) or {})
        meta = dict(snapshot.get("meta", {}) or {})
        operator_summary = dict(snapshot.get("operator_summary", {}) or {})
        snapshot_compare = dict(workbench_payload.get("snapshot_compare", {}) or {})
        now_text = datetime.now().isoformat(timespec="seconds")
        run_dir = Path(self.facade.result_store.run_dir)
        run_dir.mkdir(parents=True, exist_ok=True)

        normalized_device = str(current_device or self._current_device_from_history()).strip().lower()
        normalized_action = str(current_action or self._current_action_from_history()).strip().lower()
        active_faults = bool(meta.get("has_fault_injection", False))
        risk_level = "medium" if active_faults else "low"
        workbench_boundary = self._workbench_evidence_boundary()
        qc_review_summary = dict(snapshot.get("evidence", {}).get("qc_review_summary", {}) or {})
        evidence_config_safety = dict(snapshot.get("evidence", {}).get("config_safety", {}) or {})
        evidence_config_safety_review = dict(snapshot.get("evidence", {}).get("config_safety_review", {}) or {})
        point_taxonomy_summary = dict(snapshot.get("evidence", {}).get("point_taxonomy_summary", {}) or {})
        measurement_core_evidence = dict(snapshot.get("evidence", {}).get("measurement_core_evidence", {}) or {})
        recognition_readiness_evidence = dict(
            snapshot.get("evidence", {}).get("recognition_readiness_evidence", {}) or {}
        )
        config_governance_payload = self._config_governance_payload(
            evidence_config_safety,
            evidence_config_safety_review,
        )
        report_payload = {
            "artifact_type": "workbench_action_report",
            "generated_at": now_text,
            "run_id": str(getattr(self.facade.session, "run_id", "") or ""),
            **workbench_boundary,
            **dict(config_governance_payload),
            "artifact_role": "diagnostic_analysis",
            "artifact_role_display": display_artifact_role("diagnostic_analysis"),
            "diagnostic_only": True,
            "publish_primary_latest_allowed": False,
            "current_view_mode": self._view_mode,
            "current_view_mode_display": t(f"pages.devices.workbench.view.{self._view_mode}", default=self._view_mode),
            "current_device": normalized_device or "workbench",
            "current_device_display": self._device_display(normalized_device or "workbench"),
            "device_category": normalized_device or "workbench",
            "device_category_display": self._device_display(normalized_device or "workbench"),
            "current_action": normalized_action or "generate_diagnostic_evidence",
            "current_action_display": self._action_display(
                "workbench" if (not normalized_action or normalized_action == "generate_diagnostic_evidence") else (normalized_device or "workbench"),
                normalized_action or "generate_diagnostic_evidence",
            ),
            "risk_level": risk_level,
            "risk_level_display": display_risk_level(risk_level),
            "has_fault_injection": active_faults,
            "has_fault_injection_display": display_bool(active_faults),
            "reference_quality_summary": str(operator_summary.get("reference_summary") or t("common.none")),
            "route_relay_summary": str(operator_summary.get("route_summary") or t("common.none")),
            "simulation_context": dict(snapshot.get("evidence", {}).get("simulation_context", {}) or {}),
            "reference_quality": dict(snapshot.get("evidence", {}).get("reference_quality", {}) or {}),
            "route_physical_validation": dict(snapshot.get("evidence", {}).get("route_physical_validation", {}) or {}),
            "qc_review_summary": qc_review_summary,
            "qc_reviewer_card": dict(qc_review_summary.get("reviewer_card") or {}),
            "qc_evidence_section": dict(qc_review_summary.get("evidence_section") or {}),
            "qc_review_cards": [dict(item) for item in list(qc_review_summary.get("cards") or []) if isinstance(item, dict)],
            "config_safety": evidence_config_safety,
            "config_safety_review": evidence_config_safety_review,
            "point_taxonomy_summary": point_taxonomy_summary,
            "measurement_core_evidence": measurement_core_evidence,
            "recognition_readiness_evidence": recognition_readiness_evidence,
            "operator_summary": operator_summary,
            "history": list(history_payload.get("items", []) or []),
            "history_filters": dict(history_payload.get("filters", {}) or {}),
            "history_detail": dict(history_payload.get("detail", {}) or {}),
            "snapshot_compare": snapshot_compare,
            "devices": {
                "analyzer": {
                    "panel_status": dict(snapshot.get("analyzer", {}).get("panel_status", {}) or {}),
                    "injection_state": dict(snapshot.get("analyzer", {}).get("injection_state", {}) or {}),
                },
                "pace": {
                    "panel_status": dict(snapshot.get("pace", {}).get("panel_status", {}) or {}),
                    "injection_state": dict(snapshot.get("pace", {}).get("injection_state", {}) or {}),
                },
                "grz": {
                    "panel_status": dict(snapshot.get("grz", {}).get("panel_status", {}) or {}),
                    "injection_state": dict(snapshot.get("grz", {}).get("injection_state", {}) or {}),
                },
                "chamber": {
                    "panel_status": dict(snapshot.get("chamber", {}).get("panel_status", {}) or {}),
                    "injection_state": dict(snapshot.get("chamber", {}).get("injection_state", {}) or {}),
                },
                "relay": {
                    "panel_status": dict(snapshot.get("relay", {}).get("panel_status", {}) or {}),
                    "injection_state": dict(snapshot.get("relay", {}).get("injection_state", {}) or {}),
                },
                "thermometer": {
                    "panel_status": dict(snapshot.get("thermometer", {}).get("panel_status", {}) or {}),
                    "injection_state": dict(snapshot.get("thermometer", {}).get("injection_state", {}) or {}),
                },
                "pressure_gauge": {
                    "panel_status": dict(snapshot.get("pressure_gauge", {}).get("panel_status", {}) or {}),
                    "injection_state": dict(snapshot.get("pressure_gauge", {}).get("injection_state", {}) or {}),
                },
            },
        }
        summary_line = t(
            "pages.devices.workbench.summary.last_evidence",
            generated_at=now_text,
            role=display_artifact_role("diagnostic_analysis"),
            state=display_evidence_state("simulated_workbench"),
        )
        report_json_path = run_dir / self.WORKBENCH_EXPORTS["workbench_action_report_json"]
        report_md_path = run_dir / self.WORKBENCH_EXPORTS["workbench_action_report_markdown"]
        snapshot_json_path = run_dir / self.WORKBENCH_EXPORTS["workbench_action_snapshot"]
        summary = {
            "generated_at": now_text,
            "artifact_role": "diagnostic_analysis",
            "artifact_role_display": display_artifact_role("diagnostic_analysis"),
            **workbench_boundary,
            **dict(config_governance_payload),
            "risk_level": risk_level,
            "risk_level_display": display_risk_level(risk_level),
            "publish_primary_latest_allowed": False,
            "qc_review_summary": qc_review_summary,
            "qc_reviewer_card": dict(qc_review_summary.get("reviewer_card") or {}),
            "qc_evidence_section": dict(qc_review_summary.get("evidence_section") or {}),
            "qc_review_cards": [dict(item) for item in list(qc_review_summary.get("cards") or []) if isinstance(item, dict)],
            "config_safety": evidence_config_safety,
            "config_safety_review": evidence_config_safety_review,
            "point_taxonomy_summary": point_taxonomy_summary,
            "measurement_core_evidence": measurement_core_evidence,
            "recognition_readiness_evidence": recognition_readiness_evidence,
            "paths": {
                "report_json": str(report_json_path),
                "report_markdown": str(report_md_path),
                "snapshot_json": str(snapshot_json_path),
            },
            "summary_line": summary_line,
            "message": t("pages.devices.workbench.message.evidence_generated"),
        }
        self._last_evidence_report = dict(summary)
        reports = self._simulation_context.setdefault("workbench_reports", [])
        if not isinstance(reports, list):
            reports = []
            self._simulation_context["workbench_reports"] = reports
        reports.append(copy.deepcopy(summary))
        report_payload["summary_line"] = summary_line
        report_payload["simulation_context"] = copy.deepcopy(self._simulation_context)
        snapshot_payload = {
            "artifact_type": "workbench_action_snapshot",
            "generated_at": now_text,
            **workbench_boundary,
            **dict(config_governance_payload),
            "qc_review_summary": qc_review_summary,
            "qc_reviewer_card": dict(qc_review_summary.get("reviewer_card") or {}),
            "qc_evidence_section": dict(qc_review_summary.get("evidence_section") or {}),
            "qc_review_cards": [dict(item) for item in list(qc_review_summary.get("cards") or []) if isinstance(item, dict)],
            "config_safety": evidence_config_safety,
            "config_safety_review": evidence_config_safety_review,
            "point_taxonomy_summary": point_taxonomy_summary,
            "measurement_core_evidence": measurement_core_evidence,
            "recognition_readiness_evidence": recognition_readiness_evidence,
            "snapshot_compare": snapshot_compare,
            "snapshot": self.build_snapshot(),
        }
        markdown_text = self._build_workbench_markdown_report(report_payload)
        report_json_path.write_text(self._json_text(report_payload) + "\n", encoding="utf-8")
        report_md_path.write_text(markdown_text, encoding="utf-8")
        snapshot_json_path.write_text(self._json_text(snapshot_payload) + "\n", encoding="utf-8")
        self._update_workbench_artifact_indexes(summary)
        return summary

    def _build_workbench_markdown_report(self, report_payload: dict[str, Any]) -> str:
        history_lines = "\n".join(
            f"- {item.get('timestamp', '--')} | #{item.get('sequence', '--')} | {item.get('device_display', '--')} | {item.get('action_display', '--')} | {item.get('message', '--')}"
            for item in list(report_payload.get("history", []) or [])[:10]
        ) or f"- {t('common.none')}"
        compare_text = str(dict(report_payload.get("snapshot_compare", {}) or {}).get("details_text") or t("pages.devices.workbench.snapshot.no_compare", default="暂无可对比快照"))
        qc_lines = "\n".join(
            f"- {line}"
            for line in self._review_lines(
                dict(report_payload.get("qc_review_summary", {}) or {}).get("review_card_lines")
                or dict(report_payload.get("qc_review_summary", {}) or {}).get("lines")
            )
        ) or f"- {t('common.none')}"
        config_safety_review = dict(report_payload.get("config_safety_review", {}) or {})
        config_safety_lines = "\n".join(
            f"- {line}"
            for line in [
                f"配置安全: {str(config_safety_review.get('summary') or '--')}",
                f"库存分类: {str(config_safety_review.get('classification_display') or config_safety_review.get('classification') or '--')}",
                f"库存摘要: {str(config_safety_review.get('inventory_summary') or '--')}",
                *[
                    f"{str(item.get('title') or '--')}: {str(item.get('summary') or '--')}"
                    for item in list(config_safety_review.get("blocked_reason_details") or [])[:2]
                ],
            ]
            if str(line or "").strip()
        ) or f"- {t('common.none')}"
        point_taxonomy_lines = "\n".join(
            f"- {line}"
            for line in self._point_taxonomy_lines(dict(report_payload.get("point_taxonomy_summary", {}) or {}))
        ) or f"- {t('common.none')}"
        measurement_core_payload = dict(report_payload.get("measurement_core_evidence", {}) or {})
        measurement_core_summary_lines = [
            str(item)
            for item in list(measurement_core_payload.get("summary_lines") or [])
            if str(item).strip()
        ]
        measurement_core_boundary_lines = [
            str(item)
            for item in list(measurement_core_payload.get("boundary_lines") or [])
            if str(item).strip()
        ]
        measurement_core_artifact_paths = dict(measurement_core_payload.get("artifact_paths") or {})
        measurement_core_lines = "\n".join(
            f"- {line}"
            for line in [
                *measurement_core_summary_lines,
                *measurement_core_boundary_lines,
                *[
                    f"{label}: {path}"
                    for label, path in (
                        ("multi_source_stability_evidence", measurement_core_artifact_paths.get("multi_source_stability_evidence")),
                        ("state_transition_evidence", measurement_core_artifact_paths.get("state_transition_evidence")),
                        (
                            "simulation_evidence_sidecar_bundle",
                            measurement_core_artifact_paths.get("simulation_evidence_sidecar_bundle"),
                        ),
                        (
                            "measurement_phase_coverage_report",
                            measurement_core_artifact_paths.get("measurement_phase_coverage_report"),
                        ),
                    )
                    if str(path or "").strip()
                ],
            ]
            if str(line).strip()
        ) or f"- {t('common.none')}"
        recognition_readiness_payload = dict(report_payload.get("recognition_readiness_evidence", {}) or {})
        recognition_readiness_summary_lines = [
            str(item)
            for item in list(recognition_readiness_payload.get("summary_lines") or [])
            if str(item).strip()
        ]
        recognition_readiness_detail_lines = [
            str(item)
            for item in list(recognition_readiness_payload.get("detail_lines") or [])
            if str(item).strip()
        ]
        recognition_readiness_boundary_lines = [
            str(item)
            for item in list(recognition_readiness_payload.get("boundary_lines") or [])
            if str(item).strip()
        ]
        recognition_readiness_artifact_paths = dict(recognition_readiness_payload.get("artifact_paths") or {})
        recognition_readiness_lines = "\n".join(
            f"- {line}"
            for line in [
                *recognition_readiness_summary_lines,
                *recognition_readiness_detail_lines,
                *recognition_readiness_boundary_lines,
                *[
                    f"{label}: {path}"
                    for label, path in recognition_readiness_artifact_paths.items()
                    if str(path or "").strip()
                ],
            ]
            if str(line).strip()
        ) or f"- {t('common.none')}"
        return "\n".join(
            [
                f"# {t('pages.devices.workbench.report.title')}",
                "",
                f"- {t('pages.devices.workbench.report.generated_at')}: {report_payload.get('generated_at', '--')}",
                f"- {t('pages.devices.workbench.report.evidence_source')}: {report_payload.get('evidence_source_display', '--')}",
                f"- {t('pages.devices.workbench.report.evidence_state')}: {report_payload.get('evidence_state_display', '--')}",
                f"- {t('pages.devices.workbench.report.acceptance_guard')}: {display_bool(bool(report_payload.get('not_real_acceptance_evidence', False)))}",
                f"- {t('pages.devices.workbench.report.current_view')}: {report_payload.get('current_view_mode_display', '--')}",
                f"- {t('pages.devices.workbench.report.current_device')}: {report_payload.get('current_device_display', '--')}",
                f"- {t('pages.devices.workbench.report.current_action')}: {report_payload.get('current_action_display', '--')}",
                f"- {t('pages.devices.workbench.report.risk_level', default='风险等级')}: {report_payload.get('risk_level_display', '--')}",
                f"- {t('pages.devices.workbench.report.device_category', default='设备类别')}: {report_payload.get('device_category_display', '--')}",
                f"- {t('pages.devices.workbench.report.fault_injection', default='故障注入')}: {report_payload.get('has_fault_injection_display', '--')}",
                "",
                f"## {t('pages.devices.workbench.report.operator_summary')}",
                "",
                f"- {t('pages.devices.workbench.summary.health_label')}: {dict(report_payload.get('operator_summary', {}) or {}).get('health_summary', '--')}",
                f"- {t('pages.devices.workbench.summary.faults_label')}: {dict(report_payload.get('operator_summary', {}) or {}).get('fault_summary', '--')}",
                f"- {t('pages.devices.workbench.summary.reference_label')}: {dict(report_payload.get('operator_summary', {}) or {}).get('reference_summary', '--')}",
                f"- {t('pages.devices.workbench.summary.route_label')}: {dict(report_payload.get('operator_summary', {}) or {}).get('route_summary', '--')}",
                f"- {t('pages.devices.workbench.report.reference_quality_summary', default='参考质量摘要')}: {report_payload.get('reference_quality_summary', '--')}",
                f"- {t('pages.devices.workbench.report.route_relay_summary', default='路由/继电器摘要')}: {report_payload.get('route_relay_summary', '--')}",
                "",
                f"## {t('results.review_center.detail.qc_summary', default='质控摘要')}",
                "",
                qc_lines,
                "",
                f"## {t('pages.devices.workbench.report.config_safety', default='配置安全治理')}",
                "",
                config_safety_lines,
                "",
                f"## {t('pages.devices.workbench.report.point_taxonomy', default='点位语义 / 门禁摘要')}",
                "",
                point_taxonomy_lines,
                "",
                f"## {t('pages.devices.workbench.report.measurement_core', default='measurement-core readiness')}",
                "",
                measurement_core_lines,
                "",
                f"## {t('pages.devices.workbench.report.recognition_readiness', default='认可就绪治理骨架')}",
                "",
                recognition_readiness_lines,
                "",
                f"## {t('pages.devices.workbench.report.action_history')}",
                "",
                history_lines,
                "",
                f"## {t('pages.devices.workbench.report.snapshot_compare', default='快照对比')}",
                "",
                compare_text,
                "",
                f"## {t('pages.devices.workbench.report.reference_quality')}",
                "",
                "```json",
                self._json_text(report_payload.get("reference_quality", {})),
                "```",
                "",
                f"## {t('pages.devices.workbench.report.route_validation')}",
                "",
                "```json",
                self._json_text(report_payload.get("route_physical_validation", {})),
                "```",
                "",
                f"## {t('pages.devices.workbench.report.simulation_context')}",
                "",
                "```json",
                self._json_text(report_payload.get("simulation_context", {})),
                "```",
            ]
        ) + "\n"

    def _current_device_from_history(self) -> str:
        if not self._action_log:
            return "workbench"
        return str(self._action_log[-1].get("device") or "workbench")

    def _current_action_from_history(self) -> str:
        if not self._action_log:
            return "generate_diagnostic_evidence"
        return str(self._action_log[-1].get("action") or "generate_diagnostic_evidence")

    def _update_workbench_artifact_indexes(self, summary: dict[str, Any]) -> None:
        run_state = getattr(getattr(self.facade.service, "orchestrator", None), "run_state", None)
        artifact_state = getattr(run_state, "artifacts", None)
        export_statuses = {}
        output_files: list[str] = []
        if artifact_state is not None:
            export_statuses = getattr(artifact_state, "export_statuses", None)
            if not isinstance(export_statuses, dict):
                export_statuses = {}
                artifact_state.export_statuses = export_statuses
            output_files = getattr(artifact_state, "output_files", None)
            if not isinstance(output_files, list):
                output_files = []
                artifact_state.output_files = output_files
        mapping = {
            "workbench_action_report_json": summary["paths"]["report_json"],
            "workbench_action_report_markdown": summary["paths"]["report_markdown"],
            "workbench_action_snapshot": summary["paths"]["snapshot_json"],
        }
        for export_name, path_text in mapping.items():
            export_statuses[export_name] = {
                "role": "diagnostic_analysis",
                "status": "ok",
                "path": str(path_text),
                "error": "",
                "evidence_source": self.WORKBENCH_EVIDENCE_SOURCE,
                "evidence_state": self.WORKBENCH_EVIDENCE_STATE,
                "diagnostic_only": True,
                "acceptance_evidence": False,
                "not_real_acceptance_evidence": True,
            }
            if str(path_text) not in output_files:
                output_files.append(str(path_text))

        summary_path = Path(self.facade.result_store.run_dir) / "summary.json"
        summary_payload = self._load_json_dict(summary_path)
        stats = dict(summary_payload.get("stats", {}) or {})
        artifact_exports = dict(stats.get("artifact_exports", {}) or {})
        artifact_exports.update(copy.deepcopy(export_statuses))
        stats["artifact_exports"] = artifact_exports
        stats["artifact_role_summary"] = self.facade.result_store._artifact_role_summary(artifact_exports)
        stats["workbench_evidence_summary"] = dict(summary)
        if output_files:
            stats["output_files"] = list(output_files)
        summary_payload["stats"] = stats
        summary_payload["config_safety"] = dict(summary.get("config_safety", {}) or {})
        summary_payload["config_safety_review"] = dict(summary.get("config_safety_review", {}) or {})
        summary_payload["config_governance_handoff"] = dict(summary.get("config_governance_handoff", {}) or {})
        summary_payload["point_taxonomy_summary"] = dict(summary.get("point_taxonomy_summary", {}) or {})
        summary_payload["artifact_role_summary"] = dict(stats["artifact_role_summary"])
        summary_payload["workbench_evidence_summary"] = dict(summary)
        self._write_json_dict(summary_path, summary_payload)

        manifest_path = Path(self.facade.result_store.run_dir) / "manifest.json"
        manifest_payload = self._load_json_dict(manifest_path)
        artifacts = dict(manifest_payload.get("artifacts", {}) or {})
        manifest_output_files = list(artifacts.get("output_files", []) or [])
        for path_text in output_files:
            if str(path_text) not in manifest_output_files:
                manifest_output_files.append(str(path_text))
        role_catalog = dict(artifacts.get("role_catalog", {}) or {})
        diagnostic_roles = list(role_catalog.get("diagnostic_analysis", []) or [])
        for export_name in mapping:
            if export_name not in diagnostic_roles:
                diagnostic_roles.append(export_name)
        role_catalog["diagnostic_analysis"] = diagnostic_roles
        artifacts["output_files"] = manifest_output_files
        artifacts["role_catalog"] = role_catalog
        manifest_payload["artifacts"] = artifacts
        manifest_payload["workbench_evidence"] = dict(summary)
        self._write_json_dict(manifest_path, manifest_payload)

    @staticmethod
    def _load_json_dict(path: Path) -> dict[str, Any]:
        if not path.exists():
            return {}
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return {}
        return dict(payload) if isinstance(payload, dict) else {}

    @staticmethod
    def _write_json_dict(path: Path, payload: dict[str, Any]) -> None:
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    def _device_config(self, device_name: str, *, analyzer_index: Optional[int] = None) -> dict[str, Any]:
        devices = getattr(self.facade.config, "devices", None)
        if device_name == "pressure_controller":
            config = getattr(devices, "pressure_controller", None)
            base = {"name": "pressure_controller", "port": "SIM-PACE5000", "enabled": True, "timeout": 1.0}
        elif device_name == "pressure_gauge":
            config = getattr(devices, "pressure_meter", None)
            base = {"name": "pressure_meter", "port": "SIM-PARO", "enabled": True, "timeout": 1.0}
        elif device_name == "humidity_generator":
            config = getattr(devices, "humidity_generator", None)
            base = {"name": "humidity_generator", "port": "SIM-GRZ5013", "enabled": True, "timeout": 1.0}
        elif device_name == "temperature_chamber":
            config = getattr(devices, "temperature_chamber", None)
            base = {"name": "temperature_chamber", "port": "SIM-TEMP-CHAMBER", "enabled": True, "timeout": 1.0}
        elif device_name == "relay":
            config = getattr(devices, "relay_a", None)
            base = {"name": "relay", "port": "SIM-RELAY-16", "enabled": True, "timeout": 1.0, "channel_count": 16}
        elif device_name == "relay_8":
            config = getattr(devices, "relay_b", None)
            base = {"name": "relay_8", "port": "SIM-RELAY-8", "enabled": True, "timeout": 1.0, "channel_count": 8}
        elif device_name == "thermometer":
            config = getattr(devices, "thermometer", None)
            base = {"name": "thermometer", "port": "SIM-THERMOMETER", "enabled": True, "timeout": 1.0}
        elif device_name.startswith("gas_analyzer_"):
            gas_analyzers = list(getattr(devices, "gas_analyzers", []) or [])
            index = 0 if analyzer_index is None else max(0, int(analyzer_index))
            config = gas_analyzers[index] if index < len(gas_analyzers) else None
            base = {
                "name": device_name,
                "port": f"SIM-YGAS-{index + 1}",
                "enabled": True,
                "timeout": 1.0,
                "device_id": f"{index + 1:03d}",
                "mode": 2,
                "active_send": True,
                "ftd_hz": 5,
            }
        else:
            config = None
            base = {"name": device_name, "port": f"SIM-{device_name.upper()}", "enabled": True, "timeout": 1.0}
        if config is None:
            return base
        payload = _copy_payload(config)
        if isinstance(payload, dict):
            base.update(payload)
        return base

    def _resolve_device(self, name: str) -> Any:
        if name in self._device_cache:
            return self._device_cache[name]
        if not self._service_device_manager_is_simulated():
            return None
        device_manager = getattr(self.facade.service, "device_manager", None)
        getter = getattr(device_manager, "get_device", None)
        if callable(getter):
            try:
                device = getter(name)
                if device is not None:
                    self._device_cache[name] = device
                    return device
            except Exception:
                pass
        private_devices = getattr(device_manager, "_devices", None)
        if isinstance(private_devices, dict) and private_devices.get(name) is not None:
            self._device_cache[name] = private_devices[name]
            return private_devices[name]
        return None

    def _ensure_device(self, name: str, device_type: DeviceType, *, analyzer_index: Optional[int] = None) -> Any:
        existing = self._resolve_device(name)
        if existing is not None:
            return existing
        config = self._device_config(name, analyzer_index=analyzer_index)
        device_manager = getattr(self.facade.service, "device_manager", None)
        creator = getattr(device_manager, "create_device", None)
        service_simulated = self._service_device_manager_is_simulated()
        device = None
        if callable(creator) and service_simulated:
            try:
                device = creator(name, device_type, config)
            except Exception:
                device = None
        if device is None:
            device = self._factory.create(device_type, config)
            register = getattr(device_manager, "register_device", None)
            if callable(register) and service_simulated:
                try:
                    register(name, device, device_type=device_type.value)
                except Exception:
                    pass
        if hasattr(device, "connect"):
            try:
                device.connect()
            except Exception:
                pass
        self._device_cache[name] = device
        return device

    def _analyzer_device(self, index: int) -> Any:
        normalized = max(0, min(self.ANALYZER_COUNT - 1, int(index)))
        return self._ensure_device(f"gas_analyzer_{normalized}", DeviceType.GAS_ANALYZER, analyzer_index=normalized)

    def _singleton_device(self, key: str) -> Any:
        device_type_map = {
            "pressure_controller": DeviceType.PRESSURE_CONTROLLER,
            "pressure_gauge": DeviceType.PRESSURE_METER,
            "humidity_generator": DeviceType.HUMIDITY_GENERATOR,
            "temperature_chamber": DeviceType.TEMPERATURE_CHAMBER,
            "relay": DeviceType.RELAY,
            "relay_8": DeviceType.RELAY,
            "thermometer": DeviceType.THERMOMETER,
        }
        return self._ensure_device(str(key), device_type_map[str(key)])

    def _add_recent_frame(self, analyzer_name: str, frame: str) -> None:
        text = str(frame or "").strip()
        if text:
            self._recent_frames.setdefault(analyzer_name, deque(maxlen=5)).append(text)

    def _add_recent_ascii(self, key: str, payload: str) -> None:
        text = str(payload or "").strip()
        if not text:
            return
        stream = self._recent_ascii_streams.setdefault(key, deque(maxlen=6))
        for line in [item.strip() for item in text.splitlines() if item.strip()]:
            stream.append(line)

    def _actions_for(self, device_kind: str, *action_ids: str) -> list[dict[str, str]]:
        return [
            {
                "id": action_id,
                "label": t(f"pages.devices.workbench.action.{device_kind}.{action_id}", default=action_id),
            }
            for action_id in action_ids
        ]

    def _build_analyzer_snapshot(self) -> dict[str, Any]:
        device = self._analyzer_device(self._selected_analyzer_index)
        device_name = f"gas_analyzer_{self._selected_analyzer_index}"
        recent = self._recent_frames.setdefault(device_name, deque(maxlen=5))
        if not recent:
            try:
                self._add_recent_frame(device_name, device.read_latest_data(allow_passive_fallback=True))
            except Exception:
                pass
        status = dict(getattr(device, "status", lambda: {})() or {})
        mode = int(status.get("mode_effective", status.get("mode", 2)) or 2)
        panel_status = {
            "selected_analyzer": self._selected_analyzer_index + 1,
            "selectors": list(range(1, self.ANALYZER_COUNT + 1)),
            "device_id": str(status.get("device_id") or f"{self._selected_analyzer_index + 1:03d}"),
            "mode": mode,
            "mode_display": t(f"pages.devices.workbench.enum.analyzer_mode.mode_{mode}"),
            "active_send": bool(status.get("active_send", True)),
            "active_send_display": t(
                "pages.devices.workbench.enum.active_state.active"
                if bool(status.get("active_send", True))
                else "pages.devices.workbench.enum.active_state.passive"
            ),
            "frequency_hz": int(status.get("ftd_hz", 5) or 5),
            "recent_frames": list(recent),
            "last_frame": recent[-1] if recent else "",
            "status_bits": str(status.get("status") or "0000"),
        }
        injection_state = {
            "mode2_stream": str(getattr(device, "_mode2_stream", "stable") or "stable"),
            "mode2_stream_display": t(
                f"pages.devices.workbench.enum.analyzer_fault.{str(getattr(device, '_mode2_stream', 'stable') or 'stable')}",
                default=str(getattr(device, "_mode2_stream", "stable") or "stable"),
            ),
            "sensor_precheck": str(getattr(device, "_sensor_precheck", "strict_pass") or "strict_pass"),
        }
        return {
            "title": t("pages.devices.workbench.device.analyzer"),
            "simulated": True,
            "actions": self._actions_for(
                "analyzer",
                "select",
                "set_mode",
                "set_active_state",
                "set_frequency",
                "read_frame",
                "broadcast",
                "inject_fault",
            ),
            "presets": self._build_presets("analyzer"),
            "device_status": status,
            "panel_status": panel_status,
            "injection_state": injection_state,
        }

    def _build_pace_snapshot(self) -> dict[str, Any]:
        device = self._singleton_device("pressure_controller")
        status = dict(getattr(device, "status", lambda: {})() or {})
        errors = list(getattr(device, "_error_queue", []) or [])
        return {
            "title": t("pages.devices.workbench.device.pace"),
            "simulated": True,
            "actions": self._actions_for(
                "pace",
                "set_vent",
                "set_output",
                "set_isolation",
                "set_pressure",
                "set_unit",
                "read_pressure",
                "query_error",
                "inject_fault",
            ),
            "presets": self._build_presets("pace"),
            "device_status": status,
            "panel_status": {
                "pressure_hpa": status.get("pressure_hpa"),
                "pressure_display": format_pressure_hpa(status.get("pressure_hpa")),
                "target_pressure_display": format_pressure_hpa(status.get("target_pressure_hpa")),
                "vent_on": int(status.get("vent_status", 0) or 0) != 0,
                "output_on": bool(status.get("output_state", 0)),
                "isolation_on": bool(status.get("isolation_state", 0)),
                "unit": str(status.get("unit") or "HPA"),
                "slew_hpa_per_s": float(getattr(device, "slew_hpa_per_s", 0.0) or 0.0),
                "error_queue": errors,
            },
            "injection_state": {
                "mode": str(getattr(device, "mode", "stable") or "stable"),
                "mode_display": t(
                    f"pages.devices.workbench.enum.pace_fault.{str(getattr(device, 'mode', 'stable') or 'stable')}",
                    default=str(getattr(device, "mode", "stable") or "stable"),
                ),
                "wrong_unit_configuration": bool(self._device_matrix().get("pressure_controller", {}).get("wrong_unit_configuration", False)),
            },
        }

    def _build_grz_snapshot(self) -> dict[str, Any]:
        device = self._singleton_device("humidity_generator")
        status = dict(getattr(device, "status", lambda: {})() or {})
        fetch_payload = dict(getattr(device, "fetch_all", lambda: {})() or {})
        return {
            "title": t("pages.devices.workbench.device.grz"),
            "simulated": True,
            "actions": self._actions_for(
                "grz",
                "set_target_temp",
                "set_target_rh",
                "set_target_flow",
                "set_cool",
                "set_heat",
                "set_control",
                "fetch_all",
                "inject_fault",
            ),
            "presets": self._build_presets("grz"),
            "device_status": status,
            "panel_status": {
                "target_temp_display": format_temperature_c(getattr(device, "target_temp_c", None)),
                "target_rh_pct": getattr(device, "target_rh_pct", None),
                "target_flow_lpm": getattr(device, "target_flow_lpm", None),
                "current_temp_display": format_temperature_c(status.get("current_temp_c")),
                "current_rh_pct": status.get("current_rh_pct"),
                "dewpoint_display": format_temperature_c(status.get("dewpoint_c")),
                "flow_lpm": status.get("flow_lpm"),
                "control_enabled": bool(status.get("control_enabled", False)),
                "cool_enabled": bool(getattr(device, "cool_enabled", False)),
                "heat_enabled": bool(getattr(device, "heat_enabled", False)),
                "snapshot_raw": str(fetch_payload.get("raw") or ""),
            },
            "injection_state": {
                "mode": str(getattr(device, "mode", "stable") or "stable"),
                "mode_display": t(
                    f"pages.devices.workbench.enum.grz_fault.{str(getattr(device, 'mode', 'stable') or 'stable')}",
                    default=str(getattr(device, "mode", "stable") or "stable"),
                ),
            },
        }

    def _build_chamber_snapshot(self) -> dict[str, Any]:
        device = self._singleton_device("temperature_chamber")
        status = dict(getattr(device, "status", lambda: {})() or {})
        soak_state = str(status.get("status") or getattr(device, "_phase", "stable"))
        return {
            "title": t("pages.devices.workbench.device.chamber"),
            "simulated": True,
            "actions": self._actions_for("chamber", "set_temperature", "set_humidity", "run", "stop", "set_mode"),
            "presets": self._build_presets("chamber"),
            "device_status": status,
            "panel_status": {
                "temperature_display": format_temperature_c(status.get("temp_c")),
                "humidity_pct": status.get("rh_pct"),
                "running": bool(status.get("running", False)),
                "setpoint_temp_display": format_temperature_c(status.get("target_temp_c")),
                "setpoint_rh_pct": status.get("target_rh_pct"),
                "soak_state": soak_state,
                "soak_state_display": t(
                    f"pages.devices.workbench.enum.chamber_mode.{soak_state}",
                    default=soak_state,
                ),
            },
            "injection_state": {
                "mode": str(getattr(device, "mode", "stable") or "stable"),
                "mode_display": t(
                    f"pages.devices.workbench.enum.chamber_mode.{str(getattr(device, 'mode', 'stable') or 'stable')}",
                    default=str(getattr(device, "mode", "stable") or "stable"),
                ),
            },
        }

    def _build_relay_snapshot(self, *, route_validation: dict[str, Any]) -> dict[str, Any]:
        relay_rows = {relay_name: self._relay_rows(relay_name) for relay_name in self.RELAY_NAMES}
        return {
            "title": t("pages.devices.workbench.device.relay"),
            "simulated": True,
            "actions": self._actions_for("relay", "write_channel", "all_off", "batch_write", "inject_fault"),
            "presets": self._build_presets("relay"),
            "device_status": {
                relay_name: dict(getattr(self._singleton_device(relay_name), "status", lambda: {})() or {})
                for relay_name in self.RELAY_NAMES
            },
            "panel_status": {
                "relay": relay_rows["relay"],
                "relay_8": relay_rows["relay_8"],
                "target_open_valves": list(route_validation.get("target_open_valves") or []),
                "actual_open_valves": list(route_validation.get("actual_open_valves") or []),
                "mismatched_channels": list(route_validation.get("mismatched_channels") or []),
                "summary_line": route_validation.get("summary_line") or "",
            },
            "injection_state": {
                relay_name: {
                    "mode": str(getattr(self._singleton_device(relay_name), "mode", "stable") or "stable"),
                    "mode_display": t(
                        f"pages.devices.workbench.enum.relay_fault.{str(getattr(self._singleton_device(relay_name), 'mode', 'stable') or 'stable')}",
                        default=str(getattr(self._singleton_device(relay_name), "mode", "stable") or "stable"),
                    ),
                    "stuck_channels": sorted(getattr(self._singleton_device(relay_name), "stuck_channels", set()) or []),
                }
                for relay_name in self.RELAY_NAMES
            },
        }

    def _build_thermometer_snapshot(self, *, reference_quality: dict[str, Any]) -> dict[str, Any]:
        device = self._singleton_device("thermometer")
        preview = self._recent_ascii_streams.setdefault("thermometer", deque(maxlen=6))
        if not preview:
            try:
                self._add_recent_ascii("thermometer", device.read_available())
            except Exception:
                pass
        status = dict(getattr(device, "status", lambda: {})() or {})
        reference_status = str(reference_quality.get("thermometer_reference_status") or "not_assessed")
        return {
            "title": t("pages.devices.workbench.device.thermometer"),
            "simulated": True,
            "actions": self._actions_for("thermometer", "set_mode"),
            "presets": self._build_presets("thermometer"),
            "device_status": status,
            "panel_status": {
                "temperature_display": format_temperature_c(status.get("temp_c")),
                "ascii_preview": list(preview),
                "reference_status": reference_status,
                "reference_status_display": t(
                    f"pages.devices.workbench.enum.reference_status.{reference_status}",
                    default=reference_status,
                ),
            },
            "injection_state": {
                "mode": str(getattr(device, "mode", "stable") or "stable"),
                "mode_display": t(
                    f"pages.devices.workbench.enum.thermometer_mode.{str(getattr(device, 'mode', 'stable') or 'stable')}",
                    default=str(getattr(device, "mode", "stable") or "stable"),
                ),
            },
        }

    def _build_pressure_gauge_snapshot(self, *, reference_quality: dict[str, Any]) -> dict[str, Any]:
        device = self._singleton_device("pressure_gauge")
        preview = self._recent_ascii_streams.setdefault("pressure_gauge", deque(maxlen=6))
        if not preview:
            try:
                self._add_recent_ascii("pressure_gauge", device.read_available())
            except Exception:
                pass
        status = dict(getattr(device, "status", lambda: {})() or {})
        reference_status = str(reference_quality.get("pressure_reference_status") or "not_assessed")
        measurement_mode = str(status.get("measurement_mode") or getattr(device, "measurement_mode", "single"))
        return {
            "title": t("pages.devices.workbench.device.pressure_gauge"),
            "simulated": True,
            "actions": self._actions_for("pressure_gauge", "set_measurement_mode", "set_unit", "inject_fault"),
            "presets": self._build_presets("pressure_gauge"),
            "device_status": status,
            "panel_status": {
                "pressure_display": format_pressure_hpa(status.get("pressure_hpa")),
                "unit": str(status.get("unit") or "HPA"),
                "reference_status": reference_status,
                "reference_status_display": t(
                    f"pages.devices.workbench.enum.reference_status.{reference_status}",
                    default=reference_status,
                ),
                "measurement_mode": measurement_mode,
                "measurement_mode_display": t(
                    f"pages.devices.workbench.enum.pressure_output_mode.{measurement_mode}",
                    default=measurement_mode,
                ),
                "stream_preview": list(preview),
            },
            "injection_state": {
                "mode": str(getattr(device, "mode", "stable") or "stable"),
                "mode_display": t(
                    f"pages.devices.workbench.enum.pressure_fault.{str(getattr(device, 'mode', 'stable') or 'stable')}",
                    default=str(getattr(device, "mode", "stable") or "stable"),
                ),
            },
        }

    def _build_reference_quality(self) -> dict[str, Any]:
        matrix = dict(self._device_matrix())
        thermometer_mode = str((matrix.get("thermometer") or {}).get("mode") or "stable")
        pressure_mode = str((matrix.get("pressure_gauge") or {}).get("mode") or "stable")
        thermometer_status = self._normalize_reference_status(thermometer_mode)
        pressure_status = self._normalize_reference_status(pressure_mode)
        degraded_statuses = {"stale", "drift", "warmup_unstable", "wrong_unit_configuration"}
        failed_statuses = {
            "no_response",
            "parse_fail",
            "hardware_missing",
            "missing",
            "corrupted_ascii",
            "truncated_ascii",
            "unsupported_command",
            "display_interrupted",
        }
        values = [thermometer_status, pressure_status]
        if any(value in failed_statuses for value in values):
            overall = "failed"
        elif any(value in degraded_statuses for value in values):
            overall = "degraded"
        else:
            overall = "healthy"
        payload = {
            "reference_integrity": overall,
            "reference_quality": overall,
            "reference_quality_degraded": overall != "healthy",
            "thermometer_reference_status": thermometer_status,
            "pressure_reference_status": pressure_status,
            "reasons": [
                reason
                for reason in (
                    None if thermometer_status in {"healthy", "skipped_by_profile"} else f"thermometer:{thermometer_status}",
                    None if pressure_status in {"healthy", "skipped_by_profile"} else f"pressure:{pressure_status}",
                )
                if reason
            ],
        }
        self._simulation_context["workbench_reference_quality"] = dict(payload)
        return payload

    @staticmethod
    def _normalize_reference_status(mode: str) -> str:
        normalized = str(mode or "").strip().lower()
        if normalized in {"", "stable", "plus_200_mode", "continuous_stream", "sample_hold", "unit_switch"}:
            return "healthy"
        return normalized

    def _relay_rows(self, relay_name: str) -> list[dict[str, Any]]:
        device = self._singleton_device(relay_name)
        channel_count = int(getattr(device, "channel_count", 16) or 16)
        desired_map = self._desired_relay_states.setdefault(relay_name, {})
        try:
            coils = list((device.read_coils(0, channel_count).bits or []))
        except Exception:
            coils = [False] * channel_count
        try:
            inputs = list((device.read_discrete_inputs(0, channel_count).bits or []))
        except Exception:
            inputs = [False] * channel_count
        mapping = self._valve_mapping_for_relay(relay_name)
        rows: list[dict[str, Any]] = []
        for channel in range(1, channel_count + 1):
            desired = bool(desired_map.get(channel, False))
            actual = bool(coils[channel - 1]) if channel - 1 < len(coils) else False
            row = {
                "channel": channel,
                "desired": desired,
                "desired_display": t("pages.devices.workbench.enum.on_off.on" if desired else "pages.devices.workbench.enum.on_off.off"),
                "actual": actual,
                "actual_display": t("pages.devices.workbench.enum.on_off.on" if actual else "pages.devices.workbench.enum.on_off.off"),
                "input": bool(inputs[channel - 1]) if channel - 1 < len(inputs) else False,
                "input_display": t(
                    "pages.devices.workbench.enum.on_off.on"
                    if (channel - 1 < len(inputs) and bool(inputs[channel - 1]))
                    else "pages.devices.workbench.enum.on_off.off"
                ),
                "valve_mapping": mapping.get(channel, t("pages.devices.workbench.common.unmapped")),
            }
            rows.append(row)
        return rows

    def _build_route_validation(self) -> dict[str, Any]:
        target_open_valves: list[str] = []
        actual_open_valves: list[str] = []
        mismatched_channels: list[dict[str, Any]] = []
        target_state: dict[str, dict[str, bool]] = {}
        actual_state: dict[str, dict[str, bool]] = {}

        for relay_name in self.RELAY_NAMES:
            rows = self._relay_rows(relay_name)
            target_state[relay_name] = {}
            actual_state[relay_name] = {}
            for row in rows:
                channel = int(row["channel"])
                channel_text = str(channel)
                target_state[relay_name][channel_text] = bool(row["desired"])
                actual_state[relay_name][channel_text] = bool(row["actual"])
                if bool(row["desired"]):
                    target_open_valves.append(f"{relay_name}:{channel}")
                if bool(row["actual"]):
                    actual_open_valves.append(f"{relay_name}:{channel}")
                if bool(row["desired"]) != bool(row["actual"]):
                    mismatched_channels.append(
                        {
                            "relay": relay_name,
                            "channel": channel,
                            "target": bool(row["desired"]),
                            "actual": bool(row["actual"]),
                            "valve_mapping": row["valve_mapping"],
                        }
                    )

        payload = {
            "target_open_valves": target_open_valves,
            "actual_open_valves": actual_open_valves,
            "target_relay_state": target_state,
            "actual_relay_state": actual_state,
            "route_physical_state_match": not mismatched_channels,
            "relay_physical_mismatch": bool(mismatched_channels),
            "mismatched_channels": mismatched_channels,
            "summary_line": t(
                "pages.devices.workbench.relay.route_match"
                if not mismatched_channels
                else "pages.devices.workbench.relay.route_mismatch",
                target=", ".join(target_open_valves) or t("common.none"),
                actual=", ".join(actual_open_valves) or t("common.none"),
            ),
        }
        self._simulation_context["workbench_route_trace"] = copy.deepcopy(payload)
        return payload

    def _valve_mapping_for_relay(self, relay_name: str) -> dict[int, str]:
        valves = getattr(self.facade.config, "valves", None)
        valve_mapping = dict(getattr(valves, "valve_mapping", {}) or {})
        relay_map = dict(getattr(valves, "relay_map", {}) or {})
        mapping: dict[int, str] = {}
        for label, logical in valve_mapping.items():
            try:
                logical_valve = int(logical)
            except Exception:
                continue
            relay_target = relay_map.get(str(logical_valve), relay_map.get(logical_valve, logical_valve))
            relay_name_for_target = relay_name
            channel = None
            if isinstance(relay_target, dict):
                relay_name_for_target = str(relay_target.get("relay") or relay_name_for_target)
                channel = relay_target.get("channel")
            elif isinstance(relay_target, (list, tuple)) and len(relay_target) >= 2:
                relay_name_for_target = str(relay_target[0] or relay_name_for_target)
                channel = relay_target[1]
            else:
                channel = relay_target
            try:
                resolved_channel = int(channel)
            except Exception:
                resolved_channel = logical_valve
            if relay_name_for_target in {relay_name, relay_name.replace("relay_", "relay")}:
                mapping[resolved_channel] = str(label)
        return mapping

    def _sync_relay_logical(self, device: Any, channel: int, desired: bool) -> None:
        updater = getattr(device, "set_logical_valve_state", None)
        if callable(updater):
            try:
                updater(channel, desired, physical_channel=channel)
            except Exception:
                pass

    def _execute_analyzer_action(self, action: str, **params: Any) -> str:
        analyzer_index = max(0, min(self.ANALYZER_COUNT - 1, int(params.get("analyzer_index", self._selected_analyzer_index) or 0)))
        self._selected_analyzer_index = analyzer_index
        device_name = f"gas_analyzer_{analyzer_index}"
        device = self._analyzer_device(analyzer_index)
        if action == "select":
            return t("pages.devices.workbench.message.selected_analyzer", index=analyzer_index + 1)
        if action == "set_mode":
            mode = max(1, min(3, int(params.get("mode", 2) or 2)))
            device.set_mode(mode)
            self._set_override(device_name, mode=mode)
            return t("pages.devices.workbench.message.analyzer_mode_set", index=analyzer_index + 1, mode=mode)
        if action == "set_active_state":
            active = bool(params.get("active", True))
            device.set_active_send(active)
            self._set_override(device_name, active_send=active)
            return t(
                "pages.devices.workbench.message.analyzer_active_set",
                index=analyzer_index + 1,
                state=t("pages.devices.workbench.enum.active_state.active" if active else "pages.devices.workbench.enum.active_state.passive"),
            )
        if action == "set_frequency":
            hz = max(1, int(float(params.get("frequency_hz", 5) or 5)))
            device.set_ftd(hz)
            self._set_override(device_name, ftd_hz=hz)
            return t("pages.devices.workbench.message.analyzer_frequency_set", index=analyzer_index + 1, frequency=hz)
        if action == "read_frame":
            frame = device.read_latest_data(allow_passive_fallback=True)
            self._add_recent_frame(device_name, frame)
            return t("pages.devices.workbench.message.analyzer_frame_read", index=analyzer_index + 1)
        if action == "broadcast":
            frame = device.process_command("ID,YGAS,FFF")
            self._add_recent_frame(device_name, frame)
            return t("pages.devices.workbench.message.analyzer_broadcast", index=analyzer_index + 1)
        if action == "inject_fault":
            fault = str(params.get("fault") or "stable").strip().lower()
            setattr(device, "_mode2_stream", fault)
            if fault == "sensor_precheck_fail":
                setattr(device, "_sensor_precheck", "strict_fail")
            self._set_override(
                device_name,
                mode2_stream=fault,
                sensor_precheck="strict_fail" if fault == "sensor_precheck_fail" else getattr(device, "_sensor_precheck", "strict_pass"),
            )
            return t(
                "pages.devices.workbench.message.analyzer_fault_set",
                index=analyzer_index + 1,
                fault=t(f"pages.devices.workbench.enum.analyzer_fault.{fault}", default=fault),
            )
        raise ValueError(f"unsupported analyzer action: {action}")

    def _execute_pace_action(self, action: str, **params: Any) -> str:
        device = self._singleton_device("pressure_controller")
        spec = dict(self._device_matrix().get("pressure_controller") or {})
        if action == "set_vent":
            enabled = bool(params.get("enabled", True))
            device.vent(enabled)
            return t("pages.devices.workbench.message.pace_vent_set", state=t("pages.devices.workbench.enum.on_off.on" if enabled else "pages.devices.workbench.enum.on_off.off"))
        if action == "set_output":
            enabled = bool(params.get("enabled", True))
            device.set_output(enabled)
            return t("pages.devices.workbench.message.pace_output_set", state=t("pages.devices.workbench.enum.on_off.on" if enabled else "pages.devices.workbench.enum.on_off.off"))
        if action == "set_isolation":
            enabled = bool(params.get("enabled", True))
            device.set_isolation_open(enabled)
            return t("pages.devices.workbench.message.pace_isolation_set", state=t("pages.devices.workbench.enum.on_off.on" if enabled else "pages.devices.workbench.enum.on_off.off"))
        if action == "set_pressure":
            pressure_hpa = float(params.get("pressure_hpa", 1000.0) or 1000.0)
            device.set_pressure_hpa(pressure_hpa)
            return t("pages.devices.workbench.message.pace_pressure_set", pressure=format_pressure_hpa(pressure_hpa))
        if action == "set_unit":
            unit = str(params.get("unit") or "HPA").strip().upper()
            device.process_command(f":UNIT:PRES {unit}")
            spec["wrong_unit_configuration"] = unit != "HPA"
            self._update_spec("pressure_controller", unit=unit, wrong_unit_configuration=spec["wrong_unit_configuration"])
            return t("pages.devices.workbench.message.pace_unit_set", unit=unit)
        if action == "read_pressure":
            device.read_pressure()
            return t("pages.devices.workbench.message.pace_pressure_read")
        if action == "query_error":
            error_text = device.query(":SYST:ERR?")
            return t("pages.devices.workbench.message.pace_error_queried", error=error_text or t("common.none"))
        if action == "inject_fault":
            fault = str(params.get("fault") or "stable").strip().lower()
            if fault == "wrong_unit_configuration":
                device.process_command(":UNIT:PRES PSIA")
                self._update_spec("pressure_controller", wrong_unit_configuration=True, unit="PSIA")
                return t("pages.devices.workbench.message.pace_fault_set", fault=t(f"pages.devices.workbench.enum.pace_fault.{fault}", default=fault))
            device.mode = fault
            if fault == "unsupported_header":
                device.unsupported_headers = [":SENS:PRES:INL?"]
            self._update_spec("pressure_controller", mode=fault, unsupported_headers=list(getattr(device, "unsupported_headers", []) or []))
            return t("pages.devices.workbench.message.pace_fault_set", fault=t(f"pages.devices.workbench.enum.pace_fault.{fault}", default=fault))
        raise ValueError(f"unsupported PACE action: {action}")

    def _execute_grz_action(self, action: str, **params: Any) -> str:
        device = self._singleton_device("humidity_generator")
        if action == "set_target_temp":
            value = float(params.get("temperature_c", 25.0) or 25.0)
            device.set_target_temp(value)
            return t("pages.devices.workbench.message.grz_target_temp_set", temperature=format_temperature_c(value))
        if action == "set_target_rh":
            value = float(params.get("humidity_pct", 35.0) or 35.0)
            device.set_target_rh(value)
            return t("pages.devices.workbench.message.grz_target_rh_set", humidity=f"{value:g}%")
        if action == "set_target_flow":
            value = float(params.get("flow_lpm", 1.0) or 1.0)
            device.set_flow_target(value)
            return t("pages.devices.workbench.message.grz_target_flow_set", flow=f"{value:g} L/min")
        if action == "set_cool":
            enabled = bool(params.get("enabled", True))
            if enabled:
                device.cool_on()
            else:
                device.cool_off()
            return t("pages.devices.workbench.message.grz_cool_set", state=t("pages.devices.workbench.enum.on_off.on" if enabled else "pages.devices.workbench.enum.on_off.off"))
        if action == "set_heat":
            enabled = bool(params.get("enabled", True))
            if enabled:
                device.heat_on()
            else:
                device.heat_off()
            return t("pages.devices.workbench.message.grz_heat_set", state=t("pages.devices.workbench.enum.on_off.on" if enabled else "pages.devices.workbench.enum.on_off.off"))
        if action == "set_control":
            enabled = bool(params.get("enabled", True))
            device.enable_control(enabled)
            return t("pages.devices.workbench.message.grz_control_set", state=t("pages.devices.workbench.enum.on_off.on" if enabled else "pages.devices.workbench.enum.on_off.off"))
        if action == "fetch_all":
            device.fetch_all()
            return t("pages.devices.workbench.message.grz_snapshot_read")
        if action == "inject_fault":
            fault = str(params.get("fault") or "stable").strip().lower()
            device.mode = fault
            self._update_spec("humidity_generator", mode=fault)
            return t("pages.devices.workbench.message.grz_fault_set", fault=t(f"pages.devices.workbench.enum.grz_fault.{fault}", default=fault))
        raise ValueError(f"unsupported GRZ action: {action}")

    def _execute_chamber_action(self, action: str, **params: Any) -> str:
        device = self._singleton_device("temperature_chamber")
        if action == "set_temperature":
            value = float(params.get("temperature_c", 25.0) or 25.0)
            device.set_temp_c(value)
            return t("pages.devices.workbench.message.chamber_temperature_set", temperature=format_temperature_c(value))
        if action == "set_humidity":
            value = float(params.get("humidity_pct", 40.0) or 40.0)
            device.set_rh_pct(value)
            return t("pages.devices.workbench.message.chamber_humidity_set", humidity=f"{value:g}%")
        if action == "run":
            device.start()
            return t("pages.devices.workbench.message.chamber_run")
        if action == "stop":
            device.stop()
            return t("pages.devices.workbench.message.chamber_stop")
        if action == "set_mode":
            mode = str(params.get("mode") or "stable").strip().lower()
            device.mode = mode
            self._update_spec("temperature_chamber", mode=mode)
            return t("pages.devices.workbench.message.chamber_mode_set", mode=t(f"pages.devices.workbench.enum.chamber_mode.{mode}", default=mode))
        raise ValueError(f"unsupported chamber action: {action}")

    def _execute_relay_action(self, action: str, **params: Any) -> str:
        relay_name = str(params.get("relay_name") or "relay").strip().lower()
        if relay_name not in self.RELAY_NAMES:
            relay_name = "relay"
        device = self._singleton_device(relay_name)
        desired_map = self._desired_relay_states.setdefault(relay_name, {})
        if action == "write_channel":
            channel = max(1, int(params.get("channel", 1) or 1))
            enabled = bool(params.get("enabled", True))
            desired_map[channel] = enabled
            device.set_valve(channel, enabled)
            self._sync_relay_logical(device, channel, enabled)
            return t(
                "pages.devices.workbench.message.relay_channel_set",
                relay=t(f"pages.devices.workbench.device.{relay_name}", default=relay_name),
                channel=channel,
                state=t("pages.devices.workbench.enum.on_off.on" if enabled else "pages.devices.workbench.enum.on_off.off"),
            )
        if action == "all_off":
            device.close_all()
            for channel in range(1, int(getattr(device, "channel_count", 16) or 16) + 1):
                desired_map[channel] = False
                self._sync_relay_logical(device, channel, False)
            return t("pages.devices.workbench.message.relay_all_off", relay=t(f"pages.devices.workbench.device.{relay_name}", default=relay_name))
        if action == "batch_write":
            raw_channels = params.get("channels") or []
            if isinstance(raw_channels, str):
                requested = [item.strip() for item in raw_channels.replace(";", ",").split(",")]
            else:
                requested = list(raw_channels)
            open_channels = {int(item) for item in requested if str(item).strip().isdigit()}
            channel_count = int(getattr(device, "channel_count", 16) or 16)
            batch = []
            for channel in range(1, channel_count + 1):
                desired = channel in open_channels
                desired_map[channel] = desired
                batch.append(desired)
            device.write_coils(0, batch)
            for channel in range(1, channel_count + 1):
                self._sync_relay_logical(device, channel, desired_map[channel])
            return t("pages.devices.workbench.message.relay_batch_set", relay=t(f"pages.devices.workbench.device.{relay_name}", default=relay_name))
        if action == "inject_fault":
            fault = str(params.get("fault") or "stable").strip().lower()
            if fault == "stuck_channel":
                stuck_channels = params.get("stuck_channels") or [1]
                if isinstance(stuck_channels, str):
                    stuck_channels = [item.strip() for item in stuck_channels.replace(";", ",").split(",")]
                normalized = sorted({int(item) for item in stuck_channels if str(item).strip().isdigit()}) or [1]
                device.stuck_channels = set(normalized)
                device.mode = "stuck_channel"
                self._update_spec(relay_name, mode="stuck_channel", stuck_channels=normalized)
                return t("pages.devices.workbench.message.relay_fault_set", fault=t("pages.devices.workbench.enum.relay_fault.stuck_channel"))
            device.mode = fault
            self._update_spec(relay_name, mode=fault)
            return t("pages.devices.workbench.message.relay_fault_set", fault=t(f"pages.devices.workbench.enum.relay_fault.{fault}", default=fault))
        raise ValueError(f"unsupported relay action: {action}")

    def _execute_thermometer_action(self, action: str, **params: Any) -> str:
        if action != "set_mode":
            raise ValueError(f"unsupported thermometer action: {action}")
        device = self._singleton_device("thermometer")
        mode = str(params.get("mode") or "stable").strip().lower()
        device.mode = mode
        device.plus_200_mode = mode == "plus_200_mode"
        self._update_spec("thermometer", mode=mode, plus_200_mode=device.plus_200_mode)
        self._add_recent_ascii("thermometer", device.read_available())
        return t("pages.devices.workbench.message.thermometer_mode_set", mode=t(f"pages.devices.workbench.enum.thermometer_mode.{mode}", default=mode))

    def _execute_pressure_gauge_action(self, action: str, **params: Any) -> str:
        device = self._singleton_device("pressure_gauge")
        if action == "set_measurement_mode":
            mode = str(params.get("measurement_mode") or "single").strip().lower()
            if mode == "continuous":
                device.measurement_mode = "continuous"
                device.mode = "continuous_stream"
            elif mode == "sample_hold":
                device.measurement_mode = "sample_hold"
                device.mode = "sample_hold"
            else:
                device.measurement_mode = "single"
                device.mode = "stable"
            self._update_spec("pressure_gauge", measurement_mode=device.measurement_mode, mode=device.mode)
            self._add_recent_ascii("pressure_gauge", device.read_available())
            return t("pages.devices.workbench.message.pressure_mode_set", mode=t(f"pages.devices.workbench.enum.pressure_output_mode.{device.measurement_mode}", default=device.measurement_mode))
        if action == "set_unit":
            unit = str(params.get("unit") or "HPA").strip().upper()
            device.unit = unit
            if device.mode == "wrong_unit_configuration" and unit == "HPA":
                device.mode = "stable"
            self._update_spec("pressure_gauge", unit=unit, mode=device.mode)
            return t("pages.devices.workbench.message.pressure_unit_set", unit=unit)
        if action == "inject_fault":
            fault = str(params.get("fault") or "stable").strip().lower()
            if fault == "wrong_unit_configuration":
                device.mode = fault
                device.unit = "PSIA"
                self._update_spec("pressure_gauge", mode=fault, unit="PSIA")
            else:
                device.mode = fault
                self._update_spec("pressure_gauge", mode=fault)
            self._add_recent_ascii("pressure_gauge", device.read_available())
            return t("pages.devices.workbench.message.pressure_fault_set", fault=t(f"pages.devices.workbench.enum.pressure_fault.{fault}", default=fault))
        raise ValueError(f"unsupported pressure-gauge action: {action}")
