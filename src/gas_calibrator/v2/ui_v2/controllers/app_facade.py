from __future__ import annotations

from collections import Counter, deque
import copy
from dataclasses import asdict, is_dataclass
from datetime import datetime
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import threading
import time
from typing import Any, Optional

from ...config import (
    AppConfig,
    build_step2_config_governance_handoff,
    build_step2_config_safety_review,
    hydrate_step2_config_safety_summary,
    summarize_step2_config_safety,
)
from ...adapters.results_gateway import ResultsGateway
from ...core.acceptance_model import build_validation_acceptance_snapshot, normalize_evidence_source
from ...core.controlled_state_machine_profile import (
    STATE_TRANSITION_EVIDENCE_FILENAME,
    STATE_TRANSITION_EVIDENCE_MARKDOWN_FILENAME,
)
from ...core.event_bus import Event, EventType
from ...core.multi_source_stability import (
    MULTI_SOURCE_STABILITY_EVIDENCE_FILENAME,
    MULTI_SOURCE_STABILITY_EVIDENCE_MARKDOWN_FILENAME,
)
from ...core.measurement_phase_coverage import MEASUREMENT_PHASE_COVERAGE_REPORT_FILENAME
from ...core.reviewer_fragments_contract import (
    BOUNDARY_FRAGMENT_FAMILY,
    NON_CLAIM_FRAGMENT_FAMILY,
    PHASE_CONTRAST_FRAGMENT_FAMILY,
    fragment_filter_rows_to_ids,
    normalize_fragment_filter_rows,
)
from ...core import recognition_readiness_artifacts as recognition_readiness
from ...core.offline_artifacts import build_point_taxonomy_handoff
from ...domain.mode_models import ModeProfile, RunMode
from ...review_surface_formatter import (
    build_measurement_review_digest_lines,
    build_readiness_review_digest_lines,
    build_offline_diagnostic_detail_line,
    build_offline_diagnostic_detail_item_line,
    build_offline_diagnostic_scope_line_from_counts,
    collect_boundary_digest_lines,
    collect_offline_diagnostic_detail_lines,
    humanize_offline_diagnostic_detail_value,
    humanize_offline_diagnostic_summary_value,
    humanize_review_center_coverage_text,
    humanize_review_surface_text,
    normalize_offline_diagnostic_line,
    offline_diagnostic_scope_label,
)
from ...storage.profile_store import ProfileStore
from ...qc.qc_report import build_qc_evidence_section, build_qc_review_payload, build_qc_reviewer_card
from .device_workbench import DeviceWorkbenchController
from .plan_gateway import PlanGateway
from ..i18n import (
    display_acceptance_value,
    display_artifact_role,
    display_bool,
    display_compare_status,
    display_device_status,
    display_evidence_source,
    display_evidence_state,
    display_fragment_value,
    display_phase,
    display_presence,
    display_reference_quality,
    display_risk_level,
    display_run_mode,
    display_route,
    display_winner_status,
    format_pressure_hpa,
    format_ppm,
    format_temperature_c,
    t,
)
from ..review_center_artifact_scope import (
    build_review_scope_manifest_payload,
    render_review_scope_manifest_markdown,
)
from ..review_scope_export_index import (
    INDEX_FILENAME as REVIEW_SCOPE_EXPORT_INDEX_FILENAME,
    build_review_scope_batch_id,
    write_review_scope_export_index,
)
from ..utils.app_info import APP_INFO
from ..utils.preferences_store import PreferencesStore, merge_preferences
from ..utils.recent_runs_store import RecentRunsStore
from ..utils.runtime_paths import RuntimePaths

VALIDATION_COMPARE_ROOT = Path(__file__).resolve().parents[2] / "output" / "v1_v2_compare"
PRIMARY_VALIDATION_PROFILE = "skip0_co2_only_replacement"
PRIMARY_REAL_VALIDATION_MISSING_STATUS = "PRIMARY_REAL_VALIDATION_LATEST_MISSING"
_REVIEW_CENTER_PHASE_OPTION_LABELS = {
    "step2_tail_stage3_bridge": "Step 2 tail / Stage 3 bridge",
    "stage_admission_review": "Stage admission review",
    "engineering_isolation_admission": "engineering-isolation admission",
    "stage3_real_validation_bridge": "Stage 3 real validation bridge",
    "stage3_standards_alignment": "Stage 3 standards alignment",
}


def _available_reviewer_artifact_entries(entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        _apply_fragment_filter_contract(dict(item))
        for item in entries
        if isinstance(item, dict) and bool(item.get("available", False))
    ]


def _dedupe_fragment_filter_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    deduped: list[dict[str, Any]] = []
    seen: set[str] = set()
    for item in list(rows or []):
        payload = dict(item or {})
        canonical_fragment_id = str(
            payload.get("canonical_fragment_id")
            or payload.get("id")
            or ""
        ).strip()
        if not canonical_fragment_id or canonical_fragment_id in seen:
            continue
        seen.add(canonical_fragment_id)
        deduped.append(payload)
    return deduped


def _normalize_boundary_filter_rows(payload: dict[str, Any]) -> list[dict[str, Any]]:
    current = dict(payload or {})
    return _dedupe_fragment_filter_rows(
        [
            *normalize_fragment_filter_rows(
                BOUNDARY_FRAGMENT_FAMILY,
                list(current.get("boundary_filter_rows") or [])
                or list(current.get("boundary_fragments") or [])
                or list(current.get("boundary_filters") or [])
                or list(current.get("boundary_statements") or []),
                display_locale="en_US",
            ),
            *normalize_fragment_filter_rows(
                NON_CLAIM_FRAGMENT_FAMILY,
                list(current.get("non_claim_filter_rows") or [])
                or list(current.get("non_claim_fragments") or [])
                or list(current.get("non_claim_filters") or [])
                or list(current.get("non_claim") or [])
                or list(current.get("boundary_filters") or []),
                display_locale="en_US",
            ),
        ]
    )


def _normalize_phase_contrast_filter_rows(payload: dict[str, Any]) -> list[dict[str, Any]]:
    current = dict(payload or {})
    return _dedupe_fragment_filter_rows(
        normalize_fragment_filter_rows(
            PHASE_CONTRAST_FRAGMENT_FAMILY,
            list(current.get("phase_contrast_filter_rows") or [])
            or list(current.get("phase_contrast_fragments") or [])
            or list(current.get("comparison_fragments") or [])
            or list(current.get("phase_contrast_filters") or []),
            display_locale="en_US",
        )
    )


def _apply_fragment_filter_contract(payload: dict[str, Any]) -> dict[str, Any]:
    current = dict(payload or {})
    boundary_filter_rows = _normalize_boundary_filter_rows(current)
    non_claim_filter_rows = _dedupe_fragment_filter_rows(
        normalize_fragment_filter_rows(
            NON_CLAIM_FRAGMENT_FAMILY,
            list(current.get("non_claim_filter_rows") or [])
            or list(current.get("non_claim_fragments") or [])
            or list(current.get("non_claim_filters") or [])
            or list(current.get("non_claim") or []),
            display_locale="en_US",
        )
    )
    phase_contrast_filter_rows = _normalize_phase_contrast_filter_rows(current)
    current["boundary_filter_rows"] = boundary_filter_rows
    current["boundary_filters"] = fragment_filter_rows_to_ids(boundary_filter_rows)
    current["non_claim_filter_rows"] = non_claim_filter_rows
    current["non_claim_filters"] = fragment_filter_rows_to_ids(non_claim_filter_rows)
    current["phase_contrast_filter_rows"] = phase_contrast_filter_rows
    current["phase_contrast_filters"] = fragment_filter_rows_to_ids(phase_contrast_filter_rows)
    return current


def _build_fragment_filter_options(
    rows: list[dict[str, Any]],
    *,
    all_label_key: str,
    all_label_default: str,
) -> list[dict[str, str]]:
    return [
        {"id": "all", "label": t(all_label_key, default=all_label_default)}
    ] + [
        {
            "id": str(item.get("canonical_fragment_id") or item.get("id") or ""),
            "label": str(
                display_fragment_value(
                    str(item.get("fragment_family") or BOUNDARY_FRAGMENT_FAMILY),
                    item.get("fragment_key"),
                    params=dict(item.get("params") or {}),
                    default=str(item.get("text") or ""),
                )
                or item.get("label")
                or item.get("display_text")
            ),
        }
        for item in _dedupe_fragment_filter_rows(rows)
        if str(item.get("canonical_fragment_id") or item.get("id") or "").strip()
    ]


def _build_reviewer_filter_options(entries: list[dict[str, Any]]) -> dict[str, list[dict[str, str]]]:
    available_entries = _available_reviewer_artifact_entries(entries)
    phase_values = _dedupe_entry_filter_values(available_entries, "phase_filters")
    artifact_role_values = _dedupe_entry_filter_values(available_entries, "artifact_role_filters")
    standard_family_values = _dedupe_entry_filter_values(available_entries, "standard_family_filters")
    evidence_category_values = _dedupe_entry_filter_values(available_entries, "evidence_category_filters")
    boundary_rows = _dedupe_fragment_filter_rows(
        [
            row
            for entry in available_entries
            for row in list(_apply_fragment_filter_contract(entry).get("boundary_filter_rows") or [])
            if isinstance(row, dict)
        ]
    )
    anchor_options = [
        {
            "id": str(item.get("anchor_id") or ""),
            "label": str(item.get("anchor_label") or item.get("name_text") or item.get("title_text") or ""),
        }
        for item in available_entries
        if str(item.get("anchor_id") or "").strip()
    ]
    return {
        "phase_options": [
            {"id": "all", "label": t("results.review_center.filter.all_phases", default="全部阶段")}
        ]
        + [
            {"id": value, "label": _REVIEW_CENTER_PHASE_OPTION_LABELS.get(value, value)}
            for value in phase_values
        ],
        "artifact_role_options": [
            {"id": "all", "label": t("results.review_center.filter.all_artifact_roles", default="全部工件角色")}
        ]
        + [
            {
                "id": value,
                "label": display_artifact_role(
                    value,
                    default=value,
                ),
            }
            for value in artifact_role_values
        ],
        "standard_family_options": [
            {"id": "all", "label": t("results.review_center.filter.all_standard_families", default="全部标准家族")}
        ]
        + [{"id": value, "label": value} for value in standard_family_values],
        "evidence_category_options": [
            {"id": "all", "label": t("results.review_center.filter.all_evidence_categories", default="全部证据类别")}
        ]
        + [{"id": value, "label": value} for value in evidence_category_values],
        "boundary_options": _build_fragment_filter_options(
            boundary_rows,
            all_label_key="results.review_center.filter.all_boundaries",
            all_label_default="全部边界",
        ),
        "anchor_options": [
            {"id": "all", "label": t("results.review_center.filter.all_anchors", default="全部锚点")}
        ]
        + anchor_options,
    }


def _dedupe_entry_filter_values(entries: list[dict[str, Any]], field_name: str) -> list[str]:
    rows: list[str] = []
    for entry in entries:
        for value in list(entry.get(field_name) or []):
            text = str(value or "").strip()
            if text and text not in rows:
                rows.append(text)
    return rows


def _dedupe_item_filter_values(items: list[dict[str, Any]], field_name: str) -> list[str]:
    rows: list[str] = []
    for item in items:
        for value in list(item.get(field_name) or []):
            text = str(value or "").strip()
            if text and text not in rows:
                rows.append(text)
    return rows


def _merge_filter_options(*option_groups: list[dict[str, str]]) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    seen: set[str] = set()
    for option_group in option_groups:
        for option in list(option_group or []):
            option_id = str(dict(option or {}).get("id") or "").strip()
            if not option_id or option_id in seen:
                continue
            seen.add(option_id)
            rows.append({"id": option_id, "label": str(dict(option or {}).get("label") or option_id)})
    return rows


def _build_measurement_core_filter_options(items: list[dict[str, Any]]) -> dict[str, list[dict[str, str]]]:
    phase_values = _dedupe_item_filter_values(items, "phase_filters")
    artifact_role_values = _dedupe_item_filter_values(items, "artifact_role_filters")
    evidence_category_values = _dedupe_item_filter_values(items, "evidence_category_filters")
    boundary_rows = _dedupe_fragment_filter_rows(
        [
            row
            for item in items
            for row in list(_apply_fragment_filter_contract(item).get("boundary_filter_rows") or [])
            if isinstance(row, dict)
        ]
    )
    anchor_values = [
        {
            "id": str(item.get("anchor_id") or ""),
            "label": str(item.get("anchor_label") or item.get("summary") or item.get("type_display") or ""),
        }
        for item in items
        if str(item.get("anchor_id") or "").strip()
    ]
    route_values = _dedupe_item_filter_values(items, "route_filters")
    signal_family_values = _dedupe_item_filter_values(items, "signal_family_filters")
    decision_result_values = _dedupe_item_filter_values(items, "decision_result_filters")
    policy_version_values = _dedupe_item_filter_values(items, "policy_version_filters")
    evidence_source_values = _dedupe_item_filter_values(items, "evidence_source_filters")
    return {
        "phase_options": [
            {"id": "all", "label": t("results.review_center.filter.all_phases", default="鍏ㄩ儴闃舵")}
        ] + [{"id": value, "label": _REVIEW_CENTER_PHASE_OPTION_LABELS.get(value, value)} for value in phase_values],
        "artifact_role_options": [
            {"id": "all", "label": t("results.review_center.filter.all_artifact_roles", default="鍏ㄩ儴宸ヤ欢瑙掕壊")}
        ] + [{"id": value, "label": display_artifact_role(value, default=value)} for value in artifact_role_values],
        "evidence_category_options": [
            {"id": "all", "label": t("results.review_center.filter.all_evidence_categories", default="鍏ㄩ儴璇佹嵁绫诲埆")}
        ] + [{"id": value, "label": value} for value in evidence_category_values],
        "boundary_options": _build_fragment_filter_options(
            boundary_rows,
            all_label_key="results.review_center.filter.all_boundaries",
            all_label_default="全部边界",
        ),
        "anchor_options": [
            {"id": "all", "label": t("results.review_center.filter.all_anchors", default="鍏ㄩ儴閿氱偣")}
        ] + anchor_values,
        "route_options": [
            {"id": "all", "label": t("results.review_center.filter.all_routes", default="鍏ㄩ儴璺敱")}
        ] + [{"id": value, "label": value} for value in route_values],
        "signal_family_options": [
            {"id": "all", "label": t("results.review_center.filter.all_signal_families", default="鍏ㄩ儴淇″彿瀹舵棌")}
        ] + [{"id": value, "label": value} for value in signal_family_values],
        "decision_result_options": [
            {"id": "all", "label": t("results.review_center.filter.all_decision_results", default="鍏ㄩ儴鍒ゅ畾缁撴灉")}
        ] + [{"id": value, "label": value} for value in decision_result_values],
        "policy_version_options": [
            {"id": "all", "label": t("results.review_center.filter.all_policy_versions", default="鍏ㄩ儴绛栫暐鐗堟湰")}
        ] + [{"id": value, "label": value} for value in policy_version_values],
        "evidence_source_options": [
            {
                "id": "all",
                "label": t("results.review_center.filter.all_evidence_sources", default="全部证据来源"),
            }
        ] + [{"id": value, "label": value} for value in evidence_source_values],
    }
REAL_VALIDATION_LATEST_INDEXES = (
    (PRIMARY_VALIDATION_PROFILE, VALIDATION_COMPARE_ROOT / "skip0_co2_only_replacement_latest.json"),
    ("skip0_co2_only_diagnostic_relaxed", VALIDATION_COMPARE_ROOT / "skip0_co2_only_diagnostic_relaxed_latest.json"),
    ("skip0_replacement", VALIDATION_COMPARE_ROOT / "skip0_replacement_latest.json"),
    ("h2o_only_replacement", VALIDATION_COMPARE_ROOT / "h2o_only_replacement_latest.json"),
)
SIMULATED_VALIDATION_LATEST_INDEXES = (
    ("replacement_full_route_simulated", VALIDATION_COMPARE_ROOT / "replacement_full_route_simulated_latest.json"),
    (
        "replacement_full_route_simulated_diagnostic",
        VALIDATION_COMPARE_ROOT / "replacement_full_route_simulated_diagnostic_latest.json",
    ),
    (
        "replacement_skip0_co2_only_simulated",
        VALIDATION_COMPARE_ROOT / "replacement_skip0_co2_only_simulated_latest.json",
    ),
    ("replacement_h2o_only_simulated", VALIDATION_COMPARE_ROOT / "replacement_h2o_only_simulated_latest.json"),
)
VALIDATION_LATEST_INDEXES = REAL_VALIDATION_LATEST_INDEXES
REVIEW_CENTER_RECENT_RUN_LIMIT = 12
REVIEW_CENTER_SIBLING_ROOT_LIMIT = 8
REVIEW_CENTER_COMPARE_SIGNATURE_LIMIT = 8
REVIEW_CENTER_ROOT_CACHE_LIMIT = 32
REVIEW_CENTER_ARTIFACT_CACHE_LIMIT = 128
REVIEW_CENTER_SCAN_ROOT_LIMIT = 24
REVIEW_CENTER_SCAN_BUDGET = 192
REVIEW_CENTER_SCAN_MATCH_LIMIT = 16


def _normalize_simulated_evidence_source(value: Any, *, default: str = "simulated_protocol") -> str:
    source = str(value or default).strip() or default
    if source.lower() in {"simulated", "simulated_protocol"}:
        return normalize_evidence_source(source)
    return source


def _normalize_workbench_evidence_payload(
    payload: dict[str, Any] | None,
    *,
    config_safety: dict[str, Any] | None = None,
    config_safety_review: dict[str, Any] | None = None,
) -> dict[str, Any]:
    normalized = dict(payload or {})
    if not normalized:
        return {}
    evidence_source = _normalize_simulated_evidence_source(normalized.get("evidence_source"))
    normalized["evidence_source"] = evidence_source
    normalized.setdefault("not_real_acceptance_evidence", True)
    normalized.setdefault("acceptance_level", "offline_regression")
    normalized.setdefault("promotion_state", "dry_run_only")
    normalized.setdefault("evidence_state", "simulated_workbench")
    hydrated_safety = hydrate_step2_config_safety_summary(config_safety or normalized.get("config_safety") or {})
    hydrated_review = build_step2_config_safety_review(hydrated_safety)
    review_overrides = dict(config_safety_review or normalized.get("config_safety_review") or {})
    if review_overrides:
        review_payload = dict(hydrated_safety)
        review_payload.update(copy.deepcopy(review_overrides))
        if "inventory" not in review_payload and hydrated_safety.get("inventory") is not None:
            review_payload["inventory"] = copy.deepcopy(hydrated_safety.get("inventory"))
        if "execution_gate" not in review_payload and hydrated_safety.get("execution_gate") is not None:
            review_payload["execution_gate"] = copy.deepcopy(hydrated_safety.get("execution_gate"))
        hydrated_review = build_step2_config_safety_review(review_payload)
    normalized["config_safety"] = hydrated_safety
    normalized["config_safety_review"] = hydrated_review
    config_governance_handoff = build_step2_config_governance_handoff(hydrated_review)
    normalized["config_governance_handoff"] = config_governance_handoff
    normalized.setdefault("config_classification", str(config_governance_handoff.get("classification") or ""))
    normalized.setdefault("config_badge_ids", list(config_governance_handoff.get("badge_ids") or []))
    normalized.setdefault("config_inventory_summary", str(config_governance_handoff.get("inventory_summary") or "--"))
    normalized.setdefault(
        "blocked_reasons",
        [str(item).strip() for item in list(config_governance_handoff.get("blocked_reasons") or []) if str(item).strip()],
    )
    normalized.setdefault(
        "blocked_reason_details",
        [dict(item) for item in list(config_governance_handoff.get("blocked_reason_details") or []) if isinstance(item, dict)],
    )
    normalized.setdefault(
        "devices_with_real_ports",
        [dict(item) for item in list(config_governance_handoff.get("devices_with_real_ports") or []) if isinstance(item, dict)],
    )
    normalized.setdefault(
        "enabled_engineering_flags",
        [dict(item) for item in list(config_governance_handoff.get("enabled_engineering_flags") or []) if isinstance(item, dict)],
    )
    normalized.setdefault("execution_gate", dict(config_governance_handoff.get("execution_gate") or {}))
    qc_review_summary = dict(normalized.get("qc_review_summary") or {})
    if qc_review_summary:
        qc_evidence_source = _normalize_simulated_evidence_source(
            qc_review_summary.get("evidence_source"),
            default=evidence_source,
        )
        qc_review_summary["evidence_source"] = qc_evidence_source
        qc_review_summary.setdefault("not_real_acceptance_evidence", True)
        qc_review_summary.setdefault("acceptance_level", str(normalized.get("acceptance_level") or "offline_regression"))
        qc_review_summary.setdefault("promotion_state", str(normalized.get("promotion_state") or "dry_run_only"))
        qc_review_summary.setdefault("evidence_state", str(normalized.get("evidence_state") or "simulated_workbench"))
        reviewer_card = dict(qc_review_summary.get("reviewer_card") or {})
        if not reviewer_card or not list(reviewer_card.get("lines") or []):
            reviewer_card = build_qc_reviewer_card(
                reviewer_digest=dict(qc_review_summary.get("reviewer_digest") or {}),
                run_gate=dict(qc_review_summary.get("run_gate") or {}),
                point_gate_summary=dict(qc_review_summary.get("point_gate_summary") or {}),
                decision_counts=dict(qc_review_summary.get("decision_counts") or {}),
                route_decision_breakdown=dict(qc_review_summary.get("route_decision_breakdown") or {}),
                reject_reason_taxonomy=list(qc_review_summary.get("reject_reason_taxonomy") or []),
                failed_check_taxonomy=list(qc_review_summary.get("failed_check_taxonomy") or []),
            )
        qc_review_summary["reviewer_card"] = reviewer_card
        qc_review_summary.setdefault(
            "review_sections",
            [dict(item) for item in list(reviewer_card.get("sections") or []) if isinstance(item, dict)],
        )
        qc_review_summary.setdefault(
            "evidence_section",
            {
                "title": "质控摘要",
                "summary": str(reviewer_card.get("summary") or qc_review_summary.get("summary") or "").strip(),
                "lines": [str(item).strip() for item in list(reviewer_card.get("lines") or []) if str(item).strip()],
                "sections": [dict(item) for item in list(reviewer_card.get("sections") or []) if isinstance(item, dict)],
            },
        )
        normalized["qc_review_summary"] = qc_review_summary
        normalized.setdefault("qc_reviewer_card", dict(reviewer_card))
        normalized.setdefault("qc_evidence_section", dict(qc_review_summary.get("evidence_section") or {}))
        qc_evidence_section = build_qc_evidence_section(
            reviewer_digest=dict(qc_review_summary.get("reviewer_digest") or {}),
            reviewer_card=reviewer_card,
            run_gate=dict(qc_review_summary.get("run_gate") or {}),
            point_gate_summary=dict(qc_review_summary.get("point_gate_summary") or {}),
            decision_counts=dict(qc_review_summary.get("decision_counts") or {}),
            route_decision_breakdown=dict(qc_review_summary.get("route_decision_breakdown") or {}),
            reject_reason_taxonomy=list(qc_review_summary.get("reject_reason_taxonomy") or []),
            failed_check_taxonomy=list(qc_review_summary.get("failed_check_taxonomy") or []),
            review_sections=list(qc_review_summary.get("review_sections") or reviewer_card.get("sections") or []),
            summary_override=str(qc_review_summary.get("summary") or reviewer_card.get("summary") or "").strip(),
            lines_override=[
                str(item).strip()
                for item in list(qc_review_summary.get("review_card_lines") or qc_review_summary.get("lines") or reviewer_card.get("lines") or [])
                if str(item).strip()
            ],
            evidence_source=qc_evidence_source,
            evidence_state=str(qc_review_summary.get("evidence_state") or normalized.get("evidence_state") or "simulated_workbench"),
            not_real_acceptance_evidence=bool(
                qc_review_summary.get("not_real_acceptance_evidence", normalized.get("not_real_acceptance_evidence", True))
            ),
            acceptance_level=str(qc_review_summary.get("acceptance_level") or normalized.get("acceptance_level") or "offline_regression"),
            promotion_state=str(qc_review_summary.get("promotion_state") or normalized.get("promotion_state") or "dry_run_only"),
        )
        qc_review_summary["reviewer_card"] = dict(qc_evidence_section.get("reviewer_card") or reviewer_card)
        qc_review_summary["review_sections"] = [
            dict(item) for item in list(qc_evidence_section.get("sections") or []) if isinstance(item, dict)
        ]
        qc_review_summary["review_card_lines"] = [
            str(item).strip() for item in list(qc_evidence_section.get("review_card_lines") or []) if str(item).strip()
        ]
        qc_review_summary["evidence_section"] = dict(qc_evidence_section)
        qc_review_summary["cards"] = [
            dict(item) for item in list(qc_evidence_section.get("cards") or []) if isinstance(item, dict)
        ]
        normalized["qc_review_summary"] = qc_review_summary
        normalized["qc_reviewer_card"] = dict(qc_evidence_section.get("reviewer_card") or reviewer_card)
        normalized["qc_evidence_section"] = dict(qc_evidence_section)
        normalized["qc_review_cards"] = [
            dict(item) for item in list(qc_evidence_section.get("cards") or []) if isinstance(item, dict)
        ]
    return normalized


class AppFacade:
    """Thin UI-safe facade over the V2 backend surfaces."""

    def __init__(
        self,
        config: Optional[AppConfig] = None,
        *,
        simulation: Optional[bool] = None,
        service: Optional[CalibrationService] = None,
        runtime_paths: Optional[RuntimePaths] = None,
        preferences_store: Optional[PreferencesStore] = None,
        recent_runs_store: Optional[RecentRunsStore] = None,
    ) -> None:
        if service is not None:
            self.service = service
            self.config = self._prepare_config(getattr(service, "config", None), simulation)
        else:
            self.config = self._prepare_config(config, simulation)
            from ...core.calibration_service import CalibrationService

            self.service = CalibrationService(self.config)
        if getattr(self.service, "_raw_cfg", None) is None:
            self.service._raw_cfg = copy.deepcopy(getattr(self.config, "_raw_cfg", None))

        self.session = self.service.session
        self.event_bus = self.service.event_bus
        self.result_store = self.service.result_store
        self.results_gateway = ResultsGateway(
            self.result_store.run_dir,
            output_files_provider=self.service.get_output_files,
        )
        self.runtime_paths = (runtime_paths or RuntimePaths.default()).ensure_dirs()
        self.preferences_store = preferences_store or PreferencesStore(self.runtime_paths.preferences_path)
        self.recent_runs_store = recent_runs_store or RecentRunsStore(self.runtime_paths.recent_runs_path)
        self.profile_store = ProfileStore(self.runtime_paths.plan_profiles_dir)
        self.plan_gateway = PlanGateway(
            profile_store=self.profile_store,
            config_provider=lambda: self.config,
            compiled_points_dir=self.runtime_paths.cache_dir / "compiled_plan_runs",
        )
        self.device_workbench = DeviceWorkbenchController(self)
        self.app_info = APP_INFO
        self._default_route_mode = str(getattr(self.config.workflow, "route_mode", "h2o_then_co2") or "h2o_then_co2")
        self._lock = threading.RLock()
        self._logs: deque[str] = deque(maxlen=400)
        self._timeseries_history: dict[str, deque[float]] = {
            "temperature_c": deque(maxlen=60),
            "pressure_hpa": deque(maxlen=60),
            "co2_signal": deque(maxlen=60),
        }
        self._last_export_result: dict[str, Any] = {
            "available_formats": ["json", "csv", "all"],
            "last_export_message": t("common.ready"),
            "last_export_dir": "",
            "last_export_batch_id": "",
            "last_export_files": [],
        }
        self._ui_error_message = ""
        self._busy_message = ""
        self._notifications: deque[dict[str, str]] = deque(maxlen=40)
        self._subscriptions: list[tuple[EventType, Any]] = []
        self._review_center_roots_cache: dict[tuple[Any, ...], list[str]] = {}
        self._review_artifact_paths_cache: dict[tuple[Any, ...], list[str]] = {}

        self.service.set_log_callback(self._on_log)
        for event_type in EventType:
            handler = self._make_event_handler(event_type)
            self.event_bus.subscribe(event_type, handler)
            self._subscriptions.append((event_type, handler))
        self._append_log(t("facade.ui_attached"))
        self._append_notification("info", t("facade.ui_attached"))
        self.add_recent_run(str(self.result_store.run_dir))

    @classmethod
    def from_config_path(
        cls,
        config_path: Optional[str] = None,
        *,
        simulation: bool = False,
        allow_unsafe_step2_config: bool = False,
    ) -> "AppFacade":
        if config_path:
            from ...entry import create_calibration_service_from_config, load_config_bundle

            _, raw_cfg, config = load_config_bundle(
                config_path,
                simulation_mode=simulation,
                allow_unsafe_step2_config=allow_unsafe_step2_config,
                enforce_step2_execution_gate=True,
            )
            service = create_calibration_service_from_config(
                config,
                raw_cfg=raw_cfg,
                preload_points=False,
            )
            return cls(service=service, simulation=simulation)
        config = AppConfig.from_dict({})
        return cls(config=config, simulation=simulation)

    @staticmethod
    def _prepare_config(config: Optional[AppConfig], simulation: Optional[bool]) -> AppConfig:
        prepared = copy.deepcopy(config) if config is not None else AppConfig.from_dict({})
        raw_cfg = copy.deepcopy(getattr(config, "_raw_cfg", None))
        if not isinstance(raw_cfg, dict):
            raw_cfg = {}

        paths_payload = raw_cfg.setdefault("paths", {})
        for key in ("points_excel", "output_dir", "logs_dir"):
            raw_value = str(getattr(prepared.paths, key, "") or "").strip()
            if not raw_value:
                continue
            resolved_path = Path(raw_value).expanduser()
            if not resolved_path.is_absolute():
                resolved_path = resolved_path.resolve()
            resolved_text = str(resolved_path)
            setattr(prepared.paths, key, resolved_text)
            paths_payload[key] = resolved_text

        if simulation is not None:
            prepared.features.simulation_mode = bool(simulation)
        prepared.features.use_v2 = True
        raw_cfg.setdefault("features", {})["simulation_mode"] = bool(prepared.features.simulation_mode)
        raw_cfg.setdefault("features", {})["use_v2"] = True
        setattr(prepared, "_raw_cfg", raw_cfg)
        return prepared

    def shutdown(self) -> None:
        for event_type, handler in self._subscriptions:
            self.event_bus.unsubscribe(event_type, handler)
        self._subscriptions.clear()

    def start(
        self,
        points_path: Optional[str] = None,
        *,
        points_source: str = "use_points_file",
        run_mode: Optional[str] = None,
    ) -> tuple[bool, str]:
        try:
            run_input = self._resolve_run_input(points_path=points_path, points_source=points_source)
            requested_mode = run_input.get("run_mode") if run_input.get("source") == "use_default_profile" else (run_mode or run_input.get("run_mode") or self.get_run_mode())
            self._apply_run_mode(
                requested_mode,
                route_mode=run_input.get("route_mode"),
                formal_calibration_report=run_input.get("formal_calibration_report"),
            )
            self._apply_profile_runtime_metadata(
                profile_name=run_input.get("profile_name"),
                profile_version=run_input.get("profile_version"),
                report_family=run_input.get("report_family"),
                report_templates=run_input.get("report_templates"),
            )
            self._apply_analyzer_setup(run_input.get("analyzer_setup"))
            self.service.start(points_path=str(run_input["path"]))
        except Exception as exc:
            message = t("facade.start_failed", error=exc)
            self.log_ui(message)
            return False, message
        message = t(
            "facade.start_requested",
            source_label=run_input["source_label"],
            display_path=run_input["display_path"],
            mode=self.get_run_mode(),
        )
        self.log_ui(message)
        return True, message

    def preview_points(
        self,
        points_path: Optional[str] = None,
        *,
        points_source: str = "use_points_file",
        run_mode: Optional[str] = None,
    ) -> dict[str, Any]:
        source = self._normalize_points_source(points_source)
        if source == "use_default_profile":
            try:
                preview = dict(self.plan_gateway.compile_default_profile_preview())
            except Exception as exc:
                message = t("facade.preview_failed", error=exc)
                self.log_ui(message)
                return {
                    "ok": False,
                    "path": "default_profile",
                    "summary": message,
                    "rows": [],
                }
            preview["path"] = f"default_profile:{preview.get('profile_name', '--')}"
            preview["summary"] = t(
                "facade.default_profile_summary",
                profile=preview.get("profile_name", "--"),
                summary=self._humanize_ui_summary(str(preview.get("summary", "") or "")),
            )
            return preview
        preview_mode = ModeProfile.from_value(run_mode or self.get_run_mode())
        preview_config = self._config_for_mode(preview_mode)
        try:
            from ...core.route_planner import RoutePlanner

            resolved_path = self._resolve_points_path(points_path)
            parser = self._build_preview_parser()
            planner = RoutePlanner(preview_config, parser)
            points = self._load_preview_points(
                resolved_path,
                point_parser=parser,
                route_planner=planner,
                config=preview_config,
            )
            preview_points = self._preview_points_in_execution_order(points, route_planner=planner)
        except Exception as exc:
            message = t("facade.preview_failed", error=exc)
            self.log_ui(message)
            return {
                "ok": False,
                "path": str(points_path or self.config.paths.points_excel),
                "summary": message,
                "rows": [],
            }

        rows = [self._preview_row(index, point) for index, point in enumerate(preview_points, start=1)]
        return {
            "ok": True,
            "path": str(resolved_path),
            "run_mode": preview_mode.run_mode.value,
            "route_mode": str(getattr(preview_config.workflow, "route_mode", "h2o_then_co2") or "h2o_then_co2"),
            "formal_calibration_report": preview_mode.formal_report_enabled(),
            "summary": t("facade.points_preview_summary", point_count=len(points), row_count=len(rows)),
            "rows": rows,
        }

    def edit_points_file(
        self,
        points_path: Optional[str] = None,
        *,
        points_source: str = "use_points_file",
    ) -> tuple[bool, str]:
        source = self._normalize_points_source(points_source)
        if source == "use_default_profile":
            message = t("facade.default_profile_no_points")
            self.log_ui(message)
            return False, message
        try:
            resolved_path = self._resolve_points_path(points_path)
            self._open_path_with_system_editor(resolved_path)
        except Exception as exc:
            message = t("facade.edit_points_failed", error=exc)
            self.log_ui(message)
            return False, message

        message = t("facade.editing_points_file", path=resolved_path)
        self.log_ui(message)
        return True, message

    def stop(self) -> tuple[bool, str]:
        try:
            self.service.stop(wait=False)
        except Exception as exc:
            message = t("facade.stop_failed", error=exc)
            self.log_ui(message)
            return False, message
        message = t("facade.stop_requested")
        self.log_ui(message)
        return True, message

    def pause(self) -> tuple[bool, str]:
        try:
            self.service.pause()
        except Exception as exc:
            message = t("facade.pause_failed", error=exc)
            self.log_ui(message)
            return False, message
        message = t("facade.pause_requested")
        self.log_ui(message)
        return True, message

    def resume(self) -> tuple[bool, str]:
        try:
            self.service.resume()
        except Exception as exc:
            message = t("facade.resume_failed", error=exc)
            self.log_ui(message)
            return False, message
        message = t("facade.resume_requested")
        self.log_ui(message)
        return True, message

    def build_snapshot(self) -> dict[str, Any]:
        run = self.build_run_snapshot()
        qc = self.build_qc_snapshot()
        results = self.build_results_snapshot()
        devices = self.get_devices_snapshot(run_snapshot=run)
        algorithms = self.get_algorithms_snapshot(results_snapshot=results)
        reports = self.get_reports_snapshot(results_snapshot=results)
        timeseries = self.get_timeseries_snapshot(run_snapshot=run)
        qc_overview = self.get_qc_overview_snapshot(qc_snapshot=qc)
        winner = self.get_winner_snapshot(algorithms_snapshot=algorithms)
        export = self.get_export_snapshot(reports_snapshot=reports)
        route_progress = self.get_route_progress_snapshot(run_snapshot=run)
        reject_reasons_chart = self.get_qc_reject_reason_snapshot(qc_snapshot=qc)
        residuals = self.get_residual_snapshot(results_snapshot=results, qc_snapshot=qc)
        analyzer_health = self.get_analyzer_health_snapshot(devices_snapshot=devices, run_snapshot=run)
        validation = self.get_validation_snapshot()
        run["timeseries"] = timeseries
        run["route_progress"] = route_progress
        run["validation"] = validation
        qc["overview"] = qc_overview
        qc["reject_reasons_chart"] = reject_reasons_chart
        algorithms["winner"] = winner
        reports["export"] = export
        results["residuals"] = residuals
        devices["analyzer_health"] = analyzer_health
        return {
            "run": run,
            "qc": qc,
            "results": results,
            "devices": devices,
            "algorithms": algorithms,
            "reports": reports,
            "timeseries": timeseries,
            "qc_overview": qc_overview,
            "winner": winner,
            "export": export,
            "route_progress": route_progress,
            "reject_reasons_chart": reject_reasons_chart,
            "residuals": residuals,
            "analyzer_health": analyzer_health,
            "validation": validation,
            "error": self.get_error_snapshot(),
            "busy": self.get_busy_snapshot(),
            "notifications": self.get_notification_snapshot(),
            "logs": self.get_recent_logs(),
        }

    def build_run_snapshot(self) -> dict[str, Any]:
        status = self.service.get_status()
        session = self.session
        route_context = getattr(self.service.orchestrator, "route_context", None)
        run_state = getattr(self.service.orchestrator, "run_state", None)
        route_state = {}
        if route_context is not None:
            route_state = {
                key: self._route_value_to_text(value)
                for key, value in dict(getattr(route_context, "route_state", {}) or {}).items()
            }

        device_rows: list[dict[str, str]] = []
        device_info_map = {}
        list_device_info = getattr(self.service.device_manager, "list_device_info", None)
        if callable(list_device_info):
            try:
                loaded = list_device_info()
                if isinstance(loaded, dict):
                    device_info_map = dict(loaded)
            except Exception:
                device_info_map = {}
        if not device_info_map:
            private_map = getattr(self.service.device_manager, "_device_info", None)
            if isinstance(private_map, dict):
                device_info_map = dict(private_map)
        if device_info_map:
            for name, info in sorted(device_info_map.items()):
                status_text = str(getattr(getattr(info, "status", None), "value", "unknown") or "unknown")
                if status_text == "disabled":
                    status_text = "skipped_by_profile"
                device_rows.append(
                    {
                        "name": name,
                        "status": status_text,
                        "status_display": display_device_status(status_text),
                        "port": "" if info is None else str(getattr(info, "port", "") or ""),
                    }
                )
        else:
            for name in sorted(session.enabled_devices):
                info = self.service.device_manager.get_info(name)
                device_rows.append(
                    {
                        "name": name,
                        "status": getattr(getattr(info, "status", None), "value", "unknown"),
                        "status_display": display_device_status(getattr(getattr(info, "status", None), "value", "unknown")),
                        "port": "" if info is None else str(getattr(info, "port", "") or ""),
                    }
                )

        disabled_analyzers = []
        if run_state is not None:
            disabled_analyzers = sorted(getattr(run_state.analyzers, "disabled", set()) or [])
        profile_skipped_devices = sorted(
            str(item.get("name") or "")
            for item in device_rows
            if str(item.get("status") or "").strip().lower() == "skipped_by_profile"
        )

        return {
            "run_id": session.run_id,
            "phase": status.phase.value,
            "phase_display": display_phase(status.phase.value),
            "run_mode": self.get_run_mode(),
            "formal_calibration_report": self.formal_calibration_report_enabled(),
            "message": status.message,
            "message_display": self._humanize_ui_summary(str(status.message or "")) or str(status.message or ""),
            "progress": float(status.progress),
            "progress_pct": round(float(status.progress) * 100.0, 1),
            "elapsed_s": float(status.elapsed_s),
            "current_point": self._point_to_text(status.current_point),
            "current_point_index": None if status.current_point is None else status.current_point.index,
            "route": "" if route_context is None else str(getattr(route_context, "current_route", "") or ""),
            "route_display": display_route("" if route_context is None else str(getattr(route_context, "current_route", "") or "")),
            "route_phase": "" if route_context is None or getattr(route_context, "current_phase", None) is None else route_context.current_phase.value,
            "source_point": self._point_to_text(None if route_context is None else getattr(route_context, "source_point", None)),
            "active_point": self._point_to_text(None if route_context is None else getattr(route_context, "active_point", None)),
            "point_tag": "" if route_context is None else str(getattr(route_context, "point_tag", "") or ""),
            "retry": 0 if route_context is None else int(getattr(route_context, "retry", 0) or 0),
            "route_state": route_state,
            "device_rows": device_rows,
            "disabled_analyzers": disabled_analyzers,
            "profile_skipped_devices": profile_skipped_devices,
            "enabled_devices": sorted(session.enabled_devices),
            "warnings": list(session.warnings),
            "errors": list(session.errors),
            "output_dir": str(session.output_dir),
            "is_running": bool(self.service.is_running),
            "points_total": int(status.total_points),
            "points_completed": int(status.completed_points),
        }

    def build_qc_snapshot(self) -> dict[str, Any]:
        qc_state = getattr(getattr(self.service.orchestrator, "run_state", None), "qc", None)
        point_rows: list[dict[str, Any]] = []
        overall_score = 0.0
        grade = "--"
        recommendations: list[str] = []
        decision_counts: dict[str, int] = {}
        run_gate: dict[str, Any] = {}
        point_gate_summary: dict[str, Any] = {}
        route_decision_breakdown: dict[str, dict[str, int]] = {}
        reject_reason_taxonomy: list[dict[str, Any]] = []
        failed_check_taxonomy: list[dict[str, Any]] = []
        rule_profile: dict[str, Any] = {}
        threshold_profile: dict[str, Any] = {}
        reviewer_digest: dict[str, Any] = {}
        evidence_boundary: dict[str, Any] = {}

        if qc_state is not None:
            report = getattr(qc_state, "qc_report", None)
            run_score = getattr(qc_state, "run_quality_score", None)
            if report is not None:
                point_rows = [dict(item) for item in list(getattr(report, "point_details", []) or [])]
                overall_score = float(getattr(report, "overall_score", 0.0) or 0.0)
                grade = str(getattr(report, "grade", "--") or "--")
                recommendations = [str(item) for item in list(getattr(report, "recommendations", []) or [])]
                decision_counts = dict(getattr(report, "decision_counts", {}) or {})
                run_gate = dict(getattr(report, "run_gate", {}) or {})
                point_gate_summary = dict(getattr(report, "point_gate_summary", {}) or {})
                route_decision_breakdown = dict(getattr(report, "route_decision_breakdown", {}) or {})
                reject_reason_taxonomy = [dict(item) for item in list(getattr(report, "reject_reason_taxonomy", []) or [])]
                failed_check_taxonomy = [dict(item) for item in list(getattr(report, "failed_check_taxonomy", []) or [])]
                rule_profile = dict(getattr(report, "rule_profile", {}) or {})
                threshold_profile = dict(getattr(report, "threshold_profile", {}) or {})
                reviewer_digest = dict(getattr(report, "reviewer_digest", {}) or {})
                evidence_boundary = dict(getattr(report, "evidence_boundary", {}) or {})
            else:
                validations = list(getattr(qc_state, "point_validations", []) or [])
                point_rows = [self._validation_to_row(item) for item in validations]
                if run_score is not None:
                    overall_score = float(getattr(run_score, "overall_score", 0.0) or 0.0)
                    grade = str(getattr(run_score, "grade", "--") or "--")
                    recommendations = [str(item) for item in list(getattr(run_score, "recommendations", []) or [])]

        valid_points = sum(1 for item in point_rows if bool(item.get("valid", False)))
        invalid_points = max(0, len(point_rows) - valid_points)
        invalid_reasons = [str(item.get("reason", "") or "") for item in point_rows if not bool(item.get("valid", False))]
        if not decision_counts:
            decision_counts = {"pass": valid_points, "warn": 0, "reject": invalid_points, "skipped": 0}
        if not run_gate:
            run_gate = {
                "status": "pass" if invalid_points == 0 and point_rows else ("reject" if invalid_points else "skipped"),
                "reason": "legacy_qc_snapshot",
            }
        if not reject_reason_taxonomy:
            reject_reason_taxonomy = [
                {"code": reason, "category": "other", "count": count}
                for reason, count in sorted(
                    {reason: invalid_reasons.count(reason) for reason in invalid_reasons if reason}.items(),
                    key=lambda item: (-item[1], item[0]),
                )
            ]
        if not reviewer_digest:
            summary_payload = build_qc_review_payload(
                point_rows=point_rows,
                run_id=self.session.run_id,
                overall_score=overall_score if point_rows else None,
                grade=grade,
                recommendations=recommendations,
            )
            if not decision_counts:
                decision_counts = dict(summary_payload.get("decision_counts", {}) or {})
            if not run_gate:
                run_gate = dict(summary_payload.get("run_gate", {}) or {})
            point_gate_summary = dict(point_gate_summary or summary_payload.get("point_gate_summary", {}) or {})
            route_decision_breakdown = dict(route_decision_breakdown or summary_payload.get("route_decision_breakdown", {}) or {})
            if not reject_reason_taxonomy:
                reject_reason_taxonomy = [dict(item) for item in list(summary_payload.get("reject_reason_taxonomy", []) or [])]
            failed_check_taxonomy = [dict(item) for item in list(failed_check_taxonomy or summary_payload.get("failed_check_taxonomy", []) or [])]
            reviewer_digest = dict(summary_payload.get("reviewer_digest", {}) or {})

        return {
            "overall_score": overall_score,
            "grade": grade,
            "total_points": len(point_rows),
            "valid_points": valid_points,
            "invalid_points": invalid_points,
            "point_rows": point_rows,
            "recommendations": recommendations,
            "invalid_reasons": [item for item in invalid_reasons if item],
            "decision_counts": decision_counts,
            "run_gate": run_gate,
            "point_gate_summary": point_gate_summary,
            "route_decision_breakdown": route_decision_breakdown,
            "reject_reason_taxonomy": reject_reason_taxonomy,
            "failed_check_taxonomy": failed_check_taxonomy,
            "rule_profile": rule_profile,
            "threshold_profile": threshold_profile,
            "reviewer_digest": reviewer_digest,
            "evidence_boundary": evidence_boundary,
        }

    def _config_safety_snapshot(
        self,
        summary: dict[str, Any] | None,
        *,
        config_safety_override: dict[str, Any] | None = None,
        config_safety_review_override: dict[str, Any] | None = None,
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        summary_payload = dict(summary or {}) if isinstance(summary, dict) else {}
        summary_stats = dict(summary_payload.get("stats", {}) or {})
        source_payload = dict(
            config_safety_override
            or summary_payload.get("config_safety")
            or summary_stats.get("config_safety")
            or getattr(self.config, "_config_safety", {})
            or {}
        )
        if source_payload:
            config_safety = hydrate_step2_config_safety_summary(source_payload)
        else:
            config_safety = summarize_step2_config_safety(self.config)
        review_overrides = dict(
            config_safety_review_override
            or summary_payload.get("config_safety_review")
            or summary_stats.get("config_safety_review")
            or {}
        )
        if review_overrides:
            review_payload = copy.deepcopy(config_safety)
            review_payload.update(copy.deepcopy(review_overrides))
            if "inventory" not in review_payload and config_safety.get("inventory") is not None:
                review_payload["inventory"] = copy.deepcopy(config_safety.get("inventory"))
            if "execution_gate" not in review_payload and config_safety.get("execution_gate") is not None:
                review_payload["execution_gate"] = copy.deepcopy(config_safety.get("execution_gate"))
            config_safety_review = build_step2_config_safety_review(review_payload)
        else:
            config_safety_review = build_step2_config_safety_review(config_safety)
        return config_safety, config_safety_review

    @staticmethod
    def _read_config_safety_section(payload: dict[str, Any] | None, key: str) -> dict[str, Any]:
        if not isinstance(payload, dict):
            return {}
        direct = payload.get(key)
        if isinstance(direct, dict):
            return dict(direct)
        stats = payload.get("stats")
        if not isinstance(stats, dict):
            return {}
        legacy = stats.get(key)
        return dict(legacy) if isinstance(legacy, dict) else {}

    def _artifact_config_safety_snapshot(self, run_dir: Path) -> tuple[dict[str, Any], dict[str, Any]]:
        summary = self._load_json_dict(run_dir / "summary.json")
        evidence_registry = self._load_json_dict(run_dir / "evidence_registry.json")
        analytics_summary = self._load_json_dict(run_dir / "analytics_summary.json")
        workbench_action_report = self._load_json_dict(run_dir / "workbench_action_report.json")
        workbench_action_snapshot = self._load_json_dict(run_dir / "workbench_action_snapshot.json")
        config_safety = {}
        config_safety_review = {}
        for payload in (summary, evidence_registry, analytics_summary, workbench_action_report, workbench_action_snapshot):
            if not config_safety:
                config_safety = self._read_config_safety_section(payload, "config_safety")
            if not config_safety_review:
                config_safety_review = self._read_config_safety_section(payload, "config_safety_review")
            if config_safety and config_safety_review:
                break
        return self._config_safety_snapshot(
            summary,
            config_safety_override=config_safety,
            config_safety_review_override=config_safety_review,
        )

    @staticmethod
    def _config_safety_detail_lines(
        config_safety: dict[str, Any],
        config_safety_review: dict[str, Any],
    ) -> list[str]:
        execution_gate = dict(config_safety_review.get("execution_gate") or {})
        inventory = dict(config_safety_review.get("inventory") or config_safety.get("inventory") or {})
        lines = [
            f"配置安全: {str(config_safety_review.get('summary') or config_safety.get('summary') or '--')}",
            f"库存分类: {str(config_safety.get('classification_display') or config_safety.get('classification') or '--')}",
            f"库存摘要: {str(config_safety_review.get('inventory_summary') or config_safety.get('inventory_summary') or '--')}",
        ]
        if execution_gate:
            lines.append(
                "默认工作流闸门: "
                + str(execution_gate.get("status") or "--")
                + " | "
                + str(execution_gate.get("summary") or "--")
            )
        if inventory:
            lines.append(
                "库存治理: "
                f"已启用设备 {int(inventory.get('enabled_device_count', 0) or 0)} 台 / "
                f"real-COM {int(inventory.get('real_port_device_count', 0) or 0)} 台 / "
                f"engineering-only {int(inventory.get('engineering_only_flag_count', 0) or 0)} 项"
            )
        blocked_reason_details = list(config_safety_review.get("blocked_reason_details") or [])
        if blocked_reason_details:
            lines.extend(
                f"阻断原因: {str(item.get('title') or '--')} | {str(item.get('summary') or '--')}"
                for item in blocked_reason_details[:2]
            )
        real_ports = [
            dict(item)
            for item in list(config_safety_review.get("devices_with_real_ports") or config_safety.get("devices_with_real_ports") or [])
            if isinstance(item, dict)
        ]
        if real_ports:
            lines.append(
                "real-COM 风险设备: "
                + ", ".join(f"{item.get('device', '--')}={item.get('port', '--')}" for item in real_ports[:4])
            )
        engineering_flags = [
            dict(item)
            for item in list(
                config_safety_review.get("enabled_engineering_flags")
                or config_safety.get("enabled_engineering_flags")
                or []
            )
            if isinstance(item, dict)
        ]
        if engineering_flags:
            lines.append(
                "engineering-only 开关: "
                + ", ".join(str(item.get("config_path") or "--") for item in engineering_flags[:4])
            )
        warnings = [str(item).strip() for item in list(config_safety_review.get("warnings") or []) if str(item).strip()]
        if warnings:
            lines.extend(f"治理提醒: {warning}" for warning in warnings[:2])
        badges = [str(item.get("label") or "--") for item in list(config_safety.get("badges") or []) if str(item.get("label") or "").strip()]
        if badges:
            lines.append("治理标记: " + " / ".join(badges))
        return [str(item).strip() for item in lines if str(item).strip()]

    @staticmethod
    def _merge_detail_lines(*groups: Any, limit: int | None = None) -> list[str]:
        merged: list[str] = []
        for group in groups:
            if isinstance(group, str):
                candidates = [group]
            elif isinstance(group, (list, tuple)):
                candidates = list(group)
            else:
                candidates = []
            for item in candidates:
                text = str(item or "").strip()
                if not text or text in merged:
                    continue
                merged.append(text)
                if limit is not None and len(merged) >= limit:
                    return merged
        return merged

    @staticmethod
    def _normalize_review_cards(value: Any) -> list[dict[str, Any]]:
        if isinstance(value, dict):
            raw_cards = value.get("cards") or value.get("review_cards") or []
        elif isinstance(value, (list, tuple)):
            raw_cards = value
        else:
            raw_cards = []
        normalized: list[dict[str, Any]] = []
        for index, item in enumerate(list(raw_cards or []), start=1):
            if not isinstance(item, dict):
                continue
            title = str(item.get("title") or "").strip()
            lines = [str(line).strip() for line in list(item.get("lines") or []) if str(line).strip()]
            summary = str(item.get("summary") or "").strip()
            if not summary and lines:
                summary = lines[0]
            if not title and not summary and not lines:
                continue
            normalized.append(
                {
                    "id": str(item.get("id") or f"card_{index}").strip() or f"card_{index}",
                    "title": title or t("pages.qc.reviewer_card_title", default="审阅卡片"),
                    "summary": summary or (lines[0] if lines else t("common.none")),
                    "lines": lines,
                }
            )
        return normalized

    @classmethod
    def _review_card_lines(cls, value: Any) -> list[str]:
        lines: list[str] = []
        for card in cls._normalize_review_cards(value):
            title = str(card.get("title") or "").strip()
            summary = str(card.get("summary") or "").strip()
            if title and summary:
                lines.append(f"{title}: {summary}")
            elif title:
                lines.append(title)
            elif summary:
                lines.append(summary)
        return lines

    def _build_results_qc_evidence_section(
        self,
        *,
        analytics_summary: dict[str, Any],
        workbench_evidence_summary: dict[str, Any],
    ) -> dict[str, Any]:
        analytics_qc_section = dict(analytics_summary.get("qc_evidence_section") or {})
        qc_overview = dict(analytics_summary.get("qc_overview") or {})
        unified_review_summary = dict(analytics_summary.get("unified_review_summary") or {})
        qc_summary = dict(unified_review_summary.get("qc_summary") or {})
        workbench_qc = dict(workbench_evidence_summary.get("qc_review_summary") or {})
        existing_evidence_section = dict(
            analytics_qc_section
            or qc_overview.get("evidence_section")
            or qc_summary.get("evidence_section")
            or workbench_qc.get("evidence_section")
            or workbench_evidence_summary.get("qc_evidence_section")
            or {}
        )
        reviewer_card = dict(
            analytics_summary.get("qc_reviewer_card")
            or analytics_qc_section.get("reviewer_card")
            or qc_overview.get("reviewer_card")
            or qc_summary.get("reviewer_card")
            or workbench_qc.get("reviewer_card")
            or existing_evidence_section.get("reviewer_card")
            or {}
        )
        if not reviewer_card or not list(reviewer_card.get("lines") or []):
            reviewer_card = build_qc_reviewer_card(
                reviewer_digest=dict(qc_overview.get("reviewer_digest") or workbench_qc.get("reviewer_digest") or {}),
                run_gate=dict(qc_overview.get("run_gate") or workbench_qc.get("run_gate") or {}),
                point_gate_summary=dict(qc_overview.get("point_gate_summary") or workbench_qc.get("point_gate_summary") or {}),
                decision_counts=dict(qc_overview.get("decision_counts") or workbench_qc.get("decision_counts") or {}),
                route_decision_breakdown=dict(
                    qc_overview.get("route_decision_breakdown") or workbench_qc.get("route_decision_breakdown") or {}
                ),
                reject_reason_taxonomy=list(
                    qc_overview.get("reject_reason_taxonomy") or workbench_qc.get("reject_reason_taxonomy") or []
                ),
                failed_check_taxonomy=list(
                    qc_overview.get("failed_check_taxonomy") or workbench_qc.get("failed_check_taxonomy") or []
                ),
            )
        lines = self._merge_detail_lines(
            self._review_card_lines(analytics_summary.get("qc_review_cards") or analytics_qc_section),
            self._review_card_lines(existing_evidence_section),
            self._build_qc_review_detail_lines(
                qc_overview=qc_overview,
                unified_review_summary=unified_review_summary,
            ),
        )
        workbench_qc = dict(workbench_evidence_summary.get("qc_review_summary") or {})
        workbench_qc_lines = self._merge_detail_lines(
            self._review_card_lines(
                workbench_evidence_summary.get("qc_evidence_section")
                or workbench_qc.get("evidence_section")
                or workbench_qc.get("cards")
                or []
            ),
            self._review_detail_lines(
                workbench_qc.get("review_card_lines") or workbench_qc.get("lines") or workbench_qc.get("summary")
            ),
            limit=3,
        )
        if workbench_qc_lines:
            workbench_summary_line = f"工作台质控摘要: {workbench_qc_lines[0]}"
            if workbench_summary_line not in lines:
                lines.append(workbench_summary_line)
        review_sections = [
            dict(item)
            for item in list(
                analytics_qc_section.get("review_sections")
                or qc_overview.get("review_sections")
                or qc_summary.get("review_sections")
                or workbench_qc.get("review_sections")
                or existing_evidence_section.get("sections")
                or reviewer_card.get("sections")
                or []
            )
            if isinstance(item, dict)
        ]
        summary_text = str(
            existing_evidence_section.get("summary")
            or reviewer_card.get("summary")
            or qc_summary.get("summary")
            or (lines[0] if lines else "")
        ).strip()
        evidence_source = _normalize_simulated_evidence_source(
            analytics_summary.get("evidence_source") or workbench_evidence_summary.get("evidence_source")
        )
        return build_qc_evidence_section(
            reviewer_digest=dict(qc_overview.get("reviewer_digest") or workbench_qc.get("reviewer_digest") or {}),
            reviewer_card=reviewer_card,
            run_gate=dict(qc_overview.get("run_gate") or workbench_qc.get("run_gate") or {}),
            point_gate_summary=dict(qc_overview.get("point_gate_summary") or workbench_qc.get("point_gate_summary") or {}),
            decision_counts=dict(qc_overview.get("decision_counts") or workbench_qc.get("decision_counts") or {}),
            route_decision_breakdown=dict(
                qc_overview.get("route_decision_breakdown") or workbench_qc.get("route_decision_breakdown") or {}
            ),
            reject_reason_taxonomy=list(
                qc_overview.get("reject_reason_taxonomy") or workbench_qc.get("reject_reason_taxonomy") or []
            ),
            failed_check_taxonomy=list(
                qc_overview.get("failed_check_taxonomy") or workbench_qc.get("failed_check_taxonomy") or []
            ),
            review_sections=review_sections,
            summary_override=summary_text,
            lines_override=[str(item).strip() for item in lines if str(item).strip()],
            evidence_source=evidence_source,
            evidence_state=str(workbench_evidence_summary.get("evidence_state") or analytics_summary.get("evidence_state") or "collected"),
            not_real_acceptance_evidence=bool(
                workbench_evidence_summary.get(
                    "not_real_acceptance_evidence",
                    analytics_summary.get("not_real_acceptance_evidence", True),
                )
            ),
            acceptance_level=str(workbench_evidence_summary.get("acceptance_level") or "offline_regression"),
            promotion_state=str(workbench_evidence_summary.get("promotion_state") or "dry_run_only"),
        )
        return {
            "title": "质控证据",
            "summary": summary_text,
            "lines": [str(item).strip() for item in lines if str(item).strip()],
            "reviewer_card": reviewer_card,
            "sections": review_sections,
        }

    def _build_results_qc_summary_text(
        self,
        *,
        analytics_summary: dict[str, Any],
        workbench_evidence_summary: dict[str, Any],
        config_safety: dict[str, Any],
        config_safety_review: dict[str, Any],
    ) -> str:
        qc_section = self._build_results_qc_evidence_section(
            analytics_summary=analytics_summary,
            workbench_evidence_summary=workbench_evidence_summary,
        )
        qc_lines = self._merge_detail_lines(
            self._review_card_lines(qc_section),
            list(qc_section.get("review_card_lines") or qc_section.get("lines") or []),
            limit=6,
        )
        workbench_qc = dict(workbench_evidence_summary.get("qc_review_summary") or {})
        workbench_qc_lines = self._merge_detail_lines(
            self._review_card_lines(
                workbench_evidence_summary.get("qc_evidence_section")
                or workbench_qc.get("evidence_section")
                or workbench_qc.get("cards")
                or []
            ),
            self._review_detail_lines(
                workbench_qc.get("review_card_lines") or workbench_qc.get("lines") or workbench_qc.get("summary")
            ),
            limit=2,
        )
        lines: list[str] = []
        if qc_lines:
            lines.extend(qc_lines[:6])
        if workbench_qc_lines:
            lines.append(f"工作台质控摘要: {workbench_qc_lines[0]}")
        if workbench_qc_lines:
            workbench_summary_line = f"工作台质控摘要: {workbench_qc_lines[0]}"
            if workbench_summary_line not in lines:
                lines.append(workbench_summary_line)
        for line in self._config_safety_detail_lines(config_safety, config_safety_review)[:4]:
            if line not in lines:
                lines.append(line)
        boundary_note = "证据边界: 仅供 simulation/offline/headless 审阅，不代表 real acceptance evidence。"
        if boundary_note not in lines:
            lines.append(boundary_note)
        return "\n".join(line for line in lines if line).strip()

    def build_results_snapshot(self) -> dict[str, Any]:
        payload = self.results_gateway.read_results_payload()
        run_dir = self.results_gateway.run_dir
        summary = payload["summary"]
        manifest = payload["manifest"]
        results = payload["results"]
        ai_summary = str(payload["ai_summary_text"] or "")
        output_files = list(payload["output_files"])
        reporting = dict(payload.get("reporting", {}) or {})
        artifact_exports = dict(payload.get("artifact_exports", {}) or {})
        artifact_role_summary = dict(payload.get("artifact_role_summary", {}) or {})
        acceptance_plan = dict(payload.get("acceptance_plan", {}) or {})
        analytics_summary = dict(payload.get("analytics_summary", {}) or {})
        spectral_quality_summary = dict(payload.get("spectral_quality_summary", {}) or {})
        trend_registry = dict(payload.get("trend_registry", {}) or {})
        lineage_summary = dict(payload.get("lineage_summary", {}) or {})
        evidence_registry = dict(payload.get("evidence_registry", {}) or {})
        coefficient_registry = dict(payload.get("coefficient_registry", {}) or {})
        suite_summary = dict(payload.get("suite_summary", {}) or {})
        suite_analytics_summary = dict(payload.get("suite_analytics_summary", {}) or {})
        suite_acceptance_plan = dict(payload.get("suite_acceptance_plan", {}) or {})
        workbench_action_report = dict(payload.get("workbench_action_report", {}) or {})
        workbench_action_snapshot = dict(payload.get("workbench_action_snapshot", {}) or {})
        offline_diagnostic_adapter_summary = dict(payload.get("offline_diagnostic_adapter_summary", {}) or {})
        point_taxonomy_summary = dict(payload.get("point_taxonomy_summary", {}) or {})
        multi_source_stability_evidence = dict(payload.get("multi_source_stability_evidence", {}) or {})
        state_transition_evidence = dict(payload.get("state_transition_evidence", {}) or {})
        simulation_evidence_sidecar_bundle = dict(payload.get("simulation_evidence_sidecar_bundle", {}) or {})
        measurement_phase_coverage_report = dict(payload.get("measurement_phase_coverage_report", {}) or {})
        scope_definition_pack = dict(payload.get("scope_definition_pack", {}) or {})
        decision_rule_profile = dict(payload.get("decision_rule_profile", {}) or {})
        reference_asset_registry = dict(payload.get("reference_asset_registry", {}) or {})
        certificate_lifecycle_summary = dict(payload.get("certificate_lifecycle_summary", {}) or {})
        scope_readiness_summary = dict(payload.get("scope_readiness_summary", {}) or {})
        certificate_readiness_summary = dict(payload.get("certificate_readiness_summary", {}) or {})
        pre_run_readiness_gate = dict(payload.get("pre_run_readiness_gate", {}) or {})
        uncertainty_model = dict(payload.get("uncertainty_model", {}) or {})
        uncertainty_input_set = dict(payload.get("uncertainty_input_set", {}) or {})
        sensitivity_coefficient_set = dict(payload.get("sensitivity_coefficient_set", {}) or {})
        budget_case = dict(payload.get("budget_case", {}) or {})
        uncertainty_golden_cases = dict(payload.get("uncertainty_golden_cases", {}) or {})
        uncertainty_report_pack = dict(payload.get("uncertainty_report_pack", {}) or {})
        uncertainty_digest = dict(payload.get("uncertainty_digest", {}) or {})
        uncertainty_rollup = dict(payload.get("uncertainty_rollup", {}) or {})
        uncertainty_method_readiness_summary = dict(payload.get("uncertainty_method_readiness_summary", {}) or {})
        audit_readiness_digest = dict(payload.get("audit_readiness_digest", {}) or {})
        run_artifact_index = dict(payload.get("run_artifact_index", {}) or {})
        artifact_contract_catalog = dict(payload.get("artifact_contract_catalog", {}) or {})
        compatibility_scan_summary = dict(payload.get("compatibility_scan_summary", {}) or {})
        compatibility_overview = dict(payload.get("compatibility_overview", {}) or {})
        compatibility_rollup = dict(payload.get("compatibility_rollup", {}) or {})
        recognition_scope_rollup = dict(payload.get("recognition_scope_rollup", {}) or {})
        reindex_manifest = dict(payload.get("reindex_manifest", {}) or {})

        sample_count = 0
        point_summary_count = 0
        if isinstance(results, dict):
            sample_count = len(list(results.get("samples", []) or []))
            point_summary_count = len(list(results.get("point_summaries", []) or []))

        summary_stats = {}
        if isinstance(summary, dict):
            summary_stats = dict(summary.get("stats", {}) or {})
        config_safety, config_safety_review = self._config_safety_snapshot(
            summary,
            config_safety_override=dict(payload.get("config_safety", {}) or {}),
            config_safety_review_override=dict(payload.get("config_safety_review", {}) or {}),
        )
        config_governance_handoff = build_step2_config_governance_handoff(config_safety_review)

        coefficient_files = [
            Path(item).name
            for item in output_files
            if "coefficient" in Path(item).name.lower() or Path(item).suffix.lower() in {".xlsx", ".csv"}
        ]
        ai_text = ai_summary or str(summary.get("ai_summary", "") if isinstance(summary, dict) else "")
        artifact_role_text = ", ".join(
            t(
                "facade.role_summary_item",
                name=display_artifact_role(name),
                count=int(dict(role_payload or {}).get("count", 0)),
            )
            for name, role_payload in sorted(artifact_role_summary.items())
        ) or "--"
        readiness_summary = dict(summary_stats.get("acceptance_readiness_summary") or acceptance_plan.get("readiness_summary") or {})
        analytics_digest = dict(summary_stats.get("analytics_summary_digest") or analytics_summary.get("digest") or {})
        lineage_digest = dict(summary_stats.get("lineage_summary") or lineage_summary or {})
        suite_digest = dict(suite_analytics_summary.get("digest") or {})
        workbench_evidence_summary = _normalize_workbench_evidence_payload(
            dict(payload.get("workbench_evidence_summary", {}) or summary_stats.get("workbench_evidence_summary") or {}),
            config_safety=config_safety,
            config_safety_review=config_safety_review,
        )
        workbench_action_report = _normalize_workbench_evidence_payload(
            workbench_action_report,
            config_safety=config_safety,
            config_safety_review=config_safety_review,
        )
        workbench_action_snapshot = _normalize_workbench_evidence_payload(
            workbench_action_snapshot,
            config_safety=config_safety,
            config_safety_review=config_safety_review,
        )
        readiness_text = self._humanize_ui_summary(str(readiness_summary.get("summary", "--") or "--"))
        analytics_text = self._humanize_ui_summary(
            str(
                dict(analytics_summary.get("unified_review_summary") or {}).get("summary")
                or analytics_digest.get("summary", "--")
                or "--"
            )
        )
        spectral_quality_text = self._spectral_quality_summary_text(spectral_quality_summary)
        suite_digest_text = self._humanize_ui_summary(str(suite_digest.get("summary", "--") or "--"))
        workbench_evidence_text = self._humanize_ui_summary(str(workbench_evidence_summary.get("summary_line", "--") or "--"))
        stability_digest = dict(multi_source_stability_evidence.get("digest") or {})
        state_transition_digest = dict(state_transition_evidence.get("digest") or {})
        measurement_phase_coverage_digest = dict(measurement_phase_coverage_report.get("digest") or {})
        stability_text = self._humanize_ui_summary(str(stability_digest.get("summary") or "--"))
        state_transition_text = self._humanize_ui_summary(str(state_transition_digest.get("summary") or "--"))
        measurement_phase_coverage_text = self._humanize_ui_summary(
            str(measurement_phase_coverage_digest.get("summary") or "--")
        )
        measurement_phase_payload_text = self._humanize_ui_summary(
            str(measurement_phase_coverage_digest.get("payload_phase_summary") or "--")
        )
        measurement_phase_payload_complete_text = self._humanize_ui_summary(
            str(measurement_phase_coverage_digest.get("payload_complete_phase_summary") or "--")
        )
        measurement_phase_payload_partial_text = self._humanize_ui_summary(
            str(measurement_phase_coverage_digest.get("payload_partial_phase_summary") or "--")
        )
        measurement_phase_trace_only_text = self._humanize_ui_summary(
            str(measurement_phase_coverage_digest.get("trace_only_phase_summary") or "--")
        )
        measurement_phase_payload_completeness_text = self._humanize_ui_summary(
            str(measurement_phase_coverage_digest.get("payload_completeness_summary") or "--")
        )
        measurement_phase_next_artifacts_text = self._humanize_ui_summary(
            str(measurement_phase_coverage_digest.get("next_required_artifacts_summary") or "--")
        )
        scope_readiness_text = self._humanize_ui_summary(
            str(dict(scope_readiness_summary.get("digest") or {}).get("summary") or "--")
        )
        reference_asset_registry_text = self._humanize_ui_summary(
            str(
                dict(reference_asset_registry.get("digest") or {}).get("asset_readiness_overview")
                or dict(reference_asset_registry.get("digest") or {}).get("summary")
                or "--"
            )
        )
        certificate_lifecycle_text = self._humanize_ui_summary(
            str(
                dict(certificate_lifecycle_summary.get("digest") or {}).get("certificate_lifecycle_overview")
                or dict(certificate_lifecycle_summary.get("digest") or {}).get("summary")
                or "--"
            )
        )
        certificate_readiness_text = self._humanize_ui_summary(
            str(dict(certificate_readiness_summary.get("digest") or {}).get("summary") or "--")
        )
        pre_run_readiness_gate_text = self._humanize_ui_summary(
            str(
                dict(pre_run_readiness_gate.get("digest") or {}).get("pre_run_gate_status")
                or pre_run_readiness_gate.get("gate_status")
                or dict(pre_run_readiness_gate.get("digest") or {}).get("summary")
                or "--"
            )
        )
        uncertainty_method_readiness_text = self._humanize_ui_summary(
            str(dict(uncertainty_method_readiness_summary.get("digest") or {}).get("summary") or "--")
        )
        audit_readiness_text = self._humanize_ui_summary(
            str(dict(audit_readiness_digest.get("digest") or {}).get("summary") or "--")
        )
        pre_run_blocking_text = self._humanize_ui_summary(
            str(
                recognition_scope_rollup.get("blocking_digest")
                or dict(pre_run_readiness_gate.get("digest") or {}).get("blocker_summary")
                or "--"
            )
        )
        pre_run_warning_text = self._humanize_ui_summary(
            str(
                recognition_scope_rollup.get("warning_digest")
                or dict(pre_run_readiness_gate.get("digest") or {}).get("warning_summary")
                or "--"
            )
        )
        scope_definition_digest = dict(scope_definition_pack.get("digest") or {})
        decision_rule_digest = dict(decision_rule_profile.get("digest") or {})
        scope_package_text = self._humanize_ui_summary(
            str(
                recognition_scope_rollup.get("scope_overview_display")
                or scope_definition_digest.get("scope_overview_summary")
                or dict(scope_definition_pack.get("scope_overview") or {}).get("summary")
                or "--"
            )
        )
        decision_rule_profile_text = self._humanize_ui_summary(
            str(
                recognition_scope_rollup.get("decision_rule_display")
                or decision_rule_digest.get("decision_rule_summary")
                or decision_rule_profile.get("decision_rule_id")
                or "--"
            )
        )
        conformity_boundary_text = self._humanize_ui_summary(
            str(
                recognition_scope_rollup.get("conformity_boundary_display")
                or decision_rule_digest.get("conformity_boundary_summary")
                or decision_rule_profile.get("non_claim_note")
                or scope_definition_pack.get("non_claim_note")
                or "--"
            )
        )
        recognition_scope_repository_text = self._humanize_ui_summary(
            " / ".join(
                part
                for part in (
                    str(recognition_scope_rollup.get("repository_mode") or "").strip(),
                    str(recognition_scope_rollup.get("gateway_mode") or "").strip(),
                )
                if part
            )
            or "--"
        )
        recognition_scope_rollup_text = self._humanize_ui_summary(
            str(recognition_scope_rollup.get("rollup_summary_display") or "--")
        )
        scope_non_claim_text = self._humanize_ui_summary(
            str(
                recognition_scope_rollup.get("non_claim_note")
                or decision_rule_profile.get("non_claim_note")
                or scope_definition_pack.get("non_claim_note")
                or "--"
            )
        )
        sidecar_store_summary = " | ".join(
            f"{key} {len(list(value or []))}"
            for key, value in dict(simulation_evidence_sidecar_bundle.get("stores") or {}).items()
        )
        sidecar_note_text = self._humanize_ui_summary(
            str(simulation_evidence_sidecar_bundle.get("reviewer_note") or "--")
        )
        measurement_core_boundary_lines = [
            self._humanize_ui_summary(str(item))
            for item in collect_boundary_digest_lines(
                measurement_phase_coverage_report,
                multi_source_stability_evidence,
                state_transition_evidence,
                simulation_evidence_sidecar_bundle,
            )
            if str(item).strip()
        ]
        measurement_review_lines = build_measurement_review_digest_lines(measurement_phase_coverage_report)
        measurement_core_summary_lines = [
            self._humanize_ui_summary(
                t(
                "facade.results.result_summary.measurement_core_stability",
                value=stability_text,
                default=f"multi-source stability shadow: {stability_text}",
                )
            )
            if multi_source_stability_evidence
            else "",
            self._humanize_ui_summary(
                t(
                "facade.results.result_summary.measurement_core_transition",
                value=state_transition_text,
                default=f"controlled state trace: {state_transition_text}",
                )
            )
            if state_transition_evidence
            else "",
            *[self._humanize_ui_summary(str(line)) for line in list(measurement_review_lines.get("summary_lines") or [])],
            *[
                self._humanize_ui_summary(str(line))
                for line in list(measurement_review_lines.get("detail_lines") or [])[:4]
            ],
            self._humanize_ui_summary(
                t(
                "facade.results.result_summary.measurement_core_sidecar_contract",
                value=(sidecar_store_summary or sidecar_note_text or "future database intake / sidecar-ready"),
                default=(
                    "sidecar-ready contract: "
                    + (sidecar_store_summary or sidecar_note_text or "future database intake / sidecar-ready")
                ),
                )
            )
            if simulation_evidence_sidecar_bundle
            else "",
            *measurement_core_boundary_lines,
        ]
        measurement_core_summary_text = "\n".join(
            line for line in measurement_core_summary_lines if str(line).strip()
        ) or t("pages.results.no_measurement_core_summary", default="暂无 measurement-core 摘要")
        result_evidence_source = _normalize_simulated_evidence_source(workbench_evidence_summary.get("evidence_source"))
        if not point_taxonomy_summary:
            point_taxonomy_summary = build_point_taxonomy_handoff(list(summary_stats.get("point_summaries", []) or []))
        offline_diagnostic_text = self._humanize_ui_summary(
            str(offline_diagnostic_adapter_summary.get("summary", "--") or "--")
        )
        offline_diagnostic_detail_lines = [
            self._humanize_ui_summary(str(item))
            for item in self._offline_diagnostic_highlight_lines(offline_diagnostic_adapter_summary)
            if str(item).strip()
        ]
        qc_evidence_section = self._build_results_qc_evidence_section(
            analytics_summary=analytics_summary,
            workbench_evidence_summary=workbench_evidence_summary,
        )
        qc_summary_text = self._build_results_qc_summary_text(
            analytics_summary=analytics_summary,
            workbench_evidence_summary=workbench_evidence_summary,
            config_safety=config_safety,
            config_safety_review=config_safety_review,
        )
        review_center = self._build_review_center(
            suite_summary=suite_summary,
            suite_analytics_summary=suite_analytics_summary,
            suite_acceptance_plan=suite_acceptance_plan,
            acceptance_plan=acceptance_plan,
            acceptance_readiness_summary=readiness_summary,
            analytics_summary=analytics_summary,
            spectral_quality_summary=spectral_quality_summary,
            analytics_digest=analytics_digest,
            lineage_summary=lineage_summary,
            lineage_digest=lineage_digest,
            workbench_evidence_summary=workbench_evidence_summary,
            offline_diagnostic_adapter_summary=offline_diagnostic_adapter_summary,
            multi_source_stability_evidence=multi_source_stability_evidence,
            state_transition_evidence=state_transition_evidence,
            measurement_phase_coverage_report=measurement_phase_coverage_report,
            scope_definition_pack=scope_definition_pack,
            decision_rule_profile=decision_rule_profile,
            reference_asset_registry=reference_asset_registry,
            certificate_lifecycle_summary=certificate_lifecycle_summary,
            scope_readiness_summary=scope_readiness_summary,
            certificate_readiness_summary=certificate_readiness_summary,
            pre_run_readiness_gate=pre_run_readiness_gate,
            uncertainty_method_readiness_summary=uncertainty_method_readiness_summary,
            audit_readiness_digest=audit_readiness_digest,
            compatibility_scan_summary=compatibility_scan_summary,
            recognition_scope_rollup=recognition_scope_rollup,
        )
        review_digest = self._build_review_digest(
            suite_summary=suite_summary,
            suite_analytics_summary=suite_analytics_summary,
            acceptance_readiness_summary=readiness_summary,
            workbench_evidence_summary=workbench_evidence_summary,
        )

        overview_text = "\n".join(
            [
                t("facade.results.overview.run_id", value=self.session.run_id),
                t("facade.results.overview.phase", value=display_phase(self.service.get_status().phase.value)),
                t("facade.results.overview.output_dir", value=run_dir),
                t("facade.results.overview.samples", value=sample_count),
                t("facade.results.overview.point_summaries", value=point_summary_count),
                t("facade.results.overview.warnings", value=summary_stats.get("warning_count", len(self.session.warnings))),
                t("facade.results.overview.errors", value=summary_stats.get("error_count", len(self.session.errors))),
                t("facade.results.overview.report_mode", value=reporting.get("mode", "--")),
                t(
                    "facade.results.overview.include_fleet_stats",
                    value=display_bool(bool(reporting.get("include_fleet_stats", False))),
                ),
                t("facade.results.overview.readiness", value=readiness_text),
                t("facade.results.overview.analytics", value=analytics_text),
            ]
        )
        algorithm_text = "\n".join(
            [
                t("facade.results.algorithm.default", value=self.config.algorithm.default_algorithm),
                t("facade.results.algorithm.candidates", value=", ".join(self.config.algorithm.candidates)),
                t("facade.results.algorithm.auto_select", value=display_bool(bool(self.config.algorithm.auto_select))),
                t("facade.results.algorithm.coefficient_model", value=self.config.coefficients.model),
            ]
        )
        result_text = "\n".join(
            [
                t("facade.results.result_summary.results_file", value=display_presence(isinstance(results, dict))),
                t("facade.results.result_summary.summary_file", value=display_presence(isinstance(summary, dict))),
                t("facade.results.result_summary.manifest_file", value=display_presence(isinstance(manifest, dict))),
                t(
                    "facade.results.result_summary.readable_points",
                    value=display_presence(any(Path(item).name == "points_readable.csv" for item in output_files)),
                ),
                t("facade.results.result_summary.sample_count", value=sample_count),
                t("facade.results.result_summary.point_summary_count", value=point_summary_count),
                t("facade.results.result_summary.export_status_count", value=len(artifact_exports)),
                t("facade.results.result_summary.artifact_roles", value=artifact_role_text),
                t("facade.results.result_summary.lineage_config_version", value=lineage_digest.get("config_version", "--")),
                t("facade.results.result_summary.suite_summary", value=suite_digest_text),
                f"证据来源: {result_evidence_source}",
                *(
                    [
                        t(
                            "facade.results.result_summary.artifact_compatibility",
                            value=" | ".join(
                                part
                                for part in (
                                    str(
                                        compatibility_scan_summary.get("current_reader_mode_display")
                                        or compatibility_scan_summary.get("current_reader_mode")
                                        or ""
                                    ).strip(),
                                    str(
                                        compatibility_scan_summary.get("compatibility_status_display")
                                        or compatibility_scan_summary.get("compatibility_status")
                                        or ""
                                    ).strip(),
                                )
                                if part
                            ),
                            default="工件兼容: "
                            + " | ".join(
                                part
                                for part in (
                                    str(
                                        compatibility_scan_summary.get("current_reader_mode_display")
                                        or compatibility_scan_summary.get("current_reader_mode")
                                        or ""
                                    ).strip(),
                                    str(
                                        compatibility_scan_summary.get("compatibility_status_display")
                                        or compatibility_scan_summary.get("compatibility_status")
                                        or ""
                                    ).strip(),
                                )
                                if part
                            ),
                        )
                    ]
                    if compatibility_scan_summary
                    else []
                ),
                *(
                    [t("facade.results.result_summary.scope_package", value=scope_package_text)]
                    if scope_definition_pack or recognition_scope_rollup
                    else []
                ),
                *(
                    [t("facade.results.result_summary.decision_rule_profile", value=decision_rule_profile_text)]
                    if decision_rule_profile or recognition_scope_rollup
                    else []
                ),
                *(
                    [t("facade.results.result_summary.conformity_boundary", value=conformity_boundary_text)]
                    if decision_rule_profile or scope_definition_pack or recognition_scope_rollup
                    else []
                ),
                *(
                    [t("facade.results.result_summary.recognition_scope_repository", value=recognition_scope_repository_text)]
                    if recognition_scope_rollup
                    else []
                ),
                *(
                    [t("facade.results.result_summary.recognition_scope_rollup", value=recognition_scope_rollup_text)]
                    if recognition_scope_rollup
                    else []
                ),
                *(
                    [t("facade.results.result_summary.scope_non_claim", value=scope_non_claim_text)]
                    if decision_rule_profile or scope_definition_pack or recognition_scope_rollup
                    else []
                ),
                *(
                    [t("facade.results.result_summary.spectral_quality", value=spectral_quality_text)]
                    if spectral_quality_text
                    else []
                ),
                *(
                    [t("facade.results.result_summary.offline_diagnostic", value=offline_diagnostic_text)]
                    if offline_diagnostic_adapter_summary
                    else []
                ),
                *(
                    [
                        t(
                            "facade.results.result_summary.offline_diagnostic_coverage",
                            value=humanize_offline_diagnostic_summary_value(
                                str(offline_diagnostic_adapter_summary.get("coverage_summary") or "")
                            ),
                        )
                    ]
                    if str(offline_diagnostic_adapter_summary.get("coverage_summary") or "").strip()
                    else []
                ),
                *(
                    [
                        t(
                            "facade.results.result_summary.offline_diagnostic_scope",
                            value=humanize_offline_diagnostic_summary_value(
                                str(offline_diagnostic_adapter_summary.get("review_scope_summary") or "")
                            ),
                        )
                    ]
                    if str(offline_diagnostic_adapter_summary.get("review_scope_summary") or "").strip()
                    else []
                ),
                *(
                    [
                        t(
                            "facade.results.result_summary.offline_diagnostic_next_checks",
                            value=str(offline_diagnostic_adapter_summary.get("next_check_summary") or ""),
                        )
                    ]
                    if str(offline_diagnostic_adapter_summary.get("next_check_summary") or "").strip()
                    else []
                ),
                *[
                    t(
                        "facade.results.result_summary.offline_diagnostic_detail",
                        value=line,
                        default=f"离线诊断补充: {line}",
                    )
                    for line in offline_diagnostic_detail_lines
                ],
                f"配置安全: {str(config_safety_review.get('summary') or '--')}",
                *(
                    [t("facade.results.result_summary.taxonomy_pressure", value=str(point_taxonomy_summary.get("pressure_summary") or ""))]
                    if str(point_taxonomy_summary.get("pressure_summary") or "").strip()
                    else []
                ),
                *(
                    [t("facade.results.result_summary.taxonomy_pressure_mode", value=str(point_taxonomy_summary.get("pressure_mode_summary") or ""))]
                    if str(point_taxonomy_summary.get("pressure_mode_summary") or "").strip()
                    and str(point_taxonomy_summary.get("pressure_mode_summary") or "").strip()
                    != str(point_taxonomy_summary.get("pressure_summary") or "").strip()
                    else []
                ),
                *(
                    [
                        t(
                            "facade.results.result_summary.taxonomy_pressure_target_label",
                            value=str(point_taxonomy_summary.get("pressure_target_label_summary") or ""),
                        )
                    ]
                    if str(point_taxonomy_summary.get("pressure_target_label_summary") or "").strip()
                    and str(point_taxonomy_summary.get("pressure_target_label_summary") or "").strip()
                    != str(point_taxonomy_summary.get("pressure_summary") or "").strip()
                    else []
                ),
                *(
                    [t("facade.results.result_summary.taxonomy_flush", value=str(point_taxonomy_summary.get("flush_gate_summary") or ""))]
                    if str(point_taxonomy_summary.get("flush_gate_summary") or "").strip()
                    else []
                ),
                *(
                    [t("facade.results.result_summary.taxonomy_preseal", value=str(point_taxonomy_summary.get("preseal_summary") or ""))]
                    if str(point_taxonomy_summary.get("preseal_summary") or "").strip()
                    else []
                ),
                *(
                    [t("facade.results.result_summary.taxonomy_postseal", value=str(point_taxonomy_summary.get("postseal_summary") or ""))]
                    if str(point_taxonomy_summary.get("postseal_summary") or "").strip()
                    else []
                ),
                *(
                    [t("facade.results.result_summary.taxonomy_stale_gauge", value=str(point_taxonomy_summary.get("stale_gauge_summary") or ""))]
                    if str(point_taxonomy_summary.get("stale_gauge_summary") or "").strip()
                    else []
                ),
                *(
                    [
                        t(
                            "facade.results.result_summary.measurement_core_stability",
                            value=stability_text,
                            default=f"multi-source stability shadow: {stability_text}",
                        )
                    ]
                    if multi_source_stability_evidence
                    else []
                ),
                *(
                    [
                        t(
                            "facade.results.result_summary.measurement_core_transition",
                            value=state_transition_text,
                            default=f"controlled state trace: {state_transition_text}",
                        )
                    ]
                    if state_transition_evidence
                    else []
                ),
                *(
                    [
                        t(
                            "facade.results.result_summary.measurement_core_phase_coverage",
                            value=measurement_phase_coverage_text,
                            default=f"measurement-core phase coverage: {measurement_phase_coverage_text}",
                        )
                    ]
                    if measurement_phase_coverage_report
                    else []
                ),
                *(
                    [
                        t(
                            "facade.results.result_summary.measurement_core_sidecar_contract",
                            value=(sidecar_store_summary or sidecar_note_text or "future database intake / sidecar-ready"),
                            default=(
                                "sidecar-ready contract: "
                                + (sidecar_store_summary or sidecar_note_text or "future database intake / sidecar-ready")
                            ),
                        )
                    ]
                    if simulation_evidence_sidecar_bundle
                    else []
                ),
                *(
                    [
                        t(
                            "facade.results.result_summary.scope_readiness",
                            value=scope_readiness_text,
                            default=f"认可范围 / decision rule readiness: {scope_readiness_text}",
                        )
                    ]
                    if scope_readiness_summary
                    else []
                ),
                *(
                    [
                        t(
                            "facade.results.result_summary.reference_certificate_readiness",
                            value=certificate_readiness_text,
                            default=f"reference / certificate readiness: {certificate_readiness_text}",
                        )
                    ]
                    if certificate_readiness_summary
                    else []
                ),
                *(
                    [
                        t(
                            "facade.results.result_summary.uncertainty_method_readiness",
                            value=uncertainty_method_readiness_text,
                            default=f"uncertainty / method readiness: {uncertainty_method_readiness_text}",
                        )
                    ]
                    if uncertainty_method_readiness_summary
                    else []
                ),
                *(
                    [
                        t(
                            "facade.results.result_summary.software_audit_readiness",
                            value=audit_readiness_text,
                            default=f"software validation / audit readiness: {audit_readiness_text}",
                        )
                    ]
                    if audit_readiness_digest
                    else []
                ),
                *(
                    [
                        t(
                            "facade.results.result_summary.asset_readiness_overview",
                            value=reference_asset_registry_text,
                            default=f"asset readiness overview: {reference_asset_registry_text}",
                        )
                    ]
                    if reference_asset_registry
                    else []
                ),
                *(
                    [
                        t(
                            "facade.results.result_summary.certificate_lifecycle_overview",
                            value=certificate_lifecycle_text,
                            default=f"certificate lifecycle overview: {certificate_lifecycle_text}",
                        )
                    ]
                    if certificate_lifecycle_summary
                    else []
                ),
                *(
                    [
                        t(
                            "facade.results.result_summary.pre_run_readiness_gate",
                            value=pre_run_readiness_gate_text,
                            default=f"pre-run readiness gate: {pre_run_readiness_gate_text}",
                        )
                    ]
                    if pre_run_readiness_gate
                    else []
                ),
                *(
                    [
                        t(
                            "facade.results.result_summary.pre_run_blocking_digest",
                            value=pre_run_blocking_text,
                            default=f"blocking digest: {pre_run_blocking_text}",
                        )
                    ]
                    if pre_run_readiness_gate
                    else []
                ),
                *(
                    [
                        t(
                            "facade.results.result_summary.pre_run_warning_digest",
                            value=pre_run_warning_text,
                            default=f"warning digest: {pre_run_warning_text}",
                        )
                    ]
                    if pre_run_readiness_gate
                    else []
                ),
                t("facade.results.result_summary.workbench_evidence", value=workbench_evidence_text),
            ]
        )
        if compatibility_scan_summary:
            compatibility_extra_lines = [
                t(
                    "facade.results.result_summary.artifact_compatibility_contracts",
                    value=str(
                        compatibility_overview.get("schema_contract_summary_display")
                        or compatibility_scan_summary.get("schema_or_contract_version_summary")
                        or "--"
                    ),
                    default="工件合同/Schema: {value}",
                ),
                t(
                    "facade.results.result_summary.artifact_compatibility_recommendation",
                    value=str(compatibility_overview.get("regenerate_recommendation_display") or "--"),
                    default="兼容建议: {value}",
                ),
                t(
                    "facade.results.result_summary.artifact_compatibility_rollup",
                    value=str(
                        compatibility_rollup.get("rollup_summary_display")
                        or compatibility_overview.get("rollup_summary_display")
                        or "--"
                    ),
                    default="兼容性 rollup：{value}",
                ),
                t(
                    "facade.results.result_summary.artifact_compatibility_boundary",
                    value=str(
                        compatibility_overview.get("non_primary_boundary_display")
                        or compatibility_overview.get("non_primary_chain_display")
                        or "--"
                    ),
                    default="兼容边界: {value}",
                ),
                t(
                    "facade.results.result_summary.artifact_compatibility_non_claim",
                    value=str(
                        compatibility_overview.get("non_claim_digest")
                        or compatibility_scan_summary.get("non_claim_digest")
                        or "--"
                    ),
                    default="兼容 non-claim: {value}",
                ),
            ]
            result_text = "\n".join(
                [result_text, *[line for line in compatibility_extra_lines if str(line).strip()]]
            )
        coefficient_text = "\n".join(coefficient_files) if coefficient_files else t("facade.no_coefficient_artifacts")
        ai_text = self._humanize_ui_summary(ai_text.strip()) or t("facade.no_ai_summary_artifact")

        return {
            "output_files": output_files,
            "summary": summary,
            "manifest": manifest,
            "results": results,
            "reporting": reporting,
            "artifact_exports": artifact_exports,
            "artifact_role_summary": artifact_role_summary,
            "acceptance_plan": acceptance_plan,
            "analytics_summary": analytics_summary,
            "spectral_quality_summary": spectral_quality_summary,
            "trend_registry": trend_registry,
            "lineage_summary": lineage_summary,
            "evidence_registry": evidence_registry,
            "coefficient_registry": coefficient_registry,
            "suite_summary": suite_summary,
            "suite_analytics_summary": suite_analytics_summary,
            "suite_acceptance_plan": suite_acceptance_plan,
            "workbench_action_report": workbench_action_report,
            "workbench_action_snapshot": workbench_action_snapshot,
            "workbench_evidence_summary": workbench_evidence_summary,
            "offline_diagnostic_adapter_summary": offline_diagnostic_adapter_summary,
            "point_taxonomy_summary": point_taxonomy_summary,
            "multi_source_stability_evidence": multi_source_stability_evidence,
            "state_transition_evidence": state_transition_evidence,
            "simulation_evidence_sidecar_bundle": simulation_evidence_sidecar_bundle,
            "measurement_phase_coverage_report": measurement_phase_coverage_report,
            "scope_definition_pack": scope_definition_pack,
            "decision_rule_profile": decision_rule_profile,
            "reference_asset_registry": reference_asset_registry,
            "certificate_lifecycle_summary": certificate_lifecycle_summary,
            "scope_readiness_summary": scope_readiness_summary,
            "certificate_readiness_summary": certificate_readiness_summary,
            "pre_run_readiness_gate": pre_run_readiness_gate,
            "uncertainty_method_readiness_summary": uncertainty_method_readiness_summary,
            "audit_readiness_digest": audit_readiness_digest,
            "run_artifact_index": run_artifact_index,
            "artifact_contract_catalog": artifact_contract_catalog,
            "compatibility_scan_summary": compatibility_scan_summary,
            "compatibility_overview": compatibility_overview,
            "compatibility_rollup": compatibility_rollup,
            "recognition_scope_rollup": recognition_scope_rollup,
            "reindex_manifest": reindex_manifest,
            "review_digest": review_digest,
            "review_digest_text": str(review_digest.get("summary_text", "") or ""),
            "review_center": review_center,
            "overview_text": overview_text,
            "algorithm_compare_text": algorithm_text,
            "result_summary_text": result_text,
            "measurement_core_summary_text": measurement_core_summary_text,
            "coefficient_summary_text": coefficient_text,
            "qc_summary_text": qc_summary_text,
            "qc_reviewer_card": dict(qc_evidence_section.get("reviewer_card") or {}),
            "qc_evidence_section": qc_evidence_section,
            "qc_review_cards": self._normalize_review_cards(qc_evidence_section),
            "ai_summary_text": ai_text,
            "config_safety": config_safety,
            "config_safety_review": config_safety_review,
            "config_governance_handoff": config_governance_handoff,
            "acceptance_readiness_summary": {
                **readiness_summary,
                "summary_display": readiness_text,
            },
            "analytics_summary_digest": {
                **analytics_digest,
                "summary_display": analytics_text,
            },
            "spectral_quality_digest": self._build_spectral_quality_digest(spectral_quality_summary),
            "lineage_digest": {
                "config_version": lineage_digest.get("config_version"),
                "points_version": lineage_digest.get("points_version"),
                "profile_version": lineage_digest.get("profile_version"),
            },
        }

    def _build_review_digest(
        self,
        *,
        suite_summary: dict[str, Any],
        suite_analytics_summary: dict[str, Any],
        acceptance_readiness_summary: dict[str, Any],
        workbench_evidence_summary: dict[str, Any],
    ) -> dict[str, Any]:
        latest_suite = self._summarize_suite_review(suite_summary, suite_analytics_summary)
        latest_parity = self._summarize_external_review_artifact(
            filename="summary_parity_report.json",
            kind="parity",
        )
        latest_resilience = self._summarize_external_review_artifact(
            filename="export_resilience_report.json",
            kind="resilience",
        )
        latest_workbench = self._summarize_workbench_review(workbench_evidence_summary)
        readiness_text = self._humanize_ui_summary(
            str(acceptance_readiness_summary.get("summary_display") or acceptance_readiness_summary.get("summary") or t("common.none"))
        )
        readiness = {
            "available": bool(acceptance_readiness_summary),
            "summary": readiness_text,
            "simulated_readiness_only": bool(acceptance_readiness_summary.get("simulated_readiness_only", True)),
        }
        items = {
            "suite": latest_suite,
            "parity": latest_parity,
            "resilience": latest_resilience,
            "workbench": latest_workbench,
            "acceptance_readiness": readiness,
        }
        summary_lines = [
            f"{t('results.review_digest.suite', default='最新套件')}: {latest_suite.get('summary', t('common.none'))}",
            f"{t('results.review_digest.parity', default='最新一致性')}: {latest_parity.get('summary', t('common.none'))}",
            f"{t('results.review_digest.resilience', default='最新导出韧性')}: {latest_resilience.get('summary', t('common.none'))}",
            f"{t('results.review_digest.workbench', default='最新工作台证据')}: {latest_workbench.get('summary', t('common.none'))}",
            f"{t('results.review_digest.readiness', default='当前验收就绪度')}: {readiness_text or t('common.none')}",
            t(
                "results.review_digest.disclaimer",
                default="以上均为离线仿真/回放审阅证据，不代表真实 acceptance。",
            ),
        ]
        return {
            "items": items,
            "summary_text": "\n".join(summary_lines),
        }

    def _build_review_center(
        self,
        *,
        suite_summary: dict[str, Any],
        suite_analytics_summary: dict[str, Any],
        suite_acceptance_plan: dict[str, Any],
        acceptance_plan: dict[str, Any],
        acceptance_readiness_summary: dict[str, Any],
        analytics_summary: dict[str, Any],
        spectral_quality_summary: dict[str, Any],
        analytics_digest: dict[str, Any],
        lineage_summary: dict[str, Any],
        lineage_digest: dict[str, Any],
        workbench_evidence_summary: dict[str, Any],
        offline_diagnostic_adapter_summary: dict[str, Any],
        multi_source_stability_evidence: dict[str, Any],
        state_transition_evidence: dict[str, Any],
        measurement_phase_coverage_report: dict[str, Any],
        scope_definition_pack: dict[str, Any],
        decision_rule_profile: dict[str, Any],
        reference_asset_registry: dict[str, Any],
        certificate_lifecycle_summary: dict[str, Any],
        scope_readiness_summary: dict[str, Any],
        certificate_readiness_summary: dict[str, Any],
        pre_run_readiness_gate: dict[str, Any],
        uncertainty_method_readiness_summary: dict[str, Any],
        audit_readiness_digest: dict[str, Any],
        compatibility_scan_summary: dict[str, Any],
        recognition_scope_rollup: dict[str, Any],
    ) -> dict[str, Any]:
        compatibility_rollup = dict(
            dict(compatibility_scan_summary or {}).get("compatibility_rollup")
            or dict(dict(compatibility_scan_summary or {}).get("compatibility_overview") or {}).get("compatibility_rollup")
            or {}
        )
        evidence_items, review_diagnostics = self._collect_review_evidence(
            suite_summary=suite_summary,
            suite_analytics_summary=suite_analytics_summary,
            analytics_summary=analytics_summary,
            spectral_quality_summary=spectral_quality_summary,
            lineage_summary=lineage_summary,
            workbench_evidence_summary=workbench_evidence_summary,
            offline_diagnostic_adapter_summary=offline_diagnostic_adapter_summary,
            multi_source_stability_evidence=multi_source_stability_evidence,
            state_transition_evidence=state_transition_evidence,
            measurement_phase_coverage_report=measurement_phase_coverage_report,
            scope_definition_pack=scope_definition_pack,
            decision_rule_profile=decision_rule_profile,
            reference_asset_registry=reference_asset_registry,
            certificate_lifecycle_summary=certificate_lifecycle_summary,
            scope_readiness_summary=scope_readiness_summary,
            certificate_readiness_summary=certificate_readiness_summary,
            pre_run_readiness_gate=pre_run_readiness_gate,
            uncertainty_method_readiness_summary=uncertainty_method_readiness_summary,
            audit_readiness_digest=audit_readiness_digest,
            compatibility_scan_summary=compatibility_scan_summary,
        )
        index_summary = self._build_review_index_summary(
            evidence_items,
            diagnostics=review_diagnostics,
            compatibility_rollup=compatibility_rollup,
            recognition_scope_rollup=recognition_scope_rollup,
        )
        readiness_text = self._humanize_ui_summary(
            str(acceptance_readiness_summary.get("summary_display") or acceptance_readiness_summary.get("summary") or t("common.none"))
        )
        analytics_text = self._humanize_ui_summary(
            str(
                dict(analytics_summary.get("unified_review_summary") or {}).get("summary")
                or analytics_digest.get("summary_display")
                or analytics_digest.get("summary")
                or t("common.none")
            )
        )
        lineage_text = t(
            "results.review_center.lineage.summary_line",
            config=str(lineage_digest.get("config_version") or "--"),
            points=str(lineage_digest.get("points_version") or "--"),
            profile=str(lineage_digest.get("profile_version") or "--"),
        )
        source_kind_summary = self._humanize_ui_summary(
            str(index_summary.get("source_kind_summary") or t("common.none"))
        )
        coverage_summary = self._humanize_ui_summary(
            humanize_review_center_coverage_text(str(index_summary.get("coverage_summary") or t("common.none")))
        )
        role_views = dict(acceptance_plan.get("role_views") or suite_acceptance_plan.get("role_views") or {})
        operator_view = dict(role_views.get("operator", {}) or {})
        reviewer_view = dict(role_views.get("reviewer", {}) or {})
        approver_view = dict(role_views.get("approver", {}) or {})
        reviewer_notes = [
            self._humanize_ui_summary(str(item))
            for item in list(reviewer_view.get("notes") or acceptance_plan.get("missing_conditions") or [])
            if str(item).strip()
        ]
        approver_missing = [
            self._humanize_ui_summary(str(item))
            for item in list(approver_view.get("missing_conditions") or acceptance_plan.get("missing_conditions") or [])
            if str(item).strip()
        ]
        complete_sources = int(index_summary.get("complete_sources", 0) or 0)
        recent_runs = int(index_summary.get("recent_runs", 0) or 0)
        coverage_gaps = self._humanize_ui_summary(
            humanize_review_center_coverage_text(str(index_summary.get("coverage_gaps_display") or t("common.none")))
        )
        risk_summary = self._build_review_risk_summary(
            evidence_items=evidence_items,
            acceptance_plan=acceptance_plan,
            acceptance_readiness_summary=acceptance_readiness_summary,
            analytics_digest=analytics_digest,
            coverage_gaps=coverage_gaps,
        )
        evidence_items = [
            self._enrich_review_evidence_item(
                dict(item),
                readiness_text=readiness_text,
                risk_summary=risk_summary,
                coverage_gaps=coverage_gaps,
                analytics_text=analytics_text,
                lineage_text=self._humanize_ui_summary(lineage_text),
                source_kind_summary=source_kind_summary,
                coverage_summary=coverage_summary,
                compatibility_summary=self._humanize_ui_summary(
                    "\n".join(
                        fragment
                        for fragment in (
                            str(compatibility_rollup.get("rollup_summary_display") or "").strip(),
                            str(index_summary.get("recognition_scope_summary") or "").strip(),
                        )
                        if fragment
                    )
                    or t("common.none")
                ),
            )
            for item in evidence_items
        ]
        latest_items = {
            evidence_type: next(
                (dict(item) for item in evidence_items if str(item.get("type") or "") == evidence_type),
                {
                    "available": False,
                    "summary": t("results.review_digest.none"),
                    "type": evidence_type,
                    "type_display": t(f"results.review_center.type.{evidence_type}"),
                },
            )
            for evidence_type in (
                "suite",
                "parity",
                "resilience",
                "workbench",
                "analytics",
                "offline_diagnostic",
                "stability",
                "state_transition",
                "measurement_phase_coverage",
                "artifact_compatibility",
            )
        }
        phase_bridge_reviewer_artifact_entry: dict[str, Any] = {}
        stage_admission_review_pack_artifact_entry: dict[str, Any] = {}
        engineering_isolation_admission_checklist_artifact_entry: dict[str, Any] = {}
        stage3_real_validation_plan_artifact_entry: dict[str, Any] = {}
        stage3_standards_alignment_matrix_artifact_entry: dict[str, Any] = {}
        try:
            reports_payload = dict(self.results_gateway.read_reports_payload() or {})
            phase_bridge_reviewer_artifact_entry = dict(
                reports_payload.get("phase_transition_bridge_reviewer_artifact_entry") or {}
            )
            stage_admission_review_pack_artifact_entry = dict(
                reports_payload.get("stage_admission_review_pack_artifact_entry") or {}
            )
            engineering_isolation_admission_checklist_artifact_entry = dict(
                reports_payload.get("engineering_isolation_admission_checklist_artifact_entry") or {}
            )
            stage3_real_validation_plan_artifact_entry = dict(
                reports_payload.get("stage3_real_validation_plan_artifact_entry") or {}
            )
            stage3_standards_alignment_matrix_artifact_entry = dict(
                reports_payload.get("stage3_standards_alignment_matrix_artifact_entry") or {}
            )
        except Exception:
            phase_bridge_reviewer_artifact_entry = {}
            stage_admission_review_pack_artifact_entry = {}
            engineering_isolation_admission_checklist_artifact_entry = {}
            stage3_real_validation_plan_artifact_entry = {}
            stage3_standards_alignment_matrix_artifact_entry = {}
        reviewer_artifact_entries = _available_reviewer_artifact_entries(
            [
                stage_admission_review_pack_artifact_entry,
                engineering_isolation_admission_checklist_artifact_entry,
                stage3_real_validation_plan_artifact_entry,
                stage3_standards_alignment_matrix_artifact_entry,
            ]
        )
        evidence_items = [
            _apply_fragment_filter_contract(dict(item))
            for item in evidence_items
            if isinstance(item, dict)
        ]
        reviewer_filter_options = _build_reviewer_filter_options(reviewer_artifact_entries)
        measurement_filter_options = _build_measurement_core_filter_options(evidence_items)
        measurement_boundary_rows = _dedupe_fragment_filter_rows(
            [
                row
                for item in evidence_items
                for row in list(item.get("boundary_filter_rows") or [])
                if isinstance(row, dict)
            ]
        )
        measurement_filter_options["boundary_options"] = _build_fragment_filter_options(
            measurement_boundary_rows,
            all_label_key="results.review_center.filter.all_boundaries",
            all_label_default="全部边界",
        )
        return {
            "latest": latest_items,
            "operator_focus": {
                "summary": t(
                    "results.review_center.focus.operator_summary",
                    health=self._humanize_ui_summary(str(operator_view.get("summary") or t("common.none"))),
                    suite=latest_items["suite"].get("summary", t("common.none")),
                    workbench=latest_items["workbench"].get("summary", t("common.none")),
                    analytics=latest_items["analytics"].get("summary", t("common.none")),
                    risk=risk_summary.get("summary", t("common.none")),
                    runs=recent_runs,
                    complete=complete_sources,
                )
            },
            "reviewer_focus": {
                "summary": t(
                    "results.review_center.focus.reviewer_summary",
                    completeness=self._humanize_ui_summary(str(reviewer_view.get("summary") or t("common.none"))),
                    parity=latest_items["parity"].get("summary", t("common.none")),
                    resilience=latest_items["resilience"].get("summary", t("common.none")),
                    analytics=latest_items["analytics"].get("summary", t("common.none")),
                    notes=len(reviewer_notes),
                    coverage=coverage_gaps,
                    risk=risk_summary.get("level_display", t("common.none")),
                )
            },
            "approver_focus": {
                "summary": t(
                    "results.review_center.focus.approver_summary",
                    readiness=readiness_text,
                    promotion=display_acceptance_value(
                        acceptance_plan.get("promotion_state"),
                        default=str(acceptance_plan.get("promotion_state") or "--"),
                    ),
                    missing=len(approver_missing),
                    coverage=coverage_gaps,
                    complete=complete_sources,
                    risk=risk_summary.get("summary", t("common.none")),
                )
            },
            "risk_summary": risk_summary,
            "acceptance_readiness": {
                "summary": readiness_text,
                "detail": list(reviewer_notes)
                + ([coverage_gaps] if coverage_gaps and coverage_gaps != t("common.none") else [])
                + ([str(risk_summary.get("summary") or "")] if str(risk_summary.get("summary") or "").strip() else []),
                "simulated_only": bool(acceptance_readiness_summary.get("simulated_readiness_only", True)),
            },
            "analytics_summary": {
                "summary": analytics_text,
                "detail": dict(analytics_summary or {}),
            },
            "lineage_summary": {
                "summary": self._humanize_ui_summary(lineage_text),
                "detail": dict(lineage_summary or {}),
            },
            "index_summary": index_summary,
            "diagnostics": dict(review_diagnostics),
            "evidence_items": evidence_items,
            "phase_transition_bridge_reviewer_artifact_entry": phase_bridge_reviewer_artifact_entry,
            "stage_admission_review_pack_artifact_entry": stage_admission_review_pack_artifact_entry,
            "engineering_isolation_admission_checklist_artifact_entry": (
                engineering_isolation_admission_checklist_artifact_entry
            ),
            "stage3_real_validation_plan_artifact_entry": stage3_real_validation_plan_artifact_entry,
            "stage3_standards_alignment_matrix_artifact_entry": stage3_standards_alignment_matrix_artifact_entry,
            "reviewer_artifact_entries": reviewer_artifact_entries,
            "filters": {
                "selected_type": "all",
                "selected_status": "all",
                "selected_time": "all",
                "selected_source": "all",
                "selected_phase": "all",
                "selected_artifact_role": "all",
                "selected_standard_family": "all",
                "selected_evidence_category": "all",
                "selected_boundary": "all",
                "selected_anchor": "all",
                "selected_route": "all",
                "selected_signal_family": "all",
                "selected_decision_result": "all",
                "selected_policy_version": "all",
                "selected_evidence_source": "all",
                "type_options": [
                    {"id": "all", "label": t("results.review_center.filter.all_types")},
                    {"id": "suite", "label": t("results.review_center.type.suite")},
                    {"id": "parity", "label": t("results.review_center.type.parity")},
                    {"id": "resilience", "label": t("results.review_center.type.resilience")},
                    {"id": "workbench", "label": t("results.review_center.type.workbench")},
                    {"id": "analytics", "label": t("results.review_center.type.analytics")},
                    {"id": "offline_diagnostic", "label": t("results.review_center.type.offline_diagnostic")},
                    {"id": "stability", "label": t("results.review_center.type.stability", default="澶氭簮鍒ょǔ")},
                    {"id": "state_transition", "label": t("results.review_center.type.state_transition", default="鍙楁帶鐘舵€佹満")},
                    {
                        "id": "measurement_phase_coverage",
                        "label": t(
                            "results.review_center.type.measurement_phase_coverage",
                            default="measurement phase coverage",
                        ),
                    },
                    {
                        "id": "artifact_compatibility",
                        "label": t(
                            "results.review_center.type.artifact_compatibility",
                            default="工件兼容 / 再索引",
                        ),
                    },
                    {
                        "id": "readiness_governance",
                        "label": t(
                            "results.review_center.type.readiness_governance",
                            default="认可就绪治理骨架",
                        ),
                    },
                ],
                "status_options": [
                    {"id": "all", "label": t("results.review_center.filter.all_statuses")},
                    {"id": "passed", "label": t("results.review_center.status.passed")},
                    {"id": "failed", "label": t("results.review_center.status.failed")},
                    {"id": "degraded", "label": t("results.review_center.status.degraded")},
                    {"id": "diagnostic_only", "label": t("results.review_center.status.diagnostic_only")},
                ],
                "time_options": [
                    {"id": "all", "label": t("results.review_center.filter.all_time"), "window_seconds": None},
                    {"id": "24h", "label": t("results.review_center.filter.time_24h"), "window_seconds": 86400},
                    {"id": "7d", "label": t("results.review_center.filter.time_7d"), "window_seconds": 604800},
                    {"id": "30d", "label": t("results.review_center.filter.time_30d"), "window_seconds": 2592000},
                ],
                "source_options": [
                    {"id": "all", "label": t("results.review_center.filter.all_sources")},
                    {"id": "run", "label": t("results.review_center.source_kind.run")},
                    {"id": "suite", "label": t("results.review_center.source_kind.suite")},
                    {"id": "workbench", "label": t("results.review_center.source_kind.workbench")},
                ],
                "phase_options": _merge_filter_options(
                    reviewer_filter_options["phase_options"],
                    measurement_filter_options["phase_options"],
                ),
                "artifact_role_options": _merge_filter_options(
                    reviewer_filter_options["artifact_role_options"],
                    measurement_filter_options["artifact_role_options"],
                ),
                "standard_family_options": reviewer_filter_options["standard_family_options"],
                "evidence_category_options": _merge_filter_options(
                    reviewer_filter_options["evidence_category_options"],
                    measurement_filter_options["evidence_category_options"],
                ),
                "boundary_options": _merge_filter_options(
                    reviewer_filter_options["boundary_options"],
                    measurement_filter_options["boundary_options"],
                ),
                "anchor_options": _merge_filter_options(
                    reviewer_filter_options["anchor_options"],
                    measurement_filter_options["anchor_options"],
                ),
                "route_options": measurement_filter_options["route_options"],
                "signal_family_options": measurement_filter_options["signal_family_options"],
                "decision_result_options": measurement_filter_options["decision_result_options"],
                "policy_version_options": measurement_filter_options["policy_version_options"],
                "evidence_source_options": measurement_filter_options["evidence_source_options"],
            },
            "detail_hint": t("results.review_center.detail_hint"),
            "empty_detail": t("results.review_center.empty"),
            "disclaimer": t("results.review_center.disclaimer"),
        }

    def _enrich_review_evidence_item(
        self,
        item: dict[str, Any],
        *,
        readiness_text: str,
        risk_summary: dict[str, Any],
        coverage_gaps: str,
        analytics_text: str,
        lineage_text: str,
        source_kind_summary: str,
        coverage_summary: str,
        compatibility_summary: str,
    ) -> dict[str, Any]:
        payload = dict(item or {})
        item_status = str(payload.get("status") or "diagnostic_only")
        item_risk_level = (
            "high"
            if item_status == "failed"
            else "medium"
            if item_status == "degraded"
            else "low"
        )
        key_fields = [
            str(value).strip()
            for value in list(payload.get("key_fields", []) or [])
            if str(value).strip()
        ]
        artifact_paths = []
        for value in [payload.get("path"), *list(payload.get("artifact_paths", []) or [])]:
            text = str(value or "").strip()
            if text and text not in artifact_paths:
                artifact_paths.append(text)
        payload["detail_summary"] = str(payload.get("summary") or t("common.none"))
        payload["detail_risk"] = t(
            "results.review_center.detail.risk_line",
            item=t(f"results.review_center.risk.{item_risk_level}"),
            overall=str(risk_summary.get("level_display") or t("common.none")),
            status=str(payload.get("status_display") or t("common.none")),
            default=(
                f"{t(f'results.review_center.risk.{item_risk_level}')} | "
                f"{risk_summary.get('level_display') or t('common.none')} | "
                f"{payload.get('status_display') or t('common.none')}"
            ),
        )
        payload["detail_key_fields"] = key_fields
        payload["detail_artifact_paths"] = artifact_paths
        payload["detail_acceptance_hint"] = t(
            "results.review_center.detail.acceptance_hint",
            readiness=readiness_text,
            coverage=coverage_gaps,
            source=str(payload.get("evidence_source_display") or t("common.none")),
            state=str(payload.get("evidence_state_display") or t("common.none")),
            default=(
                f"{readiness_text} | {coverage_gaps} | "
                f"{payload.get('evidence_source_display') or t('common.none')} | "
                f"{payload.get('evidence_state_display') or t('common.none')}"
            ),
        )
        detail_fallbacks = {
            "detail_qc_summary": [],
            "detail_analytics_summary": [analytics_text, source_kind_summary, compatibility_summary],
            "detail_lineage_summary": [lineage_text, coverage_summary, compatibility_summary],
            "detail_spectral_summary": [t("results.spectral_quality.none")],
        }
        detail_qc_cards = self._normalize_review_cards(payload.get("detail_qc_cards"))
        if detail_qc_cards:
            payload["detail_qc_cards"] = detail_qc_cards
            payload["detail_qc_summary"] = self._merge_detail_lines(
                self._review_card_lines(detail_qc_cards),
                payload.get("detail_qc_summary"),
            )
        for detail_key in ("detail_qc_summary", "detail_analytics_summary", "detail_lineage_summary", "detail_spectral_summary"):
            detail_value = payload.get(detail_key)
            if isinstance(detail_value, (list, tuple)):
                lines = [str(entry).strip() for entry in detail_value if str(entry).strip()]
                if not lines:
                    lines = [line for line in detail_fallbacks.get(detail_key, []) if str(line).strip()]
                payload[detail_key] = lines or [t("common.none")]
                continue
            detail_text = str(detail_value or "").strip()
            if detail_text:
                payload[detail_key] = detail_text
                continue
            fallback_lines = [line for line in detail_fallbacks.get(detail_key, []) if str(line).strip()]
            payload[detail_key] = fallback_lines or [t("common.none")]
        return payload

    def _build_review_risk_summary(
        self,
        *,
        evidence_items: list[dict[str, Any]],
        acceptance_plan: dict[str, Any],
        acceptance_readiness_summary: dict[str, Any],
        analytics_digest: dict[str, Any],
        coverage_gaps: str,
    ) -> dict[str, Any]:
        failed_count = sum(1 for item in evidence_items if str(item.get("status") or "") == "failed")
        degraded_count = sum(1 for item in evidence_items if str(item.get("status") or "") == "degraded")
        diagnostic_only_count = sum(1 for item in evidence_items if str(item.get("status") or "") == "diagnostic_only")
        missing_conditions = len(list(acceptance_plan.get("missing_conditions") or []))
        analytics_health = str(analytics_digest.get("health") or "").strip().lower()
        readiness_blocked = not bool(acceptance_readiness_summary.get("simulated_readiness_only", True)) and bool(missing_conditions)
        if failed_count > 0 or missing_conditions > 0 or readiness_blocked:
            level = "high"
        elif degraded_count > 0 or diagnostic_only_count > 0 or analytics_health in {"attention", "warning"}:
            level = "medium"
        else:
            level = "low"
        summary = t(
            "results.review_center.risk.summary",
            level=t(f"results.review_center.risk.{level}"),
            failed=failed_count,
            degraded=degraded_count,
            diagnostic=diagnostic_only_count,
            missing=missing_conditions,
            coverage=coverage_gaps,
            default=(
                f"{t(f'results.review_center.risk.{level}')} | failed {failed_count}"
                f" | degraded {degraded_count} | diagnostic {diagnostic_only_count}"
                f" | missing {missing_conditions}"
            ),
        )
        return {
            "level": level,
            "level_display": t(f"results.review_center.risk.{level}"),
            "summary": self._humanize_ui_summary(summary),
            "failed_count": failed_count,
            "degraded_count": degraded_count,
            "diagnostic_only_count": diagnostic_only_count,
            "missing_conditions": missing_conditions,
        }

    def _collect_review_evidence(
        self,
        *,
        suite_summary: dict[str, Any],
        suite_analytics_summary: dict[str, Any],
        analytics_summary: dict[str, Any],
        spectral_quality_summary: dict[str, Any],
        lineage_summary: dict[str, Any],
        workbench_evidence_summary: dict[str, Any],
        offline_diagnostic_adapter_summary: dict[str, Any],
        multi_source_stability_evidence: dict[str, Any],
        state_transition_evidence: dict[str, Any],
        measurement_phase_coverage_report: dict[str, Any],
        scope_definition_pack: dict[str, Any],
        decision_rule_profile: dict[str, Any],
        reference_asset_registry: dict[str, Any],
        certificate_lifecycle_summary: dict[str, Any],
        scope_readiness_summary: dict[str, Any],
        certificate_readiness_summary: dict[str, Any],
        pre_run_readiness_gate: dict[str, Any],
        uncertainty_method_readiness_summary: dict[str, Any],
        audit_readiness_digest: dict[str, Any],
        compatibility_scan_summary: dict[str, Any],
        force_refresh: bool = False,
    ) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        started_at = time.perf_counter()
        diagnostics = self._new_review_center_diagnostics()
        items: list[dict[str, Any]] = []
        run_roots = self._review_center_roots(
            metrics=diagnostics,
            force_refresh=force_refresh,
        )
        suite_roots = self._review_center_roots(
            extra_paths=[
                Path(str(suite_summary.get("summary_json") or "")).parent,
                Path(str(suite_summary.get("summary_json") or "")).parent.parent,
            ],
            metrics=diagnostics,
            force_refresh=force_refresh,
        )
        compare_roots = self._review_center_roots(
            include_compare_root=True,
            metrics=diagnostics,
            force_refresh=force_refresh,
        )
        suite_paths = self._review_artifact_paths(
            "suite_summary.json",
            roots=suite_roots,
            explicit_paths=[Path(str(suite_summary.get("summary_json") or ""))] if str(suite_summary.get("summary_json") or "").strip() else None,
            limit=8,
            metrics=diagnostics,
            force_refresh=force_refresh,
        )
        for path in suite_paths:
            item = self._parse_review_artifact(path, evidence_type="suite", fallback_payload=suite_summary, fallback_digest=suite_analytics_summary)
            if item:
                items.append(item)

        parity_paths = self._review_artifact_paths(
            "summary_parity_report.json",
            roots=compare_roots,
            limit=8,
            metrics=diagnostics,
            force_refresh=force_refresh,
        )
        for path in parity_paths:
            item = self._parse_review_artifact(path, evidence_type="parity")
            if item:
                items.append(item)

        resilience_paths = self._review_artifact_paths(
            "export_resilience_report.json",
            roots=compare_roots,
            limit=8,
            metrics=diagnostics,
            force_refresh=force_refresh,
        )
        for path in resilience_paths:
            item = self._parse_review_artifact(path, evidence_type="resilience")
            if item:
                items.append(item)

        workbench_path = str(dict(workbench_evidence_summary.get("paths", {}) or {}).get("report_json", "") or "")
        workbench_paths = self._review_artifact_paths(
            "workbench_action_report.json",
            roots=run_roots,
            explicit_paths=[Path(workbench_path)] if workbench_path.strip() else None,
            limit=8,
            metrics=diagnostics,
            force_refresh=force_refresh,
        )
        for path in workbench_paths:
            item = self._parse_review_artifact(path, evidence_type="workbench")
            if item:
                items.append(item)
        stability_path = str(
            dict(multi_source_stability_evidence.get("artifact_paths") or {}).get("multi_source_stability_evidence")
            or multi_source_stability_evidence.get("path")
            or ""
        )
        stability_paths = self._review_artifact_paths(
            MULTI_SOURCE_STABILITY_EVIDENCE_FILENAME,
            roots=run_roots,
            explicit_paths=[Path(stability_path)] if stability_path.strip() else None,
            limit=8,
            metrics=diagnostics,
            force_refresh=force_refresh,
        )
        for path in stability_paths:
            item = self._parse_review_artifact(
                path,
                evidence_type="stability",
                fallback_payload=multi_source_stability_evidence,
            )
            if item:
                items.append(item)
        transition_path = str(
            dict(state_transition_evidence.get("artifact_paths") or {}).get("state_transition_evidence")
            or state_transition_evidence.get("path")
            or ""
        )
        transition_paths = self._review_artifact_paths(
            STATE_TRANSITION_EVIDENCE_FILENAME,
            roots=run_roots,
            explicit_paths=[Path(transition_path)] if transition_path.strip() else None,
            limit=8,
            metrics=diagnostics,
            force_refresh=force_refresh,
        )
        for path in transition_paths:
            item = self._parse_review_artifact(
                path,
                evidence_type="state_transition",
                fallback_payload=state_transition_evidence,
            )
            if item:
                items.append(item)
        measurement_phase_coverage_path = str(
            dict(measurement_phase_coverage_report.get("artifact_paths") or {}).get("measurement_phase_coverage_report")
            or measurement_phase_coverage_report.get("path")
            or ""
        )
        measurement_phase_coverage_paths = self._review_artifact_paths(
            MEASUREMENT_PHASE_COVERAGE_REPORT_FILENAME,
            roots=run_roots,
            explicit_paths=[Path(measurement_phase_coverage_path)] if measurement_phase_coverage_path.strip() else None,
            limit=8,
            metrics=diagnostics,
            force_refresh=force_refresh,
        )
        for path in measurement_phase_coverage_paths:
            item = self._parse_review_artifact(
                path,
                evidence_type="measurement_phase_coverage",
                fallback_payload=measurement_phase_coverage_report,
            )
            if item:
                items.append(item)
        compatibility_scan_path = str(
            dict(compatibility_scan_summary.get("artifact_paths") or {}).get("compatibility_scan_summary")
            or compatibility_scan_summary.get("path")
            or ""
        )
        compatibility_scan_paths = self._review_artifact_paths(
            "compatibility_scan_summary.json",
            roots=run_roots,
            explicit_paths=[Path(compatibility_scan_path)] if compatibility_scan_path.strip() else None,
            limit=8,
            metrics=diagnostics,
            force_refresh=force_refresh,
        )
        for path in compatibility_scan_paths:
            item = self._parse_review_artifact(
                path,
                evidence_type="artifact_compatibility",
                fallback_payload=compatibility_scan_summary,
            )
            if item:
                items.append(item)
        readiness_summary_payloads = [
            (
                recognition_readiness.SCOPE_DEFINITION_PACK_FILENAME,
                dict(scope_definition_pack or {}),
            ),
            (
                recognition_readiness.DECISION_RULE_PROFILE_FILENAME,
                dict(decision_rule_profile or {}),
            ),
            (
                recognition_readiness.REFERENCE_ASSET_REGISTRY_FILENAME,
                dict(reference_asset_registry or {}),
            ),
            (
                recognition_readiness.CERTIFICATE_LIFECYCLE_SUMMARY_FILENAME,
                dict(certificate_lifecycle_summary or {}),
            ),
            (
                recognition_readiness.SCOPE_READINESS_SUMMARY_FILENAME,
                dict(scope_readiness_summary or {}),
            ),
            (
                recognition_readiness.CERTIFICATE_READINESS_SUMMARY_FILENAME,
                dict(certificate_readiness_summary or {}),
            ),
            (
                recognition_readiness.PRE_RUN_READINESS_GATE_FILENAME,
                dict(pre_run_readiness_gate or {}),
            ),
            (
                recognition_readiness.UNCERTAINTY_METHOD_READINESS_SUMMARY_FILENAME,
                dict(uncertainty_method_readiness_summary or {}),
            ),
            (
                recognition_readiness.AUDIT_READINESS_DIGEST_FILENAME,
                dict(audit_readiness_digest or {}),
            ),
        ]
        for filename, fallback_payload in readiness_summary_payloads:
            explicit_path = str(
                dict(fallback_payload.get("artifact_paths") or {}).get(Path(filename).stem)
                or fallback_payload.get("path")
                or ""
            ).strip()
            readiness_paths = self._review_artifact_paths(
                filename,
                roots=run_roots,
                explicit_paths=[Path(explicit_path)] if explicit_path else None,
                limit=8,
                metrics=diagnostics,
                force_refresh=force_refresh,
            )
            for path in readiness_paths:
                item = self._parse_review_artifact(
                    path,
                    evidence_type="readiness_governance",
                    fallback_payload=fallback_payload,
                )
                if item:
                    items.append(item)

        analytics_path = str(analytics_summary.get("path") or analytics_summary.get("analytics_summary_json") or "")
        lineage_path = str(lineage_summary.get("path") or lineage_summary.get("lineage_summary_json") or "")
        analytics_paths = self._review_artifact_paths(
            "analytics_summary.json",
            roots=run_roots,
            explicit_paths=[Path(analytics_path)] if analytics_path.strip() else None,
            limit=8,
            metrics=diagnostics,
            force_refresh=force_refresh,
        )
        for path in analytics_paths:
            item = self._parse_review_artifact(
                path,
                evidence_type="analytics",
                fallback_payload=analytics_summary,
                fallback_digest=lineage_summary,
                fallback_spectral=spectral_quality_summary,
            )
            if item:
                items.append(item)
        if not analytics_paths and analytics_path.strip():
            item = self._parse_review_artifact(
                Path(analytics_path),
                evidence_type="analytics",
                fallback_payload=analytics_summary,
                fallback_digest=lineage_summary,
                fallback_spectral=spectral_quality_summary,
            )
            if item:
                items.append(item)
        elif not analytics_paths and lineage_path.strip():
            item = self._parse_review_artifact(
                Path(lineage_path),
                evidence_type="analytics",
                fallback_payload=analytics_summary,
                fallback_digest=lineage_summary,
                fallback_spectral=spectral_quality_summary,
            )
            if item:
                items.append(item)
        offline_primary_paths = [
            Path(str(path))
            for path in list(offline_diagnostic_adapter_summary.get("primary_artifact_paths") or [])
            if str(path or "").strip()
        ]
        offline_paths: list[Path] = []
        if offline_primary_paths:
            offline_paths = self._review_artifact_paths(
                "diagnostic_summary.json",
                roots=run_roots,
                explicit_paths=offline_primary_paths,
                limit=8,
                metrics=diagnostics,
                force_refresh=force_refresh,
            )
            if len(offline_paths) < len(offline_primary_paths):
                seen = {str(path) for path in offline_paths}
                for path in offline_primary_paths:
                    key = str(path)
                    if path.exists() and key not in seen:
                        seen.add(key)
                        offline_paths.append(path)
        for path in offline_paths:
            item = self._parse_review_artifact(path, evidence_type="offline_diagnostic")
            if item:
                items.append(item)
        diagnostics["elapsed_ms"] = int(round((time.perf_counter() - started_at) * 1000))
        diagnostics["cache_hit"] = bool(diagnostics.get("cache_hit", False))
        return (
            sorted(items, key=lambda item: float(item.get("sort_key", 0.0) or 0.0), reverse=True)[:20],
            diagnostics,
        )

    @staticmethod
    def _new_review_center_diagnostics() -> dict[str, Any]:
        return {
            "cache_hit": True,
            "scanned_root_count": 0,
            "scanned_candidate_count": 0,
            "elapsed_ms": 0,
            "scan_budget_used": 0,
        }

    @staticmethod
    def _merge_review_center_diagnostics(metrics: Optional[dict[str, Any]], *, cache_hit: bool) -> None:
        if metrics is None:
            return
        metrics["cache_hit"] = bool(metrics.get("cache_hit", True)) and bool(cache_hit)

    @staticmethod
    def _remember_review_center_cache(
        cache: dict[tuple[Any, ...], list[str]],
        key: tuple[Any, ...],
        value: list[str],
        *,
        limit: int,
    ) -> None:
        cache[key] = list(value)
        while len(cache) > max(1, int(limit)):
            cache.pop(next(iter(cache)))

    @staticmethod
    def _review_path_signature(path: Optional[Path]) -> tuple[str, float, str]:
        if path in (None, Path("")):
            return ("", 0.0, "missing")
        try:
            candidate = Path(path).expanduser()
        except Exception:
            return (str(path or ""), 0.0, "invalid")
        if not str(candidate or "").strip():
            return ("", 0.0, "missing")
        try:
            resolved = str(candidate.resolve())
        except Exception:
            resolved = str(candidate)
        try:
            timestamp = float(candidate.stat().st_mtime)
        except Exception:
            timestamp = 0.0
        kind = "file" if candidate.is_file() else "dir" if candidate.exists() else "missing"
        return (resolved, timestamp, kind)

    def _review_run_dir_signature(self) -> tuple[tuple[str, float, str], tuple[str, float, str]]:
        run_dir = Path(self.results_gateway.run_dir)
        return (
            self._review_path_signature(run_dir),
            self._review_path_signature(run_dir.parent),
        )

    def _review_recent_runs_signature(self) -> tuple[tuple[str, str, float], ...]:
        rows: list[tuple[str, str, float]] = []
        for item in list(self.get_recent_runs())[:REVIEW_CENTER_RECENT_RUN_LIMIT]:
            path = Path(str(item.get("path") or "")).expanduser()
            path_signature = self._review_path_signature(path)
            rows.append(
                (
                    path_signature[0],
                    str(item.get("opened_at") or ""),
                    path_signature[1],
                )
            )
        return tuple(rows)

    def _review_compare_root_signature(self) -> tuple[Any, ...]:
        root_signature = self._review_path_signature(VALIDATION_COMPARE_ROOT)
        child_rows: list[tuple[str, float, str]] = []
        if VALIDATION_COMPARE_ROOT.exists():
            try:
                children = sorted(
                    (child for child in VALIDATION_COMPARE_ROOT.iterdir()),
                    key=self._path_mtime,
                    reverse=True,
                )
            except Exception:
                children = []
            for child in children[:REVIEW_CENTER_COMPARE_SIGNATURE_LIMIT]:
                child_rows.append(
                    (
                        child.name,
                        self._path_mtime(child),
                        "file" if child.is_file() else "dir",
                    )
                )
        return root_signature + (tuple(child_rows),)

    def _review_center_roots_cache_key(
        self,
        *,
        include_compare_root: bool,
        extra_paths: Optional[list[Path]],
    ) -> tuple[Any, ...]:
        extra_signature = tuple(
            self._review_path_signature(Path(path))
            for path in list(extra_paths or [])
            if str(path or "").strip()
        )
        return (
            "review_center_roots",
            include_compare_root,
            self._review_run_dir_signature(),
            self._review_recent_runs_signature(),
            self._review_compare_root_signature(),
            extra_signature,
        )

    def _review_artifact_paths_cache_key(
        self,
        filename: str,
        *,
        roots: list[Path],
        explicit_paths: Optional[list[Path]],
        limit: int,
    ) -> tuple[Any, ...]:
        return (
            "review_artifact_paths",
            str(filename or "").strip(),
            int(limit),
            self._review_run_dir_signature(),
            self._review_recent_runs_signature(),
            self._review_compare_root_signature(),
            tuple(self._review_path_signature(path) for path in list(roots or [])),
            tuple(
                self._review_path_signature(Path(path))
                for path in list(explicit_paths or [])
                if str(path or "").strip()
            ),
        )

    @staticmethod
    def _bounded_review_artifact_scan(
        root: Path,
        filename: str,
        *,
        remaining_budget: int,
        match_limit: int,
    ) -> tuple[list[Path], int]:
        if remaining_budget <= 0 or not root.exists() or not root.is_dir():
            return [], 0
        matches: list[Path] = []
        budget_used = 0
        try:
            for current_root, dirnames, filenames in os.walk(root):
                budget_used += 1
                if budget_used > remaining_budget:
                    budget_used = remaining_budget
                    break
                dirnames.sort()
                filenames.sort()
                if filename in filenames:
                    matches.append(Path(current_root) / filename)
                    if len(matches) >= max(1, int(match_limit)):
                        break
        except Exception:
            return matches, budget_used
        return matches, budget_used

    def _review_center_roots(
        self,
        *,
        include_compare_root: bool = False,
        extra_paths: Optional[list[Path]] = None,
        metrics: Optional[dict[str, Any]] = None,
        force_refresh: bool = False,
    ) -> list[Path]:
        cache_key = self._review_center_roots_cache_key(
            include_compare_root=include_compare_root,
            extra_paths=extra_paths,
        )
        if not force_refresh and cache_key in self._review_center_roots_cache:
            self._merge_review_center_diagnostics(metrics, cache_hit=True)
            return [Path(path) for path in list(self._review_center_roots_cache.get(cache_key, []))]
        self._merge_review_center_diagnostics(metrics, cache_hit=False)
        roots: list[Path] = []
        seen: set[str] = set()

        def _remember(path: Optional[Path]) -> None:
            if path in (None, Path("")):
                return
            try:
                candidate = Path(path)
            except Exception:
                return
            if not str(candidate or "").strip():
                return
            if candidate.is_file():
                candidate = candidate.parent
            if not candidate.exists():
                return
            try:
                key = str(candidate.resolve())
            except Exception:
                key = str(candidate)
            if key in seen:
                return
            seen.add(key)
            roots.append(candidate)

        _remember(self.results_gateway.run_dir)
        _remember(self.results_gateway.run_dir.parent)
        for item in list(self.get_recent_runs())[:REVIEW_CENTER_RECENT_RUN_LIMIT]:
            _remember(Path(str(item.get("path") or "")).expanduser())
        parent = Path(self.results_gateway.run_dir).parent
        if parent.exists():
            try:
                sibling_dirs = sorted(
                    (child for child in parent.iterdir() if child.is_dir()),
                    key=self._path_mtime,
                    reverse=True,
                )
            except Exception:
                sibling_dirs = []
            for child in sibling_dirs[:REVIEW_CENTER_SIBLING_ROOT_LIMIT]:
                _remember(child)
        for path in list(extra_paths or []):
            _remember(path)
        if include_compare_root:
            _remember(VALIDATION_COMPARE_ROOT)
        self._remember_review_center_cache(
            self._review_center_roots_cache,
            cache_key,
            [str(path) for path in roots],
            limit=REVIEW_CENTER_ROOT_CACHE_LIMIT,
        )
        return roots

    def _review_artifact_paths(
        self,
        filename: str,
        *,
        roots: list[Path],
        explicit_paths: Optional[list[Path]] = None,
        limit: int = 4,
        metrics: Optional[dict[str, Any]] = None,
        force_refresh: bool = False,
    ) -> list[Path]:
        explicit_list = [Path(path) for path in list(explicit_paths or []) if str(path or "").strip()]
        cache_key = self._review_artifact_paths_cache_key(
            filename,
            roots=roots,
            explicit_paths=explicit_list,
            limit=limit,
        )
        if not force_refresh and cache_key in self._review_artifact_paths_cache:
            self._merge_review_center_diagnostics(metrics, cache_hit=True)
            return [Path(path) for path in list(self._review_artifact_paths_cache.get(cache_key, []))]
        self._merge_review_center_diagnostics(metrics, cache_hit=False)
        candidates: list[Path] = []
        seen: set[str] = set()

        def _remember(path: Path) -> None:
            if not path or not path.exists() or not path.is_file():
                return
            try:
                key = str(path.resolve())
            except Exception:
                key = str(path)
            if key in seen:
                return
            seen.add(key)
            candidates.append(path)

        for path in explicit_list:
            if metrics is not None:
                metrics["scanned_candidate_count"] = int(metrics.get("scanned_candidate_count", 0) or 0) + 1
            _remember(path)
        for root in list(roots or [])[:REVIEW_CENTER_SCAN_ROOT_LIMIT]:
            if metrics is not None:
                metrics["scanned_root_count"] = int(metrics.get("scanned_root_count", 0) or 0) + 1
            if not root.exists():
                continue
            if root.is_file():
                if metrics is not None:
                    metrics["scanned_candidate_count"] = int(metrics.get("scanned_candidate_count", 0) or 0) + 1
                if root.name == filename:
                    _remember(root)
                continue
            direct = root / filename
            if metrics is not None:
                metrics["scanned_candidate_count"] = int(metrics.get("scanned_candidate_count", 0) or 0) + 1
            _remember(direct)
            if len(candidates) >= max(1, int(limit)):
                continue
            remaining_budget = max(
                0,
                REVIEW_CENTER_SCAN_BUDGET - int(metrics.get("scan_budget_used", 0) or 0) if metrics is not None else REVIEW_CENTER_SCAN_BUDGET,
            )
            if remaining_budget <= 0:
                continue
            scanned_paths, budget_used = self._bounded_review_artifact_scan(
                root,
                filename,
                remaining_budget=remaining_budget,
                match_limit=max(int(limit) * 2, REVIEW_CENTER_SCAN_MATCH_LIMIT),
            )
            if metrics is not None:
                metrics["scan_budget_used"] = int(metrics.get("scan_budget_used", 0) or 0) + int(budget_used or 0)
            for path in scanned_paths:
                if metrics is not None:
                    metrics["scanned_candidate_count"] = int(metrics.get("scanned_candidate_count", 0) or 0) + 1
                _remember(path)
        resolved = sorted(candidates, key=self._path_mtime, reverse=True)[:limit]
        self._remember_review_center_cache(
            self._review_artifact_paths_cache,
            cache_key,
            [str(path) for path in resolved],
            limit=REVIEW_CENTER_ARTIFACT_CACHE_LIMIT,
        )
        return resolved

    def _parse_review_artifact(
        self,
        path: Path,
        *,
        evidence_type: str,
        fallback_payload: Optional[dict[str, Any]] = None,
        fallback_digest: Optional[dict[str, Any]] = None,
        fallback_spectral: Optional[dict[str, Any]] = None,
    ) -> dict[str, Any] | None:
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            payload = {}
        if not isinstance(payload, dict) or not payload:
            payload = dict(fallback_payload or {})
        if not payload:
            return None
        if evidence_type == "suite":
            return self._build_suite_review_item(payload, path, fallback_digest=dict(fallback_digest or {}))
        if evidence_type == "parity":
            return self._build_parity_review_item(payload, path)
        if evidence_type == "resilience":
            return self._build_resilience_review_item(payload, path)
        if evidence_type == "workbench":
            return self._build_workbench_review_item(payload, path)
        if evidence_type == "analytics":
            return self._build_analytics_review_item(
                payload,
                path,
                fallback_lineage=dict(fallback_digest or {}),
                fallback_spectral=dict(fallback_spectral or {}),
            )
        if evidence_type == "offline_diagnostic":
            return self._build_offline_diagnostic_review_item(payload, path)
        if evidence_type == "stability":
            return self._build_stability_review_item(payload, path)
        if evidence_type == "state_transition":
            return self._build_state_transition_review_item(payload, path)
        if evidence_type == "measurement_phase_coverage":
            return self._build_measurement_phase_coverage_review_item(payload, path)
        if evidence_type == "artifact_compatibility":
            return self._build_artifact_compatibility_review_item(payload, path)
        if evidence_type == "readiness_governance":
            return self._build_readiness_governance_review_item(payload, path)
        return None

    def _build_suite_review_item(
        self,
        payload: dict[str, Any],
        path: Path,
        *,
        fallback_digest: dict[str, Any],
    ) -> dict[str, Any]:
        counts = dict(payload.get("counts", {}) or {})
        digest = dict(payload.get("suite_digest", {}) or fallback_digest.get("digest", {}) or {})
        raw_status = "MATCH" if bool(payload.get("all_passed", False)) else "MISMATCH"
        summary = self._humanize_ui_summary(
            str(
                digest.get("summary")
                or t(
                    "results.review_digest.suite_summary_line",
                    suite=payload.get("suite", "--"),
                    passed=counts.get("passed", 0),
                    total=counts.get("total", 0),
                )
            )
        )
        detail = "\n".join(
            [
                f"{t('results.review_center.detail.summary')}: {summary}",
                f"{t('results.review_center.detail.source')}: {display_evidence_source(payload.get('evidence_source'), default=str(payload.get('evidence_source') or 'simulated_protocol'))}",
                f"{t('results.review_center.detail.state')}: {display_evidence_state(payload.get('evidence_state'), default=str(payload.get('evidence_state') or 'collected'))}",
                f"{t('results.review_center.detail.path')}: {path}",
                f"{t('results.review_center.detail.failed_cases')}: {', '.join(list(payload.get('failed_cases') or [])) or t('common.none')}",
                t("results.review_center.disclaimer"),
            ]
        )
        return self._review_entry(
            evidence_type="suite",
            path=path,
            generated_at=payload.get("generated_at"),
            summary=summary,
            detail_text=detail,
            raw_status=raw_status,
            status="passed" if raw_status == "MATCH" else "failed",
            source_kind="suite",
            evidence_source=str(payload.get("evidence_source") or "simulated_protocol"),
            evidence_state=str(payload.get("evidence_state") or "collected"),
            not_real_acceptance_evidence=True,
            key_fields=[
                str(payload.get("suite") or "--"),
                f"{counts.get('passed', 0)}/{counts.get('total', 0)}",
                display_evidence_source(payload.get("evidence_source"), default=str(payload.get("evidence_source") or "simulated_protocol")),
            ],
            artifact_paths=[str(path)],
        )

    def _build_parity_review_item(self, payload: dict[str, Any], path: Path) -> dict[str, Any]:
        summary_payload = dict(payload.get("summary", {}) or {})
        raw_status = str(payload.get("status") or "--")
        summary = self._humanize_ui_summary(
            t(
                "results.review_digest.parity_summary_line",
                matched=summary_payload.get("cases_matched", 0),
                total=summary_payload.get("cases_total", 0),
                failed=",".join(summary_payload.get("failed_cases", []) or []) or t("common.none"),
            )
        )
        detail = "\n".join(
            [
                f"{t('results.review_center.detail.summary')}: {summary}",
                f"{t('results.review_center.detail.status')}: {display_compare_status(raw_status, default=str(raw_status))}",
                f"{t('results.review_center.detail.source')}: {display_evidence_source(payload.get('evidence_source'), default=str(payload.get('evidence_source') or 'diagnostic'))}",
                f"{t('results.review_center.detail.path')}: {path}",
                f"{t('results.review_center.detail.failed_cases')}: {', '.join(summary_payload.get('failed_cases', []) or []) or t('common.none')}",
                t("results.review_center.disclaimer"),
            ]
        )
        return self._review_entry(
            evidence_type="parity",
            path=path,
            generated_at=payload.get("generated_at"),
            summary=summary,
            detail_text=detail,
            raw_status=raw_status,
            status="passed" if raw_status == "MATCH" else "failed",
            source_kind="run",
            evidence_source=str(payload.get("evidence_source") or "diagnostic"),
            evidence_state=str(payload.get("evidence_state") or "collected"),
            not_real_acceptance_evidence=bool(payload.get("not_real_acceptance_evidence", True)),
            key_fields=[
                f"{summary_payload.get('cases_matched', 0)}/{summary_payload.get('cases_total', 0)}",
                ",".join(summary_payload.get("failed_cases", []) or []) or t("common.none"),
                display_compare_status(raw_status, default=str(raw_status)),
            ],
            artifact_paths=[str(path)],
        )

    def _build_resilience_review_item(self, payload: dict[str, Any], path: Path) -> dict[str, Any]:
        cases = list(payload.get("cases", []) or [])
        matched = sum(1 for item in cases if str(item.get("status") or "") == "MATCH")
        raw_status = str(payload.get("status") or "--")
        summary = self._humanize_ui_summary(
            t(
                "results.review_digest.resilience_summary_line",
                matched=matched,
                total=len(cases),
            )
        )
        failing = [str(item.get("name") or "") for item in cases if str(item.get("status") or "") != "MATCH"]
        detail = "\n".join(
            [
                f"{t('results.review_center.detail.summary')}: {summary}",
                f"{t('results.review_center.detail.status')}: {display_compare_status(raw_status, default=str(raw_status))}",
                f"{t('results.review_center.detail.source')}: {display_evidence_source(payload.get('evidence_source'), default=str(payload.get('evidence_source') or 'diagnostic'))}",
                f"{t('results.review_center.detail.path')}: {path}",
                f"{t('results.review_center.detail.failed_cases')}: {', '.join(failing) or t('common.none')}",
                t("results.review_center.disclaimer"),
            ]
        )
        return self._review_entry(
            evidence_type="resilience",
            path=path,
            generated_at=payload.get("generated_at"),
            summary=summary,
            detail_text=detail,
            raw_status=raw_status,
            status="passed" if raw_status == "MATCH" else "failed",
            source_kind="run",
            evidence_source=str(payload.get("evidence_source") or "diagnostic"),
            evidence_state=str(payload.get("evidence_state") or "collected"),
            not_real_acceptance_evidence=bool(payload.get("not_real_acceptance_evidence", True)),
            key_fields=[
                f"{matched}/{len(cases)}",
                ", ".join(failing) or t("common.none"),
                display_compare_status(raw_status, default=str(raw_status)),
            ],
            artifact_paths=[str(path)],
        )

    def _build_workbench_review_item(self, payload: dict[str, Any], path: Path) -> dict[str, Any]:
        config_safety, config_safety_review = self._artifact_config_safety_snapshot(path.parent)
        payload = _normalize_workbench_evidence_payload(
            payload,
            config_safety=config_safety,
            config_safety_review=config_safety_review,
        )
        risk_level = str(payload.get("risk_level") or "low")
        status = "diagnostic_only" if risk_level == "low" else "degraded"
        evidence_source = _normalize_simulated_evidence_source(payload.get("evidence_source"))
        analytics_path = path.parent / "analytics_summary.json"
        analytics_payload = self._load_json_dict(analytics_path) if analytics_path.exists() else {}
        qc_review_summary = dict(payload.get("qc_review_summary") or {})
        qc_cards = self._normalize_review_cards(
            payload.get("qc_evidence_section") or qc_review_summary.get("evidence_section") or qc_review_summary.get("cards")
        )
        if qc_review_summary:
            qc_detail_summary = self._merge_detail_lines(
                self._review_card_lines(qc_cards),
                self._review_detail_lines(qc_review_summary.get("review_card_lines") or qc_review_summary.get("lines")),
            )
            if not qc_detail_summary:
                qc_detail_summary = self._review_detail_lines(qc_review_summary.get("summary"))
        else:
            qc_detail_summary = self._build_qc_review_detail_lines(
                qc_overview=dict(analytics_payload.get("qc_overview") or {}),
                unified_review_summary=dict(analytics_payload.get("unified_review_summary") or {}),
            )
        acceptance_level = str(payload.get("acceptance_level") or "offline_regression")
        promotion_state = str(payload.get("promotion_state") or "dry_run_only")
        config_safety = dict(payload.get("config_safety") or {})
        config_safety_review = dict(payload.get("config_safety_review") or {})
        config_detail_summary = self._config_safety_detail_lines(config_safety, config_safety_review)
        summary = self._humanize_ui_summary(
            str(payload.get("summary_line") or payload.get("route_relay_summary") or payload.get("reference_quality_summary") or t("common.none"))
        )
        detail = "\n".join(
            [
                f"{t('results.review_center.detail.summary')}: {summary}",
                f"{t('results.review_center.detail.status')}: {display_risk_level(risk_level)}",
                f"{t('results.review_center.detail.source')}: {display_evidence_source(evidence_source, default=evidence_source)}",
                f"{t('results.review_center.detail.state')}: {display_evidence_state(payload.get('evidence_state'), default=str(payload.get('evidence_state') or 'simulated_workbench'))}",
                f"{t('results.review_center.detail.current_device')}: {payload.get('current_device_display', '--')}",
                f"{t('results.review_center.detail.current_action')}: {payload.get('current_action_display', '--')}",
                f"{t('results.review_center.detail.acceptance_level', default='验收层级')}: {acceptance_level}",
                f"{t('results.review_center.detail.promotion_state', default='晋升状态')}: {promotion_state}",
                f"{t('results.review_center.detail.path')}: {path}",
                t("results.review_center.disclaimer"),
            ]
        )
        return self._review_entry(
            evidence_type="workbench",
            path=path,
            generated_at=payload.get("generated_at"),
            summary=summary,
            detail_text=detail,
            raw_status=risk_level,
            status=status,
            source_kind="workbench",
            evidence_source=evidence_source,
            evidence_state=str(payload.get("evidence_state") or "simulated_workbench"),
            not_real_acceptance_evidence=bool(payload.get("not_real_acceptance_evidence", True)),
            key_fields=[
                display_risk_level(risk_level),
                str(payload.get("current_device_display") or "--"),
                str(payload.get("current_action_display") or "--"),
                acceptance_level,
            ],
            artifact_paths=[
                str(path),
                str(dict(payload.get("paths", {}) or {}).get("snapshot_json") or ""),
                str(analytics_path) if analytics_path.exists() else "",
            ],
            detail_qc_summary=qc_detail_summary,
            detail_qc_cards=qc_cards,
            detail_analytics_summary=config_detail_summary,
        )

    @staticmethod
    def _review_detail_lines(value: Any) -> list[str]:
        if isinstance(value, (list, tuple)):
            return [str(item).strip() for item in value if str(item).strip()]
        text = str(value or "").strip()
        return [text] if text else []

    def _build_qc_review_detail_lines(
        self,
        *,
        qc_overview: dict[str, Any],
        unified_review_summary: dict[str, Any],
    ) -> list[str]:
        qc_summary = dict(unified_review_summary.get("qc_summary") or {})
        reviewer_digest = dict(qc_overview.get("reviewer_digest") or {})
        run_gate = dict(qc_overview.get("run_gate") or {})
        point_gate = dict(qc_overview.get("point_gate_summary") or {})
        decision_counts = dict(qc_overview.get("decision_counts") or {})
        route_decision_breakdown = dict(qc_overview.get("route_decision_breakdown") or {})
        reject_reason_taxonomy = list(qc_overview.get("reject_reason_taxonomy") or [])
        failed_check_taxonomy = list(qc_overview.get("failed_check_taxonomy") or [])
        reviewer_card = dict(qc_overview.get("reviewer_card") or qc_summary.get("reviewer_card") or {})
        if not reviewer_card or not self._review_detail_lines(reviewer_card.get("lines")):
            reviewer_card = build_qc_reviewer_card(
                reviewer_digest=reviewer_digest,
                run_gate=run_gate,
                point_gate_summary=point_gate,
                decision_counts=decision_counts,
                route_decision_breakdown=route_decision_breakdown,
                reject_reason_taxonomy=reject_reason_taxonomy,
                failed_check_taxonomy=failed_check_taxonomy,
            )
        summary_text = str(qc_summary.get("summary") or reviewer_digest.get("summary") or t("common.none"))
        explicit_lines = self._review_detail_lines(qc_summary.get("lines") or reviewer_card.get("lines"))
        reviewer_lines = self._review_detail_lines(reviewer_digest.get("lines"))
        flagged_routes = ", ".join(str(item) for item in list(point_gate.get("flagged_routes") or []) if str(item).strip()) or "--"
        top_reject_reason = str((reject_reason_taxonomy[0] or {}).get("code") or "--") if reject_reason_taxonomy else "--"
        top_failed_check = str((failed_check_taxonomy[0] or {}).get("code") or "--") if failed_check_taxonomy else "--"
        route_breakdown_lines = []
        for route_name, route_counts in sorted(route_decision_breakdown.items()):
            counts_payload = dict(route_counts or {})
            route_breakdown_lines.append(
                (
                    f"{t('results.review_center.detail.qc_route_breakdown', default='路由分布')} {route_name}: "
                    f"{t('pages.qc.level.pass', default='通过')} {int(counts_payload.get('pass', 0) or 0)} / "
                    f"{t('pages.qc.level.warn', default='预警')} {int(counts_payload.get('warn', 0) or 0)} / "
                    f"{t('pages.qc.level.reject', default='拒绝')} {int(counts_payload.get('reject', 0) or 0)} / "
                    f"{t('pages.qc.level.skipped', default='跳过')} {int(counts_payload.get('skipped', 0) or 0)}"
                )
            )
        taxonomy_fragments = []
        for item in reject_reason_taxonomy[:2]:
            taxonomy_fragments.append(
                f"{str(item.get('code') or '--')}({int(item.get('count', 0) or 0)})"
            )
        taxonomy_summary = ", ".join(taxonomy_fragments) or "--"
        failed_check_fragments = []
        for item in failed_check_taxonomy[:2]:
            failed_check_fragments.append(
                f"{str(item.get('code') or '--')}({int(item.get('count', 0) or 0)})"
            )
        failed_check_summary = ", ".join(failed_check_fragments) or "--"
        lines = self._review_detail_lines(reviewer_card.get("lines")) or [
            f"{t('results.review_center.detail.qc_summary', default='质控摘要')}: {summary_text}",
            f"{t('results.review_center.detail.qc_reviewer_card', default='审阅卡片')}: {summary_text}",
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
            *route_breakdown_lines[:2],
            (
                f"{t('results.review_center.detail.qc_reject_taxonomy', default='拒绝原因分类')}: {taxonomy_summary} | "
                f"{t('results.review_center.detail.qc_failed_check_taxonomy', default='失败检查分类')}: {failed_check_summary}"
            ),
        ]
        for line in explicit_lines[:3]:
            if line not in lines:
                lines.append(f"{t('results.review_center.detail.qc_reviewer_note', default='审阅结论')}: {line}")
        for line in reviewer_lines[:2]:
            if line not in lines:
                lines.append(line)
        boundary_note = t(
            "results.review_center.detail.qc_boundary",
            default="证据边界: 仅供 simulation/offline 审阅，不代表 real acceptance evidence。",
        )
        if boundary_note not in lines:
            lines.append(boundary_note)
        return [str(item).strip() for item in lines if str(item).strip()]

    def _build_analytics_review_item(
        self,
        payload: dict[str, Any],
        path: Path,
        *,
        fallback_lineage: dict[str, Any],
        fallback_spectral: dict[str, Any],
    ) -> dict[str, Any]:
        digest = dict(payload.get("digest", {}) or {})
        reference_stats = dict(payload.get("reference_quality_statistics", {}) or {})
        export_status = dict(payload.get("export_resilience_status", {}) or {})
        qc_overview = dict(payload.get("qc_overview", {}) or {})
        drift_summary = dict(payload.get("drift_summary", {}) or {})
        control_chart_summary = dict(payload.get("control_chart_summary", {}) or {})
        analyzer_health_digest = dict(payload.get("analyzer_health_digest", {}) or {})
        fault_attribution_summary = dict(payload.get("fault_attribution_summary", {}) or {})
        unified_review_summary = dict(payload.get("unified_review_summary", {}) or {})
        lineage_path = path.parent / "lineage_summary.json"
        lineage_payload = self._load_json_dict(lineage_path) if lineage_path.exists() else dict(fallback_lineage or {})
        spectral_path = path.parent / "spectral_quality_summary.json"
        spectral_payload = self._load_json_dict(spectral_path) if spectral_path.exists() else dict(fallback_spectral or {})
        evidence_source = _normalize_simulated_evidence_source(payload.get("evidence_source"))
        config_safety, config_safety_review = self._artifact_config_safety_snapshot(path.parent)
        summary = self._humanize_ui_summary(
            str(
                unified_review_summary.get("summary")
                or digest.get("summary")
                or t(
                    "results.review_center.analytics.summary_line",
                    coverage=str(dict(payload.get("analyzer_coverage", {}) or {}).get("coverage_text") or "--"),
                    reference=display_reference_quality(reference_stats.get("reference_quality")),
                    exports=str(export_status.get("overall_status") or "--"),
                    lineage=str(lineage_payload.get("config_version") or "--"),
                    default="analytics summary",
                )
            )
        )
        health = str(
            digest.get("health")
            or export_status.get("overall_status")
            or reference_stats.get("reference_quality_trend")
            or "healthy"
        ).strip().lower()
        if health in {"failed", "error", "critical"}:
            status = "failed"
        elif health in {"attention", "warning", "degraded", "missing"}:
            status = "degraded"
        else:
            status = "diagnostic_only"
        lineage_summary = t(
            "results.review_center.lineage.summary_line",
            config=str(lineage_payload.get("config_version") or "--"),
            points=str(lineage_payload.get("points_version") or "--"),
            profile=str(lineage_payload.get("profile_version") or "--"),
        )
        qc_evidence_section = self._build_results_qc_evidence_section(
            analytics_summary=payload,
            workbench_evidence_summary={},
        )
        qc_detail_summary = self._merge_detail_lines(
            self._review_card_lines(qc_evidence_section),
            self._build_qc_review_detail_lines(
                qc_overview=qc_overview,
                unified_review_summary=unified_review_summary,
            ),
        )
        analytics_detail_summary = [
            summary,
            f"{t('results.review_center.detail.analytics_reference_quality')}: "
            f"{display_reference_quality(reference_stats.get('reference_quality'))}",
            f"{t('results.review_center.detail.analytics_reference_trend')}: "
            f"{str(reference_stats.get('reference_quality_trend') or '--')}",
            f"{t('results.review_center.detail.analytics_export_status')}: "
            f"{str(export_status.get('overall_status') or '--')}",
            f"{t('results.review_center.detail.analytics_drift', default='漂移趋势')}: "
            f"{str(drift_summary.get('overall_trend') or '--')}",
            f"{t('results.review_center.detail.analytics_control_chart', default='控制图状态')}: "
            f"{str(control_chart_summary.get('status') or '--')}",
            f"{t('results.review_center.detail.analytics_health', default='分析仪健康')}: "
            f"{str(analyzer_health_digest.get('overall_status') or '--')}",
            f"{t('results.review_center.detail.analytics_fault', default='主故障归因')}: "
            f"{str(fault_attribution_summary.get('primary_fault') or '--')}",
        ]
        for line in self._review_detail_lines(dict(unified_review_summary.get("analytics_summary") or {}).get("lines")):
            if line not in analytics_detail_summary:
                analytics_detail_summary.append(line)
        for note in list(unified_review_summary.get("reviewer_notes") or []):
            text = str(note or "").strip()
            if text:
                analytics_detail_summary.append(text)
        for line in self._config_safety_detail_lines(config_safety, config_safety_review):
            if line not in analytics_detail_summary:
                analytics_detail_summary.append(line)
        lineage_detail_summary = [
            lineage_summary,
            f"{t('results.review_center.detail.path')}: "
            f"{str(lineage_path) if lineage_path.exists() else t('common.none')}",
        ]
        spectral_detail_summary = self._build_spectral_quality_detail_lines(spectral_payload)
        detail = "\n".join(
            [
                f"{t('results.review_center.detail.summary')}: {summary}",
                f"{t('results.review_center.detail.status')}: {health or '--'}",
                f"{t('results.review_center.detail.source')}: {display_evidence_source(evidence_source, default=evidence_source)}",
                f"{t('results.review_center.detail.state')}: {display_evidence_state(payload.get('evidence_state'), default=str(payload.get('evidence_state') or 'collected'))}",
                f"{t('results.review_center.detail.path')}: {path}",
                f"{t('results.review_center.detail.qc_summary', default='质控摘要')}: {qc_detail_summary[0] if qc_detail_summary else t('common.none')}",
                f"{t('results.review_center.detail.qc_run_gate', default='运行门禁')}: {str(dict(qc_overview.get('run_gate') or {}).get('status') or '--')}",
                f"{t('results.review_center.detail.qc_point_gate', default='点级门禁')}: {str(dict(qc_overview.get('point_gate_summary') or {}).get('status') or '--')}",
                f"{t('results.review_center.detail.analytics_reference_quality')}: {display_reference_quality(reference_stats.get('reference_quality'))}",
                f"{t('results.review_center.detail.analytics_reference_trend')}: {str(reference_stats.get('reference_quality_trend') or '--')}",
                f"{t('results.review_center.detail.analytics_export_status')}: {str(export_status.get('overall_status') or '--')}",
                f"{t('results.review_center.detail.analytics_drift', default='漂移趋势')}: {str(drift_summary.get('overall_trend') or '--')}",
                f"{t('results.review_center.detail.analytics_control_chart', default='控制图状态')}: {str(control_chart_summary.get('status') or '--')}",
                f"{t('results.review_center.detail.analytics_health', default='分析仪健康')}: {str(analyzer_health_digest.get('overall_status') or '--')}",
                f"{t('results.review_center.detail.analytics_fault', default='主故障归因')}: {str(fault_attribution_summary.get('primary_fault') or '--')}",
                f"{t('results.review_center.detail.analytics_lineage')}: {lineage_summary}",
                f"{t('results.review_center.detail.spectral_status')}: {self._display_spectral_quality_status(spectral_payload.get('status'))}",
                t("results.review_center.disclaimer"),
            ]
        )
        return self._review_entry(
            evidence_type="analytics",
            path=path,
            generated_at=payload.get("generated_at") or lineage_payload.get("generated_at"),
            summary=summary,
            detail_text=detail,
            raw_status=health or "--",
            status=status,
            source_kind="run",
            evidence_source=evidence_source,
            evidence_state=str(payload.get("evidence_state") or "collected"),
            not_real_acceptance_evidence=bool(payload.get("not_real_acceptance_evidence", True)),
            key_fields=[
                str(dict(payload.get("analyzer_coverage", {}) or {}).get("coverage_text") or "--"),
                display_reference_quality(reference_stats.get("reference_quality")),
                str(export_status.get("overall_status") or "--"),
                str(lineage_payload.get("config_version") or "--"),
            ],
            artifact_paths=[
                str(path),
                str(lineage_path) if lineage_path.exists() else "",
                str(spectral_path) if spectral_path.exists() else "",
            ],
            detail_qc_summary=qc_detail_summary,
            detail_qc_cards=self._normalize_review_cards(qc_evidence_section),
            detail_analytics_summary=analytics_detail_summary,
            detail_lineage_summary=lineage_detail_summary,
            detail_spectral_summary=spectral_detail_summary,
        )

    @staticmethod
    def _offline_diagnostic_payload_paths(source_dir: Path, value: Any) -> list[str]:
        paths: list[str] = []

        def _collect(item: Any) -> None:
            if item in (None, ""):
                return
            if isinstance(item, dict):
                for child in item.values():
                    _collect(child)
                return
            if isinstance(item, (list, tuple, set)):
                for child in item:
                    _collect(child)
                return
            candidate = Path(str(item).strip()).expanduser()
            resolved = candidate if candidate.is_absolute() else source_dir / candidate
            paths.append(str(resolved))

        _collect(value)
        return paths

    @staticmethod
    def _offline_diagnostic_existing_paths(values: list[Any]) -> list[str]:
        normalized: list[str] = []
        seen: set[str] = set()
        for value in values:
            text = str(value or "").strip()
            if not text:
                continue
            path = Path(text)
            if not path.exists():
                continue
            resolved = str(path.resolve())
            if resolved in seen:
                continue
            seen.add(resolved)
            normalized.append(resolved)
        return normalized

    @staticmethod
    def _offline_diagnostic_scope_line(*, artifact_count: int, plot_count: int) -> str:
        return build_offline_diagnostic_scope_line_from_counts(
            artifact_count=artifact_count,
            plot_count=plot_count,
        )

    @classmethod
    def _offline_diagnostic_highlight_lines(
        cls,
        offline_diagnostic_adapter_summary: dict[str, Any] | None,
        *,
        limit: int = 3,
    ) -> list[str]:
        return collect_offline_diagnostic_detail_lines(offline_diagnostic_adapter_summary, limit=limit)

    @staticmethod
    def _offline_diagnostic_detail_item_line(item: Any) -> str:
        return build_offline_diagnostic_detail_item_line(item)

    @staticmethod
    def _normalize_offline_diagnostic_line(line: str) -> str:
        return normalize_offline_diagnostic_line(line)

    @staticmethod
    def _offline_diagnostic_scope_label() -> str:
        return offline_diagnostic_scope_label()

    def _build_offline_diagnostic_review_item(self, payload: dict[str, Any], path: Path) -> dict[str, Any]:
        if path.name == "isolation_comparison_summary.json":
            return self._build_analyzer_chain_diagnostic_review_item(payload, path)
        return self._build_room_temp_diagnostic_review_item(payload, path)

    def _build_room_temp_diagnostic_review_item(self, payload: dict[str, Any], path: Path) -> dict[str, Any]:
        source_dir = path.parent
        classification = str(payload.get("classification") or payload.get("status") or "--").strip() or "--"
        recommended_variant = str(
            payload.get("recommended_variant")
            or payload.get("recommended_route")
            or payload.get("recommended_mode")
            or "--"
        ).strip() or "--"
        dominant_error = str(
            payload.get("dominant_error")
            or payload.get("dominant_error_code")
            or payload.get("dominant_issue")
            or "--"
        ).strip() or "--"
        next_check = str(
            payload.get("recommended_next_check")
            or payload.get("next_check")
            or payload.get("recommendation")
            or "--"
        ).strip() or "--"
        classification_display = humanize_offline_diagnostic_detail_value("classification", classification)
        summary = self._humanize_ui_summary(
            str(
                payload.get("summary")
                or f"Room-temp diagnostic | classification {classification} | variant {recommended_variant} | dominant {dominant_error}"
            )
        )
        classification_key = classification.lower()
        if classification_key == "fail":
            status = "failed"
        elif classification_key in {"warn", "warning", "insufficient_evidence"}:
            status = "degraded"
        else:
            status = "diagnostic_only"
        evidence_source = str(payload.get("evidence_source") or "diagnostic").strip() or "diagnostic"
        if evidence_source.lower() in {"simulated", "simulated_protocol"}:
            evidence_source = _normalize_simulated_evidence_source(evidence_source)
        plot_artifact_paths = self._offline_diagnostic_existing_paths(
            self._offline_diagnostic_payload_paths(source_dir, payload.get("plot_files"))
        )
        artifact_paths = self._offline_diagnostic_existing_paths(
            [
                path,
                source_dir / "readable_report.md",
                source_dir / "diagnostic_workbook.xlsx",
                *plot_artifact_paths,
            ]
        )
        artifact_scope_line = self._offline_diagnostic_scope_line(
            artifact_count=len(artifact_paths),
            plot_count=len(plot_artifact_paths),
        )
        classification_line = build_offline_diagnostic_detail_line("classification", classification)
        variant_line = build_offline_diagnostic_detail_line("recommended_variant", recommended_variant)
        dominant_error_line = build_offline_diagnostic_detail_line("dominant_error", dominant_error)
        next_check_line = build_offline_diagnostic_detail_line("next_check", next_check)
        bundle_dir_line = build_offline_diagnostic_detail_line("bundle_dir", source_dir)
        primary_artifact_line = build_offline_diagnostic_detail_line("primary_artifact", path)
        analytics_detail_summary = [
            summary,
            artifact_scope_line,
            classification_line,
            variant_line,
            dominant_error_line,
            next_check_line,
        ]
        lineage_detail_summary = [
            artifact_scope_line,
            bundle_dir_line,
            primary_artifact_line,
        ]
        detail = "\n".join(
            [
                f"{t('results.review_center.detail.summary')}: {summary}",
                f"{t('results.review_center.detail.status')}: {classification_display}",
                f"{t('results.review_center.detail.source')}: {display_evidence_source(evidence_source, default=evidence_source)}",
                f"{t('results.review_center.detail.state')}: {display_evidence_state(payload.get('evidence_state'), default=str(payload.get('evidence_state') or 'collected'))}",
                f"{t('results.review_center.detail.path')}: {path}",
                artifact_scope_line,
                classification_line,
                variant_line,
                dominant_error_line,
                next_check_line,
                t("results.review_center.disclaimer"),
            ]
        )
        return self._review_entry(
            evidence_type="offline_diagnostic",
            path=path,
            generated_at=payload.get("generated_at"),
            summary=summary,
            detail_text=detail,
            raw_status=classification,
            status=status,
            source_kind="run",
            evidence_source=evidence_source,
            evidence_state=str(payload.get("evidence_state") or "collected"),
            not_real_acceptance_evidence=bool(payload.get("not_real_acceptance_evidence", True)),
            key_fields=[classification_display, recommended_variant, dominant_error, next_check],
            artifact_paths=artifact_paths,
            detail_analytics_summary=analytics_detail_summary,
            detail_lineage_summary=lineage_detail_summary,
        )

    def _build_analyzer_chain_diagnostic_review_item(self, payload: dict[str, Any], path: Path) -> dict[str, Any]:
        source_dir = path.parent
        should_continue_s1 = payload.get("should_continue_s1")
        continue_text = "--" if should_continue_s1 is None else ("continue" if bool(should_continue_s1) else "hold")
        dominant_conclusion = str(
            payload.get("dominant_conclusion")
            or payload.get("dominant_issue")
            or payload.get("dominant_delta")
            or "--"
        ).strip() or "--"
        recommendation = str(
            payload.get("recommended_next_check")
            or payload.get("next_check")
            or payload.get("recommendation")
            or "--"
        ).strip() or "--"
        continue_display = humanize_offline_diagnostic_detail_value("continue_s1", continue_text)
        summary = self._humanize_ui_summary(
            str(
                payload.get("summary")
                or f"Analyzer-chain isolation | continue_s1 {continue_text} | conclusion {dominant_conclusion} | next {recommendation}"
            )
        )
        status = "diagnostic_only" if should_continue_s1 is not False else "degraded"
        evidence_source = str(payload.get("evidence_source") or "diagnostic").strip() or "diagnostic"
        if evidence_source.lower() in {"simulated", "simulated_protocol"}:
            evidence_source = _normalize_simulated_evidence_source(evidence_source)
        plot_artifact_paths = self._offline_diagnostic_existing_paths(
            self._offline_diagnostic_payload_paths(source_dir, payload.get("plot_files"))
        )
        artifact_paths = self._offline_diagnostic_existing_paths(
            [
                path,
                source_dir / "summary.json",
                source_dir / "readable_report.md",
                source_dir / "diagnostic_workbook.xlsx",
                source_dir / "operator_checklist.md",
                source_dir / "compare_vs_8ch.md",
                source_dir / "compare_vs_baseline.md",
                *plot_artifact_paths,
            ]
        )
        artifact_scope_line = self._offline_diagnostic_scope_line(
            artifact_count=len(artifact_paths),
            plot_count=len(plot_artifact_paths),
        )
        continue_line = build_offline_diagnostic_detail_line("continue_s1", continue_text)
        dominant_conclusion_line = build_offline_diagnostic_detail_line("dominant_conclusion", dominant_conclusion)
        recommended_next_check_line = build_offline_diagnostic_detail_line("recommended_next_check", recommendation)
        bundle_dir_line = build_offline_diagnostic_detail_line("bundle_dir", source_dir)
        primary_artifact_line = build_offline_diagnostic_detail_line("primary_artifact", path)
        analytics_detail_summary = [
            summary,
            artifact_scope_line,
            continue_line,
            dominant_conclusion_line,
            recommended_next_check_line,
        ]
        lineage_detail_summary = [
            artifact_scope_line,
            bundle_dir_line,
            primary_artifact_line,
        ]
        detail = "\n".join(
            [
                f"{t('results.review_center.detail.summary')}: {summary}",
                f"{t('results.review_center.detail.status')}: {continue_display}",
                f"{t('results.review_center.detail.source')}: {display_evidence_source(evidence_source, default=evidence_source)}",
                f"{t('results.review_center.detail.state')}: {display_evidence_state(payload.get('evidence_state'), default=str(payload.get('evidence_state') or 'collected'))}",
                f"{t('results.review_center.detail.path')}: {path}",
                artifact_scope_line,
                continue_line,
                dominant_conclusion_line,
                recommended_next_check_line,
                t("results.review_center.disclaimer"),
            ]
        )
        return self._review_entry(
            evidence_type="offline_diagnostic",
            path=path,
            generated_at=payload.get("generated_at"),
            summary=summary,
            detail_text=detail,
            raw_status=continue_text,
            status=status,
            source_kind="run",
            evidence_source=evidence_source,
            evidence_state=str(payload.get("evidence_state") or "collected"),
            not_real_acceptance_evidence=bool(payload.get("not_real_acceptance_evidence", True)),
            key_fields=[continue_display, dominant_conclusion, recommendation],
            artifact_paths=artifact_paths,
            detail_analytics_summary=analytics_detail_summary,
            detail_lineage_summary=lineage_detail_summary,
        )

    def _build_stability_review_item(self, payload: dict[str, Any], path: Path) -> dict[str, Any]:
        review_surface = dict(payload.get("review_surface") or {})
        digest = dict(payload.get("digest") or {})
        summary = self._humanize_ui_summary(
            str(
                digest.get("summary")
                or review_surface.get("summary_text")
                or payload.get("coverage_status")
                or "multi-source stability shadow evaluation"
            )
        )
        detail_lines = [
            f"{t('results.review_center.detail.summary')}: {summary}",
            f"{t('results.review_center.detail.status')}: {t(f'results.review_center.status.{str(payload.get('overall_status') or 'diagnostic_only')}', default=str(payload.get('overall_status') or 'diagnostic_only'))}",
            f"{t('results.review_center.detail.source')}: {display_evidence_source(payload.get('evidence_source'), default=str(payload.get('evidence_source') or 'simulated_protocol'))}",
            f"{t('results.review_center.detail.state')}: {display_evidence_state(payload.get('evidence_state'), default=str(payload.get('evidence_state') or 'shadow_only'))}",
            f"{t('results.review_center.detail.path')}: {path}",
            *[str(item) for item in list(review_surface.get("summary_lines") or []) if str(item).strip()],
            *[str(item) for item in list(review_surface.get("detail_lines") or []) if str(item).strip()],
            t("results.review_center.disclaimer"),
        ]
        artifact_paths = [
            str(path),
            *[
                str(item)
                for item in dict(payload.get("artifact_paths") or {}).values()
                if str(item).strip()
            ],
        ]
        return self._review_entry(
            evidence_type="stability",
            path=path,
            generated_at=payload.get("generated_at"),
            summary=summary,
            detail_text="\n".join(detail_lines),
            raw_status=str(payload.get("coverage_status") or payload.get("overall_status") or "diagnostic_only"),
            status=str(payload.get("overall_status") or "diagnostic_only"),
            source_kind="run",
            evidence_source=str(payload.get("evidence_source") or "simulated_protocol"),
            evidence_state=str(payload.get("evidence_state") or "shadow_only"),
            not_real_acceptance_evidence=bool(payload.get("not_real_acceptance_evidence", True)),
            key_fields=[
                str(digest.get("policy_summary") or ""),
                str(digest.get("coverage_summary") or ""),
                str(digest.get("decision_summary") or ""),
            ],
            artifact_paths=artifact_paths,
            detail_analytics_summary=list(review_surface.get("summary_lines") or []),
            detail_lineage_summary=[
                str(digest.get("gap_summary") or ""),
                *[str(item) for item in list(review_surface.get("detail_lines") or []) if str(item).strip()],
            ],
            phase_filters=list(review_surface.get("phase_filters") or []),
            artifact_role_filters=["diagnostic_analysis"],
            evidence_category_filters=["measurement_core", "shadow_stability"],
            boundary_filter_rows=[dict(item) for item in list(review_surface.get("boundary_filter_rows") or []) if isinstance(item, dict)],
            boundary_filters=list(review_surface.get("boundary_filters") or []),
            anchor_id=str(review_surface.get("anchor_id") or ""),
            anchor_label=str(review_surface.get("anchor_label") or ""),
            route_filters=list(review_surface.get("route_filters") or []),
            signal_family_filters=list(review_surface.get("signal_family_filters") or []),
            decision_result_filters=list(review_surface.get("decision_result_filters") or []),
            policy_version_filters=list(review_surface.get("policy_version_filters") or []),
            evidence_source_filters=list(review_surface.get("evidence_source_filters") or []),
            non_claim_filter_rows=[dict(item) for item in list(review_surface.get("non_claim_filter_rows") or []) if isinstance(item, dict)],
            non_claim_filters=list(review_surface.get("non_claim_filters") or []),
            phase_contrast_filter_rows=[dict(item) for item in list(review_surface.get("phase_contrast_filter_rows") or []) if isinstance(item, dict)],
            phase_contrast_filters=list(review_surface.get("phase_contrast_filters") or []),
        )

    def _build_state_transition_review_item(self, payload: dict[str, Any], path: Path) -> dict[str, Any]:
        review_surface = dict(payload.get("review_surface") or {})
        digest = dict(payload.get("digest") or {})
        summary = self._humanize_ui_summary(
            str(
                digest.get("summary")
                or review_surface.get("summary_text")
                or payload.get("overall_status")
                or "controlled-flex state transition trace"
            )
        )
        detail_lines = [
            f"{t('results.review_center.detail.summary')}: {summary}",
            f"{t('results.review_center.detail.status')}: {t(f'results.review_center.status.{str(payload.get('overall_status') or 'diagnostic_only')}', default=str(payload.get('overall_status') or 'diagnostic_only'))}",
            f"{t('results.review_center.detail.source')}: {display_evidence_source(payload.get('evidence_source'), default=str(payload.get('evidence_source') or 'simulated_protocol'))}",
            f"{t('results.review_center.detail.state')}: {display_evidence_state(payload.get('evidence_state'), default=str(payload.get('evidence_state') or 'shadow_only'))}",
            f"{t('results.review_center.detail.path')}: {path}",
            *[str(item) for item in list(review_surface.get("summary_lines") or []) if str(item).strip()],
            *[str(item) for item in list(review_surface.get("detail_lines") or []) if str(item).strip()],
            t("results.review_center.disclaimer"),
        ]
        artifact_paths = [
            str(path),
            *[
                str(item)
                for item in dict(payload.get("artifact_paths") or {}).values()
                if str(item).strip()
            ],
        ]
        return self._review_entry(
            evidence_type="state_transition",
            path=path,
            generated_at=payload.get("generated_at"),
            summary=summary,
            detail_text="\n".join(detail_lines),
            raw_status=str(payload.get("overall_status") or "diagnostic_only"),
            status=str(payload.get("overall_status") or "diagnostic_only"),
            source_kind="run",
            evidence_source=str(payload.get("evidence_source") or "simulated_protocol"),
            evidence_state=str(payload.get("evidence_state") or "shadow_only"),
            not_real_acceptance_evidence=bool(payload.get("not_real_acceptance_evidence", True)),
            key_fields=[
                str(digest.get("transition_summary") or ""),
                str(digest.get("recovery_summary") or ""),
                f"illegal {len(list(payload.get('illegal_transitions') or []))}",
            ],
            artifact_paths=artifact_paths,
            detail_analytics_summary=list(review_surface.get("summary_lines") or []),
            detail_lineage_summary=[
                str(digest.get("boundary_summary") or ""),
                *[str(item) for item in list(review_surface.get("detail_lines") or []) if str(item).strip()],
            ],
            phase_filters=list(review_surface.get("phase_filters") or []),
            artifact_role_filters=["diagnostic_analysis"],
            evidence_category_filters=["measurement_core", "controlled_transition"],
            boundary_filter_rows=[dict(item) for item in list(review_surface.get("boundary_filter_rows") or []) if isinstance(item, dict)],
            boundary_filters=list(review_surface.get("boundary_filters") or []),
            anchor_id=str(review_surface.get("anchor_id") or ""),
            anchor_label=str(review_surface.get("anchor_label") or ""),
            route_filters=list(review_surface.get("route_filters") or []),
            signal_family_filters=list(review_surface.get("signal_family_filters") or []),
            decision_result_filters=list(review_surface.get("decision_result_filters") or []),
            policy_version_filters=list(review_surface.get("policy_version_filters") or []),
            evidence_source_filters=list(review_surface.get("evidence_source_filters") or []),
            non_claim_filter_rows=[dict(item) for item in list(review_surface.get("non_claim_filter_rows") or []) if isinstance(item, dict)],
            non_claim_filters=list(review_surface.get("non_claim_filters") or []),
            phase_contrast_filter_rows=[dict(item) for item in list(review_surface.get("phase_contrast_filter_rows") or []) if isinstance(item, dict)],
            phase_contrast_filters=list(review_surface.get("phase_contrast_filters") or []),
        )

    def _build_measurement_phase_coverage_review_item(
        self,
        payload: dict[str, Any],
        path: Path,
    ) -> dict[str, Any]:
        review_surface = dict(payload.get("review_surface") or {})
        digest = dict(payload.get("digest") or {})
        localized_review_lines = build_measurement_review_digest_lines(payload)
        summary = self._humanize_ui_summary(
            str(
                digest.get("summary")
                or review_surface.get("summary_text")
                or payload.get("overall_status")
                or "measurement phase coverage"
            )
        )
        detail_lines = [
            f"{t('results.review_center.detail.summary')}: {summary}",
            f"{t('results.review_center.detail.status')}: {t(f'results.review_center.status.{str(payload.get('overall_status') or 'diagnostic_only')}', default=str(payload.get('overall_status') or 'diagnostic_only'))}",
            f"{t('results.review_center.detail.source')}: {display_evidence_source(payload.get('evidence_source'), default=str(payload.get('evidence_source') or 'simulated'))}",
            f"{t('results.review_center.detail.state')}: {display_evidence_state(payload.get('evidence_state'), default=str(payload.get('evidence_state') or 'shadow_only'))}",
            f"{t('results.review_center.detail.path')}: {path}",
            *[str(item) for item in list(localized_review_lines.get("summary_lines") or []) if str(item).strip()],
            *[str(item) for item in list(localized_review_lines.get("detail_lines") or []) if str(item).strip()],
            t("results.review_center.disclaimer"),
        ]
        artifact_paths = [
            str(path),
            *[
                str(item)
                for item in dict(payload.get("artifact_paths") or {}).values()
                if str(item).strip()
            ],
        ]
        return self._review_entry(
            evidence_type="measurement_phase_coverage",
            path=path,
            generated_at=payload.get("generated_at"),
            summary=summary,
            detail_text="\n".join(detail_lines),
            raw_status=str(payload.get("overall_status") or "diagnostic_only"),
            status=str(payload.get("overall_status") or "diagnostic_only"),
            source_kind="run",
            evidence_source=str(payload.get("evidence_source") or "simulated"),
            evidence_state=str(payload.get("evidence_state") or "shadow_only"),
            not_real_acceptance_evidence=bool(payload.get("not_real_acceptance_evidence", True)),
            key_fields=[
                str(digest.get("payload_phase_summary") or ""),
                str(digest.get("payload_complete_phase_summary") or ""),
                str(digest.get("payload_partial_phase_summary") or ""),
                str(digest.get("actual_phase_summary") or ""),
                str(digest.get("trace_only_phase_summary") or ""),
                str(digest.get("coverage_summary") or ""),
                str(digest.get("gap_summary") or ""),
                str(digest.get("next_required_artifacts_summary") or ""),
            ],
            artifact_paths=artifact_paths,
            detail_analytics_summary=list(localized_review_lines.get("summary_lines") or []),
            detail_lineage_summary=[
                self._humanize_ui_summary(str(digest.get("gap_summary") or "")),
                self._humanize_ui_summary(str(digest.get("readiness_impact_summary") or "")),
                self._humanize_ui_summary(str(digest.get("linked_readiness_summary") or "")),
                *[str(item) for item in list(localized_review_lines.get("detail_lines") or []) if str(item).strip()],
            ],
            phase_filters=list(review_surface.get("phase_filters") or []),
            artifact_role_filters=["diagnostic_analysis"],
            evidence_category_filters=["measurement_core", "phase_coverage"],
            boundary_filter_rows=[dict(item) for item in list(review_surface.get("boundary_filter_rows") or []) if isinstance(item, dict)],
            boundary_filters=list(review_surface.get("boundary_filters") or []),
            anchor_id=str(review_surface.get("anchor_id") or ""),
            anchor_label=str(review_surface.get("anchor_label") or ""),
            route_filters=list(review_surface.get("route_filters") or []),
            signal_family_filters=list(review_surface.get("signal_family_filters") or []),
            decision_result_filters=list(review_surface.get("decision_result_filters") or []),
            policy_version_filters=list(review_surface.get("policy_version_filters") or []),
            evidence_source_filters=list(review_surface.get("evidence_source_filters") or []),
            non_claim_filter_rows=[dict(item) for item in list(review_surface.get("non_claim_filter_rows") or []) if isinstance(item, dict)],
            non_claim_filters=list(review_surface.get("non_claim_filters") or []),
            phase_contrast_filter_rows=[dict(item) for item in list(review_surface.get("phase_contrast_filter_rows") or []) if isinstance(item, dict)],
            phase_contrast_filters=list(review_surface.get("phase_contrast_filters") or []),
        )

    def _build_artifact_compatibility_review_item(self, payload: dict[str, Any], path: Path) -> dict[str, Any]:
        review_surface = dict(payload.get("review_surface") or {})
        digest = dict(payload.get("digest") or {})
        compatibility_overview = dict(payload.get("compatibility_overview") or {})
        compatibility_rollup = dict(
            payload.get("compatibility_rollup")
            or compatibility_overview.get("compatibility_rollup")
            or {}
        )
        summary = self._humanize_ui_summary(
            str(
                digest.get("summary")
                or review_surface.get("summary_text")
                or payload.get("summary")
                or "artifact compatibility"
            )
        )
        detail_lines = [
            f"{t('results.review_center.detail.summary')}: {summary}",
            f"{t('results.review_center.detail.status')}: {str(payload.get('compatibility_status_display') or payload.get('compatibility_status') or '--')}",
            f"{t('results.review_center.detail.source')}: {display_evidence_source(payload.get('evidence_source'), default=str(payload.get('evidence_source') or 'simulated_protocol'))}",
            f"{t('results.review_center.detail.state')}: {display_evidence_state(payload.get('evidence_state'), default=str(payload.get('evidence_state') or 'shadow_only'))}",
            f"{t('results.review_center.detail.path')}: {path}",
            f"读取方式: {str(payload.get('current_reader_mode_display') or payload.get('current_reader_mode') or '--')}",
            f"兼容状态: {str(payload.get('compatibility_status_display') or payload.get('compatibility_status') or '--')}",
            f"建议动作: {'仅重建 reviewer/index sidecar' if bool(payload.get('regenerate_recommended', False)) else '当前 compatibility sidecar 已就绪'}",
            f"边界摘要: {str(payload.get('boundary_digest') or digest.get('boundary_summary') or '--')}",
            f"非主张摘要: {str(payload.get('non_claim_digest') or digest.get('non_claim_summary') or '--')}",
            "说明: regenerate/reindex 仅作用于 reviewer/index sidecar，不改写原始主证据",
            t("results.review_center.disclaimer"),
        ]
        if str(compatibility_overview.get("schema_contract_summary_display") or "").strip():
            detail_lines.insert(
                5,
                f"合同/Schema: {str(compatibility_overview.get('schema_contract_summary_display') or '--')}",
            )
        if str(compatibility_overview.get("regenerate_recommendation_display") or "").strip():
            detail_lines.append(
                f"兼容建议: {str(compatibility_overview.get('regenerate_recommendation_display') or '--')}"
            )
        if str(compatibility_rollup.get("rollup_summary_display") or "").strip():
            detail_lines.append(
                t(
                    "facade.results.result_summary.artifact_compatibility_rollup",
                    value=str(compatibility_rollup.get("rollup_summary_display") or "--"),
                    default="兼容性 rollup：{value}",
                )
            )
        if str(compatibility_overview.get("non_primary_boundary_display") or "").strip():
            detail_lines.append(
                f"兼容边界: {str(compatibility_overview.get('non_primary_boundary_display') or '--')}"
            )
        artifact_paths = [
            str(path),
            *[
                str(item)
                for item in dict(payload.get("artifact_paths") or {}).values()
                if str(item).strip()
            ],
        ]
        return self._review_entry(
            evidence_type="artifact_compatibility",
            path=path,
            generated_at=payload.get("generated_at"),
            summary=summary,
            detail_text="\n".join(detail_lines),
            raw_status=str(payload.get("compatibility_status") or "diagnostic_only"),
            status="diagnostic_only",
            source_kind="run",
            evidence_source=str(payload.get("evidence_source") or "simulated_protocol"),
            evidence_state=str(payload.get("evidence_state") or "shadow_only"),
            not_real_acceptance_evidence=bool(payload.get("not_real_acceptance_evidence", True)),
            key_fields=[
                str(payload.get("current_reader_mode_display") or payload.get("current_reader_mode") or ""),
                str(payload.get("compatibility_status_display") or payload.get("compatibility_status") or ""),
                str(compatibility_overview.get("schema_contract_summary_display") or ""),
                t(
                    "facade.results.result_summary.artifact_compatibility_rollup",
                    value=str(compatibility_rollup.get("rollup_summary_display") or ""),
                    default="兼容性 rollup：{value}",
                ),
                str(digest.get("regenerate_summary") or ""),
            ],
            artifact_paths=artifact_paths,
            detail_analytics_summary=[str(item) for item in list(review_surface.get("summary_lines") or []) if str(item).strip()],
            detail_lineage_summary=[
                self._humanize_ui_summary(str(item))
                for item in list(review_surface.get("detail_lines") or [])
                if str(item).strip()
            ],
            phase_filters=list(review_surface.get("phase_filters") or []),
            artifact_role_filters=list(review_surface.get("artifact_role_filters") or ["diagnostic_analysis", "execution_summary"]),
            evidence_category_filters=list(review_surface.get("evidence_category_filters") or ["artifact_compatibility"]),
            boundary_filter_rows=[dict(item) for item in list(review_surface.get("boundary_filter_rows") or []) if isinstance(item, dict)],
            boundary_filters=list(review_surface.get("boundary_filters") or []),
            anchor_id=str(review_surface.get("anchor_id") or ""),
            anchor_label=str(review_surface.get("anchor_label") or ""),
            evidence_source_filters=list(review_surface.get("evidence_source_filters") or []),
            non_claim_filter_rows=[dict(item) for item in list(review_surface.get("non_claim_filter_rows") or []) if isinstance(item, dict)],
            non_claim_filters=list(review_surface.get("non_claim_filters") or []),
        )

    def _build_readiness_governance_review_item(self, payload: dict[str, Any], path: Path) -> dict[str, Any]:
        review_surface = dict(payload.get("review_surface") or {})
        digest = dict(payload.get("digest") or {})
        localized_review_lines = build_readiness_review_digest_lines(payload)
        summary = self._humanize_ui_summary(
            str(
                digest.get("summary")
                or review_surface.get("summary_text")
                or payload.get("overall_status")
                or "recognition readiness governance"
            )
        )
        detail_lines = [
            f"{t('results.review_center.detail.summary')}: {summary}",
            f"{t('results.review_center.detail.status')}: {t(f'results.review_center.status.{str(payload.get('overall_status') or 'diagnostic_only')}', default=str(payload.get('overall_status') or 'diagnostic_only'))}",
            f"{t('results.review_center.detail.source')}: {display_evidence_source(payload.get('evidence_source'), default=str(payload.get('evidence_source') or 'simulated_protocol'))}",
            f"{t('results.review_center.detail.state')}: {display_evidence_state(payload.get('evidence_state'), default=str(payload.get('evidence_state') or 'reviewer_readiness_only'))}",
            f"{t('results.review_center.detail.path')}: {path}",
            *[str(item) for item in list(localized_review_lines.get("summary_lines") or []) if str(item).strip()],
            *[str(item) for item in list(localized_review_lines.get("detail_lines") or []) if str(item).strip()],
            t("results.review_center.disclaimer"),
        ]
        artifact_paths = [
            str(path),
            *[
                str(item)
                for item in dict(payload.get("artifact_paths") or {}).values()
                if str(item).strip()
            ],
        ]
        return self._review_entry(
            evidence_type="readiness_governance",
            path=path,
            generated_at=payload.get("generated_at"),
            summary=summary,
            detail_text="\n".join(detail_lines),
            raw_status=str(payload.get("overall_status") or "diagnostic_only"),
            status=str(payload.get("overall_status") or "diagnostic_only"),
            source_kind="run",
            evidence_source=str(payload.get("evidence_source") or "simulated_protocol"),
            evidence_state=str(payload.get("evidence_state") or "reviewer_readiness_only"),
            not_real_acceptance_evidence=bool(payload.get("not_real_acceptance_evidence", True)),
            key_fields=[
                str(digest.get("scope_overview_summary") or ""),
                str(digest.get("decision_rule_summary") or ""),
                str(digest.get("conformity_boundary_summary") or ""),
                str(digest.get("standard_family_summary") or ""),
                str(digest.get("required_evidence_categories_summary") or ""),
                str(digest.get("current_coverage_summary") or ""),
                str(digest.get("missing_evidence_summary") or ""),
                str(digest.get("blocker_summary") or ""),
                str(digest.get("artifact_hash_summary") or ""),
                str(digest.get("linked_measurement_phase_summary") or ""),
                str(digest.get("next_required_artifacts_summary") or ""),
            ],
            artifact_paths=artifact_paths,
            detail_analytics_summary=list(localized_review_lines.get("summary_lines") or []),
            detail_lineage_summary=[
                self._humanize_ui_summary(str(digest.get("missing_evidence_summary") or "")),
                self._humanize_ui_summary(str(digest.get("linked_measurement_phase_summary") or "")),
                self._humanize_ui_summary(str(digest.get("next_required_artifacts_summary") or "")),
                *[str(item) for item in list(localized_review_lines.get("detail_lines") or []) if str(item).strip()],
            ],
            phase_filters=list(review_surface.get("phase_filters") or []),
            artifact_role_filters=["diagnostic_analysis"],
            standard_family_filters=list(review_surface.get("standard_family_filters") or []),
            evidence_category_filters=list(payload.get("evidence_categories") or []),
            boundary_filter_rows=[dict(item) for item in list(review_surface.get("boundary_filter_rows") or []) if isinstance(item, dict)],
            boundary_filters=list(review_surface.get("boundary_filters") or []),
            anchor_id=str(review_surface.get("anchor_id") or ""),
            anchor_label=str(review_surface.get("anchor_label") or ""),
            route_filters=list(review_surface.get("route_filters") or []),
            signal_family_filters=list(review_surface.get("signal_family_filters") or []),
            decision_result_filters=list(review_surface.get("decision_result_filters") or []),
            policy_version_filters=list(review_surface.get("policy_version_filters") or []),
            evidence_source_filters=list(review_surface.get("evidence_source_filters") or []),
            non_claim_filter_rows=[dict(item) for item in list(review_surface.get("non_claim_filter_rows") or []) if isinstance(item, dict)],
            non_claim_filters=list(review_surface.get("non_claim_filters") or []),
            phase_contrast_filter_rows=[dict(item) for item in list(review_surface.get("phase_contrast_filter_rows") or []) if isinstance(item, dict)],
            phase_contrast_filters=list(review_surface.get("phase_contrast_filters") or []),
        )

    def _review_entry(
        self,
        *,
        evidence_type: str,
        path: Path,
        generated_at: Any,
        summary: str,
        detail_text: str,
        raw_status: str,
        status: str,
        source_kind: str,
        evidence_source: str,
        evidence_state: str,
        not_real_acceptance_evidence: bool,
        key_fields: Optional[list[str]] = None,
        artifact_paths: Optional[list[str]] = None,
        detail_qc_summary: Any = None,
        detail_qc_cards: Any = None,
        detail_analytics_summary: Any = None,
        detail_lineage_summary: Any = None,
        detail_spectral_summary: Any = None,
        phase_filters: Optional[list[str]] = None,
        artifact_role_filters: Optional[list[str]] = None,
        standard_family_filters: Optional[list[str]] = None,
        evidence_category_filters: Optional[list[str]] = None,
        boundary_filter_rows: Optional[list[dict[str, Any]]] = None,
        boundary_filters: Optional[list[str]] = None,
        anchor_id: str = "",
        anchor_label: str = "",
        route_filters: Optional[list[str]] = None,
        signal_family_filters: Optional[list[str]] = None,
        decision_result_filters: Optional[list[str]] = None,
        policy_version_filters: Optional[list[str]] = None,
        evidence_source_filters: Optional[list[str]] = None,
        non_claim_filter_rows: Optional[list[dict[str, Any]]] = None,
        non_claim_filters: Optional[list[str]] = None,
        phase_contrast_filter_rows: Optional[list[dict[str, Any]]] = None,
        phase_contrast_filters: Optional[list[str]] = None,
    ) -> dict[str, Any]:
        sort_key, display_time = self._review_time(generated_at, path)
        source_label = Path(path).parent.name or Path(path).name
        source_dir = str(Path(path).parent)
        normalized_source_kind = str(source_kind or "run").strip().lower() or "run"
        entry = {
            "available": True,
            "type": evidence_type,
            "type_display": t(
                f"results.review_center.type.{evidence_type}",
                default=evidence_type,
            ),
            "path": str(path),
            "source_id": source_dir or source_label,
            "source_label": source_label,
            "source_kind": normalized_source_kind,
            "source_scope": normalized_source_kind,
            "source_kind_display": t(
                f"results.review_center.source_kind.{normalized_source_kind}",
                default=normalized_source_kind,
            ),
            "source_dir": source_dir,
            "generated_at": str(generated_at or ""),
            "generated_at_display": display_time,
            "sort_key": sort_key,
            "summary": summary,
            "detail_text": detail_text,
            "detail_hint": t(
                "results.review_center.detail.source_hint",
                kind=t(
                    f"results.review_center.source_kind.{normalized_source_kind}",
                    default=normalized_source_kind,
                ),
                source=source_label,
                default=f"{t(f'results.review_center.source_kind.{normalized_source_kind}', default=normalized_source_kind)} / {source_label}",
            ),
            "status": status,
            "status_display": t(f"results.review_center.status.{status}"),
            "raw_status": raw_status,
            "evidence_source": evidence_source,
            "evidence_source_display": display_evidence_source(evidence_source, default=evidence_source),
            "evidence_state": evidence_state,
            "evidence_state_display": display_evidence_state(evidence_state, default=evidence_state),
            "not_real_acceptance_evidence": not_real_acceptance_evidence,
            "key_fields": [str(item).strip() for item in list(key_fields or []) if str(item).strip()],
            "artifact_paths": [str(item).strip() for item in list(artifact_paths or []) if str(item).strip()],
            "detail_qc_summary": detail_qc_summary,
            "detail_qc_cards": detail_qc_cards,
            "detail_analytics_summary": detail_analytics_summary,
            "detail_lineage_summary": detail_lineage_summary,
            "detail_spectral_summary": detail_spectral_summary,
            "phase_filters": [str(item).strip() for item in list(phase_filters or []) if str(item).strip()],
            "artifact_role_filters": [
                str(item).strip() for item in list(artifact_role_filters or []) if str(item).strip()
            ],
            "standard_family_filters": [
                str(item).strip() for item in list(standard_family_filters or []) if str(item).strip()
            ],
            "evidence_category_filters": [
                str(item).strip() for item in list(evidence_category_filters or []) if str(item).strip()
            ],
            "boundary_filter_rows": [dict(item) for item in list(boundary_filter_rows or []) if isinstance(item, dict)],
            "boundary_filters": [str(item).strip() for item in list(boundary_filters or []) if str(item).strip()],
            "anchor_id": str(anchor_id or "").strip(),
            "anchor_label": str(anchor_label or "").strip(),
            "route_filters": [str(item).strip() for item in list(route_filters or []) if str(item).strip()],
            "signal_family_filters": [
                str(item).strip() for item in list(signal_family_filters or []) if str(item).strip()
            ],
            "decision_result_filters": [
                str(item).strip() for item in list(decision_result_filters or []) if str(item).strip()
            ],
            "policy_version_filters": [
                str(item).strip() for item in list(policy_version_filters or []) if str(item).strip()
            ],
            "evidence_source_filters": [
                str(item).strip() for item in list(evidence_source_filters or []) if str(item).strip()
            ],
            "non_claim_filter_rows": [dict(item) for item in list(non_claim_filter_rows or []) if isinstance(item, dict)],
            "non_claim_filters": [str(item).strip() for item in list(non_claim_filters or []) if str(item).strip()],
            "phase_contrast_filter_rows": [
                dict(item) for item in list(phase_contrast_filter_rows or []) if isinstance(item, dict)
            ],
            "phase_contrast_filters": [str(item).strip() for item in list(phase_contrast_filters or []) if str(item).strip()],
        }
        return _apply_fragment_filter_contract(entry)

    def _build_review_index_summary(
        self,
        items: list[dict[str, Any]],
        *,
        diagnostics: Optional[dict[str, Any]] = None,
        compatibility_rollup: Optional[dict[str, Any]] = None,
        recognition_scope_rollup: Optional[dict[str, Any]] = None,
    ) -> dict[str, Any]:
        expected_types = ("suite", "parity", "resilience", "workbench", "analytics")
        source_groups: dict[str, dict[str, Any]] = {}
        status_priority = {"failed": 3, "degraded": 2, "diagnostic_only": 1, "passed": 0}
        source_kind_counts = Counter(str(item.get("source_kind") or "run") for item in items)
        for item in items:
            source_id = str(item.get("source_id") or item.get("source_dir") or item.get("source_label") or "").strip()
            source_label = str(item.get("source_label") or source_id or "").strip()
            if not source_id and not source_label:
                continue
            group = source_groups.setdefault(
                source_id or source_label,
                {
                    "source_id": source_id or source_label,
                    "source_label": source_label,
                    "source_dir": str(item.get("source_dir") or "").strip(),
                    "latest_sort": 0.0,
                    "latest_display": "--",
                    "types": set(),
                    "missing_types": set(expected_types),
                    "status": "passed",
                    "status_rank": -1,
                    "evidence_count": 0,
                    "source_scope_counts": Counter(),
                },
            )
            group["evidence_count"] = int(group.get("evidence_count", 0) or 0) + 1
            group["types"].add(str(item.get("type") or ""))
            source_scope = str(item.get("source_scope") or item.get("source_kind") or "run").strip().lower() or "run"
            scope_counts = group.get("source_scope_counts")
            if isinstance(scope_counts, Counter):
                scope_counts[source_scope] += 1
            if not str(group.get("source_dir") or "").strip():
                group["source_dir"] = str(item.get("source_dir") or "").strip()
            sort_key = float(item.get("sort_key", 0.0) or 0.0)
            if sort_key >= float(group.get("latest_sort", 0.0) or 0.0):
                group["latest_sort"] = sort_key
                group["latest_display"] = str(item.get("generated_at_display") or "--")
            status = str(item.get("status") or "passed")
            rank = status_priority.get(status, 0)
            if rank >= int(group.get("status_rank", -1) or -1):
                group["status"] = status
                group["status_rank"] = rank
        sources: list[dict[str, Any]] = []
        missing_by_type = {name: 0 for name in expected_types}
        for group in source_groups.values():
            types_present = [name for name in expected_types if name in set(group.get("types") or set())]
            missing_types = [name for name in expected_types if name not in set(group.get("types") or set())]
            scope_counts = group.get("source_scope_counts")
            if isinstance(scope_counts, Counter) and scope_counts:
                source_scope = str(scope_counts.most_common(1)[0][0]) if len(scope_counts) == 1 else "mixed"
            else:
                source_scope = "run"
            for missing_type in missing_types:
                missing_by_type[missing_type] += 1
            coverage_labels = [t(f"results.review_center.type.{name}") for name in types_present]
            missing_labels = [t(f"results.review_center.type.{name}") for name in missing_types]
            coverage_display = t(
                "results.review_center.index.coverage_line",
                count=len(types_present),
                total=len(expected_types),
                types=" / ".join(coverage_labels) or t("common.none"),
                default=f"{len(types_present)}/{len(expected_types)} | {' / '.join(coverage_labels) or t('common.none')}",
            )
            gaps_display = (
                t("results.review_center.index.gaps_none", default="缺口已补齐")
                if not missing_types
                else t(
                    "results.review_center.index.gaps_line",
                    missing=" / ".join(missing_labels),
                    default=f"缺 {', '.join(missing_labels)}",
                )
            )
            sources.append(
                {
                    "source_id": str(group.get("source_id") or group.get("source_label") or "--"),
                    "source_label": str(group.get("source_label") or "--"),
                    "source_dir": str(group.get("source_dir") or ""),
                    "source_scope": source_scope,
                    "source_scope_display": t(
                        f"results.review_center.source_kind.{source_scope}",
                        default=source_scope,
                    ),
                    "latest_display": str(group.get("latest_display") or "--"),
                    "coverage_display": coverage_display,
                    "gaps_display": gaps_display,
                    "status": str(group.get("status") or "passed"),
                    "status_display": t(f"results.review_center.status.{str(group.get('status') or 'passed')}"),
                    "evidence_count": int(group.get("evidence_count", 0) or 0),
                    "missing_types": missing_types,
                    "type_ids": types_present,
                    "latest_sort": float(group.get("latest_sort", 0.0) or 0.0),
                }
            )
        sources.sort(key=lambda item: float(item.get("latest_sort", 0.0) or 0.0), reverse=True)
        source_labels = set(source_groups)
        suite_count = sum(1 for item in items if str(item.get("type") or "") == "suite")
        parity_count = sum(1 for item in items if str(item.get("type") or "") == "parity")
        resilience_count = sum(1 for item in items if str(item.get("type") or "") == "resilience")
        workbench_count = sum(1 for item in items if str(item.get("type") or "") == "workbench")
        analytics_count = sum(1 for item in items if str(item.get("type") or "") == "analytics")
        complete_sources = sum(1 for item in sources if not list(item.get("missing_types") or []))
        gapped_sources = max(0, len(sources) - complete_sources)
        source_kind_summary = t(
            "results.review_center.index.source_kind_summary",
            run=int(source_kind_counts.get("run", 0) or 0),
            suite=int(source_kind_counts.get("suite", 0) or 0),
            workbench=int(source_kind_counts.get("workbench", 0) or 0),
            default=(
                f"run {int(source_kind_counts.get('run', 0) or 0)} | "
                f"suite {int(source_kind_counts.get('suite', 0) or 0)} | "
                f"workbench {int(source_kind_counts.get('workbench', 0) or 0)}"
            ),
        )
        gap_fragments = [
            t(
                "results.review_center.index.missing_type_count",
                type=t(f"results.review_center.type.{name}"),
                count=count,
                default=f"{t(f'results.review_center.type.{name}')} 缺 {count}",
            )
            for name, count in missing_by_type.items()
            if int(count or 0) > 0
        ]
        coverage_gaps_display = humanize_review_center_coverage_text(
            " | ".join(gap_fragments) if gap_fragments else t("results.review_center.index.gaps_none", default="No gaps")
        )
        coverage_summary = humanize_review_center_coverage_text(
            t(
            "results.review_center.index.coverage_summary",
            complete=complete_sources,
            gapped=gapped_sources,
            gaps=coverage_gaps_display,
            default=f"complete {complete_sources} | gapped {gapped_sources} | {coverage_gaps_display}",
        )
        )
        normalized_diagnostics = {
            "cache_hit": bool(dict(diagnostics or {}).get("cache_hit", False)),
            "scanned_root_count": int(dict(diagnostics or {}).get("scanned_root_count", 0) or 0),
            "scanned_candidate_count": int(dict(diagnostics or {}).get("scanned_candidate_count", 0) or 0),
            "elapsed_ms": int(dict(diagnostics or {}).get("elapsed_ms", 0) or 0),
            "scan_budget_used": int(dict(diagnostics or {}).get("scan_budget_used", 0) or 0),
        }
        diagnostics_summary = t(
            "results.review_center.index.diagnostics_summary",
            cache_hit=display_bool(normalized_diagnostics["cache_hit"]),
            roots=normalized_diagnostics["scanned_root_count"],
            candidates=normalized_diagnostics["scanned_candidate_count"],
            elapsed_ms=normalized_diagnostics["elapsed_ms"],
            budget=normalized_diagnostics["scan_budget_used"],
            default=(
                f"cache {display_bool(normalized_diagnostics['cache_hit'])} | "
                f"roots {normalized_diagnostics['scanned_root_count']} | "
                f"candidates {normalized_diagnostics['scanned_candidate_count']} | "
                f"elapsed {normalized_diagnostics['elapsed_ms']} ms | "
                f"budget {normalized_diagnostics['scan_budget_used']}"
            ),
        )
        compatibility_rollup_payload = dict(compatibility_rollup or {})
        compatibility_summary = self._humanize_ui_summary(
            t(
                "facade.results.result_summary.artifact_compatibility_rollup",
                value=str(compatibility_rollup_payload.get("rollup_summary_display") or t("common.none")),
                default="兼容性 rollup：{value}",
            )
        )
        recognition_scope_rollup_payload = dict(recognition_scope_rollup or {})
        recognition_scope_summary = self._humanize_ui_summary(
            t(
                "facade.results.result_summary.recognition_scope_rollup",
                value=str(recognition_scope_rollup_payload.get("rollup_summary_display") or t("common.none")),
                default="范围/规则 rollup：{value}",
            )
        )
        summary_text = t(
            "results.review_center.index.summary",
            runs=len(source_labels),
            suites=suite_count,
            parity=parity_count,
            resilience=resilience_count,
            workbench=workbench_count,
            analytics=analytics_count,
            default=(
                f"recent sources {len(source_labels)} | suites {suite_count} | parity {parity_count} | "
                f"resilience {resilience_count} | workbench {workbench_count} | analytics {analytics_count}"
            ),
        )
        return {
            "recent_runs": len(source_labels),
            "suite_count": suite_count,
            "parity_count": parity_count,
            "resilience_count": resilience_count,
            "workbench_count": workbench_count,
            "analytics_count": analytics_count,
            "complete_sources": complete_sources,
            "gapped_sources": gapped_sources,
            "missing_by_type": missing_by_type,
            "source_kind_counts": {
                "run": int(source_kind_counts.get("run", 0) or 0),
                "suite": int(source_kind_counts.get("suite", 0) or 0),
                "workbench": int(source_kind_counts.get("workbench", 0) or 0),
            },
            "diagnostics": normalized_diagnostics,
            "diagnostics_summary": diagnostics_summary,
            "compatibility_rollup": compatibility_rollup_payload,
            "compatibility_summary": compatibility_summary,
            "recognition_scope_rollup": recognition_scope_rollup_payload,
            "recognition_scope_summary": recognition_scope_summary,
            "coverage_gaps_display": coverage_gaps_display,
            "source_kind_summary": source_kind_summary,
            "coverage_summary": coverage_summary,
            "sources": sources[:8],
            "summary": "\n".join(
                fragment
                for fragment in (
                    summary_text,
                    source_kind_summary,
                    coverage_summary,
                    compatibility_summary,
                    recognition_scope_summary,
                    diagnostics_summary,
                )
                if str(fragment or "").strip()
            ),
        }

    def _review_time(self, generated_at: Any, path: Path) -> tuple[float, str]:
        raw = str(generated_at or "").strip()
        if raw:
            try:
                parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
                return parsed.timestamp(), parsed.strftime("%m-%d %H:%M")
            except Exception:
                pass
        timestamp = self._path_mtime(path)
        if timestamp > 0:
            return timestamp, datetime.fromtimestamp(timestamp).strftime("%m-%d %H:%M")
        return 0.0, "--"

    @staticmethod
    def _path_mtime(path: Path) -> float:
        try:
            return float(path.stat().st_mtime)
        except Exception:
            return 0.0

    @staticmethod
    def _load_json_dict(path: Path) -> dict[str, Any]:
        if not path.exists():
            return {}
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return {}
        return dict(payload) if isinstance(payload, dict) else {}

    def _build_spectral_quality_digest(self, payload: dict[str, Any]) -> dict[str, Any]:
        if not payload:
            return {}
        flags = [self._display_spectral_quality_flag(item) for item in list(payload.get("flags") or []) if str(item).strip()]
        return {
            "status": str(payload.get("status") or ""),
            "status_display": self._display_spectral_quality_status(payload.get("status")),
            "channel_count": int(payload.get("channel_count", 0) or 0),
            "ok_channel_count": int(payload.get("ok_channel_count", 0) or 0),
            "overall_score": payload.get("overall_score"),
            "flags": list(payload.get("flags") or []),
            "flags_display": flags,
            "summary": self._spectral_quality_summary_text(payload),
        }

    def _build_review_scope_spectral_payload(self, payload: dict[str, Any]) -> dict[str, Any]:
        if not payload:
            return {}
        channels = []
        for name, channel in sorted(dict(payload.get("channels") or {}).items())[:6]:
            channel_payload = dict(channel or {})
            channels.append(
                {
                    "channel": str(name),
                    "status": str(channel_payload.get("status") or ""),
                    "status_display": self._display_spectral_quality_status(channel_payload.get("status")),
                    "stability_score": channel_payload.get("stability_score"),
                    "low_freq_energy_ratio": channel_payload.get("low_freq_energy_ratio"),
                    "dominant_frequency_hz": channel_payload.get("dominant_frequency_hz"),
                    "flags": [str(item) for item in list(channel_payload.get("anomaly_flags") or []) if str(item).strip()],
                }
            )
        digest = self._build_spectral_quality_digest(payload)
        return {
            "status": digest.get("status"),
            "status_display": digest.get("status_display"),
            "channel_count": digest.get("channel_count"),
            "ok_channel_count": digest.get("ok_channel_count"),
            "overall_score": digest.get("overall_score"),
            "flags": list(payload.get("flags") or []),
            "flags_display": list(digest.get("flags_display") or []),
            "channels": channels,
            "not_real_acceptance_evidence": bool(payload.get("not_real_acceptance_evidence", True)),
        }

    def _spectral_quality_summary_text(self, payload: dict[str, Any]) -> str:
        if not payload:
            return ""
        flags = [self._display_spectral_quality_flag(item) for item in list(payload.get("flags") or []) if str(item).strip()]
        return self._humanize_ui_summary(
            t(
                "results.spectral_quality.summary_line",
                status=self._display_spectral_quality_status(payload.get("status")),
                ok=int(payload.get("ok_channel_count", 0) or 0),
                total=int(payload.get("channel_count", 0) or 0),
                score=self._format_spectral_number(payload.get("overall_score")),
                flags=" / ".join(flags) or t("common.none"),
                default=(
                    f"{self._display_spectral_quality_status(payload.get('status'))} | "
                    f"{int(payload.get('ok_channel_count', 0) or 0)}/{int(payload.get('channel_count', 0) or 0)} | "
                    f"{self._format_spectral_number(payload.get('overall_score'))}"
                ),
            )
        )

    def _build_spectral_quality_detail_lines(self, payload: dict[str, Any]) -> list[str]:
        if not payload:
            return [t("results.spectral_quality.none")]
        flags = [self._display_spectral_quality_flag(item) for item in list(payload.get("flags") or []) if str(item).strip()]
        lines = [
            self._spectral_quality_summary_text(payload) or t("results.spectral_quality.none"),
            t(
                "results.spectral_quality.detail.status_line",
                status=self._display_spectral_quality_status(payload.get("status")),
                default=f"status {self._display_spectral_quality_status(payload.get('status'))}",
            ),
            t(
                "results.spectral_quality.detail.channel_count_line",
                ok=int(payload.get("ok_channel_count", 0) or 0),
                total=int(payload.get("channel_count", 0) or 0),
                default=f"channels {int(payload.get('ok_channel_count', 0) or 0)}/{int(payload.get('channel_count', 0) or 0)}",
            ),
            t(
                "results.spectral_quality.detail.flags_line",
                flags=" / ".join(flags) or t("common.none"),
                default=f"flags {' / '.join(flags) or t('common.none')}",
            ),
        ]
        if payload.get("overall_score") is not None:
            lines.append(
                t(
                    "results.spectral_quality.detail.score_line",
                    score=self._format_spectral_number(payload.get("overall_score")),
                    default=f"score {self._format_spectral_number(payload.get('overall_score'))}",
                )
            )
        diagnostics = dict(payload.get("diagnostics") or {})
        if str(payload.get("status") or "").strip().lower() == "skipped" and str(diagnostics.get("error") or "").strip():
            lines.append(
                t(
                    "results.spectral_quality.detail.skipped_line",
                    error=str(diagnostics.get("error") or "").strip(),
                    default=f"skipped: {str(diagnostics.get('error') or '').strip()}",
                )
            )
        for name, channel in sorted(dict(payload.get("channels") or {}).items())[:4]:
            channel_payload = dict(channel or {})
            channel_flags = [
                self._display_spectral_quality_flag(item)
                for item in list(channel_payload.get("anomaly_flags") or [])
                if str(item).strip()
            ]
            lines.append(
                t(
                    "results.spectral_quality.channel_line",
                    name=str(name),
                    status=self._display_spectral_quality_status(channel_payload.get("status")),
                    score=self._format_spectral_number(channel_payload.get("stability_score")),
                    low_freq=self._format_spectral_number(channel_payload.get("low_freq_energy_ratio")),
                    dominant=self._format_spectral_number(channel_payload.get("dominant_frequency_hz")),
                    flags=" / ".join(channel_flags) or t("common.none"),
                    default=(
                        f"{name} | {self._display_spectral_quality_status(channel_payload.get('status'))} | "
                        f"{self._format_spectral_number(channel_payload.get('stability_score'))}"
                    ),
                )
            )
        return [str(line).strip() for line in lines if str(line).strip()]

    @staticmethod
    def _format_spectral_number(value: Any) -> str:
        if isinstance(value, bool):
            return "--"
        if isinstance(value, (int, float)):
            return f"{float(value):.3f}"
        return "--"

    @staticmethod
    def _display_spectral_quality_status(value: Any) -> str:
        status = str(value or "").strip().lower()
        if not status:
            return t("common.none")
        return t(f"results.spectral_quality.status.{status}", default=status)

    @staticmethod
    def _display_spectral_quality_flag(value: Any) -> str:
        flag = str(value or "").strip().lower()
        if not flag:
            return t("common.none")
        return t(f"results.spectral_quality.flag.{flag}", default=flag)

    def _summarize_suite_review(
        self,
        suite_summary: dict[str, Any],
        suite_analytics_summary: dict[str, Any],
    ) -> dict[str, Any]:
        if not suite_summary:
            external = self._summarize_external_review_artifact("suite_summary.json", kind="suite")
            if external.get("available"):
                return external
            return {"available": False, "summary": t("common.none")}
        counts = dict(suite_summary.get("counts", {}) or {})
        digest = dict(suite_summary.get("suite_digest", {}) or suite_analytics_summary.get("digest", {}) or {})
        summary_text = self._humanize_ui_summary(
            str(
                digest.get("summary")
                or t(
                    "results.review_digest.suite_summary_line",
                    suite=suite_summary.get("suite", "--"),
                    passed=counts.get("passed", 0),
                    total=counts.get("total", 0),
                    default=f"{suite_summary.get('suite', '--')} {counts.get('passed', 0)}/{counts.get('total', 0)} 通过",
                )
            )
        )
        return {
            "available": True,
            "status": "MATCH" if bool(suite_summary.get("all_passed", False)) else "MISMATCH",
            "summary": summary_text,
            "path": str(suite_summary.get("summary_json", "") or ""),
        }

    def _summarize_external_review_artifact(self, filename: str, *, kind: str) -> dict[str, Any]:
        path = self._latest_review_artifact_path(filename)
        if path is None:
            return {"available": False, "summary": t("results.review_digest.none", default="暂无"), "path": ""}
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return {"available": True, "summary": t("results.review_digest.unreadable", default="已找到但无法读取"), "path": str(path)}
        if not isinstance(payload, dict):
            return {"available": True, "summary": t("results.review_digest.unreadable", default="已找到但无法读取"), "path": str(path)}
        if kind == "suite":
            counts = dict(payload.get("counts", {}) or {})
            summary = t(
                "results.review_digest.suite_summary_line",
                suite=payload.get("suite", "--"),
                passed=counts.get("passed", 0),
                total=counts.get("total", 0),
                default=f"{payload.get('suite', '--')} {counts.get('passed', 0)}/{counts.get('total', 0)} 通过",
            )
            status = "MATCH" if bool(payload.get("all_passed", False)) else "MISMATCH"
        elif kind == "parity":
            summary_payload = dict(payload.get("summary", {}) or {})
            summary = t(
                "results.review_digest.parity_summary_line",
                matched=summary_payload.get("cases_matched", 0),
                total=summary_payload.get("cases_total", 0),
                failed=",".join(summary_payload.get("failed_cases", []) or []) or t("common.none"),
                default=f"{summary_payload.get('cases_matched', 0)}/{summary_payload.get('cases_total', 0)} 匹配",
            )
            status = str(payload.get("status", "--") or "--")
        else:
            cases = list(payload.get("cases", []) or [])
            matched = sum(1 for item in cases if str(item.get("status", "")) == "MATCH")
            summary = t(
                "results.review_digest.resilience_summary_line",
                matched=matched,
                total=len(cases),
                default=f"{matched}/{len(cases)} 项通过",
            )
            status = str(payload.get("status", "--") or "--")
        return {
            "available": True,
            "status": status,
            "status_display": display_compare_status(status, default=str(status)),
            "summary": self._humanize_ui_summary(summary),
            "path": str(path),
            "evidence_source": str(payload.get("evidence_source", "") or ""),
            "not_real_acceptance_evidence": bool(payload.get("not_real_acceptance_evidence", True)),
        }

    def _summarize_workbench_review(self, workbench_evidence_summary: dict[str, Any]) -> dict[str, Any]:
        if not workbench_evidence_summary:
            return {"available": False, "summary": t("results.review_digest.none", default="暂无")}
        evidence_source = _normalize_simulated_evidence_source(workbench_evidence_summary.get("evidence_source"))
        return {
            "available": True,
            "summary": self._humanize_ui_summary(str(workbench_evidence_summary.get("summary_line", "") or t("common.none"))),
            "path": str(dict(workbench_evidence_summary.get("paths", {}) or {}).get("report_json", "") or ""),
            "evidence_source": evidence_source,
            "not_real_acceptance_evidence": bool(workbench_evidence_summary.get("not_real_acceptance_evidence", True)),
            "acceptance_level": str(workbench_evidence_summary.get("acceptance_level", "") or ""),
            "promotion_state": str(workbench_evidence_summary.get("promotion_state", "") or ""),
            "evidence_state": str(workbench_evidence_summary.get("evidence_state", "") or ""),
        }

    def _latest_review_artifact_path(self, filename: str) -> Path | None:
        candidates = self._review_artifact_paths(
            filename,
            roots=self._review_center_roots(include_compare_root=True),
            limit=1,
        )
        if not candidates:
            return None
        return candidates[0]

    def get_devices_snapshot(self, *, run_snapshot: Optional[dict[str, Any]] = None) -> dict[str, Any]:
        run = run_snapshot or self.build_run_snapshot()
        rows = list(run.get("device_rows", []) or [])
        return {
            "rows": rows,
            "enabled_count": len(list(run.get("enabled_devices", []) or [])),
            "disabled_analyzers": list(run.get("disabled_analyzers", []) or []),
            "profile_skipped_devices": list(run.get("profile_skipped_devices", []) or []),
            "warning_count": len(list(run.get("warnings", []) or [])),
            "error_count": len(list(run.get("errors", []) or [])),
            "workbench": self.get_device_workbench_snapshot(),
        }

    def get_algorithms_snapshot(self, *, results_snapshot: Optional[dict[str, Any]] = None) -> dict[str, Any]:
        rows: list[dict[str, Any]] = []
        default_algorithm = str(self.config.algorithm.default_algorithm or "--")
        candidates = [str(item) for item in list(self.config.algorithm.candidates or [])]
        rows.append(
            {
                "algorithm": default_algorithm,
                "source": "config.algorithm",
                "source_display": "配置.algorithm",
                "status": "default",
                "status_display": "默认",
                "note": "Current default algorithm",
                "note_display": "当前默认算法",
            }
        )
        for candidate in candidates:
            if candidate == default_algorithm:
                continue
            rows.append(
                {
                    "algorithm": candidate,
                    "source": "config.algorithm",
                    "source_display": "配置.algorithm",
                    "status": "candidate",
                    "status_display": "候选",
                    "note": "Configured candidate",
                    "note_display": "已配置候选算法",
                }
            )
        rows.append(
            {
                "algorithm": str(self.config.coefficients.model or "--"),
                "source": "config.coefficients",
                "source_display": "配置.coefficients",
                "status": "enabled" if bool(self.config.coefficients.enabled) else "disabled",
                "status_display": "启用" if bool(self.config.coefficients.enabled) else "禁用",
                "note": "Coefficient fitting model",
                "note_display": "系数拟合模型",
            }
        )
        if bool(self.config.coefficients.auto_fit):
            rows.append(
                {
                    "algorithm": "ratio_poly_report",
                    "source": "artifacts",
                    "source_display": "工件",
                    "status": "active",
                    "status_display": "启用",
                    "note": "Ratio-poly export path enabled",
                    "note_display": "已启用 ratio-poly 导出路径",
                }
            )
        return {
            "default_algorithm": default_algorithm,
            "candidate_count": len(candidates),
            "candidates": candidates,
            "coefficient_model": str(self.config.coefficients.model or "--"),
            "auto_select": bool(self.config.algorithm.auto_select),
            "rows": rows,
        }

    def get_reports_snapshot(self, *, results_snapshot: Optional[dict[str, Any]] = None) -> dict[str, Any]:
        results = results_snapshot or self.build_results_snapshot()
        payload = self.results_gateway.read_reports_payload()
        payload["review_digest"] = dict(results.get("review_digest", {}) or {})
        payload["review_digest_text"] = str(results.get("review_digest_text", "") or "")
        payload["result_summary_text"] = str(results.get("result_summary_text", "") or payload.get("result_summary_text", "") or "")
        payload["measurement_core_summary_text"] = str(results.get("measurement_core_summary_text", "") or "")
        payload["review_center"] = dict(results.get("review_center", {}) or {})
        payload["qc_summary_text"] = str(results.get("qc_summary_text", "") or "")
        payload["qc_reviewer_card"] = dict(results.get("qc_reviewer_card", {}) or {})
        payload["qc_evidence_section"] = dict(results.get("qc_evidence_section", {}) or {})
        payload["qc_review_cards"] = [dict(item) for item in list(results.get("qc_review_cards", []) or []) if isinstance(item, dict)]
        payload["spectral_quality_summary"] = dict(results.get("spectral_quality_summary", {}) or {})
        payload["offline_diagnostic_adapter_summary"] = dict(
            results.get("offline_diagnostic_adapter_summary", {}) or payload.get("offline_diagnostic_adapter_summary", {}) or {}
        )
        payload["point_taxonomy_summary"] = dict(
            results.get("point_taxonomy_summary", {}) or payload.get("point_taxonomy_summary", {}) or {}
        )
        payload["measurement_phase_coverage_report"] = dict(
            results.get("measurement_phase_coverage_report", {})
            or payload.get("measurement_phase_coverage_report", {})
            or {}
        )
        payload["compatibility_scan_summary"] = dict(
            results.get("compatibility_scan_summary", {})
            or payload.get("compatibility_scan_summary", {})
            or {}
        )
        payload["run_artifact_index"] = dict(
            results.get("run_artifact_index", {})
            or payload.get("run_artifact_index", {})
            or {}
        )
        payload["config_safety"] = dict(results.get("config_safety", {}) or payload.get("config_safety", {}) or {})
        payload["config_safety_review"] = dict(
            results.get("config_safety_review", {}) or payload.get("config_safety_review", {}) or {}
        )
        payload["config_governance_handoff"] = dict(
            results.get("config_governance_handoff", {}) or payload.get("config_governance_handoff", {}) or {}
        )
        return payload

    def get_timeseries_snapshot(self, *, run_snapshot: Optional[dict[str, Any]] = None) -> dict[str, Any]:
        run = run_snapshot or self.build_run_snapshot()
        latest_sample = None
        samples = list(self.service.get_results() or [])
        if samples:
            latest_sample = samples[-1]
        current_point = self.session.current_point
        temperature_value = (
            getattr(latest_sample, "temperature_c", None)
            if latest_sample is not None
            else getattr(current_point, "temperature_c", None)
        )
        pressure_value = (
            getattr(latest_sample, "pressure_hpa", None)
            if latest_sample is not None
            else getattr(current_point, "pressure_hpa", None)
        )
        signal_value = getattr(latest_sample, "co2_signal", None) if latest_sample is not None else None

        for name, value in {
            "temperature_c": temperature_value,
            "pressure_hpa": pressure_value,
            "co2_signal": signal_value,
        }.items():
            if value is not None:
                self._timeseries_history[name].append(float(value))

        return {
            "window": 60,
            "series": {name: list(values) for name, values in self._timeseries_history.items()},
            "phase": run.get("phase", "--"),
        }

    def get_qc_overview_snapshot(self, *, qc_snapshot: Optional[dict[str, Any]] = None) -> dict[str, Any]:
        qc = qc_snapshot or self.build_qc_snapshot()
        total = int(qc.get("total_points", 0) or 0)
        valid = int(qc.get("valid_points", 0) or 0)
        invalid = int(qc.get("invalid_points", 0) or 0)
        return {
            "score": float(qc.get("overall_score", 0.0) or 0.0),
            "grade": str(qc.get("grade", "--") or "--"),
            "valid_points": valid,
            "invalid_points": invalid,
            "total_points": total,
            "valid_ratio": 0.0 if total <= 0 else valid / total,
        }

    def get_winner_snapshot(self, *, algorithms_snapshot: Optional[dict[str, Any]] = None) -> dict[str, Any]:
        algorithms = algorithms_snapshot or self.get_algorithms_snapshot()
        winner = str(algorithms.get("default_algorithm", "--") or "--")
        auto_select = bool(algorithms.get("auto_select", False))
        reason = t("facade.winner_auto_select_reason")
        if not auto_select:
            reason = t("facade.winner_configured_reason")
        status = "recommended" if auto_select else "configured"
        return {
            "winner": winner,
            "status": status,
            "status_display": display_winner_status(status),
            "reason": reason,
        }

    def get_export_snapshot(self, *, reports_snapshot: Optional[dict[str, Any]] = None) -> dict[str, Any]:
        reports = reports_snapshot or self.get_reports_snapshot()
        payload = dict(self._last_export_result)
        payload["artifact_count"] = len(list(reports.get("files", []) or []))
        return payload

    def get_route_progress_snapshot(self, *, run_snapshot: Optional[dict[str, Any]] = None) -> dict[str, Any]:
        run = run_snapshot or self.build_run_snapshot()
        route = str(run.get("route", "") or "")
        phase = str(run.get("route_phase", "") or "")
        return {
            "route": route,
            "route_display": display_route(route),
            "route_phase": phase,
            "route_phase_display": display_phase(phase),
            "points_completed": int(run.get("points_completed", 0) or 0),
            "points_total": int(run.get("points_total", 0) or 0),
            "source_point": str(run.get("source_point", "--") or "--"),
            "active_point": str(run.get("active_point", "--") or "--"),
            "steps": [t("widgets.route_progress.step_h2o"), t("widgets.route_progress.step_co2"), t("widgets.route_progress.step_finalize")],
        }

    def get_qc_reject_reason_snapshot(self, *, qc_snapshot: Optional[dict[str, Any]] = None) -> dict[str, Any]:
        qc = qc_snapshot or self.build_qc_snapshot()
        taxonomy = [dict(item) for item in list(qc.get("reject_reason_taxonomy", []) or [])]
        if taxonomy:
            rows = [
                {
                    "reason": str(item.get("code") or "unknown"),
                    "count": int(item.get("count", 0) or 0),
                    "category": str(item.get("category") or "other"),
                }
                for item in taxonomy
            ]
        else:
            counts: dict[str, int] = {}
            for reason in list(qc.get("invalid_reasons", []) or []):
                key = str(reason or "unknown")
                counts[key] = counts.get(key, 0) + 1
            rows = [{"reason": key, "count": value} for key, value in sorted(counts.items(), key=lambda item: (-item[1], item[0]))]
        return {"rows": rows}

    def get_residual_snapshot(
        self,
        *,
        results_snapshot: Optional[dict[str, Any]] = None,
        qc_snapshot: Optional[dict[str, Any]] = None,
    ) -> dict[str, Any]:
        _ = results_snapshot or self.build_results_snapshot()
        cleaned = list(self.service.get_cleaned_results() or [])
        base_residuals: list[float] = []
        for sample in cleaned:
            point = getattr(sample, "point", None)
            observed = getattr(sample, "co2_ppm", None)
            target = None if point is None else getattr(point, "co2_ppm", None)
            if observed is not None and target is not None:
                base_residuals.append(float(observed) - float(target))
        if not base_residuals:
            qc = qc_snapshot or self.build_qc_snapshot()
            for row in list(qc.get("point_rows", []) or []):
                quality = float(row.get("quality_score", 0.0) or 0.0)
                base_residuals.append(round((1.0 - quality) * 10.0, 3))

        candidates = [str(item) for item in list(self.config.algorithm.candidates or [])]
        series = []
        for index, algorithm in enumerate(candidates):
            scale = 1.0 + (index * 0.1)
            series.append(
                {
                    "algorithm": algorithm,
                    "residuals": [round(item * scale, 3) for item in base_residuals],
                }
            )
        return {"series": series}

    def get_analyzer_health_snapshot(
        self,
        *,
        devices_snapshot: Optional[dict[str, Any]] = None,
        run_snapshot: Optional[dict[str, Any]] = None,
    ) -> dict[str, Any]:
        devices = devices_snapshot or self.get_devices_snapshot()
        run = run_snapshot or self.build_run_snapshot()
        disabled = set(str(item) for item in list(devices.get("disabled_analyzers", []) or []))
        warning_count = int(devices.get("warning_count", 0) or 0)
        error_count = int(devices.get("error_count", 0) or 0)
        rows = []
        for row in list(devices.get("rows", []) or []):
            name = str(row.get("name", "--") or "--")
            status = str(row.get("status", "unknown") or "unknown")
            health = 95
            notes = []
            if name in disabled:
                health -= 55
                notes.append("disabled")
            if status == "skipped_by_profile":
                health = min(health, 100)
                notes.append("skipped_by_profile")
            if status != "online":
                if status != "skipped_by_profile":
                    health -= 30
                    notes.append(status)
            if warning_count:
                health -= min(20, warning_count * 5)
                notes.append(f"{warning_count} warning")
            if error_count:
                health -= min(30, error_count * 10)
                notes.append(f"{error_count} error")
            rows.append(
                {
                    "analyzer": name,
                    "status": status if name not in disabled else "disabled",
                    "status_display": display_device_status(status if name not in disabled else "disabled"),
                    "health": max(0, health),
                    "note": self._humanize_ui_summary(", ".join(notes)) if notes else "稳定",
                }
            )
        for name in sorted(disabled):
            if any(item["analyzer"] == name for item in rows):
                continue
            rows.append(
                {
                    "analyzer": name,
                    "status": "disabled",
                    "status_display": display_device_status("disabled"),
                    "health": 35,
                    "note": display_device_status("disabled"),
                }
            )
        return {"rows": rows, "route": run.get("route", "--")}

    def export_artifacts(self, export_format: str) -> dict[str, Any]:
        normalized = str(export_format or "all").strip().lower()
        if normalized not in {"json", "csv", "all"}:
            result = {"ok": False, "message": t("facade.unsupported_export_format", format=normalized)}
            self._last_export_result.update(last_export_message=result["message"])
            self._ui_error_message = result["message"]
            self._append_notification("error", result["message"])
            return result

        run_dir = self.result_store.run_dir
        destination = run_dir / "ui_exports" / normalized
        destination.mkdir(parents=True, exist_ok=True)
        copied: list[str] = []
        self._busy_message = t("facade.exporting", format=normalized)
        try:
            for source in sorted({Path(item) for item in self.results_gateway.list_output_files()}):
                if not source.exists() or not source.is_file():
                    continue
                suffix = source.suffix.lower()
                if normalized == "json" and suffix != ".json":
                    continue
                if normalized == "csv" and suffix != ".csv":
                    continue
                target = destination / source.name
                shutil.copy2(source, target)
                copied.append(str(target))
        except Exception as exc:
            message = t("facade.export_failed", error=exc)
            self._ui_error_message = message
            self._append_notification("error", message)
            self._last_export_result.update(last_export_message=message)
            return {"ok": False, "message": message}
        finally:
            self._busy_message = ""

        message = t("facade.exported", count=len(copied), destination=destination)
        self._last_export_result.update(
            {
                "available_formats": ["json", "csv", "all"],
                "last_export_message": message,
                "last_export_dir": str(destination),
                "last_export_batch_id": "",
                "last_export_files": list(copied),
            }
        )
        self._ui_error_message = ""
        self.log_ui(message)
        self._append_notification("success", message)
        return {
            "ok": True,
            "message": message,
            "directory": str(destination),
            "exported_files": copied,
        }

    def export_review_scope_manifest(self, *, selection: Optional[dict[str, Any]] = None) -> dict[str, Any]:
        reports_snapshot = self.get_reports_snapshot()
        destination = self.result_store.run_dir / "ui_exports" / "review_scope"
        destination.mkdir(parents=True, exist_ok=True)
        self._busy_message = t("facade.export_review_manifesting")
        try:
            payload = build_review_scope_manifest_payload(
                list(reports_snapshot.get("files", []) or []),
                selection=dict(selection or {"scope": "all"}),
                run_dir=str(self.result_store.run_dir),
            )
            spectral_quality = self._build_review_scope_spectral_payload(
                dict(reports_snapshot.get("spectral_quality_summary", {}) or {})
            )
            if spectral_quality:
                payload["spectral_quality"] = spectral_quality
            batch_id = build_review_scope_batch_id(
                destination,
                scope=dict(payload.get("scope_summary", {}) or {}).get("scope") or "all",
                generated_at=payload.get("generated_at"),
            )
            json_path = destination / f"{batch_id}.json"
            markdown_path = destination / f"{batch_id}.md"
            json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
            markdown_path.write_text(render_review_scope_manifest_markdown(payload), encoding="utf-8")
            index_payload = write_review_scope_export_index(
                destination,
                run_dir=self.result_store.run_dir,
                payload=payload,
                batch_id=batch_id,
                exported_files=[str(json_path), str(markdown_path)],
            )
            index_path = destination / REVIEW_SCOPE_EXPORT_INDEX_FILENAME
        except Exception as exc:
            message = t("facade.review_scope_manifest_export_failed", error=exc)
            self._ui_error_message = message
            self._append_notification("error", message)
            self._last_export_result.update(last_export_message=message)
            return {"ok": False, "message": message}
        finally:
            self._busy_message = ""

        scope_label = str(dict(payload.get("scope_summary", {}) or {}).get("scope_label") or t("common.none"))
        message = t(
            "facade.review_scope_manifest_exported",
            scope=scope_label,
            batch_id=batch_id,
            destination=json_path,
        )
        exported_files = [str(json_path), str(markdown_path), str(index_path)]
        self._last_export_result.update(
            {
                "available_formats": ["json", "csv", "all"],
                "last_export_message": message,
                "last_export_dir": str(destination),
                "last_export_batch_id": batch_id,
                "last_export_files": list(exported_files),
            }
        )
        self._ui_error_message = ""
        self.log_ui(message)
        self._append_notification("success", message)
        return {
            "ok": True,
            "message": message,
            "directory": str(destination),
            "batch_id": batch_id,
            "json_path": str(json_path),
            "markdown_path": str(markdown_path),
            "index_path": str(index_path),
            "index": dict(index_payload),
            "exported_files": exported_files,
        }

    def get_error_snapshot(self) -> dict[str, Any]:
        message = ""
        if self.session.errors:
            message = str(self.session.errors[-1] or "")
        elif self._ui_error_message:
            message = self._ui_error_message
        return {
            "visible": bool(message),
            "message": message,
            "count": len(self.session.errors),
        }

    def get_busy_snapshot(self) -> dict[str, Any]:
        active = bool(self.service.is_running) or bool(self._busy_message)
        message = self._busy_message or (t("facade.run_in_progress") if bool(self.service.is_running) else "")
        return {
            "active": active,
            "message": message or t("common.working"),
        }

    def get_notification_snapshot(self) -> dict[str, Any]:
        items = list(self._notifications)
        if not items:
            for line in self.get_recent_logs(limit=5):
                items.append({"level": "info", "message": str(line)})
        return {"items": items[-8:]}

    def get_recent_logs(self, limit: int = 200) -> list[str]:
        with self._lock:
            items = list(self._logs)
        if limit <= 0:
            return items
        return items[-int(limit):]

    def get_preferences(self) -> dict[str, Any]:
        return self.preferences_store.load()

    def save_preferences(self, preferences: dict[str, Any]) -> dict[str, Any]:
        saved = self.preferences_store.save(merge_preferences(self.preferences_store.load(), dict(preferences or {})))
        self._append_notification("success", t("facade.preferences_saved"))
        return saved

    def save_ui_layout_preferences(self, updates: dict[str, Any]) -> dict[str, Any]:
        current = self.preferences_store.load()
        return self.preferences_store.save(merge_preferences(current, dict(updates or {})))

    def get_recent_runs(self) -> list[dict[str, Any]]:
        return self.recent_runs_store.load()

    def add_recent_run(self, path: str) -> list[dict[str, Any]]:
        return self.recent_runs_store.add(path)

    def get_app_info(self) -> dict[str, Any]:
        return self.app_info.as_dict()

    def get_plan_gateway(self) -> PlanGateway:
        return self.plan_gateway

    def get_device_workbench_snapshot(self) -> dict[str, Any]:
        return self.device_workbench.build_snapshot()

    def execute_device_workbench_action(self, target_device: str, action: str, **params: Any) -> dict[str, Any]:
        return self.device_workbench.execute_action(target_device, action, **params)

    def get_validation_snapshot(self) -> dict[str, Any]:
        payload = self._load_preferred_validation_payload()
        if not payload:
            return {
                "available": False,
                "validation_profile": "--",
                "compare_status": "--",
                "compare_status_display": "--",
                "evidence_source": "",
                "evidence_source_display": "",
                "first_failure_phase": "--",
                "first_failure_phase_display": "--",
                "entered_target_route": {},
                "target_route_event_count": {},
                "artifact_bundle_path": "",
                "report_dir": "",
                "bench_context": {},
                "simulation_context": {},
                "reference_quality": {},
                "route_physical_validation": {},
                "evidence_state": "",
                "evidence_state_display": "",
                "evidence_layers": [],
                "diagnostic_only": False,
                "acceptance_evidence": False,
                "not_real_acceptance_evidence": False,
                "fallback_candidates": [],
                "primary_latest_missing": False,
                "primary_real_latest_missing": False,
                "gate_state": {
                    "checklist_gate": "--",
                    "checklist_gate_display": "--",
                    "target_route": "--",
                    "target_route_display": "--",
                    "single_temp": False,
                },
                "acceptance_level": "offline_regression",
                "acceptance_level_display": display_acceptance_value("offline_regression"),
                "acceptance_scope": "validation_latest",
                "acceptance_scope_display": display_acceptance_value("validation_latest"),
                "promotion_state": "dry_run_only",
                "promotion_state_display": display_acceptance_value("dry_run_only"),
                "review_state": "pending",
                "review_state_display": display_acceptance_value("pending"),
                "approval_state": "blocked",
                "approval_state_display": display_acceptance_value("blocked"),
                "ready_for_promotion": False,
                "missing_conditions": [],
                "missing_conditions_display": [],
                "promotion_plan": {},
                "role_views": {},
                "readiness_summary": {},
                "state_machine": {},
            }
        route_execution = dict(payload.get("route_execution_summary") or {})
        validation_profile = str(payload.get("validation_profile") or "--")
        bench_context = dict(payload.get("bench_context") or route_execution.get("bench_context") or {})
        simulation_context = dict(payload.get("simulation_context") or {})
        metadata = dict(payload.get("metadata") or {})
        artifact_bundle_key = f"{validation_profile}_bundle"
        artifact_bundle_path = str(
            payload.get("artifacts", {}).get(artifact_bundle_key, payload.get("source_latest_index_path", ""))
            or payload.get("source_latest_index_path", "")
        )
        compare_status = str(payload.get("compare_status") or "--")
        temp_c = metadata.get("temp_c")
        fallback_candidates = list(payload.get("fallback_candidates") or [])
        simulated_candidates = list(payload.get("simulated_candidates") or [])
        evidence_source = str(payload.get("evidence_source") or "real")
        acceptance_model = build_validation_acceptance_snapshot(payload)
        evidence_layers: list[dict[str, Any]] = [
            {
                "tier": "primary" if evidence_source == "real" and not bool(payload.get("diagnostic_only", False)) else (
                    "diagnostic" if evidence_source == "real" else "simulated"
                ),
                "tier_display": display_acceptance_value(
                    "primary" if evidence_source == "real" and not bool(payload.get("diagnostic_only", False)) else (
                        "diagnostic" if evidence_source == "real" else "simulated"
                    )
                ),
                "validation_profile": validation_profile,
                "compare_status": compare_status,
                "compare_status_display": display_compare_status(compare_status),
                "evidence_source": evidence_source,
                "evidence_source_display": display_evidence_source(evidence_source),
                "evidence_state": str(payload.get("evidence_state") or ""),
                "evidence_state_display": display_evidence_state(str(payload.get("evidence_state") or "")),
                "diagnostic_only": bool(payload.get("diagnostic_only", False)),
                "acceptance_evidence": bool(payload.get("acceptance_evidence", False)),
                "not_real_acceptance_evidence": bool(payload.get("not_real_acceptance_evidence", False)),
                "source_latest_index_path": str(payload.get("source_latest_index_path") or ""),
            }
        ]
        for candidate in fallback_candidates:
            candidate_state = str(candidate.get("evidence_state") or "").strip().lower()
            tier = "stale" if ("stale" in candidate_state or "superseded" in candidate_state or "obsolete" in candidate_state) else "diagnostic"
            evidence_layers.append(
                {
                    "tier": tier,
                    "tier_display": display_acceptance_value(tier),
                    "validation_profile": str(candidate.get("validation_profile") or "--"),
                    "compare_status": str(candidate.get("compare_status") or "--"),
                    "compare_status_display": display_compare_status(str(candidate.get("compare_status") or "--")),
                    "evidence_source": str(candidate.get("evidence_source") or "real"),
                    "evidence_source_display": display_evidence_source(str(candidate.get("evidence_source") or "real")),
                    "evidence_state": str(candidate.get("evidence_state") or ""),
                    "evidence_state_display": display_evidence_state(str(candidate.get("evidence_state") or "")),
                    "diagnostic_only": bool(candidate.get("diagnostic_only", False)),
                    "acceptance_evidence": False,
                    "not_real_acceptance_evidence": bool(candidate.get("not_real_acceptance_evidence", False)),
                    "source_latest_index_path": str(candidate.get("source_latest_index_path") or ""),
                }
            )
        for candidate in simulated_candidates:
            simulated_source = _normalize_simulated_evidence_source(candidate.get("evidence_source"))
            evidence_layers.append(
                {
                    "tier": "simulated_diagnostic" if bool(candidate.get("diagnostic_only", False)) else "simulated_coverage",
                    "tier_display": display_acceptance_value(
                        "simulated_diagnostic" if bool(candidate.get("diagnostic_only", False)) else "simulated_coverage"
                    ),
                    "validation_profile": str(candidate.get("validation_profile") or "--"),
                    "compare_status": str(candidate.get("compare_status") or "--"),
                    "compare_status_display": display_compare_status(str(candidate.get("compare_status") or "--")),
                    "evidence_source": simulated_source,
                    "evidence_source_display": display_evidence_source(simulated_source),
                    "evidence_state": str(candidate.get("evidence_state") or "simulated_validation"),
                    "evidence_state_display": display_evidence_state(str(candidate.get("evidence_state") or "simulated_validation")),
                    "diagnostic_only": bool(candidate.get("diagnostic_only", False)),
                    "acceptance_evidence": False,
                    "not_real_acceptance_evidence": True,
                    "source_latest_index_path": str(candidate.get("source_latest_index_path") or ""),
                }
            )
        return {
            "available": True,
            "validation_profile": validation_profile,
            "compare_status": compare_status,
            "compare_status_display": display_compare_status(compare_status),
            "evidence_source": evidence_source,
            "evidence_source_display": display_evidence_source(evidence_source),
            "first_failure_phase": str(payload.get("first_failure_phase") or route_execution.get("first_failure_phase") or "--"),
            "first_failure_phase_display": self._humanize_ui_summary(
                str(payload.get("first_failure_phase") or route_execution.get("first_failure_phase") or "--")
            ),
            "entered_target_route": dict(payload.get("entered_target_route") or route_execution.get("entered_target_route") or {}),
            "target_route_event_count": dict(
                payload.get("target_route_event_count") or route_execution.get("target_route_event_count") or {}
            ),
            "artifact_bundle_path": artifact_bundle_path,
            "report_dir": str(payload.get("report_dir") or ""),
            "bench_context": bench_context,
            "simulation_context": simulation_context,
            "reference_quality": dict(payload.get("reference_quality") or {}),
            "route_physical_validation": {
                "route_physical_state_match": dict(route_execution.get("route_physical_state_match") or {}),
                "relay_physical_mismatch": dict(route_execution.get("relay_physical_mismatch") or {}),
                "sides": {
                    side: {
                        "target_open_valves": list(((route_execution.get("sides") or {}).get(side, {}) or {}).get("target_open_valves") or []),
                        "actual_open_valves": list(((route_execution.get("sides") or {}).get(side, {}) or {}).get("actual_open_valves") or []),
                        "target_relay_state": dict(((route_execution.get("sides") or {}).get(side, {}) or {}).get("target_relay_state") or {}),
                        "actual_relay_state": dict(((route_execution.get("sides") or {}).get(side, {}) or {}).get("actual_relay_state") or {}),
                        "cleanup_all_relays_off": ((route_execution.get("sides") or {}).get(side, {}) or {}).get("cleanup_all_relays_off"),
                        "cleanup_relay_state": dict(((route_execution.get("sides") or {}).get(side, {}) or {}).get("cleanup_relay_state") or {}),
                    }
                    for side in ("v1", "v2")
                },
            },
            "evidence_state": str(payload.get("evidence_state") or ""),
            "evidence_state_display": display_evidence_state(str(payload.get("evidence_state") or "")),
            "evidence_layers": evidence_layers,
            "diagnostic_only": bool(payload.get("diagnostic_only", False)),
            "acceptance_evidence": bool(payload.get("acceptance_evidence", False)),
            "not_real_acceptance_evidence": bool(payload.get("not_real_acceptance_evidence", False)),
            "fallback_candidates": fallback_candidates,
            "primary_latest_missing": compare_status == PRIMARY_REAL_VALIDATION_MISSING_STATUS,
            "primary_real_latest_missing": compare_status == PRIMARY_REAL_VALIDATION_MISSING_STATUS,
            "gate_state": {
                "checklist_gate": str(payload.get("checklist_gate") or "--"),
                "checklist_gate_display": str(payload.get("checklist_gate") or "--"),
                "target_route": str(route_execution.get("target_route") or bench_context.get("target_route") or "--"),
                "target_route_display": display_route(str(route_execution.get("target_route") or bench_context.get("target_route") or "--")),
                "single_temp": temp_c not in (None, "", []),
            },
            "acceptance_level": acceptance_model.get("acceptance_level"),
            "acceptance_level_display": display_acceptance_value(acceptance_model.get("acceptance_level")),
            "acceptance_scope": acceptance_model.get("acceptance_scope"),
            "acceptance_scope_display": display_acceptance_value(acceptance_model.get("acceptance_scope")),
            "promotion_state": acceptance_model.get("promotion_state"),
            "promotion_state_display": display_acceptance_value(acceptance_model.get("promotion_state")),
            "review_state": acceptance_model.get("review_state"),
            "review_state_display": display_acceptance_value(acceptance_model.get("review_state")),
            "approval_state": acceptance_model.get("approval_state"),
            "approval_state_display": display_acceptance_value(acceptance_model.get("approval_state")),
            "ready_for_promotion": acceptance_model.get("ready_for_promotion"),
            "missing_conditions": list(acceptance_model.get("missing_conditions") or []),
            "missing_conditions_display": [
                self._humanize_ui_summary(str(item)) for item in list(acceptance_model.get("missing_conditions") or [])
            ],
            "promotion_plan": dict(acceptance_model.get("promotion_plan") or {}),
            "role_views": dict(acceptance_model.get("role_views") or {}),
            "readiness_summary": {
                **dict(acceptance_model.get("readiness_summary") or {}),
                "summary_display": self._humanize_ui_summary(
                    str(dict(acceptance_model.get("readiness_summary") or {}).get("summary", "--") or "--")
                ),
            },
            "state_machine": dict(acceptance_model.get("state_machine") or {}),
        }

    def get_run_mode(self) -> str:
        return str(getattr(self.config.workflow, "run_mode", "auto_calibration") or "auto_calibration")

    def formal_calibration_report_enabled(self) -> bool:
        return self.get_run_mode() == RunMode.AUTO_CALIBRATION.value

    def log_ui(self, message: str) -> None:
        self._append_log(f"UI {message}")
        self._append_notification("info", message)

    def _make_event_handler(self, event_type: EventType):
        def _handler(event: Event) -> None:
            self._append_log(self._format_event(event_type, event.data))

        return _handler

    def _on_log(self, message: str) -> None:
        self._append_log(str(message))
        self._append_notification("info", str(message))

    def _append_log(self, message: str) -> None:
        with self._lock:
            self._logs.append(message.strip())

    def _append_notification(self, level: str, message: str) -> None:
        text = str(message or "").strip()
        if not text:
            return
        with self._lock:
            self._notifications.append({"level": str(level or "info"), "message": text})

    def _load_preferred_validation_payload(self) -> dict[str, Any]:
        real_payloads = self._load_validation_index_payloads(VALIDATION_LATEST_INDEXES, evidence_source="real")
        simulated_payloads = self._load_validation_index_payloads(
            SIMULATED_VALIDATION_LATEST_INDEXES,
            evidence_source="simulated_protocol",
        )
        candidates: list[tuple[tuple[int, int, int], dict[str, Any]]] = []
        for ordinal, payload in enumerate(real_payloads):
            validation_profile = str(payload.get("validation_profile") or "--")
            bench_context = dict(payload.get("bench_context") or {})
            validation_role = str(bench_context.get("validation_role") or "").strip().lower()
            primary_profile = str(bench_context.get("primary_replacement_route") or "").strip()
            evidence_state = str(payload.get("evidence_state") or "").strip().lower()
            stale_flag = bool(payload.get("stale_for_current_bench", False)) or "stale" in evidence_state or "obsolete" in evidence_state
            profile_priority = 2
            if validation_profile == primary_profile or validation_profile == PRIMARY_VALIDATION_PROFILE:
                profile_priority = 0
            elif validation_role == "primary":
                profile_priority = 1
            elif validation_role == "diagnostic_route_unblock":
                profile_priority = 3
            elif validation_role == "legacy_mixed_route":
                profile_priority = 5
            elif validation_role == "diagnostic":
                profile_priority = 10
            candidates.append(((10 if stale_flag else 0, profile_priority, ordinal), payload))
        primary_path = dict(VALIDATION_LATEST_INDEXES).get(PRIMARY_VALIDATION_PROFILE)
        primary_missing = primary_path is not None and not primary_path.exists()
        if primary_missing:
            fallback_candidates = [self._serialize_validation_candidate(payload) for payload in real_payloads]
            return {
                "validation_profile": PRIMARY_VALIDATION_PROFILE,
                "checklist_gate": "12A",
                "compare_status": PRIMARY_REAL_VALIDATION_MISSING_STATUS,
                "evidence_source": "real",
                "first_failure_phase": "primary_validation_latest_missing",
                "evidence_state": "primary_validation_latest_missing",
                "diagnostic_only": False,
                "acceptance_evidence": True,
                "not_real_acceptance_evidence": False,
                "entered_target_route": {},
                "target_route_event_count": {},
                "bench_context": {
                    "co2_0ppm_available": False,
                    "other_gases_available": True,
                    "h2o_route_available": False,
                    "humidity_generator_humidity_feedback_valid": False,
                    "primary_replacement_route": PRIMARY_VALIDATION_PROFILE,
                    "validation_role": "primary",
                    "target_route": "co2",
                },
                "route_execution_summary": {
                    "target_route": "co2",
                    "compare_status": "PRIMARY_VALIDATION_LATEST_MISSING",
                    "entered_target_route": {},
                    "target_route_event_count": {},
                    "valid_for_route_diff": False,
                    "first_failure_phase": "primary_validation_latest_missing",
                    "reason": t("facade.validation_primary_missing_reason"),
                },
                "artifacts": {},
                "report_dir": "",
                "source_latest_index_path": str(primary_path),
                "fallback_candidates": fallback_candidates,
                "simulated_candidates": [self._serialize_validation_candidate(payload) for payload in simulated_payloads],
            }
        if not candidates:
            return {}
        candidates.sort(key=lambda item: item[0])
        selected = dict(candidates[0][1])
        selected["fallback_candidates"] = [
            self._serialize_validation_candidate(payload)
            for payload in real_payloads
            if str(payload.get("source_latest_index_path") or "") != str(selected.get("source_latest_index_path") or "")
        ]
        selected["simulated_candidates"] = [self._serialize_validation_candidate(payload) for payload in simulated_payloads]
        return selected

    @staticmethod
    def _serialize_validation_candidate(payload: dict[str, Any]) -> dict[str, Any]:
        evidence_source = str(payload.get("evidence_source") or "real")
        if evidence_source.strip().lower() in {"simulated", "simulated_protocol"}:
            evidence_source = _normalize_simulated_evidence_source(evidence_source)
        return {
            "validation_profile": str(payload.get("validation_profile") or "--"),
            "compare_status": str(payload.get("compare_status") or "--"),
            "evidence_source": evidence_source,
            "evidence_state": str(payload.get("evidence_state") or ""),
            "diagnostic_only": bool(payload.get("diagnostic_only", False)),
            "acceptance_evidence": bool(payload.get("acceptance_evidence", False)),
            "not_real_acceptance_evidence": bool(payload.get("not_real_acceptance_evidence", False)),
            "source_latest_index_path": str(payload.get("source_latest_index_path") or ""),
        }

    @staticmethod
    def _load_validation_index_payloads(
        indexes: tuple[tuple[str, Path], ...],
        *,
        evidence_source: str,
    ) -> list[dict[str, Any]]:
        loaded_payloads: list[dict[str, Any]] = []
        for expected_profile, path in indexes:
            if not path.exists():
                continue
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
            except Exception:
                continue
            if not isinstance(payload, dict):
                continue
            payload.setdefault("validation_profile", expected_profile)
            payload.setdefault("evidence_source", evidence_source)
            payload["source_latest_index_path"] = str(path)
            loaded_payloads.append(payload)
        return loaded_payloads

    def _resolve_points_path(self, points_path: Optional[str] = None) -> Path:
        raw = str(points_path or self.config.paths.points_excel or "").strip()
        if not raw:
            raise ValueError("points path is not configured")
        path = Path(raw).expanduser()
        if not path.is_absolute():
            path = path.resolve()
        if not path.exists():
            raise FileNotFoundError(path)
        return path

    def _resolve_run_input(
        self,
        *,
        points_path: Optional[str] = None,
        points_source: str = "use_points_file",
    ) -> dict[str, Any]:
        source = self._normalize_points_source(points_source)
        if source == "use_points_file":
            raw = str(points_path or self.config.paths.points_excel or "").strip()
            if not raw:
                raise ValueError("points path is not configured")
            return {
                "source": source,
                "path": raw,
                "display_path": raw,
                "source_label": t("facade.source_label.points_file"),
                "run_mode": self.get_run_mode(),
                "route_mode": None,
                "formal_calibration_report": None,
                "profile_name": getattr(self.config.workflow, "profile_name", None),
                "profile_version": getattr(self.config.workflow, "profile_version", None),
                "report_family": getattr(self.config.workflow, "report_family", None),
                "report_templates": dict(getattr(self.config.workflow, "report_templates", {}) or {}),
                "analyzer_setup": dict(getattr(self.config.workflow, "analyzer_setup", {}) or {}),
            }
        compiled = self.plan_gateway.build_default_runtime_points_file()
        metadata = dict(compiled.get("metadata") or {})
        return {
            "source": source,
            "path": str(compiled["path"]),
            "display_path": t("facade.default_profile_summary", profile=compiled.get("profile_name", "--"), summary=""),
            "source_label": t("facade.source_label.default_profile"),
            "run_mode": metadata.get("run_mode", self.get_run_mode()),
            "route_mode": metadata.get("route_mode"),
            "formal_calibration_report": metadata.get("formal_calibration_report"),
            "profile_name": compiled.get("profile_name"),
            "profile_version": compiled.get("profile_version"),
            "report_family": metadata.get("report_family", compiled.get("report_family")),
            "report_templates": dict(metadata.get("report_templates") or compiled.get("report_templates") or {}),
            "analyzer_setup": dict(metadata.get("analyzer_setup") or compiled.get("analyzer_setup") or {}),
        }

    @staticmethod
    def _normalize_points_source(points_source: str) -> str:
        normalized = str(points_source or "use_points_file").strip() or "use_points_file"
        if normalized not in {"use_points_file", "use_default_profile"}:
            raise ValueError(f"unsupported points source: {normalized}")
        return normalized

    def _build_preview_parser(self):
        from ...core.point_parser import LegacyExcelPointLoader, PointParser

        return PointParser(
            legacy_excel_loader=LegacyExcelPointLoader(
                missing_pressure_policy=str(getattr(self.config.workflow, "missing_pressure_policy", "require") or "require"),
                carry_forward_h2o=bool(getattr(self.config.workflow, "h2o_carry_forward", False)),
            )
        )

    def _load_preview_points(
        self,
        path: Path,
        *,
        point_parser,
        route_planner,
        config: Optional[AppConfig] = None,
    ) -> list[Any]:
        from ...core.calibration_service import parse_points_for_execution

        effective_config = config or self.config
        return parse_points_for_execution(
            path,
            point_parser=point_parser,
            selected_temps_c=getattr(effective_config.workflow, "selected_temps_c", None),
            temperature_descending=bool(getattr(effective_config.workflow, "temperature_descending", True)),
            route_planner=route_planner,
        )

    def _config_for_mode(self, mode_profile: ModeProfile) -> AppConfig:
        config = copy.deepcopy(self.config)
        workflow = config.workflow
        workflow.run_mode = mode_profile.run_mode.value
        workflow.route_mode = mode_profile.effective_route_mode(
            self._default_route_mode
        )
        return config

    @staticmethod
    def _humanize_ui_summary(text: str) -> str:
        raw = str(text or "").strip()
        if not raw:
            return ""
        translators = (
            display_compare_status,
            display_evidence_state,
            display_acceptance_value,
            display_phase,
            display_run_mode,
            display_route,
            display_device_status,
            display_reference_quality,
            display_risk_level,
            display_winner_status,
        )
        for translator in translators:
            translated = translator(raw)
            if translated != raw:
                return translated
        normalized = raw
        phrase_replacements = (
            ("Run looks stable.", "运行状态稳定。"),
            ("Review invalid points before fitting.", "拟合前请复核无效点。"),
            ("Readable Points:", "可读点表："),
            ("Results File:", "结果文件："),
            ("Default:", "默认算法："),
            ("No AI summary artifact available.", "暂无 AI 摘要工件。"),
        )
        for source, target in phrase_replacements:
            normalized = normalized.replace(source, target)
        match = re.search(r"missing\s+(\d+)\s+gates", normalized, flags=re.IGNORECASE)
        if match:
            normalized = re.sub(
                r"missing\s+\d+\s+gates",
                t("terms.missing_gates", count=match.group(1)),
                normalized,
                flags=re.IGNORECASE,
            )
        replacements = {
            "offline_regression": display_acceptance_value("offline_regression"),
            "dry_run_only": display_acceptance_value("dry_run_only"),
            "coverage": t("terms.coverage"),
            "reference": t("terms.reference"),
            "degraded": display_acceptance_value("degraded"),
            "exports": t("terms.exports"),
            "errors": t("terms.error"),
            "error": t("terms.error"),
            "parity": t("terms.parity"),
            "present": display_presence(True),
            "missing": t("common.missing"),
            "stable": t("terms.stable"),
            "disabled": display_device_status("disabled"),
            "warning": t("terms.warning"),
            "warnings": t("terms.warning"),
        }
        for source, target in replacements.items():
            normalized = re.sub(rf"\b{re.escape(source)}\b", target, normalized, flags=re.IGNORECASE)
        return humanize_review_surface_text(normalized)

    def _apply_run_mode(
        self,
        run_mode: Optional[str],
        *,
        route_mode: Optional[str] = None,
        formal_calibration_report: Optional[bool] = None,
    ) -> None:
        mode_profile = ModeProfile.from_value(
            {
                "run_mode": run_mode,
                "route_mode": route_mode,
                "formal_calibration_report": formal_calibration_report,
            }
        )
        for config in self._mutable_configs():
            workflow = getattr(config, "workflow", None)
            if workflow is None:
                continue
            workflow.run_mode = mode_profile.run_mode.value
            workflow.route_mode = mode_profile.effective_route_mode(
                self._default_route_mode
            )
        raw_cfg = getattr(self.service, "_raw_cfg", None)
        if isinstance(raw_cfg, dict):
            workflow = raw_cfg.setdefault("workflow", {})
            if isinstance(workflow, dict):
                workflow["run_mode"] = mode_profile.run_mode.value
                workflow["route_mode"] = mode_profile.effective_route_mode(
                    str(raw_cfg.get("_default_route_mode", self._default_route_mode) or self._default_route_mode)
                )
                raw_cfg["_default_route_mode"] = self._default_route_mode

    def _mutable_configs(self) -> list[AppConfig]:
        configs: list[AppConfig] = []
        seen_ids: set[int] = set()
        for config in (
            self.config,
            getattr(self.service, "config", None),
            getattr(getattr(self.service, "session", None), "config", None),
            getattr(getattr(getattr(self.service, "orchestrator", None), "context", None), "config", None),
        ):
            if config is None or id(config) in seen_ids:
                continue
            seen_ids.add(id(config))
            configs.append(config)
        return configs

    def _apply_analyzer_setup(self, analyzer_setup: Optional[dict[str, Any]]) -> None:
        payload = dict(analyzer_setup or {})
        if not payload:
            return
        for config in self._mutable_configs():
            workflow = getattr(config, "workflow", None)
            if workflow is not None:
                setattr(workflow, "analyzer_setup", dict(payload))
            raw_cfg = getattr(config, "_raw_cfg", None)
            if isinstance(raw_cfg, dict):
                workflow_payload = raw_cfg.setdefault("workflow", {})
                if isinstance(workflow_payload, dict):
                    workflow_payload["analyzer_setup"] = dict(payload)

    def _apply_profile_runtime_metadata(
        self,
        *,
        profile_name: Optional[str],
        profile_version: Optional[str],
        report_family: Optional[str],
        report_templates: Optional[dict[str, Any]],
    ) -> None:
        normalized_profile_name = str(profile_name).strip() if profile_name not in (None, "") else None
        normalized_profile_version = str(profile_version).strip() if profile_version not in (None, "") else None
        normalized_report_family = str(report_family).strip() if report_family not in (None, "") else None
        templates = dict(report_templates or {})
        for config in self._mutable_configs():
            workflow = getattr(config, "workflow", None)
            if workflow is not None:
                setattr(workflow, "profile_name", normalized_profile_name)
                setattr(workflow, "profile_version", normalized_profile_version)
                setattr(workflow, "report_family", normalized_report_family)
                setattr(workflow, "report_templates", dict(templates))
            raw_cfg = getattr(config, "_raw_cfg", None)
            if isinstance(raw_cfg, dict):
                workflow_payload = raw_cfg.setdefault("workflow", {})
                if isinstance(workflow_payload, dict):
                    workflow_payload["profile_name"] = normalized_profile_name
                    workflow_payload["profile_version"] = normalized_profile_version
                    workflow_payload["report_family"] = normalized_report_family
                    workflow_payload["report_templates"] = dict(templates)

    def _preview_points_in_execution_order(
        self,
        points: list[Any],
        *,
        route_planner,
    ) -> list[Any]:
        ordered: list[Any] = []
        route_mode = route_planner.route_mode()
        for group in route_planner.group_by_temperature(points):
            group_points = list(group.points)
            if route_mode != "co2_only" and route_planner.should_run_h2o(group_points):
                pressure_points = route_planner.h2o_pressure_points(group_points)
                for h2o_group in route_planner.group_h2o_points(group_points):
                    if not h2o_group:
                        continue
                    lead = h2o_group[0]
                    for pressure_point in pressure_points or h2o_group:
                        ordered.append(route_planner.build_h2o_pressure_point(lead, pressure_point))
            if route_mode == "h2o_only":
                continue
            for source_point in route_planner.co2_sources(group_points):
                pressure_points = route_planner.co2_pressure_points(source_point, group_points) or [source_point]
                for pressure_point in pressure_points:
                    ordered.append(route_planner.build_co2_pressure_point(source_point, pressure_point))
        return ordered

    @staticmethod
    def _preview_row(sequence: int, point: Any) -> dict[str, str]:
        is_h2o = bool(point.is_h2o_point)
        hgen_temp = point.hgen_temp_c
        hgen_rh = point.hgen_rh_pct
        if hgen_temp is not None or hgen_rh is not None:
            temp_text = format_temperature_c(hgen_temp) if hgen_temp is not None else "--"
            rh_text = f"{float(hgen_rh):g}%RH" if hgen_rh is not None else "--"
            hgen_text = f"{temp_text} / {rh_text}"
        else:
            hgen_text = "--"
        co2_text = "--" if is_h2o or point.co2_ppm is None else format_ppm(int(round(float(point.co2_ppm))))
        pressure_text = str(point.pressure_display_label or "--")
        group = str(point.co2_group or "").strip().upper()
        status = (
            t("facade.route_status.h2o_execution")
            if is_h2o
            else (
                t("facade.route_status.co2_subzero_execution")
                if float(point.temp_chamber_c) < 0.0
                else t("facade.route_status.co2_execution")
            )
        )
        return {
            "seq": str(sequence),
            "row": str(point.index),
            "temp": format_temperature_c(point.temp_chamber_c),
            "route": display_route("h2o" if is_h2o else "co2"),
            "hgen": hgen_text,
            "co2": co2_text,
            "pressure": pressure_text,
            "group": "--" if is_h2o else (group or "--"),
            "status": status,
        }

    @staticmethod
    def _open_path_with_system_editor(path: Path) -> None:
        if hasattr(os, "startfile"):
            os.startfile(str(path))  # type: ignore[attr-defined]
            return
        command = ["open", str(path)] if sys.platform == "darwin" else ["xdg-open", str(path)]
        subprocess.Popen(command)

    @staticmethod
    def _point_to_text(point: Optional[CalibrationPoint]) -> str:
        if point is None:
            return "--"
        route = str(getattr(point, "route", "") or "").strip()
        pressure_text = str(getattr(point, "pressure_display_label", None) or "--")
        return (
            f"#{point.index} 温度={format_temperature_c(point.temperature_c)} "
            f"CO2={format_ppm(point.co2_ppm)} 压力={pressure_text} 路由={display_route(route or '--', default='--')}"
        )

    @staticmethod
    def _route_value_to_text(value: Any) -> Any:
        if value is None or isinstance(value, (str, int, float, bool)):
            return value
        if isinstance(value, dict):
            return {key: AppFacade._route_value_to_text(item) for key, item in value.items()}
        if is_dataclass(value):
            return {key: AppFacade._route_value_to_text(item) for key, item in asdict(value).items()}
        if hasattr(value, "value"):
            return getattr(value, "value")
        if isinstance(value, CalibrationPoint):
            return AppFacade._point_to_text(value)
        return str(value)

    @staticmethod
    def _validation_to_row(validation: Any) -> dict[str, Any]:
        if isinstance(validation, dict):
            payload = dict(validation)
        elif is_dataclass(validation):
            payload = asdict(validation)
        else:
            payload = {
                "point_index": getattr(validation, "point_index", None),
                "quality_score": getattr(validation, "quality_score", 0.0),
                "valid": getattr(validation, "valid", False),
                "recommendation": getattr(validation, "recommendation", ""),
                "reason": getattr(validation, "reason", ""),
            }
        payload.setdefault(
            "result_level",
            (
                "pass"
                if bool(payload.get("valid", False))
                else "warn"
                if str(payload.get("recommendation", "") or "").strip().lower() == "review"
                else "reject"
            ),
        )
        payload.setdefault("route", "")
        payload.setdefault("temperature_c", None)
        payload.setdefault("co2_ppm", None)
        return payload

    @staticmethod
    def _format_event(event_type: EventType, data: Any) -> str:
        text = json.dumps(data, ensure_ascii=False) if isinstance(data, (dict, list)) else str(data or "")
        text = text.strip()
        return f"{event_type.value}: {text}" if text else event_type.value
