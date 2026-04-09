from __future__ import annotations

import time
from typing import Any

from ..core.phase_transition_bridge_presenter import (
    build_phase_transition_bridge_digest,
    build_phase_transition_bridge_panel_payload,
)
from ..review_surface_formatter import (
    humanize_review_center_coverage_text,
    humanize_review_surface_text,
)
from .i18n import t
from .review_center_artifact_scope import (
    build_artifact_scope_view,
    build_review_center_selection_snapshot as build_selection_snapshot,
    decorate_source_rows,
)

def build_review_center_view(
    payload: dict[str, Any],
    *,
    selected_type: str = "all",
    selected_status: str = "all",
    selected_time: str = "all",
    selected_source_kind: str = "all",
    selected_source_id: str = "all",
    selected_phase: str = "all",
    selected_artifact_role: str = "all",
    selected_standard_family: str = "all",
    selected_evidence_category: str = "all",
    selected_boundary: str = "all",
    selected_anchor: str = "all",
    selected_route: str = "all",
    selected_signal_family: str = "all",
    selected_decision_result: str = "all",
    selected_policy_version: str = "all",
    selected_evidence_source: str = "all",
    now_ts: float | None = None,
) -> dict[str, Any]:
    base_payload = dict(payload or {})
    base_index = dict(base_payload.get("index_summary", {}) or {})
    items = _normalize_review_items(base_payload.get("evidence_items"))
    sources = _normalize_source_rows(base_index.get("sources"), items=items)
    scope_items = _filter_review_items(
        items,
        selected_type=selected_type,
        selected_status=selected_status,
        selected_time=selected_time,
        selected_source_kind=selected_source_kind,
        selected_source_row=None,
        selected_phase=selected_phase,
        selected_artifact_role=selected_artifact_role,
        selected_standard_family=selected_standard_family,
        selected_evidence_category=selected_evidence_category,
        selected_boundary=selected_boundary,
        selected_anchor=selected_anchor,
        selected_route=selected_route,
        selected_signal_family=selected_signal_family,
        selected_decision_result=selected_decision_result,
        selected_policy_version=selected_policy_version,
        selected_evidence_source=selected_evidence_source,
        time_windows=_time_windows(base_payload),
        now_ts=now_ts,
    )
    sources = decorate_source_rows(sources, visible_items=scope_items, item_matcher=_item_matches_selected_source)
    selected_source_row = _match_selected_source_row(sources, selected_source_id)
    filtered_items = [
        dict(item)
        for item in (
            scope_items
            if not selected_source_row
            else [item for item in scope_items if _item_matches_selected_source(item, selected_source_row)]
        )
    ]
    source_scope_view = _build_source_scope_view(
        base_payload,
        base_index=base_index,
        scope_items=scope_items,
        filtered_items=filtered_items,
        selected_source_row=selected_source_row,
    )
    reviewer_artifact_entries = _filter_reviewer_artifact_entries(
        _normalize_reviewer_artifact_entries(base_payload),
        selected_phase=selected_phase,
        selected_artifact_role=selected_artifact_role,
        selected_standard_family=selected_standard_family,
        selected_evidence_category=selected_evidence_category,
        selected_boundary=selected_boundary,
        selected_anchor=selected_anchor,
    )
    reviewer_artifact_entry_by_key = {
        str(item.get("artifact_key") or ""): dict(item)
        for item in reviewer_artifact_entries
        if str(item.get("artifact_key") or "").strip()
    }
    selected_item = dict(filtered_items[0]) if filtered_items else {
        "detail_text": str(base_payload.get("empty_detail") or t("results.review_center.empty")),
        "detail_hint": str(base_payload.get("detail_hint") or t("results.review_center.detail_hint")),
    }
    active_view = {
        "items": filtered_items,
        "scope_items": [dict(item) for item in scope_items],
        "sources": sources,
        "selected_source_row": dict(selected_source_row or {}),
        "selected_item": selected_item,
        "count_text": t(
            "results.review_center.filter.count",
            visible=len(filtered_items),
            total=len(items),
        ),
        "index_text": source_scope_view["index_text"],
        "operator_summary": source_scope_view["operator_summary"],
        "reviewer_summary": source_scope_view["reviewer_summary"],
        "approver_summary": source_scope_view["approver_summary"],
        "risk_summary": source_scope_view["risk_summary"],
        "readiness_summary": source_scope_view["readiness_summary"],
        "analytics_summary": source_scope_view["analytics_summary"],
        "lineage_summary": source_scope_view["lineage_summary"],
        "phase_bridge_summary": source_scope_view["phase_bridge_summary"],
        "phase_bridge_panel": source_scope_view["phase_bridge_panel"],
        "reviewer_artifact_entries": reviewer_artifact_entries,
        "phase_bridge_reviewer_artifact_entry": dict(
            base_payload.get("phase_transition_bridge_reviewer_artifact_entry", {}) or {}
        ),
        "stage_admission_review_pack_artifact_entry": dict(
            reviewer_artifact_entry_by_key.get("stage_admission_review_pack", {})
        ),
        "engineering_isolation_admission_checklist_artifact_entry": dict(
            reviewer_artifact_entry_by_key.get("engineering_isolation_admission_checklist", {})
        ),
        "stage3_real_validation_plan_artifact_entry": dict(
            reviewer_artifact_entry_by_key.get("stage3_real_validation_plan", {})
        ),
        "stage3_standards_alignment_matrix_artifact_entry": dict(
            reviewer_artifact_entry_by_key.get("stage3_standards_alignment_matrix", {})
        ),
        "source_scope_label": source_scope_view["source_scope_label"],
        "source_scope_active": bool(selected_source_row),
    }
    active_view["selection_snapshot"] = build_selection_snapshot(
        active_view,
        scope="all",
        selected_item=dict(selected_item) if filtered_items else None,
        item_matcher=_item_matches_selected_source,
    )
    return active_view


def build_review_center_selection_snapshot(
    active_view: dict[str, Any],
    *,
    scope: str = "all",
    selected_item: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return build_selection_snapshot(
        active_view,
        scope=scope,
        selected_item=selected_item,
        item_matcher=_item_matches_selected_source,
    )


def _normalize_reviewer_artifact_entries(payload: dict[str, Any]) -> list[dict[str, Any]]:
    explicit_entries = [
        dict(item)
        for item in list(payload.get("reviewer_artifact_entries", []) or [])
        if isinstance(item, dict) and bool(item.get("available", False))
    ]
    if explicit_entries:
        return explicit_entries

    entries: list[dict[str, Any]] = []
    for key in (
        "stage_admission_review_pack_artifact_entry",
        "engineering_isolation_admission_checklist_artifact_entry",
        "stage3_real_validation_plan_artifact_entry",
        "stage3_standards_alignment_matrix_artifact_entry",
    ):
        entry = dict(payload.get(key, {}) or {})
        if bool(entry.get("available", False)):
            entries.append(entry)
    return entries


def _filter_reviewer_artifact_entries(
    entries: list[dict[str, Any]],
    *,
    selected_phase: str,
    selected_artifact_role: str,
    selected_standard_family: str,
    selected_evidence_category: str,
    selected_boundary: str,
    selected_anchor: str,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for entry in entries:
        if not _entry_matches_reviewer_filters(
            entry,
            selected_phase=selected_phase,
            selected_artifact_role=selected_artifact_role,
            selected_standard_family=selected_standard_family,
            selected_evidence_category=selected_evidence_category,
            selected_boundary=selected_boundary,
            selected_anchor=selected_anchor,
        ):
            continue
        rows.append(dict(entry))
    return rows


def _entry_matches_reviewer_filters(
    entry: dict[str, Any],
    *,
    selected_phase: str,
    selected_artifact_role: str,
    selected_standard_family: str,
    selected_evidence_category: str,
    selected_boundary: str,
    selected_anchor: str,
) -> bool:
    if selected_anchor not in {"", "all"} and str(entry.get("anchor_id") or "") != selected_anchor:
        return False
    if not _matches_list_filter(entry.get("phase_filters"), selected_phase):
        return False
    if not _matches_list_filter(entry.get("artifact_role_filters"), selected_artifact_role):
        return False
    if not _matches_list_filter(entry.get("standard_family_filters"), selected_standard_family):
        return False
    if not _matches_list_filter(entry.get("evidence_category_filters"), selected_evidence_category):
        return False
    if not _matches_list_filter(entry.get("boundary_filters"), selected_boundary):
        return False
    return True


def _matches_list_filter(values: Any, selected_value: str) -> bool:
    if selected_value in {"", "all"}:
        return True
    rows = {str(item).strip() for item in list(values or []) if str(item).strip()}
    return selected_value in rows


def _phase_transition_bridge_digest(payload: dict[str, Any]) -> dict[str, Any]:
    analytics_summary = dict(payload.get("analytics_summary", {}) or {})
    analytics_detail = dict(analytics_summary.get("detail", {}) or {})
    return build_phase_transition_bridge_digest(dict(analytics_detail.get("phase_transition_bridge", {}) or {}))


def _phase_transition_bridge_panel(payload: dict[str, Any]) -> dict[str, Any]:
    analytics_summary = dict(payload.get("analytics_summary", {}) or {})
    analytics_detail = dict(analytics_summary.get("detail", {}) or {})
    return build_phase_transition_bridge_panel_payload(dict(analytics_detail.get("phase_transition_bridge", {}) or {}))


def _merge_summary_text(*parts: Any) -> str:
    rows: list[str] = []
    for value in parts:
        text = str(value or "").strip()
        if text and text not in rows:
            rows.append(text)
    return " | ".join(rows) if rows else t("common.none")


def _normalize_review_items(raw_items: Any) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for index, raw_item in enumerate(list(raw_items or [])):
        item = dict(raw_item or {})
        source_label = str(item.get("source_label") or "").strip() or t("common.none")
        source_dir = str(item.get("source_dir") or "").strip()
        source_id = str(item.get("source_id") or source_dir or source_label).strip() or f"source-{index}"
        source_scope = str(item.get("source_scope") or item.get("source_kind") or "run").strip().lower() or "run"
        item["source_label"] = source_label
        item["source_dir"] = source_dir
        item["source_id"] = source_id
        item["source_scope"] = source_scope
        item["source_scope_display"] = t(
            f"results.review_center.source_kind.{source_scope}",
            default=source_scope,
        )
        item["source_label_display"] = str(item.get("source_label_display") or source_label)
        items.append(item)
    return items


def _normalize_source_rows(raw_sources: Any, *, items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = [dict(item or {}) for item in list(raw_sources or []) if isinstance(item, dict)]
    if not rows and items:
        synthesized: dict[str, dict[str, Any]] = {}
        for item in items:
            source_id = str(item.get("source_id") or "").strip()
            if not source_id:
                continue
            row = synthesized.setdefault(
                source_id,
                {
                    "source_id": source_id,
                    "source_label": str(item.get("source_label") or t("common.none")),
                    "source_scope": str(item.get("source_scope") or item.get("source_kind") or "run"),
                    "source_dir": str(item.get("source_dir") or ""),
                    "latest_display": str(item.get("generated_at_display") or "--"),
                    "coverage_display": t("common.none"),
                    "gaps_display": t("common.none"),
                    "evidence_count": 0,
                },
            )
            row["evidence_count"] = int(row.get("evidence_count", 0) or 0) + 1
            sort_key = float(item.get("sort_key", 0.0) or 0.0)
            if sort_key >= float(row.get("_latest_sort", 0.0) or 0.0):
                row["_latest_sort"] = sort_key
                row["latest_display"] = str(item.get("generated_at_display") or "--")
        rows = list(synthesized.values())
    normalized: list[dict[str, Any]] = []
    label_to_ids: dict[str, set[str]] = {}
    for item in items:
        label = str(item.get("source_label") or "").strip()
        source_id = str(item.get("source_id") or "").strip()
        if label and source_id:
            label_to_ids.setdefault(label, set()).add(source_id)
    for index, raw_row in enumerate(rows):
        row = dict(raw_row or {})
        source_label = str(row.get("source_label") or "").strip() or t("common.none")
        source_id = str(row.get("source_id") or row.get("source_dir") or "").strip()
        if not source_id:
            matching_ids = sorted(label_to_ids.get(source_label, set()))
            source_id = matching_ids[0] if len(matching_ids) == 1 else source_label or f"source-row-{index}"
        source_scope = str(row.get("source_scope") or row.get("source_kind") or "run").strip().lower() or "run"
        row["source_label"] = source_label
        row["source_id"] = source_id
        row["source_scope"] = source_scope
        row["source_scope_display"] = t(
            f"results.review_center.source_kind.{source_scope}",
            default=source_scope,
        )
        row["source_dir"] = str(row.get("source_dir") or "").strip()
        row["latest_display"] = str(row.get("latest_display") or "--")
        row["coverage_display"] = humanize_review_center_coverage_text(str(row.get("coverage_display") or t("common.none")))
        row["gaps_display"] = humanize_review_center_coverage_text(str(row.get("gaps_display") or t("common.none")))
        row["evidence_count"] = int(row.get("evidence_count", 0) or 0)
        normalized.append(row)
    return normalized


def _time_windows(payload: dict[str, Any]) -> dict[str, float | None]:
    filters = dict(payload.get("filters", {}) or {})
    return {
        str(item.get("id") or ""): (
            None
            if item.get("window_seconds") in ("", None)
            else float(item.get("window_seconds") or 0.0)
        )
        for item in list(filters.get("time_options", []) or [])
        if isinstance(item, dict)
    }


def _match_selected_source_row(sources: list[dict[str, Any]], selected_source_id: str) -> dict[str, Any] | None:
    normalized_id = str(selected_source_id or "").strip()
    if normalized_id in {"", "all"}:
        return None
    for row in sources:
        if str(row.get("source_id") or "").strip() == normalized_id:
            return dict(row)
    return None


def _filter_review_items(
    items: list[dict[str, Any]],
    *,
    selected_type: str,
    selected_status: str,
    selected_time: str,
    selected_source_kind: str,
    selected_source_row: dict[str, Any] | None,
    selected_phase: str,
    selected_artifact_role: str,
    selected_standard_family: str,
    selected_evidence_category: str,
    selected_boundary: str,
    selected_anchor: str,
    selected_route: str,
    selected_signal_family: str,
    selected_decision_result: str,
    selected_policy_version: str,
    selected_evidence_source: str,
    time_windows: dict[str, float | None],
    now_ts: float | None,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for item in items:
        if selected_type not in {"", "all"} and str(item.get("type") or "") != selected_type:
            continue
        if selected_status not in {"", "all"} and str(item.get("status") or "") != selected_status:
            continue
        if selected_source_kind not in {"", "all"} and str(item.get("source_kind") or "") != selected_source_kind:
            continue
        if selected_source_row and not _item_matches_selected_source(item, selected_source_row):
            continue
        if selected_anchor not in {"", "all"} and str(item.get("anchor_id") or "") != selected_anchor:
            continue
        if not _matches_list_filter(item.get("phase_filters"), selected_phase):
            continue
        if not _matches_list_filter(item.get("artifact_role_filters"), selected_artifact_role):
            continue
        if not _matches_list_filter(item.get("standard_family_filters"), selected_standard_family):
            continue
        if not _matches_list_filter(item.get("evidence_category_filters"), selected_evidence_category):
            continue
        if not _matches_list_filter(item.get("boundary_filters"), selected_boundary):
            continue
        if not _matches_list_filter(item.get("route_filters"), selected_route):
            continue
        if not _matches_list_filter(item.get("signal_family_filters"), selected_signal_family):
            continue
        if not _matches_list_filter(item.get("decision_result_filters"), selected_decision_result):
            continue
        if not _matches_list_filter(item.get("policy_version_filters"), selected_policy_version):
            continue
        if not _matches_list_filter(item.get("evidence_source_filters"), selected_evidence_source):
            continue
        if not _matches_time_filter(item, selected_time, time_windows=time_windows, now_ts=now_ts):
            continue
        rows.append(dict(item))
    return rows


def _item_matches_selected_source(item: dict[str, Any], selected_source_row: dict[str, Any]) -> bool:
    item_source_id = str(item.get("source_id") or "").strip()
    item_source_label = str(item.get("source_label") or "").strip()
    selected_source_id = str(selected_source_row.get("source_id") or "").strip()
    selected_source_label = str(selected_source_row.get("source_label") or "").strip()
    if selected_source_id and item_source_id:
        if item_source_id == selected_source_id:
            return True
        if selected_source_id != selected_source_label:
            return False
    if selected_source_label and item_source_label == selected_source_label:
        return True
    return False


def _matches_time_filter(
    item: dict[str, Any],
    selected_time: str,
    *,
    time_windows: dict[str, float | None],
    now_ts: float | None,
) -> bool:
    if selected_time in {"", "all"}:
        return True
    window_seconds = time_windows.get(str(selected_time or ""), None)
    if window_seconds in (None, 0):
        return True
    try:
        sort_key = float(item.get("sort_key", 0.0) or 0.0)
    except Exception:
        return False
    if sort_key <= 0:
        return False
    return ((time.time() if now_ts is None else float(now_ts)) - sort_key) <= float(window_seconds)


def _build_source_scope_view(
    payload: dict[str, Any],
    *,
    base_index: dict[str, Any],
    scope_items: list[dict[str, Any]],
    filtered_items: list[dict[str, Any]],
    selected_source_row: dict[str, Any] | None,
) -> dict[str, Any]:
    bridge_digest = _phase_transition_bridge_digest(payload)
    bridge_panel = _phase_transition_bridge_panel(payload)
    bridge_panel_display = dict(bridge_panel.get("display", {}) or {})
    if not selected_source_row:
        readiness_summary = _merge_summary_text(
            str(dict(payload.get("acceptance_readiness", {}) or {}).get("summary") or t("common.none")),
            bridge_digest.get("status_line"),
        )
        analytics_summary = _merge_summary_text(
            str(dict(payload.get("analytics_summary", {}) or {}).get("summary") or t("common.none")),
            bridge_digest.get("summary_text"),
        )
        return {
            "index_text": "\n".join(
                line
                for line in (
                    str(base_index.get("summary") or "").strip(),
                    str(base_index.get("source_kind_summary") or "").strip(),
                    humanize_review_center_coverage_text(str(base_index.get("coverage_summary") or "").strip()),
                    str(base_index.get("diagnostics_summary") or "").strip(),
                )
                if line
            ) or t("common.none"),
            "operator_summary": str(dict(payload.get("operator_focus", {}) or {}).get("summary") or t("common.none")),
            "reviewer_summary": str(dict(payload.get("reviewer_focus", {}) or {}).get("summary") or t("common.none")),
            "approver_summary": str(dict(payload.get("approver_focus", {}) or {}).get("summary") or t("common.none")),
            "risk_summary": str(dict(payload.get("risk_summary", {}) or {}).get("summary") or t("common.none")),
            "readiness_summary": readiness_summary,
            "analytics_summary": analytics_summary,
            "lineage_summary": str(dict(payload.get("lineage_summary", {}) or {}).get("summary") or t("common.none")),
            "phase_bridge_summary": str(bridge_panel_display.get("card_text") or t("common.none")),
            "phase_bridge_panel": bridge_panel,
            "source_scope_label": t(
                "results.review_center.filter.active_source",
                source=t("results.review_center.filter.all_sources"),
                default=t("results.review_center.filter.all_sources"),
            ),
        }
    source_label = str(
        selected_source_row.get("source_label_display")
        or selected_source_row.get("source_label")
        or t("common.none")
    )
    latest_display = str(selected_source_row.get("latest_display") or "--")
    coverage_display = str(selected_source_row.get("coverage_display") or t("common.none"))
    gaps_display = str(selected_source_row.get("gaps_display") or t("common.none"))
    scope_display = str(
        selected_source_row.get("source_scope_display")
        or t(f"results.review_center.source_kind.{str(selected_source_row.get('source_scope') or 'run')}")
    )
    total_count = int(selected_source_row.get("evidence_count", len(scope_items)) or len(scope_items))
    visible_count = int(selected_source_row.get("visible_evidence_count", len(filtered_items)) or len(filtered_items))
    risk_summary = _source_scope_risk_summary(filtered_items, coverage_display=coverage_display, gaps_display=gaps_display)
    source_readiness_summary = humanize_review_surface_text(
        t(
            "results.review_center.scope.readiness_summary",
            source=source_label,
            coverage=coverage_display,
            gaps=gaps_display,
            visible=visible_count,
            total=total_count,
            default=f"{source_label} | {coverage_display} | {gaps_display} | {visible_count}/{total_count} | offline only",
        )
    )
    source_analytics_summary = t(
        "results.review_center.scope.analytics_summary",
        source=source_label,
        summary=_collect_scope_detail_summary(filtered_items, "detail_analytics_summary"),
        default=f"{source_label} | {_collect_scope_detail_summary(filtered_items, 'detail_analytics_summary')}",
    )
    lineage_summary = t(
        "results.review_center.scope.lineage_summary",
        source=source_label,
        summary=_collect_scope_detail_summary(filtered_items, "detail_lineage_summary"),
        default=f"{source_label} | {_collect_scope_detail_summary(filtered_items, 'detail_lineage_summary')}",
    )
    return {
        "index_text": "\n".join(
            line
            for line in (
                humanize_review_surface_text(
                    t(
                        "results.review_center.index.source_drilldown_summary",
                        source=source_label,
                        latest=latest_display,
                        coverage=coverage_display,
                        gaps=gaps_display,
                        scope=scope_display,
                        visible=visible_count,
                        total=total_count,
                        default=f"{source_label} | {latest_display} | {coverage_display} | {gaps_display} | {scope_display} | {visible_count}/{total_count}",
                    )
                ),
                str(base_index.get("diagnostics_summary") or "").strip(),
            )
            if line
        ) or t("common.none"),
        "operator_summary": humanize_review_surface_text(
            t(
                "results.review_center.focus.operator_source_summary",
                source=source_label,
                latest=latest_display,
                coverage=coverage_display,
                gaps=gaps_display,
                default=f"{source_label} | {latest_display} | {coverage_display} | {gaps_display} | offline only",
            )
        ),
        "reviewer_summary": humanize_review_surface_text(
            t(
                "results.review_center.focus.reviewer_source_summary",
                source=source_label,
                visible=visible_count,
                total=total_count,
                scope=scope_display,
                risk=risk_summary["summary"],
                gaps=gaps_display,
                default=f"{source_label} | {visible_count}/{total_count} | {scope_display} | {risk_summary['summary']}",
            )
        ),
        "approver_summary": humanize_review_surface_text(
            t(
                "results.review_center.focus.approver_source_summary",
                source=source_label,
                readiness=source_readiness_summary,
                visible=visible_count,
                total=total_count,
                risk=risk_summary["summary"],
                default=f"{source_label} | {source_readiness_summary} | {visible_count}/{total_count} | {risk_summary['summary']}",
            )
        ),
        "risk_summary": risk_summary["summary"],
        "readiness_summary": _merge_summary_text(source_readiness_summary, bridge_digest.get("status_line")),
        "analytics_summary": _merge_summary_text(source_analytics_summary, bridge_digest.get("next_stage_text")),
        "lineage_summary": lineage_summary,
        "phase_bridge_summary": str(bridge_panel_display.get("card_text") or t("common.none")),
        "phase_bridge_panel": bridge_panel,
        "source_scope_label": t(
            "results.review_center.filter.active_source",
            source=source_label,
            default=source_label,
        ),
    }


def _source_scope_risk_summary(
    filtered_items: list[dict[str, Any]],
    *,
    coverage_display: str,
    gaps_display: str,
) -> dict[str, str]:
    failed_count = sum(1 for item in filtered_items if str(item.get("status") or "") == "failed")
    degraded_count = sum(1 for item in filtered_items if str(item.get("status") or "") == "degraded")
    diagnostic_only_count = sum(1 for item in filtered_items if str(item.get("status") or "") == "diagnostic_only")
    missing_count = 0 if gaps_display == humanize_review_center_coverage_text(
        t("results.review_center.index.gaps_none", default="No gaps")
    ) else 1
    if failed_count > 0 or missing_count > 0:
        level = "high"
    elif degraded_count > 0 or diagnostic_only_count > 0:
        level = "medium"
    else:
        level = "low"
    return {
        "level": level,
        "summary": humanize_review_surface_text(
            t(
                "results.review_center.risk.summary",
                level=t(f"results.review_center.risk.{level}"),
                failed=failed_count,
                degraded=degraded_count,
                diagnostic=diagnostic_only_count,
                missing=missing_count,
                coverage=coverage_display,
                default=f"{level} | failed {failed_count} | degraded {degraded_count} | diagnostic {diagnostic_only_count} | missing {missing_count} | {coverage_display}",
            )
        ),
    }


def _collect_scope_detail_summary(filtered_items: list[dict[str, Any]], field_name: str) -> str:
    lines: list[str] = []
    for item in filtered_items:
        value = item.get(field_name)
        if isinstance(value, (list, tuple)):
            candidates = [str(part).strip() for part in value if str(part).strip()]
        else:
            candidates = [str(value).strip()] if str(value or "").strip() else []
        for candidate in candidates:
            if candidate not in lines:
                lines.append(candidate)
    return " | ".join(lines[:2]) if lines else t("results.review_center.scope.no_detail")
