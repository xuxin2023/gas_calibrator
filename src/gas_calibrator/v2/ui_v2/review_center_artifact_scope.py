from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ..review_surface_formatter import (
    build_artifact_scope_view_reviewer_display,
    build_review_scope_reviewer_display,
    hydrate_review_scope_reviewer_display,
    humanize_review_center_coverage_text,
    humanize_review_surface_text,
)
from .artifact_registry_governance import (
    OFFICIAL_EXPORT_STATUSES,
    build_current_run_governance,
    infer_artifact_identity,
    normalize_artifact_role,
    normalize_export_status,
    normalize_path_token,
)
from .i18n import display_artifact_role, t

try:
    from ..core.reviewer_summary_packs import build_compact_summary_render_context
except ImportError:
    build_compact_summary_render_context = None

SOURCE_SCAN_ENTRY_LIMIT = 128
SOURCE_SCAN_FILE_LIMIT = 64


def _build_compact_summary_pack_fields(
    packs: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Build compact summary pack fields for review_center surface rendering."""
    _raw_packs = list(packs or [])
    if _raw_packs and build_compact_summary_render_context is not None:
        try:
            _ctx = build_compact_summary_render_context(_raw_packs, surface="review_center")
            return {
                "compact_summary_packs": list(_ctx.get("compact_summary_packs") or []),
                "compact_summary_sections": list(_ctx.get("compact_summary_sections") or []),
                "compact_summary_order": list(_ctx.get("compact_summary_order") or []),
                "compact_summary_budget": dict(_ctx.get("compact_summary_budget") or {}),
            }
        except Exception:
            pass
    return {
        "compact_summary_packs": [],
        "compact_summary_sections": [],
        "compact_summary_order": [],
        "compact_summary_budget": {},
    }


def decorate_source_rows(
    rows: list[dict[str, Any]],
    *,
    visible_items: list[dict[str, Any]],
    item_matcher,
) -> list[dict[str, Any]]:
    label_counts: dict[str, int] = {}
    for row in rows:
        label = str(row.get("source_label") or "").strip()
        if label:
            label_counts[label] = label_counts.get(label, 0) + 1
    decorated: list[dict[str, Any]] = []
    for row in rows:
        payload = dict(row)
        source_label = str(payload.get("source_label") or t("common.none"))
        coverage_display = humanize_review_center_coverage_text(str(payload.get("coverage_display") or t("common.none")))
        gaps_display = humanize_review_center_coverage_text(str(payload.get("gaps_display") or t("common.none")))
        visible_count = sum(1 for item in visible_items if item_matcher(item, payload))
        total_count = int(payload.get("evidence_count", visible_count) or visible_count)
        source_hint = _source_disambiguation_hint(payload) if label_counts.get(source_label, 0) > 1 else ""
        payload["coverage_display"] = coverage_display
        payload["gaps_display"] = gaps_display
        payload["source_hint"] = source_hint
        payload["source_label_display"] = (
            t(
                "results.review_center.index.source_disambiguation",
                source=source_label,
                hint=source_hint,
                default=f"{source_label} ({source_hint})",
            )
            if source_hint
            else source_label
        )
        payload["visible_evidence_count"] = visible_count
        payload["scope_count_display"] = t(
            "results.review_center.index.source_scope_count",
            coverage=coverage_display,
            visible=visible_count,
            total=total_count,
            default=(
                f"{coverage_display or t('common.none')} | "
                f"filtered {visible_count}/{total_count}"
            ),
        )
        payload["scope_count_display"] = humanize_review_surface_text(str(payload.get("scope_count_display") or ""))
        decorated.append(payload)
    return decorated


def build_review_center_selection_snapshot(
    active_view: dict[str, Any],
    *,
    scope: str = "all",
    selected_item: dict[str, Any] | None = None,
    item_matcher,
) -> dict[str, Any]:
    scope_items = [dict(item) for item in list(active_view.get("scope_items", active_view.get("items", [])) or [])]
    filtered_items = [dict(item) for item in list(active_view.get("items", []) or [])]
    selected_source_row = dict(active_view.get("selected_source_row", {}) or {})
    current_item = dict(selected_item or {})
    if not current_item and filtered_items:
        current_item = dict(filtered_items[0])
    if not selected_source_row and current_item:
        selected_source_row = {
            "source_id": str(current_item.get("source_id") or ""),
            "source_label": str(current_item.get("source_label") or ""),
            "source_label_display": str(
                current_item.get("source_label_display")
                or current_item.get("source_label")
                or ""
            ),
            "source_dir": str(current_item.get("source_dir") or ""),
            "source_scope": str(current_item.get("source_scope") or current_item.get("source_kind") or "run"),
            "source_scope_display": str(
                current_item.get("source_scope_display")
                or current_item.get("source_kind_display")
                or ""
            ),
        }

    normalized_scope = _normalize_scope(scope)
    if normalized_scope == "source" and not selected_source_row:
        normalized_scope = "all"
    if normalized_scope == "evidence" and not current_item:
        normalized_scope = "source" if selected_source_row else "all"

    source_items = [
        dict(item)
        for item in scope_items
        if selected_source_row and item_matcher(item, selected_source_row)
    ]
    source_label = str(selected_source_row.get("source_label") or "")
    source_label_display = str(
        selected_source_row.get("source_label_display")
        or source_label
        or t("results.review_center.filter.all_sources")
    )
    source_total = int(selected_source_row.get("evidence_count", len(source_items)) or len(source_items))
    source_visible = int(selected_source_row.get("visible_evidence_count", len(source_items)) or len(source_items))
    return {
        "scope": normalized_scope,
        "selected_source_id": str(selected_source_row.get("source_id") or ""),
        "selected_source_label": source_label,
        "selected_source_label_display": source_label_display,
        "selected_source_dir": str(selected_source_row.get("source_dir") or ""),
        "selected_source_scope": str(selected_source_row.get("source_scope") or ""),
        "selected_source_scope_display": str(selected_source_row.get("source_scope_display") or ""),
        "selected_source_artifact_paths": _collect_artifact_paths(source_items),
        "selected_source_visible_count": source_visible,
        "selected_source_total_count": source_total,
        "selected_evidence_item": dict(current_item or {}),
        "selected_evidence_summary": str(
            current_item.get("detail_summary")
            or current_item.get("summary")
            or current_item.get("type_display")
            or t("common.none")
        ),
        "selected_evidence_artifact_paths": _collect_artifact_paths([current_item] if current_item else []),
        "visible_item_count": len(filtered_items),
        "scope_item_count": len(scope_items),
    }


def build_artifact_scope_view(
    files: list[dict[str, Any]],
    *,
    selection: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return build_review_artifact_registry(files, selection=selection)


def build_review_artifact_registry(
    files: list[dict[str, Any]],
    *,
    selection: dict[str, Any] | None = None,
    compact_summary_packs: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    all_rows = _normalize_artifact_rows(files)
    selection_payload = dict(selection or {})
    scope = _normalize_scope(selection_payload.get("scope"))
    source_dir = str(selection_payload.get("selected_source_dir") or "")
    scanned_paths = _scan_source_dir_paths(source_dir) if scope in {"source", "evidence"} else []
    current_lookup = {
        normalize_path_token(row.get("path")): dict(row)
        for row in all_rows
        if normalize_path_token(row.get("path"))
    }
    catalog_total_count = len(all_rows)
    catalog_present_count = sum(1 for row in all_rows if bool(row.get("present_on_disk", False)))

    if scope == "source":
        scope_label = t("pages.reports.artifact_scope.label_source")
        source_label = str(
            selection_payload.get("selected_source_label_display")
            or selection_payload.get("selected_source_label")
            or t("common.none")
        )
        rows = _resolve_source_artifact_rows(
            all_rows,
            current_lookup=current_lookup,
            source_dir=source_dir,
            related_paths=list(selection_payload.get("selected_source_artifact_paths") or []),
            scanned_paths=scanned_paths,
        )
        summary_text = _build_scope_summary_text(
            scope_key="pages.reports.artifact_scope.summary_source",
            scope_label=scope_label,
            scope_rows=rows,
            source=source_label,
            catalog_present_count=catalog_present_count,
            catalog_total_count=catalog_total_count,
        )
    elif scope == "evidence":
        scope_label = t("pages.reports.artifact_scope.label_evidence")
        source_label = str(
            selection_payload.get("selected_source_label_display")
            or selection_payload.get("selected_source_label")
            or t("common.none")
        )
        evidence_label = str(selection_payload.get("selected_evidence_summary") or t("common.none"))
        rows = _resolve_related_artifact_rows(
            current_lookup=current_lookup,
            related_paths=list(selection_payload.get("selected_evidence_artifact_paths") or []),
            scanned_paths=scanned_paths,
        )
        summary_text = _build_scope_summary_text(
            scope_key="pages.reports.artifact_scope.summary_evidence",
            scope_label=scope_label,
            scope_rows=rows,
            source=source_label,
            evidence=evidence_label,
            catalog_present_count=catalog_present_count,
            catalog_total_count=catalog_total_count,
        )
    else:
        scope_label = t("pages.reports.artifact_scope.label_all")
        rows = [_decorate_artifact_row({**dict(row), "scope_match": "all"}) for row in all_rows]
        summary_text = _build_scope_summary_text(
            scope_key="pages.reports.artifact_scope.summary_all",
            scope_label=scope_label,
            scope_rows=rows,
            catalog_present_count=catalog_present_count,
            catalog_total_count=catalog_total_count,
        )

    scope_visible_count = len(rows)
    scope_present_count = sum(1 for row in rows if bool(row.get("present_on_disk", False)))
    scope_external_count = sum(
        1
        for row in rows
        if str(row.get("artifact_origin") or "") in {"review_reference", "source_scan"}
    )
    scope_missing_count = sum(1 for row in rows if not bool(row.get("present_on_disk", False)))
    catalog_note_text = t(
        "pages.reports.artifact_scope.catalog_note",
        present=catalog_present_count,
        total=catalog_total_count,
        default=f"Current-run catalog baseline {catalog_present_count}/{catalog_total_count}",
    )
    empty_text = (
        ""
        if rows
        else t(
            "pages.reports.artifact_scope.empty",
            scope=scope_label,
            default=f"No artifacts for {scope_label}. Offline review only.",
        )
    )
    export_warning_text = (
        ""
        if scope == "all"
        else t(
            "pages.reports.artifact_scope.export_scope_warning",
            scope=scope_label,
            default=f"Export still targets current-run artifacts and does not follow the {scope_label} review scope.",
        )
    )
    reviewer_display = build_artifact_scope_view_reviewer_display(
        summary_text=summary_text,
        scope_label=scope_label,
        visible_count=scope_visible_count,
        present_count=scope_present_count,
        scope_total_count=scope_visible_count,
        external_count=scope_external_count,
        missing_count=scope_missing_count,
        catalog_present_count=catalog_present_count,
        catalog_total_count=catalog_total_count,
        catalog_note_text=catalog_note_text,
        empty_text=empty_text,
        export_warning_text=export_warning_text,
    )

    return {
        "scope": scope,
        "rows": rows,
        "catalog_total_count": catalog_total_count,
        "catalog_present_count": catalog_present_count,
        "scope_visible_count": scope_visible_count,
        "scope_present_count": scope_present_count,
        "scope_external_count": scope_external_count,
        "scope_missing_count": scope_missing_count,
        "visible_count": scope_visible_count,
        "present_count": scope_present_count,
        "total_count": scope_visible_count,
        "total_present_count": catalog_present_count,
        "scope_label": scope_label,
        "summary_text": reviewer_display["summary_text"],
        "reviewer_display": reviewer_display,
        "run_dir_note_text": reviewer_display["run_dir_note_text"],
        "scope_note_text": reviewer_display["scope_note_text"],
        "present_note_text": reviewer_display["present_note_text"],
        "catalog_note_text": reviewer_display["catalog_note_text"],
        "empty_text": reviewer_display["empty_text"],
        "disclaimer_text": t("pages.reports.artifact_scope.disclaimer"),
        "export_warning_text": reviewer_display["export_warning_text"],
        "clear_enabled": scope != "all",
        **_build_compact_summary_pack_fields(compact_summary_packs),
    }


def build_review_scope_manifest_payload(
    files: list[dict[str, Any]],
    *,
    selection: dict[str, Any] | None = None,
    run_dir: str = "",
) -> dict[str, Any]:
    registry = build_review_artifact_registry(files, selection=selection)
    rows = [_build_manifest_row(row) for row in list(registry.get("rows", []) or [])]
    selection_snapshot = _sanitize_selection_snapshot(selection)
    scope_summary = {
        "scope": str(registry.get("scope") or "all"),
        "scope_label": str(registry.get("scope_label") or t("common.none")),
        "summary_text": str(registry.get("summary_text") or t("common.none")),
        "catalog_total_count": int(registry.get("catalog_total_count", 0) or 0),
        "catalog_present_count": int(registry.get("catalog_present_count", 0) or 0),
        "scope_visible_count": int(registry.get("scope_visible_count", 0) or 0),
        "scope_present_count": int(registry.get("scope_present_count", 0) or 0),
        "scope_external_count": int(registry.get("scope_external_count", 0) or 0),
        "scope_missing_count": int(registry.get("scope_missing_count", 0) or 0),
    }
    reviewer_display = {
        **dict(registry.get("reviewer_display", {}) or {}),
        **build_review_scope_reviewer_display(
            selection=selection_snapshot,
            scope_summary=scope_summary,
        ),
    }
    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "run_dir": str(run_dir or ""),
        "selection": selection_snapshot,
        "scope_summary": scope_summary,
        "reviewer_display": reviewer_display,
        "disclaimer": {
            "text": t("pages.reports.review_scope_manifest.disclaimer"),
            "offline_review_only": True,
            "simulated_or_replay_context": True,
            "diagnostic_context": True,
            "not_real_acceptance_evidence": True,
        },
        "rows": rows,
    }
    reviewer_artifact_entry = _find_phase_transition_bridge_reviewer_artifact_entry(
        list(registry.get("rows", []) or [])
    )
    if reviewer_artifact_entry:
        payload["phase_transition_bridge_reviewer_artifact_entry"] = reviewer_artifact_entry
    stage_admission_review_pack_entry = _find_stage_admission_review_pack_artifact_entry(
        list(registry.get("rows", []) or [])
    )
    if stage_admission_review_pack_entry:
        payload["stage_admission_review_pack_artifact_entry"] = stage_admission_review_pack_entry
    engineering_isolation_admission_checklist_entry = _find_engineering_isolation_admission_checklist_artifact_entry(
        list(registry.get("rows", []) or [])
    )
    if engineering_isolation_admission_checklist_entry:
        payload["engineering_isolation_admission_checklist_artifact_entry"] = (
            engineering_isolation_admission_checklist_entry
        )
    stage3_real_validation_plan_entry = _find_stage3_real_validation_plan_artifact_entry(
        list(registry.get("rows", []) or [])
    )
    if stage3_real_validation_plan_entry:
        payload["stage3_real_validation_plan_artifact_entry"] = stage3_real_validation_plan_entry
    stage3_standards_alignment_matrix_entry = _find_stage3_standards_alignment_matrix_artifact_entry(
        list(registry.get("rows", []) or [])
    )
    if stage3_standards_alignment_matrix_entry:
        payload["stage3_standards_alignment_matrix_artifact_entry"] = stage3_standards_alignment_matrix_entry
    return payload


def render_review_scope_manifest_markdown(payload: dict[str, Any]) -> str:
    selection = dict(payload.get("selection", {}) or {})
    scope_summary = dict(payload.get("scope_summary", {}) or {})
    disclaimer = dict(payload.get("disclaimer", {}) or {})
    reviewer_display = hydrate_review_scope_reviewer_display(
        payload,
        selection=selection,
        scope_summary=scope_summary,
    ) or build_review_scope_reviewer_display(
        selection=selection,
        scope_summary=scope_summary,
    )
    lines = [
        f"# {t('pages.reports.review_scope_manifest.title')}",
        "",
        f"- {t('pages.reports.review_scope_manifest.generated_at')}: {payload.get('generated_at', '--')}",
        f"- {t('pages.reports.review_scope_manifest.scope')}: {scope_summary.get('scope_label', '--')}",
        f"- {t('pages.reports.review_scope_manifest.selection')}: {reviewer_display.get('selection_line', t('common.none'))}",
        f"- {t('pages.reports.review_scope_manifest.counts')}: {reviewer_display.get('counts_line', t('common.none'))}",
        f"- {t('pages.reports.review_scope_manifest.disclaimer_label')}: {disclaimer.get('text', t('common.none'))}",
        "",
        f"## {t('pages.reports.review_scope_manifest.rows')}",
        "",
        "| "
        + " | ".join(
            [
                t("widgets.artifact_list.artifact"),
                t("widgets.artifact_list.origin"),
                t("widgets.artifact_list.role_status"),
                t("widgets.artifact_list.path"),
                t("pages.reports.review_scope_manifest.note"),
            ]
        )
        + " |",
        "| --- | --- | --- | --- | --- |",
    ]
    for row in list(payload.get("rows", []) or []):
        lines.append(
            "| "
            + " | ".join(
                [
                    _escape_markdown_table_cell(row.get("name")),
                    _escape_markdown_table_cell(row.get("artifact_origin_display")),
                    _escape_markdown_table_cell(row.get("role_status_display")),
                    _escape_markdown_table_cell(row.get("path")),
                    _escape_markdown_table_cell(row.get("note")),
                ]
            )
            + " |"
        )
    lines.append("")
    return "\n".join(lines)


def _normalize_scope(value: Any) -> str:
    scope = str(value or "all").strip().lower() or "all"
    return scope if scope in {"all", "source", "evidence"} else "all"


def _build_scope_summary_text(
    *,
    scope_key: str,
    scope_label: str,
    scope_rows: list[dict[str, Any]],
    catalog_present_count: int,
    catalog_total_count: int,
    source: str = "",
    evidence: str = "",
) -> str:
    scope_visible_count = len(scope_rows)
    scope_present_count = sum(1 for row in scope_rows if bool(row.get("present_on_disk", False)))
    scope_external_count = sum(
        1
        for row in scope_rows
        if str(row.get("artifact_origin") or "") in {"review_reference", "source_scan"}
    )
    scope_missing_count = sum(1 for row in scope_rows if not bool(row.get("present_on_disk", False)))
    return humanize_review_surface_text(
        t(
        scope_key,
        source=source,
        evidence=evidence,
        visible=scope_visible_count,
        present=scope_present_count,
        total=scope_visible_count,
        external=scope_external_count,
        missing=scope_missing_count,
        catalog_present=catalog_present_count,
        catalog_total=catalog_total_count,
        default=(
            f"{scope_label} | visible {scope_visible_count} | "
            f"present {scope_present_count}/{scope_visible_count} | "
            f"external {scope_external_count} | missing {scope_missing_count} | "
            f"catalog {catalog_present_count}/{catalog_total_count}"
        ),
    )
    )


def _source_disambiguation_hint(row: dict[str, Any]) -> str:
    source_dir = str(row.get("source_dir") or "").strip()
    source_label = str(row.get("source_label") or "").strip()
    if source_dir:
        try:
            source_path = Path(source_dir)
            parent_name = source_path.parent.name.strip()
            if parent_name and parent_name != source_label:
                return parent_name
            grandparent_name = source_path.parent.parent.name.strip()
            if grandparent_name and grandparent_name != source_label:
                return f"{grandparent_name}/{source_path.parent.name}".strip("/")
        except Exception:
            pass
    source_scope_display = str(row.get("source_scope_display") or "").strip()
    return source_scope_display if source_scope_display and source_scope_display != source_label else ""


def _collect_artifact_paths(items: list[dict[str, Any]]) -> list[str]:
    rows: list[str] = []
    for item in items:
        for value in list(item.get("detail_artifact_paths") or []) + list(item.get("artifact_paths") or []):
            text = str(value or "").strip()
            if text and text not in rows:
                rows.append(text)
    return rows


def _normalize_artifact_rows(raw_files: Any) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    for raw in list(raw_files or []):
        item = dict(raw or {})
        path = str(item.get("path") or "").strip()
        name = str(item.get("name") or Path(path).name or t("common.none"))
        key = normalize_path_token(path) or f"name::{name.strip().lower()}"
        if key in seen:
            continue
        seen.add(key)
        present_on_disk = bool(item.get("present_on_disk", item.get("present", False))) or _path_exists(path)
        listed_in_current_run = bool(item.get("listed_in_current_run", True))
        inferred_identity = infer_artifact_identity(path or name)
        governance = (
            {
                **build_current_run_governance(path, present_on_disk=present_on_disk),
                "artifact_key": str(item.get("artifact_key") or inferred_identity.get("artifact_key") or ""),
                "artifact_role": str(item.get("artifact_role") or inferred_identity.get("artifact_role") or "unclassified"),
                "export_status": item.get("export_status"),
                "export_status_known": bool(item.get("export_status_known", item.get("export_status") in OFFICIAL_EXPORT_STATUSES)),
                "exportable_in_current_run": bool(item.get("exportable_in_current_run", present_on_disk)),
            }
            if listed_in_current_run
            else {
                "artifact_key": str(item.get("artifact_key") or inferred_identity.get("artifact_key") or ""),
                "artifact_role": str(item.get("artifact_role") or inferred_identity.get("artifact_role") or "unclassified"),
                "export_status": item.get("export_status"),
                "export_status_known": bool(item.get("export_status_known", False)),
                "exportable_in_current_run": bool(item.get("exportable_in_current_run", False)),
            }
        )
        rows.append(
            _decorate_artifact_row(
                {
                    **item,
                    "name": name,
                    "path": path,
                    "present_on_disk": present_on_disk,
                    "listed_in_current_run": listed_in_current_run,
                    "artifact_origin": str(
                        item.get("artifact_origin")
                        or ("current_run" if listed_in_current_run else "review_reference")
                    ),
                    "scope_match": str(item.get("scope_match") or "all"),
                    **governance,
                }
            )
        )
    return rows


def _resolve_source_artifact_rows(
    all_rows: list[dict[str, Any]],
    *,
    current_lookup: dict[str, dict[str, Any]],
    source_dir: str,
    related_paths: list[str],
    scanned_paths: list[str],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    trailing_rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    source_dir_token = normalize_path_token(source_dir)
    for row in all_rows:
        path_token = normalize_path_token(row.get("path"))
        if source_dir_token and _path_is_within(path_token, source_dir_token):
            _append_unique_row(
                rows,
                seen,
                _decorate_artifact_row({**dict(row), "scope_match": "source"}),
            )
    for path in related_paths:
        row = _build_reference_row(path, current_lookup=current_lookup, scope_match="source")
        if row is None:
            continue
        if str(row.get("artifact_origin") or "") == "missing_reference":
            _append_unique_row(trailing_rows, seen, row)
            continue
        _append_unique_row(rows, seen, row)
    for path in scanned_paths:
        row = _build_scanned_row(path, current_lookup=current_lookup, scope_match="source")
        if row is None:
            continue
        _append_unique_row(rows, seen, row)
    rows.extend(trailing_rows)
    return rows


def _resolve_related_artifact_rows(
    *,
    current_lookup: dict[str, dict[str, Any]],
    related_paths: list[str],
    scanned_paths: list[str],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    trailing_rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    for path in related_paths:
        row = _build_reference_row(path, current_lookup=current_lookup, scope_match="evidence")
        if row is None:
            continue
        if str(row.get("artifact_origin") or "") == "missing_reference":
            _append_unique_row(trailing_rows, seen, row)
            continue
        _append_unique_row(rows, seen, row)
    for path in scanned_paths:
        row = _build_scanned_row(path, current_lookup=current_lookup, scope_match="source")
        if row is None:
            continue
        _append_unique_row(rows, seen, row)
    rows.extend(trailing_rows)
    return rows


def _path_is_within(path_token: str, source_dir_token: str) -> bool:
    if not path_token or not source_dir_token:
        return False
    return path_token == source_dir_token or path_token.startswith(source_dir_token + "/")


def _append_unique_row(rows: list[dict[str, Any]], seen: set[str], row: dict[str, Any]) -> None:
    key = normalize_path_token(row.get("path")) or f"name::{str(row.get('name') or '').strip().lower()}"
    if key in seen:
        return
    seen.add(key)
    rows.append(dict(row))


def _decorate_artifact_row(row: dict[str, Any]) -> dict[str, Any]:
    payload = dict(row or {})
    artifact_origin = str(payload.get("artifact_origin") or "current_run").strip().lower() or "current_run"
    if artifact_origin not in {"current_run", "review_reference", "source_scan", "missing_reference"}:
        artifact_origin = "current_run"
    scope_match = _normalize_scope(payload.get("scope_match"))
    present_on_disk = bool(payload.get("present_on_disk", payload.get("present", False)))
    listed_in_current_run = bool(payload.get("listed_in_current_run", False))
    path = str(payload.get("path") or "").strip()
    name = str(payload.get("name") or Path(path).name or t("common.none"))

    inferred_identity = infer_artifact_identity(path or name)
    artifact_key = str(payload.get("artifact_key") or inferred_identity.get("artifact_key") or "")
    artifact_role = normalize_artifact_role(payload.get("artifact_role") or inferred_identity.get("artifact_role"))
    export_status = normalize_export_status(payload.get("export_status"))
    export_status_known = bool(payload.get("export_status_known", export_status in OFFICIAL_EXPORT_STATUSES))
    exportable_in_current_run = bool(
        payload.get("exportable_in_current_run", listed_in_current_run and present_on_disk)
    )
    export_status_display = (
        t(f"widgets.artifact_list.export_status_{export_status}", default=export_status)
        if export_status_known and export_status
        else t("widgets.artifact_list.export_status_unregistered")
    )
    role_display = display_artifact_role(
        artifact_role,
        default=t("widgets.artifact_list.unclassified"),
    )
    exportability_display = _build_exportability_display(
        artifact_origin=artifact_origin,
        listed_in_current_run=listed_in_current_run,
        present_on_disk=present_on_disk,
        exportable_in_current_run=exportable_in_current_run,
    )
    note = str(payload.get("note") or payload.get("reason") or "").strip()
    return {
        **payload,
        "name": name,
        "path": path,
        "present": present_on_disk,
        "present_on_disk": present_on_disk,
        "listed_in_current_run": listed_in_current_run,
        "artifact_origin": artifact_origin,
        "artifact_origin_display": t(
            f"widgets.artifact_list.origin_{artifact_origin}",
            default=artifact_origin,
        ),
        "scope_match": scope_match,
        "artifact_key": artifact_key,
        "artifact_role": artifact_role,
        "artifact_role_display": role_display,
        "export_status": export_status,
        "export_status_display": export_status_display,
        "export_status_known": export_status_known,
        "exportable_in_current_run": exportable_in_current_run,
        "exportability_display": exportability_display,
        "role_status_display": str(
            payload.get("role_status_display") or f"{role_display} | {export_status_display} | {exportability_display}"
        ),
        "note": note,
    }


def _build_reference_row(
    value: Any,
    *,
    current_lookup: dict[str, dict[str, Any]],
    scope_match: str,
) -> dict[str, Any] | None:
    path = str(value or "").strip()
    if not path:
        return None
    token = normalize_path_token(path)
    if token in current_lookup:
        return _decorate_artifact_row({**dict(current_lookup[token]), "scope_match": scope_match})
    present_on_disk = _path_exists(path)
    inferred_identity = infer_artifact_identity(path)
    return _decorate_artifact_row(
        {
            "name": Path(path).name or path,
            "path": path,
            "present_on_disk": present_on_disk,
            "listed_in_current_run": False,
            "artifact_origin": "review_reference" if present_on_disk else "missing_reference",
            "scope_match": scope_match,
            "artifact_key": inferred_identity.get("artifact_key"),
            "artifact_role": inferred_identity.get("artifact_role"),
            "export_status": None,
            "export_status_known": False,
            "exportable_in_current_run": False,
        }
    )


def _build_scanned_row(
    value: Any,
    *,
    current_lookup: dict[str, dict[str, Any]],
    scope_match: str,
) -> dict[str, Any] | None:
    path = str(value or "").strip()
    if not path:
        return None
    token = normalize_path_token(path)
    if token in current_lookup:
        return _decorate_artifact_row({**dict(current_lookup[token]), "scope_match": scope_match})
    inferred_identity = infer_artifact_identity(path)
    return _decorate_artifact_row(
        {
            "name": Path(path).name or path,
            "path": path,
            "present_on_disk": _path_exists(path),
            "listed_in_current_run": False,
            "artifact_origin": "source_scan",
            "scope_match": scope_match,
            "artifact_key": inferred_identity.get("artifact_key"),
            "artifact_role": inferred_identity.get("artifact_role"),
            "export_status": None,
            "export_status_known": False,
            "exportable_in_current_run": False,
        }
    )


def _scan_source_dir_paths(source_dir: str) -> list[str]:
    text = str(source_dir or "").strip()
    if not text:
        return []
    try:
        source_path = Path(text)
    except Exception:
        return []
    if not source_path.exists() or not source_path.is_dir():
        return []
    try:
        children = sorted(source_path.iterdir(), key=lambda item: item.name.lower())
    except Exception:
        return []
    rows: list[str] = []
    for index, child in enumerate(children):
        if index >= SOURCE_SCAN_ENTRY_LIMIT or len(rows) >= SOURCE_SCAN_FILE_LIMIT:
            break
        try:
            if child.is_file():
                rows.append(str(child))
        except Exception:
            return rows
    return rows


def _path_exists(value: Any) -> bool:
    text = str(value or "").strip()
    if not text:
        return False
    try:
        return Path(text).exists()
    except Exception:
        return False


def _build_exportability_display(
    *,
    artifact_origin: str,
    listed_in_current_run: bool,
    present_on_disk: bool,
    exportable_in_current_run: bool,
) -> str:
    if exportable_in_current_run:
        return t("widgets.artifact_list.exportability_current_run")
    if listed_in_current_run and not present_on_disk:
        return t("widgets.artifact_list.exportability_current_run_missing")
    if artifact_origin == "review_reference":
        return t("widgets.artifact_list.exportability_review_reference")
    if artifact_origin == "source_scan":
        return t("widgets.artifact_list.exportability_source_scan")
    if artifact_origin == "missing_reference":
        return t("widgets.artifact_list.exportability_missing_reference")
    return t("widgets.artifact_list.exportability_review_reference")


def _sanitize_selection_snapshot(selection: dict[str, Any] | None) -> dict[str, Any]:
    payload = dict(selection or {})
    return {
        "scope": _normalize_scope(payload.get("scope")),
        "selected_source_id": str(payload.get("selected_source_id") or ""),
        "selected_source_label": str(payload.get("selected_source_label") or ""),
        "selected_source_label_display": str(payload.get("selected_source_label_display") or ""),
        "selected_source_dir": str(payload.get("selected_source_dir") or ""),
        "selected_source_scope": str(payload.get("selected_source_scope") or ""),
        "selected_source_scope_display": str(payload.get("selected_source_scope_display") or ""),
        "selected_source_artifact_paths": [str(item) for item in list(payload.get("selected_source_artifact_paths") or [])],
        "selected_source_visible_count": int(payload.get("selected_source_visible_count", 0) or 0),
        "selected_source_total_count": int(payload.get("selected_source_total_count", 0) or 0),
        "selected_evidence_summary": str(payload.get("selected_evidence_summary") or ""),
        "selected_evidence_artifact_paths": [str(item) for item in list(payload.get("selected_evidence_artifact_paths") or [])],
        "visible_item_count": int(payload.get("visible_item_count", 0) or 0),
        "scope_item_count": int(payload.get("scope_item_count", 0) or 0),
    }


def _build_manifest_row(row: dict[str, Any]) -> dict[str, Any]:
    payload = dict(row or {})
    note = _build_manifest_note(payload)
    return {
        "name": str(payload.get("name") or t("common.none")),
        "path": str(payload.get("path") or ""),
        "present_on_disk": bool(payload.get("present_on_disk", False)),
        "listed_in_current_run": bool(payload.get("listed_in_current_run", False)),
        "artifact_origin": str(payload.get("artifact_origin") or ""),
        "artifact_origin_display": str(payload.get("artifact_origin_display") or ""),
        "scope_match": str(payload.get("scope_match") or ""),
        "artifact_role": str(payload.get("artifact_role") or "unclassified"),
        "artifact_role_display": str(payload.get("artifact_role_display") or t("widgets.artifact_list.unclassified")),
        "export_status": payload.get("export_status"),
        "export_status_display": str(payload.get("export_status_display") or t("widgets.artifact_list.export_status_unregistered")),
        "export_status_known": bool(payload.get("export_status_known", False)),
        "exportable_in_current_run": bool(payload.get("exportable_in_current_run", False)),
        "role_status_display": str(payload.get("role_status_display") or ""),
        "note": note,
    }


def _build_manifest_note(row: dict[str, Any]) -> str:
    note_override = str(row.get("note") or "").strip()
    if note_override:
        return note_override
    artifact_origin = str(row.get("artifact_origin") or "")
    if bool(row.get("listed_in_current_run", False)):
        if bool(row.get("exportable_in_current_run", False)):
            if bool(row.get("export_status_known", False)):
                return humanize_review_surface_text(
                    t(
                        "pages.reports.review_scope_manifest.note_current_run_status",
                        status=str(row.get("export_status_display") or t("widgets.artifact_list.export_status_unregistered")),
                        default=f"Current-run artifact with export status {row.get('export_status_display')}",
                    )
                )
            return t("pages.reports.review_scope_manifest.note_current_run_unregistered")
        return t("pages.reports.review_scope_manifest.note_current_run_missing")
    if artifact_origin == "review_reference":
        return t("pages.reports.review_scope_manifest.note_review_reference")
    if artifact_origin == "source_scan":
        return t("pages.reports.review_scope_manifest.note_source_scan")
    if artifact_origin == "missing_reference":
        return t("pages.reports.review_scope_manifest.note_missing_reference")
    return t("pages.reports.review_scope_manifest.note_review_reference")

def _escape_markdown_table_cell(value: Any) -> str:
    return str(value or "").replace("|", "\\|").replace("\n", "<br/>")


def _find_phase_transition_bridge_reviewer_artifact_entry(files: list[dict[str, Any]]) -> dict[str, Any]:
    for item in list(files or []):
        entry = dict(dict(item or {}).get("phase_transition_bridge_reviewer_artifact_entry") or {})
        if entry:
            return entry
    return {}


def _find_stage_admission_review_pack_artifact_entry(files: list[dict[str, Any]]) -> dict[str, Any]:
    for item in list(files or []):
        entry = dict(dict(item or {}).get("stage_admission_review_pack_artifact_entry") or {})
        if entry:
            return entry
    return {}


def _find_engineering_isolation_admission_checklist_artifact_entry(files: list[dict[str, Any]]) -> dict[str, Any]:
    for item in list(files or []):
        entry = dict(dict(item or {}).get("engineering_isolation_admission_checklist_artifact_entry") or {})
        if entry:
            return entry
    return {}


def _find_stage3_real_validation_plan_artifact_entry(files: list[dict[str, Any]]) -> dict[str, Any]:
    for item in list(files or []):
        entry = dict(dict(item or {}).get("stage3_real_validation_plan_artifact_entry") or {})
        if entry:
            return entry
    return {}


def _find_stage3_standards_alignment_matrix_artifact_entry(files: list[dict[str, Any]]) -> dict[str, Any]:
    for item in list(files or []):
        entry = dict(dict(item or {}).get("stage3_standards_alignment_matrix_artifact_entry") or {})
        if entry:
            return entry
    return {}
