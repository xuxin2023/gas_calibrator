from __future__ import annotations

from typing import Any

from .ui_v2.i18n import t

_OFFLINE_DIAGNOSTIC_DISPLAY_LABELS = {
    "artifacts": "\u5de5\u4ef6",
    "plots": "\u56fe\u8868",
    "primary": "\u4e3b\u5de5\u4ef6",
    "supporting": "\u652f\u6491\u5de5\u4ef6",
}

_OFFLINE_DIAGNOSTIC_DETAIL_LABELS = {
    "classification": ("results.review_center.detail.offline_diagnostic_classification", "\u5206\u7c7b"),
    "recommended_variant": ("results.review_center.detail.offline_diagnostic_recommended_variant", "\u5efa\u8bae\u53d8\u4f53"),
    "dominant_error": ("results.review_center.detail.offline_diagnostic_dominant_error", "\u4e3b\u5bfc\u8bef\u5dee"),
    "next_check": ("results.review_center.detail.offline_diagnostic_next_check", "\u4e0b\u4e00\u6b65\u68c0\u67e5"),
    "continue_s1": ("results.review_center.detail.offline_diagnostic_continue_s1", "S1 \u7ee7\u7eed\u5224\u5b9a"),
    "dominant_conclusion": ("results.review_center.detail.offline_diagnostic_dominant_conclusion", "\u4e3b\u5bfc\u7ed3\u8bba"),
    "recommended_next_check": (
        "results.review_center.detail.offline_diagnostic_recommended_next_check",
        "\u5efa\u8bae\u4e0b\u4e00\u6b65\u68c0\u67e5",
    ),
    "bundle_dir": ("results.review_center.detail.offline_diagnostic_bundle_dir", "\u5de5\u4ef6\u76ee\u5f55"),
    "primary_artifact": ("results.review_center.detail.offline_diagnostic_primary_artifact", "\u4e3b\u5de5\u4ef6"),
}

_OFFLINE_DIAGNOSTIC_DETAIL_VALUE_LABELS = {
    "classification": {
        "warn": ("results.review_center.detail.offline_diagnostic_value.warn", "\u9884\u8b66"),
        "warning": ("results.review_center.detail.offline_diagnostic_value.warning", "\u9884\u8b66"),
        "fail": ("results.review_center.detail.offline_diagnostic_value.fail", "\u5931\u8d25"),
        "pass": ("results.review_center.detail.offline_diagnostic_value.pass", "\u901a\u8fc7"),
        "insufficient_evidence": (
            "results.review_center.detail.offline_diagnostic_value.insufficient_evidence",
            "\u8bc1\u636e\u4e0d\u8db3",
        ),
    },
    "continue_s1": {
        "continue": ("results.review_center.detail.offline_diagnostic_value.continue", "\u7ee7\u7eed"),
        "hold": ("results.review_center.detail.offline_diagnostic_value.hold", "\u4fdd\u6301"),
    },
}

_REVIEW_CENTER_COVERAGE_LABELS = {
    "coverage": "\u8986\u76d6",
    "complete": "\u5b8c\u6574",
    "gapped": "\u7f3a\u53e3",
    "missing": "\u7f3a\u5c11",
}

_REVIEW_SURFACE_FRAGMENT_LABELS = {
    "visible": "\u53ef\u89c1",
    "present": "\u5b58\u5728",
    "external": "\u5916\u90e8",
    "catalog": "\u5f53\u524d\u8fd0\u884c\u57fa\u7ebf",
    "filtered": "\u5f53\u524d\u7b5b\u9009",
    "failed": "\u5931\u8d25",
    "degraded": "\u964d\u7ea7",
    "diagnostic": "\u4ec5\u8bca\u65ad",
    "high": "\u9ad8",
    "medium": "\u4e2d",
    "low": "\u4f4e",
    "artifacts": "\u5de5\u4ef6",
    "plots": "\u56fe\u8868",
    "primary": "\u4e3b\u5de5\u4ef6",
    "supporting": "\u652f\u6491\u5de5\u4ef6",
}

_REVIEW_SURFACE_INLINE_REPLACEMENTS = (
    ("Current-run catalog baseline", "\u5f53\u524d\u8fd0\u884c\u57fa\u7ebf"),
    ("current-run catalog baseline", "\u5f53\u524d\u8fd0\u884c\u57fa\u7ebf"),
    ("current-run catalog", "\u5f53\u524d\u8fd0\u884c\u57fa\u7ebf"),
    ("current-run \u57fa\u7ebf", "\u5f53\u524d\u8fd0\u884c\u57fa\u7ebf"),
    ("Current review scope:", "\u5f53\u524d\u5ba1\u9605\u8303\u56f4\uff1a"),
    ("current review scope:", "\u5f53\u524d\u5ba1\u9605\u8303\u56f4\uff1a"),
    ("current-run", "\u5f53\u524d\u8fd0\u884c"),
    ("\u5f53\u524d\u8fd0\u884c \u57fa\u7ebf", "\u5f53\u524d\u8fd0\u884c\u57fa\u7ebf"),
    ("\u5f53\u524d scope \u603b\u91cf", "\u5f53\u524d\u53ef\u89c1"),
    ("\u5f53\u524d scope ", "\u5f53\u524d\u8303\u56f4 "),
    ("scope \u53ef\u89c1", "\u53ef\u89c1"),
    ("scope \u5b58\u5728", "\u5b58\u5728"),
    ("scope=", "\u8303\u56f4="),
    ("source=", "\u6765\u6e90="),
    ("evidence=", "\u8bc1\u636e="),
    ("offline only", "\u4ec5\u4f9b\u79bb\u7ebf\u5ba1\u9605"),
)


def offline_diagnostic_scope_label() -> str:
    return t("results.review_center.detail.offline_diagnostic_scope", default="Artifact Scope")


def build_offline_diagnostic_scope_summary(*, artifact_count: int, plot_count: int) -> str:
    parts = [f"artifacts {max(0, int(artifact_count or 0))}"]
    if int(plot_count or 0) > 0:
        parts.append(f"plots {int(plot_count or 0)}")
    return " | ".join(parts)


def build_offline_diagnostic_scope_line(scope_summary: str) -> str:
    text = humanize_offline_diagnostic_summary_value(scope_summary)
    if not text:
        return ""
    return offline_diagnostic_scope_label() + ": " + text


def build_offline_diagnostic_scope_line_from_counts(*, artifact_count: int, plot_count: int) -> str:
    return build_offline_diagnostic_scope_line(
        build_offline_diagnostic_scope_summary(
            artifact_count=artifact_count,
            plot_count=plot_count,
        )
    )


def normalize_offline_diagnostic_line(line: str) -> str:
    text = str(line or "").strip()
    marker = " | scope "
    if marker in text:
        prefix, suffix = text.split(marker, 1)
        scope_line = build_offline_diagnostic_scope_line(str(suffix or "").strip())
        return f"{prefix.strip()} | {scope_line}" if prefix.strip() else scope_line
    return text


def humanize_offline_diagnostic_summary_value(summary_value: str) -> str:
    text = str(summary_value or "").strip()
    if not text:
        return ""
    normalized_parts: list[str] = []
    for fragment in text.split("|"):
        part = str(fragment or "").strip()
        if not part:
            continue
        prefix, remainder = (part.split(" ", 1) + [""])[:2]
        label = _OFFLINE_DIAGNOSTIC_DISPLAY_LABELS.get(prefix.lower())
        if label:
            normalized_parts.append(f"{label} {remainder}".strip())
            continue
        normalized_parts.append(part)
    return " | ".join(normalized_parts)


def humanize_offline_diagnostic_detail_value(field_key: str, value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    key = str(field_key or "").strip().lower()
    value_key, default = _OFFLINE_DIAGNOSTIC_DETAIL_VALUE_LABELS.get(key, {}).get(text.lower(), (None, None))
    if value_key:
        return t(value_key, default=default)
    return text


def build_offline_diagnostic_detail_line(field_key: str, value: Any) -> str:
    text = humanize_offline_diagnostic_detail_value(field_key, value)
    if not text:
        return ""
    label_key, default = _OFFLINE_DIAGNOSTIC_DETAIL_LABELS.get(
        str(field_key or "").strip().lower(),
        ("", str(field_key or "").strip() or t("common.none")),
    )
    label = t(label_key, default=default) if label_key else default
    return f"{label}: {text}"


def humanize_review_center_coverage_text(summary_value: str) -> str:
    text = str(summary_value or "").strip()
    if not text:
        return ""
    normalized_parts: list[str] = []
    for fragment in text.split("|"):
        part = str(fragment or "").strip()
        if not part:
            continue
        if part.lower() == "no gaps":
            normalized_parts.append("\u65e0\u7f3a\u53e3")
            continue
        prefix, remainder = (part.split(" ", 1) + [""])[:2]
        label = _REVIEW_CENTER_COVERAGE_LABELS.get(prefix.lower())
        if label:
            normalized_parts.append(f"{label} {remainder}".strip())
            continue
        normalized_parts.append(part)
    return " | ".join(normalized_parts)


def humanize_review_surface_text(summary_value: str) -> str:
    text = normalize_offline_diagnostic_line(str(summary_value or "").strip())
    if not text:
        return ""
    text = humanize_review_center_coverage_text(text)
    for source, target in _REVIEW_SURFACE_INLINE_REPLACEMENTS:
        text = text.replace(source, target)
    normalized_parts: list[str] = []
    for fragment in text.split("|"):
        part = str(fragment or "").strip()
        if not part:
            continue
        lower = part.lower()
        for prefix, label in _REVIEW_SURFACE_FRAGMENT_LABELS.items():
            marker = prefix + " "
            if lower == prefix:
                part = label
                break
            if lower.startswith(marker):
                part = f"{label} {part[len(marker):]}".strip()
                break
        normalized_parts.append(part)
    return " | ".join(normalized_parts)


def build_review_scope_selection_line(
    *,
    scope: Any,
    source: Any,
    evidence: Any,
) -> str:
    return humanize_review_surface_text(
        f"scope={str(scope or 'all').strip() or 'all'} | "
        f"source={str(source or t('common.none')).strip() or t('common.none')} | "
        f"evidence={str(evidence or t('common.none')).strip() or t('common.none')}"
    )


def build_review_scope_counts_line(
    *,
    visible: Any,
    present: Any,
    external: Any,
    missing: Any,
    catalog_present: Any,
    catalog_total: Any,
) -> str:
    return humanize_review_surface_text(
        " | ".join(
            [
                f"visible {int(visible or 0)}",
                f"present {int(present or 0)}",
                f"external {int(external or 0)}",
                f"missing {int(missing or 0)}",
                f"catalog {int(catalog_present or 0)}/{int(catalog_total or 0)}",
            ]
        )
    )


def build_review_scope_reviewer_display(
    *,
    selection: dict[str, Any] | None = None,
    scope_summary: dict[str, Any] | None = None,
) -> dict[str, str]:
    selection_payload = dict(selection or {})
    summary_payload = dict(scope_summary or {})
    return {
        "selection_line": build_review_scope_selection_line(
            scope=str(selection_payload.get("scope") or summary_payload.get("scope") or "all"),
            source=str(
                selection_payload.get("selected_source_label_display")
                or selection_payload.get("selected_source_label")
                or t("common.none")
            ),
            evidence=str(selection_payload.get("selected_evidence_summary") or t("common.none")),
        ),
        "counts_line": build_review_scope_counts_line(
            visible=int(summary_payload.get("scope_visible_count", 0) or 0),
            present=int(summary_payload.get("scope_present_count", 0) or 0),
            external=int(summary_payload.get("scope_external_count", 0) or 0),
            missing=int(summary_payload.get("scope_missing_count", 0) or 0),
            catalog_present=int(summary_payload.get("catalog_present_count", 0) or 0),
            catalog_total=int(summary_payload.get("catalog_total_count", 0) or 0),
        ),
    }


def build_review_scope_payload_reviewer_display(
    *,
    selection: dict[str, Any] | None = None,
    scope_summary: dict[str, Any] | None = None,
) -> dict[str, str]:
    selection_payload = dict(selection or {})
    summary_payload = dict(scope_summary or {})
    return {
        **build_review_scope_reviewer_display(
            selection=selection_payload,
            scope_summary=summary_payload,
        ),
        **build_artifact_scope_reviewer_notes(
            scope_label=str(summary_payload.get("scope_label") or t("common.none")),
            visible_count=int(summary_payload.get("scope_visible_count", 0) or 0),
            present_count=int(summary_payload.get("scope_present_count", 0) or 0),
            scope_total_count=int(summary_payload.get("scope_visible_count", 0) or 0),
            external_count=int(summary_payload.get("scope_external_count", 0) or 0),
            missing_count=int(summary_payload.get("scope_missing_count", 0) or 0),
            catalog_present_count=int(summary_payload.get("catalog_present_count", 0) or 0),
            catalog_total_count=int(summary_payload.get("catalog_total_count", 0) or 0),
        ),
    }


def build_artifact_scope_reviewer_notes(
    *,
    scope_label: Any,
    visible_count: Any,
    present_count: Any,
    scope_total_count: Any,
    external_count: Any,
    missing_count: Any,
    catalog_present_count: Any,
    catalog_total_count: Any,
) -> dict[str, str]:
    scope_text = str(scope_label or t("pages.reports.artifact_scope.label_all")).strip() or t(
        "pages.reports.artifact_scope.label_all"
    )
    visible_value = int(visible_count or 0)
    present_value = int(present_count or 0)
    total_value = int(scope_total_count or 0)
    external_value = int(external_count or 0)
    missing_value = int(missing_count or 0)
    catalog_present_value = int(catalog_present_count or 0)
    catalog_total_value = int(catalog_total_count or 0)
    return {
        "run_dir_note_text": humanize_review_surface_text(
            t(
                "pages.reports.artifact_scope.run_dir_note",
                scope=scope_text,
                catalog_present=catalog_present_value,
                catalog_total=catalog_total_value,
                default=f"Current review scope: {scope_text} | catalog {catalog_present_value}/{catalog_total_value}",
            )
        ),
        "scope_note_text": humanize_review_surface_text(
            t(
                "pages.reports.artifact_scope.scope_note",
                scope=scope_text,
                visible=visible_value,
                total=total_value,
                external=external_value,
                missing=missing_value,
                catalog_total=catalog_total_value,
                default=(
                    f"{scope_text} | visible {visible_value} | external {external_value} | "
                    f"missing {missing_value} | catalog {catalog_total_value}"
                ),
            )
        ),
        "present_note_text": humanize_review_surface_text(
            t(
                "pages.reports.artifact_scope.present_note",
                scope=scope_text,
                present=present_value,
                visible=visible_value,
                total=total_value,
                missing=missing_value,
                catalog_present=catalog_present_value,
                catalog_total=catalog_total_value,
                default=(
                    f"{scope_text} | present {present_value}/{total_value} | "
                    f"missing {missing_value} | catalog {catalog_present_value}/{catalog_total_value}"
                ),
            )
        ),
    }


def build_offline_diagnostic_detail_item_line(item: Any) -> str:
    payload = dict(item or {}) if isinstance(item, dict) else {}
    if not payload:
        return ""
    line = normalize_offline_diagnostic_line(str(payload.get("detail_line") or payload.get("summary") or "").strip())
    scope = str(payload.get("artifact_scope_summary") or "").strip()
    if scope and scope.lower() not in line.lower():
        scope_line = build_offline_diagnostic_scope_line(scope)
        return f"{line} | {scope_line}" if line else scope_line
    return line


def collect_offline_diagnostic_detail_lines(
    offline_diagnostic_adapter_summary: dict[str, Any] | None,
    *,
    limit: int = 3,
) -> list[str]:
    summary = dict(offline_diagnostic_adapter_summary or {})
    lines: list[str] = []
    for item in list(summary.get("review_highlight_lines") or summary.get("detail_lines") or []):
        text = normalize_offline_diagnostic_line(str(item).strip())
        if text and text not in lines:
            lines.append(text)
    if len(lines) < limit:
        for item in list(summary.get("detail_items") or []):
            text = build_offline_diagnostic_detail_item_line(item)
            if text and text not in lines:
                lines.append(text)
            if len(lines) >= limit:
                break
    return lines[:limit]
