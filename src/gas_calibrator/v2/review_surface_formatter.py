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
    ("Current review scope:", "\u5f53\u524d\u5ba1\u9605\u8303\u56f4\uff1a"),
    ("current review scope:", "\u5f53\u524d\u5ba1\u9605\u8303\u56f4\uff1a"),
    ("current-run", "\u5f53\u524d\u8fd0\u884c"),
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
