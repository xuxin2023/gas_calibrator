from __future__ import annotations

from typing import Any

from .ui_v2.i18n import t

_OFFLINE_DIAGNOSTIC_DISPLAY_LABELS = {
    "artifacts": "\u5de5\u4ef6",
    "plots": "\u56fe\u8868",
    "primary": "\u4e3b\u5de5\u4ef6",
    "supporting": "\u652f\u6491\u5de5\u4ef6",
}

_REVIEW_CENTER_COVERAGE_LABELS = {
    "coverage": "\u8986\u76d6",
    "complete": "\u5b8c\u6574",
    "gapped": "\u7f3a\u53e3",
    "missing": "\u7f3a\u5c11",
}


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
