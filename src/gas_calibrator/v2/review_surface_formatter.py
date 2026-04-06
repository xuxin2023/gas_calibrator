from __future__ import annotations

from typing import Any

from .ui_v2.i18n import t


def offline_diagnostic_scope_label() -> str:
    return t("results.review_center.detail.offline_diagnostic_scope", default="Artifact Scope")


def build_offline_diagnostic_scope_summary(*, artifact_count: int, plot_count: int) -> str:
    parts = [f"artifacts {max(0, int(artifact_count or 0))}"]
    if int(plot_count or 0) > 0:
        parts.append(f"plots {int(plot_count or 0)}")
    return " | ".join(parts)


def build_offline_diagnostic_scope_line(scope_summary: str) -> str:
    text = str(scope_summary or "").strip()
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
