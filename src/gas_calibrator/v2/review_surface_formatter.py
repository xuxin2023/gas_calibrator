from __future__ import annotations

from typing import Any

from .core.phase_taxonomy_contract import (
    GAP_CLASSIFICATION_FAMILY,
    GAP_SEVERITY_FAMILY,
    METHOD_CONFIRMATION_FAMILY,
    TRACEABILITY_NODE_FAMILY,
    UNCERTAINTY_INPUT_FAMILY,
    normalize_phase_taxonomy_row,
    taxonomy_text_replacements,
)
from .core.reviewer_fragments_contract import (
    BLOCKER_FRAGMENT_FAMILY,
    BOUNDARY_FRAGMENT_FAMILY,
    GAP_REASON_FRAGMENT_FAMILY,
    NON_CLAIM_FRAGMENT_FAMILY,
    PHASE_CONTRAST_FRAGMENT_FAMILY,
    READINESS_IMPACT_FRAGMENT_FAMILY,
    REVIEWER_NEXT_STEP_FRAGMENT_FAMILY,
    fragment_text_replacements,
)
from .ui_v2.i18n import (
    display_fragment_value,
    display_fragment_values,
    display_phase,
    display_route,
    display_taxonomy_value,
    display_taxonomy_values,
    t,
)

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

_MEASUREMENT_LAYER_LABELS = {
    "reference": ("results.review_center.detail.measurement.layer.reference", "\u53c2\u8003\u5c42"),
    "analyzer_raw": ("results.review_center.detail.measurement.layer.analyzer_raw", "\u5206\u6790\u4eea\u539f\u59cb\u5c42"),
    "output": ("results.review_center.detail.measurement.layer.output", "\u8f93\u51fa\u5c42"),
    "data_quality": ("results.review_center.detail.measurement.layer.data_quality", "\u6570\u636e\u8d28\u91cf\u5c42"),
}

_MEASUREMENT_BUCKET_LABELS = {
    "payload_complete": ("results.review_center.detail.measurement.bucket.payload_complete", "payload \u5b8c\u6574"),
    "payload_partial": ("results.review_center.detail.measurement.bucket.payload_partial", "payload \u90e8\u5206"),
    "trace_only": ("results.review_center.detail.measurement.bucket.trace_only", "\u4ec5 trace"),
    "model_only": ("results.review_center.detail.measurement.bucket.model_only", "\u4ec5\u6a21\u578b"),
    "test_only": ("results.review_center.detail.measurement.bucket.test_only", "\u4ec5\u6d4b\u8bd5"),
    "gap": ("results.review_center.detail.measurement.bucket.gap", "\u7f3a\u53e3"),
    "actual_simulated_run_with_payload_complete": (
        "results.review_center.detail.measurement.bucket.payload_complete",
        "payload \u5b8c\u6574",
    ),
    "actual_simulated_run_with_payload_partial": (
        "results.review_center.detail.measurement.bucket.payload_partial",
        "payload \u90e8\u5206",
    ),
    "trace_only_not_evaluated": ("results.review_center.detail.measurement.bucket.trace_only", "\u4ec5 trace"),
}

_REVIEW_SURFACE_PREFIX_LABELS = {
    "payload-backed phases": ("results.review_center.detail.measurement.payload_phases", "payload \u9636\u6bb5"),
    "payload-complete phases": (
        "results.review_center.detail.measurement.payload_complete_phases",
        "payload \u5b8c\u6574\u9636\u6bb5",
    ),
    "payload-partial phases": (
        "results.review_center.detail.measurement.payload_partial_phases",
        "payload \u90e8\u5206\u9636\u6bb5",
    ),
    "trace-only phases": ("results.review_center.detail.measurement.trace_only_phases", "\u4ec5 trace \u9636\u6bb5"),
    "payload completeness": (
        "results.review_center.detail.measurement.payload_completeness",
        "payload \u5b8c\u6574\u5ea6",
    ),
    "phase gaps": ("results.review_center.detail.measurement.phase_gaps", "\u9636\u6bb5\u7f3a\u53e3"),
    "blockers": ("results.review_center.detail.measurement.blockers", "\u5f53\u524d\u963b\u585e"),
    "next artifacts": ("results.review_center.detail.measurement.next_artifacts", "\u4e0b\u4e00\u6b65\u8865\u8bc1\u5de5\u4ef6"),
    "preseal partial guidance": (
        "results.review_center.detail.measurement.preseal_partial_guidance",
        "preseal \u90e8\u5206 payload \u63d0\u793a",
    ),
    "linked method confirmation items": (
        "results.review_center.detail.measurement.linked_method_items",
        "\u5173\u8054\u65b9\u6cd5\u786e\u8ba4\u6761\u76ee",
    ),
    "linked uncertainty inputs": (
        "results.review_center.detail.measurement.linked_uncertainty_inputs",
        "\u5173\u8054\u4e0d\u786e\u5b9a\u5ea6\u8f93\u5165",
    ),
    "linked traceability stub nodes": (
        "results.review_center.detail.measurement.linked_traceability_nodes",
        "\u5173\u8054\u6eaf\u6e90\u8282\u70b9",
    ),
    "reviewer next steps": (
        "results.review_center.detail.measurement.reviewer_next_steps",
        "\u5ba1\u9605\u4e0b\u4e00\u6b65",
    ),
    "phase contrast": (
        "results.review_center.detail.measurement.phase_contrast",
        "preseal / pressure_stable \u5bf9\u7167",
    ),
    "route families": ("results.review_center.detail.measurement.route_families", "\u8def\u7531\u5bb6\u65cf"),
    "phase buckets": ("results.review_center.detail.measurement.phase_buckets", "\u9636\u6bb5\u6876\u4f4d"),
    "provenance summary": (
        "results.review_center.detail.measurement.provenance_summary",
        "\u8bc1\u636e\u6765\u6e90\u6458\u8981",
    ),
    "linked readiness anchors": (
        "results.review_center.detail.measurement.linked_readiness",
        "\u5173\u8054\u5c31\u7eea\u5de5\u4ef6",
    ),
    "readiness impact": (
        "results.review_center.detail.measurement.readiness_impact",
        "\u5c31\u7eea\u5ea6\u5f71\u54cd",
    ),
    "synthetic provenance": (
        "results.review_center.detail.measurement.synthetic_provenance",
        "\u4eff\u771f\u6765\u6e90",
    ),
    "linked measurement phases": (
        "results.review_center.detail.readiness.linked_measurement_phases",
        "\u5173\u8054\u6d4b\u91cf\u9636\u6bb5",
    ),
    "linked measurement gaps": (
        "results.review_center.detail.readiness.linked_measurement_gaps",
        "\u5173\u8054\u6d4b\u91cf\u7f3a\u53e3",
    ),
    "gap index": ("results.review_center.detail.measurement.gap_index", "\u7f3a\u53e3\u7d22\u5f15"),
    "preseal partial gap": (
        "results.review_center.detail.readiness.preseal_partial_gap",
        "preseal \u90e8\u5206 payload \u7f3a\u53e3",
    ),
    "linked artifacts": ("results.review_center.detail.readiness.linked_artifacts", "\u5173\u8054\u5de5\u4ef6"),
    "linked method confirmation items": (
        "results.review_center.detail.readiness.linked_method_items",
        "\u5173\u8054\u65b9\u6cd5\u786e\u8ba4\u6761\u76ee",
    ),
    "linked uncertainty inputs": (
        "results.review_center.detail.readiness.linked_uncertainty_inputs",
        "\u5173\u8054\u4e0d\u786e\u5b9a\u5ea6\u8f93\u5165",
    ),
    "linked traceability nodes": (
        "results.review_center.detail.readiness.linked_traceability_nodes",
        "\u5173\u8054\u6eaf\u6e90\u8282\u70b9",
    ),
    "missing evidence": ("results.review_center.detail.readiness.missing_evidence", "\u4ecd\u7f3a\u8bc1\u636e"),
    "blockers": ("results.review_center.detail.readiness.blockers", "\u5f53\u524d\u963b\u585e"),
    "gap reason": ("results.review_center.detail.readiness.gap_reason", "\u7f3a\u53e3\u539f\u56e0"),
    "readiness status": ("results.review_center.detail.readiness.status", "\u5c31\u7eea\u72b6\u6001"),
    "next required artifacts": (
        "results.review_center.detail.readiness.next_artifacts",
        "\u4e0b\u4e00\u6b65\u8865\u8bc1\u5de5\u4ef6",
    ),
    "reviewer next step": (
        "results.review_center.detail.readiness.reviewer_next_step",
        "\u5ba1\u9605\u4e0b\u4e00\u6b65",
    ),
    "non-claim digest": (
        "results.review_center.detail.readiness.non_claim",
        "\u975e\u58f0\u660e\u8fb9\u754c",
    ),
    "boundary": ("results.review_center.detail.boundary", "\u8fb9\u754c"),
}

_REVIEW_SCOPE_REVIEWER_DISPLAY_FIELDS = (
    "summary_text",
    "selection_line",
    "counts_line",
    "run_dir_note_text",
    "scope_note_text",
    "present_note_text",
    "catalog_note_text",
    "empty_text",
    "export_warning_text",
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
    for source, (key, default) in _MEASUREMENT_BUCKET_LABELS.items():
        text = text.replace(source, t(key, default=default))
    for source, target in taxonomy_text_replacements():
        text = text.replace(source, target)
    for source, target in fragment_text_replacements():
        text = text.replace(source, target)
    for source, target in _REVIEW_SURFACE_INLINE_REPLACEMENTS:
        text = text.replace(source, target)
    if ":" in text:
        prefix, remainder = text.split(":", 1)
        prefix_key, prefix_default = _REVIEW_SURFACE_PREFIX_LABELS.get(prefix.strip().lower(), ("", ""))
        if prefix_key:
            text = f"{t(prefix_key, default=prefix_default)}: {remainder.strip()}"
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


def _dedupe_lines(lines: list[str]) -> list[str]:
    rows: list[str] = []
    for line in lines:
        text = str(line or "").strip()
        if text and text not in rows:
            rows.append(text)
    return rows


def _dedupe(values: Any) -> list[str]:
    rows: list[str] = []
    for value in values:
        text = str(value or "").strip()
        if text and text not in rows:
            rows.append(text)
    return rows


def _display_measurement_layer_list(layers: list[Any]) -> str:
    values = []
    for layer in list(layers or []):
        key, default = _MEASUREMENT_LAYER_LABELS.get(str(layer or "").strip(), ("", str(layer or "").strip()))
        values.append(t(key, default=default) if key else default)
    return "、".join(item for item in values if item) or t("common.none")


def _display_measurement_bucket(bucket: Any) -> str:
    key, default = _MEASUREMENT_BUCKET_LABELS.get(str(bucket or "").strip(), ("", str(bucket or "").strip()))
    return t(key, default=default) if key else str(bucket or "").strip()


def _display_route_phase(row: dict[str, Any]) -> str:
    route = display_route(row.get("route_family"), default=str(row.get("route_family") or "--"))
    phase = display_phase(row.get("phase_name"), default=str(row.get("phase_name") or "--"))
    return f"{route}/{phase}"


def _display_text_list(values: list[Any]) -> str:
    return " | ".join(str(item).strip() for item in list(values or []) if str(item).strip()) or t("common.none")


def _normalize_measurement_phase_row(row: dict[str, Any]) -> dict[str, Any]:
    return normalize_phase_taxonomy_row(dict(row or {}), display_locale="en_US")


def _display_taxonomy_list(
    family: str,
    *,
    key_values: list[Any] | None = None,
    display_values: list[Any] | None = None,
) -> str:
    values = list(key_values or []) or list(display_values or [])
    labels = display_taxonomy_values(family, values)
    return " | ".join(labels) if labels else t("common.none")


def _display_fragment_list(
    family: str,
    *,
    fragment_rows: list[Any] | None = None,
    fragment_keys: list[Any] | None = None,
    text_values: list[Any] | None = None,
    default_text: str | None = None,
) -> str:
    labels = display_fragment_values(family, list(fragment_rows or []))
    if not labels and list(fragment_keys or []):
        labels = display_fragment_values(family, list(fragment_keys or []))
    if not labels and list(text_values or []):
        labels = display_fragment_values(family, list(text_values or []))
    if labels:
        return " | ".join(labels)
    text = humanize_review_surface_text(str(default_text or "").strip())
    return text or t("common.none")


def _display_gap_classification(row: dict[str, Any]) -> str:
    payload = _normalize_measurement_phase_row(row)
    return display_taxonomy_value(
        GAP_CLASSIFICATION_FAMILY,
        payload.get("gap_classification"),
        default=str(payload.get("gap_classification") or t("common.none")),
    )


def _display_gap_severity(row: dict[str, Any]) -> str:
    payload = _normalize_measurement_phase_row(row)
    return display_taxonomy_value(
        GAP_SEVERITY_FAMILY,
        payload.get("gap_severity"),
        default=str(payload.get("gap_severity") or t("common.none")),
    )


def _display_gap_reason(row: dict[str, Any]) -> str:
    payload = _normalize_measurement_phase_row(row)
    return _display_fragment_list(
        GAP_REASON_FRAGMENT_FAMILY,
        fragment_rows=list(payload.get("gap_reason_fragments") or []),
        fragment_keys=list(payload.get("gap_reason_fragment_keys") or []),
        text_values=[payload.get("gap_reason") or payload.get("missing_reason_digest")],
        default_text=str(payload.get("gap_reason") or payload.get("missing_reason_digest") or ""),
    )


def _display_readiness_impact(row: dict[str, Any]) -> str:
    payload = _normalize_measurement_phase_row(row)
    return _display_fragment_list(
        READINESS_IMPACT_FRAGMENT_FAMILY,
        fragment_rows=list(payload.get("readiness_impact_fragments") or []),
        fragment_keys=list(payload.get("readiness_impact_fragment_keys") or []),
        text_values=[payload.get("readiness_impact_digest")],
        default_text=str(payload.get("readiness_impact_digest") or ""),
    )


def _display_blockers(row: dict[str, Any]) -> str:
    payload = _normalize_measurement_phase_row(row)
    return _display_fragment_list(
        BLOCKER_FRAGMENT_FAMILY,
        fragment_rows=list(payload.get("blocker_fragments") or []),
        fragment_keys=list(payload.get("blocker_fragment_keys") or []),
        text_values=list(payload.get("blockers") or []),
        default_text=" | ".join(str(item).strip() for item in list(payload.get("blockers") or []) if str(item).strip()),
    )


def _display_reviewer_next_step(row: dict[str, Any]) -> str:
    payload = _normalize_measurement_phase_row(row)
    return _display_fragment_list(
        REVIEWER_NEXT_STEP_FRAGMENT_FAMILY,
        fragment_rows=list(payload.get("reviewer_next_step_fragments") or []),
        fragment_keys=list(payload.get("reviewer_next_step_fragment_keys") or [payload.get("reviewer_next_step_template_key")]),
        text_values=[payload.get("reviewer_next_step_digest")],
        default_text=str(payload.get("reviewer_next_step_digest") or ""),
    )


def _display_boundary_summary(payload: dict[str, Any]) -> str:
    return _display_fragment_list(
        BOUNDARY_FRAGMENT_FAMILY,
        fragment_rows=list(payload.get("boundary_fragments") or []),
        fragment_keys=list(payload.get("boundary_fragment_keys") or []),
        text_values=list(payload.get("boundary_statements") or [])
        or [payload.get("phase_boundary_digest") or payload.get("boundary_digest")],
        default_text=str(payload.get("phase_boundary_digest") or payload.get("boundary_digest") or ""),
    )


def _display_non_claim_summary(payload: dict[str, Any]) -> str:
    return _display_fragment_list(
        NON_CLAIM_FRAGMENT_FAMILY,
        fragment_rows=list(payload.get("non_claim_fragments") or []),
        fragment_keys=list(payload.get("non_claim_fragment_keys") or []),
        text_values=list(payload.get("non_claim") or []) or [payload.get("non_claim_digest")],
        default_text=str(payload.get("non_claim_digest") or ""),
    )


def _display_phase_contrast_summary(payload: dict[str, Any], fallback: str) -> str:
    return _display_fragment_list(
        PHASE_CONTRAST_FRAGMENT_FAMILY,
        fragment_rows=list(payload.get("phase_contrast_fragments") or payload.get("comparison_fragments") or []),
        fragment_keys=list(payload.get("phase_contrast_fragment_keys") or payload.get("comparison_fragment_keys") or []),
        text_values=[payload.get("phase_contrast_summary") or payload.get("comparison_digest") or fallback],
        default_text=str(payload.get("phase_contrast_summary") or payload.get("comparison_digest") or fallback or ""),
    )


def _summary_to_lines(text: str) -> list[str]:
    return _dedupe(part.strip() for part in str(text or "").split("|"))


def collect_boundary_digest_lines(*payloads: Any) -> list[str]:
    rows: list[str] = []
    for payload in payloads:
        current = dict(payload or {}) if isinstance(payload, dict) else {}
        if not current:
            continue
        for summary in (_display_boundary_summary(current), _display_non_claim_summary(current)):
            for line in _summary_to_lines(summary):
                if line and line != t("common.none") and line not in rows:
                    rows.append(line)
    return rows


def _phase_field_summary(
    rows: list[dict[str, Any]],
    *,
    family: str,
    key_field_name: str,
    display_field_name: str,
) -> str:
    return " | ".join(
        _dedupe(
            f"{_display_route_phase(row)}: {_display_taxonomy_list(family, key_values=list(row.get(key_field_name) or []), display_values=list(row.get(display_field_name) or []))}"
            for row in rows
            if list(row.get(key_field_name) or []) or list(row.get(display_field_name) or [])
        )
    ) or t("common.none")


def _gap_index_summary(rows: list[dict[str, Any]]) -> str:
    return " | ".join(
        _dedupe(
            f"{_display_route_phase(row)}: {_display_gap_classification(row)} / {_display_gap_severity(row)}"
            for row in rows
            if str(row.get("gap_classification") or "").strip()
        )
    ) or t("common.none")


def _reviewer_next_step_summary(rows: list[dict[str, Any]]) -> str:
    return " | ".join(
        _dedupe(
            _display_reviewer_next_step(row)
            for row in rows
            if list(row.get("reviewer_next_step_fragments") or [])
            or str(row.get("reviewer_next_step_digest") or "").strip()
        )
    ) or t("common.none")


def _localized_phase_contrast_summary(rows: list[dict[str, Any]], fallback: str) -> str:
    parts: list[str] = []
    preseal_row = next(
        (
            row
            for row in rows
            if str(row.get("phase_name") or "").strip() == "preseal"
            and str(row.get("coverage_bucket") or "").strip() == "actual_simulated_run_with_payload_partial"
        ),
        {},
    )
    pressure_row = next(
        (
            row
            for row in rows
            if str(row.get("phase_name") or "").strip() == "pressure_stable"
            and str(row.get("coverage_bucket") or "").strip() == "actual_simulated_run_with_payload_complete"
        ),
        {},
    )
    if preseal_row and pressure_row:
        preseal_missing = _display_measurement_layer_list(list(preseal_row.get("missing_signal_layers") or []))
        pressure_available = _display_measurement_layer_list(list(pressure_row.get("available_signal_layers") or []))
        parts.append(
            t(
                "results.review_center.detail.measurement.phase_contrast_preseal_vs_stable",
                preseal=display_phase("preseal", default="preseal"),
                stable=display_phase("pressure_stable", default="pressure_stable"),
                missing=preseal_missing,
                available=pressure_available,
                default=f"preseal 保持 payload-partial，因为 {preseal_missing} 仍需明确；pressure_stable 在 {pressure_available} 全部具备时可达到 payload-complete，并可挂接更完整的方法/不确定度/溯源审阅链。",
            )
        )
    payload_backed_rows = [
        row
        for row in rows
        if str(row.get("phase_name") or "").strip() in {"ambient_diagnostic", "sample_ready", "recovery_retry"}
        and str(row.get("coverage_bucket") or "").strip() == "actual_simulated_run_with_payload_complete"
    ]
    if payload_backed_rows:
        phases = " | ".join(_dedupe(_display_route_phase(row) for row in payload_backed_rows))
        method_summary = " | ".join(
            _dedupe(
                label
                for row in payload_backed_rows
                for label in display_taxonomy_values(
                    METHOD_CONFIRMATION_FAMILY,
                    list(row.get("linked_method_confirmation_item_keys") or row.get("linked_method_confirmation_items") or []),
                )
            )
        ) or t("common.none")
        uncertainty_summary = " | ".join(
            _dedupe(
                label
                for row in payload_backed_rows
                for label in display_taxonomy_values(
                    UNCERTAINTY_INPUT_FAMILY,
                    list(row.get("linked_uncertainty_input_keys") or row.get("linked_uncertainty_inputs") or []),
                )
            )
        ) or t("common.none")
        traceability_summary = " | ".join(
            _dedupe(
                label
                for row in payload_backed_rows
                for label in display_taxonomy_values(
                    TRACEABILITY_NODE_FAMILY,
                    list(row.get("linked_traceability_node_keys") or row.get("linked_traceability_nodes") or row.get("linked_traceability_stub_nodes") or []),
                )
            )
        ) or t("common.none")
        parts.append(
            t(
                "results.review_center.detail.measurement.phase_contrast_payload_backed",
                phases=phases,
                method=method_summary,
                uncertainty=uncertainty_summary,
                traceability=traceability_summary,
                default=f"payload-backed ambient/recovery 阶段 {phases} 会继续公开方法 {method_summary}、不确定度 {uncertainty_summary} 与溯源 {traceability_summary}，但仍然保持 Step 2 仿真审阅边界。",
            )
        )
    trace_only_rows = [
        row
        for row in rows
        if str(row.get("phase_name") or "").strip() in {"ambient_diagnostic", "sample_ready", "recovery_retry"}
        and str(row.get("payload_completeness") or "").strip() == "trace_only"
    ]
    if trace_only_rows:
        phases = " | ".join(_dedupe(_display_route_phase(row) for row in trace_only_rows))
        parts.append(
            t(
                "results.review_center.detail.measurement.phase_contrast_trace_only",
                phases=phases,
                default=f"仅 trace 阶段 {phases} 仍保持相同 taxonomy 可见，但在提升为 payload-backed 证据前，审阅闭环不会关闭。",
            )
        )
    return " | ".join(part for part in parts if str(part).strip()) or humanize_review_surface_text(fallback)


def build_measurement_review_digest_lines(payload: dict[str, Any]) -> dict[str, list[str]]:
    raw = dict(payload.get("raw") or payload or {})
    digest = dict(raw.get("digest") or payload.get("digest") or {})
    phase_rows = [
        _normalize_measurement_phase_row(dict(item))
        for item in list(raw.get("phase_rows") or payload.get("phase_rows") or [])
        if isinstance(item, dict)
    ]
    gap_rows = [dict(item) for item in list(raw.get("linked_measurement_gaps") or payload.get("linked_measurement_gaps") or []) if isinstance(item, dict)]
    linked_method_summary = _phase_field_summary(
        phase_rows,
        family=METHOD_CONFIRMATION_FAMILY,
        key_field_name="linked_method_confirmation_item_keys",
        display_field_name="linked_method_confirmation_items",
    )
    linked_uncertainty_summary = _phase_field_summary(
        phase_rows,
        family=UNCERTAINTY_INPUT_FAMILY,
        key_field_name="linked_uncertainty_input_keys",
        display_field_name="linked_uncertainty_inputs",
    )
    linked_traceability_summary = _phase_field_summary(
        phase_rows,
        family=TRACEABILITY_NODE_FAMILY,
        key_field_name="linked_traceability_node_keys",
        display_field_name="linked_traceability_stub_nodes",
    )
    gap_index_summary = _gap_index_summary(phase_rows)
    reviewer_next_step_summary = _reviewer_next_step_summary(phase_rows)
    readiness_impact_summary = " | ".join(
        _dedupe(
            f"{_display_route_phase(row)}: {_display_readiness_impact(row)}"
            for row in phase_rows
            if str(row.get("coverage_bucket") or "").strip() != "actual_simulated_run_with_payload_complete"
        )
    ) or humanize_review_surface_text(str(digest.get("readiness_impact_summary") or t("common.none")))
    blocker_summary = " | ".join(
        _dedupe(
            f"{_display_route_phase(row)}: {_display_blockers(row)}"
            for row in phase_rows
            if str(row.get("coverage_bucket") or "").strip() != "actual_simulated_run_with_payload_complete"
            and (
                list(row.get("blocker_fragments") or [])
                or list(row.get("blockers") or [])
            )
        )
    ) or humanize_review_surface_text(str(digest.get("blocker_summary") or t("common.none")))
    phase_contrast_summary = _localized_phase_contrast_summary(
        phase_rows,
        str(digest.get("phase_contrast_summary") or ""),
    )
    summary_lines = [
        humanize_review_surface_text(str(digest.get("summary") or "")),
        t(
            "results.review_center.detail.measurement.payload_complete_phases_line",
            value=str(digest.get("payload_complete_phase_summary") or t("common.none")),
            default=f"payload 完整阶段：{str(digest.get('payload_complete_phase_summary') or t('common.none'))}",
        ),
        t(
            "results.review_center.detail.measurement.payload_partial_phases_line",
            value=str(digest.get("payload_partial_phase_summary") or t("common.none")),
            default=f"payload 部分阶段：{str(digest.get('payload_partial_phase_summary') or t('common.none'))}",
        ),
        t(
            "results.review_center.detail.measurement.trace_only_phases_line",
            value=str(digest.get("trace_only_phase_summary") or t("common.none")),
            default=f"仅 trace 阶段：{str(digest.get('trace_only_phase_summary') or t('common.none'))}",
        ),
        t(
            "results.review_center.detail.measurement.next_artifacts_line",
            value=str(digest.get("next_required_artifacts_summary") or t("common.none")),
            default=f"下一步补证工件：{str(digest.get('next_required_artifacts_summary') or t('common.none'))}",
        ),
        t(
            "results.review_center.detail.measurement.blockers_line",
            value=blocker_summary,
            default=f"当前阻塞：{blocker_summary}",
        ),
        t(
            "results.review_center.detail.measurement.preseal_partial_guidance_line",
            value=str(digest.get("preseal_partial_guidance_summary") or t("common.none")),
            default=f"preseal 部分 payload 提示：{str(digest.get('preseal_partial_guidance_summary') or t('common.none'))}",
        ),
        t(
            "results.review_center.detail.measurement.linked_method_items_line",
            value=linked_method_summary,
            default=f"关联方法确认条目：{str(digest.get('linked_method_confirmation_summary') or t('common.none'))}",
        ),
        t(
            "results.review_center.detail.measurement.linked_uncertainty_inputs_line",
            value=linked_uncertainty_summary,
            default=f"关联不确定度输入：{str(digest.get('linked_uncertainty_input_summary') or t('common.none'))}",
        ),
        t(
            "results.review_center.detail.measurement.linked_traceability_nodes_line",
            value=linked_traceability_summary,
            default=f"关联溯源节点：{str(digest.get('linked_traceability_stub_summary') or t('common.none'))}",
        ),
        t(
            "results.review_center.detail.measurement.reviewer_next_steps_line",
            value=reviewer_next_step_summary,
            default=f"审阅下一步：{str(digest.get('reviewer_next_step_summary') or t('common.none'))}",
        ),
        t(
            "results.review_center.detail.measurement.phase_contrast_line",
            value=phase_contrast_summary,
            default=f"preseal / pressure_stable 对照：{str(digest.get('phase_contrast_summary') or t('common.none'))}",
        ),
    ]
    detail_lines = [
        t(
            "results.review_center.detail.measurement.readiness_impact_line",
            value=readiness_impact_summary,
            default=f"就绪度影响：{readiness_impact_summary}",
        ),
        t(
            "results.review_center.detail.measurement.linked_readiness_line",
            value=str(digest.get("linked_readiness_summary") or t("common.none")),
            default=f"关联就绪工件：{str(digest.get('linked_readiness_summary') or t('common.none'))}",
        ),
        t(
            "results.review_center.detail.measurement.gap_index_line",
            value=gap_index_summary,
            default=f"缺口索引：{str(digest.get('gap_index_summary') or t('common.none'))}",
        ),
    ]
    for row in phase_rows:
        route_phase = _display_route_phase(row)
        next_artifacts_text = _display_text_list(list(row.get("next_required_artifacts") or []))
        blockers_text = _display_blockers(row)
        gap_reason_text = _display_gap_reason(row)
        readiness_impact_text = _display_readiness_impact(row)
        reviewer_next_step_text = _display_reviewer_next_step(row)
        detail_lines.append(
            t(
                "results.review_center.detail.measurement.phase_guidance_line",
                phase=route_phase,
                bucket=_display_measurement_bucket(row.get("coverage_bucket_display") or row.get("coverage_bucket")),
                available=_display_measurement_layer_list(list(row.get("available_signal_layers") or [])),
                missing=_display_measurement_layer_list(list(row.get("missing_signal_layers") or [])),
                reason=gap_reason_text,
                impact=readiness_impact_text,
                next="、".join(str(item).strip() for item in list(row.get("next_required_artifacts") or []) if str(item).strip()) or t("common.none"),
                boundary=str(row.get("phase_boundary_digest") or t("common.none")),
                default=(
                    f"{route_phase}：桶位 {_display_measurement_bucket(row.get('coverage_bucket_display') or row.get('coverage_bucket'))}"
                    f"；已有 {_display_measurement_layer_list(list(row.get('available_signal_layers') or []))}"
                    f"；仍缺 {_display_measurement_layer_list(list(row.get('missing_signal_layers') or []))}"
                    f"；原因 {gap_reason_text}"
                    f"；影响 {readiness_impact_text}"
                    f"；下一步 {'、'.join(str(item).strip() for item in list(row.get('next_required_artifacts') or []) if str(item).strip()) or t('common.none')}"
                    f"；边界 {str(row.get('phase_boundary_digest') or t('common.none'))}"
                ),
            )
        )
        detail_lines.append(
            t(
                "results.review_center.detail.measurement.phase_navigation_line",
                phase=route_phase,
                method=_display_taxonomy_list(
                    METHOD_CONFIRMATION_FAMILY,
                    key_values=list(row.get("linked_method_confirmation_item_keys") or []),
                    display_values=list(row.get("linked_method_confirmation_items") or []),
                ),
                uncertainty=_display_taxonomy_list(
                    UNCERTAINTY_INPUT_FAMILY,
                    key_values=list(row.get("linked_uncertainty_input_keys") or []),
                    display_values=list(row.get("linked_uncertainty_inputs") or []),
                ),
                traceability=_display_taxonomy_list(
                    TRACEABILITY_NODE_FAMILY,
                    key_values=list(row.get("linked_traceability_node_keys") or []),
                    display_values=list(
                        row.get("linked_traceability_nodes") or row.get("linked_traceability_stub_nodes") or []
                    ),
                ),
                blockers=blockers_text,
                next=next_artifacts_text,
                reviewer_next_step=reviewer_next_step_text,
                default=(
                    f"{route_phase}：方法 {_display_text_list(list(row.get('linked_method_confirmation_items') or []))}"
                    f"；不确定度 {_display_text_list(list(row.get('linked_uncertainty_inputs') or []))}"
                    f"；溯源 {_display_text_list(list(row.get('linked_traceability_stub_nodes') or []))}"
                    f"；阻塞 {blockers_text}"
                    f"；下一步 {_display_text_list(list(row.get('next_required_artifacts') or []))}"
                    f"；审阅下一步 {reviewer_next_step_text}"
                ),
            )
        )
        if str(row.get("gap_classification") or "").strip() or str(row.get("gap_severity") or "").strip():
            detail_lines.append(
                t(
                    "results.review_center.detail.measurement.phase_gap_line",
                    phase=route_phase,
                    classification=_display_gap_classification(row),
                    severity=_display_gap_severity(row),
                    default=(
                        f"{route_phase}：差距分类 {_display_gap_classification(row)}"
                        f"；差距等级 {_display_gap_severity(row)}"
                    ),
                )
            )
        comparison_digest = str(row.get("comparison_digest") or "").strip()
        if comparison_digest:
            detail_lines.append(
                t(
                    "results.review_center.detail.measurement.phase_comparison_line",
                    phase=route_phase,
                    value=humanize_review_surface_text(comparison_digest),
                    default=f"{route_phase} 对照：{comparison_digest}",
                )
            )
    for row in gap_rows:
        route_phase = str(row.get("route_phase") or "").strip()
        if not route_phase:
            continue
        detail_lines.append(
            t(
                "results.review_center.detail.measurement.phase_gap_line",
                phase=route_phase,
                classification=display_taxonomy_value(
                    GAP_CLASSIFICATION_FAMILY,
                    row.get("gap_classification"),
                    default=str(row.get("gap_classification_label") or row.get("gap_classification") or t("common.none")),
                ),
                severity=display_taxonomy_value(
                    GAP_SEVERITY_FAMILY,
                    row.get("gap_severity"),
                    default=str(row.get("gap_severity_label") or row.get("gap_severity") or t("common.none")),
                ),
                default=(
                    f"{route_phase}：差距分类 "
                    f"{display_taxonomy_value(GAP_CLASSIFICATION_FAMILY, row.get('gap_classification'), default=str(row.get('gap_classification_label') or row.get('gap_classification') or t('common.none')))}"
                    f"；差距等级 "
                    f"{display_taxonomy_value(GAP_SEVERITY_FAMILY, row.get('gap_severity'), default=str(row.get('gap_severity_label') or row.get('gap_severity') or t('common.none')))}"
                ),
            )
        )
    return {
        "summary_lines": _dedupe_lines(summary_lines),
        "detail_lines": _dedupe_lines(detail_lines),
    }


def build_measurement_review_digest_lines(payload: dict[str, Any]) -> dict[str, list[str]]:
    raw = dict(payload.get("raw") or payload or {})
    digest = dict(raw.get("digest") or payload.get("digest") or {})
    phase_rows = [
        _normalize_measurement_phase_row(dict(item))
        for item in list(raw.get("phase_rows") or payload.get("phase_rows") or [])
        if isinstance(item, dict)
    ]
    gap_rows = [
        dict(item)
        for item in list(raw.get("linked_measurement_gaps") or payload.get("linked_measurement_gaps") or [])
        if isinstance(item, dict)
    ]
    linked_method_summary = _phase_field_summary(
        phase_rows,
        family=METHOD_CONFIRMATION_FAMILY,
        key_field_name="linked_method_confirmation_item_keys",
        display_field_name="linked_method_confirmation_items",
    )
    linked_uncertainty_summary = _phase_field_summary(
        phase_rows,
        family=UNCERTAINTY_INPUT_FAMILY,
        key_field_name="linked_uncertainty_input_keys",
        display_field_name="linked_uncertainty_inputs",
    )
    linked_traceability_summary = _phase_field_summary(
        phase_rows,
        family=TRACEABILITY_NODE_FAMILY,
        key_field_name="linked_traceability_node_keys",
        display_field_name="linked_traceability_stub_nodes",
    )
    gap_index_summary = _gap_index_summary(phase_rows)
    reviewer_next_step_summary = _reviewer_next_step_summary(phase_rows)
    readiness_impact_summary = " | ".join(
        _dedupe(
            f"{_display_route_phase(row)}: {_display_readiness_impact(row)}"
            for row in phase_rows
            if str(row.get("coverage_bucket") or "").strip() != "actual_simulated_run_with_payload_complete"
        )
    ) or humanize_review_surface_text(str(digest.get("readiness_impact_summary") or t("common.none")))
    blocker_summary = " | ".join(
        _dedupe(
            f"{_display_route_phase(row)}: {_display_blockers(row)}"
            for row in phase_rows
            if str(row.get("coverage_bucket") or "").strip() != "actual_simulated_run_with_payload_complete"
            and (list(row.get("blocker_fragments") or []) or list(row.get("blockers") or []))
        )
    ) or humanize_review_surface_text(str(digest.get("blocker_summary") or t("common.none")))
    phase_contrast_fallback = _localized_phase_contrast_summary(
        phase_rows,
        str(digest.get("phase_contrast_summary") or ""),
    )
    phase_contrast_summary = _display_phase_contrast_summary(raw, phase_contrast_fallback)
    boundary_summary = _display_boundary_summary(raw)
    non_claim_summary = _display_non_claim_summary(raw)

    summary_lines = [
        humanize_review_surface_text(str(digest.get("summary") or "")),
        t(
            "results.review_center.detail.measurement.payload_complete_phases_line",
            value=str(digest.get("payload_complete_phase_summary") or t("common.none")),
            default=f"payload complete phases: {str(digest.get('payload_complete_phase_summary') or t('common.none'))}",
        ),
        t(
            "results.review_center.detail.measurement.payload_partial_phases_line",
            value=str(digest.get("payload_partial_phase_summary") or t("common.none")),
            default=f"payload partial phases: {str(digest.get('payload_partial_phase_summary') or t('common.none'))}",
        ),
        t(
            "results.review_center.detail.measurement.trace_only_phases_line",
            value=str(digest.get("trace_only_phase_summary") or t("common.none")),
            default=f"trace-only phases: {str(digest.get('trace_only_phase_summary') or t('common.none'))}",
        ),
        t(
            "results.review_center.detail.measurement.next_artifacts_line",
            value=str(digest.get("next_required_artifacts_summary") or t("common.none")),
            default=f"next required artifacts: {str(digest.get('next_required_artifacts_summary') or t('common.none'))}",
        ),
        t(
            "results.review_center.detail.measurement.blockers_line",
            value=blocker_summary,
            default=f"current blockers: {blocker_summary}",
        ),
        t(
            "results.review_center.detail.measurement.preseal_partial_guidance_line",
            value=str(digest.get("preseal_partial_guidance_summary") or t("common.none")),
            default=f"preseal partial guidance: {str(digest.get('preseal_partial_guidance_summary') or t('common.none'))}",
        ),
        t(
            "results.review_center.detail.measurement.linked_method_items_line",
            value=linked_method_summary,
            default=f"linked method confirmation items: {linked_method_summary}",
        ),
        t(
            "results.review_center.detail.measurement.linked_uncertainty_inputs_line",
            value=linked_uncertainty_summary,
            default=f"linked uncertainty inputs: {linked_uncertainty_summary}",
        ),
        t(
            "results.review_center.detail.measurement.linked_traceability_nodes_line",
            value=linked_traceability_summary,
            default=f"linked traceability nodes: {linked_traceability_summary}",
        ),
        t(
            "results.review_center.detail.measurement.reviewer_next_steps_line",
            value=reviewer_next_step_summary,
            default=f"reviewer next steps: {reviewer_next_step_summary}",
        ),
        t(
            "results.review_center.detail.measurement.phase_contrast_line",
            value=phase_contrast_summary,
            default=f"preseal / pressure_stable contrast: {phase_contrast_summary}",
        ),
        t(
            "results.review_center.detail.measurement.boundary_line",
            value=boundary_summary,
            default=f"{t('results.review_center.detail.boundary', default='边界')}：{boundary_summary}",
        ),
        t(
            "results.review_center.detail.measurement.non_claim_line",
            value=non_claim_summary,
            default=f"{t('results.review_center.detail.readiness.non_claim', default='非声明边界')}：{non_claim_summary}",
        ),
    ]

    detail_lines = [
        t(
            "results.review_center.detail.measurement.readiness_impact_line",
            value=readiness_impact_summary,
            default=f"readiness impact: {readiness_impact_summary}",
        ),
        t(
            "results.review_center.detail.measurement.linked_readiness_line",
            value=str(digest.get("linked_readiness_summary") or t("common.none")),
            default=f"linked readiness artifacts: {str(digest.get('linked_readiness_summary') or t('common.none'))}",
        ),
        t(
            "results.review_center.detail.measurement.gap_index_line",
            value=gap_index_summary,
            default=f"gap index: {gap_index_summary}",
        ),
        t(
            "results.review_center.detail.measurement.boundary_line",
            value=boundary_summary,
            default=f"{t('results.review_center.detail.boundary', default='边界')}：{boundary_summary}",
        ),
        t(
            "results.review_center.detail.measurement.non_claim_line",
            value=non_claim_summary,
            default=f"{t('results.review_center.detail.readiness.non_claim', default='非声明边界')}：{non_claim_summary}",
        ),
    ]

    for row in phase_rows:
        route_phase = _display_route_phase(row)
        next_artifacts_text = _display_text_list(list(row.get("next_required_artifacts") or []))
        blockers_text = _display_blockers(row)
        gap_reason_text = _display_gap_reason(row)
        readiness_impact_text = _display_readiness_impact(row)
        reviewer_next_step_text = _display_reviewer_next_step(row)
        row_boundary_text = _display_boundary_summary(row)
        row_non_claim_text = _display_non_claim_summary(row)
        detail_lines.append(
            t(
                "results.review_center.detail.measurement.phase_guidance_line",
                phase=route_phase,
                bucket=_display_measurement_bucket(row.get("coverage_bucket_display") or row.get("coverage_bucket")),
                available=_display_measurement_layer_list(list(row.get("available_signal_layers") or [])),
                missing=_display_measurement_layer_list(list(row.get("missing_signal_layers") or [])),
                reason=gap_reason_text,
                impact=readiness_impact_text,
                next="、".join(
                    str(item).strip() for item in list(row.get("next_required_artifacts") or []) if str(item).strip()
                )
                or t("common.none"),
                boundary=row_boundary_text,
                default=(
                    f"{route_phase}: bucket {_display_measurement_bucket(row.get('coverage_bucket_display') or row.get('coverage_bucket'))}; "
                    f"available {_display_measurement_layer_list(list(row.get('available_signal_layers') or []))}; "
                    f"missing {_display_measurement_layer_list(list(row.get('missing_signal_layers') or []))}; "
                    f"reason {gap_reason_text}; impact {readiness_impact_text}; "
                    f"next {'、'.join(str(item).strip() for item in list(row.get('next_required_artifacts') or []) if str(item).strip()) or t('common.none')}; "
                    f"boundary {row_boundary_text}"
                ),
            )
        )
        detail_lines.append(
            t(
                "results.review_center.detail.measurement.phase_navigation_line",
                phase=route_phase,
                method=_display_taxonomy_list(
                    METHOD_CONFIRMATION_FAMILY,
                    key_values=list(row.get("linked_method_confirmation_item_keys") or []),
                    display_values=list(row.get("linked_method_confirmation_items") or []),
                ),
                uncertainty=_display_taxonomy_list(
                    UNCERTAINTY_INPUT_FAMILY,
                    key_values=list(row.get("linked_uncertainty_input_keys") or []),
                    display_values=list(row.get("linked_uncertainty_inputs") or []),
                ),
                traceability=_display_taxonomy_list(
                    TRACEABILITY_NODE_FAMILY,
                    key_values=list(row.get("linked_traceability_node_keys") or []),
                    display_values=list(row.get("linked_traceability_nodes") or row.get("linked_traceability_stub_nodes") or []),
                ),
                blockers=blockers_text,
                next=next_artifacts_text,
                reviewer_next_step=reviewer_next_step_text,
                default=(
                    f"{route_phase}: method {_display_text_list(list(row.get('linked_method_confirmation_items') or []))}; "
                    f"uncertainty {_display_text_list(list(row.get('linked_uncertainty_inputs') or []))}; "
                    f"traceability {_display_text_list(list(row.get('linked_traceability_stub_nodes') or []))}; "
                    f"blockers {blockers_text}; next {next_artifacts_text}; reviewer next step {reviewer_next_step_text}"
                ),
            )
        )
        if str(row.get("gap_classification") or "").strip() or str(row.get("gap_severity") or "").strip():
            detail_lines.append(
                t(
                    "results.review_center.detail.measurement.phase_gap_line",
                    phase=route_phase,
                    classification=_display_gap_classification(row),
                    severity=_display_gap_severity(row),
                    default=(
                        f"{route_phase}: gap classification {_display_gap_classification(row)}; "
                        f"gap severity {_display_gap_severity(row)}"
                    ),
                )
            )
        comparison_digest = str(row.get("comparison_digest") or "").strip()
        if comparison_digest:
            comparison_summary = _display_phase_contrast_summary(row, comparison_digest)
            detail_lines.append(
                t(
                    "results.review_center.detail.measurement.phase_comparison_line",
                    phase=route_phase,
                    value=comparison_summary,
                    default=f"{route_phase} contrast: {comparison_summary}",
                )
            )
        if row_non_claim_text and row_non_claim_text != t("common.none"):
            detail_lines.append(
                t(
                    "results.review_center.detail.measurement.phase_non_claim_line",
                    phase=route_phase,
                    value=row_non_claim_text,
                    default=(
                        f"{route_phase} {t('results.review_center.detail.readiness.non_claim', default='非声明边界')}："
                        f"{row_non_claim_text}"
                    ),
                )
            )

    for row in gap_rows:
        route_phase = str(row.get("route_phase") or "").strip()
        if not route_phase:
            continue
        detail_lines.append(
            t(
                "results.review_center.detail.measurement.phase_gap_line",
                phase=route_phase,
                classification=display_taxonomy_value(
                    GAP_CLASSIFICATION_FAMILY,
                    row.get("gap_classification"),
                    default=str(row.get("gap_classification_label") or row.get("gap_classification") or t("common.none")),
                ),
                severity=display_taxonomy_value(
                    GAP_SEVERITY_FAMILY,
                    row.get("gap_severity"),
                    default=str(row.get("gap_severity_label") or row.get("gap_severity") or t("common.none")),
                ),
                default=(
                    f"{route_phase}: gap classification "
                    f"{display_taxonomy_value(GAP_CLASSIFICATION_FAMILY, row.get('gap_classification'), default=str(row.get('gap_classification_label') or row.get('gap_classification') or t('common.none')))}; "
                    f"gap severity "
                    f"{display_taxonomy_value(GAP_SEVERITY_FAMILY, row.get('gap_severity'), default=str(row.get('gap_severity_label') or row.get('gap_severity') or t('common.none')))}"
                ),
            )
        )
    return {
        "summary_lines": _dedupe_lines(summary_lines),
        "detail_lines": _dedupe_lines(detail_lines),
    }


def build_readiness_review_digest_lines(payload: dict[str, Any]) -> dict[str, list[str]]:
    raw = dict(payload.get("raw") or payload or {})
    digest = dict(raw.get("digest") or payload.get("digest") or {})
    title = str(
        dict(raw.get("review_surface") or payload.get("review_surface") or {}).get("title_text")
        or raw.get("artifact_type")
        or "--"
    )
    phase_rows = [
        _normalize_measurement_phase_row(dict(item))
        for item in list(raw.get("linked_measurement_phase_artifacts") or [])
        if isinstance(item, dict)
    ]
    gap_rows = [dict(item) for item in list(raw.get("linked_measurement_gaps") or []) if isinstance(item, dict)]
    linked_method_summary = _phase_field_summary(
        phase_rows,
        family=METHOD_CONFIRMATION_FAMILY,
        key_field_name="linked_method_confirmation_item_keys",
        display_field_name="linked_method_confirmation_items",
    )
    linked_uncertainty_summary = _phase_field_summary(
        phase_rows,
        family=UNCERTAINTY_INPUT_FAMILY,
        key_field_name="linked_uncertainty_input_keys",
        display_field_name="linked_uncertainty_inputs",
    )
    linked_traceability_summary = _phase_field_summary(
        phase_rows,
        family=TRACEABILITY_NODE_FAMILY,
        key_field_name="linked_traceability_node_keys",
        display_field_name="linked_traceability_nodes",
    )
    linked_gap_classification_summary = " | ".join(
        _dedupe(
            f"{str(row.get('route_phase') or '').strip()}: {display_taxonomy_value(GAP_CLASSIFICATION_FAMILY, row.get('gap_classification'), default=str(row.get('gap_classification_label') or row.get('gap_classification') or t('common.none')))}"
            for row in gap_rows
            if str(row.get("route_phase") or "").strip()
        )
    ) or str(digest.get("linked_gap_classification_summary") or t("common.none"))
    linked_gap_severity_summary = " | ".join(
        _dedupe(
            f"{str(row.get('route_phase') or '').strip()}: {display_taxonomy_value(GAP_SEVERITY_FAMILY, row.get('gap_severity'), default=str(row.get('gap_severity_label') or row.get('gap_severity') or t('common.none')))}"
            for row in gap_rows
            if str(row.get("route_phase") or "").strip()
        )
    ) or str(digest.get("linked_gap_severity_summary") or t("common.none"))
    linked_gap_reason_summary = " | ".join(
        _dedupe(
            f"{str(row.get('route_phase') or '').strip()}: {_display_gap_reason(row)}"
            for row in gap_rows
            if str(row.get("route_phase") or "").strip()
        )
    ) or humanize_review_surface_text(str(digest.get("gap_reason") or t("common.none")))
    linked_readiness_impact_summary = " | ".join(
        _dedupe(
            f"{str(row.get('route_phase') or '').strip()}: {_display_readiness_impact(row)}"
            for row in gap_rows
            if str(row.get("route_phase") or "").strip()
        )
    ) or humanize_review_surface_text(
        str(raw.get("linked_readiness_impact_summary") or digest.get("linked_readiness_impact_summary") or t("common.none"))
    )
    blocker_summary = " | ".join(
        _dedupe(
            f"{str(row.get('route_phase') or '').strip()}: {_display_blockers(row)}"
            for row in gap_rows
            if str(row.get("route_phase") or "").strip()
            and (list(row.get("blocker_fragments") or []) or list(row.get("blockers") or []))
        )
    ) or humanize_review_surface_text(str(digest.get("blocker_summary") or t("common.none")))
    reviewer_next_step_summary = " | ".join(
        _dedupe(
            _display_reviewer_next_step(row)
            for row in gap_rows
            if list(row.get("reviewer_next_step_fragments") or [])
            or str(row.get("reviewer_next_step_digest") or "").strip()
        )
    ) or humanize_review_surface_text(str(digest.get("reviewer_next_step_digest") or t("common.none")))
    boundary_summary = _display_boundary_summary(raw)
    non_claim_summary = _display_non_claim_summary(raw)
    scope_overview_summary = humanize_review_surface_text(
        str(
            digest.get("scope_overview_summary")
            or dict(raw.get("scope_overview") or {}).get("summary")
            or t("common.none")
        )
    )
    decision_rule_summary = humanize_review_surface_text(
        str(
            digest.get("decision_rule_summary")
            or dict(raw.get("decision_rule_overview") or {}).get("summary")
            or raw.get("decision_rule_id")
            or t("common.none")
        )
    )
    conformity_boundary_summary = humanize_review_surface_text(
        str(
            digest.get("conformity_boundary_summary")
            or dict(raw.get("conformity_boundary") or {}).get("summary")
            or raw.get("non_claim_note")
            or digest.get("non_claim_digest")
            or t("common.none")
        )
    )
    standard_family_summary = humanize_review_surface_text(
        " | ".join(str(item).strip() for item in list(raw.get("standard_family") or []) if str(item).strip())
        or str(digest.get("standard_family_summary") or t("common.none"))
    )
    required_evidence_categories_summary = humanize_review_surface_text(
        " | ".join(
            str(item).strip() for item in list(raw.get("required_evidence_categories") or []) if str(item).strip()
        )
        or str(digest.get("required_evidence_categories_summary") or t("common.none"))
    )
    asset_readiness_overview = humanize_review_surface_text(
        str(raw.get("asset_readiness_overview") or digest.get("asset_readiness_overview") or t("common.none"))
    )
    certificate_lifecycle_overview = humanize_review_surface_text(
        str(
            raw.get("certificate_lifecycle_overview")
            or digest.get("certificate_lifecycle_overview")
            or t("common.none")
        )
    )
    pre_run_gate_status = humanize_review_surface_text(
        str(raw.get("gate_status") or digest.get("pre_run_gate_status") or t("common.none"))
    )
    warning_summary = humanize_review_surface_text(str(digest.get("warning_summary") or t("common.none")))
    reviewer_action_summary = humanize_review_surface_text(
        str(digest.get("reviewer_action_summary") or t("common.none"))
    )
    scope_reference_assets_summary = humanize_review_surface_text(
        str(digest.get("scope_reference_assets_summary") or t("common.none"))
    )
    decision_rule_dependency_summary = humanize_review_surface_text(
        str(digest.get("decision_rule_dependency_summary") or t("common.none"))
    )

    summary_lines = [
        f"{title}: {humanize_review_surface_text(str(digest.get('summary') or ''))}".strip(": "),
        t(
            "results.review_center.detail.readiness.scope_overview_line",
            value=scope_overview_summary,
            default=f"认可范围概览：{scope_overview_summary}",
        ),
        t(
            "results.review_center.detail.readiness.decision_rule_line",
            value=decision_rule_summary,
            default=f"决策规则概览：{decision_rule_summary}",
        ),
        t(
            "results.review_center.detail.readiness.conformity_boundary_line",
            value=conformity_boundary_summary,
            default=f"符合性边界：{conformity_boundary_summary}",
        ),
        t(
            "results.review_center.detail.readiness.asset_readiness_overview_line",
            value=asset_readiness_overview,
            default=f"asset readiness overview: {asset_readiness_overview}",
        ),
        t(
            "results.review_center.detail.readiness.certificate_lifecycle_overview_line",
            value=certificate_lifecycle_overview,
            default=f"certificate lifecycle overview: {certificate_lifecycle_overview}",
        ),
        t(
            "results.review_center.detail.readiness.pre_run_gate_status_line",
            value=pre_run_gate_status,
            default=f"pre-run gate status: {pre_run_gate_status}",
        ),
        t(
            "results.review_center.detail.readiness.linked_measurement_line",
            value=str(digest.get("linked_measurement_phase_summary") or t("common.none")),
            default=f"linked measurement phases: {str(digest.get('linked_measurement_phase_summary') or t('common.none'))}",
        ),
        t(
            "results.review_center.detail.readiness.linked_measurement_gap_line",
            value=str(digest.get("linked_measurement_gap_summary") or t("common.none")),
            default=f"linked measurement gaps: {str(digest.get('linked_measurement_gap_summary') or t('common.none'))}",
        ),
        t(
            "results.review_center.detail.readiness.readiness_impact_line",
            value=linked_readiness_impact_summary,
            default=f"readiness impact: {linked_readiness_impact_summary}",
        ),
        t(
            "results.review_center.detail.readiness.linked_method_items_line",
            value=linked_method_summary,
            default=f"linked method confirmation items: {linked_method_summary}",
        ),
        t(
            "results.review_center.detail.readiness.linked_uncertainty_inputs_line",
            value=linked_uncertainty_summary,
            default=f"linked uncertainty inputs: {linked_uncertainty_summary}",
        ),
        t(
            "results.review_center.detail.readiness.linked_traceability_nodes_line",
            value=linked_traceability_summary,
            default=f"linked traceability nodes: {linked_traceability_summary}",
        ),
        t(
            "results.review_center.detail.readiness.warning_items_line",
            value=warning_summary,
            default=f"warning items: {warning_summary}",
        ),
        t(
            "results.review_center.detail.readiness.reviewer_next_step_line",
            value=reviewer_next_step_summary,
            default=f"reviewer next step: {reviewer_next_step_summary}",
        ),
        t(
            "results.review_center.detail.readiness.boundary_line",
            value=boundary_summary,
            default=f"{t('results.review_center.detail.boundary', default='边界')}：{boundary_summary}",
        ),
        t(
            "results.review_center.detail.readiness.non_claim_line",
            value=non_claim_summary,
            default=f"{t('results.review_center.detail.readiness.non_claim', default='非声明边界')}：{non_claim_summary}",
        ),
    ]

    detail_lines = [
        t(
            "results.review_center.detail.readiness.current_coverage_line",
            value=str(digest.get("current_coverage_summary") or t("common.none")),
            default=f"current coverage: {str(digest.get('current_coverage_summary') or t('common.none'))}",
        ),
        t(
            "results.review_center.detail.readiness.standard_family_line",
            value=standard_family_summary,
            default=f"标准族：{standard_family_summary}",
        ),
        t(
            "results.review_center.detail.readiness.required_evidence_categories_line",
            value=required_evidence_categories_summary,
            default=f"要求证据类别：{required_evidence_categories_summary}",
        ),
        t(
            "results.review_center.detail.readiness.missing_evidence_line",
            value=str(digest.get("missing_evidence_summary") or t("common.none")),
            default=f"missing evidence: {str(digest.get('missing_evidence_summary') or t('common.none'))}",
        ),
        t(
            "results.review_center.detail.readiness.blockers_line",
            value=blocker_summary,
            default=f"current blockers: {blocker_summary}",
        ),
        t(
            "results.review_center.detail.readiness.linked_measurement_line",
            value=str(digest.get("linked_measurement_phase_summary") or t("common.none")),
            default=f"linked measurement phases: {str(digest.get('linked_measurement_phase_summary') or t('common.none'))}",
        ),
        t(
            "results.review_center.detail.readiness.linked_measurement_gap_line",
            value=str(digest.get("linked_measurement_gap_summary") or t("common.none")),
            default=f"linked measurement gaps: {str(digest.get('linked_measurement_gap_summary') or t('common.none'))}",
        ),
        t(
            "results.review_center.detail.readiness.readiness_impact_line",
            value=linked_readiness_impact_summary,
            default=f"readiness impact: {linked_readiness_impact_summary}",
        ),
        t(
            "results.review_center.detail.readiness.preseal_partial_gap_line",
            value=str(digest.get("preseal_partial_gap_summary") or t("common.none")),
            default=f"preseal partial gap: {str(digest.get('preseal_partial_gap_summary') or t('common.none'))}",
        ),
        t(
            "results.review_center.detail.readiness.linked_method_items_line",
            value=linked_method_summary,
            default=f"linked method confirmation items: {linked_method_summary}",
        ),
        t(
            "results.review_center.detail.readiness.linked_uncertainty_inputs_line",
            value=linked_uncertainty_summary,
            default=f"linked uncertainty inputs: {linked_uncertainty_summary}",
        ),
        t(
            "results.review_center.detail.readiness.linked_traceability_nodes_line",
            value=linked_traceability_summary,
            default=f"linked traceability nodes: {linked_traceability_summary}",
        ),
        t(
            "results.review_center.detail.readiness.linked_gap_classification_line",
            value=linked_gap_classification_summary,
            default=f"linked gap classification: {linked_gap_classification_summary}",
        ),
        t(
            "results.review_center.detail.readiness.linked_gap_severity_line",
            value=linked_gap_severity_summary,
            default=f"linked gap severity: {linked_gap_severity_summary}",
        ),
        t(
            "results.review_center.detail.readiness.gap_reason_line",
            value=linked_gap_reason_summary,
            default=f"gap reason: {linked_gap_reason_summary}",
        ),
        t(
            "results.review_center.detail.readiness.next_artifacts_line",
            value=str(digest.get("next_required_artifacts_summary") or t("common.none")),
            default=f"next required artifacts: {str(digest.get('next_required_artifacts_summary') or t('common.none'))}",
        ),
        t(
            "results.review_center.detail.readiness.reviewer_next_step_line",
            value=reviewer_next_step_summary,
            default=f"reviewer next step: {reviewer_next_step_summary}",
        ),
        t(
            "results.review_center.detail.readiness.boundary_line",
            value=boundary_summary,
            default=f"{t('results.review_center.detail.boundary', default='边界')}：{boundary_summary}",
        ),
        t(
            "results.review_center.detail.readiness.non_claim_line",
            value=non_claim_summary,
            default=f"{t('results.review_center.detail.readiness.non_claim', default='非声明边界')}：{non_claim_summary}",
        ),
    ]
    return {
        "summary_lines": _dedupe_lines(summary_lines),
        "detail_lines": _dedupe_lines(detail_lines),
    }


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
        "summary_text": humanize_review_surface_text(str(summary_payload.get("summary_text") or "").strip()),
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


def hydrate_review_scope_reviewer_display(
    payload: dict[str, Any] | None,
    *,
    selection: dict[str, Any] | None = None,
    scope_summary: dict[str, Any] | None = None,
) -> dict[str, str]:
    payload_dict = dict(payload or {})
    selection_payload = dict(selection or payload_dict.get("selection", {}) or {})
    summary_payload = dict(scope_summary or payload_dict.get("scope_summary", {}) or {})
    hydrated = build_review_scope_payload_reviewer_display(
        selection=selection_payload,
        scope_summary=summary_payload,
    )
    for field in _REVIEW_SCOPE_REVIEWER_DISPLAY_FIELDS:
        fallback_value = str(payload_dict.get(field) or "").strip()
        if fallback_value:
            hydrated[field] = fallback_value
    for field in _REVIEW_SCOPE_REVIEWER_DISPLAY_FIELDS:
        nested_value = str(dict(payload_dict.get("reviewer_display", {}) or {}).get(field) or "").strip()
        if nested_value:
            hydrated[field] = nested_value
    return hydrated


def build_artifact_scope_view_reviewer_display(
    *,
    summary_text: Any,
    scope_label: Any,
    visible_count: Any,
    present_count: Any,
    scope_total_count: Any,
    external_count: Any,
    missing_count: Any,
    catalog_present_count: Any,
    catalog_total_count: Any,
    catalog_note_text: Any = "",
    empty_text: Any = "",
    export_warning_text: Any = "",
) -> dict[str, str]:
    return {
        "summary_text": humanize_review_surface_text(str(summary_text or "").strip()),
        **build_artifact_scope_reviewer_notes(
            scope_label=scope_label,
            visible_count=visible_count,
            present_count=present_count,
            scope_total_count=scope_total_count,
            external_count=external_count,
            missing_count=missing_count,
            catalog_present_count=catalog_present_count,
            catalog_total_count=catalog_total_count,
        ),
        "catalog_note_text": humanize_review_surface_text(str(catalog_note_text or "").strip()),
        "empty_text": humanize_review_surface_text(str(empty_text or "").strip()),
        "export_warning_text": humanize_review_surface_text(str(export_warning_text or "").strip()),
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
