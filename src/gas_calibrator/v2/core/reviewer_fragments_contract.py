from __future__ import annotations

from typing import Any, Iterable
import re

from .phase_taxonomy_contract import (
    REVIEWER_NEXT_STEP_TEMPLATE_FAMILY,
    normalize_taxonomy_key,
    taxonomy_display_label,
    taxonomy_i18n_key,
)


REVIEWER_FRAGMENTS_CONTRACT_VERSION = "step2-reviewer-fragments-v1"

GAP_REASON_FRAGMENT_FAMILY = "gap_reason"
READINESS_IMPACT_FRAGMENT_FAMILY = "readiness_impact"
BLOCKER_FRAGMENT_FAMILY = "blocker"
REVIEWER_NEXT_STEP_FRAGMENT_FAMILY = "reviewer_next_step"

_TOKEN_RE = re.compile(r"[^0-9a-z\u4e00-\u9fff]+")


def _entry(
    family: str,
    canonical_key: str,
    *,
    i18n_key: str,
    zh_label: str,
    en_label: str,
    aliases: tuple[str, ...] = (),
    parameter_names: tuple[str, ...] = (),
) -> dict[str, Any]:
    return {
        "family": family,
        "canonical_key": canonical_key,
        "fragment_key": canonical_key,
        "i18n_key": i18n_key,
        "zh_label": zh_label,
        "en_label": en_label,
        "aliases": tuple(str(item).strip() for item in aliases if str(item).strip()),
        "parameter_names": tuple(str(item).strip() for item in parameter_names if str(item).strip()),
    }


_FRAGMENT_REGISTRY: dict[str, dict[str, dict[str, Any]]] = {
    GAP_REASON_FRAGMENT_FAMILY: {},
    READINESS_IMPACT_FRAGMENT_FAMILY: {},
    BLOCKER_FRAGMENT_FAMILY: {},
}

_ALIAS_INDEX: dict[str, dict[str, str]] = {
    GAP_REASON_FRAGMENT_FAMILY: {},
    READINESS_IMPACT_FRAGMENT_FAMILY: {},
    BLOCKER_FRAGMENT_FAMILY: {},
    REVIEWER_NEXT_STEP_FRAGMENT_FAMILY: {},
}


_FRAGMENT_REGISTRY[GAP_REASON_FRAGMENT_FAMILY].update(
    {
        "conditioning_window_output_layer_open": _entry(
            GAP_REASON_FRAGMENT_FAMILY,
            "conditioning_window_output_layer_open",
            i18n_key="reviewer_fragments.gap_reason.conditioning_window_output_layer_open",
            zh_label="\u8c03\u7406\u7a97\u53e3\u4ecd\u5904\u4e8e setup / conditioning \u8bc1\u636e\u8fb9\u754c\uff0c\u8f93\u51fa\u5c42\u4fdd\u6301\u5f00\u653e\uff1a{details}",
            en_label="Conditioning window remains setup evidence so the output layer stays open: {details}",
            aliases=("conditioning window remains setup evidence until same-route pressure_stable closes full payload-backed output capture",),
            parameter_names=("details",),
        ),
        "missing_layer_reason_explicit": _entry(
            GAP_REASON_FRAGMENT_FAMILY,
            "missing_layer_reason_explicit",
            i18n_key="reviewer_fragments.gap_reason.missing_layer_reason_explicit",
            zh_label="\u7f3a\u5931\u5c42\u539f\u56e0\uff1a{details}",
            en_label="Missing-layer reason: {details}",
            parameter_names=("details",),
        ),
        "partial_payload_boundary_open": _entry(
            GAP_REASON_FRAGMENT_FAMILY,
            "partial_payload_boundary_open",
            i18n_key="reviewer_fragments.gap_reason.partial_payload_boundary_open",
            zh_label="payload \u4ecd\u4e3a\u90e8\u5206\u8986\u76d6\uff0c\u5f00\u53e3\u5c42\u4fdd\u6301\u663e\u5f0f\uff1a{missing_layers}",
            en_label="Payload remains partial and the open layers stay explicit: {missing_layers}",
            aliases=("payload is partial and missing layers stay explicit",),
            parameter_names=("missing_layers",),
        ),
        "trace_only_not_payload_evaluated": _entry(
            GAP_REASON_FRAGMENT_FAMILY,
            "trace_only_not_payload_evaluated",
            i18n_key="reviewer_fragments.gap_reason.trace_only_not_payload_evaluated",
            zh_label="\u8be5\u9636\u6bb5\u4ecd\u4e3a\u4ec5 trace\uff0cpayload-backed \u8bc1\u636e\u5c1a\u672a\u63d0\u5347",
            en_label="This phase is still trace-only and payload-backed evidence has not been promoted yet",
            aliases=("this phase is still trace-only and not payload-evaluated",),
        ),
        "reviewer_coverage_only_gap": _entry(
            GAP_REASON_FRAGMENT_FAMILY,
            "reviewer_coverage_only_gap",
            i18n_key="reviewer_fragments.gap_reason.reviewer_coverage_only_gap",
            zh_label="\u8be5\u9636\u6bb5\u5f53\u524d\u4ec5\u6709 {coverage_bucket_label} \u5ba1\u9605\u8986\u76d6",
            en_label="This phase currently has only {coverage_bucket_label} reviewer coverage",
            aliases=("this phase has only reviewer coverage",),
            parameter_names=("coverage_bucket_label",),
        ),
    }
)

_FRAGMENT_REGISTRY[READINESS_IMPACT_FRAGMENT_FAMILY].update(
    {
        "payload_backed_linkage_available": _entry(
            READINESS_IMPACT_FRAGMENT_FAMILY,
            "payload_backed_linkage_available",
            i18n_key="reviewer_fragments.readiness_impact.payload_backed_linkage_available",
            zh_label="{dimensions} \u94fe\u63a5\u5df2\u53ef\u7531 synthetic payload-backed \u5ba1\u9605\u8bc1\u636e\u652f\u6491",
            en_label="{dimensions} linkage is available from synthetic payload-backed reviewer evidence",
            parameter_names=("dimensions",),
        ),
        "payload_partial_linkage_open": _entry(
            READINESS_IMPACT_FRAGMENT_FAMILY,
            "payload_partial_linkage_open",
            i18n_key="reviewer_fragments.readiness_impact.payload_partial_linkage_open",
            zh_label="{dimensions} \u4ecd\u4e3a\u5f00\u53e3\uff0cpayload \u4ecd\u4e3a\u90e8\u5206\u8986\u76d6\uff0c\u7f3a\u5931\u5c42\u4fdd\u6301\u663e\u5f0f\uff1a{missing_layers}",
            en_label="{dimensions} remains open because payload is partial and missing layers stay explicit: {missing_layers}",
            parameter_names=("dimensions", "missing_layers"),
        ),
        "trace_only_linkage_open": _entry(
            READINESS_IMPACT_FRAGMENT_FAMILY,
            "trace_only_linkage_open",
            i18n_key="reviewer_fragments.readiness_impact.trace_only_linkage_open",
            zh_label="{dimensions} \u4ecd\u4e3a\u5f00\u53e3\uff0c\u56e0\u4e3a\u8be5\u9636\u6bb5\u4ecd\u4e3a\u4ec5 trace\uff0c\u5c1a\u672a\u8fdb\u5165 payload \u8bc4\u4f30",
            en_label="{dimensions} remains open because this phase is still trace-only and not payload-evaluated",
            parameter_names=("dimensions",),
        ),
        "reviewer_coverage_only_linkage_open": _entry(
            READINESS_IMPACT_FRAGMENT_FAMILY,
            "reviewer_coverage_only_linkage_open",
            i18n_key="reviewer_fragments.readiness_impact.reviewer_coverage_only_linkage_open",
            zh_label="{dimensions} \u4ecd\u4e3a\u5f00\u53e3\uff0c\u56e0\u4e3a\u8be5\u9636\u6bb5\u5f53\u524d\u4ec5\u6709 {coverage_bucket_label} \u5ba1\u9605\u8986\u76d6",
            en_label="{dimensions} remains open because this phase has only {coverage_bucket_label} reviewer coverage",
            parameter_names=("dimensions", "coverage_bucket_label"),
        ),
        "payload_evidence_not_complete": _entry(
            READINESS_IMPACT_FRAGMENT_FAMILY,
            "payload_evidence_not_complete",
            i18n_key="reviewer_fragments.readiness_impact.payload_evidence_not_complete",
            zh_label="{dimensions} \u4ecd\u4e3a\u5f00\u53e3\uff0c\u56e0\u4e3a payload \u8bc1\u636e\u5c1a\u672a\u5b8c\u6574",
            en_label="{dimensions} remains open because payload evidence is not complete",
            parameter_names=("dimensions",),
        ),
    }
)

_FRAGMENT_REGISTRY[BLOCKER_FRAGMENT_FAMILY].update(
    {
        "partial_payload_not_phase_complete": _entry(
            BLOCKER_FRAGMENT_FAMILY,
            "partial_payload_not_phase_complete",
            i18n_key="reviewer_fragments.blocker.partial_payload_not_phase_complete",
            zh_label="payload \u4ecd\u4e3a\u90e8\u5206\u8986\u76d6\uff0c\u5ba1\u9605\u8bc1\u636e\u4e0d\u80fd\u88ab\u8fc7\u5ea6\u8868\u8ff0\u4e3a\u9636\u6bb5\u5b8c\u6574\u7684 measurement evidence",
            en_label="Payload stays partial so reviewer evidence cannot be overstated as phase-complete measurement evidence",
            aliases=("payload stays partial so reviewer evidence cannot be overstated as phase-complete measurement evidence",),
        ),
        "trace_only_payload_not_promoted": _entry(
            BLOCKER_FRAGMENT_FAMILY,
            "trace_only_payload_not_promoted",
            i18n_key="reviewer_fragments.blocker.trace_only_payload_not_promoted",
            zh_label="\u8be5\u9636\u6bb5\u4ecd\u4e3a\u4ec5 trace\uff0c\u4eff\u771f payload \u5c42\u5c1a\u672a\u63d0\u5347",
            en_label="Phase is still trace-only; simulated payload layers have not been promoted yet",
            aliases=("phase is still trace-only; simulated payload layers have not been promoted yet",),
        ),
        "coverage_bucket_richer_payload_missing": _entry(
            BLOCKER_FRAGMENT_FAMILY,
            "coverage_bucket_richer_payload_missing",
            i18n_key="reviewer_fragments.blocker.coverage_bucket_richer_payload_missing",
            zh_label="\u8be5\u9636\u6bb5\u4ecd\u4e3a {coverage_bucket_label}\uff0c\u66f4\u5b8c\u6574\u7684\u4eff\u771f payload \u8bc1\u636e\u4ecd\u7f3a\u5931",
            en_label="Phase remains {coverage_bucket_label}; richer simulated payload evidence is still missing",
            parameter_names=("coverage_bucket_label",),
        ),
        "actual_simulated_payload_still_open": _entry(
            BLOCKER_FRAGMENT_FAMILY,
            "actual_simulated_payload_still_open",
            i18n_key="reviewer_fragments.blocker.actual_simulated_payload_still_open",
            zh_label="\u5df2\u6709 actual simulated evidence\uff0c\u4f46 payload \u5b8c\u6574\u5ea6\u4ecd\u4e3a\u5f00\u53e3",
            en_label="Actual simulated evidence exists but payload completeness remains open",
            aliases=("actual simulated evidence exists but payload completeness remains open",),
        ),
        "missing_signal_layers_explicit": _entry(
            BLOCKER_FRAGMENT_FAMILY,
            "missing_signal_layers_explicit",
            i18n_key="reviewer_fragments.blocker.missing_signal_layers_explicit",
            zh_label="\u7f3a\u5931\u4fe1\u53f7\u5c42\uff1a{missing_layers}",
            en_label="Missing signal layers: {missing_layers}",
            parameter_names=("missing_layers",),
        ),
        "linked_method_items_open": _entry(
            BLOCKER_FRAGMENT_FAMILY,
            "linked_method_items_open",
            i18n_key="reviewer_fragments.blocker.linked_method_items_open",
            zh_label="\u5173\u8054\u65b9\u6cd5\u786e\u8ba4\u6761\u76ee\u4ecd\u4e3a\u5f00\u53e3\uff1a{items}",
            en_label="Linked method confirmation items remain open: {items}",
            parameter_names=("items",),
        ),
        "linked_uncertainty_inputs_open": _entry(
            BLOCKER_FRAGMENT_FAMILY,
            "linked_uncertainty_inputs_open",
            i18n_key="reviewer_fragments.blocker.linked_uncertainty_inputs_open",
            zh_label="\u5173\u8054\u4e0d\u786e\u5b9a\u5ea6\u8f93\u5165\u4ecd\u4e3a\u5f00\u53e3\uff1a{items}",
            en_label="Linked uncertainty inputs remain open: {items}",
            parameter_names=("items",),
        ),
        "linked_traceability_nodes_stub_only": _entry(
            BLOCKER_FRAGMENT_FAMILY,
            "linked_traceability_nodes_stub_only",
            i18n_key="reviewer_fragments.blocker.linked_traceability_nodes_stub_only",
            zh_label="\u5173\u8054\u6eaf\u6e90\u8282\u70b9\u4ecd\u4e3a stub-only\uff1a{items}",
            en_label="Linked traceability nodes remain stub-only: {items}",
            parameter_names=("items",),
        ),
        "preseal_honesty_boundary": _entry(
            BLOCKER_FRAGMENT_FAMILY,
            "preseal_honesty_boundary",
            i18n_key="reviewer_fragments.blocker.preseal_honesty_boundary",
            zh_label="preseal partial \u662f\u8bda\u5b9e\u8fb9\u754c\uff0c\u4e0d\u662f measurement-core bug",
            en_label="Preseal partial is an honesty boundary, not a measurement-core bug",
            aliases=("preseal partial is an honesty boundary, not a measurement-core bug",),
        ),
        "preseal_setup_conditioning_only": _entry(
            BLOCKER_FRAGMENT_FAMILY,
            "preseal_setup_conditioning_only",
            i18n_key="reviewer_fragments.blocker.preseal_setup_conditioning_only",
            zh_label="preseal \u4ecd\u5c5e\u4e8e setup / conditioning \u8bc1\u636e\uff0c\u4e0d\u610f\u5473\u5df2\u53d1\u5e03\u7684 measurement output",
            en_label="Preseal remains setup/conditioning evidence and does not imply released measurement output",
            aliases=("preseal remains setup/conditioning evidence and does not imply released measurement output",),
        ),
        "scope_package_reviewer_only": _entry(
            BLOCKER_FRAGMENT_FAMILY,
            "scope_package_reviewer_only",
            i18n_key="reviewer_fragments.blocker.scope_package_reviewer_only",
            zh_label="scope package \u4ecd\u4e3a reviewer-facing only",
            en_label="Scope package remains reviewer-facing only",
            aliases=("scope package remains reviewer-facing only", "scope package is reviewer-facing only"),
        ),
        "formal_scope_approval_open": _entry(
            BLOCKER_FRAGMENT_FAMILY,
            "formal_scope_approval_open",
            i18n_key="reviewer_fragments.blocker.formal_scope_approval_open",
            zh_label="\u6b63\u5f0f scope approval chain \u5c1a\u672a\u95ed\u5408",
            en_label="Formal scope approval chain is not closed",
            aliases=("formal scope approval chain is not closed",),
        ),
        "decision_rule_not_live_gate": _entry(
            BLOCKER_FRAGMENT_FAMILY,
            "decision_rule_not_live_gate",
            i18n_key="reviewer_fragments.blocker.decision_rule_not_live_gate",
            zh_label="decision rule profile \u4e0d\u9a71\u52a8 live gate",
            en_label="Decision rule profile does not drive live gate",
            aliases=("decision rule profile does not drive live gate",),
        ),
        "release_accreditation_out_of_scope": _entry(
            BLOCKER_FRAGMENT_FAMILY,
            "release_accreditation_out_of_scope",
            i18n_key="reviewer_fragments.blocker.release_accreditation_out_of_scope",
            zh_label="release / accreditation \u8bed\u4e49\u4ecd\u660e\u786e\u8d85\u51fa\u672c\u8f6e\u8303\u56f4",
            en_label="Release / accreditation semantics remain explicitly out of scope",
            aliases=("release / accreditation semantics remain explicitly out of scope",),
        ),
    }
)


def _normalize_lookup_value(value: Any) -> str:
    text = str(value or "").strip().lower()
    return _TOKEN_RE.sub("_", text).strip("_")


def _stringify_param(value: Any) -> str:
    if isinstance(value, (list, tuple, set)):
        return ", ".join(str(item).strip() for item in value if str(item).strip()) or "--"
    text = str(value or "").strip()
    return text or "--"


class _SafeFormatDict(dict[str, str]):
    def __missing__(self, key: str) -> str:
        return "{" + key + "}"


def _render_template(template: str, params: dict[str, Any] | None) -> str:
    payload = _SafeFormatDict({key: _stringify_param(value) for key, value in dict(params or {}).items()})
    return str(template or "").format_map(payload).strip()


def _rebuild_alias_index() -> None:
    for family, items in _FRAGMENT_REGISTRY.items():
        alias_map = _ALIAS_INDEX.setdefault(family, {})
        alias_map.clear()
        for canonical_key, entry in items.items():
            for source in (
                canonical_key,
                canonical_key.replace("_", " "),
                str(entry.get("en_label") or ""),
                str(entry.get("zh_label") or ""),
                *tuple(entry.get("aliases") or ()),
            ):
                normalized = _normalize_lookup_value(source)
                if normalized:
                    alias_map[normalized] = canonical_key


_GAP_REASON_PATTERNS: tuple[tuple[re.Pattern[str], str, tuple[str, ...]], ...] = (
    (re.compile(r"^(?P<details>.*conditioning window.*setup evidence.*)$", re.IGNORECASE), "conditioning_window_output_layer_open", ("details",)),
    (re.compile(r"^(?P<details>(?:[a-z_]+: .+)(?: \| [a-z_]+: .+)*)$", re.IGNORECASE), "missing_layer_reason_explicit", ("details",)),
    (re.compile(r"^payload (?:remains|is) partial.*explicit: (?P<missing_layers>.+)$", re.IGNORECASE), "partial_payload_boundary_open", ("missing_layers",)),
    (re.compile(r"^this phase is still trace-only and not payload-evaluated$", re.IGNORECASE), "trace_only_not_payload_evaluated", ()),
    (re.compile(r"^this phase has only (?P<coverage_bucket_label>.+?) reviewer coverage$", re.IGNORECASE), "reviewer_coverage_only_gap", ("coverage_bucket_label",)),
)

_READINESS_IMPACT_PATTERNS: tuple[tuple[re.Pattern[str], str, tuple[str, ...]], ...] = (
    (re.compile(r"^(?P<dimensions>.+?) linkage is available from synthetic payload-backed reviewer evidence$", re.IGNORECASE), "payload_backed_linkage_available", ("dimensions",)),
    (re.compile(r"^(?P<dimensions>.+?) remains open because payload is partial and missing layers stay explicit: (?P<missing_layers>.+)$", re.IGNORECASE), "payload_partial_linkage_open", ("dimensions", "missing_layers")),
    (re.compile(r"^(?P<dimensions>.+?) remains open because this phase is still trace-only and not payload-evaluated$", re.IGNORECASE), "trace_only_linkage_open", ("dimensions",)),
    (re.compile(r"^(?P<dimensions>.+?) remains open because this phase has only (?P<coverage_bucket_label>.+?) reviewer coverage$", re.IGNORECASE), "reviewer_coverage_only_linkage_open", ("dimensions", "coverage_bucket_label")),
    (re.compile(r"^(?P<dimensions>.+?) remains open because payload evidence is not complete$", re.IGNORECASE), "payload_evidence_not_complete", ("dimensions",)),
)

_BLOCKER_PATTERNS: tuple[tuple[re.Pattern[str], str, tuple[str, ...]], ...] = (
    (re.compile(r"^phase remains (?P<coverage_bucket_label>.+?); richer simulated payload evidence is still missing$", re.IGNORECASE), "coverage_bucket_richer_payload_missing", ("coverage_bucket_label",)),
    (re.compile(r"^missing signal layers: (?P<missing_layers>.+)$", re.IGNORECASE), "missing_signal_layers_explicit", ("missing_layers",)),
    (re.compile(r"^linked method confirmation items remain open: (?P<items>.+)$", re.IGNORECASE), "linked_method_items_open", ("items",)),
    (re.compile(r"^linked uncertainty inputs remain open: (?P<items>.+)$", re.IGNORECASE), "linked_uncertainty_inputs_open", ("items",)),
    (re.compile(r"^linked traceability nodes remain stub-only: (?P<items>.+)$", re.IGNORECASE), "linked_traceability_nodes_stub_only", ("items",)),
)


def _infer_fragment_row(family: str, value: Any) -> tuple[str, dict[str, Any]]:
    text = str(value or "").strip()
    if not text:
        return "", {}
    if family == REVIEWER_NEXT_STEP_FRAGMENT_FAMILY:
        return normalize_taxonomy_key(REVIEWER_NEXT_STEP_TEMPLATE_FAMILY, text, default=""), {}
    patterns = {
        GAP_REASON_FRAGMENT_FAMILY: _GAP_REASON_PATTERNS,
        READINESS_IMPACT_FRAGMENT_FAMILY: _READINESS_IMPACT_PATTERNS,
        BLOCKER_FRAGMENT_FAMILY: _BLOCKER_PATTERNS,
    }.get(family, ())
    for pattern, canonical_key, field_names in patterns:
        match = pattern.match(text)
        if not match:
            continue
        params = {
            field_name: _stringify_param(match.group(field_name))
            for field_name in field_names
            if match.groupdict().get(field_name) is not None
        }
        if family == GAP_REASON_FRAGMENT_FAMILY and canonical_key == "conditioning_window_output_layer_open" and not params.get("details"):
            params["details"] = text
        return canonical_key, params
    return _ALIAS_INDEX.get(family, {}).get(_normalize_lookup_value(text), ""), {}


def normalize_fragment_key(family: str, value: Any, *, default: str | None = None) -> str:
    family_name = str(family or "").strip()
    if not family_name:
        return str(default or "")
    if family_name == REVIEWER_NEXT_STEP_FRAGMENT_FAMILY:
        normalized = normalize_taxonomy_key(REVIEWER_NEXT_STEP_TEMPLATE_FAMILY, value, default="")
        return str(normalized or default or "")
    normalized = _ALIAS_INDEX.get(family_name, {}).get(_normalize_lookup_value(value), "")
    if normalized:
        return normalized
    inferred_key, _ = _infer_fragment_row(family_name, value)
    return str(inferred_key or default or "")


def fragment_entry(family: str, value: Any) -> dict[str, Any] | None:
    family_name = str(family or "").strip()
    canonical_key = normalize_fragment_key(family_name, value)
    if not canonical_key:
        return None
    if family_name == REVIEWER_NEXT_STEP_FRAGMENT_FAMILY:
        return {
            "family": family_name,
            "canonical_key": canonical_key,
            "fragment_key": canonical_key,
            "i18n_key": taxonomy_i18n_key(REVIEWER_NEXT_STEP_TEMPLATE_FAMILY, canonical_key),
            "zh_label": taxonomy_display_label(REVIEWER_NEXT_STEP_TEMPLATE_FAMILY, canonical_key, locale="zh_CN", default=""),
            "en_label": taxonomy_display_label(REVIEWER_NEXT_STEP_TEMPLATE_FAMILY, canonical_key, locale="en_US", default=""),
            "aliases": (),
            "parameter_names": (),
        }
    entry = _FRAGMENT_REGISTRY.get(family_name, {}).get(canonical_key)
    return dict(entry) if entry else None


def fragment_i18n_key(family: str, value: Any) -> str:
    entry = fragment_entry(family, value)
    return str((entry or {}).get("i18n_key") or "")


def fragment_display_label(
    family: str,
    value: Any,
    *,
    locale: str = "zh_CN",
    params: dict[str, Any] | None = None,
    default: str | None = None,
) -> str:
    family_name = str(family or "").strip()
    entry = fragment_entry(family_name, value)
    if family_name == REVIEWER_NEXT_STEP_FRAGMENT_FAMILY:
        rendered = taxonomy_display_label(
            REVIEWER_NEXT_STEP_TEMPLATE_FAMILY,
            normalize_fragment_key(family_name, value, default=str(value or "").strip()),
            locale=locale,
            default=default or str(value or "").strip(),
        )
        return _render_template(rendered, params)
    if not entry:
        if default is not None:
            return _render_template(default, params)
        return _render_template(str(value or "").strip(), params)
    template = str(entry.get("en_label") or default or "") if str(locale or "").lower().startswith("en") else str(entry.get("zh_label") or default or "")
    return _render_template(template, params)


def build_fragment_row(
    family: str,
    value: Any,
    *,
    params: dict[str, Any] | None = None,
    display_locale: str = "en_US",
    default_text: str | None = None,
) -> dict[str, Any]:
    payload = dict(value or {}) if isinstance(value, dict) else {}
    family_name = str(payload.get("fragment_family") or family or "").strip()
    input_value = payload.get("fragment_key") or payload.get("canonical_key") or payload.get("text") or value
    inferred_key, inferred_params = _infer_fragment_row(family_name, input_value)
    canonical_key = str(payload.get("canonical_key") or payload.get("fragment_key") or normalize_fragment_key(family_name, input_value, default="") or inferred_key or "").strip()
    merged_params = {
        **{key: value for key, value in inferred_params.items() if _stringify_param(value) != "--"},
        **{key: value for key, value in dict(payload.get("params") or {}).items() if str(key).strip()},
        **{key: value for key, value in dict(params or {}).items() if str(key).strip()},
    }
    fallback_text = str(payload.get("text") or default_text or (value if isinstance(value, str) else "")).strip()
    rendered_text = fragment_display_label(
        family_name,
        canonical_key or input_value,
        locale=display_locale,
        params=merged_params,
        default=fallback_text,
    )
    return {
        "fragment_family": family_name,
        "fragment_key": canonical_key,
        "canonical_key": canonical_key,
        "i18n_key": fragment_i18n_key(family_name, canonical_key or input_value),
        "params": merged_params,
        "text": rendered_text,
        "display_text": rendered_text,
        "reviewer_fragments_contract_version": REVIEWER_FRAGMENTS_CONTRACT_VERSION,
    }


def normalize_fragment_rows(
    family: str,
    values: Iterable[Any] | None,
    *,
    display_locale: str = "en_US",
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    seen: set[tuple[str, str, tuple[tuple[str, str], ...]]] = set()
    for value in list(values or []):
        row = build_fragment_row(family, value, display_locale=display_locale)
        text = str(row.get("text") or "").strip()
        canonical_key = str(row.get("canonical_key") or "").strip()
        params = tuple(sorted((str(key), _stringify_param(val)) for key, val in dict(row.get("params") or {}).items() if str(key).strip()))
        if not text and not canonical_key:
            continue
        marker = (canonical_key, text, params)
        if marker in seen:
            continue
        seen.add(marker)
        rows.append(row)
    return rows


def fragment_rows_to_keys(rows: Iterable[dict[str, Any]] | None) -> list[str]:
    values: list[str] = []
    for row in list(rows or []):
        canonical_key = str(dict(row or {}).get("canonical_key") or dict(row or {}).get("fragment_key") or "").strip()
        if canonical_key and canonical_key not in values:
            values.append(canonical_key)
    return values


def fragment_rows_to_texts(rows: Iterable[dict[str, Any]] | None) -> list[str]:
    values: list[str] = []
    for row in list(rows or []):
        text = str(dict(row or {}).get("text") or dict(row or {}).get("display_text") or "").strip()
        if text and text not in values:
            values.append(text)
    return values


def fragment_summary(
    rows: Iterable[dict[str, Any]] | None,
    *,
    default: str = "--",
    separator: str = " | ",
) -> str:
    values = fragment_rows_to_texts(rows)
    return separator.join(values) if values else str(default or "")


def fragment_text_replacements(*, locale: str = "zh_CN") -> list[tuple[str, str]]:
    rows: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for family, items in _FRAGMENT_REGISTRY.items():
        for canonical_key, entry in items.items():
            target = fragment_display_label(family, canonical_key, locale=locale)
            for source in (
                canonical_key,
                canonical_key.replace("_", " "),
                str(entry.get("en_label") or ""),
                str(entry.get("zh_label") or ""),
                *tuple(entry.get("aliases") or ()),
            ):
                source_text = str(source or "").strip()
                if not source_text or source_text == target:
                    continue
                pair = (source_text, target)
                if pair not in seen:
                    seen.add(pair)
                    rows.append(pair)
    return rows


_FRAGMENT_REGISTRY[BLOCKER_FRAGMENT_FAMILY].update(
    {
        "reference_registry_stub_only": _entry(
            BLOCKER_FRAGMENT_FAMILY,
            "reference_registry_stub_only",
            i18n_key="reviewer_fragments.blocker.reference_registry_stub_only",
            zh_label="reference registry \u4ecd\u4e3a stub\uff0c\u4e0d\u662f released traceability chain",
            en_label="Reference registry is still a stub and not a released traceability chain",
            aliases=("reference registry is still a stub and not a released traceability chain",),
        ),
        "certificate_backed_chain_open": _entry(
            BLOCKER_FRAGMENT_FAMILY,
            "certificate_backed_chain_open",
            i18n_key="reviewer_fragments.blocker.certificate_backed_chain_open",
            zh_label="certificate-backed \u53c2\u8003\u94fe\u5c1a\u672a\u95ed\u5408",
            en_label="Certificate-backed reference chain is not closed",
            aliases=("certificate-backed reference chain is not closed", "certificate-backed asset closure is missing", "certificate-backed release chain is not closed"),
        ),
        "certificate_files_missing": _entry(
            BLOCKER_FRAGMENT_FAMILY,
            "certificate_files_missing",
            i18n_key="reviewer_fragments.blocker.certificate_files_missing",
            zh_label="\u8bc1\u4e66\u6587\u4ef6\u4e0e intermediate checks \u4ecd\u7f3a\u5931",
            en_label="Certificate files and intermediate checks remain missing",
            aliases=("certificate files and intermediate checks remain missing",),
        ),
        "traceability_stub_only": _entry(
            BLOCKER_FRAGMENT_FAMILY,
            "traceability_stub_only",
            i18n_key="reviewer_fragments.blocker.traceability_stub_only",
            zh_label="traceability \u94fe\u4ecd\u4e3a reviewer-facing stub-only",
            en_label="Traceability chain stays reviewer-facing only",
            aliases=("traceability chain stays reviewer-facing only", "traceability rows remain stub-only"),
        ),
        "uncertainty_placeholder_only": _entry(
            BLOCKER_FRAGMENT_FAMILY,
            "uncertainty_placeholder_only",
            i18n_key="reviewer_fragments.blocker.uncertainty_placeholder_only",
            zh_label="\u4e0d\u786e\u5b9a\u5ea6\u6e90\u4ecd\u4e3a placeholders only",
            en_label="Uncertainty sources are placeholders only",
            aliases=("uncertainty sources are placeholders only",),
        ),
        "simulation_not_close_uncertainty_budget": _entry(
            BLOCKER_FRAGMENT_FAMILY,
            "simulation_not_close_uncertainty_budget",
            i18n_key="reviewer_fragments.blocker.simulation_not_close_uncertainty_budget",
            zh_label="simulation \u4e0d\u4f1a\u95ed\u5408 released uncertainty budgets",
            en_label="Simulation does not close released uncertainty budgets",
            aliases=("simulation does not close released uncertainty budgets",),
        ),
        "protocol_placeholder_only": _entry(
            BLOCKER_FRAGMENT_FAMILY,
            "protocol_placeholder_only",
            i18n_key="reviewer_fragments.blocker.protocol_placeholder_only",
            zh_label="protocol \u4ecd\u4e3a placeholder-only",
            en_label="Protocol remains placeholder-only",
            aliases=("protocol remains placeholder-only",),
        ),
        "simulation_not_close_method_confirmation": _entry(
            BLOCKER_FRAGMENT_FAMILY,
            "simulation_not_close_method_confirmation",
            i18n_key="reviewer_fragments.blocker.simulation_not_close_method_confirmation",
            zh_label="simulation \u4e0d\u4f1a\u95ed\u5408 method confirmation evidence",
            en_label="Simulation does not close method confirmation evidence",
            aliases=("simulation does not close method confirmation evidence",),
        ),
        "method_matrix_reviewer_only": _entry(
            BLOCKER_FRAGMENT_FAMILY,
            "method_matrix_reviewer_only",
            i18n_key="reviewer_fragments.blocker.method_matrix_reviewer_only",
            zh_label="matrix rows \u4ecd\u4e3a reviewer-only\uff0c\u4e0d\u662f released method confirmation evidence",
            en_label="Matrix rows remain reviewer-only and not released method confirmation evidence",
            aliases=("matrix rows remain reviewer-only and not released method confirmation evidence",),
        ),
        "uncertainty_method_readiness_open": _entry(
            BLOCKER_FRAGMENT_FAMILY,
            "uncertainty_method_readiness_open",
            i18n_key="reviewer_fragments.blocker.uncertainty_method_readiness_open",
            zh_label="\u4e0d\u786e\u5b9a\u5ea6 / method readiness \u4ecd\u4e3a\u5f00\u53e3\uff0c\u9700\u5728 Step 2 \u5916\u90e8\u5173\u95ed\u7f3a\u5931\u8bc1\u636e",
            en_label="Uncertainty / method readiness remains open until missing evidence is closed outside Step 2",
            aliases=("uncertainty / method readiness remains open until missing evidence is closed outside Step 2",),
        ),
        "software_traceability_reviewer_only": _entry(
            BLOCKER_FRAGMENT_FAMILY,
            "software_traceability_reviewer_only",
            i18n_key="reviewer_fragments.blocker.software_traceability_reviewer_only",
            zh_label="software traceability matrix \u4ecd\u4e3a reviewer-facing only",
            en_label="Software traceability matrix remains reviewer-facing only",
            aliases=("software traceability matrix remains reviewer-facing only",),
        ),
        "no_live_release_qualification_claim": _entry(
            BLOCKER_FRAGMENT_FAMILY,
            "no_live_release_qualification_claim",
            i18n_key="reviewer_fragments.blocker.no_live_release_qualification_claim",
            zh_label="\u6b64\u5904\u4e0d\u4ea7\u751f live release qualification claim",
            en_label="No live release qualification claim is produced here",
            aliases=("no live release qualification claim is produced here",),
        ),
        "artifact_hash_stub_only": _entry(
            BLOCKER_FRAGMENT_FAMILY,
            "artifact_hash_stub_only",
            i18n_key="reviewer_fragments.blocker.artifact_hash_stub_only",
            zh_label="artifact hash closure \u4ecd\u4e3a stub-only",
            en_label="Artifact hash closure is still stub-only",
            aliases=("artifact hash closure is still stub-only",),
        ),
        "manifest_not_released_validation_record": _entry(
            BLOCKER_FRAGMENT_FAMILY,
            "manifest_not_released_validation_record",
            i18n_key="reviewer_fragments.blocker.manifest_not_released_validation_record",
            zh_label="manifest \u4e0d\u662f released validation record",
            en_label="Manifest is not a released validation record",
            aliases=("manifest is not a released validation record",),
        ),
        "audit_digest_traceability_skeleton": _entry(
            BLOCKER_FRAGMENT_FAMILY,
            "audit_digest_traceability_skeleton",
            i18n_key="reviewer_fragments.blocker.audit_digest_traceability_skeleton",
            zh_label="audit digest \u4ecd\u4e3a reviewer traceability skeleton only",
            en_label="Audit digest remains a reviewer traceability skeleton only",
            aliases=("audit digest remains a reviewer traceability skeleton only",),
        ),
        "no_formal_audit_conclusion": _entry(
            BLOCKER_FRAGMENT_FAMILY,
            "no_formal_audit_conclusion",
            i18n_key="reviewer_fragments.blocker.no_formal_audit_conclusion",
            zh_label="\u6b64\u5904\u4e0d\u4ea7\u751f formal audit conclusion",
            en_label="No formal audit conclusion is produced here",
            aliases=("no formal audit conclusion is produced here",),
        ),
    }
)


_rebuild_alias_index()
