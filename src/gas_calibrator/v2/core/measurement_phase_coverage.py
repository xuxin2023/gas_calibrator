from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Iterable

from .models import SamplingResult
from .multi_source_stability import CANONICAL_BOUNDARY_STATEMENTS, SIGNAL_GROUP_CHANNELS, SIGNAL_GROUP_ORDER


MEASUREMENT_PHASE_COVERAGE_REPORT_FILENAME = "measurement_phase_coverage_report.json"
MEASUREMENT_PHASE_COVERAGE_REPORT_MARKDOWN_FILENAME = "measurement_phase_coverage_report.md"

_PHASE_DEFINITIONS = (
    {
        "phase_name": "ambient_diagnostic",
        "route_family": "ambient",
        "policy_version": "shadow_ambient_v1",
        "fallback_evidence_source": "model_only",
        "reviewer_note": "Ambient/diagnostic phase readiness coverage for simulation-only reviewer evidence.",
    },
    {
        "phase_name": "preseal",
        "route_family": "water",
        "policy_version": "shadow_water_v1",
        "fallback_evidence_source": "model_only",
        "reviewer_note": "Water-route preseal phase coverage for Step 2 tail / Stage 3 bridge review.",
    },
    {
        "phase_name": "preseal",
        "route_family": "gas",
        "policy_version": "shadow_gas_v1",
        "fallback_evidence_source": "model_only",
        "reviewer_note": "Gas-route preseal phase coverage for Step 2 tail / Stage 3 bridge review.",
    },
    {
        "phase_name": "pressure_stable",
        "route_family": "water",
        "policy_version": "shadow_water_v1",
        "fallback_evidence_source": "model_only",
        "reviewer_note": "Water-route pressure-stable phase coverage for simulation-only shadow review.",
    },
    {
        "phase_name": "pressure_stable",
        "route_family": "gas",
        "policy_version": "shadow_gas_v1",
        "fallback_evidence_source": "model_only",
        "reviewer_note": "Gas-route pressure-stable phase coverage for simulation-only shadow review.",
    },
    {
        "phase_name": "sample_ready",
        "route_family": "ambient",
        "policy_version": "shadow_ambient_v1",
        "fallback_evidence_source": "model_only",
        "reviewer_note": "Ambient sample-ready coverage uses simulation outputs only and is not a real acceptance path.",
    },
    {
        "phase_name": "sample_ready",
        "route_family": "water",
        "policy_version": "shadow_water_v1",
        "fallback_evidence_source": "model_only",
        "reviewer_note": "Water sample-ready coverage uses shadow decisions only and cannot replace metrology validation.",
    },
    {
        "phase_name": "sample_ready",
        "route_family": "gas",
        "policy_version": "shadow_gas_v1",
        "fallback_evidence_source": "model_only",
        "reviewer_note": "Gas sample-ready coverage uses shadow decisions only and cannot replace metrology validation.",
    },
    {
        "phase_name": "recovery_retry",
        "route_family": "system",
        "policy_version": "controlled_flex_v1",
        "fallback_evidence_source": "test_only",
        "reviewer_note": "Recovery/retry coverage is currently model/test oriented unless a simulated trace captures it explicitly.",
    },
)

_PHASE_ACTIONS: dict[str, tuple[str, ...]] = {
    "ambient_diagnostic": ("ambient", "diagnostic", "wait_temperature", "wait_humidity"),
    "preseal": ("set_h2o_path", "set_co2_valves", "wait_route_ready", "wait_dewpoint", "wait_route_soak", "seal_route"),
    "pressure_stable": ("set_pressure", "wait_post_pressure"),
    "sample_ready": ("sample_start", "sample_end"),
    "recovery_retry": ("retry", "recovery", "abort", "fault_capture", "safe_recovery"),
}


def build_measurement_phase_coverage_report(
    *,
    run_id: str,
    samples: Iterable[SamplingResult],
    point_summaries: Iterable[dict[str, Any]] | None = None,
    route_trace_events: Iterable[dict[str, Any]] | None = None,
    multi_source_stability_evidence: dict[str, Any] | None = None,
    state_transition_evidence: dict[str, Any] | None = None,
    artifact_paths: dict[str, Any] | None = None,
    synthetic_trace_provenance: dict[str, Any] | None = None,
) -> dict[str, Any]:
    sample_rows = [sample for sample in list(samples or []) if isinstance(sample, SamplingResult)]
    summary_rows = [dict(item) for item in list(point_summaries or []) if isinstance(item, dict)]
    trace_rows = [dict(item) for item in list(route_trace_events or []) if isinstance(item, dict)]
    stability_raw = dict((multi_source_stability_evidence or {}).get("raw") or multi_source_stability_evidence or {})
    transition_raw = dict((state_transition_evidence or {}).get("raw") or state_transition_evidence or {})
    artifact_path_map = {
        "measurement_phase_coverage_report": str(
            dict(artifact_paths or {}).get("measurement_phase_coverage_report")
            or MEASUREMENT_PHASE_COVERAGE_REPORT_FILENAME
        ),
        "measurement_phase_coverage_report_markdown": str(
            dict(artifact_paths or {}).get("measurement_phase_coverage_report_markdown")
            or MEASUREMENT_PHASE_COVERAGE_REPORT_MARKDOWN_FILENAME
        ),
        "multi_source_stability_evidence": str(
            dict(artifact_paths or {}).get("multi_source_stability_evidence")
            or "multi_source_stability_evidence.json"
        ),
        "state_transition_evidence": str(
            dict(artifact_paths or {}).get("state_transition_evidence")
            or "state_transition_evidence.json"
        ),
        "simulation_evidence_sidecar_bundle": str(
            dict(artifact_paths or {}).get("simulation_evidence_sidecar_bundle")
            or "simulation_evidence_sidecar_bundle.json"
        ),
    }

    configured_routes = _configured_route_families(sample_rows, summary_rows, trace_rows)
    sample_groups = _sample_groups(sample_rows)
    trace_groups = _trace_groups(trace_rows)
    stability_groups = _stability_groups(stability_raw)
    transition_groups = _transition_groups(transition_raw)
    phase_rows: list[dict[str, Any]] = []
    for definition in _PHASE_DEFINITIONS:
        row = _build_phase_row(
            definition=definition,
            configured_routes=configured_routes,
            sample_groups=sample_groups,
            trace_groups=trace_groups,
            stability_groups=stability_groups,
            transition_groups=transition_groups,
            artifact_paths=artifact_path_map,
        )
        phase_rows.append(row)

    actual_count = sum(1 for row in phase_rows if row.get("evidence_source") == "actual_simulated_run")
    model_only_count = sum(1 for row in phase_rows if row.get("evidence_source") == "model_only")
    test_only_count = sum(1 for row in phase_rows if row.get("evidence_source") == "test_only")
    gap_count = sum(1 for row in phase_rows if row.get("evidence_source") == "gap")
    routes = _dedupe(row.get("route_family") for row in phase_rows)
    phases = _dedupe(row.get("phase_name") for row in phase_rows)
    signal_families = _dedupe(
        group_name
        for row in phase_rows
        for group_name, payload in dict(row.get("signal_group_coverage") or {}).items()
        if str(dict(payload).get("coverage_status") or "") not in {"", "gap"}
    )
    evidence_sources = _dedupe(row.get("evidence_source") for row in phase_rows)
    decision_results = _dedupe(row.get("decision_result") for row in phase_rows)
    policy_versions = _dedupe(row.get("policy_version") for row in phase_rows)
    actual_summary = " | ".join(
        f"{row['route_family']}/{row['phase_name']}"
        for row in phase_rows
        if row.get("evidence_source") == "actual_simulated_run"
    ) or "no actual simulated phase evidence"
    gap_summary = " | ".join(
        f"{row['route_family']}/{row['phase_name']}"
        for row in phase_rows
        if row.get("evidence_source") in {"gap", "model_only", "test_only"}
    ) or "no phase coverage gaps"
    coverage_summary = " | ".join(
        f"{row['route_family']}/{row['phase_name']}={row['evidence_source']}"
        for row in phase_rows
    )
    digest = {
        "summary": (
            "Step 2 tail / Stage 3 bridge | measurement phase coverage | "
            f"actual {actual_count} | model-only {model_only_count} | test-only {test_only_count} | gap {gap_count}"
        ),
        "actual_phase_summary": actual_summary,
        "coverage_summary": coverage_summary,
        "gap_summary": gap_summary,
        "boundary_summary": " | ".join(CANONICAL_BOUNDARY_STATEMENTS),
    }
    review_surface = {
        "title_text": "Measurement Phase Coverage Report",
        "role_text": "diagnostic_analysis",
        "reviewer_note": (
            "Step 2 tail / Stage 3 bridge reviewer evidence for richer simulation coverage only. "
            "This is readiness mapping for measurement-core evidence and not a runtime control surface."
        ),
        "summary_text": digest["summary"],
        "summary_lines": [
            digest["summary"],
            f"actual simulated phases: {actual_summary}",
            f"coverage digest: {coverage_summary}",
            f"phase gaps: {gap_summary}",
        ],
        "detail_lines": [
            f"route families: {', '.join(routes) or '--'}",
            f"phase buckets: {', '.join(phases) or '--'}",
            f"synthetic provenance: {dict(synthetic_trace_provenance or {}).get('summary', 'simulation trace only')}",
            *[f"boundary: {line}" for line in CANONICAL_BOUNDARY_STATEMENTS],
        ],
        "anchor_id": "measurement-phase-coverage-report",
        "anchor_label": "Measurement phase coverage report",
        "phase_filters": phases,
        "route_filters": routes,
        "signal_family_filters": signal_families,
        "decision_result_filters": decision_results,
        "policy_version_filters": policy_versions,
        "boundary_filters": list(CANONICAL_BOUNDARY_STATEMENTS),
        "evidence_source_filters": evidence_sources,
        "artifact_paths": dict(artifact_path_map),
    }
    raw = {
        "schema_version": "1.0",
        "artifact_type": "measurement_phase_coverage_report",
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "run_id": str(run_id or ""),
        "artifact_role": "diagnostic_analysis",
        "evidence_source": "simulated",
        "not_real_acceptance_evidence": True,
        "boundary_statements": list(CANONICAL_BOUNDARY_STATEMENTS),
        "phase_rows": phase_rows,
        "phase_index": {str(row.get("phase_route_key") or ""): dict(row) for row in phase_rows},
        "synthetic_trace_provenance": dict(synthetic_trace_provenance or {}),
        "digest": digest,
        "review_surface": review_surface,
        "artifact_paths": artifact_path_map,
        "overall_status": "diagnostic_only" if actual_count == 0 else "degraded" if gap_count else "passed",
    }
    return {
        "available": True,
        "artifact_type": "measurement_phase_coverage_report",
        "filename": MEASUREMENT_PHASE_COVERAGE_REPORT_FILENAME,
        "markdown_filename": MEASUREMENT_PHASE_COVERAGE_REPORT_MARKDOWN_FILENAME,
        "raw": raw,
        "markdown": _render_markdown(raw=raw),
        "digest": digest,
    }


def _build_phase_row(
    *,
    definition: dict[str, Any],
    configured_routes: set[str],
    sample_groups: dict[tuple[str, str], list[SamplingResult]],
    trace_groups: dict[tuple[str, str], list[dict[str, Any]]],
    stability_groups: dict[tuple[str, str], dict[str, Any]],
    transition_groups: dict[tuple[str, str], dict[str, Any]],
    artifact_paths: dict[str, str],
) -> dict[str, Any]:
    phase_name = str(definition.get("phase_name") or "")
    route_family = str(definition.get("route_family") or "")
    key = (route_family, phase_name)
    sample_rows = list(sample_groups.get(key) or [])
    trace_rows = list(trace_groups.get(key) or [])
    stability_row = dict(stability_groups.get(key) or {})
    if not stability_row and route_family == "ambient" and phase_name == "ambient_diagnostic":
        stability_row = dict(stability_groups.get(("ambient", "sample_ready")) or {})
    transition_row = dict(transition_groups.get(key) or {})
    actual_run_evidence_present = bool(sample_rows or trace_rows or stability_row or transition_row)
    fallback_source = str(definition.get("fallback_evidence_source") or "model_only")
    route_in_scope = route_family in configured_routes or route_family == "system"
    if actual_run_evidence_present:
        evidence_source = "actual_simulated_run"
    elif route_family == "system":
        evidence_source = fallback_source
    elif route_in_scope:
        evidence_source = fallback_source
    else:
        evidence_source = "gap"

    signal_group_coverage, available_channels, missing_channels = _phase_signal_coverage(
        phase_name=phase_name,
        route_family=route_family,
        sample_rows=sample_rows,
    )
    decision_result = str(
        stability_row.get("decision_result")
        or transition_row.get("decision_result")
        or ("trace_only_no_shadow_window" if actual_run_evidence_present else f"{evidence_source}_coverage")
    )
    hold_time_summary = _hold_time_summary(stability_row, actual_run_evidence_present=actual_run_evidence_present)
    summary = (
        f"{route_family}/{phase_name} | {evidence_source} | "
        f"decision {decision_result} | hold {hold_time_summary}"
    )
    return {
        "phase_name": phase_name,
        "route_family": route_family,
        "phase_route_key": f"{route_family}:{phase_name}",
        "actual_run_evidence_present": actual_run_evidence_present,
        "evidence_source": evidence_source,
        "signal_group_coverage": signal_group_coverage,
        "available_channels": available_channels,
        "missing_channels": missing_channels,
        "policy_version": str(
            stability_row.get("policy_version")
            or definition.get("policy_version")
            or transition_row.get("policy_version")
            or "--"
        ),
        "decision_result": decision_result,
        "decision_summary": str(
            stability_row.get("decision_result")
            or transition_row.get("decision_result")
            or ("actual trace without shadow window" if actual_run_evidence_present else f"{evidence_source} phase coverage")
        ),
        "hold_time_summary": hold_time_summary,
        "linked_artifacts": {
            "multi_source_stability_evidence": str(artifact_paths.get("multi_source_stability_evidence") or ""),
            "state_transition_evidence": str(artifact_paths.get("state_transition_evidence") or ""),
            "simulation_evidence_sidecar_bundle": str(artifact_paths.get("simulation_evidence_sidecar_bundle") or ""),
        },
        "reviewer_note": str(definition.get("reviewer_note") or ""),
        "digest": summary,
    }


def _configured_route_families(
    samples: list[SamplingResult],
    point_summaries: list[dict[str, Any]],
    route_trace_events: list[dict[str, Any]],
) -> set[str]:
    rows = {
        _route_family_from_sample(sample)
        for sample in samples
    }
    rows.update(
        _route_family(
            str(dict(item.get("point") or {}).get("route") or ""),
            pressure_mode=str(dict(item.get("point") or {}).get("pressure_mode") or ""),
        )
        for item in point_summaries
        if isinstance(item, dict)
    )
    rows.update(_route_family_from_trace(event) for event in route_trace_events)
    return {str(item).strip() for item in rows if str(item).strip()}


def _sample_groups(samples: list[SamplingResult]) -> dict[tuple[str, str], list[SamplingResult]]:
    rows: dict[tuple[str, str], list[SamplingResult]] = {}
    for sample in samples:
        key = (_route_family_from_sample(sample), _phase_name_from_sample(sample))
        rows.setdefault(key, []).append(sample)
    return rows


def _trace_groups(route_trace_events: list[dict[str, Any]]) -> dict[tuple[str, str], list[dict[str, Any]]]:
    rows: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for event in route_trace_events:
        for phase_name in _phase_names_from_trace(event):
            key = (_route_family_from_trace(event), phase_name)
            rows.setdefault(key, []).append(dict(event))
    return rows


def _stability_groups(stability_raw: dict[str, Any]) -> dict[tuple[str, str], dict[str, Any]]:
    rows: dict[tuple[str, str], dict[str, Any]] = {}
    for decision in list(stability_raw.get("stability_decisions") or []):
        if not isinstance(decision, dict):
            continue
        route_family = str(decision.get("route_family") or "").strip()
        phase_policy = str(decision.get("phase_policy") or "").strip()
        if route_family and phase_policy:
            rows[(route_family, phase_policy)] = dict(decision)
    return rows


def _transition_groups(transition_raw: dict[str, Any]) -> dict[tuple[str, str], dict[str, Any]]:
    rows: dict[tuple[str, str], dict[str, Any]] = {}
    for decision in list(transition_raw.get("phase_decision_logs") or []):
        if not isinstance(decision, dict):
            continue
        route_family = str(decision.get("route_family") or "").strip()
        phase_policy = str(decision.get("phase_policy") or "").strip()
        if route_family and phase_policy:
            rows[(route_family, phase_policy)] = dict(decision)
    return rows


def _phase_signal_coverage(
    *,
    phase_name: str,
    route_family: str,
    sample_rows: list[SamplingResult],
) -> tuple[dict[str, dict[str, Any]], list[str], list[str]]:
    if not sample_rows:
        coverage = {
            group_name: {
                "coverage_status": "gap",
                "available_channels": [],
                "missing_channels": _required_channels_for_route(group_name, route_family),
            }
            for group_name in SIGNAL_GROUP_ORDER
        }
        return coverage, [], _dedupe(
            channel
            for group_name in SIGNAL_GROUP_ORDER
            for channel in coverage[group_name]["missing_channels"]
        )

    coverage: dict[str, dict[str, Any]] = {}
    available_all: list[str] = []
    missing_all: list[str] = []
    for group_name in SIGNAL_GROUP_ORDER:
        expected = _required_channels_for_route(group_name, route_family)
        available = [
            channel
            for channel in SIGNAL_GROUP_CHANNELS[group_name]
            if any(_has_value(getattr(sample, channel, None)) for sample in sample_rows)
        ]
        if not expected:
            status = "gap"
            missing = []
        else:
            missing = [channel for channel in expected if channel not in set(available)]
            if len(available) >= len(expected):
                status = "complete"
            elif available:
                status = "partial"
            else:
                status = "gap"
        coverage[group_name] = {
            "coverage_status": status,
            "available_channels": available,
            "missing_channels": missing,
        }
        available_all.extend(available)
        missing_all.extend(missing)
    return coverage, _dedupe(available_all), _dedupe(missing_all)


def _required_channels_for_route(group_name: str, route_family: str) -> list[str]:
    if route_family == "water":
        requirements = {
            "reference": ["temperature_c", "dew_point_c", "pressure_hpa"],
            "analyzer_raw": ["h2o_ratio_raw", "h2o_signal", "ref_signal"],
            "output": ["h2o_mmol", "h2o_ratio_f"],
            "data_quality": ["frame_has_data", "frame_usable", "stability_time_s"],
        }
        return list(requirements.get(group_name, []))
    if route_family == "ambient":
        requirements = {
            "reference": ["temperature_c", "pressure_hpa"],
            "analyzer_raw": ["ref_signal"],
            "output": ["co2_ppm"],
            "data_quality": ["frame_has_data", "frame_usable"],
        }
        return list(requirements.get(group_name, []))
    if route_family == "system":
        return []
    requirements = {
        "reference": ["temperature_c", "pressure_hpa"],
        "analyzer_raw": ["co2_ratio_raw", "co2_signal", "ref_signal"],
        "output": ["co2_ppm", "co2_ratio_f"],
        "data_quality": ["frame_has_data", "frame_usable", "stability_time_s"],
    }
    return list(requirements.get(group_name, []))


def _hold_time_summary(stability_row: dict[str, Any], *, actual_run_evidence_present: bool) -> str:
    if stability_row:
        if stability_row.get("hold_time_met") is True:
            return "hold_time_met"
        if stability_row.get("hold_time_met") is False:
            return "hold_time_gap"
    return "trace_only_not_evaluated" if actual_run_evidence_present else "not_applicable"


def _phase_name_from_sample(sample: SamplingResult) -> str:
    route_family = _route_family_from_sample(sample)
    phase_text = str(getattr(sample, "point_phase", "") or "").strip().lower()
    if route_family == "ambient":
        return "ambient_diagnostic"
    if "preseal" in phase_text or "seal" in phase_text:
        return "preseal"
    if "pressure" in phase_text:
        return "pressure_stable"
    if "recovery" in phase_text or "retry" in phase_text or "abort" in phase_text:
        return "recovery_retry"
    return "sample_ready"


def _phase_names_from_trace(event: dict[str, Any]) -> list[str]:
    text = " ".join(
        str(event.get(key) or "")
        for key in ("action", "message", "route", "point_tag", "result")
    ).strip().lower()
    route_family = _route_family_from_trace(event)
    rows: list[str] = []
    if route_family == "ambient":
        rows.append("ambient_diagnostic")
    if any(token in text for token in _PHASE_ACTIONS["recovery_retry"]):
        rows.append("recovery_retry")
    if any(token in text for token in _PHASE_ACTIONS["pressure_stable"]):
        rows.append("pressure_stable")
    if any(token in text for token in _PHASE_ACTIONS["sample_ready"]):
        rows.append("sample_ready")
    if any(token in text for token in _PHASE_ACTIONS["preseal"]):
        rows.append("preseal")
    return _dedupe(rows)


def _route_family_from_trace(event: dict[str, Any]) -> str:
    route = str(event.get("route") or "").strip().lower()
    if route == "h2o":
        return "water"
    if "ambient" in route:
        return "ambient"
    if route:
        return "gas"
    text = " ".join(str(event.get(key) or "") for key in ("action", "message", "point_tag")).lower()
    if "ambient" in text or "diagnostic" in text:
        return "ambient"
    return "system" if any(token in text for token in _PHASE_ACTIONS["recovery_retry"]) else "gas"


def _route_family_from_sample(sample: SamplingResult) -> str:
    point = getattr(sample, "point", None)
    route = str(getattr(point, "route", "") or "").strip().lower()
    pressure_mode = str(getattr(point, "effective_pressure_mode", "") or "").strip().lower()
    return _route_family(route, pressure_mode=pressure_mode)


def _route_family(route: str, *, pressure_mode: str = "") -> str:
    route_text = str(route or "").strip().lower()
    pressure_text = str(pressure_mode or "").strip().lower()
    if route_text == "h2o":
        return "water"
    if pressure_text == "ambient_open" or "ambient" in route_text:
        return "ambient"
    return "gas" if route_text else "system"


def _has_value(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    return True


def _render_markdown(*, raw: dict[str, Any]) -> str:
    review_surface = dict(raw.get("review_surface") or {})
    phase_rows = [dict(item) for item in list(raw.get("phase_rows") or []) if isinstance(item, dict)]
    lines = [
        "# Measurement Phase Coverage Report",
        "",
        f"- title: {review_surface.get('title_text', '--')}",
        f"- role: {review_surface.get('role_text', '--')}",
        f"- reviewer_note: {review_surface.get('reviewer_note', '--')}",
        "",
        "## Boundary",
        "",
        *[f"- {line}" for line in CANONICAL_BOUNDARY_STATEMENTS],
        "",
        "## Phase Coverage",
        "",
    ]
    for row in phase_rows:
        lines.append(
            f"- {row.get('route_family', '--')}/{row.get('phase_name', '--')}: "
            f"{row.get('evidence_source', '--')} | decision {row.get('decision_result', '--')} | "
            f"hold {row.get('hold_time_summary', '--')}"
        )
        lines.append(
            f"  available {', '.join(list(row.get('available_channels') or [])[:6]) or '--'} | "
            f"missing {', '.join(list(row.get('missing_channels') or [])[:6]) or '--'}"
        )
    lines.extend(
        [
            "",
            "## Artifact Paths",
            "",
            f"- json: {dict(raw.get('artifact_paths') or {}).get('measurement_phase_coverage_report', '--')}",
            f"- markdown: {dict(raw.get('artifact_paths') or {}).get('measurement_phase_coverage_report_markdown', '--')}",
            "",
        ]
    )
    return "\n".join(lines).strip() + "\n"


def _dedupe(values: Iterable[Any]) -> list[str]:
    rows: list[str] = []
    for value in values:
        text = str(value or "").strip()
        if text and text not in rows:
            rows.append(text)
    return rows
