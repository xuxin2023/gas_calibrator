from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import statistics
from typing import Any, Iterable

from .models import SamplingResult


MULTI_SOURCE_STABILITY_EVIDENCE_FILENAME = "multi_source_stability_evidence.json"
MULTI_SOURCE_STABILITY_EVIDENCE_MARKDOWN_FILENAME = "multi_source_stability_evidence.md"
SIMULATION_EVIDENCE_SIDECAR_BUNDLE_FILENAME = "simulation_evidence_sidecar_bundle.json"

CANONICAL_BOUNDARY_STATEMENTS = [
    "Step 2 tail / Stage 3 bridge",
    "simulation / offline / headless only",
    "not real acceptance",
    "cannot replace real metrology validation",
    "shadow evaluation only",
    "does not modify live sampling gate by default",
]

SIGNAL_GROUP_ORDER = ("reference", "analyzer_raw", "output", "data_quality")
SIGNAL_GROUP_CHANNELS: dict[str, tuple[str, ...]] = {
    "reference": (
        "temperature_c",
        "dew_point_c",
        "pressure_hpa",
        "pressure_gauge_hpa",
        "pressure_reference_status",
        "thermometer_temp_c",
        "thermometer_reference_status",
    ),
    "analyzer_raw": (
        "co2_ratio_raw",
        "h2o_ratio_raw",
        "ref_signal",
        "co2_signal",
        "h2o_signal",
        "analyzer_pressure_kpa",
        "analyzer_chamber_temp_c",
        "case_temp_c",
        "frame_status",
    ),
    "output": (
        "co2_ppm",
        "h2o_mmol",
        "co2_ratio_f",
        "h2o_ratio_f",
    ),
    "data_quality": (
        "frame_has_data",
        "frame_usable",
        "sample_index",
        "stability_time_s",
        "total_time_s",
        "point_phase",
        "point_tag",
    ),
}

ROUTE_POLICY_TEMPLATES: dict[str, dict[str, Any]] = {
    "water": {
        "policy_version": "shadow_water_v1",
        "min_valid_ratio": 0.75,
        "max_freshness_gap_s": 12.0,
        "min_hold_time_s": 10.0,
        "pass_score": 0.82,
        "warn_score": 0.65,
        "required_channels": {
            "reference": ("temperature_c", "dew_point_c", "pressure_hpa"),
            "analyzer_raw": ("h2o_ratio_raw", "h2o_signal", "ref_signal"),
            "output": ("h2o_mmol", "h2o_ratio_f"),
            "data_quality": ("frame_has_data", "frame_usable", "stability_time_s"),
        },
        "score_weights": {
            "reference": 0.20,
            "analyzer_raw": 0.35,
            "output": 0.25,
            "data_quality": 0.20,
        },
    },
    "gas": {
        "policy_version": "shadow_gas_v1",
        "min_valid_ratio": 0.80,
        "max_freshness_gap_s": 10.0,
        "min_hold_time_s": 12.0,
        "pass_score": 0.84,
        "warn_score": 0.68,
        "required_channels": {
            "reference": ("temperature_c", "pressure_hpa"),
            "analyzer_raw": ("co2_ratio_raw", "co2_signal", "ref_signal"),
            "output": ("co2_ppm", "co2_ratio_f"),
            "data_quality": ("frame_has_data", "frame_usable", "stability_time_s"),
        },
        "score_weights": {
            "reference": 0.20,
            "analyzer_raw": 0.40,
            "output": 0.25,
            "data_quality": 0.15,
        },
    },
    "ambient": {
        "policy_version": "shadow_ambient_v1",
        "min_valid_ratio": 0.70,
        "max_freshness_gap_s": 20.0,
        "min_hold_time_s": 6.0,
        "pass_score": 0.72,
        "warn_score": 0.55,
        "required_channels": {
            "reference": ("temperature_c", "pressure_hpa"),
            "analyzer_raw": ("ref_signal",),
            "output": ("co2_ppm",),
            "data_quality": ("frame_has_data", "frame_usable"),
        },
        "score_weights": {
            "reference": 0.25,
            "analyzer_raw": 0.25,
            "output": 0.20,
            "data_quality": 0.30,
        },
    },
}


@dataclass(frozen=True)
class StabilityPolicyVersion:
    route_family: str
    phase_policy: str
    policy_version: str
    min_valid_ratio: float
    max_freshness_gap_s: float
    min_hold_time_s: float
    pass_score: float
    warn_score: float
    required_signal_groups: tuple[str, ...]


@dataclass(frozen=True)
class StabilityWindowStats:
    window_id: str
    point_index: int
    route_family: str
    phase_policy: str
    sample_count: int
    valid_ratio: float
    hold_time_observed_s: float
    freshness_gap_s: float | None
    coverage_status: str


@dataclass(frozen=True)
class StabilityDecisionResult:
    window_id: str
    route_family: str
    phase_policy: str
    policy_version: str
    decision_result: str
    status: str
    weighted_score: float
    hold_time_met: bool
    hard_gate_passed: bool
    partial_coverage: bool


def build_multi_source_stability_evidence(
    *,
    run_id: str,
    samples: Iterable[SamplingResult],
    point_summaries: Iterable[dict[str, Any]] | None = None,
    artifact_paths: dict[str, Any] | None = None,
) -> dict[str, Any]:
    sample_rows = [sample for sample in list(samples or []) if isinstance(sample, SamplingResult)]
    summary_rows = [dict(item) for item in list(point_summaries or []) if isinstance(item, dict)]
    windows = _build_windows(sample_rows, summary_rows)
    policy_versions = _policy_versions_for_windows(windows)
    evaluations = [_evaluate_window(window) for window in windows]
    decision_counts = _count_by_key(evaluations, "decision_result")
    status_counts = _count_by_key(evaluations, "status")
    coverage_counts = _count_by_key(evaluations, "coverage_status")
    raw_signal_snapshots = [_snapshot_from_evaluation(item) for item in evaluations]
    signal_group_coverage = _aggregate_signal_group_coverage(evaluations)
    missing_channels = {
        group_name: _dedupe(
            channel
            for item in evaluations
            for channel in list(dict(item.get("missing_channels_by_group") or {}).get(group_name, []) or [])
        )
        for group_name in SIGNAL_GROUP_ORDER
    }
    available_channels = {
        group_name: _dedupe(
            channel
            for item in evaluations
            for channel in list(dict(item.get("available_channels_by_group") or {}).get(group_name, []) or [])
        )
        for group_name in SIGNAL_GROUP_ORDER
    }
    routes = _dedupe(item.get("route_family") for item in evaluations)
    phases = _dedupe(item.get("phase_policy") for item in evaluations)
    policy_version_filters = _dedupe(item.get("policy_version") for item in evaluations)
    decision_result_filters = _dedupe(item.get("decision_result") for item in evaluations)
    signal_family_filters = _dedupe(
        group_name
        for group_name, coverage in signal_group_coverage.items()
        if int(dict(coverage or {}).get("window_count", 0) or 0) > 0
    )
    overall_status = _overall_status_from_evaluations(evaluations)
    coverage_status = _coverage_status_from_groups(signal_group_coverage)
    digest = {
        "summary": (
            f"Step 2 tail / Stage 3 bridge | shadow evaluation only | "
            f"windows {len(evaluations)} | routes {', '.join(routes) or '--'} | "
            f"coverage {coverage_status}"
        ),
        "policy_summary": " | ".join(policy_version_filters) or "--",
        "coverage_summary": " | ".join(
            f"{group_name} {dict(signal_group_coverage.get(group_name) or {}).get('coverage_status', 'missing')}"
            for group_name in SIGNAL_GROUP_ORDER
        ),
        "decision_summary": " | ".join(f"{key} {value}" for key, value in decision_counts.items()) or "--",
        "gap_summary": " | ".join(
            f"{group_name}: {', '.join(channels[:3])}" for group_name, channels in missing_channels.items() if channels
        )
        or "no channel gaps detected",
        "boundary_summary": " | ".join(CANONICAL_BOUNDARY_STATEMENTS),
    }
    artifact_path_map = {
        "multi_source_stability_evidence": str(
            dict(artifact_paths or {}).get("multi_source_stability_evidence")
            or MULTI_SOURCE_STABILITY_EVIDENCE_FILENAME
        ),
        "multi_source_stability_evidence_markdown": str(
            dict(artifact_paths or {}).get("multi_source_stability_evidence_markdown")
            or MULTI_SOURCE_STABILITY_EVIDENCE_MARKDOWN_FILENAME
        ),
    }
    review_surface = {
        "title_text": "Multi-source Stability Evidence",
        "role_text": "diagnostic_analysis",
        "reviewer_note": (
            "Step 2 tail / Stage 3 bridge reviewer evidence. "
            "Shadow evaluation only and does not modify live sampling gate by default."
        ),
        "summary_text": digest["summary"],
        "summary_lines": [
            digest["summary"],
            f"policy versions: {digest['policy_summary']}",
            f"decision summary: {digest['decision_summary']}",
            f"coverage: {digest['coverage_summary']}",
        ],
        "detail_lines": [
            f"route families: {', '.join(routes) or '--'}",
            f"phase policies: {', '.join(phases) or '--'}",
            f"missing channels: {digest['gap_summary']}",
            *[f"boundary: {line}" for line in CANONICAL_BOUNDARY_STATEMENTS],
        ],
        "anchor_id": "multi-source-stability-evidence",
        "anchor_label": "Multi-source stability evidence",
        "phase_filters": phases,
        "route_filters": routes,
        "signal_family_filters": signal_family_filters,
        "decision_result_filters": decision_result_filters,
        "policy_version_filters": policy_version_filters,
        "boundary_filters": list(CANONICAL_BOUNDARY_STATEMENTS),
        "artifact_paths": dict(artifact_path_map),
    }
    markdown = _render_markdown(
        title="Multi-source Stability Evidence",
        review_surface=review_surface,
        signal_group_coverage=signal_group_coverage,
        evaluations=evaluations,
        artifact_path_map=artifact_path_map,
    )
    raw = {
        "schema_version": "1.0",
        "artifact_type": "multi_source_stability_evidence",
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "run_id": str(run_id or ""),
        "artifact_role": "diagnostic_analysis",
        "evidence_source": "simulated",
        "not_real_acceptance_evidence": True,
        "coverage_status": coverage_status,
        "overall_status": overall_status,
        "boundary_statements": list(CANONICAL_BOUNDARY_STATEMENTS),
        "policy_versions": [asdict(item) for item in policy_versions],
        "signal_group_coverage": signal_group_coverage,
        "available_channels_by_group": available_channels,
        "missing_channels_by_group": missing_channels,
        "stability_windows": [dict(item.get("window_stats") or {}) for item in evaluations],
        "stability_decisions": [dict(item.get("decision") or {}) for item in evaluations],
        "shadow_evaluation_results": [dict(item.get("shadow_result") or {}) for item in evaluations],
        "raw_signal_snapshots": raw_signal_snapshots,
        "decision_counts": decision_counts,
        "status_counts": status_counts,
        "coverage_counts": coverage_counts,
        "digest": digest,
        "review_surface": review_surface,
        "artifact_paths": artifact_path_map,
    }
    return {
        "available": True,
        "artifact_type": "multi_source_stability_evidence",
        "filename": MULTI_SOURCE_STABILITY_EVIDENCE_FILENAME,
        "markdown_filename": MULTI_SOURCE_STABILITY_EVIDENCE_MARKDOWN_FILENAME,
        "raw": raw,
        "markdown": markdown,
        "digest": digest,
    }


def build_simulation_evidence_sidecar_bundle(
    *,
    run_id: str,
    multi_source_stability_evidence: dict[str, Any] | None,
    state_transition_evidence: dict[str, Any] | None,
    artifact_paths: dict[str, Any] | None = None,
) -> dict[str, Any]:
    stability_raw = dict((multi_source_stability_evidence or {}).get("raw") or {})
    transition_raw = dict((state_transition_evidence or {}).get("raw") or {})
    paths = {
        "simulation_evidence_sidecar_bundle": str(
            dict(artifact_paths or {}).get("simulation_evidence_sidecar_bundle")
            or SIMULATION_EVIDENCE_SIDECAR_BUNDLE_FILENAME
        ),
        "multi_source_stability_evidence": str(
            dict(artifact_paths or {}).get("multi_source_stability_evidence")
            or MULTI_SOURCE_STABILITY_EVIDENCE_FILENAME
        ),
        "state_transition_evidence": str(
            dict(artifact_paths or {}).get("state_transition_evidence")
            or "state_transition_evidence.json"
        ),
    }
    return {
        "schema_version": "1.0",
        "artifact_type": "simulation_evidence_sidecar_bundle",
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "run_id": str(run_id or ""),
        "title_text": "Simulation Evidence Sidecar Bundle",
        "reviewer_note": (
            "Step 2 tail / Stage 3 bridge sidecar-ready contract. "
            "Future database intake only, not the primary evidence chain."
        ),
        "boundary_statements": [
            *CANONICAL_BOUNDARY_STATEMENTS,
            "future database intake / sidecar-ready",
            "not the primary evidence chain",
        ],
        "artifact_paths": paths,
        "stores": {
            "stability_policy_versions": list(stability_raw.get("policy_versions") or []),
            "stability_windows": list(stability_raw.get("stability_windows") or []),
            "stability_decisions": list(stability_raw.get("stability_decisions") or []),
            "raw_signal_snapshots": list(stability_raw.get("raw_signal_snapshots") or []),
            "shadow_evaluation_results": list(stability_raw.get("shadow_evaluation_results") or []),
            "state_transition_logs": list(transition_raw.get("state_transition_logs") or []),
            "phase_decision_logs": list(transition_raw.get("phase_decision_logs") or []),
        },
        "artifact_refs": {
            "multi_source_stability_evidence": paths["multi_source_stability_evidence"],
            "state_transition_evidence": paths["state_transition_evidence"],
        },
        "digest": {
            "summary": (
                "sidecar-ready contract only | future database intake | "
                "simulation / offline / headless only"
            ),
            "stability_windows": len(list(stability_raw.get("stability_windows") or [])),
            "transition_logs": len(list(transition_raw.get("state_transition_logs") or [])),
        },
    }


def _build_windows(
    samples: list[SamplingResult],
    point_summaries: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    summaries_by_index = {
        int(dict(item.get("point") or {}).get("index", 0) or 0): dict(item)
        for item in point_summaries
        if int(dict(item.get("point") or {}).get("index", 0) or 0) > 0
    }
    windows_by_key: dict[tuple[int, str, str, str], dict[str, Any]] = {}
    for sample in samples:
        route_family = _route_family_for_sample(sample)
        phase_policy = _phase_policy_for_sample(sample)
        point_index = int(getattr(getattr(sample, "point", None), "index", 0) or 0)
        point_tag = str(getattr(sample, "point_tag", "") or getattr(getattr(sample, "point", None), "route", "") or "--")
        key = (point_index, route_family, phase_policy, point_tag)
        window = windows_by_key.setdefault(
            key,
            {
                "window_id": f"{route_family}-{phase_policy}-{point_index or len(windows_by_key) + 1}",
                "point_index": point_index,
                "route_family": route_family,
                "phase_policy": phase_policy,
                "point_tag": point_tag,
                "samples": [],
                "point_summary": dict(summaries_by_index.get(point_index) or {}),
            },
        )
        window["samples"].append(sample)
    if windows_by_key:
        return [dict(item) for item in windows_by_key.values()]

    windows: list[dict[str, Any]] = []
    for index, summary in enumerate(point_summaries, start=1):
        point_payload = dict(summary.get("point") or {})
        route_text = str(point_payload.get("route") or "").strip().lower()
        pressure_mode = str(point_payload.get("pressure_mode") or "").strip().lower()
        route_family = _route_family(route_text, pressure_mode=pressure_mode)
        phase_policy = _phase_policy_from_text(str(dict(summary.get("stats") or {}).get("point_phase") or "sample_ready"))
        point_index = int(point_payload.get("index", index) or index)
        windows.append(
            {
                "window_id": f"{route_family}-{phase_policy}-{point_index}",
                "point_index": point_index,
                "route_family": route_family,
                "phase_policy": phase_policy,
                "point_tag": str(point_payload.get("pressure_target_label") or point_payload.get("route") or "--"),
                "samples": [],
                "point_summary": dict(summary),
            }
        )
    return windows


def _policy_versions_for_windows(windows: list[dict[str, Any]]) -> list[StabilityPolicyVersion]:
    rows: list[StabilityPolicyVersion] = []
    seen: set[tuple[str, str]] = set()
    for window in windows:
        route_family = str(window.get("route_family") or "gas")
        phase_policy = str(window.get("phase_policy") or "sample_ready")
        key = (route_family, phase_policy)
        if key in seen:
            continue
        seen.add(key)
        template = dict(ROUTE_POLICY_TEMPLATES.get(route_family) or ROUTE_POLICY_TEMPLATES["gas"])
        rows.append(
            StabilityPolicyVersion(
                route_family=route_family,
                phase_policy=phase_policy,
                policy_version=str(template.get("policy_version") or f"shadow_{route_family}_v1"),
                min_valid_ratio=float(template.get("min_valid_ratio", 0.8) or 0.8),
                max_freshness_gap_s=float(template.get("max_freshness_gap_s", 10.0) or 10.0),
                min_hold_time_s=float(template.get("min_hold_time_s", 10.0) or 10.0),
                pass_score=float(template.get("pass_score", 0.8) or 0.8),
                warn_score=float(template.get("warn_score", 0.65) or 0.65),
                required_signal_groups=tuple(SIGNAL_GROUP_ORDER),
            )
        )
    return rows


def _evaluate_window(window: dict[str, Any]) -> dict[str, Any]:
    samples = [sample for sample in list(window.get("samples") or []) if isinstance(sample, SamplingResult)]
    point_summary = dict(window.get("point_summary") or {})
    route_family = str(window.get("route_family") or "gas")
    phase_policy = str(window.get("phase_policy") or "sample_ready")
    template = dict(ROUTE_POLICY_TEMPLATES.get(route_family) or ROUTE_POLICY_TEMPLATES["gas"])
    required_channels = {
        key: tuple(value)
        for key, value in dict(template.get("required_channels") or {}).items()
    }
    available_channels_by_group = {
        group_name: _available_channels(samples, SIGNAL_GROUP_CHANNELS[group_name])
        for group_name in SIGNAL_GROUP_ORDER
    }
    missing_channels_by_group = {
        group_name: [
            channel
            for channel in required_channels.get(group_name, ())
            if channel not in set(available_channels_by_group.get(group_name) or [])
        ]
        for group_name in SIGNAL_GROUP_ORDER
    }
    group_scores = {
        "reference": _group_score(samples, required_channels.get("reference", ()), kind="reference"),
        "analyzer_raw": _group_score(samples, required_channels.get("analyzer_raw", ()), kind="analyzer_raw"),
        "output": _group_score(samples, required_channels.get("output", ()), kind="output"),
        "data_quality": _data_quality_score(samples, point_summary=point_summary),
    }
    weighted_score = _weighted_score(group_scores, dict(template.get("score_weights") or {}))
    valid_ratio = _valid_ratio(samples)
    hold_time_observed_s = _hold_time_observed(samples, point_summary=point_summary)
    freshness_gap_s = _freshness_gap_s(samples)
    hard_gates = [
        {
            "gate_id": "data_quality_ratio",
            "label": "usable frame ratio",
            "passed": valid_ratio >= float(template.get("min_valid_ratio", 0.8) or 0.8),
            "actual": round(valid_ratio, 4),
            "required": float(template.get("min_valid_ratio", 0.8) or 0.8),
        },
        {
            "gate_id": "freshness_gap",
            "label": "frame freshness gap",
            "passed": freshness_gap_s is None or freshness_gap_s <= float(template.get("max_freshness_gap_s", 10.0) or 10.0),
            "actual": freshness_gap_s,
            "required": float(template.get("max_freshness_gap_s", 10.0) or 10.0),
        },
        {
            "gate_id": "required_channel_presence",
            "label": "required channel presence",
            "passed": not any(missing_channels_by_group.values()),
            "actual": sum(len(list(item or [])) for item in missing_channels_by_group.values()),
            "required": 0,
        },
    ]
    hard_gate_passed = all(bool(item.get("passed", False)) for item in hard_gates)
    hold_time_met = hold_time_observed_s >= float(template.get("min_hold_time_s", 10.0) or 10.0)
    coverage_status = _coverage_status_from_window(
        available_channels_by_group=available_channels_by_group,
        missing_channels_by_group=missing_channels_by_group,
    )
    partial_coverage = coverage_status != "complete"
    if not samples:
        decision_result = "shadow_no_samples"
        status = "diagnostic_only"
    elif hard_gate_passed and hold_time_met and weighted_score >= float(template.get("pass_score", 0.8) or 0.8):
        decision_result = "stable_shadow_pass"
        status = "passed"
    elif partial_coverage:
        decision_result = "partial_coverage_gap"
        status = "degraded"
    elif not hold_time_met:
        decision_result = "hold_time_gap"
        status = "degraded"
    elif not hard_gate_passed:
        decision_result = "hard_gate_gap"
        status = "degraded"
    else:
        decision_result = "shadow_monitor"
        status = "diagnostic_only"
    policy_version = str(template.get("policy_version") or f"shadow_{route_family}_v1")
    window_stats = StabilityWindowStats(
        window_id=str(window.get("window_id") or ""),
        point_index=int(window.get("point_index", 0) or 0),
        route_family=route_family,
        phase_policy=phase_policy,
        sample_count=len(samples),
        valid_ratio=round(valid_ratio, 4),
        hold_time_observed_s=round(hold_time_observed_s, 3),
        freshness_gap_s=None if freshness_gap_s is None else round(freshness_gap_s, 3),
        coverage_status=coverage_status,
    )
    decision = StabilityDecisionResult(
        window_id=window_stats.window_id,
        route_family=route_family,
        phase_policy=phase_policy,
        policy_version=policy_version,
        decision_result=decision_result,
        status=status,
        weighted_score=round(weighted_score, 4),
        hold_time_met=hold_time_met,
        hard_gate_passed=hard_gate_passed,
        partial_coverage=partial_coverage,
    )
    return {
        "window_id": window_stats.window_id,
        "route_family": route_family,
        "phase_policy": phase_policy,
        "policy_version": policy_version,
        "window_stats": asdict(window_stats),
        "decision": asdict(decision),
        "shadow_result": {
            "window_id": window_stats.window_id,
            "decision_result": decision_result,
            "status": status,
            "weighted_score": round(weighted_score, 4),
            "summary": (
                f"{route_family}/{phase_policy} | {decision_result} | score {weighted_score:.2f} | "
                f"hold {hold_time_observed_s:.1f}s"
            ),
        },
        "hard_gates": hard_gates,
        "group_scores": {key: round(float(value), 4) for key, value in group_scores.items()},
        "available_channels_by_group": available_channels_by_group,
        "missing_channels_by_group": missing_channels_by_group,
        "coverage_status": coverage_status,
        "status": status,
        "decision_result": decision_result,
        "sample_count": len(samples),
    }


def _group_score(samples: list[SamplingResult], required_channels: tuple[str, ...], *, kind: str) -> float:
    if not required_channels:
        return 1.0
    available_channels = _available_channels(samples, required_channels)
    coverage_ratio = 0.0 if not required_channels else len(available_channels) / len(required_channels)
    if not available_channels:
        return 0.0
    numeric_scores: list[float] = []
    for channel in available_channels:
        numeric_values = [
            value
            for value in (_coerce_float(getattr(sample, channel, None)) for sample in samples)
            if value is not None
        ]
        if not numeric_values:
            numeric_scores.append(1.0)
            continue
        if len(numeric_values) == 1:
            numeric_scores.append(0.95)
            continue
        mean_value = abs(statistics.fmean(numeric_values))
        if mean_value <= 1e-9:
            spread_ratio = abs(max(numeric_values) - min(numeric_values))
        else:
            spread_ratio = abs(max(numeric_values) - min(numeric_values)) / max(mean_value, 1e-9)
        threshold = 0.08 if kind == "analyzer_raw" else 0.05 if kind == "output" else 0.03
        numeric_scores.append(max(0.0, 1.0 - min(spread_ratio / max(threshold, 1e-9), 1.0)))
    return round((coverage_ratio * 0.55) + ((statistics.fmean(numeric_scores) if numeric_scores else 0.0) * 0.45), 4)


def _data_quality_score(samples: list[SamplingResult], *, point_summary: dict[str, Any]) -> float:
    if not samples:
        return 0.0
    valid_ratio = _valid_ratio(samples)
    freshness_gap_s = _freshness_gap_s(samples)
    jump_penalty = 0.0
    status_penalty = 0.0
    for channel, threshold in (
        ("co2_ratio_raw", 0.05),
        ("h2o_ratio_raw", 0.05),
        ("co2_ppm", 25.0),
        ("h2o_mmol", 0.25),
        ("pressure_hpa", 25.0),
    ):
        numeric_values = [
            value
            for value in (_coerce_float(getattr(sample, channel, None)) for sample in samples)
            if value is not None
        ]
        if len(numeric_values) < 2:
            continue
        if abs(max(numeric_values) - min(numeric_values)) > float(threshold):
            jump_penalty += 0.08
    if any(not bool(getattr(sample, "frame_has_data", True)) for sample in samples):
        status_penalty += 0.08
    if any(str(getattr(sample, "frame_status", "") or "").strip() for sample in samples):
        status_penalty += 0.04
    stats_payload = dict(point_summary.get("stats") or {})
    if bool(stats_payload.get("postseal_timeout_blocked")) or bool(stats_payload.get("dewpoint_rebound_detected")):
        status_penalty += 0.08
    freshness_score = 1.0
    if freshness_gap_s is not None and freshness_gap_s > 0:
        freshness_score = max(0.0, 1.0 - min(freshness_gap_s / 20.0, 1.0))
    return max(
        0.0,
        min(1.0, (valid_ratio * 0.55) + (freshness_score * 0.35) + (0.10 - jump_penalty - status_penalty)),
    )


def _weighted_score(group_scores: dict[str, float], weights: dict[str, Any]) -> float:
    total = 0.0
    weight_total = 0.0
    for group_name in SIGNAL_GROUP_ORDER:
        weight = float(weights.get(group_name, 0.0) or 0.0)
        total += float(group_scores.get(group_name, 0.0) or 0.0) * weight
        weight_total += weight
    if weight_total <= 0:
        return 0.0
    return total / weight_total


def _aggregate_signal_group_coverage(evaluations: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    rows: dict[str, dict[str, Any]] = {}
    for group_name in SIGNAL_GROUP_ORDER:
        available_union = _dedupe(
            channel
            for item in evaluations
            for channel in list(dict(item.get("available_channels_by_group") or {}).get(group_name, []) or [])
        )
        missing_union = _dedupe(
            channel
            for item in evaluations
            for channel in list(dict(item.get("missing_channels_by_group") or {}).get(group_name, []) or [])
        )
        rows[group_name] = {
            "window_count": sum(
                1
                for item in evaluations
                if dict(item.get("available_channels_by_group") or {}).get(group_name)
                or dict(item.get("missing_channels_by_group") or {}).get(group_name)
            ),
            "available_channels": available_union,
            "missing_channels": missing_union,
            "coverage_status": _coverage_status_from_lists(available_union, missing_union),
        }
    return rows


def _snapshot_from_evaluation(evaluation: dict[str, Any]) -> dict[str, Any]:
    analyzer_channels = dict(evaluation.get("available_channels_by_group") or {}).get("analyzer_raw", [])
    output_channels = dict(evaluation.get("available_channels_by_group") or {}).get("output", [])
    return {
        "window_id": str(evaluation.get("window_id") or ""),
        "route_family": str(evaluation.get("route_family") or ""),
        "phase_policy": str(evaluation.get("phase_policy") or ""),
        "policy_version": str(evaluation.get("policy_version") or ""),
        "available_analyzer_raw_channels": list(analyzer_channels or []),
        "available_output_channels": list(output_channels or []),
        "decision_result": str(evaluation.get("decision_result") or ""),
    }


def _render_markdown(
    *,
    title: str,
    review_surface: dict[str, Any],
    signal_group_coverage: dict[str, dict[str, Any]],
    evaluations: list[dict[str, Any]],
    artifact_path_map: dict[str, str],
) -> str:
    lines = [
        f"# {title}",
        "",
        f"- title: {title}",
        f"- role: {review_surface.get('role_text', '--')}",
        f"- reviewer_note: {review_surface.get('reviewer_note', '--')}",
        "",
        "## Boundary",
        "",
        *[f"- {line}" for line in CANONICAL_BOUNDARY_STATEMENTS],
        "",
        "## Signal Group Coverage",
        "",
    ]
    for group_name in SIGNAL_GROUP_ORDER:
        coverage = dict(signal_group_coverage.get(group_name) or {})
        lines.append(
            f"- {group_name}: {coverage.get('coverage_status', 'missing')} | "
            f"available {', '.join(list(coverage.get('available_channels') or [])[:5]) or '--'} | "
            f"missing {', '.join(list(coverage.get('missing_channels') or [])[:5]) or '--'}"
        )
    lines.extend(["", "## Decisions", ""])
    for evaluation in evaluations:
        decision = dict(evaluation.get("decision") or {})
        lines.append(
            f"- {decision.get('route_family', '--')}/{decision.get('phase_policy', '--')}: "
            f"{decision.get('decision_result', '--')} | "
            f"score {float(decision.get('weighted_score', 0.0) or 0.0):.2f} | "
            f"hold {decision.get('hold_time_met', False)} | "
            f"partial_coverage {decision.get('partial_coverage', False)}"
        )
    lines.extend(
        [
            "",
            "## Artifact Paths",
            "",
            f"- json: {artifact_path_map.get('multi_source_stability_evidence', '--')}",
            f"- markdown: {artifact_path_map.get('multi_source_stability_evidence_markdown', '--')}",
            "",
        ]
    )
    return "\n".join(lines).strip() + "\n"


def _route_family_for_sample(sample: SamplingResult) -> str:
    route_text = str(getattr(getattr(sample, "point", None), "route", "") or "").strip().lower()
    pressure_mode = str(getattr(getattr(sample, "point", None), "effective_pressure_mode", "") or "").strip().lower()
    phase_text = str(getattr(sample, "point_phase", "") or "").strip().lower()
    return _route_family(route_text, pressure_mode=pressure_mode, phase_text=phase_text)


def _route_family(route_text: str, *, pressure_mode: str = "", phase_text: str = "") -> str:
    route = str(route_text or "").strip().lower()
    pressure = str(pressure_mode or "").strip().lower()
    phase = str(phase_text or "").strip().lower()
    if route in {"h2o", "water", "humidity"}:
        return "water"
    if pressure == "ambient_open" or "ambient" in route or "ambient" in phase or "diagnostic" in phase:
        return "ambient"
    return "gas"


def _phase_policy_for_sample(sample: SamplingResult) -> str:
    phase_text = str(getattr(sample, "point_phase", "") or "").strip().lower()
    if not phase_text:
        phase_text = str(getattr(getattr(sample, "point", None), "route", "") or "").strip().lower()
    return _phase_policy_from_text(phase_text)


def _phase_policy_from_text(value: str) -> str:
    text = str(value or "").strip().lower()
    if "preseal" in text or "seal" in text:
        return "preseal"
    if "pressure" in text:
        return "pressure_stable"
    return "sample_ready"


def _available_channels(samples: list[SamplingResult], channels: Iterable[str]) -> list[str]:
    available: list[str] = []
    for channel in channels:
        if any(_has_value(getattr(sample, channel, None)) for sample in samples):
            available.append(str(channel))
    return available


def _valid_ratio(samples: list[SamplingResult]) -> float:
    if not samples:
        return 0.0
    usable = sum(1 for sample in samples if bool(getattr(sample, "frame_usable", False)))
    return usable / len(samples)


def _hold_time_observed(samples: list[SamplingResult], *, point_summary: dict[str, Any]) -> float:
    values = [_coerce_float(getattr(sample, "stability_time_s", None)) for sample in samples]
    values.extend(
        _coerce_float(dict(point_summary.get("stats") or {}).get(key))
        for key in ("stability_time_s", "hold_time_s")
    )
    numeric = [value for value in values if value is not None]
    return max(numeric) if numeric else 0.0


def _freshness_gap_s(samples: list[SamplingResult]) -> float | None:
    timestamps = sorted(
        sample.timestamp.timestamp()
        for sample in samples
        if isinstance(getattr(sample, "timestamp", None), datetime)
    )
    if len(timestamps) < 2:
        return 0.0 if timestamps else None
    gaps = [later - earlier for earlier, later in zip(timestamps, timestamps[1:])]
    return max(gaps) if gaps else 0.0


def _coverage_status_from_window(
    *,
    available_channels_by_group: dict[str, list[str]],
    missing_channels_by_group: dict[str, list[str]],
) -> str:
    statuses = [
        _coverage_status_from_lists(
            list(available_channels_by_group.get(group_name) or []),
            list(missing_channels_by_group.get(group_name) or []),
        )
        for group_name in SIGNAL_GROUP_ORDER
    ]
    if statuses and all(status == "complete" for status in statuses):
        return "complete"
    if any(status == "partial" for status in statuses):
        return "partial"
    if any(status == "complete" for status in statuses):
        return "partial"
    return "missing"


def _coverage_status_from_groups(signal_group_coverage: dict[str, dict[str, Any]]) -> str:
    statuses = [
        str(dict(signal_group_coverage.get(group_name) or {}).get("coverage_status") or "missing")
        for group_name in SIGNAL_GROUP_ORDER
    ]
    if statuses and all(status == "complete" for status in statuses):
        return "complete"
    if any(status == "partial" for status in statuses):
        return "partial"
    if any(status == "complete" for status in statuses):
        return "partial"
    return "missing"


def _coverage_status_from_lists(available_channels: list[str], missing_channels: list[str]) -> str:
    if available_channels and not missing_channels:
        return "complete"
    if available_channels:
        return "partial"
    return "missing"


def _overall_status_from_evaluations(evaluations: list[dict[str, Any]]) -> str:
    statuses = [str(item.get("status") or "") for item in evaluations]
    if any(status == "degraded" for status in statuses):
        return "degraded"
    if any(status == "diagnostic_only" for status in statuses):
        return "diagnostic_only"
    if all(status == "passed" for status in statuses) and statuses:
        return "passed"
    return "diagnostic_only"


def _count_by_key(rows: Iterable[dict[str, Any]], key_name: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        key = str(dict(row or {}).get(key_name) or "").strip()
        if not key:
            continue
        counts[key] = int(counts.get(key, 0)) + 1
    return counts


def _dedupe(values: Iterable[Any]) -> list[str]:
    rows: list[str] = []
    for value in values:
        text = str(value or "").strip()
        if text and text not in rows:
            rows.append(text)
    return rows


def _coerce_float(value: Any) -> float | None:
    if value in (None, "", False):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _has_value(value: Any) -> bool:
    if value in (None, "", [], {}):
        return False
    return True
