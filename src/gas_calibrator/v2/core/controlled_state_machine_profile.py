from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Iterable

from .models import CalibrationPoint, SamplingResult

if TYPE_CHECKING:
    from .plan_compiler import CompiledPlan


STATE_TRANSITION_EVIDENCE_FILENAME = "state_transition_evidence.json"
STATE_TRANSITION_EVIDENCE_MARKDOWN_FILENAME = "state_transition_evidence.md"
CONTROLLED_STATE_MACHINE_PROFILE_VERSION = "controlled_flex_v2"
TRANSITION_POLICY_PROFILE_VERSION = "transition_policy_profile_v2"
TRANSITION_FEATURE_SET_VERSION = "controlled_state_machine.step2_offline_v2"

CANONICAL_STATES = (
    "INIT",
    "DEVICE_READY",
    "PLAN_COMPILED",
    "TEMP_SOAK",
    "ROUTE_FLUSH",
    "PRESEAL_STABILITY",
    "SEAL",
    "PRESSURE_HANDOFF",
    "PRESSURE_STABLE",
    "RAW_SIGNAL_STABLE",
    "OUTPUT_STABLE",
    "SAMPLE_WINDOW",
    "POINT_COMPLETE",
    "NEXT_POINT",
    "NEXT_ROUTE",
    "NEXT_TEMP",
    "RUN_COMPLETE",
    "FAULT_CAPTURE",
    "SAFE_RECOVERY",
    "ABORT",
)

ALLOWED_TRANSITIONS: dict[str, tuple[str, ...]] = {
    "INIT": ("DEVICE_READY", "ABORT"),
    "DEVICE_READY": ("PLAN_COMPILED", "FAULT_CAPTURE", "ABORT"),
    "PLAN_COMPILED": ("TEMP_SOAK", "FAULT_CAPTURE", "ABORT"),
    "TEMP_SOAK": ("ROUTE_FLUSH", "FAULT_CAPTURE", "ABORT"),
    "ROUTE_FLUSH": ("PRESEAL_STABILITY", "SEAL", "RAW_SIGNAL_STABLE", "FAULT_CAPTURE", "ABORT"),
    "PRESEAL_STABILITY": ("SEAL", "FAULT_CAPTURE", "ABORT"),
    "SEAL": ("PRESSURE_HANDOFF", "FAULT_CAPTURE", "ABORT"),
    "PRESSURE_HANDOFF": ("PRESSURE_STABLE", "FAULT_CAPTURE", "ABORT"),
    "PRESSURE_STABLE": ("RAW_SIGNAL_STABLE", "FAULT_CAPTURE", "ABORT"),
    "RAW_SIGNAL_STABLE": ("OUTPUT_STABLE", "FAULT_CAPTURE", "ABORT"),
    "OUTPUT_STABLE": ("SAMPLE_WINDOW", "FAULT_CAPTURE", "ABORT"),
    "SAMPLE_WINDOW": ("POINT_COMPLETE", "FAULT_CAPTURE", "ABORT"),
    "POINT_COMPLETE": ("NEXT_POINT", "NEXT_ROUTE", "NEXT_TEMP", "RUN_COMPLETE", "FAULT_CAPTURE", "ABORT"),
    "NEXT_POINT": ("TEMP_SOAK", "ROUTE_FLUSH", "RUN_COMPLETE", "ABORT"),
    "NEXT_ROUTE": ("TEMP_SOAK", "ROUTE_FLUSH", "RUN_COMPLETE", "ABORT"),
    "NEXT_TEMP": ("TEMP_SOAK", "RUN_COMPLETE", "ABORT"),
    "FAULT_CAPTURE": ("SAFE_RECOVERY", "ABORT"),
    "SAFE_RECOVERY": ("TEMP_SOAK", "ROUTE_FLUSH", "RUN_COMPLETE", "ABORT"),
    "RUN_COMPLETE": (),
    "ABORT": (),
}

CANONICAL_BOUNDARY_STATEMENTS = [
    "Step 2 tail / Stage 3 bridge",
    "simulation / offline / headless only",
    "not real acceptance",
    "cannot replace real metrology validation",
    "shadow evaluation only",
    "does not modify live sampling gate by default",
]

STEP2_REVIEWER_FLAGS = {
    "reviewer_only": True,
    "readiness_mapping_only": True,
    "not_real_acceptance_evidence": True,
    "not_ready_for_formal_claim": True,
    "evidence_source": "simulated",
    "evidence_state": "shadow_only",
}

STATE_GROUPS = {
    "INIT": "bootstrap",
    "DEVICE_READY": "bootstrap",
    "PLAN_COMPILED": "bootstrap",
    "TEMP_SOAK": "conditioning",
    "ROUTE_FLUSH": "conditioning",
    "PRESEAL_STABILITY": "conditioning",
    "SEAL": "pressure_control",
    "PRESSURE_HANDOFF": "pressure_control",
    "PRESSURE_STABLE": "pressure_control",
    "RAW_SIGNAL_STABLE": "stability",
    "OUTPUT_STABLE": "stability",
    "SAMPLE_WINDOW": "sampling",
    "POINT_COMPLETE": "completion",
    "NEXT_POINT": "routing",
    "NEXT_ROUTE": "routing",
    "NEXT_TEMP": "routing",
    "RUN_COMPLETE": "terminal",
    "FAULT_CAPTURE": "recovery",
    "SAFE_RECOVERY": "recovery",
    "ABORT": "terminal",
}

TERMINAL_STATES = {"RUN_COMPLETE", "ABORT"}
DIAGNOSTIC_ONLY_STATES = {"FAULT_CAPTURE", "SAFE_RECOVERY", "ABORT"}


@dataclass(frozen=True)
class ControlledStateMachineProfile:
    profile_version: str
    enabled_states: tuple[str, ...]
    skipped_states: tuple[str, ...]
    allowed_transitions: dict[str, tuple[str, ...]]
    route_families: tuple[str, ...]


@dataclass(frozen=True)
class StateTransitionEvent:
    sequence: int
    from_state: str
    to_state: str
    reason: str
    point_index: int
    route_family: str
    recovery_marker: str


def compile_controlled_state_machine_profile(
    compiled_plan: CompiledPlan,
    *,
    profile_version: str = CONTROLLED_STATE_MACHINE_PROFILE_VERSION,
    policy_profile: dict[str, Any] | None = None,
) -> dict[str, Any]:
    points = list(compiled_plan.preview_points or compiled_plan.points or [])
    payload = _compile_controlled_layers(
        points=points,
        profile_version=profile_version,
        policy_profile=policy_profile,
    )
    payload["metadata"] = {
        "profile_name": compiled_plan.profile_name,
        "runtime_row_count": len(list(compiled_plan.runtime_rows or [])),
        "preview_point_count": len(points),
        "plan_profile_version": str(dict(compiled_plan.metadata or {}).get("profile_version") or "1.0"),
    }
    return payload


def validate_transition(from_state: str, to_state: str) -> None:
    allowed = set(ALLOWED_TRANSITIONS.get(str(from_state or ""), ()))
    if str(to_state or "") not in allowed:
        raise ValueError(f"illegal transition: {from_state} -> {to_state}")


def _compile_controlled_layers(
    *,
    points: list[CalibrationPoint],
    profile_version: str = CONTROLLED_STATE_MACHINE_PROFILE_VERSION,
    policy_profile: dict[str, Any] | None = None,
) -> dict[str, Any]:
    route_families = _route_families_from_points(points)
    enabled_states = _enabled_states_from_points(points)
    skipped_states = [state for state in CANONICAL_STATES if state not in set(enabled_states)]
    profile = ControlledStateMachineProfile(
        profile_version=profile_version,
        enabled_states=tuple(enabled_states),
        skipped_states=tuple(skipped_states),
        allowed_transitions={key: tuple(value) for key, value in ALLOWED_TRANSITIONS.items()},
        route_families=tuple(route_families),
    )
    transition_policy_profile = _build_transition_policy_profile(
        route_families=route_families,
        enabled_states=enabled_states,
        profile_version=profile_version,
        overrides=policy_profile,
    )
    compiled_route_state_graph = _build_compiled_route_state_graph(
        points=points,
        enabled_states=enabled_states,
        transition_policy_profile=transition_policy_profile,
    )
    point_profiles = [
        {
            "point_index": int(getattr(point, "index", 0) or 0),
            "route_family": _route_family_from_point(point),
            "pressure_mode": str(
                getattr(point, "effective_pressure_mode", "")
                or getattr(point, "pressure_mode", "")
                or ""
            ),
            "active_state_path": _active_state_path_for_point(point),
        }
        for point in points
    ]
    return {
        "profile_version": profile.profile_version,
        "enabled_states": list(profile.enabled_states),
        "skipped_states": list(profile.skipped_states),
        "allowed_transitions": {key: list(value) for key, value in profile.allowed_transitions.items()},
        "route_families": list(profile.route_families),
        "point_profiles": point_profiles,
        "state_library": _build_state_library(route_families, enabled_states),
        "policy_profile": dict(transition_policy_profile.get("policy_profile") or {}),
        "compiled_route_state_graph": compiled_route_state_graph,
        "transition_policy_profile": transition_policy_profile,
    }


def _enabled_states_from_points(points: list[CalibrationPoint]) -> list[str]:
    requires_pressure_states = any(
        not bool(getattr(point, "is_ambient_pressure_point", False))
        for point in points
    ) if points else True
    requires_preseal = any(_route_family_from_point(point) == "water" for point in points)
    enabled_states = ["INIT", "DEVICE_READY", "PLAN_COMPILED", "TEMP_SOAK", "ROUTE_FLUSH"]
    if requires_preseal:
        enabled_states.append("PRESEAL_STABILITY")
    if requires_pressure_states:
        enabled_states.extend(["SEAL", "PRESSURE_HANDOFF", "PRESSURE_STABLE"])
    enabled_states.extend(
        [
            "RAW_SIGNAL_STABLE",
            "OUTPUT_STABLE",
            "SAMPLE_WINDOW",
            "POINT_COMPLETE",
            "NEXT_POINT",
            "NEXT_ROUTE",
            "NEXT_TEMP",
            "RUN_COMPLETE",
            "FAULT_CAPTURE",
            "SAFE_RECOVERY",
            "ABORT",
        ]
    )
    enabled_set = set(enabled_states)
    return [state for state in CANONICAL_STATES if state in enabled_set]


def _build_state_library(route_families: list[str], enabled_states: list[str]) -> dict[str, Any]:
    return {
        "artifact_type": "state_library",
        "schema_version": "2.0",
        "canonical_states": [
            {
                "state_id": state,
                "state_group": STATE_GROUPS.get(state, "runtime"),
                "display_name": state.replace("_", " ").title(),
                "enabled": state in set(enabled_states),
                "terminal": state in TERMINAL_STATES,
                "diagnostic_only": state in DIAGNOSTIC_ONLY_STATES,
                "route_families": list(route_families or ["gas"]),
            }
            for state in CANONICAL_STATES
        ],
        "allowed_transition_rows": [
            {
                "from_state": from_state,
                "to_state": to_state,
                "transition_kind": _transition_kind(from_state, to_state),
            }
            for from_state, next_states in ALLOWED_TRANSITIONS.items()
            for to_state in next_states
        ],
        "terminal_states": sorted(TERMINAL_STATES),
        "diagnostic_only_states": sorted(DIAGNOSTIC_ONLY_STATES),
        "summary_line": (
            f"canonical states {len(CANONICAL_STATES)} | enabled {len(enabled_states)} | "
            f"allowed transitions {sum(len(value) for value in ALLOWED_TRANSITIONS.values())}"
        ),
        **STEP2_REVIEWER_FLAGS,
    }


def _build_transition_policy_profile(
    *,
    route_families: list[str],
    enabled_states: list[str],
    profile_version: str,
    overrides: dict[str, Any] | None = None,
) -> dict[str, Any]:
    policy_profile = {
        "profile_version": TRANSITION_POLICY_PROFILE_VERSION,
        "controlled_state_machine_profile_version": profile_version,
        "feature_set_version": TRANSITION_FEATURE_SET_VERSION,
        "state_library_version": "canonical_state_library_v1",
        "retry_policy": {
            "default_retry_limit": 1,
            "by_state": {
                "PRESEAL_STABILITY": 2,
                "PRESSURE_STABLE": 2,
                "RAW_SIGNAL_STABLE": 2,
                "OUTPUT_STABLE": 1,
                "SAMPLE_WINDOW": 1,
            },
        },
        "rollback_policy": {"default_rollback_state": "ROUTE_FLUSH"},
        "skip_policy": {"default_skip_allowed": False},
        "diagnostic_only_policy": {"states": sorted(DIAGNOSTIC_ONLY_STATES)},
        "route_family_scope": list(route_families or ["gas"]),
        "enabled_states": list(enabled_states),
    }
    for key, value in dict(overrides or {}).items():
        policy_profile[key] = value
    return {
        "artifact_type": "transition_policy_profile",
        "policy_version": str(policy_profile.get("profile_version") or TRANSITION_POLICY_PROFILE_VERSION),
        "feature_set_version": str(policy_profile.get("feature_set_version") or TRANSITION_FEATURE_SET_VERSION),
        "policy_profile": policy_profile,
        "summary_line": (
            f"retry {len(dict(policy_profile.get('retry_policy') or {}).get('by_state', {}))} | "
            f"routes {len(list(policy_profile.get('route_family_scope') or []))} | "
            f"diagnostic {len(list(dict(policy_profile.get('diagnostic_only_policy') or {}).get('states') or []))}"
        ),
        **STEP2_REVIEWER_FLAGS,
    }


def _build_compiled_route_state_graph(
    *,
    points: list[CalibrationPoint],
    enabled_states: list[str],
    transition_policy_profile: dict[str, Any],
) -> dict[str, Any]:
    retry_policy = dict(dict(transition_policy_profile.get("policy_profile") or {}).get("retry_policy") or {})
    retry_by_state = dict(retry_policy.get("by_state") or {})
    enabled_set = set(enabled_states)
    edges = [
        {
            "from_state": from_state,
            "to_state": to_state,
            "transition_kind": _transition_kind(from_state, to_state),
            "enabled_for_profile": True,
            "retry_limit": int(retry_by_state.get(to_state, retry_policy.get("default_retry_limit", 0)) or 0),
            "rollback_state": "ROUTE_FLUSH",
            "skip_allowed": False,
            "diagnostic_only": to_state in DIAGNOSTIC_ONLY_STATES,
        }
        for from_state, next_states in ALLOWED_TRANSITIONS.items()
        for to_state in next_states
        if from_state in enabled_set and to_state in enabled_set
    ]
    route_paths: list[dict[str, Any]] = []
    for route_family in _route_families_from_points(points):
        exemplar = next((point for point in points if _route_family_from_point(point) == route_family), None)
        compiled_path = _dedupe(
            [
                *_active_state_path_for_point(exemplar),
                "RAW_SIGNAL_STABLE",
                "OUTPUT_STABLE",
                "SAMPLE_WINDOW",
                "POINT_COMPLETE",
                "RUN_COMPLETE",
            ]
        )
        route_paths.append(
            {
                "route_family": route_family,
                "compiled_state_path": compiled_path,
                "graph_nodes": len(compiled_path),
            }
        )
    if not route_paths:
        route_paths.append(
            {
                "route_family": "gas",
                "compiled_state_path": [
                    "TEMP_SOAK",
                    "ROUTE_FLUSH",
                    "SEAL",
                    "PRESSURE_HANDOFF",
                    "PRESSURE_STABLE",
                    "RAW_SIGNAL_STABLE",
                    "OUTPUT_STABLE",
                    "SAMPLE_WINDOW",
                    "POINT_COMPLETE",
                    "RUN_COMPLETE",
                ],
                "graph_nodes": 10,
            }
        )
    return {
        "artifact_type": "compiled_route_state_graph",
        "graph_version": "compiled_route_state_graph_v2",
        "enabled_state_count": len(enabled_states),
        "edge_count": len(edges),
        "nodes": [
            {
                "state_id": state,
                "enabled": True,
                "state_group": STATE_GROUPS.get(state, "runtime"),
                "terminal": state in TERMINAL_STATES,
                "diagnostic_only": state in DIAGNOSTIC_ONLY_STATES,
            }
            for state in enabled_states
        ],
        "edges": edges,
        "route_paths": route_paths,
        "summary_line": (
            f"routes {len(route_paths)} | nodes {len(enabled_states)} | edges {len(edges)} | "
            f"profile {transition_policy_profile.get('policy_version', '--')}"
        ),
        **STEP2_REVIEWER_FLAGS,
    }


def _runtime_points_from_inputs(
    samples: list[SamplingResult],
    point_summaries: list[dict[str, Any]],
) -> list[CalibrationPoint]:
    points: list[CalibrationPoint] = []
    seen: set[tuple[int, str, str]] = set()
    for sample in samples:
        point = getattr(sample, "point", None)
        if not isinstance(point, CalibrationPoint):
            continue
        key = (
            int(getattr(point, "index", 0) or 0),
            str(getattr(point, "route", "") or ""),
            str(
                getattr(point, "pressure_mode", "")
                or getattr(point, "effective_pressure_mode", "")
                or ""
            ),
        )
        if key in seen:
            continue
        seen.add(key)
        points.append(point)
    for summary in point_summaries:
        point_payload = dict(summary.get("point") or {})
        key = (
            int(point_payload.get("index", 0) or 0),
            str(point_payload.get("route") or ""),
            str(point_payload.get("pressure_mode") or ""),
        )
        if key in seen or key[0] <= 0:
            continue
        seen.add(key)
        points.append(
            CalibrationPoint(
                index=key[0],
                temperature_c=25.0,
                co2_ppm=_coerce_float(point_payload.get("co2_ppm")),
                humidity_pct=_coerce_float(point_payload.get("humidity_pct")),
                pressure_hpa=_coerce_float(point_payload.get("pressure_hpa")),
                route=key[1],
                pressure_mode=key[2],
                pressure_target_label=str(point_payload.get("pressure_target_label") or "") or None,
                pressure_selection_token=str(point_payload.get("pressure_selection_token") or "") or None,
            )
        )
    return points

def build_state_transition_evidence(
    *,
    run_id: str,
    samples: Iterable[SamplingResult],
    point_summaries: Iterable[dict[str, Any]] | None = None,
    route_trace_events: Iterable[dict[str, Any]] | None = None,
    artifact_paths: dict[str, Any] | None = None,
) -> dict[str, Any]:
    sample_rows = [sample for sample in list(samples or []) if isinstance(sample, SamplingResult)]
    summary_rows = [dict(item) for item in list(point_summaries or []) if isinstance(item, dict)]
    route_rows = [dict(item) for item in list(route_trace_events or []) if isinstance(item, dict)]
    compiled_layers = _compile_controlled_layers(points=_runtime_points_from_inputs(sample_rows, summary_rows))
    trace = _build_runtime_trace(sample_rows, summary_rows, route_rows)
    phase_decision_logs = _build_phase_decision_logs(sample_rows, summary_rows, route_rows)
    illegal_transitions = [
        dict(item)
        for item in trace
        if not bool(item.get("allowed", False))
    ]
    replay_trace = _build_replay_trace(trace)
    comparison_rollup = _build_transition_comparison(
        trace,
        dict(compiled_layers.get("compiled_route_state_graph") or {}),
    )
    status = "passed" if trace and not illegal_transitions else "degraded" if trace else "diagnostic_only"
    phase_summary = " | ".join(
        f"{key} {value}"
        for key, value in _count_by_key(phase_decision_logs, "phase_policy").items()
    ) or "--"
    route_summary = " | ".join(
        f"{key} {value}"
        for key, value in _count_by_key(phase_decision_logs, "route_family").items()
    ) or "--"
    evidence_source_filters = _dedupe(
        item.get("evidence_source")
        for item in phase_decision_logs
    ) or ["model_only"]
    transition_policy_profile = dict(compiled_layers.get("transition_policy_profile") or {})
    digest = {
        "summary": (
            "Step 2 tail / Stage 3 bridge | controlled-flex trace | "
            f"events {len(trace)} | illegal {len(illegal_transitions)}"
        ),
        "phase_summary": phase_summary,
        "route_summary": route_summary,
        "policy_summary": str(transition_policy_profile.get("summary_line") or "--"),
        "graph_summary": str(
            dict(compiled_layers.get("compiled_route_state_graph") or {}).get("summary_line") or "--"
        ),
        "replay_summary": str(replay_trace.get("summary") or "--"),
        "comparison_summary": str(comparison_rollup.get("summary") or "--"),
        "transition_summary": " | ".join(
            f"{item.get('from_state', '--')}->{item.get('to_state', '--')}" for item in trace[:6]
        )
        or "--",
        "recovery_summary": " | ".join(
            _dedupe(item.get("recovery_marker") for item in trace if str(item.get("recovery_marker") or "").strip())
        )
        or "no recovery markers",
        "boundary_summary": " | ".join(CANONICAL_BOUNDARY_STATEMENTS),
    }
    artifact_path_map = {
        "state_transition_evidence": str(
            dict(artifact_paths or {}).get("state_transition_evidence")
            or STATE_TRANSITION_EVIDENCE_FILENAME
        ),
        "state_transition_evidence_markdown": str(
            dict(artifact_paths or {}).get("state_transition_evidence_markdown")
            or STATE_TRANSITION_EVIDENCE_MARKDOWN_FILENAME
        ),
    }
    review_surface = {
        "title_text": "State Transition Evidence",
        "role_text": "diagnostic_analysis",
        "reviewer_note": (
            "Controlled state machine trace, fixed canonical states, transition policy profile, and "
            "compiled route graph for Step 2 offline replay only."
        ),
        "summary_text": digest["summary"],
        "summary_lines": [
            digest["summary"],
            f"phase buckets: {digest['phase_summary']}",
            f"route families: {digest['route_summary']}",
            f"policy profile: {digest['policy_summary']}",
            f"compiled graph: {digest['graph_summary']}",
            f"replay: {digest['replay_summary']}",
            f"comparison: {digest['comparison_summary']}",
        ],
        "detail_lines": [
            f"transitions: {digest['transition_summary']}",
            f"recovery: {digest['recovery_summary']}",
            *[f"boundary: {line}" for line in CANONICAL_BOUNDARY_STATEMENTS],
            f"illegal transitions: {len(illegal_transitions)}",
        ],
        "anchor_id": "state-transition-evidence",
        "anchor_label": "State transition evidence",
        "phase_filters": _dedupe(item.get("phase_policy") for item in phase_decision_logs),
        "route_filters": _dedupe(item.get("route_family") for item in phase_decision_logs),
        "signal_family_filters": ["state_machine", "route_graph", "transition_policy"],
        "decision_result_filters": _dedupe(item.get("decision_result") for item in phase_decision_logs),
        "policy_version_filters": _dedupe(
            [
                transition_policy_profile.get("policy_version"),
                transition_policy_profile.get("feature_set_version"),
            ]
        ),
        "boundary_filters": list(CANONICAL_BOUNDARY_STATEMENTS),
        "evidence_source_filters": evidence_source_filters,
        "artifact_paths": dict(artifact_path_map),
    }
    markdown = _render_markdown(
        trace=trace,
        review_surface=review_surface,
        artifact_paths=artifact_path_map,
        transition_policy_profile=transition_policy_profile,
        compiled_route_state_graph=dict(compiled_layers.get("compiled_route_state_graph") or {}),
        comparison_rollup=comparison_rollup,
    )
    raw = {
        "schema_version": "2.0",
        "artifact_type": "state_transition_evidence",
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "run_id": str(run_id or ""),
        "artifact_role": "diagnostic_analysis",
        **STEP2_REVIEWER_FLAGS,
        "boundary_statements": list(CANONICAL_BOUNDARY_STATEMENTS),
        "canonical_states": list(CANONICAL_STATES),
        "allowed_transitions": {key: list(value) for key, value in ALLOWED_TRANSITIONS.items()},
        "state_library": dict(compiled_layers.get("state_library") or {}),
        "policy_profile": dict(compiled_layers.get("policy_profile") or {}),
        "compiled_route_state_graph": dict(compiled_layers.get("compiled_route_state_graph") or {}),
        "transition_policy_profile": transition_policy_profile,
        "state_transition_logs": trace,
        "phase_decision_logs": phase_decision_logs,
        "illegal_transitions": illegal_transitions,
        "replay_trace": replay_trace,
        "comparison_rollup": comparison_rollup,
        "digest": digest,
        "review_surface": review_surface,
        "artifact_paths": artifact_path_map,
        "overall_status": status,
    }
    return {
        "available": True,
        "artifact_type": "state_transition_evidence",
        "filename": STATE_TRANSITION_EVIDENCE_FILENAME,
        "markdown_filename": STATE_TRANSITION_EVIDENCE_MARKDOWN_FILENAME,
        "raw": raw,
        "markdown": markdown,
        "digest": digest,
    }


def _build_runtime_trace(
    samples: list[SamplingResult],
    point_summaries: list[dict[str, Any]],
    route_trace_events: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    trace: list[dict[str, Any]] = []
    sequence = 0
    base_path = ["INIT", "DEVICE_READY", "PLAN_COMPILED"]
    sequence = _append_trace(trace, base_path, sequence=sequence, reason="bootstrap", point_index=0, route_family="system")
    points = _point_order(samples, point_summaries, route_trace_events)
    for item in points:
        point_index = int(item.get("point_index", 0) or 0)
        route_family = str(item.get("route_family") or "gas")
        state_path = list(item.get("state_path") or [])
        sequence = _append_trace(
            trace,
            state_path,
            sequence=sequence,
            reason=str(item.get("reason") or "point_flow"),
            point_index=point_index,
            route_family=route_family,
            recovery_marker=str(item.get("recovery_marker") or ""),
        )
    if trace and str(trace[-1].get("to_state") or "") not in {"RUN_COMPLETE", "ABORT"}:
        sequence = _append_trace(
            trace,
            ["RUN_COMPLETE"],
            sequence=sequence,
            reason="run_complete",
            point_index=int(points[-1].get("point_index", 0) or 0) if points else 0,
            route_family=str(points[-1].get("route_family") or "system") if points else "system",
        )
    return trace


def _build_phase_decision_logs(
    samples: list[SamplingResult],
    point_summaries: list[dict[str, Any]],
    route_trace_events: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for item in _point_order(samples, point_summaries, route_trace_events):
        rows.append(
            {
                "point_index": int(item.get("point_index", 0) or 0),
                "route_family": str(item.get("route_family") or "gas"),
                "phase_policy": str(item.get("phase_policy") or "sample_ready"),
                "decision_result": str(item.get("decision_result") or "trace_only"),
                "active_states": list(item.get("state_path") or []),
                "recovery_marker": str(item.get("recovery_marker") or ""),
                "recommended_policy_action": (
                    "replay_fault_window_before_next_point"
                    if str(item.get("decision_result") or "") == "fault_capture_recovery"
                    or str(item.get("recovery_marker") or "").strip()
                    else "compare_observed_trace_with_compiled_graph"
                    if bool(item.get("observed_in_trace"))
                    else "continue_reviewer_trace_replay"
                ),
                "evidence_source": "actual_simulated_run" if bool(item.get("observed_in_trace")) else "model_only",
            }
        )
    seen = {
        (
            int(item.get("point_index", 0) or 0),
            str(item.get("route_family") or ""),
            str(item.get("phase_policy") or ""),
            str(item.get("decision_result") or ""),
        )
        for item in rows
    }
    for event in route_trace_events:
        route_family = _route_family(str(event.get("route") or ""), pressure_mode="")
        point_index = int(event.get("point_index", 0) or 0)
        for phase_policy in _phase_policies_from_trace_event(event):
            decision_result = "fault_capture_recovery" if phase_policy == "recovery_retry" else "actual_trace_observed"
            key = (point_index, route_family, phase_policy, decision_result)
            if key in seen:
                continue
            seen.add(key)
            rows.append(
                {
                    "point_index": point_index,
                    "route_family": route_family,
                    "phase_policy": phase_policy,
                    "decision_result": decision_result,
                    "active_states": [],
                    "recovery_marker": "retry_or_recovery" if phase_policy == "recovery_retry" else "",
                    "recommended_policy_action": (
                        "replay_fault_window_before_next_point"
                        if phase_policy == "recovery_retry"
                        else "compare_observed_trace_with_compiled_graph"
                    ),
                    "evidence_source": "actual_simulated_run",
                }
            )
    return rows


def _append_trace(
    trace: list[dict[str, Any]],
    state_path: list[str],
    *,
    sequence: int,
    reason: str,
    point_index: int,
    route_family: str,
    recovery_marker: str = "",
) -> int:
    current_state = str(trace[-1].get("to_state") or "") if trace else ""
    for next_state in state_path:
        if current_state == next_state:
            continue
        sequence += 1
        allowed = next_state in set(ALLOWED_TRANSITIONS.get(current_state, ())) if current_state else True
        trace.append(
            asdict(
                StateTransitionEvent(
                    sequence=sequence,
                    from_state=current_state or "--",
                    to_state=next_state,
                    reason=reason,
                    point_index=point_index,
                    route_family=route_family,
                    recovery_marker=recovery_marker,
                )
            )
            | {"allowed": allowed}
        )
        current_state = next_state
    return sequence


def _point_order(
    samples: list[SamplingResult],
    point_summaries: list[dict[str, Any]],
    route_trace_events: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    by_point: dict[tuple[int, str, str], dict[str, Any]] = {}
    encounter_order: list[tuple[int, str, str]] = []
    for sample in samples:
        point = getattr(sample, "point", None)
        point_index = int(getattr(point, "index", 0) or 0)
        route_family = _route_family_from_point(point)
        point_tag = str(getattr(sample, "point_tag", "") or getattr(point, "pressure_display_label", "") or getattr(point, "route", "") or "--")
        key = (point_index, route_family, point_tag)
        if key not in by_point:
            encounter_order.append(key)
        entry = by_point.setdefault(
            key,
            {
                "point_index": point_index,
                "route_family": route_family,
                "phase_policy": _phase_policy_from_text(str(getattr(sample, "point_phase", "") or "sample_ready")),
                "point": point,
                "point_tag": point_tag,
                "summary": {},
                "has_usable_raw": any(
                    _has_value(getattr(sample, channel, None))
                    for channel in ("co2_ratio_raw", "h2o_ratio_raw", "ref_signal", "co2_signal", "h2o_signal")
                ),
                "has_output": any(
                    _has_value(getattr(sample, channel, None))
                    for channel in ("co2_ppm", "h2o_mmol", "co2_ratio_f", "h2o_ratio_f")
                ),
                "observed_in_trace": False,
            },
        )
        entry["phase_policy"] = _phase_policy_from_text(str(getattr(sample, "point_phase", "") or entry.get("phase_policy") or "sample_ready"))
        entry["has_usable_raw"] = bool(entry.get("has_usable_raw")) or any(
            _has_value(getattr(sample, channel, None))
            for channel in ("co2_ratio_raw", "h2o_ratio_raw", "ref_signal", "co2_signal", "h2o_signal")
        )
        entry["has_output"] = bool(entry.get("has_output")) or any(
            _has_value(getattr(sample, channel, None))
            for channel in ("co2_ppm", "h2o_mmol", "co2_ratio_f", "h2o_ratio_f")
        )
    for summary in point_summaries:
        point_payload = dict(summary.get("point") or {})
        stats_payload = dict(summary.get("stats") or {})
        point_index = int(point_payload.get("index", 0) or 0)
        route_family = _route_family(str(point_payload.get("route") or ""), pressure_mode=str(point_payload.get("pressure_mode") or ""))
        point_tag = str(
            point_payload.get("pressure_target_label")
            or point_payload.get("pressure_selection_token")
            or point_payload.get("route")
            or "--"
        )
        key = (point_index, route_family, point_tag)
        if key not in by_point:
            encounter_order.append(key)
        entry = by_point.setdefault(
            key,
            {
                "point_index": point_index,
                "route_family": route_family,
                "phase_policy": _phase_policy_from_text(str(stats_payload.get("point_phase") or "sample_ready")),
                "point": None,
                "point_tag": point_tag,
                "summary": dict(summary),
                "has_usable_raw": False,
                "has_output": False,
                "observed_in_trace": False,
            },
        )
        entry["summary"] = dict(summary)
        if str(stats_payload.get("point_phase") or "").strip():
            entry["phase_policy"] = _phase_policy_from_text(str(stats_payload.get("point_phase") or ""))
    for event in route_trace_events:
        route_family = _route_family(str(event.get("route") or ""), pressure_mode="")
        point_index = int(event.get("point_index", 0) or 0)
        point_tag = str(event.get("point_tag") or "--")
        key = (point_index, route_family, point_tag)
        if key not in by_point:
            encounter_order.append(key)
        entry = by_point.setdefault(
            key,
            {
                "point_index": point_index,
                "route_family": route_family,
                "phase_policy": "sample_ready",
                "point": None,
                "point_tag": point_tag,
                "summary": {},
                "has_usable_raw": False,
                "has_output": False,
                "observed_in_trace": True,
            },
        )
        entry["observed_in_trace"] = True
        phase_candidates = _phase_policies_from_trace_event(event)
        if phase_candidates:
            entry["phase_policy"] = phase_candidates[-1]
    rows: list[dict[str, Any]] = []
    positive_keys = [key for key in encounter_order if key in by_point and int(key[0] or 0) > 0]
    ordered_keys = sorted(
        positive_keys,
        key=lambda item: (int(item[0] or 0), positive_keys.index(item)),
    )
    max_point_index = max((int(key[0] or 0) for key in ordered_keys), default=0)
    for key in ordered_keys:
        item = dict(by_point.get(key) or {})
        point = item.get("point")
        summary = dict(item.get("summary") or {})
        state_path = _active_state_path_for_point(point)
        if bool(item.get("has_usable_raw")):
            state_path.append("RAW_SIGNAL_STABLE")
        if bool(item.get("has_output")):
            state_path.append("OUTPUT_STABLE")
        state_path.extend(["SAMPLE_WINDOW", "POINT_COMPLETE"])
        stats_payload = dict(summary.get("stats") or {})
        recovery_marker = ""
        decision_result = "trace_only"
        if stats_payload.get("valid") is False or list(stats_payload.get("failed_checks") or []):
            state_path.extend(["FAULT_CAPTURE", "SAFE_RECOVERY"])
            recovery_marker = "retry_or_recovery"
            decision_result = "fault_capture_recovery"
        if bool(item.get("observed_in_trace")) and str(item.get("phase_policy") or "") == "recovery_retry":
            state_path.extend(["FAULT_CAPTURE", "SAFE_RECOVERY"])
            recovery_marker = recovery_marker or "retry_or_recovery"
            decision_result = "fault_capture_recovery"
        if key != ordered_keys[-1] and point_index < max_point_index:
            state_path.append("NEXT_POINT")
        rows.append(
            {
                "point_index": point_index,
                "route_family": str(item.get("route_family") or "gas"),
                "phase_policy": str(item.get("phase_policy") or "sample_ready"),
                "state_path": state_path,
                "reason": "point_path",
                "recovery_marker": recovery_marker,
                "decision_result": decision_result,
                "observed_in_trace": bool(item.get("observed_in_trace")),
            }
        )
    if not rows:
        rows.append(
            {
                "point_index": 0,
                "route_family": "system",
                "phase_policy": "sample_ready",
                "state_path": ["TEMP_SOAK", "ROUTE_FLUSH", "RAW_SIGNAL_STABLE", "OUTPUT_STABLE", "SAMPLE_WINDOW", "POINT_COMPLETE", "RUN_COMPLETE"],
                "reason": "empty_trace_fallback",
                "recovery_marker": "",
                "decision_result": "trace_only",
            }
        )
    return rows


def _active_state_path_for_point(point: CalibrationPoint | None) -> list[str]:
    route_family = _route_family_from_point(point)
    state_path = ["TEMP_SOAK", "ROUTE_FLUSH"]
    if route_family == "water":
        state_path.append("PRESEAL_STABILITY")
    if point is None or not bool(getattr(point, "is_ambient_pressure_point", False)):
        state_path.extend(["SEAL", "PRESSURE_HANDOFF", "PRESSURE_STABLE"])
    return state_path


def _route_families_from_points(points: Iterable[CalibrationPoint]) -> list[str]:
    return _dedupe(_route_family_from_point(point) for point in points)


def _route_family_from_point(point: CalibrationPoint | None) -> str:
    if point is None:
        return "gas"
    route = str(getattr(point, "route", "") or "").strip().lower()
    pressure_mode = str(getattr(point, "effective_pressure_mode", "") or "").strip().lower()
    return "water" if route == "h2o" else "ambient" if pressure_mode == "ambient_open" or "ambient" in route else "gas"


def _route_family(route_text: str, *, pressure_mode: str = "") -> str:
    route = str(route_text or "").strip().lower()
    pressure_token = str(pressure_mode or "").strip().lower()
    if route == "h2o":
        return "water"
    if pressure_token == "ambient_open" or "ambient" in route:
        return "ambient"
    return "gas"


def _transition_kind(from_state: str, to_state: str) -> str:
    if to_state in {"FAULT_CAPTURE", "SAFE_RECOVERY"}:
        return "recovery"
    if to_state in {"NEXT_POINT", "NEXT_ROUTE", "NEXT_TEMP"}:
        return "routing"
    if to_state in {"RUN_COMPLETE", "ABORT"}:
        return "terminal"
    if to_state in {"RAW_SIGNAL_STABLE", "OUTPUT_STABLE", "PRESSURE_STABLE", "PRESEAL_STABILITY"}:
        return "stability"
    if to_state == "SAMPLE_WINDOW":
        return "sampling"
    if from_state in {"", "--"}:
        return "bootstrap"
    return "progression"


def _phase_policy_from_text(value: str) -> str:
    text = str(value or "").strip().lower()
    if "diagnostic" in text or "ambient" in text:
        return "ambient_diagnostic"
    if "recovery" in text or "retry" in text or "abort" in text:
        return "recovery_retry"
    if "preseal" in text or "seal" in text:
        return "preseal"
    if "pressure" in text:
        return "pressure_stable"
    return "sample_ready"


def _phase_policies_from_trace_event(event: dict[str, Any]) -> list[str]:
    text = " ".join(str(event.get(key) or "") for key in ("action", "message", "route")).strip().lower()
    rows: list[str] = []
    if "ambient" in text or "diagnostic" in text:
        rows.append("ambient_diagnostic")
    if any(token in text for token in ("retry", "recovery", "abort", "fault_capture")):
        rows.append("recovery_retry")
    if any(token in text for token in ("set_pressure", "wait_post_pressure", "pressure stabilized")):
        rows.append("pressure_stable")
    if any(token in text for token in ("sample_start", "sample_end", "sampling")):
        rows.append("sample_ready")
    if any(token in text for token in ("pre-seal", "preseal", "wait_route_ready", "wait_dewpoint", "wait_route_soak", "seal_route")):
        rows.append("preseal")
    return _dedupe(rows)


def _count_by_key(rows: Iterable[dict[str, Any]], key: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        value = str(dict(row or {}).get(key) or "").strip()
        if not value:
            continue
        counts[value] = counts.get(value, 0) + 1
    return counts


def _coerce_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _has_value(value: Any) -> bool:
    return value not in (None, "", [], {})


def _build_replay_trace(trace: list[dict[str, Any]]) -> dict[str, Any]:
    grouped: dict[int, list[str]] = {}
    for row in trace:
        grouped.setdefault(int(row.get("point_index", 0) or 0), []).append(str(row.get("to_state") or "--"))
    lines = [
        f"point {point_index}: {' -> '.join(states[:8])}"
        for point_index, states in sorted(grouped.items())
    ]
    return {
        "summary": " | ".join(lines[:4]) or "no replay trace",
        "point_replay_lines": lines,
        "sequence_count": len(trace),
    }


def _build_transition_comparison(
    trace: list[dict[str, Any]],
    compiled_route_state_graph: dict[str, Any],
) -> dict[str, Any]:
    observed_pairs = {
        (str(item.get("from_state") or ""), str(item.get("to_state") or ""))
        for item in trace
        if str(item.get("from_state") or "").strip() and str(item.get("from_state") or "") != "--"
    }
    compiled_pairs = {
        (str(item.get("from_state") or ""), str(item.get("to_state") or ""))
        for item in list(compiled_route_state_graph.get("edges") or [])
        if bool(item.get("enabled_for_profile", False))
    }
    unexpected_pairs = sorted(observed_pairs - compiled_pairs)
    missing_pairs = sorted(compiled_pairs - observed_pairs)
    return {
        "summary": (
            f"observed {len(observed_pairs)} | unexpected {len(unexpected_pairs)} | "
            f"compiled_not_visited {len(missing_pairs)}"
        ),
        "unexpected_transitions": [
            {"from_state": from_state, "to_state": to_state}
            for from_state, to_state in unexpected_pairs[:10]
        ],
        "compiled_not_visited": [
            {"from_state": from_state, "to_state": to_state}
            for from_state, to_state in missing_pairs[:10]
        ],
    }


def _render_markdown(
    *,
    trace: list[dict[str, Any]],
    review_surface: dict[str, Any],
    artifact_paths: dict[str, str],
    transition_policy_profile: dict[str, Any],
    compiled_route_state_graph: dict[str, Any],
    comparison_rollup: dict[str, Any],
) -> str:
    lines = [
        "# State Transition Evidence",
        "",
        f"- title: {review_surface.get('title_text', '--')}",
        f"- role: {review_surface.get('role_text', '--')}",
        f"- reviewer_note: {review_surface.get('reviewer_note', '--')}",
        "",
        "## Boundary",
        "",
        *[f"- {line}" for line in CANONICAL_BOUNDARY_STATEMENTS],
        "",
        "## Transition Policy Profile",
        "",
        f"- policy_version: {transition_policy_profile.get('policy_version', '--')}",
        f"- feature_set_version: {transition_policy_profile.get('feature_set_version', '--')}",
        f"- summary: {transition_policy_profile.get('summary_line', '--')}",
        "",
        "## Compiled Route Graph",
        "",
        f"- summary: {compiled_route_state_graph.get('summary_line', '--')}",
    ]
    for row in list(compiled_route_state_graph.get("route_paths") or []):
        lines.append(
            f"- {row.get('route_family', '--')}: "
            f"{' -> '.join(list(row.get('compiled_state_path') or [])[:12]) or '--'}"
        )
    lines.extend(
        [
            "",
        "## Transition Trace",
        "",
        ]
    )
    for item in trace:
        lines.append(
            f"- #{item.get('sequence', '--')}: {item.get('from_state', '--')} -> {item.get('to_state', '--')} | "
            f"allowed {item.get('allowed', False)} | point {item.get('point_index', 0)} | "
            f"route {item.get('route_family', '--')}"
        )
    lines.extend(
        [
            "",
            "## Compiled vs Observed",
            "",
            f"- {comparison_rollup.get('summary', '--')}",
            "",
            "## Artifact Paths",
            "",
            f"- json: {artifact_paths.get('state_transition_evidence', '--')}",
            f"- markdown: {artifact_paths.get('state_transition_evidence_markdown', '--')}",
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
