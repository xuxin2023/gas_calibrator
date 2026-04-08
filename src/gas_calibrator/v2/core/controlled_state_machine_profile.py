from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any, Iterable

from .models import CalibrationPoint, SamplingResult
from .plan_compiler import CompiledPlan


STATE_TRANSITION_EVIDENCE_FILENAME = "state_transition_evidence.json"
STATE_TRANSITION_EVIDENCE_MARKDOWN_FILENAME = "state_transition_evidence.md"

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
    "ROUTE_FLUSH": ("PRESEAL_STABILITY", "RAW_SIGNAL_STABLE", "FAULT_CAPTURE", "ABORT"),
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
    profile_version: str = "controlled_flex_v1",
) -> dict[str, Any]:
    points = list(compiled_plan.preview_points or compiled_plan.points or [])
    route_families = _route_families_from_points(points)
    requires_pressure_states = any(not bool(getattr(point, "is_ambient_pressure_point", False)) for point in points)
    requires_preseal = any(str(getattr(point, "route", "") or "").strip().lower() == "h2o" for point in points)
    enabled_states = [
        "INIT",
        "DEVICE_READY",
        "PLAN_COMPILED",
        "TEMP_SOAK",
        "ROUTE_FLUSH",
    ]
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
    enabled_states = [state for state in CANONICAL_STATES if state in set(enabled_states)]
    skipped_states = [state for state in CANONICAL_STATES if state not in set(enabled_states)]
    profile = ControlledStateMachineProfile(
        profile_version=profile_version,
        enabled_states=tuple(enabled_states),
        skipped_states=tuple(skipped_states),
        allowed_transitions={key: tuple(value) for key, value in ALLOWED_TRANSITIONS.items()},
        route_families=tuple(route_families),
    )
    point_profiles = [
        {
            "point_index": int(getattr(point, "index", 0) or 0),
            "route_family": _route_family_from_point(point),
            "pressure_mode": str(getattr(point, "effective_pressure_mode", "") or ""),
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
        "metadata": {
            "profile_name": compiled_plan.profile_name,
            "runtime_row_count": len(list(compiled_plan.runtime_rows or [])),
            "preview_point_count": len(points),
            "plan_profile_version": str(dict(compiled_plan.metadata or {}).get("profile_version") or "1.0"),
        },
    }


def validate_transition(from_state: str, to_state: str) -> None:
    allowed = set(ALLOWED_TRANSITIONS.get(str(from_state or ""), ()))
    if str(to_state or "") not in allowed:
        raise ValueError(f"illegal transition: {from_state} -> {to_state}")


def build_state_transition_evidence(
    *,
    run_id: str,
    samples: Iterable[SamplingResult],
    point_summaries: Iterable[dict[str, Any]] | None = None,
    artifact_paths: dict[str, Any] | None = None,
) -> dict[str, Any]:
    sample_rows = [sample for sample in list(samples or []) if isinstance(sample, SamplingResult)]
    summary_rows = [dict(item) for item in list(point_summaries or []) if isinstance(item, dict)]
    trace = _build_runtime_trace(sample_rows, summary_rows)
    phase_decision_logs = _build_phase_decision_logs(sample_rows, summary_rows)
    illegal_transitions = [
        dict(item)
        for item in trace
        if not bool(item.get("allowed", False))
    ]
    status = "passed" if trace and not illegal_transitions else "degraded" if trace else "diagnostic_only"
    digest = {
        "summary": (
            "Step 2 tail / Stage 3 bridge | controlled-flex trace | "
            f"events {len(trace)} | illegal {len(illegal_transitions)}"
        ),
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
            "Controlled-flex state machine trace with fixed canonical states and allowed transitions only. "
            "This is reviewer evidence and not a runtime control surface."
        ),
        "summary_text": digest["summary"],
        "summary_lines": [
            digest["summary"],
            f"transitions: {digest['transition_summary']}",
            f"recovery: {digest['recovery_summary']}",
        ],
        "detail_lines": [
            *[f"boundary: {line}" for line in CANONICAL_BOUNDARY_STATEMENTS],
            f"illegal transitions: {len(illegal_transitions)}",
        ],
        "anchor_id": "state-transition-evidence",
        "anchor_label": "State transition evidence",
        "phase_filters": _dedupe(item.get("phase_policy") for item in phase_decision_logs),
        "route_filters": _dedupe(item.get("route_family") for item in phase_decision_logs),
        "signal_family_filters": ["analyzer_raw", "output"],
        "decision_result_filters": _dedupe(item.get("decision_result") for item in phase_decision_logs),
        "policy_version_filters": ["controlled_flex_v1"],
        "boundary_filters": list(CANONICAL_BOUNDARY_STATEMENTS),
        "artifact_paths": dict(artifact_path_map),
    }
    markdown = _render_markdown(trace=trace, review_surface=review_surface, artifact_paths=artifact_path_map)
    raw = {
        "schema_version": "1.0",
        "artifact_type": "state_transition_evidence",
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "run_id": str(run_id or ""),
        "artifact_role": "diagnostic_analysis",
        "evidence_source": "simulated",
        "not_real_acceptance_evidence": True,
        "boundary_statements": list(CANONICAL_BOUNDARY_STATEMENTS),
        "canonical_states": list(CANONICAL_STATES),
        "allowed_transitions": {key: list(value) for key, value in ALLOWED_TRANSITIONS.items()},
        "state_transition_logs": trace,
        "phase_decision_logs": phase_decision_logs,
        "illegal_transitions": illegal_transitions,
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


def _build_runtime_trace(samples: list[SamplingResult], point_summaries: list[dict[str, Any]]) -> list[dict[str, Any]]:
    trace: list[dict[str, Any]] = []
    sequence = 0
    base_path = ["INIT", "DEVICE_READY", "PLAN_COMPILED"]
    sequence = _append_trace(trace, base_path, sequence=sequence, reason="bootstrap", point_index=0, route_family="system")
    points = _point_order(samples, point_summaries)
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


def _build_phase_decision_logs(samples: list[SamplingResult], point_summaries: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for item in _point_order(samples, point_summaries):
        rows.append(
            {
                "point_index": int(item.get("point_index", 0) or 0),
                "route_family": str(item.get("route_family") or "gas"),
                "phase_policy": str(item.get("phase_policy") or "sample_ready"),
                "decision_result": str(item.get("decision_result") or "trace_only"),
                "active_states": list(item.get("state_path") or []),
                "recovery_marker": str(item.get("recovery_marker") or ""),
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


def _point_order(samples: list[SamplingResult], point_summaries: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_point: dict[int, dict[str, Any]] = {}
    for sample in samples:
        point = getattr(sample, "point", None)
        point_index = int(getattr(point, "index", 0) or 0)
        route_family = _route_family_from_point(point)
        entry = by_point.setdefault(
            point_index,
            {
                "point_index": point_index,
                "route_family": route_family,
                "phase_policy": _phase_policy_from_text(str(getattr(sample, "point_phase", "") or "sample_ready")),
                "point": point,
                "summary": {},
                "has_usable_raw": any(
                    getattr(sample, channel, None) not in (None, "")
                    for channel in ("co2_ratio_raw", "h2o_ratio_raw", "ref_signal", "co2_signal", "h2o_signal")
                ),
                "has_output": any(
                    getattr(sample, channel, None) not in (None, "")
                    for channel in ("co2_ppm", "h2o_mmol", "co2_ratio_f", "h2o_ratio_f")
                ),
            },
        )
        entry["has_usable_raw"] = bool(entry.get("has_usable_raw")) or any(
            getattr(sample, channel, None) not in (None, "")
            for channel in ("co2_ratio_raw", "h2o_ratio_raw", "ref_signal", "co2_signal", "h2o_signal")
        )
        entry["has_output"] = bool(entry.get("has_output")) or any(
            getattr(sample, channel, None) not in (None, "")
            for channel in ("co2_ppm", "h2o_mmol", "co2_ratio_f", "h2o_ratio_f")
        )
    for summary in point_summaries:
        point_payload = dict(summary.get("point") or {})
        stats_payload = dict(summary.get("stats") or {})
        point_index = int(point_payload.get("index", 0) or 0)
        route_family = _route_family(str(point_payload.get("route") or ""), pressure_mode=str(point_payload.get("pressure_mode") or ""))
        entry = by_point.setdefault(
            point_index,
            {
                "point_index": point_index,
                "route_family": route_family,
                "phase_policy": _phase_policy_from_text(str(stats_payload.get("point_phase") or "sample_ready")),
                "point": None,
                "summary": dict(summary),
                "has_usable_raw": False,
                "has_output": False,
            },
        )
        entry["summary"] = dict(summary)
    rows: list[dict[str, Any]] = []
    for point_index in sorted(key for key in by_point if key > 0):
        item = dict(by_point.get(point_index) or {})
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
        if point_index < max(by_point):
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


def _render_markdown(
    *,
    trace: list[dict[str, Any]],
    review_surface: dict[str, Any],
    artifact_paths: dict[str, str],
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
        "## Transition Trace",
        "",
    ]
    for item in trace:
        lines.append(
            f"- #{item.get('sequence', '--')}: {item.get('from_state', '--')} -> {item.get('to_state', '--')} | "
            f"allowed {item.get('allowed', False)} | point {item.get('point_index', 0)} | "
            f"route {item.get('route_family', '--')}"
        )
    lines.extend(
        [
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
