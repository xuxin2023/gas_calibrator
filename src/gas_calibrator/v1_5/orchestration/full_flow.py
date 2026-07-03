"""V1.5 formal calibration full-flow dry-run planner.

This module builds a reviewer-facing execution plan around the V1.5 tools that
have been validated during recent pressure, CO2, H2O, coefficient, evidence,
database, and report work. It is not a device runner. The first implementation
only writes a plan and command list so that the full physical sequence can be
reviewed before any real COM, route control, or SENCO write is enabled.
"""

from __future__ import annotations

import json
import subprocess
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence


PLAN_SCHEMA = "v1_5_full_calibration_flow_plan_v0"
PLAN_CONTRACT = "pressure_first_temperature_review_then_open_flow_components"
STAGE_MANIFEST_SCHEMA = "v1_5_full_flow_stage_manifest_v1"
LIVE_RUNNER_READINESS_SCHEMA = "v1_5_full_flow_live_runner_readiness_v1"


@dataclass(frozen=True)
class FullFlowStep:
    """One planned V1.5 calibration flow step."""

    step_id: str
    title: str
    phase: str
    tool_module: str | None
    command: tuple[str, ...] = ()
    required_inputs: tuple[str, ...] = ()
    expected_outputs: tuple[str, ...] = ()
    physical_meaning: str = ""
    execution_mode: str = "offline"
    gate: str = "review"
    uses_validated_v1_5_entry: bool = True
    may_reuse_v1_shared_core: bool = False
    opens_com_ports: bool = False
    controls_pressure: bool = False
    controls_gas_route: bool = False
    controls_water_route: bool = False
    writes_coefficients: bool = False
    writes_device_id: bool = False
    coefficient_epoch_event: str = "none"
    notes: tuple[str, ...] = ()

    def to_json(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class FullFlowPlan:
    """V1.5 formal calibration plan payload."""

    schema: str
    contract: str
    run_id: str
    created_at: str
    config_path: str
    output_dir: str
    dry_run_only: bool
    safety_contract: Mapping[str, Any]
    coefficient_epoch_contract: Mapping[str, Any]
    physical_order: tuple[str, ...]
    steps: tuple[FullFlowStep, ...]
    warnings: tuple[str, ...] = ()
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def to_json(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["steps"] = [step.to_json() for step in self.steps]
        return payload


@dataclass(frozen=True)
class FullFlowStageState:
    """Reviewer-facing state for one V1.5 full-flow stage."""

    step_id: str
    title: str
    phase: str
    status: str
    reason: str
    can_execute_now: bool
    command: tuple[str, ...] = ()
    requires_real_com_authorization: bool = False
    requires_pressure_authorization: bool = False
    requires_route_authorization: bool = False
    requires_write_authorization: bool = False
    has_unresolved_placeholders: bool = False
    opens_com_ports: bool = False
    controls_gas_route: bool = False
    controls_water_route: bool = False
    writes_coefficients: bool = False
    coefficient_epoch_event: str = "none"

    def to_json(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class FullFlowState:
    """Resumable execution state for the V1.5 full-flow plan."""

    schema: str
    run_id: str
    created_at: str
    current_step_id: str
    current_status: str
    ready_step_ids: tuple[str, ...]
    blocked_step_ids: tuple[str, ...]
    completed_step_ids: tuple[str, ...]
    failed_step_ids: tuple[str, ...]
    allow_real_com: bool
    allow_pressure_control: bool
    allow_route_control: bool
    allow_writes: bool
    stage_states: tuple[FullFlowStageState, ...]

    def to_json(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["stage_states"] = [stage.to_json() for stage in self.stage_states]
        return payload


@dataclass(frozen=True)
class FullFlowExecutionEvent:
    """One supervised execution attempt or stop reason."""

    step_id: str
    status: str
    reason: str
    command: tuple[str, ...] = ()
    returncode: int | None = None
    stdout_path: str = ""
    stderr_path: str = ""

    def to_json(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class FullFlowSupervisedRun:
    """Result of a supervised, offline-only V1.5 flow advance."""

    schema: str
    run_id: str
    created_at: str
    execute_commands: bool
    max_steps: int
    allow_database_import: bool
    initial_state: FullFlowState
    final_state: FullFlowState
    events: tuple[FullFlowExecutionEvent, ...]

    def to_json(self) -> dict[str, Any]:
        return {
            "schema": self.schema,
            "run_id": self.run_id,
            "created_at": self.created_at,
            "execute_commands": self.execute_commands,
            "max_steps": self.max_steps,
            "allow_database_import": self.allow_database_import,
            "initial_state": self.initial_state.to_json(),
            "final_state": self.final_state.to_json(),
            "events": [event.to_json() for event in self.events],
        }


@dataclass(frozen=True)
class FullFlowStageManifestEntry:
    """Machine-readable contract for one V1.5 full-flow stage."""

    order: int
    step_id: str
    title: str
    phase: str
    tool_module: str
    automation_state: str
    execution_mode: str
    gate: str
    required_inputs: tuple[str, ...]
    expected_outputs: tuple[str, ...]
    physical_meaning: str
    command: tuple[str, ...]
    safety_boundaries: Mapping[str, Any]
    authorization_required: Mapping[str, bool]
    evidence_contract: Mapping[str, Any]
    coefficient_epoch_event: str
    notes: tuple[str, ...]

    def to_json(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class FullFlowStageManifest:
    """Stable stage manifest for UI, audit, and future executors."""

    schema: str
    run_id: str
    contract: str
    created_at: str
    current_automation_level: str
    one_button_live_runner_ready: bool
    safety_summary: Mapping[str, Any]
    automation_summary: Mapping[str, int]
    stages: tuple[FullFlowStageManifestEntry, ...]

    def to_json(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["stages"] = [stage.to_json() for stage in self.stages]
        return payload


@dataclass(frozen=True)
class FullFlowLiveRunnerReadinessDomain:
    """One physical/automation domain in the V1.5 live-runner readiness review."""

    domain: str
    status: str
    reason: str
    stage_ids: tuple[str, ...]
    required_authorizations: tuple[str, ...] = ()
    physical_risk: str = ""
    next_action: str = ""

    def to_json(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class FullFlowLiveRunnerReadiness:
    """Reviewer-facing readiness summary for future one-button V1.5 execution."""

    schema: str
    run_id: str
    created_at: str
    one_button_live_runner_ready: bool
    current_automation_level: str
    summary: str
    ready_domains: tuple[str, ...]
    blocked_domains: tuple[str, ...]
    required_authorizations: tuple[str, ...]
    not_ready_reasons: tuple[str, ...]
    domains: tuple[FullFlowLiveRunnerReadinessDomain, ...]

    def to_json(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["domains"] = [domain.to_json() for domain in self.domains]
        return payload


def _resolve_optional(path: str | Path | None) -> str:
    if path is None or str(path).strip() == "":
        return ""
    return str(Path(path).resolve())


def _cmd(*parts: object) -> tuple[str, ...]:
    return tuple(str(part) for part in parts if str(part) != "")


def _python_module(module: str, *args: object) -> tuple[str, ...]:
    return _cmd("python", "-m", module, *args)


def _artifact(root: Path, *parts: str) -> str:
    return str(root.joinpath(*parts))


def _maybe_arg(flag: str, value: str | Path | None) -> tuple[str, ...]:
    text = _resolve_optional(value)
    if not text:
        return ()
    return (flag, text)


def _maybe_text_arg(flag: str, value: str | None) -> tuple[str, ...]:
    text = str(value or "").strip()
    if not text:
        return ()
    return (flag, text)


def _validate_steps(steps: Sequence[FullFlowStep]) -> None:
    for step in steps:
        if step.tool_module and not step.tool_module.startswith("gas_calibrator.tools."):
            raise ValueError(f"Step {step.step_id} does not use a tools module: {step.tool_module}")
        if step.tool_module and ".v2" in step.tool_module.lower():
            raise ValueError(f"Step {step.step_id} references V2: {step.tool_module}")
        if step.writes_device_id:
            raise ValueError(f"Step {step.step_id} is not allowed to write device IDs")


def _normalize_step_ids(values: Iterable[str] | None) -> tuple[str, ...]:
    out: list[str] = []
    for value in values or ():
        text = str(value or "").strip()
        if text and text not in out:
            out.append(text)
    return tuple(out)


def _has_unresolved_placeholder(command: Sequence[str]) -> bool:
    return any("<" in str(part) and ">" in str(part) for part in command)


def _step_by_id(plan: FullFlowPlan, step_id: str) -> FullFlowStep:
    for step in plan.steps:
        if step.step_id == step_id:
            return step
    raise KeyError(f"Unknown V1.5 full-flow step: {step_id}")


def _is_supervised_stage_safe(
    step: FullFlowStep,
    *,
    allow_real_com: bool = False,
    allow_database_import: bool = False,
) -> bool:
    if step.controls_pressure or step.controls_gas_route or step.controls_water_route:
        return False
    if step.writes_coefficients or step.writes_device_id:
        return False
    if "database" in step.execution_mode.lower() or step.step_id == "database_import":
        return bool(allow_database_import)
    if step.opens_com_ports:
        return bool(allow_real_com) and step.execution_mode == "read_only_real_com_requires_authorization"
    return step.execution_mode.startswith("offline")


def build_full_flow_state(
    plan: FullFlowPlan,
    *,
    completed_steps: Iterable[str] | None = None,
    failed_steps: Iterable[str] | None = None,
    allow_real_com: bool = False,
    allow_pressure_control: bool = False,
    allow_route_control: bool = False,
    allow_writes: bool = False,
) -> FullFlowState:
    """Build a resumable state view without executing any stage."""

    completed = set(_normalize_step_ids(completed_steps))
    failed = set(_normalize_step_ids(failed_steps))
    states: list[FullFlowStageState] = []
    previous_open_step: str | None = None

    for step in plan.steps:
        placeholders = _has_unresolved_placeholder(step.command)
        real_needed = bool(step.opens_com_ports)
        pressure_needed = bool(step.controls_pressure)
        route_needed = bool(step.controls_gas_route or step.controls_water_route)
        write_needed = bool(step.writes_coefficients)

        if step.step_id in completed:
            status = "completed"
            reason = "marked_completed_by_state_input"
            can_execute = False
        elif step.step_id in failed:
            status = "failed"
            reason = "marked_failed_by_state_input"
            can_execute = False
        elif previous_open_step:
            status = "pending_previous_stage"
            reason = f"waiting_for_{previous_open_step}"
            can_execute = False
        elif write_needed and not allow_writes:
            status = "blocked_write_authorization"
            reason = "coefficient_write_requires_separate_explicit_authorization"
            can_execute = False
        elif real_needed and not allow_real_com:
            status = "blocked_real_com_authorization"
            reason = "real_COM_stage_requires_explicit_operator_authorization"
            can_execute = False
        elif pressure_needed and not allow_pressure_control:
            status = "blocked_pressure_authorization"
            reason = "pressure_control_stage_requires_explicit_operator_authorization"
            can_execute = False
        elif route_needed and not allow_route_control:
            status = "blocked_route_authorization"
            reason = "gas_or_water_route_control_requires_explicit_operator_authorization"
            can_execute = False
        elif placeholders:
            status = "waiting_for_inputs"
            reason = "command_contains_review_placeholder_inputs"
            can_execute = False
        elif not step.command:
            status = "manual_review"
            reason = "stage_has_no_auto_command_in_full_flow_planner"
            can_execute = False
        else:
            status = "ready"
            reason = "ready_for_manual_or_supervised_execution"
            can_execute = True

        states.append(
            FullFlowStageState(
                step_id=step.step_id,
                title=step.title,
                phase=step.phase,
                status=status,
                reason=reason,
                can_execute_now=can_execute,
                command=step.command,
                requires_real_com_authorization=real_needed and not allow_real_com,
                requires_pressure_authorization=pressure_needed and not allow_pressure_control,
                requires_route_authorization=route_needed and not allow_route_control,
                requires_write_authorization=write_needed and not allow_writes,
                has_unresolved_placeholders=placeholders,
                opens_com_ports=step.opens_com_ports,
                controls_gas_route=step.controls_gas_route,
                controls_water_route=step.controls_water_route,
                writes_coefficients=step.writes_coefficients,
                coefficient_epoch_event=step.coefficient_epoch_event,
            )
        )
        if status not in {"completed", "skipped"} and previous_open_step is None:
            previous_open_step = step.step_id

    ready = tuple(stage.step_id for stage in states if stage.status == "ready")
    blocked = tuple(stage.step_id for stage in states if stage.status.startswith("blocked"))
    current = next((stage for stage in states if stage.status != "completed"), states[-1] if states else None)
    return FullFlowState(
        schema="v1_5_full_calibration_flow_state_v0",
        run_id=plan.run_id,
        created_at=datetime.now().isoformat(timespec="seconds"),
        current_step_id=current.step_id if current else "",
        current_status=current.status if current else "complete",
        ready_step_ids=ready,
        blocked_step_ids=blocked,
        completed_step_ids=tuple(item for item in plan_step_ids(plan) if item in completed),
        failed_step_ids=tuple(item for item in plan_step_ids(plan) if item in failed),
        allow_real_com=bool(allow_real_com),
        allow_pressure_control=bool(allow_pressure_control),
        allow_route_control=bool(allow_route_control),
        allow_writes=bool(allow_writes),
        stage_states=tuple(states),
    )


def plan_step_ids(plan: FullFlowPlan) -> tuple[str, ...]:
    return tuple(step.step_id for step in plan.steps)


def _stage_automation_state(step: FullFlowStep) -> str:
    if step.writes_coefficients:
        return "blocked_controlled_write"
    if step.controls_pressure:
        return "dedicated_pressure_runner_requires_authorization"
    if step.controls_gas_route or step.controls_water_route:
        return "dedicated_open_flow_runner_requires_authorization"
    if step.opens_com_ports:
        return "read_only_real_com_requires_authorization"
    if "database" in step.execution_mode.lower() or step.step_id == "database_import":
        return "offline_database_requires_dsn"
    if not step.command:
        return "manual_review_gate"
    if _has_unresolved_placeholder(step.command):
        return "offline_review_waiting_for_run_artifacts"
    if step.execution_mode.startswith("offline"):
        return "offline_review_auto_candidate"
    return "review_required"


def _stage_safety_boundaries(step: FullFlowStep) -> dict[str, Any]:
    return {
        "opens_com_ports": bool(step.opens_com_ports),
        "controls_pressure": bool(step.controls_pressure),
        "controls_gas_route": bool(step.controls_gas_route),
        "controls_water_route": bool(step.controls_water_route),
        "writes_coefficients": bool(step.writes_coefficients),
        "writes_device_id": bool(step.writes_device_id),
        "may_reuse_v1_shared_core": bool(step.may_reuse_v1_shared_core),
        "uses_validated_v1_5_entry": bool(step.uses_validated_v1_5_entry),
    }


def _stage_authorization_required(step: FullFlowStep) -> dict[str, bool]:
    return {
        "real_com": bool(step.opens_com_ports),
        "pressure_control": bool(step.controls_pressure),
        "route_control": bool(step.controls_gas_route or step.controls_water_route),
        "coefficient_write": bool(step.writes_coefficients),
        "device_id_write": bool(step.writes_device_id),
    }


def _stage_evidence_contract(step: FullFlowStep) -> dict[str, Any]:
    return {
        "required_inputs": list(step.required_inputs),
        "expected_outputs": list(step.expected_outputs),
        "raw_frames_preserved": step.step_id in {"co2_open_flow_sampling", "h2o_open_flow_sampling"},
        "reject_reasons_required": step.step_id
        in {
            "co2_open_flow_sampling",
            "h2o_open_flow_sampling",
            "fit_input_quality_review",
            "post_run_coefficient_executor",
            "full_flow_closure_readiness",
        },
        "readback_required": step.step_id
        in {
            "device_identity_and_getco_snapshot",
            "auxiliary_senco56789_neutralization_gate",
            "controlled_component_write_placeholder",
        },
        "post_write_reverify_required": step.step_id == "controlled_component_write_placeholder",
        "database_or_report_only": step.phase in {
            "DATABASE_IMPORT",
            "REPORTS",
            "FINAL_EVIDENCE_STATUS",
            "FORMAL_RUN_STATUS",
        },
    }


def build_full_flow_stage_manifest(plan: FullFlowPlan) -> FullFlowStageManifest:
    """Build a stable manifest for UI, audit, and future full-flow executors."""

    stages: list[FullFlowStageManifestEntry] = []
    automation_counts: dict[str, int] = {}
    for index, step in enumerate(plan.steps, start=1):
        automation_state = _stage_automation_state(step)
        automation_counts[automation_state] = automation_counts.get(automation_state, 0) + 1
        stages.append(
            FullFlowStageManifestEntry(
                order=index,
                step_id=step.step_id,
                title=step.title,
                phase=step.phase,
                tool_module=step.tool_module or "",
                automation_state=automation_state,
                execution_mode=step.execution_mode,
                gate=step.gate,
                required_inputs=step.required_inputs,
                expected_outputs=step.expected_outputs,
                physical_meaning=step.physical_meaning,
                command=step.command,
                safety_boundaries=_stage_safety_boundaries(step),
                authorization_required=_stage_authorization_required(step),
                evidence_contract=_stage_evidence_contract(step),
                coefficient_epoch_event=step.coefficient_epoch_event,
                notes=step.notes,
            )
        )

    return FullFlowStageManifest(
        schema=STAGE_MANIFEST_SCHEMA,
        run_id=plan.run_id,
        contract=plan.contract,
        created_at=datetime.now().isoformat(timespec="seconds"),
        current_automation_level="supervised_tool_chain_with_controlled_live_gates",
        one_button_live_runner_ready=False,
        safety_summary={
            "does_not_modify_run_app": bool(plan.safety_contract.get("does_not_modify_run_app")),
            "planner_opens_com_ports": bool(plan.safety_contract.get("planner_opens_com_ports")),
            "planner_controls_routes": bool(plan.safety_contract.get("planner_controls_routes")),
            "planner_writes_coefficients": bool(plan.safety_contract.get("planner_writes_coefficients")),
            "planner_writes_device_id": bool(plan.safety_contract.get("planner_writes_device_id")),
            "identity_key": plan.coefficient_epoch_contract.get("identity_key", ""),
            "live_runner_readiness_artifact": "v1_5_full_flow_live_runner_readiness.json",
            "not_one_button_live_runner_reason": (
                "pressure, open-flow route, and coefficient-write stages remain explicit controlled gates"
            ),
        },
        automation_summary=dict(sorted(automation_counts.items())),
        stages=tuple(stages),
    )


def _existing_stage_ids(plan: FullFlowPlan, stage_ids: Iterable[str]) -> tuple[str, ...]:
    known = {step.step_id for step in plan.steps}
    return tuple(step_id for step_id in stage_ids if step_id in known)


def _readiness_domain(
    plan: FullFlowPlan,
    *,
    domain: str,
    status: str,
    reason: str,
    stage_ids: Iterable[str],
    required_authorizations: Iterable[str] = (),
    physical_risk: str = "",
    next_action: str = "",
) -> FullFlowLiveRunnerReadinessDomain:
    return FullFlowLiveRunnerReadinessDomain(
        domain=domain,
        status=status,
        reason=reason,
        stage_ids=_existing_stage_ids(plan, stage_ids),
        required_authorizations=tuple(dict.fromkeys(str(item) for item in required_authorizations if str(item))),
        physical_risk=physical_risk,
        next_action=next_action,
    )


def build_full_flow_live_runner_readiness(plan: FullFlowPlan) -> FullFlowLiveRunnerReadiness:
    """Build an offline readiness review for a future one-button V1.5 runner.

    The readiness artifact does not grant live authority. It summarizes which
    physical domains are already supervised offline and which still require a
    controlled live gate before an executor may run hardware or write SENCO.
    """

    domains = (
        _readiness_domain(
            plan,
            domain="offline_planning",
            status="ready_offline_supervised",
            reason="plan, state, stage manifest, contract audit, operation console, and offline status can be generated without hardware",
            stage_ids=(
                "load_plan_and_traceability",
                "post_run_coefficient_executor",
                "full_flow_closure_readiness",
                "formal_evidence_sidecar",
                "formal_database_dry_run_snapshot",
                "formal_database_import_preflight_snapshot",
                "formal_database_import_authorization_snapshot",
                "formal_database_import_command_contract_snapshot",
                "formal_database_import_blocked_executor_snapshot",
                "formal_database_import_controlled_executor_design_snapshot",
                "database_import",
                "final_evidence_status_refresh",
                "algorithm_profile_runner_dry_run_snapshot",
                "formal_run_status_snapshot",
            ),
            physical_risk="none during generation; artifacts remain review evidence, not real acceptance",
            next_action="keep these stages as offline prerequisites before any controlled live step",
        ),
        _readiness_domain(
            plan,
            domain="initialization_contract",
            status="ready_offline_supervised",
            reason="formal initialization contract, PostgreSQL 18 sidecar, readiness snapshot, and pre-gas gap list can be generated offline before live identity gates",
            stage_ids=(
                "formal_initialization_contract_plan",
                "formal_initialization_executor_dry_run_snapshot",
                "formal_initialization_blocked_executor_snapshot",
                "formal_initialization_controlled_executor_design_snapshot",
                "formal_initialization_readonly_com_preflight_design_snapshot",
                "formal_initialization_readonly_com_preflight_blocked_executor_snapshot",
                "initialization_readiness_snapshot",
                "pre_gas_readiness_snapshot",
            ),
            physical_risk="none during generation; missing live GETCO/SENCO/CHECK evidence remains a gate rather than being repaired automatically",
            next_action="review the generated initialization plan, then run only the dedicated V1.5 identity/SN/auxiliary tools that have explicit authorization",
        ),
        _readiness_domain(
            plan,
            domain="identity_and_epoch0",
            status="requires_real_com_authorization",
            reason="formal calibration must bind every COM transport to analyzer device ID, freeze GETCO1-9, and pass the offline identity/GETCO readiness sidecar before sampling",
            stage_ids=("device_identity_and_getco_snapshot", "identity_getco_readiness_snapshot"),
            required_authorizations=("real_com",),
            physical_risk="wrong ID/COM binding corrupts traceability and may write coefficients to the wrong analyzer",
            next_action="run the validated identity/GETCO snapshot with 1s or longer command spacing, then verify the no-write epoch-0 evidence offline",
        ),
        _readiness_domain(
            plan,
            domain="auxiliary_coefficients",
            status="blocked_controlled_write",
            reason="S5/S6 display trims and S7/S8/S9 input channels must be backed up, neutralized, repaired, or explicitly modeled",
            stage_ids=("auxiliary_senco56789_neutralization_gate",),
            required_authorizations=("real_com", "coefficient_write"),
            physical_risk="old output trims or bad T/P input coefficients can be silently absorbed into CO2/H2O candidate fits",
            next_action="backup old coefficients; neutralize S5/S6 when appropriate; review S7/S8/S9 before component fitting",
        ),
        _readiness_domain(
            plan,
            domain="pressure_channel",
            status="requires_pressure_authorization",
            reason="analyzer pressure P is an input to CO2/H2O firmware calculations and must be verified or calibrated first",
            stage_ids=(
                "pressure_quick_check",
                "pressure_senco9_no_write_acquisition",
                "pressure_senco9_no_write_review",
            ),
            required_authorizations=("real_com", "pressure_control"),
            physical_risk="bad internal pressure contaminates component outputs and makes pressure compensation impossible to interpret",
            next_action="run pressure SENCO9 acquisition/review with the restored closed-volume PACE control contract before component sampling",
        ),
        _readiness_domain(
            plan,
            domain="temperature_channel",
            status="offline_review_waiting_for_temperature_evidence",
            reason="temperature evidence can be reviewed offline, but abnormal S7/S8 must be repaired before component acceptance",
            stage_ids=("temperature_channel_fast_review", "temperature_senco78_candidate_review"),
            required_authorizations=("real_com",),
            physical_risk="bad chamber/case temperature input can be absorbed into CO2/H2O coefficients across temperature groups",
            next_action="use digital thermometer evidence to review S7/S8; do single-point repair only as documented recovery, not final full-range proof",
        ),
        _readiness_domain(
            plan,
            domain="co2_open_flow",
            status="requires_route_authorization",
            reason="CO2 sampling controls gas valves and must prove clean open-flow gas, dewpoint condition, ratio stability, and per-device sample eligibility",
            stage_ids=("co2_open_flow_sampling",),
            required_authorizations=("real_com", "route_control"),
            physical_risk="sampling after valve closure, dirty/damp line state, or unstable ratio creates non-representative CO2 fit data",
            next_action="run validated CO2 open-flow runner; sample only while the gas route remains open and preserve per-device reject reasons",
        ),
        _readiness_domain(
            plan,
            domain="h2o_open_flow",
            status="requires_route_authorization",
            reason="H2O sampling controls water route/HGEN and must prove dewpoint, H2O ratio, dry/wet ppmv, and reference evidence stability",
            stage_ids=("h2o_open_flow_sampling",),
            required_authorizations=("real_com", "route_control"),
            physical_risk="HGEN cycling, wet line memory, or unstable dewpoint can produce water evidence that is not a stable humidity state",
            next_action="run validated H2O open-flow runner with continuous HGEN strategy during the water route and per-device grading",
        ),
        _readiness_domain(
            plan,
            domain="candidate_fit_and_qc",
            status="offline_review_waiting_for_run_artifacts",
            reason="candidate fitting is offline-ready after raw open-flow samples, factory-signal health, and fit-input QC exist",
            stage_ids=("factory_signal_health_review", "fit_input_quality_review", "co2_candidate_write_review"),
            physical_risk="optical saturation, invalid frames, wrong low-end anchors, or unmodeled trims can make offline residuals disagree with real output",
            next_action="use all traceable stable fit-eligible points while keeping CO2 zero gas and H2O dry-gas anchors conceptually separate",
        ),
        _readiness_domain(
            plan,
            domain="controlled_write_and_reverify",
            status="blocked_controlled_write",
            reason="SENCO writes change the analyzer measurement model and require per-device review, readback, rollback plan, and independent reverification",
            stage_ids=("controlled_component_write_placeholder", "post_write_reverification_placeholder"),
            required_authorizations=("real_com", "coefficient_write", "route_control"),
            physical_risk="wrong coefficient epoch or missing post-write proof can create a calibrated-looking but invalid analyzer",
            next_action="write only reviewed per-device candidates, then run independent open-flow reverification before release",
        ),
        _readiness_domain(
            plan,
            domain="archive_and_release",
            status="offline_review_waiting_for_run_artifacts",
            reason="database import, evidence status refresh, and reports are offline-ready once final run artifacts exist",
            stage_ids=("database_import", "zh_calibration_reports", "final_evidence_status_refresh"),
            physical_risk="if evidence status is stale, the operator may believe a run is complete while audit artifacts remain incomplete",
            next_action="refresh evidence status after acquisition/write/report steps; generate per-device certificates from the evidence package",
        ),
    )

    ready_statuses = {"ready", "ready_offline_supervised"}
    ready_domains = tuple(domain.domain for domain in domains if domain.status in ready_statuses)
    blocked_domains = tuple(domain.domain for domain in domains if domain.status not in ready_statuses)
    authorization_order = ("real_com", "pressure_control", "route_control", "coefficient_write")
    required = {
        authorization
        for domain in domains
        for authorization in domain.required_authorizations
        if authorization
    }
    not_ready_reasons = tuple(
        f"{domain.domain}: {domain.reason}" for domain in domains if domain.status not in ready_statuses
    )

    return FullFlowLiveRunnerReadiness(
        schema=LIVE_RUNNER_READINESS_SCHEMA,
        run_id=plan.run_id,
        created_at=datetime.now().isoformat(timespec="seconds"),
        one_button_live_runner_ready=False,
        current_automation_level="supervised_tool_chain_with_controlled_live_gates",
        summary=(
            "V1.5 can generate an auditable supervised plan and offline review chain, "
            "but it is not yet a one-button unattended live runner because identity, "
            "pressure, CO2/H2O route control, and SENCO writes remain explicit controlled gates."
        ),
        ready_domains=ready_domains,
        blocked_domains=blocked_domains,
        required_authorizations=tuple(item for item in authorization_order if item in required),
        not_ready_reasons=not_ready_reasons,
        domains=domains,
    )


def run_supervised_full_flow(
    plan: FullFlowPlan,
    *,
    completed_steps: Iterable[str] | None = None,
    failed_steps: Iterable[str] | None = None,
    allow_real_com: bool = False,
    allow_pressure_control: bool = False,
    allow_route_control: bool = False,
    allow_writes: bool = False,
    allow_database_import: bool = False,
    execute_commands: bool = False,
    max_steps: int = 1,
    output_dir: str | Path | None = None,
    cwd: str | Path | None = None,
) -> FullFlowSupervisedRun:
    """Advance a V1.5 plan through safe offline stages only.

    The supervisor never executes stages that open COM ports, control pressure,
    touch gas/water routes, write device IDs, or write SENCO coefficients. Those
    stages remain visible in the state file for operator review and explicit
    real-run tooling.
    """

    completed = set(_normalize_step_ids(completed_steps))
    failed = set(_normalize_step_ids(failed_steps))
    initial_state = build_full_flow_state(
        plan,
        completed_steps=completed,
        failed_steps=failed,
        allow_real_com=allow_real_com,
        allow_pressure_control=allow_pressure_control,
        allow_route_control=allow_route_control,
        allow_writes=allow_writes,
    )
    events: list[FullFlowExecutionEvent] = []
    log_root = Path(output_dir or plan.output_dir).resolve() / "v1_5_full_flow_supervised_execution"
    if execute_commands:
        log_root.mkdir(parents=True, exist_ok=True)

    for _ in range(max(0, int(max_steps))):
        state = build_full_flow_state(
            plan,
            completed_steps=completed,
            failed_steps=failed,
            allow_real_com=allow_real_com,
            allow_pressure_control=allow_pressure_control,
            allow_route_control=allow_route_control,
            allow_writes=allow_writes,
        )
        if state.current_status != "ready" or not state.current_step_id:
            events.append(
                FullFlowExecutionEvent(
                    step_id=state.current_step_id,
                    status="stopped",
                    reason=f"current_stage_is_{state.current_status}",
                )
            )
            break

        step = _step_by_id(plan, state.current_step_id)
        if not _is_supervised_stage_safe(
            step,
            allow_real_com=allow_real_com,
            allow_database_import=allow_database_import,
        ):
            events.append(
                FullFlowExecutionEvent(
                    step_id=step.step_id,
                    status="blocked_non_offline_stage",
                    reason="supervisor_refuses_pressure_route_write_or_unauthorized_COM_stage",
                    command=step.command,
                )
            )
            break
        if not execute_commands:
            events.append(
                FullFlowExecutionEvent(
                    step_id=step.step_id,
                    status="planned_only",
                    reason="execute_commands_false_no_stage_was_run",
                    command=step.command,
                )
            )
            break

        event_index = len(events) + 1
        safe_name = "".join(ch if ch.isalnum() or ch in {"_", "-"} else "_" for ch in step.step_id)
        stdout_path = log_root / f"{event_index:02d}_{safe_name}_stdout.log"
        stderr_path = log_root / f"{event_index:02d}_{safe_name}_stderr.log"
        try:
            completed_process = subprocess.run(
                list(step.command),
                cwd=str(Path(cwd).resolve()) if cwd else None,
                capture_output=True,
                text=True,
                check=False,
            )
        except OSError as exc:
            stderr_path.write_text(str(exc), encoding="utf-8")
            failed.add(step.step_id)
            events.append(
                FullFlowExecutionEvent(
                    step_id=step.step_id,
                    status="failed",
                    reason="subprocess_start_failed",
                    command=step.command,
                    returncode=None,
                    stdout_path=str(stdout_path),
                    stderr_path=str(stderr_path),
                )
            )
            break

        stdout_path.write_text(completed_process.stdout or "", encoding="utf-8")
        stderr_path.write_text(completed_process.stderr or "", encoding="utf-8")
        if completed_process.returncode == 0:
            completed.add(step.step_id)
            events.append(
                FullFlowExecutionEvent(
                    step_id=step.step_id,
                    status="completed",
                    reason="offline_command_returned_zero",
                    command=step.command,
                    returncode=completed_process.returncode,
                    stdout_path=str(stdout_path),
                    stderr_path=str(stderr_path),
                )
            )
        else:
            failed.add(step.step_id)
            events.append(
                FullFlowExecutionEvent(
                    step_id=step.step_id,
                    status="failed",
                    reason="offline_command_returned_nonzero",
                    command=step.command,
                    returncode=completed_process.returncode,
                    stdout_path=str(stdout_path),
                    stderr_path=str(stderr_path),
                )
            )
            break

    final_state = build_full_flow_state(
        plan,
        completed_steps=completed,
        failed_steps=failed,
        allow_real_com=allow_real_com,
        allow_pressure_control=allow_pressure_control,
        allow_route_control=allow_route_control,
        allow_writes=allow_writes,
    )
    return FullFlowSupervisedRun(
        schema="v1_5_full_calibration_supervised_run_v0",
        run_id=plan.run_id,
        created_at=datetime.now().isoformat(timespec="seconds"),
        execute_commands=bool(execute_commands),
        max_steps=max(0, int(max_steps)),
        allow_database_import=bool(allow_database_import),
        initial_state=initial_state,
        final_state=final_state,
        events=tuple(events),
    )


def build_full_flow_plan(
    *,
    config_path: str | Path,
    output_dir: str | Path,
    run_id: str | None = None,
    operator: str = "",
    analyzer_id: str = "multi_device",
    pressure_reference_json: str | Path | None = None,
    standard_gases_json: str | Path | None = None,
    co2_queue_csv: str | Path | None = None,
    h2o_queue_csv: str | Path | None = None,
    temperature_h2o_points_parent: str | Path | None = None,
    reviewed_run_dir: str | Path | None = None,
    evidence_bundle_json: str | Path | None = None,
    reviewer: str = "",
    approver: str = "",
) -> FullFlowPlan:
    """Build the V1.5 full-chain plan without touching devices."""

    cfg = Path(config_path).resolve()
    root = Path(output_dir).resolve()
    repo_root = Path(__file__).resolve().parents[4]
    rid = run_id or f"v1_5_full_flow_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    formal_pkg = root / "formal_run_package"
    formal_initialization_dir = root / "formal_initialization"
    formal_initialization_executor_dry_run_dir = root / "formal_initialization_executor_dry_run"
    formal_initialization_blocked_executor_dir = root / "formal_initialization_blocked_executor"
    formal_initialization_controlled_executor_design_dir = (
        root / "formal_initialization_controlled_executor_design"
    )
    formal_initialization_readonly_com_preflight_design_dir = (
        root / "formal_initialization_readonly_com_preflight_design"
    )
    formal_initialization_readonly_com_preflight_blocked_executor_dir = (
        root / "formal_initialization_readonly_com_preflight_blocked_executor"
    )
    pre_gas_readiness_dir = root / "pre_gas_readiness"
    getco_dir = root / "coefficient_epoch_0_getco_snapshot"
    getco_readiness_dir = root / "identity_getco_readiness"
    aux_neutral_dir = root / "auxiliary_senco56789_neutralization"
    pressure_dir = root / "pressure_channel"
    co2_dir = root / "co2_open_flow"
    h2o_dir = root / "h2o_open_flow"
    temp_dir = root / "temperature_channel_review"
    factory_signal_dir = root / "factory_signal_health_review"
    fit_quality_dir = root / "fit_input_quality"
    post_run_executor_dir = root / "post_run_coefficient_executor"
    closure_readiness_dir = root / "full_flow_closure_readiness"
    post_write_verify_dir = root / "post_write_reverification"
    reports_dir = root / "reports"
    formal_status_dir = root / "formal_run_status"
    algorithm_profile_runner_dir = root / "algorithm_profile_runner_dry_run"
    formal_database_dry_run_dir = root / "formal_database_dry_run"
    formal_database_import_preflight_dir = root / "formal_database_import_preflight"
    formal_database_import_authorization_dir = root / "formal_database_import_authorization"
    formal_database_import_command_contract_dir = root / "formal_database_import_command_contract"
    formal_database_import_blocked_executor_dir = root / "formal_database_import_blocked_executor"
    formal_database_import_controlled_executor_design_dir = (
        root / "formal_database_import_controlled_executor_design"
    )
    algorithm_profile_path = repo_root / "configs" / "v1_5_algorithm_route_profiles.json"
    runtime_bound_cfg = getco_dir / "runtime_identity_bound_config.json"

    pressure_ref = _resolve_optional(pressure_reference_json)
    gases = _resolve_optional(standard_gases_json)
    co2_queue = _resolve_optional(co2_queue_csv)
    h2o_queue = _resolve_optional(h2o_queue_csv)
    h2o_points_parent = _resolve_optional(temperature_h2o_points_parent) or str(h2o_dir)
    run_dir = _resolve_optional(reviewed_run_dir) or "<completed_v1_5_run_dir>"
    bundle = _resolve_optional(evidence_bundle_json) or _artifact(run_dir_as_path(run_dir), "formal_evidence_sidecar", "evidence_bundle.json")

    steps: list[FullFlowStep] = []

    steps.append(
        FullFlowStep(
            step_id="load_plan_and_traceability",
            title="Prepare V1.5 formal plan and certificate snapshots",
            phase="LOAD_PLAN",
            tool_module="gas_calibrator.tools.prepare_v1_5_formal_run_package",
            command=_python_module(
                "gas_calibrator.tools.prepare_v1_5_formal_run_package",
                "--output-dir",
                formal_pkg,
                "--operator",
                operator or "<operator>",
                "--analyzer-id",
                analyzer_id,
                "--run-id",
                rid,
                "--config",
                cfg,
                *_maybe_arg("--standard-gases-json", gases),
                *_maybe_arg("--pressure-reference-json", pressure_ref),
            ),
            required_inputs=("runtime config", "standard gas certificates", "COM22 pressure certificate"),
            expected_outputs=("formal_plan_snapshot.json", "com22_pressure_reference.json", "evidence_run_manifest.json"),
            physical_meaning="Freeze traceability inputs before any physical sampling.",
            execution_mode="offline_sidecar",
            gate="required_before_sampling",
        )
    )

    steps.append(
        FullFlowStep(
            step_id="formal_initialization_contract_plan",
            title="Generate formal initialization contract and DB bundle",
            phase="INITIALIZATION_CONTRACT",
            tool_module="gas_calibrator.tools.run_v1_5_formal_initialization_runner",
            command=_python_module(
                "gas_calibrator.tools.run_v1_5_formal_initialization_runner",
                "--config",
                cfg,
                "--output-dir",
                formal_initialization_dir,
                "--run-id",
                f"{rid}_initialization",
                *_maybe_text_arg("--operator", operator),
                *_maybe_text_arg("--reviewer", reviewer),
                *_maybe_text_arg("--approver", approver),
            ),
            required_inputs=("runtime config", "operator identity for traceability"),
            expected_outputs=(
                "formal_initialization/v1_5_formal_initialization_plan.json",
                "formal_initialization/v1_5_formal_initialization_contract.json",
                "formal_initialization/v1_5_formal_initialization_db_bundle.json",
                "formal_initialization/v1_5_formal_initialization_commands.ps1",
            ),
            physical_meaning=(
                "Before any real analyzer contact, freeze the V1.5 initialization contract: "
                "SN/device_code identity, PostgreSQL 18 DB preflight, MODE2/1Hz/filter setup, "
                "S7/S8 neutral policy, CHECK monitor timing, and route-readiness gates. "
                "This planner command does not execute SN writes, COM reads, SENCO writes, pressure control, or open-flow routes."
            ),
            execution_mode="offline_sidecar",
            gate="required_before_identity_getco_snapshot",
            notes=(
                "Do not pass --execute from the full-flow supervised planner.",
                "The generated initialization commands remain a review artifact until a dedicated controlled tool is authorized.",
            ),
        )
    )

    steps.append(
        FullFlowStep(
            step_id="formal_initialization_executor_dry_run_snapshot",
            title="Review formal initialization executor dry-run boundary",
            phase="INITIALIZATION_EXECUTOR_DRY_RUN",
            tool_module="gas_calibrator.tools.export_v1_5_formal_initialization_executor_dry_run",
            command=_python_module(
                "gas_calibrator.tools.export_v1_5_formal_initialization_executor_dry_run",
                "--formal-initialization-plan-json",
                formal_initialization_dir / "v1_5_formal_initialization_plan.json",
                "--output-dir",
                formal_initialization_executor_dry_run_dir,
            ),
            required_inputs=("formal initialization plan JSON",),
            expected_outputs=(
                "formal_initialization_executor_dry_run/v1_5_formal_initialization_executor_dry_run.json",
                "formal_initialization_executor_dry_run/V1_5_FORMAL_INITIALIZATION_EXECUTOR_DRY_RUN.md",
                "formal_initialization_executor_dry_run/v1_5_formal_initialization_executor_dry_run_steps.csv",
                "formal_initialization_executor_dry_run/v1_5_formal_initialization_executor_dry_run_checks.csv",
            ),
            physical_meaning=(
                "Classify the formal initialization plan before any executor use: offline dry-run commands, "
                "read-only real-COM steps, and controlled-write steps stay visibly separated. This sidecar "
                "does not pass --execute, open COM, write SN/device_code, write SENCO, connect PostgreSQL, "
                "or control pressure/routes."
            ),
            execution_mode="offline_sidecar",
            gate="required_before_initialization_executor_use",
            notes=(
                "This is a review sidecar for automation wiring; it is not a live initialization runner.",
                "Future live executor work must be a separate reviewed PR with explicit unlocks and readback evidence.",
            ),
        )
    )

    steps.append(
        FullFlowStep(
            step_id="formal_initialization_blocked_executor_snapshot",
            title="Run blocked formal initialization executor stub without COM",
            phase="INITIALIZATION_BLOCKED_EXECUTOR",
            tool_module="gas_calibrator.tools.run_v1_5_formal_initialization_blocked_executor",
            command=_python_module(
                "gas_calibrator.tools.run_v1_5_formal_initialization_blocked_executor",
                "--formal-initialization-executor-dry-run-json",
                formal_initialization_executor_dry_run_dir / "v1_5_formal_initialization_executor_dry_run.json",
                "--formal-initialization-plan-json",
                formal_initialization_dir / "v1_5_formal_initialization_plan.json",
                "--output-dir",
                formal_initialization_blocked_executor_dir,
                "--fail-on-blocked",
            ),
            required_inputs=("formal initialization executor dry-run sidecar", "formal initialization plan JSON"),
            expected_outputs=(
                "formal_initialization_blocked_executor/v1_5_formal_initialization_blocked_executor.json",
                "formal_initialization_blocked_executor/V1_5_FORMAL_INITIALIZATION_BLOCKED_EXECUTOR.md",
                "formal_initialization_blocked_executor/v1_5_formal_initialization_blocked_executor_checks.csv",
                "formal_initialization_blocked_executor/v1_5_formal_initialization_blocked_executor_summary.csv",
            ),
            physical_meaning=(
                "Invoke the future initialization executor only in blocked-stub mode. This proves the entrypoint "
                "consumes the reviewed plan/dry-run artifacts and still refuses live COM, SN/device_code writes, "
                "SENCO writes, PostgreSQL access, pressure control, and gas/water route control."
            ),
            execution_mode="offline_sidecar",
            gate="required_before_initialization_executor_live_review",
            notes=(
                "This stub intentionally returns non-zero when --fail-on-blocked is used, proving live initialization remains locked.",
                "A future real initialization executor must be a separate controlled package with explicit authorization and readback evidence.",
            ),
        )
    )

    steps.append(
        FullFlowStep(
            step_id="formal_initialization_controlled_executor_design_snapshot",
            title="Review future controlled initialization executor design",
            phase="INITIALIZATION_CONTROLLED_EXECUTOR_DESIGN",
            tool_module="gas_calibrator.tools.export_v1_5_formal_initialization_controlled_executor_design",
            command=_python_module(
                "gas_calibrator.tools.export_v1_5_formal_initialization_controlled_executor_design",
                "--formal-initialization-blocked-executor-json",
                formal_initialization_blocked_executor_dir / "v1_5_formal_initialization_blocked_executor.json",
                "--output-dir",
                formal_initialization_controlled_executor_design_dir,
            ),
            required_inputs=("formal initialization blocked executor JSON",),
            expected_outputs=(
                "formal_initialization_controlled_executor_design/v1_5_formal_initialization_controlled_executor_design.json",
                "formal_initialization_controlled_executor_design/V1_5_FORMAL_INITIALIZATION_CONTROLLED_EXECUTOR_DESIGN.md",
                "formal_initialization_controlled_executor_design/v1_5_formal_initialization_controlled_executor_authorization_contract.csv",
                "formal_initialization_controlled_executor_design/v1_5_formal_initialization_controlled_executor_real_com_contract.csv",
                "formal_initialization_controlled_executor_design/v1_5_formal_initialization_controlled_executor_controlled_write_contract.csv",
                "formal_initialization_controlled_executor_design/v1_5_formal_initialization_controlled_executor_readback_contract.csv",
                "formal_initialization_controlled_executor_design/v1_5_formal_initialization_controlled_executor_hold_contract.csv",
                "formal_initialization_controlled_executor_design/v1_5_formal_initialization_controlled_executor_boundary_gates.csv",
            ),
            physical_meaning=(
                "Freeze the future live initialization executor contract without running it: explicit controlled "
                "initialization authorization, separate read-only real-COM and controlled-write unlocks, 1 to 6 "
                "active analyzer scope, >=1s serial command spacing, SN/device_code readback, GETCO epoch-0, "
                "S7/S8 neutral temperature policy, CHECK-capable analyzer handling after all active chambers are stable, "
                "and hold policies for identity/write/readback failures."
            ),
            execution_mode="offline_sidecar",
            gate="required_before_initialization_executor_live_implementation",
            notes=(
                "This design exporter does not implement --execute-controlled-initialization.",
                "It does not open COM, write SN/device_code, write SENCO, connect PostgreSQL, control pressure, or control gas/water routes.",
            ),
        )
    )

    steps.append(
        FullFlowStep(
            step_id="formal_initialization_readonly_com_preflight_design_snapshot",
            title="Review future read-only initialization COM preflight design",
            phase="INITIALIZATION_READONLY_COM_PREFLIGHT_DESIGN",
            tool_module="gas_calibrator.tools.export_v1_5_formal_initialization_readonly_com_preflight_design",
            command=_python_module(
                "gas_calibrator.tools.export_v1_5_formal_initialization_readonly_com_preflight_design",
                "--formal-initialization-controlled-executor-design-json",
                formal_initialization_controlled_executor_design_dir
                / "v1_5_formal_initialization_controlled_executor_design.json",
                "--output-dir",
                formal_initialization_readonly_com_preflight_design_dir,
            ),
            required_inputs=("formal initialization controlled executor design JSON",),
            expected_outputs=(
                "formal_initialization_readonly_com_preflight_design/v1_5_formal_initialization_readonly_com_preflight_design.json",
                "formal_initialization_readonly_com_preflight_design/V1_5_FORMAL_INITIALIZATION_READONLY_COM_PREFLIGHT_DESIGN.md",
                "formal_initialization_readonly_com_preflight_design/v1_5_formal_initialization_readonly_com_preflight_authorization_contract.csv",
                "formal_initialization_readonly_com_preflight_design/v1_5_formal_initialization_readonly_com_preflight_serial_contract.csv",
                "formal_initialization_readonly_com_preflight_design/v1_5_formal_initialization_readonly_com_preflight_identity_read_contract.csv",
                "formal_initialization_readonly_com_preflight_design/v1_5_formal_initialization_readonly_com_preflight_getco_read_contract.csv",
                "formal_initialization_readonly_com_preflight_design/v1_5_formal_initialization_readonly_com_preflight_check_read_contract.csv",
                "formal_initialization_readonly_com_preflight_design/v1_5_formal_initialization_readonly_com_preflight_failure_hold_contract.csv",
                "formal_initialization_readonly_com_preflight_design/v1_5_formal_initialization_readonly_com_preflight_boundary_gates.csv",
            ),
            physical_meaning=(
                "Freeze the future read-only real-COM preflight contract without touching analyzers: reviewed "
                "1 to 6 active ports, >=1s command/retry spacing, protocol ID plus 8-digit SN/device_code, "
                "GETCO1-9 epoch-0 snapshot, CHECK only for CHECK-capable/new-algorithm analyzers after all "
                "active chambers are stable, old-algorithm CHECK skip, and hold policies for serial, identity, "
                "GETCO, CHECK, or pacing failures."
            ),
            execution_mode="offline_sidecar",
            gate="required_before_readonly_real_com_preflight_implementation",
            notes=(
                "This design exporter does not implement --execute-read-only-real-com.",
                "It does not open COM, write SN/device_code, write SENCO, connect PostgreSQL, control pressure, or control gas/water routes.",
            ),
        )
    )

    steps.append(
        FullFlowStep(
            step_id="formal_initialization_readonly_com_preflight_blocked_executor_snapshot",
            title="Run blocked read-only initialization COM preflight stub without COM",
            phase="INITIALIZATION_READONLY_COM_PREFLIGHT_BLOCKED_EXECUTOR",
            tool_module="gas_calibrator.tools.run_v1_5_formal_initialization_readonly_com_preflight_blocked_executor",
            command=_python_module(
                "gas_calibrator.tools.run_v1_5_formal_initialization_readonly_com_preflight_blocked_executor",
                "--formal-initialization-readonly-com-preflight-design-json",
                formal_initialization_readonly_com_preflight_design_dir
                / "v1_5_formal_initialization_readonly_com_preflight_design.json",
                "--output-dir",
                formal_initialization_readonly_com_preflight_blocked_executor_dir,
                "--fail-on-blocked",
            ),
            required_inputs=("formal initialization read-only COM preflight design JSON",),
            expected_outputs=(
                "formal_initialization_readonly_com_preflight_blocked_executor/v1_5_formal_initialization_readonly_com_preflight_blocked_executor.json",
                "formal_initialization_readonly_com_preflight_blocked_executor/V1_5_FORMAL_INITIALIZATION_READONLY_COM_PREFLIGHT_BLOCKED_EXECUTOR.md",
                "formal_initialization_readonly_com_preflight_blocked_executor/v1_5_formal_initialization_readonly_com_preflight_blocked_executor_checks.csv",
                "formal_initialization_readonly_com_preflight_blocked_executor/v1_5_formal_initialization_readonly_com_preflight_blocked_executor_summary.csv",
            ),
            physical_meaning=(
                "Invoke the future read-only COM preflight only in blocked-stub mode. This proves the entrypoint "
                "consumes the reviewed design artifact and still refuses analyzer serial contact, SN/device_code "
                "writes, SENCO writes, PostgreSQL access, pressure control, and gas/water route control."
            ),
            execution_mode="offline_sidecar",
            gate="required_before_readonly_real_com_preflight_live_review",
            notes=(
                "This stub intentionally returns non-zero when --fail-on-blocked is used, proving read-only COM remains locked.",
                "A future real read-only COM preflight must be a separate controlled package with explicit authorization and read evidence.",
            ),
        )
    )

    steps.append(
        FullFlowStep(
            step_id="initialization_readiness_snapshot",
            title="Export initialization readiness sidecar before live gates",
            phase="INITIALIZATION_READINESS",
            tool_module="gas_calibrator.tools.export_v1_5_initialization_readiness",
            command=_python_module(
                "gas_calibrator.tools.export_v1_5_initialization_readiness",
                "--run-dir",
                formal_initialization_dir,
                "--config",
                cfg,
                "--getco-snapshot-dir",
                getco_dir,
                "--aux-neutralization-dir",
                aux_neutral_dir,
                "--output-dir",
                formal_initialization_dir,
            ),
            required_inputs=("runtime config", "planned initialization evidence directory"),
            expected_outputs=(
                "formal_initialization/v1_5_initialization_readiness.json",
                "formal_initialization/v1_5_initialization_readiness.md",
                "formal_initialization/v1_5_initialization_database_sidecar.json",
            ),
            physical_meaning=(
                "Generate the offline readiness/sidecar view that explains which initialization evidence is present "
                "or missing before the first real COM identity gate. Missing live evidence remains a gate; this step only reports it."
            ),
            execution_mode="offline_sidecar",
            gate="required_before_identity_getco_snapshot",
            notes=(
                "This readiness exporter reads files only; it does not open COM or connect to PostgreSQL.",
                "PostgreSQL 18 and SN/device_code identity remain pre-open-flow requirements, not implicit repairs.",
            ),
        )
    )

    steps.append(
        FullFlowStep(
            step_id="pre_gas_readiness_snapshot",
            title="Summarize pre-gas readiness gates before live identity",
            phase="PRE_GAS_READINESS",
            tool_module="gas_calibrator.tools.export_v1_5_pre_gas_readiness",
            command=_python_module(
                "gas_calibrator.tools.export_v1_5_pre_gas_readiness",
                "--run-dir",
                root,
                "--initialization-dir",
                formal_initialization_dir,
                "--config",
                cfg,
                "--initialization-readiness-json",
                formal_initialization_dir / "v1_5_initialization_readiness.json",
                "--database-sidecar-json",
                formal_initialization_dir / "v1_5_initialization_database_sidecar.json",
                "--output-dir",
                pre_gas_readiness_dir,
            ),
            required_inputs=(
                "formal initialization contract plan",
                "initialization readiness sidecar",
                "PostgreSQL 18/SN/device_code/CHECK/S7-S8/S9 contracts",
            ),
            expected_outputs=(
                "pre_gas_readiness/v1_5_pre_gas_readiness.json",
                "pre_gas_readiness/v1_5_pre_gas_readiness.md",
                "pre_gas_readiness/v1_5_pre_gas_readiness_checks.csv",
            ),
            physical_meaning=(
                "Before any live identity or open-flow action, collapse initialization readiness into a single "
                "pre-gas gap list: SN/device_code, PostgreSQL 18, MODE2/1Hz, GETCO epoch 0, S7/S8 neutral, "
                "SENCO9 pressure completion, route readiness, and CHECK timing. This is only a sidecar; "
                "pending live gates are not treated as release evidence."
            ),
            execution_mode="offline_sidecar",
            gate="required_before_identity_getco_snapshot",
            notes=(
                "This sidecar does not open COM, connect PostgreSQL, control PACE/valves, or write coefficients.",
                "CO2/H2O route control remains only in the mature V1.5 queue runners.",
            ),
        )
    )

    steps.append(
        FullFlowStep(
            step_id="device_identity_and_getco_snapshot",
            title="Read device IDs and GETCO1-9 before calibration",
            phase="PRECHECK",
            tool_module="gas_calibrator.tools.probe_v1_5_getco_component_snapshot",
            command=_python_module(
                "gas_calibrator.tools.probe_v1_5_getco_component_snapshot",
                "--config",
                cfg,
                "--output-dir",
                getco_dir,
                "--groups",
                "1,2,3,4,5,6,7,8,9",
                "--response-timeout-s",
                "2.5",
                "--command-gap-s",
                "1.2",
                "--attempts-per-group",
                "3",
                "--pre-drain-s",
                "0.5",
                "--identity-timeout-s",
                "5.0",
                "--include-legacy",
                "--allow-runtime-identity-rebind",
            ),
            required_inputs=("enabled analyzer device_id mapping",),
            expected_outputs=(
                "old_component_coefficients_snapshot.json",
                "getco_component_snapshot_identity.csv",
                "runtime_identity_bound_config.json",
            ),
            physical_meaning=(
                "Existing internal coefficients affect displayed CO2/H2O, pressure, and temperature values. "
                "The pre-run GETCO snapshot defines coefficient epoch 0 and freezes the runtime port-to-device-ID binding; COM ports are only transport."
            ),
            execution_mode="read_only_real_com_requires_authorization",
            gate="required_before_any_write",
            may_reuse_v1_shared_core=True,
            opens_com_ports=True,
            coefficient_epoch_event="start_epoch_0",
            notes=(
                "Do not write analyzer IDs.",
                "After this immutable epoch-0 snapshot, auxiliary groups SENCO5/6/7/8/9 must be neutralized or cleared by controlled tools before pressure/component sampling.",
                "Subsequent physical stages use runtime_identity_bound_config.json generated by this step.",
            ),
        )
    )

    steps.append(
        FullFlowStep(
            step_id="identity_getco_readiness_snapshot",
            title="Validate identity-bound GETCO epoch-0 evidence",
            phase="IDENTITY_GETCO_READINESS",
            tool_module="gas_calibrator.tools.export_v1_5_getco_identity_readiness",
            command=_python_module(
                "gas_calibrator.tools.export_v1_5_getco_identity_readiness",
                "--getco-dir",
                getco_dir,
                "--output-dir",
                getco_readiness_dir,
                "--fail-on-not-ready",
            ),
            required_inputs=(
                "old_component_coefficients_snapshot.json",
                "getco_component_snapshot_identity.csv",
                "getco_component_snapshot_conclusion.csv",
                "runtime_identity_bound_config.json",
            ),
            expected_outputs=(
                "identity_getco_readiness/v1_5_getco_identity_readiness.json",
                "identity_getco_readiness/v1_5_getco_identity_readiness.md",
                "identity_getco_readiness/v1_5_getco_identity_readiness_checks.csv",
            ),
            physical_meaning=(
                "After the authorized read-only GETCO probe, verify that every active analyzer has a bound "
                "runtime device ID, complete GETCO1-9 epoch-0 backup, no-write conclusion, and frozen "
                "runtime_identity_bound_config before auxiliary SENCO5/6/7/8/9 neutralization."
            ),
            execution_mode="offline_sidecar",
            gate="required_after_epoch0_getco_before_auxiliary_neutralization",
            notes=(
                "This sidecar only reads artifacts; it does not open COM, write device IDs, write SENCO, or control routes.",
                "Missing GETCO artifacts remain a live identity gate, not automatic release evidence.",
            ),
        )
    )

    steps.append(
        FullFlowStep(
            step_id="auxiliary_senco56789_neutralization_gate",
            title="Neutralize auxiliary SENCO5-9 after epoch-0 GETCO backup",
            phase="AUXILIARY_COEFFICIENT_NEUTRALIZATION",
            tool_module=None,
            required_inputs=(
                "old GETCO1-9 epoch-0 snapshot",
                "runtime_identity_bound_config.json",
                "identity_getco_readiness/v1_5_getco_identity_readiness.json",
                "reviewer",
                "approver",
            ),
            expected_outputs=(
                str(aux_neutral_dir / "senco5_neutral_write_events.csv"),
                str(aux_neutral_dir / "senco6_neutral_write_events.csv"),
                str(aux_neutral_dir / "senco78_neutral_write_events.csv"),
                str(aux_neutral_dir / "senco9_clear_write_events.csv"),
                "auxiliary_coefficient_epoch_neutralized_snapshot.json",
            ),
            physical_meaning=(
                "SENCO5/SENCO6 are final CO2/H2O displayed-concentration affine trims; "
                "SENCO7/SENCO8 shape analyzer temperature inputs; SENCO9 shapes analyzer pressure input. "
                "These auxiliary layers must not silently contaminate the pressure quick-check, CO2 ratio fit, "
                "or H2O ratio fit. The correct sequence is immutable backup first, then controlled neutralization."
            ),
            execution_mode="blocked_pending_explicit_authorization",
            gate="required_after_epoch0_getco_before_pressure_and_component_sampling",
            opens_com_ports=True,
            writes_coefficients=True,
            coefficient_epoch_event="start_epoch_auxiliary_neutralized_after_epoch_0",
            notes=(
                "Use run_v1_5_co2_senco5_neutral_controlled_write for SENCO5.",
                "Use run_v1_5_h2o_senco6_neutral_controlled_write for SENCO6.",
                "Use run_v1_5_temperature_senco78_neutral_controlled_write for SENCO7/SENCO8.",
                "Use run_v1_5_pressure_senco9_clear_controlled_write for SENCO9 neutral/clear before pressure-channel recovery.",
                "The full-flow planner records this as a required gate but never auto-executes these writes.",
            ),
        )
    )

    steps.append(
        FullFlowStep(
            step_id="pressure_quick_check",
            title="Verify analyzer pressure input against COM22 at atmosphere",
            phase="PRESSURE_CHANNEL_QUICK_CHECK",
            tool_module="gas_calibrator.tools.validate_pressure_only",
            command=_python_module(
                "gas_calibrator.tools.validate_pressure_only",
                "--config",
                runtime_bound_cfg,
                "--output-dir",
                pressure_dir,
                "--run-id",
                f"{rid}_pressure_quick",
                *_maybe_arg("--pressure-reference-json", pressure_ref),
                "--pressure-points",
                "ambient",
                "--count",
                "10",
                "--interval-s",
                "1",
                "--continuous-atmosphere-hold",
                "--require-continuous-atmosphere-hold",
                "--no-prompt",
            ),
            required_inputs=("GETCO9 snapshot", "COM22 pressure certificate"),
            expected_outputs=("pressure quick-check CSV",),
            physical_meaning="Pressure P is an input to analyzer compensation and must be verified before component fitting.",
            execution_mode="real_com_pressure_only_requires_authorization",
            gate="block_component_write_if_failed",
            may_reuse_v1_shared_core=True,
            opens_com_ports=True,
            controls_pressure=True,
        )
    )

    steps.append(
        FullFlowStep(
            step_id="pressure_senco9_no_write_acquisition",
            title="If pressure fails, collect SENCO9 no-write multi-pressure evidence",
            phase="PRESSURE_CHANNEL_SENCO9_ACQUISITION",
            tool_module="gas_calibrator.tools.validate_pressure_only",
            command=_python_module(
                "gas_calibrator.tools.validate_pressure_only",
                "--config",
                runtime_bound_cfg,
                "--output-dir",
                pressure_dir / "senco9_no_write_acquisition",
                "--run-id",
                f"{rid}_pressure_senco9_no_write",
                *_maybe_arg("--pressure-reference-json", pressure_ref),
                "--pressure-points",
                "1100,1000,900,800,700,600,500",
                "--count",
                "12",
                "--interval-s",
                "1",
                "--continuous-atmosphere-hold",
                "--require-continuous-atmosphere-hold",
                "--control-pressure-points",
                "--pressure-control-tolerance-hpa",
                "1.0",
                "--pressure-control-stable-s",
                "8.0",
                "--pressure-control-timeout-s",
                "240.0",
                "--pressure-control-poll-s",
                "0.5",
                "--pressure-control-slew-mode",
                "max",
                "--pressure-control-atmosphere-release-wait-s",
                "1.5",
                "--pressure-control-post-stable-wait-s",
                "8.0",
                "--pressure-control-analyzer-stream-flush-s",
                "2.0",
                "--pre-sample-freshness-timeout-s",
                "4.0",
                "--pre-sample-signal-max-age-s",
                "1.0",
                "--analyzer-active-upload-hz",
                "1",
                "--no-prompt",
            ),
            required_inputs=("failed or marginal pressure quick-check", "GETCO9 snapshot", "COM22 pressure certificate"),
            expected_outputs=(
                "pressure_transition_trace.csv",
                "pressure_only_validation_meta.json",
                "pressure_channel_multi_analyzer_summary.csv",
                "pressure quick-check CSVs by analyzer",
            ),
            physical_meaning=(
                "This is the validated V1.5 SENCO9 no-write pressure runner: ambient is checked by the preceding quick-check; "
                "for sealed pressure points, stop continuous atmosphere, wait 1.5 s, require the pressure volume to already be externally/manual sealed, command PACE OUTP ACT with "
                "the K0472/PACE legacy MAX slew control contract, record pressure_transition_trace.csv, then wait for analyzer internal P cache "
                "refresh before sampling. It is not a diagnostic smoke test."
            ),
            execution_mode="real_com_pressure_only_requires_authorization",
            gate="only_needed_when_pressure_quick_check_fails_before_component_sampling",
            may_reuse_v1_shared_core=True,
            opens_com_ports=True,
            controls_pressure=True,
            notes=(
                "No SENCO write is performed by this step.",
                "Do not substitute open-flow dynamic pressure diagnostic tools for this acquisition.",
                "600 hPa and 500 hPa are included to preserve the full pressure span used for SENCO9 review.",
            ),
        )
    )

    steps.append(
        FullFlowStep(
            step_id="pressure_senco9_no_write_review",
            title="If pressure acquisition ran, evaluate SENCO9 no-write candidates",
            phase="PRESSURE_CHANNEL_SENCO9_REVIEW",
            tool_module="gas_calibrator.tools.export_v1_5_pressure_senco9_evaluation",
            command=_python_module(
                "gas_calibrator.tools.export_v1_5_pressure_senco9_evaluation",
                "--run-dir",
                pressure_dir / "senco9_no_write_acquisition" / f"{rid}_pressure_senco9_no_write",
                "--output-dir",
                pressure_dir / "senco9_no_write_evaluation",
                *_maybe_arg("--pressure-reference-json", pressure_ref),
                "--analyzer-prefix",
                "all",
                "--min-distinct-pressure-points",
                "3",
                "--min-pressure-span-hpa",
                "300.0",
                "--discard-initial-samples-per-pressure-point",
                "1",
            ),
            required_inputs=("pressure_senco9_no_write_acquisition artifacts",),
            expected_outputs=("pressure_senco9_fit_evaluation workbook", "pressure_fit_summary.csv"),
            physical_meaning=(
                "Pressure correction is independent from CO2/H2O fitting. This offline review uses the "
                "multi-pressure no-write trace and COM22 reference evidence to decide whether a SENCO9 "
                "offset candidate is justified."
            ),
            execution_mode="offline_sidecar",
            gate="only_needed_when_pressure_quick_check_fails_after_no_write_acquisition",
        )
    )

    steps.append(
        FullFlowStep(
            step_id="pressure_channel_completion_audit",
            title="Export pressure-channel completion package after SENCO9 write and post-write verification",
            phase="PRESSURE_CHANNEL_COMPLETION",
            tool_module="gas_calibrator.tools.export_v1_5_pressure_channel_completion",
            command=_python_module(
                "gas_calibrator.tools.export_v1_5_pressure_channel_completion",
                "--senco9-write-summary",
                pressure_dir / "senco9_controlled_write" / "senco9_write_summary.csv",
                "--post-write-fit-summary",
                pressure_dir / "post_write_pressure_verify" / "pressure_fit_summary.csv",
                "--pressure-reference-json",
                pressure_ref or "<pressure_reference_json>",
                "--old-getco-json",
                getco_dir / "old_component_coefficients_snapshot.json",
                "--output-dir",
                pressure_dir / "pressure_channel_completion",
            ),
            required_inputs=(
                "SENCO9 controlled write summary",
                "post-write pressure verification fit summary",
                "COM22 pressure certificate",
                "GETCO9 epoch-0 snapshot",
            ),
            expected_outputs=(
                "pressure_channel_completion/pressure_channel_completion_summary.csv",
                "pressure_channel_completion/pressure_channel_device_readiness.csv",
                "pressure_channel_completion/pressure_channel_completion_report.md",
            ),
            physical_meaning=(
                "This offline audit is the bridge between pressure-channel repair and component calibration. "
                "It proves the analyzer pressure input P is traceably ready before CO2/H2O fitting consumes it."
            ),
            execution_mode="offline_sidecar_after_controlled_senco9_write_and_reverify",
            gate="required_when_senco9_was_written_before_component_sampling",
            notes=(
                "No COM is opened and no SENCO is written by this step.",
                "If pressure quick-check passes and no SENCO9 write was needed, this step documents that completion evidence is not applicable.",
            ),
        )
    )

    steps.append(
        FullFlowStep(
            step_id="temperature_channel_fast_review",
            title="Review SENCO7/SENCO8 temperature input evidence",
            phase="TEMPERATURE_CHANNEL_REVIEW",
            tool_module="gas_calibrator.tools.export_v1_5_temperature_channel_review",
            command=_python_module(
                "gas_calibrator.tools.export_v1_5_temperature_channel_review",
                "--h2o-points-parent",
                h2o_points_parent,
                "--output-dir",
                temp_dir,
            ),
            required_inputs=("digital thermometer evidence", "analyzer chamber/case temperature evidence"),
            expected_outputs=("temperature_channel_review_summary.json", "temperature_channel_review.md"),
            physical_meaning=(
                "Temperature T is a model input for CO2/H2O ratio compensation. "
                "This review determines whether SENCO7/8 must be corrected before final coefficient approval."
            ),
            execution_mode="offline_review",
            gate="review_before_final_component_write",
        )
    )

    steps.append(
        FullFlowStep(
            step_id="co2_open_flow_sampling",
            title="Run V1.5 CO2 open-flow multi-temperature queue",
            phase="CO2_OPEN_FLOW",
            tool_module="gas_calibrator.tools.run_v1_5_formal_co2_open_flow_queue",
            command=_python_module(
                "gas_calibrator.tools.run_v1_5_formal_co2_open_flow_queue",
                "--config",
                runtime_bound_cfg,
                "--queue-csv",
                co2_queue or "<co2_runner_queue.csv>",
                "--output-dir",
                co2_dir,
                "--run-id",
                f"{rid}_co2",
                "--analyzer-acquisition",
                "active_stream_1hz",
                "--temperature-order",
                "desc",
                "--no-prompt",
            ),
            required_inputs=("CO2 queue", "pressure verified", "temperature evidence policy"),
            expected_outputs=("CO2 point sidecars", "MODE2 ratio/signal evidence", "digital thermometer evidence"),
            physical_meaning=(
                "Open flow continuously refreshes the optical cavity. CO2 fitting uses factory ratio evidence, "
                "not old displayed concentration affected by existing coefficients."
            ),
            execution_mode="real_com_route_requires_authorization",
            gate="requires_pressure_pass",
            may_reuse_v1_shared_core=True,
            opens_com_ports=True,
            controls_gas_route=True,
        )
    )

    steps.append(
        FullFlowStep(
            step_id="h2o_open_flow_sampling",
            title="Run V1.5 H2O open-flow multi-temperature queue",
            phase="H2O_OPEN_FLOW",
            tool_module="gas_calibrator.tools.run_v1_5_formal_h2o_open_flow_queue",
            command=_python_module(
                "gas_calibrator.tools.run_v1_5_formal_h2o_open_flow_queue",
                "--config",
                runtime_bound_cfg,
                "--queue-csv",
                h2o_queue or "<h2o_runner_queue.csv>",
                "--output-dir",
                h2o_dir,
                "--run-id",
                f"{rid}_h2o",
                "--analyzer-acquisition",
                "active_stream_1hz",
                "--temperature-order",
                "asc",
                "--h2o-pressure-presample-policy",
                "skip",
                "--no-prompt",
            ),
            required_inputs=("H2O queue", "dewpoint reference", "pressure verified", "temperature evidence policy"),
            expected_outputs=("H2O point sidecars", "dewpoint/H2O mmol evidence", "H2O ratio/signal evidence"),
            physical_meaning=(
                "H2O fitting must use dewpoint/reference-backed water evidence and preserve dry-gas low-water anchors separately from CO2 zero-gas anchors."
            ),
            execution_mode="real_com_route_requires_authorization",
            gate="requires_pressure_pass",
            may_reuse_v1_shared_core=True,
            opens_com_ports=True,
            controls_water_route=True,
        )
    )

    steps.append(
        FullFlowStep(
            step_id="factory_signal_health_review",
            title="Review factory-mode optical reference and signal health",
            phase="FACTORY_SIGNAL_HEALTH_REVIEW",
            tool_module="gas_calibrator.tools.export_v1_5_factory_signal_health_review",
            command=_python_module(
                "gas_calibrator.tools.export_v1_5_factory_signal_health_review",
                "--point-means-csv",
                "<offline_fit_point_means.csv>",
                "--residuals-csv",
                "<candidate_fit_residuals.csv>",
                "--output-dir",
                factory_signal_dir,
            ),
            required_inputs=("MODE2 point means", "candidate residuals"),
            expected_outputs=(
                "factory_signal_health_summary.csv",
                "factory_signal_health_point_flags.csv",
                "factory_signal_health_report_zh.md",
            ),
            physical_meaning=(
                "A stable ratio is not sufficient if the reference, CO2, or H2O optical signals are "
                "in an abnormal working region. This offline gate prevents optical reference-chain "
                "faults from being absorbed into SENCO1/2/3/4."
            ),
            execution_mode="offline_review",
            gate="required_before_component_write_review",
            notes=(
                "Devices whose factory-signal gate is not pass_factory_signal_health must not enter formal component coefficient review.",
                "SETILLUM no-argument readback is not treated as numeric evidence; use MODE2 ref_signal and explicit configuration snapshots.",
            ),
        )
    )

    steps.append(
        FullFlowStep(
            step_id="fit_input_quality_review",
            title="Audit CO2/H2O fit inputs before candidate coefficients",
            phase="QC_AND_FIT_INPUT_REVIEW",
            tool_module="gas_calibrator.tools.export_v1_5_fit_input_quality",
            command=_python_module(
                "gas_calibrator.tools.export_v1_5_fit_input_quality",
                "--co2-policy-csv",
                "<co2_candidate_policy.csv>",
                "--co2-residuals-csv",
                "<co2_candidate_residuals.csv>",
                "--h2o-policy-csv",
                "<h2o_candidate_policy.csv>",
                "--h2o-residuals-csv",
                "<h2o_candidate_residuals.csv>",
                "--output-dir",
                fit_quality_dir,
            ),
            required_inputs=("CO2 candidate residuals", "H2O candidate residuals"),
            expected_outputs=("fit_input_quality.md",),
            physical_meaning="Only traceable, stable, role-eligible samples should enter candidate coefficient fitting.",
            execution_mode="offline_review",
            gate="requires_factory_signal_health_review",
        )
    )

    steps.append(
        FullFlowStep(
            step_id="post_run_coefficient_executor",
            title="Build post-run coefficient review, write, reverify, and archive gap plan",
            phase="POST_RUN_COEFFICIENT_EXECUTOR",
            tool_module="gas_calibrator.tools.export_v1_5_post_run_coefficient_executor",
            command=_python_module(
                "gas_calibrator.tools.export_v1_5_post_run_coefficient_executor",
                "--run-dir",
                run_dir,
                "--output-dir",
                post_run_executor_dir,
                "--plan-json",
                formal_pkg / "formal_plan_snapshot.json",
                "--pressure-reference-json",
                formal_pkg / "com22_pressure_reference.json",
                "--run-evidence-status-json",
                root / "v1_5_run_evidence_status.json",
                "--pressure-review-json",
                pressure_dir / "pressure_senco9_review.json",
                "--pressure-completion-summary-csv",
                pressure_dir / "pressure_channel_completion" / "pressure_channel_completion_summary.csv",
                "--pressure-device-readiness-csv",
                pressure_dir / "pressure_channel_completion" / "pressure_channel_device_readiness.csv",
                "--temperature-review-csv",
                temp_dir / "temperature_current_point_review.csv",
                "--main-precheck-meta-json",
                "<main_senco_write_precheck_meta.json>",
                "--post-write-reverification-json",
                post_write_verify_dir / "post_write_reverification_review.json",
                "--archive-closure-json",
                root / "formal_archive_closure_from_full_chain" / "v1_5_formal_archive_closure_index.json",
            ),
            required_inputs=(
                "completed CO2 open-flow evidence",
                "completed H2O open-flow evidence",
                "pressure and temperature input reviews",
                "factory signal health review",
                "fit input quality review",
            ),
            expected_outputs=(
                "post_run_coefficient_executor/executor_manifest.json",
                "post_run_coefficient_executor/executor_summary.md",
                "post_run_coefficient_executor/device_eligibility.csv",
                "post_run_coefficient_executor/coefficient_execution_plan.csv",
                "post_run_coefficient_executor/controlled_write_package.csv",
                "post_run_coefficient_executor/post_write_reverification_plan.csv",
                "post_run_coefficient_executor/archive_gap_list.csv",
            ),
            physical_meaning=(
                "After gas and water acquisition, coefficient closure must be deterministic: "
                "pressure and temperature input quantities are checked first, all eligible stable "
                "CO2/H2O points are fit, S5/S6 trims are reviewed after main coefficients, and "
                "missing post-write reverification or archive evidence remains an explicit gap."
            ),
            execution_mode="offline_review",
            gate="required_after_component_acquisition_before_controlled_write",
            notes=(
                "This exporter is no-write and does not open COM ports.",
                "Missing H2O post-write reverification blocks final acceptance but does not block CO2 candidate review.",
                "A failed device must be excluded per-device with a reason; it must not drag down all other devices.",
            ),
        )
    )

    steps.append(
        FullFlowStep(
            step_id="full_flow_closure_readiness",
            title="Review full-flow closure readiness before controlled writes",
            phase="FULL_FLOW_CLOSURE_READINESS",
            tool_module="gas_calibrator.tools.export_v1_5_full_flow_closure_readiness",
            command=_python_module(
                "gas_calibrator.tools.export_v1_5_full_flow_closure_readiness",
                "--run-dir",
                root,
                "--output-dir",
                closure_readiness_dir,
                "--full-flow-plan-json",
                root / "v1_5_full_flow_plan.json",
                "--run-evidence-status-json",
                root / "v1_5_run_evidence_status.json",
                "--post-run-executor-json",
                post_run_executor_dir / "executor_manifest.json",
                "--controlled-write-package-csv",
                post_run_executor_dir / "controlled_write_package.csv",
                "--post-write-reverification-plan-csv",
                post_run_executor_dir / "post_write_reverification_plan.csv",
                "--archive-gap-list-csv",
                post_run_executor_dir / "archive_gap_list.csv",
                "--archive-closure-json",
                root / "formal_archive_closure_from_full_chain" / "v1_5_formal_archive_closure_index.json",
            ),
            required_inputs=(
                "v1_5_run_evidence_status.json",
                "post_run_coefficient_executor/executor_manifest.json",
                "post_run_coefficient_executor/controlled_write_package.csv",
                "post_run_coefficient_executor/post_write_reverification_plan.csv",
            ),
            expected_outputs=(
                "full_flow_closure_readiness/v1_5_full_flow_closure_readiness.json",
                "full_flow_closure_readiness/v1_5_full_flow_closure_readiness.md",
                "full_flow_closure_readiness/v1_5_full_flow_closure_gaps.csv",
                "full_flow_closure_readiness/v1_5_full_flow_device_closure.csv",
                "full_flow_closure_readiness/v1_5_full_flow_release_domains.csv",
            ),
            physical_meaning=(
                "This offline gate checks whether plan, evidence index, candidate write package, "
                "post-write reverification plan, and archive gaps form one auditable chain before any "
                "controlled SENCO write is considered."
            ),
            execution_mode="offline_review",
            gate="required_before_controlled_write_review",
            notes=(
                "No COM ports are opened and no coefficient is written.",
                "Per-device blockers remain per-device; one failed analyzer must not hide ready analyzers.",
                "Fit/verification labels do not exclude otherwise stable traceable samples by default.",
            ),
        )
    )

    steps.append(
        FullFlowStep(
            step_id="co2_candidate_write_review",
            title="Review CO2 SENCO1/SENCO3 and optional SENCO5 candidates",
            phase="CO2_CANDIDATE_REVIEW",
            tool_module="gas_calibrator.tools.export_v1_5_co2_senco_pair_model_scope",
            command=_python_module(
                "gas_calibrator.tools.export_v1_5_co2_senco_pair_model_scope",
                "--original-points-xlsx",
                "<v1_5_original_points.xlsx>",
                "--candidate-dir",
                "<co2_candidate_dir>",
                "--output-dir",
                root / "co2_candidate_write_review",
            ),
            required_inputs=("CO2 ratio fit candidates", "zero-gas/low-end CO2 anchor evidence"),
            expected_outputs=("CO2 SENCO model-scope review",),
            physical_meaning="CO2 main coefficients must freeze pressure terms under current-atmosphere V1.5 policy; SENCO5 is final displayed concentration trim.",
            execution_mode="offline_review",
            gate="reviewer_approver_required_before_write",
        )
    )

    steps.append(
        FullFlowStep(
            step_id="controlled_component_write_placeholder",
            title="Controlled component writes create new coefficient epochs",
            phase="CONTROLLED_WRITE",
            tool_module=None,
            required_inputs=("approved CO2/H2O/S5/S6 write packages", "old GETCO1-9 snapshot", "reviewer", "approver"),
            expected_outputs=("write events", "readback verification", "coefficient_epoch_n snapshots"),
            physical_meaning=(
                "SENCO1/3, SENCO2/4, and optional SENCO5/6 writes are high-risk model changes. "
                "Each successful write starts a new coefficient epoch and must be followed by verification."
            ),
            execution_mode="blocked_pending_explicit_authorization",
            gate="never_auto_execute_from_full_flow_planner",
            writes_coefficients=True,
            coefficient_epoch_event="start_new_epoch_after_each_verified_write",
            notes=(
                "Use run_v1_5_co2_senco13_controlled_write only after approval.",
                "Use run_v1_5_h2o_senco24_controlled_write only after approval.",
                "Use linear SENCO5/SENCO6 writers only when final-output trim is reviewed.",
            ),
        )
    )

    steps.append(
        FullFlowStep(
            step_id="post_write_reverification_placeholder",
            title="Verify analyzer outputs after controlled coefficient writes",
            phase="POST_WRITE_REVERIFY",
            tool_module="gas_calibrator.tools.export_v1_5_post_write_reverification",
            command=_python_module(
                "gas_calibrator.tools.export_v1_5_post_write_reverification",
                "--verification-csv",
                "<post_write_verification_points.csv>",
                "--output-dir",
                post_write_verify_dir,
                "--write-event-json",
                "<coefficient_write_events.json>",
                "--coefficient-snapshot-json",
                "<coefficient_epoch_n_snapshot.json>",
            ),
            required_inputs=(
                "coefficient_epoch_n readback snapshots",
                "approved post-write verification gas/H2O points",
                "pressure channel pass evidence",
            ),
            expected_outputs=(
                "post_write_reverification_summary.json",
                "post_write_reverification_point_errors.csv",
                str(post_write_verify_dir),
            ),
            physical_meaning=(
                "A coefficient write changes the analyzer measurement model. "
                "Before final evidence bundling, the updated model must be checked against independent open-flow verification points."
            ),
            execution_mode="blocked_pending_post_write_reverification",
            gate="required_after_controlled_write_before_final_archive",
            coefficient_epoch_event="verify_current_epoch_before_archive",
            notes=(
                "Use short V1.5 open-flow reverification after any approved SENCO write.",
                "Keep verification points separate from the fit-training points when possible.",
                "Do not treat engineering diagnostic pressure points as post-write component acceptance.",
            ),
        )
    )

    steps.append(
        FullFlowStep(
            step_id="formal_evidence_sidecar",
            title="Build formal evidence bundle from completed run artifacts",
            phase="EVIDENCE_BUNDLE",
            tool_module="gas_calibrator.tools.run_v1_5_formal_evidence_sidecar",
            command=_python_module(
                "gas_calibrator.tools.run_v1_5_formal_evidence_sidecar",
                "--run-dir",
                run_dir,
                "--plan-json",
                formal_pkg / "formal_plan_snapshot.json",
                "--pressure-reference-json",
                formal_pkg / "com22_pressure_reference.json",
                "--config",
                runtime_bound_cfg,
            ),
            required_inputs=("completed V1.5 run directory", "pressure quick-check", "formal plan snapshot"),
            expected_outputs=("formal_evidence_sidecar/evidence_bundle.json",),
            physical_meaning="Bundle raw frames, QC, traceability, coefficient snapshots, and reports so the result can be reconstructed.",
            execution_mode="offline_sidecar",
            gate="required_before_report_and_database_import",
        )
    )

    steps.append(
        FullFlowStep(
            step_id="formal_database_dry_run_snapshot",
            title="Preview V1.5 PostgreSQL 18 schema and insert contract",
            phase="FORMAL_DATABASE_DRY_RUN",
            tool_module="gas_calibrator.tools.export_v1_5_formal_database_dry_run",
            command=_python_module(
                "gas_calibrator.tools.export_v1_5_formal_database_dry_run",
                "--output-dir",
                formal_database_dry_run_dir,
                "--fail-on-blocker",
            ),
            required_inputs=(
                "V1.5 core storage schema",
                "V1.5 evidence registry schema",
                "SN/device_code identity contract",
                "formal evidence sidecar or reviewed insert-preview inputs",
            ),
            expected_outputs=(
                "formal_database_dry_run/v1_5_formal_database_dry_run.json",
                "formal_database_dry_run/V1_5_FORMAL_DATABASE_DRY_RUN.md",
                "formal_database_dry_run/v1_5_formal_database_dry_run_checks.csv",
                "formal_database_dry_run/v1_5_formal_database_insert_preview.csv",
            ),
            physical_meaning=(
                "Before any production database import, verify the PostgreSQL 18 schema, SN/device_code "
                "primary identity, protocol ID alias, and stage-by-stage insert contract as a dry-run preview only."
            ),
            execution_mode="offline_sidecar",
            gate="required_before_database_import_authorization",
            notes=(
                "This dry-run never connects PostgreSQL and never imports production data.",
                "A passing database dry-run is schema/insert-preview evidence only; archive release and database import remain separate gates.",
            ),
        )
    )

    steps.append(
        FullFlowStep(
            step_id="formal_database_import_preflight_snapshot",
            title="Review PostgreSQL 18 import preflight without connecting",
            phase="FORMAL_DATABASE_IMPORT_PREFLIGHT",
            tool_module="gas_calibrator.tools.export_v1_5_formal_database_import_preflight",
            command=_python_module(
                "gas_calibrator.tools.export_v1_5_formal_database_import_preflight",
                "--formal-database-dry-run-json",
                formal_database_dry_run_dir / "v1_5_formal_database_dry_run.json",
                "--dsn-env",
                "V1_5_POSTGRES_DSN",
                "--output-dir",
                formal_database_import_preflight_dir,
                "--fail-on-blocker",
            ),
            required_inputs=(
                "formal PostgreSQL 18 database dry-run contract",
                "DSN configuration presence or reviewed missing-DSN gap",
                "explicit database-import authorization policy",
            ),
            expected_outputs=(
                "formal_database_import_preflight/v1_5_formal_database_import_preflight.json",
                "formal_database_import_preflight/V1_5_FORMAL_DATABASE_IMPORT_PREFLIGHT.md",
                "formal_database_import_preflight/v1_5_formal_database_import_preflight_checks.csv",
                "formal_database_import_preflight/v1_5_formal_database_import_preflight_summary.csv",
            ),
            physical_meaning=(
                "Before a real production import, confirm the dry-run contract, DSN presence/fingerprint, "
                "migration lock, and explicit authorization boundary without opening a database connection."
            ),
            execution_mode="offline_sidecar",
            gate="required_before_database_import_execution",
            notes=(
                "This preflight never connects PostgreSQL, applies migrations, or imports rows.",
                "A passing import preflight is still not database import authorization; archive release and operator authorization remain separate.",
            ),
        )
    )

    steps.append(
        FullFlowStep(
            step_id="formal_database_import_authorization_snapshot",
            title="Review PostgreSQL 18 manual import authorization without connecting",
            phase="FORMAL_DATABASE_IMPORT_AUTHORIZATION",
            tool_module="gas_calibrator.tools.export_v1_5_formal_database_import_authorization",
            command=_python_module(
                "gas_calibrator.tools.export_v1_5_formal_database_import_authorization",
                "--formal-database-import-preflight-json",
                formal_database_import_preflight_dir / "v1_5_formal_database_import_preflight.json",
                "--archive-closure-json",
                root / "formal_archive_closure_from_full_chain" / "v1_5_formal_archive_closure_index.json",
                *_maybe_arg("--operator", operator),
                *_maybe_arg("--reviewer", reviewer),
                *_maybe_arg("--approver", approver),
                "--authorization-id",
                "<database_import_authorization_id>",
                "--output-dir",
                formal_database_import_authorization_dir,
                "--fail-on-blocker",
            ),
            required_inputs=(
                "formal database import preflight sidecar",
                "formal archive closure index",
                "operator/reviewer/approver authorization record",
                "database import authorization id",
            ),
            expected_outputs=(
                "formal_database_import_authorization/v1_5_formal_database_import_authorization.json",
                "formal_database_import_authorization/V1_5_FORMAL_DATABASE_IMPORT_AUTHORIZATION.md",
                "formal_database_import_authorization/v1_5_formal_database_import_authorization_checks.csv",
                "formal_database_import_authorization/v1_5_formal_database_import_authorization_summary.csv",
            ),
            physical_meaning=(
                "Before any production database import command can run, confirm archive release, "
                "preflight readiness, and explicit operator/reviewer/approver authorization without opening a database connection."
            ),
            execution_mode="offline_sidecar",
            gate="required_before_database_import_execution",
            notes=(
                "This authorization guard never connects PostgreSQL, applies migrations, or imports rows.",
                "A ready authorization artifact must still be consumed by a separate controlled import command.",
            ),
        )
    )

    steps.append(
        FullFlowStep(
            step_id="formal_database_import_command_contract_snapshot",
            title="Review PostgreSQL 18 import command contract without connecting",
            phase="FORMAL_DATABASE_IMPORT_COMMAND_CONTRACT",
            tool_module="gas_calibrator.tools.export_v1_5_formal_database_import_command_contract",
            command=_python_module(
                "gas_calibrator.tools.export_v1_5_formal_database_import_command_contract",
                "--formal-database-import-authorization-json",
                formal_database_import_authorization_dir / "v1_5_formal_database_import_authorization.json",
                "--formal-database-import-preflight-json",
                formal_database_import_preflight_dir / "v1_5_formal_database_import_preflight.json",
                "--archive-closure-json",
                root / "formal_archive_closure_from_full_chain" / "v1_5_formal_archive_closure_index.json",
                "--evidence-bundle-json",
                root / "formal_archive_closure_from_full_chain" / "evidence_bundle.json",
                "--dsn-env",
                "V1_5_POSTGRES_DSN",
                "--requested-command-module",
                "gas_calibrator.tools.import_v1_5_evidence_package",
                "--output-dir",
                formal_database_import_command_contract_dir,
                "--fail-on-blocker",
            ),
            required_inputs=(
                "formal database import authorization sidecar",
                "formal database import preflight sidecar",
                "formal archive closure index",
                "formal evidence bundle",
                "PostgreSQL DSN environment variable contract",
            ),
            expected_outputs=(
                "formal_database_import_command_contract/v1_5_formal_database_import_command_contract.json",
                "formal_database_import_command_contract/V1_5_FORMAL_DATABASE_IMPORT_COMMAND_CONTRACT.md",
                "formal_database_import_command_contract/v1_5_formal_database_import_command_contract_checks.csv",
                "formal_database_import_command_contract/v1_5_formal_database_import_command_contract_summary.csv",
            ),
            physical_meaning=(
                "After manual import authorization, review the exact future import command inputs, DSN env contract, "
                "archive/evidence-bundle references, migration lock, and no-execution boundary without opening PostgreSQL."
            ),
            execution_mode="offline_sidecar",
            gate="required_before_database_import_execution",
            notes=(
                "This command contract never connects PostgreSQL, applies migrations, or imports rows.",
                "The actual import remains a separate controlled command that must consume this artifact and re-check all inputs.",
            ),
        )
    )

    steps.append(
        FullFlowStep(
            step_id="formal_database_import_blocked_executor_snapshot",
            title="Run blocked PostgreSQL 18 import executor stub without connecting",
            phase="FORMAL_DATABASE_IMPORT_BLOCKED_EXECUTOR",
            tool_module="gas_calibrator.tools.import_v1_5_evidence_package",
            command=_python_module(
                "gas_calibrator.tools.import_v1_5_evidence_package",
                "--formal-database-import-command-contract-json",
                formal_database_import_command_contract_dir / "v1_5_formal_database_import_command_contract.json",
                "--formal-database-import-authorization-json",
                formal_database_import_authorization_dir / "v1_5_formal_database_import_authorization.json",
                "--formal-database-import-preflight-json",
                formal_database_import_preflight_dir / "v1_5_formal_database_import_preflight.json",
                "--archive-closure-json",
                root / "formal_archive_closure_from_full_chain" / "v1_5_formal_archive_closure_index.json",
                "--evidence-bundle-json",
                root / "formal_archive_closure_from_full_chain" / "evidence_bundle.json",
                "--dsn-env",
                "V1_5_POSTGRES_DSN",
                "--output-dir",
                formal_database_import_blocked_executor_dir,
                "--fail-on-blocked",
            ),
            required_inputs=(
                "formal database import command contract sidecar",
                "formal database import authorization sidecar",
                "formal database import preflight sidecar",
                "formal archive closure index",
                "formal evidence bundle",
                "PostgreSQL DSN environment variable contract",
            ),
            expected_outputs=(
                "formal_database_import_blocked_executor/v1_5_formal_database_import_blocked_executor.json",
                "formal_database_import_blocked_executor/V1_5_FORMAL_DATABASE_IMPORT_BLOCKED_EXECUTOR.md",
                "formal_database_import_blocked_executor/v1_5_formal_database_import_blocked_executor_checks.csv",
                "formal_database_import_blocked_executor/v1_5_formal_database_import_blocked_executor_summary.csv",
            ),
            physical_meaning=(
                "After the command contract exists, invoke the future import command only in its blocked-stub mode. "
                "This proves the command consumes the reviewed inputs and still refuses PostgreSQL connection, "
                "migration, row import, COM access, route control, and analyzer writes."
            ),
            execution_mode="offline_sidecar",
            gate="required_before_database_import_execution",
            notes=(
                "This stub intentionally returns non-zero when --fail-on-blocked is used, proving production import remains locked.",
                "A future real import executor must be a separate controlled package with explicit double authorization and readback/import evidence.",
            ),
        )
    )

    steps.append(
        FullFlowStep(
            step_id="formal_database_import_controlled_executor_design_snapshot",
            title="Review controlled PostgreSQL 18 import executor design without connecting",
            phase="FORMAL_DATABASE_IMPORT_CONTROLLED_EXECUTOR_DESIGN",
            tool_module="gas_calibrator.tools.export_v1_5_formal_database_import_controlled_executor_design",
            command=_python_module(
                "gas_calibrator.tools.export_v1_5_formal_database_import_controlled_executor_design",
                "--formal-database-import-blocked-executor-json",
                formal_database_import_blocked_executor_dir / "v1_5_formal_database_import_blocked_executor.json",
                "--dsn-env",
                "V1_5_POSTGRES_DSN",
                "--output-dir",
                formal_database_import_controlled_executor_design_dir,
            ),
            required_inputs=(
                "blocked database import executor sidecar",
                "PostgreSQL DSN environment variable contract",
            ),
            expected_outputs=(
                "formal_database_import_controlled_executor_design/v1_5_formal_database_import_controlled_executor_design.json",
                "formal_database_import_controlled_executor_design/V1_5_FORMAL_DATABASE_IMPORT_CONTROLLED_EXECUTOR_DESIGN.md",
                "formal_database_import_controlled_executor_design/v1_5_formal_database_import_controlled_executor_authorization_contract.csv",
                "formal_database_import_controlled_executor_design/v1_5_formal_database_import_controlled_executor_transaction_contract.csv",
                "formal_database_import_controlled_executor_design/v1_5_formal_database_import_controlled_executor_readback_contract.csv",
                "formal_database_import_controlled_executor_design/v1_5_formal_database_import_controlled_executor_rollback_contract.csv",
                "formal_database_import_controlled_executor_design/v1_5_formal_database_import_controlled_executor_boundary_gates.csv",
            ),
            physical_meaning=(
                "Before any real PostgreSQL import executor is implemented, freeze the future execution contract: "
                "double authorization, DSN secret handling, transaction/rollback behavior, pre-commit readback, "
                "post-commit hold policy, and no-current-execution boundary."
            ),
            execution_mode="offline_sidecar",
            gate="required_before_database_import_execution",
            notes=(
                "This design review does not add a real --execute path and does not connect PostgreSQL.",
                "It exists to prevent a future database importer from bypassing authorization, transaction, readback, or rollback evidence.",
            ),
        )
    )

    steps.append(
        FullFlowStep(
            step_id="database_import",
            title="Import evidence bundle into V1.5 PostgreSQL registry",
            phase="DATABASE_IMPORT",
            tool_module="gas_calibrator.tools.import_v1_5_evidence_package",
            command=_python_module(
                "gas_calibrator.tools.import_v1_5_evidence_package",
                "--run-dir",
                run_dir,
            ),
            required_inputs=("V1.5 evidence DSN", "evidence_bundle.json"),
            expected_outputs=("database_imported=true summary",),
            physical_meaning="The database indexes traceability and audit state; raw evidence remains in hashed evidence packages.",
            execution_mode="offline_database_requires_configured_dsn",
            gate="required_for_formal_archive",
        )
    )

    steps.append(
        FullFlowStep(
            step_id="zh_calibration_reports",
            title="Generate Chinese V1.5 calibration reports",
            phase="REPORTS",
            tool_module="gas_calibrator.tools.export_v1_5_calibration_reports",
            command=_python_module(
                "gas_calibrator.tools.export_v1_5_calibration_reports",
                "--evidence-bundle-json",
                bundle,
                "--output-dir",
                reports_dir,
                *_maybe_arg("--reviewer", reviewer),
                *_maybe_arg("--approver", approver),
            ),
            required_inputs=("evidence_bundle.json",),
            expected_outputs=(
                "run report",
                "technical report",
                "formal calibration report",
                "per_device_certificates",
                "per_device_certificate_manifest.json",
                "per_device_certificate_artifact_hashes.csv",
            ),
            physical_meaning=(
                "Reports summarize the physical open-flow method, QC decisions, traceability, uncertainty, and write status. "
                "Per-device certificates are generated from the same frozen evidence bundle and include artifact hashes for audit."
            ),
            execution_mode="offline_sidecar",
            gate="final_deliverable",
            notes=(
                "H2O queue abort/exclusion evidence remains diagnostic-only and blocks affected rows from formal fit/acceptance.",
                "This report exporter does not open COM ports, control routes, or write coefficients.",
            ),
        )
    )

    steps.append(
        FullFlowStep(
            step_id="final_evidence_status_refresh",
            title="Refresh final V1.5 evidence status after reports and per-device certificates",
            phase="FINAL_EVIDENCE_STATUS",
            tool_module="gas_calibrator.tools.export_v1_5_run_evidence_status",
            command=_python_module(
                "gas_calibrator.tools.export_v1_5_run_evidence_status",
                "--run-dir",
                root,
                "--output-dir",
                root,
                "--full-flow-plan-json",
                root / "v1_5_full_flow_plan.json",
                "--contract-json",
                root / "v1_5_formal_flow_contract.json",
                "--evidence-bundle-json",
                bundle,
            ),
            required_inputs=("evidence_bundle.json", "report artifacts", "per-device certificate package"),
            expected_outputs=("v1_5_run_evidence_status.json", "v1_5_run_evidence_status.md"),
            physical_meaning=(
                "The final evidence-status tree must be rebuilt after certificates are generated so audit state, hashes, "
                "H2O exclusions, and report readiness all point to the same evidence package."
            ),
            execution_mode="offline_sidecar",
            gate="required_after_reports_for_archive_closure",
        )
    )

    steps.append(
        FullFlowStep(
            step_id="algorithm_profile_runner_dry_run_snapshot",
            title="Generate new-algorithm profile runner dry-run bundle",
            phase="ALGORITHM_PROFILE_RUNNER_DRY_RUN",
            tool_module="gas_calibrator.tools.export_v1_5_algorithm_profile_runner_dry_run",
            command=_python_module(
                "gas_calibrator.tools.export_v1_5_algorithm_profile_runner_dry_run",
                "--profile-path",
                algorithm_profile_path,
                "--output-dir",
                algorithm_profile_runner_dir,
                "--fail-on-blocker",
            ),
            required_inputs=("configs/v1_5_algorithm_route_profiles.json",),
            expected_outputs=(
                "algorithm_profile_runner_dry_run/v1_5_algorithm_profile_runner_dry_run.json",
                "algorithm_profile_runner_dry_run/v1_5_algorithm_profile_runner_dry_run_checks.csv",
                "algorithm_profile_runner_dry_run/algorithm_formal_runlist_preview/v1_5_new_algorithm_formal_co2_runlist_preview.csv",
                "algorithm_profile_runner_dry_run/algorithm_formal_runlist_preview/v1_5_new_algorithm_formal_h2o_runlist_preview.csv",
                "algorithm_profile_runner_dry_run/algorithm_runlist_readiness/v1_5_algorithm_runlist_readiness.json",
                "algorithm_profile_runner_dry_run/algorithm_runner_integration_dry_run/v1_5_algorithm_runner_integration_dry_run.json",
            ),
            physical_meaning=(
                "For the new absorption algorithm, generate the profile-driven CO2 47 / H2O 14 runlist preview, "
                "readiness gate, and dry-run mature-queue handoff evidence without executing the queues."
            ),
            execution_mode="offline_sidecar",
            gate="optional_new_algorithm_runner_preflight_before_status_rollup",
            notes=(
                "This bundle does not open COM, connect PostgreSQL, control gas/water routes, write SN/device IDs, write coefficients, or modify mature runners.",
                "A passing dry-run bundle does not authorize live runner wiring or real acceptance.",
            ),
        )
    )

    steps.append(
        FullFlowStep(
            step_id="formal_run_status_snapshot",
            title="Export top-level formal run status dashboard",
            phase="FORMAL_RUN_STATUS",
            tool_module="gas_calibrator.tools.export_v1_5_formal_run_status",
            command=_python_module(
                "gas_calibrator.tools.export_v1_5_formal_run_status",
                "--run-dir",
                root,
                "--output-dir",
                formal_status_dir,
                "--initialization-readiness-json",
                formal_initialization_dir / "v1_5_initialization_readiness.json",
                "--formal-initialization-controlled-executor-design-json",
                formal_initialization_controlled_executor_design_dir
                / "v1_5_formal_initialization_controlled_executor_design.json",
                "--formal-initialization-readonly-com-preflight-design-json",
                formal_initialization_readonly_com_preflight_design_dir
                / "v1_5_formal_initialization_readonly_com_preflight_design.json",
                "--formal-initialization-readonly-com-preflight-blocked-executor-json",
                formal_initialization_readonly_com_preflight_blocked_executor_dir
                / "v1_5_formal_initialization_readonly_com_preflight_blocked_executor.json",
                "--pre-gas-readiness-json",
                pre_gas_readiness_dir / "v1_5_pre_gas_readiness.json",
                "--getco-readiness-json",
                getco_readiness_dir / "v1_5_getco_identity_readiness.json",
                "--run-evidence-status-json",
                root / "v1_5_run_evidence_status.json",
                "--full-flow-closure-readiness-json",
                closure_readiness_dir / "v1_5_full_flow_closure_readiness.json",
                "--archive-closure-json",
                root / "formal_archive_closure_from_full_chain" / "v1_5_formal_archive_closure_index.json",
                "--algorithm-profile-runner-dry-run-json",
                algorithm_profile_runner_dir / "v1_5_algorithm_profile_runner_dry_run.json",
                "--formal-database-dry-run-json",
                formal_database_dry_run_dir / "v1_5_formal_database_dry_run.json",
                "--formal-database-import-preflight-json",
                formal_database_import_preflight_dir / "v1_5_formal_database_import_preflight.json",
                "--formal-database-import-authorization-json",
                formal_database_import_authorization_dir / "v1_5_formal_database_import_authorization.json",
                "--formal-database-import-command-contract-json",
                formal_database_import_command_contract_dir / "v1_5_formal_database_import_command_contract.json",
                "--formal-database-import-blocked-executor-json",
                formal_database_import_blocked_executor_dir / "v1_5_formal_database_import_blocked_executor.json",
                "--formal-database-import-controlled-executor-design-json",
                formal_database_import_controlled_executor_design_dir
                / "v1_5_formal_database_import_controlled_executor_design.json",
            ),
            required_inputs=(
                "initialization readiness sidecar",
                "controlled initialization executor design sidecar",
                "read-only initialization COM preflight design sidecar",
                "read-only initialization COM preflight blocked executor sidecar",
                "identity/GETCO readiness sidecar",
                "pre-gas readiness sidecar",
                "v1_5_run_evidence_status.json",
                "full-flow closure readiness or archive sidecar when available",
                "optional new-algorithm profile runner dry-run bundle",
                "formal PostgreSQL 18 database dry-run contract",
                "formal database import preflight sidecar",
                "formal database import authorization sidecar",
                "formal database import command contract sidecar",
                "blocked database import executor sidecar",
                "controlled database import executor design sidecar",
            ),
            expected_outputs=(
                "formal_run_status/v1_5_formal_run_status.json",
                "formal_run_status/v1_5_formal_run_status.md",
                "formal_run_status/v1_5_formal_run_status_gates.csv",
                "formal_run_status/v1_5_formal_run_status_gaps.csv",
            ),
            physical_meaning=(
                "After evidence status and closure sidecars are refreshed, this dashboard answers the production "
                "question directly: current stage, next action, whether physical flow can continue, and whether "
                "formal archive/database release is allowed."
            ),
            execution_mode="offline_sidecar",
            gate="final_reviewer_status_overview",
            notes=(
                "This exporter reads sidecars only; it does not open COM, connect PostgreSQL, control routes, or write coefficients.",
                "A release-ready status is still reviewer evidence, not an implicit real-device action.",
            ),
        )
    )

    _validate_steps(steps)
    warnings = tuple(
        item
        for item in (
            "" if co2_queue else "CO2 queue CSV is not specified; command uses a placeholder.",
            "" if h2o_queue else "H2O queue CSV is not specified; command uses a placeholder.",
            "" if pressure_ref else "Pressure reference JSON is not specified; formal pressure review will need it.",
            "" if gases else "Standard-gas certificate JSON is not specified; formal run package will use templates.",
        )
        if item
    )

    return FullFlowPlan(
        schema=PLAN_SCHEMA,
        contract=PLAN_CONTRACT,
        run_id=rid,
        created_at=datetime.now().isoformat(timespec="seconds"),
        config_path=str(cfg),
        output_dir=str(root),
        dry_run_only=True,
        safety_contract={
            "does_not_modify_run_app": True,
            "planner_opens_com_ports": False,
            "planner_controls_routes": False,
            "planner_writes_coefficients": False,
            "planner_writes_device_id": False,
            "v2_real_com_forbidden": True,
            "uses_validated_v1_5_behavior_not_folder_name_only": True,
            "reference_serial_bank_shift_default_enabled": False,
            "reference_serial_bank_shift_allowed_scope": "COM24-COM31_between_COM16-COM23_only",
            "reference_serial_protocol_match_default_enabled": False,
            "reference_serial_protocol_match_source": "operator_or_ui_supplied_inventory_only_no_default_real_COM_probe",
            "gas_analyzer_serial_ports_protected": "COM35-COM42_use_MODE2_identity_binding",
        },
        coefficient_epoch_contract={
            "initialization": "plan_initialization_contract_then_read_and_freeze_GETCO1_to_GETCO9_before_auxiliary_neutralization_and_sampling",
            "do_not_clear_existing_coefficients_on_startup": False,
            "clear_or_neutralize_auxiliary_groups_after_epoch0_snapshot": "SENCO5,SENCO6,SENCO7,SENCO8,SENCO9",
            "displayed_values_are_coefficient_affected": True,
            "fit_primary_evidence": "MODE2_factory_ratio_raw_ratio_signal_plus_traceable_reference",
            "new_epoch_after_verified_write": True,
            "identity_key": "analyzer_device_id_not_com_port_or_ga_alias",
        },
        physical_order=(
            "formal_initialization_contract_readiness_and_pre_gas_sidecar",
            "formal_initialization_executor_dry_run_review",
            "formal_initialization_blocked_executor_before_live_initialization",
            "formal_initialization_controlled_executor_design_before_live_initialization",
            "formal_initialization_readonly_com_preflight_design_before_readonly_real_com",
            "formal_initialization_readonly_com_preflight_blocked_executor_before_readonly_real_com",
            "device_identity_and_GETCO_snapshot",
            "identity_GETCO_readiness_snapshot",
            "controlled_auxiliary_SENCO5_6_7_8_9_neutralization_after_GETCO_backup",
            "pressure_quick_check_then_SENCO9_no_write_acquisition_if_needed",
            "pressure_channel_completion_evidence_after_SENCO9_write_and_reverify",
            "temperature_fast_review_and_process_evidence",
            "CO2_open_flow",
            "H2O_open_flow",
            "temperature_SENCO7_8_candidate_review",
            "factory_signal_health_review",
            "CO2_H2O_candidate_review",
            "controlled_write_only_after_approval",
            "post_write_reverification",
            "formal_database_dry_run_before_database_import",
            "formal_database_import_preflight_before_database_import",
            "formal_database_import_authorization_before_database_import",
            "formal_database_import_command_contract_before_database_import",
            "formal_database_import_blocked_executor_before_database_import",
            "formal_database_import_controlled_executor_design_before_database_import",
            "evidence_bundle_database_report",
            "formal_run_status_dashboard",
        ),
        steps=tuple(steps),
        warnings=warnings,
        metadata={
            "operator": operator,
            "analyzer_id": analyzer_id,
            "standard_gases_json": gases,
            "pressure_reference_json": pressure_ref,
            "co2_queue_csv": co2_queue,
            "h2o_queue_csv": h2o_queue,
            "optional_reference_serial_port_binding_tool": (
                "gas_calibrator.tools.prepare_v1_5_runtime_serial_port_binding"
            ),
        },
    )


def run_dir_as_path(value: str) -> Path:
    if value.startswith("<") and value.endswith(">"):
        return Path(value)
    return Path(value)


def _command_line(command: Sequence[str]) -> str:
    def quote(part: str) -> str:
        if not part:
            return '""'
        if any(ch.isspace() for ch in part) or "`" in part or "<" in part or ">" in part:
            return '"' + part.replace('"', '\\"') + '"'
        return part

    return " ".join(quote(str(part)) for part in command)


def render_full_flow_stage_manifest_markdown(manifest: FullFlowStageManifest) -> str:
    """Render a human-readable V1.5 stage manifest review."""

    lines = [
        "# V1.5 Full-Flow Stage Manifest",
        "",
        f"- schema: `{manifest.schema}`",
        f"- run_id: `{manifest.run_id}`",
        f"- contract: `{manifest.contract}`",
        f"- automation_level: `{manifest.current_automation_level}`",
        f"- one_button_live_runner_ready: `{manifest.one_button_live_runner_ready}`",
        "",
        "## Safety Summary",
        "",
    ]
    for key, value in manifest.safety_summary.items():
        lines.append(f"- `{key}`: `{value}`")

    lines.extend(["", "## Automation Summary", "", "| State | Count |", "| --- | ---: |"])
    for key, value in manifest.automation_summary.items():
        lines.append(f"| `{key}` | {value} |")

    lines.extend(
        [
            "",
            "## Stage Contract",
            "",
            "| Order | Phase | Step | Automation | Gate | Tool |",
            "| ---: | --- | --- | --- | --- | --- |",
        ]
    )
    for stage in manifest.stages:
        tool = stage.tool_module or "manual/placeholder"
        lines.append(
            f"| {stage.order} | `{stage.phase}` | `{stage.step_id}` | "
            f"`{stage.automation_state}` | `{stage.gate}` | `{tool}` |"
        )

    lines.extend(["", "## Live And Write Gates", ""])
    gated = [
        stage
        for stage in manifest.stages
        if any(stage.authorization_required.values()) or "requires_authorization" in stage.automation_state
    ]
    if gated:
        lines.extend(
            [
                "| Step | Real COM | Pressure | Route | Write | Device ID |",
                "| --- | --- | --- | --- | --- | --- |",
            ]
        )
        for stage in gated:
            auth = stage.authorization_required
            lines.append(
                f"| `{stage.step_id}` | `{auth['real_com']}` | `{auth['pressure_control']}` | "
                f"`{auth['route_control']}` | `{auth['coefficient_write']}` | `{auth['device_id_write']}` |"
            )
    else:
        lines.append("- none")

    lines.extend(["", "## Evidence Requirements", ""])
    for stage in manifest.stages:
        if not stage.required_inputs and not stage.expected_outputs:
            continue
        lines.append(f"### {stage.order}. `{stage.step_id}`")
        if stage.required_inputs:
            lines.append("")
            lines.append("Required inputs:")
            for item in stage.required_inputs:
                lines.append(f"- {item}")
        if stage.expected_outputs:
            lines.append("")
            lines.append("Expected outputs:")
            for item in stage.expected_outputs:
                lines.append(f"- {item}")
        lines.append("")

    lines.extend(
        [
            "## Guardrail",
            "",
            "- This manifest is generated from the V1.5 full-flow plan.",
            "- It does not execute commands, open COM ports, control valves, control PACE, or write SENCO.",
            "- A future executor must treat `automation_state` and `authorization_required` as hard gates.",
        ]
    )
    return "\n".join(lines).rstrip() + "\n"


def write_full_flow_stage_manifest(
    manifest: FullFlowStageManifest,
    output_dir: str | Path,
) -> dict[str, Path]:
    """Write machine-readable and reviewer-facing stage manifest artifacts."""

    root = Path(output_dir).resolve()
    root.mkdir(parents=True, exist_ok=True)
    json_path = root / "v1_5_full_flow_stage_manifest.json"
    md_path = root / "v1_5_full_flow_stage_manifest.md"
    json_path.write_text(json.dumps(manifest.to_json(), ensure_ascii=False, indent=2), encoding="utf-8")
    md_path.write_text(render_full_flow_stage_manifest_markdown(manifest), encoding="utf-8")
    return {"stage_manifest_json": json_path, "stage_manifest_markdown": md_path}


def render_full_flow_live_runner_readiness_markdown(readiness: FullFlowLiveRunnerReadiness) -> str:
    """Render a reviewer-facing live-runner readiness report."""

    lines = [
        "# V1.5 全流程 Live Runner Readiness",
        "",
        f"- schema: `{readiness.schema}`",
        f"- run_id: `{readiness.run_id}`",
        f"- one_button_live_runner_ready: `{readiness.one_button_live_runner_ready}`",
        f"- automation_level: `{readiness.current_automation_level}`",
        f"- summary: {readiness.summary}",
        "",
        "## Required Authorizations",
        "",
    ]
    if readiness.required_authorizations:
        for item in readiness.required_authorizations:
            lines.append(f"- `{item}`")
    else:
        lines.append("- none")

    lines.extend(
        [
            "",
            "## Domain Readiness",
            "",
            "| Domain | Status | Required authorization | Physical risk | Next action |",
            "| --- | --- | --- | --- | --- |",
        ]
    )
    for domain in readiness.domains:
        auth = ", ".join(domain.required_authorizations) if domain.required_authorizations else "none"
        lines.append(
            f"| `{domain.domain}` | `{domain.status}` | `{auth}` | "
            f"{domain.physical_risk or '-'} | {domain.next_action or '-'} |"
        )

    lines.extend(["", "## Not Ready Reasons", ""])
    for reason in readiness.not_ready_reasons:
        lines.append(f"- {reason}")

    lines.extend(
        [
            "",
            "## Guardrail",
            "",
            "- This readiness artifact is generated offline from the V1.5 full-flow plan.",
            "- It does not open COM ports, control PACE, control gas/water routes, or write SENCO.",
            "- `one_button_live_runner_ready=false` is intentional until controlled live gates are implemented end to end.",
        ]
    )
    return "\n".join(lines).rstrip() + "\n"


def write_full_flow_live_runner_readiness(
    readiness: FullFlowLiveRunnerReadiness,
    output_dir: str | Path,
) -> dict[str, Path]:
    """Write machine-readable and reviewer-facing live-runner readiness artifacts."""

    root = Path(output_dir).resolve()
    root.mkdir(parents=True, exist_ok=True)
    json_path = root / "v1_5_full_flow_live_runner_readiness.json"
    md_path = root / "v1_5_full_flow_live_runner_readiness.md"
    json_path.write_text(json.dumps(readiness.to_json(), ensure_ascii=False, indent=2), encoding="utf-8")
    md_path.write_text(render_full_flow_live_runner_readiness_markdown(readiness), encoding="utf-8")
    return {"live_runner_readiness_json": json_path, "live_runner_readiness_markdown": md_path}


def write_full_flow_plan(plan: FullFlowPlan, output_dir: str | Path | None = None) -> dict[str, Path]:
    """Write JSON, Markdown, and PowerShell command artifacts for a plan."""

    root = Path(output_dir or plan.output_dir).resolve()
    root.mkdir(parents=True, exist_ok=True)
    json_path = root / "v1_5_full_flow_plan.json"
    md_path = root / "v1_5_full_flow_plan.md"
    ps1_path = root / "v1_5_full_flow_commands.ps1"

    json_path.write_text(json.dumps(plan.to_json(), ensure_ascii=False, indent=2), encoding="utf-8")

    lines = [
        "# V1.5 Full Calibration Flow Plan",
        "",
        f"- run_id: `{plan.run_id}`",
        f"- contract: `{plan.contract}`",
        f"- dry_run_only: `{plan.dry_run_only}`",
        "",
        "## Safety Contract",
        "",
    ]
    for key, value in plan.safety_contract.items():
        lines.append(f"- `{key}`: `{value}`")
    lines.extend(["", "## Coefficient Epoch Contract", ""])
    for key, value in plan.coefficient_epoch_contract.items():
        lines.append(f"- `{key}`: `{value}`")
    lines.extend(["", "## Steps", ""])
    for index, step in enumerate(plan.steps, start=1):
        lines.extend(
            [
                f"### {index}. {step.title}",
                "",
                f"- step_id: `{step.step_id}`",
                f"- phase: `{step.phase}`",
                f"- execution_mode: `{step.execution_mode}`",
                f"- gate: `{step.gate}`",
                f"- opens_com_ports: `{step.opens_com_ports}`",
                f"- controls_gas_route: `{step.controls_gas_route}`",
                f"- controls_water_route: `{step.controls_water_route}`",
                f"- writes_coefficients: `{step.writes_coefficients}`",
                f"- coefficient_epoch_event: `{step.coefficient_epoch_event}`",
                f"- physical_meaning: {step.physical_meaning}",
            ]
        )
        if step.command:
            lines.extend(["", "```powershell", _command_line(step.command), "```"])
        if step.notes:
            lines.append("")
            for note in step.notes:
                lines.append(f"- note: {note}")
        lines.append("")
    if plan.warnings:
        lines.extend(["## Warnings", ""])
        for warning in plan.warnings:
            lines.append(f"- {warning}")
    md_path.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")

    ps_lines = [
        "# V1.5 full-flow dry-run command list.",
        "# Review each command and gate before executing. Controlled writes are placeholders only.",
        "$env:PYTHONPATH = \"src\"",
        "",
    ]
    for step in plan.steps:
        ps_lines.append(f"# {step.step_id}: {step.title}")
        if step.command and not step.writes_coefficients:
            ps_lines.append(_command_line(step.command))
        else:
            ps_lines.append(f"# blocked/manual: {step.execution_mode}")
        ps_lines.append("")
    ps1_path.write_text("\n".join(ps_lines).rstrip() + "\n", encoding="utf-8")

    outputs = {"json": json_path, "markdown": md_path, "powershell": ps1_path}
    outputs.update(write_full_flow_stage_manifest(build_full_flow_stage_manifest(plan), root))
    outputs.update(write_full_flow_live_runner_readiness(build_full_flow_live_runner_readiness(plan), root))
    return outputs


def write_full_flow_state(
    state: FullFlowState,
    output_dir: str | Path,
) -> dict[str, Path]:
    """Write resumable V1.5 full-flow state artifacts."""

    root = Path(output_dir).resolve()
    root.mkdir(parents=True, exist_ok=True)
    json_path = root / "v1_5_full_flow_state.json"
    md_path = root / "v1_5_full_flow_state.md"

    json_path.write_text(json.dumps(state.to_json(), ensure_ascii=False, indent=2), encoding="utf-8")

    lines = [
        "# V1.5 Full Calibration Flow State",
        "",
        f"- run_id: `{state.run_id}`",
        f"- current_step_id: `{state.current_step_id}`",
        f"- current_status: `{state.current_status}`",
        f"- allow_real_com: `{state.allow_real_com}`",
        f"- allow_pressure_control: `{state.allow_pressure_control}`",
        f"- allow_route_control: `{state.allow_route_control}`",
        f"- allow_writes: `{state.allow_writes}`",
        "",
        "## Stage State",
        "",
        "| Step | Status | Can execute | Reason |",
        "| --- | --- | --- | --- |",
    ]
    for stage in state.stage_states:
        lines.append(
            f"| `{stage.step_id}` | `{stage.status}` | `{stage.can_execute_now}` | {stage.reason} |"
        )
    lines.extend(
        [
            "",
            "## Safety Notes",
            "",
            "- This state file does not execute any command by itself.",
            "- Real COM, pressure, gas route, water route, and coefficient-write stages stay blocked unless separately authorized.",
            "- Coefficient-write stages remain manual review stages in this planner even if write authorization is recorded elsewhere.",
        ]
    )
    md_path.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")
    return {"state_json": json_path, "state_markdown": md_path}


def write_full_flow_supervised_run(
    result: FullFlowSupervisedRun,
    output_dir: str | Path,
) -> dict[str, Path]:
    """Write supervised offline-advance evidence for a V1.5 full-flow plan."""

    root = Path(output_dir).resolve()
    root.mkdir(parents=True, exist_ok=True)
    json_path = root / "v1_5_full_flow_supervised_run.json"
    md_path = root / "v1_5_full_flow_supervised_run.md"

    json_path.write_text(json.dumps(result.to_json(), ensure_ascii=False, indent=2), encoding="utf-8")

    lines = [
        "# V1.5 Full Calibration Supervised Run",
        "",
        f"- run_id: `{result.run_id}`",
        f"- execute_commands: `{result.execute_commands}`",
        f"- max_steps: `{result.max_steps}`",
        f"- allow_database_import: `{result.allow_database_import}`",
        f"- initial_current_step: `{result.initial_state.current_step_id}`",
        f"- initial_status: `{result.initial_state.current_status}`",
        f"- final_current_step: `{result.final_state.current_step_id}`",
        f"- final_status: `{result.final_state.current_status}`",
        "",
        "## Events",
        "",
        "| Step | Status | Return code | Reason |",
        "| --- | --- | --- | --- |",
    ]
    if result.events:
        for event in result.events:
            returncode = "" if event.returncode is None else str(event.returncode)
            lines.append(f"| `{event.step_id}` | `{event.status}` | `{returncode}` | {event.reason} |")
    else:
        lines.append("|  | `no_events` |  | supervisor_had_no_ready_stage_to_process |")
    lines.extend(
        [
            "",
            "## Safety Notes",
            "",
            "- The supervisor only runs stages with no COM, no pressure control, no gas/water route control, no device ID write, and no SENCO write.",
            "- Database import is excluded unless `allow_database_import` is explicitly set.",
            "- Physical stages must continue through their dedicated V1.5 real-run tools and operator authorization.",
        ]
    )
    md_path.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")
    return {"supervised_json": json_path, "supervised_markdown": md_path}
