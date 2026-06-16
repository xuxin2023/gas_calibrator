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
    return step.execution_mode in {"offline_sidecar", "offline_review"}


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
    rid = run_id or f"v1_5_full_flow_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    formal_pkg = root / "formal_run_package"
    getco_dir = root / "coefficient_epoch_0_getco_snapshot"
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
            step_id="auxiliary_senco56789_neutralization_gate",
            title="Neutralize auxiliary SENCO5-9 after epoch-0 GETCO backup",
            phase="AUXILIARY_COEFFICIENT_NEUTRALIZATION",
            tool_module=None,
            required_inputs=(
                "old GETCO1-9 epoch-0 snapshot",
                "runtime_identity_bound_config.json",
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
            "initialization": "read_and_freeze_GETCO1_to_GETCO9_before_auxiliary_neutralization_and_sampling",
            "do_not_clear_existing_coefficients_on_startup": False,
            "clear_or_neutralize_auxiliary_groups_after_epoch0_snapshot": "SENCO5,SENCO6,SENCO7,SENCO8,SENCO9",
            "displayed_values_are_coefficient_affected": True,
            "fit_primary_evidence": "MODE2_factory_ratio_raw_ratio_signal_plus_traceable_reference",
            "new_epoch_after_verified_write": True,
            "identity_key": "analyzer_device_id_not_com_port_or_ga_alias",
        },
        physical_order=(
            "device_identity_and_GETCO_snapshot",
            "controlled_auxiliary_SENCO5_6_7_8_9_neutralization_after_GETCO_backup",
            "pressure_quick_check_then_SENCO9_no_write_acquisition_if_needed",
            "temperature_fast_review_and_process_evidence",
            "CO2_open_flow",
            "H2O_open_flow",
            "temperature_SENCO7_8_candidate_review",
            "factory_signal_health_review",
            "CO2_H2O_candidate_review",
            "controlled_write_only_after_approval",
            "post_write_reverification",
            "evidence_bundle_database_report",
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

    return {"json": json_path, "markdown": md_path, "powershell": ps1_path}


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
