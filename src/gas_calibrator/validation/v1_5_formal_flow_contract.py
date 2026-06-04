"""Validate the V1.5 formal calibration flow contract.

This module is an offline reviewer. It checks that a generated full-flow plan
still follows the V1.5 physical calibration sequence and does not quietly pull
diagnostic tools, V2 modules, or automatic coefficient writes into the formal
route.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence


EXPECTED_CONTRACT = "pressure_first_temperature_review_then_open_flow_components"
REPORT_SCHEMA = "v1_5_formal_flow_contract_report_v1"

REQUIRED_STEP_IDS = (
    "load_plan_and_traceability",
    "device_identity_and_getco_snapshot",
    "pressure_quick_check",
    "pressure_senco9_no_write_review",
    "temperature_channel_fast_review",
    "co2_open_flow_sampling",
    "h2o_open_flow_sampling",
    "fit_input_quality_review",
    "co2_candidate_write_review",
    "controlled_component_write_placeholder",
    "post_write_reverification_placeholder",
    "formal_evidence_sidecar",
    "database_import",
    "zh_calibration_reports",
)

REQUIRED_ORDER = (
    "load_plan_and_traceability",
    "device_identity_and_getco_snapshot",
    "pressure_quick_check",
    "pressure_senco9_no_write_review",
    "temperature_channel_fast_review",
    "co2_open_flow_sampling",
    "h2o_open_flow_sampling",
    "fit_input_quality_review",
    "co2_candidate_write_review",
    "controlled_component_write_placeholder",
    "post_write_reverification_placeholder",
    "formal_evidence_sidecar",
)

ALLOWED_SHARED_TOOL_MODULES = {
    "gas_calibrator.tools.validate_pressure_only",
}

POST_WRITE_REVERIFY_MODULE = "gas_calibrator.tools.export_v1_5_post_write_reverification"

FORMAL_PHYSICAL_FLOW = (
    "LOAD_PLAN: freeze plan, certificates, config hash, and run identity",
    "PRECHECK: bind analyzer device IDs to ports and snapshot GETCO1-9",
    "PRESSURE: verify analyzer P against COM22 before component calibration",
    "TEMPERATURE: review chamber/case temperature evidence before final approval",
    "CO2_OPEN_FLOW: sample clean dry gas under continuous open flow",
    "H2O_OPEN_FLOW: sample water route under dewpoint/reference evidence",
    "QC: keep raw frames, rejected frames, reasons, and fit-eligible samples",
    "CANDIDATE_REVIEW: derive coefficients only from role-eligible evidence",
    "CONTROLLED_WRITE: write only through explicit controlled tools and readback",
    "POST_WRITE_REVERIFY: verify updated output before archive and report",
    "ARCHIVE_REPORT: bundle evidence, database index, and Chinese reports",
)


@dataclass(frozen=True)
class FlowContractIssue:
    severity: str
    code: str
    message: str
    step_id: str = ""

    def to_json(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class FlowContractReport:
    schema: str
    status: str
    contract: str
    phase_sequence: tuple[str, ...]
    step_sequence: tuple[str, ...]
    formal_runner_steps: tuple[str, ...]
    physical_flow: tuple[str, ...]
    issues: tuple[FlowContractIssue, ...]
    warnings: tuple[FlowContractIssue, ...]

    def to_json(self) -> dict[str, Any]:
        return {
            "schema": self.schema,
            "status": self.status,
            "contract": self.contract,
            "phase_sequence": list(self.phase_sequence),
            "step_sequence": list(self.step_sequence),
            "formal_runner_steps": list(self.formal_runner_steps),
            "physical_flow": list(self.physical_flow),
            "issues": [item.to_json() for item in self.issues],
            "warnings": [item.to_json() for item in self.warnings],
        }


def _plan_payload(plan: Mapping[str, Any] | Any) -> Mapping[str, Any]:
    if isinstance(plan, Mapping):
        return plan
    if hasattr(plan, "to_json"):
        payload = plan.to_json()
        if isinstance(payload, Mapping):
            return payload
    raise TypeError("plan must be a mapping or expose to_json()")


def _steps(plan: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    values = plan.get("steps") or []
    return [item for item in values if isinstance(item, Mapping)]


def _step_id(step: Mapping[str, Any]) -> str:
    return str(step.get("step_id") or "")


def _tool_module(step: Mapping[str, Any]) -> str:
    return str(step.get("tool_module") or "")


def _module_from_entry_path(path: str) -> str:
    rel = path.replace("\\", "/")
    if rel.startswith("src/"):
        rel = rel[4:]
    if rel.endswith(".py"):
        rel = rel[:-3]
    return rel.replace("/", ".")


def _inventory_by_module(
    inventory_entries: Iterable[Mapping[str, Any]] | Mapping[str, Any] | None,
) -> dict[str, Mapping[str, Any]]:
    if inventory_entries is None:
        return {}
    if isinstance(inventory_entries, Mapping):
        raw_entries = inventory_entries.get("entries") or []
    else:
        raw_entries = inventory_entries
    by_module: dict[str, Mapping[str, Any]] = {}
    for entry in raw_entries:
        if not isinstance(entry, Mapping):
            continue
        module = _module_from_entry_path(str(entry.get("path") or ""))
        if module:
            by_module[module] = entry
    return by_module


def _issue(severity: str, code: str, message: str, step_id: str = "") -> FlowContractIssue:
    return FlowContractIssue(severity=severity, code=code, message=message, step_id=step_id)


def _order_index(step_ids: Sequence[str], step_id: str) -> int:
    try:
        return step_ids.index(step_id)
    except ValueError:
        return -1


def _require_before(
    step_ids: Sequence[str],
    before: str,
    after: str,
    issues: list[FlowContractIssue],
) -> None:
    left = _order_index(step_ids, before)
    right = _order_index(step_ids, after)
    if left < 0 or right < 0:
        return
    if left >= right:
        issues.append(
            _issue(
                "error",
                "physical_order_violation",
                f"{before} must appear before {after}",
                after,
            )
        )


def validate_v1_5_formal_flow_contract(
    plan: Mapping[str, Any] | Any,
    *,
    inventory_entries: Iterable[Mapping[str, Any]] | Mapping[str, Any] | None = None,
) -> FlowContractReport:
    """Return an offline contract report for a V1.5 full-flow plan."""

    payload = _plan_payload(plan)
    steps = _steps(payload)
    step_ids = tuple(_step_id(step) for step in steps)
    phases = tuple(str(step.get("phase") or "") for step in steps)
    issues: list[FlowContractIssue] = []
    warnings: list[FlowContractIssue] = []

    if payload.get("contract") != EXPECTED_CONTRACT:
        issues.append(
            _issue(
                "error",
                "unexpected_contract",
                f"expected {EXPECTED_CONTRACT}, got {payload.get('contract')!r}",
            )
        )
    if payload.get("dry_run_only") is not True:
        issues.append(_issue("error", "plan_not_dry_run_only", "full-flow planner must stay dry-run only"))

    safety = payload.get("safety_contract") or {}
    expected_safety = {
        "does_not_modify_run_app": True,
        "planner_opens_com_ports": False,
        "planner_controls_routes": False,
        "planner_writes_coefficients": False,
        "planner_writes_device_id": False,
        "v2_real_com_forbidden": True,
    }
    for key, expected in expected_safety.items():
        if safety.get(key) is not expected:
            issues.append(_issue("error", "safety_contract_violation", f"{key} must be {expected!r}"))

    for required in REQUIRED_STEP_IDS:
        if required not in step_ids:
            issues.append(_issue("error", "missing_required_step", f"missing required step {required}", required))

    if len(step_ids) != len(set(step_ids)):
        issues.append(_issue("error", "duplicate_step_id", "step IDs must be unique"))

    for before, after in zip(REQUIRED_ORDER, REQUIRED_ORDER[1:]):
        _require_before(step_ids, before, after, issues)
    _require_before(step_ids, "pressure_quick_check", "co2_open_flow_sampling", issues)
    _require_before(step_ids, "pressure_quick_check", "h2o_open_flow_sampling", issues)
    _require_before(step_ids, "temperature_channel_fast_review", "co2_open_flow_sampling", issues)
    _require_before(step_ids, "temperature_channel_fast_review", "h2o_open_flow_sampling", issues)
    _require_before(step_ids, "controlled_component_write_placeholder", "post_write_reverification_placeholder", issues)
    _require_before(step_ids, "post_write_reverification_placeholder", "formal_evidence_sidecar", issues)
    _require_before(step_ids, "formal_evidence_sidecar", "database_import", issues)
    _require_before(step_ids, "formal_evidence_sidecar", "zh_calibration_reports", issues)

    by_module = _inventory_by_module(inventory_entries)
    formal_runner_steps: list[str] = []
    for step in steps:
        step_id = _step_id(step)
        module = _tool_module(step)
        command = step.get("command") or ()
        writes = bool(step.get("writes_coefficients"))
        controls_route = bool(step.get("controls_gas_route") or step.get("controls_water_route"))

        if ".v2" in module.lower() or any(".v2" in str(part).lower() for part in command):
            issues.append(_issue("error", "v2_reference_forbidden", "V1.5 formal plan must not reference V2", step_id))

        if writes:
            if module or command:
                issues.append(
                    _issue(
                        "error",
                        "auto_write_forbidden",
                        "coefficient writes must remain manual placeholders in the full-flow planner",
                        step_id,
                    )
                )
            if step.get("execution_mode") != "blocked_pending_explicit_authorization":
                issues.append(
                    _issue(
                        "error",
                        "write_gate_not_blocked",
                        "controlled write step must require explicit authorization",
                        step_id,
                    )
                )

        if step_id == "post_write_reverification_placeholder":
            if module != POST_WRITE_REVERIFY_MODULE:
                issues.append(
                    _issue(
                        "error",
                        "post_write_reverify_wrong_tool",
                        "post-write reverification must use the offline V1.5 review exporter",
                        step_id,
                    )
                )
            if not any("<" in str(part) and ">" in str(part) for part in command):
                issues.append(
                    _issue(
                        "error",
                        "post_write_reverify_missing_placeholders",
                        "post-write reverification must keep explicit review placeholders in the full-flow plan",
                        step_id,
                    )
                )

        if not module:
            continue
        if module in ALLOWED_SHARED_TOOL_MODULES:
            continue
        entry = by_module.get(module)
        if entry is None:
            warnings.append(
                _issue(
                    "warning",
                    "entrypoint_not_in_inventory",
                    f"{module} is not present in the supplied V1.5 inventory",
                    step_id,
                )
            )
            continue
        category = str(entry.get("category") or "")
        if category == "diagnostic_only":
            issues.append(
                _issue(
                    "error",
                    "diagnostic_tool_in_formal_flow",
                    f"{module} is diagnostic-only and must not be a formal flow step",
                    step_id,
                )
            )
        if category == "controlled_write":
            issues.append(
                _issue(
                    "error",
                    "write_tool_in_formal_planner",
                    f"{module} is a controlled write tool and must not be auto-planned",
                    step_id,
                )
            )
        if controls_route:
            if category != "formal_runner":
                issues.append(
                    _issue(
                        "error",
                        "route_stage_not_formal_runner",
                        f"{module} controls a route but is classified as {category}",
                        step_id,
                    )
                )
            else:
                formal_runner_steps.append(step_id)

    status = "blocked" if any(item.severity == "error" for item in issues) else "pass"
    return FlowContractReport(
        schema=REPORT_SCHEMA,
        status=status,
        contract=str(payload.get("contract") or ""),
        phase_sequence=phases,
        step_sequence=step_ids,
        formal_runner_steps=tuple(formal_runner_steps),
        physical_flow=FORMAL_PHYSICAL_FLOW,
        issues=tuple(issues),
        warnings=tuple(warnings),
    )


def render_v1_5_formal_flow_contract_markdown(report: FlowContractReport) -> str:
    """Render a reviewer-facing Markdown summary."""

    lines = [
        "# V1.5 Formal Flow Contract",
        "",
        f"- status: `{report.status}`",
        f"- contract: `{report.contract}`",
        "",
        "## Physical Flow",
        "",
    ]
    for item in report.physical_flow:
        lines.append(f"- {item}")
    lines.extend(["", "## Step Sequence", ""])
    for index, step_id in enumerate(report.step_sequence, start=1):
        phase = report.phase_sequence[index - 1] if index - 1 < len(report.phase_sequence) else ""
        lines.append(f"{index}. `{phase}` / `{step_id}`")
    lines.extend(["", "## Formal Route Runners", ""])
    if report.formal_runner_steps:
        for item in report.formal_runner_steps:
            lines.append(f"- `{item}`")
    else:
        lines.append("- none")
    lines.extend(["", "## Issues", ""])
    if report.issues:
        for issue in report.issues:
            suffix = f" ({issue.step_id})" if issue.step_id else ""
            lines.append(f"- `{issue.severity}` `{issue.code}`{suffix}: {issue.message}")
    else:
        lines.append("- none")
    lines.extend(["", "## Warnings", ""])
    if report.warnings:
        for warning in report.warnings:
            suffix = f" ({warning.step_id})" if warning.step_id else ""
            lines.append(f"- `{warning.severity}` `{warning.code}`{suffix}: {warning.message}")
    else:
        lines.append("- none")
    return "\n".join(lines).rstrip() + "\n"


def read_json(path: str | Path) -> Mapping[str, Any]:
    import json

    return json.loads(Path(path).read_text(encoding="utf-8"))
