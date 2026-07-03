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
    "formal_initialization_contract_plan",
    "formal_initialization_executor_dry_run_snapshot",
    "formal_initialization_blocked_executor_snapshot",
    "formal_initialization_controlled_executor_design_snapshot",
    "formal_initialization_readonly_com_preflight_design_snapshot",
    "formal_initialization_readonly_com_preflight_blocked_executor_snapshot",
    "formal_initialization_readonly_com_preflight_controlled_executor_design_snapshot",
    "formal_initialization_readonly_com_preflight_controlled_blocked_executor_snapshot",
    "initialization_readiness_snapshot",
    "pre_gas_readiness_snapshot",
    "device_identity_and_getco_snapshot",
    "identity_getco_readiness_snapshot",
    "auxiliary_senco56789_neutralization_gate",
    "pressure_quick_check",
    "pressure_senco9_no_write_acquisition",
    "pressure_senco9_no_write_review",
    "pressure_channel_completion_audit",
    "temperature_channel_fast_review",
    "co2_open_flow_sampling",
    "h2o_open_flow_sampling",
    "fit_input_quality_review",
    "co2_candidate_write_review",
    "controlled_component_write_placeholder",
    "post_write_reverification_placeholder",
    "formal_evidence_sidecar",
    "formal_database_dry_run_snapshot",
    "formal_database_import_preflight_snapshot",
    "formal_database_import_authorization_snapshot",
    "formal_database_import_command_contract_snapshot",
    "formal_database_import_blocked_executor_snapshot",
    "formal_database_import_controlled_executor_design_snapshot",
    "database_import",
    "zh_calibration_reports",
    "final_evidence_status_refresh",
    "formal_run_status_snapshot",
)

REQUIRED_ORDER = (
    "load_plan_and_traceability",
    "formal_initialization_contract_plan",
    "formal_initialization_executor_dry_run_snapshot",
    "formal_initialization_blocked_executor_snapshot",
    "formal_initialization_controlled_executor_design_snapshot",
    "formal_initialization_readonly_com_preflight_design_snapshot",
    "formal_initialization_readonly_com_preflight_blocked_executor_snapshot",
    "formal_initialization_readonly_com_preflight_controlled_executor_design_snapshot",
    "formal_initialization_readonly_com_preflight_controlled_blocked_executor_snapshot",
    "initialization_readiness_snapshot",
    "pre_gas_readiness_snapshot",
    "device_identity_and_getco_snapshot",
    "identity_getco_readiness_snapshot",
    "auxiliary_senco56789_neutralization_gate",
    "pressure_quick_check",
    "pressure_senco9_no_write_acquisition",
    "pressure_senco9_no_write_review",
    "pressure_channel_completion_audit",
    "temperature_channel_fast_review",
    "co2_open_flow_sampling",
    "h2o_open_flow_sampling",
    "fit_input_quality_review",
    "co2_candidate_write_review",
    "controlled_component_write_placeholder",
    "post_write_reverification_placeholder",
    "formal_evidence_sidecar",
    "formal_database_dry_run_snapshot",
    "formal_database_import_preflight_snapshot",
    "formal_database_import_authorization_snapshot",
    "formal_database_import_command_contract_snapshot",
    "formal_database_import_blocked_executor_snapshot",
    "formal_database_import_controlled_executor_design_snapshot",
    "database_import",
    "zh_calibration_reports",
    "final_evidence_status_refresh",
    "formal_run_status_snapshot",
)

ALLOWED_SHARED_TOOL_MODULES = {
    "gas_calibrator.tools.validate_pressure_only",
}

PRESSURE_SENCO9_EVALUATION_MODULE = "gas_calibrator.tools.export_v1_5_pressure_senco9_evaluation"
PRESSURE_CHANNEL_COMPLETION_MODULE = "gas_calibrator.tools.export_v1_5_pressure_channel_completion"
POST_WRITE_REVERIFY_MODULE = "gas_calibrator.tools.export_v1_5_post_write_reverification"

FORMAL_CO2_TEMPERATURE_ORDER = "desc"
FORMAL_H2O_TEMPERATURE_ORDER = "asc"
FORMAL_CO2_FORBIDDEN_FLAGS = {
    "--skip-stability-gate",
    "--no-control-temperature",
}
FORMAL_H2O_FORBIDDEN_FLAGS = {
    "--skip-dewpoint-gate",
    "--skip-humidity-generator-gate",
    "--pressure-diagnostic-only",
    "--pressure-diagnostic-after-purge",
    "--pressure-diagnostic-observe-hgen-only",
    "--pressure-diagnostic-route-closed-baseline",
    "--pace-vent-after-valve-diagnostic",
    "--no-control-temperature",
}

FORMAL_PHYSICAL_FLOW = (
    "LOAD_PLAN: freeze plan, certificates, config hash, and run identity",
    "INITIALIZATION_CONTRACT: generate the formal initialization plan, PostgreSQL 18 sidecar, and readiness snapshot without COM or writes",
    "INITIALIZATION_EXECUTOR_DRY_RUN: classify initialization plan steps before any live executor is considered",
    "INITIALIZATION_BLOCKED_EXECUTOR: run the no-COM initialization executor stub and prove live initialization remains refused",
    "INITIALIZATION_CONTROLLED_EXECUTOR_DESIGN: freeze future real-COM, write, readback, CHECK, and hold requirements without executing",
    "INITIALIZATION_READONLY_COM_PREFLIGHT_DESIGN: freeze future read-only real-COM port, pacing, identity, GETCO, CHECK, and hold requirements without executing",
    "INITIALIZATION_READONLY_COM_PREFLIGHT_BLOCKED_EXECUTOR: run the no-COM read-only preflight stub and prove analyzer contact remains refused",
    "INITIALIZATION_READONLY_COM_PREFLIGHT_CONTROLLED_EXECUTOR_DESIGN: freeze future controlled read-only COM executor authorization, port, read sequence, evidence, and hold requirements without executing",
    "INITIALIZATION_READONLY_COM_PREFLIGHT_CONTROLLED_BLOCKED_EXECUTOR: prove the controlled read-only COM executor remains blocked and no-COM",
    "PRE_GAS_READINESS: summarize SN/device_code, PostgreSQL 18, MODE2/1Hz, GETCO, S7/S8, S9, route, and CHECK gates before live identity",
    "PRECHECK: bind analyzer device IDs to ports and snapshot GETCO1-9",
    "IDENTITY_GETCO_READINESS: verify epoch-0 GETCO artifacts, no-write conclusion, and runtime identity-bound config before auxiliary coefficient changes",
    "AUX_NEUTRALIZE: after immutable GETCO backup, neutralize SENCO5/6/7/8/9 through controlled tools",
    "PRESSURE: verify analyzer P against COM22 before component calibration",
    "PRESSURE_SENCO9: if needed, use the full V1.5 no-write sealed-pressure runner and transition trace",
    "PRESSURE_COMPLETION: after SENCO9 write and reverification, freeze traceable pressure-channel completion evidence",
    "TEMPERATURE: review chamber/case temperature evidence before final approval",
    "CO2_OPEN_FLOW: sample clean dry gas under continuous open flow",
    "H2O_OPEN_FLOW: sample water route under dewpoint/reference evidence",
    "QC: keep raw frames, rejected frames, reasons, and fit-eligible samples",
    "CANDIDATE_REVIEW: derive coefficients only from role-eligible evidence",
    "CONTROLLED_WRITE: write only through explicit controlled tools and readback",
    "POST_WRITE_REVERIFY: verify updated output before archive and report",
    "FORMAL_DATABASE_DRY_RUN: preview PostgreSQL 18 schema, SN/device_code identity, and insert contracts without connecting or importing",
    "FORMAL_DATABASE_IMPORT_PREFLIGHT: review DSN, migration lock, archive-release dependency, and import authorization without connecting",
    "FORMAL_DATABASE_IMPORT_AUTHORIZATION: review archive release and manual import authorization without connecting",
    "FORMAL_DATABASE_IMPORT_COMMAND_CONTRACT: review the future import command inputs and execution lock without connecting or importing",
    "FORMAL_DATABASE_IMPORT_BLOCKED_EXECUTOR: run the no-connect import command stub and prove PostgreSQL writes remain refused",
    "FORMAL_DATABASE_IMPORT_CONTROLLED_EXECUTOR_DESIGN: freeze future double-authorization, transaction, readback, and rollback requirements without connecting",
    "ARCHIVE_REPORT: bundle evidence, database index, and Chinese reports",
    "FORMAL_RUN_STATUS: refresh the top-level current-stage and release-readiness dashboard from offline sidecars",
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
    physical_boundaries: Mapping[str, Any]
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
            "physical_boundaries": dict(self.physical_boundaries),
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


def _repo_root_from_anchor(anchor: Path) -> Path | None:
    for candidate in (anchor, *anchor.parents):
        if (candidate / "src" / "gas_calibrator" / "validation" / "v1_5_entrypoint_inventory.py").exists():
            return candidate
    return None


def discover_current_v1_5_inventory(*, anchor_paths: Iterable[str | Path] = ()) -> dict[str, list[dict[str, Any]]]:
    """Build the current repo inventory for offline formal-flow contract checks.

    The formal-flow contract is often exported from generated evidence folders.
    When callers do not provide a pinned inventory JSON, use the current V1.5
    source tree so generated reports do not retain stale missing-entrypoint
    warnings after new formal support tools are added.
    """

    anchors = [Path(path).resolve() for path in anchor_paths if str(path or "")]
    anchors.extend((Path.cwd().resolve(), Path(__file__).resolve()))
    repo_root: Path | None = None
    for anchor in anchors:
        root = _repo_root_from_anchor(anchor if anchor.is_dir() else anchor.parent)
        if root is not None:
            repo_root = root
            break
    if repo_root is None:
        repo_root = Path.cwd().resolve()

    from .v1_5_entrypoint_inventory import discover_v1_5_entrypoints

    return {"entries": [entry.to_json() for entry in discover_v1_5_entrypoints(repo_root)]}


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


def _command_value_after(command: Sequence[Any], flag: str) -> str:
    values = [str(part) for part in command]
    try:
        index = values.index(flag)
    except ValueError:
        return ""
    if index + 1 >= len(values):
        return ""
    return values[index + 1]


def _command_has_flag(command: Sequence[Any], flag: str) -> bool:
    return flag in {str(part) for part in command}


def _uses_runtime_identity_bound_config(step: Mapping[str, Any]) -> bool:
    config = _command_value_after(step.get("command") or (), "--config")
    normalized = config.replace("\\", "/")
    return normalized.endswith("coefficient_epoch_0_getco_snapshot/runtime_identity_bound_config.json")


def _require_temperature_order(
    command: Sequence[Any],
    *,
    expected: str,
    step_id: str,
    issues: list[FlowContractIssue],
) -> None:
    actual = _command_value_after(command, "--temperature-order")
    if actual != expected:
        issues.append(
            _issue(
                "error",
                "formal_temperature_order_violation",
                f"{step_id} must use --temperature-order {expected}, got {actual!r}",
                step_id,
            )
        )


def _block_forbidden_flags(
    command: Sequence[Any],
    *,
    forbidden_flags: Iterable[str],
    step_id: str,
    issues: list[FlowContractIssue],
) -> None:
    for flag in sorted(forbidden_flags):
        if _command_has_flag(command, flag):
            issues.append(
                _issue(
                    "error",
                    "formal_open_flow_forbidden_flag",
                    f"{flag} must not be used in the V1.5 formal open-flow full-flow plan",
                    step_id,
                )
            )


def _normalized_pressure_tokens(raw: str) -> set[str]:
    out: set[str] = set()
    for part in str(raw or "").split(","):
        text = part.strip().lower()
        if not text:
            continue
        if text == "ambient":
            out.add("ambient")
            continue
        try:
            value = float(text)
        except ValueError:
            out.add(text)
            continue
        if abs(value - round(value)) < 1e-9:
            out.add(str(int(round(value))))
        else:
            out.add(f"{value:g}")
    return out


def _float_command_value(command: Sequence[Any], flag: str) -> float | None:
    text = _command_value_after(command, flag)
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _require_flag(
    command: Sequence[Any],
    flag: str,
    *,
    step_id: str,
    issues: list[FlowContractIssue],
    code: str,
    message: str,
) -> None:
    if not _command_has_flag(command, flag):
        issues.append(_issue("error", code, message, step_id))


def _require_float_value(
    command: Sequence[Any],
    flag: str,
    *,
    expected: float,
    step_id: str,
    issues: list[FlowContractIssue],
    code: str,
    message: str,
) -> None:
    actual = _float_command_value(command, flag)
    if actual is None or abs(actual - float(expected)) > 1e-9:
        issues.append(_issue("error", code, f"{message}; got {actual!r}", step_id))


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

    coefficient_epoch = payload.get("coefficient_epoch_contract") or {}
    expected_epoch = {
        "do_not_clear_existing_coefficients_on_startup": False,
        "clear_or_neutralize_auxiliary_groups_after_epoch0_snapshot": "SENCO5,SENCO6,SENCO7,SENCO8,SENCO9",
        "displayed_values_are_coefficient_affected": True,
        "identity_key": "analyzer_device_id_not_com_port_or_ga_alias",
    }
    for key, expected in expected_epoch.items():
        if coefficient_epoch.get(key) != expected:
            issues.append(_issue("error", "coefficient_epoch_contract_violation", f"{key} must be {expected!r}"))

    for required in REQUIRED_STEP_IDS:
        if required not in step_ids:
            issues.append(_issue("error", "missing_required_step", f"missing required step {required}", required))

    if len(step_ids) != len(set(step_ids)):
        issues.append(_issue("error", "duplicate_step_id", "step IDs must be unique"))

    for before, after in zip(REQUIRED_ORDER, REQUIRED_ORDER[1:]):
        _require_before(step_ids, before, after, issues)
    _require_before(step_ids, "device_identity_and_getco_snapshot", "identity_getco_readiness_snapshot", issues)
    _require_before(step_ids, "identity_getco_readiness_snapshot", "auxiliary_senco56789_neutralization_gate", issues)
    _require_before(step_ids, "auxiliary_senco56789_neutralization_gate", "pressure_quick_check", issues)
    _require_before(step_ids, "pressure_quick_check", "co2_open_flow_sampling", issues)
    _require_before(step_ids, "pressure_quick_check", "h2o_open_flow_sampling", issues)
    _require_before(step_ids, "pressure_senco9_no_write_review", "pressure_channel_completion_audit", issues)
    _require_before(step_ids, "pressure_channel_completion_audit", "temperature_channel_fast_review", issues)
    _require_before(step_ids, "temperature_channel_fast_review", "co2_open_flow_sampling", issues)
    _require_before(step_ids, "temperature_channel_fast_review", "h2o_open_flow_sampling", issues)
    _require_before(step_ids, "controlled_component_write_placeholder", "post_write_reverification_placeholder", issues)
    _require_before(step_ids, "post_write_reverification_placeholder", "formal_evidence_sidecar", issues)
    _require_before(step_ids, "formal_evidence_sidecar", "formal_database_dry_run_snapshot", issues)
    _require_before(step_ids, "formal_database_dry_run_snapshot", "formal_database_import_preflight_snapshot", issues)
    _require_before(
        step_ids,
        "formal_database_import_preflight_snapshot",
        "formal_database_import_authorization_snapshot",
        issues,
    )
    _require_before(
        step_ids,
        "formal_database_import_authorization_snapshot",
        "formal_database_import_command_contract_snapshot",
        issues,
    )
    _require_before(
        step_ids,
        "formal_database_import_command_contract_snapshot",
        "formal_database_import_blocked_executor_snapshot",
        issues,
    )
    _require_before(
        step_ids,
        "formal_database_import_blocked_executor_snapshot",
        "formal_database_import_controlled_executor_design_snapshot",
        issues,
    )
    _require_before(
        step_ids,
        "formal_initialization_readonly_com_preflight_blocked_executor_snapshot",
        "formal_initialization_readonly_com_preflight_controlled_executor_design_snapshot",
        issues,
    )
    _require_before(
        step_ids,
        "formal_initialization_readonly_com_preflight_controlled_executor_design_snapshot",
        "initialization_readiness_snapshot",
        issues,
    )
    _require_before(
        step_ids,
        "formal_database_import_controlled_executor_design_snapshot",
        "database_import",
        issues,
    )
    _require_before(step_ids, "formal_evidence_sidecar", "zh_calibration_reports", issues)
    _require_before(step_ids, "zh_calibration_reports", "final_evidence_status_refresh", issues)
    _require_before(step_ids, "final_evidence_status_refresh", "formal_run_status_snapshot", issues)

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

        if step_id in {
            "pressure_quick_check",
            "pressure_senco9_no_write_acquisition",
            "co2_open_flow_sampling",
            "h2o_open_flow_sampling",
        }:
            if not _uses_runtime_identity_bound_config(step):
                issues.append(
                    _issue(
                        "error",
                        "physical_stage_not_runtime_identity_bound",
                        (
                            "pressure and open-flow stages must use runtime_identity_bound_config.json "
                            "from the GETCO/device-ID snapshot, not the original COM alias config"
                        ),
                        step_id,
                    )
                )

        if step_id == "formal_initialization_blocked_executor_snapshot":
            if module != "gas_calibrator.tools.run_v1_5_formal_initialization_blocked_executor":
                issues.append(
                    _issue(
                        "error",
                        "formal_initialization_blocked_executor_wrong_tool",
                        "Formal initialization blocked executor must use the no-COM run_v1_5_formal_initialization_blocked_executor stub",
                        step_id,
                    )
                )
            if bool(step.get("opens_com_ports")) or bool(step.get("controls_pressure")) or controls_route or writes:
                issues.append(
                    _issue(
                        "error",
                        "formal_initialization_blocked_executor_must_be_offline_no_write",
                        "Formal initialization blocked executor must not open COM, connect PostgreSQL, control routes/pressure, or write coefficients",
                        step_id,
                    )
                )
            if bool(step.get("writes_device_id")):
                issues.append(
                    _issue(
                        "error",
                        "formal_initialization_blocked_executor_must_not_write_device_id",
                        "Formal initialization blocked executor must not write SN/device_code or protocol device ID",
                        step_id,
                    )
                )
            if not str(step.get("execution_mode") or "").startswith("offline"):
                issues.append(
                    _issue(
                        "error",
                        "formal_initialization_blocked_executor_must_be_offline",
                        "Formal initialization blocked executor execution_mode must be offline",
                        step_id,
                    )
                )
            for flag in (
                "--formal-initialization-executor-dry-run-json",
                "--formal-initialization-plan-json",
                "--output-dir",
                "--fail-on-blocked",
            ):
                _require_flag(
                    command,
                    flag,
                    step_id=step_id,
                    issues=issues,
                    code="formal_initialization_blocked_executor_missing_required_flag",
                    message=f"Formal initialization blocked executor command must include {flag}",
                )

        if step_id == "formal_initialization_controlled_executor_design_snapshot":
            if module != "gas_calibrator.tools.export_v1_5_formal_initialization_controlled_executor_design":
                issues.append(
                    _issue(
                        "error",
                        "formal_initialization_controlled_executor_design_wrong_tool",
                        "Formal initialization controlled executor design must use the offline design exporter",
                        step_id,
                    )
                )
            if bool(step.get("opens_com_ports")) or bool(step.get("controls_pressure")) or controls_route or writes:
                issues.append(
                    _issue(
                        "error",
                        "formal_initialization_controlled_executor_design_must_be_offline_no_write",
                        "Formal initialization controlled executor design must not open COM, connect PostgreSQL, control routes/pressure, or write coefficients",
                        step_id,
                    )
                )
            if bool(step.get("writes_device_id")):
                issues.append(
                    _issue(
                        "error",
                        "formal_initialization_controlled_executor_design_must_not_write_device_id",
                        "Formal initialization controlled executor design must not write SN/device_code or protocol device ID",
                        step_id,
                    )
                )
            if not str(step.get("execution_mode") or "").startswith("offline"):
                issues.append(
                    _issue(
                        "error",
                        "formal_initialization_controlled_executor_design_must_be_offline",
                        "Formal initialization controlled executor design execution_mode must be offline",
                        step_id,
                    )
                )
            for flag in (
                "--formal-initialization-blocked-executor-json",
                "--output-dir",
            ):
                _require_flag(
                    command,
                    flag,
                    step_id=step_id,
                    issues=issues,
                    code="formal_initialization_controlled_executor_design_missing_required_flag",
                    message=f"Formal initialization controlled executor design command must include {flag}",
                )

        if step_id == "formal_initialization_readonly_com_preflight_design_snapshot":
            if module != "gas_calibrator.tools.export_v1_5_formal_initialization_readonly_com_preflight_design":
                issues.append(
                    _issue(
                        "error",
                        "formal_initialization_readonly_com_preflight_design_wrong_tool",
                        "Formal initialization read-only COM preflight design must use the offline design exporter",
                        step_id,
                    )
                )
            if bool(step.get("opens_com_ports")) or bool(step.get("controls_pressure")) or controls_route or writes:
                issues.append(
                    _issue(
                        "error",
                        "formal_initialization_readonly_com_preflight_design_must_be_offline_no_write",
                        "Formal initialization read-only COM preflight design must not open COM, connect PostgreSQL, control routes/pressure, or write coefficients",
                        step_id,
                    )
                )
            if bool(step.get("writes_device_id")):
                issues.append(
                    _issue(
                        "error",
                        "formal_initialization_readonly_com_preflight_design_must_not_write_device_id",
                        "Formal initialization read-only COM preflight design must not write SN/device_code or protocol device ID",
                        step_id,
                    )
                )
            if not str(step.get("execution_mode") or "").startswith("offline"):
                issues.append(
                    _issue(
                        "error",
                        "formal_initialization_readonly_com_preflight_design_must_be_offline",
                        "Formal initialization read-only COM preflight design execution_mode must be offline",
                        step_id,
                    )
                )
            for flag in (
                "--formal-initialization-controlled-executor-design-json",
                "--output-dir",
            ):
                _require_flag(
                    command,
                    flag,
                    step_id=step_id,
                    issues=issues,
                    code="formal_initialization_readonly_com_preflight_design_missing_required_flag",
                    message=f"Formal initialization read-only COM preflight design command must include {flag}",
                )

        if step_id == "formal_initialization_readonly_com_preflight_blocked_executor_snapshot":
            if (
                module
                != "gas_calibrator.tools.run_v1_5_formal_initialization_readonly_com_preflight_blocked_executor"
            ):
                issues.append(
                    _issue(
                        "error",
                        "formal_initialization_readonly_com_preflight_blocked_executor_wrong_tool",
                        "Formal initialization read-only COM preflight blocked executor must use the no-COM stub",
                        step_id,
                    )
                )
            if bool(step.get("opens_com_ports")) or bool(step.get("controls_pressure")) or controls_route or writes:
                issues.append(
                    _issue(
                        "error",
                        "formal_initialization_readonly_com_preflight_blocked_executor_must_be_offline_no_write",
                        "Formal initialization read-only COM preflight blocked executor must not open COM, connect PostgreSQL, control routes/pressure, or write coefficients",
                        step_id,
                    )
                )
            if bool(step.get("writes_device_id")):
                issues.append(
                    _issue(
                        "error",
                        "formal_initialization_readonly_com_preflight_blocked_executor_must_not_write_device_id",
                        "Formal initialization read-only COM preflight blocked executor must not write SN/device_code or protocol device ID",
                        step_id,
                    )
                )
            if not str(step.get("execution_mode") or "").startswith("offline"):
                issues.append(
                    _issue(
                        "error",
                        "formal_initialization_readonly_com_preflight_blocked_executor_must_be_offline",
                        "Formal initialization read-only COM preflight blocked executor execution_mode must be offline",
                        step_id,
                    )
                )
            for flag in (
                "--formal-initialization-readonly-com-preflight-design-json",
                "--output-dir",
                "--fail-on-blocked",
            ):
                _require_flag(
                    command,
                    flag,
                    step_id=step_id,
                    issues=issues,
                    code="formal_initialization_readonly_com_preflight_blocked_executor_missing_required_flag",
                    message=f"Formal initialization read-only COM preflight blocked executor command must include {flag}",
                )

        if step_id == "formal_initialization_readonly_com_preflight_controlled_executor_design_snapshot":
            if (
                module
                != "gas_calibrator.tools.export_v1_5_formal_initialization_readonly_com_preflight_controlled_executor_design"
            ):
                issues.append(
                    _issue(
                        "error",
                        "formal_initialization_readonly_com_preflight_controlled_executor_design_wrong_tool",
                        "Formal initialization read-only COM preflight controlled executor design must use the offline design exporter",
                        step_id,
                    )
                )
            if bool(step.get("opens_com_ports")) or bool(step.get("controls_pressure")) or controls_route or writes:
                issues.append(
                    _issue(
                        "error",
                        "formal_initialization_readonly_com_preflight_controlled_executor_design_must_be_offline_no_write",
                        "Formal initialization read-only COM preflight controlled executor design must not open COM, connect PostgreSQL, control routes/pressure, or write coefficients",
                        step_id,
                    )
                )
            if bool(step.get("writes_device_id")):
                issues.append(
                    _issue(
                        "error",
                        "formal_initialization_readonly_com_preflight_controlled_executor_design_must_not_write_device_id",
                        "Formal initialization read-only COM preflight controlled executor design must not write SN/device_code or protocol device ID",
                        step_id,
                    )
                )
            if not str(step.get("execution_mode") or "").startswith("offline"):
                issues.append(
                    _issue(
                        "error",
                        "formal_initialization_readonly_com_preflight_controlled_executor_design_must_be_offline",
                        "Formal initialization read-only COM preflight controlled executor design execution_mode must be offline",
                        step_id,
                    )
                )
            for flag in (
                "--formal-initialization-readonly-com-preflight-blocked-executor-json",
                "--output-dir",
            ):
                _require_flag(
                    command,
                    flag,
                    step_id=step_id,
                    issues=issues,
                    code="formal_initialization_readonly_com_preflight_controlled_executor_design_missing_required_flag",
                    message=f"Formal initialization read-only COM preflight controlled executor design command must include {flag}",
                )

        if step_id == "formal_initialization_readonly_com_preflight_controlled_blocked_executor_snapshot":
            if (
                module
                != "gas_calibrator.tools.run_v1_5_formal_initialization_readonly_com_preflight_controlled_blocked_executor"
            ):
                issues.append(
                    _issue(
                        "error",
                        "formal_initialization_readonly_com_preflight_controlled_blocked_executor_wrong_tool",
                        "Formal initialization read-only COM preflight controlled blocked executor must use the blocked stub",
                        step_id,
                    )
                )
            if bool(step.get("opens_com_ports")) or bool(step.get("controls_pressure")) or controls_route or writes:
                issues.append(
                    _issue(
                        "error",
                        "formal_initialization_readonly_com_preflight_controlled_blocked_executor_must_be_offline_no_write",
                        "Formal initialization read-only COM preflight controlled blocked executor must not open COM, connect PostgreSQL, control routes/pressure, or write coefficients",
                        step_id,
                    )
                )
            if bool(step.get("writes_device_id")):
                issues.append(
                    _issue(
                        "error",
                        "formal_initialization_readonly_com_preflight_controlled_blocked_executor_must_not_write_device_id",
                        "Formal initialization read-only COM preflight controlled blocked executor must not write SN/device_code or protocol device ID",
                        step_id,
                    )
                )
            if not str(step.get("execution_mode") or "").startswith("offline"):
                issues.append(
                    _issue(
                        "error",
                        "formal_initialization_readonly_com_preflight_controlled_blocked_executor_must_be_offline",
                        "Formal initialization read-only COM preflight controlled blocked executor execution_mode must be offline",
                        step_id,
                    )
                )
            for flag in (
                "--formal-initialization-readonly-com-preflight-controlled-executor-design-json",
                "--output-dir",
                "--fail-on-blocked",
            ):
                _require_flag(
                    command,
                    flag,
                    step_id=step_id,
                    issues=issues,
                    code="formal_initialization_readonly_com_preflight_controlled_blocked_executor_missing_required_flag",
                    message=(
                        "Formal initialization read-only COM preflight controlled blocked executor "
                        f"command must include {flag}"
                    ),
                )
            for forbidden in (
                "--execute",
                "--execute-read-only-real-com",
                "--execute-controlled-writes",
                "--allow-real-com",
            ):
                if forbidden in command:
                    issues.append(
                        _issue(
                            "error",
                            "formal_initialization_readonly_com_preflight_controlled_blocked_executor_forbidden_unlock",
                            f"Formal initialization read-only COM preflight controlled blocked executor must not include {forbidden}",
                            step_id,
                        )
                    )

        if step_id == "identity_getco_readiness_snapshot":
            if module != "gas_calibrator.tools.export_v1_5_getco_identity_readiness":
                issues.append(
                    _issue(
                        "error",
                        "identity_getco_readiness_wrong_tool",
                        "Identity/GETCO readiness must use the offline export_v1_5_getco_identity_readiness sidecar",
                        step_id,
                    )
                )
            if bool(step.get("opens_com_ports")) or bool(step.get("controls_pressure")) or controls_route or writes:
                issues.append(
                    _issue(
                        "error",
                        "identity_getco_readiness_must_be_offline_no_write",
                        "Identity/GETCO readiness must only consume GETCO artifacts and must not open COM, control routes, or write coefficients",
                        step_id,
                    )
                )
            if not str(step.get("execution_mode") or "").startswith("offline"):
                issues.append(
                    _issue(
                        "error",
                        "identity_getco_readiness_must_be_offline",
                        "Identity/GETCO readiness execution_mode must be offline",
                        step_id,
                    )
                )
            getco_dir = _command_value_after(command, "--getco-dir").replace("\\", "/")
            if not getco_dir.endswith("coefficient_epoch_0_getco_snapshot"):
                issues.append(
                    _issue(
                        "error",
                        "identity_getco_readiness_getco_dir_violation",
                        "Identity/GETCO readiness must read the coefficient_epoch_0_getco_snapshot artifacts",
                        step_id,
                    )
                )
            if not _command_has_flag(command, "--fail-on-not-ready"):
                issues.append(
                    _issue(
                        "error",
                        "identity_getco_readiness_must_fail_when_not_ready",
                        "Identity/GETCO readiness command must fail on incomplete evidence",
                        step_id,
                    )
                )

        if step_id == "formal_database_dry_run_snapshot":
            if module != "gas_calibrator.tools.export_v1_5_formal_database_dry_run":
                issues.append(
                    _issue(
                        "error",
                        "formal_database_dry_run_wrong_tool",
                        "Formal database dry-run must use the offline export_v1_5_formal_database_dry_run contract",
                        step_id,
                    )
                )
            if bool(step.get("opens_com_ports")) or bool(step.get("controls_pressure")) or controls_route or writes:
                issues.append(
                    _issue(
                        "error",
                        "formal_database_dry_run_must_be_offline_no_write",
                        "Formal database dry-run must not open COM, connect PostgreSQL, control routes/pressure, or write coefficients",
                        step_id,
                    )
                )
            if not str(step.get("execution_mode") or "").startswith("offline"):
                issues.append(
                    _issue(
                        "error",
                        "formal_database_dry_run_must_be_offline",
                        "Formal database dry-run execution_mode must be offline",
                        step_id,
                    )
                )
            if not _command_has_flag(command, "--fail-on-blocker"):
                issues.append(
                    _issue(
                        "error",
                        "formal_database_dry_run_must_fail_on_blocker",
                        "Formal database dry-run command must fail on schema/identity blockers",
                        step_id,
                    )
                )

        if step_id == "formal_database_import_preflight_snapshot":
            if module != "gas_calibrator.tools.export_v1_5_formal_database_import_preflight":
                issues.append(
                    _issue(
                        "error",
                        "formal_database_import_preflight_wrong_tool",
                        "Formal database import preflight must use the offline export_v1_5_formal_database_import_preflight contract",
                        step_id,
                    )
                )
            if bool(step.get("opens_com_ports")) or bool(step.get("controls_pressure")) or controls_route or writes:
                issues.append(
                    _issue(
                        "error",
                        "formal_database_import_preflight_must_be_offline_no_write",
                        "Formal database import preflight must not open COM, connect PostgreSQL, control routes/pressure, or write coefficients",
                        step_id,
                    )
                )
            if not str(step.get("execution_mode") or "").startswith("offline"):
                issues.append(
                    _issue(
                        "error",
                        "formal_database_import_preflight_must_be_offline",
                        "Formal database import preflight execution_mode must be offline",
                        step_id,
                    )
                )
            for flag in (
                "--formal-database-dry-run-json",
                "--dsn-env",
                "--fail-on-blocker",
            ):
                _require_flag(
                    command,
                    flag,
                    step_id=step_id,
                    issues=issues,
                    code="formal_database_import_preflight_missing_required_flag",
                    message=f"Formal database import preflight command must include {flag}",
                )

        if step_id == "formal_database_import_authorization_snapshot":
            if module != "gas_calibrator.tools.export_v1_5_formal_database_import_authorization":
                issues.append(
                    _issue(
                        "error",
                        "formal_database_import_authorization_wrong_tool",
                        "Formal database import authorization must use the offline export_v1_5_formal_database_import_authorization guard",
                        step_id,
                    )
                )
            if bool(step.get("opens_com_ports")) or bool(step.get("controls_pressure")) or controls_route or writes:
                issues.append(
                    _issue(
                        "error",
                        "formal_database_import_authorization_must_be_offline_no_write",
                        "Formal database import authorization must not open COM, connect PostgreSQL, control routes/pressure, or write coefficients",
                        step_id,
                    )
                )
            if not str(step.get("execution_mode") or "").startswith("offline"):
                issues.append(
                    _issue(
                        "error",
                        "formal_database_import_authorization_must_be_offline",
                        "Formal database import authorization execution_mode must be offline",
                        step_id,
                    )
                )
            for flag in (
                "--formal-database-import-preflight-json",
                "--archive-closure-json",
                "--authorization-id",
                "--fail-on-blocker",
            ):
                _require_flag(
                    command,
                    flag,
                    step_id=step_id,
                    issues=issues,
                    code="formal_database_import_authorization_missing_required_flag",
                    message=f"Formal database import authorization command must include {flag}",
                )

        if step_id == "formal_database_import_command_contract_snapshot":
            if module != "gas_calibrator.tools.export_v1_5_formal_database_import_command_contract":
                issues.append(
                    _issue(
                        "error",
                        "formal_database_import_command_contract_wrong_tool",
                        "Formal database import command contract must use the offline export_v1_5_formal_database_import_command_contract guard",
                        step_id,
                    )
                )
            if bool(step.get("opens_com_ports")) or bool(step.get("controls_pressure")) or controls_route or writes:
                issues.append(
                    _issue(
                        "error",
                        "formal_database_import_command_contract_must_be_offline_no_write",
                        "Formal database import command contract must not open COM, connect PostgreSQL, control routes/pressure, or write coefficients",
                        step_id,
                    )
                )
            if not str(step.get("execution_mode") or "").startswith("offline"):
                issues.append(
                    _issue(
                        "error",
                        "formal_database_import_command_contract_must_be_offline",
                        "Formal database import command contract execution_mode must be offline",
                        step_id,
                    )
                )
            for flag in (
                "--formal-database-import-authorization-json",
                "--formal-database-import-preflight-json",
                "--archive-closure-json",
                "--evidence-bundle-json",
                "--dsn-env",
                "--requested-command-module",
                "--fail-on-blocker",
            ):
                _require_flag(
                    command,
                    flag,
                    step_id=step_id,
                    issues=issues,
                    code="formal_database_import_command_contract_missing_required_flag",
                    message=f"Formal database import command contract command must include {flag}",
                )

        if step_id == "formal_database_import_blocked_executor_snapshot":
            if module != "gas_calibrator.tools.import_v1_5_evidence_package":
                issues.append(
                    _issue(
                        "error",
                        "formal_database_import_blocked_executor_wrong_tool",
                        "Formal database import blocked executor must use import_v1_5_evidence_package in no-connect stub mode",
                        step_id,
                    )
                )
            if bool(step.get("opens_com_ports")) or bool(step.get("controls_pressure")) or controls_route or writes:
                issues.append(
                    _issue(
                        "error",
                        "formal_database_import_blocked_executor_must_be_offline_no_write",
                        "Formal database import blocked executor must not open COM, connect PostgreSQL, control routes/pressure, or write coefficients",
                        step_id,
                    )
                )
            if not str(step.get("execution_mode") or "").startswith("offline"):
                issues.append(
                    _issue(
                        "error",
                        "formal_database_import_blocked_executor_must_be_offline",
                        "Formal database import blocked executor execution_mode must be offline",
                        step_id,
                    )
                )
            for flag in (
                "--formal-database-import-command-contract-json",
                "--formal-database-import-authorization-json",
                "--formal-database-import-preflight-json",
                "--archive-closure-json",
                "--evidence-bundle-json",
                "--dsn-env",
                "--output-dir",
                "--fail-on-blocked",
            ):
                _require_flag(
                    command,
                    flag,
                    step_id=step_id,
                    issues=issues,
                    code="formal_database_import_blocked_executor_missing_required_flag",
                    message=f"Formal database import blocked executor command must include {flag}",
                )

        if step_id == "formal_database_import_controlled_executor_design_snapshot":
            if module != "gas_calibrator.tools.export_v1_5_formal_database_import_controlled_executor_design":
                issues.append(
                    _issue(
                        "error",
                        "formal_database_import_controlled_executor_design_wrong_tool",
                        "Formal database import controlled executor design must use the offline design exporter",
                        step_id,
                    )
                )
            if bool(step.get("opens_com_ports")) or bool(step.get("controls_pressure")) or controls_route or writes:
                issues.append(
                    _issue(
                        "error",
                        "formal_database_import_controlled_executor_design_must_be_offline_no_write",
                        "Formal database import controlled executor design must not open COM, connect PostgreSQL, control routes/pressure, or write coefficients",
                        step_id,
                    )
                )
            if not str(step.get("execution_mode") or "").startswith("offline"):
                issues.append(
                    _issue(
                        "error",
                        "formal_database_import_controlled_executor_design_must_be_offline",
                        "Formal database import controlled executor design execution_mode must be offline",
                        step_id,
                    )
                )
            for flag in (
                "--formal-database-import-blocked-executor-json",
                "--dsn-env",
                "--output-dir",
            ):
                _require_flag(
                    command,
                    flag,
                    step_id=step_id,
                    issues=issues,
                    code="formal_database_import_controlled_executor_design_missing_required_flag",
                    message=f"Formal database import controlled executor design command must include {flag}",
                )

        if step_id == "formal_run_status_snapshot":
            if module != "gas_calibrator.tools.export_v1_5_formal_run_status":
                issues.append(
                    _issue(
                        "error",
                        "formal_run_status_wrong_tool",
                        "Formal run status must use the offline export_v1_5_formal_run_status rollup",
                        step_id,
                    )
                )
            if bool(step.get("opens_com_ports")) or bool(step.get("controls_pressure")) or controls_route or writes:
                issues.append(
                    _issue(
                        "error",
                        "formal_run_status_must_be_offline_no_write",
                        "Formal run status must only consume sidecars and must not open COM, control routes/pressure, or write coefficients",
                        step_id,
                    )
                )
            if not str(step.get("execution_mode") or "").startswith("offline"):
                issues.append(
                    _issue(
                        "error",
                        "formal_run_status_must_be_offline",
                        "Formal run status execution_mode must be offline",
                        step_id,
                    )
                )
            for flag in (
                "--initialization-readiness-json",
                "--formal-initialization-controlled-executor-design-json",
                "--pre-gas-readiness-json",
                "--getco-readiness-json",
                "--run-evidence-status-json",
                "--formal-database-dry-run-json",
                "--formal-database-import-preflight-json",
                "--formal-database-import-authorization-json",
                "--formal-database-import-command-contract-json",
                "--formal-database-import-blocked-executor-json",
                "--formal-database-import-controlled-executor-design-json",
            ):
                _require_flag(
                    command,
                    flag,
                    step_id=step_id,
                    issues=issues,
                    code="formal_run_status_missing_sidecar_input",
                    message=f"Formal run status command must include {flag}",
                )

        if step_id == "pressure_senco9_no_write_acquisition":
            if module != "gas_calibrator.tools.validate_pressure_only":
                issues.append(
                    _issue(
                        "error",
                        "pressure_senco9_acquisition_wrong_tool",
                        "SENCO9 pressure acquisition must use validate_pressure_only with controlled pressure enabled",
                        step_id,
                    )
                )
            if not bool(step.get("controls_pressure")):
                issues.append(
                    _issue(
                        "error",
                        "pressure_senco9_acquisition_not_pressure_control",
                        "SENCO9 acquisition must be marked controls_pressure=true",
                        step_id,
                    )
                )
            for flag in (
                "--control-pressure-points",
                "--continuous-atmosphere-hold",
                "--require-continuous-atmosphere-hold",
                "--no-prompt",
            ):
                _require_flag(
                    command,
                    flag,
                    step_id=step_id,
                    issues=issues,
                    code="pressure_senco9_acquisition_contract_violation",
                    message=f"SENCO9 acquisition command must include {flag}",
                )
            pressure_tokens = _normalized_pressure_tokens(_command_value_after(command, "--pressure-points"))
            required_pressure_tokens = {"1100", "1000", "900", "800", "700", "600", "500"}
            if not required_pressure_tokens.issubset(pressure_tokens):
                issues.append(
                    _issue(
                        "error",
                        "pressure_senco9_point_matrix_incomplete",
                        (
                            "SENCO9 no-write acquisition must include "
                            "1100,1000,900,800,700,600,500 hPa sealed control points; "
                            "ambient belongs to the preceding open-atmosphere quick check"
                        ),
                        step_id,
                    )
                )
            if "ambient" in pressure_tokens:
                issues.append(
                    _issue(
                        "error",
                        "pressure_senco9_sealed_matrix_must_not_include_ambient",
                        "SENCO9 sealed no-write acquisition must not include ambient as a controlled pressure point",
                        step_id,
                    )
                )
            if _command_value_after(command, "--pressure-control-slew-mode") != "max":
                issues.append(
                    _issue(
                        "error",
                        "pressure_senco9_slew_contract_violation",
                        "SENCO9 no-write acquisition must use the verified K0472/PACE legacy MAX slew pressure-control contract",
                        step_id,
                    )
                )
            _require_float_value(
                command,
                "--pressure-control-atmosphere-release-wait-s",
                expected=1.5,
                step_id=step_id,
                issues=issues,
                code="pressure_senco9_atmosphere_release_contract_violation",
                message="SENCO9 acquisition must wait 1.5 s after stopping atmosphere hold before control",
            )
            _require_float_value(
                command,
                "--pressure-control-post-stable-wait-s",
                expected=8.0,
                step_id=step_id,
                issues=issues,
                code="pressure_senco9_analyzer_cache_contract_violation",
                message="SENCO9 acquisition must wait after pressure stability before analyzer pressure sampling",
            )
            expected_outputs = " ".join(str(item) for item in (step.get("expected_outputs") or ()))
            if "pressure_transition_trace.csv" not in expected_outputs:
                issues.append(
                    _issue(
                        "error",
                        "pressure_senco9_trace_missing",
                        "SENCO9 no-write acquisition must require pressure_transition_trace.csv evidence",
                        step_id,
                    )
                )

        if step_id == "pressure_senco9_no_write_review" and module != PRESSURE_SENCO9_EVALUATION_MODULE:
            issues.append(
                _issue(
                    "error",
                    "pressure_senco9_review_wrong_tool",
                    "SENCO9 no-write review must evaluate acquired multi-pressure artifacts, not only export a preflight runbook",
                    step_id,
                )
            )

        if step_id == "pressure_channel_completion_audit":
            if module != PRESSURE_CHANNEL_COMPLETION_MODULE:
                issues.append(
                    _issue(
                        "error",
                        "pressure_channel_completion_wrong_tool",
                        "Pressure-channel completion must be exported from SENCO9 write and post-write verification evidence",
                        step_id,
                    )
                )
            expected_outputs = " ".join(str(item) for item in (step.get("expected_outputs") or ()))
            if "pressure_channel_completion_summary.csv" not in expected_outputs:
                issues.append(
                    _issue(
                        "error",
                        "pressure_channel_completion_summary_missing",
                        "Pressure-channel completion audit must require pressure_channel_completion_summary.csv",
                        step_id,
                    )
                )
            if bool(step.get("opens_com_ports")) or bool(step.get("writes_coefficients")):
                issues.append(
                    _issue(
                        "error",
                        "pressure_channel_completion_must_be_offline",
                        "Pressure-channel completion audit must be offline and cannot open COM or write SENCO",
                        step_id,
                    )
                )

        if step_id == "co2_open_flow_sampling":
            _require_temperature_order(
                command,
                expected=FORMAL_CO2_TEMPERATURE_ORDER,
                step_id=step_id,
                issues=issues,
            )
            _block_forbidden_flags(
                command,
                forbidden_flags=FORMAL_CO2_FORBIDDEN_FLAGS,
                step_id=step_id,
                issues=issues,
            )
            co2_ratio_policy = _command_value_after(command, "--co2-ratio-f-preseal-policy")
            if co2_ratio_policy and co2_ratio_policy != "reject":
                issues.append(
                    _issue(
                        "error",
                        "formal_co2_ratio_gate_policy_violation",
                        "CO2 formal open-flow sampling must not downgrade the ratio gate to warn/pass",
                        step_id,
                    )
                )

        if step_id == "h2o_open_flow_sampling":
            _require_temperature_order(
                command,
                expected=FORMAL_H2O_TEMPERATURE_ORDER,
                step_id=step_id,
                issues=issues,
            )
            _block_forbidden_flags(
                command,
                forbidden_flags=FORMAL_H2O_FORBIDDEN_FLAGS,
                step_id=step_id,
                issues=issues,
            )

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
        physical_boundaries={
            "offline_contract_audit": True,
            "opens_com_ports": False,
            "controls_water_or_gas_routes": False,
            "controls_valves_or_pace": False,
            "writes_coefficients": False,
            "not_real_acceptance_evidence": True,
        },
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
        "## Physical Boundaries",
        "",
    ]
    for key, value in report.physical_boundaries.items():
        lines.append(f"- `{key}`: `{value}`")
    lines.extend(["", "## Physical Flow", ""])
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
