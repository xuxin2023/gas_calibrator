"""Offline review gate for a future minimal V1.5 read-only COM executor.

This module consumes the read-only COM plan preview and turns it into an
implementation-review contract for the first real executor. It deliberately
does not implement serial I/O. The output defines expected evidence and hold
rules while keeping the real executor blocked by default.
"""

from __future__ import annotations

import csv
import json
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping, Sequence


SCHEMA = "v1_5_formal_readonly_com_minimal_executor_review_v1"
PLAN_PREVIEW_SCHEMA = "v1_5_formal_readonly_com_execution_plan_preview_v1"
PLAN_PREVIEW_READY_STATUSES = {
    "blocked_pending_validated_readonly_com_execution_packet",
    "ready_for_readonly_com_execution_plan_preview_review",
}
READY_STATUS = "blocked_pending_minimal_readonly_com_executor_implementation"
REVIEW_STATUS = "review_required"
MIN_SERIAL_COMMAND_GAP_S = 1.0


@dataclass(frozen=True)
class ReadonlyComMinimalExecutorReviewCheck:
    check: str
    status: str
    evidence_role: str
    reasons: tuple[str, ...]
    physical_meaning: str
    next_action: str
    details: Mapping[str, Any]

    def to_json(self) -> dict[str, Any]:
        return asdict(self)


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _load_json(path: str | Path | None) -> dict[str, Any]:
    if not path:
        return {}
    source = Path(path)
    if not source.exists() or not source.is_file():
        return {}
    payload = json.loads(source.read_text(encoding="utf-8-sig"))
    return payload if isinstance(payload, dict) else {}


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8-sig")


def _write_csv(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    fields: list[str] = []
    for row in rows:
        for key in row:
            if key not in fields:
                fields.append(str(key))
    if not fields:
        fields = ["message"]
        rows = [{"message": "no_rows"}]
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key, "") for key in fields})


def _check(
    *,
    check: str,
    status: str,
    evidence_role: str,
    reasons: Sequence[str] = (),
    physical_meaning: str,
    next_action: str,
    details: Mapping[str, Any],
) -> ReadonlyComMinimalExecutorReviewCheck:
    return ReadonlyComMinimalExecutorReviewCheck(
        check=check,
        status=status,
        evidence_role=evidence_role,
        reasons=tuple(reasons),
        physical_meaning=physical_meaning,
        next_action=next_action,
        details=details,
    )


def _plan_preview_reasons(payload: Mapping[str, Any]) -> list[str]:
    reasons: list[str] = []
    if not payload:
        return ["plan_preview_missing"]
    if payload.get("schema") != PLAN_PREVIEW_SCHEMA:
        reasons.append(f"schema={payload.get('schema') or 'missing'}")
    if payload.get("overall_status") not in PLAN_PREVIEW_READY_STATUSES:
        reasons.append(f"overall_status={payload.get('overall_status') or 'missing'}")
    if int(payload.get("review_required_count") or 0):
        reasons.append(f"review_required_count={payload.get('review_required_count')}")
    if float(payload.get("minimum_serial_command_gap_s") or 0.0) < MIN_SERIAL_COMMAND_GAP_S:
        reasons.append(f"minimum_serial_command_gap_s={payload.get('minimum_serial_command_gap_s')!r}")
    for field in (
        "execution_supported",
        "live_execution_allowed",
        "read_only_real_com_execution_allowed",
        "controlled_write_execution_allowed",
        "real_com_execution_allowed",
        "execute_flag_allowed",
        "opens_com_ports",
        "connects_postgresql",
        "controls_pressure",
        "controls_water_or_gas_routes",
        "writes_sn",
        "writes_device_id",
        "writes_coefficients",
        "database_written",
        "formal_release_allowed",
        "database_import_allowed",
    ):
        if payload.get(field) is not False:
            reasons.append(f"plan_preview_boundary_{field}={payload.get(field)!r}")
    if payload.get("does_not_execute_commands") is not True:
        reasons.append(f"does_not_execute_commands={payload.get('does_not_execute_commands')!r}")
    if payload.get("not_real_acceptance_evidence") is not True:
        reasons.append(f"not_real_acceptance_evidence={payload.get('not_real_acceptance_evidence')!r}")
    return reasons


def _output_evidence_contract() -> list[dict[str, Any]]:
    return [
        {
            "artifact": "readonly_com_executor_invocation.json",
            "required": True,
            "physical_meaning": "Records run id, operator authorization id, reviewed plan hash, active analyzer count, and no-write scope before any COM can open.",
            "failure_policy": "hold_before_opening_any_com_port",
        },
        {
            "artifact": "readonly_com_command_attempts.csv",
            "required": True,
            "physical_meaning": "One row per future read attempt with GA label, COM port, protocol ID, SN, command/source, start/end timestamps, and result status.",
            "failure_policy": "hold_if_command_attempt_trace_missing",
        },
        {
            "artifact": "readonly_com_raw_responses.csv",
            "required": True,
            "physical_meaning": "Preserves raw SN/GETCO/CHECK responses and passive MODE2 evidence without rewriting analyzer state.",
            "failure_policy": "hold_if_raw_response_or_passive_frame_evidence_missing",
        },
        {
            "artifact": "readonly_com_hold_events.csv",
            "required": True,
            "physical_meaning": "Records every timeout, parse failure, identity mismatch, pacing violation, CHECK review, or old-algorithm CHECK-skip decision.",
            "failure_policy": "hold_if_failure_or_skip_is_not_traceable",
        },
        {
            "artifact": "readonly_com_identity_getco_snapshot.json",
            "required": True,
            "physical_meaning": "Binds COM/GA transport to protocol ID, 8-digit SN/device_code, GETCO1-9 epoch-0, runtime state, and optional CHECK monitor evidence.",
            "failure_policy": "not_release_or_database_import_evidence_by_itself",
        },
    ]


def _hold_matrix() -> list[dict[str, Any]]:
    return [
        {
            "stage": "pre_open_authorization",
            "hold_condition": "authorization_missing_or_not_bound_to_reviewed_ports_and_active_analyzers",
            "executor_action": "abort_before_opening_any_com_port",
            "required_evidence": "readonly_com_executor_invocation.json",
        },
        {
            "stage": "port_open",
            "hold_condition": "unreviewed_port_duplicate_port_or_ga_label_mismatch",
            "executor_action": "abort_before_opening_any_com_port",
            "required_evidence": "readonly_com_hold_events.csv",
        },
        {
            "stage": "serial_pacing",
            "hold_condition": "any_serial_command_or_retry_gap_below_1s",
            "executor_action": "hold_current_device_and_stop_batch",
            "required_evidence": "readonly_com_command_attempts.csv",
        },
        {
            "stage": "protocol_device_id",
            "hold_condition": "missing_or_unparseable_mode2_protocol_device_id",
            "executor_action": "hold_current_device_without_repair_write",
            "required_evidence": "readonly_com_raw_responses.csv",
        },
        {
            "stage": "sn_device_code",
            "hold_condition": "sn_missing_non_numeric_not_8_digits_duplicate_or_mismatched",
            "executor_action": "hold_current_device_without_sn_write",
            "required_evidence": "readonly_com_hold_events.csv",
        },
        {
            "stage": "getco_epoch0",
            "hold_condition": "any_getco1_to_getco9_timeout_or_parse_failure",
            "executor_action": "hold_current_device_without_senco_write",
            "required_evidence": "readonly_com_raw_responses.csv",
        },
        {
            "stage": "runtime_state",
            "hold_condition": "mode2_1hz_average_runtime_mismatch",
            "executor_action": "hold_without_runtime_repair_write",
            "required_evidence": "readonly_com_hold_events.csv",
        },
        {
            "stage": "check_monitor",
            "hold_condition": "legacy_ratio_device_has_check_command_planned_or_required",
            "executor_action": "hold_plan_as_invalid_before_sending_check",
            "required_evidence": "readonly_com_hold_events.csv",
        },
        {
            "stage": "check_monitor",
            "hold_condition": "new_algorithm_check_timeout_parse_error_or_voltage_state_review",
            "executor_action": "hold_current_device_after_read_only_raw_response_record",
            "required_evidence": "readonly_com_raw_responses.csv",
        },
        {
            "stage": "post_read_release_boundary",
            "hold_condition": "attempt_to_write_sn_senco_connect_database_or_control_route",
            "executor_action": "abort_and_mark_not_real_acceptance_evidence",
            "required_evidence": "readonly_com_hold_events.csv",
        },
    ]


def build_v1_5_formal_readonly_com_minimal_executor_review(
    *,
    formal_readonly_com_execution_plan_preview_json: str | Path | None,
) -> dict[str, Any]:
    plan_path = (
        Path(formal_readonly_com_execution_plan_preview_json).resolve()
        if formal_readonly_com_execution_plan_preview_json
        else None
    )
    plan_payload = _load_json(plan_path)
    plan_reasons = _plan_preview_reasons(plan_payload)
    command_plan = plan_payload.get("command_plan") if isinstance(plan_payload.get("command_plan"), list) else []
    future_command_count = int(plan_payload.get("future_command_count") or 0)
    future_check_command_count = int(plan_payload.get("future_check_command_count") or 0)
    old_algorithm_check_skip_count = int(plan_payload.get("old_algorithm_check_skip_count") or 0)

    checks = [
        _check(
            check="plan_preview_consumed",
            status="ready" if not plan_reasons else "review_required",
            evidence_role="required_plan_preview_evidence",
            reasons=plan_reasons,
            physical_meaning=(
                "The minimal real executor review must start from the reviewed plan preview, so it "
                "inherits SN/device_code, GETCO, runtime, CHECK, old-algorithm skip, and >=1s pacing rules."
            ),
            next_action="Regenerate the plan preview until its no-COM/no-write boundaries are clean.",
            details={"source_path": str(plan_path) if plan_path else ""},
        ),
        _check(
            check="future_output_evidence_contract_defined",
            status="ready",
            evidence_role="future_executor_output_contract",
            physical_meaning=(
                "The first real executor must emit invocation, command-attempt, raw-response, hold-event, "
                "and identity/GETCO snapshot artifacts before any result can feed later readiness gates."
            ),
            next_action="Keep these artifacts mandatory in the later real executor implementation.",
            details={"required_output_count": len(_output_evidence_contract())},
        ),
        _check(
            check="future_failure_hold_matrix_defined",
            status="ready",
            evidence_role="future_executor_hold_contract",
            physical_meaning=(
                "Timeouts, parse failures, identity mismatches, SN problems, GETCO errors, runtime mismatch, "
                "legacy CHECK mistakes, CHECK voltage review, pacing violations, and side-effect attempts all hold."
            ),
            next_action="Use the hold matrix as the minimum implementation checklist for the future real executor.",
            details={"hold_rule_count": len(_hold_matrix())},
        ),
        _check(
            check="blocked_by_default_no_com_boundary",
            status="ready",
            evidence_role="current_package_safety_lock",
            physical_meaning=(
                "This package remains an implementation-review sidecar; it does not support --execute-read-only-real-com "
                "and cannot open COM, write analyzer state, connect PostgreSQL, or control routes."
            ),
            next_action="Implement the real executor in a separate PR only after this review artifact is accepted.",
            details={
                "execution_supported": False,
                "opens_com_ports": False,
                "writes_coefficients": False,
                "connects_postgresql": False,
            },
        ),
    ]

    review_required_count = sum(1 for row in checks if row.status == "review_required")
    review_ready = review_required_count == 0
    return {
        "schema": SCHEMA,
        "generated_at": _now(),
        "overall_status": READY_STATUS if review_ready else REVIEW_STATUS,
        "blocker_count": 0,
        "review_required_count": review_required_count,
        "minimal_executor_review_ready": review_ready,
        "production_state": "implementation_review_only_blocked_by_default",
        "formal_readonly_com_execution_plan_preview_json": str(plan_path) if plan_path else "",
        "plan_preview_status": str(plan_payload.get("overall_status") or ""),
        "plan_preview_ready": bool(plan_payload.get("plan_preview_ready")),
        "plan_preview_command_plan_present": bool(command_plan),
        "future_command_count": future_command_count,
        "future_check_command_count": future_check_command_count,
        "old_algorithm_check_skip_count": old_algorithm_check_skip_count,
        "execution_supported": False,
        "execution_requested": False,
        "live_execution_allowed": False,
        "read_only_real_com_execution_allowed": False,
        "controlled_write_execution_allowed": False,
        "real_com_execution_allowed": False,
        "execute_flag_allowed": False,
        "opens_com_ports": False,
        "connects_postgresql": False,
        "controls_pressure": False,
        "controls_water_or_gas_routes": False,
        "writes_sn": False,
        "writes_device_id": False,
        "writes_coefficients": False,
        "database_written": False,
        "formal_release_allowed": False,
        "database_import_allowed": False,
        "not_real_acceptance_evidence": True,
        "required_future_read_only_real_com_flag": "--execute-read-only-real-com",
        "minimum_serial_command_gap_s": MIN_SERIAL_COMMAND_GAP_S,
        "supports_1_to_6_active_analyzers": True,
        "supports_old_algorithm_check_skip": True,
        "future_output_evidence_contract": _output_evidence_contract(),
        "future_failure_hold_matrix": _hold_matrix(),
        "checks": [row.to_json() for row in checks],
        "next_action": (
            "Keep COM locked. Review the minimal executor output and hold contracts before a later PR "
            "implements the first real read-only COM executor."
        ),
    }


def write_v1_5_formal_readonly_com_minimal_executor_review_outputs(
    model: Mapping[str, Any],
    output_dir: str | Path,
) -> dict[str, Path]:
    out = Path(output_dir)
    paths = {
        "json": out / "v1_5_formal_readonly_com_minimal_executor_review.json",
        "checks_csv": out / "v1_5_formal_readonly_com_minimal_executor_review_checks.csv",
        "outputs_csv": out / "v1_5_formal_readonly_com_minimal_executor_output_contract.csv",
        "holds_csv": out / "v1_5_formal_readonly_com_minimal_executor_hold_matrix.csv",
        "summary_csv": out / "v1_5_formal_readonly_com_minimal_executor_review_summary.csv",
        "markdown": out / "V1_5_FORMAL_READONLY_COM_MINIMAL_EXECUTOR_REVIEW.md",
    }
    _write_json(paths["json"], model)
    _write_csv(paths["checks_csv"], model.get("checks", []))
    _write_csv(paths["outputs_csv"], model.get("future_output_evidence_contract", []))
    _write_csv(paths["holds_csv"], model.get("future_failure_hold_matrix", []))
    _write_csv(
        paths["summary_csv"],
        [
            {
                "overall_status": model.get("overall_status"),
                "minimal_executor_review_ready": model.get("minimal_executor_review_ready"),
                "plan_preview_status": model.get("plan_preview_status"),
                "plan_preview_command_plan_present": model.get("plan_preview_command_plan_present"),
                "future_command_count": model.get("future_command_count"),
                "future_check_command_count": model.get("future_check_command_count"),
                "old_algorithm_check_skip_count": model.get("old_algorithm_check_skip_count"),
                "execution_supported": model.get("execution_supported"),
                "read_only_real_com_execution_allowed": model.get("read_only_real_com_execution_allowed"),
                "opens_com_ports": model.get("opens_com_ports"),
                "writes_sn": model.get("writes_sn"),
                "writes_coefficients": model.get("writes_coefficients"),
                "connects_postgresql": model.get("connects_postgresql"),
            }
        ],
    )
    lines = [
        "# V1.5 formal read-only COM minimal executor review",
        "",
        "This is an offline implementation-review sidecar. It does not open COM.",
        "",
        f"- overall_status: `{model.get('overall_status')}`",
        f"- minimal_executor_review_ready: `{model.get('minimal_executor_review_ready')}`",
        f"- plan_preview_status: `{model.get('plan_preview_status')}`",
        f"- plan_preview_command_plan_present: `{model.get('plan_preview_command_plan_present')}`",
        f"- future_command_count: `{model.get('future_command_count')}`",
        f"- future_check_command_count: `{model.get('future_check_command_count')}`",
        f"- old_algorithm_check_skip_count: `{model.get('old_algorithm_check_skip_count')}`",
        f"- read_only_real_com_execution_allowed: `{model.get('read_only_real_com_execution_allowed')}`",
        f"- opens_com_ports: `{model.get('opens_com_ports')}`",
        f"- writes_sn: `{model.get('writes_sn')}`",
        f"- writes_coefficients: `{model.get('writes_coefficients')}`",
        f"- connects_postgresql: `{model.get('connects_postgresql')}`",
        "",
        "## Checks",
        "",
    ]
    for row in model.get("checks", []):
        reasons = ";".join(row.get("reasons") or ())
        lines.append(f"- `{row.get('check')}`: `{row.get('status')}` {reasons}".rstrip())
    lines.extend(
        [
            "",
            "## Output Evidence Contract",
            "",
            "See `v1_5_formal_readonly_com_minimal_executor_output_contract.csv`.",
            "",
            "## Failure Hold Matrix",
            "",
            "See `v1_5_formal_readonly_com_minimal_executor_hold_matrix.csv`.",
        ]
    )
    paths["markdown"].parent.mkdir(parents=True, exist_ok=True)
    paths["markdown"].write_text("\n".join(lines) + "\n", encoding="utf-8")
    return paths
