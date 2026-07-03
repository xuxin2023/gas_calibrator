"""Plan-only stub for the future V1.5 read-only COM executor.

This is the first executor-shaped surface after the minimal executor review,
but it is still intentionally no-COM. It may record future operator/reviewer
context as inert metadata and write a would-execute artifact, while every real
execution flag remains locked.
"""

from __future__ import annotations

import csv
import json
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping, Sequence


SCHEMA = "v1_5_formal_readonly_com_minimal_executor_stub_v1"
MINIMAL_REVIEW_SCHEMA = "v1_5_formal_readonly_com_minimal_executor_review_v1"
MINIMAL_REVIEW_READY_STATUS = "blocked_pending_minimal_readonly_com_executor_implementation"
BLOCKED_STATUS = "blocked_plan_only_minimal_readonly_com_executor_stub"
REVIEW_STATUS = "review_required"
MIN_SERIAL_COMMAND_GAP_S = 1.0


@dataclass(frozen=True)
class ReadonlyComMinimalExecutorStubCheck:
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
) -> ReadonlyComMinimalExecutorStubCheck:
    return ReadonlyComMinimalExecutorStubCheck(
        check=check,
        status=status,
        evidence_role=evidence_role,
        reasons=tuple(reasons),
        physical_meaning=physical_meaning,
        next_action=next_action,
        details=details,
    )


def _review_reasons(payload: Mapping[str, Any]) -> list[str]:
    reasons: list[str] = []
    if not payload:
        return ["minimal_executor_review_missing"]
    if payload.get("schema") != MINIMAL_REVIEW_SCHEMA:
        reasons.append(f"schema={payload.get('schema') or 'missing'}")
    if payload.get("overall_status") != MINIMAL_REVIEW_READY_STATUS:
        reasons.append(f"overall_status={payload.get('overall_status') or 'missing'}")
    if payload.get("minimal_executor_review_ready") is not True:
        reasons.append(f"minimal_executor_review_ready={payload.get('minimal_executor_review_ready')!r}")
    if int(payload.get("review_required_count") or 0):
        reasons.append(f"review_required_count={payload.get('review_required_count')}")
    if float(payload.get("minimum_serial_command_gap_s") or 0.0) < MIN_SERIAL_COMMAND_GAP_S:
        reasons.append(f"minimum_serial_command_gap_s={payload.get('minimum_serial_command_gap_s')!r}")
    if not payload.get("future_output_evidence_contract"):
        reasons.append("future_output_evidence_contract_missing")
    if not payload.get("future_failure_hold_matrix"):
        reasons.append("future_failure_hold_matrix_missing")
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
            reasons.append(f"minimal_review_boundary_{field}={payload.get(field)!r}")
    if payload.get("not_real_acceptance_evidence") is not True:
        reasons.append(f"not_real_acceptance_evidence={payload.get('not_real_acceptance_evidence')!r}")
    return reasons


def _nonempty(value: str | Path | None) -> bool:
    return bool(str(value or "").strip())


def _would_execute_rows(review: Mapping[str, Any]) -> list[dict[str, Any]]:
    output_contract = review.get("future_output_evidence_contract")
    hold_matrix = review.get("future_failure_hold_matrix")
    return [
        {
            "order": 1,
            "would_execute": False,
            "stage": "load_reviewed_plan_and_authorization_context",
            "reason": "stub_only_no_com",
            "future_required_artifact_count": len(output_contract) if isinstance(output_contract, list) else 0,
            "future_hold_rule_count": len(hold_matrix) if isinstance(hold_matrix, list) else 0,
            "future_command_count": review.get("future_command_count", 0),
            "future_check_command_count": review.get("future_check_command_count", 0),
        },
        {
            "order": 2,
            "would_execute": False,
            "stage": "open_serial_ports",
            "reason": "locked_until_separate_real_executor_pr",
            "future_required_artifact_count": "",
            "future_hold_rule_count": "",
            "future_command_count": 0,
            "future_check_command_count": 0,
        },
        {
            "order": 3,
            "would_execute": False,
            "stage": "read_sn_getco_runtime_check",
            "reason": "no_serial_io_in_stub",
            "future_required_artifact_count": "",
            "future_hold_rule_count": "",
            "future_command_count": review.get("future_command_count", 0),
            "future_check_command_count": review.get("future_check_command_count", 0),
        },
    ]


def build_v1_5_formal_readonly_com_minimal_executor_stub(
    *,
    formal_readonly_com_minimal_executor_review_json: str | Path | None,
    operator_confirmation_text: str | None = None,
    authorization_id: str | None = None,
    reviewer: str | None = None,
    approver: str | None = None,
    reviewed_port_inventory_json: str | Path | None = None,
    active_analyzer_list_json: str | Path | None = None,
) -> dict[str, Any]:
    review_path = (
        Path(formal_readonly_com_minimal_executor_review_json).resolve()
        if formal_readonly_com_minimal_executor_review_json
        else None
    )
    review = _load_json(review_path)
    review_reasons = _review_reasons(review)
    authorization_context_present = any(
        (
            _nonempty(operator_confirmation_text),
            _nonempty(authorization_id),
            _nonempty(reviewer),
            _nonempty(approver),
            _nonempty(reviewed_port_inventory_json),
            _nonempty(active_analyzer_list_json),
        )
    )

    checks = [
        _check(
            check="minimal_executor_review_consumed",
            status="ready" if not review_reasons else "review_required",
            evidence_role="required_minimal_executor_review",
            reasons=review_reasons,
            physical_meaning=(
                "The first executor-shaped stub must start from the accepted minimal executor review "
                "so it inherits output evidence, hold matrix, old-algorithm CHECK skip, and >=1s pacing."
            ),
            next_action="Regenerate the minimal executor review until its no-COM/no-write boundaries are clean.",
            details={"source_path": str(review_path) if review_path else ""},
        ),
        _check(
            check="authorization_context_recorded_but_not_used_as_unlock",
            status="ready",
            evidence_role="inert_operator_context",
            physical_meaning=(
                "Operator/reviewer/port inputs may be parsed as future context, but this stub never treats "
                "them as permission to open COM or read an analyzer."
            ),
            next_action="Keep the real authorization packet validation in the later real executor PR.",
            details={
                "authorization_context_present": authorization_context_present,
                "authorization_context_consumed_as_unlock": False,
                "reviewed_port_inventory_json": str(reviewed_port_inventory_json or ""),
                "active_analyzer_list_json": str(active_analyzer_list_json or ""),
            },
        ),
        _check(
            check="would_execute_artifact_only",
            status="ready",
            evidence_role="plan_only_would_execute_trace",
            physical_meaning=(
                "The stub records what a future executor would be expected to prepare, but every row remains "
                "would_execute=false and no serial attempt artifacts are produced."
            ),
            next_action="Do not connect this artifact to live device acceptance.",
            details={"would_execute_row_count": len(_would_execute_rows(review))},
        ),
        _check(
            check="real_com_write_database_route_locks",
            status="ready",
            evidence_role="hard_side_effect_lock",
            physical_meaning=(
                "COM, SN/device_code writes, SENCO writes, PostgreSQL import, pressure control, and gas/water "
                "route control remain outside this package."
            ),
            next_action="Implement actual read-only COM in a separate PR with explicit user authorization.",
            details={
                "opens_com_ports": False,
                "writes_sn": False,
                "writes_coefficients": False,
                "connects_postgresql": False,
            },
        ),
    ]

    review_required_count = sum(1 for row in checks if row.status == "review_required")
    stub_ready = review_required_count == 0
    return {
        "schema": SCHEMA,
        "generated_at": _now(),
        "overall_status": BLOCKED_STATUS if stub_ready else REVIEW_STATUS,
        "blocker_count": 0,
        "review_required_count": review_required_count,
        "minimal_executor_stub_ready": stub_ready,
        "would_execute_artifact_ready": stub_ready,
        "production_state": "plan_only_stub_no_com",
        "formal_readonly_com_minimal_executor_review_json": str(review_path) if review_path else "",
        "minimal_executor_review_status": str(review.get("overall_status") or ""),
        "execution_supported": False,
        "execution_requested": False,
        "dry_run_only": True,
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
        "minimum_serial_command_gap_s": MIN_SERIAL_COMMAND_GAP_S,
        "authorization_context_present": authorization_context_present,
        "authorization_context_consumed_as_unlock": False,
        "operator_confirmation_text_present": _nonempty(operator_confirmation_text),
        "authorization_id_present": _nonempty(authorization_id),
        "reviewer_present": _nonempty(reviewer),
        "approver_present": _nonempty(approver),
        "reviewed_port_inventory_json": str(reviewed_port_inventory_json or ""),
        "active_analyzer_list_json": str(active_analyzer_list_json or ""),
        "future_command_count": int(review.get("future_command_count") or 0),
        "future_check_command_count": int(review.get("future_check_command_count") or 0),
        "old_algorithm_check_skip_count": int(review.get("old_algorithm_check_skip_count") or 0),
        "would_execute_rows": _would_execute_rows(review),
        "checks": [row.to_json() for row in checks],
        "next_action": (
            "Keep COM locked. The next PR may implement real read-only COM only with explicit authorization, "
            "reviewed ports, active analyzers, >=1s pacing, and hold evidence."
        ),
    }


def write_v1_5_formal_readonly_com_minimal_executor_stub_outputs(
    model: Mapping[str, Any],
    output_dir: str | Path,
) -> dict[str, Path]:
    out = Path(output_dir)
    paths = {
        "json": out / "v1_5_formal_readonly_com_minimal_executor_stub.json",
        "checks_csv": out / "v1_5_formal_readonly_com_minimal_executor_stub_checks.csv",
        "would_execute_csv": out / "v1_5_formal_readonly_com_minimal_executor_would_execute.csv",
        "summary_csv": out / "v1_5_formal_readonly_com_minimal_executor_stub_summary.csv",
        "markdown": out / "V1_5_FORMAL_READONLY_COM_MINIMAL_EXECUTOR_STUB.md",
    }
    _write_json(paths["json"], model)
    _write_csv(paths["checks_csv"], model.get("checks", []))
    _write_csv(paths["would_execute_csv"], model.get("would_execute_rows", []))
    _write_csv(
        paths["summary_csv"],
        [
            {
                "overall_status": model.get("overall_status"),
                "minimal_executor_stub_ready": model.get("minimal_executor_stub_ready"),
                "would_execute_artifact_ready": model.get("would_execute_artifact_ready"),
                "authorization_context_present": model.get("authorization_context_present"),
                "authorization_context_consumed_as_unlock": model.get(
                    "authorization_context_consumed_as_unlock"
                ),
                "execution_supported": model.get("execution_supported"),
                "read_only_real_com_execution_allowed": model.get(
                    "read_only_real_com_execution_allowed"
                ),
                "opens_com_ports": model.get("opens_com_ports"),
                "writes_sn": model.get("writes_sn"),
                "writes_coefficients": model.get("writes_coefficients"),
                "connects_postgresql": model.get("connects_postgresql"),
            }
        ],
    )
    lines = [
        "# V1.5 formal read-only COM minimal executor stub",
        "",
        "This is a plan-only stub. It does not open COM or read analyzers.",
        "",
        f"- overall_status: `{model.get('overall_status')}`",
        f"- minimal_executor_stub_ready: `{model.get('minimal_executor_stub_ready')}`",
        f"- authorization_context_present: `{model.get('authorization_context_present')}`",
        f"- authorization_context_consumed_as_unlock: `{model.get('authorization_context_consumed_as_unlock')}`",
        f"- execution_supported: `{model.get('execution_supported')}`",
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
            "## Would Execute Trace",
            "",
            "See `v1_5_formal_readonly_com_minimal_executor_would_execute.csv`.",
        ]
    )
    paths["markdown"].parent.mkdir(parents=True, exist_ok=True)
    paths["markdown"].write_text("\n".join(lines) + "\n", encoding="utf-8")
    return paths
