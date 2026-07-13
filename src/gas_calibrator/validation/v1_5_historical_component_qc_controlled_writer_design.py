"""Design a future controlled historical component-QC writer without writing."""

from __future__ import annotations

import csv
import hashlib
import json
from pathlib import Path
from typing import Any, Mapping, Sequence

from .v1_5_historical_component_qc_blocked_generator_plan import (
    READY_STATUS as PLAN_READY_STATUS,
    SCHEMA as PLAN_SCHEMA,
    build_v1_5_historical_component_qc_blocked_generator_plan,
)


SCHEMA = "v1_5_historical_component_qc_controlled_writer_design_v1"
READY_STATUS = "ready_for_historical_component_qc_controlled_writer_design_review"
BLOCKED_STATUS = "blocked_historical_component_qc_controlled_writer_design"
FUTURE_AUTHORIZATION_SCHEMA = "v1_5_historical_component_qc_write_authorization_v1"
REVIEW_OUTPUT_SUFFIX = (
    "docs",
    "v1_5_flow_contract",
    "historical_component_qc_controlled_writer_design",
)

_EXPECTED_PLAN_PRODUCTION_STATE = "blocked_plan_only_no_evaluation_no_write"
_PLAN_FALSE_LOCKS = (
    "execution_supported",
    "component_qc_evaluation_allowed",
    "component_qc_grade_derivation_allowed",
    "production_component_qc_generator_available",
    "historical_component_qc_generation_allowed",
    "historical_component_qc_write_allowed",
    "component_qc_overwrite_allowed",
    "component_qc_backfill_allowed",
    "historical_fit_allowed",
    "formal_release_allowed",
    "database_import_allowed",
    "opens_com_ports",
    "controls_pressure",
    "controls_water_or_gas_routes",
    "writes_coefficients",
    "writes_sn_or_device_code",
    "connects_postgresql",
)
_OUTPUT_FILENAME = "formal_open_flow_data_quality_by_analyzer.csv"


def _read_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8-sig") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise ValueError(f"Expected JSON object: {path}")
    return payload


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _sha256_value(value: Any) -> str:
    raw = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def _path_key(value: Any) -> str:
    raw = str(value or "").strip()
    return str(Path(raw).resolve()).casefold() if raw else ""


def _plan_reasons(plan_path: Path, plan: Mapping[str, Any]) -> tuple[list[str], dict[str, Any]]:
    reasons: list[str] = []
    if plan.get("schema") != PLAN_SCHEMA:
        reasons.append("blocked_plan_schema_mismatch")
    if plan.get("overall_status") != PLAN_READY_STATUS:
        reasons.append("blocked_plan_status_not_ready_for_review")
    if plan.get("production_state") != _EXPECTED_PLAN_PRODUCTION_STATE:
        reasons.append("blocked_plan_production_state_invalid")
    if plan.get("blocked_generator_plan_ready") is not True:
        reasons.append("blocked_plan_ready_flag_not_true")
    if plan.get("global_blocker_codes") not in ([], ()):
        reasons.append("blocked_plan_global_blockers_present")
    operation_plan = plan.get("operation_plan")
    if not isinstance(operation_plan, list) or not operation_plan:
        reasons.append("blocked_plan_operations_missing_or_empty")
    elif plan.get("candidate_count") != len(operation_plan):
        reasons.append("blocked_plan_candidate_count_mismatch")
    if plan.get("candidate_blocked_count") != 0:
        reasons.append("blocked_plan_candidate_blockers_present")
    if isinstance(operation_plan, list) and plan.get("candidate_plan_ready_count") != len(
        operation_plan
    ):
        reasons.append("blocked_plan_not_all_candidates_ready")
    if plan.get("source_evidence_check_blocked_count") != 0:
        reasons.append("blocked_plan_source_evidence_blockers_present")
    if plan.get("source_artifact_check_blocked_count") != 0:
        reasons.append("blocked_plan_source_artifact_blockers_present")
    locks = plan.get("locks")
    if not isinstance(locks, Mapping):
        reasons.append("blocked_plan_locks_missing")
    else:
        if locks.get("blocked_generator_plan_available") is not True:
            reasons.append("blocked_plan_available_flag_missing")
        for key in _PLAN_FALSE_LOCKS:
            if locks.get(key) is not False:
                reasons.append(f"blocked_plan_lock_not_false:{key}")
    if plan.get("evidence_source") != "historical_replay":
        reasons.append("blocked_plan_evidence_source_invalid")
    if plan.get("not_real_acceptance_evidence") is not True:
        reasons.append("blocked_plan_real_acceptance_lock_missing")

    preflight_path = Path(str(plan.get("preflight_json_path") or "")).resolve()
    recorded_preflight_sha = str(plan.get("preflight_json_sha256") or "").lower()
    if not str(plan.get("preflight_json_path") or "").strip():
        reasons.append("blocked_plan_preflight_path_missing")
        recomputed: dict[str, Any] = {}
    elif not preflight_path.is_file():
        reasons.append("blocked_plan_preflight_file_missing")
        recomputed = {}
    else:
        if _sha256_file(preflight_path) != recorded_preflight_sha:
            reasons.append("blocked_plan_preflight_sha256_mismatch")
        try:
            recomputed = build_v1_5_historical_component_qc_blocked_generator_plan(
                preflight_json_path=preflight_path
            )
        except (OSError, ValueError, json.JSONDecodeError):
            recomputed = {}
            reasons.append("blocked_plan_recompute_failed")
    if recomputed and dict(plan) != recomputed:
        reasons.append("blocked_plan_recompute_mismatch")
    if not plan_path.is_file():
        reasons.append("blocked_plan_evidence_missing")
    return reasons, recomputed


def _candidate_bindings(
    plan: Mapping[str, Any], plan_sha256: str
) -> tuple[list[dict[str, Any]], list[str]]:
    bindings: list[dict[str, Any]] = []
    reasons: list[str] = []
    seen_points: set[str] = set()
    seen_targets: set[str] = set()
    operations = plan.get("operation_plan")
    rows = operations if isinstance(operations, list) else []
    for index, row in enumerate(rows, start=1):
        if not isinstance(row, Mapping):
            reasons.append(f"blocked_plan_operation_not_object:row_{index}")
            continue
        point_dir = Path(str(row.get("point_dir") or "")).resolve()
        target = Path(str(row.get("planned_output_path") or "")).resolve()
        point_key = _path_key(point_dir)
        target_key = _path_key(target)
        row_reasons: list[str] = []
        if not str(row.get("point_dir") or "").strip():
            row_reasons.append("point_dir_missing")
        if row.get("route_kind") not in {"co2", "h2o"}:
            row_reasons.append("route_kind_invalid")
        if target != (point_dir / _OUTPUT_FILENAME).resolve():
            row_reasons.append("target_path_invalid")
        if point_key in seen_points:
            row_reasons.append("duplicate_point_dir")
        if target_key in seen_targets:
            row_reasons.append("duplicate_target_path")
        seen_points.add(point_key)
        seen_targets.add(target_key)
        if target.exists():
            row_reasons.append("target_now_exists")
        for key in ("would_evaluate", "would_derive_grades", "would_write", "overwrite_allowed"):
            if row.get(key) is not False:
                row_reasons.append(f"operation_lock_not_false:{key}")
        if row.get("requires_distinct_authorization") is not True:
            row_reasons.append("distinct_authorization_requirement_missing")
        if row.get("manual_gate_review_required") is not True:
            row_reasons.append("manual_gate_requirement_missing")
        if row.get("formal_fit_allowed") is not False:
            row_reasons.append("formal_fit_lock_not_false")
        if row.get("preflight_json_sha256") != plan.get("preflight_json_sha256"):
            row_reasons.append("operation_preflight_sha256_mismatch")
        source_packet_sha = str(row.get("source_packet_sha256") or "").lower()
        if len(source_packet_sha) != 64:
            row_reasons.append("source_packet_sha256_invalid")
        row_reasons = sorted(set(row_reasons))
        binding_material = {
            "plan_sha256": plan_sha256,
            "preflight_sha256": plan.get("preflight_json_sha256"),
            "point_dir": str(point_dir),
            "target_path": str(target),
            "source_packet_sha256": source_packet_sha,
        }
        bindings.append(
            {
                "candidate_index": index,
                "route_kind": row.get("route_kind"),
                "point_name": row.get("point_name"),
                "point_dir": str(point_dir),
                "planned_output_path": str(target),
                "source_artifact_count": row.get("source_artifact_count"),
                "source_packet_sha256": source_packet_sha,
                "preflight_json_sha256": plan.get("preflight_json_sha256"),
                "blocked_plan_json_sha256": plan_sha256,
                "candidate_binding_sha256": _sha256_value(binding_material),
                "binding_ready_for_design_review": not row_reasons,
                "blocker_codes": row_reasons,
                "payload_derivation_supported": False,
                "write_execution_supported": False,
                "target_create_mode": "exclusive_create_only_future_contract",
                "overwrite_allowed": False,
                "manual_gate_review_required": True,
            }
        )
    return bindings, reasons


def _authorization_contract() -> list[dict[str, Any]]:
    return [
        {
            "field": "authorization_id_and_nonce",
            "required": True,
            "rule": "Unique one-time authorization identity; reuse is rejected.",
        },
        {
            "field": "operator_reviewer_approver",
            "required": True,
            "rule": "All identities are recorded; reviewer and approver must be distinct.",
        },
        {
            "field": "issued_at_expires_at_utc",
            "required": True,
            "rule": "Authorization is time bounded and rejected after expiry.",
        },
        {
            "field": "blocked_plan_binding",
            "required": True,
            "rule": "Bind exact blocked-plan path, SHA256, schema, ready status, and candidate count.",
        },
        {
            "field": "preflight_and_candidate_set_binding",
            "required": True,
            "rule": "Bind preflight SHA256 and the complete ordered candidate-binding-set SHA256.",
        },
        {
            "field": "reviewed_evaluator_and_payload_bundle_binding",
            "required": True,
            "rule": "Bind a separately reviewed evaluator version plus all staged payload byte hashes; absent today.",
        },
        {
            "field": "structured_scope_confirmation",
            "required": True,
            "rule": "Explicitly allow only historical component-QC create-only targets; deny overwrite, fit, release, import, COM, route, identity, and coefficient actions.",
        },
        {
            "field": "rollback_scope_confirmation",
            "required": True,
            "rule": "Compensating rollback may remove only files created by this authorization whose readback hash matches the transaction ledger.",
        },
    ]


def _atomic_write_contract() -> list[dict[str, Any]]:
    return [
        {
            "sequence": 1,
            "stage": "revalidate_inputs",
            "rule": "Recompute blocked plan from exact preflight and rehash all source artifacts; any drift holds the batch.",
        },
        {
            "sequence": 2,
            "stage": "derive_in_isolated_staging",
            "rule": "A future reviewed evaluator derives every payload outside historical point directories; current design has no evaluator.",
        },
        {
            "sequence": 3,
            "stage": "validate_complete_payload_batch",
            "rule": "Validate canonical schema, analyzer coverage, grade semantics, source lineage, and byte hashes for all candidates before creating any target.",
        },
        {
            "sequence": 4,
            "stage": "recheck_all_targets_absent",
            "rule": "Every exact target must still be absent immediately before commit; replacement and overwrite APIs are forbidden.",
        },
        {
            "sequence": 5,
            "stage": "exclusive_create_commit",
            "rule": "Create exact targets with OS-level exclusive create semantics. Cross-directory batch atomicity is not assumed.",
        },
        {
            "sequence": 6,
            "stage": "fsync_and_readback",
            "rule": "Flush each new file and directory, read exact bytes back, compare SHA256, and parse canonical schema before proceeding.",
        },
        {
            "sequence": 7,
            "stage": "batch_closeout",
            "rule": "Only a complete 125/125 readback ledger may close the write transaction; fitting remains separately blocked.",
        },
    ]


def _readback_rollback_contract() -> list[dict[str, Any]]:
    return [
        {
            "trigger": "authorization_or_binding_invalid",
            "action": "hold_before_payload_derivation",
            "rollback": "none",
        },
        {
            "trigger": "source_drift_or_target_exists",
            "action": "hold_before_first_create",
            "rollback": "none_and_never_touch_existing_target",
        },
        {
            "trigger": "payload_schema_or_batch_incomplete",
            "action": "discard_staging_and_hold",
            "rollback": "historical_targets_untouched",
        },
        {
            "trigger": "exclusive_create_failure_mid_batch",
            "action": "stop_all_remaining_creates_and_hold",
            "rollback": "remove_only_current_transaction_files_with_matching_ledger_hash",
        },
        {
            "trigger": "readback_hash_or_parse_mismatch",
            "action": "stop_all_remaining_creates_and_hold",
            "rollback": "remove_only_current_transaction_files_with_matching_ledger_hash",
        },
        {
            "trigger": "compensating_rollback_incomplete",
            "action": "manual_incident_hold_no_fit_no_release_no_import",
            "rollback": "preserve_full_attempt_and_partial_state_evidence",
        },
        {
            "trigger": "all_readbacks_pass",
            "action": "record_write_transaction_complete_only",
            "rollback": "none_but_fit_still_requires_separate_authorization",
        },
    ]


def build_v1_5_historical_component_qc_controlled_writer_design(
    *, blocked_generator_plan_json_path: str | Path
) -> dict[str, Any]:
    """Review a future controlled writer contract while all execution stays absent."""

    plan_path = Path(blocked_generator_plan_json_path).resolve()
    plan = _read_json(plan_path)
    reasons, _recomputed = _plan_reasons(plan_path, plan)
    plan_sha = _sha256_file(plan_path)
    bindings, binding_global_reasons = _candidate_bindings(plan, plan_sha)
    reasons.extend(binding_global_reasons)
    blocked_binding_count = sum(not row["binding_ready_for_design_review"] for row in bindings)
    if blocked_binding_count:
        reasons.append("candidate_binding_blockers_present")
    if len(bindings) != plan.get("candidate_count"):
        reasons.append("candidate_binding_count_mismatch")
    reasons = sorted(set(reasons))
    ready = not reasons and bool(bindings)
    binding_set = [
        {
            "candidate_index": row["candidate_index"],
            "candidate_binding_sha256": row["candidate_binding_sha256"],
        }
        for row in bindings
    ]
    return {
        "schema": SCHEMA,
        "overall_status": READY_STATUS if ready else BLOCKED_STATUS,
        "production_state": "blocked_design_only_no_evaluator_no_writer",
        "controlled_writer_design_ready": ready,
        "review_required_count": len(reasons),
        "review_reasons": reasons,
        "blocked_generator_plan_json_path": str(plan_path),
        "blocked_generator_plan_json_sha256": plan_sha,
        "preflight_json_sha256": plan.get("preflight_json_sha256"),
        "candidate_binding_count": len(bindings),
        "candidate_binding_blocked_count": blocked_binding_count,
        "candidate_binding_set_sha256": _sha256_value(binding_set),
        "future_authorization_schema": FUTURE_AUTHORIZATION_SCHEMA,
        "candidate_bindings": bindings,
        "authorization_contract": _authorization_contract(),
        "atomic_write_contract": _atomic_write_contract(),
        "readback_rollback_contract": _readback_rollback_contract(),
        "locks": {
            "controlled_writer_design_available": True,
            "authorization_validator_available": False,
            "component_qc_payload_evaluator_available": False,
            "atomic_create_only_writer_available": False,
            "writer_execution_supported": False,
            "component_qc_evaluation_allowed": False,
            "component_qc_grade_derivation_allowed": False,
            "historical_component_qc_generation_allowed": False,
            "historical_component_qc_write_allowed": False,
            "component_qc_overwrite_allowed": False,
            "component_qc_backfill_allowed": False,
            "readback_verification_execution_allowed": False,
            "compensating_rollback_execution_allowed": False,
            "historical_fit_allowed": False,
            "formal_release_allowed": False,
            "database_import_allowed": False,
            "opens_com_ports": False,
            "controls_pressure": False,
            "controls_water_or_gas_routes": False,
            "writes_coefficients": False,
            "writes_sn_or_device_code": False,
            "connects_postgresql": False,
        },
        "evidence_source": "historical_replay",
        "not_real_acceptance_evidence": True,
        "next_action": (
            "Keep historical writes locked. Build a separate authorization validator and reviewed payload evaluator before implementing any create-only writer."
        ),
    }


def _write_csv(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    fields: list[str] = []
    for row in rows:
        for key in row:
            if key not in fields:
                fields.append(str(key))
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields or ["empty"])
        writer.writeheader()
        for row in rows:
            writer.writerow(
                {
                    key: json.dumps(value, ensure_ascii=False, sort_keys=True)
                    if isinstance(value, (list, dict))
                    else value
                    for key, value in row.items()
                }
            )


def write_v1_5_historical_component_qc_controlled_writer_design(
    model: Mapping[str, Any], output_dir: str | Path
) -> dict[str, Path]:
    """Write design-review artifacts only, never historical component-QC targets."""

    out = Path(output_dir).resolve()
    suffix = tuple(part.lower() for part in out.parts[-len(REVIEW_OUTPUT_SUFFIX) :])
    if suffix != REVIEW_OUTPUT_SUFFIX:
        raise ValueError(
            "output_dir_must_be_historical_component_qc_controlled_writer_design_directory"
        )
    out.mkdir(parents=True, exist_ok=True)
    outputs = {
        "json": out / "v1_5_historical_component_qc_controlled_writer_design.json",
        "candidate_bindings_csv": out
        / "v1_5_historical_component_qc_controlled_writer_candidate_bindings.csv",
        "authorization_contract_csv": out
        / "v1_5_historical_component_qc_write_authorization_contract.csv",
        "atomic_write_contract_csv": out
        / "v1_5_historical_component_qc_atomic_write_contract.csv",
        "readback_rollback_contract_csv": out
        / "v1_5_historical_component_qc_readback_rollback_contract.csv",
        "markdown": out / "V1_5_HISTORICAL_COMPONENT_QC_CONTROLLED_WRITER_DESIGN.md",
    }
    outputs["json"].write_text(
        json.dumps(dict(model), ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    _write_csv(outputs["candidate_bindings_csv"], model.get("candidate_bindings") or [])
    _write_csv(outputs["authorization_contract_csv"], model.get("authorization_contract") or [])
    _write_csv(outputs["atomic_write_contract_csv"], model.get("atomic_write_contract") or [])
    _write_csv(
        outputs["readback_rollback_contract_csv"],
        model.get("readback_rollback_contract") or [],
    )
    locks = model.get("locks") or {}
    lines = [
        "# V1.5 Historical Component-QC Controlled Writer Design",
        "",
        f"- overall_status: `{model.get('overall_status')}`",
        f"- production_state: `{model.get('production_state')}`",
        f"- candidate_binding_count: `{model.get('candidate_binding_count')}`",
        f"- candidate_binding_blocked_count: `{model.get('candidate_binding_blocked_count')}`",
        f"- authorization_validator_available: `{locks.get('authorization_validator_available')}`",
        f"- component_qc_payload_evaluator_available: `{locks.get('component_qc_payload_evaluator_available')}`",
        f"- atomic_create_only_writer_available: `{locks.get('atomic_create_only_writer_available')}`",
        f"- historical_component_qc_write_allowed: `{locks.get('historical_component_qc_write_allowed')}`",
        f"- component_qc_overwrite_allowed: `{locks.get('component_qc_overwrite_allowed')}`",
        f"- historical_fit_allowed: `{locks.get('historical_fit_allowed')}`",
        "- evidence_source: `historical_replay`",
        "- not_real_acceptance_evidence: `true`",
        "",
        "This is a controlled-writer design review only. It defines future authorization, exclusive-create, readback, and compensating-rollback contracts but implements none of them.",
    ]
    outputs["markdown"].write_text("\n".join(lines) + "\n", encoding="utf-8")
    return outputs


__all__ = [
    "BLOCKED_STATUS",
    "FUTURE_AUTHORIZATION_SCHEMA",
    "READY_STATUS",
    "REVIEW_OUTPUT_SUFFIX",
    "SCHEMA",
    "build_v1_5_historical_component_qc_controlled_writer_design",
    "write_v1_5_historical_component_qc_controlled_writer_design",
]
