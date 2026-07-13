"""Final offline program-level acceptance suite for V1.5.

The suite binds reviewed repository artifacts and an allowlisted pytest run. It
never promotes offline evidence to real acceptance and never opens hardware,
executes a route, writes analyzer state, or connects to PostgreSQL.
"""

from __future__ import annotations

import csv
import hashlib
import json
import re
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping, Sequence


SCHEMA = "v1_5_final_offline_acceptance_suite_v1"
PLAN_READY_STATUS = "ready_for_final_offline_acceptance_suite_execution"
PASS_STATUS = "offline_program_acceptance_passed_real_acceptance_blocked"
REVIEW_STATUS = "final_offline_acceptance_review_required"

_SHA_RE = re.compile(r"^[0-9a-f]{40}$")

SUITE_TEST_FILES: tuple[str, ...] = (
    "tests/test_v1_5_final_offline_acceptance_suite.py",
    "tests/test_v1_5_final_production_gap_freeze.py",
    "tests/test_v1_5_legacy_full_flow_offline_replay.py",
    "tests/test_v1_5_mature_route_contract.py",
    "tests/test_v1_5_production_entrypoint_gate.py",
    "tests/test_v1_5_historical_replay_contract.py",
    "tests/test_v1_5_production_component_qc_fit_matrix.py",
    "tests/test_v1_5_historical_fit_profile_parity.py",
    "tests/test_v1_5_unified_controlled_write_reverify.py",
    "tests/test_v1_5_new_algorithm_mature_queue_live_handoff.py",
    "tests/test_v1_5_formal_database_import_transaction_plan.py",
    "tests/test_v1_5_formal_database_import_transaction_blocked_executor.py",
    "tests/test_v1_5_entrypoint_inventory.py",
    "tests/test_v1_5_formal_flow_contract.py",
    "tests/test_v1_5_full_flow_orchestration.py",
    "tests/test_v1_5_formal_run_status.py",
    "tests/test_v1_5_formal_archive_closure.py",
    "tests/test_v1_5_dirty_zone_audit.py",
    "tests/v2/sim/test_suites.py",
    "tests/v2/test_orchestrator.py",
    "tests/v2/test_run_simulation_suite.py",
    "tests/v2/test_export_resilience.py",
    "tests/v2/test_summary_parity.py",
)


@dataclass(frozen=True)
class ArtifactContract:
    role: str
    relative_path: str
    schema: str
    status_path: str
    accepted_statuses: tuple[str, ...]
    expectations: tuple[tuple[str, Any], ...]
    physical_meaning: str


ARTIFACT_CONTRACTS: tuple[ArtifactContract, ...] = (
    ArtifactContract(
        "production_gap_freeze",
        "docs/v1_5_flow_contract/final_production_gap_freeze/v1_5_final_production_gap_freeze.json",
        "v1_5_final_production_gap_freeze_v1",
        "overall_status",
        ("production_gap_scope_frozen_offline_replay_next",),
        (("scope_frozen", True), ("critical_gap_count", 7)),
        "Freezes the seven reviewed production gaps and prevents scope drift.",
    ),
    ArtifactContract(
        "legacy_full_flow_offline_replay",
        "docs/v1_5_flow_contract/legacy_full_flow_offline_replay/v1_5_legacy_full_flow_offline_replay.json",
        "v1_5_legacy_full_flow_offline_replay_v1",
        "overall_status",
        ("legacy_full_flow_replay_complete_production_evidence_incomplete",),
        (
            ("orchestrator_replay_complete", True),
            ("production_flow_complete", False),
            ("expected_point_counts.co2", 45),
            ("expected_point_counts.h2o", 13),
        ),
        "Proves the mature legacy stage order while retaining real-evidence holds.",
    ),
    ArtifactContract(
        "mature_route_contract",
        "docs/v1_5_flow_contract/mature_route_contract/v1_5_mature_route_contract.json",
        "v1_5_mature_route_contract_v1",
        "manifest.status",
        ("pass",),
        (("manifest.blocker_count", 0),),
        "Protects the 0613 fitting and 0620/0621 physical route baselines.",
    ),
    ArtifactContract(
        "production_entrypoint_gate",
        "docs/v1_5_flow_contract/production_entrypoint_gate/v1_5_production_entrypoint_gate.json",
        "v1_5_production_entrypoint_gate_v1",
        "manifest.status",
        ("pass",),
        (("manifest.blocker_count", 0),),
        "Blocks migration, diagnostic, worker, V1, V2, and handoff surfaces from formal plans.",
    ),
    ArtifactContract(
        "historical_replay_contract",
        "docs/v1_5_flow_contract/historical_replay_contract/v1_5_historical_replay_contract.json",
        "v1_5_historical_replay_contract_v1",
        "manifest.status",
        ("pass",),
        (("manifest.blocker_count", 0),),
        "Keeps historical replay explanatory, component-aware, and non-promotional.",
    ),
    ArtifactContract(
        "production_component_qc_fit_matrix",
        "docs/v1_5_flow_contract/production_component_qc_fit_matrix/v1_5_production_component_qc_fit_matrix.json",
        "v1_5_production_component_qc_fit_matrix_v1",
        "overall_status",
        ("production_component_qc_evaluated_fit_matrix_blocked_by_continuity",),
        (
            ("production_component_qc_evaluator_available", True),
            ("canonical_0613_strategy_matrix_available", True),
            ("production_component_qc_evaluation_complete", True),
            ("production_fit_allowed", False),
        ),
        "Exercises production QC and 0613 strategies without inventing continuous-route evidence.",
    ),
    ArtifactContract(
        "unified_controlled_write_reverify",
        "docs/v1_5_flow_contract/unified_controlled_write_reverify/v1_5_unified_controlled_write_readback_reverify.json",
        "v1_5_unified_controlled_write_readback_reverify_v1",
        "overall_status",
        ("blocked_no_fit_approved_candidate",),
        (
            ("unified_contract_available", True),
            ("frozen_gap_program_contract_closed", True),
            ("frozen_gap_production_evidence_closed", False),
            ("write_success_separate_from_validation_success", True),
        ),
        "Freezes calc, authorization, write, readback, rollback, and reverify as separate events.",
    ),
    ArtifactContract(
        "new_algorithm_47_14_handoff",
        "docs/v1_5_flow_contract/new_algorithm_mature_queue_live_handoff/v1_5_new_algorithm_mature_queue_live_handoff.json",
        "v1_5_new_algorithm_mature_queue_live_handoff_v1",
        "overall_status",
        ("offline_contract_ready_live_execution_blocked",),
        (
            ("offline_handoff_contract_ready", True),
            ("production_live_gap_closed", False),
            ("legacy_default_preserved", True),
            ("queue_contract.new_algorithm_counts.co2", 47),
            ("queue_contract.new_algorithm_counts.h2o", 14),
        ),
        "Binds 47/14 profile inputs to mature runners without enabling a live queue.",
    ),
    ArtifactContract(
        "postgresql18_transaction_plan",
        "docs/v1_5_flow_contract/formal_database_import_transaction_plan/v1_5_formal_database_import_transaction_plan.json",
        "v1_5_formal_database_import_transaction_plan_v1",
        "overall_status",
        ("ready_for_postgresql18_transaction_plan_review",),
        (
            ("transaction_plan_contract_ready", True),
            ("production_transaction_package_ready", False),
            ("production_postgresql_major", 18),
            ("emits_executable_sql", False),
        ),
        "Freezes PostgreSQL 18 transaction semantics while keeping SQL and connections absent.",
    ),
    ArtifactContract(
        "postgresql18_blocked_executor",
        "docs/v1_5_flow_contract/formal_database_import_transaction_blocked_executor/v1_5_formal_database_import_transaction_blocked_executor.json",
        "v1_5_formal_database_import_transaction_blocked_executor_v1",
        "overall_status",
        ("blocked_pending_controlled_transaction_executor",),
        (
            ("blocked_executor_ready", True),
            ("transaction_plan_contract_ready", True),
            ("execution_supported", False),
            ("would_execute", False),
        ),
        "Proves that no real PostgreSQL transaction executor is available in this package.",
    ),
    ArtifactContract(
        "formal_run_status_locks",
        "docs/v1_5_flow_contract/final_acceptance_status/v1_5_formal_run_status.json",
        "v1_5_formal_run_status_v1",
        "overall_status",
        ("review_required",),
        (
            ("formal_release_allowed", False),
            ("database_import_allowed", False),
            ("can_continue_physical_flow", False),
        ),
        "Keeps current release, database, and physical continuation decisions conservative.",
    ),
)

_TOP_LEVEL_FALSE_LOCKS = (
    "opens_com_ports",
    "controls_pressure",
    "controls_water_or_gas_routes",
    "writes_coefficients",
    "writes_sn_or_device_code",
    "connects_postgresql",
    "formal_release_allowed",
    "database_import_allowed",
    "live_queue_execution_allowed",
)


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _read_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8-sig"))
    if not isinstance(payload, dict):
        raise ValueError(f"JSON object required: {path}")
    return payload


def _value(payload: Mapping[str, Any], dotted_path: str) -> Any:
    value: Any = payload
    for part in dotted_path.split("."):
        if not isinstance(value, Mapping) or part not in value:
            return None
        value = value[part]
    return value


def _artifact_schema(payload: Mapping[str, Any]) -> str:
    return str(payload.get("schema") or _value(payload, "manifest.schema") or "")


def _boundary_reasons(payload: Mapping[str, Any]) -> list[str]:
    reasons: list[str] = []
    boundary = payload.get("physical_boundaries")
    manifest = payload.get("manifest")
    boundary_payload = (
        boundary
        if isinstance(boundary, Mapping)
        else manifest
        if isinstance(manifest, Mapping)
        else payload
    )
    if boundary_payload.get("not_real_acceptance_evidence") is not True:
        reasons.append("not_real_acceptance_evidence_not_true")
    for key in _TOP_LEVEL_FALSE_LOCKS:
        if key in payload and payload.get(key) is not False:
            reasons.append(f"safety_lock_not_false:{key}")
        if key in boundary_payload and boundary_payload.get(key) is not False:
            reasons.append(f"physical_boundary_not_false:{key}")
    return reasons


def _artifact_row(repo_root: Path, contract: ArtifactContract) -> dict[str, Any]:
    path = repo_root / contract.relative_path
    reasons: list[str] = []
    payload: dict[str, Any] = {}
    if not path.is_file():
        reasons.append("artifact_missing")
    else:
        try:
            payload = _read_json(path)
        except (OSError, UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
            reasons.append(f"artifact_invalid_json:{type(exc).__name__}")
    observed_schema = _artifact_schema(payload)
    observed_status = str(_value(payload, contract.status_path) or "")
    if payload and observed_schema != contract.schema:
        reasons.append(f"schema_mismatch:{observed_schema or '<missing>'}")
    if payload and observed_status not in contract.accepted_statuses:
        reasons.append(f"status_mismatch:{observed_status or '<missing>'}")
    for dotted_path, expected in contract.expectations:
        observed = _value(payload, dotted_path)
        if observed != expected:
            reasons.append(f"expectation_mismatch:{dotted_path}:{observed!r}")
    if payload:
        reasons.extend(_boundary_reasons(payload))
    return {
        "role": contract.role,
        "relative_path": contract.relative_path,
        "exists": path.is_file(),
        "sha256": _sha256(path) if path.is_file() else "",
        "expected_schema": contract.schema,
        "observed_schema": observed_schema,
        "observed_status": observed_status,
        "status": "pass" if not reasons else "blocker",
        "reasons": sorted(set(reasons)),
        "physical_meaning": contract.physical_meaning,
    }


def build_v1_5_final_offline_acceptance_suite(
    *,
    repository_root: str | Path,
    source_origin_main_commit: str,
    test_execution: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    repo_root = Path(repository_root).resolve()
    source_commit = source_origin_main_commit.strip().lower()
    rows = [_artifact_row(repo_root, contract) for contract in ARTIFACT_CONTRACTS]
    reasons = [
        f"artifact_contract_failed:{row['role']}:{reason}"
        for row in rows
        for reason in row["reasons"]
    ]
    missing_tests = [relative for relative in SUITE_TEST_FILES if not (repo_root / relative).is_file()]
    reasons.extend(f"allowlisted_test_missing:{relative}" for relative in missing_tests)
    if not _SHA_RE.fullmatch(source_commit):
        reasons.append("source_origin_main_commit_invalid")

    execution = dict(test_execution or {})
    executed = bool(execution.get("executed"))
    passed = executed and int(execution.get("returncode", 1)) == 0
    if executed and not passed:
        reasons.append(f"offline_pytest_failed:returncode={execution.get('returncode')}")
    if executed and execution.get("command_test_files") != list(SUITE_TEST_FILES):
        reasons.append("offline_pytest_allowlist_mismatch")

    reasons = sorted(set(reasons))
    contracts_ready = not any(row["status"] != "pass" for row in rows) and not missing_tests
    offline_program_acceptance_ready = contracts_ready and passed and not reasons
    if offline_program_acceptance_ready:
        overall_status = PASS_STATUS
    elif contracts_ready and not executed and not reasons:
        overall_status = PLAN_READY_STATUS
    else:
        overall_status = REVIEW_STATUS

    return {
        "schema": SCHEMA,
        "generated_at": _now(),
        "overall_status": overall_status,
        "source_origin_main_commit": source_commit,
        "artifact_contract_count": len(rows),
        "artifact_contract_pass_count": sum(row["status"] == "pass" for row in rows),
        "allowlisted_test_file_count": len(SUITE_TEST_FILES),
        "artifact_contracts_ready": contracts_ready,
        "offline_suite_executed": executed,
        "offline_suite_tests_passed": passed,
        "offline_program_acceptance_ready": offline_program_acceptance_ready,
        "production_acceptance_ready": False,
        "production_gap_status": {
            "legacy_full_flow_orchestrator_offline_replay": "offline_program_layer_complete",
            "production_component_qc_and_0613_fit_matrix": "offline_program_layer_complete_live_evidence_pending",
            "unified_controlled_write_readback_reverify": "offline_contract_complete_authorized_write_pending",
            "new_algorithm_47_14_live_mature_queue_handoff": "offline_contract_complete_live_handoff_pending",
            "postgresql18_controlled_import": "offline_transaction_plan_complete_real_executor_pending",
            "final_offline_acceptance_suite": (
                "offline_program_layer_complete" if offline_program_acceptance_ready else "pending_or_review_required"
            ),
            "real_batch_acceptance_when_hardware_available": "hardware_deferred",
        },
        "artifact_rows": rows,
        "allowlisted_test_files": list(SUITE_TEST_FILES),
        "test_execution": execution,
        "review_reasons": reasons,
        "next_action": (
            "Keep live execution locked and prepare separately authorized real batch acceptance."
            if offline_program_acceptance_ready
            else "Resolve the listed offline contract or test failures, then rerun this suite."
        ),
        "evidence_source": "offline_repository_contracts_and_allowlisted_tests",
        "not_real_acceptance_evidence": True,
        "full_production_auto_allowed": False,
        "live_queue_execution_allowed": False,
        "formal_release_allowed": False,
        "database_import_allowed": False,
        "opens_com_ports": False,
        "controls_pressure": False,
        "controls_water_or_gas_routes": False,
        "writes_coefficients": False,
        "writes_sn_or_device_code": False,
        "connects_postgresql": False,
        "database_written": False,
    }


def render_v1_5_final_offline_acceptance_suite_markdown(model: Mapping[str, Any]) -> str:
    lines = [
        "# V1.5 final offline acceptance suite",
        "",
        f"- overall_status: `{model.get('overall_status')}`",
        f"- source_origin_main_commit: `{model.get('source_origin_main_commit')}`",
        f"- artifact_contracts_ready: `{model.get('artifact_contracts_ready')}`",
        f"- offline_suite_tests_passed: `{model.get('offline_suite_tests_passed')}`",
        f"- offline_program_acceptance_ready: `{model.get('offline_program_acceptance_ready')}`",
        f"- production_acceptance_ready: `{model.get('production_acceptance_ready')}`",
        f"- not_real_acceptance_evidence: `{model.get('not_real_acceptance_evidence')}`",
        "",
        "## Artifact contracts",
        "",
        "| Role | Status | Observed status | SHA256 |",
        "|---|---|---|---|",
    ]
    for row in model.get("artifact_rows") or []:
        lines.append(
            f"| {row.get('role')} | {row.get('status')} | {row.get('observed_status')} | {row.get('sha256')} |"
        )
    lines.extend(["", "## Production gap status", ""])
    for gap_id, status in (model.get("production_gap_status") or {}).items():
        lines.append(f"- `{gap_id}`: `{status}`")
    lines.extend(["", "## Safety locks", ""])
    for key in (
        "full_production_auto_allowed",
        "live_queue_execution_allowed",
        "formal_release_allowed",
        "database_import_allowed",
        "opens_com_ports",
        "controls_pressure",
        "controls_water_or_gas_routes",
        "writes_coefficients",
        "connects_postgresql",
        "database_written",
    ):
        lines.append(f"- `{key}`: `{model.get(key)}`")
    lines.extend(["", "## Review reasons", ""])
    reasons = list(model.get("review_reasons") or [])
    lines.extend(f"- {reason}" for reason in reasons) if reasons else lines.append("- none")
    lines.extend(["", "## Interpretation", ""])
    lines.append(
        "A pass is program-level offline evidence only. It cannot authorize COM, routes, analyzer writes, formal release, or PostgreSQL import."
    )
    return "\n".join(lines) + "\n"


def write_v1_5_final_offline_acceptance_suite(
    *,
    output_dir: str | Path,
    repository_root: str | Path,
    source_origin_main_commit: str,
    test_execution: Mapping[str, Any] | None = None,
) -> dict[str, Path]:
    destination = Path(output_dir).resolve()
    destination.mkdir(parents=True, exist_ok=True)
    model = build_v1_5_final_offline_acceptance_suite(
        repository_root=repository_root,
        source_origin_main_commit=source_origin_main_commit,
        test_execution=test_execution,
    )
    manifest = destination / "v1_5_final_offline_acceptance_suite.json"
    markdown = destination / "V1_5_FINAL_OFFLINE_ACCEPTANCE_SUITE.md"
    artifacts_csv = destination / "v1_5_final_offline_acceptance_artifacts.csv"
    tests_csv = destination / "v1_5_final_offline_acceptance_tests.csv"
    manifest.write_text(json.dumps(model, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    markdown.write_text(render_v1_5_final_offline_acceptance_suite_markdown(model), encoding="utf-8")
    with artifacts_csv.open("w", encoding="utf-8-sig", newline="") as handle:
        fieldnames = (
            "role",
            "relative_path",
            "status",
            "observed_schema",
            "observed_status",
            "sha256",
            "physical_meaning",
            "reasons",
        )
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in model["artifact_rows"]:
            writer.writerow({**{key: row.get(key, "") for key in fieldnames}, "reasons": "|".join(row["reasons"])})
    with tests_csv.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=("order", "test_file", "exists"))
        writer.writeheader()
        repo_root = Path(repository_root).resolve()
        for index, relative in enumerate(SUITE_TEST_FILES, start=1):
            writer.writerow({"order": index, "test_file": relative, "exists": (repo_root / relative).is_file()})
    return {
        "manifest": manifest,
        "markdown": markdown,
        "artifacts_csv": artifacts_csv,
        "tests_csv": tests_csv,
    }
