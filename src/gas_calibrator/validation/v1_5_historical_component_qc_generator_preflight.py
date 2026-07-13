"""Preflight historical V1.5 component-QC inputs without generating QC."""

from __future__ import annotations

import csv
import hashlib
import json
from pathlib import Path
from typing import Any, Mapping, Sequence

from .v1_5_component_qc_generator_contract import (
    validate_v1_5_component_qc_generator_contract,
)
from .v1_5_component_qc_reference_evaluator import SCHEMA as REFERENCE_SCHEMA
from .v1_5_p2_qc_derivation_design import SCHEMA as P2_SCHEMA


SCHEMA = "v1_5_historical_component_qc_generator_preflight_v1"
EXPECTED_P2_STATUS = "blocked_missing_reviewed_qc_generator_contract"
REVIEW_OUTPUT_SUFFIX = (
    "docs",
    "v1_5_flow_contract",
    "historical_component_qc_generator_preflight",
)

_P2_FALSE_LOCKS = (
    "opens_com_ports",
    "controls_pressure",
    "controls_water_or_gas_routes",
    "writes_coefficients",
    "writes_sn_or_device_code",
    "connects_postgresql",
    "reviewed_generator_available",
    "qc_derivation_execution_allowed",
    "generated_qc_write_allowed",
    "cross_run_qc_direct_bind_allowed",
    "historical_fit_allowed",
    "formal_release_allowed",
    "database_import_allowed",
)
_REFERENCE_FALSE_LOCKS = (
    "production_component_qc_generator_available",
    "historical_component_qc_generation_allowed",
    "historical_component_qc_write_allowed",
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
_COMMON_ROLES = {"samples", "frame_qc", "runtime_config", "sidecar"}
_ROUTE_ROLES = {
    "co2": _COMMON_ROLES | {"route_timing"},
    "h2o": _COMMON_ROLES | {"hgen_flow_set", "humidity_reference_review", "point_timing_summary"},
}


def _read_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8-sig") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise ValueError(f"Expected JSON object: {path}")
    return payload


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


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


def _global_reasons(
    p2: Mapping[str, Any], contract: Mapping[str, Any], reference: Mapping[str, Any]
) -> list[str]:
    reasons: list[str] = []
    if p2.get("schema") != P2_SCHEMA:
        reasons.append("p2_design_schema_mismatch")
    if p2.get("overall_status") != EXPECTED_P2_STATUS:
        reasons.append("p2_design_status_unexpected")
    candidates = p2.get("candidates")
    if not isinstance(candidates, list):
        reasons.append("p2_candidates_must_be_list")
    elif not candidates:
        reasons.append("p2_candidates_empty")
    elif p2.get("candidate_count") != len(candidates):
        reasons.append("p2_candidate_count_mismatch")
    for key in _P2_FALSE_LOCKS:
        if p2.get(key) is not False:
            reasons.append(f"p2_lock_not_false:{key}")
    if p2.get("not_real_acceptance_evidence") is not True:
        reasons.append("p2_real_acceptance_lock_missing")

    reasons.extend(
        f"contract:{reason}" for reason in validate_v1_5_component_qc_generator_contract(contract)
    )
    if reference.get("schema") != REFERENCE_SCHEMA:
        reasons.append("reference_evaluator_schema_mismatch")
    if reference.get("overall_status") != "synthetic_reference_evaluation_complete":
        reasons.append("reference_evaluator_status_invalid")
    if reference.get("evidence_source") != "simulated":
        reasons.append("reference_evaluator_must_be_simulated")
    if reference.get("not_real_acceptance_evidence") is not True:
        reasons.append("reference_evaluator_real_acceptance_lock_missing")
    if reference.get("contract_sha256") != _sha256_value(contract):
        reasons.append("reference_evaluator_contract_hash_mismatch")
    locks = reference.get("locks") or {}
    if locks.get("reference_evaluator_available") is not True:
        reasons.append("reference_evaluator_available_flag_missing")
    for key in _REFERENCE_FALSE_LOCKS:
        if locks.get(key) is not False:
            reasons.append(f"reference_lock_not_false:{key}")
    return sorted(set(reasons))


def _artifact_index(
    rows: Sequence[Mapping[str, Any]],
) -> tuple[dict[str, dict[str, Mapping[str, Any]]], list[str]]:
    index: dict[str, dict[str, Mapping[str, Any]]] = {}
    reasons: list[str] = []
    required_columns = {"point_dir", "artifact_role", "artifact_path", "size_bytes", "sha256"}
    for row_index, row in enumerate(rows, start=1):
        if not required_columns.issubset(row):
            reasons.append(f"artifact_inventory_columns_missing:row_{row_index}")
            continue
        point_key = _path_key(row.get("point_dir"))
        role = str(row.get("artifact_role") or "").strip()
        if not point_key or not role:
            reasons.append(f"artifact_inventory_identity_missing:row_{row_index}")
            continue
        role_map = index.setdefault(point_key, {})
        if role in role_map:
            reasons.append(f"duplicate_artifact_role:{point_key}:{role}")
        else:
            role_map[role] = row
    return index, sorted(set(reasons))


def _candidate_result(
    candidate: Mapping[str, Any], artifact_rows: Mapping[str, Mapping[str, Any]], output_filename: str
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    point_dir = Path(str(candidate.get("point_dir") or "")).resolve()
    route_kind = str(candidate.get("route_kind") or "").strip().lower()
    reasons: list[str] = []
    artifact_checks: list[dict[str, Any]] = []
    if route_kind not in _ROUTE_ROLES:
        reasons.append("route_kind_invalid")
        required_roles: set[str] = set()
    else:
        required_roles = _ROUTE_ROLES[route_kind]
    if not str(candidate.get("point_dir") or "").strip():
        reasons.append("candidate_point_dir_missing")
    if candidate.get("input_complete") is not True:
        reasons.append("p2_candidate_input_not_complete")
    if candidate.get("derivation_design_review_candidate") is not True:
        reasons.append("p2_derivation_design_candidate_flag_missing")
    if candidate.get("qc_derivation_execution_allowed") is not False:
        reasons.append("p2_candidate_execution_lock_not_false")
    if candidate.get("generated_qc_write_allowed") is not False:
        reasons.append("p2_candidate_write_lock_not_false")
    if candidate.get("formal_fit_allowed") is not False:
        reasons.append("p2_candidate_fit_lock_not_false")

    missing_roles = sorted(required_roles.difference(artifact_rows))
    reasons.extend(f"artifact_role_missing:{role}" for role in missing_roles)
    for role in sorted(required_roles.intersection(artifact_rows)):
        row = artifact_rows[role]
        path = Path(str(row.get("artifact_path") or "")).resolve()
        row_reasons: list[str] = []
        if _path_key(row.get("point_dir")) != _path_key(point_dir):
            row_reasons.append("artifact_point_dir_mismatch")
        try:
            path.relative_to(point_dir)
        except ValueError:
            row_reasons.append("artifact_path_outside_point_dir")
        if not path.is_file():
            row_reasons.append("artifact_file_missing")
            actual_size: int | None = None
            actual_sha: str | None = None
        else:
            actual_size = path.stat().st_size
            actual_sha = _sha256_file(path)
            if actual_size <= 0:
                row_reasons.append("artifact_file_empty")
            try:
                expected_size = int(str(row.get("size_bytes") or ""))
            except ValueError:
                expected_size = -1
            if actual_size != expected_size:
                row_reasons.append("artifact_size_mismatch")
            if actual_sha != str(row.get("sha256") or "").lower():
                row_reasons.append("artifact_sha256_mismatch")
        reasons.extend(f"{role}:{reason}" for reason in row_reasons)
        artifact_checks.append(
            {
                "role": role,
                "path": str(path),
                "recorded_sha256": row.get("sha256"),
                "actual_sha256": actual_sha,
                "recorded_size_bytes": row.get("size_bytes"),
                "actual_size_bytes": actual_size,
                "status": "pass" if not row_reasons else "blocked",
                "blocker_codes": sorted(set(row_reasons)),
            }
        )

    target = point_dir / output_filename
    if target.exists():
        reasons.append("component_qc_output_target_already_exists")
    reasons = sorted(set(reasons))
    return (
        {
            "source_role": candidate.get("source_role"),
            "route_kind": route_kind,
            "point_name": candidate.get("point_name"),
            "point_dir": str(point_dir),
            "preflight_status": "input_packet_ready_for_manual_review" if not reasons else "blocked",
            "preflight_ready": not reasons,
            "blocker_codes": reasons,
            "manual_gate_review_required": candidate.get("manual_gate_review_required") is True,
            "sample_alignment_false_count": candidate.get("sample_alignment_false_count"),
            "point_quality_blocked_count": candidate.get("point_quality_blocked_count"),
            "purge_below_declared_minimum": candidate.get("purge_below_declared_minimum"),
            "planned_output_path": str(target),
            "planned_output_exists": target.exists(),
            "historical_component_qc_generation_allowed": False,
            "historical_component_qc_write_allowed": False,
            "formal_fit_allowed": False,
        },
        artifact_checks,
    )


def build_v1_5_historical_component_qc_generator_preflight(
    *,
    p2_design_json_path: str | Path,
    p2_artifact_inventory_csv_path: str | Path,
    contract_json_path: str | Path,
    reference_evaluation_json_path: str | Path,
) -> dict[str, Any]:
    """Revalidate P2 source hashes and overwrite boundaries without generating QC."""

    p2_path = Path(p2_design_json_path).resolve()
    inventory_path = Path(p2_artifact_inventory_csv_path).resolve()
    contract_path = Path(contract_json_path).resolve()
    reference_path = Path(reference_evaluation_json_path).resolve()
    p2 = _read_json(p2_path)
    contract = _read_json(contract_path)
    reference = _read_json(reference_path)
    inventory_rows = _read_csv(inventory_path)
    global_reasons = _global_reasons(p2, contract, reference)
    declared_inventory = str(p2.get("artifact_inventory_csv") or "").strip()
    if not declared_inventory:
        global_reasons.append("p2_artifact_inventory_declaration_missing")
    else:
        expected_inventory_path = (p2_path.parent / declared_inventory).resolve()
        if expected_inventory_path != inventory_path:
            global_reasons.append("p2_artifact_inventory_path_mismatch")
    artifact_index, inventory_reasons = _artifact_index(inventory_rows)
    global_reasons.extend(inventory_reasons)

    output_filename = str((contract.get("output_contract") or {}).get("filename") or "")
    if output_filename != "formal_open_flow_data_quality_by_analyzer.csv":
        global_reasons.append("component_qc_output_filename_invalid")
    candidates: list[dict[str, Any]] = []
    artifact_checks: list[dict[str, Any]] = []
    seen_candidate_paths: set[str] = set()
    raw_candidates = p2.get("candidates")
    candidate_rows = raw_candidates if isinstance(raw_candidates, list) else []
    for row_index, candidate in enumerate(candidate_rows, start=1):
        if not isinstance(candidate, Mapping):
            global_reasons.append(f"p2_candidate_row_not_object:row_{row_index}")
            continue
        point_key = _path_key(candidate.get("point_dir"))
        if point_key in seen_candidate_paths:
            global_reasons.append(f"duplicate_p2_candidate_point_dir:{point_key}")
        seen_candidate_paths.add(point_key)
        result, checks = _candidate_result(
            candidate,
            artifact_index.get(point_key, {}),
            output_filename or "formal_open_flow_data_quality_by_analyzer.csv",
        )
        candidates.append(result)
        artifact_checks.extend({"point_dir": result["point_dir"], **row} for row in checks)

    global_reasons = sorted(set(global_reasons))
    ready_count = sum(row["preflight_ready"] is True for row in candidates)
    candidate_blocker_count = sum(bool(row["blocker_codes"]) for row in candidates)
    if global_reasons:
        overall_status = "blocked_historical_component_qc_generator_preflight"
    elif candidate_blocker_count:
        overall_status = "review_required_partial_historical_component_qc_preflight"
    else:
        overall_status = "ready_for_historical_component_qc_generator_preflight_manual_review"
    return {
        "schema": SCHEMA,
        "overall_status": overall_status,
        "production_state": "preflight_only_generator_and_writer_blocked",
        "global_blocker_codes": global_reasons,
        "candidate_count": len(candidates),
        "candidate_preflight_ready_count": ready_count,
        "candidate_blocked_count": candidate_blocker_count,
        "manual_gate_review_count": sum(
            row["manual_gate_review_required"] is True for row in candidates
        ),
        "artifact_check_count": len(artifact_checks),
        "artifact_check_blocked_count": sum(row["status"] == "blocked" for row in artifact_checks),
        "source_paths": {
            "p2_design_json": str(p2_path),
            "p2_design_sha256": _sha256_file(p2_path),
            "p2_artifact_inventory_csv": str(inventory_path),
            "p2_artifact_inventory_sha256": _sha256_file(inventory_path),
            "contract_json": str(contract_path),
            "contract_file_sha256": _sha256_file(contract_path),
            "contract_semantic_sha256": _sha256_value(contract),
            "reference_evaluation_json": str(reference_path),
            "reference_evaluation_sha256": _sha256_file(reference_path),
        },
        "candidates": candidates,
        "artifact_checks": artifact_checks,
        "locks": {
            "preflight_available": True,
            "production_component_qc_generator_available": False,
            "historical_component_qc_generation_allowed": False,
            "historical_component_qc_write_allowed": False,
            "component_qc_backfill_allowed": False,
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
    }


def _write_csv(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    fields: list[str] = []
    for row in rows:
        for key in row:
            if key not in fields:
                fields.append(key)
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


def write_v1_5_historical_component_qc_generator_preflight(
    model: Mapping[str, Any], output_dir: str | Path
) -> dict[str, Path]:
    """Write only dedicated review artifacts, never historical point outputs."""

    out = Path(output_dir).resolve()
    suffix = tuple(part.lower() for part in out.parts[-len(REVIEW_OUTPUT_SUFFIX) :])
    if suffix != REVIEW_OUTPUT_SUFFIX:
        raise ValueError("output_dir_must_be_historical_component_qc_preflight_review_directory")
    out.mkdir(parents=True, exist_ok=True)
    outputs = {
        "json": out / "v1_5_historical_component_qc_generator_preflight.json",
        "candidates_csv": out / "v1_5_historical_component_qc_generator_preflight_candidates.csv",
        "artifact_checks_csv": out
        / "v1_5_historical_component_qc_generator_preflight_artifact_checks.csv",
        "markdown": out / "V1_5_HISTORICAL_COMPONENT_QC_GENERATOR_PREFLIGHT.md",
    }
    outputs["json"].write_text(
        json.dumps(dict(model), ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    _write_csv(outputs["candidates_csv"], model.get("candidates") or [])
    _write_csv(outputs["artifact_checks_csv"], model.get("artifact_checks") or [])
    lines = [
        "# V1.5 Historical Component-QC Generator Preflight",
        "",
        f"- overall_status: `{model.get('overall_status')}`",
        f"- candidate_count: `{model.get('candidate_count')}`",
        f"- candidate_preflight_ready_count: `{model.get('candidate_preflight_ready_count')}`",
        f"- candidate_blocked_count: `{model.get('candidate_blocked_count')}`",
        f"- manual_gate_review_count: `{model.get('manual_gate_review_count')}`",
        f"- artifact_check_blocked_count: `{model.get('artifact_check_blocked_count')}`",
        "- evidence_source: `historical_replay`",
        "- not_real_acceptance_evidence: `true`",
        "- historical_component_qc_generation_allowed: `false`",
        "- historical_component_qc_write_allowed: `false`",
        "- historical_fit_allowed: `false`",
        "- opens_com_ports: `false`",
        "",
        "This preflight revalidates immutable source packets and overwrite boundaries. It does not derive grades or write the planned component-QC CSV.",
    ]
    outputs["markdown"].write_text("\n".join(lines) + "\n", encoding="utf-8")
    return outputs


__all__ = [
    "REVIEW_OUTPUT_SUFFIX",
    "SCHEMA",
    "build_v1_5_historical_component_qc_generator_preflight",
    "write_v1_5_historical_component_qc_generator_preflight",
]
