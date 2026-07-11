"""Fail-closed fit-input traceability gate for final V1.5 SENCO writers."""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Dict, List, Mapping, Sequence, Tuple

from .v1_5_artifact_hash_binding import validate_artifact_hash_manifest
from .v1_5_senco_artifact_authorization import (
    SCHEMA as ARTIFACT_AUTHORIZATION_SCHEMA,
    validate_senco_artifact_authorization,
)


GLOBAL_CHECK = "fit_input_traceability_required_before_final_senco_review"
META_FILENAME = "main_senco_write_precheck_meta.json"
CHECKS_FILENAME = "candidate_write_review_checks.csv"
SUMMARY_FILENAME = "main_senco_write_precheck_summary.csv"
HASH_MANIFEST_FILENAME = "main_senco_artifact_hash_manifest.json"
ARTIFACT_AUTHORIZATION_FILENAME = "main_senco_artifact_authorization.json"


def _device_id(value: Any) -> str:
    text = str(value or "").strip()
    return text.zfill(3) if text.isdigit() else text


def _read_csv(path: Path) -> List[Dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def _truthy(value: Any) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "y", "pass"}


def validate_final_senco_prewrite_gate(
    precheck_dir: str | Path | None,
    *,
    component: str,
    device_ids: Sequence[str],
    reviewer: str,
    approver: str,
    writer_scope: str,
    required_artifact_paths: Mapping[str, str | Path] | None = None,
) -> Tuple[bool, List[str], Mapping[str, Any]]:
    component_key = str(component or "").strip().lower()
    reasons: List[str] = []
    if component_key not in {"co2", "h2o"}:
        return False, [f"unsupported_component:{component_key or 'missing'}"], {}
    if not str(precheck_dir or "").strip():
        return False, ["main_senco_precheck_dir_missing"], {}

    root = Path(precheck_dir).resolve()
    meta_path = root / META_FILENAME
    checks_path = root / CHECKS_FILENAME
    summary_path = root / SUMMARY_FILENAME
    hash_manifest_path = root / HASH_MANIFEST_FILENAME
    artifact_authorization_path = root / ARTIFACT_AUTHORIZATION_FILENAME
    for path, label in (
        (meta_path, "main_senco_precheck_meta_missing"),
        (checks_path, "candidate_write_review_checks_missing"),
        (summary_path, "main_senco_precheck_summary_missing"),
        (hash_manifest_path, "main_senco_artifact_hash_manifest_missing"),
        (artifact_authorization_path, "main_senco_artifact_authorization_missing"),
    ):
        if not path.is_file():
            reasons.append(label)
    if reasons:
        return False, reasons, {"precheck_dir": str(root)}

    try:
        meta = json.loads(meta_path.read_text(encoding="utf-8"))
    except Exception as exc:
        return False, [f"main_senco_precheck_meta_invalid:{type(exc).__name__}"], {"precheck_dir": str(root)}
    if not isinstance(meta, Mapping):
        return False, ["main_senco_precheck_meta_not_object"], {"precheck_dir": str(root)}

    boundary_contract = {
        "no_write": True,
        "opens_com": False,
        "writes_senco": False,
        "controls_routes": False,
    }
    for field, expected in boundary_contract.items():
        if field not in meta:
            reasons.append(f"main_senco_precheck_boundary_missing:{field}")
            continue
        actual = _truthy(meta.get(field))
        if actual != expected:
            reasons.append(f"main_senco_precheck_boundary_mismatch:{field}")
    if not _truthy(meta.get("fit_input_traceability_required")):
        reasons.append("fit_input_traceability_not_required_by_precheck")
    if not _truthy(meta.get("artifact_hash_manifest_required")):
        reasons.append("artifact_hash_manifest_not_required_by_precheck")
    declared_manifest = str(meta.get("artifact_hash_manifest_path") or "").strip()
    if not declared_manifest or Path(declared_manifest).resolve() != hash_manifest_path:
        reasons.append("artifact_hash_manifest_path_mismatch_with_precheck_meta")
    if str(meta.get("artifact_hash_algorithm") or "").strip().lower() != "sha256":
        reasons.append("artifact_hash_algorithm_not_sha256")
    if not _truthy(meta.get("artifact_authorization_required")):
        reasons.append("artifact_authorization_not_required_by_precheck")
    declared_authorization = str(meta.get("artifact_authorization_path") or "").strip()
    if not declared_authorization or Path(declared_authorization).resolve() != artifact_authorization_path:
        reasons.append("artifact_authorization_path_mismatch_with_precheck_meta")
    if str(meta.get("artifact_authorization_schema") or "").strip() != ARTIFACT_AUTHORIZATION_SCHEMA:
        reasons.append("artifact_authorization_schema_mismatch_with_precheck_meta")
    package_traceability_status = str(meta.get("fit_input_traceability_status") or "").strip().lower()
    if package_traceability_status not in {"pass", "blocked"}:
        reasons.append(f"fit_input_traceability_package_status_invalid:{package_traceability_status or 'missing'}")

    checks = {
        str(row.get("check") or "").strip(): str(row.get("status") or "").strip().lower()
        for row in _read_csv(checks_path)
    }
    package_check_status = checks.get(GLOBAL_CHECK)
    if package_check_status not in {"pass", "block_write"}:
        reasons.append(f"{GLOBAL_CHECK}:{package_check_status or 'missing'}")

    summary_by_device = {
        _device_id(row.get("analyzer_device_id") or row.get("device_id")): row
        for row in _read_csv(summary_path)
    }
    normalized_devices = list(dict.fromkeys(_device_id(value) for value in device_ids if _device_id(value)))
    if not normalized_devices:
        reasons.append("selected_device_ids_missing")
    for device_id in normalized_devices:
        check_name = f"fit_input_traceability_bound:{component_key}:{device_id}"
        if checks.get(check_name) != "pass":
            reasons.append(f"{check_name}:{checks.get(check_name) or 'missing'}")
        summary = summary_by_device.get(device_id)
        if not summary:
            reasons.append(f"main_senco_precheck_summary_device_missing:{device_id}")
            continue
        status = str(summary.get(f"{component_key}_fit_input_traceability_status") or "").strip().lower()
        if status != "pass":
            reasons.append(f"{component_key}_fit_input_traceability_status:{device_id}:{status or 'missing'}")
        blockers = str(summary.get(f"{component_key}_fit_input_traceability_blockers") or "").strip()
        if blockers:
            reasons.append(f"{component_key}_fit_input_traceability_blockers:{device_id}:{blockers}")

    required_hash_roles = [
        f"{component_key}_fit_input_quality_summary",
        f"{component_key}_fit_input_quality_devices",
        f"{component_key}_candidate_run_summary",
        f"{component_key}_candidate_policy_summary",
        f"{component_key}_model_selection_summary",
        "precheck_summary",
        "precheck_checks",
    ]
    expected_hash_paths: Dict[str, Path] = {
        "precheck_summary": summary_path,
        "precheck_checks": checks_path,
    }
    if component_key == "co2":
        required_hash_roles.append("precheck_co2_mapping")
        expected_hash_paths["precheck_co2_mapping"] = root / "candidate_senco_mapping_review.csv"
    else:
        required_hash_roles.extend(
            ["precheck_h2o_payload", "precheck_h2o_policy", "precheck_h2o_diagnostics"]
        )
        expected_hash_paths.update(
            {
                "precheck_h2o_payload": root / "h2o_senco24_payload_preview.csv",
                "precheck_h2o_policy": root / "h2o_senco24_device_policy.csv",
                "precheck_h2o_diagnostics": root / "h2o_senco24_output_diagnostics.csv",
            }
        )
    for role, source in (required_artifact_paths or {}).items():
        normalized_role = str(role).strip()
        if not normalized_role:
            reasons.append("artifact_hash_required_role_name_missing")
            continue
        required_hash_roles.append(normalized_role)
        expected_hash_paths[normalized_role] = Path(source).resolve()
    hash_ok, hash_reasons, hash_detail = validate_artifact_hash_manifest(
        hash_manifest_path,
        required_roles=required_hash_roles,
        expected_paths=expected_hash_paths,
    )
    if not hash_ok:
        reasons.extend(hash_reasons)
    authorization_ok, authorization_reasons, authorization_detail = validate_senco_artifact_authorization(
        artifact_authorization_path,
        manifest_path=hash_manifest_path,
        reviewer=reviewer,
        approver=approver,
        writer_scope=writer_scope,
        device_ids=normalized_devices,
    )
    if not authorization_ok:
        reasons.extend(authorization_reasons)

    detail = {
        "precheck_dir": str(root),
        "meta_path": str(meta_path),
        "checks_path": str(checks_path),
        "summary_path": str(summary_path),
        "hash_manifest_path": str(hash_manifest_path),
        "artifact_hash_status": str(hash_detail.get("status") or "blocked"),
        "artifact_hash_count": int(hash_detail.get("artifact_count") or 0),
        "artifact_authorization_path": str(artifact_authorization_path),
        "artifact_authorization_status": str(authorization_detail.get("status") or "blocked"),
        "artifact_authorization_id": str(authorization_detail.get("authorization_id") or ""),
        "artifact_authorization_writer_scope": str(authorization_detail.get("writer_scope") or ""),
        "artifact_authorized_device_ids": list(authorization_detail.get("authorized_device_ids") or ()),
        "component": component_key,
        "device_ids": normalized_devices,
        "package_fit_input_traceability_status": package_traceability_status,
        "package_fit_input_traceability_check_status": package_check_status,
        "fit_input_traceability_status": "pass" if not reasons else "blocked",
    }
    return not reasons, list(dict.fromkeys(reasons)), detail
