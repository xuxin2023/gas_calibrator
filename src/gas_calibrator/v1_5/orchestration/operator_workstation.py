"""Thin V1.5 dry-run and response-only simulation orchestration seam.

The workstation deliberately calls the reviewed 45/13 queue runners instead
of reproducing their physical logic.  It is an integration seam for a future
operator UI, not a replacement calibration kernel.
"""

from __future__ import annotations

import hashlib
import json
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping

from ...config import load_config
from ...tools.run_v1_5_formal_co2_open_flow_queue import (
    _load_queue_rows as _load_co2_queue_rows,
)
from ...tools.run_v1_5_formal_co2_open_flow_queue import (
    _parse_text_filter as _parse_co2_roles,
)
from ...tools.run_v1_5_formal_co2_open_flow_queue import (
    _select_queue_rows as _select_co2_queue_rows,
)
from ...tools.run_v1_5_formal_co2_open_flow_queue import main as _run_co2_queue
from ...tools.run_v1_5_formal_h2o_open_flow_queue import (
    _load_queue_rows as _load_h2o_queue_rows,
)
from ...tools.run_v1_5_formal_h2o_open_flow_queue import (
    _select_queue_rows as _select_h2o_queue_rows,
)
from ...tools.run_v1_5_formal_h2o_open_flow_queue import main as _run_h2o_queue
from ...tools.run_v1_5_formal_open_flow_sampling import (
    V1_5_ENGINEERING_PROBE_CONFIRMATION_TEXT,
    V1_5_OPERATOR_CONFIRMATION_RECORD_FILENAME,
)
from .serial_port_binding import (
    REFERENCE_DEVICE_KEYS,
    allowed_bank_shift_map,
    normalize_com_port,
)


SCHEMA = "v1_5_operator_workstation_dry_run_v1"
CONTROLLED_MATURE_ROUTE_EXECUTION_SCHEMA = (
    "v1_5_controlled_mature_route_execution_v1"
)
CONTROLLED_MATURE_ROUTE_PREFLIGHT_RECEIPT_SCHEMA = (
    "v1_5_controlled_mature_route_preflight_receipt_v1"
)
STARTUP_RECEIPT_SCHEMA = "v1_5_operator_workstation_startup_receipt_v1"
ARCHIVE_AUTHORITY_CONFIRMATION_RECEIPT_SCHEMA = (
    "v1_5_archive_authority_confirmation_receipt_v1"
)
V1_5_ARCHIVE_AUTHORITY_CONFIRMATION_TEXT = (
    "I CONFIRM THE SELECTED V1.5 FORMAL ARCHIVE MATCHES THE CURRENT BATCH "
    "AND DEVICE SET; THIS RECEIPT DOES NOT AUTHORIZE COM ACCESS, DEVICE "
    "WRITES, COEFFICIENT WRITES, OR FORMAL CERTIFICATE ISSUE."
)
V1_5_CONTROLLED_MATURE_ROUTE_AUTHORIZATION_TEXT = (
    "I AUTHORIZE ONE V1.5 MATURE ROUTE NO-WRITE ENGINEERING PROBE"
)
PRODUCT_NAME = "V1.5 气体分析仪校准工作站"
CALIBRATION_KERNEL = "v1_5_legacy_ratio_0613_0620_0621"
PROFILE_ID = "legacy_ratio_production"
CO2_RUNNER = "gas_calibrator.tools.run_v1_5_formal_co2_open_flow_queue"
H2O_RUNNER = "gas_calibrator.tools.run_v1_5_formal_h2o_open_flow_queue"
EXPECTED_POINT_COUNTS = {"co2": 45, "h2o": 13}
RESPONSE_ONLY_SCOPE = "response_only"
RESPONSE_ONLY_ALLOWED_ACTIONS = (
    "passive_listen",
    "identity_query",
    "status_query",
)
_RUN_ID_PATTERN = re.compile(r"^[A-Za-z0-9_.-]+$")
_OPERATOR_CONFIRMATION_PLACEHOLDER = "<OPERATOR_CONFIRMATION_REQUIRED_AT_EXECUTION>"
_FORMAL_ARCHIVE_SCHEMA = "v1_5_formal_archive_closure_v1"
_FORMAL_RUN_STATUS_SCHEMA = "v1_5_formal_run_status_v1"
_EVIDENCE_BUNDLE_SCHEMA = "v1_5_evidence_registry"
_EVIDENCE_BUNDLE_SCHEMA_VERSION = "001"
_DECISION_AUTHORITY_ROLES = {
    "formal_run_status": "formal_run_status_json",
    "report_model": "report_report_model",
    "evidence_bundle": "evidence_bundle",
}
_AUTHORITY_IDENTITY_CHECK_KEYS = (
    "run_id_match",
    "run_db_id_match",
    "run_dir_match",
    "device_ids_match",
    "runtime_config_sha256_match",
    "artifact_cross_links_match",
    "report_formal_status_match",
)
_RESPONSE_ONLY_REQUEST_FIELDS = frozenset(
    {
        "request_id",
        "device_key",
        "port",
        "action",
        "expected_protocol_id",
    }
)
_DECISION_REASON_ZH = {
    "startup_gate_blocked": "工作站启动门禁未通过",
    "selected_scope_missing": "受控执行范围不存在",
    "simulation_executor_unavailable": "所选范围没有离线模拟执行器",
    "startup_and_scope_ready": "启动配置、45/13 队列与模拟范围均已就绪",
    "real_scope_not_unlocked": "真实执行范围尚未显式解锁",
    "formal_run_status_missing": "尚未载入正式运行状态权威",
    "physical_flow_not_ready": "正式运行状态不允许继续物理流程",
    "controlled_write_authorization_not_ready": "受控系数写入授权尚未就绪",
    "formal_release_not_allowed": "正式运行状态尚未允许正式放行",
    "report_release_decision_missing": "尚未载入正式报告放行判定",
    "formal_report_issue_not_allowed": "正式报告判定尚未允许签发",
    "all_required_gates_passed": "所有必需权威门禁均已通过",
}
_DECISION_REASON_EN = {
    "startup_gate_blocked": "workstation startup gate is blocked",
    "selected_scope_missing": "controlled execution scope is missing",
    "simulation_executor_unavailable": "selected scope has no simulation executor",
    "startup_and_scope_ready": "startup inputs and simulation scope are ready",
    "real_scope_not_unlocked": "real execution scope is not explicitly unlocked",
    "formal_run_status_missing": "formal run status authority is not loaded",
    "physical_flow_not_ready": "formal run status does not allow physical flow",
    "controlled_write_authorization_not_ready": "controlled coefficient write is not authorized",
    "formal_release_not_allowed": "formal run status does not allow release",
    "report_release_decision_missing": "formal report release decision is not loaded",
    "formal_report_issue_not_allowed": "formal report decision does not allow issue",
    "all_required_gates_passed": "all required authority gates passed",
}


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _path_text(path: Path) -> str:
    return str(path.resolve())


def _sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest() if path.is_file() else ""


def _normalize_device_ids(value: str | Iterable[Any] | None) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        values = re.split(r"[,;\s]+", value)
    else:
        values = [str(item) for item in value]
    return sorted(
        {
            str(item).strip().upper()
            for item in values
            if str(item).strip()
        }
    )


def _path_identity(value: Any) -> str:
    text = str(value or "").strip()
    return str(Path(text).resolve()).casefold() if text else ""


def _decision_authority_identity(
    *,
    index_payload: Mapping[str, Any],
    source_payloads: Mapping[str, Mapping[str, Any]],
    artifacts: Mapping[str, Mapping[str, Any]],
    expected_run_id: str | None,
    expected_device_ids: str | Iterable[Any] | None,
    expected_runtime_config_sha256: str | None,
) -> tuple[dict[str, Any], list[str]]:
    blockers: list[str] = []
    expected_run = str(expected_run_id or "").strip()
    expected_devices = _normalize_device_ids(expected_device_ids)
    expected_config_sha = str(expected_runtime_config_sha256 or "").strip().lower()
    if not expected_run:
        blockers.append("decision_authority_expected_run_id_missing")
    if not expected_devices:
        blockers.append("decision_authority_expected_device_ids_missing")
    if not re.fullmatch(r"[0-9a-f]{64}", expected_config_sha):
        blockers.append("decision_authority_expected_runtime_config_sha256_invalid")

    formal = dict(source_payloads.get("formal_run_status") or {})
    report = dict(source_payloads.get("report_model") or {})
    bundle = dict(source_payloads.get("evidence_bundle") or {})
    tables = bundle.get("tables")
    run_rows = list(tables.get("runs") or []) if isinstance(tables, Mapping) else []
    run_row = dict(run_rows[0]) if len(run_rows) == 1 and isinstance(run_rows[0], Mapping) else {}

    observed_run_ids = {
        "archive_index": str(index_payload.get("run_id") or "").strip(),
        "report_model": str(report.get("run_id") or "").strip(),
        "evidence_bundle": str(bundle.get("run_id") or "").strip(),
        "evidence_bundle_run_row": str(run_row.get("run_id") or "").strip(),
    }
    if not all(observed_run_ids.values()):
        blockers.append("decision_authority_run_id_source_missing")
    elif len(set(observed_run_ids.values())) != 1:
        blockers.append("decision_authority_run_id_source_mismatch")
    elif expected_run and observed_run_ids["archive_index"] != expected_run:
        blockers.append("decision_authority_expected_run_id_mismatch")

    observed_run_db_ids = {
        "archive_index": str(index_payload.get("run_db_id") or "").strip(),
        "report_model": str(report.get("run_db_id") or "").strip(),
        "evidence_bundle": str(bundle.get("run_db_id") or "").strip(),
        "evidence_bundle_run_row": str(run_row.get("id") or "").strip(),
    }
    if not all(observed_run_db_ids.values()):
        blockers.append("decision_authority_run_db_id_source_missing")
    elif len(set(observed_run_db_ids.values())) != 1:
        blockers.append("decision_authority_run_db_id_source_mismatch")

    observed_run_dirs = {
        "archive_index": _path_identity(index_payload.get("run_dir")),
        "formal_run_status": _path_identity(formal.get("run_dir")),
        "evidence_bundle_run_row": _path_identity(run_row.get("run_dir")),
    }
    if not all(observed_run_dirs.values()):
        blockers.append("decision_authority_run_dir_source_missing")
    elif len(set(observed_run_dirs.values())) != 1:
        blockers.append("decision_authority_run_dir_source_mismatch")

    formal_summary = index_payload.get("formal_run_status")
    report_paths = index_payload.get("reports")
    formal_index_path = (
        str(formal_summary.get("json_path") or "")
        if isinstance(formal_summary, Mapping)
        else ""
    )
    report_index_path = (
        str(report_paths.get("report_model") or "")
        if isinstance(report_paths, Mapping)
        else ""
    )
    artifact_paths = {
        "formal_run_status": str(
            (artifacts.get("formal_run_status") or {}).get("path") or ""
        ),
        "report_model": str(
            (artifacts.get("report_model") or {}).get("path") or ""
        ),
    }
    artifact_cross_links_match = bool(
        _path_identity(formal_index_path)
        and _path_identity(report_index_path)
        and _path_identity(formal_index_path)
        == _path_identity(artifact_paths["formal_run_status"])
        and _path_identity(report_index_path)
        == _path_identity(artifact_paths["report_model"])
    )
    if not artifact_cross_links_match:
        blockers.append("decision_authority_artifact_cross_link_mismatch")

    embedded_formal = report.get("formal_run_status")
    formal_snapshot_match = False
    if isinstance(embedded_formal, Mapping) and embedded_formal.get("available") is True:
        formal_snapshot_match = all(
            embedded_formal.get(key) == formal.get(key)
            for key in (
                "overall_status",
                "current_stage",
                "formal_release_allowed",
                "can_continue_physical_flow",
            )
        )
    if not formal_snapshot_match:
        blockers.append("decision_authority_report_formal_status_mismatch")

    archive_identity = index_payload.get("identity_getco_traceability")
    archive_devices = _normalize_device_ids(
        archive_identity.get("analyzer_device_ids")
        if isinstance(archive_identity, Mapping)
        else None
    )
    formal_authorization = formal.get("senco_artifact_authorization")
    formal_devices = _normalize_device_ids(
        formal_authorization.get("authorized_device_ids")
        if isinstance(formal_authorization, Mapping)
        else None
    )
    report_devices = _normalize_device_ids(
        row.get("analyzer_device_id")
        for row in list(report.get("per_device_certificate_readiness") or [])
        if isinstance(row, Mapping)
    )
    run_metadata = run_row.get("metadata")
    bundle_devices = _normalize_device_ids(
        run_metadata.get("analyzer_device_ids")
        if isinstance(run_metadata, Mapping)
        else None
    )
    senco_traceability = index_payload.get(
        "senco_authorization_write_traceability"
    )
    traceability_devices = _normalize_device_ids(
        senco_traceability.get("device_ids")
        if isinstance(senco_traceability, Mapping)
        else None
    )
    observed_device_sets = {
        "archive_device_codes": archive_devices,
        "formal_authorized_device_codes": formal_devices,
        "senco_write_device_codes": traceability_devices,
        "report_identifiers": report_devices,
        "evidence_bundle_dut_identifiers": bundle_devices,
    }
    device_code_sets = (
        archive_devices,
        formal_devices,
        traceability_devices,
    )
    if not all(device_code_sets):
        blockers.append("decision_authority_device_code_source_missing")
    elif len({tuple(value) for value in device_code_sets}) != 1:
        blockers.append("decision_authority_device_code_source_mismatch")
    elif expected_devices and archive_devices != expected_devices:
        blockers.append("decision_authority_expected_device_ids_mismatch")
    if not bundle_devices:
        blockers.append("decision_authority_bundle_dut_identity_missing")
    if expected_devices and not set(expected_devices).issubset(report_devices):
        blockers.append("decision_authority_report_device_codes_missing")
    if bundle_devices and not set(bundle_devices).issubset(report_devices):
        blockers.append("decision_authority_report_dut_identities_missing")

    observed_config_sha = str(run_row.get("config_hash") or "").strip().lower()
    if not re.fullmatch(r"[0-9a-f]{64}", observed_config_sha):
        blockers.append("decision_authority_evidence_config_sha256_invalid")
    elif expected_config_sha and observed_config_sha != expected_config_sha:
        blockers.append("decision_authority_runtime_config_sha256_mismatch")

    return (
        {
            "status": "ready" if not blockers else "blocked",
            "expected": {
                "run_id": expected_run,
                "device_ids": expected_devices,
                "runtime_config_sha256": expected_config_sha,
            },
            "observed": {
                "run_ids": observed_run_ids,
                "run_db_ids": observed_run_db_ids,
                "run_dirs": observed_run_dirs,
                "device_ids": observed_device_sets,
                "runtime_config_sha256": observed_config_sha,
            },
            "checks": {
                "run_id_match": "decision_authority_run_id_source_mismatch" not in blockers
                and "decision_authority_expected_run_id_mismatch" not in blockers
                and "decision_authority_run_id_source_missing" not in blockers
                and "decision_authority_expected_run_id_missing" not in blockers,
                "run_db_id_match": "decision_authority_run_db_id_source_mismatch" not in blockers
                and "decision_authority_run_db_id_source_missing" not in blockers,
                "run_dir_match": "decision_authority_run_dir_source_mismatch" not in blockers
                and "decision_authority_run_dir_source_missing" not in blockers,
                "device_ids_match": not any(
                    code.startswith("decision_authority_device_code_")
                    or code
                    in {
                        "decision_authority_expected_device_ids_mismatch",
                        "decision_authority_expected_device_ids_missing",
                        "decision_authority_bundle_dut_identity_missing",
                        "decision_authority_report_device_codes_missing",
                        "decision_authority_report_dut_identities_missing",
                    }
                    for code in blockers
                ),
                "runtime_config_sha256_match": not any(
                    code in {
                        "decision_authority_expected_runtime_config_sha256_invalid",
                        "decision_authority_evidence_config_sha256_invalid",
                        "decision_authority_runtime_config_sha256_mismatch",
                    }
                    for code in blockers
                ),
                "artifact_cross_links_match": artifact_cross_links_match,
                "report_formal_status_match": formal_snapshot_match,
            },
        },
        blockers,
    )


def load_v1_5_decision_authorities(
    archive_index_json: str | Path | None,
    *,
    expected_run_id: str | None = None,
    expected_device_ids: str | Iterable[Any] | None = None,
    expected_runtime_config_sha256: str | None = None,
) -> dict[str, Any]:
    """Load existing formal authorities through the archive's hash manifest."""

    boundary = {
        "opens_com_ports": False,
        "controls_water_or_gas_routes": False,
        "writes_coefficients": False,
        "writes_device_id": False,
        "reads_existing_files_only": True,
    }
    if archive_index_json is None or not str(archive_index_json).strip():
        return {
            "status": "not_configured",
            "blockers": [],
            "archive_index": {"configured": False},
            "artifacts": {},
            "payloads": {},
            "identity_binding": {
                "status": "not_configured",
                "expected": {
                    "run_id": str(expected_run_id or "").strip(),
                    "device_ids": _normalize_device_ids(expected_device_ids),
                    "runtime_config_sha256": str(
                        expected_runtime_config_sha256 or ""
                    ).strip().lower(),
                },
            },
            **boundary,
        }
    index_path = Path(archive_index_json).resolve()
    result: dict[str, Any] = {
        "status": "blocked",
        "blockers": [],
        "archive_index": {
            "configured": True,
            "path": _path_text(index_path),
            "exists": index_path.is_file(),
            "sha256": _sha256_file(index_path),
        },
        "artifacts": {},
        "payloads": {},
        "identity_binding": {"status": "blocked"},
        **boundary,
    }
    blockers = result["blockers"]
    if not index_path.is_file():
        blockers.append("decision_authority_archive_index_missing")
        return result
    try:
        index_payload = json.loads(index_path.read_text(encoding="utf-8-sig"))
    except Exception as exc:
        blockers.append(
            f"decision_authority_archive_index_invalid:{type(exc).__name__}"
        )
        return result
    if not isinstance(index_payload, Mapping):
        blockers.append("decision_authority_archive_index_not_object")
        return result
    result["archive_index"]["schema"] = str(index_payload.get("schema") or "")
    if index_payload.get("schema") != _FORMAL_ARCHIVE_SCHEMA:
        blockers.append("decision_authority_archive_schema_mismatch")
        return result
    artifact_rows = [
        dict(row)
        for row in index_payload.get("artifacts") or []
        if isinstance(row, Mapping)
    ]
    source_payloads: dict[str, Mapping[str, Any]] = {}
    for authority_key, role in _DECISION_AUTHORITY_ROLES.items():
        matches = [row for row in artifact_rows if row.get("role") == role]
        artifact = {
            "role": role,
            "status": "blocked",
            "path": "",
            "expected_sha256": "",
            "actual_sha256": "",
        }
        result["artifacts"][authority_key] = artifact
        if len(matches) != 1:
            blockers.append(
                f"decision_authority_{authority_key}_role_count_invalid:{len(matches)}"
            )
            continue
        row = matches[0]
        source = Path(str(row.get("path") or "")).resolve()
        expected_sha = str(row.get("sha256") or "").strip().lower()
        actual_sha = _sha256_file(source)
        artifact.update(
            {
                "path": _path_text(source),
                "expected_sha256": expected_sha,
                "actual_sha256": actual_sha,
            }
        )
        if not source.is_file():
            blockers.append(f"decision_authority_{authority_key}_missing")
            continue
        if not re.fullmatch(r"[0-9a-f]{64}", expected_sha):
            blockers.append(
                f"decision_authority_{authority_key}_manifest_sha256_invalid"
            )
            continue
        if actual_sha != expected_sha:
            blockers.append(f"decision_authority_{authority_key}_sha256_mismatch")
            continue
        try:
            payload = json.loads(source.read_text(encoding="utf-8-sig"))
        except Exception as exc:
            blockers.append(
                f"decision_authority_{authority_key}_invalid:{type(exc).__name__}"
            )
            continue
        if not isinstance(payload, Mapping):
            blockers.append(f"decision_authority_{authority_key}_not_object")
            continue
        payload = dict(payload)
        if payload.get("evidence_source") == "simulated" or payload.get(
            "not_real_acceptance_evidence"
        ) is True:
            blockers.append(
                f"decision_authority_{authority_key}_simulated_evidence_forbidden"
            )
            continue
        source_payloads[authority_key] = payload
        if authority_key == "formal_run_status":
            if payload.get("schema") != _FORMAL_RUN_STATUS_SCHEMA:
                blockers.append("decision_authority_formal_run_status_schema_mismatch")
                continue
            authorization = payload.get("senco_artifact_authorization")
            if (
                not isinstance(payload.get("can_continue_physical_flow"), bool)
                or not isinstance(payload.get("formal_release_allowed"), bool)
                or not isinstance(authorization, Mapping)
                or not isinstance(
                    authorization.get("controlled_write_authorization_ready"),
                    bool,
                )
            ):
                blockers.append(
                    "decision_authority_formal_run_status_shape_invalid"
                )
                continue
        elif authority_key == "report_model":
            release = payload.get("report_release_decision")
            if not isinstance(release, Mapping):
                blockers.append("decision_authority_report_release_decision_missing")
                continue
            payload = dict(release)
            if (
                not isinstance(payload.get("formal_issue_allowed"), bool)
                or not isinstance(payload.get("release_status"), str)
                or not payload["release_status"].strip()
            ):
                blockers.append("decision_authority_report_release_shape_invalid")
                continue
        else:
            tables = payload.get("tables")
            run_rows = (
                list(tables.get("runs") or [])
                if isinstance(tables, Mapping)
                else []
            )
            if (
                payload.get("schema") != _EVIDENCE_BUNDLE_SCHEMA
                or payload.get("schema_version") != _EVIDENCE_BUNDLE_SCHEMA_VERSION
            ):
                blockers.append("decision_authority_evidence_bundle_schema_mismatch")
                continue
            if len(run_rows) != 1 or not isinstance(run_rows[0], Mapping):
                blockers.append("decision_authority_evidence_bundle_run_shape_invalid")
                continue
        artifact["status"] = "hash_bound"
        result["payloads"][authority_key] = payload
    if not blockers:
        identity_binding, identity_blockers = _decision_authority_identity(
            index_payload=index_payload,
            source_payloads=source_payloads,
            artifacts=result["artifacts"],
            expected_run_id=expected_run_id,
            expected_device_ids=expected_device_ids,
            expected_runtime_config_sha256=expected_runtime_config_sha256,
        )
        result["identity_binding"] = identity_binding
        blockers.extend(identity_blockers)
    if blockers:
        result["payloads"] = {}
        result["status"] = "blocked"
    else:
        result["status"] = "ready"
        for artifact in result["artifacts"].values():
            artifact["status"] = "bound"
    return result


def _inspect_certificate_registry(path: str | Path | None) -> tuple[dict[str, Any], list[str]]:
    """Inspect optional certificate metadata without turning it into a start gate."""

    if path is None:
        return (
            {
                "configured": False,
                "readable": False,
                "policy": "advisory_for_start_formal_release_reviewed_separately",
            },
            ["certificate_registry_not_configured_non_blocking"],
        )
    registry_path = Path(path).resolve()
    if not registry_path.exists():
        return (
            {
                "configured": True,
                "readable": False,
                "path": _path_text(registry_path),
                "policy": "advisory_for_start_formal_release_reviewed_separately",
            },
            ["certificate_registry_missing_non_blocking"],
        )
    try:
        payload = json.loads(registry_path.read_text(encoding="utf-8-sig"))
    except Exception as exc:
        return (
            {
                "configured": True,
                "readable": False,
                "path": _path_text(registry_path),
                "error": f"{type(exc).__name__}: {exc}",
                "policy": "advisory_for_start_formal_release_reviewed_separately",
            },
            ["certificate_registry_unreadable_non_blocking"],
        )
    return (
        {
            "configured": True,
            "readable": isinstance(payload, Mapping),
            "path": _path_text(registry_path),
            "schema_version": payload.get("schema_version") if isinstance(payload, Mapping) else None,
            "policy": "advisory_for_start_formal_release_reviewed_separately",
        },
        [] if isinstance(payload, Mapping) else ["certificate_registry_shape_invalid_non_blocking"],
    )


def _queue_point_counts(
    co2_queue_csv: Path,
    h2o_queue_csv: Path,
) -> tuple[dict[str, int], list[str]]:
    counts = {"co2": 0, "h2o": 0}
    blockers: list[str] = []
    if not co2_queue_csv.exists():
        blockers.append("co2_queue_csv_missing")
    else:
        try:
            counts["co2"] = len(
                _select_co2_queue_rows(
                    _load_co2_queue_rows(co2_queue_csv),
                    temps=None,
                    roles=_parse_co2_roles("fit,verification"),
                    max_points=None,
                )
            )
        except Exception as exc:
            blockers.append(f"co2_queue_csv_invalid:{type(exc).__name__}")
    if not h2o_queue_csv.exists():
        blockers.append("h2o_queue_csv_missing")
    else:
        try:
            counts["h2o"] = len(
                _select_h2o_queue_rows(
                    _load_h2o_queue_rows(h2o_queue_csv),
                    temps=None,
                    max_points=None,
                )
            )
        except Exception as exc:
            blockers.append(f"h2o_queue_csv_invalid:{type(exc).__name__}")
    for route_kind, expected in EXPECTED_POINT_COUNTS.items():
        if counts[route_kind] != expected:
            blockers.append(
                f"{route_kind}_legacy_point_count_mismatch:"
                f"expected={expected},observed={counts[route_kind]}"
            )
    return counts, blockers


def inspect_v1_5_runtime_config(config_path: str | Path) -> dict[str, Any]:
    """Inspect one runtime config without opening COM ports or changing it."""

    path = Path(config_path).resolve()
    blockers: list[str] = []
    warnings: list[str] = []
    result: dict[str, Any] = {
        "schema": "v1_5_runtime_config_start_gate_v1",
        "path": _path_text(path),
        "exists": path.is_file(),
        "readable": False,
        "sha256": "",
        "binding_mode": "unknown",
        "status": "blocked",
        "blockers": blockers,
        "warnings": warnings,
        "opens_com_ports": False,
        "writes_config": False,
    }
    if not path.is_file():
        blockers.append("runtime_config_missing")
        return result

    try:
        result["sha256"] = hashlib.sha256(path.read_bytes()).hexdigest()
        cfg = load_config(path)
    except Exception as exc:
        blockers.append(f"runtime_config_invalid:{type(exc).__name__}")
        return result
    result["readable"] = True

    devices = cfg.get("devices", {})
    if not isinstance(devices, Mapping):
        blockers.append("runtime_config_devices_invalid")
        return result

    reference_rows: dict[str, dict[str, Any]] = {}
    for key in sorted(REFERENCE_DEVICE_KEYS):
        item = devices.get(key)
        if not isinstance(item, Mapping):
            continue
        runtime_port = normalize_com_port(item.get("port"))
        configured_port = normalize_com_port(item.get("configured_port") or runtime_port)
        reference_rows[key] = {
            "configured_port": configured_port,
            "runtime_port": runtime_port,
            "changed": bool(configured_port and runtime_port and configured_port != runtime_port),
            "binding_source": str(item.get("runtime_port_binding_source") or ""),
            "binding_frozen": item.get("runtime_port_binding_frozen") is True,
        }
    device_rows = {
        key: reference_rows.get(key, {})
        for key in ("pressure_controller", "pressure_gauge")
    }
    for key, row in device_rows.items():
        runtime_port = str(row.get("runtime_port") or "")
        if not runtime_port:
            blockers.append(f"{key}_runtime_port_missing")
    result["pressure_devices"] = device_rows
    result["reference_devices"] = reference_rows

    runtime_ports = [
        row["runtime_port"] for row in device_rows.values() if row["runtime_port"]
    ]
    if len(runtime_ports) != len(set(runtime_ports)):
        blockers.append("pressure_runtime_ports_not_unique")

    binding = cfg.get("v1_5_serial_port_binding")
    if not isinstance(binding, Mapping):
        result["binding_mode"] = "static_config"
        result["status"] = "ready_static_runtime_config" if not blockers else "blocked"
        return result

    result["binding_mode"] = "evidence_bound_runtime_config"
    result["binding_metadata"] = dict(binding)
    if binding.get("enabled") is not True:
        blockers.append("runtime_serial_port_binding_not_enabled")
    try:
        blocked_count = int(binding.get("blocked_count") or 0)
    except (TypeError, ValueError):
        blocked_count = -1
    if blocked_count != 0:
        blockers.append("runtime_serial_port_binding_has_blockers")
    if binding.get("gas_analyzer_ports_protected") is not True:
        blockers.append("runtime_serial_port_binding_analyzer_protection_missing")

    allowed_map = allowed_bank_shift_map()
    available_ports = {
        normalize_com_port(port)
        for port in list(binding.get("available_ports") or [])
        if str(port or "").strip()
    }
    changed_rows = [row for row in reference_rows.values() if row["changed"]]
    for key, row in reference_rows.items():
        if not row["changed"]:
            continue
        configured_port = row["configured_port"]
        runtime_port = row["runtime_port"]
        if allowed_map.get(configured_port) != runtime_port:
            blockers.append(f"{key}_runtime_port_shift_outside_allowlist")
        if not row["binding_frozen"]:
            blockers.append(f"{key}_runtime_port_binding_not_frozen")
        both_bank_ports_present = (
            configured_port in available_ports and runtime_port in available_ports
        )
        if both_bank_ports_present and (
            binding.get("require_protocol_match") is not True
            or row["binding_source"] != "v1_5_reference_bank_shift_protocol_identity"
        ):
            blockers.append(f"{key}_dual_bank_unique_protocol_identity_missing")

    try:
        changed_count = int(binding.get("changed_count") or 0)
    except (TypeError, ValueError):
        changed_count = -1
    if changed_count != len(changed_rows):
        blockers.append("runtime_serial_port_binding_changed_count_mismatch")

    result["status"] = "ready_bound_runtime_config" if not blockers else "blocked"
    return result


def _route_plan(
    *,
    route_kind: str,
    runner_module: str,
    config_path: Path,
    queue_csv: Path,
    output_dir: Path,
    queue_run_id: str,
) -> dict[str, Any]:
    argv = [
        "--config",
        _path_text(config_path),
        "--queue-csv",
        _path_text(queue_csv),
        "--output-dir",
        _path_text(output_dir),
        "--run-id",
        queue_run_id,
        "--dry-run",
        "--no-prompt",
        "--no-ftd-write",
    ]
    if route_kind == "co2":
        argv.extend(["--temperature-order", "desc", "--roles", "fit,verification"])
    else:
        argv.extend(
            [
                "--temperature-order",
                "asc",
                "--h2o-pressure-presample-policy",
                "skip",
            ]
        )
    return {
        "route_kind": route_kind,
        "runner_module": runner_module,
        "queue_csv": _path_text(queue_csv),
        "queue_csv_sha256": _sha256_file(queue_csv),
        "output_dir": _path_text(output_dir),
        "queue_run_id": queue_run_id,
        "expected_point_count": EXPECTED_POINT_COUNTS[route_kind],
        "argv": argv,
        "command_preview": " ".join([sys.executable, "-m", runner_module, *argv]),
        "execution_mode": "mature_runner_dry_run",
        "opens_com_ports": False,
        "writes_coefficients": False,
    }


def _controlled_execution_handoff(
    routes: Iterable[Mapping[str, Any]],
    *,
    runtime_config_inspection: Mapping[str, Any],
    blockers: Iterable[str],
) -> dict[str, Any]:
    """Build a preview-only handoff to the existing no-write queue runners."""

    commands: list[dict[str, Any]] = []
    confirmation_sha256 = hashlib.sha256(
        V1_5_ENGINEERING_PROBE_CONFIRMATION_TEXT.encode("utf-8")
    ).hexdigest()
    for route in routes:
        route_kind = str(route.get("route_kind") or "")
        argv = [
            str(value)
            for value in route.get("argv", [])
            if str(value) != "--dry-run"
        ]
        argv.extend(
            [
                "--engineering-probe-only",
                "--operator-confirmation",
                _OPERATOR_CONFIRMATION_PLACEHOLDER,
            ]
        )
        commands.append(
            {
                "route_kind": route_kind,
                "runner_module": str(route.get("runner_module") or ""),
                "queue_run_id": str(route.get("queue_run_id") or ""),
                "queue_csv": str(route.get("queue_csv") or ""),
                "queue_csv_sha256": str(route.get("queue_csv_sha256") or ""),
                "argv_template": argv,
                "command_preview": " ".join(
                    [
                        sys.executable,
                        "-m",
                        str(route.get("runner_module") or ""),
                        *argv,
                    ]
                ),
                "preview_only": True,
                "execution_allowed": False,
                "no_write": True,
                "runner_confirmation_record_expectation": {
                    "filename": V1_5_OPERATOR_CONFIRMATION_RECORD_FILENAME,
                    "schema_version": "v1_5_operator_confirmation_record_v0",
                    "scope": (
                        f"v1_5_{route_kind}_open_flow_queue_"
                        "no_write_engineering_probe"
                    ),
                    "written_by_mature_runner_before_device_construction": True,
                },
            }
        )
    blocked = list(blockers)
    available_scopes = [
        {
            "scope_id": RESPONSE_ONLY_SCOPE,
            "label_zh": "仅响应检查",
            "label_en": "Response-only check",
            "status": (
                "blocked_by_startup_gate"
                if blocked
                else "simulation_ready_real_locked"
            ),
            "allowed_actions": list(RESPONSE_ONLY_ALLOWED_ACTIONS),
            "simulation_executor_available": True,
            "real_execution_allowed": False,
            "operator_confirmation_required_for_real": True,
            "opens_com_ports_in_simulation": False,
            "opens_com_ports_if_real": True,
            "controls_water_or_gas_routes": False,
            "controls_pressure": False,
            "controls_temperature": False,
            "changes_analyzer_mode": False,
            "sends_setpoints": False,
            "runs_calibration_sampling": False,
            "writes_serial_configuration": False,
            "writes_coefficients": False,
            "writes_device_id": False,
            "not_real_acceptance_evidence": True,
        },
        {
            "scope_id": "no_write_route_sampling",
            "label_zh": "成熟路由无写入采样",
            "label_en": "Mature route no-write sampling",
            "status": (
                "blocked_by_startup_gate"
                if blocked
                else "blocked_pending_explicit_double_unlock"
            ),
            "simulation_executor_available": False,
            "real_execution_allowed": False,
            "operator_confirmation_required_for_real": True,
            "opens_com_ports_if_real": True,
            "controls_water_or_gas_routes": True,
            "runs_calibration_sampling": True,
            "writes_coefficients": False,
            "writes_device_id": False,
            "not_real_acceptance_evidence": True,
        },
    ]
    return {
        "schema": "v1_5_controlled_execution_handoff_v1",
        "status": (
            "blocked_by_startup_gate"
            if blocked
            else "blocked_pending_explicit_double_unlock"
        ),
        "blockers": blocked,
        "runtime_config_sha256": str(
            runtime_config_inspection.get("sha256") or ""
        ),
        "commands": commands,
        "available_scopes": available_scopes,
        "default_scope": RESPONSE_ONLY_SCOPE,
        "selected_scope": None,
        "preview_only": True,
        "execution_allowed": False,
        "engineering_probe_only": True,
        "operator_confirmation_required": True,
        "operator_confirmation_embedded": False,
        "operator_confirmation_required_sha256": confirmation_sha256,
        "opens_com_ports_if_executed": True,
        "controls_water_or_gas_routes_if_executed": True,
        "writes_coefficients": False,
        "writes_device_id": False,
        "allows_ftd_write": False,
        "promotion_state": "blocked",
        "not_real_acceptance_evidence": True,
        "uses_existing_mature_runners": True,
    }


def _decision(
    *,
    allowed: bool,
    reason_codes: Iterable[str],
    authority: str,
) -> dict[str, Any]:
    codes = list(dict.fromkeys(str(code) for code in reason_codes if str(code)))
    return {
        "allowed": bool(allowed),
        "status": "allowed" if allowed else "blocked",
        "reason_codes": codes,
        "reasons_zh": [_DECISION_REASON_ZH.get(code, code) for code in codes],
        "reasons_en": [_DECISION_REASON_EN.get(code, code) for code in codes],
        "authority": authority,
    }


def build_v1_5_workstation_decision_model(
    plan: Mapping[str, Any],
    *,
    formal_run_status: Mapping[str, Any] | None = None,
    report_release_decision: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Unify start, controlled-write, and formal-issue decisions.

    This adapter does not replace the mature formal-run or formal-report gates.
    It only presents their decisions alongside the workstation start gate and
    fails closed whenever an upstream authority is absent.
    """

    handoff = dict(plan.get("controlled_execution_handoff") or {})
    scopes = [dict(scope) for scope in handoff.get("available_scopes") or []]
    selected_scope_id = str(
        handoff.get("selected_scope") or handoff.get("default_scope") or ""
    )
    selected_scope = next(
        (scope for scope in scopes if scope.get("scope_id") == selected_scope_id),
        None,
    )
    startup_ready = (
        plan.get("overall_status") == "ready_for_v1_5_dry_run"
        and not list(plan.get("blockers") or [])
    )

    simulation_reasons: list[str] = []
    if not startup_ready:
        simulation_reasons.append("startup_gate_blocked")
    if selected_scope is None:
        simulation_reasons.append("selected_scope_missing")
    elif selected_scope.get("simulation_executor_available") is not True:
        simulation_reasons.append("simulation_executor_unavailable")
    simulation_allowed = not simulation_reasons
    if simulation_allowed:
        simulation_reasons.append("startup_and_scope_ready")

    formal_status = dict(formal_run_status or {})
    real_reasons: list[str] = []
    if not startup_ready:
        real_reasons.append("startup_gate_blocked")
    if selected_scope is None:
        real_reasons.append("selected_scope_missing")
    elif selected_scope.get("real_execution_allowed") is not True:
        real_reasons.append("real_scope_not_unlocked")
    if not formal_status:
        real_reasons.append("formal_run_status_missing")
    elif formal_status.get("can_continue_physical_flow") is not True:
        real_reasons.append("physical_flow_not_ready")
    real_start_allowed = not real_reasons
    if real_start_allowed:
        real_reasons.append("all_required_gates_passed")

    write_reasons: list[str] = []
    authorization = dict(formal_status.get("senco_artifact_authorization") or {})
    if not formal_status:
        write_reasons.append("formal_run_status_missing")
    elif authorization.get("controlled_write_authorization_ready") is not True:
        write_reasons.append("controlled_write_authorization_not_ready")
    write_allowed = not write_reasons
    if write_allowed:
        write_reasons.append("all_required_gates_passed")

    report_decision = dict(report_release_decision or {})
    issue_reasons: list[str] = []
    if not formal_status:
        issue_reasons.append("formal_run_status_missing")
    elif formal_status.get("formal_release_allowed") is not True:
        issue_reasons.append("formal_release_not_allowed")
    if not report_decision:
        issue_reasons.append("report_release_decision_missing")
    elif report_decision.get("formal_issue_allowed") is not True:
        issue_reasons.append("formal_report_issue_not_allowed")
    issue_allowed = not issue_reasons
    if issue_allowed:
        issue_reasons.append("all_required_gates_passed")

    if issue_allowed:
        aggregate_status = "formal_issue_ready"
    elif write_allowed:
        aggregate_status = "controlled_write_ready"
    elif real_start_allowed:
        aggregate_status = "real_execution_ready"
    elif simulation_allowed:
        aggregate_status = "simulation_ready_real_locked"
    else:
        aggregate_status = "blocked"
    decisions = {
        "start_simulation": _decision(
            allowed=simulation_allowed,
            reason_codes=simulation_reasons,
            authority="v1_5_operator_workstation_start_gate",
        ),
        "start_real_execution": _decision(
            allowed=real_start_allowed,
            reason_codes=real_reasons,
            authority="controlled_execution_handoff_plus_formal_run_status",
        ),
        "write_coefficients": _decision(
            allowed=write_allowed,
            reason_codes=write_reasons,
            authority="v1_5_formal_run_status.senco_artifact_authorization",
        ),
        "issue_formal_certificate": _decision(
            allowed=issue_allowed,
            reason_codes=issue_reasons,
            authority="v1_5_formal_run_status_plus_formal_reports",
        ),
    }
    return {
        "schema": "v1_5_workstation_decision_model_v1",
        "aggregate_status": aggregate_status,
        "selected_scope": selected_scope_id,
        "decisions": decisions,
        "can_start_simulation": simulation_allowed,
        "can_start_real_execution": real_start_allowed,
        "can_write_coefficients": write_allowed,
        "can_issue_formal_certificate": issue_allowed,
        "fail_closed": True,
        "single_source_for_ui_cli_snapshot": True,
        "modifies_mature_runners": False,
        "opens_com_ports": False,
        "writes_coefficients": False,
        "writes_device_id": False,
        "not_real_acceptance_evidence": True,
    }


def execute_v1_5_response_only_simulation(
    plan: Mapping[str, Any],
    requests: Iterable[Mapping[str, Any]],
    *,
    client: Callable[[Mapping[str, Any]], Mapping[str, Any]],
) -> dict[str, Any]:
    """Exercise response-only orchestration through an injected simulation client.

    This function has deliberately no serial factory or device dependency.  It
    rejects raw commands and all fields outside the read-only request contract,
    so it cannot become an accidental real-COM escape hatch.
    """

    base = {
        "schema": "v1_5_response_only_simulation_v1",
        "execution_scope": RESPONSE_ONLY_SCOPE,
        "evidence_source": "simulated",
        "simulation_only": True,
        "not_real_acceptance_evidence": True,
        "opens_com_ports": False,
        "controls_water_or_gas_routes": False,
        "controls_pressure": False,
        "controls_temperature": False,
        "changes_analyzer_mode": False,
        "runs_calibration_sampling": False,
        "writes_serial_configuration": False,
        "writes_coefficients": False,
        "writes_device_id": False,
        "promotion_state": "blocked",
    }
    plan_blockers = list(plan.get("blockers") or [])
    handoff = dict(plan.get("controlled_execution_handoff") or {})
    response_scopes = [
        dict(scope)
        for scope in handoff.get("available_scopes") or []
        if scope.get("scope_id") == RESPONSE_ONLY_SCOPE
    ]
    if not response_scopes:
        plan_blockers.append("response_only_scope_missing_from_controlled_handoff")
    elif response_scopes[0].get("simulation_executor_available") is not True:
        plan_blockers.append("response_only_simulation_not_enabled_by_controlled_handoff")
    if plan.get("overall_status") != "ready_for_v1_5_dry_run":
        plan_blockers.append("operator_workstation_start_gate_not_ready")
    if plan_blockers:
        return {
            **base,
            "overall_status": "blocked",
            "execution_started": False,
            "blockers": plan_blockers,
            "request_results": [],
        }

    normalized_requests = [dict(request) for request in requests]
    validation_blockers: list[str] = []
    if not normalized_requests:
        validation_blockers.append("response_only_requests_empty")
    for index, request in enumerate(normalized_requests):
        unknown_fields = sorted(set(request) - _RESPONSE_ONLY_REQUEST_FIELDS)
        if unknown_fields:
            validation_blockers.append(
                f"request_{index}_fields_not_response_only:{','.join(unknown_fields)}"
            )
        action = str(request.get("action") or "").strip()
        if action not in RESPONSE_ONLY_ALLOWED_ACTIONS:
            validation_blockers.append(
                f"request_{index}_action_not_allowed:{action or 'missing'}"
            )
        if not str(request.get("device_key") or "").strip():
            validation_blockers.append(f"request_{index}_device_key_missing")
    if validation_blockers:
        return {
            **base,
            "overall_status": "blocked",
            "execution_started": False,
            "blockers": validation_blockers,
            "request_results": [],
        }

    request_results: list[dict[str, Any]] = []
    for request in normalized_requests:
        try:
            response = client(dict(request))
            if not isinstance(response, Mapping):
                raise TypeError("simulation client response must be a mapping")
            response_payload = dict(response)
            response_ok = response_payload.get("ok") is True
            request_results.append(
                {
                    "request": dict(request),
                    "response": response_payload,
                    "status": "pass" if response_ok else "failed",
                }
            )
            if not response_ok:
                break
        except Exception as exc:
            request_results.append(
                {
                    "request": dict(request),
                    "response": {},
                    "status": "failed",
                    "error": f"{type(exc).__name__}: {exc}",
                }
            )
            break
    failed = [row for row in request_results if row.get("status") != "pass"]
    return {
        **base,
        "completed_at": _now(),
        "overall_status": "pass" if not failed else "failed",
        "execution_started": True,
        "blockers": [],
        "request_count": len(normalized_requests),
        "completed_request_count": len(request_results),
        "request_results": request_results,
    }


def build_v1_5_operator_workstation_startup_receipt(
    plan: Mapping[str, Any],
) -> dict[str, Any]:
    """Freeze one no-COM startup review without authorizing probe execution."""

    blockers = list(plan.get("blockers") or [])
    inspection = dict(plan.get("runtime_config_inspection") or {})
    handoff = dict(plan.get("controlled_execution_handoff") or {})
    routes = [dict(route) for route in plan.get("routes") or []]
    observed_point_counts = dict(plan.get("point_counts") or {})
    queues = {
        str(route.get("route_kind") or ""): {
            "path": str(route.get("queue_csv") or ""),
            "sha256": str(route.get("queue_csv_sha256") or ""),
            "point_count": int(
                observed_point_counts.get(str(route.get("route_kind") or ""))
                or 0
            ),
            "expected_point_count": int(
                route.get("expected_point_count") or 0
            ),
        }
        for route in routes
    }
    startup_gate_passed = not blockers and (
        plan.get("overall_status") == "ready_for_v1_5_dry_run"
    )
    checklist = [
        {
            "id": "runtime_config_hash_bound",
            "status": "pass" if inspection.get("sha256") else "blocked",
        },
        {
            "id": "legacy_45_13_queue_hashes_bound",
            "status": (
                "pass"
                if all(
                    queues.get(kind, {}).get("sha256")
                    and queues.get(kind, {}).get("point_count")
                    == queues.get(kind, {}).get("expected_point_count")
                    for kind in ("co2", "h2o")
                )
                else "blocked"
            ),
        },
        {
            "id": "startup_gate_passed",
            "status": "pass" if startup_gate_passed else "blocked",
        },
        {
            "id": "probe_scope_selected_by_operator",
            "status": "pending_operator_action",
        },
        {
            "id": "physical_port_inventory_rechecked_at_execution",
            "status": "pending_operator_action",
        },
        {
            "id": "operator_confirmation_reentered_at_execution",
            "status": "pending_operator_action",
        },
    ]
    return {
        "schema": STARTUP_RECEIPT_SCHEMA,
        "generated_at": _now(),
        "status": (
            "startup_preflight_recorded_execution_locked"
            if startup_gate_passed
            else "startup_preflight_blocked"
        ),
        "source_plan_schema": str(plan.get("schema") or ""),
        "run_id": str(plan.get("run_id") or ""),
        "calibration_kernel": str(plan.get("calibration_kernel") or ""),
        "profile_id": str(plan.get("profile_id") or ""),
        "startup_gate_passed": startup_gate_passed,
        "blockers": blockers,
        "warnings": list(plan.get("warnings") or []),
        "runtime_config": {
            "path": str(plan.get("runtime_config") or ""),
            "sha256": str(inspection.get("sha256") or ""),
            "status": str(inspection.get("status") or ""),
            "binding_mode": str(inspection.get("binding_mode") or ""),
            "pressure_devices": dict(inspection.get("pressure_devices") or {}),
            "reference_devices": dict(inspection.get("reference_devices") or {}),
        },
        "queues": queues,
        "controlled_execution_handoff": handoff,
        "decision_model": dict(plan.get("decision_model") or {}),
        "decision_authority_binding": dict(
            plan.get("decision_authority_binding") or {}
        ),
        "pre_execution_checklist": checklist,
        "operator_acknowledgement_template": {
            "schema": "v1_5_operator_probe_acknowledgement_template_v1",
            "template_only": True,
            "completed": False,
            "operator_name": "",
            "timestamp": "",
            "selected_route": "",
            "selected_scope": "",
            "observed_connected_ports": [],
            "explicit_acknowledgement": {
                "engineering_probe_only": False,
                "no_write": False,
                "not_real_acceptance": False,
                "v1_fallback_preserved": False,
                "do_not_refresh_real_primary_latest": False,
            },
            "execution_authorization": False,
        },
        "runner_confirmation_record_written_only_at_execution": True,
        "probe_scope_selected": False,
        "probe_execution_allowed": False,
        "preflight_only": True,
        "opens_com_ports": False,
        "controls_water_or_gas_routes": False,
        "writes_coefficients": False,
        "writes_device_id": False,
        "promotion_state": "blocked",
        "not_real_acceptance_evidence": True,
        "v1_fallback_preserved": True,
    }


def write_v1_5_operator_workstation_startup_receipt(
    plan: Mapping[str, Any],
    output_path: str | Path,
) -> dict[str, Any]:
    """Write one immutable startup receipt and return its content hash."""

    receipt = build_v1_5_operator_workstation_startup_receipt(plan)
    path, sha256 = _write_immutable_json(receipt, output_path)
    return {
        "path": _path_text(path),
        "sha256": sha256,
        "status": receipt["status"],
        "probe_execution_allowed": False,
        "opens_com_ports": False,
    }


def _write_immutable_json(
    payload: Mapping[str, Any],
    output_path: str | Path,
) -> tuple[Path, str]:
    """Write one canonical local JSON record without replacing existing evidence."""

    path = Path(output_path).resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    text = json.dumps(dict(payload), ensure_ascii=False, indent=2) + "\n"
    with path.open("x", encoding="utf-8", newline="\n") as handle:
        handle.write(text)
    return path, _sha256_file(path)


def build_v1_5_archive_authority_confirmation_receipt(
    plan: Mapping[str, Any],
    *,
    operator_name: str,
    confirmation_text: str,
) -> dict[str, Any]:
    """Record an operator's archive selection without granting physical authority."""

    binding = dict(plan.get("decision_authority_binding") or {})
    identity = dict(binding.get("identity_binding") or {})
    identity_checks = dict(identity.get("checks") or {})
    expected = dict(identity.get("expected") or {})
    archive_index = dict(binding.get("archive_index") or {})
    artifacts = {
        str(key): dict(value)
        for key, value in dict(binding.get("artifacts") or {}).items()
        if isinstance(value, Mapping)
    }
    operator = str(operator_name or "").strip()
    provided_text = str(confirmation_text or "")
    required_text_sha256 = hashlib.sha256(
        V1_5_ARCHIVE_AUTHORITY_CONFIRMATION_TEXT.encode("utf-8")
    ).hexdigest()
    provided_text_sha256 = hashlib.sha256(
        provided_text.encode("utf-8")
    ).hexdigest()
    confirmation_text_matches = (
        provided_text == V1_5_ARCHIVE_AUTHORITY_CONFIRMATION_TEXT
    )
    blockers: list[str] = []
    if binding.get("status") != "ready":
        blockers.append("authority_confirmation_binding_not_ready")
    if identity.get("status") != "ready":
        blockers.append("authority_confirmation_identity_not_ready")
    if not all(
        identity_checks.get(key) is True
        for key in _AUTHORITY_IDENTITY_CHECK_KEYS
    ):
        blockers.append("authority_confirmation_identity_checks_incomplete")
    if not operator:
        blockers.append("authority_confirmation_operator_missing")
    if not confirmation_text_matches:
        blockers.append("authority_confirmation_text_mismatch")

    archive_path_text = str(archive_index.get("path") or "").strip()
    archive_path = Path(archive_path_text).resolve() if archive_path_text else None
    bound_archive_sha256 = str(
        archive_index.get("sha256") or ""
    ).strip().lower()
    current_archive_sha256 = _sha256_file(archive_path) if archive_path else ""
    if (
        not re.fullmatch(r"[0-9a-f]{64}", bound_archive_sha256)
        or current_archive_sha256 != bound_archive_sha256
    ):
        blockers.append("authority_confirmation_archive_hash_binding_invalid")
    expected_run_id = str(expected.get("run_id") or "").strip()
    expected_device_ids = _normalize_device_ids(expected.get("device_ids"))
    expected_config_sha256 = str(
        expected.get("runtime_config_sha256") or ""
    ).strip().lower()
    if not expected_run_id:
        blockers.append("authority_confirmation_expected_run_id_missing")
    if not expected_device_ids:
        blockers.append("authority_confirmation_expected_device_ids_missing")
    if not re.fullmatch(r"[0-9a-f]{64}", expected_config_sha256):
        blockers.append("authority_confirmation_runtime_config_sha256_invalid")

    artifact_hashes: dict[str, dict[str, str]] = {}
    for authority_key in _DECISION_AUTHORITY_ROLES:
        artifact = artifacts.get(authority_key, {})
        expected_sha256 = str(
            artifact.get("expected_sha256") or ""
        ).strip().lower()
        artifact_path_text = str(artifact.get("path") or "").strip()
        artifact_path = (
            Path(artifact_path_text).resolve() if artifact_path_text else None
        )
        current_sha256 = _sha256_file(artifact_path) if artifact_path else ""
        artifact_hashes[authority_key] = {
            "role": str(artifact.get("role") or ""),
            "path": str(artifact_path) if artifact_path else "",
            "sha256": current_sha256,
        }
        if (
            not re.fullmatch(r"[0-9a-f]{64}", expected_sha256)
            or current_sha256 != expected_sha256
        ):
            blockers.append(
                f"authority_confirmation_{authority_key}_hash_binding_invalid"
            )

    confirmed = not blockers
    return {
        "schema": ARCHIVE_AUTHORITY_CONFIRMATION_RECEIPT_SCHEMA,
        "generated_at": _now(),
        "status": "confirmed" if confirmed else "blocked",
        "blockers": blockers,
        "source_plan": {
            "schema": str(plan.get("schema") or ""),
            "run_id": str(plan.get("run_id") or ""),
            "calibration_kernel": str(plan.get("calibration_kernel") or ""),
            "profile_id": str(plan.get("profile_id") or ""),
        },
        "operator_confirmation": {
            "operator_name": operator,
            "required_text_sha256": required_text_sha256,
            "provided_text_sha256": provided_text_sha256,
            "confirmation_text_matches": confirmation_text_matches,
            "recorded": bool(operator and confirmation_text_matches),
        },
        "archive_selection": {
            "archive_index": {
                "path": str(archive_path) if archive_path else "",
                "sha256": current_archive_sha256,
            },
            "expected_identity": {
                "run_id": expected_run_id,
                "device_ids": expected_device_ids,
                "runtime_config_sha256": expected_config_sha256,
            },
            "observed_identity": dict(identity.get("observed") or {}),
            "identity_checks": identity_checks,
            "artifact_hashes": artifact_hashes,
        },
        "decision_model_at_confirmation": dict(plan.get("decision_model") or {}),
        "meaning": (
            "records_archive_selection_only_does_not_grant_write_or_issue_authority"
        ),
        "formal_actions_unlocked_by_receipt": False,
        "probe_execution_allowed": False,
        "opens_com_ports": False,
        "controls_water_or_gas_routes": False,
        "writes_coefficients": False,
        "writes_device_id": False,
        "formal_certificate_issue_performed": False,
        "promotion_state": "blocked",
        "not_real_acceptance_evidence": True,
        "v1_fallback_preserved": True,
    }


def write_v1_5_archive_authority_confirmation_receipt(
    plan: Mapping[str, Any],
    output_path: str | Path,
    *,
    operator_name: str,
    confirmation_text: str,
) -> dict[str, Any]:
    """Write one immutable archive-selection confirmation receipt."""

    receipt = build_v1_5_archive_authority_confirmation_receipt(
        plan,
        operator_name=operator_name,
        confirmation_text=confirmation_text,
    )
    path, sha256 = _write_immutable_json(receipt, output_path)
    return {
        "path": _path_text(path),
        "sha256": sha256,
        "status": receipt["status"],
        "confirmation_valid": receipt["status"] == "confirmed",
        "formal_actions_unlocked_by_receipt": False,
        "opens_com_ports": False,
    }


def build_v1_5_operator_workstation_plan(
    *,
    config_path: str | Path,
    co2_queue_csv: str | Path,
    h2o_queue_csv: str | Path,
    output_dir: str | Path,
    run_id: str,
    certificate_registry_json: str | Path | None = None,
    decision_authority_archive_json: str | Path | None = None,
    expected_authority_run_id: str | None = None,
    expected_authority_device_ids: str | Iterable[Any] | None = None,
) -> dict[str, Any]:
    """Build the V1.5-first operator plan without executing either route."""

    config = Path(config_path).resolve()
    co2_queue = Path(co2_queue_csv).resolve()
    h2o_queue = Path(h2o_queue_csv).resolve()
    root = Path(output_dir).resolve()
    blockers: list[str] = []

    if not run_id or not _RUN_ID_PATTERN.fullmatch(run_id):
        blockers.append("run_id_must_use_ascii_letters_digits_dot_dash_or_underscore")
    runtime_config_inspection = inspect_v1_5_runtime_config(config)
    blockers.extend(runtime_config_inspection["blockers"])

    point_counts, queue_blockers = _queue_point_counts(co2_queue, h2o_queue)
    blockers.extend(queue_blockers)
    certificate, warnings = _inspect_certificate_registry(certificate_registry_json)
    warnings.extend(runtime_config_inspection["warnings"])

    routes = [
        _route_plan(
            route_kind="co2",
            runner_module=CO2_RUNNER,
            config_path=config,
            queue_csv=co2_queue,
            output_dir=root / "co2",
            queue_run_id=f"{run_id}_co2",
        ),
        _route_plan(
            route_kind="h2o",
            runner_module=H2O_RUNNER,
            config_path=config,
            queue_csv=h2o_queue,
            output_dir=root / "h2o",
            queue_run_id=f"{run_id}_h2o",
        ),
    ]
    controlled_execution_handoff = _controlled_execution_handoff(
        routes,
        runtime_config_inspection=runtime_config_inspection,
        blockers=blockers,
    )
    authority_loading = load_v1_5_decision_authorities(
        decision_authority_archive_json,
        expected_run_id=expected_authority_run_id,
        expected_device_ids=expected_authority_device_ids,
        expected_runtime_config_sha256=str(
            runtime_config_inspection.get("sha256") or ""
        ),
    )
    authority_payloads = dict(authority_loading.get("payloads") or {})
    authority_binding = {
        key: value
        for key, value in authority_loading.items()
        if key != "payloads"
    }
    plan = {
        "schema": SCHEMA,
        "generated_at": _now(),
        "product_name": PRODUCT_NAME,
        "calibration_kernel": CALIBRATION_KERNEL,
        "profile_id": PROFILE_ID,
        "overall_status": "blocked" if blockers else "ready_for_v1_5_dry_run",
        "blockers": blockers,
        "warnings": warnings,
        "run_id": run_id,
        "runtime_config": _path_text(config),
        "runtime_config_inspection": runtime_config_inspection,
        "point_counts": point_counts,
        "expected_point_counts": dict(EXPECTED_POINT_COUNTS),
        "route_order": ["co2", "h2o"],
        "routes": routes,
        "controlled_execution_handoff": controlled_execution_handoff,
        "decision_authority_binding": authority_binding,
        "certificate_registry": certificate,
        "certificate_start_gate": "non_blocking",
        "formal_release_assessment": "not_evaluated_by_workstation_start_gate",
        "evidence_source": "dry_run",
        "not_real_acceptance_evidence": True,
        "no_write": True,
        "opens_com_ports": False,
        "controls_water_or_gas_routes": False,
        "writes_coefficients": False,
        "writes_device_id": False,
        "modifies_mature_runners": False,
        "modifies_run_app": False,
        "v1_fallback_preserved": True,
        "v2_role": "temporary_migration_and_deletion_pool_not_product_runtime",
    }
    plan["decision_model"] = build_v1_5_workstation_decision_model(
        plan,
        formal_run_status=authority_payloads.get("formal_run_status"),
        report_release_decision=authority_payloads.get("report_model"),
    )
    return plan


def _find_queue_summary(route: Mapping[str, Any]) -> tuple[Path | None, dict[str, Any]]:
    output_dir = Path(str(route["output_dir"]))
    run_id = str(route["queue_run_id"])
    for path in sorted(output_dir.rglob("queue_summary.json")):
        try:
            payload = json.loads(path.read_text(encoding="utf-8-sig"))
        except Exception:
            continue
        if str(payload.get("queue_run_id") or "") == run_id:
            return path, payload
    return None, {}


def execute_v1_5_operator_workstation_dry_run(
    plan: Mapping[str, Any],
    *,
    runner_overrides: Mapping[str, Callable[[Iterable[str]], int]] | None = None,
) -> dict[str, Any]:
    """Execute only the mature runners' built-in dry-run branches."""

    if plan.get("blockers"):
        return {
            **dict(plan),
            "overall_status": "blocked",
            "execution_started": False,
            "route_results": [],
        }
    runners: dict[str, Callable[[Iterable[str]], int]] = {
        "co2": _run_co2_queue,
        "h2o": _run_h2o_queue,
    }
    runners.update(dict(runner_overrides or {}))
    route_results: list[dict[str, Any]] = []
    for route in plan.get("routes", []):
        route_kind = str(route["route_kind"])
        returncode = int(runners[route_kind](list(route["argv"])))
        summary_path, summary = _find_queue_summary(route)
        result_blockers: list[str] = []
        if returncode != 0:
            result_blockers.append(f"runner_returncode={returncode}")
        if not summary_path:
            result_blockers.append("queue_summary_missing")
        if summary and summary.get("dry_run") is not True:
            result_blockers.append("queue_summary_not_dry_run")
        if summary and int(summary.get("dry_run_points") or 0) != int(route["expected_point_count"]):
            result_blockers.append(
                "dry_run_point_count_mismatch:"
                f"expected={route['expected_point_count']},"
                f"observed={summary.get('dry_run_points')}"
            )
        if summary and summary.get("no_write") is not True:
            result_blockers.append("queue_summary_no_write_false")
        route_results.append(
            {
                "route_kind": route_kind,
                "status": "pass" if not result_blockers else "failed",
                "returncode": returncode,
                "blockers": result_blockers,
                "queue_summary": _path_text(summary_path) if summary_path else "",
                "dry_run_points": summary.get("dry_run_points"),
                "opens_com_ports": False,
                "writes_coefficients": False,
            }
        )
        if result_blockers:
            break
    execution_blockers = [
        f"{row['route_kind']}:{reason}"
        for row in route_results
        for reason in row.get("blockers", [])
    ]
    return {
        **dict(plan),
        "completed_at": _now(),
        "overall_status": "pass" if not execution_blockers else "failed",
        "execution_started": True,
        "execution_blockers": execution_blockers,
        "route_results": route_results,
    }


def execute_v1_5_controlled_mature_route(
    plan: Mapping[str, Any],
    *,
    route_kind: str,
    execute: bool = False,
    authorization_text: str = "",
    operator_confirmation_text: str = "",
    expected_runtime_config_sha256: str = "",
    expected_queue_csv_sha256: str = "",
    runner_overrides: Mapping[str, Callable[[Iterable[str]], int]] | None = None,
) -> dict[str, Any]:
    """Invoke one mature route after hash binding and two exact confirmations."""

    selected = str(route_kind or "").strip().lower()
    blockers: list[str] = []
    result: dict[str, Any] = {
        "schema": CONTROLLED_MATURE_ROUTE_EXECUTION_SCHEMA,
        "requested_at": _now(),
        "route_kind": selected,
        "overall_status": "blocked",
        "status": "blocked",
        "preflight_passed": False,
        "execution_started": False,
        "execution_allowed": False,
        "runner_invocation_count": 0,
        "automatic_retry_count": 0,
        "blockers": blockers,
        "engineering_probe_only": True,
        "promotion_state": "blocked",
        "not_real_acceptance_evidence": True,
        "no_write": True,
        "allows_ftd_write": False,
        "writes_coefficients": False,
        "writes_senco": False,
        "writes_device_id": False,
    }
    modules = {"co2": CO2_RUNNER, "h2o": H2O_RUNNER}
    if selected not in modules:
        blockers.append("route_kind_not_supported")
        return result

    handoff_value = plan.get("controlled_execution_handoff")
    handoff = handoff_value if isinstance(handoff_value, Mapping) else {}
    required_confirmation_sha = hashlib.sha256(
        V1_5_ENGINEERING_PROBE_CONFIRMATION_TEXT.encode("utf-8")
    ).hexdigest()
    if (
        plan.get("overall_status") != "ready_for_v1_5_dry_run"
        or list(plan.get("blockers") or [])
        or plan.get("no_write") is not True
    ):
        blockers.append("workstation_plan_not_ready")
    if (
        handoff.get("schema") != "v1_5_controlled_execution_handoff_v1"
        or handoff.get("status") != "blocked_pending_explicit_double_unlock"
        or handoff.get("execution_allowed") is not False
        or handoff.get("allows_ftd_write") is not False
        or list(handoff.get("blockers") or [])
        or handoff.get("operator_confirmation_required_sha256")
        != required_confirmation_sha
    ):
        blockers.append("controlled_handoff_invalid")

    route_rows = [
        row for row in list(plan.get("routes") or []) if isinstance(row, Mapping)
    ]
    routes_by_kind = {
        str(row.get("route_kind") or ""): row
        for row in route_rows
    }
    commands = [
        row
        for row in list(handoff.get("commands") or [])
        if isinstance(row, Mapping) and row.get("route_kind") == selected
    ]
    if len(route_rows) != 2 or set(routes_by_kind) != {"co2", "h2o"}:
        blockers.append("workstation_route_contract_mismatch")
    if selected not in routes_by_kind or len(commands) != 1:
        blockers.append("selected_route_or_command_count_mismatch")
        return result

    route = routes_by_kind[selected]
    command = commands[0]
    module = modules[selected]
    config_text = str(plan.get("runtime_config") or "").strip()
    queue_text = str(route.get("queue_csv") or "").strip()
    output_text = str(route.get("output_dir") or "").strip()
    queue_run_id = str(route.get("queue_run_id") or "").strip()
    if (
        route.get("runner_module") != module
        or command.get("runner_module") != module
        or route.get("expected_point_count") != EXPECTED_POINT_COUNTS[selected]
        or not isinstance(plan.get("point_counts"), Mapping)
        or plan["point_counts"].get(selected)
        != EXPECTED_POINT_COUNTS[selected]
    ):
        blockers.append("selected_route_contract_mismatch")
    if not all((config_text, queue_text, output_text, queue_run_id)):
        blockers.append("selected_route_path_or_run_id_missing")

    inspection_value = plan.get("runtime_config_inspection")
    inspection = inspection_value if isinstance(inspection_value, Mapping) else {}
    declared_config_sha = str(inspection.get("sha256") or "").lower()
    supplied_config_sha = str(expected_runtime_config_sha256 or "").lower()
    declared_queue_sha = str(route.get("queue_csv_sha256") or "").lower()
    supplied_queue_sha = str(expected_queue_csv_sha256 or "").lower()
    current_config_sha = _sha256_file(Path(config_text).resolve()) if config_text else ""
    current_queue_sha = _sha256_file(Path(queue_text).resolve()) if queue_text else ""
    if (
        not re.fullmatch(r"[0-9a-f]{64}", supplied_config_sha)
        or len({declared_config_sha, supplied_config_sha, current_config_sha}) != 1
        or str(handoff.get("runtime_config_sha256") or "").lower()
        != current_config_sha
    ):
        blockers.append("runtime_config_sha256_binding_mismatch")
    if (
        not re.fullmatch(r"[0-9a-f]{64}", supplied_queue_sha)
        or len({declared_queue_sha, supplied_queue_sha, current_queue_sha}) != 1
        or str(command.get("queue_csv_sha256") or "").lower() != current_queue_sha
    ):
        blockers.append("queue_csv_sha256_binding_mismatch")

    route_paths = {
        kind: Path(str(row.get("queue_csv") or ""))
        for kind, row in routes_by_kind.items()
    }
    if set(route_paths) == {"co2", "h2o"}:
        current_counts, queue_blockers = _queue_point_counts(
            route_paths["co2"],
            route_paths["h2o"],
        )
        if queue_blockers or current_counts != EXPECTED_POINT_COUNTS:
            blockers.append("current_45_13_queue_contract_mismatch")

    canonical_template: list[str] = []
    if current_config_sha and current_queue_sha and output_text and queue_run_id:
        canonical_route = _route_plan(
            route_kind=selected,
            runner_module=module,
            config_path=Path(config_text),
            queue_csv=Path(queue_text),
            output_dir=Path(output_text),
            queue_run_id=queue_run_id,
        )
        canonical_template = [
            str(value)
            for value in canonical_route["argv"]
            if str(value) != "--dry-run"
        ]
        canonical_template.extend(
            [
                "--engineering-probe-only",
                "--operator-confirmation",
                _OPERATOR_CONFIRMATION_PLACEHOLDER,
            ]
        )
        if (
            list(route.get("argv") or []) != canonical_route["argv"]
            or list(command.get("argv_template") or []) != canonical_template
        ):
            blockers.append("selected_route_argv_not_canonical")

    result["blockers"] = list(dict.fromkeys(blockers))
    if result["blockers"]:
        return result
    result.update(
        {
            "preflight_passed": True,
            "runner_module": module,
            "queue_csv_sha256": current_queue_sha,
            "runtime_config_sha256": current_config_sha,
        }
    )
    if not execute:
        result.update(
            {"overall_status": "ready", "status": "preflight_ready_execution_locked"}
        )
        return result

    if authorization_text != V1_5_CONTROLLED_MATURE_ROUTE_AUTHORIZATION_TEXT:
        result["blockers"].append("application_authorization_text_mismatch")
    if operator_confirmation_text != V1_5_ENGINEERING_PROBE_CONFIRMATION_TEXT:
        result["blockers"].append("runner_operator_confirmation_text_mismatch")
    if result["blockers"]:
        return result

    argv = [
        operator_confirmation_text
        if value == _OPERATOR_CONFIRMATION_PLACEHOLDER
        else value
        for value in canonical_template
    ]
    runners: dict[str, Callable[[Iterable[str]], int]] = {
        "co2": _run_co2_queue,
        "h2o": _run_h2o_queue,
    }
    runners.update(dict(runner_overrides or {}))
    result.update(
        {
            "execution_allowed": True,
            "execution_started": True,
            "runner_invocation_count": 1,
        }
    )
    try:
        returncode = int(runners[selected](argv))
    except Exception as exc:
        result.update(
            {
                "completed_at": _now(),
                "overall_status": "failed",
                "status": "runner_exception",
                "runner_error": f"{type(exc).__name__}: {exc}",
            }
        )
        result["blockers"].append(f"runner_exception:{type(exc).__name__}")
        return result

    result.update(
        {
            "completed_at": _now(),
            "runner_returncode": returncode,
            "overall_status": "pass" if returncode == 0 else "failed",
            "status": "completed" if returncode == 0 else "runner_failed",
        }
    )
    if returncode != 0:
        result["blockers"].append(f"runner_returncode={returncode}")
    return result


def preflight_v1_5_controlled_mature_route(
    plan: Mapping[str, Any],
    *,
    route_kind: str,
) -> dict[str, Any]:
    """Run the shared hash-bound mature-route preflight with execution locked."""

    selected = str(route_kind or "").strip().lower()
    inspection_value = plan.get("runtime_config_inspection")
    inspection = inspection_value if isinstance(inspection_value, Mapping) else {}
    route = next(
        (
            row
            for row in plan.get("routes") or []
            if isinstance(row, Mapping) and row.get("route_kind") == selected
        ),
        {},
    )
    return execute_v1_5_controlled_mature_route(
        plan,
        route_kind=selected,
        execute=False,
        expected_runtime_config_sha256=str(inspection.get("sha256") or ""),
        expected_queue_csv_sha256=str(route.get("queue_csv_sha256") or ""),
    )


def build_v1_5_controlled_route_preflight_receipt(
    plan: Mapping[str, Any],
    preflight: Mapping[str, Any],
) -> dict[str, Any]:
    """Freeze one offline route preflight without granting execution authority."""

    route_kind = str(preflight.get("route_kind") or "").strip().lower()
    routes = [
        dict(row)
        for row in plan.get("routes") or []
        if isinstance(row, Mapping) and row.get("route_kind") == route_kind
    ]
    route = routes[0] if len(routes) == 1 else {}
    inspection_value = plan.get("runtime_config_inspection")
    inspection = inspection_value if isinstance(inspection_value, Mapping) else {}
    point_counts_value = plan.get("point_counts")
    point_counts = point_counts_value if isinstance(point_counts_value, Mapping) else {}
    handoff_value = plan.get("controlled_execution_handoff")
    handoff = handoff_value if isinstance(handoff_value, Mapping) else {}
    config_path = Path(str(plan.get("runtime_config") or "")).resolve()
    queue_path = Path(str(route.get("queue_csv") or "")).resolve()
    current_config_sha = _sha256_file(config_path)
    current_queue_sha = _sha256_file(queue_path)
    result_config_sha = str(preflight.get("runtime_config_sha256") or "").lower()
    result_queue_sha = str(preflight.get("queue_csv_sha256") or "").lower()
    blockers = list(preflight.get("blockers") or [])
    checks = {
        "preflight_schema_valid": (
            preflight.get("schema") == CONTROLLED_MATURE_ROUTE_EXECUTION_SCHEMA
        ),
        "route_binding_unique": len(routes) == 1 and route_kind in EXPECTED_POINT_COUNTS,
        "preflight_ready_and_locked": (
            preflight.get("status") == "preflight_ready_execution_locked"
            and preflight.get("execution_started") is False
            and preflight.get("execution_allowed") is False
            and int(preflight.get("runner_invocation_count") or 0) == 0
        ),
        "runtime_config_hash_still_bound": (
            bool(current_config_sha)
            and current_config_sha == result_config_sha
            and current_config_sha == str(inspection.get("sha256") or "").lower()
        ),
        "queue_hash_still_bound": (
            bool(current_queue_sha)
            and current_queue_sha == result_queue_sha
            and current_queue_sha
            == str(route.get("queue_csv_sha256") or "").lower()
        ),
        "point_count_contract_preserved": (
            route_kind in EXPECTED_POINT_COUNTS
            and int(point_counts.get(route_kind) or 0)
            == EXPECTED_POINT_COUNTS.get(route_kind)
            and int(route.get("expected_point_count") or 0)
            == EXPECTED_POINT_COUNTS.get(route_kind)
        ),
    }
    blockers.extend(
        f"preflight_receipt_check_failed:{key}"
        for key, passed in checks.items()
        if not passed
    )
    blockers = list(dict.fromkeys(blockers))
    ready = not blockers and all(checks.values())
    return {
        "schema": CONTROLLED_MATURE_ROUTE_PREFLIGHT_RECEIPT_SCHEMA,
        "generated_at": _now(),
        "status": (
            "preflight_recorded_execution_locked"
            if ready
            else "blocked_preflight_recorded_execution_locked"
        ),
        "preflight_passed": ready,
        "blockers": blockers,
        "checks": checks,
        "source_plan": {
            "schema": str(plan.get("schema") or ""),
            "run_id": str(plan.get("run_id") or ""),
            "calibration_kernel": str(plan.get("calibration_kernel") or ""),
            "profile_id": str(plan.get("profile_id") or ""),
        },
        "route_binding": {
            "route_kind": route_kind,
            "runner_module": str(route.get("runner_module") or ""),
            "runtime_config": {
                "path": _path_text(config_path),
                "sha256": current_config_sha,
            },
            "queue_csv": {
                "path": _path_text(queue_path),
                "sha256": current_queue_sha,
                "point_count": int(
                    point_counts.get(route_kind) or 0
                ),
                "expected_point_count": int(
                    route.get("expected_point_count") or 0
                ),
            },
            "argv_template": list(
                next(
                    (
                        command.get("argv_template") or []
                        for command in handoff.get("commands") or []
                        if isinstance(command, Mapping)
                        and command.get("route_kind") == route_kind
                    ),
                    [],
                )
            ),
        },
        "preflight_result": {
            "schema": str(preflight.get("schema") or ""),
            "status": str(preflight.get("status") or ""),
            "overall_status": str(preflight.get("overall_status") or ""),
            "execution_started": preflight.get("execution_started") is True,
            "execution_allowed": preflight.get("execution_allowed") is True,
            "runner_invocation_count": int(
                preflight.get("runner_invocation_count") or 0
            ),
        },
        "meaning": "records_offline_preflight_only_does_not_authorize_execution",
        "preflight_only": True,
        "real_execution_authorized": False,
        "opens_com_ports": False,
        "controls_water_or_gas_routes": False,
        "writes_coefficients": False,
        "writes_device_id": False,
        "promotion_state": "blocked",
        "not_real_acceptance_evidence": True,
        "v1_fallback_preserved": True,
    }


def write_v1_5_controlled_route_preflight_receipt(
    plan: Mapping[str, Any],
    preflight: Mapping[str, Any],
    output_path: str | Path,
) -> dict[str, Any]:
    """Write one immutable offline preflight receipt and return its hash."""

    receipt = build_v1_5_controlled_route_preflight_receipt(plan, preflight)
    path, sha256 = _write_immutable_json(receipt, output_path)
    return {
        "path": _path_text(path),
        "sha256": sha256,
        "status": receipt["status"],
        "preflight_passed": receipt["preflight_passed"],
        "real_execution_authorized": False,
        "opens_com_ports": False,
    }


def write_v1_5_operator_workstation_outputs(
    result: Mapping[str, Any],
    output_dir: str | Path,
) -> dict[str, Path]:
    root = Path(output_dir).resolve()
    root.mkdir(parents=True, exist_ok=True)
    json_path = root / "v1_5_operator_workstation_dry_run.json"
    markdown_path = root / "V1_5_OPERATOR_WORKSTATION_DRY_RUN.md"
    json_path.write_text(
        json.dumps(result, ensure_ascii=False, indent=2),
        encoding="utf-8-sig",
    )
    lines = [
        "# V1.5 气体分析仪校准工作站 dry-run",
        "",
        f"- 状态：`{result.get('overall_status')}`",
        f"- 校准内核：`{result.get('calibration_kernel')}`",
        f"- 生产配置：`{result.get('profile_id')}`",
        (
            "- CO2/H2O 点数："
            f"`{(result.get('point_counts') or {}).get('co2')}` / "
            f"`{(result.get('point_counts') or {}).get('h2o')}`"
        ),
        "- 证书资料：不阻断工作站启动和 dry-run；正式签发资格另行评审。",
        "- 边界：不打开 COM、不控制气路/水路、不写 SENCO/设备 ID、不构成真实验收。",
        "- V1 fallback 与 run_app.py 未修改；V2 仅保留分析、报告和治理职责。",
        "",
        "## 路由结果",
        "",
    ]
    for row in result.get("route_results", []):
        lines.append(
            f"- `{row.get('route_kind')}`: `{row.get('status')}`, "
            f"dry-run points=`{row.get('dry_run_points')}`"
        )
    if result.get("warnings"):
        lines.extend(["", "## 非阻断提醒", ""])
        lines.extend(f"- `{warning}`" for warning in result.get("warnings", []))
    if result.get("blockers") or result.get("execution_blockers"):
        lines.extend(["", "## 阻断项", ""])
        lines.extend(
            f"- `{blocker}`"
            for blocker in [
                *result.get("blockers", []),
                *result.get("execution_blockers", []),
            ]
        )
    markdown_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return {"json": json_path, "markdown": markdown_path}


def run_v1_5_operator_workstation_application(
    plan: Mapping[str, Any],
    *,
    output_dir: str | Path,
    executor: Callable[[Mapping[str, Any]], dict[str, Any]] | None = None,
) -> tuple[dict[str, Any], dict[str, Path]]:
    """Execute once and write once through the shared GUI/CLI seam."""

    active_executor = executor or execute_v1_5_operator_workstation_dry_run
    result = dict(active_executor(plan))
    outputs = write_v1_5_operator_workstation_outputs(result, output_dir)
    return result, outputs


__all__ = [
    "ARCHIVE_AUTHORITY_CONFIRMATION_RECEIPT_SCHEMA",
    "CALIBRATION_KERNEL",
    "CONTROLLED_MATURE_ROUTE_EXECUTION_SCHEMA",
    "CONTROLLED_MATURE_ROUTE_PREFLIGHT_RECEIPT_SCHEMA",
    "EXPECTED_POINT_COUNTS",
    "PRODUCT_NAME",
    "PROFILE_ID",
    "RESPONSE_ONLY_ALLOWED_ACTIONS",
    "RESPONSE_ONLY_SCOPE",
    "STARTUP_RECEIPT_SCHEMA",
    "V1_5_ARCHIVE_AUTHORITY_CONFIRMATION_TEXT",
    "V1_5_CONTROLLED_MATURE_ROUTE_AUTHORIZATION_TEXT",
    "V1_5_ENGINEERING_PROBE_CONFIRMATION_TEXT",
    "build_v1_5_archive_authority_confirmation_receipt",
    "build_v1_5_controlled_route_preflight_receipt",
    "build_v1_5_operator_workstation_plan",
    "build_v1_5_operator_workstation_startup_receipt",
    "build_v1_5_workstation_decision_model",
    "execute_v1_5_controlled_mature_route",
    "execute_v1_5_operator_workstation_dry_run",
    "execute_v1_5_response_only_simulation",
    "inspect_v1_5_runtime_config",
    "load_v1_5_decision_authorities",
    "preflight_v1_5_controlled_mature_route",
    "run_v1_5_operator_workstation_application",
    "write_v1_5_archive_authority_confirmation_receipt",
    "write_v1_5_controlled_route_preflight_receipt",
    "write_v1_5_operator_workstation_outputs",
    "write_v1_5_operator_workstation_startup_receipt",
]
