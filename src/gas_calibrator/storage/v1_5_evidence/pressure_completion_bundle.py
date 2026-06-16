"""Evidence-registry bundle for V1.5 pressure-channel completion packages."""

from __future__ import annotations

import csv
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence

from .bundle import TABLE_NAMES, sha256_file, sha256_json, stable_id


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _read_csv(path: str | Path) -> List[Dict[str, Any]]:
    source = Path(path)
    if not source.exists():
        return []
    with source.open("r", encoding="utf-8-sig", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _load_json(path: str | Path) -> Dict[str, Any]:
    source = Path(path)
    if not source.exists():
        return {}
    return json.loads(source.read_text(encoding="utf-8-sig"))


def _truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value or "").strip().lower() in {"1", "true", "yes", "y", "on", "ok", "pass", "verified"}


def _safe_int(value: Any) -> int:
    try:
        return int(float(value))
    except Exception:
        return 0


def _safe_float(value: Any) -> Optional[float]:
    if value in (None, "", "null", "None"):
        return None
    try:
        return float(value)
    except Exception:
        return None


def _split_reasons(value: Any) -> List[str]:
    if value in (None, ""):
        return []
    if isinstance(value, list):
        return [str(item) for item in value if str(item)]
    text = str(value).strip()
    if text.startswith("[") and text.endswith("]"):
        try:
            parsed = json.loads(text)
        except Exception:
            parsed = None
        if isinstance(parsed, list):
            return [str(item) for item in parsed if str(item)]
    return [item for item in text.split(";") if item]


def _iso_date_or_none(value: Any) -> Optional[str]:
    text = str(value or "").strip()
    if not text:
        return None
    return text[:10]


def _device_id(value: Any) -> str:
    text = str(value or "").strip()
    if text.isdigit():
        return f"{int(text):03d}"
    return text


def _first(rows: Sequence[Mapping[str, Any]]) -> Mapping[str, Any]:
    return rows[0] if rows else {}


def _artifact_role_from_completion(role: str, path: Path) -> str:
    name = path.name.lower()
    if role == "senco9_write_summary":
        return "pressure_senco9_write_summary"
    if role == "post_write_pressure_fit_summary":
        return "pressure_senco9_post_write_fit_summary"
    if role == "pressure_reference_json":
        return "pressure_reference_snapshot"
    if role == "pressure_reference_traceability":
        return "pressure_reference_traceability"
    if role == "old_getco9_snapshot":
        return "pressure_senco9_old_getco_snapshot"
    if name == "pressure_channel_completion_summary.csv":
        return "pressure_channel_completion_summary"
    if name == "pressure_channel_device_readiness.csv":
        return "pressure_channel_device_readiness"
    if name == "pressure_channel_excluded_devices.csv":
        return "pressure_channel_excluded_devices"
    if name == "pressure_channel_known_limitations.csv":
        return "pressure_channel_known_limitations"
    if name == "pressure_channel_traceability.csv":
        return "pressure_channel_traceability"
    if name == "pressure_channel_readiness_gate.csv":
        return "pressure_channel_readiness_gate"
    if name == "pressure_channel_completion_report.md":
        return "pressure_channel_completion_report"
    if name == "pressure_channel_completion.xlsx":
        return "pressure_channel_completion_workbook"
    if name == "pressure_channel_completion_meta.json":
        return "pressure_channel_completion_meta"
    if name == "pressure_channel_completion_evidence_bundle.json":
        return "pressure_channel_completion_evidence_bundle"
    if name in {
        "pressure_channel_completion_evidence_summary.json",
        "pressure_channel_completion_db_import_summary.json",
    }:
        return "pressure_channel_completion_import_summary"
    return role or "pressure_channel_completion_artifact"


def _file_row(
    *,
    run_db_id: str,
    path: str | Path,
    role: str,
    required: bool,
    metadata: Optional[Mapping[str, Any]] = None,
) -> Optional[Dict[str, Any]]:
    source = Path(path).resolve()
    if not source.exists() or not source.is_file():
        return None
    stat = source.stat()
    return {
        "id": stable_id("sample_file", run_db_id, str(source)),
        "run_db_id": run_db_id,
        "artifact_role": role,
        "path": str(source),
        "sha256": sha256_file(source),
        "size_bytes": int(stat.st_size),
        "modified_at": datetime.fromtimestamp(stat.st_mtime, timezone.utc).isoformat(timespec="seconds"),
        "required": bool(required),
        "metadata": dict(metadata or {}),
    }


def _build_file_rows(
    *,
    run_db_id: str,
    completion_dir: Path,
    artifact_rows: Sequence[Mapping[str, Any]],
    pressure_reference: Mapping[str, Any],
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    seen: set[str] = set()

    required_names = {
        "pressure_channel_completion_summary.csv",
        "pressure_channel_device_readiness.csv",
        "pressure_channel_traceability.csv",
        "pressure_channel_readiness_gate.csv",
        "pressure_channel_completion_report.md",
        "pressure_channel_completion.xlsx",
        "pressure_channel_completion_meta.json",
    }
    for path in sorted(completion_dir.iterdir(), key=lambda item: item.name.lower()):
        if not path.is_file():
            continue
        role = _artifact_role_from_completion("", path)
        if role in {"pressure_channel_completion_evidence_bundle", "pressure_channel_completion_import_summary"}:
            continue
        row = _file_row(
            run_db_id=run_db_id,
            path=path,
            role=role,
            required=path.name in required_names,
            metadata={"source": "pressure_channel_completion_output"},
        )
        if row and row["path"].lower() not in seen:
            rows.append(row)
            seen.add(row["path"].lower())

    required_input_roles = {
        "senco9_write_summary",
        "post_write_pressure_fit_summary",
        "pressure_reference_json",
        "pressure_reference_traceability",
        "old_getco9_snapshot",
    }
    for item in artifact_rows:
        raw_path = str(item.get("path") or "").strip()
        if not raw_path:
            continue
        path = Path(raw_path).resolve()
        role_text = str(item.get("artifact_role") or "")
        role = _artifact_role_from_completion(role_text, path)
        row = _file_row(
            run_db_id=run_db_id,
            path=path,
            role=role,
            required=role_text in required_input_roles,
            metadata={
                "source": "pressure_channel_completion_input",
                "input_role": role_text,
                "source_sha256": item.get("sha256"),
            },
        )
        if row and row["path"].lower() not in seen:
            rows.append(row)
            seen.add(row["path"].lower())

    certificate_file = str(pressure_reference.get("certificate_file") or "").strip()
    if certificate_file:
        row = _file_row(
            run_db_id=run_db_id,
            path=certificate_file,
            role="pressure_reference_certificate_pdf",
            required=True,
            metadata={
                "source": "pressure_reference_snapshot.certificate_file",
                "certificate_id": pressure_reference.get("certificate_id"),
            },
        )
        if row and row["path"].lower() not in seen:
            rows.append(row)
            seen.add(row["path"].lower())
    return rows


def _file_id_by_role(files: Sequence[Mapping[str, Any]], role: str) -> Optional[str]:
    for row in files:
        if row.get("artifact_role") == role:
            return str(row.get("id") or "")
    return None


def _build_devices(
    *,
    run_db_id: str,
    device_rows: Sequence[Mapping[str, Any]],
    pressure_reference: Mapping[str, Any],
) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    devices: List[Dict[str, Any]] = []
    links: List[Dict[str, Any]] = []

    for row in device_rows:
        device_id = _device_id(row.get("analyzer_device_id"))
        if not device_id:
            continue
        prefix = str(row.get("analyzer_prefix") or "")
        device_row = {
            "id": stable_id("device", "gas_analyzer", device_id),
            "device_type": "gas_analyzer",
            "device_role": "device_under_test",
            "display_name": device_id,
            "serial_number": device_id,
            "metadata": {
                "analyzer_prefix": prefix,
                "acquisition_channel_only": bool(prefix),
                "identity_source": "analyzer_reported_device_id",
                "pressure_channel_only": True,
            },
        }
        devices.append(device_row)
        links.append(
            {
                "id": stable_id("run_device", run_db_id, device_row["id"], "device_under_test"),
                "run_db_id": run_db_id,
                "device_id": device_row["id"],
                "role": "device_under_test",
                "metadata": {"analyzer_prefix": prefix},
            }
        )

    pressure_device_id = str(pressure_reference.get("device_id") or "COM22").strip() or "COM22"
    pressure_row = {
        "id": stable_id("device", "pressure_reference", pressure_device_id),
        "device_type": "digital_pressure_gauge",
        "device_role": "primary_pressure_reference",
        "display_name": pressure_device_id,
        "serial_number": pressure_device_id,
        "metadata": {
            "source": "pressure_reference_snapshot",
            "model": pressure_reference.get("model"),
            "manufacturer": pressure_reference.get("manufacturer"),
        },
    }
    devices.append(pressure_row)
    links.append(
        {
            "id": stable_id("run_device", run_db_id, pressure_row["id"], "primary_pressure_reference"),
            "run_db_id": run_db_id,
            "device_id": pressure_row["id"],
            "role": "primary_pressure_reference",
            "metadata": {"reference_role": "COM22"},
        }
    )
    return devices, links


def _build_reference_certificate(
    *,
    run_db_id: str,
    pressure_reference: Mapping[str, Any],
    devices: Sequence[Mapping[str, Any]],
) -> List[Dict[str, Any]]:
    pressure_device_id = None
    for row in devices:
        if row.get("device_role") == "primary_pressure_reference":
            pressure_device_id = str(row.get("id") or "")
            break
    return [
        {
            "id": stable_id("reference_certificate", run_db_id, "primary_pressure_reference", pressure_reference.get("certificate_id")),
            "run_db_id": run_db_id,
            "device_id": pressure_device_id,
            "reference_role": "primary_pressure_reference",
            "certificate_id": str(pressure_reference.get("certificate_id") or "") or None,
            "certificate_hash": str(pressure_reference.get("certificate_hash") or "") or None,
            "valid_until": _iso_date_or_none(pressure_reference.get("valid_until")),
            "uncertainty": _safe_float(pressure_reference.get("certificate_uncertainty")),
            "unit": str(pressure_reference.get("unit") or "hPa"),
            "metadata": dict(pressure_reference),
        }
    ]


def _build_calibration_points(
    *,
    run_db_id: str,
    device_rows: Sequence[Mapping[str, Any]],
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for row in device_rows:
        device_id = _device_id(row.get("analyzer_device_id"))
        prefix = str(row.get("analyzer_prefix") or "")
        rows.append(
            {
                "id": stable_id("calibration_point", run_db_id, "pressure_channel", device_id),
                "run_db_id": run_db_id,
                "component": "pressure",
                "point_key": f"{prefix or device_id}_senco9_post_write_verification",
                "point_tag": device_id,
                "pressure_mode": "controlled_pressure_channel_validation",
                "target_value": None,
                "sample_count": _safe_int(row.get("valid_pair_count")),
                "a_grade_count": _safe_int(row.get("valid_pair_count")),
                "b_grade_count": 0,
                "rejected_count": 0 if str(row.get("readiness_status") or "") == "pass" else 1,
                "metadata": dict(row),
            }
        )
    return rows


def _status_for_qc(status: str) -> str:
    text = status.strip()
    if text in {"ready_for_open_flow_main_calibration", "written_readback_verified"}:
        return "pass"
    return text or "unknown"


def _build_qc_rows(
    *,
    run_db_id: str,
    summary_row: Mapping[str, Any],
    trace_row: Mapping[str, Any],
    gate_row: Mapping[str, Any],
    device_rows: Sequence[Mapping[str, Any]],
    files: Sequence[Mapping[str, Any]],
) -> List[Dict[str, Any]]:
    summary_artifact = _file_id_by_role(files, "pressure_channel_completion_summary")
    trace_artifact = _file_id_by_role(files, "pressure_channel_traceability") or _file_id_by_role(files, "pressure_reference_snapshot")
    gate_artifact = _file_id_by_role(files, "pressure_channel_readiness_gate")
    device_artifact = _file_id_by_role(files, "pressure_channel_device_readiness")
    rows = [
        {
            "id": stable_id("qc", run_db_id, "pressure_channel_completion_overall"),
            "run_db_id": run_db_id,
            "scope": "pressure_channel",
            "subject_id": "all_analyzers",
            "rule_name": "pressure_channel_completion_overall",
            "status": _status_for_qc(str(summary_row.get("overall_status") or "")),
            "severity": "error" if summary_row.get("overall_status") != "ready_for_open_flow_main_calibration" else "info",
            "reasons": [],
            "metrics": dict(summary_row),
            "source_artifact_id": summary_artifact,
            "metadata": {"physical_scope": "pressure_input_validation"},
        },
        {
            "id": stable_id("qc", run_db_id, "pressure_reference_traceability"),
            "run_db_id": run_db_id,
            "scope": "traceability",
            "subject_id": str(trace_row.get("device_id") or "COM22"),
            "rule_name": "pressure_reference_traceability",
            "status": str(trace_row.get("status") or "unknown"),
            "severity": "error" if str(trace_row.get("status") or "") != "pass" else "info",
            "reasons": _split_reasons(trace_row.get("reasons") or trace_row.get("fit_traceability_reasons")),
            "metrics": dict(trace_row),
            "source_artifact_id": trace_artifact,
            "metadata": {"physical_scope": "COM22_certificate"},
        },
        {
            "id": stable_id("qc", run_db_id, "pressure_channel_readiness_gate"),
            "run_db_id": run_db_id,
            "scope": "run_gate",
            "subject_id": str(gate_row.get("gate") or "pressure_channel_precondition_for_open_flow"),
            "rule_name": "pressure_channel_precondition_for_open_flow",
            "status": str(gate_row.get("status") or "unknown"),
            "severity": "error" if str(gate_row.get("status") or "") != "pass" else "info",
            "reasons": _split_reasons(gate_row.get("reason")),
            "metrics": dict(gate_row),
            "source_artifact_id": gate_artifact,
            "metadata": {"physical_scope": "open_flow_precondition"},
        },
    ]
    for row in device_rows:
        device_id = _device_id(row.get("analyzer_device_id"))
        rows.append(
            {
                "id": stable_id("qc", run_db_id, "pressure_channel_device_readiness", device_id),
                "run_db_id": run_db_id,
                "scope": "pressure_channel_device",
                "subject_id": device_id,
                "rule_name": "pressure_channel_device_readiness",
                "status": str(row.get("readiness_status") or "unknown"),
                "severity": "error" if str(row.get("readiness_status") or "") != "pass" else "info",
                "reasons": _split_reasons(row.get("readiness_reasons")),
                "metrics": dict(row),
                "source_artifact_id": device_artifact,
                "metadata": {
                    "physical_scope": "analyzer_internal_pressure_P",
                    "analyzer_prefix": row.get("analyzer_prefix"),
                    "not_co2_h2o_fit_evidence": True,
                },
            }
        )
    return rows


def _build_coefficient_snapshots(
    *,
    run_db_id: str,
    old_getco_path: str,
    old_getco: Mapping[str, Any],
    files: Sequence[Mapping[str, Any]],
) -> List[Dict[str, Any]]:
    artifact_id = _file_id_by_role(files, "pressure_senco9_old_getco_snapshot")
    rows: List[Dict[str, Any]] = []
    for device_id, payload in sorted(old_getco.items()):
        if not isinstance(payload, Mapping):
            continue
        coefficients = {
            "GETCO9_before": payload.get("GETCO9_before", []),
            "candidate_values": payload.get("candidate_values", []),
            "readback": payload.get("readback", []),
            "candidate_offset_mode": payload.get("candidate_offset_mode"),
        }
        rows.append(
            {
                "id": stable_id("coefficient_snapshot", run_db_id, "senco9", _device_id(device_id)),
                "run_db_id": run_db_id,
                "analyzer_id": _device_id(device_id),
                "snapshot_type": "senco9_before_and_readback",
                "coefficients": coefficients,
                "coefficients_hash": sha256_json(coefficients),
                "source_artifact_id": artifact_id,
                "metadata": {
                    "path": old_getco_path,
                    "analyzer_prefix": payload.get("analyzer_prefix"),
                    "pressure_channel_only": True,
                },
            }
        )
    return rows


def _build_write_events(
    *,
    run_db_id: str,
    write_rows: Sequence[Mapping[str, Any]],
    snapshots: Sequence[Mapping[str, Any]],
) -> List[Dict[str, Any]]:
    hash_by_device = {str(row.get("analyzer_id") or ""): row.get("coefficients_hash") for row in snapshots}
    rows: List[Dict[str, Any]] = []
    for row in write_rows:
        device_id = _device_id(row.get("analyzer_device_id"))
        rows.append(
            {
                "id": stable_id("coefficient_write_event", run_db_id, "senco9", device_id),
                "run_db_id": run_db_id,
                "analyzer_id": device_id,
                "event_type": "senco9_pressure_channel_write",
                "status": str(row.get("status") or ""),
                "approved_by": str(row.get("approver") or "") or None,
                "command_summary": (
                    "SENCO9 pressure-channel C0 update; add-to-current-c0; device ID unchanged; "
                    "water/gas routes not controlled."
                ),
                "old_coefficients_hash": hash_by_device.get(device_id),
                "candidate_id": None,
                "readback": {
                    "target_senco9_values": row.get("target_senco9_values"),
                    "old_senco9_c0": row.get("old_senco9_c0"),
                    "target_senco9_c0": row.get("target_senco9_c0"),
                    "readback_verified": _truthy(row.get("readback_verified")),
                    "identity_before": _device_id(row.get("identity_before")),
                    "identity_after": _device_id(row.get("identity_after")),
                },
                "metadata": {
                    "analyzer_prefix": row.get("analyzer_prefix"),
                    "acquisition_port": row.get("port"),
                    "candidate_offset_kpa": row.get("candidate_offset_kpa"),
                    "candidate_offset_mode": row.get("candidate_offset_mode"),
                    "reviewer": row.get("reviewer"),
                    "write_applied": _truthy(row.get("write_applied")),
                    "rollback_attempted": _truthy(row.get("rollback_attempted")),
                    "controls_water_or_gas_routes": _truthy(row.get("controls_water_or_gas_routes")),
                    "writes_device_id": _truthy(row.get("writes_device_id")),
                    "writes_senco9": _truthy(row.get("writes_senco9")),
                    "pressure_channel_only": True,
                    "not_co2_h2o_fit_evidence": True,
                },
            }
        )
    return rows


def _build_reports(run_db_id: str, files: Sequence[Mapping[str, Any]]) -> List[Dict[str, Any]]:
    roles = {"pressure_channel_completion_report", "pressure_channel_completion_workbook"}
    rows: List[Dict[str, Any]] = []
    for file_row in files:
        role = str(file_row.get("artifact_role") or "")
        if role not in roles:
            continue
        rows.append(
            {
                "id": stable_id("report", run_db_id, file_row.get("path")),
                "run_db_id": run_db_id,
                "report_type": role,
                "path": str(file_row.get("path") or ""),
                "sha256": str(file_row.get("sha256") or ""),
                "status": "available",
                "generated_at": file_row.get("modified_at"),
                "metadata": {"source_artifact_id": file_row.get("id"), "pressure_channel_only": True},
            }
        )
    return rows


def _build_integrity_checks(
    *,
    run_db_id: str,
    summary_row: Mapping[str, Any],
    trace_row: Mapping[str, Any],
    gate_row: Mapping[str, Any],
    device_rows: Sequence[Mapping[str, Any]],
    files: Sequence[Mapping[str, Any]],
    write_rows: Sequence[Mapping[str, Any]],
) -> List[Dict[str, Any]]:
    required = [row for row in files if bool(row.get("required"))]
    missing_required_hashes = [
        str(row.get("artifact_role") or row.get("path") or "")
        for row in required
        if not str(row.get("sha256") or "").strip()
    ]
    all_devices_ready = bool(device_rows) and all(str(row.get("readiness_status") or "") == "pass" for row in device_rows)
    identity_safe = all(
        _device_id(row.get("identity_before")) == _device_id(row.get("analyzer_device_id"))
        and _device_id(row.get("identity_after")) == _device_id(row.get("analyzer_device_id"))
        and not _truthy(row.get("writes_device_id"))
        for row in write_rows
    )
    water_gas_untouched = (
        not _truthy(summary_row.get("controls_water_or_gas_routes"))
        and all(not _truthy(row.get("controls_water_or_gas_routes")) for row in write_rows)
    )
    checks = [
        (
            "pressure_completion_required_artifacts_hashed",
            "pass" if required and not missing_required_hashes else "fail",
            "error",
            {"required_count": len(required), "missing_required_hashes": missing_required_hashes},
        ),
        (
            "pressure_reference_traceability_pass",
            "pass" if str(trace_row.get("status") or "") == "pass" else "fail",
            "error",
            {"certificate_id": trace_row.get("certificate_id"), "status": trace_row.get("status")},
        ),
        (
            "pressure_channel_all_devices_ready",
            "pass" if all_devices_ready else "fail",
            "error",
            {
                "device_count": len(device_rows),
                "ready_device_count": sum(1 for row in device_rows if row.get("readiness_status") == "pass"),
            },
        ),
        (
            "pressure_channel_open_flow_gate_pass",
            "pass" if str(gate_row.get("status") or "") == "pass" else "fail",
            "error",
            dict(gate_row),
        ),
        (
            "device_identity_not_rewritten",
            "pass" if identity_safe else "fail",
            "error",
            {"write_rows": len(write_rows)},
        ),
        (
            "water_gas_routes_not_controlled",
            "pass" if water_gas_untouched else "fail",
            "error",
            {"summary_controls_water_or_gas_routes": summary_row.get("controls_water_or_gas_routes")},
        ),
        (
            "pressure_completion_not_co2_h2o_fit_evidence",
            "pass"
            if all(_truthy(row.get("pressure_channel_only")) and _truthy(row.get("not_co2_h2o_fit_evidence")) for row in device_rows)
            else "fail",
            "error",
            {"device_count": len(device_rows)},
        ),
    ]
    return [
        {
            "id": stable_id("integrity_check", run_db_id, name),
            "run_db_id": run_db_id,
            "check_name": name,
            "status": status,
            "severity": severity,
            "details": details,
        }
        for name, status, severity, details in checks
    ]


def _build_audit_events(
    *,
    run_db_id: str,
    summary_row: Mapping[str, Any],
    files: Sequence[Mapping[str, Any]],
    write_events: Sequence[Mapping[str, Any]],
) -> List[Dict[str, Any]]:
    return [
        {
            "id": stable_id("audit", run_db_id, "pressure_channel_completion_bundle_built", sha256_json(summary_row)),
            "run_db_id": run_db_id,
            "event_type": "pressure_channel_completion_bundle_built",
            "actor": None,
            "event_at": _now_iso(),
            "payload": {
                "artifact_count": len(files),
                "pressure_channel_status": summary_row.get("overall_status"),
                "senco9_write_event_count": len(write_events),
                "opens_com_ports": False,
                "controls_water_or_gas_routes": False,
                "controls_valves_or_pace": False,
                "indexes_prior_senco9_writes": True,
            },
        }
    ]


def build_pressure_channel_completion_evidence_bundle(
    *,
    completion_dir: str | Path,
) -> Dict[str, Any]:
    """Build a database-ready bundle from an existing pressure completion package."""

    root = Path(completion_dir).resolve()
    summary_rows = _read_csv(root / "pressure_channel_completion_summary.csv")
    device_rows = _read_csv(root / "pressure_channel_device_readiness.csv")
    trace_rows = _read_csv(root / "pressure_channel_traceability.csv")
    gate_rows = _read_csv(root / "pressure_channel_readiness_gate.csv")
    artifact_rows = _read_csv(root / "pressure_channel_completion_artifacts.csv")
    if not summary_rows:
        raise ValueError(f"Missing pressure completion summary CSV in {root}")
    if not device_rows:
        raise ValueError(f"Missing pressure device readiness CSV in {root}")

    summary_row = dict(_first(summary_rows))
    trace_row = dict(_first(trace_rows))
    gate_row = dict(_first(gate_rows))
    artifact_path_by_role = {str(row.get("artifact_role") or ""): str(row.get("path") or "") for row in artifact_rows}
    pressure_reference_path = artifact_path_by_role.get("pressure_reference_json") or ""
    pressure_reference = _load_json(pressure_reference_path) if pressure_reference_path else {}
    write_rows = _read_csv(artifact_path_by_role.get("senco9_write_summary", ""))
    old_getco_path = artifact_path_by_role.get("old_getco9_snapshot", "")
    old_getco = _load_json(old_getco_path) if old_getco_path else {}

    run_id = root.name
    run_db_id = stable_id("pressure_channel_completion_run", str(root), run_id)
    files = _build_file_rows(
        run_db_id=run_db_id,
        completion_dir=root,
        artifact_rows=artifact_rows,
        pressure_reference=pressure_reference,
    )
    devices, run_devices = _build_devices(
        run_db_id=run_db_id,
        device_rows=device_rows,
        pressure_reference=pressure_reference,
    )
    snapshots = _build_coefficient_snapshots(
        run_db_id=run_db_id,
        old_getco_path=old_getco_path,
        old_getco=old_getco,
        files=files,
    )
    write_events = _build_write_events(
        run_db_id=run_db_id,
        write_rows=write_rows,
        snapshots=snapshots,
    )
    checks = _build_integrity_checks(
        run_db_id=run_db_id,
        summary_row=summary_row,
        trace_row=trace_row,
        gate_row=gate_row,
        device_rows=device_rows,
        files=files,
        write_rows=write_rows,
    )
    evidence_status = "ready_for_open_flow_sampling" if all(row.get("status") == "pass" for row in checks) else "blocked"
    bundle = {
        "schema": "v1_5_evidence_registry",
        "schema_version": "001",
        "created_at": _now_iso(),
        "run_db_id": run_db_id,
        "run_id": run_id,
        "tables": {
            "runs": [
                {
                    "id": run_db_id,
                    "run_id": run_id,
                    "run_dir": str(root),
                    "plan_id": "v1_5_pressure_channel_completion",
                    "plan_version": "2026-05-25",
                    "analyzer_id": "multi_analyzer_pressure_channel",
                    "operator_name": None,
                    "config_hash": None,
                    "package_status": str(summary_row.get("overall_status") or ""),
                    "package_blockers": [] if evidence_status != "blocked" else ["pressure_channel_completion_blocked"],
                    "evidence_status": evidence_status,
                    "metadata": {
                        "component": "pressure",
                        "pressure_channel_only": True,
                        "ready_for_open_flow_sampling": _truthy(summary_row.get("ready_for_open_flow_sampling")),
                        "ready_for_co2_h2o_candidate_review": False,
                        "can_write_co2_h2o_coefficients": False,
                        "opens_com_ports": False,
                        "controls_water_or_gas_routes": False,
                        "controls_valves_or_pace": False,
                        "indexes_prior_senco9_writes": True,
                        "writes_pressure_senco9": bool(write_events),
                        "not_co2_h2o_fit_evidence": True,
                    },
                }
            ],
            "devices": devices,
            "run_devices": run_devices,
            "standard_gases": [],
            "reference_certificates": _build_reference_certificate(
                run_db_id=run_db_id,
                pressure_reference=pressure_reference,
                devices=devices,
            ),
            "calibration_points": _build_calibration_points(
                run_db_id=run_db_id,
                device_rows=device_rows,
            ),
            "sample_files": files,
            "qc_results": _build_qc_rows(
                run_db_id=run_db_id,
                summary_row=summary_row,
                trace_row=trace_row,
                gate_row=gate_row,
                device_rows=device_rows,
                files=files,
            ),
            "coefficient_snapshots": snapshots,
            "coefficient_candidates": [],
            "coefficient_write_events": write_events,
            "reports": _build_reports(run_db_id, files),
            "audit_events": _build_audit_events(
                run_db_id=run_db_id,
                summary_row=summary_row,
                files=files,
                write_events=write_events,
            ),
            "evidence_integrity_checks": checks,
        },
    }
    for table_name in TABLE_NAMES:
        bundle["tables"].setdefault(table_name, [])
    return bundle
