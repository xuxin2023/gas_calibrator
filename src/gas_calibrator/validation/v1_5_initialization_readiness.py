"""Offline V1.5 initialization-readiness checks.

This module reads existing evidence/config files only. It does not open COM
ports, control PACE/valves/routes, or write analyzer coefficients.
"""

from __future__ import annotations

import csv
import hashlib
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence


REQUIRED_GETCO_GROUPS = tuple(range(1, 10))
AUXILIARY_GROUPS = ("senco5", "senco6", "senco78", "senco9")
AUXILIARY_EVENT_FILES = {
    "senco5": "senco5_neutral_write_events.csv",
    "senco6": "senco6_neutral_write_events.csv",
    "senco78": "senco78_neutral_write_events.csv",
    "senco9": "senco9_clear_write_events.csv",
}
TEMPERATURE_REVIEW_FILE = "temperature_current_point_review.csv"
ARCHIVE_CONFIRMATION_FILE = "v1_5_initialization_archive_confirmation.json"
PRESSURE_HARDWARE_KEYS = ("pressure_controller", "pressure_gauge")
PASS_STATUSES = {
    "pass",
    "passed",
    "success",
    "ok",
    "cleared",
    "neutralized",
    "already_neutral",
    "already_clear",
}
TEMPERATURE_REVIEW_PASS_STATUSES = {"pass", "single_point_repair_written"}
TEMPERATURE_REVIEW_WARNING_STATUSES = {"reference_equivalence_required"}


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _load_json(path: str | Path | None) -> Optional[Dict[str, Any]]:
    if not path:
        return None
    source = Path(path)
    if not source.exists():
        return None
    return json.loads(source.read_text(encoding="utf-8"))


def _load_csv(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def _sha256_file(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value or "").strip().lower() in {"1", "true", "yes", "y", "on", "pass", "ok"}


def _explicit_false(value: Any) -> bool:
    if isinstance(value, bool):
        return value is False
    return str(value or "").strip().lower() in {"0", "false", "no", "n", "off"}


def _as_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _nested_get(mapping: Mapping[str, Any], path: str, default: Any = None) -> Any:
    current: Any = mapping
    for part in path.split("."):
        if not isinstance(current, Mapping):
            return default
        current = current.get(part)
    return default if current is None else current


def _check(
    name: str,
    status: str,
    reasons: Sequence[str] | None = None,
    *,
    stage: str = "initialization",
    evidence_role: str = "",
    path: str = "",
    details: Mapping[str, Any] | None = None,
) -> Dict[str, Any]:
    return {
        "check": name,
        "status": status,
        "reasons": ";".join(str(item) for item in reasons or [] if str(item)),
        "stage": stage,
        "evidence_role": evidence_role,
        "path": path,
        "details": dict(details or {}),
    }


def _discover_config_analyzers(config: Mapping[str, Any] | None) -> List[Dict[str, str]]:
    if not isinstance(config, Mapping):
        return []
    devices = config.get("devices", {})
    if not isinstance(devices, Mapping):
        return []
    out: List[Dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    gas_analyzers = devices.get("gas_analyzers")
    if isinstance(gas_analyzers, list):
        for index, item in enumerate(gas_analyzers, start=1):
            if not isinstance(item, Mapping) or _explicit_false(item.get("enabled", True)):
                continue
            row = {
                "name": str(item.get("name") or f"GA{index:02d}"),
                "device_id": str(item.get("device_id") or item.get("id") or "").strip(),
                "port": str(item.get("port") or "").strip(),
            }
            key = (row["device_id"], row["port"])
            if key not in seen:
                out.append(row)
                seen.add(key)
    single = devices.get("gas_analyzer")
    if isinstance(single, Mapping) and not _explicit_false(single.get("enabled", True)):
        row = {
            "name": str(single.get("name") or "GA01"),
            "device_id": str(single.get("device_id") or single.get("id") or "").strip(),
            "port": str(single.get("port") or "").strip(),
        }
        key = (row["device_id"], row["port"])
        if key not in seen:
            out.append(row)
            seen.add(key)
    return out


def _find_getco_snapshot(run_dir: Path, getco_snapshot_dir: str | Path | None) -> Optional[Path]:
    if getco_snapshot_dir:
        candidate = Path(getco_snapshot_dir) / "old_component_coefficients_snapshot.json"
        if candidate.exists():
            return candidate
        source = Path(getco_snapshot_dir)
        if source.is_file():
            return source
    direct = run_dir / "coefficient_epoch_0_getco_snapshot" / "old_component_coefficients_snapshot.json"
    if direct.exists():
        return direct
    matches = sorted(run_dir.glob("**/old_component_coefficients_snapshot.json"), key=lambda p: p.stat().st_mtime, reverse=True)
    return matches[0] if matches else None


def _snapshot_device_map(payload: Mapping[str, Any] | None) -> Dict[str, Mapping[str, Any]]:
    if not isinstance(payload, Mapping):
        return {}
    source: Mapping[str, Any]
    if isinstance(payload.get("devices"), Mapping):
        source = payload["devices"]  # type: ignore[assignment]
    else:
        source = payload
    out: Dict[str, Mapping[str, Any]] = {}
    for key, value in source.items():
        if not isinstance(value, Mapping):
            continue
        device_id = str(value.get("analyzer_device_id") or value.get("device_id") or key).strip()
        if device_id:
            out[device_id] = value
    return out


def _expected_device_ids(config_devices: Sequence[Mapping[str, str]], snapshot_devices: Mapping[str, Any]) -> List[str]:
    ids = [str(item.get("device_id") or "").strip() for item in config_devices if str(item.get("device_id") or "").strip()]
    if ids:
        return ids
    return sorted(snapshot_devices)


def _assess_getco_snapshot(
    *,
    run_dir: Path,
    config: Mapping[str, Any] | None,
    getco_snapshot_dir: str | Path | None,
) -> tuple[Dict[str, Any], Dict[str, Mapping[str, Any]], List[str]]:
    path = _find_getco_snapshot(run_dir, getco_snapshot_dir)
    if not path:
        return _check(
            "getco1_to_getco9_epoch0_snapshot",
            "fail",
            ["old_component_coefficients_snapshot_missing"],
            evidence_role="epoch0_getco_snapshot",
        ), {}, []

    payload = _load_json(path)
    devices = _snapshot_device_map(payload)
    expected_ids = _expected_device_ids(_discover_config_analyzers(config), devices)
    reasons: List[str] = []
    for device_id in expected_ids:
        item = devices.get(device_id)
        if not item:
            reasons.append(f"{device_id}:device_missing_in_getco_snapshot")
            continue
        missing = [group for group in REQUIRED_GETCO_GROUPS if f"GETCO{group}_before" not in item]
        if missing:
            reasons.append(f"{device_id}:missing_GETCO_groups={','.join(str(group) for group in missing)}")

    if not expected_ids:
        reasons.append("no_expected_or_snapshot_analyzers")
    status = "pass" if not reasons else "fail"
    return _check(
        "getco1_to_getco9_epoch0_snapshot",
        status,
        reasons,
        evidence_role="epoch0_getco_snapshot",
        path=str(path.resolve()),
        details={"device_count": len(expected_ids), "expected_device_ids": expected_ids},
    ), devices, expected_ids


def _find_aux_dir(run_dir: Path, aux_neutralization_dir: str | Path | None) -> Optional[Path]:
    if aux_neutralization_dir:
        source = Path(aux_neutralization_dir)
        return source if source.exists() else None
    direct = run_dir / "auxiliary_senco56789_neutralization"
    if direct.exists():
        return direct
    matches = [path for path in run_dir.glob("**/auxiliary_senco56789_neutralization") if path.is_dir()]
    return max(matches, key=lambda p: p.stat().st_mtime) if matches else None


def _row_device_id(row: Mapping[str, Any]) -> str:
    for key in ("device_id", "analyzer_device_id", "target_device_id", "runtime_device_id"):
        value = str(row.get(key) or "").strip()
        if value:
            return value
    return ""


def _row_passed(row: Mapping[str, Any]) -> bool:
    status = str(row.get("status") or row.get("result") or row.get("write_status") or "").strip().lower()
    if status in PASS_STATUSES:
        return True
    for key in ("write_applied", "readback_verified", "neutralized", "cleared"):
        if _truthy(row.get(key)):
            return True
    return False


def _row_senco_group(row: Mapping[str, Any]) -> str:
    raw = str(row.get("senco_group") or row.get("group") or row.get("channel") or "").strip().upper()
    if raw in {"7", "S7"}:
        return "SENCO7"
    if raw in {"8", "S8"}:
        return "SENCO8"
    if raw.startswith("SENCO7"):
        return "SENCO7"
    if raw.startswith("SENCO8"):
        return "SENCO8"
    return raw


def _find_temperature_review_csv(run_dir: Path, aux_dir: Optional[Path]) -> Optional[Path]:
    candidates: List[Path] = []
    if aux_dir:
        candidates.extend(
            [
                aux_dir / "temperature_current_point_review" / TEMPERATURE_REVIEW_FILE,
                aux_dir / TEMPERATURE_REVIEW_FILE,
            ]
        )
    candidates.extend(
        [
            run_dir / "temperature_current_point_review" / TEMPERATURE_REVIEW_FILE,
            run_dir / TEMPERATURE_REVIEW_FILE,
        ]
    )
    candidates.extend(run_dir.glob(f"**/{TEMPERATURE_REVIEW_FILE}"))
    existing = []
    seen: set[Path] = set()
    for candidate in candidates:
        resolved = candidate.resolve()
        if resolved in seen or not candidate.exists():
            continue
        seen.add(resolved)
        existing.append(candidate)
    if not existing:
        return None
    return max(existing, key=lambda path: path.stat().st_mtime)


def _find_initialization_archive_confirmation(run_dir: Path) -> Optional[Path]:
    candidates = [
        run_dir / "initialization_archive_confirmation_20260611" / ARCHIVE_CONFIRMATION_FILE,
        run_dir / "initialization_archive_confirmation" / ARCHIVE_CONFIRMATION_FILE,
        run_dir / ARCHIVE_CONFIRMATION_FILE,
    ]
    candidates.extend(run_dir.glob(f"**/{ARCHIVE_CONFIRMATION_FILE}"))
    existing: List[Path] = []
    seen: set[Path] = set()
    for candidate in candidates:
        resolved = candidate.resolve()
        if resolved in seen or not candidate.exists():
            continue
        seen.add(resolved)
        existing.append(candidate)
    if not existing:
        return None
    return max(existing, key=lambda path: path.stat().st_mtime)


def _archive_device_id(row: Mapping[str, Any]) -> str:
    for key in ("runtime_device_id", "device_id", "analyzer_device_id", "configured_device_id"):
        value = str(row.get(key) or "").strip()
        if value:
            return value
    return ""


def _archive_device_rows(payload: Mapping[str, Any]) -> Dict[str, Mapping[str, Any]]:
    rows = payload.get("device_rows")
    if not isinstance(rows, list):
        return {}
    out: Dict[str, Mapping[str, Any]] = {}
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        device_id = _archive_device_id(row)
        if device_id:
            out[device_id] = row
    return out


def _assess_auxiliary_archive_confirmation(
    *,
    run_dir: Path,
    group: str,
    expected_device_ids: Sequence[str],
) -> Optional[Dict[str, Any]]:
    path = _find_initialization_archive_confirmation(run_dir)
    if not path:
        return None
    payload = _load_json(path)
    if not isinstance(payload, Mapping):
        return _check(
            f"{group}_archive_snapshot_evidence",
            "fail",
            [f"{ARCHIVE_CONFIRMATION_FILE}_invalid_json"],
            evidence_role="initialization_archive_confirmation",
            path=str(path.resolve()),
        )

    group_to_field = {"senco5": "s5_epoch0", "senco6": "s6_epoch0", "senco9": "s9_epoch0"}
    field_name = group_to_field.get(group)
    if not field_name:
        return None

    row_by_id = _archive_device_rows(payload)
    archived_ids = [str(item).strip() for item in payload.get("device_ids", []) if str(item).strip()] if isinstance(payload.get("device_ids"), list) else sorted(row_by_id)
    devices_to_check = list(expected_device_ids) or archived_ids
    reasons: List[str] = []
    if str(payload.get("conclusion_status") or "").strip().lower() not in PASS_STATUSES:
        reasons.append(f"archive_conclusion_status={payload.get('conclusion_status') or 'missing'}")
    if not _truthy(payload.get("all_identity_verified")):
        reasons.append("archive_all_identity_verified_false_or_missing")
    if not _truthy(payload.get("all_getco1_to_9_complete")):
        reasons.append("archive_all_getco1_to_9_complete_false_or_missing")
    if not devices_to_check:
        reasons.append("no_expected_or_archive_devices")

    confirmed_ids: List[str] = []
    for device_id in devices_to_check:
        row = row_by_id.get(device_id)
        if not row:
            reasons.append(f"{device_id}:device_missing_in_archive_confirmation")
            continue
        values = row.get(field_name)
        if not isinstance(values, list) or not values:
            reasons.append(f"{device_id}:{field_name}_missing")
            continue
        confirmed_ids.append(device_id)

    if group == "senco9":
        pressure_status = str(payload.get("pressure_channel_status") or "").strip().lower()
        if pressure_status != "senco9_written_and_post_write_verified":
            reasons.append(f"pressure_channel_status={pressure_status or 'missing'}")
        pressure_ids = {
            str(item).strip()
            for item in payload.get("pressure_channel_device_ids", [])
            if str(item).strip()
        } if isinstance(payload.get("pressure_channel_device_ids"), list) else set()
        missing_pressure_ids = [device_id for device_id in devices_to_check if device_id not in pressure_ids]
        if missing_pressure_ids:
            reasons.append(f"pressure_channel_device_ids_missing={','.join(missing_pressure_ids)}")

    return _check(
        f"{group}_archive_snapshot_evidence",
        "pass" if not reasons else "fail",
        reasons,
        evidence_role="initialization_archive_confirmation",
        path=str(path.resolve()),
        details={
            "confirmed_device_ids": sorted(confirmed_ids),
            "source_snapshot_dir": str(payload.get("source_snapshot_dir") or ""),
            "archive_decision": str(payload.get("archive_decision") or ""),
            "pressure_channel_status": str(payload.get("pressure_channel_status") or ""),
            "physical_meaning": (
                "Archive confirmation is read-only evidence that identity binding and GETCO1-9 "
                "snapshots were completed. S5/S6 archive snapshots allow downstream fitting to "
                "model output-layer trims without requiring duplicate neutralization event CSVs; "
                "S9 also requires pressure-channel write and post-write verification."
            ),
        },
    )


def _assess_senco78_temperature_review(
    *,
    run_dir: Path,
    expected_device_ids: Sequence[str],
    aux_dir: Optional[Path],
) -> Optional[Dict[str, Any]]:
    path = _find_temperature_review_csv(run_dir, aux_dir)
    if not path:
        return None
    rows = _load_csv(path)
    if not rows:
        return _check(
            "senco78_temperature_current_point_review_evidence",
            "fail",
            [f"{TEMPERATURE_REVIEW_FILE}_empty"],
            evidence_role="temperature_input_quantity_review",
            path=str(path.resolve()),
        )

    seen_by_device: Dict[str, set[str]] = {}
    passed_devices: set[str] = set()
    reference_equivalence_devices: set[str] = set()
    reasons: List[str] = []
    review_reasons: List[str] = []
    for row in rows:
        device_id = _row_device_id(row)
        group = _row_senco_group(row)
        status = str(row.get("status") or "").strip().lower()
        if device_id:
            seen_by_device.setdefault(device_id, set()).add(group)
        if status in TEMPERATURE_REVIEW_PASS_STATUSES:
            if device_id:
                passed_devices.add(device_id)
            continue
        if status in TEMPERATURE_REVIEW_WARNING_STATUSES:
            if device_id:
                reference_equivalence_devices.add(device_id)
            reason = str(row.get("reason") or "temperature_reference_equivalence_required").strip()
            review_reasons.append(f"{device_id or 'unknown'}:{group or 'unknown'}:reference_equivalence_required:{reason}")
            continue
        reasons.append(f"{device_id or 'unknown'}:{group or 'unknown'}:temperature_review_status={status or 'missing'}")

    devices_to_check = list(expected_device_ids) or sorted(seen_by_device)
    if not devices_to_check:
        reasons.append("no_expected_or_review_analyzers")
    for device_id in devices_to_check:
        groups = seen_by_device.get(device_id, set())
        missing = [group for group in ("SENCO7", "SENCO8") if group not in groups]
        if missing:
            reasons.append(f"{device_id}:missing_temperature_review_groups={','.join(missing)}")

    status = "fail" if reasons else ("warning" if review_reasons else "pass")
    return _check(
        "senco78_temperature_current_point_review_evidence",
        status,
        reasons or review_reasons,
        evidence_role="temperature_input_quantity_review",
        path=str(path.resolve()),
        details={
            "passed_device_ids": sorted(passed_devices),
            "reference_equivalence_device_ids": sorted(reference_equivalence_devices),
            "review_row_count": len(rows),
            "physical_meaning": (
                "SENCO7/SENCO8 are temperature input corrections. A current-point review can "
                "prove either that the coefficients are acceptable, that a hard repair is required, "
                "or that the external thermometer is not thermally equivalent to the analyzer body."
            ),
        },
    )


def _assess_auxiliary_neutralization(
    *,
    run_dir: Path,
    expected_device_ids: Sequence[str],
    aux_neutralization_dir: str | Path | None,
    continuation_recovery: bool,
) -> List[Dict[str, Any]]:
    aux_dir = _find_aux_dir(run_dir, aux_neutralization_dir)
    checks: List[Dict[str, Any]] = []
    for group in AUXILIARY_GROUPS:
        if group == "senco78":
            temp_review_check = _assess_senco78_temperature_review(
                run_dir=run_dir,
                expected_device_ids=expected_device_ids,
                aux_dir=aux_dir,
            )
            if temp_review_check is not None:
                checks.append(temp_review_check)
                continue
        file_name = AUXILIARY_EVENT_FILES[group]
        path = aux_dir / file_name if aux_dir else None
        if not path or not path.exists():
            archive_check = _assess_auxiliary_archive_confirmation(
                run_dir=run_dir,
                group=group,
                expected_device_ids=expected_device_ids,
            )
            if archive_check is not None:
                checks.append(archive_check)
                continue
            status = "warning" if continuation_recovery else "fail"
            checks.append(
                _check(
                    f"{group}_neutralization_evidence",
                    status,
                    [f"{file_name}_missing"],
                    evidence_role="auxiliary_coefficient_neutralization",
                    path=str(path.resolve()) if path else "",
                )
            )
            continue
        rows = _load_csv(path)
        seen_passed = {_row_device_id(row) for row in rows if _row_passed(row)}
        seen_passed.discard("")
        reasons = [
            f"{device_id}:neutralization_readback_missing_or_failed"
            for device_id in expected_device_ids
            if device_id not in seen_passed
        ]
        if not expected_device_ids and not seen_passed:
            reasons.append("no_passed_neutralization_rows")
        checks.append(
            _check(
                f"{group}_neutralization_evidence",
                "pass" if not reasons else ("warning" if continuation_recovery else "fail"),
                reasons,
                evidence_role="auxiliary_coefficient_neutralization",
                path=str(path.resolve()),
                details={"passed_device_ids": sorted(seen_passed)},
            )
        )
    return checks


def _assess_config(config: Mapping[str, Any] | None) -> List[Dict[str, Any]]:
    if not isinstance(config, Mapping):
        return [_check("initialization_runtime_config", "warning", ["config_missing"])]

    metadata = config.get("metadata", {}) if isinstance(config.get("metadata"), Mapping) else {}
    workflow = config.get("workflow", {}) if isinstance(config.get("workflow"), Mapping) else {}
    reasons: List[str] = []
    if _truthy(metadata.get("writes_senco")):
        reasons.append("metadata_writes_senco_true")
    if _truthy(metadata.get("writes_device_id")):
        reasons.append("metadata_writes_device_id_true")
    if _truthy(_nested_get(workflow, "controlled_write")):
        reasons.append("workflow_controlled_write_true")
    if _truthy(_nested_get(workflow, "startup_pressure_sensor_calibration.apply_write")):
        reasons.append("startup_pressure_apply_write_true")

    startup_allowed = {str(item).upper() for item in metadata.get("startup_allowed_analyzer_commands", []) if str(item)}
    startup_forbidden = {str(item).upper() for item in metadata.get("startup_forbidden_analyzer_commands", []) if str(item)}
    required_forbidden = {"ID", "SENCO", "CLEARSENCO", "SETPOW", "SETILLUM", "SETCO2"}
    missing_forbidden = sorted(required_forbidden - startup_forbidden) if startup_forbidden else sorted(required_forbidden)
    if missing_forbidden:
        reasons.append(f"startup_forbidden_commands_not_declared={','.join(missing_forbidden)}")
    unexpected_allowed = sorted(startup_allowed & required_forbidden)
    if unexpected_allowed:
        reasons.append(f"forbidden_commands_declared_allowed={','.join(unexpected_allowed)}")

    analyzer_init = _nested_get(workflow, "analyzer_mode2_init", {})
    if isinstance(analyzer_init, Mapping):
        command_gap_s = _as_float(analyzer_init.get("command_gap_s"), 0.0)
        reapply_delay_s = _as_float(analyzer_init.get("reapply_delay_s"), 0.0)
        if command_gap_s < 1.0:
            reasons.append(f"analyzer_mode2_init_command_gap_too_short={command_gap_s:g}s")
        if reapply_delay_s and reapply_delay_s < 1.0:
            reasons.append(f"analyzer_mode2_init_reapply_delay_too_short={reapply_delay_s:g}s")
    details = {
        "allowed_startup_commands": sorted(startup_allowed),
        "forbidden_startup_commands": sorted(startup_forbidden),
        "analyzer_mode2_init": analyzer_init if isinstance(analyzer_init, Mapping) else {},
    }
    return [
        _check(
            "initialization_runtime_config",
            "pass" if not reasons else "fail",
            reasons,
            evidence_role="runtime_config",
            details=details,
        )
    ]


def _pressure_device_present(config: Mapping[str, Any] | None, key: str) -> tuple[Optional[bool], str]:
    if not isinstance(config, Mapping):
        return None, "config_missing"
    devices = config.get("devices", {})
    if not isinstance(devices, Mapping):
        return None, "devices_config_missing"
    item = devices.get(key)
    if not isinstance(item, Mapping):
        return None, f"{key}_config_missing"
    if "present" in item and _explicit_false(item.get("present")):
        return False, f"{key}_present_false"
    if "enabled" in item and _explicit_false(item.get("enabled")):
        return False, f"{key}_enabled_false"
    if item.get("port"):
        return True, ""
    return None, f"{key}_port_missing"


def _assess_pressure_hardware(
    *,
    config: Mapping[str, Any] | None,
    pressure_hardware_missing: bool,
) -> Dict[str, Any]:
    if pressure_hardware_missing:
        return _check(
            "pressure_hardware_presence",
            "blocked",
            ["operator_declared_pressure_controller_or_gauge_missing"],
            stage="pressure_precheck",
            evidence_role="hardware_presence_gate",
            details={
                "pressure_controller_present": False,
                "pressure_gauge_present": False,
                "physical_meaning": (
                    "SENCO9 pressure calibration requires both PACE pressure control and COM22 "
                    "reference pressure evidence before CO2/H2O formal calibration."
                ),
            },
        )

    reasons: List[str] = []
    details: Dict[str, Any] = {}
    for key in PRESSURE_HARDWARE_KEYS:
        present, reason = _pressure_device_present(config, key)
        details[f"{key}_present"] = present
        if present is False:
            reasons.append(reason)
        elif present is None:
            reasons.append(reason)

    if any(details.get(f"{key}_present") is False for key in PRESSURE_HARDWARE_KEYS):
        status = "blocked"
    elif any(details.get(f"{key}_present") is None for key in PRESSURE_HARDWARE_KEYS):
        status = "warning"
    else:
        status = "pass"
    details["physical_meaning"] = (
        "Pressure hardware must be available before direct SENCO9 calibration and pressure-channel verification."
    )
    return _check(
        "pressure_hardware_presence",
        status,
        reasons,
        stage="pressure_precheck",
        evidence_role="hardware_presence_gate",
        details=details,
    )


def build_initialization_readiness_model(
    *,
    run_dir: str | Path,
    config_path: str | Path | None = None,
    getco_snapshot_dir: str | Path | None = None,
    aux_neutralization_dir: str | Path | None = None,
    continuation_recovery: bool = False,
    pressure_hardware_missing: bool = False,
) -> Dict[str, Any]:
    """Build an offline V1.5 initialization-readiness model."""

    root = Path(run_dir).resolve()
    config = _load_json(config_path) if config_path else None
    checks: List[Dict[str, Any]] = [
        _check(
            "run_dir",
            "pass" if root.exists() and root.is_dir() else "fail",
            [] if root.exists() and root.is_dir() else ["run_dir_missing"],
            path=str(root),
        )
    ]
    checks.extend(_assess_config(config))
    checks.append(_assess_pressure_hardware(config=config, pressure_hardware_missing=pressure_hardware_missing))
    getco_check, _devices, expected_device_ids = _assess_getco_snapshot(
        run_dir=root,
        config=config,
        getco_snapshot_dir=getco_snapshot_dir,
    )
    checks.append(getco_check)
    checks.extend(
        _assess_auxiliary_neutralization(
            run_dir=root,
            expected_device_ids=expected_device_ids,
            aux_neutralization_dir=aux_neutralization_dir,
            continuation_recovery=continuation_recovery,
        )
    )

    failures = [row for row in checks if row["status"] == "fail"]
    pressure_blockers = [row for row in checks if row["check"] == "pressure_hardware_presence" and row["status"] == "blocked"]
    warnings = [row for row in checks if row["status"] == "warning"]
    if failures:
        readiness_status = "initialization_blocked"
    elif pressure_blockers:
        readiness_status = "pressure_hardware_blocked"
    elif warnings and continuation_recovery:
        readiness_status = "continuation_requires_review"
    elif warnings:
        readiness_status = "initialization_ready_with_warnings"
    else:
        readiness_status = "initialization_ready"

    return {
        "created_at": _now(),
        "readiness_status": readiness_status,
        "run_dir": str(root),
        "config_path": str(Path(config_path).resolve()) if config_path else "",
        "continuation_recovery": bool(continuation_recovery),
        "pressure_hardware_missing": bool(pressure_hardware_missing),
        "opens_com_ports": False,
        "controls_water_or_gas_routes": False,
        "controls_valves_or_pace": False,
        "writes_coefficients": False,
        "writes_device_id": False,
        "not_real_acceptance_evidence": True,
        "expected_device_ids": expected_device_ids,
        "checks": checks,
        "next_actions": _next_actions(readiness_status, failures, warnings),
    }


def _next_actions(
    readiness_status: str,
    failures: Sequence[Mapping[str, Any]],
    warnings: Sequence[Mapping[str, Any]],
) -> List[Dict[str, Any]]:
    temperature_reference_warnings = [
        row
        for row in warnings
        if row.get("check") == "senco78_temperature_current_point_review_evidence"
        and "reference_equivalence" in str(row.get("reasons") or "")
    ]
    if readiness_status == "initialization_ready":
        return [
            {
                "action": "proceed_to_pressure_or_component_stage",
                "owner": "operator+engineer",
                "meaning": "初始化证据完整，可进入压力/温度/开放流通采样阶段。",
            }
        ]
    if readiness_status == "continuation_requires_review":
        actions = [
            {
                "action": "review_continuation_evidence_before_accepting_samples",
                "owner": "engineer+reviewer",
                "meaning": "当前可作为恢复/续跑证据，但正式评审前必须补齐或人工确认 S5-S9/GETCO 初始化证据。",
            }
        ]
        if temperature_reference_warnings:
            actions.append(
                {
                    "action": "review_temperature_reference_equivalence",
                    "owner": "engineer+reviewer",
                    "meaning": (
                        "SENCO7/SENCO8 没有硬故障，但当前数字测温仪与分析仪热状态不等效；"
                        "不要据此做单点温度写入，应使用多温度证据或重新确认测温位置。"
                    ),
                }
            )
        return actions
    if readiness_status == "initialization_ready_with_warnings":
        actions = [
            {
                "action": "review_initialization_warnings_before_formal_sampling",
                "owner": "engineer+reviewer",
                "meaning": "初始化没有硬失败，但存在需要审核的警告；审核通过后再进入正式采样或系数计算。",
            }
        ]
        if temperature_reference_warnings:
            actions.append(
                {
                    "action": "do_not_single_point_repair_temperature_reference_offset",
                    "owner": "engineer+reviewer",
                    "meaning": (
                        "当前温度差异更像参考位置/分析仪自热造成的等效性问题，不是温度系数硬故障；"
                        "禁止把这类共态偏差直接写入 SENCO7/SENCO8。"
                    ),
                }
            )
        return actions
    if readiness_status == "pressure_hardware_blocked":
        return [
            {
                "action": "wait_for_pressure_hardware_then_run_senco9",
                "owner": "operator+engineer",
                "meaning": "压力控制器或数字压力计缺席，自动流程必须停在初始化后；只能继续数据库、报告、证据审核等离线工作。",
            }
        ]
    return [
        {
            "action": "repair_initialization_evidence",
            "owner": "engineer",
            "meaning": "补齐 GETCO1-9 备份、S5-S9 中性化读回、禁写配置或设备身份绑定后再进入正式采样。",
            "blocked_checks": [row.get("check") for row in failures] or [row.get("check") for row in warnings],
        }
    ]


def render_initialization_readiness_markdown(model: Mapping[str, Any]) -> str:
    lines = [
        "# V1.5 Initialization Readiness",
        "",
        f"- readiness_status: `{model.get('readiness_status')}`",
        f"- run_dir: `{model.get('run_dir')}`",
        f"- opens_com_ports: `{model.get('opens_com_ports')}`",
        f"- writes_coefficients: `{model.get('writes_coefficients')}`",
        f"- writes_device_id: `{model.get('writes_device_id')}`",
        f"- continuation_recovery: `{model.get('continuation_recovery')}`",
        f"- pressure_hardware_missing: `{model.get('pressure_hardware_missing')}`",
        "",
        "## Physical Meaning",
        "",
        "V1.5 初始化不是普通通信准备，而是冻结设备身份和旧系数状态，再把会污染主拟合的辅助层归零或中性化。",
        "GETCO1-9 是 epoch-0 证据；S5/S6 是最终 CO2/H2O 输出层线性修正；S7/S8 影响温度输入；S9 影响压力输入。",
        "温度当前点评审若提示 reference_equivalence_required，表示外部测温参考与分析仪热状态不等效，需要审核测温位置或多温度证据，不能直接写成单点温度系数。",
        "",
        "## Checks",
        "",
    ]
    for row in model.get("checks") or []:
        reason = row.get("reasons") or ""
        suffix = f" - {reason}" if reason else ""
        lines.append(f"- `{row.get('check')}`: `{row.get('status')}`{suffix}")
    lines.extend(["", "## Next Actions", ""])
    for row in model.get("next_actions") or []:
        lines.append(f"- `{row.get('action')}`: {row.get('meaning')}")
    lines.append("")
    return "\n".join(lines)


def _file_artifact_row(
    *,
    path: str | Path,
    artifact_role: str,
    required: bool,
    evidence_status: str,
    source_check: str = "",
    stage: str = "",
    device_ids: Sequence[str] = (),
    physical_meaning: str = "",
    metadata: Mapping[str, Any] | None = None,
) -> Optional[Dict[str, Any]]:
    source = Path(path)
    if not source.exists() or not source.is_file():
        return None
    stat = source.stat()
    return {
        "artifact_role": artifact_role,
        "path": str(source.resolve()),
        "sha256": _sha256_file(source),
        "size_bytes": stat.st_size,
        "modified_at": datetime.fromtimestamp(stat.st_mtime).isoformat(timespec="seconds"),
        "required": bool(required),
        "evidence_status": evidence_status,
        "source_check": source_check,
        "stage": stage,
        "device_ids": ",".join(str(item) for item in device_ids if str(item)),
        "physical_meaning": physical_meaning,
        "metadata_json": json.dumps(dict(metadata or {}), ensure_ascii=False, sort_keys=True, default=str),
    }


def _check_physical_meaning(row: Mapping[str, Any]) -> str:
    details = row.get("details")
    if isinstance(details, Mapping):
        text = str(details.get("physical_meaning") or "").strip()
        if text:
            return text
    role = str(row.get("evidence_role") or "")
    if role == "epoch0_getco_snapshot":
        return "GETCO1-9 epoch-0 snapshot freezes the analyzer coefficients before any calibration repair."
    if role == "initialization_archive_confirmation":
        return "Archive confirmation proves identity binding, GETCO completeness, and S5/S6/S9 initialization status for this batch."
    if role == "temperature_input_quantity_review":
        return "Temperature review protects CO2/H2O fitting from absorbing analyzer temperature-channel errors."
    if role == "auxiliary_coefficient_neutralization":
        return "Auxiliary coefficient evidence prevents output-layer trims from silently contaminating main CO2/H2O fitting."
    return "Initialization readiness evidence used for offline audit and database traceability."


def build_initialization_evidence_index_rows(
    model: Mapping[str, Any],
    *,
    generated_paths: Mapping[str, str | Path] | None = None,
) -> List[Dict[str, Any]]:
    """Build file-index rows for initialization readiness evidence.

    The rows are sidecar-only and are generated from existing files. This
    function does not open COM ports, control routes, or write coefficients.
    """

    expected_ids = [str(item) for item in model.get("expected_device_ids") or [] if str(item)]
    rows: List[Dict[str, Any]] = []
    seen: set[tuple[str, str, str]] = set()

    for key, path in (generated_paths or {}).items():
        role = {
            "json": "initialization_readiness_model",
            "markdown": "initialization_readiness_report",
            "database_sidecar": "initialization_database_sidecar",
        }.get(str(key), f"initialization_{key}")
        row = _file_artifact_row(
            path=path,
            artifact_role=role,
            required=True,
            evidence_status=str(model.get("readiness_status") or ""),
            source_check="initialization_readiness_export",
            stage="initialization",
            device_ids=expected_ids,
            physical_meaning="Generated initialization readiness artifact for audit, report reconstruction, and database indexing.",
            metadata={"source": "write_initialization_readiness_report"},
        )
        if row:
            token = (row["artifact_role"], row["path"], row["source_check"])
            if token not in seen:
                rows.append(row)
                seen.add(token)

    config_path = str(model.get("config_path") or "").strip()
    if config_path:
        row = _file_artifact_row(
            path=config_path,
            artifact_role="initialization_runtime_config",
            required=True,
            evidence_status=str(model.get("readiness_status") or ""),
            source_check="initialization_runtime_config",
            stage="initialization",
            device_ids=expected_ids,
            physical_meaning="Runtime config binds device IDs, serial ports, command gaps, no-write policy, and pressure-hardware declarations.",
            metadata={"source": "model.config_path"},
        )
        if row:
            token = (row["artifact_role"], row["path"], row["source_check"])
            if token not in seen:
                rows.append(row)
                seen.add(token)

    for check in model.get("checks") or []:
        if not isinstance(check, Mapping):
            continue
        path = str(check.get("path") or "").strip()
        if not path:
            continue
        role = str(check.get("evidence_role") or check.get("check") or "initialization_evidence")
        source_check = str(check.get("check") or "")
        row = _file_artifact_row(
            path=path,
            artifact_role=role,
            required=str(check.get("status") or "") != "warning",
            evidence_status=str(check.get("status") or ""),
            source_check=source_check,
            stage=str(check.get("stage") or "initialization"),
            device_ids=expected_ids,
            physical_meaning=_check_physical_meaning(check),
            metadata={
                "check": source_check,
                "reasons": check.get("reasons") or "",
                "details": check.get("details") or {},
            },
        )
        if not row:
            continue
        token = (row["artifact_role"], row["path"], row["source_check"])
        if token in seen:
            continue
        rows.append(row)
        seen.add(token)
    return rows


def _write_evidence_index_csv(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    fieldnames = [
        "artifact_role",
        "path",
        "sha256",
        "size_bytes",
        "modified_at",
        "required",
        "evidence_status",
        "source_check",
        "stage",
        "device_ids",
        "physical_meaning",
        "metadata_json",
    ]
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key, "") for key in fieldnames})


def build_initialization_database_sidecar(
    model: Mapping[str, Any],
    artifact_rows: Sequence[Mapping[str, Any]],
) -> Dict[str, Any]:
    """Build a database-sidecar payload for initialization readiness evidence."""

    expected_ids = [str(item) for item in model.get("expected_device_ids") or [] if str(item)]
    suggested_rows: List[Dict[str, Any]] = []
    for row in artifact_rows:
        suggested_rows.append(
            {
                "db_table": "sample_files",
                "record_key": f"initialization_artifact:{row.get('artifact_role')}:{row.get('source_check')}",
                "artifact_role": row.get("artifact_role"),
                "path": row.get("path"),
                "sha256": row.get("sha256"),
                "required": row.get("required"),
                "metadata_json": {
                    "stage": row.get("stage"),
                    "evidence_status": row.get("evidence_status"),
                    "source_check": row.get("source_check"),
                    "physical_meaning": row.get("physical_meaning"),
                    "device_ids": row.get("device_ids"),
                },
            }
        )

    for check in model.get("checks") or []:
        if not isinstance(check, Mapping):
            continue
        status = str(check.get("status") or "")
        suggested_rows.append(
            {
                "db_table": "qc_results",
                "record_key": f"initialization_check:{check.get('check')}",
                "analyzer_device_id": ",".join(expected_ids),
                "candidate_status": status,
                "metadata_json": {
                    "scope": "v1_5_formal_initialization_readiness",
                    "rule_name": check.get("check"),
                    "status": status,
                    "severity": "error" if status == "fail" else ("warning" if status in {"warning", "blocked"} else "info"),
                    "reason": check.get("reasons") or "",
                    "stage": check.get("stage"),
                    "evidence_role": check.get("evidence_role"),
                    "source_path": check.get("path") or "",
                    "physical_meaning": _check_physical_meaning(check),
                },
            }
        )

    suggested_rows.append(
        {
            "db_table": "audit_events",
            "record_key": "initialization_readiness_sidecar_built",
            "event_type": "initialization_readiness_sidecar_built",
            "metadata_json": {
                "readiness_status": model.get("readiness_status"),
                "expected_device_ids": expected_ids,
                "opens_com_ports": model.get("opens_com_ports"),
                "writes_coefficients": model.get("writes_coefficients"),
                "controls_water_or_gas_routes": model.get("controls_water_or_gas_routes"),
                "artifact_count": len(artifact_rows),
                "physical_meaning": (
                    "Initialization readiness sidecar links identity, GETCO snapshots, auxiliary output trims, "
                    "pressure input status, and temperature input review into a traceable audit chain."
                ),
            },
        }
    )

    return {
        "schema": "v1_5_initialization_readiness_database_sidecar_v1",
        "generated_at": _now(),
        "sidecar_only": True,
        "run_dir": model.get("run_dir"),
        "readiness_status": model.get("readiness_status"),
        "expected_device_ids": expected_ids,
        "opens_com_ports": False,
        "controls_water_or_gas_routes": False,
        "controls_valves_or_pace": False,
        "writes_coefficients": False,
        "writes_device_id": False,
        "artifact_rows": [dict(row) for row in artifact_rows],
        "suggested_rows": suggested_rows,
        "import_notes": [
            "This sidecar is generated offline from existing initialization evidence.",
            "Use analyzer device ID as identity; COM/GA labels are transport mapping only.",
            "S5/S6/S9 archive confirmations are valid initialization evidence only when identity and GETCO completeness are present.",
            "SENCO7/SENCO8 current-point reference-equivalence warnings require reviewer judgment before formal fitting.",
        ],
    }


def write_initialization_readiness_report(
    *,
    run_dir: str | Path,
    output_dir: str | Path,
    config_path: str | Path | None = None,
    getco_snapshot_dir: str | Path | None = None,
    aux_neutralization_dir: str | Path | None = None,
    continuation_recovery: bool = False,
    pressure_hardware_missing: bool = False,
) -> Dict[str, Path]:
    model = build_initialization_readiness_model(
        run_dir=run_dir,
        config_path=config_path,
        getco_snapshot_dir=getco_snapshot_dir,
        aux_neutralization_dir=aux_neutralization_dir,
        continuation_recovery=continuation_recovery,
        pressure_hardware_missing=pressure_hardware_missing,
    )
    root = Path(output_dir).resolve()
    root.mkdir(parents=True, exist_ok=True)
    json_path = root / "v1_5_initialization_readiness.json"
    md_path = root / "v1_5_initialization_readiness.md"
    sidecar_path = root / "v1_5_initialization_database_sidecar.json"
    evidence_index_path = root / "v1_5_initialization_evidence_index.csv"
    json_path.write_text(json.dumps(model, ensure_ascii=False, indent=2), encoding="utf-8")
    md_path.write_text(render_initialization_readiness_markdown(model), encoding="utf-8-sig")
    artifact_rows = build_initialization_evidence_index_rows(
        model,
        generated_paths={"json": json_path, "markdown": md_path},
    )
    sidecar = build_initialization_database_sidecar(model, artifact_rows)
    sidecar_path.write_text(json.dumps(sidecar, ensure_ascii=False, indent=2), encoding="utf-8")
    artifact_rows = build_initialization_evidence_index_rows(
        model,
        generated_paths={"json": json_path, "markdown": md_path, "database_sidecar": sidecar_path},
    )
    _write_evidence_index_csv(evidence_index_path, artifact_rows)
    return {
        "json": json_path,
        "markdown": md_path,
        "evidence_index_csv": evidence_index_path,
        "database_sidecar_json": sidecar_path,
    }
