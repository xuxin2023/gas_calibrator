"""Fail-closed fit-input traceability gate for final V1.5 SENCO writers."""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Dict, List, Mapping, Sequence, Tuple


GLOBAL_CHECK = "fit_input_traceability_required_before_final_senco_review"
META_FILENAME = "main_senco_write_precheck_meta.json"
CHECKS_FILENAME = "candidate_write_review_checks.csv"
SUMMARY_FILENAME = "main_senco_write_precheck_summary.csv"


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
    for path, label in (
        (meta_path, "main_senco_precheck_meta_missing"),
        (checks_path, "candidate_write_review_checks_missing"),
        (summary_path, "main_senco_precheck_summary_missing"),
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

    detail = {
        "precheck_dir": str(root),
        "meta_path": str(meta_path),
        "checks_path": str(checks_path),
        "summary_path": str(summary_path),
        "component": component_key,
        "device_ids": normalized_devices,
        "package_fit_input_traceability_status": package_traceability_status,
        "package_fit_input_traceability_check_status": package_check_status,
        "fit_input_traceability_status": "pass" if not reasons else "blocked",
    }
    return not reasons, list(dict.fromkeys(reasons)), detail
