"""Offline preflight for V1.5 formal calibration evidence readiness."""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional

from .artifact_rows import normalize_sample_row
from .common import latest_artifact, load_csv_rows
from .formal_calibration_package import build_formal_calibration_package_tables
from .formal_contracts import (
    validate_formal_plan_contract,
    validate_pressure_quick_check_contract,
    validate_pressure_reference_contract,
)
from .formal_open_flow_artifacts import load_plan_snapshot, load_pressure_reference_snapshot
from .reporting import ValidationMetadata, write_validation_report


def _compact_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def _table_value(value: Any) -> Any:
    if value is None:
        return ""
    if isinstance(value, (dict, list, tuple)):
        return _compact_json(value)
    return value


def _nested_get(mapping: Mapping[str, Any], path: str, default: Any = None) -> Any:
    current: Any = mapping
    for part in path.split("."):
        if not isinstance(current, Mapping):
            return default
        current = current.get(part)
    return default if current is None else current


def _load_json(path: str | Path | None) -> Optional[Dict[str, Any]]:
    if path is None:
        return None
    json_path = Path(path)
    if not json_path.exists():
        raise FileNotFoundError(f"JSON file not found: {json_path}")
    return json.loads(json_path.read_text(encoding="utf-8"))


def _resolve_config_path(run_dir: Path, config_path: str | Path | None) -> Optional[Path]:
    if config_path:
        return Path(config_path).resolve()
    snapshot = run_dir / "runtime_config_snapshot.json"
    return snapshot if snapshot.exists() else None


def assess_no_write_config(config: Optional[Mapping[str, Any]]) -> tuple[str, List[str]]:
    if config is None:
        return "fail", ["config_missing"]

    reasons: List[str] = []
    if bool(_nested_get(config, "workflow.controlled_write", False)):
        reasons.append("workflow.controlled_write_enabled")
    if bool(_nested_get(config, "workflow.postrun_corrected_delivery.enabled", False)) and bool(
        _nested_get(config, "workflow.postrun_corrected_delivery.write_devices", False)
    ):
        reasons.append("postrun_corrected_delivery_write_devices_enabled")
    if bool(_nested_get(config, "validation.dry_collect.write_coefficients", False)):
        reasons.append("dry_collect_write_coefficients_enabled")
    if bool(_nested_get(config, "validation.coefficient_roundtrip.write_back_same", False)):
        reasons.append("coefficient_roundtrip_write_back_same_enabled")
    if bool(_nested_get(config, "validation.coefficient_roundtrip.allow_write_modified", False)):
        reasons.append("coefficient_roundtrip_allow_write_modified_enabled")
    sencos = config.get("sencos") if isinstance(config, Mapping) else None
    if isinstance(sencos, Mapping) and sencos:
        reasons.append("static_sencos_present")
    return "pass" if not reasons else "fail", reasons


def _check_row(name: str, status: str, reasons: List[str], **extra: Any) -> Dict[str, Any]:
    row = {"check": name, "status": status, "reasons": ";".join(reasons)}
    row.update({str(key): _table_value(value) for key, value in extra.items()})
    return row


def build_formal_preflight_tables(
    *,
    run_dir: str | Path,
    plan_path: str | Path,
    pressure_reference_path: str | Path,
    config_path: str | Path | None = None,
    component: str = "both",
    analyzer_prefix: str = "ga01",
    require_quick_check_artifact: bool = True,
    today: Any = None,
) -> tuple[Dict[str, List[Dict[str, Any]]], Dict[str, Any]]:
    root = Path(run_dir).resolve()
    checks: List[Dict[str, Any]] = []

    run_status = "pass" if root.exists() and root.is_dir() else "fail"
    checks.append(_check_row("run_dir", run_status, [] if run_status == "pass" else ["run_dir_missing"], path=str(root)))

    samples_path = latest_artifact(root, "samples_*.csv") if root.exists() else None
    checks.append(
        _check_row(
            "samples_artifact",
            "pass" if samples_path else "fail",
            [] if samples_path else ["samples_artifact_missing"],
            path=str(samples_path) if samples_path else "",
        )
    )

    plan = load_plan_snapshot(plan_path)
    plan_check = validate_formal_plan_contract(plan, today=today)
    checks.append(_check_row("formal_plan_contract", plan_check.status, plan_check.reasons, path=str(Path(plan_path).resolve())))

    pressure_reference = load_pressure_reference_snapshot(pressure_reference_path)
    reference_check = validate_pressure_reference_contract(pressure_reference, today=today)
    checks.append(
        _check_row(
            "pressure_reference_contract",
            reference_check.status,
            reference_check.reasons,
            path=str(Path(pressure_reference_path).resolve()),
        )
    )

    resolved_config_path = _resolve_config_path(root, config_path)
    config = _load_json(resolved_config_path) if resolved_config_path else None
    no_write_status, no_write_reasons = assess_no_write_config(config)
    checks.append(
        _check_row(
            "no_write_config",
            no_write_status,
            no_write_reasons,
            path=str(resolved_config_path) if resolved_config_path else "",
        )
    )

    quick_path = latest_artifact(root, "pressure_channel_quick_check*.csv") if root.exists() else None
    quick_status = "pass" if quick_path else ("fail" if require_quick_check_artifact else "skipped")
    quick_reasons = [] if quick_path or not require_quick_check_artifact else ["pressure_quick_check_artifact_missing"]
    quick_rows: List[Dict[str, Any]] = []
    if quick_path is not None:
        quick_rows = [normalize_sample_row(row) for row in load_csv_rows(quick_path)]
        quick_contract = validate_pressure_quick_check_contract(quick_rows)
        quick_status = quick_contract.status
        quick_reasons = quick_contract.reasons
    checks.append(
        _check_row(
            "pressure_quick_check_contract",
            quick_status,
            quick_reasons,
            path=str(quick_path) if quick_path else "",
        )
    )

    package_tables: Dict[str, List[Dict[str, Any]]] = {}
    package_status = "not_run"
    package_reasons: List[str] = []
    if samples_path:
        try:
            package_tables, _ = build_formal_calibration_package_tables(
                run_dir=root,
                plan=plan,
                pressure_reference=pressure_reference,
                component=component,
                analyzer_prefix=analyzer_prefix,
                require_quick_check_artifact=require_quick_check_artifact,
                today=today,
            )
            package_summary = package_tables.get("package_summary", [{}])[0]
            package_status = str(package_summary.get("package_status") or "")
            package_reasons = [
                item for item in str(package_summary.get("package_blockers") or "").split(";") if item
            ]
        except Exception as exc:
            package_status = "fail"
            package_reasons = [f"package_build_failed:{exc}"]
    checks.append(_check_row("formal_package_readiness", "pass" if package_status == "ready_for_reviewer" else "fail", package_reasons))

    failed = [row for row in checks if row["status"] == "fail"]
    summary = [
        {
            "preflight_status": "pass" if not failed else "fail",
            "failed_checks": ";".join(str(row["check"]) for row in failed),
            "run_dir": str(root),
            "component": component,
            "analyzer_prefix": analyzer_prefix,
            "require_quick_check_artifact": bool(require_quick_check_artifact),
            "opens_com_ports": False,
            "controls_routes_or_valves": False,
            "writes_coefficients": False,
        }
    ]

    tables: Dict[str, List[Dict[str, Any]]] = {
        "preflight_summary": summary,
        "preflight_checks": checks,
    }
    if package_tables:
        tables["package_summary"] = package_tables.get("package_summary", [])
        tables["candidate_coefficient_review"] = package_tables.get("candidate_coefficient_review", [])
    context = {
        "preflight_status": summary[0]["preflight_status"],
        "samples_path": str(samples_path) if samples_path else "",
        "pressure_quick_check_path": str(quick_path) if quick_path else "",
        "config_path": str(resolved_config_path) if resolved_config_path else "",
    }
    return tables, context


def write_formal_preflight_report(
    *,
    run_dir: str | Path,
    plan_path: str | Path,
    pressure_reference_path: str | Path,
    config_path: str | Path | None = None,
    output_dir: str | Path | None = None,
    component: str = "both",
    analyzer_prefix: str = "ga01",
    require_quick_check_artifact: bool = True,
    today: Any = None,
) -> Dict[str, Path]:
    root = Path(run_dir).resolve()
    tables, context = build_formal_preflight_tables(
        run_dir=root,
        plan_path=plan_path,
        pressure_reference_path=pressure_reference_path,
        config_path=config_path,
        component=component,
        analyzer_prefix=analyzer_prefix,
        require_quick_check_artifact=require_quick_check_artifact,
        today=today,
    )
    destination = Path(output_dir).resolve() if output_dir else root / "formal_preflight_report"
    metadata = ValidationMetadata(
        tool_name="export_v1_5_formal_preflight",
        created_at=datetime.now().isoformat(timespec="seconds"),
        analyzers=[analyzer_prefix],
        input_paths=[
            str(root),
            str(Path(plan_path).resolve()),
            str(Path(pressure_reference_path).resolve()),
            context.get("config_path", ""),
            context.get("samples_path", ""),
            context.get("pressure_quick_check_path", ""),
        ],
        output_dir=str(destination),
        config_summary={
            "component": component,
            "analyzer_prefix": analyzer_prefix,
            "require_quick_check_artifact": bool(require_quick_check_artifact),
            "preflight_status": context.get("preflight_status", ""),
        },
        notes=[
            "Offline V1.5 formal preflight. It reads files only.",
            "No COM ports are opened and no water/gas route, valve, PACE, SENCO9, or coefficient writes are performed.",
        ],
    )
    return write_validation_report(
        destination,
        prefix="formal_preflight",
        metadata=metadata,
        tables=tables,
    )
