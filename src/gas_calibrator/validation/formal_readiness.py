"""V1.5 formal no-write readiness assessment.

This module only reads evidence files and writes readiness reports. It does
not open COM ports, control water/gas routes, control valves/PACE, or write
analyzer coefficients.
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence

from .artifact_rows import normalize_sample_row
from .common import latest_artifact, load_csv_rows
from .formal_calibration_package import build_formal_calibration_package_tables
from .formal_contracts import (
    validate_formal_plan_contract,
    validate_pressure_quick_check_contract,
    validate_pressure_reference_contract,
)
from .formal_open_flow_artifacts import load_plan_snapshot, load_pressure_reference_snapshot
from .formal_preflight import assess_no_write_config
from .pressure_channel import build_pressure_channel_tables
from .reporting import ValidationMetadata, write_validation_report


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _load_json(path: str | Path | None) -> Optional[Dict[str, Any]]:
    if path is None:
        return None
    source = Path(path)
    if not source.exists():
        raise FileNotFoundError(f"JSON file not found: {source}")
    return json.loads(source.read_text(encoding="utf-8"))


def _resolve_config_path(run_dir: Path, config_path: str | Path | None) -> Optional[Path]:
    if config_path:
        return Path(config_path).resolve()
    candidate = run_dir / "runtime_config_snapshot.json"
    return candidate if candidate.exists() else None


def _split_reasons(value: Any) -> List[str]:
    if value in (None, ""):
        return []
    if isinstance(value, list):
        return [str(item) for item in value if str(item)]
    return [item for item in str(value).split(";") if item]


def _truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    text = str(value or "").strip().lower()
    return text in {"1", "true", "yes", "y", "on", "pass", "verified"}


def _prefixes_in_pressure_rows(rows: Sequence[Mapping[str, Any]]) -> List[str]:
    prefixes: List[str] = []
    for row in rows:
        prefix = str(row.get("analyzer_prefix") or "").strip().lower()
        if prefix and prefix not in prefixes:
            prefixes.append(prefix)
    return prefixes


def _paired_count(rows: Sequence[Mapping[str, Any]]) -> int:
    return sum(
        1
        for row in rows
        if str(row.get("pressure_channel_row_status") or "").strip().lower() == "paired"
    )


def _select_pressure_quick_check_artifact(
    root: Path,
    analyzer_prefix: str,
) -> tuple[Optional[Path], List[Dict[str, Any]]]:
    candidates = [path for path in root.glob("pressure_channel_quick_check*.csv") if path.is_file()]
    if not candidates:
        return None, []

    requested = str(analyzer_prefix or "ga01").strip().lower()
    is_fleet = requested in {"all", "*", "fleet"}
    scored: List[tuple[int, int, int, int, float, Path, List[Dict[str, Any]]]] = []
    for path in candidates:
        rows = [normalize_sample_row(row) for row in load_csv_rows(path)]
        prefixes = _prefixes_in_pressure_rows(rows)
        prefix_set = set(prefixes)
        if is_fleet:
            scope_score = 3 if len(prefix_set) > 1 else 1
        elif prefix_set == {requested}:
            scope_score = 3
        elif requested in prefix_set:
            scope_score = 2
        else:
            scope_score = 0
        if scope_score <= 0:
            continue
        contract_score = 1 if validate_pressure_quick_check_contract(rows).status == "pass" else 0
        scored.append(
            (
                scope_score,
                contract_score,
                _paired_count(rows),
                len(rows),
                path.stat().st_mtime,
                path,
                rows,
            )
        )

    if not scored:
        latest = latest_artifact(root, "pressure_channel_quick_check*.csv")
        if latest is None:
            return None, []
        return latest, [normalize_sample_row(row) for row in load_csv_rows(latest)]

    scored.sort(key=lambda item: item[:5], reverse=True)
    _, _, _, _, _, path, rows = scored[0]
    return path, rows


def _component_matches(row: Mapping[str, Any], component: str) -> bool:
    required = set(_required_reference_components(component))
    row_component = str(row.get("point_phase") or row.get("route") or "").strip().lower()
    return row_component in required


def _is_open_flow_component_sample(row: Mapping[str, Any], component: str) -> bool:
    if not _component_matches(row, component):
        return False
    point_tag = str(row.get("point_tag") or "").strip().lower()
    point_title = str(row.get("point_title") or "").strip().lower()
    if "pressure_only" in point_tag or "pressure_only" in point_title:
        return False
    if _truthy(row.get("pressure_control_not_real_acceptance_evidence")) and "pressure" in point_tag:
        return False
    return True


def _select_open_flow_samples_artifact(
    root: Path,
    component: str,
) -> tuple[Optional[Path], int, List[str]]:
    candidates = sorted(
        [path for path in root.glob("samples_*.csv") if path.is_file()],
        key=lambda item: item.stat().st_mtime,
        reverse=True,
    )
    if not candidates:
        return None, 0, ["samples_artifact_missing"]

    saw_samples = False
    for path in candidates:
        rows = [normalize_sample_row(row) for row in load_csv_rows(path)]
        saw_samples = saw_samples or bool(rows)
        eligible_count = sum(1 for row in rows if _is_open_flow_component_sample(row, component))
        if eligible_count > 0:
            return path, eligible_count, []

    reasons = ["open_flow_component_samples_missing"]
    if saw_samples:
        reasons.append("samples_artifact_contains_pressure_only_or_noncomponent_rows")
    return candidates[0], 0, reasons


def _required_reference_components(component: str) -> List[str]:
    text = str(component or "").strip().lower()
    if text in {"both", "all", "co2+h2o", "h2o+co2"}:
        return ["co2", "h2o"]
    if text in {"co2", "h2o"}:
        return [text]
    return ["co2", "h2o"]


def _component_reference_scope(plan: Mapping[str, Any], component: str) -> tuple[str, List[str]]:
    gases = plan.get("standard_gases")
    available: set[str] = set()
    if isinstance(gases, list):
        for gas in gases:
            if not isinstance(gas, Mapping):
                continue
            value = str(gas.get("component") or "").strip().lower()
            if value:
                available.add(value)

    reasons = [
        f"missing_{required}_standard_gas_or_reference"
        for required in _required_reference_components(component)
        if required not in available
    ]
    return ("pass", []) if not reasons else ("fail", reasons)


def _nested_get(mapping: Mapping[str, Any], path: str, default: Any = None) -> Any:
    current: Any = mapping
    for part in path.split("."):
        if not isinstance(current, Mapping):
            return default
        current = current.get(part)
    return default if current is None else current


def _explicit_false(value: Any) -> bool:
    if isinstance(value, bool):
        return value is False
    text = str(value).strip().lower()
    return text in {"0", "false", "no", "n", "off", "absent", "unavailable", "missing"}


def _device_marked_unavailable(config: Mapping[str, Any], device_key: str) -> bool:
    device_cfg = _nested_get(config, f"devices.{device_key}", {})
    if isinstance(device_cfg, Mapping):
        for key in ("present", "available", "installed"):
            if key in device_cfg and _explicit_false(device_cfg.get(key)):
                return True
        if "enabled" in device_cfg and _explicit_false(device_cfg.get("enabled")):
            return True

    availability = _nested_get(config, f"hardware_availability.{device_key}", None)
    if _explicit_false(availability):
        return True
    if isinstance(availability, Mapping):
        for key in ("present", "available", "installed", "enabled"):
            if key in availability and _explicit_false(availability.get(key)):
                return True
    return False


def assess_pressure_hardware_availability(config: Optional[Mapping[str, Any]]) -> tuple[str, List[str]]:
    """Return whether the current bench can run a real pressure quick check."""

    if config is None:
        return "unknown", ["config_missing"]

    unavailable: List[str] = []
    if _device_marked_unavailable(config, "pressure_controller"):
        unavailable.append("pressure_controller_unavailable")
    if _device_marked_unavailable(config, "pressure_gauge"):
        unavailable.append("pressure_gauge_unavailable")
    if unavailable:
        return "fail", unavailable

    devices = config.get("devices") if isinstance(config, Mapping) else None
    if not isinstance(devices, Mapping) or not any(key in devices for key in ("pressure_controller", "pressure_gauge")):
        return "unknown", ["pressure_hardware_not_declared"]
    return "pass", []


def _check(
    name: str,
    status: str,
    reasons: List[str] | None = None,
    *,
    path: str = "",
    stage: str = "",
    evidence_role: str = "",
) -> Dict[str, Any]:
    return {
        "check": name,
        "status": status,
        "reasons": ";".join(reasons or []),
        "path": path,
        "stage": stage,
        "evidence_role": evidence_role,
    }


def _action(action_id: str, status: str, owner: str, description: str) -> Dict[str, Any]:
    return {
        "action_id": action_id,
        "status": status,
        "owner": owner,
        "description": description,
    }


def _run_order_rows(
    *,
    pressure_quick_check_status: str,
    samples_status: str,
    package_status: str,
) -> List[Dict[str, Any]]:
    return [
        {
            "step": "LOAD_PLAN",
            "status": "pass",
            "meaning": "formal plan and traceability snapshots are loaded",
        },
        {
            "step": "PRECHECK",
            "status": "pending_real_device",
            "meaning": "device communication/status precheck is not performed by this offline report",
        },
        {
            "step": "PRESSURE_CHANNEL_QUICK_CHECK",
            "status": pressure_quick_check_status,
            "meaning": "analyzer internal pressure P is compared with COM22 before component sampling",
        },
        {
            "step": "OPEN_FLOW_PURGE",
            "status": samples_status,
            "meaning": "standard gas continuously refreshes analyzer chamber and downstream volume",
        },
        {
            "step": "STABILITY_GATE",
            "status": samples_status,
            "meaning": "CO2/H2O, dewpoint, pressure, and factory signals must be stable",
        },
        {
            "step": "SAMPLE_WINDOW",
            "status": samples_status,
            "meaning": "formal component samples are collected only in open flow",
        },
        {
            "step": "QC_CLASSIFICATION",
            "status": package_status,
            "meaning": "A/B/rejected sample classes are built from evidence rows",
        },
        {
            "step": "POINT_REVIEW",
            "status": package_status,
            "meaning": "candidate fit eligibility is reviewed without automatic coefficient write",
        },
        {
            "step": "RUN_SUMMARY",
            "status": package_status,
            "meaning": "evidence package can be reviewed or remains blocked with reasons",
        },
    ]


def build_formal_readiness_model(
    *,
    run_dir: str | Path,
    plan_path: str | Path,
    pressure_reference_path: str | Path,
    config_path: str | Path | None = None,
    component: str = "both",
    analyzer_prefix: str = "ga01",
    today: Any = None,
) -> Dict[str, Any]:
    """Build a human-facing readiness model for the V1.5 formal no-write flow."""

    root = Path(run_dir).resolve()
    checks: List[Dict[str, Any]] = []
    next_actions: List[Dict[str, Any]] = []

    run_dir_status = "pass" if root.exists() and root.is_dir() else "pending"
    checks.append(
        _check(
            "run_dir",
            run_dir_status,
            [] if run_dir_status == "pass" else ["planned_run_dir_not_created"],
            path=str(root),
            stage="offline_setup",
            evidence_role="run_artifact_root",
        )
    )

    plan = load_plan_snapshot(plan_path)
    plan_check = validate_formal_plan_contract(plan, today=today)
    checks.append(
        _check(
            "formal_plan_contract",
            plan_check.status,
            plan_check.reasons,
            path=str(Path(plan_path).resolve()),
            stage="offline_setup",
            evidence_role="formal_plan_snapshot",
        )
    )
    component_scope_status, component_scope_reasons = _component_reference_scope(plan, component)
    checks.append(
        _check(
            "component_reference_scope",
            component_scope_status,
            component_scope_reasons,
            path=str(Path(plan_path).resolve()),
            stage="offline_setup",
            evidence_role="standard_gas_and_humidity_reference_scope",
        )
    )

    pressure_reference = load_pressure_reference_snapshot(pressure_reference_path)
    pressure_ref_check = validate_pressure_reference_contract(pressure_reference, today=today)
    checks.append(
        _check(
            "pressure_reference_contract",
            pressure_ref_check.status,
            pressure_ref_check.reasons,
            path=str(Path(pressure_reference_path).resolve()),
            stage="offline_setup",
            evidence_role="com22_pressure_reference",
        )
    )

    resolved_config_path = _resolve_config_path(root, config_path)
    config = _load_json(resolved_config_path) if resolved_config_path else None
    no_write_status, no_write_reasons = assess_no_write_config(config)
    pressure_hardware_status, pressure_hardware_reasons = assess_pressure_hardware_availability(config)
    checks.append(
        _check(
            "no_write_config",
            no_write_status,
            no_write_reasons,
            path=str(resolved_config_path) if resolved_config_path else "",
            stage="offline_setup",
            evidence_role="runtime_config",
        )
    )
    checks.append(
        _check(
            "pressure_hardware_availability",
            pressure_hardware_status,
            pressure_hardware_reasons,
            path=str(resolved_config_path) if resolved_config_path else "",
            stage="device_precheck",
            evidence_role="pressure_controller_and_com22",
        )
    )

    setup_failed = any(
        row["status"] == "fail"
        for row in checks
        if row["check"]
        in {"formal_plan_contract", "component_reference_scope", "pressure_reference_contract", "no_write_config"}
    )

    quick_path, quick_rows = _select_pressure_quick_check_artifact(root, analyzer_prefix) if root.exists() else (None, [])
    quick_status = "pending_real_no_write" if not quick_path else "pass"
    quick_reasons: List[str] = [] if quick_path else ["pressure_quick_check_artifact_missing"]
    if not quick_path and pressure_hardware_status == "fail":
        quick_status = "blocked_hardware_unavailable"
        quick_reasons = list(pressure_hardware_reasons)
    if quick_path:
        quick_contract = validate_pressure_quick_check_contract(quick_rows)
        quick_status = quick_contract.status
        quick_reasons = quick_contract.reasons
        if quick_status == "pass":
            pressure_tables = build_pressure_channel_tables(
                quick_rows,
                pressure_reference=pressure_reference,
                analyzer_prefix=analyzer_prefix,
                today=today,
            )
            pressure_summaries = pressure_tables.get("pressure_validation_summary", []) or [{}]
            passed_summaries: List[Mapping[str, Any]] = []
            failed_summaries: List[Mapping[str, Any]] = []
            for summary_row in pressure_summaries:
                pressure_allowed = _truthy(summary_row.get("allowed_for_co2_h2o_formal_work"))
                if str(summary_row.get("status") or "") == "pass" and pressure_allowed:
                    passed_summaries.append(summary_row)
                else:
                    failed_summaries.append(summary_row)
            if failed_summaries:
                quick_status = "partial" if passed_summaries else "fail"
                blocked = [
                    f"{row.get('analyzer_prefix')}:{row.get('reason') or row.get('status') or 'pressure_channel_validation_failed'}"
                    for row in failed_summaries
                ]
                passed = [
                    str(row.get("analyzer_prefix") or "")
                    for row in passed_summaries
                    if str(row.get("analyzer_prefix") or "")
                ]
                quick_reasons = [f"blocked_analyzers={','.join(blocked)}"]
                if passed:
                    quick_reasons.append(f"passed_analyzers={','.join(passed)}")
            elif not passed_summaries:
                quick_status = "fail"
                quick_reasons = ["pressure_channel_validation_missing"]
    checks.append(
        _check(
            "pressure_quick_check_contract",
            quick_status,
            quick_reasons,
            path=str(quick_path) if quick_path else "",
            stage="pressure_channel_quick_check",
            evidence_role="pressure_channel_quick_check",
        )
    )

    samples_path, open_flow_sample_count, sample_reasons = (
        _select_open_flow_samples_artifact(root, component) if root.exists() else (None, 0, ["samples_artifact_missing"])
    )
    samples_status = "pass" if open_flow_sample_count > 0 else "pending_real_no_write"
    checks.append(
        _check(
            "open_flow_samples",
            samples_status,
            sample_reasons,
            path=str(samples_path) if samples_path else "",
            stage="open_flow_component_sampling",
            evidence_role="samples",
        )
    )

    package_tables: Dict[str, List[Dict[str, Any]]] = {}
    package_status = "not_ready"
    package_blockers: List[str] = []
    if samples_status == "pass" and quick_path and quick_status == "pass" and not setup_failed:
        try:
            package_tables, _ = build_formal_calibration_package_tables(
                run_dir=root,
                plan=plan,
                pressure_reference=pressure_reference,
                component=component,
                analyzer_prefix=analyzer_prefix,
                require_quick_check_artifact=True,
                today=today,
            )
            package_summary = package_tables.get("package_summary", [{}])[0]
            package_status = str(package_summary.get("package_status") or "blocked")
            package_blockers = _split_reasons(package_summary.get("package_blockers"))
        except Exception as exc:
            package_status = "blocked"
            package_blockers = [f"package_build_failed:{exc}"]
    elif setup_failed:
        package_status = "blocked"
        package_blockers = ["offline_setup_failed"]
    elif not quick_path:
        package_status = "pending_pressure_quick_check"
        package_blockers = (
            ["pressure_hardware_unavailable_before_pressure_quick_check", *pressure_hardware_reasons]
            if pressure_hardware_status == "fail"
            else ["pressure_quick_check_required_before_sampling"]
        )
    elif quick_status != "pass":
        package_status = "blocked"
        package_blockers = ["pressure_quick_check_contract_failed"]
    elif samples_status != "pass":
        package_status = "pending_open_flow_samples"
        package_blockers = ["open_flow_samples_required"]

    checks.append(
        _check(
            "formal_package_readiness",
            "pass" if package_status == "ready_for_reviewer" else package_status,
            package_blockers,
            stage="evidence_review",
            evidence_role="formal_calibration_package",
        )
    )

    if setup_failed:
        readiness_status = "setup_blocked"
        next_actions.append(_action("fix_offline_setup", "required", "engineer", "修正计划、COM22 证书或 no-write 配置后重新评估。"))
    elif not quick_path and pressure_hardware_status == "fail":
        readiness_status = "pressure_hardware_blocked"
        next_actions.append(
            _action(
                "restore_pressure_hardware",
                "required_before_real_pressure_check",
                "engineer",
                "PACE/COM22 pressure hardware is unavailable; continue only offline sidecar work until the pressure chain is restored.",
            )
        )
    elif not quick_path:
        readiness_status = "ready_for_pressure_quick_check_authorization"
        next_actions.append(
            _action(
                "perform_pressure_quick_check",
                "requires_explicit_v1_5_no_write_authorization",
                "operator+engineer",
                "执行当前大气压压力通道快速验证，只采集 analyzer P 对 COM22，不做 CO2/H2O 拟合。",
            )
        )
    elif quick_status != "pass":
        readiness_status = "pressure_channel_partial" if quick_status == "partial" else "pressure_channel_blocked"
        next_actions.append(_action("resolve_pressure_channel", "required", "engineer", "压力快速验证证据不合格，暂停正式 CO2/H2O 系数评审。"))
    elif samples_status != "pass":
        readiness_status = "ready_for_open_flow_sampling_authorization"
        next_actions.append(
            _action(
                "perform_open_flow_sampling",
                "requires_explicit_v1_5_no_write_authorization",
                "operator+engineer",
                "按 V1.5 开放流通路线采集 CO2/H2O，保持 PACE 通大气，不引入封路压力点作为正式拟合输入。",
            )
        )
    elif package_status == "ready_for_reviewer":
        readiness_status = "ready_for_reviewer"
        next_actions.append(_action("review_candidate_package", "ready", "reviewer", "审核 A 级样本、拒绝原因、压力通道结果和报告状态。"))
    else:
        readiness_status = "evidence_blocked"
        next_actions.append(_action("repair_evidence_package", "required", "engineer", "根据 formal package blockers 补齐或修正证据。"))

    requires_real = readiness_status in {
        "ready_for_pressure_quick_check_authorization",
        "ready_for_open_flow_sampling_authorization",
    }
    summary = {
        "schema_version": "v1_5_formal_readiness_v0",
        "generated_at": _now(),
        "readiness_status": readiness_status,
        "run_dir": str(root),
        "component": component,
        "analyzer_prefix": analyzer_prefix,
        "requires_real_device_authorization": requires_real,
        "pressure_hardware_status": pressure_hardware_status,
        "pressure_hardware_reasons": pressure_hardware_reasons,
        "can_run_pressure_quick_check": pressure_hardware_status in {"pass", "unknown"} and quick_path is None,
        "sidecar_only": True,
        "opens_com_ports": False,
        "controls_water_or_gas_routes": False,
        "controls_valves_or_pace": False,
        "writes_coefficients": False,
        "real_acceptance_evidence": False,
    }

    return {
        **summary,
        "checks": checks,
        "next_actions": next_actions,
        "formal_run_order": _run_order_rows(
            pressure_quick_check_status=quick_status,
            samples_status=samples_status,
            package_status=package_status,
        ),
        "package_summary": package_tables.get("package_summary", []),
        "candidate_coefficient_review": package_tables.get("candidate_coefficient_review", []),
    }


def render_formal_readiness_markdown(model: Mapping[str, Any]) -> str:
    lines = [
        "# V1.5 formal no-write readiness",
        "",
        f"- readiness_status: {model.get('readiness_status')}",
        f"- run_dir: {model.get('run_dir')}",
        f"- requires_real_device_authorization: {model.get('requires_real_device_authorization')}",
        f"- pressure_hardware_status: {model.get('pressure_hardware_status')}",
        f"- can_run_pressure_quick_check: {model.get('can_run_pressure_quick_check')}",
        f"- opens_com_ports: {model.get('opens_com_ports')}",
        f"- controls_water_or_gas_routes: {model.get('controls_water_or_gas_routes')}",
        f"- controls_valves_or_pace: {model.get('controls_valves_or_pace')}",
        f"- writes_coefficients: {model.get('writes_coefficients')}",
        "",
        "## Checks",
    ]
    for row in model.get("checks") or []:
        reason = row.get("reasons") or ""
        lines.append(f"- {row.get('check')}: {row.get('status')} {reason}".rstrip())
    lines.extend(["", "## Next Actions"])
    for row in model.get("next_actions") or []:
        lines.append(f"- {row.get('action_id')}: {row.get('status')} - {row.get('description')}")
    lines.extend(["", "## Formal Run Order"])
    for row in model.get("formal_run_order") or []:
        lines.append(f"- {row.get('step')}: {row.get('status')}")
    return "\n".join(lines) + "\n"


def write_formal_readiness_report(
    *,
    run_dir: str | Path,
    plan_path: str | Path,
    pressure_reference_path: str | Path,
    config_path: str | Path | None = None,
    output_dir: str | Path | None = None,
    component: str = "both",
    analyzer_prefix: str = "ga01",
    today: Any = None,
) -> Dict[str, Path]:
    root = Path(output_dir).resolve() if output_dir else Path(run_dir).resolve() / "formal_readiness"
    root.mkdir(parents=True, exist_ok=True)
    model = build_formal_readiness_model(
        run_dir=run_dir,
        plan_path=plan_path,
        pressure_reference_path=pressure_reference_path,
        config_path=config_path,
        component=component,
        analyzer_prefix=analyzer_prefix,
        today=today,
    )
    json_path = root / "formal_readiness.json"
    markdown_path = root / "formal_readiness.md"
    json_path.write_text(json.dumps(model, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    markdown_path.write_text(render_formal_readiness_markdown(model), encoding="utf-8")

    tables = {
        "readiness_summary": [
            {
                key: ";".join(str(item) for item in model.get(key)) if isinstance(model.get(key), list) else model.get(key)
                for key in (
                    "schema_version",
                    "readiness_status",
                    "run_dir",
                    "component",
                    "analyzer_prefix",
                    "requires_real_device_authorization",
                    "pressure_hardware_status",
                    "pressure_hardware_reasons",
                    "can_run_pressure_quick_check",
                    "opens_com_ports",
                    "controls_water_or_gas_routes",
                    "controls_valves_or_pace",
                    "writes_coefficients",
                    "real_acceptance_evidence",
                )
            }
        ],
        "readiness_checks": model.get("checks") or [],
        "next_actions": model.get("next_actions") or [],
        "formal_run_order": model.get("formal_run_order") or [],
    }
    if model.get("package_summary"):
        tables["package_summary"] = model.get("package_summary") or []
    if model.get("candidate_coefficient_review"):
        tables["candidate_coefficient_review"] = model.get("candidate_coefficient_review") or []

    metadata = ValidationMetadata(
        tool_name="export_v1_5_formal_readiness",
        created_at=str(model.get("generated_at") or _now()),
        analyzers=[analyzer_prefix],
        input_paths=[
            str(Path(run_dir).resolve()),
            str(Path(plan_path).resolve()),
            str(Path(pressure_reference_path).resolve()),
            str(Path(config_path).resolve()) if config_path else "",
        ],
        output_dir=str(root),
        config_summary={
            "component": component,
            "readiness_status": model.get("readiness_status"),
            "requires_real_device_authorization": model.get("requires_real_device_authorization"),
        },
        notes=[
            "Offline V1.5 formal no-write readiness report.",
            "No COM ports are opened and no water/gas route, valve, PACE, SENCO9, or coefficient writes are performed.",
            "Statuses that require real-device authorization are not real acceptance evidence.",
        ],
    )
    workbook_outputs = write_validation_report(
        root,
        prefix="formal_readiness",
        metadata=metadata,
        tables=tables,
    )
    return {
        "summary_json": json_path,
        "summary_markdown": markdown_path,
        **workbook_outputs,
    }
