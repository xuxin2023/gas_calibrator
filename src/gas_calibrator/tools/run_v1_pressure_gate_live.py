"""Guarded minimal V1 live validation for AtmosphereGate and RouteFlush gates.

This tool is intentionally narrower than formal smoke:
- no full calibration run
- no setpoint sweep
- no analyzer sampling flow
- only V1 live engineering validation
"""

from __future__ import annotations

import argparse
import copy
import csv
import json
import os
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional

from ..config import load_config
from ..data.points import CalibrationPoint
from ..logging_utils import RunLogger
from ..workflow.runner import CalibrationRunner
from .run_headless import _build_devices, _close_devices
from .safe_stop import perform_safe_stop_with_retries


REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_OUTPUT_DIR = REPO_ROOT / "results" / "pressure_gate_live"
DEFAULT_STAGED_SOURCE_FINAL_OUTPUT_DIR = Path("D:/gas_calibrator_staged_source_final_dry_run_artifacts")
CO2_A_STAGED_SOURCE_FINAL_RELEASE_DRY_RUN = "co2_a_staged_source_final_release_dry_run"
CO2_A_STAGED_RELEASE_SCOPE = "staged_source_final_release_dry_run"
CO2_A_STAGED_APPROVAL_SCOPE = "CO2_A_VALVE_4_STAGED_DRY_RUN_ONLY"
CO2_A_FRONT_VALVES = [8, 11, 7]
CO2_A_SOURCE_FINAL_VALVE = 4
CO2_A_BLOCKED_VALVES = [24, 10]
CO2_A_APPLY_EXPECTED_BLOCKED_VALVES = [4, 24, 10]
CO2_A_STAGED_REQUIRED_ENV = {
    "ALLOW_STAGED_SOURCE_FINAL_DRY_RUN": "CO2_A_VALVE_4_ONLY",
    "OPERATOR_INTENT_CONFIRMED": "YES",
    "CONFIRM_NOT_FULL_PRODUCTION": "YES",
    "CONFIRM_NO_ROUTE_FLUSH_DEWPOINT_GATE": "YES",
    "CONFIRM_SINGLE_ROUTE_CO2_A_ONLY": "YES",
}
_SOURCE_OPEN_SCENARIOS = {
    CO2_A_STAGED_SOURCE_FINAL_RELEASE_DRY_RUN,
    "route_synchronized_atmosphere_flush_co2_a_source_guarded",
    "route_synchronized_atmosphere_flush_co2_b_source_guarded",
    "route_synchronized_atmosphere_flowthrough_co2_a_source_guarded",
    "route_synchronized_atmosphere_flowthrough_co2_b_source_guarded",
    "route_synchronized_atmosphere_flush_co2_a",
    "route_synchronized_atmosphere_flush_co2_b",
}
_H2O_FINAL_STAGE_SCENARIOS = {
    "route_synchronized_atmosphere_flush_h2o",
    "route_synchronized_atmosphere_flowthrough_h2o_final_guarded",
}


def _log(message: str) -> None:
    print(message, flush=True)


def _normalized_valves(values: Optional[Iterable[Any]]) -> List[int]:
    normalized = []
    seen = set()
    for value in list(values or []):
        try:
            parsed = int(value)
        except (TypeError, ValueError):
            continue
        if parsed in seen:
            continue
        seen.add(parsed)
        normalized.append(parsed)
    return normalized


def _path_within_repo(path: Path) -> bool:
    try:
        path.resolve().relative_to(REPO_ROOT.resolve())
        return True
    except ValueError:
        return False


def _resolve_output_dir(args: argparse.Namespace) -> Path:
    requested = Path(args.output_dir).resolve() if getattr(args, "output_dir", None) else DEFAULT_OUTPUT_DIR.resolve()
    if str(getattr(args, "scenario", "") or "") != CO2_A_STAGED_SOURCE_FINAL_RELEASE_DRY_RUN:
        return requested
    if requested == DEFAULT_OUTPUT_DIR.resolve():
        requested = DEFAULT_STAGED_SOURCE_FINAL_OUTPUT_DIR.resolve()
    if _path_within_repo(requested):
        raise RuntimeError("staged_source_final_artifacts_must_be_outside_repo")
    return requested


def _runner_current_open_valves(runner: CalibrationRunner) -> List[int]:
    return _normalized_valves(getattr(runner, "_current_open_valves", ()))


def _runner_command_list(runner: CalibrationRunner, attr_name: str) -> List[Any]:
    values = getattr(runner, attr_name, [])
    if isinstance(values, list):
        return list(values)
    if isinstance(values, tuple):
        return list(values)
    return []


def _co2_a_staged_source_final_env_status() -> Dict[str, Any]:
    missing: List[str] = []
    for name, expected in CO2_A_STAGED_REQUIRED_ENV.items():
        if str(os.environ.get(name) or "").strip() != expected:
            missing.append(name)
    release_reason = str(os.environ.get("RELEASE_REASON") or "").strip()
    if not release_reason:
        missing.append("RELEASE_REASON")
    return {
        "missing": missing,
        "release_reason": release_reason,
        "operator_confirmation_missing": bool(missing),
    }


def _co2_a_pressure_protection_sections(run_cfg: Mapping[str, Any]) -> List[Mapping[str, Any]]:
    root_cfg = run_cfg if isinstance(run_cfg, Mapping) else {}
    workflow_cfg = root_cfg.get("workflow", {}) if isinstance(root_cfg, Mapping) else {}
    workflow_cfg = workflow_cfg if isinstance(workflow_cfg, Mapping) else {}
    pressure_cfg = workflow_cfg.get("pressure", {}) if isinstance(workflow_cfg, Mapping) else {}
    pressure_cfg = pressure_cfg if isinstance(pressure_cfg, Mapping) else {}
    return [pressure_cfg, workflow_cfg, root_cfg]


def _first_config_value(sections: Iterable[Mapping[str, Any]], key: str) -> Any:
    for section in sections:
        if isinstance(section, Mapping) and key in section:
            return section.get(key)
    return None


def _extract_co2_a_staged_pressure_protection_config(run_cfg: Mapping[str, Any]) -> Optional[Dict[str, Any]]:
    for section in _co2_a_pressure_protection_sections(run_cfg):
        candidate = section.get("co2_a_staged_pressure_protection") if isinstance(section, Mapping) else None
        if isinstance(candidate, Mapping):
            return dict(candidate)
    return None


def _extract_co2_a_legacy_pressure_protection_config(
    run_cfg: Mapping[str, Any],
    *,
    source_key: str,
) -> Optional[Dict[str, Any]]:
    sections = _co2_a_pressure_protection_sections(run_cfg)
    if not any(isinstance(section, Mapping) and source_key in section for section in sections):
        return None
    keys = (
        "route",
        "source_final_valve_under_test",
        "release_scope",
        "approval_scope",
        "retry_allowed_for_scope",
        "not_full_v1_production_approval",
        "not_full_formal_approval",
        "does_not_open_4_24_10",
        "does_not_run_real_sealed_pressure_transition",
        "analyzer_pressure_protection_active",
        "mechanical_pressure_protection_confirmed",
    )
    return {
        key: value
        for key in keys
        if (value := _first_config_value(sections, key)) is not None
    }


def _resolve_co2_a_pressure_protection_approval_path(
    run_cfg: Mapping[str, Any],
    explicit_path: Optional[str],
) -> str:
    path_text = str(explicit_path or "").strip()
    if path_text:
        return path_text
    sections = _co2_a_pressure_protection_sections(run_cfg)
    for key in ("pressure_protection_approval_json", "co2_a_staged_pressure_protection_approval_json"):
        value = _first_config_value(sections, key)
        if str(value or "").strip():
            return str(value).strip()
    return ""


def _load_pressure_protection_approval_artifact(path_text: str) -> Optional[Dict[str, Any]]:
    if not str(path_text or "").strip():
        return None
    try:
        raw = json.loads(Path(path_text).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return dict(raw) if isinstance(raw, Mapping) else None


def _evaluate_co2_a_staged_pressure_protection_source(
    *,
    source: str,
    payload: Optional[Mapping[str, Any]],
    route: str,
    source_final_valve: int,
    release_scope: str,
) -> Dict[str, Any]:
    result = {
        "pressure_protection_source": source,
        "pressure_protection_precheck_satisfied": False,
        "analyzer_pressure_protection_active": False,
        "mechanical_pressure_protection_confirmed": False,
        "route": str(route or "").strip().upper(),
        "source_final_valve_under_test": int(source_final_valve),
        "release_scope": str(release_scope or "").strip(),
        "approval_scope": "",
        "retry_allowed_for_scope": False,
        "not_full_v1_production_approval": False,
        "not_full_formal_approval": False,
        "does_not_open_4_24_10": False,
        "does_not_run_real_sealed_pressure_transition": False,
        "reasons": [],
    }
    if not payload:
        result["reasons"] = ["PressureProtectionApprovalMissing"]
        return result

    approval_scope = str(payload.get("approval_scope") or "").strip()
    result["approval_scope"] = approval_scope
    result["retry_allowed_for_scope"] = bool(payload.get("retry_allowed_for_scope"))
    result["not_full_v1_production_approval"] = bool(payload.get("not_full_v1_production_approval"))
    result["not_full_formal_approval"] = bool(payload.get("not_full_formal_approval"))
    result["does_not_open_4_24_10"] = bool(payload.get("does_not_open_4_24_10"))
    result["does_not_run_real_sealed_pressure_transition"] = bool(
        payload.get("does_not_run_real_sealed_pressure_transition")
    )
    result["analyzer_pressure_protection_active"] = bool(payload.get("analyzer_pressure_protection_active"))
    result["mechanical_pressure_protection_confirmed"] = bool(payload.get("mechanical_pressure_protection_confirmed"))

    try:
        payload_valve = int(payload.get("source_final_valve_under_test"))
    except (TypeError, ValueError):
        payload_valve = None
    payload_route = str(payload.get("route") or "").strip().upper()
    payload_release_scope = str(payload.get("retry_scope") or payload.get("release_scope") or "").strip()

    reasons: List[str] = []
    if (
        payload_route != "CO2_A"
        or payload_valve != CO2_A_SOURCE_FINAL_VALVE
        or payload_release_scope != CO2_A_STAGED_RELEASE_SCOPE
        or approval_scope != CO2_A_STAGED_APPROVAL_SCOPE
        or not result["retry_allowed_for_scope"]
        or not result["not_full_v1_production_approval"]
        or not result["not_full_formal_approval"]
        or not result["does_not_open_4_24_10"]
        or not result["does_not_run_real_sealed_pressure_transition"]
    ):
        reasons.append("PressureProtectionScopeInvalid")
    if not (
        result["mechanical_pressure_protection_confirmed"]
        or result["analyzer_pressure_protection_active"]
    ):
        reasons.append("PressureProtectionApprovalMissing")
    result["reasons"] = reasons
    result["pressure_protection_precheck_satisfied"] = not reasons
    return result


def resolve_co2_a_staged_pressure_protection(
    run_cfg: Mapping[str, Any],
    *,
    approval_json_path: Optional[str],
    route: str,
    source_final_valve: int,
    release_scope: str,
) -> Dict[str, Any]:
    route_text = str(route or "").strip().upper()
    release_scope_text = str(release_scope or "").strip()
    result = {
        "pressure_protection_source": "missing",
        "pressure_protection_precheck_satisfied": False,
        "analyzer_pressure_protection_active": False,
        "mechanical_pressure_protection_confirmed": False,
        "route": route_text,
        "source_final_valve_under_test": int(source_final_valve),
        "release_scope": release_scope_text,
        "approval_scope": "",
        "retry_allowed_for_scope": False,
        "not_full_v1_production_approval": False,
        "not_full_formal_approval": False,
        "does_not_open_4_24_10": False,
        "does_not_run_real_sealed_pressure_transition": False,
        "reasons": [],
    }
    if (
        route_text != "CO2_A"
        or int(source_final_valve) != CO2_A_SOURCE_FINAL_VALVE
        or release_scope_text != CO2_A_STAGED_RELEASE_SCOPE
    ):
        result["reasons"] = ["PressureProtectionScopeInvalid"]
        return result

    explicit_config = _extract_co2_a_staged_pressure_protection_config(run_cfg)
    if explicit_config is not None:
        return _evaluate_co2_a_staged_pressure_protection_source(
            source="config",
            payload=explicit_config,
            route=route_text,
            source_final_valve=source_final_valve,
            release_scope=release_scope_text,
        )

    resolved_approval_path = _resolve_co2_a_pressure_protection_approval_path(run_cfg, approval_json_path)
    approval_payload = _load_pressure_protection_approval_artifact(resolved_approval_path)
    if approval_payload is not None:
        return _evaluate_co2_a_staged_pressure_protection_source(
            source="approval_artifact",
            payload=approval_payload,
            route=route_text,
            source_final_valve=source_final_valve,
            release_scope=release_scope_text,
        )

    analyzer_config = _extract_co2_a_legacy_pressure_protection_config(
        run_cfg,
        source_key="analyzer_pressure_protection_active",
    )
    if analyzer_config is not None:
        return _evaluate_co2_a_staged_pressure_protection_source(
            source="analyzer_config",
            payload=analyzer_config,
            route=route_text,
            source_final_valve=source_final_valve,
            release_scope=release_scope_text,
        )

    mechanical_config = _extract_co2_a_legacy_pressure_protection_config(
        run_cfg,
        source_key="mechanical_pressure_protection_confirmed",
    )
    if mechanical_config is not None:
        return _evaluate_co2_a_staged_pressure_protection_source(
            source="mechanical_config",
            payload=mechanical_config,
            route=route_text,
            source_final_valve=source_final_valve,
            release_scope=release_scope_text,
        )

    result["reasons"] = ["PressureProtectionApprovalMissing"]
    return result


def _validate_co2_a_staged_source_final_scope(
    *,
    route: str,
    release_scope: str,
    front_valves: Iterable[Any],
    source_final_valve_under_test: int,
    blocked_valves_required_closed: Iterable[Any],
) -> List[str]:
    reasons: List[str] = []
    if str(route or "").strip().upper() != "CO2_A":
        reasons.append("OnlyCO2ARouteSupported")
    if str(release_scope or "").strip() != CO2_A_STAGED_RELEASE_SCOPE:
        reasons.append("OnlyStagedSourceFinalReleaseDryRunSupported")
    if _normalized_valves(front_valves) != CO2_A_FRONT_VALVES:
        reasons.append("CO2AFrontPathMustRemain8_11_7")
    if int(source_final_valve_under_test) != CO2_A_SOURCE_FINAL_VALVE:
        reasons.append("OnlyValve4SourceFinalStageSupported")
    if _normalized_valves(blocked_valves_required_closed) != CO2_A_BLOCKED_VALVES:
        reasons.append("Valve24And10MustRemainBlocked")
    return reasons


def _build_co2_a_staged_source_final_result(
    *,
    trace_path: Path,
    trace_start: int,
    route: str,
    release_scope: str,
    front_valves: Iterable[Any],
    source_final_valve_under_test: int,
    blocked_valves_required_closed: Iterable[Any],
) -> Dict[str, Any]:
    return {
        "scenario": CO2_A_STAGED_SOURCE_FINAL_RELEASE_DRY_RUN,
        "status": "pending",
        "abort_reason": "",
        "evidence_source": CO2_A_STAGED_RELEASE_SCOPE,
        "route": str(route or "").strip().upper(),
        "front_valves": _normalized_valves(front_valves),
        "source_final_valve_under_test": int(source_final_valve_under_test),
        "blocked_valves_required_closed": _normalized_valves(blocked_valves_required_closed),
        "release_scope": str(release_scope or "").strip(),
        "source_stage_key": "",
        "not_full_v1_production_acceptance": True,
        "not_full_formal_acceptance": True,
        "real_sealed_pressure_transition_verified": False,
        "pressure_point_switch_executed": False,
        "sampling_under_pressure_executed": False,
        "candidate_eligible": False,
        "explicit_apply_succeeded": False,
        "route_final_stage_seal_safety_updated": False,
        "route_final_stage_seal_safety_key": "",
        "co2_4_opened": False,
        "co2_24_opened": False,
        "h2o_10_opened": False,
        "source_final_stage_opened": False,
        "opened_valves": [],
        "closed_valves_final": [],
        "pace_commands_sent": [],
        "vent_write_commands": [],
        "vent_query_responses": [],
        "vent2_tx_observed": False,
        "final_syst_err": "",
        "hidden_syst_err_count": 0,
        "unclassified_syst_err_count": 0,
        "pre_route_drain_syst_err_count": 0,
        "cleanup_completed": False,
        "dry_run_passed": False,
        "operator_env": {},
        "missing_operator_env": [],
        "operator_confirmation_missing": False,
        "scope_validation_reasons": [],
        "pressure_protection_source": "missing",
        "pressure_protection_precheck_satisfied": False,
        "analyzer_pressure_protection_active": False,
        "mechanical_pressure_protection_confirmed": False,
        "approval_scope": "",
        "retry_allowed_for_scope": False,
        "pressure_protection_resolution": {},
        "k0472_capability_snapshot": {},
        "precheck": {},
        "verification": {},
        "candidate": {},
        "apply_result": {},
        "point_runtime_state": {},
        "route_pressure_guard_summary": {},
        "pressure_trace_rows": _scenario_trace_rows(trace_path, trace_start),
    }


def _build_co2_a_staged_source_final_artifact(summary: Mapping[str, Any]) -> Dict[str, Any]:
    scenario_result = dict(summary.get("scenario_result") or {})
    cleanup_result = dict(summary.get("cleanup_safe_stop") or {})
    candidate = dict(scenario_result.get("candidate") or {})
    apply_result = dict(scenario_result.get("apply_result") or {})
    artifact = _build_co2_a_staged_source_final_result(
        trace_path=Path(summary.get("pressure_transition_trace_csv") or summary.get("io_csv") or DEFAULT_OUTPUT_DIR / "trace.csv"),
        trace_start=0,
        route=scenario_result.get("route") or "CO2_A",
        release_scope=scenario_result.get("release_scope") or CO2_A_STAGED_RELEASE_SCOPE,
        front_valves=scenario_result.get("front_valves") or CO2_A_FRONT_VALVES,
        source_final_valve_under_test=int(
            scenario_result.get("source_final_valve_under_test") or CO2_A_SOURCE_FINAL_VALVE
        ),
        blocked_valves_required_closed=scenario_result.get("blocked_valves_required_closed") or CO2_A_BLOCKED_VALVES,
    )
    artifact.update(dict(scenario_result))
    artifact["pressure_trace_rows"] = list(scenario_result.get("pressure_trace_rows") or [])
    artifact["candidate_eligible"] = bool(candidate.get("eligible_for_explicit_release"))
    artifact["explicit_apply_succeeded"] = bool(apply_result.get("release_performed"))
    artifact["route_final_stage_seal_safety_updated"] = bool(
        apply_result.get("route_final_stage_seal_safety_updated")
        or scenario_result.get("route_final_stage_seal_safety_updated")
    )
    artifact["route_final_stage_seal_safety_key"] = str(
        apply_result.get("route_final_stage_seal_safety_key")
        or scenario_result.get("route_final_stage_seal_safety_key")
        or ""
    )
    artifact["final_syst_err"] = str(summary.get("final_syst_err") or scenario_result.get("final_syst_err") or "").strip()
    artifact["hidden_syst_err_count"] = int(
        scenario_result.get("hidden_syst_err_count", summary.get("hidden_syst_err_count", 0)) or 0
    )
    artifact["unclassified_syst_err_count"] = int(
        scenario_result.get("unclassified_syst_err_count", summary.get("unclassified_syst_err_count", 0)) or 0
    )
    artifact["pre_route_drain_syst_err_count"] = int(
        scenario_result.get("pre_route_drain_syst_err_count", summary.get("pre_route_drain_syst_err_count", 0)) or 0
    )
    artifact["cleanup_completed"] = bool(
        cleanup_result.get("safe_stop_verified", cleanup_result.get("ok"))
    ) and "error" not in cleanup_result
    if artifact["cleanup_completed"]:
        artifact["closed_valves_final"] = list(CO2_A_APPLY_EXPECTED_BLOCKED_VALVES)
    artifact["dry_run_passed"] = bool(
        scenario_result.get("dry_run_passed")
        and artifact["cleanup_completed"]
        and not artifact.get("co2_24_opened")
        and not artifact.get("h2o_10_opened")
        and not artifact.get("vent2_tx_observed")
    )
    return artifact


def _read_csv_rows(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def _trace_row_count(path: Path) -> int:
    return len(_read_csv_rows(path))


def _disable_unneeded_devices(
    runtime_cfg: Dict[str, Any],
    *,
    need_dewpoint: bool,
    need_analyzer_pressure: bool,
) -> None:
    devices_cfg = runtime_cfg.setdefault("devices", {})
    keep_enabled = {"pressure_controller", "pressure_gauge", "relay", "relay_8"}
    if need_dewpoint:
        keep_enabled.add("dewpoint_meter")
    if need_analyzer_pressure:
        keep_enabled.add("gas_analyzer")
    for name, dev_cfg in devices_cfg.items():
        if name == "gas_analyzers" and isinstance(dev_cfg, list):
            for item in dev_cfg:
                if isinstance(item, dict):
                    item["enabled"] = need_analyzer_pressure and bool(item.get("enabled", True))
            continue
        if not isinstance(dev_cfg, dict) or "enabled" not in dev_cfg:
            continue
        dev_cfg["enabled"] = name in keep_enabled and bool(dev_cfg.get("enabled", False))


def _enabled_configured_analyzer_count(devices_cfg: Mapping[str, Any]) -> int:
    gas_list_cfg = devices_cfg.get("gas_analyzers", [])
    if isinstance(gas_list_cfg, list) and gas_list_cfg:
        return sum(
            1
            for item in gas_list_cfg
            if isinstance(item, dict) and bool(item.get("enabled", True))
        )
    gas_cfg = devices_cfg.get("gas_analyzer", {})
    if isinstance(gas_cfg, dict) and bool(gas_cfg.get("enabled", False)):
        return 1
    return 0


def _mechanical_pressure_protection_confirmed(
    args: argparse.Namespace,
    runtime_cfg: Mapping[str, Any],
) -> bool:
    workflow_cfg = runtime_cfg.get("workflow", {})
    pressure_cfg = workflow_cfg.get("pressure", {}) if isinstance(workflow_cfg, Mapping) else {}
    return bool(
        getattr(args, "mechanical_pressure_protection_confirmed", False)
        or pressure_cfg.get("mechanical_pressure_protection_confirmed", False)
        or workflow_cfg.get("mechanical_pressure_protection_confirmed", False)
        or runtime_cfg.get("mechanical_pressure_protection_confirmed", False)
    )


def _source_or_final_stage_allowed(args: argparse.Namespace) -> bool:
    scenario = str(getattr(args, "scenario", "") or "")
    return bool(
        (scenario in _SOURCE_OPEN_SCENARIOS and getattr(args, "allow_source_open", False))
        or (
            scenario in _H2O_FINAL_STAGE_SCENARIOS
            and getattr(args, "allow_h2o_final_stage_open", False)
        )
    )


def _analyzer_pressure_required(
    args: argparse.Namespace,
    runtime_cfg: Mapping[str, Any],
) -> bool:
    if bool(getattr(args, "analyzer_pressure_required", False)):
        return True
    if not _source_or_final_stage_allowed(args):
        return False
    return not _mechanical_pressure_protection_confirmed(args, runtime_cfg)


def _build_analyzer_pressure_summary(
    cfg: Mapping[str, Any],
    runtime_cfg: Mapping[str, Any],
    args: argparse.Namespace,
) -> Dict[str, Any]:
    before_count = _enabled_configured_analyzer_count(dict(cfg.get("devices", {})))
    after_count = _enabled_configured_analyzer_count(dict(runtime_cfg.get("devices", {})))
    required = _analyzer_pressure_required(args, runtime_cfg)
    mechanical_confirmed = _mechanical_pressure_protection_confirmed(args, runtime_cfg)
    source_or_final_allowed = _source_or_final_stage_allowed(args)
    disabled_reason = ""
    if not required and after_count <= 0:
        if mechanical_confirmed and source_or_final_allowed:
            disabled_reason = "MechanicalPressureProtectionConfirmed"
        elif before_count > 0:
            disabled_reason = "AnalyzerPressureOptionalForScenario"
        else:
            disabled_reason = "AnalyzerNotConfigured"
    return {
        "analyzer_pressure_required": required,
        "analyzer_pressure_available": False,
        "analyzer_pressure_protection_active": False,
        "analyzer_count_before_filter": before_count,
        "analyzer_count_after_filter": after_count,
        "analyzer_list_preserved_for_required_pressure": bool(
            required and before_count > 0 and before_count == after_count
        ),
        "analyzer_disabled_reason": disabled_reason,
        "analyzer_pressure_abort_reason": "",
        "mechanical_pressure_protection_confirmed": mechanical_confirmed,
    }


def _mark_analyzer_pressure_unavailable(
    analyzer_summary: Mapping[str, Any],
    *,
    disabled_reason: str = "",
) -> Dict[str, Any]:
    updated = dict(analyzer_summary or {})
    updated["analyzer_pressure_available"] = False
    updated["analyzer_pressure_protection_active"] = False
    updated["analyzer_pressure_abort_reason"] = "AnalyzerPressureRequiredButUnavailable"
    if disabled_reason:
        updated["analyzer_disabled_reason"] = disabled_reason
    return updated


def _verify_required_analyzer_pressure_protection(
    runner: CalibrationRunner,
    analyzer_summary: Mapping[str, Any],
) -> Dict[str, Any]:
    updated = dict(analyzer_summary or {})
    if not bool(updated.get("analyzer_pressure_required", False)):
        return updated
    if int(updated.get("analyzer_count_after_filter") or 0) <= 0:
        return _mark_analyzer_pressure_unavailable(
            updated,
            disabled_reason="AnalyzerPressureRequiredButUnavailable",
        )
    active_analyzers = list(runner._active_gas_analyzers() or [])
    if not active_analyzers:
        return _mark_analyzer_pressure_unavailable(
            updated,
            disabled_reason="AnalyzerStartupUnavailable",
        )
    try:
        analyzer_pressure_kpa, analyzer_label = runner._read_route_guard_analyzer_pressure_kpa()
    except Exception:
        analyzer_pressure_kpa, analyzer_label = None, ""
    if analyzer_pressure_kpa is None:
        return _mark_analyzer_pressure_unavailable(
            updated,
            disabled_reason="AnalyzerPressureReadUnavailable",
        )
    updated["analyzer_pressure_available"] = True
    updated["analyzer_pressure_protection_active"] = True
    updated["analyzer_disabled_reason"] = ""
    updated["analyzer_pressure_abort_reason"] = ""
    updated["analyzer_pressure_probe_kpa"] = float(analyzer_pressure_kpa)
    updated["analyzer_pressure_probe_label"] = str(analyzer_label or "")
    return updated


def _build_analyzer_pressure_preflight_failure_result(
    *,
    scenario: str,
    trace_path: Path,
    analyzer_summary: Mapping[str, Any],
) -> Dict[str, Any]:
    result = dict(analyzer_summary or {})
    result.update(
        {
            "scenario": scenario,
            "status": "diagnostic_error",
            "route_open_passed": False,
            "abort_reason": "AnalyzerPressureRequiredButUnavailable",
            "pressure_trace_rows": _scenario_trace_rows(trace_path, 0),
        }
    )
    return result


def _merge_summary_analyzer_pressure_fields(
    summary: Mapping[str, Any],
    *,
    fallback: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Any]:
    merged = dict(fallback or {})
    merged.update(
        {
            key: summary.get(key, merged.get(key))
            for key in (
                "analyzer_pressure_required",
                "analyzer_pressure_available",
                "analyzer_pressure_protection_active",
                "analyzer_count_before_filter",
                "analyzer_count_after_filter",
                "analyzer_list_preserved_for_required_pressure",
                "analyzer_disabled_reason",
                "analyzer_pressure_abort_reason",
                "mechanical_pressure_protection_confirmed",
            )
        }
    )
    scenario_result = dict(summary.get("scenario_result") or {})
    route_guard = dict(scenario_result.get("route_pressure_guard_summary") or {})
    point_state = dict(scenario_result.get("point_runtime_state") or {})
    for key in (
        "analyzer_pressure_required",
        "analyzer_pressure_available",
        "analyzer_pressure_protection_active",
        "analyzer_count_before_filter",
        "analyzer_count_after_filter",
        "analyzer_list_preserved_for_required_pressure",
        "analyzer_disabled_reason",
        "analyzer_pressure_abort_reason",
        "mechanical_pressure_protection_confirmed",
    ):
        for source in (scenario_result, route_guard, point_state):
            if source.get(key) not in (None, ""):
                merged[key] = source.get(key)
                break
    return merged


def _prepare_runtime_cfg(
    cfg: Mapping[str, Any],
    args: argparse.Namespace,
    *,
    need_dewpoint: bool,
    need_analyzer_pressure: bool,
) -> Dict[str, Any]:
    runtime_cfg = copy.deepcopy(dict(cfg))
    runtime_cfg.setdefault("paths", {})["output_dir"] = str(_resolve_output_dir(args))
    runtime_cfg.setdefault("workflow", {})["collect_only"] = True
    runtime_cfg["workflow"]["skip_h2o"] = True
    _disable_unneeded_devices(
        runtime_cfg,
        need_dewpoint=need_dewpoint,
        need_analyzer_pressure=need_analyzer_pressure,
    )
    devices_cfg = runtime_cfg.setdefault("devices", {})
    if args.relay_port and isinstance(devices_cfg.get("relay"), dict):
        devices_cfg["relay"]["port"] = str(args.relay_port).strip()
    if args.relay_8_port and isinstance(devices_cfg.get("relay_8"), dict):
        devices_cfg["relay_8"]["port"] = str(args.relay_8_port).strip()

    pressure_cfg = runtime_cfg["workflow"].setdefault("pressure", {})
    pressure_cfg["vent_time_s"] = 0.0
    pressure_cfg["atmosphere_gate_monitor_s"] = float(args.atmosphere_monitor_s)
    pressure_cfg["atmosphere_gate_poll_s"] = float(args.atmosphere_poll_s)
    pressure_cfg["atmosphere_gate_min_samples"] = int(args.atmosphere_min_samples)
    pressure_cfg["atmosphere_gate_pressure_tolerance_hpa"] = float(args.atmosphere_tolerance_hpa)
    pressure_cfg["atmosphere_gate_pressure_rising_slope_max_hpa_s"] = float(args.atmosphere_rising_slope_max_hpa_s)
    pressure_cfg["atmosphere_gate_pressure_rising_min_delta_hpa"] = float(args.atmosphere_rising_min_delta_hpa)
    pressure_cfg["flush_guard_pressure_tolerance_hpa"] = float(args.flush_guard_tolerance_hpa)
    pressure_cfg["flush_guard_pressure_rising_slope_max_hpa_s"] = float(args.flush_guard_rising_slope_max_hpa_s)
    pressure_cfg["flush_guard_pressure_rising_min_delta_hpa"] = float(args.flush_guard_rising_min_delta_hpa)
    pressure_cfg["route_open_guard_enabled"] = True
    pressure_cfg["route_open_guard_monitor_s"] = float(args.route_open_guard_monitor_s)
    pressure_cfg["route_open_guard_poll_s"] = float(args.route_open_guard_poll_s)
    pressure_cfg["route_open_guard_pressure_tolerance_hpa"] = float(args.route_open_guard_tolerance_hpa)
    pressure_cfg["route_open_guard_pressure_rising_slope_max_hpa_s"] = float(
        args.route_open_guard_rising_slope_max_hpa_s
    )
    pressure_cfg["route_open_guard_pressure_rising_min_delta_hpa"] = float(
        args.route_open_guard_rising_min_delta_hpa
    )
    pressure_cfg["route_open_guard_analyzer_warning_kpa"] = float(args.route_open_guard_analyzer_warning_kpa)
    pressure_cfg["route_open_guard_analyzer_abort_kpa"] = float(args.route_open_guard_analyzer_abort_kpa)
    pressure_cfg["route_open_guard_dewpoint_line_tolerance_hpa"] = float(
        args.route_open_guard_dewpoint_line_tolerance_hpa
    )
    approval_path = str(getattr(args, "pressure_protection_approval_json", "") or "").strip()
    if approval_path:
        pressure_cfg["pressure_protection_approval_json"] = approval_path

    stability_cfg = runtime_cfg["workflow"].setdefault("stability", {})
    stability_cfg["gas_route_dewpoint_gate_enabled"] = True
    stability_cfg["gas_route_dewpoint_gate_policy"] = "reject"
    stability_cfg["gas_route_dewpoint_gate_window_s"] = float(args.dewpoint_gate_window_s)
    stability_cfg["gas_route_dewpoint_gate_max_total_wait_s"] = float(args.dewpoint_gate_max_wait_s)
    stability_cfg["gas_route_dewpoint_gate_poll_s"] = float(args.dewpoint_gate_poll_s)
    stability_cfg["gas_route_dewpoint_gate_log_interval_s"] = float(args.dewpoint_gate_log_interval_s)
    return runtime_cfg


def _build_co2_point(
    args: argparse.Namespace,
    *,
    index: int,
    co2_ppm: Optional[float] = None,
    co2_group: Optional[str] = None,
) -> CalibrationPoint:
    point = CalibrationPoint(
        index=index,
        temp_chamber_c=20.0,
        co2_ppm=float(co2_ppm if co2_ppm is not None else args.co2_ppm),
        hgen_temp_c=None,
        hgen_rh_pct=None,
        target_pressure_hpa=float(args.target_pressure_hpa),
        dewpoint_c=None,
        h2o_mmol=None,
        raw_h2o=None,
        co2_group=co2_group,
    )
    return point


def _build_h2o_point(args: argparse.Namespace, *, index: int) -> CalibrationPoint:
    return CalibrationPoint(
        index=index,
        temp_chamber_c=20.0,
        co2_ppm=None,
        hgen_temp_c=20.0,
        hgen_rh_pct=30.0,
        target_pressure_hpa=float(args.target_pressure_hpa),
        dewpoint_c=None,
        h2o_mmol=None,
        raw_h2o=None,
    )


def _build_point(args: argparse.Namespace, *, index: int) -> CalibrationPoint:
    return _build_co2_point(args, index=index)


def _scenario_trace_rows(trace_path: Path, start_row_count: int) -> List[Dict[str, Any]]:
    rows = _read_csv_rows(trace_path)
    return rows[start_row_count:]


def _relay_state_snapshot(devices: Mapping[str, Any]) -> Dict[str, Any]:
    snapshot: Dict[str, Any] = {}
    for relay_name, count in (("relay", 16), ("relay_8", 8)):
        relay = devices.get(relay_name)
        read_coils = getattr(relay, "read_coils", None) if relay is not None else None
        if not callable(read_coils):
            continue
        try:
            bits = list(read_coils(0, count)[:count])
        except Exception as exc:
            snapshot[relay_name] = {"error": str(exc)}
        else:
            snapshot[relay_name] = [bool(value) for value in bits]
    return snapshot


def _drain_pace_errors_for_live_step(runner: CalibrationRunner, *, reason: str) -> Dict[str, Any]:
    drained_errors = list(runner._drain_pace_system_errors(reason=reason) or [])
    post_drain_error = str(runner._read_pace_system_error_text() or "").strip()
    return {
        "pre_existing_error_drained": bool(drained_errors),
        "drained_errors": drained_errors,
        "post_drain_error": post_drain_error,
    }


def _status_from_abort(ok: bool, abort_reason: str) -> str:
    if ok:
        return "pass"
    if abort_reason:
        return "aborted"
    return "fail"


def _pace_error_is_clear(text: Any) -> bool:
    raw = str(text or "").strip()
    if not raw:
        return True
    return raw.startswith("0,") or raw.startswith(':SYST:ERR 0') or raw.startswith(':SYST:ERR 0,')


def _route_guard_summary_payload(runner: CalibrationRunner) -> Dict[str, Any]:
    summary = dict(runner._last_route_pressure_guard_summary or {})
    summary.setdefault("analyzer_pressure_available", False)
    summary.setdefault("analyzer_pressure_protection_active", False)
    summary.setdefault("analyzer_pressure_status", "unavailable")
    if hasattr(runner, "_pace_error_attribution_counts"):
        summary.update(dict(runner._pace_error_attribution_counts() or {}))
    return summary


def _runner_source_stage_safety(runner: CalibrationRunner) -> Dict[str, Any]:
    if hasattr(runner, "_source_stage_safety_snapshot"):
        return dict(runner._source_stage_safety_snapshot() or {})
    return {}


def _runner_route_final_stage_atmosphere_safety(runner: CalibrationRunner) -> Dict[str, Any]:
    if hasattr(runner, "_route_final_stage_atmosphere_safety_snapshot"):
        return dict(runner._route_final_stage_atmosphere_safety_snapshot() or {})
    return {}


def _runner_route_final_stage_seal_safety(runner: CalibrationRunner) -> Dict[str, Any]:
    if hasattr(runner, "_route_final_stage_seal_safety_snapshot"):
        return dict(runner._route_final_stage_seal_safety_snapshot() or {})
    return {}


def _runner_continuous_atmosphere_state(runner: CalibrationRunner) -> Dict[str, Any]:
    if hasattr(runner, "_continuous_atmosphere_state_snapshot"):
        return dict(runner._continuous_atmosphere_state_snapshot() or {})
    return {}


def _build_summary_extract(summary: Mapping[str, Any]) -> Dict[str, Any]:
    scenario_result = dict(summary.get("scenario_result") or {})
    point_state = dict(scenario_result.get("point_runtime_state") or {})
    route_guard = dict(scenario_result.get("route_pressure_guard_summary") or {})
    continuous_state = dict(scenario_result.get("continuous_atmosphere_state") or {})
    pressure_delta = scenario_result.get("pressure_delta_from_ambient_hpa")
    if pressure_delta in (None, ""):
        pressure_delta = point_state.get("pressure_delta_from_ambient_hpa")
    if pressure_delta in (None, ""):
        pressure_delta = route_guard.get("pressure_delta_from_ambient_hpa")
    extract = {
        "pressure_delta_from_ambient_hpa": pressure_delta,
        "route_pressure_guard_status": point_state.get(
            "route_pressure_guard_status",
            route_guard.get("route_pressure_guard_status"),
        ),
        "route_pressure_guard_reason": point_state.get(
            "route_pressure_guard_reason",
            route_guard.get("route_pressure_guard_reason"),
        ),
        "source_stage_safety": scenario_result.get("source_stage_safety", {}),
        "atmosphere_flow_safe": point_state.get(
            "atmosphere_flow_safe",
            point_state.get("route_final_stage_atmosphere_safe"),
        ),
        "seal_pressure_safe": point_state.get(
            "seal_pressure_safe",
            point_state.get("route_final_stage_seal_safe"),
        ),
        "analyzer_pressure_available": route_guard.get(
            "analyzer_pressure_available",
            point_state.get(
                "analyzer_pressure_available",
                scenario_result.get(
                    "analyzer_pressure_available",
                    summary.get("analyzer_pressure_available"),
                ),
            ),
        ),
        "analyzer_pressure_required": scenario_result.get(
            "analyzer_pressure_required",
            summary.get("analyzer_pressure_required"),
        ),
        "analyzer_pressure_protection_active": route_guard.get(
            "analyzer_pressure_protection_active",
            scenario_result.get(
                "analyzer_pressure_protection_active",
                summary.get("analyzer_pressure_protection_active"),
            ),
        ),
        "analyzer_count_before_filter": scenario_result.get(
            "analyzer_count_before_filter",
            summary.get("analyzer_count_before_filter"),
        ),
        "analyzer_count_after_filter": scenario_result.get(
            "analyzer_count_after_filter",
            summary.get("analyzer_count_after_filter"),
        ),
        "analyzer_list_preserved_for_required_pressure": scenario_result.get(
            "analyzer_list_preserved_for_required_pressure",
            summary.get("analyzer_list_preserved_for_required_pressure"),
        ),
        "analyzer_disabled_reason": scenario_result.get(
            "analyzer_disabled_reason",
            summary.get("analyzer_disabled_reason"),
        ),
        "analyzer_pressure_abort_reason": scenario_result.get(
            "analyzer_pressure_abort_reason",
            summary.get("analyzer_pressure_abort_reason"),
        ),
        "mechanical_pressure_protection_confirmed": scenario_result.get(
            "mechanical_pressure_protection_confirmed",
            summary.get("mechanical_pressure_protection_confirmed"),
        ),
        "continuous_atmosphere_active": point_state.get(
            "continuous_atmosphere_active",
            continuous_state.get("active"),
        ),
        "vent_keepalive_count": point_state.get(
            "vent_keepalive_count",
            continuous_state.get("keepalive_count"),
        ),
        "pre_route_drain_syst_err_count": scenario_result.get("pre_route_drain_syst_err_count", 0),
    }
    return extract


def _enrich_live_result_with_pace_diagnostics(
    runner: CalibrationRunner,
    result: Dict[str, Any],
    *,
    final_syst_err: str = "",
) -> Dict[str, Any]:
    enriched = dict(result or {})
    if hasattr(runner, "_pace_error_attribution_counts"):
        enriched.update(dict(runner._pace_error_attribution_counts() or {}))
    else:
        enriched.setdefault("pace_error_attribution_count", 0)
        enriched.setdefault("optional_probe_error_count", 0)
        enriched.setdefault("hidden_syst_err_count", 0)
        enriched.setdefault("unclassified_syst_err_count", 0)
    if hasattr(runner, "_pace_error_attribution_log_snapshot"):
        enriched["pace_error_attribution_log"] = list(runner._pace_error_attribution_log_snapshot() or [])
    else:
        enriched.setdefault("pace_error_attribution_log", [])
    enriched["source_stage_safety"] = _runner_source_stage_safety(runner)
    enriched["route_final_stage_atmosphere_safety"] = _runner_route_final_stage_atmosphere_safety(runner)
    enriched["route_final_stage_seal_safety"] = _runner_route_final_stage_seal_safety(runner)
    enriched["continuous_atmosphere_state"] = _runner_continuous_atmosphere_state(runner)
    enriched["atmosphere_flow_safe"] = dict(enriched["route_final_stage_atmosphere_safety"])
    enriched["seal_pressure_safe"] = dict(enriched["route_final_stage_seal_safety"])
    effective_final_syst_err = str(final_syst_err or runner._read_pace_system_error_text() or "").strip()
    enriched["final_syst_err"] = effective_final_syst_err
    if (
        str(enriched.get("status") or "") == "pass"
        and (
            int(enriched.get("hidden_syst_err_count") or 0) > 0
            or int(enriched.get("unclassified_syst_err_count") or 0) > 0
            or int(enriched.get("pre_route_drain_syst_err_count") or 0) > 0
            or not _pace_error_is_clear(effective_final_syst_err)
        )
    ):
        enriched["status"] = (
            "diagnostic_error"
            if int(enriched.get("unclassified_syst_err_count") or 0) > 0
            else "pass_with_diagnostic_error"
        )
        if int(enriched.get("unclassified_syst_err_count") or 0) > 0 and not str(enriched.get("abort_reason") or "").strip():
            enriched["abort_reason"] = "UnclassifiedPaceSystErrDuringRouteStage"
    if int(enriched.get("pre_route_drain_syst_err_count") or 0) > 0:
        enriched["not_real_acceptance_evidence"] = True
    return enriched


def _run_atmosphere_gate_only(
    runner: CalibrationRunner,
    trace_path: Path,
) -> Dict[str, Any]:
    trace_start = _trace_row_count(trace_path)
    runner._set_co2_route_baseline(reason="live AtmosphereGate-only validation")
    summary = dict(runner._last_atmosphere_gate_summary or {})
    return {
        "scenario": "atmosphere_gate_only",
        "status": "pass" if bool(summary.get("atmosphere_ready")) else "fail",
        "atmosphere_summary": summary,
        "pressure_trace_rows": _scenario_trace_rows(trace_path, trace_start),
    }


def _run_baseline_atmosphere_hold_60s(
    runner: CalibrationRunner,
    trace_path: Path,
    devices: Mapping[str, Any],
    args: argparse.Namespace,
) -> Dict[str, Any]:
    trace_start = _trace_row_count(trace_path)
    drain_summary = _drain_pace_errors_for_live_step(runner, reason="baseline_atmosphere_hold_60s pre-step")
    runner._set_co2_route_baseline(reason="baseline atmosphere hold 60s")
    baseline_summary = dict(runner._last_atmosphere_gate_summary or {})
    pressure_rows: List[tuple[float, float]] = []
    first_sample: Optional[float] = None
    last_sample: Optional[float] = None
    deadline = time.monotonic() + float(args.baseline_hold_monitor_s)
    while True:
        sample = runner._read_current_pressure_hpa_for_atmosphere()
        pressure_hpa = runner._as_float(sample.get("pressure_hpa"))
        now_monotonic = time.monotonic()
        if pressure_hpa is not None:
            pressure_rows.append((now_monotonic, float(pressure_hpa)))
            if first_sample is None:
                first_sample = float(pressure_hpa)
            last_sample = float(pressure_hpa)
        runner._append_pressure_trace_row(
            point=None,
            route="pressure",
            trace_stage="baseline_atmosphere_hold_sample",
            pressure_gauge_hpa=runner._as_float(sample.get("pressure_gauge_hpa")),
            pace_pressure_hpa=runner._as_float(sample.get("pace_pressure_hpa")),
            refresh_pace_state=False,
            note=(
                f"scenario=baseline_atmosphere_hold_60s valve_route_state={runner._current_valve_route_state_text()} "
                f"relay_states={json.dumps(_relay_state_snapshot(devices), ensure_ascii=False)}"
            ),
        )
        if now_monotonic >= deadline:
            break
        time.sleep(max(0.1, float(args.baseline_hold_poll_s)))
    metrics = runner._numeric_series_metrics(pressure_rows)
    pressure_first = runner._as_float(metrics.get("first_value"))
    pressure_last = runner._as_float(metrics.get("last_value"))
    pressure_rise = None if pressure_first is None or pressure_last is None else float(pressure_last) - float(pressure_first)
    pressure_delta = None
    ambient_hpa = runner._as_float(baseline_summary.get("ambient_hpa"))
    if pressure_last is not None and ambient_hpa is not None:
        pressure_delta = float(pressure_last) - float(ambient_hpa)
    result = {
        "scenario": "baseline_atmosphere_hold_60s",
        "status": "pass"
        if bool(baseline_summary.get("atmosphere_ready")) and (pressure_delta is None or abs(float(pressure_delta)) <= 15.0)
        else "fail",
        "atmosphere_summary": baseline_summary,
        "pressure_first_hpa": pressure_first,
        "pressure_last_hpa": pressure_last,
        "pressure_delta_from_ambient_hpa": pressure_delta,
        "pressure_rise_hpa": pressure_rise,
        "pressure_slope_hpa_s": runner._as_float(metrics.get("slope_per_s")),
        "sample_count": int(metrics.get("count") or len(pressure_rows)),
        "valve_route_state": runner._current_valve_route_state_text(),
        "relay_states": _relay_state_snapshot(devices),
        **drain_summary,
        "pressure_trace_rows": _scenario_trace_rows(trace_path, trace_start),
    }
    return _enrich_live_result_with_pace_diagnostics(runner, result)


def _run_route_flush_dewpoint_gate(
    runner: CalibrationRunner,
    trace_path: Path,
    args: argparse.Namespace,
) -> Dict[str, Any]:
    point = _build_point(args, index=9001)
    trace_start = _trace_row_count(trace_path)
    route_open_ok = runner._open_co2_route_for_conditioning(point, point_tag="live_route_flush_dewpoint_gate")
    if route_open_ok:
        ok = runner._wait_co2_route_dewpoint_gate_before_seal(
            point,
            base_soak_s=float(args.route_flush_soak_s),
            log_context="minimal live route flush + dewpoint gate",
        )
    else:
        ok = False
    point_state = dict(runner._point_runtime_state(point, phase="co2") or {})
    abort_reason = str(point_state.get("abort_reason") or "").strip()
    return {
        "scenario": "route_flush_dewpoint_gate",
        "status": _status_from_abort(bool(ok), abort_reason),
        "gate_passed": bool(ok),
        "route_open_passed": bool(route_open_ok),
        "abort_reason": abort_reason,
        "point_runtime_state": point_state,
        "atmosphere_summary": dict(runner._last_atmosphere_gate_summary or {}),
        "route_pressure_guard_summary": dict(runner._last_route_pressure_guard_summary or {}),
        "pressure_trace_rows": _scenario_trace_rows(trace_path, trace_start),
    }


def _run_route_synchronized_atmosphere_flush_co2_a_no_source(
    runner: CalibrationRunner,
    trace_path: Path,
    args: argparse.Namespace,
) -> Dict[str, Any]:
    point = _build_co2_point(args, index=9010, co2_ppm=600.0, co2_group="A")
    trace_start = _trace_row_count(trace_path)
    drain_summary = _drain_pace_errors_for_live_step(runner, reason="route_synchronized_atmosphere_flush_co2_a_no_source pre-step")
    runner._clear_last_sealed_pressure_route_context(reason="live synchronized CO2 A no-source route flush")
    runner._clear_pressure_sequence_context(reason="live synchronized CO2 A no-source route flush")
    runner._set_co2_route_baseline(reason="live synchronized CO2 A no-source route flush baseline")
    open_valves = runner._co2_open_valves(point, include_total_valve=True, include_source_valve=False)
    ok = runner._open_route_with_pressure_guard(
        point,
        phase="co2",
        point_tag="live_route_sync_atmosphere_flush_co2_a_no_source",
        open_valves=open_valves,
        log_context="live synchronized CO2 A no-source route flush",
    )
    point_state = dict(runner._point_runtime_state(point, phase="co2") or {})
    abort_reason = str(point_state.get("abort_reason") or "").strip()
    return _enrich_live_result_with_pace_diagnostics(runner, {
        "scenario": "route_synchronized_atmosphere_flush_co2_a_no_source",
        "status": _status_from_abort(bool(ok), abort_reason),
        "route_open_passed": bool(ok),
        "abort_reason": abort_reason,
        "point_runtime_state": point_state,
        "open_valves": open_valves,
        "atmosphere_summary": dict(runner._last_atmosphere_gate_summary or {}),
        "route_pressure_guard_summary": _route_guard_summary_payload(runner),
        **drain_summary,
        "pressure_trace_rows": _scenario_trace_rows(trace_path, trace_start),
    })


def _run_route_synchronized_atmosphere_flush_co2_b_no_source(
    runner: CalibrationRunner,
    trace_path: Path,
    args: argparse.Namespace,
) -> Dict[str, Any]:
    point = _build_co2_point(args, index=9011, co2_ppm=500.0, co2_group="B")
    trace_start = _trace_row_count(trace_path)
    drain_summary = _drain_pace_errors_for_live_step(runner, reason="route_synchronized_atmosphere_flush_co2_b_no_source pre-step")
    runner._clear_last_sealed_pressure_route_context(reason="live synchronized CO2 B no-source route flush")
    runner._clear_pressure_sequence_context(reason="live synchronized CO2 B no-source route flush")
    runner._set_co2_route_baseline(reason="live synchronized CO2 B no-source route flush baseline")
    open_valves = runner._co2_open_valves(point, include_total_valve=True, include_source_valve=False)
    ok = runner._open_route_with_pressure_guard(
        point,
        phase="co2",
        point_tag="live_route_sync_atmosphere_flush_co2_b_no_source",
        open_valves=open_valves,
        log_context="live synchronized CO2 B no-source route flush",
    )
    point_state = dict(runner._point_runtime_state(point, phase="co2") or {})
    abort_reason = str(point_state.get("abort_reason") or "").strip()
    return _enrich_live_result_with_pace_diagnostics(runner, {
        "scenario": "route_synchronized_atmosphere_flush_co2_b_no_source",
        "status": _status_from_abort(bool(ok), abort_reason),
        "route_open_passed": bool(ok),
        "abort_reason": abort_reason,
        "point_runtime_state": point_state,
        "open_valves": open_valves,
        "atmosphere_summary": dict(runner._last_atmosphere_gate_summary or {}),
        "route_pressure_guard_summary": _route_guard_summary_payload(runner),
        **drain_summary,
        "pressure_trace_rows": _scenario_trace_rows(trace_path, trace_start),
    })


def _run_co2_a_staged_source_final_release_dry_run(
    runner: CalibrationRunner,
    trace_path: Path,
    args: argparse.Namespace,
    *,
    route: str = "CO2_A",
    release_scope: str = CO2_A_STAGED_RELEASE_SCOPE,
    front_valves: Optional[Iterable[Any]] = None,
    source_final_valve_under_test: int = CO2_A_SOURCE_FINAL_VALVE,
    blocked_valves_required_closed: Optional[Iterable[Any]] = None,
) -> Dict[str, Any]:
    point = _build_co2_point(args, index=9017, co2_ppm=600.0, co2_group="A")
    trace_start = _trace_row_count(trace_path)
    front_valves_list = _normalized_valves(front_valves or CO2_A_FRONT_VALVES)
    blocked_valves_list = _normalized_valves(blocked_valves_required_closed or CO2_A_BLOCKED_VALVES)
    result = _build_co2_a_staged_source_final_result(
        trace_path=trace_path,
        trace_start=trace_start,
        route=route,
        release_scope=release_scope,
        front_valves=front_valves_list,
        source_final_valve_under_test=source_final_valve_under_test,
        blocked_valves_required_closed=blocked_valves_list,
    )

    def _refresh_result_snapshot() -> None:
        point_state = dict(runner._point_runtime_state(point, phase="co2") or {})
        route_guard_summary = _route_guard_summary_payload(runner)
        counts = dict(runner._pace_error_attribution_counts() or {}) if hasattr(runner, "_pace_error_attribution_counts") else {}
        open_valves = _runner_current_open_valves(runner)
        pace_commands = _runner_command_list(runner, "pace_commands_sent")
        vent_write_commands = _runner_command_list(runner, "vent_write_commands")
        vent_query_responses = _runner_command_list(runner, "vent_query_responses")
        result["point_runtime_state"] = point_state
        result["route_pressure_guard_summary"] = route_guard_summary
        result["opened_valves"] = open_valves
        result["source_stage_key"] = str(
            result.get("source_stage_key")
            or point_state.get("source_stage_key")
            or (
                runner._source_stage_key_for_point(point, phase="co2")
                if hasattr(runner, "_source_stage_key_for_point")
                else "co2_a"
            )
            or "co2_a"
        ).strip()
        result["co2_4_opened"] = CO2_A_SOURCE_FINAL_VALVE in open_valves
        result["co2_24_opened"] = 24 in open_valves
        result["h2o_10_opened"] = 10 in open_valves
        result["source_final_stage_opened"] = any(valve in {4, 24, 10} for valve in open_valves)
        result["pace_commands_sent"] = pace_commands
        result["vent_write_commands"] = vent_write_commands
        result["vent_query_responses"] = vent_query_responses
        result["vent2_tx_observed"] = any("VENT 2" in str(item or "") for item in [*pace_commands, *vent_write_commands])
        result["final_syst_err"] = str(runner._read_pace_system_error_text() or "").strip()
        result["hidden_syst_err_count"] = int(counts.get("hidden_syst_err_count") or 0)
        result["unclassified_syst_err_count"] = int(counts.get("unclassified_syst_err_count") or 0)
        result["pre_route_drain_syst_err_count"] = int(counts.get("pre_route_drain_syst_err_count") or 0)
        result["pressure_trace_rows"] = _scenario_trace_rows(trace_path, trace_start)

    def _finalize(status: str, *, abort_reason: str = "", dry_run_passed: bool = False) -> Dict[str, Any]:
        if abort_reason:
            result["abort_reason"] = str(abort_reason)
        result["status"] = status
        result["dry_run_passed"] = bool(dry_run_passed)
        _refresh_result_snapshot()
        return _enrich_live_result_with_pace_diagnostics(
            runner,
            result,
            final_syst_err=str(result.get("final_syst_err") or ""),
        )

    result["scope_validation_reasons"] = _validate_co2_a_staged_source_final_scope(
        route=route,
        release_scope=release_scope,
        front_valves=front_valves_list,
        source_final_valve_under_test=source_final_valve_under_test,
        blocked_valves_required_closed=blocked_valves_list,
    )
    if result["scope_validation_reasons"]:
        return _finalize("skipped", abort_reason=str(result["scope_validation_reasons"][0] or "InvalidStagedDryRunScope"))

    runtime_cfg = getattr(args, "_runtime_cfg", {})
    pressure_protection_resolution = resolve_co2_a_staged_pressure_protection(
        runtime_cfg if isinstance(runtime_cfg, Mapping) else {},
        approval_json_path=getattr(args, "pressure_protection_approval_json", None),
        route=result["route"],
        source_final_valve=int(result["source_final_valve_under_test"]),
        release_scope=result["release_scope"],
    )
    result["pressure_protection_resolution"] = dict(pressure_protection_resolution)
    result["pressure_protection_source"] = str(pressure_protection_resolution.get("pressure_protection_source") or "missing")
    result["approval_scope"] = str(pressure_protection_resolution.get("approval_scope") or "")
    result["retry_allowed_for_scope"] = bool(pressure_protection_resolution.get("retry_allowed_for_scope"))
    result["analyzer_pressure_protection_active"] = bool(
        pressure_protection_resolution.get("analyzer_pressure_protection_active")
    )
    result["mechanical_pressure_protection_confirmed"] = bool(
        pressure_protection_resolution.get("mechanical_pressure_protection_confirmed")
    )
    result["pressure_protection_precheck_satisfied"] = bool(
        pressure_protection_resolution.get("pressure_protection_precheck_satisfied")
    )

    operator_env = _co2_a_staged_source_final_env_status()
    result["operator_env"] = operator_env
    result["missing_operator_env"] = list(operator_env.get("missing") or [])
    result["operator_confirmation_missing"] = bool(operator_env.get("operator_confirmation_missing"))
    if result["operator_confirmation_missing"]:
        return _finalize("skipped", abort_reason="operator_confirmation_missing")

    capability_snapshot = (
        dict(
            runner._capture_pace_capability_snapshot(
                reason="co2_a_staged_source_final_release_dry_run precheck",
                include_optional_probe=True,
            )
            or {}
        )
        if hasattr(runner, "_capture_pace_capability_snapshot")
        else {}
    )
    result["k0472_capability_snapshot"] = capability_snapshot
    drain_summary = _drain_pace_errors_for_live_step(
        runner,
        reason="co2_a_staged_source_final_release_dry_run pre-step",
    )

    expected_no_source_front_valves = (
        _normalized_valves(runner._co2_open_valves(point, include_total_valve=True, include_source_valve=False))
        if hasattr(runner, "_co2_open_valves")
        else []
    )
    current_open_before = _runner_current_open_valves(runner)
    precheck_reasons: List[str] = []
    if not capability_snapshot:
        precheck_reasons.append("PaceCommunicationUnavailable")
    if str(capability_snapshot.get("profile") or "").strip().upper() != "OLD_PACE5000":
        precheck_reasons.append("OnlyOldPace5000ProfileSupported")
    precheck_syst_err = str(
        drain_summary.get("post_drain_error")
        or capability_snapshot.get("final_syst_err")
        or runner._read_pace_system_error_text()
        or ""
    ).strip()
    if not _pace_error_is_clear(precheck_syst_err):
        precheck_reasons.append("PaceSystemErrorNotClear")
    if expected_no_source_front_valves != front_valves_list:
        precheck_reasons.append("CO2AFrontPathMustRemain8_11_7")
    if any(valve in current_open_before for valve in blocked_valves_list):
        precheck_reasons.append("BlockedValvesAlreadyOpen")

    runner._clear_last_sealed_pressure_route_context(reason="co2_a staged source/final dry-run precheck")
    runner._clear_pressure_sequence_context(reason="co2_a staged source/final dry-run precheck")
    runner._set_co2_route_baseline(reason="co2_a staged source/final dry-run precheck baseline")
    precheck_open_ok = False
    if not precheck_reasons:
        precheck_open_ok = runner._open_route_with_pressure_guard(
            point,
            phase="co2",
            point_tag="live_route_sync_co2_a_staged_source_final_precheck",
            open_valves=front_valves_list,
            log_context="CO2 A staged source/final dry-run precheck",
        )
        if not precheck_open_ok:
            precheck_reasons.append(
                str((runner._point_runtime_state(point, phase="co2") or {}).get("abort_reason") or "RouteOpenPressureGuardFailed")
            )

    pressure_sample = (
        dict(runner._read_current_pressure_hpa_for_atmosphere() or {})
        if hasattr(runner, "_read_current_pressure_hpa_for_atmosphere")
        else {}
    )
    point_state = dict(runner._point_runtime_state(point, phase="co2") or {})
    route_guard_summary = _route_guard_summary_payload(runner)
    continuous_state = _runner_continuous_atmosphere_state(runner)
    pace_state_snapshot = (
        dict(runner._pace_state_snapshot(refresh=True) or {})
        if hasattr(runner, "_pace_state_snapshot")
        else {}
    )
    pace_cache = dict(getattr(runner, "_pace_state_cache", {}) or {})
    open_after_precheck = _runner_current_open_valves(runner)
    route_key = str(
        (runner._source_stage_key_for_point(point, phase="co2") if hasattr(runner, "_source_stage_key_for_point") else "")
        or point_state.get("source_stage_key")
        or "co2_a"
    ).strip()
    result["source_stage_key"] = route_key

    pressure_hpa = None
    if hasattr(runner, "_as_float"):
        pressure_hpa = runner._as_float(pressure_sample.get("pressure_hpa"))
    pressure_gauge_hpa = None
    if hasattr(runner, "_as_float"):
        pressure_gauge_hpa = runner._as_float(pressure_sample.get("pressure_gauge_hpa"))
    pressure_gauge_available = pressure_gauge_hpa is not None or pressure_hpa is not None or bool(
        point_state.get("pressure_gauge_available")
    )
    pressure_read_fresh = bool(pressure_gauge_available)
    in_limits_cache_fresh = bool(
        pace_cache.get("in_limits_cache_valid")
        or pace_state_snapshot.get("pace_oper_pres_in_limits_bit") not in ("", None)
        or pace_state_snapshot.get("pace_oper_pres_even_query") not in ("", None)
    )
    target_pressure_supported = (
        bool(runner._seal_pressure_target_supported_by_hardware(point))
        if hasattr(runner, "_seal_pressure_target_supported_by_hardware")
        else True
    )
    analyzer_pressure_protection_active = bool(route_guard_summary.get("analyzer_pressure_protection_active")) or bool(
        pressure_protection_resolution.get("analyzer_pressure_protection_active")
    )
    mechanical_pressure_protection_confirmed = (
        bool(runner._mechanical_pressure_protection_confirmed())
        if hasattr(runner, "_mechanical_pressure_protection_confirmed")
        else False
    ) or bool(pressure_protection_resolution.get("mechanical_pressure_protection_confirmed"))
    result["analyzer_pressure_protection_active"] = bool(analyzer_pressure_protection_active)
    result["mechanical_pressure_protection_confirmed"] = bool(mechanical_pressure_protection_confirmed)
    result["pressure_protection_precheck_satisfied"] = bool(
        analyzer_pressure_protection_active or mechanical_pressure_protection_confirmed
    )
    source_stage_safe = bool((_runner_source_stage_safety(runner) or {}).get(route_key, False))
    route_final_stage_atmosphere_safe = bool((_runner_route_final_stage_atmosphere_safety(runner) or {}).get(route_key, False))
    source_final_valves_open = [valve for valve in open_after_precheck if valve in CO2_A_APPLY_EXPECTED_BLOCKED_VALVES]
    if open_after_precheck != front_valves_list:
        precheck_reasons.append("NoSourceFrontPathNotConfirmed")
    if CO2_A_SOURCE_FINAL_VALVE in open_after_precheck:
        precheck_reasons.append("Valve4OpenedBeforeExplicitApply")
    if any(valve in open_after_precheck for valve in blocked_valves_list):
        precheck_reasons.append("NonTargetBlockedValveOpenedDuringPrecheck")
    if bool(continuous_state.get("active")) or bool(continuous_state.get("route_flow_active")):
        precheck_reasons.append("ActiveAtmosphereKeepalive")
    if not pressure_read_fresh:
        precheck_reasons.append("PressureReadNotFresh")
    if not in_limits_cache_fresh:
        precheck_reasons.append("StaleInLimitsCache")
    if not target_pressure_supported:
        precheck_reasons.append("PressureTargetUnsupportedByHardware")
    if not (analyzer_pressure_protection_active or mechanical_pressure_protection_confirmed):
        precheck_reasons.extend(
            [
                str(item)
                for item in list(pressure_protection_resolution.get("reasons") or [])
                if str(item or "").strip()
            ]
        )
        precheck_reasons.append("PressureProtectionNotConfirmed")
    if not source_stage_safe:
        precheck_reasons.append("SourceStageNotVerified")
    if not route_final_stage_atmosphere_safe:
        precheck_reasons.append("AtmosphereFlowStageNotVerified")

    result["precheck"] = {
        "pace_communication_ok": bool(capability_snapshot),
        "profile": str(capability_snapshot.get("profile") or ""),
        "front_valves_confirmed": front_valves_list,
        "expected_front_valves": expected_no_source_front_valves,
        "current_open_valves_before": current_open_before,
        "current_open_valves_after_precheck": open_after_precheck,
        "precheck_open_ok": bool(precheck_open_ok),
        "pressure_read_fresh": bool(pressure_read_fresh),
        "pressure_gauge_available": bool(pressure_gauge_available),
        "in_limits_cache_fresh": bool(in_limits_cache_fresh),
        "target_pressure_supported": bool(target_pressure_supported),
        "analyzer_pressure_protection_active": bool(analyzer_pressure_protection_active),
        "mechanical_pressure_protection_confirmed": bool(mechanical_pressure_protection_confirmed),
        "pressure_protection_source": str(pressure_protection_resolution.get("pressure_protection_source") or "missing"),
        "pressure_protection_precheck_satisfied": bool(result["pressure_protection_precheck_satisfied"]),
        "pressure_protection_resolution": dict(pressure_protection_resolution),
        "source_stage_safe": bool(source_stage_safe),
        "route_final_stage_atmosphere_safe": bool(route_final_stage_atmosphere_safe),
        "source_final_valves_open": list(source_final_valves_open),
        "post_drain_error": precheck_syst_err,
        "blocked_reasons": list(precheck_reasons),
        **drain_summary,
    }
    if precheck_reasons:
        return _finalize("diagnostic_error", abort_reason=str(precheck_reasons[0] or "StagedSourceFinalPrecheckFailed"))

    verification = runner.verify_seal_pressure_stage_preconditions(
        point,
        phase="co2",
        evidence_source="live_safe_preflight",
        verification_inputs={
            "source_stage_key": route_key,
            "source_stage_safe": source_stage_safe,
            "route_final_stage_atmosphere_safe": route_final_stage_atmosphere_safe,
            "route_final_stage_seal_safe": False,
            "pressure_gauge_available": pressure_gauge_available,
            "pressure_read_fresh": pressure_read_fresh,
            "in_limits_cache_fresh": in_limits_cache_fresh,
            "target_pressure_supported": target_pressure_supported,
            "analyzer_pressure_protection_active": analyzer_pressure_protection_active,
            "mechanical_pressure_protection_confirmed": mechanical_pressure_protection_confirmed,
            "post_exit_vent1_count": 0,
            "vent2_tx_count": 0,
            "exit_boundary_vent0_count": 0,
            "blocked_valves": CO2_A_APPLY_EXPECTED_BLOCKED_VALVES,
            "source_final_valves_open": [],
            "final_syst_err": precheck_syst_err,
            "hidden_syst_err_count": int(result.get("hidden_syst_err_count") or 0),
            "unclassified_syst_err_count": int(result.get("unclassified_syst_err_count") or 0),
            "pre_route_drain_syst_err_count": int(result.get("pre_route_drain_syst_err_count") or 0),
        },
    )
    result["verification"] = dict(verification or {})
    if not bool(verification.get("eligible")):
        return _finalize(
            "diagnostic_error",
            abort_reason=str(verification.get("reason") or "SealPressureStagePreconditionsNotEligible"),
        )

    candidate = runner.evaluate_seal_pressure_verified_release_candidate(
        verification,
        explicit_allow=True,
        pressure_read_fresh=pressure_read_fresh,
        in_limits_cache_fresh=in_limits_cache_fresh,
        target_pressure_supported=target_pressure_supported,
        analyzer_pressure_protection_confirmed=analyzer_pressure_protection_active,
        mechanical_pressure_protection_confirmed=mechanical_pressure_protection_confirmed,
        active_atmosphere_keepalive=False,
        post_exit_vent_leak=False,
        hidden_syst_err_count=int(result.get("hidden_syst_err_count") or 0),
        unclassified_syst_err_count=int(result.get("unclassified_syst_err_count") or 0),
        pre_route_drain_syst_err_count=int(result.get("pre_route_drain_syst_err_count") or 0),
        vent2_tx_observed=False,
        source_final_stage_explicit_safety=True,
    )
    result["candidate"] = dict(candidate or {})
    result["candidate_eligible"] = bool(candidate.get("eligible_for_explicit_release"))
    if not result["candidate_eligible"]:
        return _finalize(
            "diagnostic_error",
            abort_reason=str(candidate.get("reason") or "CandidateNotEligible"),
        )

    apply_result = runner.apply_seal_pressure_verified_release_candidate(
        source_stage_key=route_key,
        candidate=candidate,
        explicit_apply=True,
        operator_intent_confirmed=True,
        release_reason=str(operator_env.get("release_reason") or ""),
        release_scope=release_scope,
        expected_blocked_valves=CO2_A_APPLY_EXPECTED_BLOCKED_VALVES,
        expected_source_final_valves=[CO2_A_SOURCE_FINAL_VALVE],
        dry_run=True,
    )
    result["apply_result"] = dict(apply_result or {})
    result["explicit_apply_succeeded"] = bool(apply_result.get("release_performed"))
    result["route_final_stage_seal_safety_updated"] = bool(apply_result.get("route_final_stage_seal_safety_updated"))
    result["route_final_stage_seal_safety_key"] = str(apply_result.get("route_final_stage_seal_safety_key") or route_key)
    if not result["explicit_apply_succeeded"]:
        return _finalize(
            "diagnostic_error",
            abort_reason=str(apply_result.get("reason") or "ExplicitApplyFailed"),
        )

    if not hasattr(runner, "_open_route_with_pressure_guard"):
        return _finalize("diagnostic_error", abort_reason="valve_open_only_dry_run_not_supported")

    staged_open_valves = (
        _normalized_valves(runner._co2_open_valves(point, include_total_valve=True, include_source_valve=True))
        if hasattr(runner, "_co2_open_valves")
        else []
    )
    if staged_open_valves != CO2_A_FRONT_VALVES + [CO2_A_SOURCE_FINAL_VALVE]:
        return _finalize("diagnostic_error", abort_reason="valve_open_only_dry_run_not_supported")

    try:
        staged_open_ok = runner._open_route_with_pressure_guard(
            point,
            phase="co2",
            point_tag="live_route_sync_co2_a_staged_source_final_release_dry_run",
            open_valves=staged_open_valves,
            log_context="CO2 A staged source/final dry-run",
        )
    except Exception as exc:
        return _finalize("fail", abort_reason=str(exc) or "StagedSourceFinalDryRunFailed")

    _refresh_result_snapshot()
    if not staged_open_ok:
        return _finalize(
            "fail",
            abort_reason=str(result["point_runtime_state"].get("abort_reason") or "StagedSourceFinalDryRunFailed"),
        )
    if result["co2_24_opened"] or result["h2o_10_opened"]:
        return _finalize("fail", abort_reason="NonTargetSourceFinalValveOpened")
    if result["vent2_tx_observed"]:
        return _finalize("fail", abort_reason="Vent2CommandObserved")
    if not result["co2_4_opened"]:
        return _finalize("fail", abort_reason="Valve4DidNotOpen")
    return _finalize("pass", dry_run_passed=True)


def _run_route_synchronized_atmosphere_flush_co2_a_source_guarded(
    runner: CalibrationRunner,
    trace_path: Path,
    args: argparse.Namespace,
) -> Dict[str, Any]:
    point = _build_co2_point(args, index=9013, co2_ppm=600.0, co2_group="A")
    trace_start = _trace_row_count(trace_path)
    open_valves = runner._co2_open_valves(point, include_total_valve=True, include_source_valve=True)
    if not bool(getattr(args, "allow_source_open", False)):
        return _enrich_live_result_with_pace_diagnostics(runner, {
            "scenario": "route_synchronized_atmosphere_flush_co2_a_source_guarded",
            "status": "skipped",
            "skipped_reason": "SourceOpenRequiresExplicitAllowFlag",
            "operator_must_confirm_upstream_source_pressure_limited": True,
            "open_valves": open_valves,
            "pressure_trace_rows": _scenario_trace_rows(trace_path, trace_start),
        })
    _log("operator_must_confirm_upstream_source_pressure_limited=true")
    drain_summary = _drain_pace_errors_for_live_step(runner, reason="route_synchronized_atmosphere_flush_co2_a_source_guarded pre-step")
    ok = runner._open_co2_route_for_conditioning(point, point_tag="live_route_sync_atmosphere_flush_co2_a_source_guarded")
    point_state = dict(runner._point_runtime_state(point, phase="co2") or {})
    abort_reason = str(point_state.get("abort_reason") or "").strip()
    return _enrich_live_result_with_pace_diagnostics(runner, {
        "scenario": "route_synchronized_atmosphere_flush_co2_a_source_guarded",
        "status": _status_from_abort(bool(ok), abort_reason),
        "route_open_passed": bool(ok),
        "abort_reason": abort_reason,
        "operator_must_confirm_upstream_source_pressure_limited": True,
        "point_runtime_state": point_state,
        "open_valves": open_valves,
        "atmosphere_summary": dict(runner._last_atmosphere_gate_summary or {}),
        "route_pressure_guard_summary": _route_guard_summary_payload(runner),
        **drain_summary,
        "pressure_trace_rows": _scenario_trace_rows(trace_path, trace_start),
    })


def _run_route_synchronized_atmosphere_flush_co2_b_source_guarded(
    runner: CalibrationRunner,
    trace_path: Path,
    args: argparse.Namespace,
) -> Dict[str, Any]:
    point = _build_co2_point(args, index=9014, co2_ppm=500.0, co2_group="B")
    trace_start = _trace_row_count(trace_path)
    open_valves = runner._co2_open_valves(point, include_total_valve=True, include_source_valve=True)
    if not bool(getattr(args, "allow_source_open", False)):
        return _enrich_live_result_with_pace_diagnostics(runner, {
            "scenario": "route_synchronized_atmosphere_flush_co2_b_source_guarded",
            "status": "skipped",
            "skipped_reason": "SourceOpenRequiresExplicitAllowFlag",
            "operator_must_confirm_upstream_source_pressure_limited": True,
            "open_valves": open_valves,
            "pressure_trace_rows": _scenario_trace_rows(trace_path, trace_start),
        })
    _log("operator_must_confirm_upstream_source_pressure_limited=true")
    drain_summary = _drain_pace_errors_for_live_step(runner, reason="route_synchronized_atmosphere_flush_co2_b_source_guarded pre-step")
    ok = runner._open_co2_route_for_conditioning(point, point_tag="live_route_sync_atmosphere_flush_co2_b_source_guarded")
    point_state = dict(runner._point_runtime_state(point, phase="co2") or {})
    abort_reason = str(point_state.get("abort_reason") or "").strip()
    return _enrich_live_result_with_pace_diagnostics(runner, {
        "scenario": "route_synchronized_atmosphere_flush_co2_b_source_guarded",
        "status": _status_from_abort(bool(ok), abort_reason),
        "route_open_passed": bool(ok),
        "abort_reason": abort_reason,
        "operator_must_confirm_upstream_source_pressure_limited": True,
        "point_runtime_state": point_state,
        "open_valves": open_valves,
        "atmosphere_summary": dict(runner._last_atmosphere_gate_summary or {}),
        "route_pressure_guard_summary": _route_guard_summary_payload(runner),
        **drain_summary,
        "pressure_trace_rows": _scenario_trace_rows(trace_path, trace_start),
    })


def _run_route_synchronized_atmosphere_flush_h2o_no_final(
    runner: CalibrationRunner,
    trace_path: Path,
    args: argparse.Namespace,
) -> Dict[str, Any]:
    point = _build_h2o_point(args, index=9015)
    trace_start = _trace_row_count(trace_path)
    drain_summary = _drain_pace_errors_for_live_step(runner, reason="route_synchronized_atmosphere_flush_h2o_no_final pre-step")
    runner._clear_last_sealed_pressure_route_context(reason="live synchronized H2O no-final route flush")
    runner._clear_pressure_sequence_context(reason="live synchronized H2O no-final route flush")
    runner._apply_route_baseline_valves()
    runner._set_pressure_controller_vent(True, reason="live synchronized H2O no-final route flush baseline")
    open_valves = runner._h2o_open_valves(point, include_final_stage=False)
    ok = runner._open_route_with_pressure_guard(
        point,
        phase="h2o",
        point_tag="live_route_sync_atmosphere_flush_h2o_no_final",
        open_valves=open_valves,
        log_context="live synchronized H2O no-final route flush",
    )
    point_state = dict(runner._point_runtime_state(point, phase="h2o") or {})
    abort_reason = str(point_state.get("abort_reason") or "").strip()
    return _enrich_live_result_with_pace_diagnostics(runner, {
        "scenario": "route_synchronized_atmosphere_flush_h2o_no_final",
        "status": _status_from_abort(bool(ok), abort_reason),
        "route_open_passed": bool(ok),
        "abort_reason": abort_reason,
        "point_runtime_state": point_state,
        "open_valves": open_valves,
        "skipped_final_stage": 10,
        "skipped_reason": "H2OFinalStage10RequiresExplicitAllowFlag",
        "atmosphere_summary": dict(runner._last_atmosphere_gate_summary or {}),
        "route_pressure_guard_summary": _route_guard_summary_payload(runner),
        **drain_summary,
        "pressure_trace_rows": _scenario_trace_rows(trace_path, trace_start),
    })


def _run_route_synchronized_atmosphere_flush_h2o(
    runner: CalibrationRunner,
    trace_path: Path,
    args: argparse.Namespace,
) -> Dict[str, Any]:
    point = _build_h2o_point(args, index=9012)
    trace_start = _trace_row_count(trace_path)
    open_valves = runner._h2o_open_valves(point)
    if not bool(getattr(args, "allow_h2o_final_stage_open", False)):
        return _enrich_live_result_with_pace_diagnostics(runner, {
            "scenario": "route_synchronized_atmosphere_flush_h2o",
            "status": "skipped",
            "skipped_reason": "H2OFinalStage10RequiresExplicitAllowFlag",
            "operator_must_confirm_h2o_upstream_pressure_limited": True,
            "open_valves": open_valves,
            "pressure_trace_rows": _scenario_trace_rows(trace_path, trace_start),
        })
    drain_summary = _drain_pace_errors_for_live_step(runner, reason="route_synchronized_atmosphere_flush_h2o pre-step")
    runner._clear_last_sealed_pressure_route_context(reason="live synchronized H2O route flush")
    runner._clear_pressure_sequence_context(reason="live synchronized H2O route flush")
    runner._apply_route_baseline_valves()
    runner._set_pressure_controller_vent(True, reason="live synchronized H2O route flush baseline")
    ok = runner._open_route_with_pressure_guard(
        point,
        phase="h2o",
        point_tag="live_route_sync_atmosphere_flush_h2o",
        open_valves=open_valves,
        log_context="live synchronized H2O route flush",
    )
    point_state = dict(runner._point_runtime_state(point, phase="h2o") or {})
    abort_reason = str(point_state.get("abort_reason") or "").strip()
    return _enrich_live_result_with_pace_diagnostics(runner, {
        "scenario": "route_synchronized_atmosphere_flush_h2o",
        "status": _status_from_abort(bool(ok), abort_reason),
        "route_open_passed": bool(ok),
        "abort_reason": abort_reason,
        "operator_must_confirm_h2o_upstream_pressure_limited": True,
        "point_runtime_state": point_state,
        "open_valves": open_valves,
        "atmosphere_summary": dict(runner._last_atmosphere_gate_summary or {}),
        "route_pressure_guard_summary": _route_guard_summary_payload(runner),
        **drain_summary,
        "pressure_trace_rows": _scenario_trace_rows(trace_path, trace_start),
    })


def _run_continuous_atmosphere_keepalive_probe_no_source(
    runner: CalibrationRunner,
    trace_path: Path,
    args: argparse.Namespace,
) -> Dict[str, Any]:
    point = _build_co2_point(args, index=9016, co2_ppm=600.0, co2_group="A")
    trace_start = _trace_row_count(trace_path)
    drain_summary = _drain_pace_errors_for_live_step(
        runner,
        reason="continuous_atmosphere_keepalive_probe_no_source pre-step",
    )
    runner._clear_last_sealed_pressure_route_context(reason="continuous atmosphere keepalive probe no-source")
    runner._clear_pressure_sequence_context(reason="continuous atmosphere keepalive probe no-source")
    runner._set_co2_route_baseline(reason="continuous atmosphere keepalive probe no-source baseline")
    open_valves = runner._co2_open_valves(point, include_total_valve=True, include_source_valve=False)
    ok = runner._open_route_with_pressure_guard(
        point,
        phase="co2",
        point_tag="continuous_atmosphere_keepalive_probe_no_source",
        open_valves=open_valves,
        log_context="continuous atmosphere keepalive probe no-source",
    )
    keepalive_samples: List[Dict[str, Any]] = []
    if ok and hasattr(runner, "maintain_continuous_atmosphere_flowthrough"):
        deadline = time.monotonic() + float(getattr(args, "continuous_keepalive_probe_s", 6.0))
        while time.monotonic() < deadline:
            keepalive_ok, keepalive_state = runner.maintain_continuous_atmosphere_flowthrough(  # type: ignore[attr-defined]
                "co2_a",
                point=point,
                phase="co2",
                point_tag="continuous_atmosphere_keepalive_probe_no_source",
                phase_name="ContinuousAtmosphereFlowThrough",
                reason="continuous atmosphere keepalive probe no-source",
            )
            keepalive_samples.append(dict(keepalive_state or {}))
            if not keepalive_ok:
                ok = False
                break
            time.sleep(min(1.0, max(0.2, float(getattr(args, "continuous_keepalive_probe_poll_s", 1.0)))))
    point_state = dict(runner._point_runtime_state(point, phase="co2") or {})
    abort_reason = str(point_state.get("abort_reason") or "").strip()
    return _enrich_live_result_with_pace_diagnostics(
        runner,
        {
            "scenario": "continuous_atmosphere_keepalive_probe_no_source",
            "status": _status_from_abort(bool(ok), abort_reason),
            "route_open_passed": bool(ok),
            "abort_reason": abort_reason,
            "point_runtime_state": point_state,
            "open_valves": open_valves,
            "keepalive_probe_samples": keepalive_samples,
            "atmosphere_summary": dict(runner._last_atmosphere_gate_summary or {}),
            "route_pressure_guard_summary": _route_guard_summary_payload(runner),
            **drain_summary,
            "pressure_trace_rows": _scenario_trace_rows(trace_path, trace_start),
        },
    )


def _run_route_open_pressure_guard(
    runner: CalibrationRunner,
    trace_path: Path,
    args: argparse.Namespace,
) -> Dict[str, Any]:
    point = _build_point(args, index=9002)
    trace_start = _trace_row_count(trace_path)
    if not bool(getattr(args, "allow_source_open", False)):
        return _enrich_live_result_with_pace_diagnostics(runner, {
            "scenario": "route_open_pressure_guard",
            "status": "skipped",
            "skipped_reason": "SourceOpenRequiresExplicitAllowFlag",
            "operator_must_confirm_upstream_source_pressure_limited": True,
            "pressure_trace_rows": _scenario_trace_rows(trace_path, trace_start),
        })
    _log("operator_must_confirm_upstream_source_pressure_limited=true")
    ok = runner._open_co2_route_for_conditioning(point, point_tag="live_route_open_pressure_guard")
    point_state = dict(runner._point_runtime_state(point, phase="co2") or {})
    abort_reason = str(point_state.get("abort_reason") or "").strip()
    return _enrich_live_result_with_pace_diagnostics(runner, {
        "scenario": "route_open_pressure_guard",
        "status": _status_from_abort(bool(ok), abort_reason),
        "route_open_passed": bool(ok),
        "abort_reason": abort_reason,
        "operator_must_confirm_upstream_source_pressure_limited": True,
        "point_runtime_state": point_state,
        "atmosphere_summary": dict(runner._last_atmosphere_gate_summary or {}),
        "route_pressure_guard_summary": _route_guard_summary_payload(runner),
        "pressure_trace_rows": _scenario_trace_rows(trace_path, trace_start),
    })


def _run_pace_optional_query_error_attribution(
    runner: CalibrationRunner,
    trace_path: Path,
) -> Dict[str, Any]:
    trace_start = _trace_row_count(trace_path)
    query_rows = runner._pace_optional_query_error_attribution(
        (
            ":STAT:OPER:PRES:EVEN?",
            ":SOUR:PRES:COMP1?",
            ":SOUR:PRES:COMP2?",
            ":SENS:PRES:SLEW?",
            ":SENS:PRES:BAR?",
            ":SENS:PRES:INL:TIME?",
        ),
        reason="pace optional query attribution",
    )
    return _enrich_live_result_with_pace_diagnostics(runner, {
        "scenario": "pace_optional_query_error_attribution",
        "status": "pass",
        "query_results": query_rows,
        "pressure_trace_rows": _scenario_trace_rows(trace_path, trace_start),
    })


def _valve_set_label(valves: Iterable[int]) -> str:
    ordered = [int(value) for value in valves]
    return "|".join(str(value) for value in ordered)


def _dedupe_valve_sets(raw_sets: Iterable[Iterable[int]]) -> List[List[int]]:
    seen: set[tuple[int, ...]] = set()
    deduped: List[List[int]] = []
    for raw_set in raw_sets:
        normalized = tuple(int(value) for value in raw_set)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        deduped.append(list(normalized))
    return deduped


def _route_valve_isolation_ladders(runner: CalibrationRunner, point: CalibrationPoint) -> List[Dict[str, Any]]:
    latest_group = [4, 7, 8, 11]
    legacy_group = [8, 11, 16, 24]
    config_co2_group = runner._co2_open_valves(point, include_total_valve=True)
    config_h2o_group = runner._h2o_open_valves(point)
    ladders: List[Dict[str, Any]] = [
        {
            "name": "latest_group",
            "steps": [[4], [4, 7], [4, 7, 8], [4, 7, 8, 11]],
        },
        {
            "name": "legacy_group",
            "steps": [[8], [8, 11], [8, 11, 16], [8, 11, 16, 24]],
        },
    ]
    if config_co2_group:
        ladders.append(
            {
                "name": "config_co2_route",
                "steps": [config_co2_group[: idx] for idx in range(1, len(config_co2_group) + 1)],
            }
        )
    if config_h2o_group:
        ladders.append(
            {
                "name": "config_h2o_route",
                "steps": [config_h2o_group[: idx] for idx in range(1, len(config_h2o_group) + 1)],
            }
        )
    return ladders


def _run_route_valve_isolation_step(
    runner: CalibrationRunner,
    *,
    point: CalibrationPoint,
    valve_set: List[int],
    step_name: str,
) -> Dict[str, Any]:
    drain_summary = _drain_pace_errors_for_live_step(runner, reason=f"route_valve_isolation {step_name} pre-step")
    runner._set_co2_route_baseline(reason=f"route valve isolation before {step_name}")
    atmosphere_summary = dict(runner._last_atmosphere_gate_summary or {})
    runner._apply_valve_states(list(valve_set))
    ok, guard_summary = runner._run_route_open_pressure_guard(
        point,
        phase="co2",
        log_context="route valve isolation",
        point_tag="route_valve_isolation",
        stage_label=_valve_set_label(valve_set),
    )
    result_summary = dict(guard_summary or {})
    post_step_error = str(runner._read_pace_system_error_text() or "").strip()
    post_step_error_clear = runner._pace_system_error_is_clear(post_step_error)
    valve_ids = [int(value) for value in valve_set]
    if ok and not post_step_error_clear:
        result_label = "diagnostic_error"
    elif ok:
        result_label = "safe"
    else:
        result_label = "pressure_rise" if result_summary.get("abort_reason") else "unknown"
    result = {
        "step_name": step_name,
        "valve_set": valve_ids,
        "valve_roles": runner.valve_role_map_for_ids(valve_ids),
        "pressure_start_hpa": result_summary.get("pressure_start_hpa", atmosphere_summary.get("pressure_hpa")),
        "pressure_end_hpa": result_summary.get("pressure_end_hpa", result_summary.get("pressure_hpa")),
        "pressure_peak_hpa": result_summary.get("pressure_peak_hpa", result_summary.get("pressure_hpa")),
        "pressure_delta_from_ambient_hpa": result_summary.get("pressure_delta_from_ambient_hpa"),
        "pressure_slope_hpa_s": result_summary.get("pressure_slope_hpa_s"),
        "ambient_hpa": result_summary.get("ambient_hpa", atmosphere_summary.get("ambient_hpa")),
        "analyzer_p_kpa": result_summary.get("analyzer_pressure_kpa"),
        "dewpoint_line_pressure_hpa": result_summary.get("dewpoint_line_pressure_hpa"),
        "abort_reason": result_summary.get("abort_reason", ""),
        "syst_err": post_step_error if post_step_error else result_summary.get("pace_syst_err_query", ""),
        "offending_route": result_summary.get("offending_route", ""),
        "offending_valve_or_group": result_summary.get("offending_valve_or_group", ""),
        "pre_existing_error_drained": drain_summary.get("pre_existing_error_drained", False),
        "drained_errors": drain_summary.get("drained_errors", []),
        "result": result_label,
    }
    runner._set_co2_route_baseline(reason=f"route valve isolation after {step_name}")
    return result


def _run_route_valve_isolation(
    runner: CalibrationRunner,
    trace_path: Path,
    args: argparse.Namespace,
) -> Dict[str, Any]:
    point = _build_point(args, index=9003)
    trace_start = _trace_row_count(trace_path)
    ladders = _route_valve_isolation_ladders(runner, point)
    single_valves = _dedupe_valve_sets(
        [[valve] for ladder in ladders for step in ladder["steps"] for valve in step]
    )
    step_results: List[Dict[str, Any]] = []
    for valve_set in single_valves:
        step_results.append(
            _run_route_valve_isolation_step(
                runner,
                point=point,
                valve_set=list(valve_set),
                step_name=f"single_{_valve_set_label(valve_set)}",
            )
        )

    blocked_single_valves = {
        int(result["valve_set"][0])
        for result in step_results
        if result.get("result") != "safe" and len(result.get("valve_set", [])) == 1
    }
    ladder_results: List[Dict[str, Any]] = []
    offending_groups: List[str] = []
    for ladder in ladders:
        ladder_step_results: List[Dict[str, Any]] = []
        for step in _dedupe_valve_sets(ladder["steps"]):
            if any(int(valve) in blocked_single_valves for valve in step) and len(step) > 1:
                ladder_step_results.append(
                    {
                        "step_name": f"{ladder['name']}_{_valve_set_label(step)}",
                        "valve_set": [int(value) for value in step],
                        "valve_roles": runner.valve_role_map_for_ids([int(value) for value in step]),
                        "pressure_start_hpa": None,
                        "pressure_end_hpa": None,
                        "pressure_peak_hpa": None,
                        "pressure_delta_from_ambient_hpa": None,
                        "pressure_slope_hpa_s": None,
                        "ambient_hpa": None,
                        "analyzer_p_kpa": None,
                        "dewpoint_line_pressure_hpa": None,
                        "abort_reason": "SkippedAfterSingleValveFailure",
                        "syst_err": "",
                        "offending_route": "",
                        "offending_valve_or_group": _valve_set_label(step),
                        "result": "skipped",
                    }
                )
                break
            result = _run_route_valve_isolation_step(
                runner,
                point=point,
                valve_set=list(step),
                step_name=f"{ladder['name']}_{_valve_set_label(step)}",
            )
            ladder_step_results.append(result)
            if result["result"] != "safe":
                offending_groups.append(str(result.get("offending_valve_or_group") or _valve_set_label(step)))
                break
        ladder_results.append({"name": ladder["name"], "steps": ladder_step_results})

    all_results = step_results + [item for ladder in ladder_results for item in ladder["steps"]]
    dangerous_groups = [
        item for item in all_results if item.get("result") in {"pressure_rise", "unknown", "diagnostic_error"}
    ]
    status = "pass" if not dangerous_groups else "aborted"
    return {
        "scenario": "route_valve_isolation",
        "status": status,
        "abort_reason": dangerous_groups[0].get("abort_reason", "") if dangerous_groups else "",
        "valve_role_map": runner.valve_role_map_for_ids([4, 7, 8, 11, 16, 24]),
        "single_steps": step_results,
        "ladder_results": ladder_results,
        "dangerous_groups": dangerous_groups,
        "offending_groups": offending_groups,
        "pressure_trace_rows": _scenario_trace_rows(trace_path, trace_start),
    }


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(dict(payload), ensure_ascii=False, indent=2, default=str) + "\n",
        encoding="utf-8",
    )


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Minimal V1 live validation for AtmosphereGate and RouteFlush gates.")
    parser.add_argument("--config", default="configs/default_config.json")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--run-id", default=None)
    parser.add_argument(
        "--scenario",
        choices=(
            "atmosphere_gate_only",
            "baseline_atmosphere_hold_60s",
            "pace_optional_query_error_attribution",
            "route_open_pressure_guard",
            CO2_A_STAGED_SOURCE_FINAL_RELEASE_DRY_RUN,
            "route_synchronized_atmosphere_flush_co2_a_no_source",
            "route_synchronized_atmosphere_flush_co2_b_no_source",
            "route_synchronized_atmosphere_flush_co2_a_source_guarded",
            "route_synchronized_atmosphere_flush_co2_b_source_guarded",
            "route_synchronized_atmosphere_flush_co2_a",
            "route_synchronized_atmosphere_flush_co2_b",
            "route_synchronized_atmosphere_flush_h2o_no_final",
            "route_synchronized_atmosphere_flush_h2o",
            "continuous_atmosphere_keepalive_probe_no_source",
            "route_synchronized_atmosphere_flowthrough_co2_a_source_guarded",
            "route_synchronized_atmosphere_flowthrough_co2_b_source_guarded",
            "route_synchronized_atmosphere_flowthrough_h2o_final_guarded",
            "route_flush_dewpoint_gate",
            "route_valve_isolation",
        ),
        required=True,
        help="Minimal live validation scenario to execute.",
    )
    parser.add_argument(
        "--real-device",
        action="store_true",
        help="Required to enable real-device COM access for this minimal V1 validation tool.",
    )
    parser.add_argument("--co2-ppm", type=float, default=600.0)
    parser.add_argument("--target-pressure-hpa", type=float, default=1000.0)
    parser.add_argument("--relay-port", default=None)
    parser.add_argument("--relay-8-port", default=None)
    parser.add_argument("--route-flush-soak-s", type=float, default=0.0)
    parser.add_argument("--baseline-hold-monitor-s", type=float, default=60.0)
    parser.add_argument("--baseline-hold-poll-s", type=float, default=1.0)
    parser.add_argument("--atmosphere-monitor-s", type=float, default=4.0)
    parser.add_argument("--atmosphere-poll-s", type=float, default=0.5)
    parser.add_argument("--atmosphere-min-samples", type=int, default=6)
    parser.add_argument("--atmosphere-tolerance-hpa", type=float, default=15.0)
    parser.add_argument("--atmosphere-rising-slope-max-hpa-s", type=float, default=0.05)
    parser.add_argument("--atmosphere-rising-min-delta-hpa", type=float, default=0.5)
    parser.add_argument("--flush-guard-tolerance-hpa", type=float, default=15.0)
    parser.add_argument("--flush-guard-rising-slope-max-hpa-s", type=float, default=0.05)
    parser.add_argument("--flush-guard-rising-min-delta-hpa", type=float, default=0.5)
    parser.add_argument("--route-open-guard-monitor-s", type=float, default=8.0)
    parser.add_argument("--route-open-guard-poll-s", type=float, default=1.0)
    parser.add_argument("--route-open-guard-tolerance-hpa", type=float, default=30.0)
    parser.add_argument("--route-open-guard-rising-slope-max-hpa-s", type=float, default=0.2)
    parser.add_argument("--route-open-guard-rising-min-delta-hpa", type=float, default=2.0)
    parser.add_argument("--route-open-guard-analyzer-warning-kpa", type=float, default=120.0)
    parser.add_argument("--route-open-guard-analyzer-abort-kpa", type=float, default=150.0)
    parser.add_argument("--route-open-guard-dewpoint-line-tolerance-hpa", type=float, default=30.0)
    parser.add_argument(
        "--allow-source-open",
        action="store_true",
        help="Required to execute guarded CO2 source-open scenarios.",
    )
    parser.add_argument(
        "--allow-h2o-final-stage-open",
        action="store_true",
        help="Required to execute H2O full-route final stage valve 10 live scenarios.",
    )
    parser.add_argument("--dewpoint-gate-window-s", type=float, default=30.0)
    parser.add_argument("--dewpoint-gate-max-wait-s", type=float, default=120.0)
    parser.add_argument("--dewpoint-gate-poll-s", type=float, default=1.0)
    parser.add_argument("--dewpoint-gate-log-interval-s", type=float, default=5.0)
    parser.add_argument("--continuous-keepalive-probe-s", type=float, default=6.0)
    parser.add_argument("--continuous-keepalive-probe-poll-s", type=float, default=1.0)
    parser.add_argument(
        "--pressure-protection-approval-json",
        default=None,
        help="Machine-readable CO2 A staged dry-run pressure protection approval JSON.",
    )
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    if not args.real_device:
        _log("Dry-run only: pass --real-device to enable the minimal V1 live validation.")
        return 2

    cfg = load_config(args.config)
    need_dewpoint = args.scenario == "route_flush_dewpoint_gate"
    need_analyzer_pressure = _analyzer_pressure_required(args, cfg)
    runtime_cfg = _prepare_runtime_cfg(
        cfg,
        args,
        need_dewpoint=need_dewpoint,
        need_analyzer_pressure=need_analyzer_pressure,
    )
    setattr(args, "_runtime_cfg", runtime_cfg)
    analyzer_pressure_summary = _build_analyzer_pressure_summary(cfg, runtime_cfg, args)
    output_root = Path(runtime_cfg["paths"]["output_dir"]).resolve()
    run_id = args.run_id or f"{args.scenario}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    logger = RunLogger(output_root, run_id=run_id, cfg=runtime_cfg)
    devices: Dict[str, Any] = {}
    runner: Optional[CalibrationRunner] = None
    summary: Dict[str, Any] = {
        "tool": "run_v1_pressure_gate_live",
        "run_id": run_id,
        "scenario": args.scenario,
        "real_device": True,
        "validation_scope": "minimal_live_engineering_validation_only",
        "not_real_acceptance_evidence": True,
        "config_path": str(Path(args.config).resolve()),
        "output_dir": str(logger.run_dir),
        "io_csv": str(logger.io_path),
        "pressure_transition_trace_csv": str(logger.run_dir / "pressure_transition_trace.csv"),
        "scenario_result": {},
        "cleanup_safe_stop": {},
        "status": "running",
        **analyzer_pressure_summary,
    }
    exit_code = 0
    trace_path = logger.run_dir / "pressure_transition_trace.csv"

    try:
        _log(f"Pressure gate live run dir: {logger.run_dir}")
        devices = _build_devices(runtime_cfg, io_logger=logger)
        runner = CalibrationRunner(runtime_cfg, devices, logger, _log, _log)
        try:
            runner._configure_devices(
                configure_gas_analyzers=bool(analyzer_pressure_summary.get("analyzer_pressure_required", False))
            )
        except RuntimeError as exc:
            if (
                bool(analyzer_pressure_summary.get("analyzer_pressure_required", False))
                and "No gas analyzers available after startup configuration" in str(exc)
            ):
                analyzer_pressure_summary = _mark_analyzer_pressure_unavailable(
                    analyzer_pressure_summary,
                    disabled_reason="AnalyzerStartupUnavailable",
                )
            else:
                raise
        if bool(analyzer_pressure_summary.get("analyzer_pressure_required", False)) and not str(
            analyzer_pressure_summary.get("analyzer_pressure_abort_reason") or ""
        ).strip():
            analyzer_pressure_summary = _verify_required_analyzer_pressure_protection(
                runner,
                analyzer_pressure_summary,
            )
        summary.update(analyzer_pressure_summary)
        summary["k0472_capability_snapshot"] = runner._capture_pace_capability_snapshot(
            reason=f"live scenario {args.scenario}",
            include_optional_probe=True,
        )
        if str(analyzer_pressure_summary.get("analyzer_pressure_abort_reason") or "").strip():
            scenario_result = _build_analyzer_pressure_preflight_failure_result(
                scenario=args.scenario,
                trace_path=trace_path,
                analyzer_summary=analyzer_pressure_summary,
            )
        elif args.scenario == "atmosphere_gate_only":
            scenario_result = _run_atmosphere_gate_only(runner, trace_path)
        elif args.scenario == "baseline_atmosphere_hold_60s":
            scenario_result = _run_baseline_atmosphere_hold_60s(runner, trace_path, devices, args)
        elif args.scenario == "pace_optional_query_error_attribution":
            scenario_result = _run_pace_optional_query_error_attribution(runner, trace_path)
        elif args.scenario == "route_open_pressure_guard":
            scenario_result = _run_route_open_pressure_guard(runner, trace_path, args)
        elif args.scenario == CO2_A_STAGED_SOURCE_FINAL_RELEASE_DRY_RUN:
            scenario_result = _run_co2_a_staged_source_final_release_dry_run(runner, trace_path, args)
        elif args.scenario == "route_synchronized_atmosphere_flush_co2_a_no_source":
            scenario_result = _run_route_synchronized_atmosphere_flush_co2_a_no_source(runner, trace_path, args)
        elif args.scenario == "route_synchronized_atmosphere_flush_co2_b_no_source":
            scenario_result = _run_route_synchronized_atmosphere_flush_co2_b_no_source(runner, trace_path, args)
        elif args.scenario == "route_synchronized_atmosphere_flush_co2_a_source_guarded":
            scenario_result = _run_route_synchronized_atmosphere_flush_co2_a_source_guarded(runner, trace_path, args)
        elif args.scenario == "route_synchronized_atmosphere_flush_co2_b_source_guarded":
            scenario_result = _run_route_synchronized_atmosphere_flush_co2_b_source_guarded(runner, trace_path, args)
        elif args.scenario == "route_synchronized_atmosphere_flowthrough_co2_a_source_guarded":
            scenario_result = _run_route_synchronized_atmosphere_flush_co2_a_source_guarded(runner, trace_path, args)
        elif args.scenario == "route_synchronized_atmosphere_flowthrough_co2_b_source_guarded":
            scenario_result = _run_route_synchronized_atmosphere_flush_co2_b_source_guarded(runner, trace_path, args)
        elif args.scenario == "route_synchronized_atmosphere_flush_co2_a":
            scenario_result = _run_route_synchronized_atmosphere_flush_co2_a_source_guarded(runner, trace_path, args)
        elif args.scenario == "route_synchronized_atmosphere_flush_co2_b":
            scenario_result = _run_route_synchronized_atmosphere_flush_co2_b_source_guarded(runner, trace_path, args)
        elif args.scenario == "route_synchronized_atmosphere_flush_h2o_no_final":
            scenario_result = _run_route_synchronized_atmosphere_flush_h2o_no_final(runner, trace_path, args)
        elif args.scenario == "route_synchronized_atmosphere_flush_h2o":
            scenario_result = _run_route_synchronized_atmosphere_flush_h2o(runner, trace_path, args)
        elif args.scenario == "route_synchronized_atmosphere_flowthrough_h2o_final_guarded":
            scenario_result = _run_route_synchronized_atmosphere_flush_h2o(runner, trace_path, args)
        elif args.scenario == "continuous_atmosphere_keepalive_probe_no_source":
            scenario_result = _run_continuous_atmosphere_keepalive_probe_no_source(runner, trace_path, args)
        elif args.scenario == "route_valve_isolation":
            scenario_result = _run_route_valve_isolation(runner, trace_path, args)
        else:
            scenario_result = _run_route_flush_dewpoint_gate(runner, trace_path, args)
        scenario_result.update(
            {
                key: analyzer_pressure_summary.get(key)
                for key in (
                    "analyzer_pressure_required",
                    "analyzer_pressure_available",
                    "analyzer_pressure_protection_active",
                    "analyzer_count_before_filter",
                    "analyzer_count_after_filter",
                    "analyzer_list_preserved_for_required_pressure",
                    "analyzer_disabled_reason",
                    "analyzer_pressure_abort_reason",
                    "mechanical_pressure_protection_confirmed",
                )
                if analyzer_pressure_summary.get(key) not in (None, "")
            }
        )
        summary["scenario_result"] = scenario_result
        summary["status"] = "completed"
        _log(
            "Scenario result: "
            f"scenario={scenario_result.get('scenario')} "
            f"status={scenario_result.get('status')} "
            f"abort_reason={scenario_result.get('abort_reason', '')}"
        )
    except KeyboardInterrupt:
        summary["status"] = "cancelled"
        exit_code = 130
        _log("Pressure gate live validation cancelled by user.")
    except Exception as exc:
        summary["status"] = "error"
        summary["error"] = str(exc)
        exit_code = 1
        _log(f"Pressure gate live validation failed: {exc}")
    finally:
        if devices:
            try:
                cleanup_result = perform_safe_stop_with_retries(devices, log_fn=_log, cfg=runtime_cfg)
                summary["cleanup_safe_stop"] = dict(cleanup_result or {})
            except Exception as exc:
                summary["cleanup_safe_stop"] = {"error": str(exc)}
                _log(f"Cleanup safe-stop failed: {exc}")
        cleanup_syst_errs: List[str] = []
        if runner is not None:
            cleanup_syst_errs = list(
                runner._drain_pace_system_errors(
                    reason=f"live scenario {args.scenario} post-cleanup final drain",
                    classification="cleanup_syst_err",
                    action="post_cleanup_drain",
                )
                or []
            )
            final_syst_err = str(runner._read_pace_system_error_text() or "").strip()
            summary["cleanup_syst_errs"] = cleanup_syst_errs
            summary["final_syst_err"] = final_syst_err
            if isinstance(summary.get("scenario_result"), dict):
                summary["scenario_result"] = _enrich_live_result_with_pace_diagnostics(
                    runner,
                    dict(summary.get("scenario_result") or {}),
                    final_syst_err=final_syst_err,
                )
                summary["scenario_result"]["cleanup_syst_errs"] = cleanup_syst_errs
                if cleanup_syst_errs and str(summary["scenario_result"].get("status") or "") == "pass":
                    summary["scenario_result"]["status"] = "pass_with_diagnostic_error"
        summary.update(
            _merge_summary_analyzer_pressure_fields(
                summary,
                fallback=analyzer_pressure_summary,
            )
        )
        summary["summary_extract"] = _build_summary_extract(summary)
        summary_path = logger.run_dir / "pressure_gate_live_summary.json"
        dry_run_summary_path = None
        if args.scenario == CO2_A_STAGED_SOURCE_FINAL_RELEASE_DRY_RUN:
            dry_run_summary_path = logger.run_dir / "dry_run_summary.json"
            summary["dry_run_summary_path"] = str(dry_run_summary_path)
        try:
            _write_json(summary_path, summary)
            if dry_run_summary_path is not None:
                _write_json(dry_run_summary_path, _build_co2_a_staged_source_final_artifact(summary))
        finally:
            _close_devices(devices)
            try:
                logger.close()
            except Exception:
                pass
        _log(f"Summary JSON: {summary_path}")
        if dry_run_summary_path is not None:
            _log(f"Dry-run summary JSON: {dry_run_summary_path}")
        _log(f"IO CSV: {logger.io_path}")
        if trace_path.exists():
            _log(f"Pressure trace CSV: {trace_path}")
    return exit_code


if __name__ == "__main__":
    sys.exit(main())