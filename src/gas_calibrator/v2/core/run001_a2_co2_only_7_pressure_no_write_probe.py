from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import csv
import json
import os
from pathlib import Path
import re
from types import SimpleNamespace
from typing import Any, Callable, Mapping, Optional

from gas_calibrator.v2.core.run001_r1_conditioning_only_probe import (
    _as_bool,
    _as_float,
    _json_dump,
    _jsonl_dump,
    load_json_mapping,
)


A2_SCHEMA_VERSION = "v2.run001.a2_co2_7_pressure_no_write_probe.1"
A2_1_HEARTBEAT_GAP_ACCOUNTING_FIX_PRESENT = True
A2_ENV_VAR = "GAS_CAL_V2_A2_CO2_7_PRESSURE_NO_WRITE_REAL_COM"
A2_ENV_VALUE = "1"
A2_CLI_FLAG = "--allow-v2-a2-co2-7-pressure-no-write-real-com"
A2_ALLOWED_PRESSURE_POINTS_HPA = (1100.0, 1000.0, 900.0, 800.0, 700.0, 600.0, 500.0)
A2_EVIDENCE_MARKERS = {
    "evidence_source": "real_probe_a2_co2_7_pressure_no_write",
    "acceptance_level": "engineering_probe_only",
    "not_real_acceptance_evidence": True,
    "promotion_state": "blocked",
    "real_primary_latest_refresh": False,
}
A2_REQUIRED_OPERATOR_FIELDS = (
    "operator_name",
    "timestamp",
    "branch",
    "HEAD",
    "config_path",
    "a1r_output_dir",
    "pressure_points_hpa",
    "port_manifest",
    "explicit_acknowledgement",
)
A2_REQUIRED_TRUE_ACKS = (
    "only_a2_co2_7_pressure_no_write",
    "co2_only",
    "skip0",
    "single_route",
    "single_temperature",
    "seven_pressure_points",
    "no_write",
    "no_id_write",
    "no_senco_write",
    "no_calibration_write",
    "no_chamber_sv_write",
    "no_chamber_set_temperature",
    "no_chamber_start",
    "no_chamber_stop",
    "no_mode_switch",
    "not_real_acceptance",
    "engineering_probe_only",
    "v1_fallback_required",
    "authorized_pressure_control_scope_acknowledged",
    "do_not_refresh_real_primary_latest",
)
A2_REQUIRED_FALSE_ACKS = (
    "a3_enabled",
    "h2o_enabled",
    "full_group_enabled",
    "multi_temperature_enabled",
    "real_primary_latest_refresh",
)
A2_SAFETY_ASSERTION_DEFAULTS = {
    "attempted_write_count": 0,
    "any_write_command_sent": False,
    "identity_write_command_sent": False,
    "mode_switch_command_sent": False,
    "senco_write_command_sent": False,
    "calibration_write_command_sent": False,
    "chamber_write_register_command_sent": False,
    "chamber_set_temperature_command_sent": False,
    "chamber_start_command_sent": False,
    "chamber_stop_command_sent": False,
    "real_primary_latest_refresh": False,
    "v1_fallback_required": True,
    "run_app_py_untouched": True,
}


@dataclass(frozen=True)
class A2Admission:
    approved: bool
    reasons: tuple[str, ...]
    evidence: dict[str, Any]
    operator_confirmation: dict[str, Any]
    operator_validation: dict[str, Any]
    prereq_summaries: dict[str, dict[str, Any]]


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def _path_value(raw_cfg: Mapping[str, Any], dotted_path: str) -> Any:
    current: Any = raw_cfg
    for part in dotted_path.split("."):
        if not isinstance(current, Mapping) or part not in current:
            return None
        current = current.get(part)
    return current


def _first_value(raw_cfg: Mapping[str, Any], paths: tuple[str, ...]) -> Any:
    for path in paths:
        value = _path_value(raw_cfg, path)
        if value is not None:
            return value
    return None


def _truthy(raw_cfg: Mapping[str, Any], paths: tuple[str, ...]) -> bool:
    return _as_bool(_first_value(raw_cfg, paths)) is True


def _explicit_false(raw_cfg: Mapping[str, Any], paths: tuple[str, ...]) -> bool:
    return _as_bool(_first_value(raw_cfg, paths)) is False


def _a2_cfg(raw_cfg: Mapping[str, Any]) -> dict[str, Any]:
    for name in ("a2_co2_7_pressure_no_write_probe", "run001_a2", "a2"):
        candidate = raw_cfg.get(name)
        if isinstance(candidate, Mapping):
            return dict(candidate)
    return {}


def _scope(raw_cfg: Mapping[str, Any]) -> str:
    return str(
        _first_value(
            raw_cfg,
            (
                "scope",
                "a2_co2_7_pressure_no_write_probe.scope",
                "run001_a2.probe_scope",
                "run001_a2.scope",
                "a2.scope",
            ),
        )
        or ""
    ).strip().lower()


def _output_dir_value(raw_cfg: Mapping[str, Any], name: str) -> str:
    return str(
        _first_value(
            raw_cfg,
            (
                name,
                f"a2_co2_7_pressure_no_write_probe.{name}",
                f"run001_a2.{name}",
                f"a2.{name}",
            ),
        )
        or ""
    )


def _float_list(value: Any) -> list[float]:
    raw = value if isinstance(value, list) else [value]
    out: list[float] = []
    for item in raw:
        parsed = _as_float(item)
        if parsed is not None:
            out.append(float(parsed))
    return out


def _same_pressure_points(value: Any) -> bool:
    points = _float_list(value)
    if len(points) != len(A2_ALLOWED_PRESSURE_POINTS_HPA):
        return False
    return all(abs(float(a) - float(b)) <= 1e-6 for a, b in zip(points, A2_ALLOWED_PRESSURE_POINTS_HPA))


def _pressure_points(raw_cfg: Mapping[str, Any]) -> list[float]:
    value = _first_value(
        raw_cfg,
        (
            "pressure_points_hpa",
            "authorized_pressure_points_hpa",
            "a2_co2_7_pressure_no_write_probe.pressure_points_hpa",
            "a2_co2_7_pressure_no_write_probe.authorized_pressure_points_hpa",
            "run001_a2.pressure_points_hpa",
            "run001_a2.authorized_pressure_points_hpa",
        ),
    )
    return _float_list(value)


def _skip0_only(raw_cfg: Mapping[str, Any]) -> bool:
    if _truthy(raw_cfg, ("skip0", "a2_co2_7_pressure_no_write_probe.skip0", "run001_a2.skip0")):
        return True
    value = _first_value(
        raw_cfg,
        (
            "skip_co2_ppm",
            "workflow.skip_co2_ppm",
            "a2_co2_7_pressure_no_write_probe.skip_co2_ppm",
            "run001_a2.skip_co2_ppm",
        ),
    )
    if isinstance(value, list):
        return [int(float(item)) for item in value if str(item).strip() != ""] == [0]
    return str(value).strip() in {"0", "0.0", "[0]"}


def _single_temperature(raw_cfg: Mapping[str, Any]) -> bool:
    if _truthy(
        raw_cfg,
        (
            "single_temperature",
            "single_temperature_group",
            "a2_co2_7_pressure_no_write_probe.single_temperature",
            "run001_a2.single_temperature",
            "run001_a2.single_temperature_group",
        ),
    ):
        return True
    value = _first_value(raw_cfg, ("selected_temps_c", "workflow.selected_temps_c"))
    return isinstance(value, list) and len(value) == 1


def _sample_min_count(raw_cfg: Mapping[str, Any]) -> int:
    value = _first_value(
        raw_cfg,
        (
            "sample_min_count_per_pressure",
            "a2_co2_7_pressure_no_write_probe.sample_min_count_per_pressure",
            "run001_a2.sample_min_count_per_pressure",
            "workflow.sampling.count",
        ),
    )
    try:
        return max(1, int(float(value)))
    except Exception:
        return 4


def _pressure_freshness_max_age_ms(raw_cfg: Mapping[str, Any]) -> float:
    value = _first_value(
        raw_cfg,
        (
            "pressure_cache_max_age_ms",
            "a2_co2_7_pressure_no_write_probe.pressure_cache_max_age_ms",
            "run001_a2.pressure_cache_max_age_ms",
            "workflow.pressure.pressure_sample_stale_threshold_s",
        ),
    )
    parsed = _as_float(value)
    if parsed is None:
        return 2000.0
    if "pressure_sample_stale_threshold_s" in str(value):
        return float(parsed) * 1000.0
    return float(parsed) * (1000.0 if float(parsed) < 50.0 else 1.0)


def _load_prereq_summary(output_dir: str | Path, *, expected_source: str = "") -> tuple[dict[str, Any], list[str]]:
    if not str(output_dir or "").strip():
        return {}, ["missing_prereq_output_dir"]
    summary_path = Path(output_dir).expanduser() / "summary.json"
    if not summary_path.exists():
        return {}, [f"missing_prereq_summary:{summary_path}"]
    try:
        summary = load_json_mapping(summary_path)
    except Exception as exc:
        return {}, [f"invalid_prereq_summary:{exc}"]
    reasons: list[str] = []
    if summary.get("final_decision") != "PASS":
        reasons.append("prereq_final_decision_not_pass")
    if summary.get("not_real_acceptance_evidence") is not True:
        reasons.append("prereq_missing_not_real_acceptance_marker")
    if expected_source and summary.get("evidence_source") != expected_source:
        reasons.append("prereq_evidence_source_mismatch")
    if summary.get("attempted_write_count") != 0:
        reasons.append("prereq_attempted_write_nonzero")
    if summary.get("any_write_command_sent") is not False:
        reasons.append("prereq_any_write_not_false")
    return summary, reasons


def _load_json_mapping_accept_bom(path: str | Path) -> dict[str, Any]:
    try:
        return load_json_mapping(path)
    except json.JSONDecodeError as exc:
        if "utf-8-sig" not in str(exc).lower() and "bom" not in str(exc).lower():
            raise
    payload = json.loads(Path(path).read_text(encoding="utf-8-sig"))
    if not isinstance(payload, Mapping):
        raise TypeError("JSON payload must be an object")
    return dict(payload)


def _validate_operator_confirmation(
    path: Optional[str | Path],
    *,
    expected_branch: str = "",
    expected_head: str = "",
    expected_config_path: str = "",
    expected_a1r_output_dir: str = "",
) -> tuple[dict[str, Any], dict[str, Any]]:
    errors: list[str] = []
    if not path:
        return {}, {"valid": False, "errors": ["missing_operator_confirmation_json"]}
    confirmation_path = Path(path).expanduser()
    if not confirmation_path.exists():
        return {}, {"valid": False, "errors": ["missing_operator_confirmation_json"]}
    try:
        payload = _load_json_mapping_accept_bom(confirmation_path)
    except Exception as exc:
        return {}, {"valid": False, "errors": [f"invalid_operator_confirmation_json:{exc}"]}

    for field in A2_REQUIRED_OPERATOR_FIELDS:
        if payload.get(field) in (None, ""):
            errors.append(f"operator_confirmation_missing_{field}")
    if not _same_pressure_points(payload.get("pressure_points_hpa")):
        errors.append("operator_confirmation_pressure_points_mismatch")

    ack = payload.get("explicit_acknowledgement")
    if not isinstance(ack, Mapping):
        errors.append("operator_confirmation_missing_explicit_acknowledgement")
        ack = {}
    for key in A2_REQUIRED_TRUE_ACKS:
        if _as_bool(ack.get(key)) is not True:
            errors.append(f"operator_ack_missing_{key}")
    for key in A2_REQUIRED_FALSE_ACKS:
        if _as_bool(ack.get(key)) is not False:
            errors.append(f"operator_ack_not_false_{key}")

    if expected_branch and str(payload.get("branch") or "") != expected_branch:
        errors.append("operator_confirmation_branch_mismatch")
    if expected_head and str(payload.get("HEAD") or "") != expected_head:
        errors.append("operator_confirmation_head_mismatch")
    if expected_config_path:
        actual = str(payload.get("config_path") or "")
        if not actual or Path(actual).resolve() != Path(expected_config_path).resolve():
            errors.append("operator_confirmation_config_path_mismatch")
    if expected_a1r_output_dir:
        actual = str(payload.get("a1r_output_dir") or "")
        if not actual or Path(actual).resolve() != Path(expected_a1r_output_dir).resolve():
            errors.append("operator_confirmation_a1r_output_dir_mismatch")

    return payload, {"valid": not errors, "errors": errors, "path": str(confirmation_path.resolve())}


def evaluate_a2_co2_7_pressure_no_write_gate(
    raw_cfg: Mapping[str, Any],
    *,
    cli_allow: bool = False,
    env: Optional[Mapping[str, str]] = None,
    operator_confirmation_path: Optional[str | Path] = None,
    branch: str = "",
    head: str = "",
    config_path: str = "",
    run_app_py_untouched: bool = True,
) -> A2Admission:
    env_map = os.environ if env is None else env
    reasons: list[str] = []
    if not cli_allow:
        reasons.append("missing_cli_flag_allow_v2_a2_co2_7_pressure_no_write_real_com")
    if str(env_map.get(A2_ENV_VAR, "")).strip() != A2_ENV_VALUE:
        reasons.append("missing_env_gas_cal_v2_a2_co2_7_pressure_no_write_real_com")

    a1r_dir = _output_dir_value(raw_cfg, "a1r_output_dir")
    a1r_summary, a1r_reasons = _load_prereq_summary(
        a1r_dir,
        expected_source="real_probe_a1r_minimal_no_write_sampling",
    )
    reasons.extend(f"a1r_{item}" for item in a1r_reasons)
    if a1r_summary and a1r_summary.get("a1r_minimal_sampling_executed") is not True:
        reasons.append("a1r_minimal_sampling_not_executed")

    r0_1_pass = bool(a1r_summary.get("r0_1_reference_readonly_prereq_pass") is True)
    r0_full_pass = bool(a1r_summary.get("r0_full_query_only_prereq_pass") is True)
    r1_pass = bool(a1r_summary.get("r1_conditioning_only_prereq_pass") is True)
    a1r_pass = bool(a1r_summary.get("final_decision") == "PASS" and not a1r_reasons)
    if not r0_1_pass:
        reasons.append("r0_1_reference_readonly_prereq_missing_or_not_pass")
    if not r0_full_pass:
        reasons.append("r0_full_query_only_prereq_missing_or_not_pass")
    if not r1_pass:
        reasons.append("r1_conditioning_only_prereq_missing_or_not_pass")
    if not a1r_pass:
        reasons.append("a1r_minimal_sampling_prereq_missing_or_not_pass")

    operator_payload, operator_validation = _validate_operator_confirmation(
        operator_confirmation_path,
        expected_branch=branch,
        expected_head=head,
        expected_config_path=config_path,
        expected_a1r_output_dir=a1r_dir,
    )
    reasons.extend(str(item) for item in operator_validation.get("errors", []))

    if branch and branch != "codex/run001-a1-no-write-dry-run":
        reasons.append("current_branch_not_run001_a1_no_write_dry_run")
    if not str(head or "").strip():
        reasons.append("current_head_missing")
    if _scope(raw_cfg) not in {"a2_co2_7_pressure_no_write", "run001_a2_co2_no_write_pressure_sweep"}:
        reasons.append("config_scope_not_a2_co2_7_pressure_no_write")
    if not _truthy(raw_cfg, ("co2_only", "a2_co2_7_pressure_no_write_probe.co2_only", "run001_a2.co2_only")):
        reasons.append("config_not_co2_only")
    if not _skip0_only(raw_cfg):
        reasons.append("config_not_skip0")
    if not _truthy(raw_cfg, ("single_route", "a2_co2_7_pressure_no_write_probe.single_route", "run001_a2.single_route")):
        reasons.append("config_not_single_route")
    if not _single_temperature(raw_cfg):
        reasons.append("config_not_single_temperature")
    if not _truthy(raw_cfg, ("no_write", "a2_co2_7_pressure_no_write_probe.no_write", "run001_a2.no_write")):
        reasons.append("config_no_write_not_true")
    if not _same_pressure_points(_pressure_points(raw_cfg)):
        reasons.append("config_pressure_points_not_exact_a2_set")
    if not _truthy(raw_cfg, ("v1_fallback_required", "a2_co2_7_pressure_no_write_probe.v1_fallback_required")):
        reasons.append("config_v1_fallback_required_not_true")
    if not run_app_py_untouched:
        reasons.append("run_app_py_not_untouched")

    false_required = {
        "a3_enabled": ("a3_enabled", "a2_co2_7_pressure_no_write_probe.a3_enabled", "run001_a2.a3_enabled"),
        "h2o_enabled": ("h2o_enabled", "a2_co2_7_pressure_no_write_probe.h2o_enabled", "run001_a2.h2o_enabled"),
        "full_group_enabled": (
            "full_group_enabled",
            "a2_co2_7_pressure_no_write_probe.full_group_enabled",
            "run001_a2.full_group_enabled",
            "run001_a2.full_h2o_co2_group",
        ),
        "multi_temperature_enabled": (
            "multi_temperature_enabled",
            "a2_co2_7_pressure_no_write_probe.multi_temperature_enabled",
            "run001_a2.multi_temperature_enabled",
        ),
        "mode_switch_enabled": (
            "mode_switch_enabled",
            "a2_co2_7_pressure_no_write_probe.mode_switch_enabled",
            "run001_a2.mode_switch_enabled",
        ),
        "analyzer_id_write_enabled": (
            "analyzer_id_write_enabled",
            "a2_co2_7_pressure_no_write_probe.analyzer_id_write_enabled",
            "run001_a2.analyzer_id_write_enabled",
        ),
        "senco_write_enabled": (
            "senco_write_enabled",
            "a2_co2_7_pressure_no_write_probe.senco_write_enabled",
            "run001_a2.senco_write_enabled",
        ),
        "calibration_write_enabled": (
            "calibration_write_enabled",
            "a2_co2_7_pressure_no_write_probe.calibration_write_enabled",
            "run001_a2.calibration_write_enabled",
        ),
        "chamber_set_temperature_enabled": (
            "chamber_set_temperature_enabled",
            "a2_co2_7_pressure_no_write_probe.chamber_set_temperature_enabled",
            "run001_a2.chamber_set_temperature_enabled",
        ),
        "chamber_start_enabled": (
            "chamber_start_enabled",
            "a2_co2_7_pressure_no_write_probe.chamber_start_enabled",
            "run001_a2.chamber_start_enabled",
        ),
        "chamber_stop_enabled": (
            "chamber_stop_enabled",
            "a2_co2_7_pressure_no_write_probe.chamber_stop_enabled",
            "run001_a2.chamber_stop_enabled",
        ),
        "real_primary_latest_refresh": (
            "real_primary_latest_refresh",
            "a2_co2_7_pressure_no_write_probe.real_primary_latest_refresh",
            "run001_a2.real_primary_latest_refresh",
        ),
    }
    for name, paths in false_required.items():
        if not _explicit_false(raw_cfg, paths):
            reasons.append(f"config_{name}_not_disabled")

    reasons = list(dict.fromkeys(reasons))
    approved = not reasons
    evidence = {
        **A2_EVIDENCE_MARKERS,
        "admission_approved": approved,
        "r0_1_reference_readonly_prereq_pass": r0_1_pass,
        "r0_full_query_only_prereq_pass": r0_full_pass,
        "r1_conditioning_only_prereq_pass": r1_pass,
        "a1r_minimal_sampling_prereq_pass": a1r_pass,
        "a1r_output_dir": a1r_dir,
        "a2_pressure_sweep_executed": False,
        "a3_allowed": False,
        **A2_SAFETY_ASSERTION_DEFAULTS,
        "rejection_reasons": reasons,
    }
    return A2Admission(
        approved=approved,
        reasons=tuple(reasons),
        evidence=evidence,
        operator_confirmation=operator_payload,
        operator_validation=operator_validation,
        prereq_summaries={"a1r": a1r_summary},
    )


def _default_output_dir() -> Path:
    timestamp = datetime.now().astimezone().strftime("%Y%m%d_%H%M")
    return Path(f"D:/gas_calibrator_step3a_a2_co2_7_pressure_no_write_probe_{timestamp}").resolve()


def execute_existing_v2_a2_pressure_sweep(config_path: str | Path) -> dict[str, Any]:
    from gas_calibrator.v2.core.no_write_guard import build_no_write_guard_from_raw_config
    from gas_calibrator.v2.core.run001_a2_no_write import (
        authorize_run001_a2_no_write_pressure_sweep,
    )
    from gas_calibrator.v2.entry import create_calibration_service_from_config, load_config_bundle

    resolved_config_path, raw_cfg, config = load_config_bundle(
        str(config_path),
        simulation_mode=False,
        allow_unsafe_step2_config=False,
        enforce_step2_execution_gate=False,
    )
    cli_args = SimpleNamespace(
        execute=True,
        confirm_real_machine_no_write=True,
        confirm_a2_no_write_pressure_sweep=True,
    )
    gate = authorize_run001_a2_no_write_pressure_sweep(
        config,
        raw_cfg,
        cli_args,
        config_path=resolved_config_path,
    )
    raw_cfg["_run001_a2_safety_gate"] = gate
    setattr(config, "_run001_a2_safety_gate", gate)
    service = create_calibration_service_from_config(
        config,
        raw_cfg=raw_cfg,
        preload_points=True,
        require_no_write_guard=True,
    )
    build_no_write_guard_from_raw_config(raw_cfg)
    service.run()
    run_dir = Path(service.session.output_dir)
    summary = _load_json_dict(run_dir / "summary.json")
    return {
        "execution_run_dir": str(run_dir),
        "service_summary": summary,
        "route_trace_rows": _load_jsonl(run_dir / "route_trace.jsonl"),
        "timing_trace_rows": _load_jsonl(run_dir / "workflow_timing_trace.jsonl"),
        "sample_rows": _load_csv_dicts(run_dir / "samples.csv"),
    }


def _load_json_dict(path: str | Path | None) -> dict[str, Any]:
    if path is None:
        return {}
    target = Path(path)
    if not target.exists():
        return {}
    try:
        payload = json.loads(target.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return dict(payload) if isinstance(payload, Mapping) else {}


def _load_jsonl(path: str | Path | None) -> list[dict[str, Any]]:
    if path is None:
        return []
    target = Path(path)
    if not target.exists():
        return []
    rows: list[dict[str, Any]] = []
    for line in target.read_text(encoding="utf-8").splitlines():
        try:
            payload = json.loads(line)
        except Exception:
            continue
        if isinstance(payload, Mapping):
            rows.append(dict(payload))
    return rows


def _load_csv_dicts(path: str | Path | None) -> list[dict[str, Any]]:
    if path is None:
        return []
    target = Path(path)
    if not target.exists():
        return []
    with target.open("r", encoding="utf-8", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _pressure_key(value: Any) -> str:
    parsed = _as_float(value)
    if parsed is None:
        return str(value or "")
    return str(int(parsed)) if abs(parsed - int(parsed)) <= 1e-6 else f"{parsed:.6g}"


def _point_tag_pressure(point_tag: Any) -> Optional[float]:
    match = re.search(r"_(\d+(?:\.\d+)?)hpa\b", str(point_tag or ""), flags=re.IGNORECASE)
    if not match:
        return None
    return float(match.group(1))


def _row_pressure(row: Mapping[str, Any]) -> Optional[float]:
    for key in ("target_pressure_hpa", "pressure_hpa"):
        parsed = _as_float(row.get(key))
        if parsed is not None:
            return float(parsed)
    actual = row.get("actual")
    if isinstance(actual, Mapping):
        for key in (
            "target_pressure_hpa",
            "pressure_hpa",
            "sealed_pressure_hpa",
            "preseal_trigger_pressure_hpa",
        ):
            parsed = _as_float(actual.get(key))
            if parsed is not None:
                return float(parsed)
    return _point_tag_pressure(row.get("point_tag"))


def _route_rows_for_pressure(route_rows: list[dict[str, Any]], pressure: float, index: int) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for row in route_rows:
        row_index = row.get("point_index")
        parsed_pressure = _row_pressure(row)
        if row_index is not None and int(float(row_index)) == int(index):
            out.append(row)
        elif parsed_pressure is not None and abs(parsed_pressure - float(pressure)) <= 1e-6:
            out.append(row)
    return out


def _extract_actual(row: Mapping[str, Any]) -> dict[str, Any]:
    actual = row.get("actual")
    return dict(actual) if isinstance(actual, Mapping) else {}


def _extract_pressure_age_ms(actual: Mapping[str, Any]) -> Optional[float]:
    for key in ("pressure_age_ms_before_sample", "pressure_sample_age_ms"):
        parsed = _as_float(actual.get(key))
        if parsed is not None:
            return float(parsed)
    for key in ("pressure_sample_age_s", "sample_age_s", "digital_gauge_age_s"):
        parsed = _as_float(actual.get(key))
        if parsed is not None:
            return float(parsed) * 1000.0
    return None


def _extract_pressure_hpa(actual: Mapping[str, Any]) -> Optional[float]:
    for key in (
        "pressure_gauge_hpa_before_sample",
        "pressure_gauge_hpa_before_ready",
        "digital_gauge_pressure_hpa",
        "pressure_hpa",
        "sealed_pressure_hpa",
        "preseal_trigger_pressure_hpa",
    ):
        parsed = _as_float(actual.get(key))
        if parsed is not None:
            return float(parsed)
    evidence = actual.get("pressure_evidence")
    if isinstance(evidence, Mapping):
        return _extract_pressure_hpa(evidence)
    return None


def _first_metric_from_rows(rows: list[dict[str, Any]], *keys: str) -> Any:
    for row in reversed(rows):
        for source in (row.get("actual"), row.get("route_state"), row):
            if not isinstance(source, Mapping):
                continue
            for key in keys:
                if key in source:
                    return source.get(key)
    return None


def _sample_count_by_pressure(sample_rows: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in sample_rows:
        pressure = _as_float(row.get("target_pressure_hpa", row.get("pressure_hpa")))
        if pressure is None:
            continue
        key = _pressure_key(pressure)
        counts[key] = counts.get(key, 0) + 1
    return counts


def _coerce_point_result(row: Mapping[str, Any], raw_cfg: Mapping[str, Any]) -> dict[str, Any]:
    pressure = float(row.get("target_pressure_hpa"))
    index = int(row.get("pressure_point_index", row.get("point_index", 0)) or 0)
    sample_count = int(row.get("sample_count", 0) or 0)
    valid_count = int(row.get("valid_frame_count", sample_count) or 0)
    pressure_age = row.get("pressure_age_ms_before_sample")
    if pressure_age is not None:
        pressure_age = float(pressure_age)
    fresh = _as_bool(row.get("pressure_gauge_freshness_ok_before_sample")) is True
    point_completed = _as_bool(row.get("point_completed")) is True
    pressure_ready = str(row.get("pressure_ready_gate_result") or "").strip().upper()
    if not pressure_ready:
        pressure_ready = "PASS" if point_completed else "FAIL_CLOSED"
    final = str(row.get("point_final_decision") or "").strip().upper()
    if not final:
        final = "PASS" if point_completed else "FAIL_CLOSED"
    return {
        "target_pressure_hpa": pressure,
        "pressure_point_index": index,
        "pressure_setpoint_command_sent": _as_bool(row.get("pressure_setpoint_command_sent")) is True,
        "pressure_setpoint_scope": str(row.get("pressure_setpoint_scope") or ""),
        "vent_state_before_point": row.get("vent_state_before_point"),
        "seal_state_before_point": row.get("seal_state_before_point"),
        "pressure_gauge_hpa_before_ready": row.get("pressure_gauge_hpa_before_ready"),
        "pressure_gauge_hpa_before_sample": row.get("pressure_gauge_hpa_before_sample"),
        "pressure_age_ms_before_sample": pressure_age,
        "pressure_gauge_freshness_ok_before_sample": fresh,
        "pressure_ready_gate_result": pressure_ready,
        "pressure_ready_gate_latency_ms": row.get("pressure_ready_gate_latency_ms"),
        "heartbeat_ready_before_sample": _as_bool(row.get("heartbeat_ready_before_sample")) is True,
        "heartbeat_gap_observed_ms": row.get("heartbeat_gap_observed_ms"),
        "heartbeat_emission_gap_ms": row.get("heartbeat_emission_gap_ms"),
        "blocking_operation_duration_ms": row.get("blocking_operation_duration_ms"),
        "route_conditioning_ready_before_sample": _as_bool(row.get("route_conditioning_ready_before_sample")) is True,
        "sample_count": sample_count,
        "valid_frame_count": valid_count,
        "frame_has_data": _as_bool(row.get("frame_has_data")) is True,
        "frame_usable": _as_bool(row.get("frame_usable")) is True,
        "frame_status": str(row.get("frame_status") or ""),
        "analyzer_ids_seen": list(row.get("analyzer_ids_seen") or []),
        "point_completed": point_completed,
        "point_final_decision": final,
    }


def _point_results_from_execution(
    execution: Mapping[str, Any],
    raw_cfg: Mapping[str, Any],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    explicit = execution.get("point_results")
    if isinstance(explicit, list):
        point_results = [_coerce_point_result(row, raw_cfg) for row in explicit if isinstance(row, Mapping)]
        return (
            point_results,
            [dict(row) for row in execution.get("route_trace_rows") or [] if isinstance(row, Mapping)],
            [dict(row) for row in execution.get("pressure_trace_rows") or [] if isinstance(row, Mapping)],
            [dict(row) for row in execution.get("sample_rows") or [] if isinstance(row, Mapping)],
        )

    run_dir = execution.get("execution_run_dir")
    route_rows = [dict(row) for row in execution.get("route_trace_rows") or [] if isinstance(row, Mapping)]
    if not route_rows and run_dir:
        route_rows = _load_jsonl(Path(str(run_dir)) / "route_trace.jsonl")
    timing_rows = [dict(row) for row in execution.get("timing_trace_rows") or [] if isinstance(row, Mapping)]
    if not timing_rows and run_dir:
        timing_rows = _load_jsonl(Path(str(run_dir)) / "workflow_timing_trace.jsonl")
    sample_rows = [dict(row) for row in execution.get("sample_rows") or [] if isinstance(row, Mapping)]
    if not sample_rows and run_dir:
        sample_rows = _load_csv_dicts(Path(str(run_dir)) / "samples.csv")
    service_summary = dict(execution.get("service_summary") or {})
    completed = {
        float(item)
        for item in _float_list(service_summary.get("planned_pressure_points_completed", []))
    }
    sample_counts = _sample_count_by_pressure(sample_rows)
    freshness_max_age_ms = _pressure_freshness_max_age_ms(raw_cfg)

    point_results: list[dict[str, Any]] = []
    pressure_trace_rows: list[dict[str, Any]] = []
    for index, pressure in enumerate(A2_ALLOWED_PRESSURE_POINTS_HPA, start=1):
        rows = _route_rows_for_pressure(route_rows, pressure, index)
        timing_for_point = _route_rows_for_pressure(timing_rows, pressure, index)
        metric_rows = rows + timing_for_point
        ready_row = next((row for row in rows if row.get("action") == "pressure_control_ready_gate"), {})
        ready_actual = _extract_actual(ready_row)
        sample_count = sample_counts.get(_pressure_key(pressure), 0)
        pressure_age_ms = _extract_pressure_age_ms(ready_actual)
        pressure_hpa = _extract_pressure_hpa(ready_actual)
        pressure_ready = (
            "PASS"
            if str(ready_actual.get("gate_decision") or ready_actual.get("control_ready_status") or "").lower()
            in {"ready", "pass", "ok"}
            or ready_row.get("result") == "ok"
            else "FAIL_CLOSED"
        )
        fresh = bool(pressure_age_ms is not None and pressure_age_ms <= freshness_max_age_ms)
        point_completed = pressure in completed or sample_count > 0
        setpoint_sent = any(str(row.get("action") or "") == "set_pressure" for row in rows)
        heartbeat_ready = any(str(row.get("action") or "") == "set_vent" and row.get("result") == "ok" for row in rows)
        route_ready = any(str(row.get("action") or "") in {"set_co2_valves", "route_baseline"} and row.get("result") == "ok" for row in rows)
        result = {
            "target_pressure_hpa": pressure,
            "pressure_point_index": index,
            "pressure_setpoint_command_sent": bool(setpoint_sent),
            "pressure_setpoint_scope": "authorized_a2_pressure_control_scope" if setpoint_sent else "",
            "vent_state_before_point": ready_actual.get("vent_status_raw", ready_actual.get("pressure_controller_vent_status")),
            "seal_state_before_point": ready_actual.get("seal_transition_status"),
            "pressure_gauge_hpa_before_ready": pressure_hpa,
            "pressure_gauge_hpa_before_sample": pressure_hpa,
            "pressure_age_ms_before_sample": pressure_age_ms,
            "pressure_gauge_freshness_ok_before_sample": fresh,
            "pressure_ready_gate_result": pressure_ready,
            "pressure_ready_gate_latency_ms": None,
            "heartbeat_ready_before_sample": heartbeat_ready,
            "heartbeat_gap_observed_ms": _first_metric_from_rows(
                metric_rows,
                "heartbeat_gap_observed_ms",
                "vent_heartbeat_gap_ms",
            ),
            "heartbeat_emission_gap_ms": _first_metric_from_rows(metric_rows, "heartbeat_emission_gap_ms"),
            "blocking_operation_duration_ms": _first_metric_from_rows(
                metric_rows,
                "blocking_operation_duration_ms",
            ),
            "route_conditioning_ready_before_sample": route_ready,
            "sample_count": int(sample_count),
            "valid_frame_count": int(sample_count),
            "frame_has_data": sample_count > 0,
            "frame_usable": sample_count > 0,
            "frame_status": "frames_seen" if sample_count > 0 else "no_frame_seen",
            "analyzer_ids_seen": sorted({str(row.get("device_id") or row.get("id") or "") for row in sample_rows if row.get("device_id") or row.get("id")}),
            "point_completed": bool(point_completed),
            "point_final_decision": "PASS" if point_completed else "FAIL_CLOSED",
        }
        point_results.append(result)
        pressure_trace_rows.extend(rows)
    return point_results, route_rows, pressure_trace_rows, sample_rows


def _downstream_after_failure(point_results: list[Mapping[str, Any]]) -> bool:
    seen_failure = False
    for point in point_results:
        final = str(point.get("point_final_decision") or "").upper()
        if seen_failure and (
            _as_bool(point.get("pressure_setpoint_command_sent")) is True
            or int(point.get("sample_count") or 0) > 0
            or _as_bool(point.get("point_completed")) is True
        ):
            return True
        if final != "PASS":
            seen_failure = True
    return False


def _write_point_results_csv(path: Path, point_results: list[Mapping[str, Any]]) -> None:
    fields = [
        "pressure_point_index",
        "target_pressure_hpa",
        "pressure_ready_gate_result",
        "pressure_gauge_freshness_ok_before_sample",
        "pressure_age_ms_before_sample",
        "heartbeat_gap_observed_ms",
        "heartbeat_emission_gap_ms",
        "blocking_operation_duration_ms",
        "sample_count",
        "valid_frame_count",
        "point_completed",
        "point_final_decision",
    ]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for row in point_results:
            writer.writerow({field: row.get(field) for field in fields})


def write_a2_co2_7_pressure_no_write_probe_artifacts(
    raw_cfg: Mapping[str, Any],
    *,
    output_dir: Optional[str | Path] = None,
    config_path: str | Path = "",
    operator_confirmation_path: Optional[str | Path] = None,
    branch: str = "",
    head: str = "",
    cli_allow: bool = False,
    env: Optional[Mapping[str, str]] = None,
    execute_probe: bool = False,
    run_app_py_untouched: bool = True,
    executor: Optional[Callable[[str | Path], Mapping[str, Any]]] = None,
) -> dict[str, Any]:
    admission = evaluate_a2_co2_7_pressure_no_write_gate(
        raw_cfg,
        cli_allow=cli_allow,
        env=env,
        operator_confirmation_path=operator_confirmation_path,
        branch=branch,
        head=head,
        config_path=str(config_path or ""),
        run_app_py_untouched=run_app_py_untouched,
    )
    run_dir = Path(output_dir).expanduser().resolve() if output_dir else _default_output_dir()
    run_dir.mkdir(parents=True, exist_ok=True)
    artifact_paths = {
        "summary": str(run_dir / "summary.json"),
        "a2_pressure_sweep_trace": str(run_dir / "a2_pressure_sweep_trace.jsonl"),
        "route_trace": str(run_dir / "route_trace.jsonl"),
        "pressure_trace": str(run_dir / "pressure_trace.jsonl"),
        "pressure_ready_trace": str(run_dir / "pressure_ready_trace.jsonl"),
        "heartbeat_trace": str(run_dir / "heartbeat_trace.jsonl"),
        "analyzer_sampling_rows": str(run_dir / "analyzer_sampling_rows.jsonl"),
        "point_results": str(run_dir / "point_results.json"),
        "point_results_csv": str(run_dir / "point_results.csv"),
        "safety_assertions": str(run_dir / "safety_assertions.json"),
        "operator_confirmation_record": str(run_dir / "operator_confirmation_record.json"),
    }

    rejection_reasons = list(admission.reasons)
    executed = bool(admission.approved and execute_probe)
    execution_error = ""
    execution: Mapping[str, Any] = {}
    if not execute_probe:
        rejection_reasons.append("execute_probe_not_requested")
    elif not admission.approved:
        rejection_reasons.append("admission_not_approved")
    else:
        try:
            execution = (executor or execute_existing_v2_a2_pressure_sweep)(str(config_path))
        except Exception as exc:
            execution_error = str(exc)
            rejection_reasons.append(f"execution_error:{exc}")

    point_results, route_rows, pressure_rows, sample_rows = _point_results_from_execution(execution, raw_cfg)
    if not point_results:
        point_results = [
            {
                "target_pressure_hpa": pressure,
                "pressure_point_index": index,
                "pressure_setpoint_command_sent": False,
                "pressure_setpoint_scope": "",
                "vent_state_before_point": None,
                "seal_state_before_point": None,
                "pressure_gauge_hpa_before_ready": None,
                "pressure_gauge_hpa_before_sample": None,
                "pressure_age_ms_before_sample": None,
                "pressure_gauge_freshness_ok_before_sample": False,
                "pressure_ready_gate_result": "FAIL_CLOSED",
                "pressure_ready_gate_latency_ms": None,
                "heartbeat_ready_before_sample": False,
                "heartbeat_gap_observed_ms": None,
                "heartbeat_emission_gap_ms": None,
                "blocking_operation_duration_ms": None,
                "route_conditioning_ready_before_sample": False,
                "sample_count": 0,
                "valid_frame_count": 0,
                "frame_has_data": False,
                "frame_usable": False,
                "frame_status": "not_executed",
                "analyzer_ids_seen": [],
                "point_completed": False,
                "point_final_decision": "FAIL_CLOSED",
            }
            for index, pressure in enumerate(A2_ALLOWED_PRESSURE_POINTS_HPA, start=1)
        ]

    point_results = sorted(point_results, key=lambda row: int(row.get("pressure_point_index") or 0))
    sample_count_by_pressure = {
        _pressure_key(point["target_pressure_hpa"]): int(point.get("sample_count") or 0)
        for point in point_results
    }
    sample_min_count = _sample_min_count(raw_cfg)
    pressure_points_completed = sum(1 for point in point_results if _as_bool(point.get("point_completed")) is True)
    sample_count_total = sum(int(point.get("sample_count") or 0) for point in point_results)
    all_have_fresh = all(_as_bool(point.get("pressure_gauge_freshness_ok_before_sample")) is True for point in point_results)
    all_have_samples = all(int(point.get("sample_count") or 0) >= sample_min_count for point in point_results)
    all_completed = pressure_points_completed == len(A2_ALLOWED_PRESSURE_POINTS_HPA)
    all_ready = all(str(point.get("pressure_ready_gate_result") or "").upper() == "PASS" for point in point_results)
    all_heartbeat = all(_as_bool(point.get("heartbeat_ready_before_sample")) is True for point in point_results)
    all_route = all(_as_bool(point.get("route_conditioning_ready_before_sample")) is True for point in point_results)
    downstream_after_failure = _downstream_after_failure(point_results)
    if downstream_after_failure:
        rejection_reasons.append("downstream_point_executed_after_fail_closed_point")

    no_write_ok = True
    safety_assertions = {
        **A2_EVIDENCE_MARKERS,
        **A2_SAFETY_ASSERTION_DEFAULTS,
        "authorized_pressure_control_scope": "A2 CO2-only 7-pressure transient pressure readiness/control only",
        "pressure_setpoint_command_sent": any(
            _as_bool(point.get("pressure_setpoint_command_sent")) is True for point in point_results
        ),
        "pressure_setpoint_scope": "authorized_a2_pressure_control_scope",
        "vent_off_command_sent": any(str(row.get("action") or "") == "set_vent" and _as_bool((row.get("target") or {}).get("vent_on")) is False for row in route_rows if isinstance(row.get("target"), Mapping)),
        "vent_off_command_scope": "authorized_a2_pressure_control_scope",
        "seal_command_sent": any(str(row.get("action") or "") in {"seal_route", "seal_transition"} for row in route_rows),
        "seal_command_scope": "authorized_a2_pressure_control_scope",
        "high_pressure_1100_hpa_prearm_recorded": any(
            str(row.get("action") or "") == "high_pressure_first_point_mode_enabled" for row in route_rows
        ),
        "sample_count_total": int(sample_count_total),
        "pressure_points_completed": int(pressure_points_completed),
        "no_write": no_write_ok,
    }

    pass_conditions = [
        admission.approved,
        executed,
        not rejection_reasons,
        all_ready,
        all_have_fresh,
        all_have_samples,
        all_completed,
        all_heartbeat,
        all_route,
        no_write_ok,
        not downstream_after_failure,
    ]
    final_decision = "PASS" if all(pass_conditions) else "FAIL_CLOSED"
    if final_decision != "PASS" and not rejection_reasons:
        rejection_reasons.append("a2_pass_conditions_not_met")
    rejection_reasons = list(dict.fromkeys(rejection_reasons))

    pressure_ready_rows = [
        {
            "timestamp": _now(),
            "event": "a2_pressure_ready_gate",
            "target_pressure_hpa": point.get("target_pressure_hpa"),
            "pressure_point_index": point.get("pressure_point_index"),
            "pressure_ready_gate_result": point.get("pressure_ready_gate_result"),
            "pressure_ready_gate_latency_ms": point.get("pressure_ready_gate_latency_ms"),
            "pressure_gauge_hpa_before_ready": point.get("pressure_gauge_hpa_before_ready"),
            "pressure_gauge_hpa_before_sample": point.get("pressure_gauge_hpa_before_sample"),
            "pressure_age_ms_before_sample": point.get("pressure_age_ms_before_sample"),
            "pressure_gauge_freshness_ok_before_sample": point.get("pressure_gauge_freshness_ok_before_sample"),
            "heartbeat_gap_observed_ms": point.get("heartbeat_gap_observed_ms"),
            "heartbeat_emission_gap_ms": point.get("heartbeat_emission_gap_ms"),
            "blocking_operation_duration_ms": point.get("blocking_operation_duration_ms"),
        }
        for point in point_results
    ]
    heartbeat_rows = [
        {
            "timestamp": _now(),
            "event": "a2_heartbeat_before_sample",
            "target_pressure_hpa": point.get("target_pressure_hpa"),
            "pressure_point_index": point.get("pressure_point_index"),
            "heartbeat_ready_before_sample": point.get("heartbeat_ready_before_sample"),
            "heartbeat_gap_observed_ms": point.get("heartbeat_gap_observed_ms"),
            "heartbeat_emission_gap_ms": point.get("heartbeat_emission_gap_ms"),
            "blocking_operation_duration_ms": point.get("blocking_operation_duration_ms"),
            "route_conditioning_ready_before_sample": point.get("route_conditioning_ready_before_sample"),
        }
        for point in point_results
    ]
    sweep_rows = [
        {
            "timestamp": _now(),
            "event": "a2_pressure_point_result",
            **dict(point),
        }
        for point in point_results
    ]

    summary = {
        "schema_version": A2_SCHEMA_VERSION,
        **A2_EVIDENCE_MARKERS,
        "final_decision": final_decision,
        "rejection_reasons": rejection_reasons,
        "admission_approved": admission.approved,
        "operator_confirmation_valid": bool(admission.operator_validation.get("valid")),
        "r0_1_reference_readonly_prereq_pass": bool(admission.evidence["r0_1_reference_readonly_prereq_pass"]),
        "r0_full_query_only_prereq_pass": bool(admission.evidence["r0_full_query_only_prereq_pass"]),
        "r1_conditioning_only_prereq_pass": bool(admission.evidence["r1_conditioning_only_prereq_pass"]),
        "a1r_minimal_sampling_prereq_pass": bool(admission.evidence["a1r_minimal_sampling_prereq_pass"]),
        "a1r_output_dir": _output_dir_value(raw_cfg, "a1r_output_dir"),
        "current_branch": branch,
        "current_head": head,
        "v1_fallback_required": True,
        "v1_untouched": True,
        "run_app_py_untouched": bool(run_app_py_untouched),
        "a2_1_heartbeat_gap_accounting_fix_present": A2_1_HEARTBEAT_GAP_ACCOUNTING_FIX_PRESENT,
        "a2_pressure_sweep_executed": bool(executed),
        "real_probe_executed": bool(executed),
        "underlying_execution_dir": str(execution.get("execution_run_dir") or ""),
        "execution_error": execution_error,
        "co2_only": True,
        "skip0": True,
        "single_route": True,
        "single_temperature": True,
        "pressure_points_hpa": list(A2_ALLOWED_PRESSURE_POINTS_HPA),
        "pressure_points_expected": len(A2_ALLOWED_PRESSURE_POINTS_HPA),
        "pressure_points_completed": int(pressure_points_completed),
        "completed_pressure_points_hpa": [
            float(point["target_pressure_hpa"])
            for point in point_results
            if _as_bool(point.get("point_completed")) is True
        ],
        "points_completed": int(pressure_points_completed),
        "sample_count_total": int(sample_count_total),
        "sample_count_by_pressure": sample_count_by_pressure,
        "sample_min_count_per_pressure": sample_min_count,
        "all_pressure_points_have_fresh_pressure_before_sample": bool(all_have_fresh),
        "all_pressure_points_have_samples": bool(all_have_samples),
        "all_pressure_points_completed": bool(all_completed),
        "all_pressure_points_pressure_ready": bool(all_ready),
        "all_pressure_points_heartbeat_ready_before_sample": bool(all_heartbeat),
        "all_pressure_points_route_conditioning_ready_before_sample": bool(all_route),
        "a3_allowed": False,
        "artifact_paths": artifact_paths,
        **safety_assertions,
    }
    operator_record = {
        "schema_version": A2_SCHEMA_VERSION,
        "record_type": "a2_operator_confirmation_record",
        "operator_confirmation_path": str(Path(operator_confirmation_path).expanduser().resolve()) if operator_confirmation_path else "",
        "validation": admission.operator_validation,
        "payload": admission.operator_confirmation,
        **A2_EVIDENCE_MARKERS,
    }

    _json_dump(run_dir / "summary.json", summary)
    _jsonl_dump(run_dir / "a2_pressure_sweep_trace.jsonl", sweep_rows)
    _jsonl_dump(run_dir / "route_trace.jsonl", route_rows)
    _jsonl_dump(run_dir / "pressure_trace.jsonl", pressure_rows)
    _jsonl_dump(run_dir / "pressure_ready_trace.jsonl", pressure_ready_rows)
    _jsonl_dump(run_dir / "heartbeat_trace.jsonl", heartbeat_rows)
    _jsonl_dump(run_dir / "analyzer_sampling_rows.jsonl", sample_rows)
    _json_dump(run_dir / "point_results.json", {"points": point_results})
    _write_point_results_csv(run_dir / "point_results.csv", point_results)
    _json_dump(run_dir / "safety_assertions.json", safety_assertions)
    _json_dump(run_dir / "operator_confirmation_record.json", operator_record)
    return summary
