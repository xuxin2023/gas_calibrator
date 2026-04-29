from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import csv
import hashlib
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
A2_3_V1_PRESSURE_GAUGE_READ_POLICY_PRESENT = True
A2_4_V1_PRESSURE_GAUGE_READ_POLICY_PRESENT = True
A2_ENV_VAR = "GAS_CAL_V2_A2_CO2_7_PRESSURE_NO_WRITE_REAL_COM"
A2_ENV_VALUE = "1"
A2_CLI_FLAG = "--allow-v2-a2-co2-7-pressure-no-write-real-com"
A2_ALLOWED_PRESSURE_POINTS_HPA = (1100.0, 1000.0, 900.0, 800.0, 700.0, 600.0, 500.0)
A2_EVIDENCE_MARKERS = {
    "evidence_source": "real_probe_a2_9_co2_7_pressure_no_write",
    "legacy_evidence_source": "real_probe_a2_8_co2_7_pressure_no_write",
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
    "pressure_source",
    "port_manifest",
    "explicit_acknowledgement",
)
A2_REQUIRED_TRUE_ACKS = (
    "only_a2_co2_7_pressure_no_write",
    "co2_only",
    "skip0",
    "single_route",
    "single_temperature",
    "skip_temperature_stabilization_wait",
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
    "final_safe_stop_chamber_stop_blocked_by_no_write": False,
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


class A2PointsConfigAlignmentError(RuntimeError):
    """Raised when wrapper/downstream A2 points config cannot be aligned safely."""


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
    return load_json_mapping(path)


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
    if str(payload.get("pressure_source") or "").strip().lower() != "v1_aligned":
        errors.append("operator_confirmation_pressure_source_not_v1_aligned")

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


def _sha256_file(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _json_clone_mapping(raw_cfg: Mapping[str, Any]) -> dict[str, Any]:
    return json.loads(json.dumps(dict(raw_cfg), ensure_ascii=False))


def _selected_temperature_c(raw_cfg: Mapping[str, Any]) -> float:
    value = _first_value(
        raw_cfg,
        (
            "workflow.selected_temps_c",
            "selected_temps_c",
            "a2_co2_7_pressure_no_write_probe.temperature_c",
            "a2_co2_7_pressure_no_write_probe.temp_chamber_c",
        ),
    )
    if isinstance(value, list) and value:
        parsed = _as_float(value[0])
    else:
        parsed = _as_float(value)
    return float(parsed) if parsed is not None else 20.0


def _co2_point_ppm(raw_cfg: Mapping[str, Any]) -> float:
    value = _first_value(
        raw_cfg,
        (
            "a2_co2_7_pressure_no_write_probe.co2_ppm",
            "a2_co2_7_pressure_no_write_probe.target_co2_ppm",
            "run001_a2.co2_ppm",
            "run001_a2.target_co2_ppm",
            "workflow.co2_ppm",
        ),
    )
    parsed = _as_float(value)
    return float(parsed) if parsed is not None else 100.0


def _build_a2_downstream_point_rows(raw_cfg: Mapping[str, Any]) -> list[dict[str, Any]]:
    temperature_c = _selected_temperature_c(raw_cfg)
    co2_ppm = _co2_point_ppm(raw_cfg)
    rows: list[dict[str, Any]] = []
    for index, pressure in enumerate(A2_ALLOWED_PRESSURE_POINTS_HPA, start=1):
        rows.append(
            {
                "index": index,
                "route": "co2",
                "pressure_hpa": float(pressure),
                "target_pressure_hpa": float(pressure),
                "co2_ppm": co2_ppm,
                "temperature_c": temperature_c,
                "temp_chamber_c": temperature_c,
                "co2_group": "A",
                "cylinder_nominal_ppm": co2_ppm,
            }
        )
    return rows


def _point_row_pressure(row: Mapping[str, Any]) -> Optional[float]:
    for key in ("pressure_hpa", "target_pressure_hpa", "pressure"):
        parsed = _as_float(row.get(key))
        if parsed is not None:
            return float(parsed)
    return None


def _validate_a2_downstream_point_rows(rows: list[Mapping[str, Any]]) -> list[str]:
    reasons: list[str] = []
    if len(rows) != len(A2_ALLOWED_PRESSURE_POINTS_HPA):
        reasons.append("a2_point_count_mismatch")
    routes = []
    pressures = []
    for row in rows:
        route = str(row.get("route", "") or "").strip().lower()
        routes.append(route)
        pressure = _point_row_pressure(row)
        if not route:
            reasons.append("a2_point_route_missing")
        if pressure is None:
            reasons.append("a2_point_pressure_missing")
        else:
            pressures.append(pressure)
    if set(routes) != {"co2"}:
        reasons.append("a2_points_not_co2_only")
    if not _same_pressure_points(pressures):
        reasons.append("a2_point_pressure_list_mismatch")
    return list(dict.fromkeys(reasons))


def _write_json_no_bom(path: Path, payload: Mapping[str, Any] | list[Mapping[str, Any]]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def prepare_a2_downstream_points_config(
    raw_cfg: Mapping[str, Any],
    *,
    config_path: str | Path,
    output_dir: str | Path,
) -> tuple[Path, dict[str, Any]]:
    from gas_calibrator.v2.core.run001_a1_dry_run import load_point_rows, resolve_config_relative_path

    run_dir = Path(output_dir).expanduser().resolve()
    run_dir.mkdir(parents=True, exist_ok=True)
    original_config_path = Path(config_path).expanduser().resolve()
    raw_paths = raw_cfg.get("paths") if isinstance(raw_cfg.get("paths"), Mapping) else {}
    raw_points_value = str((raw_paths or {}).get("points_excel") or "").strip()
    resolved_points = resolve_config_relative_path(original_config_path, raw_points_value)

    if raw_points_value:
        suffix = Path(raw_points_value).suffix.lower() or (resolved_points.suffix.lower() if resolved_points else "")
        if suffix and suffix != ".json":
            raise A2PointsConfigAlignmentError("a2_points_json_suffix_not_json")

    generated = False
    if resolved_points is not None and resolved_points.exists():
        points_path = resolved_points
        rows = load_point_rows(original_config_path, raw_cfg)
        reasons = _validate_a2_downstream_point_rows(rows)
        if reasons:
            raise A2PointsConfigAlignmentError("; ".join(reasons))
    else:
        generated = True
        rows = _build_a2_downstream_point_rows(raw_cfg)
        reasons = _validate_a2_downstream_point_rows(rows)
        if reasons:
            raise A2PointsConfigAlignmentError("; ".join(reasons))
        points_path = run_dir / "a2_3_v1_aligned_points.json"
        _write_json_no_bom(points_path, rows)

    aligned_raw_cfg = _json_clone_mapping(raw_cfg)
    paths = aligned_raw_cfg.setdefault("paths", {})
    if not isinstance(paths, dict):
        paths = {}
        aligned_raw_cfg["paths"] = paths
    paths["points_excel"] = str(points_path)
    workflow = aligned_raw_cfg.setdefault("workflow", {})
    if not isinstance(workflow, dict):
        workflow = {}
        aligned_raw_cfg["workflow"] = workflow
    pressure_cfg = workflow.setdefault("pressure", {})
    if not isinstance(pressure_cfg, dict):
        pressure_cfg = {}
        workflow["pressure"] = pressure_cfg
    pressure_cfg["a2_conditioning_pressure_source"] = "v1_aligned"
    pressure_cfg.setdefault("route_conditioning_high_frequency_vent_interval_s", 0.5)
    pressure_cfg.setdefault("route_conditioning_high_frequency_max_gap_s", 1.0)
    pressure_cfg.setdefault("route_conditioning_high_frequency_vent_window_s", 20.0)
    pressure_cfg.setdefault("route_conditioning_vent_maintenance_interval_s", 1.0)
    pressure_cfg.setdefault("route_conditioning_vent_maintenance_max_gap_s", 2.0)
    pressure_cfg.setdefault("route_conditioning_fast_vent_max_duration_s", 0.5)
    pressure_cfg.setdefault("route_conditioning_scheduler_sleep_step_s", 0.1)
    pressure_cfg.setdefault("route_conditioning_diagnostic_budget_ms", 100.0)
    pressure_cfg.setdefault("route_conditioning_pressure_monitor_budget_ms", 100.0)
    pressure_cfg.setdefault("route_conditioning_trace_write_budget_ms", 50.0)
    stability_cfg = workflow.setdefault("stability", {})
    if not isinstance(stability_cfg, dict):
        stability_cfg = {}
        workflow["stability"] = stability_cfg
    temperature_cfg = stability_cfg.setdefault("temperature", {})
    if not isinstance(temperature_cfg, dict):
        temperature_cfg = {}
        stability_cfg["temperature"] = temperature_cfg
    temperature_cfg["skip_temperature_stabilization_wait"] = True
    temperature_cfg["temperature_stabilization_wait_skipped"] = True
    temperature_cfg["temperature_gate_mode"] = "current_pv_engineering_probe"
    temperature_cfg["temperature_not_part_of_acceptance"] = True
    temperature_cfg["wait_for_target_before_continue"] = False
    temperature_cfg["analyzer_chamber_temp_enabled"] = False
    temperature_cfg["soak_after_reach_s"] = 0.0
    probe_cfg = aligned_raw_cfg.setdefault("a2_co2_7_pressure_no_write_probe", {})
    if isinstance(probe_cfg, dict):
        probe_cfg["pressure_source"] = "v1_aligned"
        probe_cfg["temperature_stabilization_wait_skipped"] = True
        probe_cfg["temperature_gate_mode"] = "current_pv_engineering_probe"
        probe_cfg["temperature_not_part_of_acceptance"] = True
    aligned_config_path = run_dir / "a2_3_v1_aligned_downstream_config.json"
    _write_json_no_bom(aligned_config_path, aligned_raw_cfg)

    loaded_rows = load_point_rows(aligned_config_path, aligned_raw_cfg)
    loaded_reasons = _validate_a2_downstream_point_rows(loaded_rows)
    if loaded_reasons:
        raise A2PointsConfigAlignmentError("; ".join(loaded_reasons))

    point_pressures = [float(_point_row_pressure(row) or 0.0) for row in loaded_rows]
    point_routes = [str(row.get("route", "") or "").strip().lower() for row in loaded_rows]
    metadata = {
        "points_config_alignment_ready": True,
        "generated_points_json_path": str(points_path) if generated else "",
        "generated_points_json_sha256": _sha256_file(points_path) if generated else "",
        "effective_points_json_path": str(points_path),
        "effective_points_json_sha256": _sha256_file(points_path),
        "downstream_aligned_config_path": str(aligned_config_path),
        "downstream_points_row_count": len(loaded_rows),
        "downstream_point_routes": point_routes,
        "downstream_point_pressures_hpa": point_pressures,
        "downstream_points_gate_reasons": [],
        "downstream_points_generated": generated,
    }
    return aligned_config_path, metadata


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


def _a2_3_pressure_source_strategy(raw_cfg: Mapping[str, Any]) -> str:
    value = str(
        _first_value(
            raw_cfg,
            (
                "workflow.pressure.a2_conditioning_pressure_source",
                "workflow.pressure.conditioning_pressure_source",
                "a2_co2_7_pressure_no_write_probe.workflow.pressure.a2_conditioning_pressure_source",
                "run001_a2.workflow.pressure.a2_conditioning_pressure_source",
            ),
        )
        or "continuous"
    ).strip().lower()
    aliases = {
        "p3": "p3_fast_poll",
        "p3_fast": "p3_fast_poll",
        "fast_poll": "p3_fast_poll",
        "continuous_stream": "continuous",
        "v1": "v1_aligned",
        "v1_aligned_p3": "v1_aligned",
    }
    value = aliases.get(value, value)
    return value if value in {"v1_aligned", "continuous", "p3_fast_poll", "auto"} else "continuous"


def _pressure_gate_source_is_v1_aligned(source: Any) -> bool:
    text = str(source or "").strip().lower()
    if not text:
        return False
    if text in {"v1_aligned", "digital_pressure_gauge_p3", "digital_pressure_gauge_p3_fast_poll"}:
        return True
    return bool("digital_pressure_gauge" in text and "p3" in text)


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
        "pressure_source_selected": row.get("pressure_source_selected"),
        "pressure_source_selection_reason": row.get("pressure_source_selection_reason"),
        "selected_pressure_source": row.get("selected_pressure_source"),
        "selected_pressure_sample_age_s": row.get("selected_pressure_sample_age_s"),
        "selected_pressure_sample_is_stale": _as_bool(row.get("selected_pressure_sample_is_stale")) is True,
        "selected_pressure_parse_ok": _as_bool(row.get("selected_pressure_parse_ok")) is True,
        "selected_pressure_freshness_ok": _as_bool(row.get("selected_pressure_freshness_ok")) is True,
        "pressure_freshness_decision_source": row.get("pressure_freshness_decision_source"),
        "selected_pressure_fail_closed_reason": row.get("selected_pressure_fail_closed_reason") or "",
        "continuous_stream_stale": _as_bool(row.get("continuous_stream_stale")) is True,
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
            "pressure_source_selected": _first_metric_from_rows(metric_rows, "pressure_source_selected"),
            "pressure_source_selection_reason": _first_metric_from_rows(
                metric_rows,
                "pressure_source_selection_reason",
                "source_selection_reason",
            ),
            "selected_pressure_source": _first_metric_from_rows(metric_rows, "selected_pressure_source"),
            "selected_pressure_sample_age_s": _first_metric_from_rows(
                metric_rows,
                "selected_pressure_sample_age_s",
            ),
            "selected_pressure_sample_is_stale": _as_bool(
                _first_metric_from_rows(metric_rows, "selected_pressure_sample_is_stale")
            )
            is True,
            "selected_pressure_parse_ok": _as_bool(
                _first_metric_from_rows(metric_rows, "selected_pressure_parse_ok")
            )
            is True,
            "selected_pressure_freshness_ok": _as_bool(
                _first_metric_from_rows(metric_rows, "selected_pressure_freshness_ok")
            )
            is True,
            "pressure_freshness_decision_source": _first_metric_from_rows(
                metric_rows,
                "pressure_freshness_decision_source",
            ),
            "selected_pressure_fail_closed_reason": _first_metric_from_rows(
                metric_rows,
                "selected_pressure_fail_closed_reason",
            )
            or "",
            "continuous_stream_stale": _as_bool(
                _first_metric_from_rows(metric_rows, "continuous_stream_stale")
            )
            is True,
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
        "pressure_source_selected",
        "pressure_source_selection_reason",
        "selected_pressure_source",
        "selected_pressure_sample_age_s",
        "selected_pressure_sample_is_stale",
        "selected_pressure_parse_ok",
        "selected_pressure_freshness_ok",
        "pressure_freshness_decision_source",
        "selected_pressure_fail_closed_reason",
        "selected_pressure_sample_stale_duration_ms",
        "selected_pressure_sample_stale_budget_ms",
        "selected_pressure_sample_stale_budget_exceeded",
        "selected_pressure_sample_stale_deferred_for_vent_priority",
        "continuous_latest_fresh_fast_path_used",
        "continuous_latest_fresh_duration_ms",
        "continuous_latest_fresh_budget_exceeded",
        "continuous_stream_stale",
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
    points_alignment: dict[str, Any] = {
        "points_config_alignment_ready": False,
        "generated_points_json_path": "",
        "generated_points_json_sha256": "",
        "effective_points_json_path": "",
        "effective_points_json_sha256": "",
        "downstream_aligned_config_path": "",
        "downstream_points_row_count": 0,
        "downstream_point_routes": [],
        "downstream_point_pressures_hpa": [],
        "downstream_points_gate_reasons": [],
        "downstream_points_generated": False,
    }
    execution_config_path = str(config_path)
    if not execute_probe:
        rejection_reasons.append("execute_probe_not_requested")
    elif not admission.approved:
        rejection_reasons.append("admission_not_approved")
    else:
        try:
            aligned_config_path, points_alignment = prepare_a2_downstream_points_config(
                raw_cfg,
                config_path=config_path,
                output_dir=run_dir,
            )
            execution_config_path = str(aligned_config_path)
            execution = (executor or execute_existing_v2_a2_pressure_sweep)(execution_config_path)
            if isinstance(execution, Mapping) and isinstance(execution.get("points_alignment"), Mapping):
                points_alignment.update(dict(execution.get("points_alignment") or {}))
            else:
                execution = {**dict(execution), "points_alignment": points_alignment}
        except Exception as exc:
            execution_error = str(exc)
            prefix = "points_config_alignment_error" if isinstance(exc, A2PointsConfigAlignmentError) else "execution_error"
            rejection_reasons.append(f"{prefix}:{exc}")

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

    service_summary = dict(execution.get("service_summary") or {}) if isinstance(execution, Mapping) else {}
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
        "chamber_stop_command_sent": _as_bool(
            service_summary.get("chamber_stop_command_sent")
            or service_summary.get("final_safe_stop_chamber_stop_command_sent")
        )
        is True,
        "final_safe_stop_warning_count": int(service_summary.get("final_safe_stop_warning_count") or 0),
        "final_safe_stop_warnings": list(service_summary.get("final_safe_stop_warnings") or []),
        "final_safe_stop_chamber_stop_warning": service_summary.get(
            "final_safe_stop_chamber_stop_warning",
            "",
        ),
        "final_safe_stop_chamber_stop_attempted": _as_bool(
            service_summary.get("final_safe_stop_chamber_stop_attempted")
        )
        is True,
        "final_safe_stop_chamber_stop_command_sent": _as_bool(
            service_summary.get("final_safe_stop_chamber_stop_command_sent")
        )
        is True,
        "final_safe_stop_chamber_stop_result": service_summary.get(
            "final_safe_stop_chamber_stop_result",
            "not_observed",
        ),
        "final_safe_stop_chamber_stop_blocked_by_no_write": _as_bool(
            service_summary.get("final_safe_stop_chamber_stop_blocked_by_no_write")
        )
        is True,
        "high_pressure_1100_hpa_prearm_recorded": any(
            str(row.get("action") or "") == "high_pressure_first_point_mode_enabled" for row in route_rows
        ),
        "sample_count_total": int(sample_count_total),
        "pressure_points_completed": int(pressure_points_completed),
        "no_write": no_write_ok,
    }
    no_write_ok = bool(
        int(safety_assertions.get("attempted_write_count") or 0) == 0
        and _as_bool(safety_assertions.get("any_write_command_sent")) is not True
        and _as_bool(safety_assertions.get("identity_write_command_sent")) is not True
        and _as_bool(safety_assertions.get("mode_switch_command_sent")) is not True
        and _as_bool(safety_assertions.get("senco_write_command_sent")) is not True
        and _as_bool(safety_assertions.get("calibration_write_command_sent")) is not True
        and _as_bool(safety_assertions.get("chamber_write_register_command_sent")) is not True
        and _as_bool(safety_assertions.get("chamber_set_temperature_command_sent")) is not True
        and _as_bool(safety_assertions.get("chamber_start_command_sent")) is not True
        and _as_bool(safety_assertions.get("chamber_stop_command_sent")) is not True
    )
    safety_assertions["no_write"] = no_write_ok

    evidence_metric_rows = route_rows + pressure_rows + sample_rows

    def metric_or_summary(*keys: str) -> Any:
        value = _first_metric_from_rows(evidence_metric_rows, *keys)
        if value is not None:
            return value
        for key in keys:
            if key in service_summary:
                return service_summary.get(key)
        return None

    route_conditioning_pressure_overlimit = _as_bool(
        metric_or_summary("route_conditioning_pressure_overlimit", "pressure_overlimit_seen")
    ) is True
    route_conditioning_vent_gap_exceeded = _as_bool(
        metric_or_summary(
            "route_conditioning_vent_gap_exceeded",
            "vent_heartbeat_gap_exceeded",
        )
    ) is True
    max_vent_pulse_gap_ms = _as_float(metric_or_summary("max_vent_pulse_gap_ms"))
    max_vent_pulse_write_gap_ms = _as_float(metric_or_summary("max_vent_pulse_write_gap_ms"))
    max_vent_pulse_write_gap_ms_including_terminal_gap = _as_float(
        metric_or_summary("max_vent_pulse_write_gap_ms_including_terminal_gap")
    )
    max_vent_pulse_gap_limit_ms = _as_float(metric_or_summary("max_vent_pulse_gap_limit_ms"))
    if (
        max_vent_pulse_gap_ms is not None
        and max_vent_pulse_gap_limit_ms is not None
        and float(max_vent_pulse_gap_ms) > float(max_vent_pulse_gap_limit_ms)
    ):
        route_conditioning_vent_gap_exceeded = True
    if (
        max_vent_pulse_write_gap_ms is not None
        and max_vent_pulse_gap_limit_ms is not None
        and float(max_vent_pulse_write_gap_ms) > float(max_vent_pulse_gap_limit_ms)
    ):
        route_conditioning_vent_gap_exceeded = True
    if (
        max_vent_pulse_write_gap_ms_including_terminal_gap is not None
        and max_vent_pulse_gap_limit_ms is not None
        and float(max_vent_pulse_write_gap_ms_including_terminal_gap) > float(max_vent_pulse_gap_limit_ms)
    ):
        route_conditioning_vent_gap_exceeded = True
    route_conditioning_fast_vent_timeout = _as_bool(
        metric_or_summary("route_conditioning_fast_vent_command_timeout", "pre_route_fast_vent_timeout")
    ) is True
    route_conditioning_fast_vent_not_supported = _as_bool(
        metric_or_summary("route_conditioning_fast_vent_not_supported")
    ) is True
    route_conditioning_diagnostic_blocked = _as_bool(
        metric_or_summary("route_conditioning_diagnostic_blocked_vent_scheduler")
    ) is True
    route_open_transition_blocked = _as_bool(
        metric_or_summary("route_open_transition_blocked_vent_scheduler")
    ) is True
    route_open_settle_wait_blocked = _as_bool(
        metric_or_summary("route_open_settle_wait_blocked_vent_scheduler")
    ) is True
    route_conditioning_vent_command_failed = _as_bool(
        metric_or_summary("vent_command_failed_during_flush")
    ) is True or any(
        str(row.get("action") or "") == "co2_route_conditioning_vent_command_failed"
        or str((row.get("actual") or {}).get("command_result") or "").lower() in {"fail", "failed", "blocked"}
        for row in route_rows
        if isinstance(row.get("actual"), Mapping)
    )
    unsafe_flush_action_seen = any(
        _as_bool(metric_or_summary(key)) is True
        for key in (
            "vent_off_command_during_flush",
            "seal_command_during_flush",
            "pressure_setpoint_command_during_flush",
            "sample_command_during_flush",
        )
    )
    unsafe_vent_command_sent = _as_bool(
        metric_or_summary("unsafe_vent_after_seal_or_pressure_control_command_sent")
    ) is True
    vent_blocked_after_flush = _as_bool(metric_or_summary("vent_pulse_blocked_after_flush_phase")) is True
    if route_conditioning_pressure_overlimit:
        rejection_reasons.append("a2_route_conditioning_pressure_overlimit")
    if route_conditioning_vent_gap_exceeded:
        rejection_reasons.append("a2_route_conditioning_vent_gap_exceeded")
    if route_conditioning_vent_command_failed:
        rejection_reasons.append("a2_route_conditioning_vent_command_failed")
    if route_conditioning_fast_vent_timeout:
        rejection_reasons.append("a2_route_conditioning_fast_vent_command_timeout")
    if route_conditioning_fast_vent_not_supported:
        rejection_reasons.append("a2_route_conditioning_fast_vent_not_supported")
    if route_conditioning_diagnostic_blocked:
        rejection_reasons.append("a2_route_conditioning_diagnostic_blocked_vent_scheduler")
    if route_open_transition_blocked:
        rejection_reasons.append("a2_route_open_transition_blocked_vent_scheduler")
    if route_open_settle_wait_blocked:
        rejection_reasons.append("a2_route_open_settle_wait_blocked_vent_scheduler")
    if unsafe_flush_action_seen:
        rejection_reasons.append("a2_route_conditioning_unsafe_action_before_flush_completed")
    if unsafe_vent_command_sent:
        rejection_reasons.append("a2_route_conditioning_unsafe_vent_after_seal_or_pressure_control")
    if vent_blocked_after_flush:
        rejection_reasons.append("a2_route_conditioning_vent_blocked_after_flush_phase")

    a2_3_strategy = _a2_3_pressure_source_strategy(raw_cfg)
    a2_4_temperature_skip_requested = _as_bool(
        _first_value(
            raw_cfg,
            (
                "a2_co2_7_pressure_no_write_probe.temperature_stabilization_wait_skipped",
                "workflow.stability.temperature.temperature_stabilization_wait_skipped",
                "workflow.stability.temperature.skip_temperature_stabilization_wait",
            ),
        )
    ) is True
    temperature_stabilization_wait_skipped = (
        _as_bool(metric_or_summary("temperature_stabilization_wait_skipped", "wait_skipped")) is True
        or a2_4_temperature_skip_requested
    )
    temperature_gate_mode = str(
        metric_or_summary("temperature_gate_mode")
        or _first_value(
            raw_cfg,
            (
                "a2_co2_7_pressure_no_write_probe.temperature_gate_mode",
                "workflow.stability.temperature.temperature_gate_mode",
                "workflow.stability.temperature.gate_mode",
            ),
        )
        or ("current_pv_engineering_probe" if temperature_stabilization_wait_skipped else "")
    )
    temperature_not_part_of_acceptance = (
        _as_bool(metric_or_summary("temperature_not_part_of_acceptance")) is True
        or _as_bool(
            _first_value(
                raw_cfg,
                (
                    "a2_co2_7_pressure_no_write_probe.temperature_not_part_of_acceptance",
                    "workflow.stability.temperature.temperature_not_part_of_acceptance",
                ),
            )
        )
        is True
    )
    a2_4_probe_required = bool(
        a2_4_temperature_skip_requested
        or str(admission.operator_confirmation.get("pressure_source") or "").strip().lower() == "v1_aligned"
    )
    conditioning_monitor_source = str(
        metric_or_summary("selected_pressure_source_for_conditioning_monitor") or ""
    ).strip()
    pressure_gate_source_observed = str(
        metric_or_summary("selected_pressure_source_for_pressure_gate") or ""
    ).strip()
    if not pressure_gate_source_observed:
        for point in reversed(point_results):
            if not isinstance(point, Mapping):
                continue
            if not (
                _as_bool(point.get("point_completed")) is True
                or int(point.get("sample_count") or 0) > 0
                or point.get("pressure_gauge_hpa_before_ready") not in (None, "")
                or point.get("pressure_gauge_hpa_before_sample") not in (None, "")
            ):
                continue
            pressure_gate_source_observed = str(
                point.get("selected_pressure_source")
                or point.get("pressure_source_selected")
                or point.get("pressure_freshness_decision_source")
                or ""
            ).strip()
            if pressure_gate_source_observed:
                break
    conditioning_monitor_pressure_source_allowed = bool(
        not conditioning_monitor_source
        or conditioning_monitor_source == "digital_pressure_gauge_continuous"
        or _pressure_gate_source_is_v1_aligned(conditioning_monitor_source)
    )
    pressure_gate_evidence_present = bool(
        pressure_gate_source_observed
        or any(
            _as_bool(point.get("point_completed")) is True
            or int(point.get("sample_count") or 0) > 0
            or point.get("pressure_gauge_hpa_before_ready") not in (None, "")
            or point.get("pressure_gauge_hpa_before_sample") not in (None, "")
            for point in point_results
        )
    )
    route_conditioning_fail_closed = bool(
        pressure_points_completed == 0
        and (
            route_conditioning_pressure_overlimit
            or route_conditioning_vent_gap_exceeded
            or route_conditioning_fast_vent_timeout
            or route_conditioning_fast_vent_not_supported
            or route_conditioning_diagnostic_blocked
            or route_open_transition_blocked
            or route_open_settle_wait_blocked
            or route_conditioning_vent_command_failed
            or unsafe_flush_action_seen
            or unsafe_vent_command_sent
            or vent_blocked_after_flush
        )
    )
    pressure_gate_reached = bool(pressure_gate_evidence_present and not route_conditioning_fail_closed)
    pressure_gate_not_reached_reason = ""
    if not pressure_gate_reached:
        pressure_gate_not_reached_reason = "route_conditioning_fail_closed" if route_conditioning_fail_closed else ""
    pressure_gate_source_required = "v1_aligned" if a2_4_probe_required else ""
    pressure_gate_source_alignment_reasons: list[str] = []
    pressure_gate_source_alignment_ready: Optional[bool] = None
    if a2_4_probe_required:
        if pressure_gate_reached:
            pressure_gate_source_alignment_ready = _pressure_gate_source_is_v1_aligned(pressure_gate_source_observed)
            if not pressure_gate_source_alignment_ready:
                pressure_gate_source_alignment_reasons.append("pressure_gate_source_not_v1_aligned")
                rejection_reasons.append("a2_pressure_gate_source_not_v1_aligned")
        else:
            pressure_gate_source_alignment_ready = False
    if a2_4_temperature_skip_requested and not temperature_stabilization_wait_skipped:
        rejection_reasons.append("a2_4_temperature_stabilization_wait_not_skipped")
    if a2_4_temperature_skip_requested and temperature_gate_mode != "current_pv_engineering_probe":
        rejection_reasons.append("a2_4_temperature_gate_mode_not_current_pv_engineering_probe")
    if a2_4_temperature_skip_requested and not temperature_not_part_of_acceptance:
        rejection_reasons.append("a2_4_temperature_not_part_of_acceptance_missing")

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
        not a2_4_temperature_skip_requested or temperature_stabilization_wait_skipped,
        not a2_4_temperature_skip_requested or temperature_gate_mode == "current_pv_engineering_probe",
        not a2_4_temperature_skip_requested or temperature_not_part_of_acceptance,
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
            "pressure_source_selected": point.get("pressure_source_selected"),
            "pressure_source_selection_reason": point.get("pressure_source_selection_reason"),
            "selected_pressure_source": point.get("selected_pressure_source"),
            "selected_pressure_sample_age_s": point.get("selected_pressure_sample_age_s"),
            "selected_pressure_sample_is_stale": point.get("selected_pressure_sample_is_stale"),
            "selected_pressure_parse_ok": point.get("selected_pressure_parse_ok"),
            "selected_pressure_freshness_ok": point.get("selected_pressure_freshness_ok"),
            "pressure_freshness_decision_source": point.get("pressure_freshness_decision_source"),
            "selected_pressure_fail_closed_reason": point.get("selected_pressure_fail_closed_reason"),
            "continuous_stream_stale": point.get("continuous_stream_stale"),
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
        "a2_3_v1_pressure_gauge_read_policy_present": A2_3_V1_PRESSURE_GAUGE_READ_POLICY_PRESENT,
        "a2_4_v1_pressure_gauge_read_policy_present": A2_4_V1_PRESSURE_GAUGE_READ_POLICY_PRESENT,
        "a2_3_pressure_source_strategy": a2_3_strategy,
        "a2_4_pressure_source_strategy": a2_3_strategy,
        "temperature_stabilization_wait_skipped": temperature_stabilization_wait_skipped,
        "temperature_gate_mode": temperature_gate_mode,
        "temperature_not_part_of_acceptance": temperature_not_part_of_acceptance,
        "pressure_source_selected": _first_metric_from_rows(evidence_metric_rows, "pressure_source_selected"),
        "pressure_source_selection_reason": _first_metric_from_rows(
            evidence_metric_rows,
            "pressure_source_selection_reason",
            "source_selection_reason",
        ),
        "continuous_stream_stale": _as_bool(metric_or_summary("continuous_stream_stale")) is True,
        "selected_pressure_source": metric_or_summary("selected_pressure_source"),
        "selected_pressure_source_for_conditioning_monitor": metric_or_summary(
            "selected_pressure_source_for_conditioning_monitor"
        )
        or "",
        "selected_pressure_source_for_pressure_gate": pressure_gate_source_observed,
        "conditioning_monitor_pressure_source_allowed": conditioning_monitor_pressure_source_allowed,
        "pressure_gate_reached": pressure_gate_reached,
        "pressure_gate_not_reached_reason": pressure_gate_not_reached_reason,
        "pressure_gate_source_required": pressure_gate_source_required,
        "pressure_gate_source_observed": pressure_gate_source_observed,
        "pressure_gate_source_alignment_ready": pressure_gate_source_alignment_ready,
        "pressure_gate_source_alignment_reasons": pressure_gate_source_alignment_reasons,
        "a2_conditioning_pressure_source_strategy": metric_or_summary(
            "a2_conditioning_pressure_source_strategy",
            "a2_conditioning_pressure_source",
        )
        or a2_3_strategy,
        "selected_pressure_sample_age_s": metric_or_summary("selected_pressure_sample_age_s"),
        "selected_pressure_sample_is_stale": _as_bool(
            metric_or_summary("selected_pressure_sample_is_stale")
        )
        is True,
        "selected_pressure_parse_ok": _as_bool(metric_or_summary("selected_pressure_parse_ok")) is True,
        "selected_pressure_freshness_ok": _as_bool(
            metric_or_summary("selected_pressure_freshness_ok")
        )
        is True,
        "pressure_freshness_decision_source": metric_or_summary("pressure_freshness_decision_source"),
        "selected_pressure_fail_closed_reason": metric_or_summary("selected_pressure_fail_closed_reason") or "",
        "selected_pressure_sample_stale_duration_ms": metric_or_summary(
            "selected_pressure_sample_stale_duration_ms"
        ),
        "selected_pressure_sample_stale_budget_ms": metric_or_summary("selected_pressure_sample_stale_budget_ms"),
        "selected_pressure_sample_stale_budget_exceeded": _as_bool(
            metric_or_summary("selected_pressure_sample_stale_budget_exceeded")
        )
        is True,
        "selected_pressure_sample_stale_performed_io": _as_bool(
            metric_or_summary("selected_pressure_sample_stale_performed_io")
        )
        is True,
        "selected_pressure_sample_stale_triggered_source_selection": _as_bool(
            metric_or_summary("selected_pressure_sample_stale_triggered_source_selection")
        )
        is True,
        "selected_pressure_sample_stale_triggered_p3_fallback": _as_bool(
            metric_or_summary("selected_pressure_sample_stale_triggered_p3_fallback")
        )
        is True,
        "selected_pressure_sample_stale_deferred_for_vent_priority": _as_bool(
            metric_or_summary("selected_pressure_sample_stale_deferred_for_vent_priority")
        )
        is True,
        "conditioning_monitor_latest_frame_age_s": metric_or_summary("conditioning_monitor_latest_frame_age_s"),
        "conditioning_monitor_latest_frame_fresh": _as_bool(
            metric_or_summary("conditioning_monitor_latest_frame_fresh")
        )
        is True,
        "conditioning_monitor_latest_frame_unavailable": _as_bool(
            metric_or_summary("conditioning_monitor_latest_frame_unavailable")
        )
        is True,
        "conditioning_monitor_pressure_deferred_count": int(
            metric_or_summary("conditioning_monitor_pressure_deferred_count") or 0
        ),
        "conditioning_monitor_pressure_deferred_elapsed_ms": metric_or_summary(
            "conditioning_monitor_pressure_deferred_elapsed_ms"
        ),
        "conditioning_monitor_max_defer_ms": metric_or_summary("conditioning_monitor_max_defer_ms"),
        "conditioning_monitor_pressure_stale_timeout": _as_bool(
            metric_or_summary("conditioning_monitor_pressure_stale_timeout")
        )
        is True,
        "conditioning_monitor_pressure_unavailable_fail_closed": _as_bool(
            metric_or_summary("conditioning_monitor_pressure_unavailable_fail_closed")
        )
        is True,
        "continuous_latest_fresh_fast_path_used": _as_bool(
            metric_or_summary("continuous_latest_fresh_fast_path_used")
        )
        is True,
        "continuous_latest_fresh_duration_ms": metric_or_summary("continuous_latest_fresh_duration_ms"),
        "continuous_latest_fresh_lock_acquire_ms": metric_or_summary("continuous_latest_fresh_lock_acquire_ms"),
        "continuous_latest_fresh_lock_timeout": _as_bool(
            metric_or_summary("continuous_latest_fresh_lock_timeout")
        )
        is True,
        "continuous_latest_fresh_waited_for_frame": _as_bool(
            metric_or_summary("continuous_latest_fresh_waited_for_frame")
        )
        is True,
        "continuous_latest_fresh_performed_io": _as_bool(
            metric_or_summary("continuous_latest_fresh_performed_io")
        )
        is True,
        "continuous_latest_fresh_triggered_stream_restart": _as_bool(
            metric_or_summary("continuous_latest_fresh_triggered_stream_restart")
        )
        is True,
        "continuous_latest_fresh_triggered_drain": _as_bool(
            metric_or_summary("continuous_latest_fresh_triggered_drain")
        )
        is True,
        "continuous_latest_fresh_triggered_p3_fallback": _as_bool(
            metric_or_summary("continuous_latest_fresh_triggered_p3_fallback")
        )
        is True,
        "continuous_latest_fresh_budget_ms": metric_or_summary("continuous_latest_fresh_budget_ms"),
        "continuous_latest_fresh_budget_exceeded": _as_bool(
            metric_or_summary("continuous_latest_fresh_budget_exceeded")
        )
        is True,
        "critical_window_uses_latest_frame": _as_bool(
            _first_metric_from_rows(evidence_metric_rows, "critical_window_uses_latest_frame")
        )
        is True,
        "critical_window_uses_query": _as_bool(
            _first_metric_from_rows(evidence_metric_rows, "critical_window_uses_query")
        )
        is True,
        "p3_fast_fallback_attempted": _as_bool(
            _first_metric_from_rows(evidence_metric_rows, "p3_fast_fallback_attempted")
        )
        is True,
        "p3_fast_fallback_result": _first_metric_from_rows(evidence_metric_rows, "p3_fast_fallback_result") or "",
        "normal_p3_fallback_attempted": _as_bool(
            _first_metric_from_rows(evidence_metric_rows, "normal_p3_fallback_attempted")
        )
        is True,
        "normal_p3_fallback_result": _first_metric_from_rows(evidence_metric_rows, "normal_p3_fallback_result") or "",
        "digital_gauge_stream_stale": _as_bool(
            _first_metric_from_rows(evidence_metric_rows, "digital_gauge_stream_stale")
        )
        is True,
        "continuous_restart_attempted": _as_bool(
            _first_metric_from_rows(evidence_metric_rows, "continuous_restart_attempted")
        )
        is True,
        "continuous_restart_result": _first_metric_from_rows(evidence_metric_rows, "continuous_restart_result") or "",
        "route_conditioning_vent_maintenance_active": _as_bool(
            metric_or_summary("route_conditioning_vent_maintenance_active")
        )
        is True,
        "route_conditioning_phase": metric_or_summary("route_conditioning_phase") or "",
        "ready_to_seal_phase_started": _as_bool(metric_or_summary("ready_to_seal_phase_started")) is True,
        "route_conditioning_flush_min_time_completed": _as_bool(
            metric_or_summary("route_conditioning_flush_min_time_completed")
        )
        is True,
        "vent_maintenance_started_at": metric_or_summary("vent_maintenance_started_at") or "",
        "vent_maintenance_started_monotonic_s": metric_or_summary("vent_maintenance_started_monotonic_s"),
        "pre_route_vent_phase_started": _as_bool(metric_or_summary("pre_route_vent_phase_started")) is True,
        "pre_route_fast_vent_required": (
            metric_or_summary("pre_route_fast_vent_required") is None
            or _as_bool(metric_or_summary("pre_route_fast_vent_required")) is True
        ),
        "pre_route_fast_vent_sent": _as_bool(metric_or_summary("pre_route_fast_vent_sent")) is True,
        "pre_route_fast_vent_duration_ms": metric_or_summary("pre_route_fast_vent_duration_ms"),
        "pre_route_fast_vent_timeout": _as_bool(metric_or_summary("pre_route_fast_vent_timeout")) is True,
        "fast_vent_reassert_supported": _as_bool(metric_or_summary("fast_vent_reassert_supported")) is True,
        "fast_vent_reassert_used": _as_bool(metric_or_summary("fast_vent_reassert_used")) is True,
        "vent_command_write_started_at": metric_or_summary("vent_command_write_started_at") or "",
        "vent_command_write_sent_at": metric_or_summary("vent_command_write_sent_at") or "",
        "vent_command_write_completed_at": metric_or_summary("vent_command_write_completed_at") or "",
        "vent_command_write_duration_ms": metric_or_summary("vent_command_write_duration_ms"),
        "vent_command_total_duration_ms": metric_or_summary("vent_command_total_duration_ms"),
        "vent_command_wait_after_command_s": metric_or_summary("vent_command_wait_after_command_s"),
        "vent_command_capture_pressure_enabled": _as_bool(
            metric_or_summary("vent_command_capture_pressure_enabled")
        )
        is True,
        "vent_command_query_state_enabled": _as_bool(metric_or_summary("vent_command_query_state_enabled")) is True,
        "vent_command_confirm_transition_enabled": _as_bool(
            metric_or_summary("vent_command_confirm_transition_enabled")
        )
        is True,
        "vent_command_blocking_phase": metric_or_summary("vent_command_blocking_phase") or "",
        "route_conditioning_fast_vent_command_timeout": route_conditioning_fast_vent_timeout,
        "route_conditioning_fast_vent_not_supported": route_conditioning_fast_vent_not_supported,
        "route_conditioning_diagnostic_blocked_vent_scheduler": route_conditioning_diagnostic_blocked,
        "vent_scheduler_priority_mode": _as_bool(metric_or_summary("vent_scheduler_priority_mode")) is True,
        "vent_scheduler_checked_before_diagnostic": _as_bool(
            metric_or_summary("vent_scheduler_checked_before_diagnostic")
        )
        is True,
        "diagnostic_deferred_for_vent_priority": _as_bool(
            metric_or_summary("diagnostic_deferred_for_vent_priority")
        )
        is True,
        "diagnostic_deferred_count": int(metric_or_summary("diagnostic_deferred_count") or 0),
        "diagnostic_budget_ms": metric_or_summary("diagnostic_budget_ms"),
        "diagnostic_budget_exceeded": _as_bool(metric_or_summary("diagnostic_budget_exceeded")) is True,
        "diagnostic_blocking_component": metric_or_summary("diagnostic_blocking_component") or "",
        "diagnostic_blocking_operation": metric_or_summary("diagnostic_blocking_operation") or "",
        "diagnostic_blocking_duration_ms": metric_or_summary("diagnostic_blocking_duration_ms"),
        "pressure_monitor_nonblocking": _as_bool(metric_or_summary("pressure_monitor_nonblocking")) is True,
        "pressure_monitor_deferred_for_vent_priority": _as_bool(
            metric_or_summary("pressure_monitor_deferred_for_vent_priority")
        )
        is True,
        "pressure_monitor_budget_ms": metric_or_summary("pressure_monitor_budget_ms"),
        "pressure_monitor_duration_ms": metric_or_summary("pressure_monitor_duration_ms"),
        "pressure_monitor_blocked_vent_scheduler": _as_bool(
            metric_or_summary("pressure_monitor_blocked_vent_scheduler")
        )
        is True,
        "conditioning_monitor_pressure_deferred": _as_bool(
            metric_or_summary("conditioning_monitor_pressure_deferred")
        )
        is True,
        "trace_write_budget_ms": metric_or_summary("trace_write_budget_ms"),
        "trace_write_duration_ms": metric_or_summary("trace_write_duration_ms"),
        "trace_write_blocked_vent_scheduler": _as_bool(
            metric_or_summary("trace_write_blocked_vent_scheduler")
        )
        is True,
        "trace_write_deferred_for_vent_priority": _as_bool(
            metric_or_summary("trace_write_deferred_for_vent_priority")
        )
        is True,
        "route_open_transition_started": _as_bool(metric_or_summary("route_open_transition_started")) is True,
        "route_open_transition_started_at": metric_or_summary("route_open_transition_started_at") or "",
        "route_open_transition_started_monotonic_s": metric_or_summary(
            "route_open_transition_started_monotonic_s"
        ),
        "route_open_command_write_started_at": metric_or_summary("route_open_command_write_started_at") or "",
        "route_open_command_write_completed_at": metric_or_summary("route_open_command_write_completed_at") or "",
        "route_open_command_write_duration_ms": metric_or_summary("route_open_command_write_duration_ms"),
        "route_open_settle_wait_sliced": _as_bool(metric_or_summary("route_open_settle_wait_sliced")) is True,
        "route_open_settle_wait_slice_count": int(metric_or_summary("route_open_settle_wait_slice_count") or 0),
        "route_open_settle_wait_total_ms": metric_or_summary("route_open_settle_wait_total_ms"),
        "route_open_transition_total_duration_ms": metric_or_summary("route_open_transition_total_duration_ms"),
        "vent_ticks_during_route_open_transition": int(
            metric_or_summary("vent_ticks_during_route_open_transition") or 0
        ),
        "route_open_transition_max_vent_write_gap_ms": metric_or_summary(
            "route_open_transition_max_vent_write_gap_ms"
        ),
        "route_open_transition_terminal_vent_write_age_ms": metric_or_summary(
            "route_open_transition_terminal_vent_write_age_ms"
        ),
        "route_open_transition_blocked_vent_scheduler": route_open_transition_blocked,
        "route_open_settle_wait_blocked_vent_scheduler": route_open_settle_wait_blocked,
        "terminal_vent_write_age_ms_at_gap_gate": metric_or_summary(
            "terminal_vent_write_age_ms_at_gap_gate"
        ),
        "max_vent_pulse_write_gap_ms_including_terminal_gap": (
            max_vent_pulse_write_gap_ms_including_terminal_gap
        ),
        "route_conditioning_vent_gap_exceeded_source": metric_or_summary(
            "route_conditioning_vent_gap_exceeded_source"
        )
        or "",
        "terminal_gap_source": metric_or_summary("terminal_gap_source") or "",
        "terminal_gap_operation": metric_or_summary("terminal_gap_operation") or "",
        "terminal_gap_duration_ms": metric_or_summary("terminal_gap_duration_ms"),
        "terminal_gap_started_at": metric_or_summary("terminal_gap_started_at") or "",
        "terminal_gap_detected_at": metric_or_summary("terminal_gap_detected_at") or "",
        "terminal_gap_stack_marker": metric_or_summary("terminal_gap_stack_marker") or "",
        "defer_returned_to_vent_loop": _as_bool(metric_or_summary("defer_returned_to_vent_loop")) is True,
        "defer_to_next_vent_loop_ms": metric_or_summary("defer_to_next_vent_loop_ms"),
        "vent_tick_after_defer_ms": metric_or_summary("vent_tick_after_defer_ms"),
        "terminal_gap_after_defer": _as_bool(metric_or_summary("terminal_gap_after_defer")) is True,
        "terminal_gap_after_defer_ms": metric_or_summary("terminal_gap_after_defer_ms"),
        "defer_path_no_reschedule": _as_bool(metric_or_summary("defer_path_no_reschedule")) is True,
        "fail_closed_path_started": _as_bool(metric_or_summary("fail_closed_path_started")) is True,
        "fail_closed_path_started_while_route_open": _as_bool(
            metric_or_summary("fail_closed_path_started_while_route_open")
        )
        is True,
        "fail_closed_path_vent_maintenance_required": _as_bool(
            metric_or_summary("fail_closed_path_vent_maintenance_required")
        )
        is True,
        "fail_closed_path_vent_maintenance_active": _as_bool(
            metric_or_summary("fail_closed_path_vent_maintenance_active")
        )
        is True,
        "fail_closed_path_duration_ms": metric_or_summary("fail_closed_path_duration_ms"),
        "fail_closed_path_blocked_vent_scheduler": _as_bool(
            metric_or_summary("fail_closed_path_blocked_vent_scheduler")
        )
        is True,
        "route_open_high_frequency_vent_phase_started": _as_bool(
            metric_or_summary("route_open_high_frequency_vent_phase_started")
        )
        is True,
        "route_open_to_first_vent_ms": metric_or_summary("route_open_to_first_vent_ms"),
        "route_open_to_first_vent_s": metric_or_summary("route_open_to_first_vent_s"),
        "route_open_to_first_vent_write_ms": metric_or_summary("route_open_to_first_vent_write_ms"),
        "route_open_to_first_pressure_read_ms": metric_or_summary("route_open_to_first_pressure_read_ms"),
        "route_open_to_overlimit_ms": metric_or_summary("route_open_to_overlimit_ms"),
        "max_vent_pulse_gap_ms": max_vent_pulse_gap_ms,
        "max_vent_pulse_write_gap_ms": max_vent_pulse_write_gap_ms,
        "max_vent_command_total_duration_ms": metric_or_summary("max_vent_command_total_duration_ms"),
        "max_vent_pulse_gap_limit_ms": max_vent_pulse_gap_limit_ms,
        "vent_scheduler_tick_count": int(metric_or_summary("vent_scheduler_tick_count") or 0),
        "vent_scheduler_loop_gap_ms": metric_or_summary("vent_scheduler_loop_gap_ms") or [],
        "max_vent_scheduler_loop_gap_ms": metric_or_summary("max_vent_scheduler_loop_gap_ms"),
        "vent_pulse_count": int(metric_or_summary("vent_pulse_count") or 0),
        "vent_pulse_interval_ms": metric_or_summary("vent_pulse_interval_ms") or [],
        "pressure_drop_after_vent_hpa": metric_or_summary("pressure_drop_after_vent_hpa") or [],
        "route_conditioning_pressure_before_route_open_hpa": metric_or_summary(
            "route_conditioning_pressure_before_route_open_hpa"
        ),
        "route_conditioning_pressure_after_route_open_hpa": metric_or_summary(
            "route_conditioning_pressure_after_route_open_hpa"
        ),
        "route_conditioning_pressure_rise_rate_hpa_per_s": metric_or_summary(
            "route_conditioning_pressure_rise_rate_hpa_per_s"
        ),
        "route_conditioning_peak_pressure_hpa": metric_or_summary("route_conditioning_peak_pressure_hpa"),
        "route_conditioning_pressure_overlimit": route_conditioning_pressure_overlimit,
        "route_conditioning_vent_gap_exceeded": route_conditioning_vent_gap_exceeded,
        "vent_pulse_blocked_after_flush_phase": vent_blocked_after_flush,
        "vent_pulse_blocked_reason": metric_or_summary("vent_pulse_blocked_reason") or "",
        "attempted_unsafe_vent_after_seal_or_pressure_control": _as_bool(
            metric_or_summary("attempted_unsafe_vent_after_seal_or_pressure_control")
        )
        is True,
        "unsafe_vent_after_seal_or_pressure_control_command_sent": unsafe_vent_command_sent,
        "vent_off_blocked_during_flush": (
            metric_or_summary("vent_off_blocked_during_flush") is None
            or _as_bool(metric_or_summary("vent_off_blocked_during_flush")) is True
        ),
        "seal_blocked_during_flush": (
            metric_or_summary("seal_blocked_during_flush") is None
            or _as_bool(metric_or_summary("seal_blocked_during_flush")) is True
        ),
        "pressure_setpoint_blocked_during_flush": (
            metric_or_summary("pressure_setpoint_blocked_during_flush") is None
            or _as_bool(metric_or_summary("pressure_setpoint_blocked_during_flush")) is True
        ),
        "sample_blocked_during_flush": (
            metric_or_summary("sample_blocked_during_flush") is None
            or _as_bool(metric_or_summary("sample_blocked_during_flush")) is True
        ),
        "a2_pressure_sweep_executed": bool(executed),
        "real_probe_executed": bool(executed),
        "underlying_execution_dir": str(execution.get("execution_run_dir") or ""),
        "execution_error": execution_error,
        "execution_config_path": execution_config_path,
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
        **points_alignment,
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
