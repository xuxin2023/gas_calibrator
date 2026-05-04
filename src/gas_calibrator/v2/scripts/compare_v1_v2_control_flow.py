from __future__ import annotations

import argparse
import copy
import json
import os
import subprocess
import sys
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Optional

from ...config import load_config
from ..config.models import _normalize_sensor_precheck_config
from . import route_trace_diff


PROJECT_ROOT = Path(__file__).resolve().parents[4]
DEFAULT_CONFIG_ROOT = PROJECT_ROOT / "src" / "gas_calibrator" / "v2" / "configs"
DEFAULT_VALIDATION_CONFIG_ROOT = DEFAULT_CONFIG_ROOT / "validation"
DEFAULT_SIMULATED_VALIDATION_CONFIG_ROOT = DEFAULT_VALIDATION_CONFIG_ROOT / "simulated"
DEFAULT_V1_CONFIG = PROJECT_ROOT / "configs" / "default_config.json"
DEFAULT_V2_CONFIG = DEFAULT_CONFIG_ROOT / "smoke_v2_minimal.json"
DEFAULT_REPORT_ROOT = PROJECT_ROOT / "src" / "gas_calibrator" / "v2" / "output" / "v1_v2_compare"
DEFAULT_SKIP0_CO2_ONLY_V2_CONFIG = DEFAULT_VALIDATION_CONFIG_ROOT / "replacement_skip0_co2_only_real.json"
DEFAULT_SKIP0_CO2_ONLY_DIAGNOSTIC_V2_CONFIG = (
    DEFAULT_VALIDATION_CONFIG_ROOT / "replacement_skip0_co2_only_diagnostic_relaxed.json"
)
DEFAULT_SKIP0_V2_CONFIG = DEFAULT_VALIDATION_CONFIG_ROOT / "replacement_skip0_real.json"
DEFAULT_H2O_ONLY_V2_CONFIG = DEFAULT_VALIDATION_CONFIG_ROOT / "replacement_h2o_only_diagnostic.json"
DEFAULT_FULL_ROUTE_SIMULATED_V2_CONFIG = (
    DEFAULT_SIMULATED_VALIDATION_CONFIG_ROOT / "replacement_full_route_simulated.json"
)
DEFAULT_FULL_ROUTE_SIMULATED_DIAGNOSTIC_V2_CONFIG = (
    DEFAULT_SIMULATED_VALIDATION_CONFIG_ROOT / "replacement_full_route_simulated_diagnostic.json"
)
DEFAULT_SKIP0_CO2_ONLY_SIMULATED_V2_CONFIG = (
    DEFAULT_SIMULATED_VALIDATION_CONFIG_ROOT / "replacement_skip0_co2_only_simulated.json"
)
DEFAULT_H2O_ONLY_SIMULATED_V2_CONFIG = (
    DEFAULT_SIMULATED_VALIDATION_CONFIG_ROOT / "replacement_h2o_only_simulated.json"
)
DEFAULT_VALIDATION_PROFILE = "standard"
SKIP0_CO2_ONLY_VALIDATION_PROFILE = "skip0_co2_only_replacement"
SKIP0_CO2_ONLY_DIAGNOSTIC_VALIDATION_PROFILE = "skip0_co2_only_diagnostic_relaxed"
SKIP0_VALIDATION_PROFILE = "skip0_replacement"
H2O_ONLY_VALIDATION_PROFILE = "h2o_only_replacement"
FULL_ROUTE_SIMULATED_VALIDATION_PROFILE = "replacement_full_route_simulated"
FULL_ROUTE_SIMULATED_DIAGNOSTIC_VALIDATION_PROFILE = "replacement_full_route_simulated_diagnostic"
SKIP0_CO2_ONLY_SIMULATED_VALIDATION_PROFILE = "replacement_skip0_co2_only_simulated"
H2O_ONLY_SIMULATED_VALIDATION_PROFILE = "replacement_h2o_only_simulated"
KEY_ACTION_REGISTRY = "gas_calibrator.v2.scripts.route_trace_diff.KEY_ACTION_GROUPS"
REVIEW_STAGE_REGISTRY = "gas_calibrator.v2.scripts.route_trace_diff.REVIEW_STAGE_GROUPS"
SKIP0_CO2_ONLY_REPLACEMENT_BUNDLE_NAME = "skip0_co2_only_replacement_bundle.json"
SKIP0_CO2_ONLY_REPLACEMENT_LATEST_NAME = "skip0_co2_only_replacement_latest.json"
SKIP0_CO2_ONLY_DIAGNOSTIC_BUNDLE_NAME = "skip0_co2_only_diagnostic_relaxed_bundle.json"
SKIP0_CO2_ONLY_DIAGNOSTIC_LATEST_NAME = "skip0_co2_only_diagnostic_relaxed_latest.json"
SKIP0_REPLACEMENT_BUNDLE_NAME = "skip0_replacement_bundle.json"
SKIP0_REPLACEMENT_LATEST_NAME = "skip0_replacement_latest.json"
H2O_ONLY_REPLACEMENT_BUNDLE_NAME = "h2o_only_replacement_bundle.json"
H2O_ONLY_REPLACEMENT_LATEST_NAME = "h2o_only_replacement_latest.json"
FULL_ROUTE_SIMULATED_BUNDLE_NAME = "replacement_full_route_simulated_bundle.json"
FULL_ROUTE_SIMULATED_LATEST_NAME = "replacement_full_route_simulated_latest.json"
FULL_ROUTE_SIMULATED_DIAGNOSTIC_BUNDLE_NAME = "replacement_full_route_simulated_diagnostic_bundle.json"
FULL_ROUTE_SIMULATED_DIAGNOSTIC_LATEST_NAME = "replacement_full_route_simulated_diagnostic_latest.json"
SKIP0_CO2_ONLY_SIMULATED_BUNDLE_NAME = "replacement_skip0_co2_only_simulated_bundle.json"
SKIP0_CO2_ONLY_SIMULATED_LATEST_NAME = "replacement_skip0_co2_only_simulated_latest.json"
H2O_ONLY_SIMULATED_BUNDLE_NAME = "replacement_h2o_only_simulated_bundle.json"
H2O_ONLY_SIMULATED_LATEST_NAME = "replacement_h2o_only_simulated_latest.json"
REQUIRED_COMPARE_ARTIFACT_KEYS = (
    "v1_route_trace",
    "v2_route_trace",
    "route_trace_diff",
    "point_presence_diff",
    "sample_count_diff",
    "control_flow_compare_report_json",
    "control_flow_compare_report_markdown",
)
COMPARE_STATUS_MATCH = "MATCH"
COMPARE_STATUS_MISMATCH = "MISMATCH"
COMPARE_STATUS_NOT_EXECUTED = "NOT_EXECUTED"
COMPARE_STATUS_INVALID_PROFILE_INPUT = "INVALID_PROFILE_INPUT"
EVIDENCE_SOURCE_REAL = "real"
EVIDENCE_SOURCE_SIMULATED = "simulated"
REPLACEMENT_VALIDATION_SCOPE_NARROWED_SKIP0_CO2_ONLY = "narrowed_skip0_co2_only"
REPLACEMENT_VALIDATION_PATH_USABLE = "replacement-validation path usable"
REPLACEMENT_VALIDATION_PATH_NOT_USABLE = "replacement-validation path not usable"
V1_TRACE_SUBPROCESS_POLL_S = 0.2
V1_TRACE_SUBPROCESS_GRACE_S = 5.0
V1_TRACE_SUBPROCESS_TERMINATE_WAIT_S = 3.0
SIMULATED_VALIDATION_PROFILES = {
    FULL_ROUTE_SIMULATED_VALIDATION_PROFILE,
    FULL_ROUTE_SIMULATED_DIAGNOSTIC_VALIDATION_PROFILE,
    SKIP0_CO2_ONLY_SIMULATED_VALIDATION_PROFILE,
    H2O_ONLY_SIMULATED_VALIDATION_PROFILE,
}


def _run_v1_trace_inprocess(argv: list[str]) -> int:
    from . import run_v1_route_trace

    return int(run_v1_route_trace.main(argv))


def create_calibration_service(*args: Any, **kwargs: Any) -> Any:
    from ..entry import create_calibration_service as _impl

    return _impl(*args, **kwargs)


def load_config_bundle(*args: Any, **kwargs: Any) -> Any:
    from ..entry import load_config_bundle as _impl

    return _impl(*args, **kwargs)


def _validation_config_for_profile(validation_profile: str) -> Optional[Path]:
    if validation_profile == FULL_ROUTE_SIMULATED_VALIDATION_PROFILE:
        return DEFAULT_FULL_ROUTE_SIMULATED_V2_CONFIG
    if validation_profile == FULL_ROUTE_SIMULATED_DIAGNOSTIC_VALIDATION_PROFILE:
        return DEFAULT_FULL_ROUTE_SIMULATED_DIAGNOSTIC_V2_CONFIG
    if validation_profile == SKIP0_CO2_ONLY_SIMULATED_VALIDATION_PROFILE:
        return DEFAULT_SKIP0_CO2_ONLY_SIMULATED_V2_CONFIG
    if validation_profile == H2O_ONLY_SIMULATED_VALIDATION_PROFILE:
        return DEFAULT_H2O_ONLY_SIMULATED_V2_CONFIG
    if validation_profile == SKIP0_CO2_ONLY_VALIDATION_PROFILE:
        return DEFAULT_SKIP0_CO2_ONLY_V2_CONFIG
    if validation_profile == SKIP0_CO2_ONLY_DIAGNOSTIC_VALIDATION_PROFILE:
        return DEFAULT_SKIP0_CO2_ONLY_DIAGNOSTIC_V2_CONFIG
    if validation_profile == SKIP0_VALIDATION_PROFILE:
        return DEFAULT_SKIP0_V2_CONFIG
    if validation_profile == H2O_ONLY_VALIDATION_PROFILE:
        return DEFAULT_H2O_ONLY_V2_CONFIG
    return None


def _validation_bench_context(validation_profile: str) -> dict[str, Any]:
    context = {
        "co2_0ppm_available": False,
        "other_gases_available": True,
        "h2o_route_available": False,
        "humidity_generator_humidity_feedback_valid": False,
        "primary_replacement_route": SKIP0_CO2_ONLY_VALIDATION_PROFILE,
    }
    if validation_profile == FULL_ROUTE_SIMULATED_VALIDATION_PROFILE:
        context.update(
            {
                "co2_0ppm_available": True,
                "other_gases_available": True,
                "h2o_route_available": True,
                "humidity_generator_humidity_feedback_valid": True,
                "validation_role": "simulated_acceptance_like_coverage",
                "target_route": "h2o_then_co2",
                "diagnostic_only": False,
                "acceptance_evidence": False,
            }
        )
        return context
    if validation_profile == FULL_ROUTE_SIMULATED_DIAGNOSTIC_VALIDATION_PROFILE:
        context.update(
            {
                "co2_0ppm_available": True,
                "other_gases_available": True,
                "h2o_route_available": True,
                "humidity_generator_humidity_feedback_valid": True,
                "validation_role": "simulated_diagnostic",
                "target_route": "h2o_then_co2",
                "diagnostic_only": True,
                "acceptance_evidence": False,
            }
        )
        return context
    if validation_profile == SKIP0_CO2_ONLY_SIMULATED_VALIDATION_PROFILE:
        context.update(
            {
                "co2_0ppm_available": True,
                "other_gases_available": True,
                "h2o_route_available": False,
                "humidity_generator_humidity_feedback_valid": True,
                "validation_role": "simulated_acceptance_like_coverage",
                "target_route": "co2",
                "diagnostic_only": False,
                "acceptance_evidence": False,
            }
        )
        return context
    if validation_profile == H2O_ONLY_SIMULATED_VALIDATION_PROFILE:
        context.update(
            {
                "co2_0ppm_available": True,
                "other_gases_available": True,
                "h2o_route_available": True,
                "humidity_generator_humidity_feedback_valid": True,
                "validation_role": "simulated_diagnostic",
                "target_route": "h2o",
                "diagnostic_only": True,
                "acceptance_evidence": False,
            }
        )
        return context
    if validation_profile == SKIP0_CO2_ONLY_VALIDATION_PROFILE:
        context["validation_role"] = "primary"
        context["target_route"] = "co2"
        context["diagnostic_only"] = False
        context["acceptance_evidence"] = True
    elif validation_profile == SKIP0_CO2_ONLY_DIAGNOSTIC_VALIDATION_PROFILE:
        context["validation_role"] = "diagnostic_route_unblock"
        context["target_route"] = "co2"
        context["diagnostic_only"] = True
        context["acceptance_evidence"] = False
    elif validation_profile == H2O_ONLY_VALIDATION_PROFILE:
        context["validation_role"] = "diagnostic"
        context["target_route"] = "h2o"
        context["diagnostic_only"] = True
        context["acceptance_evidence"] = False
    elif validation_profile == SKIP0_VALIDATION_PROFILE:
        context["validation_role"] = "legacy_mixed_route"
        context["target_route"] = "h2o_then_co2_skip0"
        context["diagnostic_only"] = True
        context["acceptance_evidence"] = False
    else:
        context["validation_role"] = "standard_compare"
        context["target_route"] = "mixed"
        context["diagnostic_only"] = False
        context["acceptance_evidence"] = False
    return context


def _resolve_input_path(path_value: str, *, anchor: Path) -> Path:
    candidate = Path(path_value).expanduser()
    if candidate.is_absolute():
        return candidate.resolve()
    if candidate.exists():
        return candidate.resolve()
    return (anchor / candidate).resolve()


def _current_repo_root() -> Path:
    return PROJECT_ROOT


def _resolve_v2_compare_config_path(*, requested_path: Path, validation_profile: str) -> Path:
    dedicated_config = _validation_config_for_profile(validation_profile)
    if dedicated_config is None:
        return requested_path
    try:
        requested_resolved = requested_path.resolve()
        default_resolved = DEFAULT_V2_CONFIG.resolve()
    except Exception:
        return requested_path
    if requested_resolved != default_resolved:
        return requested_path
    try:
        if dedicated_config.exists():
            return dedicated_config.resolve()
    except Exception:
        return requested_path
    return requested_path


def _relocate_repo_path(path_value: Any, *, source_base: Any) -> Any:
    if not isinstance(path_value, str) or not path_value.strip():
        return path_value
    candidate = Path(path_value).expanduser()
    if not candidate.is_absolute():
        return path_value
    source_text = str(source_base or "").strip()
    if not source_text:
        return path_value
    try:
        old_base = Path(source_text).expanduser()
        relative = candidate.relative_to(old_base)
    except Exception:
        return path_value
    return str((_current_repo_root() / relative).resolve())


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run and compare V1/V2 control-flow route traces.")
    parser.add_argument("--v1-config", default=str(DEFAULT_V1_CONFIG), help="Path to V1 config json.")
    parser.add_argument("--v2-config", default=str(DEFAULT_V2_CONFIG), help="Path to V2 config json.")
    parser.add_argument("--temp", type=float, default=None, help="Optional single temperature filter.")
    parser.add_argument(
        "--skip-co2-ppm",
        default=None,
        help="Optional comma-separated CO2 ppm values to skip, for example `0` or `0,400`.",
    )
    parser.add_argument(
        "--replacement-skip0-co2-only",
        action="store_true",
        help="Use the CO2-only replacement-validation preset: route_mode=co2_only and skip_co2_ppm=[0].",
    )
    parser.add_argument(
        "--replacement-skip0-co2-only-diagnostic-relaxed",
        action="store_true",
        help=(
            "Use the CO2-only route-unblock diagnostic preset: route_mode=co2_only, "
            "skip_co2_ppm=[0], and a relaxed sensor-precheck policy. Diagnostic only."
        ),
    )
    parser.add_argument(
        "--replacement-skip0",
        action="store_true",
        help="Use the narrowed replacement-validation preset: skip_co2_ppm=[0].",
    )
    parser.add_argument(
        "--replacement-h2o-only",
        action="store_true",
        help="Use the H2O-only replacement-validation preset and skip all gas-point routes.",
    )
    parser.add_argument(
        "--skip-connect-check",
        action="store_true",
        help="Disable startup connectivity checks in the runtime compare configs.",
    )
    parser.add_argument("--simulation", action="store_true", help="Run the V2 side in simulation mode.")
    parser.add_argument(
        "--report-root",
        default=str(DEFAULT_REPORT_ROOT),
        help="Directory where compare reports will be written.",
    )
    parser.add_argument(
        "--run-name",
        default=None,
        help="Optional fixed compare run name. Defaults to control_flow_compare_YYYYmmdd_HHMMSS.",
    )
    return parser.parse_args(list(argv) if argv is not None else None)


def _parse_skip_co2_ppm(raw: Optional[str]) -> Optional[list[int]]:
    if raw is None:
        return None
    values: set[int] = set()
    for part in str(raw).split(","):
        text = part.strip()
        if not text:
            continue
        values.add(int(text))
    return sorted(values)


def _resolve_validation_profile(
    args: argparse.Namespace,
) -> tuple[str, Optional[list[int]], Optional[str], bool]:
    skip_co2_ppm = _parse_skip_co2_ppm(args.skip_co2_ppm)
    if bool(args.replacement_skip0_co2_only_diagnostic_relaxed):
        if skip_co2_ppm is None:
            skip_co2_ppm = [0]
        elif 0 not in skip_co2_ppm:
            skip_co2_ppm = sorted({*skip_co2_ppm, 0})
        return SKIP0_CO2_ONLY_DIAGNOSTIC_VALIDATION_PROFILE, skip_co2_ppm, "co2_only", False
    if bool(args.replacement_skip0_co2_only):
        if skip_co2_ppm is None:
            skip_co2_ppm = [0]
        elif 0 not in skip_co2_ppm:
            skip_co2_ppm = sorted({*skip_co2_ppm, 0})
        return SKIP0_CO2_ONLY_VALIDATION_PROFILE, skip_co2_ppm, "co2_only", False
    if bool(args.replacement_h2o_only):
        return H2O_ONLY_VALIDATION_PROFILE, skip_co2_ppm, "h2o_only", True
    if bool(args.replacement_skip0):
        if skip_co2_ppm is None:
            skip_co2_ppm = [0]
        elif 0 not in skip_co2_ppm:
            skip_co2_ppm = sorted({*skip_co2_ppm, 0})
        return SKIP0_VALIDATION_PROFILE, skip_co2_ppm, None, False
    return DEFAULT_VALIDATION_PROFILE, skip_co2_ppm, None, False


def _ensure_dict(parent: dict[str, Any], key: str) -> dict[str, Any]:
    value = parent.get(key)
    if isinstance(value, dict):
        return value
    value = {}
    parent[key] = value
    return value


def _apply_runtime_overrides(
    raw_cfg: dict[str, Any],
    *,
    output_dir: Path,
    temp_c: Optional[float],
    skip_co2_ppm: Optional[list[int]],
    route_mode: Optional[str] = None,
    skip_connect_check: bool,
) -> dict[str, Any]:
    runtime_cfg = copy.deepcopy(raw_cfg)
    source_base = runtime_cfg.get("_base_dir")
    runtime_cfg["_base_dir"] = str(PROJECT_ROOT)
    paths = _ensure_dict(runtime_cfg, "paths")
    for key in ("points_excel", "output_dir", "logs_dir"):
        paths[key] = _relocate_repo_path(paths.get(key), source_base=source_base)
    paths["output_dir"] = str(output_dir.resolve())

    storage = runtime_cfg.get("storage")
    if isinstance(storage, dict):
        storage["database"] = _relocate_repo_path(storage.get("database"), source_base=source_base)

    modeling = runtime_cfg.get("modeling")
    if isinstance(modeling, dict):
        data_source = modeling.get("data_source")
        if isinstance(data_source, dict):
            data_source["path"] = _relocate_repo_path(data_source.get("path"), source_base=source_base)
        export = modeling.get("export")
        if isinstance(export, dict):
            export["output_dir"] = _relocate_repo_path(export.get("output_dir"), source_base=source_base)

    if "_user_tuning_path" in runtime_cfg:
        runtime_cfg["_user_tuning_path"] = _relocate_repo_path(
            runtime_cfg.get("_user_tuning_path"),
            source_base=source_base,
        )
    workflow = _ensure_dict(runtime_cfg, "workflow")
    if temp_c is not None:
        workflow["selected_temps_c"] = [float(temp_c)]
    if skip_co2_ppm is not None:
        workflow["skip_co2_ppm"] = list(skip_co2_ppm)
    if route_mode:
        workflow["route_mode"] = str(route_mode).strip().lower()
    if skip_connect_check:
        _ensure_dict(workflow, "startup_connect_check")["enabled"] = False
    return runtime_cfg


def _apply_h2o_only_quick_compare_overrides(
    runtime_cfg: dict[str, Any],
    *,
    runtime_side: str,
) -> dict[str, Any]:
    devices = _ensure_dict(runtime_cfg, "devices")
    pressure_controller = _ensure_dict(devices, "pressure_controller")
    pressure_controller["in_limits_pct"] = 1.0
    pressure_controller["in_limits_time_s"] = 0.2

    workflow = _ensure_dict(runtime_cfg, "workflow")
    workflow["collect_only"] = True
    if str(runtime_side).strip().lower() == "v2":
        workflow["collect_only_fast_path"] = True
    workflow["restore_baseline_on_finish"] = True
    workflow["h2o_carry_forward"] = True
    workflow["route_mode"] = "h2o_only"
    workflow["water_first_all_temps"] = False
    workflow["water_first_temp_gte"] = 100
    _ensure_dict(workflow, "startup_connect_check")["enabled"] = False
    _ensure_dict(workflow, "startup_pressure_precheck")["enabled"] = False
    sensor_precheck = _ensure_dict(workflow, "sensor_precheck")
    sensor_precheck["enabled"] = True
    sensor_precheck["profile"] = "raw_frame_first"
    sensor_precheck["scope"] = "first_analyzer_only"
    sensor_precheck["validation_mode"] = "v1_frame_like"
    sensor_precheck["strict"] = False
    sensor_precheck["min_valid_frames"] = 1
    sensor_precheck["duration_s"] = 4.0
    sensor_precheck["poll_s"] = 0.2
    precheck = _ensure_dict(workflow, "precheck")
    precheck["device_connection"] = False
    precheck["sensor_check"] = False

    humidity_generator_cfg = _ensure_dict(workflow, "humidity_generator")
    humidity_generator_cfg["ensure_run"] = True
    humidity_generator_cfg["tries"] = 1
    humidity_generator_cfg["wait_s"] = 0.2
    humidity_generator_cfg["poll_s"] = 0.05

    pressure_cfg = _ensure_dict(workflow, "pressure")
    pressure_cfg["pressurize_high_hpa"] = 1000.0
    pressure_cfg["pressurize_wait_after_vent_off_s"] = 0.0
    pressure_cfg["pressurize_timeout_s"] = 1.0
    pressure_cfg["post_stable_sample_delay_s"] = 0.0
    pressure_cfg["co2_post_stable_sample_delay_s"] = 0.0
    pressure_cfg["co2_post_h2o_vent_off_wait_s"] = 0.0
    pressure_cfg["vent_time_s"] = 0.0
    pressure_cfg["vent_transition_timeout_s"] = 1.0
    pressure_cfg["continuous_atmosphere_hold"] = True
    pressure_cfg["vent_hold_interval_s"] = 0.1
    pressure_cfg["stabilize_timeout_s"] = 2.0
    pressure_cfg["restabilize_retries"] = 0
    pressure_cfg["restabilize_retry_interval_s"] = 0.1

    sampling_cfg = _ensure_dict(workflow, "sampling")
    sampling_cfg["count"] = 1
    sampling_cfg["stable_count"] = 1
    sampling_cfg["interval_s"] = 0.1
    sampling_cfg["h2o_interval_s"] = 0.1
    sampling_cfg["co2_interval_s"] = 0.1
    _ensure_dict(sampling_cfg, "quality")["enabled"] = False

    stability_cfg = _ensure_dict(workflow, "stability")

    temperature_cfg = _ensure_dict(stability_cfg, "temperature")
    temperature_cfg["tol"] = 1.0
    temperature_cfg["window_s"] = 0.5
    temperature_cfg["soak_after_reach_s"] = 0.0
    temperature_cfg["timeout_s"] = 2.0
    temperature_cfg["precondition_next_group_enabled"] = False
    temperature_cfg["analyzer_chamber_temp_enabled"] = False
    temperature_cfg["analyzer_chamber_temp_window_s"] = 0.5
    temperature_cfg["analyzer_chamber_temp_span_c"] = 2.0
    temperature_cfg["analyzer_chamber_temp_timeout_s"] = 2.0
    temperature_cfg["analyzer_chamber_temp_first_valid_timeout_s"] = 0.5
    temperature_cfg["analyzer_chamber_temp_poll_s"] = 0.1

    humidity_stability_cfg = _ensure_dict(stability_cfg, "humidity_generator")
    humidity_stability_cfg["enabled"] = True
    humidity_stability_cfg["temp_tol_c"] = 2.0
    humidity_stability_cfg["rh_tol_pct"] = 8.0
    humidity_stability_cfg["rh_stable_window_s"] = 0.5
    humidity_stability_cfg["rh_stable_span_pct"] = 2.0
    humidity_stability_cfg["precondition_next_group_enabled"] = False
    humidity_stability_cfg["reach_confirm_count"] = 1
    humidity_stability_cfg["window_s"] = 0.5
    humidity_stability_cfg["timeout_s"] = 2.0
    humidity_stability_cfg["dewpoint_timeout_s"] = 2.0
    humidity_stability_cfg["poll_s"] = 0.1

    _ensure_dict(stability_cfg, "h2o_route")["preseal_soak_s"] = 0.1
    _ensure_dict(stability_cfg, "h2o_route")["humidity_timeout_policy"] = "abort_like_v1"
    co2_route_cfg = _ensure_dict(stability_cfg, "co2_route")
    co2_route_cfg["preseal_soak_s"] = 0.1
    co2_route_cfg["post_h2o_zero_ppm_soak_s"] = 0.1

    sensor_stability_cfg = _ensure_dict(stability_cfg, "sensor")
    sensor_stability_cfg["enabled"] = True
    sensor_stability_cfg["co2_tol"] = 1_000_000.0
    sensor_stability_cfg["h2o_tol"] = 1_000_000.0
    sensor_stability_cfg["read_interval_s"] = 0.1
    sensor_stability_cfg["window_s"] = 0.5
    sensor_stability_cfg["timeout_s"] = 2.0
    sensor_stability_cfg["poll_s"] = 0.1

    dewpoint_cfg = _ensure_dict(stability_cfg, "dewpoint")
    dewpoint_cfg["target_tol_c"] = 0.2
    dewpoint_cfg["temp_match_tol_c"] = 0.55
    dewpoint_cfg["rh_match_tol_pct"] = 5.5
    dewpoint_cfg["window_s"] = 0.5
    dewpoint_cfg["stability_tol_c"] = 1.0
    dewpoint_cfg["timeout_s"] = 2.0
    dewpoint_cfg["poll_s"] = 0.1
    dewpoint_cfg["min_samples"] = 1

    coefficients_cfg = _ensure_dict(runtime_cfg, "coefficients")
    coefficients_cfg["fit_h2o"] = False
    coefficients_cfg["min_samples"] = 0
    return runtime_cfg


def _apply_skip0_co2_only_compare_overrides(runtime_cfg: dict[str, Any]) -> dict[str, Any]:
    workflow = _ensure_dict(runtime_cfg, "workflow")
    workflow["collect_only"] = True
    workflow["collect_only_fast_path"] = False
    workflow["route_mode"] = "co2_only"
    workflow["skip_co2_ppm"] = [0]
    workflow["water_first_all_temps"] = False
    workflow["water_first_temp_gte"] = 100
    _ensure_dict(workflow, "humidity_generator")["ensure_run"] = False
    sensor_precheck = _ensure_dict(workflow, "sensor_precheck")
    sensor_precheck["enabled"] = True
    sensor_precheck["profile"] = "raw_frame_first"
    sensor_precheck["scope"] = "first_analyzer_only"
    sensor_precheck["validation_mode"] = "v1_frame_like"
    sensor_precheck["strict"] = True
    precheck = _ensure_dict(workflow, "precheck")
    precheck["sensor_check"] = False

    stability = _ensure_dict(workflow, "stability")
    _ensure_dict(stability, "humidity_generator")["enabled"] = False
    _ensure_dict(stability, "co2_route")["post_h2o_zero_ppm_soak_s"] = 0

    devices = _ensure_dict(runtime_cfg, "devices")
    _ensure_dict(devices, "humidity_generator")["enabled"] = False
    _ensure_dict(devices, "dewpoint_meter")["enabled"] = False
    return runtime_cfg


def _apply_skip0_co2_only_diagnostic_relaxed_compare_overrides(runtime_cfg: dict[str, Any]) -> dict[str, Any]:
    runtime_cfg = _apply_skip0_co2_only_compare_overrides(runtime_cfg)
    workflow = _ensure_dict(runtime_cfg, "workflow")
    sensor_precheck = _ensure_dict(workflow, "sensor_precheck")
    sensor_precheck["active_send"] = False
    sensor_precheck["strict"] = False
    sensor_precheck["min_valid_frames"] = 1
    sensor_precheck["duration_s"] = 4.0
    sensor_precheck["poll_s"] = 0.2
    return runtime_cfg


def _requested_temps_from_runtime_cfg(runtime_cfg: dict[str, Any]) -> list[float]:
    workflow = runtime_cfg.get("workflow")
    if not isinstance(workflow, dict):
        return []
    raw = workflow.get("selected_temps_c", [])
    if not isinstance(raw, list):
        raw = [raw]
    out: list[float] = []
    for value in raw:
        try:
            out.append(float(value))
        except Exception:
            continue
    return out


def _skip_co2_list_from_runtime_cfg(runtime_cfg: dict[str, Any]) -> list[int]:
    workflow = runtime_cfg.get("workflow")
    if not isinstance(workflow, dict):
        return []
    raw = workflow.get("skip_co2_ppm", [])
    if not isinstance(raw, list):
        raw = [raw]
    out: list[int] = []
    for value in raw:
        try:
            out.append(int(value))
        except Exception:
            continue
    return sorted(set(out))


def _target_route_for_compare(*, validation_profile: str, route_mode: Any) -> Optional[str]:
    route_mode_text = str(route_mode or "").strip().lower()
    if validation_profile in {
        SKIP0_CO2_ONLY_VALIDATION_PROFILE,
        SKIP0_CO2_ONLY_DIAGNOSTIC_VALIDATION_PROFILE,
    }:
        return "co2"
    if route_mode_text == "h2o_only" or validation_profile == H2O_ONLY_VALIDATION_PROFILE:
        return "h2o"
    if route_mode_text == "co2_only":
        return "co2"
    return None


def _matches_requested_temp(point_temp: float, requested_temps: list[float]) -> bool:
    if not requested_temps:
        return True
    for requested in requested_temps:
        try:
            if abs(float(point_temp) - float(requested)) <= 1e-6:
                return True
        except Exception:
            continue
    return False


def _collect_filtered_points(
    points: list[Any],
    *,
    requested_temps: list[float],
    target_route: Optional[str],
    skip_co2_ppm: list[int],
) -> list[Any]:
    filtered: list[Any] = []
    skip_set = set(skip_co2_ppm)
    target_route_text = str(target_route or "").strip().lower()
    for point in points:
        point_route = str(getattr(point, "route", "") or "").strip().lower()
        point_temp = float(getattr(point, "temperature_c", 0.0))
        if target_route_text and point_route != target_route_text:
            continue
        if not _matches_requested_temp(point_temp, requested_temps):
            continue
        point_co2 = getattr(point, "co2_ppm", None)
        try:
            normalized_co2 = None if point_co2 is None else int(round(float(point_co2)))
        except Exception:
            normalized_co2 = None
        if normalized_co2 is not None and normalized_co2 in skip_set:
            continue
        filtered.append(point)
    return filtered


def _preflight_points_input(
    runtime_cfg: dict[str, Any],
    *,
    side: str,
    validation_profile: str,
) -> dict[str, Any]:
    from ..core.point_parser import PointParser

    workflow = runtime_cfg.get("workflow") if isinstance(runtime_cfg, dict) else {}
    paths = runtime_cfg.get("paths") if isinstance(runtime_cfg, dict) else {}
    points_path_text = ""
    if isinstance(paths, dict):
        points_path_text = str(paths.get("points_excel") or "").strip()
    route_mode = workflow.get("route_mode") if isinstance(workflow, dict) else None
    requested_temps = _requested_temps_from_runtime_cfg(runtime_cfg)
    skip_co2_ppm = _skip_co2_list_from_runtime_cfg(runtime_cfg)
    target_route = _target_route_for_compare(
        validation_profile=validation_profile,
        route_mode=route_mode,
    )
    summary: dict[str, Any] = {
        "side": side,
        "status": "ok",
        "ok": True,
        "points_path": points_path_text,
        "target_route": target_route,
        "requested_temps": requested_temps,
        "skip_co2_ppm": skip_co2_ppm,
        "available_temps": [],
        "available_target_route_temps": [],
        "filtered_count": 0,
        "total_count": 0,
        "reason": "",
    }
    if not points_path_text:
        summary["status"] = COMPARE_STATUS_INVALID_PROFILE_INPUT
        summary["ok"] = False
        summary["reason"] = "points_excel is not configured"
        return summary

    points_path = _resolve_input_path(points_path_text, anchor=PROJECT_ROOT)
    summary["points_path"] = str(points_path)
    parser = PointParser()
    try:
        points = parser.parse(points_path)
    except Exception as exc:
        summary["status"] = COMPARE_STATUS_INVALID_PROFILE_INPUT
        summary["ok"] = False
        summary["reason"] = f"failed to parse points source: {exc}"
        return summary

    summary["total_count"] = len(points)
    all_temps = sorted({float(getattr(point, "temperature_c", 0.0)) for point in points})
    target_route_points = [
        point
        for point in points
        if not target_route or str(getattr(point, "route", "") or "").strip().lower() == target_route
    ]
    summary["available_temps"] = all_temps
    summary["available_target_route_temps"] = sorted(
        {float(getattr(point, "temperature_c", 0.0)) for point in target_route_points}
    )
    filtered_points = _collect_filtered_points(
        points,
        requested_temps=requested_temps,
        target_route=target_route,
        skip_co2_ppm=skip_co2_ppm,
    )
    summary["filtered_count"] = len(filtered_points)
    if filtered_points:
        return summary

    reason_parts = [
        f"points_path={summary['points_path']}",
        f"available_temps={summary['available_temps']}",
        f"requested_temps={requested_temps}",
        f"filtered_count={summary['filtered_count']}",
    ]
    if target_route:
        reason_parts.insert(2, f"target_route={target_route}")
        reason_parts.insert(3, f"available_target_route_temps={summary['available_target_route_temps']}")
    if skip_co2_ppm:
        reason_parts.append(f"skip_co2_ppm={skip_co2_ppm}")
    summary["status"] = COMPARE_STATUS_INVALID_PROFILE_INPUT
    summary["ok"] = False
    summary["reason"] = "filtered point set is empty: " + ", ".join(reason_parts)
    return summary


def _build_preflight_summary(
    *,
    v1_runtime_cfg: dict[str, Any],
    v2_runtime_cfg: dict[str, Any],
    validation_profile: str,
) -> dict[str, Any]:
    sides = {
        "v1": _preflight_points_input(v1_runtime_cfg, side="v1", validation_profile=validation_profile),
        "v2": _preflight_points_input(v2_runtime_cfg, side="v2", validation_profile=validation_profile),
    }
    failing = [payload for payload in sides.values() if not bool(payload.get("ok", False))]
    return {
        "ok": not failing,
        "status": "ok" if not failing else COMPARE_STATUS_INVALID_PROFILE_INPUT,
        "sides": sides,
        "reason": "" if not failing else "; ".join(str(item.get("reason") or "") for item in failing),
    }


def _build_skipped_run_summary(
    *,
    runtime_cfg_path: Path,
    status_error: str,
    status_phase: str,
    error_category: Optional[str] = None,
) -> dict[str, Any]:
    runtime_cfg = json.loads(runtime_cfg_path.read_text(encoding="utf-8"))
    run_dir = Path(runtime_cfg.get("paths", {}).get("output_dir", ""))
    trace_path = run_dir / "route_trace.jsonl"
    return {
        "ok": False,
        "exit_code": 1,
        "run_id": "",
        "run_dir": str(run_dir),
        "trace_path": str(trace_path),
        "runtime_config_path": str(runtime_cfg_path),
        "status_phase": status_phase,
        "status_error": str(status_error),
        "error_category": error_category or _classify_status_error(status_error) or status_phase,
        "derived_failure_phase": None,
        "last_runner_stage": None,
        "last_runner_event": None,
        "abort_message": None,
        "trace_expected_but_missing": trace_path.exists() is False,
        "result_count": 0,
    }


def _runtime_policy_summary(
    runtime_cfg: dict[str, Any],
    *,
    effective_compare_config: Optional[Path] = None,
    bench_context: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    workflow = runtime_cfg.get("workflow") if isinstance(runtime_cfg, dict) else {}
    stability = workflow.get("stability") if isinstance(workflow, dict) else {}
    h2o_route = stability.get("h2o_route") if isinstance(stability, dict) else {}
    precheck = workflow.get("precheck") if isinstance(workflow, dict) else {}
    sensor_precheck = workflow.get("sensor_precheck") if isinstance(workflow, dict) else {}
    devices = runtime_cfg.get("devices") if isinstance(runtime_cfg, dict) else {}
    normalized_sensor_precheck = _normalize_sensor_precheck_config(sensor_precheck if isinstance(sensor_precheck, dict) else {})
    return {
        "route_mode": workflow.get("route_mode") if isinstance(workflow, dict) else None,
        "collect_only": bool(workflow.get("collect_only", False)) if isinstance(workflow, dict) else False,
        "collect_only_fast_path": bool(workflow.get("collect_only_fast_path", False))
        if isinstance(workflow, dict)
        else False,
        "precheck_device_connection": bool(precheck.get("device_connection", True))
        if isinstance(precheck, dict)
        else True,
        "precheck_sensor_check": bool(precheck.get("sensor_check", True))
        if isinstance(precheck, dict)
        else True,
        "sensor_precheck_enabled": bool(sensor_precheck.get("enabled", False))
        if isinstance(sensor_precheck, dict)
        else False,
        "sensor_precheck_profile": str(normalized_sensor_precheck.get("profile") or "snapshot"),
        "sensor_precheck_scope": str(normalized_sensor_precheck.get("scope") or "all_analyzers"),
        "sensor_precheck_validation_mode": str(normalized_sensor_precheck.get("validation_mode") or "snapshot"),
        "sensor_precheck_active_send": bool(normalized_sensor_precheck.get("active_send", True)),
        "sensor_precheck_strict": bool(normalized_sensor_precheck.get("strict", True)),
        "expected_disabled_devices": sorted(
            name
            for name, payload in (devices.items() if isinstance(devices, dict) else [])
            if isinstance(payload, dict) and not bool(payload.get("enabled", True))
        ),
        "h2o_humidity_timeout_policy": (
            str(h2o_route.get("humidity_timeout_policy") or "").strip().lower() or "abort_like_v1"
        )
        if isinstance(h2o_route, dict)
        else "abort_like_v1",
        "effective_v2_compare_config": str(effective_compare_config) if effective_compare_config is not None else None,
        "bench_context": dict(bench_context or {}),
    }


def _write_json(path: Path, payload: Any) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return path


def _load_json_payload(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        loaded = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return loaded if isinstance(loaded, dict) else {}


def _derive_v1_status_from_run_dir(run_dir: Path) -> dict[str, Any]:
    try:
        from .run_v1_route_trace import _derive_runner_failure
    except Exception:
        return {}
    try:
        derived = _derive_runner_failure(run_dir)
    except Exception:
        return {}
    if not isinstance(derived, dict):
        return {}
    payload = dict(derived)
    derived_phase = str(payload.get("derived_failure_phase") or "").strip()
    if derived_phase and not payload.get("status_phase"):
        payload["status_phase"] = "output.route_trace_missing"
    if derived_phase and not payload.get("error_category"):
        payload["error_category"] = derived_phase
    if payload.get("abort_message") and not payload.get("status_error"):
        payload["status_error"] = payload.get("abort_message")
    return payload


def _write_text(path: Path, content: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(str(content), encoding="utf-8")
    return path


def _print_effective_paths(label: str, runtime_cfg: dict[str, Any], runtime_cfg_path: Path) -> None:
    paths = runtime_cfg.get("paths", {}) if isinstance(runtime_cfg, dict) else {}
    points_excel = paths.get("points_excel", "")
    output_dir = paths.get("output_dir", "")
    logs_dir = paths.get("logs_dir", "")
    print(f"{label} points_excel: {points_excel}")
    print(f"{label} output_dir: {output_dir}")
    if logs_dir:
        print(f"{label} logs_dir: {logs_dir}")
    print(f"{label} runtime config: {runtime_cfg_path}")


def _load_trace_records(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    records: list[dict[str, Any]] = []
    for raw_line in path.read_text(encoding="utf-8-sig").splitlines():
        line = raw_line.strip()
        if not line:
            continue
        payload = json.loads(line)
        if isinstance(payload, dict):
            records.append(payload)
    return records


def _sample_count_from_record(record: dict[str, Any]) -> Optional[int]:
    actual = record.get("actual")
    target = record.get("target")
    for source in (actual, target):
        if not isinstance(source, dict):
            continue
        value = source.get("sample_count")
        try:
            if value is not None:
                return int(value)
        except Exception:
            continue
    return None


def _extract_sample_end_map(records: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    samples: dict[str, dict[str, Any]] = {}
    for record in records:
        if str(record.get("action") or "").strip() != "sample_end":
            continue
        point_tag = str(record.get("point_tag") or "").strip()
        if not point_tag:
            continue
        samples[point_tag] = {
            "point_tag": point_tag,
            "route": str(record.get("route") or "").strip().lower(),
            "point_index": record.get("point_index"),
            "result": str(record.get("result") or "ok").strip().lower() or "ok",
            "sample_count": _sample_count_from_record(record),
        }
    return samples


def _group_presence(sample_map: dict[str, dict[str, Any]], route: str = "") -> set[str]:
    tags: set[str] = set()
    for point_tag, payload in sample_map.items():
        if route and str(payload.get("route") or "") != route:
            continue
        if str(payload.get("result") or "") == "ok":
            tags.add(point_tag)
    return tags


def summarize_presence(
    v1_records: list[dict[str, Any]],
    v2_records: list[dict[str, Any]],
) -> dict[str, Any]:
    v1_samples = _extract_sample_end_map(v1_records)
    v2_samples = _extract_sample_end_map(v2_records)
    routes = sorted(
        {
            str(payload.get("route") or "").strip().lower()
            for payload in list(v1_samples.values()) + list(v2_samples.values())
            if str(payload.get("route") or "").strip()
        }
    )
    route_summaries: dict[str, Any] = {}
    for route in routes:
        v1_tags = _group_presence(v1_samples, route=route)
        v2_tags = _group_presence(v2_samples, route=route)
        route_summaries[route] = {
            "matches": v1_tags == v2_tags,
            "v1_only": sorted(v1_tags - v2_tags),
            "v2_only": sorted(v2_tags - v1_tags),
            "v1_count": len(v1_tags),
            "v2_count": len(v2_tags),
        }
    v1_all = _group_presence(v1_samples)
    v2_all = _group_presence(v2_samples)
    return {
        "matches": v1_all == v2_all,
        "v1_only": sorted(v1_all - v2_all),
        "v2_only": sorted(v2_all - v1_all),
        "v1_count": len(v1_all),
        "v2_count": len(v2_all),
        "routes": route_summaries,
    }


def summarize_sample_counts(
    v1_records: list[dict[str, Any]],
    v2_records: list[dict[str, Any]],
) -> dict[str, Any]:
    v1_samples = _extract_sample_end_map(v1_records)
    v2_samples = _extract_sample_end_map(v2_records)
    mismatches: list[dict[str, Any]] = []
    for point_tag in sorted(set(v1_samples) | set(v2_samples)):
        v1_payload = v1_samples.get(point_tag)
        v2_payload = v2_samples.get(point_tag)
        left_count = None if v1_payload is None else v1_payload.get("sample_count")
        right_count = None if v2_payload is None else v2_payload.get("sample_count")
        left_result = "missing" if v1_payload is None else str(v1_payload.get("result") or "missing")
        right_result = "missing" if v2_payload is None else str(v2_payload.get("result") or "missing")
        if left_count == right_count and left_result == right_result:
            continue
        mismatches.append(
            {
                "point_tag": point_tag,
                "route": (
                    str((v1_payload or {}).get("route") or "")
                    or str((v2_payload or {}).get("route") or "")
                ).strip().lower(),
                "v1_sample_count": left_count,
                "v2_sample_count": right_count,
                "v1_result": left_result,
                "v2_result": right_result,
            }
        )
    return {
        "matches": not mismatches,
        "mismatches": mismatches,
    }


def _route_diff_summary_to_dict(summary: route_trace_diff.RouteDiffSummary) -> dict[str, Any]:
    return {
        "route": summary.route,
        "matches": summary.matches,
        "v1_count": summary.v1_count,
        "v2_count": summary.v2_count,
        "missing_in_v2": list(summary.missing_in_v2),
        "extra_in_v2": list(summary.extra_in_v2),
        "order_mismatches": [
            {"index": index, "v1": left, "v2": right}
            for index, left, right in summary.order_mismatches
        ],
        "unified_diff": list(summary.unified_diff),
    }


def summarize_key_action_diffs(
    v1_events: list[route_trace_diff.RouteTraceEvent],
    v2_events: list[route_trace_diff.RouteTraceEvent],
) -> dict[str, Any]:
    report: dict[str, Any] = {}
    for category, actions in route_trace_diff.key_action_groups().items():
        filtered_v1 = [event for event in v1_events if event.action in actions]
        filtered_v2 = [event for event in v2_events if event.action in actions]
        summaries = route_trace_diff.compare_route_traces(filtered_v1, filtered_v2, max_diff_lines=8)
        report[category] = {
            "matches": all(summary.matches for summary in summaries),
            "actions": list(actions),
            "v1_action_counts": dict(sorted(Counter(event.action for event in filtered_v1).items())),
            "v2_action_counts": dict(sorted(Counter(event.action for event in filtered_v2).items())),
            "routes": [_route_diff_summary_to_dict(summary) for summary in summaries],
        }
    return report


def summarize_review_stage_diffs(
    v1_events: list[route_trace_diff.RouteTraceEvent],
    v2_events: list[route_trace_diff.RouteTraceEvent],
) -> dict[str, Any]:
    report: dict[str, Any] = {}
    for stage, actions in route_trace_diff.review_stage_groups().items():
        filtered_v1 = [event for event in v1_events if event.action in actions]
        filtered_v2 = [event for event in v2_events if event.action in actions]
        summaries = route_trace_diff.compare_route_traces(filtered_v1, filtered_v2, max_diff_lines=8)
        report[stage] = {
            "matches": all(summary.matches for summary in summaries),
            "actions": list(actions),
            "v1_action_counts": dict(sorted(Counter(event.action for event in filtered_v1).items())),
            "v2_action_counts": dict(sorted(Counter(event.action for event in filtered_v2).items())),
            "routes": [_route_diff_summary_to_dict(summary) for summary in summaries],
        }
    return report


def _replacement_scope_id(validation_profile: str, skip_co2_ppm: Any) -> str:
    skip_set = set()
    for value in list(skip_co2_ppm or []):
        try:
            skip_set.add(int(value))
        except Exception:
            continue
    if validation_profile in {SKIP0_CO2_ONLY_VALIDATION_PROFILE, SKIP0_CO2_ONLY_SIMULATED_VALIDATION_PROFILE}:
        return REPLACEMENT_VALIDATION_SCOPE_NARROWED_SKIP0_CO2_ONLY
    if validation_profile == SKIP0_CO2_ONLY_DIAGNOSTIC_VALIDATION_PROFILE:
        return "narrowed_skip0_co2_only_diagnostic"
    if validation_profile == SKIP0_VALIDATION_PROFILE or 0 in skip_set:
        return "mixed_route_skip0_review_aid"
    if validation_profile == H2O_ONLY_VALIDATION_PROFILE:
        return "h2o_only_diagnostic"
    return "standard_compare"


def _classify_route_action_difference(
    item: dict[str, Any],
    *,
    target_route: Optional[str],
) -> dict[str, Any]:
    route = str(item.get("route") or "").strip().lower()
    target_route_text = str(target_route or "").strip().lower()
    missing = list(item.get("missing_in_v2") or [])
    extra = list(item.get("extra_in_v2") or [])
    order = list(item.get("order_mismatches") or [])
    difference_types: list[str] = []
    if missing or extra:
        difference_types.append("missing_action")
    if order:
        difference_types.append("ordering_difference")
    expected_divergence = bool(target_route_text and route and route != target_route_text)
    if expected_divergence:
        difference_types.append("expected_divergence")
    return {
        "route": route,
        "missing_in_v2": missing,
        "extra_in_v2": extra,
        "order_mismatches": order,
        "difference_types": difference_types,
        "expected_divergence": expected_divergence,
        "classification": "expected_divergence" if expected_divergence else ",".join(difference_types),
    }


def summarize_replacement_validation(
    *,
    presence: dict[str, Any],
    sample_count: dict[str, Any],
    route_sequence: dict[str, Any],
    key_actions: dict[str, Any],
    compare_status: str,
    valid_for_route_diff: bool,
    validation_profile: str,
    skip_co2_ppm: Any,
    target_route: Optional[str],
    evidence_state: str,
    route_execution_summary: dict[str, Any],
) -> dict[str, Any]:
    route_action_order_differences = [
        _classify_route_action_difference(item, target_route=target_route)
        for item in route_sequence.get("routes") or []
        if (item.get("missing_in_v2") or item.get("extra_in_v2") or item.get("order_mismatches"))
    ]
    blocking_route_action_order_differences = [
        item for item in route_action_order_differences if not bool(item.get("expected_divergence"))
    ]
    evaluable = compare_status not in {
        COMPARE_STATUS_INVALID_PROFILE_INPUT,
        COMPARE_STATUS_NOT_EXECUTED,
    } and bool(valid_for_route_diff)
    presence_matches = bool(presence.get("matches", False)) if evaluable else None
    sample_count_matches = bool(sample_count.get("matches", False)) if evaluable else None
    route_action_order_matches = (not blocking_route_action_order_differences) if evaluable else None
    route_has_failures = bool(route_execution_summary.get("has_route_failures"))
    route_has_physical_mismatches = bool(route_execution_summary.get("has_physical_route_mismatches"))
    path_usable = bool(
        evaluable
        and compare_status == COMPARE_STATUS_MATCH
        and presence_matches is True
        and sample_count_matches is True
        and route_action_order_matches is True
        and not route_has_failures
        and not route_has_physical_mismatches
    )
    missing_in_v1 = list(presence.get("v2_only") or [])
    missing_in_v2 = list(presence.get("v1_only") or [])
    sample_count_mismatches = list(sample_count.get("mismatches") or [])
    return {
        "scope": _replacement_scope_id(validation_profile, skip_co2_ppm),
        "scope_statement": "co2_only + skip_co2_ppm=[0] narrowed replacement-validation path"
        if _replacement_scope_id(validation_profile, skip_co2_ppm)
        == REPLACEMENT_VALIDATION_SCOPE_NARROWED_SKIP0_CO2_ONLY
        else "",
        "conclusion": REPLACEMENT_VALIDATION_PATH_USABLE if path_usable else REPLACEMENT_VALIDATION_PATH_NOT_USABLE,
        "path_usable": path_usable,
        "cutover_ready": False,
        "default_replacement_ready": False,
        "full_equivalence_established": False,
        "numeric_equivalence_established": False,
        "evidence_state": evidence_state,
        "first_failure_phase": route_execution_summary.get("first_failure_phase"),
        "only_in_v1": list(presence.get("v1_only") or []),
        "only_in_v2": list(presence.get("v2_only") or []),
        "missing_points": {
            "missing_in_v1": missing_in_v1,
            "missing_in_v2": missing_in_v2,
        },
        "sample_count_mismatch": None if sample_count_matches is None else not bool(sample_count_matches),
        "sample_count_mismatches": sample_count_mismatches,
        "presence_evaluable": evaluable,
        "sample_count_evaluable": evaluable,
        "route_action_order_evaluable": evaluable,
        "presence_matches": presence_matches,
        "sample_count_matches": sample_count_matches,
        "route_action_order_matches": route_action_order_matches,
        "route_action_order_differences": route_action_order_differences,
        "blocking_route_action_order_differences": blocking_route_action_order_differences,
        "key_action_groups_evaluable": evaluable,
        "key_action_group_matches": {
            category: bool(payload.get("matches", False)) if evaluable else None
            for category, payload in sorted((key_actions or {}).items())
        },
        "key_action_group_registry": KEY_ACTION_REGISTRY,
        "review_stage_registry": REVIEW_STAGE_REGISTRY,
    }


def _count_target_route_events(
    events: list[route_trace_diff.RouteTraceEvent],
    *,
    target_route: Optional[str],
) -> int:
    if not target_route:
        return 0
    target_route_text = str(target_route).strip().lower()
    return sum(1 for event in events if event.route == target_route_text)


def _collect_target_route_failures(
    records: list[dict[str, Any]],
    *,
    target_route: Optional[str],
) -> list[dict[str, Any]]:
    target_route_text = str(target_route or "").strip().lower()
    failures: list[dict[str, Any]] = []
    for record in records:
        result = str(record.get("result") or "ok").strip().lower()
        if result not in {"fail", "error"}:
            continue
        route = str(record.get("route") or "").strip().lower()
        if target_route_text and route != target_route_text:
            continue
        failures.append(
            {
                "route": route,
                "action": str(record.get("action") or "").strip(),
                "point_tag": str(record.get("point_tag") or "").strip(),
                "point_index": record.get("point_index"),
                "result": result,
                "message": str(record.get("message") or "").strip(),
            }
        )
    return failures


def _normalize_open_valves(value: Any) -> list[int]:
    normalized: list[int] = []
    for item in list(value or []):
        try:
            normalized.append(int(item))
        except Exception:
            continue
    return sorted(set(normalized))


def _normalize_relay_state(value: Any) -> dict[str, dict[str, bool]]:
    if not isinstance(value, dict):
        return {}
    normalized: dict[str, dict[str, bool]] = {}
    for relay_name, channels in value.items():
        if not isinstance(channels, dict):
            continue
        channel_map: dict[str, bool] = {}
        for channel, state in channels.items():
            channel_map[str(channel)] = bool(state)
        normalized[str(relay_name)] = channel_map
    return normalized


def _extract_route_physical_state(record: dict[str, Any]) -> Optional[dict[str, Any]]:
    target = record.get("target") if isinstance(record.get("target"), dict) else {}
    actual = record.get("actual") if isinstance(record.get("actual"), dict) else {}
    target_open_valves = _normalize_open_valves(target.get("target_open_valves", target.get("open_valves")))
    actual_open_valves = _normalize_open_valves(actual.get("actual_open_valves"))
    target_relay_state = _normalize_relay_state(target.get("target_relay_state"))
    actual_relay_state = _normalize_relay_state(actual.get("actual_relay_state"))
    if not actual_relay_state:
        actual_relay_state = _normalize_relay_state(record.get("relay_state"))
    route_physical_state_match_raw = actual.get("route_physical_state_match")
    relay_physical_mismatch_raw = actual.get("relay_physical_mismatch")
    route_physical_state_match = (
        None if route_physical_state_match_raw is None else bool(route_physical_state_match_raw)
    )
    relay_physical_mismatch = None if relay_physical_mismatch_raw is None else bool(relay_physical_mismatch_raw)
    mismatched_valves = _normalize_open_valves(actual.get("mismatched_valves"))
    mismatched_channels = list(actual.get("mismatched_channels") or [])
    if not any(
        (
            target_open_valves,
            actual_open_valves,
            target_relay_state,
            actual_relay_state,
            route_physical_state_match is not None,
            relay_physical_mismatch is not None,
            mismatched_valves,
            mismatched_channels,
        )
    ):
        return None
    return {
        "target_open_valves": target_open_valves,
        "actual_open_valves": actual_open_valves,
        "target_relay_state": target_relay_state,
        "actual_relay_state": actual_relay_state,
        "route_physical_state_match": route_physical_state_match,
        "relay_physical_mismatch": relay_physical_mismatch,
        "mismatched_valves": mismatched_valves,
        "mismatched_channels": mismatched_channels,
    }


def _collect_route_physical_failures(
    records: list[dict[str, Any]],
    *,
    target_route: Optional[str],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    target_route_text = str(target_route or "").strip().lower()
    failures: list[dict[str, Any]] = []
    latest_state: dict[str, Any] = {}
    for record in records:
        route = str(record.get("route") or "").strip().lower()
        if target_route_text and route != target_route_text:
            continue
        physical = _extract_route_physical_state(record)
        if physical is None:
            continue
        latest_state = {
            "route": route,
            "action": str(record.get("action") or "").strip(),
            "point_tag": str(record.get("point_tag") or "").strip(),
            "point_index": record.get("point_index"),
            **physical,
        }
        relay_physical_mismatch = bool(physical.get("relay_physical_mismatch"))
        route_match = physical.get("route_physical_state_match")
        if relay_physical_mismatch or route_match is False:
            failures.append(
                {
                    "route": route,
                    "action": str(record.get("action") or "").strip(),
                    "point_tag": str(record.get("point_tag") or "").strip(),
                    "point_index": record.get("point_index"),
                    "message": str(record.get("message") or "").strip(),
                    **physical,
                }
            )
    return failures, latest_state


def _collect_cleanup_relay_state(records: list[dict[str, Any]]) -> dict[str, Any]:
    cleanup_actions = {"restore_baseline", "final_safe_stop_routes", "cleanup", "route_baseline", "safe_stop"}
    latest_state: dict[str, Any] = {}
    for record in records:
        action = str(record.get("action") or "").strip().lower()
        if action not in cleanup_actions:
            continue
        physical = _extract_route_physical_state(record)
        if physical is None:
            relay_state = _normalize_relay_state(record.get("relay_state"))
            if not relay_state:
                continue
            physical = {
                "actual_relay_state": relay_state,
                "target_relay_state": {},
                "target_open_valves": [],
                "actual_open_valves": [],
                "route_physical_state_match": None,
                "relay_physical_mismatch": None,
                "mismatched_valves": [],
                "mismatched_channels": [],
            }
        latest_state = {
            "action": str(record.get("action") or "").strip(),
            **physical,
        }
    actual_relay_state = _normalize_relay_state(latest_state.get("actual_relay_state"))
    flattened = [state for channels in actual_relay_state.values() for state in channels.values()]
    cleanup_all_relays_off = None if not flattened else all(not bool(state) for state in flattened)
    if not latest_state:
        return {}
    latest_state["cleanup_all_relays_off"] = cleanup_all_relays_off
    return latest_state


def _derive_route_failure_phase(failure: dict[str, Any], *, target_route: Optional[str]) -> str:
    route = str(failure.get("route") or target_route or "").strip().lower()
    action = str(failure.get("action") or "").strip().lower()
    prefix = f"route.{route}" if route else "route"
    if action:
        return f"{prefix}.{action}"
    return prefix


def _derive_route_physical_failure_phase(failure: dict[str, Any], *, target_route: Optional[str]) -> str:
    route = str(failure.get("route") or target_route or "").strip().lower()
    action = str(failure.get("action") or "").strip().lower()
    prefix = f"route.{route}" if route else "route"
    if action:
        return f"{prefix}.{action}.relay_physical_mismatch"
    return f"{prefix}.relay_physical_mismatch"


def _normalize_reference_device_status(mode: Any, *, skipped_by_profile: bool = False) -> str:
    if bool(skipped_by_profile):
        return "skipped_by_profile"
    text = str(mode or "").strip().lower()
    if not text or text in {"stable", "plus_200_mode", "continuous_stream", "sample_hold", "unit_switch"}:
        return "healthy"
    if text in {"stale", "drift", "warmup_unstable", "wrong_unit_configuration"}:
        return text
    if text in {
        "no_response",
        "parse_fail",
        "hardware_missing",
        "corrupted_ascii",
        "truncated_ascii",
        "unsupported_command",
        "display_interrupted",
    }:
        return text
    return text


def _reference_quality_summary(metadata: dict[str, Any]) -> dict[str, Any]:
    simulation_context = metadata.get("simulation_context") if isinstance(metadata.get("simulation_context"), dict) else {}
    device_matrix = simulation_context.get("device_matrix") if isinstance(simulation_context, dict) else {}
    if not isinstance(device_matrix, dict) or not device_matrix:
        return {
            "reference_integrity": "not_assessed",
            "reference_quality": "not_assessed",
            "reference_quality_degraded": False,
            "thermometer_reference_status": "not_assessed",
            "pressure_reference_status": "not_assessed",
            "reasons": [],
        }
    thermometer = device_matrix.get("thermometer") if isinstance(device_matrix, dict) else {}
    pressure_gauge = device_matrix.get("pressure_gauge") if isinstance(device_matrix, dict) else {}
    thermometer_status = _normalize_reference_device_status(
        (thermometer or {}).get("mode"),
        skipped_by_profile=bool((thermometer or {}).get("skipped_by_profile", False)),
    )
    pressure_status = _normalize_reference_device_status(
        (pressure_gauge or {}).get("mode"),
        skipped_by_profile=bool((pressure_gauge or {}).get("skipped_by_profile", False)),
    )
    degraded_statuses = {"stale", "drift", "warmup_unstable", "wrong_unit_configuration"}
    failed_statuses = {
        "no_response",
        "parse_fail",
        "hardware_missing",
        "missing",
        "corrupted_ascii",
        "truncated_ascii",
        "unsupported_command",
        "display_interrupted",
    }
    status_values = [thermometer_status, pressure_status]
    if any(value in failed_statuses for value in status_values):
        overall = "failed"
    elif any(value in degraded_statuses for value in status_values):
        overall = "degraded"
    else:
        overall = "healthy"
    degraded = overall != "healthy"
    reasons = [
        reason
        for reason in (
            None if thermometer_status in {"healthy", "skipped_by_profile"} else f"thermometer:{thermometer_status}",
            None if pressure_status in {"healthy", "skipped_by_profile"} else f"pressure:{pressure_status}",
        )
        if reason
    ]
    return {
        "reference_integrity": overall,
        "reference_quality": overall,
        "reference_quality_degraded": degraded,
        "thermometer_reference_status": thermometer_status,
        "pressure_reference_status": pressure_status,
        "reasons": reasons,
    }


def _classify_status_error(status_error: Any) -> Optional[str]:
    text = str(status_error or "").strip().lower()
    if not text:
        return None
    if "port_busy" in text:
        return "startup.device_connection.port_busy"
    if "permission denied" in text or "access denied" in text or "拒绝访问" in text:
        return "startup.device_connection.port_busy"
    if "could not open port" in text or "port is busy" in text:
        return "startup.device_connection.port_busy"
    if "no calibration points loaded" in text:
        return "input_validation.points_filter"
    if "points file" in text and ("does not exist" in text or "unsupported" in text):
        return "input_validation.points_source"
    if "device precheck failed" in text:
        return "precheck.device_connection"
    if "sensor check failed" in text:
        return "precheck.sensor_check"
    if "sensor precheck" in text:
        return "startup.sensor_precheck"
    if "startup connect" in text:
        return "startup_connect_check"
    if "pressure" in text and "precheck" in text:
        return "startup_pressure_precheck"
    return None


def _build_route_execution_summary(
    *,
    target_route: Optional[str],
    metadata: dict[str, Any],
    v1_records: list[dict[str, Any]],
    v2_records: list[dict[str, Any]],
    v1_events: list[route_trace_diff.RouteTraceEvent],
    v2_events: list[route_trace_diff.RouteTraceEvent],
) -> dict[str, Any]:
    preflight = metadata.get("preflight") if isinstance(metadata.get("preflight"), dict) else {}
    preflight_sides = preflight.get("sides") if isinstance(preflight.get("sides"), dict) else {}
    runtime_policies = metadata.get("runtime_policies") if isinstance(metadata.get("runtime_policies"), dict) else {}
    bench_context = metadata.get("bench_context") if isinstance(metadata.get("bench_context"), dict) else {}
    sides: dict[str, Any] = {}
    entered_target_route: dict[str, bool] = {}
    target_route_event_count: dict[str, int] = {}
    route_physical_state_match: dict[str, Optional[bool]] = {}
    relay_physical_mismatch: dict[str, bool] = {}
    first_failure_phase_candidates: list[tuple[str, str]] = []

    for side, events, records in (
        ("v1", v1_events, v1_records),
        ("v2", v2_events, v2_records),
    ):
        side_meta = metadata.get(side) if isinstance(metadata.get(side), dict) else {}
        side_preflight = preflight_sides.get(side) if isinstance(preflight_sides, dict) else {}
        route_count = _count_target_route_events(events, target_route=target_route)
        entered = route_count > 0 if target_route else bool(events)
        entered_target_route[side] = entered
        target_route_event_count[side] = route_count
        route_failures = _collect_target_route_failures(records, target_route=target_route)
        route_physical_failures, latest_physical_state = _collect_route_physical_failures(records, target_route=target_route)
        cleanup_relay_state = _collect_cleanup_relay_state(records)
        physical_state_match = False if route_physical_failures else latest_physical_state.get("route_physical_state_match")
        route_physical_state_match[side] = physical_state_match
        relay_physical_mismatch[side] = bool(route_physical_failures)

        failure_phase = None
        if isinstance(side_preflight, dict) and not bool(side_preflight.get("ok", True)):
            failure_phase = "input_validation.points_filter"
        else:
            derived_failure_phase = str(side_meta.get("derived_failure_phase") or "").strip().lower()
            explicit_error_category = str(side_meta.get("error_category") or "").strip().lower()
            explicit_status_phase = str(side_meta.get("status_phase") or "").strip().lower()
            if explicit_status_phase in {"completed", "complete", "ok"} or (
                bool(side_meta.get("ok", False))
                and not str(side_meta.get("status_error") or "").strip()
                and explicit_status_phase.startswith("simulated.")
            ):
                explicit_status_phase = ""
            failure_phase = (
                derived_failure_phase
                or explicit_error_category
                or _classify_status_error(side_meta.get("status_error"))
                or explicit_status_phase
            )
        if failure_phase is None and route_physical_failures:
            failure_phase = _derive_route_physical_failure_phase(route_physical_failures[0], target_route=target_route)
        if failure_phase is None and route_failures:
            failure_phase = _derive_route_failure_phase(route_failures[0], target_route=target_route)
        if failure_phase is None and target_route and not entered and not bool(side_meta.get("ok", False)):
            status_phase = str(side_meta.get("status_phase") or "").strip().lower()
            failure_phase = status_phase or "target_route_not_executed"
        if failure_phase is None and target_route and not entered:
            failure_phase = "target_route_not_executed"
        if failure_phase:
            first_failure_phase_candidates.append((side, failure_phase))

        sides[side] = {
            "ok": bool(side_meta.get("ok", False)),
            "status_phase": side_meta.get("status_phase"),
            "status_error": side_meta.get("status_error"),
            "error_category": side_meta.get("error_category"),
            "derived_failure_phase": side_meta.get("derived_failure_phase"),
            "last_runner_stage": side_meta.get("last_runner_stage"),
            "last_runner_event": side_meta.get("last_runner_event"),
            "abort_message": side_meta.get("abort_message"),
            "trace_expected_but_missing": side_meta.get("trace_expected_but_missing"),
            "cleanup_terminated": side_meta.get("cleanup_terminated"),
            "cleanup_termination_reason": side_meta.get("cleanup_termination_reason"),
            "entered_target_route": entered,
            "target_route_event_count": route_count,
            "route_failures": route_failures,
            "route_physical_failures": route_physical_failures,
            "target_open_valves": list(latest_physical_state.get("target_open_valves") or []),
            "actual_open_valves": list(latest_physical_state.get("actual_open_valves") or []),
            "target_relay_state": dict(latest_physical_state.get("target_relay_state") or {}),
            "actual_relay_state": dict(latest_physical_state.get("actual_relay_state") or {}),
            "route_physical_state_match": physical_state_match,
            "relay_physical_mismatch": bool(route_physical_failures),
            "mismatched_valves": list(latest_physical_state.get("mismatched_valves") or []),
            "mismatched_channels": list(latest_physical_state.get("mismatched_channels") or []),
            "cleanup_relay_state": dict(cleanup_relay_state.get("actual_relay_state") or {}),
            "cleanup_all_relays_off": cleanup_relay_state.get("cleanup_all_relays_off"),
            "first_failure_phase": failure_phase,
            "preflight": side_preflight if isinstance(side_preflight, dict) else {},
            "runtime_policy": runtime_policies.get(side, {}),
        }

    has_route_failures = any(bool((payload or {}).get("route_failures")) for payload in sides.values())
    has_physical_route_mismatches = any(bool(value) for value in relay_physical_mismatch.values())
    valid_for_route_diff = (
        bool(all(entered_target_route.values()))
        and not has_physical_route_mismatches
        and not has_route_failures
    )
    if preflight and not bool(preflight.get("ok", True)):
        compare_status = COMPARE_STATUS_INVALID_PROFILE_INPUT
    elif has_route_failures:
        compare_status = COMPARE_STATUS_MISMATCH
    elif not valid_for_route_diff:
        compare_status = COMPARE_STATUS_MISMATCH if has_physical_route_mismatches else COMPARE_STATUS_NOT_EXECUTED
    else:
        compare_status = COMPARE_STATUS_MISMATCH

    first_failure_phase = None
    if first_failure_phase_candidates:
        side, phase = first_failure_phase_candidates[0]
        first_failure_phase = f"{side}:{phase}"

    reason = ""
    if compare_status == COMPARE_STATUS_INVALID_PROFILE_INPUT:
        reason = str(preflight.get("reason") or "compare inputs filtered to zero executable points")
    elif has_physical_route_mismatches:
        failing_sides = [side for side, mismatch in relay_physical_mismatch.items() if bool(mismatch)]
        reason = f"target route physical relay state did not match commanded valves on: {', '.join(failing_sides)}"
    elif compare_status == COMPARE_STATUS_NOT_EXECUTED:
        missing_sides = [side for side, entered in entered_target_route.items() if not entered]
        if target_route:
            reason = f"target route `{target_route}` was not entered on: {', '.join(missing_sides)}"
        else:
            reason = f"route compare scope was not entered on: {', '.join(missing_sides)}"
    elif has_route_failures:
        failing_sides = [side for side, payload in sides.items() if payload.get("route_failures")]
        reason = f"in-scope route failures were recorded on: {', '.join(failing_sides)}"

    return {
        "target_route": target_route,
        "compare_status": compare_status,
        "entered_target_route": entered_target_route,
        "target_route_event_count": target_route_event_count,
        "valid_for_route_diff": valid_for_route_diff,
        "has_route_failures": has_route_failures,
        "has_physical_route_mismatches": has_physical_route_mismatches,
        "route_physical_state_match": route_physical_state_match,
        "relay_physical_mismatch": relay_physical_mismatch,
        "first_failure_phase": first_failure_phase,
        "reason": reason,
        "bench_context": dict(bench_context),
        "effective_v2_compare_config": metadata.get("effective_v2_compare_config"),
        "sides": sides,
    }


def build_validation_scope(
    *,
    validation_profile: str,
    skip_co2_ppm: Optional[list[int]],
) -> dict[str, Any]:
    skip_list = [] if skip_co2_ppm is None else sorted(int(value) for value in skip_co2_ppm)
    if validation_profile == FULL_ROUTE_SIMULATED_VALIDATION_PROFILE:
        return {
            "profile": validation_profile,
            "summary": (
                "Full-route simulated replacement coverage exercises 0 ppm, H2O, CO2, multiple temperatures, "
                "multiple pressures, and full route/action ordering without touching real devices."
            ),
            "proves": [
                "Full logical route/action coverage for H2O + CO2 + 0 ppm in simulation.",
                "Compare/report/latest/bundle generation can be regression-tested without bench access.",
                "UI validation cockpit can consume full-route evidence in a device-free environment.",
            ],
            "does_not_prove": [
                "Real-device acceptance evidence.",
                "Bench cutover readiness.",
                "Physical device timing, transport, or numeric equivalence.",
            ],
        }
    if validation_profile == FULL_ROUTE_SIMULATED_DIAGNOSTIC_VALIDATION_PROFILE:
        return {
            "profile": validation_profile,
            "summary": (
                "Full-route simulated diagnostic coverage injects failures into the complete H2O + CO2 flow "
                "to verify failure classification, partial artifact generation, and recovery reporting."
            ),
            "proves": [
                "Failure classification and artifact integrity across injected faults.",
                "Diagnostic compare/report/UI behavior for full-route scenarios.",
                "Golden regression of first_failure_phase and evidence-state semantics.",
            ],
            "does_not_prove": [
                "Real-device acceptance evidence.",
                "Bench cutover readiness.",
                "Physical transport or hardware recovery behavior.",
            ],
        }
    if validation_profile == SKIP0_CO2_ONLY_SIMULATED_VALIDATION_PROFILE:
        return {
            "profile": validation_profile,
            "summary": (
                "Simulated parity coverage for the real main acceptance route: CO2-only with skip_co2_ppm=[0]. "
                "Use it to regression-test the real profile logic without occupying the bench."
            ),
            "proves": [
                "CO2-only + skip0 compare/report/latest/bundle logic can regress without bench access.",
                "Profile-governed skip_h2o and expected_disabled semantics remain stable.",
                "UI can distinguish simulated parity evidence from real acceptance evidence.",
            ],
            "does_not_prove": [
                "Real-device acceptance evidence.",
                "True 0 ppm equivalence.",
                "Bench cutover readiness.",
            ],
        }
    if validation_profile == H2O_ONLY_SIMULATED_VALIDATION_PROFILE:
        return {
            "profile": validation_profile,
            "summary": (
                "Simulated H2O-only route coverage exercises H2O route-entry, stability, timeout, and cleanup "
                "logic without the current humidity-generator hardware constraints."
            ),
            "proves": [
                "H2O route logic can be regression-tested without a healthy humidity generator.",
                "Diagnostic H2O compare/report/UI paths remain stable while H2O is out of scope on the real bench.",
                "Timeout and early-stop classification can be replayed consistently.",
            ],
            "does_not_prove": [
                "Real H2O acceptance evidence.",
                "Humidity-generator physical behavior on the bench.",
                "Bench cutover readiness.",
            ],
        }
    if validation_profile == SKIP0_CO2_ONLY_VALIDATION_PROFILE:
        return {
            "profile": validation_profile,
            "summary": (
                "CO2-only + skip_co2_ppm=[0] is the current main replacement-validation route for the real bench: "
                "0 ppm is unavailable, other gases are restored, and the H2O route is out of scope because the "
                "humidity generator temperature changes while humidity feedback stays static."
            ),
            "proves": [
                "CO2 route action order can be reviewed against V1 for the non-zero point set.",
                "CO2 point presence can be compared for the in-scope gas routes.",
                "CO2 sample count can be compared for the in-scope gas routes.",
            ],
            "does_not_prove": [
                "True 0 ppm behavior equivalence.",
                "H2O route equivalence while the humidity generator is faulted.",
                "Full numeric equivalence between V1 and V2.",
                "Bench or device cutover readiness by itself.",
            ],
        }
    if validation_profile == SKIP0_CO2_ONLY_DIAGNOSTIC_VALIDATION_PROFILE:
        return {
            "profile": validation_profile,
            "summary": (
                "CO2-only relaxed diagnostic validation is a route-unblock aid for the current bench. "
                "It keeps `co2_only + skip_co2_ppm=[0]` and H2O out of scope, but relaxes sensor precheck "
                "so a real CO2 route/action diff can be collected faster. Diagnostic only."
            ),
            "proves": [
                "Whether V1 and V2 can both enter the in-scope CO2 route on the current bench.",
                "Whether a real CO2 route/action diff can be produced after relaxing startup sensor precheck.",
                "Whether route-unblock progress is improving without changing the strict acceptance profile.",
            ],
            "does_not_prove": [
                "Acceptance-grade replacement evidence.",
                "True 0 ppm behavior equivalence.",
                "H2O route equivalence while the humidity generator is faulted.",
                "Bench or device cutover readiness by itself.",
            ],
        }
    if validation_profile == H2O_ONLY_VALIDATION_PROFILE:
        return {
            "profile": validation_profile,
            "summary": (
                "H2O-only replacement validation is now a fallback diagnostic route only. It is out of scope for "
                "current primary acceptance because the humidity generator humidity feedback is not valid."
            ),
            "proves": [
                "H2O early-stop and route-entry behavior can be reviewed against V1 for the selected temperature group.",
                "H2O point presence can be compared without requiring the CO2 route to pass.",
                "H2O sample count can be compared without CO2 route participation when the route is actually entered.",
            ],
            "does_not_prove": [
                "CO2 route or gas-path behavior equivalence.",
                "Full H2O seal/sample equivalence unless both sides actually enter the H2O route.",
                "Full numeric equivalence between V1 and V2.",
                "Bench or device cutover readiness by itself.",
            ],
        }
    if validation_profile == SKIP0_VALIDATION_PROFILE or 0 in skip_list:
        return {
            "profile": validation_profile,
            "summary": (
                "Mixed-route skip_co2_ppm=[0] compare is retained as a secondary review aid, but it is no longer "
                "the current primary acceptance route on the bench because H2O is out of scope."
            ),
            "proves": [
                "Historical mixed-route traces can still be reviewed against V1 for the non-skipped point set.",
                "Point presence can be compared for the non-skipped mixed-route point set when the route actually executes.",
                "Sample count can be compared for the non-skipped mixed-route point set when the route actually executes.",
            ],
            "does_not_prove": [
                "True 0 ppm behavior equivalence.",
                "Current-bench H2O replacement readiness.",
                "Full numeric equivalence between V1 and V2.",
                "Bench or device cutover readiness by itself.",
            ],
        }
    return {
        "profile": validation_profile,
        "summary": (
            "Standard control-flow compare report. Numeric equivalence still requires separate compare-review "
            "evidence."
        ),
        "proves": [
            "Route action order can be reviewed against V1 for the selected point set.",
            "Point presence can be compared for the selected point set.",
            "Sample count can be compared for the selected point set.",
        ],
        "does_not_prove": [
            "Bench or device cutover readiness by itself.",
        ],
    }


def _evidence_descriptor(*, validation_profile: str, evidence_source: str) -> dict[str, Any]:
    if evidence_source == EVIDENCE_SOURCE_SIMULATED:
        if validation_profile == FULL_ROUTE_SIMULATED_VALIDATION_PROFILE:
            return {
                "checklist_gate": "SIM-FULL",
                "evidence_state": "simulated_acceptance_like_coverage",
                "diagnostic_only": False,
                "acceptance_evidence": False,
                "not_real_acceptance_evidence": True,
            }
        if validation_profile == FULL_ROUTE_SIMULATED_DIAGNOSTIC_VALIDATION_PROFILE:
            return {
                "checklist_gate": "SIM-FULL-DIAG",
                "evidence_state": "simulated_diagnostic_coverage",
                "diagnostic_only": True,
                "acceptance_evidence": False,
                "not_real_acceptance_evidence": True,
            }
        if validation_profile == SKIP0_CO2_ONLY_SIMULATED_VALIDATION_PROFILE:
            return {
                "checklist_gate": "SIM-12A",
                "evidence_state": "simulated_acceptance_like_coverage",
                "diagnostic_only": False,
                "acceptance_evidence": False,
                "not_real_acceptance_evidence": True,
            }
        if validation_profile == H2O_ONLY_SIMULATED_VALIDATION_PROFILE:
            return {
                "checklist_gate": "SIM-12B",
                "evidence_state": "simulated_diagnostic_coverage",
                "diagnostic_only": True,
                "acceptance_evidence": False,
                "not_real_acceptance_evidence": True,
            }
        return {
            "checklist_gate": "SIM",
            "evidence_state": "simulated_validation",
            "diagnostic_only": True,
            "acceptance_evidence": False,
            "not_real_acceptance_evidence": True,
        }
    if validation_profile == H2O_ONLY_VALIDATION_PROFILE:
        return {
            "checklist_gate": "12B",
            "evidence_state": "diagnostic_fallback_validation",
            "diagnostic_only": True,
            "acceptance_evidence": False,
            "not_real_acceptance_evidence": False,
        }
    if validation_profile == SKIP0_CO2_ONLY_DIAGNOSTIC_VALIDATION_PROFILE:
        return {
            "checklist_gate": "12A",
            "evidence_state": "route_unblock_diagnostic",
            "diagnostic_only": True,
            "acceptance_evidence": False,
            "not_real_acceptance_evidence": False,
        }
    if validation_profile == SKIP0_VALIDATION_PROFILE:
        return {
            "checklist_gate": "12A",
            "evidence_state": "superseded_mixed_route_validation",
            "diagnostic_only": True,
            "acceptance_evidence": False,
            "not_real_acceptance_evidence": False,
        }
    if validation_profile == SKIP0_CO2_ONLY_VALIDATION_PROFILE:
        return {
            "checklist_gate": "12A",
            "evidence_state": "narrowed_replacement_validation",
            "diagnostic_only": False,
            "acceptance_evidence": False,
            "not_real_acceptance_evidence": False,
        }
    return {
        "checklist_gate": "STANDARD",
        "evidence_state": "standard_compare",
        "diagnostic_only": False,
        "acceptance_evidence": False,
        "not_real_acceptance_evidence": False,
    }


def build_control_flow_report(
    *,
    v1_trace_path: Path,
    v2_trace_path: Path,
    metadata: dict[str, Any],
) -> dict[str, Any]:
    v1_records = _load_trace_records(v1_trace_path)
    v2_records = _load_trace_records(v2_trace_path)
    v1_events = route_trace_diff.load_route_trace(v1_trace_path) if v1_trace_path.exists() else []
    v2_events = route_trace_diff.load_route_trace(v2_trace_path) if v2_trace_path.exists() else []
    route_summaries = route_trace_diff.compare_route_traces(v1_events, v2_events, max_diff_lines=8)
    presence = summarize_presence(v1_records, v2_records)
    sample_count = summarize_sample_counts(v1_records, v2_records)
    key_actions = summarize_key_action_diffs(v1_events, v2_events)
    review_stages = summarize_review_stage_diffs(v1_events, v2_events)
    route_sequence = {
        "matches": all(summary.matches for summary in route_summaries),
        "routes": [_route_diff_summary_to_dict(summary) for summary in route_summaries],
    }
    validation_profile = str(metadata.get("validation_profile") or DEFAULT_VALIDATION_PROFILE)
    evidence_source = str(metadata.get("evidence_source") or EVIDENCE_SOURCE_REAL).strip().lower() or EVIDENCE_SOURCE_REAL
    evidence = _evidence_descriptor(validation_profile=validation_profile, evidence_source=evidence_source)
    evidence_state = str(metadata.get("evidence_state_override") or evidence["evidence_state"])
    validation_scope = build_validation_scope(
        validation_profile=validation_profile,
        skip_co2_ppm=metadata.get("skip_co2_ppm"),
    )
    reference_quality = _reference_quality_summary(metadata)
    target_route = _target_route_for_compare(
        validation_profile=validation_profile,
        route_mode=metadata.get("route_mode"),
    )
    route_execution_summary = _build_route_execution_summary(
        target_route=target_route,
        metadata=metadata,
        v1_records=v1_records,
        v2_records=v2_records,
        v1_events=v1_events,
        v2_events=v2_events,
    )
    compare_status = str(route_execution_summary.get("compare_status") or COMPARE_STATUS_MISMATCH)
    replacement_validation = summarize_replacement_validation(
        presence=presence,
        sample_count=sample_count,
        route_sequence=route_sequence,
        key_actions=key_actions,
        compare_status=compare_status,
        valid_for_route_diff=bool(route_execution_summary.get("valid_for_route_diff", True)),
        validation_profile=validation_profile,
        skip_co2_ppm=metadata.get("skip_co2_ppm"),
        target_route=target_route,
        evidence_state=evidence_state,
        route_execution_summary=route_execution_summary,
    )
    overall_ok = compare_status == COMPARE_STATUS_MATCH
    if compare_status == COMPARE_STATUS_MISMATCH:
        overall_ok = (
            bool(metadata.get("v1", {}).get("ok"))
            and bool(metadata.get("v2", {}).get("ok"))
            and presence["matches"]
            and sample_count["matches"]
            and bool(replacement_validation.get("route_action_order_matches", False))
            and all(payload.get("matches", False) for payload in key_actions.values())
            and not bool(route_execution_summary.get("has_route_failures"))
            and not bool(route_execution_summary.get("has_physical_route_mismatches"))
        )
        compare_status = COMPARE_STATUS_MATCH if overall_ok else COMPARE_STATUS_MISMATCH
    route_execution_summary["compare_status"] = compare_status
    replacement_validation = summarize_replacement_validation(
        presence=presence,
        sample_count=sample_count,
        route_sequence=route_sequence,
        key_actions=key_actions,
        compare_status=compare_status,
        valid_for_route_diff=bool(route_execution_summary.get("valid_for_route_diff", True)),
        validation_profile=validation_profile,
        skip_co2_ppm=metadata.get("skip_co2_ppm"),
        target_route=target_route,
        evidence_state=evidence_state,
        route_execution_summary=route_execution_summary,
    )
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "compare_status": compare_status,
        "overall_match": overall_ok,
        "checklist_gate": evidence["checklist_gate"],
        "evidence_source": evidence_source,
        "evidence_state": evidence_state,
        "diagnostic_only": bool(evidence["diagnostic_only"]),
        "acceptance_evidence": bool(evidence["acceptance_evidence"]),
        "not_real_acceptance_evidence": bool(evidence["not_real_acceptance_evidence"]),
        "metadata": metadata,
        "bench_context": dict(metadata.get("bench_context") or {}),
        "simulation_context": dict(metadata.get("simulation_context") or {}),
        "effective_validation_mode": dict(metadata.get("effective_validation_mode") or {}),
        "reference_quality": reference_quality,
        "validation_scope": validation_scope,
        "entered_target_route": route_execution_summary.get("entered_target_route", {}),
        "target_route_event_count": route_execution_summary.get("target_route_event_count", {}),
        "valid_for_route_diff": bool(route_execution_summary.get("valid_for_route_diff", True)),
        "first_failure_phase": route_execution_summary.get("first_failure_phase"),
        "route_execution_summary": route_execution_summary,
        "presence": presence,
        "sample_count": sample_count,
        "route_sequence": route_sequence,
        "key_actions": key_actions,
        "route_review_stages": review_stages,
        "replacement_validation": replacement_validation,
    }


def format_control_flow_report_markdown(report: dict[str, Any]) -> str:
    metadata = report.get("metadata") or {}
    validation_scope = report.get("validation_scope") or {}
    artifacts = report.get("artifacts") or {}
    artifact_inventory = report.get("artifact_inventory") or {}
    route_execution_summary = report.get("route_execution_summary") or {}
    overall_status_label = "MATCH" if bool(report.get("overall_match")) else str(
        report.get("compare_status", COMPARE_STATUS_MISMATCH)
    )
    lines = [
        "# V1/V2 Control Flow Compare",
        "",
        f"- Generated at: {report.get('generated_at', '-')}",
        f"- Evidence source: {report.get('evidence_source', EVIDENCE_SOURCE_REAL)}",
        f"- Evidence state: {report.get('evidence_state', '-')}",
        f"- Diagnostic only: {report.get('diagnostic_only', False)}",
        f"- Acceptance evidence: {report.get('acceptance_evidence', False)}",
        f"- Not real acceptance evidence: {report.get('not_real_acceptance_evidence', False)}",
        f"- Compare status: {report.get('compare_status', COMPARE_STATUS_MISMATCH)}",
        f"- Overall status: {overall_status_label}",
        f"- Validation profile: {metadata.get('validation_profile', DEFAULT_VALIDATION_PROFILE)}",
        f"- Temp filter: {metadata.get('temp_c', '-')}",
        f"- skip_co2_ppm: {metadata.get('skip_co2_ppm', [])}",
        f"- Key action registry: {report.get('replacement_validation', {}).get('key_action_group_registry', KEY_ACTION_REGISTRY)}",
        f"- V1 route trace: {artifacts.get('v1_route_trace', metadata.get('v1', {}).get('trace_path', '-'))}",
        f"- V2 route trace: {artifacts.get('v2_route_trace', metadata.get('v2', {}).get('trace_path', '-'))}",
        f"- Route trace diff: {artifacts.get('route_trace_diff', '-')}",
        f"- Point presence diff: {artifacts.get('point_presence_diff', '-')}",
        f"- Sample count diff: {artifacts.get('sample_count_diff', '-')}",
        f"- Artifact inventory complete: {artifact_inventory.get('complete', False)}",
        "",
        "## Route Execution",
        f"- Target route: {route_execution_summary.get('target_route') or '-'}",
        f"- valid_for_route_diff: {report.get('valid_for_route_diff', True)}",
        f"- first_failure_phase: {report.get('first_failure_phase') or '-'}",
        f"- entered_target_route: {report.get('entered_target_route', {})}",
        f"- target_route_event_count: {report.get('target_route_event_count', {})}",
        f"- bench_context: {report.get('bench_context', {})}",
        f"- simulation_context: {report.get('simulation_context', {})}",
        f"- route_physical_state_match: {route_execution_summary.get('route_physical_state_match', {})}",
        f"- relay_physical_mismatch: {route_execution_summary.get('relay_physical_mismatch', {})}",
    ]
    route_reason = str(route_execution_summary.get("reason") or "").strip()
    if route_reason:
        lines.append(f"- Reason: {route_reason}")
    reference_quality = report.get("reference_quality") or {}
    lines.extend(
        [
            "",
            "## Reference Quality",
            f"- reference_quality: {reference_quality.get('reference_quality', '-')}",
            f"- reference_integrity: {reference_quality.get('reference_integrity', '-')}",
            f"- reference_quality_degraded: {reference_quality.get('reference_quality_degraded', False)}",
            f"- thermometer_reference_status: {reference_quality.get('thermometer_reference_status', '-')}",
            f"- pressure_reference_status: {reference_quality.get('pressure_reference_status', '-')}",
            f"- reasons: {reference_quality.get('reasons', [])}",
        ]
    )
    for side in ("v1", "v2"):
        side_summary = route_execution_summary.get("sides", {}).get(side, {})
        if not side_summary:
            continue
        lines.append(
            f"- {side.upper()}: ok={side_summary.get('ok')} phase={side_summary.get('status_phase') or '-'} "
            f"entered_target_route={side_summary.get('entered_target_route')} "
            f"target_route_event_count={side_summary.get('target_route_event_count')} "
            f"first_failure_phase={side_summary.get('first_failure_phase') or '-'}"
        )
        if side_summary.get("error_category"):
            lines.append(f"- {side.upper()} error_category: {side_summary.get('error_category')}")
        if side_summary.get("derived_failure_phase"):
            lines.append(f"- {side.upper()} derived_failure_phase: {side_summary.get('derived_failure_phase')}")
        if side_summary.get("status_error"):
            lines.append(f"- {side.upper()} status_error: {side_summary.get('status_error')}")
        if side_summary.get("last_runner_stage"):
            lines.append(f"- {side.upper()} last_runner_stage: {side_summary.get('last_runner_stage')}")
        if side_summary.get("last_runner_event"):
            lines.append(f"- {side.upper()} last_runner_event: {side_summary.get('last_runner_event')}")
        if side_summary.get("abort_message"):
            lines.append(f"- {side.upper()} abort_message: {side_summary.get('abort_message')}")
        if side_summary.get("trace_expected_but_missing") is not None:
            lines.append(
                f"- {side.upper()} trace_expected_but_missing: {side_summary.get('trace_expected_but_missing')}"
            )
        if side_summary.get("target_open_valves") is not None:
            lines.append(f"- {side.upper()} target_open_valves: {side_summary.get('target_open_valves')}")
            lines.append(f"- {side.upper()} actual_open_valves: {side_summary.get('actual_open_valves')}")
            lines.append(f"- {side.upper()} target_relay_state: {side_summary.get('target_relay_state')}")
            lines.append(f"- {side.upper()} actual_relay_state: {side_summary.get('actual_relay_state')}")
            lines.append(f"- {side.upper()} route_physical_state_match: {side_summary.get('route_physical_state_match')}")
            lines.append(f"- {side.upper()} relay_physical_mismatch: {side_summary.get('relay_physical_mismatch')}")
            lines.append(f"- {side.upper()} mismatched_valves: {side_summary.get('mismatched_valves')}")
            lines.append(f"- {side.upper()} mismatched_channels: {side_summary.get('mismatched_channels')}")
        if side_summary.get("cleanup_all_relays_off") is not None:
            lines.append(f"- {side.upper()} cleanup_all_relays_off: {side_summary.get('cleanup_all_relays_off')}")
            lines.append(f"- {side.upper()} cleanup_relay_state: {side_summary.get('cleanup_relay_state')}")
        if side_summary.get("cleanup_terminated") is not None:
            lines.append(f"- {side.upper()} cleanup_terminated: {side_summary.get('cleanup_terminated')}")
        if side_summary.get("cleanup_termination_reason"):
            lines.append(f"- {side.upper()} cleanup_termination_reason: {side_summary.get('cleanup_termination_reason')}")
        preflight = side_summary.get("preflight") or {}
        if preflight and not preflight.get("ok", True):
            lines.append(f"- {side.upper()} preflight: {preflight.get('reason')}")
        runtime_policy = side_summary.get("runtime_policy") or {}
        if runtime_policy:
            lines.append(
                f"- {side.upper()} runtime_policy: "
                f"collect_only={runtime_policy.get('collect_only')} "
                f"collect_only_fast_path={runtime_policy.get('collect_only_fast_path')} "
                f"precheck_device_connection={runtime_policy.get('precheck_device_connection')} "
                f"precheck_sensor_check={runtime_policy.get('precheck_sensor_check')} "
                f"sensor_precheck_enabled={runtime_policy.get('sensor_precheck_enabled')} "
                f"sensor_precheck_profile={runtime_policy.get('sensor_precheck_profile')} "
                f"sensor_precheck_scope={runtime_policy.get('sensor_precheck_scope')} "
                f"sensor_precheck_validation_mode={runtime_policy.get('sensor_precheck_validation_mode')} "
                f"sensor_precheck_active_send={runtime_policy.get('sensor_precheck_active_send')} "
                f"sensor_precheck_strict={runtime_policy.get('sensor_precheck_strict')} "
                f"expected_disabled_devices={runtime_policy.get('expected_disabled_devices')} "
                f"h2o_humidity_timeout_policy={runtime_policy.get('h2o_humidity_timeout_policy')}"
            )
            if runtime_policy.get("effective_v2_compare_config"):
                lines.append(
                    f"- {side.upper()} effective_v2_compare_config: "
                    f"{runtime_policy.get('effective_v2_compare_config')}"
                )
    lines.extend(
        [
            "",
        "## Validation Scope",
        f"- Summary: {validation_scope.get('summary', '-')}",
        ]
    )
    for item in validation_scope.get("proves") or []:
        lines.append(f"- Proves: {item}")
    for item in validation_scope.get("does_not_prove") or []:
        lines.append(f"- Does not prove: {item}")
    lines.extend(
        [
            "",
        "## Replacement Validation",
        f"- scope: {report['replacement_validation'].get('scope')}",
        f"- scope_statement: {report['replacement_validation'].get('scope_statement') or '-'}",
        f"- conclusion: {report['replacement_validation'].get('conclusion')}",
        f"- path_usable: {report['replacement_validation'].get('path_usable')}",
        f"- cutover_ready: {report['replacement_validation'].get('cutover_ready')}",
        f"- default_replacement_ready: {report['replacement_validation'].get('default_replacement_ready')}",
        f"- full_equivalence_established: {report['replacement_validation'].get('full_equivalence_established')}",
        f"- numeric_equivalence_established: {report['replacement_validation'].get('numeric_equivalence_established')}",
        f"- evidence_state: {report['replacement_validation'].get('evidence_state')}",
        f"- first_failure_phase: {report['replacement_validation'].get('first_failure_phase') or '-'}",
        f"- presence_evaluable: {report['replacement_validation'].get('presence_evaluable')}",
        f"- sample_count_evaluable: {report['replacement_validation'].get('sample_count_evaluable')}",
        f"- route_action_order_evaluable: {report['replacement_validation'].get('route_action_order_evaluable')}",
        f"- only_in_v1: {', '.join(report['replacement_validation']['only_in_v1']) or '-'}",
        f"- only_in_v2: {', '.join(report['replacement_validation']['only_in_v2']) or '-'}",
        f"- missing_points: {report['replacement_validation'].get('missing_points')}",
        f"- sample_count_mismatch: {report['replacement_validation'].get('sample_count_mismatch')}",
        f"- sample_count_matches: {report['replacement_validation']['sample_count_matches']}",
        f"- route_action_order_matches: {report['replacement_validation']['route_action_order_matches']}",
        "",
        "## Presence",
        f"- Match: {report['presence']['matches']}",
        f"- V1 only: {', '.join(report['presence']['v1_only']) or '-'}",
        f"- V2 only: {', '.join(report['presence']['v2_only']) or '-'}",
        "",
        "## Sample Count",
        f"- Match: {report['sample_count']['matches']}",
        ]
    )
    mismatches = report["sample_count"].get("mismatches") or []
    if mismatches:
        for item in mismatches[:8]:
            lines.append(
                f"- {item['point_tag']}: V1={item['v1_sample_count']} ({item['v1_result']}) | "
                f"V2={item['v2_sample_count']} ({item['v2_result']})"
            )
    else:
        lines.append("- No sample-count mismatches")
    lines.extend(
        [
            "",
            "## Route Sequence",
            f"- Match: {report['route_sequence']['matches']}",
        ]
    )
    for item in report["route_sequence"].get("routes") or []:
        if item.get("matches"):
            continue
        lines.append(
            f"- {item['route']}: missing={len(item['missing_in_v2'])} extra={len(item['extra_in_v2'])} "
            f"order_mismatches={len(item['order_mismatches'])}"
        )
    if report["replacement_validation"].get("route_action_order_differences"):
        lines.extend(["", "## Route Action Order Differences"])
        for item in report["replacement_validation"]["route_action_order_differences"]:
            lines.append(
                f"- {item['route']}: missing={len(item['missing_in_v2'])} extra={len(item['extra_in_v2'])} "
                f"order_mismatches={len(item['order_mismatches'])} "
                f"classification={item.get('classification') or '-'}"
            )
    lines.extend(["", "## Route Review Stages"])
    for stage, payload in sorted((report.get("route_review_stages") or {}).items()):
        lines.append(
            f"- {stage}: match={payload.get('matches')} "
            f"v1={payload.get('v1_action_counts')} v2={payload.get('v2_action_counts')}"
        )
    lines.extend(["", "## Key Actions"])
    for category, payload in sorted((report.get("key_actions") or {}).items()):
        lines.append(
            f"- {category}: match={payload.get('matches')} "
            f"v1={payload.get('v1_action_counts')} v2={payload.get('v2_action_counts')}"
        )
    return "\n".join(lines).rstrip() + "\n"


def _format_route_execution_only_report(
    *,
    report: dict[str, Any],
    v1_trace_path: Path,
    v2_trace_path: Path,
) -> str:
    route_execution_summary = report.get("route_execution_summary") or {}
    lines = [
        "Route Trace Diff",
        f"V1 trace: {v1_trace_path}",
        f"V2 trace: {v2_trace_path}",
        f"Compare status: {report.get('compare_status', COMPARE_STATUS_MISMATCH)}",
        f"Target route: {route_execution_summary.get('target_route') or '-'}",
        f"Valid for route diff: {report.get('valid_for_route_diff', True)}",
        f"First failure phase: {report.get('first_failure_phase') or '-'}",
        f"Bench context: {report.get('bench_context', {})}",
        "",
    ]
    reason = str(route_execution_summary.get("reason") or "").strip()
    if reason:
        lines.append(f"Reason: {reason}")
        lines.append("")
    for side in ("v1", "v2"):
        payload = route_execution_summary.get("sides", {}).get(side, {})
        if not payload:
            continue
        lines.append(f"[{side.upper()}]")
        lines.append(f"  ok: {payload.get('ok')}")
        lines.append(f"  status_phase: {payload.get('status_phase') or '-'}")
        lines.append(f"  error_category: {payload.get('error_category') or '-'}")
        lines.append(f"  derived_failure_phase: {payload.get('derived_failure_phase') or '-'}")
        lines.append(f"  status_error: {payload.get('status_error') or '-'}")
        lines.append(f"  last_runner_stage: {payload.get('last_runner_stage') or '-'}")
        lines.append(f"  last_runner_event: {payload.get('last_runner_event') or '-'}")
        lines.append(f"  abort_message: {payload.get('abort_message') or '-'}")
        lines.append(f"  trace_expected_but_missing: {payload.get('trace_expected_but_missing')}")
        lines.append(f"  entered_target_route: {payload.get('entered_target_route')}")
        lines.append(f"  target_route_event_count: {payload.get('target_route_event_count')}")
        lines.append(f"  first_failure_phase: {payload.get('first_failure_phase') or '-'}")
        lines.append(f"  route_physical_state_match: {payload.get('route_physical_state_match')}")
        lines.append(f"  relay_physical_mismatch: {payload.get('relay_physical_mismatch')}")
        lines.append(f"  target_open_valves: {payload.get('target_open_valves') or []}")
        lines.append(f"  actual_open_valves: {payload.get('actual_open_valves') or []}")
        lines.append(f"  mismatched_channels: {payload.get('mismatched_channels') or []}")
        lines.append(f"  cleanup_all_relays_off: {payload.get('cleanup_all_relays_off')}")
        runtime_policy = payload.get("runtime_policy") or {}
        if runtime_policy:
            lines.append(f"  precheck_device_connection: {runtime_policy.get('precheck_device_connection')}")
            lines.append(f"  precheck_sensor_check: {runtime_policy.get('precheck_sensor_check')}")
            lines.append(f"  sensor_precheck_enabled: {runtime_policy.get('sensor_precheck_enabled')}")
            lines.append(f"  sensor_precheck_profile: {runtime_policy.get('sensor_precheck_profile')}")
            lines.append(f"  sensor_precheck_scope: {runtime_policy.get('sensor_precheck_scope')}")
            lines.append(f"  sensor_precheck_validation_mode: {runtime_policy.get('sensor_precheck_validation_mode')}")
            lines.append(f"  sensor_precheck_active_send: {runtime_policy.get('sensor_precheck_active_send')}")
            lines.append(f"  sensor_precheck_strict: {runtime_policy.get('sensor_precheck_strict')}")
            lines.append(f"  expected_disabled_devices: {runtime_policy.get('expected_disabled_devices')}")
            lines.append(f"  h2o_humidity_timeout_policy: {runtime_policy.get('h2o_humidity_timeout_policy')}")
        preflight = payload.get("preflight") or {}
        if preflight and not preflight.get("ok", True):
            lines.append(f"  preflight_reason: {preflight.get('reason')}")
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def _write_compare_side_artifacts(
    *,
    report_dir: Path,
    report: dict[str, Any],
    v1_trace_path: Path,
    v2_trace_path: Path,
) -> dict[str, str]:
    v1_events = route_trace_diff.load_route_trace(v1_trace_path) if v1_trace_path.exists() else []
    v2_events = route_trace_diff.load_route_trace(v2_trace_path) if v2_trace_path.exists() else []
    if bool(report.get("valid_for_route_diff", True)):
        route_summaries = route_trace_diff.compare_route_traces(v1_events, v2_events, max_diff_lines=8)
        route_text = route_trace_diff.format_route_diff_report(v1_trace_path, v2_trace_path, route_summaries)
    else:
        route_text = _format_route_execution_only_report(
            report=report,
            v1_trace_path=v1_trace_path,
            v2_trace_path=v2_trace_path,
        )
    route_trace_diff_path = _write_text(report_dir / "route_trace_diff.txt", route_text)
    point_presence_diff_path = _write_json(report_dir / "point_presence_diff.json", report.get("presence") or {})
    sample_count_diff_path = _write_json(report_dir / "sample_count_diff.json", report.get("sample_count") or {})
    return {
        "route_trace_diff": str(route_trace_diff_path),
        "point_presence_diff": str(point_presence_diff_path),
        "sample_count_diff": str(sample_count_diff_path),
    }


def _write_trace_artifact_copies(
    *,
    report_dir: Path,
    v1_trace_path: Path,
    v2_trace_path: Path,
) -> dict[str, str]:
    artifacts: dict[str, str] = {}
    for label, source_path in (("v1_route_trace", v1_trace_path), ("v2_route_trace", v2_trace_path)):
        if not source_path.exists():
            continue
        target_path = report_dir / f"{label}.jsonl"
        target_path.parent.mkdir(parents=True, exist_ok=True)
        target_path.write_text(source_path.read_text(encoding="utf-8-sig"), encoding="utf-8")
        artifacts[label] = str(target_path)
    return artifacts


def build_artifact_inventory(artifacts: dict[str, str]) -> dict[str, Any]:
    items: dict[str, Any] = {}
    for key, value in sorted((artifacts or {}).items()):
        path_text = str(value or "").strip()
        exists = False
        if path_text:
            try:
                exists = Path(path_text).exists()
            except Exception:
                exists = False
        items[key] = {
            "path": path_text,
            "exists": exists,
        }
    required = {
        key: items.get(key, {"path": "", "exists": False})
        for key in REQUIRED_COMPARE_ARTIFACT_KEYS
    }
    return {
        "required_keys": list(REQUIRED_COMPARE_ARTIFACT_KEYS),
        "required": required,
        "complete": all(bool(entry.get("exists")) for entry in required.values()),
    }


def _validation_profile_artifact_names(validation_profile: str) -> Optional[dict[str, str]]:
    if validation_profile == FULL_ROUTE_SIMULATED_VALIDATION_PROFILE:
        return {
            "bundle_name": FULL_ROUTE_SIMULATED_BUNDLE_NAME,
            "latest_name": FULL_ROUTE_SIMULATED_LATEST_NAME,
            "bundle_artifact_key": "replacement_full_route_simulated_bundle",
            "latest_artifact_key": "replacement_full_route_simulated_latest",
            "command_hint": (
                "python -m gas_calibrator.v2.scripts.run_simulated_compare "
                "--profile replacement_full_route_simulated --scenario full_route_success_all_temps_all_sources "
                "[--run-name <name>]"
            ),
        }
    if validation_profile == FULL_ROUTE_SIMULATED_DIAGNOSTIC_VALIDATION_PROFILE:
        return {
            "bundle_name": FULL_ROUTE_SIMULATED_DIAGNOSTIC_BUNDLE_NAME,
            "latest_name": FULL_ROUTE_SIMULATED_DIAGNOSTIC_LATEST_NAME,
            "bundle_artifact_key": "replacement_full_route_simulated_diagnostic_bundle",
            "latest_artifact_key": "replacement_full_route_simulated_diagnostic_latest",
            "command_hint": (
                "python -m gas_calibrator.v2.scripts.run_simulated_compare "
                "--profile replacement_full_route_simulated_diagnostic --scenario pace_no_response_on_cleanup "
                "[--run-name <name>]"
            ),
        }
    if validation_profile == SKIP0_CO2_ONLY_SIMULATED_VALIDATION_PROFILE:
        return {
            "bundle_name": SKIP0_CO2_ONLY_SIMULATED_BUNDLE_NAME,
            "latest_name": SKIP0_CO2_ONLY_SIMULATED_LATEST_NAME,
            "bundle_artifact_key": "replacement_skip0_co2_only_simulated_bundle",
            "latest_artifact_key": "replacement_skip0_co2_only_simulated_latest",
            "command_hint": (
                "python -m gas_calibrator.v2.scripts.run_simulated_compare "
                "--profile replacement_skip0_co2_only_simulated --scenario co2_only_skip0_success_single_temp "
                "[--run-name <name>]"
            ),
        }
    if validation_profile == H2O_ONLY_SIMULATED_VALIDATION_PROFILE:
        return {
            "bundle_name": H2O_ONLY_SIMULATED_BUNDLE_NAME,
            "latest_name": H2O_ONLY_SIMULATED_LATEST_NAME,
            "bundle_artifact_key": "replacement_h2o_only_simulated_bundle",
            "latest_artifact_key": "replacement_h2o_only_simulated_latest",
            "command_hint": (
                "python -m gas_calibrator.v2.scripts.run_simulated_compare "
                "--profile replacement_h2o_only_simulated --scenario h2o_route_success_single_temp "
                "[--run-name <name>]"
            ),
        }
    if validation_profile == SKIP0_CO2_ONLY_VALIDATION_PROFILE:
        return {
            "bundle_name": SKIP0_CO2_ONLY_REPLACEMENT_BUNDLE_NAME,
            "latest_name": SKIP0_CO2_ONLY_REPLACEMENT_LATEST_NAME,
            "bundle_artifact_key": "skip0_co2_only_replacement_bundle",
            "latest_artifact_key": "skip0_co2_only_replacement_latest",
            "command_hint": (
                "python -m gas_calibrator.v2.scripts.verify_v1_v2_skip0_co2_only_replacement "
                "--v1-config <v1_config.json> --v2-config <v2_config.json> [--temp <temp_c>] "
                "[--simulation] [--run-name <name>]"
            ),
        }
    if validation_profile == SKIP0_CO2_ONLY_DIAGNOSTIC_VALIDATION_PROFILE:
        return {
            "bundle_name": SKIP0_CO2_ONLY_DIAGNOSTIC_BUNDLE_NAME,
            "latest_name": SKIP0_CO2_ONLY_DIAGNOSTIC_LATEST_NAME,
            "bundle_artifact_key": "skip0_co2_only_diagnostic_relaxed_bundle",
            "latest_artifact_key": "skip0_co2_only_diagnostic_relaxed_latest",
            "command_hint": (
                "python -m gas_calibrator.v2.scripts.verify_v1_v2_skip0_co2_only_diagnostic_relaxed "
                "--v1-config <v1_config.json> --v2-config <v2_config.json> [--temp <temp_c>] "
                "[--simulation] [--run-name <name>]"
            ),
        }
    if validation_profile == SKIP0_VALIDATION_PROFILE:
        return {
            "bundle_name": SKIP0_REPLACEMENT_BUNDLE_NAME,
            "latest_name": SKIP0_REPLACEMENT_LATEST_NAME,
            "bundle_artifact_key": "skip0_replacement_bundle",
            "latest_artifact_key": "skip0_replacement_latest",
            "command_hint": (
                "python -m gas_calibrator.v2.scripts.verify_v1_v2_skip0_replacement "
                "--v1-config <v1_config.json> --v2-config <v2_config.json> [--temp <temp_c>] "
                "[--simulation] [--run-name <name>]"
            ),
        }
    if validation_profile == H2O_ONLY_VALIDATION_PROFILE:
        return {
            "bundle_name": H2O_ONLY_REPLACEMENT_BUNDLE_NAME,
            "latest_name": H2O_ONLY_REPLACEMENT_LATEST_NAME,
            "bundle_artifact_key": "h2o_only_replacement_bundle",
            "latest_artifact_key": "h2o_only_replacement_latest",
            "command_hint": (
                "python -m gas_calibrator.v2.scripts.verify_v1_v2_h2o_only_replacement "
                "--v1-config <v1_config.json> --v2-config <v2_config.json> [--temp <temp_c>] "
                "[--simulation] [--run-name <name>]"
            ),
        }
    return None


def build_replacement_validation_bundle(
    *,
    report_root: Path,
    report_dir: Path,
    report: dict[str, Any],
) -> dict[str, Any]:
    metadata = dict(report.get("metadata") or {})
    validation_scope = dict(report.get("validation_scope") or {})
    replacement_validation = dict(report.get("replacement_validation") or {})
    artifacts = dict(report.get("artifacts") or {})
    artifact_inventory = dict(report.get("artifact_inventory") or {})
    names = _validation_profile_artifact_names(str(metadata.get("validation_profile") or ""))
    command_hint = "" if names is None else str(names.get("command_hint") or "")
    validation_profile = str(metadata.get("validation_profile", SKIP0_VALIDATION_PROFILE) or SKIP0_VALIDATION_PROFILE)
    evidence_source = str(report.get("evidence_source") or metadata.get("evidence_source") or EVIDENCE_SOURCE_REAL)
    evidence = _evidence_descriptor(validation_profile=validation_profile, evidence_source=evidence_source)
    return {
        "generated_at": report.get("generated_at"),
        "classification": "control_flow_replacement_validation",
        "checklist_gate": report.get("checklist_gate") or evidence["checklist_gate"],
        "validation_profile": validation_profile,
        "evidence_source": evidence_source,
        "evidence_state": report.get("evidence_state") or evidence["evidence_state"],
        "diagnostic_only": bool(report.get("diagnostic_only", evidence["diagnostic_only"])),
        "acceptance_evidence": bool(report.get("acceptance_evidence", evidence["acceptance_evidence"])),
        "not_real_acceptance_evidence": bool(
            report.get("not_real_acceptance_evidence", evidence["not_real_acceptance_evidence"])
        ),
        "run_name": metadata.get("run_name"),
        "report_root": str(report_root),
        "report_dir": str(report_dir),
        "compare_status": report.get("compare_status", COMPARE_STATUS_MISMATCH),
        "overall_match": bool(report.get("overall_match", False)),
        "bench_context": dict(report.get("bench_context") or {}),
        "simulation_context": dict(report.get("simulation_context") or {}),
        "effective_validation_mode": dict(report.get("effective_validation_mode") or {}),
        "reference_quality": dict(report.get("reference_quality") or {}),
        "summary": validation_scope.get("summary", ""),
        "validation_scope": validation_scope,
        "replacement_validation": replacement_validation,
        "route_execution_summary": report.get("route_execution_summary") or {},
        "artifacts": artifacts,
        "artifact_inventory": artifact_inventory,
        "command_hint": command_hint,
    }


def _write_replacement_validation_indexes(
    *,
    report_root: Path,
    report_dir: Path,
    report: dict[str, Any],
    update_latest: bool = True,
) -> dict[str, str]:
    validation_profile = str((report.get("metadata") or {}).get("validation_profile") or "")
    names = _validation_profile_artifact_names(validation_profile)
    if names is None:
        return {}
    bundle = build_replacement_validation_bundle(
        report_root=report_root,
        report_dir=report_dir,
        report=report,
    )
    evidence_source = str(bundle.get("evidence_source") or report.get("evidence_source") or "").strip().lower()
    bundle["latest_governance"] = {
        "root_latest_update_requested": bool(update_latest),
        "root_latest_update_allowed": bool(update_latest),
        "root_latest_update_blocked": not bool(update_latest),
        "reason": ""
        if update_latest
        else f"{evidence_source or 'unknown'} evidence is isolated to the report directory and must not overwrite root latest",
    }
    bundle_path = _write_json(report_dir / str(names["bundle_name"]), bundle)
    latest_parent = report_root if update_latest else report_dir
    latest_path = _write_json(latest_parent / str(names["latest_name"]), bundle)
    return {
        str(names["bundle_artifact_key"]): str(bundle_path),
        str(names["latest_artifact_key"]): str(latest_path),
    }


def _run_v1_trace(
    runtime_config_path: Path,
    *,
    temp_c: Optional[float],
    h2o_only: bool,
    skip_connect_check: bool,
    run_id: str,
) -> dict[str, Any]:
    runtime_cfg = json.loads(runtime_config_path.read_text(encoding="utf-8"))
    argv = ["--config", str(runtime_config_path), "--run-id", run_id]
    if temp_c is not None:
        argv.extend(["--temp", f"{float(temp_c):g}"])
    if h2o_only:
        argv.append("--h2o-only")
    if skip_connect_check:
        argv.append("--skip-connect-check")
    run_dir = Path(runtime_cfg.get("paths", {}).get("output_dir", "")) / run_id
    trace_path = run_dir / "route_trace.jsonl"
    status_path = run_dir / "route_trace_status.json"
    status_payload: dict[str, Any] = {}
    exit_code = 1
    cleanup_terminated = False
    cleanup_termination_reason: Optional[str] = None

    use_subprocess = getattr(_run_v1_trace_inprocess, "__module__", "") == __name__
    if use_subprocess:
        command = [sys.executable, "-m", "gas_calibrator.v2.scripts.run_v1_route_trace", *argv]
        proc = subprocess.Popen(
            command,
            cwd=str(PROJECT_ROOT),
            env=dict(os.environ),
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        ready_since: Optional[float] = None
        while True:
            polled = proc.poll()
            status_payload = _load_json_payload(status_path)
            status_ready = status_path.exists() and bool(
                status_payload
                or status_payload.get("status_phase")
                or status_payload.get("status_error")
                or status_payload.get("ok") is not None
            )
            if polled is not None:
                exit_code = int(polled)
                break
            if status_ready:
                if ready_since is None:
                    ready_since = time.monotonic()
                elif (time.monotonic() - ready_since) >= V1_TRACE_SUBPROCESS_GRACE_S:
                    cleanup_terminated = True
                    cleanup_termination_reason = "post_run_cleanup_timeout"
                    proc.terminate()
                    try:
                        proc.wait(timeout=V1_TRACE_SUBPROCESS_TERMINATE_WAIT_S)
                    except subprocess.TimeoutExpired:
                        proc.kill()
                        proc.wait(timeout=V1_TRACE_SUBPROCESS_TERMINATE_WAIT_S)
                    exit_code = int(proc.returncode or 1)
                    break
            time.sleep(V1_TRACE_SUBPROCESS_POLL_S)
    else:
        exit_code = int(_run_v1_trace_inprocess(argv))

    status_payload = _load_json_payload(status_path)
    derived_payload = _derive_v1_status_from_run_dir(run_dir)
    if not status_payload:
        status_payload = derived_payload
    elif derived_payload:
        weak_phase = str(status_payload.get("status_phase") or "").strip().lower() in {"", "output.route_trace_missing"}
        weak_category = not str(status_payload.get("error_category") or "").strip()
        if weak_phase or weak_category:
            merged_payload = dict(derived_payload)
            merged_payload.update({key: value for key, value in status_payload.items() if value not in (None, "", [])})
            if str(status_payload.get("status_phase") or "").strip().lower() == "output.route_trace_missing":
                merged_payload["status_phase"] = "output.route_trace_missing"
            status_payload = merged_payload
    return {
        "ok": bool(status_payload.get("ok", exit_code == 0 and trace_path.exists())),
        "exit_code": exit_code,
        "run_id": run_id,
        "run_dir": str(run_dir),
        "trace_path": str(trace_path),
        "runtime_config_path": str(runtime_config_path),
        "status_phase": status_payload.get("status_phase"),
        "status_error": status_payload.get("status_error"),
        "error_category": status_payload.get("error_category"),
        "derived_failure_phase": status_payload.get("derived_failure_phase"),
        "last_runner_stage": status_payload.get("last_runner_stage"),
        "last_runner_event": status_payload.get("last_runner_event"),
        "abort_message": status_payload.get("abort_message"),
        "trace_expected_but_missing": status_payload.get("trace_expected_but_missing"),
        "cleanup_terminated": cleanup_terminated,
        "cleanup_termination_reason": cleanup_termination_reason,
    }


def _run_v2_trace(
    runtime_config_path: Path,
    *,
    simulation_mode: bool,
) -> dict[str, Any]:
    service = create_calibration_service(str(runtime_config_path), simulation_mode=simulation_mode)
    run_dir = Path(service.result_store.run_dir)
    trace_path = run_dir / "route_trace.jsonl"
    status = None
    error_text: Optional[str] = None
    try:
        service.run()
        status = service.get_status()
    except Exception as exc:
        error_text = str(exc)
    finally:
        if service.is_running:
            service.stop(wait=True)
        else:
            close_all = getattr(service.device_manager, "close_all", None)
            if callable(close_all):
                close_all()
    if status is None:
        status = service.get_status()
    phase = getattr(getattr(status, "phase", None), "value", str(getattr(status, "phase", "") or ""))
    status_error = getattr(status, "error", None) or error_text
    return {
        "ok": str(phase) == "completed" and not status_error and trace_path.exists(),
        "exit_code": 0 if str(phase) == "completed" and not status_error else 1,
        "run_id": getattr(service.session, "run_id", ""),
        "run_dir": str(run_dir),
        "trace_path": str(trace_path),
        "runtime_config_path": str(runtime_config_path),
        "status_phase": str(phase),
        "status_error": None if status_error in (None, "") else str(status_error),
        "result_count": len(service.get_results()),
    }


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    validation_profile, skip_co2_ppm, route_mode, v1_h2o_only = _resolve_validation_profile(args)
    v1_config_path = _resolve_input_path(str(args.v1_config), anchor=PROJECT_ROOT)
    v2_config_path = _resolve_v2_compare_config_path(
        requested_path=_resolve_input_path(str(args.v2_config), anchor=PROJECT_ROOT),
        validation_profile=validation_profile,
    )
    bench_context = dict(_validation_bench_context(validation_profile))
    report_root = _resolve_input_path(str(args.report_root), anchor=PROJECT_ROOT)
    run_name_prefix = "control_flow_compare" if validation_profile == DEFAULT_VALIDATION_PROFILE else validation_profile
    run_name = str(args.run_name or f"{run_name_prefix}_{datetime.now().strftime('%Y%m%d_%H%M%S')}")
    report_dir = report_root / run_name
    report_dir.mkdir(parents=True, exist_ok=True)

    v1_base_cfg = load_config(v1_config_path)
    _, v2_raw_cfg, _ = load_config_bundle(str(v2_config_path), simulation_mode=bool(args.simulation))
    v1_runtime_cfg = _apply_runtime_overrides(
        v1_base_cfg,
        output_dir=report_dir / "v1_output",
        temp_c=args.temp,
        skip_co2_ppm=skip_co2_ppm,
        route_mode=route_mode,
        skip_connect_check=bool(args.skip_connect_check),
    )
    if validation_profile == SKIP0_CO2_ONLY_VALIDATION_PROFILE:
        v1_runtime_cfg = _apply_skip0_co2_only_compare_overrides(v1_runtime_cfg)
    if validation_profile == SKIP0_CO2_ONLY_DIAGNOSTIC_VALIDATION_PROFILE:
        v1_runtime_cfg = _apply_skip0_co2_only_diagnostic_relaxed_compare_overrides(v1_runtime_cfg)
    if validation_profile == H2O_ONLY_VALIDATION_PROFILE:
        v1_runtime_cfg = _apply_h2o_only_quick_compare_overrides(v1_runtime_cfg, runtime_side="v1")
    v2_runtime_cfg = _apply_runtime_overrides(
        v2_raw_cfg,
        output_dir=report_dir / "v2_output",
        temp_c=args.temp,
        skip_co2_ppm=skip_co2_ppm,
        route_mode=route_mode,
        skip_connect_check=bool(args.skip_connect_check),
    )
    if validation_profile == SKIP0_CO2_ONLY_VALIDATION_PROFILE:
        v2_runtime_cfg = _apply_skip0_co2_only_compare_overrides(v2_runtime_cfg)
    if validation_profile == SKIP0_CO2_ONLY_DIAGNOSTIC_VALIDATION_PROFILE:
        v2_runtime_cfg = _apply_skip0_co2_only_diagnostic_relaxed_compare_overrides(v2_runtime_cfg)
    if validation_profile == H2O_ONLY_VALIDATION_PROFILE:
        v2_runtime_cfg = _apply_h2o_only_quick_compare_overrides(v2_runtime_cfg, runtime_side="v2")
    v1_runtime_cfg_path = _write_json(report_dir / "runtime_v1_config.json", v1_runtime_cfg)
    v2_runtime_cfg_path = _write_json(report_dir / "runtime_v2_config.json", v2_runtime_cfg)
    v1_runtime_policy = _runtime_policy_summary(v1_runtime_cfg, bench_context=bench_context)
    v2_runtime_policy = _runtime_policy_summary(
        v2_runtime_cfg,
        effective_compare_config=v2_config_path,
        bench_context=bench_context,
    )
    effective_route_mode = route_mode or v2_runtime_cfg.get("workflow", {}).get("route_mode")
    effective_target_route = _target_route_for_compare(
        validation_profile=validation_profile,
        route_mode=effective_route_mode,
    )
    if effective_target_route:
        bench_context["target_route"] = effective_target_route
    preflight = _build_preflight_summary(
        v1_runtime_cfg=v1_runtime_cfg,
        v2_runtime_cfg=v2_runtime_cfg,
        validation_profile=validation_profile,
    )
    print(f"V1 config: {v1_config_path}")
    print(f"V2 config: {v2_config_path}")
    _print_effective_paths("V1", v1_runtime_cfg, v1_runtime_cfg_path)
    _print_effective_paths("V2", v2_runtime_cfg, v2_runtime_cfg_path)
    if not bool(preflight.get("ok", True)):
        print(f"Compare input validation failed: {preflight.get('reason')}")
        v1_run = _build_skipped_run_summary(
            runtime_cfg_path=v1_runtime_cfg_path,
            status_error=str(preflight.get("sides", {}).get("v1", {}).get("reason") or preflight.get("reason") or ""),
            status_phase="invalid_profile_input",
        )
        v2_run = _build_skipped_run_summary(
            runtime_cfg_path=v2_runtime_cfg_path,
            status_error=str(preflight.get("sides", {}).get("v2", {}).get("reason") or preflight.get("reason") or ""),
            status_phase="invalid_profile_input",
        )
    else:
        v1_run = _run_v1_trace(
            v1_runtime_cfg_path,
            temp_c=args.temp,
            h2o_only=v1_h2o_only,
            skip_connect_check=bool(args.skip_connect_check),
            run_id=f"{run_name}_v1",
        )
        v2_run = _run_v2_trace(
            v2_runtime_cfg_path,
            simulation_mode=bool(args.simulation),
        )
    evidence_source = EVIDENCE_SOURCE_SIMULATED if bool(args.simulation) else EVIDENCE_SOURCE_REAL
    report = build_control_flow_report(
        v1_trace_path=Path(v1_run["trace_path"]),
        v2_trace_path=Path(v2_run["trace_path"]),
        metadata={
            "run_name": run_name,
            "validation_profile": validation_profile,
            "evidence_source": evidence_source,
            "temp_c": args.temp,
            "skip_co2_ppm": [] if skip_co2_ppm is None else list(skip_co2_ppm),
            "route_mode": route_mode,
            "skip_connect_check": bool(args.skip_connect_check),
            "simulation": bool(args.simulation),
            "preflight": preflight,
            "bench_context": bench_context,
            "effective_v2_compare_config": str(v2_config_path),
            "effective_validation_mode": {
                "validation_profile": validation_profile,
                "route_mode": effective_route_mode,
                "target_route": effective_target_route,
                "sensor_precheck_validation_mode": v2_runtime_policy.get("sensor_precheck_validation_mode"),
                "sensor_precheck_active_send": v2_runtime_policy.get("sensor_precheck_active_send"),
                "sensor_precheck_strict": v2_runtime_policy.get("sensor_precheck_strict"),
                "h2o_humidity_timeout_policy": v2_runtime_policy.get("h2o_humidity_timeout_policy"),
                "diagnostic_only": bool(bench_context.get("diagnostic_only", False)),
                "acceptance_evidence": bool(bench_context.get("acceptance_evidence", False)),
            },
            "runtime_policies": {
                "v1": v1_runtime_policy,
                "v2": v2_runtime_policy,
            },
            "v1": {
                "config_path": str(v1_config_path),
                **v1_run,
            },
            "v2": {
                "config_path": str(v2_config_path),
                **v2_run,
            },
        },
    )
    report["artifacts"] = {
        **_write_trace_artifact_copies(
            report_dir=report_dir,
            v1_trace_path=Path(v1_run["trace_path"]),
            v2_trace_path=Path(v2_run["trace_path"]),
        ),
        **_write_compare_side_artifacts(
            report_dir=report_dir,
            report=report,
            v1_trace_path=Path(v1_run["trace_path"]),
            v2_trace_path=Path(v2_run["trace_path"]),
        ),
        "control_flow_compare_report_json": str(report_dir / "control_flow_compare_report.json"),
        "control_flow_compare_report_markdown": str(report_dir / "control_flow_compare_report.md"),
    }
    json_path = _write_json(report_dir / "control_flow_compare_report.json", report)
    markdown_path = _write_text(report_dir / "control_flow_compare_report.md", format_control_flow_report_markdown(report))
    report["artifact_inventory"] = build_artifact_inventory(report.get("artifacts") or {})
    artifact_inventory_path = _write_json(report_dir / "artifact_inventory.json", report["artifact_inventory"])
    report["artifacts"]["artifact_inventory"] = str(artifact_inventory_path)
    if validation_profile in {
        SKIP0_CO2_ONLY_VALIDATION_PROFILE,
        SKIP0_CO2_ONLY_DIAGNOSTIC_VALIDATION_PROFILE,
        SKIP0_VALIDATION_PROFILE,
        H2O_ONLY_VALIDATION_PROFILE,
    }:
        report["artifacts"].update(
            _write_replacement_validation_indexes(
                report_root=report_root,
                report_dir=report_dir,
                report=report,
                update_latest=not bool(args.simulation),
            )
        )
    json_path = _write_json(report_dir / "control_flow_compare_report.json", report)
    markdown_path = _write_text(report_dir / "control_flow_compare_report.md", format_control_flow_report_markdown(report))

    print(f"Report dir: {report_dir}")
    if "v1_route_trace" in report["artifacts"]:
        print(f"V1 route trace: {report['artifacts']['v1_route_trace']}")
    if "v2_route_trace" in report["artifacts"]:
        print(f"V2 route trace: {report['artifacts']['v2_route_trace']}")
    print(f"Route trace diff: {report['artifacts']['route_trace_diff']}")
    print(f"Point presence diff: {report['artifacts']['point_presence_diff']}")
    print(f"Sample count diff: {report['artifacts']['sample_count_diff']}")
    print(f"JSON report: {json_path}")
    print(f"Markdown report: {markdown_path}")
    if validation_profile == SKIP0_CO2_ONLY_VALIDATION_PROFILE:
        print(f"Skip0 CO2-only bundle: {report['artifacts']['skip0_co2_only_replacement_bundle']}")
        print(f"Skip0 CO2-only latest index: {report['artifacts']['skip0_co2_only_replacement_latest']}")
    if validation_profile == SKIP0_CO2_ONLY_DIAGNOSTIC_VALIDATION_PROFILE:
        print(f"Skip0 CO2-only diagnostic bundle: {report['artifacts']['skip0_co2_only_diagnostic_relaxed_bundle']}")
        print(f"Skip0 CO2-only diagnostic latest index: {report['artifacts']['skip0_co2_only_diagnostic_relaxed_latest']}")
    if validation_profile == SKIP0_VALIDATION_PROFILE:
        print(f"Skip0 bundle: {report['artifacts']['skip0_replacement_bundle']}")
        print(f"Skip0 latest index: {report['artifacts']['skip0_replacement_latest']}")
    if validation_profile == H2O_ONLY_VALIDATION_PROFILE:
        print(f"H2O-only bundle: {report['artifacts']['h2o_only_replacement_bundle']}")
        print(f"H2O-only latest index: {report['artifacts']['h2o_only_replacement_latest']}")
    print(f"Compare status: {report['compare_status']}")
    print(f"Overall status: {'MATCH' if report['overall_match'] else report['compare_status']}")
    return 0 if report["overall_match"] else 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
