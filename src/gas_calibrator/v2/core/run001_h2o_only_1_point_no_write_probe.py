from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import contextlib
import hashlib
import io
import json
import os
import re
import signal
import sys
import time
import traceback
from pathlib import Path
from typing import Any, Callable, Mapping, Optional

from gas_calibrator.v2.core.run001_r1_conditioning_only_probe import (
    _as_bool,
    _as_float,
    _json_dump,
    load_json_mapping,
)
from gas_calibrator.v2.core.services.trace_size_guard import (
    load_guarded_jsonl,
    summarize_trace_guard_rows,
    write_guarded_jsonl,
)


H2O_SCHEMA_VERSION = "v2.run001.h2o_only_1_point_no_write_probe.1"
H2O_ENV_VAR = "GAS_CAL_V2_H2O_1_POINT_NO_WRITE_REAL_COM"
H2O_ENV_VALUE = "1"
H2O_CLI_FLAG = "--allow-v2-h2o-1-point-no-write-real-com"
H2O_ALLOWED_PRESSURE_POINTS_HPA = (1100.0, 1000.0, 900.0, 800.0, 700.0, 600.0, 500.0)
H2O_ACCEPTED_SINGLE_PRESSURE_POINTS_HPA = H2O_ALLOWED_PRESSURE_POINTS_HPA
H2O_CURRENT_EVIDENCE_SOURCE = "real_probe_h2o_1r_1_point_no_write"
H2O_EVIDENCE_MARKERS = {
    "probe_identity": "H2O.1R H2O-only single-point no-write engineering probe",
    "probe_version": "H2O.1R",
    "evidence_source": H2O_CURRENT_EVIDENCE_SOURCE,
    "acceptance_level": "engineering_probe_only",
    "not_real_acceptance_evidence": True,
    "promotion_state": "blocked",
    "real_primary_latest_refresh": False,
}
H2O_REQUIRED_OPERATOR_FIELDS = (
    "operator_name",
    "timestamp",
    "branch",
    "HEAD",
    "config_path",
    "port_manifest",
    "explicit_acknowledgement",
)
H2O_REQUIRED_TRUE_ACKS = (
    "h2o_only",
    "only_h2o_1_point_no_write",
    "skip0",
    "single_route",
    "single_temperature",
    "single_pressure_point",
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
    "do_not_refresh_real_primary_latest",
)
H2O_REQUIRED_FALSE_ACKS = (
    "a3_enabled",
    "co2_enabled",
    "full_group_enabled",
    "multi_temperature_enabled",
    "real_primary_latest_refresh",
)
H2O_SAFETY_ASSERTION_DEFAULTS = {
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
H2O_INTERRUPTED_FAIL_CLOSED_REASON = "probe_execution_interrupted_required_artifacts_incomplete"
H2O_HANDOFF_REQUIRED_ARTIFACT_KEYS = (
    "probe_admission_record",
    "operator_confirmation_record",
    "summary",
    "safety_assertions",
    "process_exit_record",
)
H2O_DOWNSTREAM_BRIDGED_ARTIFACT_KEYS = (
    "route_trace",
    "pressure_trace_like",
    "point_results_json",
    "point_results_csv",
    "samples_runtime",
    "run_log",
    "downstream_summary",
)
H2O_OPTIONAL_DOWNSTREAM_ARTIFACT_KEYS = (
    "pressure_ready_trace_like",
    "heartbeat_trace_like",
    "analyzer_sampling_rows_like",
)
H2O_REQUIRED_ARTIFACT_KEYS = (
    *H2O_HANDOFF_REQUIRED_ARTIFACT_KEYS,
    *H2O_DOWNSTREAM_BRIDGED_ARTIFACT_KEYS,
)
H2O_LEGACY_REQUIRED_ARTIFACT_KEYS = (
    *H2O_HANDOFF_REQUIRED_ARTIFACT_KEYS,
    "route_trace",
    "pressure_trace",
    "pressure_ready_trace",
    "heartbeat_trace",
    "analyzer_sampling_rows",
    "point_results",
    "point_results_csv",
)


@dataclass(frozen=True)
class H2OAdmission:
    approved: bool
    reasons: tuple[str, ...]
    evidence: dict[str, Any]
    operator_confirmation: dict[str, Any]
    operator_validation: dict[str, Any]


class H2OPointsConfigAlignmentError(RuntimeError):
    """Raised when H2O points config cannot be aligned safely."""


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


def _allowed_h2o_branches(raw_cfg: Mapping[str, Any]) -> list[str]:
    default = ["codex/run001-a1-no-write-dry-run"]
    value = _first_value(
        raw_cfg,
        (
            "run001_h2o_1_point.allowed_branches",
            "h2o_only_1_point_no_write_probe.allowed_branches",
            "allowed_branches",
        ),
    )
    if isinstance(value, list) and value:
        return [str(v) for v in value]
    return default


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


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
    if not points:
        return False
    allowed = set(round(float(ref), 6) for ref in H2O_ACCEPTED_SINGLE_PRESSURE_POINTS_HPA)
    for p in points:
        if round(float(p), 6) not in allowed:
            return False
    return True


def _scope(raw_cfg: Mapping[str, Any]) -> str:
    value = _first_value(
        raw_cfg,
        (
            "scope",
            "run001_h2o_1_point.scope",
            "h2o_only_1_point_no_write_probe.scope",
        ),
    )
    if value is not None and str(value).strip():
        return str(value).strip().lower()
    h2o_section = raw_cfg.get("run001_h2o_1_point")
    if isinstance(h2o_section, Mapping) and _as_bool(h2o_section.get("h2o_only")) is True:
        return "run001_h2o_1_point"
    return ""


def _output_dir_value(raw_cfg: Mapping[str, Any], name: str) -> str:
    return str(
        _first_value(
            raw_cfg,
            (
                name,
                f"h2o_only_1_point_no_write_probe.{name}",
                f"run001_h2o_1_point.{name}",
            ),
        )
        or ""
    )


def _pressure_points(raw_cfg: Mapping[str, Any]) -> list[float]:
    value = _first_value(
        raw_cfg,
        (
            "workflow.pressure.pressurize_high_hpa",
            "pressure_points_hpa",
            "run001_h2o_1_point.pressure_points_hpa",
            "h2o_only_1_point_no_write_probe.pressure_points_hpa",
        ),
    )
    return _float_list(value)


def _skip0_only(raw_cfg: Mapping[str, Any]) -> bool:
    skip = _first_value(raw_cfg, ("workflow.skip_co2_ppm", "skip_co2_ppm"))
    if skip is None:
        return True
    if isinstance(skip, list) and not skip:
        return True
    if isinstance(skip, list) and skip == [0]:
        return True
    return _as_bool(skip) is False


def _single_temperature(raw_cfg: Mapping[str, Any]) -> bool:
    temps = _first_value(raw_cfg, ("workflow.selected_temps_c", "selected_temps_c"))
    if temps is None:
        return True
    if isinstance(temps, list) and len(temps) <= 1:
        return True
    if isinstance(temps, (int, float)):
        return True
    return False


def _selected_temperature_c(raw_cfg: Mapping[str, Any]) -> float:
    value = _first_value(
        raw_cfg,
        (
            "workflow.selected_temps_c",
            "selected_temps_c",
        ),
    )
    if isinstance(value, list) and value:
        parsed = _as_float(value[0])
    else:
        parsed = _as_float(value)
    return float(parsed) if parsed is not None else 20.0


def _load_h2o_points_from_v1_excel(excel_path: Path, raw_cfg: Mapping[str, Any]) -> list[dict[str, Any]]:
    try:
        import pandas as pd
    except Exception as e:
        print(f"[H2O 探针] pandas import failed: {e}", file=sys.stderr, flush=True)
        return []

    try:
        df = pd.read_excel(str(excel_path), header=None)
    except Exception as e:
        print(f"[H2O 探针] Excel read failed: {e}", file=sys.stderr, flush=True)
        return []
    if df.empty or df.shape[0] < 3:
        return []

    _header_row = 0
    for try_row in range(min(3, df.shape[0])):
        vals = [str(v or "").strip().lower() for v in df.iloc[try_row, :].tolist()]
        hits = sum(1 for v in vals if v and any(k in v for k in ("temp", "co2", "h2o", "pressure", "hpa", "kpa")))
        if hits >= 2:
            _header_row = try_row
            break

    raw_header = df.iloc[_header_row, :].tolist()
    col_map: dict[str, int] = {}
    for idx, val in enumerate(raw_header):
        text = str(val or "").strip().lower()
        if "temp" in text or "℃" in text:
            col_map.setdefault("temp", idx)
        elif "co2" in text:
            col_map.setdefault("co2", idx)
        elif "h2o" in text:
            col_map.setdefault("h2o", idx)
        elif "pressure" in text or "hpa" in text or "kpa" in text:
            col_map.setdefault("pressure", idx)

    if "h2o" not in col_map:
        return []

    h2o_col = col_map["h2o"]
    temp_col = col_map.get("temp", 0)
    pressure_col = col_map.get("pressure")

    _H2O_RE = re.compile(
        r'(?P<temp>\d+\.?\d*).+?'
        r'(?P<rh>\d+\.?\d*)\s*%'
        r'.+?'
        r'(?P<dp>\d+\.?\d*)'
        r'.+?'
        r'(?P<mmol>\d+\.?\d*)\s*mmol/mol'
    )
    h2o_rows: list[dict[str, Any]] = []
    for row_idx in range(_header_row + 1, len(df)):
        h2o_text = str(df.iloc[row_idx, h2o_col] or "").strip()
        if not h2o_text or str(h2o_text).lower() == "nan":
            continue
        m = _H2O_RE.search(h2o_text)
        if not m:
            continue
        hgen_temp_c = float(m.group("temp"))
        hgen_rh_pct = float(m.group("rh"))
        dewpoint_c = float(m.group("dp"))
        h2o_mmol = float(m.group("mmol"))

        temp_c = _as_float(df.iloc[row_idx, temp_col])
        if temp_c is None:
            temp_c = hgen_temp_c

        pressure_hpa: Optional[float] = None
        if pressure_col is not None:
            pressure_hpa = _as_float(df.iloc[row_idx, pressure_col])

        h2o_rows.append(
            {
                "index": len(h2o_rows) + 1,
                "route": "h2o",
                "temperature_c": float(temp_c or 20.0),
                "humidity_generator_temp_c": hgen_temp_c,
                "humidity_pct": hgen_rh_pct,
                "dewpoint_c": dewpoint_c,
                "h2o_mmol": h2o_mmol,
                "pressure_hpa": float(pressure_hpa or 1013.25),
                "target_pressure_hpa": float(pressure_hpa or 1013.25),
            }
        )

    if not h2o_rows:
        return []
    filtered: list[dict[str, Any]] = []
    for row in h2o_rows:
        p = _as_float(row.get("pressure_hpa"))
        if p is not None and p > 1150:
            continue
        filtered.append(row)
    if not filtered:
        filtered = h2o_rows[:1]
    for idx, row in enumerate(filtered, start=1):
        row["index"] = idx
    return filtered


def _build_h2o_downstream_point_rows(raw_cfg: Mapping[str, Any]) -> list[dict[str, Any]]:
    temperature_c = _selected_temperature_c(raw_cfg)
    rows: list[dict[str, Any]] = []
    for index, pressure in enumerate(H2O_ALLOWED_PRESSURE_POINTS_HPA, start=1):
        rows.append(
            {
                "index": index,
                "route": "h2o",
                "pressure_hpa": float(pressure),
                "target_pressure_hpa": float(pressure),
                "temperature_c": temperature_c,
                "temp_chamber_c": temperature_c,
            }
        )
    return rows


def _point_row_pressure(row: Mapping[str, Any]) -> Optional[float]:
    for key in ("pressure_hpa", "target_pressure_hpa", "pressure"):
        parsed = _as_float(row.get(key))
        if parsed is not None:
            return float(parsed)
    return None


def _validate_h2o_downstream_point_rows(rows: list[Mapping[str, Any]]) -> list[str]:
    reasons: list[str] = []
    if len(rows) < 1:
        reasons.append("h2o_point_count_zero")
    routes = []
    pressures = []
    for row in rows:
        route = str(row.get("route", "") or "").strip().lower()
        routes.append(route)
        pressure = _point_row_pressure(row)
        if not route:
            reasons.append("h2o_point_route_missing")
        if pressure is None:
            reasons.append("h2o_point_pressure_missing")
        else:
            pressures.append(pressure)
    if not all(r == "h2o" for r in routes):
        reasons.append("h2o_points_not_h2o_only")
    return list(dict.fromkeys(reasons))


def _write_json_no_bom(path: Path, payload: Mapping[str, Any] | list[Mapping[str, Any]]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _json_clone_mapping(raw_cfg: Mapping[str, Any]) -> dict[str, Any]:
    return json.loads(json.dumps(dict(raw_cfg), ensure_ascii=False))


def _load_json_mapping_accept_bom(path: str | Path) -> dict[str, Any]:
    return load_json_mapping(path)


def _validate_operator_confirmation(
    path: Optional[str | Path],
    *,
    expected_branch: str = "",
    expected_head: str = "",
    expected_config_path: str = "",
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

    for field in H2O_REQUIRED_OPERATOR_FIELDS:
        if payload.get(field) in (None, ""):
            errors.append(f"operator_confirmation_missing_{field}")

    ack = payload.get("explicit_acknowledgement")
    if not isinstance(ack, Mapping):
        errors.append("operator_confirmation_missing_explicit_acknowledgement")
        ack = {}
    for key in H2O_REQUIRED_TRUE_ACKS:
        if _as_bool(ack.get(key)) is not True:
            errors.append(f"operator_ack_missing_{key}")
    for key in H2O_REQUIRED_FALSE_ACKS:
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

    return payload, {"valid": not errors, "errors": errors, "path": str(confirmation_path.resolve())}


def _artifact_map_entry(key: str, path: Path, *, role: str, source: str, hard_required: bool) -> dict[str, Any]:
    exists = path.exists()
    return {
        "key": key,
        "role": role,
        "source": source,
        "source_path": str(path),
        "exists": exists,
        "status": "linked" if exists else "missing",
        "hard_required": bool(hard_required),
        "reason": "" if exists else "source_artifact_missing",
    }


def _missing_artifact_entry(key: str, *, role: str, source: str, hard_required: bool, reason: str) -> dict[str, Any]:
    return {
        "key": key,
        "role": role,
        "source": source,
        "source_path": "",
        "exists": False,
        "status": "missing" if hard_required else "not_available_or_not_generated",
        "hard_required": bool(hard_required),
        "reason": reason,
    }


def _first_existing_artifact(
    run_dir: Path,
    filenames: tuple[str, ...],
    *,
    key: str,
    role: str,
    source: str,
    hard_required: bool,
) -> dict[str, Any]:
    candidates = [run_dir / filename for filename in filenames]
    for candidate in candidates:
        if candidate.exists():
            return _artifact_map_entry(key, candidate, role=role, source=source, hard_required=hard_required)
    entry = _missing_artifact_entry(
        key,
        role=role,
        source=source,
        hard_required=hard_required,
        reason="candidate_artifacts_missing",
    )
    entry["candidate_paths"] = [str(candidate) for candidate in candidates]
    return entry


def _build_downstream_artifact_map(execution: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    run_dir_text = str(execution.get("execution_run_dir") or "").strip()
    if not run_dir_text:
        return {
            key: _missing_artifact_entry(
                key,
                role="downstream_runtime_artifact",
                source="downstream_run_dir",
                hard_required=key in H2O_DOWNSTREAM_BRIDGED_ARTIFACT_KEYS,
                reason="downstream_run_dir_missing",
            )
            for key in (*H2O_DOWNSTREAM_BRIDGED_ARTIFACT_KEYS, *H2O_OPTIONAL_DOWNSTREAM_ARTIFACT_KEYS)
        }
    run_dir = Path(run_dir_text)
    return {
        "route_trace": _artifact_map_entry(
            "route_trace",
            run_dir / "route_trace.jsonl",
            role="route_trace",
            source="downstream_route_trace_jsonl",
            hard_required=True,
        ),
        "run_log": _artifact_map_entry(
            "run_log",
            run_dir / "run.log",
            role="run_log",
            source="downstream_run_log",
            hard_required=True,
        ),
        "downstream_summary": _artifact_map_entry(
            "downstream_summary",
            run_dir / "summary.json",
            role="downstream_summary",
            source="downstream_summary_json",
            hard_required=True,
        ),
        "point_results_json": _first_existing_artifact(
            run_dir,
            ("results.json", "point_summaries.json"),
            key="point_results_json",
            role="point_results_json",
            source="downstream_results_or_point_summaries_json",
            hard_required=True,
        ),
        "point_results_csv": _first_existing_artifact(
            run_dir,
            ("points.csv", "points_readable.csv"),
            key="point_results_csv",
            role="point_results_csv",
            source="downstream_points_or_points_readable_csv",
            hard_required=True,
        ),
        "samples_runtime": _first_existing_artifact(
            run_dir,
            ("samples.csv", "samples_runtime.csv"),
            key="samples_runtime",
            role="samples_runtime",
            source="downstream_samples_csv_or_samples_runtime_csv",
            hard_required=True,
        ),
        "pressure_trace_like": _first_existing_artifact(
            run_dir,
            ("route_pressure_sample_trace.json", "io_log.csv", "route_trace.jsonl"),
            key="pressure_trace_like",
            role="pressure_trace_like",
            source="downstream_route_pressure_sample_trace_or_io_log_or_route_trace",
            hard_required=True,
        ),
        "pressure_ready_trace_like": _first_existing_artifact(
            run_dir,
            ("route_pressure_sample_trace.json", "route_trace.jsonl"),
            key="pressure_ready_trace_like",
            role="pressure_ready_trace_like",
            source="downstream_route_pressure_sample_trace_or_route_trace",
            hard_required=False,
        ),
        "heartbeat_trace_like": _missing_artifact_entry(
            "heartbeat_trace_like",
            role="heartbeat_trace_like",
            source="no_dedicated_downstream_heartbeat_trace",
            hard_required=False,
            reason="not_available_or_not_generated",
        ),
        "analyzer_sampling_rows_like": _first_existing_artifact(
            run_dir,
            ("samples.csv", "samples_runtime.csv"),
            key="analyzer_sampling_rows_like",
            role="analyzer_sampling_rows_like",
            source="downstream_samples_csv_or_samples_runtime_csv_no_jsonl_source",
            hard_required=False,
        ),
    }


def _downstream_artifact_paths(artifact_map: Mapping[str, Mapping[str, Any]]) -> dict[str, str]:
    return {
        str(key): str(value.get("source_path") or "")
        for key, value in artifact_map.items()
        if str(value.get("source_path") or "")
    }


def _extract_downstream_run_dir_from_text(text: str) -> str:
    if not text:
        return ""
    patterns = (
        r"(?:downstream_execution_run_dir|execution_run_dir|run_dir|output_dir)\s*[:=]\s*[\"']?([^\"'\r\n]+)",
        r"(D:\\[^\r\n\"']*run_\d{8}[^\r\n\"']*)",
    )
    for pattern in patterns:
        match = re.search(pattern, text, flags=re.IGNORECASE)
        if match:
            candidate = match.group(1).strip().strip(",; ")
            if candidate:
                return candidate
    return ""


def _downstream_search_roots(probe_dir: Path) -> list[Path]:
    roots = [
        Path(r"D:\gas_calibrator\output\run001_h2o\1_point_no_write"),
        Path(r"D:\gas_calibrator\_handoff"),
        probe_dir,
    ]
    return list(dict.fromkeys(path.resolve() for path in roots))


def _is_downstream_candidate(path: Path, *, started_at: float) -> bool:
    if not path.is_dir():
        return False
    try:
        if path.stat().st_mtime < started_at - 5.0:
            return False
    except Exception:
        return False
    key_files = ("run.log", "summary.json", "route_trace.jsonl", "readiness.json", "no_write_guard.json")
    return any((path / name).exists() for name in key_files)


def _locate_downstream_run_dir(
    *,
    probe_dir: Path,
    started_at: float,
    stdout_text: str = "",
    stderr_text: str = "",
    execution: Optional[Mapping[str, Any]] = None,
) -> dict[str, Any]:
    search_roots = _downstream_search_roots(probe_dir)
    candidates: list[Path] = []
    checked: list[str] = []
    for value in (
        str((execution or {}).get("execution_run_dir") or "") if isinstance(execution, Mapping) else "",
        _extract_downstream_run_dir_from_text(stdout_text),
        _extract_downstream_run_dir_from_text(stderr_text),
    ):
        if value:
            candidates.append(Path(value).expanduser())
    for root in search_roots:
        checked.append(str(root))
        if not root.exists():
            continue
        glob_patterns = ("run_*", "**/run_*")
        for pattern in glob_patterns:
            for candidate in root.glob(pattern):
                if _is_downstream_candidate(candidate, started_at=started_at):
                    candidates.append(candidate)
    unique: list[Path] = []
    seen: set[str] = set()
    for candidate in candidates:
        try:
            resolved = candidate.resolve()
        except Exception:
            resolved = candidate
        key = str(resolved).lower()
        if key in seen:
            continue
        seen.add(key)
        unique.append(resolved)
    valid = [candidate for candidate in unique if candidate.exists() and candidate.is_dir()]
    valid.sort(key=lambda p: p.stat().st_mtime if p.exists() else 0.0, reverse=True)
    selected = valid[0] if valid else None
    return {
        "downstream_run_dir_locator_search_roots": checked,
        "downstream_run_dir_locator_candidates": [str(candidate) for candidate in unique],
        "downstream_run_dir_locator_selected": str(selected) if selected else "",
        "downstream_run_dir_locator_started_at": started_at,
        "downstream_run_dir_locator_completed_at": time.time(),
    }


def _hydrate_execution_from_run_dir(execution: Mapping[str, Any], run_dir: Path) -> dict[str, Any]:
    hydrated = dict(execution)
    hydrated["execution_run_dir"] = str(run_dir)
    hydrated.setdefault("run_manifest", _load_json_dict(run_dir / "run_manifest.json"))
    hydrated.setdefault("no_write_guard", _load_json_dict(run_dir / "no_write_guard.json"))
    hydrated.setdefault("workflow_timing_summary", _load_json_dict(run_dir / "workflow_timing_summary.json"))
    hydrated.setdefault("service_summary", _load_json_dict(run_dir / "summary.json"))
    hydrated.setdefault("route_trace_rows", _load_jsonl(run_dir / "route_trace.jsonl"))
    hydrated.setdefault("timing_trace_rows", _load_jsonl(run_dir / "workflow_timing_trace.jsonl"))
    hydrated.setdefault("io_log_rows", _load_csv_dicts(run_dir / "io_log.csv"))
    hydrated.setdefault("sample_rows", _load_csv_dicts(run_dir / "samples.csv"))
    run_log_path = run_dir / "run.log"
    if not hydrated.get("run_log_tail") and run_log_path.exists():
        try:
            hydrated["run_log_tail"] = run_log_path.read_text(encoding="utf-8", errors="ignore")[-20000:]
        except Exception:
            hydrated["run_log_tail"] = ""
    return hydrated


def _downstream_failed_before_sampling(service_summary: Optional[Mapping[str, Any]]) -> bool:
    if not isinstance(service_summary, Mapping):
        return False
    if int(service_summary.get("sample_count") or 0) > 0:
        return False
    if bool(service_summary.get("sample_completed")):
        return False
    reason = " ".join(
        str(service_summary.get(key) or "")
        for key in ("failure_reason", "a1_fail_reason", "service_status_error", "service_status_message")
    ).lower()
    if "route readiness failed" in reason or "route_ready" in reason or "before_sampling" in reason:
        return True
    if service_summary.get("pressure_completed") is False and int(service_summary.get("points_completed") or 0) == 0:
        return True
    return False


def _route_ready_failure(service_summary: Optional[Mapping[str, Any]], route_trace_rows: Optional[list[Mapping[str, Any]]] = None) -> dict[str, str]:
    rows = route_trace_rows if isinstance(route_trace_rows, list) else []
    for row in reversed(rows):
        message = str(row.get("message") or "")
        actual = row.get("actual") if isinstance(row.get("actual"), Mapping) else {}
        step = str(actual.get("step") or "")
        reason = str(actual.get("reason") or "")
        if row.get("action") == "wait_route_ready" or step:
            if "failure_step=" in message:
                step = message.split("failure_step=", 1)[1].split()[0].strip(";, ")
            if "reason=" in message:
                reason = message.split("reason=", 1)[1].split()[0].strip(";, ")
            if step or reason:
                return {"route_ready_failure_step": step, "route_ready_failure_reason": reason}
    reason_text = " ".join(
        str((service_summary or {}).get(key) or "")
        for key in ("failure_reason", "a1_fail_reason", "service_status_error", "service_status_message")
    )
    return {
        "route_ready_failure_step": "route_ready" if "route readiness failed" in reason_text.lower() else "",
        "route_ready_failure_reason": "H2O route readiness failed" if "route readiness failed" in reason_text else "",
    }


def _classify_downstream_engineering_green(
    service_summary: Mapping[str, Any],
    no_write_guard: Mapping[str, Any],
    any_write_command_sent: Any,
) -> bool:
    if not isinstance(service_summary, Mapping):
        return False
    points_completed = int(service_summary.get("points_completed") or 0)
    sample_count = int(service_summary.get("sample_count") or 0)
    route_completed = bool(service_summary.get("route_completed"))
    pressure_completed = bool(service_summary.get("pressure_completed"))
    wait_gate_completed = bool(service_summary.get("wait_gate_completed"))
    sample_completed = bool(service_summary.get("sample_completed"))
    service_status_phase = str(service_summary.get("service_status_phase") or "")
    service_status_error = str(service_summary.get("service_status_error") or "")
    if points_completed < 7:
        return False
    if sample_count < 1:
        return False
    if not route_completed:
        return False
    if not pressure_completed:
        return False
    if not wait_gate_completed:
        return False
    if not sample_completed:
        return False
    if service_status_phase != "completed":
        return False
    if service_status_error:
        return False
    if any_write_command_sent is True:
        return False
    if isinstance(no_write_guard, Mapping):
        attempted = int(no_write_guard.get("attempted_write_count") or 0)
        if attempted > 0:
            return False
    return True


def _artifact_completeness(
    artifact_paths: Mapping[str, str],
    *,
    assume_present_keys: Optional[set[str]] = None,
    downstream_artifact_map: Optional[Mapping[str, Mapping[str, Any]]] = None,
    before_sampling_failure: bool = False,
) -> dict[str, Any]:
    assumed = assume_present_keys or set()
    linked = downstream_artifact_map or {}
    expected: list[str] = []
    present: list[str] = []
    missing: list[str] = []
    present_keys: list[str] = []
    missing_keys: list[str] = []
    linked_keys: list[str] = []
    missing_reasons: dict[str, str] = {}
    for key in H2O_HANDOFF_REQUIRED_ARTIFACT_KEYS:
        path_text = artifact_paths.get(key, "")
        if not path_text:
            continue
        filename = Path(path_text).name
        expected.append(filename)
        if key in assumed or Path(path_text).exists():
            present.append(filename)
            present_keys.append(key)
        else:
            missing.append(filename)
            missing_keys.append(key)
            missing_reasons[key] = "handoff_required_artifact_missing"
    for key in H2O_DOWNSTREAM_BRIDGED_ARTIFACT_KEYS:
        entry = linked.get(key, {}) if isinstance(linked, Mapping) else {}
        source_path = str(entry.get("source_path") or "") if isinstance(entry, Mapping) else ""
        expected_name = Path(source_path).name if source_path else key
        expected.append(expected_name)
        if before_sampling_failure and key in {"samples_runtime", "point_results_json"}:
            present_keys.append(key)
            missing_reasons[key] = "expected_missing_due_to_before_sampling_failure"
            continue
        if isinstance(entry, Mapping) and entry.get("exists") and source_path and Path(source_path).exists():
            present.append(expected_name)
            present_keys.append(key)
            linked_keys.append(key)
        else:
            missing.append(expected_name)
            missing_keys.append(key)
            reason = str(entry.get("reason") or "artifact_contract_missing") if isinstance(entry, Mapping) else "artifact_contract_missing"
            missing_reasons[key] = reason
    if not missing:
        fail_reason = ""
    elif before_sampling_failure and not any(key not in {"samples_runtime", "point_results_json"} for key in missing_keys):
        fail_reason = "downstream_business_failed_before_sampling"
    elif any(missing_reasons.get(key) == "downstream_run_dir_missing" for key in missing_keys):
        fail_reason = "downstream_run_dir_missing"
    elif any(key in H2O_DOWNSTREAM_BRIDGED_ARTIFACT_KEYS for key in missing_keys):
        fail_reason = "artifact_bridge_incomplete"
    else:
        fail_reason = "artifact_contract_missing"
    return {
        "required_artifacts_expected": expected,
        "required_artifacts_present": present,
        "required_artifacts_missing": missing,
        "required_artifact_keys_present": present_keys,
        "required_artifact_keys_missing": missing_keys,
        "downstream_linked_artifact_keys_present": linked_keys,
        "required_artifact_missing_reasons": missing_reasons,
        "artifact_completeness_pass": not missing,
        "artifact_completeness_fail_reason": fail_reason,
        "missing_artifacts_expected_due_to_before_sampling": bool(before_sampling_failure),
    }


def _build_operator_record(
    admission: H2OAdmission,
    *,
    operator_confirmation_path: Optional[str | Path],
) -> dict[str, Any]:
    return {
        "schema_version": H2O_SCHEMA_VERSION,
        "record_type": "h2o_operator_confirmation_record",
        "operator_confirmation_path": (
            str(Path(operator_confirmation_path).expanduser().resolve())
            if operator_confirmation_path
            else ""
        ),
        "validation": admission.operator_validation,
        "payload": admission.operator_confirmation,
        **H2O_EVIDENCE_MARKERS,
    }


def _build_probe_admission_record(
    admission: H2OAdmission,
    *,
    artifact_paths: Mapping[str, str],
    config_path: str | Path,
    branch: str,
    head: str,
    cli_allow: bool,
    execute_probe: bool,
    run_app_py_untouched: bool,
) -> dict[str, Any]:
    return {
        "schema_version": H2O_SCHEMA_VERSION,
        "record_type": "h2o_probe_admission_record",
        "created_at": _now(),
        "admission_approved": bool(admission.approved),
        "admission_reasons": list(admission.reasons),
        "operator_confirmation_valid": bool(admission.operator_validation.get("valid")),
        "operator_validation": admission.operator_validation,
        "operator_confirmation": admission.operator_confirmation,
        "evidence": admission.evidence,
        "config_path": str(config_path or ""),
        "current_branch": branch,
        "current_head": head,
        "cli_allow": bool(cli_allow),
        "execute_probe_requested": bool(execute_probe),
        "run_app_py_untouched": bool(run_app_py_untouched),
        "artifact_paths": dict(artifact_paths),
        **H2O_EVIDENCE_MARKERS,
    }


def _partial_safety_assertions(
    *,
    real_probe_executed: bool,
    real_com_opened: Any,
    any_device_command_sent: Any,
    any_write_command_sent: Any,
    no_write_assertion_status: str,
    device_command_audit_complete: bool,
    must_not_claim_no_write_pass: bool,
    safe_stop_triggered: Any,
) -> dict[str, Any]:
    no_write_pass_like = str(no_write_assertion_status).startswith("pass")
    return {
        **H2O_EVIDENCE_MARKERS,
        **H2O_SAFETY_ASSERTION_DEFAULTS,
        "record_type": "h2o_safety_assertions_partial",
        "safety_assertions_complete": False,
        "real_probe_executed": bool(real_probe_executed),
        "real_com_opened": real_com_opened,
        "any_device_command_sent": any_device_command_sent,
        "any_write_command_sent": any_write_command_sent,
        "no_write": bool(no_write_pass_like and not must_not_claim_no_write_pass),
        "no_write_assertion_status": no_write_assertion_status,
        "device_command_audit_complete": bool(device_command_audit_complete),
        "must_not_claim_no_write_pass": bool(must_not_claim_no_write_pass),
        "safe_stop_triggered": safe_stop_triggered,
    }


def _final_safety_assertions(no_write_guard: Mapping[str, Any], *, real_probe_executed: bool, real_com_opened: Any, safe_stop_triggered: Any) -> dict[str, Any]:
    attempted = int(no_write_guard.get("attempted_write_count") or 0)
    identity = bool(no_write_guard.get("identity_write_command_sent", False))
    persistent = bool(no_write_guard.get("persistent_write_command_sent", False))
    blocked = list(no_write_guard.get("blocked_write_events") or []) if isinstance(no_write_guard.get("blocked_write_events"), list) else []
    any_write = bool(attempted or identity or persistent or blocked)
    safe = bool(no_write_guard.get("no_write") is True and not any_write)
    return {
        **H2O_EVIDENCE_MARKERS,
        **H2O_SAFETY_ASSERTION_DEFAULTS,
        "record_type": "h2o_safety_assertions_final",
        "safety_assertions_complete": True,
        "real_probe_executed": bool(real_probe_executed),
        "real_com_opened": real_com_opened,
        "any_device_command_sent": True,
        "attempted_write_count": attempted,
        "blocked_write_events": blocked,
        "any_write_command_sent": any_write,
        "identity_write_command_sent": identity,
        "persistent_write_command_sent": persistent,
        "no_write": safe,
        "no_write_assertion_status": "pass_engineering_evidence" if safe else "fail_write_evidence",
        "device_command_audit_complete": True,
        "must_not_claim_no_write_pass": False if safe else True,
        "safe_stop_triggered": safe_stop_triggered,
        "engineering_probe_only": True,
        "not_real_acceptance_evidence": True,
        "promotion_state": "blocked",
        "real_primary_latest_refresh": False,
    }


def _write_process_exit_record(
    run_dir: Path,
    *,
    artifact_paths: Mapping[str, str],
    process_state: str,
    final_decision: str,
    interrupted_execution: bool,
    interruption_source: str,
    interruption_stage: str,
    fail_closed_reason: str,
    execution_error: str,
    real_probe_executed: bool,
    real_com_opened: Any,
    any_device_command_sent: Any,
    any_write_command_sent: Any,
    safe_stop_triggered: Any,
    no_write_assertion_status: str,
    downstream_diagnostics: Optional[Mapping[str, Any]] = None,
    downstream_artifact_map: Optional[Mapping[str, Mapping[str, Any]]] = None,
    before_sampling_failure: bool = False,
) -> None:
    diagnostics = dict(downstream_diagnostics or {})
    bridge_map = downstream_artifact_map or {}
    completeness = _artifact_completeness(
        artifact_paths,
        assume_present_keys={"process_exit_record", "summary"},
        downstream_artifact_map=bridge_map,
        before_sampling_failure=before_sampling_failure,
    )
    payload = {
        "schema_version": H2O_SCHEMA_VERSION,
        "record_type": "h2o_process_exit_record",
        "updated_at": _now(),
        "process_state": process_state,
        "final_decision": final_decision,
        "fail_closed_reason": fail_closed_reason,
        "interrupted_execution": bool(interrupted_execution),
        "interruption_source": interruption_source,
        "interruption_stage": interruption_stage,
        "execution_error": execution_error,
        "real_probe_executed": bool(real_probe_executed),
        "real_com_opened": real_com_opened,
        "any_device_command_sent": any_device_command_sent,
        "any_write_command_sent": any_write_command_sent,
        "safe_stop_triggered": safe_stop_triggered,
        "no_write_assertion_status": no_write_assertion_status,
        "artifact_paths": dict(artifact_paths),
        **diagnostics,
        "downstream_artifact_map": dict(bridge_map),
        "downstream_artifact_paths": _downstream_artifact_paths(bridge_map),
        **completeness,
        **H2O_EVIDENCE_MARKERS,
    }
    _json_dump(run_dir / "process_exit_record.json", payload)


def _write_interrupted_guard_artifacts(
    run_dir: Path,
    *,
    admission: H2OAdmission,
    artifact_paths: Mapping[str, str],
    operator_confirmation_path: Optional[str | Path],
    config_path: str | Path,
    branch: str,
    head: str,
    cli_allow: bool,
    execute_probe: bool,
    run_app_py_untouched: bool,
    interruption_source: str,
    interruption_stage: str,
    real_probe_executed: bool,
    real_com_opened: Any,
    any_device_command_sent: Any,
    any_write_command_sent: Any,
    no_write_assertion_status: str,
    safe_stop_triggered: Any,
    device_command_audit_complete: bool,
    must_not_claim_no_write_pass: bool,
    execution_error: str = "",
) -> dict[str, Any]:
    operator_record = _build_operator_record(
        admission,
        operator_confirmation_path=operator_confirmation_path,
    )
    admission_record = _build_probe_admission_record(
        admission,
        artifact_paths=artifact_paths,
        config_path=config_path,
        branch=branch,
        head=head,
        cli_allow=cli_allow,
        execute_probe=execute_probe,
        run_app_py_untouched=run_app_py_untouched,
    )
    partial_safety = _partial_safety_assertions(
        real_probe_executed=real_probe_executed,
        real_com_opened=real_com_opened,
        any_device_command_sent=any_device_command_sent,
        any_write_command_sent=any_write_command_sent,
        no_write_assertion_status=no_write_assertion_status,
        device_command_audit_complete=device_command_audit_complete,
        must_not_claim_no_write_pass=must_not_claim_no_write_pass,
        safe_stop_triggered=safe_stop_triggered,
    )
    _json_dump(run_dir / "probe_admission_record.json", admission_record)
    _json_dump(run_dir / "operator_confirmation_record.json", operator_record)
    _json_dump(run_dir / "safety_assertions.json", partial_safety)
    _write_process_exit_record(
        run_dir,
        artifact_paths=artifact_paths,
        process_state="interrupted_guard_active",
        final_decision="FAIL_CLOSED",
        interrupted_execution=True,
        interruption_source=interruption_source,
        interruption_stage=interruption_stage,
        fail_closed_reason=H2O_INTERRUPTED_FAIL_CLOSED_REASON,
        execution_error=execution_error,
        real_probe_executed=real_probe_executed,
        real_com_opened=real_com_opened,
        any_device_command_sent=any_device_command_sent,
        any_write_command_sent=any_write_command_sent,
        safe_stop_triggered=safe_stop_triggered,
        no_write_assertion_status=no_write_assertion_status,
    )
    completeness = _artifact_completeness(artifact_paths, assume_present_keys={"summary"})
    summary = {
        "schema_version": H2O_SCHEMA_VERSION,
        **H2O_EVIDENCE_MARKERS,
        "final_decision": "FAIL_CLOSED",
        "fail_closed_reason": H2O_INTERRUPTED_FAIL_CLOSED_REASON,
        "interrupted_execution": True,
        "interrupted_at": _now(),
        "interruption_source": interruption_source,
        "interruption_stage": interruption_stage,
        "rejection_reasons": [H2O_INTERRUPTED_FAIL_CLOSED_REASON],
        "admission_approved": bool(admission.approved),
        "operator_confirmation_valid": bool(admission.operator_validation.get("valid")),
        "real_probe_executed": bool(real_probe_executed),
        "real_com_opened": real_com_opened,
        "any_device_command_sent": any_device_command_sent,
        "any_write_command_sent": any_write_command_sent,
        "safe_stop_triggered": safe_stop_triggered,
        "no_write_assertion_status": no_write_assertion_status,
        "safety_assertions_complete": False,
        "device_command_audit_complete": bool(device_command_audit_complete),
        "must_not_claim_no_write_pass": bool(must_not_claim_no_write_pass),
        "a3_allowed": False,
        "real_primary_latest_refresh": False,
        "execution_error": execution_error,
        "artifact_paths": dict(artifact_paths),
        **completeness,
    }
    _json_dump(run_dir / "summary.json", summary)
    return summary


def evaluate_h2o_1_point_no_write_gate(
    raw_cfg: Mapping[str, Any],
    *,
    cli_allow: bool = False,
    env: Optional[Mapping[str, str]] = None,
    operator_confirmation_path: Optional[str | Path] = None,
    branch: str = "",
    head: str = "",
    config_path: str = "",
    run_app_py_untouched: bool = True,
) -> H2OAdmission:
    env_map = os.environ if env is None else env
    reasons: list[str] = []
    if not cli_allow:
        reasons.append("missing_cli_flag_allow_v2_h2o_1_point_no_write_real_com")
    if str(env_map.get(H2O_ENV_VAR, "")).strip() != H2O_ENV_VALUE:
        reasons.append("missing_env_gas_cal_v2_h2o_1_point_no_write_real_com")

    operator_payload, operator_validation = _validate_operator_confirmation(
        operator_confirmation_path,
        expected_branch=branch,
        expected_head=head,
        expected_config_path=config_path,
    )

    reasons.extend(str(item) for item in operator_validation.get("errors", []))

    allowed_branches = _allowed_h2o_branches(raw_cfg)
    branch_allowed = not branch or branch in allowed_branches
    if branch and not branch_allowed:
        reasons.append("current_branch_not_allowed_for_h2o_1_point_no_write")
    if not str(head or "").strip():
        reasons.append("current_head_missing")
    if _scope(raw_cfg) not in {"run001_h2o_1_point", "h2o_only_1_point_no_write"}:
        reasons.append("config_scope_not_h2o_1_point_no_write")
    if not _truthy(raw_cfg, ("h2o_only", "run001_h2o_1_point.h2o_only")):
        reasons.append("config_not_h2o_only")
    if not _skip0_only(raw_cfg):
        reasons.append("config_not_skip0")
    if not _truthy(raw_cfg, ("single_route", "run001_h2o_1_point.single_route")):
        reasons.append("config_not_single_route")
    if not _single_temperature(raw_cfg):
        reasons.append("config_not_single_temperature")
    config_no_write = _truthy(
        raw_cfg,
        (
            "no_write",
            "run001_h2o_1_point.no_write",
            "h2o_only_1_point_no_write_probe.no_write",
        ),
    )
    if not config_no_write:
        reasons.append("config_no_write_not_true")
    if not _same_pressure_points(_pressure_points(raw_cfg)):
        reasons.append("config_pressure_points_not_exact_h2o_set")
    v1_fb = _truthy(
        raw_cfg,
        (
            "v1_fallback_required",
            "run001_h2o_1_point.v1_fallback_required",
            "run001_h2o_1_point.disable_v1",
        ),
    )
    if v1_fb is False and not _explicit_false(raw_cfg, ("run001_h2o_1_point.disable_v1",)):
        reasons.append("config_v1_fallback_required_not_true")
    elif not v1_fb and not _truthy(raw_cfg, ("run001_h2o_1_point.default_cutover_to_v2",)):
        pass
    elif not v1_fb:
        reasons.append("config_v1_fallback_required_not_true")
    if not run_app_py_untouched:
        reasons.append("run_app_py_not_untouched")

    false_required = {
        "a3_enabled": ("a3_enabled",),
        "co2_enabled": ("co2_enabled", "co2_only", "full_h2o_co2_group", "run001_h2o_1_point.full_h2o_co2_group"),
        "full_group_enabled": ("full_group_enabled", "full_h2o_co2_group", "run001_h2o_1_point.full_h2o_co2_group"),
        "multi_temperature_enabled": ("multi_temperature_enabled",),
        "mode_switch_enabled": ("mode_switch_enabled",),
        "analyzer_id_write_enabled": ("analyzer_id_write_enabled",),
        "senco_write_enabled": ("senco_write_enabled",),
        "calibration_write_enabled": ("calibration_write_enabled",),
        "chamber_set_temperature_enabled": ("chamber_set_temperature_enabled",),
        "chamber_start_enabled": ("chamber_start_enabled",),
        "chamber_stop_enabled": ("chamber_stop_enabled",),
        "real_primary_latest_refresh": ("real_primary_latest_refresh",),
    }
    for name, paths in false_required.items():
        if _explicit_false(raw_cfg, paths):
            continue
        if _truthy(raw_cfg, paths):
            reasons.append(f"config_{name}_not_disabled")
        elif _first_value(raw_cfg, paths) is not None:
            reasons.append(f"config_{name}_not_disabled")

    reasons = list(dict.fromkeys(reasons))
    safety_evidence = dict(H2O_SAFETY_ASSERTION_DEFAULTS)
    operator_ack = operator_payload.get("explicit_acknowledgement")
    operator_no_write_ack = (
        _as_bool(operator_ack.get("no_write")) is True
        if isinstance(operator_ack, Mapping)
        else False
    )
    write_flags_safe = (
        safety_evidence.get("attempted_write_count") == 0
        and safety_evidence.get("any_write_command_sent") is False
        and safety_evidence.get("identity_write_command_sent") is False
        and safety_evidence.get("mode_switch_command_sent") is False
        and safety_evidence.get("senco_write_command_sent") is False
        and safety_evidence.get("calibration_write_command_sent") is False
        and safety_evidence.get("chamber_write_register_command_sent") is False
        and safety_evidence.get("chamber_set_temperature_command_sent") is False
        and safety_evidence.get("chamber_start_command_sent") is False
        and safety_evidence.get("chamber_stop_command_sent") is False
        and safety_evidence.get("real_primary_latest_refresh") is False
    )
    no_write = bool(config_no_write and operator_no_write_ack and write_flags_safe)
    if not write_flags_safe:
        reasons.append("no_write_write_flags_not_safe")
    reasons = list(dict.fromkeys(reasons))
    approved = not reasons
    evidence = {
        **H2O_EVIDENCE_MARKERS,
        "admission_approved": approved,
        "a3_allowed": False,
        **safety_evidence,
        "config_no_write": bool(config_no_write),
        "operator_no_write_ack": bool(operator_no_write_ack),
        "write_flags_safe_for_no_write": bool(write_flags_safe),
        "no_write": no_write,
        "no_write_contract_source": "config_no_write && operator_no_write_ack && write_flags_safe_for_no_write",
        "rejection_reasons": reasons,
        "current_branch": branch,
        "allowed_branches": allowed_branches,
        "branch_allowed": branch_allowed,
    }
    return H2OAdmission(
        approved=approved,
        reasons=tuple(reasons),
        evidence=evidence,
        operator_confirmation=operator_payload,
        operator_validation=operator_validation,
    )


def prepare_h2o_downstream_points_config(
    raw_cfg: Mapping[str, Any],
    *,
    config_path: str | Path,
    output_dir: str | Path,
) -> tuple[Path, dict[str, Any]]:
    run_dir = Path(output_dir).expanduser().resolve()
    run_dir.mkdir(parents=True, exist_ok=True)
    generated = True
    rows = _build_h2o_downstream_point_rows(raw_cfg)
    reasons = _validate_h2o_downstream_point_rows(rows)
    if reasons:
        raise H2OPointsConfigAlignmentError("; ".join(reasons))
    points_path = run_dir / "h2o_1r_v1_aligned_points.json"
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
    pressure_cfg.setdefault("pressurize_high_hpa", 1013.25)
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
    aligned_config_path = run_dir / "h2o_1r_v1_aligned_downstream_config.json"
    _write_json_no_bom(aligned_config_path, aligned_raw_cfg)

    loaded_rows = rows
    loaded_reasons = _validate_h2o_downstream_point_rows(loaded_rows)
    if loaded_reasons:
        raise H2OPointsConfigAlignmentError("; ".join(loaded_reasons))

    point_pressures = [float(_point_row_pressure(row) or 0.0) for row in loaded_rows]
    point_routes = [str(row.get("route", "") or "").strip().lower() for row in loaded_rows]
    metadata = {
        "points_config_alignment_ready": True,
        "generated_points_json_path": str(points_path) if generated else "",
        "effective_points_json_path": str(points_path),
        "downstream_aligned_config_path": str(aligned_config_path),
        "downstream_points_row_count": len(loaded_rows),
        "downstream_point_routes": point_routes,
        "downstream_point_pressures_hpa": point_pressures,
        "downstream_points_gate_reasons": [],
        "downstream_points_generated": generated,
    }
    return aligned_config_path, metadata


def _shutdown_humidity_generator(service: Any) -> None:
    generator = service.device_manager.get_device("humidity_generator")
    if generator is None:
        return
    ser = getattr(generator, "ser", None)
    if ser is not None and not getattr(getattr(ser, "_ser", None), "is_open", True):
        return
    stopper = getattr(generator, "safe_stop", None)
    if callable(stopper):
        try:
            stopper()
        except Exception:
            pass
    waiter = getattr(generator, "wait_stopped", None)
    if callable(waiter):
        try:
            waiter(max_flow_lpm=0.05, timeout_s=30.0, poll_s=0.5)
        except Exception:
            pass


def execute_h2o_single_point_probe(config_path: str | Path) -> dict[str, Any]:
    from gas_calibrator.v2.core.no_write_guard import build_no_write_guard_from_raw_config
    from gas_calibrator.v2.entry import create_calibration_service_from_config, load_config_bundle

    resolved_config_path, raw_cfg, config = load_config_bundle(
        str(config_path),
        simulation_mode=False,
        allow_unsafe_step2_config=False,
        enforce_step2_execution_gate=False,
    )
    service = create_calibration_service_from_config(
        config,
        raw_cfg=raw_cfg,
        preload_points=True,
        require_no_write_guard=True,
    )
    build_no_write_guard_from_raw_config(raw_cfg)
    timeout_s = max(3600.0, float(raw_cfg.get("max_runtime_s", 3600.0)))

    _original_sigint = signal.getsignal(signal.SIGINT)

    def _on_first_interrupt(signum, frame):
        print("\n[H2O 探针] 收到中断信号，等待当前阶段安全完成...", flush=True)
        print("[H2O 探针] 再次按 Ctrl+C 可强制退出", flush=True)
        signal.signal(signal.SIGINT, _on_force_exit)

    def _on_force_exit(signum, frame):
        print("\n[H2O 探针] 强制退出", flush=True)
        sys.exit(1)

    signal.signal(signal.SIGINT, _on_first_interrupt)

    started = time.time()
    last_status = ""
    last_report = 0.0
    try:
        service.start()
        while True:
            elapsed = time.time() - started
            if service._done_event.wait(timeout=30.0):
                break
            if elapsed >= timeout_s:
                print(
                    f"\n[H2O 探针] 运行 {elapsed:.0f}s 超过总超时 {timeout_s:.0f}s，正在停止...",
                    flush=True,
                )
                break
            status = service.get_status()
            if status is None:
                continue
            phase_val = getattr(status.phase, "value", str(status.phase or "unknown"))
            phase_text = str(phase_val or "unknown")
            msg = str(status.message or "")
            status_text = f"phase={phase_text}"
            if msg:
                status_text += f" msg={msg[:150]}"
            if status_text != last_status:
                print(f"[H2O 探针] {status_text}", flush=True)
                last_status = status_text
                last_report = elapsed
            elif elapsed - last_report >= 120.0:
                print(f"[H2O 探针] 仍在运行 {elapsed:.0f}s: {status_text}", flush=True)
                last_report = elapsed
    except KeyboardInterrupt:
        print("\n[H2O 探针] KeyboardInterrupt，等待工作流安全结束...", flush=True)
        if not service._done_event.is_set():
            service._done_event.wait(timeout=60.0)
    finally:
        signal.signal(signal.SIGINT, _original_sigint)
        try:
            service.stop(wait=True, timeout=30.0)
        except Exception:
            pass
        _shutdown_humidity_generator(service)
    run_dir = Path(service.session.output_dir)
    summary = _load_json_dict(run_dir / "summary.json")
    run_log_text = ""
    run_log_path = run_dir / "run.log"
    if run_log_path.exists():
        try:
            run_log_text = run_log_path.read_text(encoding="utf-8", errors="ignore")[-20000:]
        except Exception:
            run_log_text = ""
    return {
        "execution_run_dir": str(run_dir),
        "underlying_config_path": str(resolved_config_path),
        "run_manifest": _load_json_dict(run_dir / "run_manifest.json"),
        "no_write_guard": _load_json_dict(run_dir / "no_write_guard.json"),
        "workflow_timing_summary": _load_json_dict(run_dir / "workflow_timing_summary.json"),
        "service_summary": summary,
        "route_trace_rows": _load_jsonl(run_dir / "route_trace.jsonl"),
        "timing_trace_rows": _load_jsonl(run_dir / "workflow_timing_trace.jsonl"),
        "io_log_rows": _load_csv_dicts(run_dir / "io_log.csv"),
        "sample_rows": _load_csv_dicts(run_dir / "samples.csv"),
        "run_log_tail": run_log_text,
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
    return load_guarded_jsonl(target, trace_name=None)


def _load_csv_dicts(path: str | Path | None) -> list[dict[str, Any]]:
    if path is None:
        return []
    target = Path(path)
    if not target.exists():
        return []
    import csv
    with target.open("r", encoding="utf-8", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def write_h2o_1_point_no_write_probe_artifacts(
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
) -> dict[str, Any]:
    admission = evaluate_h2o_1_point_no_write_gate(
        raw_cfg,
        cli_allow=cli_allow,
        env=env,
        operator_confirmation_path=operator_confirmation_path,
        branch=branch,
        head=head,
        config_path=str(config_path or ""),
        run_app_py_untouched=run_app_py_untouched,
    )
    if output_dir:
        run_dir = Path(output_dir).expanduser().resolve()
    else:
        timestamp = datetime.now().astimezone().strftime("%Y%m%d_%H%M")
        run_dir = Path(f"D:/gas_calibrator_step3a_h2o_1_point_no_write_probe_{timestamp}").resolve()
    run_dir.mkdir(parents=True, exist_ok=True)
    artifact_paths = {
        "probe_admission_record": str(run_dir / "probe_admission_record.json"),
        "summary": str(run_dir / "summary.json"),
        "route_trace": str(run_dir / "route_trace.jsonl"),
        "pressure_trace": str(run_dir / "pressure_trace.jsonl"),
        "pressure_ready_trace": str(run_dir / "pressure_ready_trace.jsonl"),
        "heartbeat_trace": str(run_dir / "heartbeat_trace.jsonl"),
        "analyzer_sampling_rows": str(run_dir / "analyzer_sampling_rows.jsonl"),
        "point_results": str(run_dir / "point_results.json"),
        "point_results_csv": str(run_dir / "point_results.csv"),
        "safety_assertions": str(run_dir / "safety_assertions.json"),
        "operator_confirmation_record": str(run_dir / "operator_confirmation_record.json"),
        "process_exit_record": str(run_dir / "process_exit_record.json"),
    }

    rejection_reasons = list(admission.reasons)
    executed = bool(admission.approved and execute_probe)
    execution_started = False
    execution_interrupted = False
    interruption_source = ""
    interruption_stage = ""
    execution_error = ""
    execution: dict[str, Any] = {}
    points_alignment: dict[str, Any] = {}
    downstream_exception_class = ""
    downstream_exception_message = ""
    downstream_exception_traceback = ""
    downstream_start_time = 0.0
    downstream_completed_at = 0.0
    downstream_stdout_text = ""
    downstream_stderr_text = ""
    downstream_process_record: dict[str, Any] = {}
    downstream_locator: dict[str, Any] = {}
    downstream_stdout_path = str(run_dir / "downstream_stdout.log")
    downstream_stderr_path = str(run_dir / "downstream_stderr.log")
    downstream_process_record_path = str(run_dir / "downstream_process_record.json")

    _write_interrupted_guard_artifacts(
        run_dir,
        admission=admission,
        artifact_paths=artifact_paths,
        operator_confirmation_path=operator_confirmation_path,
        config_path=config_path,
        branch=branch,
        head=head,
        cli_allow=cli_allow,
        execute_probe=execute_probe,
        run_app_py_untouched=run_app_py_untouched,
        interruption_source="pre_com_guard",
        interruption_stage="admission_output_dir_created",
        real_probe_executed=False,
        real_com_opened=False,
        any_device_command_sent=False,
        any_write_command_sent=False,
        no_write_assertion_status="pass_pre_com",
        safe_stop_triggered=False,
        device_command_audit_complete=True,
        must_not_claim_no_write_pass=False,
    )

    if not execute_probe:
        rejection_reasons.append("execute_probe_not_requested")
    elif not admission.approved:
        rejection_reasons.append("admission_not_approved")
    else:
        try:
            aligned_config_path, points_alignment = prepare_h2o_downstream_points_config(
                raw_cfg,
                config_path=config_path,
                output_dir=run_dir,
            )
        except Exception as exc:
            execution_error = str(exc)
            rejection_reasons.append(f"points_config_alignment_error:{exc}")
        else:
            _write_interrupted_guard_artifacts(
                run_dir,
                admission=admission,
                artifact_paths=artifact_paths,
                operator_confirmation_path=operator_confirmation_path,
                config_path=config_path,
                branch=branch,
                head=head,
                cli_allow=cli_allow,
                execute_probe=execute_probe,
                run_app_py_untouched=run_app_py_untouched,
                interruption_source="downstream_executor_started",
                interruption_stage="downstream_executor_started_real_com_status_unknown",
                real_probe_executed=True,
                real_com_opened="unknown",
                any_device_command_sent="unknown",
                any_write_command_sent="unknown",
                no_write_assertion_status="unknown",
                safe_stop_triggered="unknown",
                device_command_audit_complete=False,
                must_not_claim_no_write_pass=True,
            )
            execution_started = True
            downstream_start_time = time.time()
            downstream_process_record = {
                "schema_version": H2O_SCHEMA_VERSION,
                "record_type": "h2o_downstream_process_record",
                "downstream_start_time": downstream_start_time,
                "downstream_start_time_iso": _now(),
                "executor_type": "function_call",
                "callable": "execute_h2o_single_point_probe",
                "command": "execute_h2o_single_point_probe(aligned_config_path)",
                "cwd": str(Path.cwd()),
                "env_allow_flag": str((env or os.environ).get(H2O_ENV_VAR, "")),
                "output_dir": str(run_dir),
                "aligned_config_path": str(aligned_config_path),
                "expected_output_roots": [str(path) for path in _downstream_search_roots(run_dir)],
                "stdout_path": downstream_stdout_path,
                "stderr_path": downstream_stderr_path,
            }
            _json_dump(run_dir / "downstream_process_record.json", downstream_process_record)
            stdout_buffer = io.StringIO()
            stderr_buffer = io.StringIO()
            try:
                with contextlib.redirect_stdout(stdout_buffer), contextlib.redirect_stderr(stderr_buffer):
                    execution = execute_h2o_single_point_probe(aligned_config_path)
            except KeyboardInterrupt:
                execution_interrupted = True
                interruption_source = "KeyboardInterrupt"
                interruption_stage = "downstream_executor_keyboard_interrupt"
                execution_error = "KeyboardInterrupt"
                downstream_exception_class = "KeyboardInterrupt"
                downstream_exception_message = "KeyboardInterrupt"
                downstream_exception_traceback = traceback.format_exc()
                rejection_reasons.append(H2O_INTERRUPTED_FAIL_CLOSED_REASON)
            except TimeoutError as exc:
                execution_interrupted = True
                interruption_source = "TimeoutError"
                interruption_stage = "downstream_executor_timeout"
                execution_error = str(exc)
                downstream_exception_class = exc.__class__.__name__
                downstream_exception_message = str(exc)
                downstream_exception_traceback = traceback.format_exc()
                rejection_reasons.append(H2O_INTERRUPTED_FAIL_CLOSED_REASON)
            except Exception as exc:
                execution_interrupted = True
                interruption_source = exc.__class__.__name__
                interruption_stage = "downstream_executor_exception"
                execution_error = str(exc)
                downstream_exception_class = exc.__class__.__name__
                downstream_exception_message = str(exc)
                downstream_exception_traceback = traceback.format_exc()
                rejection_reasons.append(H2O_INTERRUPTED_FAIL_CLOSED_REASON)
                rejection_reasons.append(f"execution_error:{exc}")
            finally:
                downstream_completed_at = time.time()
                downstream_stdout_text = stdout_buffer.getvalue()
                downstream_stderr_text = stderr_buffer.getvalue()
                Path(downstream_stdout_path).write_text(downstream_stdout_text, encoding="utf-8")
                Path(downstream_stderr_path).write_text(downstream_stderr_text, encoding="utf-8")
                downstream_locator = _locate_downstream_run_dir(
                    probe_dir=run_dir,
                    started_at=downstream_start_time,
                    stdout_text=downstream_stdout_text,
                    stderr_text=downstream_stderr_text,
                    execution=execution if isinstance(execution, Mapping) else {},
                )
                selected_run_dir = str(downstream_locator.get("downstream_run_dir_locator_selected") or "")
                if selected_run_dir:
                    execution = _hydrate_execution_from_run_dir(execution if isinstance(execution, Mapping) else {}, Path(selected_run_dir))
                downstream_process_record.update(
                    {
                        "downstream_completed_at": downstream_completed_at,
                        "downstream_completed_at_iso": _now(),
                        "execution_interrupted": bool(execution_interrupted),
                        "execution_error": execution_error,
                        "downstream_exception_class": downstream_exception_class,
                        "downstream_exception_message": downstream_exception_message,
                        "execution_returned_mapping": isinstance(execution, Mapping) and bool(execution),
                        **downstream_locator,
                    }
                )
                _json_dump(run_dir / "downstream_process_record.json", downstream_process_record)

    service_summary = dict(execution.get("service_summary") or {}) if isinstance(execution, dict) else {}
    downstream_artifact_map = _build_downstream_artifact_map(execution if isinstance(execution, Mapping) else {})
    downstream_run_dir = str(execution.get("execution_run_dir") or "") if isinstance(execution, Mapping) else ""
    downstream_config_path = str(execution.get("underlying_config_path") or "") if isinstance(execution, Mapping) else ""
    downstream_run_id = str(service_summary.get("run_id") or (Path(downstream_run_dir).name if downstream_run_dir else ""))
    downstream_final_decision = str(service_summary.get("final_decision") or "")
    downstream_failure_reason = str(
        service_summary.get("fail_closed_reason")
        or service_summary.get("failure_reason")
        or service_summary.get("error")
        or ""
    )
    downstream_run_log_path = str(downstream_artifact_map.get("run_log", {}).get("source_path") or "")
    downstream_run_log_tail = str(execution.get("run_log_tail") or "") if isinstance(execution, Mapping) else ""
    if execution_started and not downstream_run_dir:
        rejection_reasons.append("downstream_run_dir_missing_after_executor")
    route_trace_rows = list(execution.get("route_trace_rows") or []) if isinstance(execution, Mapping) else []
    before_sampling_failure = bool(_downstream_failed_before_sampling(service_summary))
    route_ready_details = _route_ready_failure(service_summary, route_trace_rows)
    downstream_completed = bool(execution_started and not execution_interrupted and execution)
    downstream_diagnostics = {
        "downstream_execution_started": bool(execution_started),
        "downstream_execution_completed": downstream_completed,
        "downstream_execution_run_dir": downstream_run_dir,
        "execution_run_dir": downstream_run_dir,
        "downstream_config_path": downstream_config_path,
        "downstream_run_id": downstream_run_id,
        "downstream_final_decision": downstream_final_decision,
        "downstream_fail_closed_reason": str(service_summary.get("fail_closed_reason") or ""),
        "downstream_failure_reason": downstream_failure_reason,
        "downstream_run_log_path": downstream_run_log_path,
        "downstream_run_log_tail": downstream_run_log_tail,
        "downstream_service_summary": service_summary,
        "downstream_artifact_paths": _downstream_artifact_paths(downstream_artifact_map),
        "downstream_stdout_path": downstream_stdout_path if execution_started else "",
        "downstream_stderr_path": downstream_stderr_path if execution_started else "",
        "downstream_process_record_path": downstream_process_record_path if execution_started else "",
        "downstream_stdout_tail": downstream_stdout_text[-20000:],
        "downstream_stderr_tail": downstream_stderr_text[-20000:],
        **downstream_locator,
        "missing_artifacts_expected_due_to_before_sampling": before_sampling_failure,
        **route_ready_details,
        "downstream_exception_class": downstream_exception_class,
        "downstream_exception_message": downstream_exception_message,
        "downstream_exception_traceback": downstream_exception_traceback,
    }

    real_probe_executed = bool(execution_started and not execution_interrupted)
    real_com_opened: Any = False if not execution_started else ("unknown" if execution_interrupted else True)
    any_device_command_sent: Any = False if not execution_started else ("unknown" if execution_interrupted else True)
    any_write_command_sent: Any = False if not execution_started else ("unknown" if execution_interrupted else False)
    no_write_assertion: str = "pass_pre_com" if not execution_started else ("unknown" if execution_interrupted else "pass")
    safe_stop_triggered: Any = False if not execution_started else ("unknown" if execution_interrupted else False)
    device_command_audit: bool = True if not execution_started else (False if execution_interrupted else True)
    must_not_claim: bool = False if not execution_started else (True if execution_interrupted else False)

    no_write_guard = dict(execution.get("no_write_guard") or {}) if isinstance(execution, Mapping) else {}
    if execution_started and not execution_interrupted and no_write_guard:
        final_safety = _final_safety_assertions(
            no_write_guard,
            real_probe_executed=True,
            real_com_opened=True,
            safe_stop_triggered=safe_stop_triggered,
        )
        _json_dump(run_dir / "safety_assertions.json", final_safety)
        any_write_command_sent = final_safety["any_write_command_sent"]
        no_write_assertion = final_safety["no_write_assertion_status"]
        device_command_audit = final_safety["device_command_audit_complete"]
        must_not_claim = final_safety["must_not_claim_no_write_pass"]
    service_final = str(service_summary.get("final_decision") or "").upper()
    if execution_started and not execution_interrupted:
        if service_final == "PASS":
            final_decision = "PASS"
        elif _classify_downstream_engineering_green(service_summary, no_write_guard, any_write_command_sent):
            final_decision = "PASS"
            rejection_reasons[:] = [r for r in rejection_reasons if r != "downstream_business_failed" and not r.startswith("service_final_decision_")]
        else:
            final_decision = "FAIL_CLOSED"
            if "downstream_business_failed" not in rejection_reasons:
                rejection_reasons.append("downstream_business_failed")
            rejection_reasons.append(f"service_final_decision_{service_final.lower()}")
    elif execution_interrupted:
        final_decision = "FAIL_CLOSED"
    else:
        final_decision = "PASS" if admission.approved else "FAIL_CLOSED"
    completeness = _artifact_completeness(
        artifact_paths,
        downstream_artifact_map=downstream_artifact_map,
        before_sampling_failure=before_sampling_failure,
    )
    if not completeness.get("artifact_completeness_pass"):
        reason = str(completeness.get("artifact_completeness_fail_reason") or "artifact_contract_missing")
        if reason not in rejection_reasons:
            rejection_reasons.append(reason)
    summary = {
        "schema_version": H2O_SCHEMA_VERSION,
        **H2O_EVIDENCE_MARKERS,
        "final_decision": final_decision,
        "admission_approved": bool(admission.approved),
        "operator_confirmation_valid": bool(admission.operator_validation.get("valid")),
        "rejection_reasons": rejection_reasons,
        "execute_probe_requested": bool(execute_probe),
        "real_probe_executed": real_probe_executed,
        "real_com_opened": real_com_opened,
        "any_device_command_sent": any_device_command_sent,
        "any_write_command_sent": any_write_command_sent,
        "safe_stop_triggered": safe_stop_triggered,
        "no_write_assertion_status": no_write_assertion,
        "safety_assertions_complete": device_command_audit,
        "device_command_audit_complete": device_command_audit,
        "must_not_claim_no_write_pass": must_not_claim,
        "a3_allowed": False,
        "real_primary_latest_refresh": False,
        "artifact_paths": dict(artifact_paths),
        **downstream_diagnostics,
        "downstream_artifact_map": downstream_artifact_map,
        "downstream_artifact_paths": _downstream_artifact_paths(downstream_artifact_map),
        **completeness,
    }
    fail_closed_reason = ""
    if final_decision == "FAIL_CLOSED":
        if "downstream_run_dir_missing_after_executor" in rejection_reasons:
            fail_closed_reason = "downstream_run_dir_missing_after_executor"
        elif "downstream_business_failed" in rejection_reasons:
            fail_closed_reason = "downstream_business_failed"
        else:
            fail_closed_reason = str(completeness.get("artifact_completeness_fail_reason") or execution_error or H2O_INTERRUPTED_FAIL_CLOSED_REASON)
    summary["fail_closed_reason"] = fail_closed_reason
    _json_dump(run_dir / "summary.json", summary)
    _write_process_exit_record(
        run_dir,
        artifact_paths=artifact_paths,
        process_state="completed" if not execution_interrupted else "interrupted",
        final_decision=final_decision,
        interrupted_execution=bool(execution_interrupted),
        interruption_source=interruption_source,
        interruption_stage=interruption_stage,
        fail_closed_reason=fail_closed_reason,
        execution_error=execution_error,
        real_probe_executed=real_probe_executed,
        real_com_opened=real_com_opened,
        any_device_command_sent=any_device_command_sent,
        any_write_command_sent=any_write_command_sent,
        safe_stop_triggered=safe_stop_triggered,
        no_write_assertion_status=no_write_assertion,
        downstream_diagnostics=downstream_diagnostics,
        downstream_artifact_map=downstream_artifact_map,
        before_sampling_failure=before_sampling_failure,
    )
    try:
        from gas_calibrator.v2.storage.indexer import index_run
        index_run(str(run_dir))
    except Exception:
        pass
    return summary
