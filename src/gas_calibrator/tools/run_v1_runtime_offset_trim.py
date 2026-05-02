from __future__ import annotations

import argparse
import csv
import json
import math
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence

from ..coefficients.write_readiness import summarize_runtime_standard_validation
from ..devices.gas_analyzer import GasAnalyzer
from ..senco_format import format_senco_value, senco_readback_matches
from .run_v1_corrected_autodelivery import _TranscriptIoLogger
from .runtime_probe_helper import _build_capture_row, _compute_stats, _safe_float, _write_csv


def _normalize_device_id(value: Any) -> str:
    text = str(value or "").strip().upper()
    if not text:
        raise ValueError("device_id is required")
    if text.isdigit():
        return f"{int(text):03d}"
    return text


def _read_csv(path: Path) -> List[Dict[str, Any]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")


def _write_markdown(path: Path, lines: Sequence[str]) -> None:
    path.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")


def _parse_senco_command(command: str) -> tuple[int, List[float]]:
    parts = [part.strip() for part in str(command or "").split(",") if str(part).strip()]
    if len(parts) < 4 or not parts[0].upper().startswith("SENCO"):
        raise ValueError(f"invalid SENCO command: {command}")
    return int(parts[0].upper().replace("SENCO", "")), [float(part) for part in parts[3:]]


def _build_senco_command(group: int, coefficients: Sequence[float]) -> str:
    payload = ",".join(format_senco_value(value) for value in coefficients)
    return f"SENCO{int(group)},YGAS,FFF,{payload}"


def _ordered_coefficients(coefficients: Mapping[str, Any], expected_len: int) -> List[float | None]:
    values: List[float | None] = []
    for index in range(expected_len):
        raw = coefficients.get(f"C{index}")
        values.append(None if raw is None else float(raw))
    return values


def _summarize_capture(
    *,
    rows: Sequence[Mapping[str, Any]],
    target_ppm: float,
    capture_seconds: float,
    port: str,
    device_id: str,
) -> Dict[str, Any]:
    co2_stats = _compute_stats([_safe_float(row.get("co2_ppm")) for row in rows])
    temp_stats = _compute_stats([_safe_float(row.get("temp_c")) for row in rows])
    pressure_stats = _compute_stats([_safe_float(row.get("pressure_kpa")) for row in rows])
    residual = None if not co2_stats else float(co2_stats["mean"]) - float(target_ppm)
    return {
        "port": str(port),
        "device_id": _normalize_device_id(device_id),
        "capture_seconds": float(capture_seconds),
        "target_ppm": float(target_ppm),
        "valid_frame_count": len(rows),
        "measured_mean": None if not co2_stats else float(co2_stats["mean"]),
        "measured_median": None if not co2_stats else float(co2_stats["median"]),
        "std": None if not co2_stats else float(co2_stats["std"]),
        "span": None if not co2_stats else float(co2_stats["span"]),
        "residual": residual,
        "co2_ppm_stats": co2_stats,
        "temp_c_stats": temp_stats,
        "pressure_kpa_stats": pressure_stats,
        "status": "ok" if rows else "no_valid_frames",
    }


def _classify_residual(residual: Optional[float]) -> Dict[str, Any]:
    if residual is None or not math.isfinite(float(residual)):
        return {
            "code": "not_executed",
            "label": "not_executed",
            "passed": False,
        }
    abs_residual = abs(float(residual))
    if abs_residual <= 20.0:
        return {"code": "pass", "label": "pass", "passed": True}
    if abs_residual <= 80.0:
        return {"code": "review", "label": "review", "passed": False}
    return {"code": "fail", "label": "fail", "passed": False}


def _capture_runtime_point(
    *,
    port: str,
    device_id: str,
    target_ppm: float,
    capture_seconds: float,
    csv_path: Path,
    device_factory: Any = GasAnalyzer,
    baudrate: int = 115200,
    timeout: float = 0.3,
    poll_interval_s: float = 0.01,
    allow_passive_fallback: bool = True,
) -> tuple[List[Dict[str, Any]], Dict[str, Any], List[str]]:
    rows: List[Dict[str, Any]] = []
    raw_lines: List[str] = []
    device_id_norm = _normalize_device_id(device_id)
    device = device_factory(
        str(port),
        int(baudrate),
        timeout=float(timeout),
        device_id=device_id_norm,
    )
    deadline = time.monotonic() + max(0.05, float(capture_seconds))

    try:
        device.open()
        while time.monotonic() <= deadline:
            raw_line = str(
                device.read_latest_data(
                    prefer_stream=True,
                    drain_s=0.25,
                    read_timeout_s=0.05,
                    allow_passive_fallback=bool(allow_passive_fallback),
                )
                or ""
            ).strip()
            if not raw_line:
                if poll_interval_s > 0:
                    time.sleep(float(poll_interval_s))
                continue
            parsed = device.parse_line(raw_line)
            if not isinstance(parsed, Mapping) or not parsed:
                if poll_interval_s > 0:
                    time.sleep(float(poll_interval_s))
                continue
            parsed_device_id = _normalize_device_id(parsed.get("id") or parsed.get("device_id") or device_id_norm)
            if parsed_device_id != device_id_norm:
                if poll_interval_s > 0:
                    time.sleep(float(poll_interval_s))
                continue
            mode_value = int(_safe_float(parsed.get("mode")) or 1)
            if mode_value != 1:
                if poll_interval_s > 0:
                    time.sleep(float(poll_interval_s))
                continue
            timestamp = datetime.now().astimezone().isoformat(timespec="milliseconds")
            rows.append(_build_capture_row(parsed, raw_line, timestamp))
            raw_lines.append(raw_line)
            if poll_interval_s > 0:
                time.sleep(float(poll_interval_s))
    finally:
        try:
            device.close()
        except Exception:
            pass

    _write_csv(csv_path, rows)
    summary = _summarize_capture(
        rows=rows,
        target_ppm=float(target_ppm),
        capture_seconds=float(capture_seconds),
        port=port,
        device_id=device_id_norm,
    )
    return rows, summary, raw_lines


def _load_candidate_co2_targets(candidate_dir: Path, device_id: str) -> Dict[str, Any]:
    device_id_norm = _normalize_device_id(device_id)
    download_rows = _read_csv(candidate_dir / "download_plan_no_500.csv")
    matches = [
        row
        for row in download_rows
        if _normalize_device_id(row.get("ActualDeviceId")) == device_id_norm
        and str(row.get("Gas") or "").strip().upper() == "CO2"
    ]
    if len(matches) != 1:
        raise RuntimeError(f"expected exactly one CO2 candidate row for device_id={device_id_norm}, got {len(matches)}")
    row = matches[0]
    groups: Dict[int, List[float]] = {}
    commands: Dict[int, str] = {}
    for key in ("PrimaryCommand", "SecondaryCommand"):
        command = str(row.get(key) or "").strip()
        if not command:
            continue
        group, coefficients = _parse_senco_command(command)
        groups[int(group)] = list(coefficients)
        commands[int(group)] = command
    if 1 not in groups or 3 not in groups:
        raise RuntimeError("candidate_dir must provide both CO2 SENCO1 and SENCO3 targets")
    return {
        "candidate_dir": str(candidate_dir),
        "device_id": device_id_norm,
        "row": row,
        "groups": groups,
        "commands": commands,
    }


def _strict_readback_group(
    ga: Any,
    *,
    group: int,
    device_id: str,
    expected: Sequence[float],
    retries: int = 10,
) -> Dict[str, Any]:
    capture = dict(
        ga.read_coefficient_group_capture(
            int(group),
            target_id=device_id,
            retries=max(0, int(retries) - 1),
            command_style="parameterized",
            prepare_io=True,
        )
    )
    actual_values = _ordered_coefficients(dict(capture.get("coefficients") or {}), len(expected))
    explicit_c0 = str(capture.get("source") or "") == GasAnalyzer.READBACK_SOURCE_EXPLICIT_C0
    matches_candidate = bool(explicit_c0) and senco_readback_matches(expected, actual_values)
    deltas = [
        abs(float(got) - float(exp))
        for exp, got in zip(expected, actual_values)
        if got is not None and math.isfinite(float(exp))
    ]
    return {
        "group": int(group),
        "command": str(capture.get("command") or ""),
        "attempt_count": int(capture.get("attempts") or 0),
        "selected_attempt": int(capture.get("attempt_index") or 0),
        "source": str(capture.get("source") or ""),
        "explicit_c0": bool(explicit_c0),
        "source_line": str(capture.get("source_line") or ""),
        "readback_value": actual_values,
        "target_value": [float(value) for value in expected],
        "matches_candidate": bool(matches_candidate),
        "max_abs_delta": max(deltas) if deltas else 0.0,
        "error": str(capture.get("error") or ""),
        "raw_transcript_lines": list(capture.get("raw_transcript_lines") or []),
        "attempt_transcripts": list(capture.get("attempt_transcripts") or []),
    }


def _build_output_dir(candidate_dir: Path, device_id: str, target_ppm: float, prefix: str) -> Path:
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    suffix = int(round(float(target_ppm)))
    output_dir = candidate_dir / f"{prefix}_{_normalize_device_id(device_id)}_{suffix}ppm_{stamp}"
    output_dir.mkdir(parents=True, exist_ok=True)
    return output_dir


def _build_post_offset_candidate(
    *,
    candidate_dir: Path,
    source_output_dir: Path,
    device_id: str,
    current_port: str,
    old_a0: float,
    new_a0: float,
    correction_delta_a0: float,
    target_ppm: float,
    measured_after: float,
    residual_after: float,
    strict_explicit_c0_verified: bool,
    high_point_checked: bool,
) -> Dict[str, Any]:
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = candidate_dir / f"post_offset_candidate_{_normalize_device_id(device_id)}_{stamp}"
    output_dir.mkdir(parents=True, exist_ok=True)
    source_plan_rows = _read_csv(candidate_dir / "download_plan_no_500.csv")
    fieldnames = list(source_plan_rows[0].keys()) if source_plan_rows else []
    updated_rows = [dict(row) for row in source_plan_rows]
    device_id_norm = _normalize_device_id(device_id)

    for row in updated_rows:
        if _normalize_device_id(row.get("ActualDeviceId")) != device_id_norm:
            continue
        if str(row.get("Gas") or "").strip().upper() != "CO2":
            continue
        group, coefficients = _parse_senco_command(str(row.get("PrimaryCommand") or ""))
        if int(group) != 1:
            continue
        coefficients[0] = float(new_a0)
        row["PrimaryValues"] = ",".join(format_senco_value(value) for value in coefficients)
        row["PrimaryCommand"] = _build_senco_command(1, coefficients)
        row["PrimaryC0"] = format_senco_value(float(new_a0))
        row["a0"] = str(float(new_a0))
        break

    new_plan_path = output_dir / "download_plan_no_500.csv"
    with new_plan_path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(updated_rows)

    if (candidate_dir / "temperature_coefficients_target.csv").exists():
        (output_dir / "temperature_coefficients_target.csv").write_text(
            (candidate_dir / "temperature_coefficients_target.csv").read_text(encoding="utf-8-sig"),
            encoding="utf-8-sig",
        )

    runtime_validation = summarize_runtime_standard_validation(
        {
            "offset_trim_status": "pass",
            "high_point_rows": [] if not high_point_checked else [{"target_ppm": target_ppm, "verdict": "pass"}],
        }
    )
    summary_rows = [
        {
            "device_id": device_id_norm,
            "current_port": str(current_port),
            "old_a0": float(old_a0),
            "new_a0": float(new_a0),
            "correction_delta_a0": float(correction_delta_a0),
            "correction_basis": "runtime_standard_gas_single_point_offset",
            "target_ppm": float(target_ppm),
            "measured_after": float(measured_after),
            "residual_after": float(residual_after),
            "strict_explicit_c0_verified": bool(strict_explicit_c0_verified),
            "retained": True,
            "full_range_verified": False,
            "high_point_checked": bool(high_point_checked),
            "final_write_ready": False,
            "final_write_ready_reason": "high-point confirmation and runtime full-range parity are not both closed",
            "runtime_standard_validation_status": runtime_validation["status"],
        }
    ]
    summary_path = output_dir / "post_offset_coefficients_summary.csv"
    with summary_path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(summary_rows[0].keys()))
        writer.writeheader()
        writer.writerows(summary_rows)

    manifest = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "device_id": device_id_norm,
        "current_port": str(current_port),
        "source_candidate_dir": str(candidate_dir),
        "source_runtime_offset_trim_dir": str(source_output_dir),
        "old_a0": float(old_a0),
        "new_a0": float(new_a0),
        "correction_delta_a0": float(correction_delta_a0),
        "correction_basis": "runtime_standard_gas_single_point_offset",
        "target_ppm": float(target_ppm),
        "measured_after": float(measured_after),
        "residual_after": float(residual_after),
        "strict_explicit_c0_verified": bool(strict_explicit_c0_verified),
        "retained": True,
        "full_range_verified": False,
        "high_point_checked": bool(high_point_checked),
        "runtime_standard_validation_status": runtime_validation["status"],
        "final_write_ready": False,
        "final_write_ready_reason": "high-point confirmation and runtime full-range parity are not both closed",
    }
    manifest_path = output_dir / "post_offset_manifest.json"
    _write_json(manifest_path, manifest)

    report_lines = [
        "# post-offset candidate",
        "",
        f"- device_id: {device_id_norm}",
        f"- current_port: {current_port}",
        f"- old_a0: {float(old_a0)}",
        f"- new_a0: {float(new_a0)}",
        f"- correction_delta_a0: {float(correction_delta_a0)}",
        "- correction_basis: runtime_standard_gas_single_point_offset",
        f"- target_ppm: {float(target_ppm)}",
        f"- measured_after: {float(measured_after)}",
        f"- residual_after: {float(residual_after)}",
        f"- strict_explicit_c0_verified: {bool(strict_explicit_c0_verified)}",
        f"- full_range_verified: False",
        f"- high_point_checked: {bool(high_point_checked)}",
        f"- final_write_ready: False",
    ]
    _write_markdown(output_dir / "post_offset_report.md", report_lines)
    return {
        "output_dir": str(output_dir),
        "download_plan_path": str(new_plan_path),
        "manifest_path": str(manifest_path),
        "summary_path": str(summary_path),
    }


def run_high_point_check(
    *,
    port: str,
    device_id: str,
    candidate_dir: str | Path,
    target_ppm: float,
    capture_duration_s: float = 120.0,
    output_dir: str | Path | None = None,
    gas: str = "co2",
    device_factory: Any = GasAnalyzer,
) -> Dict[str, Any]:
    if str(gas or "").strip().lower() != "co2":
        raise ValueError("run_high_point_check currently supports gas=co2 only")

    candidate_dir_path = Path(candidate_dir).resolve()
    target_dir = Path(output_dir).resolve() if output_dir else _build_output_dir(
        candidate_dir_path,
        device_id,
        target_ppm,
        "high_point_check",
    )
    rows, summary, raw_lines = _capture_runtime_point(
        port=port,
        device_id=device_id,
        target_ppm=float(target_ppm),
        capture_seconds=float(capture_duration_s),
        csv_path=target_dir / "high_point_stream.csv",
        device_factory=device_factory,
    )
    verdict = _classify_residual(summary.get("residual"))
    payload = {
        **summary,
        "high_point_verdict": verdict["code"],
        "retain_a0_19707_1_recommended": verdict["code"] in {"pass", "review"},
        "post_offset_candidate_dir": str(candidate_dir_path),
    }
    _write_json(target_dir / "high_point_summary.json", payload)
    report_lines = [
        "# runtime high-point check",
        "",
        f"- port: {port}",
        f"- device_id: {_normalize_device_id(device_id)}",
        f"- target_ppm: {float(target_ppm)}",
        f"- measured_mean: {payload.get('measured_mean')}",
        f"- measured_median: {payload.get('measured_median')}",
        f"- std: {payload.get('std')}",
        f"- span: {payload.get('span')}",
        f"- residual: {payload.get('residual')}",
        f"- valid_frame_count: {payload.get('valid_frame_count')}",
        f"- verdict: {payload.get('high_point_verdict')}",
        "",
        "- read-only high-point confirmation only",
    ]
    _write_markdown(target_dir / "high_point_report.md", report_lines)
    (target_dir / "raw_transcript.log").write_text("\n".join(raw_lines).rstrip() + "\n", encoding="utf-8")
    return {
        "output_dir": str(target_dir),
        "summary": payload,
    }


def run_from_cli(
    *,
    port: str,
    device_id: str,
    candidate_dir: str | Path,
    target_ppm: float,
    gas: str = "co2",
    capture_duration_s: float = 120.0,
    response_slope: float = 1.0,
    execute: bool = False,
    dry_run: bool = True,
    output_dir: str | Path | None = None,
    device_factory: Any = GasAnalyzer,
) -> Dict[str, Any]:
    if str(gas or "").strip().lower() != "co2":
        raise ValueError("run_v1_runtime_offset_trim currently supports gas=co2 only")

    candidate_dir_path = Path(candidate_dir).resolve()
    target_dir = Path(output_dir).resolve() if output_dir else _build_output_dir(
        candidate_dir_path,
        device_id,
        target_ppm,
        "runtime_offset_trim",
    )
    candidate = _load_candidate_co2_targets(candidate_dir_path, device_id)
    expected_group1 = list(candidate["groups"][1])
    expected_group3 = list(candidate["groups"][3])

    raw_sections: List[str] = []
    def _append_section(title: str, lines: Sequence[str]) -> None:
        raw_sections.append(f"=== {title} ===")
        raw_sections.extend(str(line) for line in lines)
        raw_sections.append("")

    prewrite_rows: List[Dict[str, Any]] = []
    io_logger = _TranscriptIoLogger()
    ga = device_factory(
        str(port),
        115200,
        timeout=0.6,
        device_id=_normalize_device_id(device_id),
        io_logger=io_logger,
    )
    try:
        ga.open()
        group1_readback = _strict_readback_group(
            ga,
            group=1,
            device_id=device_id,
            expected=expected_group1,
        )
        group3_readback = _strict_readback_group(
            ga,
            group=3,
            device_id=device_id,
            expected=expected_group3,
        )
    finally:
        try:
            ga.close()
        except Exception:
            pass

    prewrite_rows.extend([group1_readback, group3_readback])
    _append_section(
        "strict_prewrite_readback",
        [json.dumps(row, ensure_ascii=False, indent=2, default=str) for row in prewrite_rows]
        + [json.dumps(row, ensure_ascii=False, default=str) for row in io_logger.rows],
    )

    pre_capture_rows, pre_summary, pre_raw_lines = _capture_runtime_point(
        port=port,
        device_id=device_id,
        target_ppm=float(target_ppm),
        capture_seconds=float(capture_duration_s),
        csv_path=target_dir / "runtime_offset_trim_pre_capture.csv",
        device_factory=device_factory,
    )
    residual_before = pre_summary.get("residual")
    _append_section("runtime_pre_capture", list(pre_raw_lines))

    strict_prewrite_verified = all(
        bool(row.get("explicit_c0")) and bool(row.get("matches_candidate")) for row in prewrite_rows
    )
    old_a0 = float(group1_readback["readback_value"][0]) if group1_readback["readback_value"][0] is not None else float(expected_group1[0])

    if not math.isfinite(float(response_slope)) or abs(float(response_slope)) <= 1e-12:
        correction_delta_a0 = None
        new_a0 = None
        planning_error = "response_slope must be finite and non-zero"
    elif residual_before is None:
        correction_delta_a0 = None
        new_a0 = None
        planning_error = "runtime pre-capture did not produce a valid measured_mean"
    else:
        correction_delta_a0 = -float(residual_before) / float(response_slope)
        new_a0_raw = float(old_a0) + float(correction_delta_a0)
        new_a0 = float(format_senco_value(new_a0_raw))
        planning_error = ""

    plan = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "candidate_dir": str(candidate_dir_path),
        "output_dir": str(target_dir),
        "port": str(port),
        "device_id": _normalize_device_id(device_id),
        "gas": "co2",
        "target_ppm": float(target_ppm),
        "capture_duration_s": float(capture_duration_s),
        "response_slope": float(response_slope),
        "strict_prewrite_verified": bool(strict_prewrite_verified),
        "prewrite_groups": prewrite_rows,
        "old_a0": float(old_a0),
        "residual_before": residual_before,
        "correction_delta_a0": correction_delta_a0,
        "new_a0": new_a0,
        "planning_error": planning_error,
        "whether_execute": bool(execute),
        "dry_run": bool(dry_run),
    }
    _write_json(target_dir / "runtime_offset_trim_plan.json", plan)
    _write_json(target_dir / "runtime_offset_trim_pre_summary.json", pre_summary)

    writeback_payload: Dict[str, Any] = {
        "execute_requested": bool(execute),
        "executed": False,
        "strict_prewrite_verified": bool(strict_prewrite_verified),
        "strict_explicit_c0_verified": False,
        "old_a0": float(old_a0),
        "response_slope": float(response_slope),
        "residual_before": residual_before,
        "correction_delta_a0": correction_delta_a0,
        "new_a0": new_a0,
        "block_reason": "",
        "post_offset_candidate_dir": "",
    }
    post_rows: List[Dict[str, Any]] = []
    post_summary: Dict[str, Any] = {
        "port": str(port),
        "device_id": _normalize_device_id(device_id),
        "target_ppm": float(target_ppm),
        "status": "not_executed",
        "measured_mean": None,
        "measured_median": None,
        "std": None,
        "span": None,
        "residual_after": None,
        "whether_passed": False,
    }

    if not execute:
        writeback_payload["block_reason"] = "execute=false"
        _write_csv(target_dir / "runtime_offset_trim_post_capture.csv", post_rows)
        _write_json(target_dir / "runtime_offset_trim_writeback.json", writeback_payload)
        _write_json(target_dir / "runtime_offset_trim_post_summary.json", post_summary)
        report_lines = [
            "# runtime offset trim",
            "",
            f"- port: {port}",
            f"- device_id: {_normalize_device_id(device_id)}",
            f"- target_ppm: {float(target_ppm)}",
            f"- old_a0: {float(old_a0)}",
            f"- response_slope: {float(response_slope)}",
            f"- residual_before: {residual_before}",
            f"- correction_delta_a0: {correction_delta_a0}",
            f"- new_a0: {new_a0}",
            "- strict_explicit_c0_verified: False",
            "- whether_execute: False",
            "- whether_passed: False",
        ]
        _write_markdown(target_dir / "runtime_offset_trim_report.md", report_lines)
        (target_dir / "raw_transcript.log").write_text("\n".join(raw_sections).rstrip() + "\n", encoding="utf-8")
        return {
            "output_dir": str(target_dir),
            "plan_path": str(target_dir / "runtime_offset_trim_plan.json"),
            "pre_summary_path": str(target_dir / "runtime_offset_trim_pre_summary.json"),
            "writeback_path": str(target_dir / "runtime_offset_trim_writeback.json"),
            "post_summary_path": str(target_dir / "runtime_offset_trim_post_summary.json"),
            "executed": False,
        }

    if not strict_prewrite_verified:
        writeback_payload["block_reason"] = "strict explicit-C0 prewrite readback did not match the candidate"
    elif planning_error:
        writeback_payload["block_reason"] = planning_error
    elif new_a0 is None:
        writeback_payload["block_reason"] = "new_a0 could not be computed"
    else:
        write_io_logger = _TranscriptIoLogger()
        write_ga = device_factory(
            str(port),
            115200,
            timeout=0.6,
            device_id=_normalize_device_id(device_id),
            io_logger=write_io_logger,
        )
        target_group1 = list(group1_readback["readback_value"])
        target_group1[0] = float(new_a0)
        mode2_ack = False
        mode1_ack = False
        try:
            write_ga.open()
            mode2_ack = bool(write_ga.set_mode_with_ack(2, require_ack=True))
            if not mode2_ack:
                writeback_payload["block_reason"] = "MODE=2 not acknowledged"
            else:
                write_ack = bool(write_ga.set_senco(1, target_group1))
                writeback_payload["write_ack"] = write_ack
                if not write_ack:
                    writeback_payload["block_reason"] = "SENCO1 write not acknowledged"
                else:
                    verify_row = _strict_readback_group(
                        write_ga,
                        group=1,
                        device_id=device_id,
                        expected=target_group1,
                        retries=3,
                    )
                    writeback_payload["verify_group"] = verify_row
                    writeback_payload["strict_explicit_c0_verified"] = bool(
                        verify_row.get("explicit_c0") and verify_row.get("matches_candidate")
                    )
            mode1_ack = bool(write_ga.set_mode_with_ack(1, require_ack=True))
        except Exception as exc:
            writeback_payload["block_reason"] = str(exc)
        finally:
            writeback_payload["mode2_ack"] = bool(mode2_ack)
            writeback_payload["mode1_ack"] = bool(mode1_ack)
            try:
                write_ga.close()
            except Exception:
                pass

        _append_section(
            "writeback_session",
            [json.dumps(row, ensure_ascii=False, default=str) for row in write_io_logger.rows],
        )
        writeback_payload["executed"] = bool(writeback_payload.get("write_ack")) and bool(
            writeback_payload.get("strict_explicit_c0_verified")
        )
        if not writeback_payload.get("strict_explicit_c0_verified") and not writeback_payload["block_reason"]:
            writeback_payload["block_reason"] = "post-write strict explicit-C0 verification failed"

        if writeback_payload.get("strict_explicit_c0_verified"):
            post_rows, post_capture_summary, post_raw_lines = _capture_runtime_point(
                port=port,
                device_id=device_id,
                target_ppm=float(target_ppm),
                capture_seconds=float(capture_duration_s),
                csv_path=target_dir / "runtime_offset_trim_post_capture.csv",
                device_factory=device_factory,
            )
            verdict = _classify_residual(post_capture_summary.get("residual"))
            post_summary = {
                **post_capture_summary,
                "residual_after": post_capture_summary.get("residual"),
                "verdict": verdict["code"],
                "whether_passed": bool(verdict["passed"]),
            }
            _append_section("runtime_post_capture", list(post_raw_lines))
            if verdict["code"] == "pass":
                post_candidate = _build_post_offset_candidate(
                    candidate_dir=candidate_dir_path,
                    source_output_dir=target_dir,
                    device_id=device_id,
                    current_port=port,
                    old_a0=float(old_a0),
                    new_a0=float(new_a0),
                    correction_delta_a0=float(correction_delta_a0),
                    target_ppm=float(target_ppm),
                    measured_after=float(post_capture_summary["measured_mean"]),
                    residual_after=float(post_capture_summary["residual"]),
                    strict_explicit_c0_verified=True,
                    high_point_checked=False,
                )
                writeback_payload["post_offset_candidate_dir"] = post_candidate["output_dir"]
        else:
            _write_csv(target_dir / "runtime_offset_trim_post_capture.csv", post_rows)

    if not (target_dir / "runtime_offset_trim_post_capture.csv").exists():
        _write_csv(target_dir / "runtime_offset_trim_post_capture.csv", post_rows)
    _write_json(target_dir / "runtime_offset_trim_writeback.json", writeback_payload)
    _write_json(target_dir / "runtime_offset_trim_post_summary.json", post_summary)

    report_passed = bool(post_summary.get("whether_passed"))
    report_lines = [
        "# runtime offset trim",
        "",
        f"- port: {port}",
        f"- device_id: {_normalize_device_id(device_id)}",
        f"- target_ppm: {float(target_ppm)}",
        f"- old_a0: {float(old_a0)}",
        f"- response_slope: {float(response_slope)}",
        f"- residual_before: {residual_before}",
        f"- correction_delta_a0: {correction_delta_a0}",
        f"- new_a0: {new_a0}",
        f"- residual_after: {post_summary.get('residual_after')}",
        f"- strict_explicit_c0_verified: {bool(writeback_payload.get('strict_explicit_c0_verified'))}",
        f"- whether_execute: {bool(execute)}",
        f"- whether_passed: {report_passed}",
    ]
    _write_markdown(target_dir / "runtime_offset_trim_report.md", report_lines)
    (target_dir / "raw_transcript.log").write_text("\n".join(raw_sections).rstrip() + "\n", encoding="utf-8")
    return {
        "output_dir": str(target_dir),
        "plan_path": str(target_dir / "runtime_offset_trim_plan.json"),
        "pre_summary_path": str(target_dir / "runtime_offset_trim_pre_summary.json"),
        "writeback_path": str(target_dir / "runtime_offset_trim_writeback.json"),
        "post_summary_path": str(target_dir / "runtime_offset_trim_post_summary.json"),
        "executed": bool(writeback_payload.get("executed")),
    }


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="V1 runtime offset trim sidecar tool for a written live candidate.")
    parser.add_argument("--port", required=True)
    parser.add_argument("--device-id", required=True)
    parser.add_argument("--candidate-dir", required=True)
    parser.add_argument("--target-ppm", required=True, type=float)
    parser.add_argument("--gas", default="co2")
    parser.add_argument("--capture-duration-s", default=120.0, type=float)
    parser.add_argument("--response-slope", default=1.0, type=float)
    parser.add_argument("--output-dir")
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--high-point-only", action="store_true")
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(list(argv) if argv is not None else None)

    if args.execute and args.dry_run:
        raise SystemExit("--execute and --dry-run cannot be used together")

    if args.high_point_only:
        result = run_high_point_check(
            port=args.port,
            device_id=args.device_id,
            candidate_dir=args.candidate_dir,
            target_ppm=float(args.target_ppm),
            capture_duration_s=float(args.capture_duration_s),
            output_dir=args.output_dir,
            gas=args.gas,
        )
    else:
        result = run_from_cli(
            port=args.port,
            device_id=args.device_id,
            candidate_dir=args.candidate_dir,
            target_ppm=float(args.target_ppm),
            gas=args.gas,
            capture_duration_s=float(args.capture_duration_s),
            response_slope=float(args.response_slope),
            execute=bool(args.execute),
            dry_run=not bool(args.execute),
            output_dir=args.output_dir,
        )
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
