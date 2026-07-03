"""Review and optionally repair V1.5 SENCO7/SENCO8 at the current temperature.

This initialization-side tool closes the gap between a full multi-temperature
SENCO7/SENCO8 calibration and doing nothing.  It checks the existing temperature
coefficients against two independent facts:

1. The current analyzer chamber/case temperature must agree with the digital
   thermometer at the current physical state.
2. The current GETCO7/8 coefficient shape must remain plausible over the formal
   temperature range, including below 0 C, so a unit that looks normal at room
   temperature but maps sub-zero points to 60 C is blocked before CO2/H2O
   sampling.

When explicitly unlocked, the tool performs a single-temperature repair only:
it writes C0=(reference-current_raw), C1=1, C2=0, C3=0 for the abnormal channel.
That is an initialization repair, not a substitute for a formal multi-point
temperature calibration certificate.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from ..config import load_config
from ..devices import GasAnalyzer, Thermometer
from ..senco_format import format_senco_values
from ..validation.reporting import ValidationMetadata, write_validation_report
from . import run_v1_5_co2_senco1_controlled_write as base
from .run_v1_5_temperature_senco78_neutral_controlled_write import TARGET_SENCO78, _send_payload
from .v1_5_serial_safety import require_fragile_serial_timing


CONFIRMATION_TEXT = "WRITE_SENCO78_SINGLE_POINT_V1_5_TEMPERATURE_REPAIR"
SUPPORTED_CHANNELS = (7, 8)
DEFAULT_PROJECTION_GRID_C = (-20.0, -10.0, 0.0, 10.0, 20.0, 30.0, 40.0)
HARD_BAD_TEMP_VALUES_C = (60.0, -40.0)


def _safe_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        numeric = float(value)
    except Exception:
        return None
    return numeric if math.isfinite(numeric) else None


def _mean(values: Iterable[Any]) -> float | None:
    nums = [float(value) for value in (_safe_float(item) for item in values) if value is not None]
    if not nums:
        return None
    return float(sum(nums) / len(nums))


def _device_id(value: Any) -> str:
    return base._device_id(value)


def _enabled_analyzers(cfg: Mapping[str, Any]) -> list[dict[str, Any]]:
    return base._enabled_analyzers(cfg)


def _selected_analyzers(cfg: Mapping[str, Any], selected_ids: Sequence[str]) -> list[dict[str, Any]]:
    wanted = {_device_id(item) for item in selected_ids if str(item or "").strip()}
    analyzers = _enabled_analyzers(cfg)
    if not wanted:
        return analyzers
    return [item for item in analyzers if _device_id(item.get("device_id")) in wanted]


def _coeff_values(parsed: Mapping[str, Any]) -> list[float]:
    values: list[float] = []
    idx = 0
    while f"C{idx}" in parsed:
        values.append(float(parsed[f"C{idx}"]))
        idx += 1
    return values


def _pad4(values: Sequence[float]) -> tuple[float, float, float, float]:
    padded = [float(item) for item in list(values)[:4]]
    while len(padded) < 4:
        padded.append(0.0)
    if len(values) < 2:
        padded[1] = 1.0
    return float(padded[0]), float(padded[1]), float(padded[2]), float(padded[3])


def _poly(coeffs: Sequence[float], x: float) -> float:
    c0, c1, c2, c3 = _pad4(coeffs)
    return float(c0 + c1 * x + c2 * x * x + c3 * x * x * x)


def _hard_bad(value: float, *, tolerance_c: float = 0.05) -> bool:
    return any(abs(float(value) - bad) <= tolerance_c for bad in HARD_BAD_TEMP_VALUES_C)


def _shape_check(
    coeffs: Sequence[float],
    *,
    grid_c: Sequence[float],
    max_projection_delta_c: float,
    projection_min_c: float,
    projection_max_c: float,
) -> tuple[bool, str, list[dict[str, Any]]]:
    rows: list[dict[str, Any]] = []
    projected: list[float] = []
    reasons: list[str] = []
    for raw_c in grid_c:
        corrected = _poly(coeffs, float(raw_c))
        projected.append(corrected)
        delta = corrected - float(raw_c)
        row = {
            "raw_temp_c": float(raw_c),
            "projected_temp_c": corrected,
            "delta_from_identity_c": delta,
            "hard_bad_projection": _hard_bad(corrected),
        }
        rows.append(row)
        if row["hard_bad_projection"]:
            reasons.append(f"hard_bad_projection_at_{raw_c:g}C")
        if corrected < float(projection_min_c) or corrected > float(projection_max_c):
            reasons.append(f"projection_out_of_range_at_{raw_c:g}C")
        if abs(delta) > float(max_projection_delta_c):
            reasons.append(f"projection_delta_too_large_at_{raw_c:g}C")

    for before, after in zip(projected, projected[1:]):
        if after < before - 0.2:
            reasons.append("projection_not_monotonic")
            break
    return (not reasons), ";".join(dict.fromkeys(reasons)), rows


def _thermometer_from_config(cfg: Mapping[str, Any]) -> Thermometer | None:
    devices = cfg.get("devices", {}) if isinstance(cfg, Mapping) else {}
    item = devices.get("thermometer") if isinstance(devices, Mapping) else None
    if not isinstance(item, Mapping) or not item.get("enabled", False) or not item.get("port"):
        return None
    return Thermometer(
        str(item.get("port")),
        int(item.get("baud", item.get("baudrate", 2400)) or 2400),
        timeout=float(item.get("timeout", 1.0) or 1.0),
        parity=str(item.get("parity", "N") or "N"),
        stopbits=float(item.get("stopbits", 1) or 1),
        bytesize=int(item.get("bytesize", 8) or 8),
    )


def _read_reference_temperature(
    thermometer: Thermometer | None,
    *,
    count: int,
    interval_s: float,
) -> tuple[float | None, list[dict[str, Any]]]:
    if thermometer is None:
        return None, [{"status": "missing_thermometer"}]
    rows: list[dict[str, Any]] = []
    values: list[float] = []
    try:
        thermometer.open()
        for index in range(max(1, int(count))):
            try:
                current = thermometer.read_current()
                value = _safe_float(current.get("temp_c") if isinstance(current, Mapping) else None)
                status = "pass" if value is not None else "invalid"
                if value is not None:
                    values.append(float(value))
                rows.append(
                    {
                        "sample_index": index + 1,
                        "status": status,
                        "temp_c": "" if value is None else value,
                        "raw": current.get("raw", "") if isinstance(current, Mapping) else "",
                    }
                )
            except Exception as exc:
                rows.append({"sample_index": index + 1, "status": "error", "error": str(exc)})
            if index + 1 < max(1, int(count)):
                time.sleep(max(0.0, float(interval_s)))
    finally:
        try:
            thermometer.close()
        except Exception:
            pass
    return _mean(values), rows


def _read_analyzer_temperature(
    ga: GasAnalyzer,
    *,
    count: int,
    interval_s: float,
    drain_s: float,
) -> tuple[dict[str, float | None], list[dict[str, Any]]]:
    rows: list[dict[str, Any]] = []
    chamber: list[float] = []
    case: list[float] = []
    for index in range(max(1, int(count))):
        try:
            line = ga.read_latest_data(prefer_stream=True, drain_s=float(drain_s), read_timeout_s=0.05, allow_passive_fallback=True)
            parsed = ga.parse_line(line)
            if isinstance(parsed, Mapping) and parsed:
                cell = _safe_float(parsed.get("chamber_temp_c"))
                shell = _safe_float(parsed.get("case_temp_c"))
                if cell is not None:
                    chamber.append(float(cell))
                if shell is not None:
                    case.append(float(shell))
                rows.append(
                    {
                        "sample_index": index + 1,
                        "status": "pass" if cell is not None or shell is not None else "missing_temperature",
                        "frame_id": _device_id(parsed.get("id")),
                        "mode2_schema_version": parsed.get("mode2_schema_version", ""),
                        "mode2_omitted_fields_json": parsed.get("mode2_omitted_fields_json", ""),
                        "chamber_temp_c": "" if cell is None else cell,
                        "case_temp_c": "" if shell is None else shell,
                        "raw": parsed.get("raw") or line,
                    }
                )
            else:
                rows.append({"sample_index": index + 1, "status": "no_valid_frame", "raw": line})
        except Exception as exc:
            rows.append({"sample_index": index + 1, "status": "error", "error": str(exc)})
        if index + 1 < max(1, int(count)):
            time.sleep(max(0.0, float(interval_s)))
    return {"chamber_temp_c": _mean(chamber), "case_temp_c": _mean(case)}, rows


def _write_csv(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    fields: list[str] = []
    for row in rows:
        for key in row:
            if key not in fields:
                fields.append(str(key))
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def _single_point_payload(group: int, offset_c: float, *, target: str) -> str:
    coeffs = (float(offset_c), 1.0, 0.0, 0.0)
    return f"SENCO{int(group)},YGAS,{target}," + ",".join(format_senco_values(coeffs))


def _neutral_payload(group: int, *, target: str) -> str:
    return f"SENCO{int(group)},YGAS,{target}," + ",".join(format_senco_values(TARGET_SENCO78))


def _read_group(ga: GasAnalyzer, group: int, args: argparse.Namespace) -> list[float]:
    return base._read_group_values_with_retry(
        ga,
        int(group),
        min_count=4,
        attempts=max(1, int(args.readback_attempts)),
        retry_delay_s=max(0.0, float(args.readback_retry_delay_s)),
    )


def _load_epoch0_senco78_snapshot(path_value: str | None) -> dict[str, dict[int, list[float]]]:
    if not str(path_value or "").strip():
        return {}
    path = Path(str(path_value)).resolve()
    if not path.exists():
        raise FileNotFoundError(f"GETCO epoch-0 snapshot not found: {path}")
    payload = json.loads(path.read_text(encoding="utf-8"))
    out: dict[str, dict[int, list[float]]] = {}
    if not isinstance(payload, Mapping):
        return out
    for raw_device_id, raw_entry in payload.items():
        device_id = _device_id(raw_device_id)
        entry = raw_entry if isinstance(raw_entry, Mapping) else {}
        coeffs = entry.get("coefficients") if isinstance(entry.get("coefficients"), Mapping) else entry
        by_group: dict[int, list[float]] = {}
        for group in (7, 8):
            for key in (f"GETCO{group}_before", f"GETCO{group}", f"SENCO{group}_before"):
                values = coeffs.get(key) if isinstance(coeffs, Mapping) else None
                if isinstance(values, Sequence) and not isinstance(values, (str, bytes)) and len(values) >= 4:
                    by_group[group] = [float(item) for item in values[:4]]
                    break
        if by_group:
            out[device_id] = by_group
    return out


def _parse_grid(value: str) -> tuple[float, ...]:
    out: list[float] = []
    for item in str(value or "").replace(";", ",").split(","):
        text = item.strip()
        if not text:
            continue
        out.append(float(text))
    if not out:
        return DEFAULT_PROJECTION_GRID_C
    return tuple(out)


def _parse_args(argv: Iterable[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="V1.5 current-temperature SENCO7/SENCO8 review and single-point repair.")
    parser.add_argument("--config", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--device-id", action="append", default=[])
    parser.add_argument("--sample-count", type=int, default=5)
    parser.add_argument("--sample-interval-s", type=float, default=1.0)
    parser.add_argument("--frame-drain-s", type=float, default=0.4)
    parser.add_argument("--max-current-delta-c", type=float, default=2.0)
    parser.add_argument("--max-projection-delta-c", type=float, default=8.0)
    parser.add_argument("--projection-grid-c", default=",".join(str(item) for item in DEFAULT_PROJECTION_GRID_C))
    parser.add_argument("--projection-min-c", type=float, default=-35.0)
    parser.add_argument("--projection-max-c", type=float, default=85.0)
    parser.add_argument(
        "--getco-snapshot-json",
        default="",
        help=(
            "Initialization epoch-0 old_component_coefficients_snapshot.json. "
            "Read-only review uses this snapshot for SENCO7/8 projection so it does not duplicate GETCO backup."
        ),
    )
    parser.add_argument("--enable-single-point-repair", action="store_true")
    parser.add_argument("--operator-confirmation", default="")
    parser.add_argument("--reviewer", default="")
    parser.add_argument("--approver", default="")
    parser.add_argument("--continue-on-failure", action="store_true")
    parser.add_argument("--readback-attempts", type=int, default=4)
    parser.add_argument("--readback-retry-delay-s", type=float, default=1.0)
    parser.add_argument("--restore-command-gap-s", type=float, default=1.0)
    parser.add_argument("--post-write-settle-s", type=float, default=2.0)
    parser.add_argument("--pre-device-cooldown-s", type=float, default=2.0)
    parser.add_argument("--inter-device-delay-s", type=float, default=5.0)
    parser.add_argument("--coefficient-quiet-settle-s", type=float, default=3.0)
    parser.add_argument("--coefficient-read-timeout-s", type=float, default=2.0)
    parser.add_argument("--coefficient-read-delay-s", type=float, default=1.0)
    parser.add_argument("--coefficient-read-retries", type=int, default=4)
    parser.add_argument("--senco78-ack-timeout-s", type=float, default=2.5)
    parser.add_argument("--senco78-target-mode", choices=("broadcast", "device_id"), default="broadcast")
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Iterable[str] | None = None) -> int:
    args = _parse_args(argv)
    output_dir = Path(args.output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    try:
        require_fragile_serial_timing(args, tool_name="run_v1_5_temperature_current_point_review")
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        return 2

    repair_unlocked = bool(args.enable_single_point_repair)
    if repair_unlocked:
        if str(args.operator_confirmation).strip() != CONFIRMATION_TEXT:
            print("Temperature single-point repair locked: operator confirmation is required.", file=sys.stderr)
            return 2
        if not str(args.reviewer).strip() or not str(args.approver).strip():
            print("Temperature single-point repair locked: reviewer and approver are required.", file=sys.stderr)
            return 2
        if str(args.reviewer).strip() == str(args.approver).strip():
            print("Temperature single-point repair locked: reviewer and approver must differ.", file=sys.stderr)
            return 2

    cfg = load_config(args.config)
    analyzers = _selected_analyzers(cfg, args.device_id)
    projection_grid = _parse_grid(args.projection_grid_c)
    epoch0_senco78 = _load_epoch0_senco78_snapshot(args.getco_snapshot_json)
    ref_temp_c, ref_rows = _read_reference_temperature(
        _thermometer_from_config(cfg),
        count=int(args.sample_count),
        interval_s=float(args.sample_interval_s),
    )
    _write_csv(output_dir / "temperature_current_point_reference_samples.csv", ref_rows)

    review_rows: list[dict[str, Any]] = []
    projection_rows: list[dict[str, Any]] = []
    sample_rows: list[dict[str, Any]] = []
    write_rows: list[dict[str, Any]] = []
    overall_ok = True

    for device_index, analyzer_cfg in enumerate(analyzers, start=1):
        device_id = _device_id(analyzer_cfg.get("device_id"))
        port = str(analyzer_cfg.get("port") or "")
        if device_index > 1:
            time.sleep(max(0.0, float(args.inter_device_delay_s)))
        ga = GasAnalyzer(
            port,
            int(analyzer_cfg.get("baud", analyzer_cfg.get("baudrate", 115200)) or 115200),
            timeout=float(analyzer_cfg.get("timeout", 1.0) or 1.0),
            device_id=device_id,
        )
        try:
            ga.open()
            base._configure_coefficient_io(ga, args)
            base._sleep_gap(float(args.pre_device_cooldown_s))
            snapshot_coeffs = epoch0_senco78.get(device_id, {})
            old7 = list(snapshot_coeffs.get(7) or [])
            old8 = list(snapshot_coeffs.get(8) or [])
            source7 = "epoch0_getco_snapshot" if len(old7) >= 4 and not repair_unlocked else "live_getco_read"
            source8 = "epoch0_getco_snapshot" if len(old8) >= 4 and not repair_unlocked else "live_getco_read"
            if repair_unlocked or len(old7) < 4:
                old7 = _read_group(ga, 7, args)
                source7 = "live_getco_read"
            if repair_unlocked or len(old8) < 4:
                old8 = _read_group(ga, 8, args)
                source8 = "live_getco_read"
            temps, rows = _read_analyzer_temperature(
                ga,
                count=int(args.sample_count),
                interval_s=float(args.sample_interval_s),
                drain_s=float(args.frame_drain_s),
            )
            for row in rows:
                sample_rows.append({"device_id": device_id, "port": port, **row})

            for group, coeffs, raw_key in (
                (7, old7, "chamber_temp_c"),
                (8, old8, "case_temp_c"),
            ):
                coefficient_source = source7 if group == 7 else source8
                current_temp = _safe_float(temps.get(raw_key))
                current_delta = (
                    current_temp - ref_temp_c
                    if current_temp is not None and ref_temp_c is not None
                    else None
                )
                shape_ok, shape_reason, shape_rows = _shape_check(
                    coeffs,
                    grid_c=projection_grid,
                    max_projection_delta_c=float(args.max_projection_delta_c),
                    projection_min_c=float(args.projection_min_c),
                    projection_max_c=float(args.projection_max_c),
                )
                for shape_row in shape_rows:
                    projection_rows.append(
                        {
                            "device_id": device_id,
                            "port": port,
                            "senco_group": f"SENCO{group}",
                            **shape_row,
                        }
                    )

                current_temp_hard_bad = current_temp is not None and _hard_bad(float(current_temp))
                current_delta_too_large = (
                    current_delta is not None
                    and abs(float(current_delta)) > float(args.max_current_delta_c)
                )
                case_temperature_not_reported = (
                    group == 8
                    and current_temp is None
                    and shape_ok
                    and any(
                        _safe_float(row.get("chamber_temp_c")) is not None
                        and _safe_float(row.get("case_temp_c")) is None
                        for row in rows
                    )
                )
                current_ok = (
                    ref_temp_c is not None
                    and current_temp is not None
                    and current_delta is not None
                    and abs(float(current_delta)) <= float(args.max_current_delta_c)
                    and not current_temp_hard_bad
                )
                reason_parts = []
                if ref_temp_c is None:
                    reason_parts.append("missing_digital_thermometer_reference")
                if current_temp is None:
                    reason_parts.append(
                        "case_temperature_not_reported_by_mode2_schema"
                        if case_temperature_not_reported
                        else "missing_current_analyzer_temperature"
                    )
                if current_temp_hard_bad:
                    reason_parts.append("current_temperature_hard_bad_value")
                if current_delta_too_large:
                    reason_parts.append("current_reference_delta_too_large")
                if not shape_ok:
                    reason_parts.append(shape_reason or "projection_shape_failed")
                reference_equivalence_required = (
                    not current_ok
                    and shape_ok
                    and not current_temp_hard_bad
                    and current_temp is not None
                    and (ref_temp_c is None or current_delta_too_large)
                )
                needs_repair = (
                    not shape_ok
                    or current_temp_hard_bad
                    or (current_temp is None and not case_temperature_not_reported)
                )
                if current_ok and shape_ok:
                    status = "pass"
                elif case_temperature_not_reported:
                    status = "not_applicable_missing_case_temperature"
                    reason_parts.append("use_chamber_temperature_for_new_algorithm_frame")
                elif reference_equivalence_required:
                    status = "reference_equivalence_required"
                    reason_parts.append("temperature_reference_not_equivalent_to_analyzer_thermal_state")
                else:
                    status = "repair_required"

                repair_offset = None
                neutral_current_temp = None
                post_repair_temp = None
                post_repair_delta = None

                write_status = "not_requested"
                write_command = ""
                readback_values: list[float] = []
                if needs_repair:
                    overall_ok = False
                    if repair_unlocked and ref_temp_c is not None:
                        target = device_id if args.senco78_target_mode == "device_id" else getattr(ga, "COMMAND_TARGET_ID", "FFF")
                        neutral_command = _neutral_payload(group, target=target)
                        neutral_acked, neutral_response_lines = _send_payload(
                            ga,
                            neutral_command,
                            ack_timeout_s=float(args.senco78_ack_timeout_s),
                        )
                        base._sleep_gap(float(args.post_write_settle_s))
                        neutral_readback_values: list[float] = []
                        try:
                            neutral_readback_values = _read_group(ga, group, args)
                        except Exception:
                            neutral_readback_values = []
                        neutral_ok = len(neutral_readback_values) >= 4 and all(
                            abs(float(got) - float(exp)) <= 0.05
                            for got, exp in zip(neutral_readback_values[:4], TARGET_SENCO78)
                        )
                        write_rows.append(
                            {
                                "device_id": device_id,
                                "port": port,
                                "senco_group": f"SENCO{group}",
                                "write_stage": "neutralize_before_single_point_repair",
                                "status": "neutral_readback_verified" if neutral_acked and neutral_ok else "neutral_write_or_readback_failed",
                                "write_command": neutral_command,
                                "acked": neutral_acked,
                                "response_lines": json.dumps(neutral_response_lines, ensure_ascii=False),
                                "target_values": json.dumps(list(TARGET_SENCO78), ensure_ascii=False),
                                "readback_values": json.dumps(neutral_readback_values, ensure_ascii=False),
                            }
                        )
                        if neutral_acked and neutral_ok:
                            neutral_temps, neutral_rows = _read_analyzer_temperature(
                                ga,
                                count=int(args.sample_count),
                                interval_s=float(args.sample_interval_s),
                                drain_s=float(args.frame_drain_s),
                            )
                            for row in neutral_rows:
                                sample_rows.append(
                                    {
                                        "device_id": device_id,
                                        "port": port,
                                        "sample_phase": f"SENCO{group}_neutral_before_repair",
                                        **row,
                                    }
                                )
                            neutral_current_temp = _safe_float(neutral_temps.get(raw_key))
                            if neutral_current_temp is not None:
                                repair_offset = float(ref_temp_c) - float(neutral_current_temp)

                    if repair_unlocked and repair_offset is not None:
                        write_command = _single_point_payload(group, repair_offset, target=target)
                        acked, response_lines = _send_payload(
                            ga,
                            write_command,
                            ack_timeout_s=float(args.senco78_ack_timeout_s),
                        )
                        base._sleep_gap(float(args.post_write_settle_s))
                        try:
                            readback_values = _read_group(ga, group, args)
                        except Exception:
                            readback_values = []
                        target_values = [float(repair_offset), 1.0, 0.0, 0.0]
                        readback_ok = len(readback_values) >= 4 and all(
                            abs(float(got) - float(exp)) <= 0.05
                            for got, exp in zip(readback_values[:4], target_values)
                        )
                        write_status = "written_readback_verified" if acked and readback_ok else "write_or_readback_failed"
                        post_repair_ok = False
                        if write_status == "written_readback_verified":
                            post_temps, post_rows = _read_analyzer_temperature(
                                ga,
                                count=int(args.sample_count),
                                interval_s=float(args.sample_interval_s),
                                drain_s=float(args.frame_drain_s),
                            )
                            for row in post_rows:
                                sample_rows.append(
                                    {
                                        "device_id": device_id,
                                        "port": port,
                                        "sample_phase": f"SENCO{group}_after_single_point_repair",
                                        **row,
                                    }
                                )
                            post_repair_temp = _safe_float(post_temps.get(raw_key))
                            if post_repair_temp is not None and ref_temp_c is not None:
                                post_repair_delta = float(post_repair_temp) - float(ref_temp_c)
                                post_repair_ok = (
                                    abs(float(post_repair_delta)) <= float(args.max_current_delta_c)
                                    and not _hard_bad(float(post_repair_temp))
                                )
                        if write_status == "written_readback_verified" and post_repair_ok:
                            status = "single_point_repair_written"
                        else:
                            if write_status == "written_readback_verified":
                                write_status = "written_readback_verified_temperature_check_failed"
                            overall_ok = False
                        write_rows.append(
                            {
                                "device_id": device_id,
                                "port": port,
                                "senco_group": f"SENCO{group}",
                                "write_stage": "single_point_repair",
                                "status": write_status,
                                "write_command": write_command,
                                "acked": acked,
                                "response_lines": json.dumps(response_lines, ensure_ascii=False),
                                "target_values": json.dumps(target_values, ensure_ascii=False),
                                "readback_values": json.dumps(readback_values, ensure_ascii=False),
                            }
                        )

                review_rows.append(
                    {
                        "device_id": device_id,
                        "port": port,
                        "senco_group": f"SENCO{group}",
                        "status": status,
                        "reason": ";".join(dict.fromkeys(reason_parts)),
                        "reference_temp_c": "" if ref_temp_c is None else ref_temp_c,
                        "current_analyzer_temp_c": "" if current_temp is None else current_temp,
                        "current_delta_from_reference_c": "" if current_delta is None else current_delta,
                        "neutral_current_analyzer_temp_c": "" if neutral_current_temp is None else neutral_current_temp,
                        "post_repair_analyzer_temp_c": "" if post_repair_temp is None else post_repair_temp,
                        "post_repair_delta_from_reference_c": "" if post_repair_delta is None else post_repair_delta,
                        "current_delta_limit_c": float(args.max_current_delta_c),
                        "projection_shape_ok": shape_ok,
                        "projection_shape_reason": shape_reason,
                        "projection_grid_c": json.dumps(list(projection_grid), ensure_ascii=False),
                        "old_coefficients": json.dumps(list(coeffs), ensure_ascii=False),
                        "coefficient_source": coefficient_source,
                        "single_point_repair_offset_c": "" if repair_offset is None else repair_offset,
                        "write_status": write_status,
                        "write_command": write_command,
                        "physical_meaning": (
                            "SENCO7/SENCO8 are temperature input corrections. A single-point repair "
                            "is allowed only for channel/coefficient failures such as hard-bad temperatures "
                            "or invalid low-temperature projections. A common current offset against an "
                            "external thermometer is reported as reference equivalence required, because it "
                            "can come from analyzer self-heating or sensor placement rather than a bad "
                            "temperature coefficient."
                        ),
                    }
                )
        except Exception as exc:
            overall_ok = False
            review_rows.append(
                {
                    "device_id": device_id,
                    "port": port,
                    "senco_group": "",
                    "status": "error",
                    "reason": str(exc),
                }
            )
            if not args.continue_on_failure:
                break
        finally:
            try:
                ga.close()
            except Exception:
                pass

    review_csv = output_dir / "temperature_current_point_review.csv"
    projection_csv = output_dir / "temperature_senco78_projection_check.csv"
    samples_csv = output_dir / "temperature_current_point_analyzer_samples.csv"
    write_csv = output_dir / "temperature_single_point_repair_write_events.csv"
    _write_csv(review_csv, review_rows)
    _write_csv(projection_csv, projection_rows)
    _write_csv(samples_csv, sample_rows)
    _write_csv(write_csv, write_rows)
    pass_statuses = {"pass", "single_point_repair_written"}
    overall_ok = bool(review_rows) and all(str(row.get("status") or "") in pass_statuses for row in review_rows)

    payload = {
        "schema": "v1_5_temperature_current_point_review_v1",
        "created_at": datetime.now().isoformat(timespec="seconds"),
        "config": str(Path(args.config).resolve()),
        "output_dir": str(output_dir),
        "reference_temp_c": ref_temp_c,
        "repair_unlocked": repair_unlocked,
        "status": "pass" if overall_ok else "review_or_repair_required",
        "artifacts": {
            "review_csv": str(review_csv),
            "projection_csv": str(projection_csv),
            "samples_csv": str(samples_csv),
            "write_csv": str(write_csv),
        },
        "notes": [
            "Current-point repair is an initialization input-quantity repair, not a formal multi-temperature SENCO7/SENCO8 certificate.",
            "Projection over sub-zero points is required to catch the 60 C low-temperature failure mode.",
        ],
    }
    json_path = output_dir / "temperature_current_point_review.json"
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    report_path = output_dir / "temperature_current_point_review.md"
    report_path.write_text(
        "\n".join(
            [
                "# V1.5 当前温度点温度通道评审",
                "",
                f"- 状态: `{payload['status']}`",
                f"- 数字测温仪参考温度: `{'' if ref_temp_c is None else f'{ref_temp_c:.3f} C'}`",
                f"- 是否执行单点修复写入: `{repair_unlocked}`",
                "",
                "## 物理意义",
                "",
                "SENCO7/SENCO8 是分析仪内部温度输入修正。当前温度点单点修复只能消除当前偏移，不能替代多温度正式拟合；但它能在初始化阶段阻断零下温度被旧系数算成 60 C 这类硬错误。",
                "",
                "## 输出",
                "",
                f"- 评审表: `{review_csv}`",
                f"- 低温投影检查: `{projection_csv}`",
                f"- 分析仪当前温度样本: `{samples_csv}`",
                f"- 写入事件: `{write_csv}`",
                "",
            ]
        ),
        encoding="utf-8",
    )
    report_path.write_text(
        "\n".join(
            [
                "# V1.5 当前温度点温度通道评审",
                "",
                f"- 状态: `{payload['status']}`",
                f"- 数字测温仪参考温度: `{'' if ref_temp_c is None else f'{ref_temp_c:.3f} C'}`",
                f"- 是否执行单点修复写入: `{repair_unlocked}`",
                "",
                "## 物理意义",
                "",
                "SENCO7/SENCO8 是分析仪内部温度输入修正。当前温度点单点修复只能消除当前偏移，不能替代多温度正式拟合；但它能在初始化阶段阻断零下温度被旧系数算成 60 C 这类硬错误。",
                "",
                "当通道需要修复时，工具会先把对应 SENCO7/SENCO8 写成中性系数，再重新读取当前温度，最后才按数字测温仪参考值计算单点偏置，避免把旧异常系数叠加进新系数。",
                "",
                "## 输出",
                "",
                f"- 评审表: `{review_csv}`",
                f"- 低温外推检查: `{projection_csv}`",
                f"- 分析仪当前温度样本: `{samples_csv}`",
                f"- 写入事件: `{write_csv}`",
                "",
            ]
        ),
        encoding="utf-8",
    )
    report_path.write_text(
        "\n".join(
            [
                "# V1.5 当前温度点温度通道评审",
                "",
                f"- 状态: `{payload['status']}`",
                f"- 数字测温仪参考温度: `{'' if ref_temp_c is None else f'{ref_temp_c:.3f} C'}`",
                f"- 是否执行单点修复写入: `{repair_unlocked}`",
                "",
                "## 物理意义",
                "",
                "SENCO7/SENCO8 是分析仪内部温度输入修正。当前点单点修复只应用于 60 C、-40 C 或低温投影异常这类明确通道/系数错误；不能把外部数字测温仪和分析仪自热造成的共同温差直接写成系数。",
                "",
                "当通道确实需要修复时，工具会先把对应 SENCO7/SENCO8 写成中性系数，再重新读取当前温度，最后按数字测温仪参考值计算单点偏置，避免把旧异常系数叠加进新系数。",
                "",
                "## 输出",
                "",
                f"- 评审表: `{review_csv}`",
                f"- 低温外推检查: `{projection_csv}`",
                f"- 分析仪当前温度样本: `{samples_csv}`",
                f"- 写入事件: `{write_csv}`",
                "",
            ]
        ),
        encoding="utf-8",
    )
    write_validation_report(
        output_dir,
        prefix="temperature_current_point_review",
        metadata=ValidationMetadata(
            tool_name="run_v1_5_temperature_current_point_review",
            analyzers=[str(row.get("device_id")) for row in review_rows if row.get("device_id")],
            input_paths=[str(Path(args.config).resolve())],
            output_dir=str(output_dir),
            config_path=str(Path(args.config).resolve()),
            config_summary={
                "max_current_delta_c": float(args.max_current_delta_c),
                "max_projection_delta_c": float(args.max_projection_delta_c),
                "projection_grid_c": list(projection_grid),
                "single_point_repair_unlocked": repair_unlocked,
            },
            notes=[
                "No gas route, water route, valve, PACE, or device ID control is performed.",
                "SENCO7/SENCO8 writes require explicit unlock, reviewer, and approver.",
            ],
        ),
        tables={
            "temperature_current_point_review": review_rows,
            "temperature_senco78_projection_check": projection_rows,
            "temperature_current_point_analyzer_samples": sample_rows,
            "temperature_single_point_repair_write_events": write_rows,
        },
    )
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0 if overall_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
