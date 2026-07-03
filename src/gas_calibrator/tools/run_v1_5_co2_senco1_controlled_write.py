"""Run controlled V1.5 CO2 SENCO1 writes.

This high-risk real-device tool writes only the CO2 primary coefficient group
SENCO1 from a reviewed no-write candidate artifact. It explicitly preserves
SENCO3, writes no device IDs, clears no coefficient group, and does not control
PACE, valves, water routes, gas routes, or humidity generation.
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
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from ..config import load_config
from ..devices import GasAnalyzer
from ..senco_format import rounded_senco_values, senco_readback_matches
from ..validation.common import load_csv_rows
from ..validation.reporting import ValidationMetadata, write_validation_report
from .v1_5_serial_safety import require_fragile_serial_timing


CONFIRMATION_TEXT = "WRITE_SENCO1_V1_5_CO2_PRIMARY_ONLY"


def _log(message: str) -> None:
    print(message, flush=True)


def _truthy(value: Any) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "y", "on", "pass", "ok", "verified"}


def _safe_float(value: Any) -> Optional[float]:
    if value in (None, ""):
        return None
    try:
        numeric = float(value)
    except Exception:
        return None
    return numeric if math.isfinite(numeric) else None


def _read_csv(path: str | Path) -> List[Dict[str, Any]]:
    return load_csv_rows(path)


def _write_csv(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    header: List[str] = []
    for row in rows:
        for key in row.keys():
            if key not in header:
                header.append(str(key))
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=header)
        writer.writeheader()
        writer.writerows(rows)


def _device_id(value: Any) -> str:
    text = str(value or "").strip()
    if text.isdigit():
        return f"{int(text):03d}"
    return text.upper()


def _enabled_analyzers(cfg: Mapping[str, Any]) -> List[Dict[str, Any]]:
    devices = cfg.get("devices", {}) if isinstance(cfg, Mapping) else {}
    source = devices.get("gas_analyzers") if isinstance(devices, Mapping) else None
    if isinstance(source, list) and source:
        analyzers = [dict(item) for item in source if isinstance(item, Mapping) and item.get("enabled", True)]
    else:
        single = devices.get("gas_analyzer") if isinstance(devices, Mapping) else None
        analyzers = [dict(single)] if isinstance(single, Mapping) and single.get("enabled", False) else []
    for idx, item in enumerate(analyzers, start=1):
        item.setdefault("name", f"ga{idx:02d}")
    return analyzers


def _build_analyzer_map(cfg: Mapping[str, Any]) -> Dict[str, Dict[str, Any]]:
    by_id: Dict[str, Dict[str, Any]] = {}
    for item in _enabled_analyzers(cfg):
        device_id = _device_id(item.get("device_id"))
        if not device_id:
            continue
        if device_id in by_id:
            raise RuntimeError(f"Duplicate analyzer device_id in config: {device_id}")
        by_id[device_id] = item
    return by_id


def _parse_values(value: Any) -> List[float]:
    text = str(value or "").strip()
    if not text:
        return []
    try:
        parsed = json.loads(text)
        if isinstance(parsed, list):
            return [float(item) for item in parsed]
    except Exception:
        pass
    out: List[float] = []
    for part in text.replace(";", ",").split(","):
        numeric = _safe_float(part.strip())
        if numeric is not None:
            out.append(float(numeric))
    return out


def _coeff_values_from_parsed(parsed: Mapping[str, Any], *, min_count: int = 4) -> List[float]:
    if not isinstance(parsed, Mapping) or not parsed:
        raise RuntimeError("READBACK_EMPTY")
    values: List[float] = []
    idx = 0
    while f"C{idx}" in parsed:
        values.append(float(parsed[f"C{idx}"]))
        idx += 1
    if len(values) < int(min_count):
        raise RuntimeError(f"READBACK_TOO_SHORT expected_min={int(min_count)} got={len(values)}")
    return values


def _read_group_values(ga: GasAnalyzer, group: int, *, min_count: int = 4) -> List[float]:
    parsed = ga.read_coefficient_group(int(group))
    return _coeff_values_from_parsed(parsed, min_count=min_count)


def _read_group_values_with_retry(
    ga: GasAnalyzer,
    group: int,
    *,
    min_count: int = 4,
    attempts: int = 3,
    retry_delay_s: float = 0.2,
) -> List[float]:
    last_error = ""
    for idx in range(max(1, int(attempts))):
        try:
            return _read_group_values(ga, int(group), min_count=int(min_count))
        except Exception as exc:
            last_error = str(exc)
        if idx + 1 < max(1, int(attempts)):
            _sleep_gap(retry_delay_s)
    raise RuntimeError(last_error or f"GETCO{int(group)}_READBACK_MISSING")


def _read_reviewed_group_values_with_retry(
    ga: GasAnalyzer,
    group: int,
    reviewed: Sequence[float],
    *,
    min_count: int = 4,
    attempts: int = 3,
    retry_delay_s: float = 0.2,
    atol: float = 1e-9,
) -> List[float]:
    last_values: List[float] = []
    last_reason = ""
    for idx in range(max(1, int(attempts))):
        try:
            values = _read_group_values(ga, int(group), min_count=int(min_count))
            last_values = list(values)
            ok, reason = _matches_review_snapshot(reviewed, values, min_required=int(min_count), atol=float(atol))
            if ok:
                return values
            last_reason = reason
        except Exception as exc:
            last_reason = str(exc)
        if idx + 1 < max(1, int(attempts)):
            _sleep_gap(retry_delay_s)
    if last_values:
        raise RuntimeError(f"live_snapshot_differs_from_review:{last_reason}; last_values={last_values}")
    raise RuntimeError(last_reason or f"GETCO{int(group)}_REVIEWED_READBACK_MISSING")


def _rounded_match_prefix(expected: Sequence[float], actual: Sequence[float], *, atol: float = 1e-9) -> bool:
    expected_values = list(rounded_senco_values(expected))
    if len(actual) < len(expected_values):
        return False
    try:
        actual_values = [float(value) for value in actual[: len(expected_values)]]
    except Exception:
        return False
    return all(abs(got - exp) <= float(atol) for exp, got in zip(expected_values, actual_values))


def _extra_values_are_zero(values: Sequence[float], start: int, *, atol: float = 1e-9) -> bool:
    try:
        return all(abs(float(value)) <= float(atol) for value in list(values)[int(start) :])
    except Exception:
        return False


def _matches_review_snapshot(
    reviewed: Sequence[float],
    observed: Sequence[float],
    *,
    min_required: int = 4,
    atol: float = 1e-9,
) -> Tuple[bool, str]:
    reviewed_values = list(reviewed)
    observed_values = list(observed)
    if len(reviewed_values) < int(min_required):
        return False, f"review_snapshot_too_short expected_min={int(min_required)} got={len(reviewed_values)}"
    if len(observed_values) < int(min_required):
        return False, f"live_snapshot_too_short expected_min={int(min_required)} got={len(observed_values)}"
    common_len = min(len(reviewed_values), len(observed_values))
    if not _rounded_match_prefix(reviewed_values[:common_len], observed_values[:common_len], atol=atol):
        return False, "live_snapshot_differs_from_review"
    if len(reviewed_values) > len(observed_values) and not _extra_values_are_zero(reviewed_values, common_len, atol=atol):
        return False, "review_has_nonzero_tail_not_visible_in_live_snapshot"
    if len(observed_values) > len(reviewed_values) and not _extra_values_are_zero(observed_values, common_len, atol=atol):
        return False, "live_has_nonzero_tail_not_present_in_review_snapshot"
    return True, ""


def _target_payload_values(target_values: Sequence[float], old_primary_values: Sequence[float]) -> List[float]:
    old_len = len(list(old_primary_values))
    payload_len = 4 if old_len <= 4 else min(len(list(target_values)), old_len)
    return [float(value) for value in list(target_values)[:payload_len]]


def _looks_like_partial_senco1_target(
    live_values: Sequence[float],
    old_values: Sequence[float],
    target_values: Sequence[float],
    *,
    atol: float,
) -> bool:
    live = [float(value) for value in live_values]
    old = list(rounded_senco_values(list(old_values)[: len(live)]))
    target = list(rounded_senco_values(list(target_values)[: len(live)]))
    if len(live) < 4 or len(old) < len(live) or len(target) < len(live):
        return False
    has_old = False
    has_target = False
    for idx, got in enumerate(live):
        old_match = abs(got - old[idx]) <= float(atol)
        target_match = abs(got - target[idx]) <= float(atol)
        if not old_match and not target_match:
            return False
        has_old = has_old or old_match
        has_target = has_target or target_match
    return has_old and has_target


def _supported_target_rows(mapping_rows: Sequence[Mapping[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for row in mapping_rows:
        if str(row.get("component") or "").strip().lower() != "co2":
            continue
        if str(row.get("primary_senco") or "").strip().upper() != "SENCO1":
            continue
        if str(row.get("secondary_senco") or "").strip().upper() != "SENCO3":
            continue
        if not _truthy(row.get("candidate_terms_complete")):
            continue
        if str(row.get("old_snapshot_status") or "").strip() != "primary_and_secondary_bound":
            continue
        if str(row.get("mapping_status") or "").strip() != "review_only_primary_preview_ready":
            continue
        target_values = _parse_values(row.get("primary_candidate_values"))
        old_primary = _parse_values(row.get("old_primary_snapshot"))
        old_secondary = _parse_values(row.get("old_secondary_snapshot"))
        if len(target_values) != 6 or len(old_primary) < 4 or len(old_secondary) < 4:
            continue
        item = dict(row)
        item["_target_values"] = target_values
        item["_old_primary_values"] = old_primary
        item["_old_secondary_values"] = old_secondary
        out.append(item)
    return out


def _select_targets(
    mapping_rows: Sequence[Mapping[str, Any]],
    *,
    selected_device_ids: Sequence[str],
    write_all_ready: bool,
) -> List[Dict[str, Any]]:
    supported = _supported_target_rows(mapping_rows)
    if write_all_ready:
        return supported
    wanted = {_device_id(item) for item in selected_device_ids if str(item or "").strip()}
    return [row for row in supported if _device_id(row.get("analyzer_device_id")) in wanted]


def _sleep_gap(seconds: float) -> None:
    delay = max(0.0, float(seconds or 0.0))
    if delay > 0:
        time.sleep(delay)


def _configure_coefficient_io(ga: GasAnalyzer, args: argparse.Namespace) -> None:
    ga.COEFFICIENT_COMM_QUIET_DELAY_S = max(0.0, float(args.coefficient_quiet_settle_s))
    ga.COEFFICIENT_READ_TIMEOUT_S = max(0.05, float(args.coefficient_read_timeout_s))
    ga.COEFFICIENT_READ_DELAY_S = max(0.0, float(args.coefficient_read_delay_s))
    ga.COEFFICIENT_READ_RETRY_COUNT = max(0, int(args.coefficient_read_retries))


def _restore_analyzer_runtime(
    ga: GasAnalyzer,
    analyzer_cfg: Mapping[str, Any],
    *,
    command_gap_s: float = 1.0,
    restore_active_freq: bool = True,
) -> Dict[str, Any]:
    mode = int(analyzer_cfg.get("mode", 2) or 2)
    active_send = bool(analyzer_cfg.get("active_send", True))
    ftd_hz = int(analyzer_cfg.get("ftd_hz", 1) or 1)
    average_filter = int(analyzer_cfg.get("average_filter", 49) or 49)
    restore = {
        "mode": mode,
        "active_send": active_send,
        "ftd_hz": ftd_hz,
        "average_filter": average_filter,
        "restore_active_freq": bool(restore_active_freq),
        "active_freq_restore_status": "skipped",
        "status": "attempted",
        "error": "",
    }
    try:
        ga.set_mode_with_ack(mode, require_ack=False)
        _sleep_gap(command_gap_s)
        if restore_active_freq:
            ga.set_active_freq_with_ack(ftd_hz, require_ack=False)
            restore["active_freq_restore_status"] = "restored"
            _sleep_gap(command_gap_s)
        ga.set_average_filter_with_ack(average_filter, require_ack=False)
        _sleep_gap(command_gap_s)
        ga.set_comm_way_with_ack(active_send, require_ack=False)
        restore["status"] = "restored"
    except Exception as exc:
        restore["status"] = "restore_failed"
        restore["error"] = str(exc)
    return restore


def _read_identity_snapshot(ga: GasAnalyzer, *, prefer_stream: bool, timeout_s: float = 3.0) -> Dict[str, Any]:
    deadline = time.time() + max(0.2, float(timeout_s))
    last: Optional[Mapping[str, Any]] = None
    while time.time() < deadline:
        try:
            snapshot = ga.read_current_mode_snapshot(
                prefer_stream=prefer_stream,
                drain_s=0.35,
                read_timeout_s=0.05,
                allow_passive_fallback=True,
            )
        except Exception:
            snapshot = None
        if isinstance(snapshot, Mapping) and snapshot:
            last = snapshot
            if snapshot.get("id"):
                break
        time.sleep(0.1)
    if not last:
        return {"ok": False, "id": "", "mode": "", "raw": "", "reason": "no_mode_snapshot"}
    return {
        "ok": bool(last.get("id")),
        "id": _device_id(last.get("id")),
        "mode": last.get("mode", ""),
        "raw": last.get("raw", ""),
        "reason": "" if last.get("id") else "missing_frame_id",
    }


def _set_senco1_with_rollback(
    ga: GasAnalyzer,
    *,
    target_values: Sequence[float],
    old_primary_live: Sequence[float],
    old_secondary_review: Sequence[float],
    mode: int,
    readback_attempts: int,
    retry_delay_s: float,
    compare_atol: float,
    preserve_atol: float,
    write_attempts: int,
) -> Dict[str, Any]:
    result: Dict[str, Any] = {
        "ok": False,
        "mode_requested": 2,
        "restore_mode": int(mode),
        "old_primary_live": list(old_primary_live),
        "target_senco1": list(target_values),
        "senco1_readback": [],
        "old_secondary_review": list(old_secondary_review),
        "senco3_readback": [],
        "senco1_write_status": "pending",
        "senco1_verify_status": "pending",
        "senco3_preserve_status": "pending",
        "rollback_attempted": False,
        "rollback_confirmed": False,
        "failure_reason": "",
    }
    write_attempted = False
    try:
        if not ga.set_mode_with_ack(2, require_ack=True):
            raise RuntimeError("MODE=2 not acknowledged before SENCO1 write")

        last_readback: List[float] = []
        last_error = ""
        for write_idx in range(max(1, int(write_attempts))):
            acked = bool(ga.set_senco(1, *target_values))
            write_attempted = True
            result["senco1_write_status"] = "success" if acked else "ack_missing_readback_check"
            last_error = "" if acked else "SENCO1_WRITE_ACK_FAILED"
            for idx in range(max(1, int(readback_attempts))):
                try:
                    last_readback = _read_group_values(ga, 1, min_count=len(target_values))
                    if senco_readback_matches(target_values, last_readback[: len(target_values)], atol=compare_atol):
                        result["senco1_readback"] = list(last_readback)
                        result["senco1_verify_status"] = "success"
                        last_error = ""
                        break
                    last_error = "SENCO1_READBACK_MISMATCH"
                except Exception as exc:
                    last_error = str(exc)
                if idx + 1 < max(1, int(readback_attempts)):
                    _sleep_gap(retry_delay_s)
            if result["senco1_verify_status"] == "success":
                break
            if write_idx + 1 < max(1, int(write_attempts)):
                _sleep_gap(retry_delay_s)

        if result["senco1_verify_status"] != "success":
            result["senco1_readback"] = list(last_readback)
            raise RuntimeError(last_error or "SENCO1_READBACK_MISSING")

        secondary_readback = _read_reviewed_group_values_with_retry(
            ga,
            3,
            old_secondary_review,
            min_count=4,
            attempts=max(1, int(readback_attempts)),
            retry_delay_s=retry_delay_s,
            atol=max(float(compare_atol), float(preserve_atol)),
        )
        result["senco3_preserve_status"] = "preserved"
        result["senco3_readback"] = list(secondary_readback)
        result["ok"] = True
    except Exception as exc:
        result["failure_reason"] = str(exc)
        if write_attempted:
            result["rollback_attempted"] = True
            try:
                ga.set_senco(1, *old_primary_live)
                rollback_values = _read_group_values_with_retry(
                    ga,
                    1,
                    min_count=min(4, len(old_primary_live)),
                    attempts=max(1, int(readback_attempts)),
                    retry_delay_s=retry_delay_s,
                )
                result["rollback_readback"] = list(rollback_values)
                rollback_ok, rollback_reason = _matches_review_snapshot(
                    old_primary_live,
                    rollback_values,
                    min_required=min(4, len(old_primary_live)),
                    atol=compare_atol,
                )
                result["rollback_confirmed"] = bool(rollback_ok)
                if not rollback_ok:
                    result["failure_reason"] = f"{result['failure_reason']}; ROLLBACK_FAILED:{rollback_reason}"
            except Exception as rollback_exc:
                result["rollback_confirmed"] = False
                result["failure_reason"] = f"{result['failure_reason']}; ROLLBACK_FAILED:{rollback_exc}"
    finally:
        try:
            ga.set_mode_with_ack(int(mode), require_ack=False)
        except Exception:
            pass
    return result


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Controlled V1.5 CO2 SENCO1 primary coefficient writer.")
    parser.add_argument("--config", required=True, help="V1.5 hardware config JSON.")
    parser.add_argument("--review-dir", required=True, help="Directory containing candidate_senco_mapping_review.csv.")
    parser.add_argument("--output-dir", required=True, help="Output directory for write evidence.")
    parser.add_argument("--device-id", action="append", default=[], help="Analyzer MODE2 device ID to write.")
    parser.add_argument("--write-all-ready", action="store_true", help="Write every ready CO2 SENCO1 candidate.")
    parser.add_argument("--enable-senco1-write", action="store_true", help="Required to write SENCO1.")
    parser.add_argument("--operator-confirmation", default="", help=f"Must equal {CONFIRMATION_TEXT!r}.")
    parser.add_argument("--reviewer", required=True)
    parser.add_argument("--approver", required=True)
    parser.add_argument("--stop-on-failure", action="store_true", default=True)
    parser.add_argument("--continue-on-failure", dest="stop_on_failure", action="store_false")
    parser.add_argument("--identity-timeout-s", type=float, default=4.0)
    parser.add_argument("--readback-attempts", type=int, default=3)
    parser.add_argument("--write-attempts", type=int, default=2)
    parser.add_argument("--readback-retry-delay-s", type=float, default=1.0)
    parser.add_argument("--compare-atol", type=float, default=1e-9)
    parser.add_argument(
        "--preserve-atol",
        type=float,
        default=1e-5,
        help="Tolerance for proving untouched SENCO3 preserved across GETCO display precision.",
    )
    parser.add_argument("--pre-device-cooldown-s", type=float, default=2.0)
    parser.add_argument("--inter-device-delay-s", type=float, default=5.0)
    parser.add_argument("--restore-command-gap-s", type=float, default=1.0)
    parser.add_argument(
        "--restore-active-freq",
        action="store_true",
        default=True,
        help="Restore FTD active-upload frequency after write. Default is on for V1.5 formal flow.",
    )
    parser.add_argument("--no-restore-active-freq", dest="restore_active_freq", action="store_false")
    parser.add_argument("--coefficient-quiet-settle-s", type=float, default=3.0)
    parser.add_argument("--coefficient-read-timeout-s", type=float, default=1.5)
    parser.add_argument("--coefficient-read-delay-s", type=float, default=1.0)
    parser.add_argument("--coefficient-read-retries", type=int, default=2)
    parser.add_argument(
        "--allow-resume-partial-senco1",
        action="store_true",
        help="Continue if selected live SENCO1 is a partial old/target mix from an interrupted write.",
    )
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    try:
        require_fragile_serial_timing(args, tool_name="run_v1_5_co2_senco1_controlled_write")
    except ValueError as exc:
        _log(str(exc))
        return 2
    if not args.enable_senco1_write or args.operator_confirmation != CONFIRMATION_TEXT:
        _log("Refusing SENCO1 write: pass --enable-senco1-write and the exact operator confirmation text.")
        return 2
    if not str(args.reviewer or "").strip() or not str(args.approver or "").strip():
        _log("Refusing SENCO1 write: reviewer and approver are required.")
        return 2
    if str(args.reviewer).strip() == str(args.approver).strip():
        _log("Refusing SENCO1 write: reviewer and approver must differ.")
        return 2

    cfg_path = Path(args.config).resolve()
    review_dir = Path(args.review_dir).resolve()
    mapping_path = review_dir / "candidate_senco_mapping_review.csv"
    if not mapping_path.exists():
        _log(f"Candidate mapping review not found: {mapping_path}")
        return 2

    cfg = load_config(cfg_path)
    mapping_rows = _read_csv(mapping_path)
    targets = _select_targets(mapping_rows, selected_device_ids=args.device_id, write_all_ready=bool(args.write_all_ready))
    if not targets:
        _log("No ready CO2 SENCO1 targets selected.")
        return 2

    analyzer_by_id = _build_analyzer_map(cfg)
    destination = Path(args.output_dir).resolve()
    destination.mkdir(parents=True, exist_ok=True)

    summary_rows: List[Dict[str, Any]] = []
    detail_rows: List[Dict[str, Any]] = []
    old_snapshot: Dict[str, Any] = {}
    start_ts = datetime.now().isoformat(timespec="seconds")

    for target_index, row in enumerate(targets):
        device_id = _device_id(row.get("analyzer_device_id"))
        analyzer_cfg = analyzer_by_id.get(device_id)
        if analyzer_cfg is None:
            summary_rows.append(
                {
                    "analyzer_device_id": device_id,
                    "analyzer_prefix": row.get("analyzer_prefix", ""),
                    "status": "skipped",
                    "reason": "device_id_not_found_in_config",
                    "write_applied": False,
                }
            )
            if args.stop_on_failure:
                break
            continue

        candidate_values = [float(value) for value in row.get("_target_values", [])]
        old_primary_review = [float(value) for value in row.get("_old_primary_values", [])]
        old_secondary_review = [float(value) for value in row.get("_old_secondary_values", [])]
        target_values = _target_payload_values(candidate_values, old_primary_review)

        ga = GasAnalyzer(
            str(analyzer_cfg["port"]),
            int(analyzer_cfg.get("baud", analyzer_cfg.get("baudrate", 115200)) or 115200),
            timeout=float(analyzer_cfg.get("timeout", 1.0) or 1.0),
            device_id=device_id,
        )
        identity_before: Dict[str, Any] = {}
        identity_after: Dict[str, Any] = {}
        restore: Dict[str, Any] = {}
        write_result: Dict[str, Any] = {}
        old_primary_live: List[float] = []
        old_secondary_live: List[float] = []
        rollback_primary_values: List[float] = []
        status = "failed"
        reason = ""
        try:
            _log(f"CO2 SENCO1 controlled write begin: device_id={device_id} port={analyzer_cfg.get('port')}")
            ga.open()
            _configure_coefficient_io(ga, args)
            _sleep_gap(float(args.pre_device_cooldown_s))
            identity_before = _read_identity_snapshot(
                ga,
                prefer_stream=bool(analyzer_cfg.get("active_send", True)),
                timeout_s=float(args.identity_timeout_s),
            )
            if _device_id(identity_before.get("id")) != device_id:
                raise RuntimeError(
                    f"identity_mismatch expected={device_id} observed={identity_before.get('id') or '<missing>'}"
                )

            try:
                old_primary_live = _read_reviewed_group_values_with_retry(
                    ga,
                    1,
                    old_primary_review,
                    min_count=4,
                    attempts=max(1, int(args.readback_attempts)),
                    retry_delay_s=float(args.readback_retry_delay_s),
                    atol=float(args.compare_atol),
                )
                rollback_primary_values = list(old_primary_live)
            except Exception as primary_exc:
                raw_primary_live = _read_group_values_with_retry(
                    ga,
                    1,
                    min_count=4,
                    attempts=max(1, int(args.readback_attempts)),
                    retry_delay_s=float(args.readback_retry_delay_s),
                )
                partial_ok = bool(args.allow_resume_partial_senco1) and _looks_like_partial_senco1_target(
                    raw_primary_live,
                    old_primary_review,
                    target_values,
                    atol=max(float(args.compare_atol), float(args.preserve_atol)),
                )
                if not partial_ok:
                    raise RuntimeError(f"live_senco1_does_not_match_review:{primary_exc}") from primary_exc
                old_primary_live = list(raw_primary_live)
                rollback_primary_values = list(old_primary_review)

            old_secondary_live = _read_reviewed_group_values_with_retry(
                ga,
                3,
                old_secondary_review,
                min_count=4,
                attempts=max(1, int(args.readback_attempts)),
                retry_delay_s=float(args.readback_retry_delay_s),
                atol=max(float(args.compare_atol), float(args.preserve_atol)),
            )

            write_result = _set_senco1_with_rollback(
                ga,
                target_values=target_values,
                old_primary_live=rollback_primary_values,
                old_secondary_review=old_secondary_review,
                mode=int(analyzer_cfg.get("mode", 2) or 2),
                readback_attempts=max(1, int(args.readback_attempts)),
                retry_delay_s=float(args.readback_retry_delay_s),
                compare_atol=float(args.compare_atol),
                preserve_atol=float(args.preserve_atol),
                write_attempts=max(1, int(args.write_attempts)),
            )
            restore = _restore_analyzer_runtime(
                ga,
                analyzer_cfg,
                command_gap_s=float(args.restore_command_gap_s),
                restore_active_freq=bool(args.restore_active_freq),
            )
            _sleep_gap(1.0)
            identity_after = _read_identity_snapshot(
                ga,
                prefer_stream=bool(analyzer_cfg.get("active_send", True)),
                timeout_s=float(args.identity_timeout_s),
            )
            if _device_id(identity_after.get("id")) != device_id:
                raise RuntimeError(
                    f"post_write_identity_mismatch expected={device_id} observed={identity_after.get('id') or '<missing>'}"
                )
            if not bool(write_result.get("ok")):
                status = "failed"
                reason = str(write_result.get("failure_reason") or "write_verification_failed")
            else:
                status = "written_readback_verified"
                reason = ""
        except Exception as exc:
            reason = str(exc)
            try:
                restore = _restore_analyzer_runtime(
                    ga,
                    analyzer_cfg,
                    command_gap_s=float(args.restore_command_gap_s),
                    restore_active_freq=bool(args.restore_active_freq),
                )
            except Exception:
                pass
        finally:
            try:
                ga.close()
            except Exception:
                pass

        old_snapshot[device_id] = {
            "analyzer_prefix": row.get("analyzer_prefix", ""),
            "port": analyzer_cfg.get("port", ""),
            "GETCO1_before_review": old_primary_review,
            "GETCO1_before_live": old_primary_live,
            "GETCO1_rollback_target": rollback_primary_values,
            "GETCO3_before_review": old_secondary_review,
            "GETCO3_before_live": old_secondary_live,
            "candidate_senco1_values": candidate_values,
            "target_senco1_payload_values": target_values,
            "senco1_readback": list(write_result.get("senco1_readback") or []),
            "senco3_readback": list(write_result.get("senco3_readback") or []),
        }
        summary_rows.append(
            {
                "analyzer_prefix": row.get("analyzer_prefix", ""),
                "analyzer_device_id": device_id,
                "port": analyzer_cfg.get("port", ""),
                "candidate_senco1_values": json.dumps(candidate_values, ensure_ascii=False),
                "target_senco1_values": json.dumps(target_values, ensure_ascii=False),
                "target_senco1_payload_len": len(target_values),
                "senco1_readback": json.dumps(write_result.get("senco1_readback") or [], ensure_ascii=False),
                "old_senco3_values": json.dumps(old_secondary_review, ensure_ascii=False),
                "senco3_readback": json.dumps(write_result.get("senco3_readback") or [], ensure_ascii=False),
                "senco3_preserve_status": write_result.get("senco3_preserve_status", ""),
                "status": status,
                "reason": reason,
                "write_applied": status == "written_readback_verified",
                "readback_verified": write_result.get("senco1_verify_status") == "success",
                "rollback_attempted": bool(write_result.get("rollback_attempted", False)),
                "rollback_confirmed": bool(write_result.get("rollback_confirmed", False)),
                "identity_before": identity_before.get("id", ""),
                "identity_after": identity_after.get("id", ""),
                "runtime_restore_status": restore.get("status", ""),
                "active_freq_restore_status": restore.get("active_freq_restore_status", ""),
                "controls_water_or_gas_routes": False,
                "writes_device_id": False,
                "writes_senco1": status == "written_readback_verified",
                "writes_senco3": False,
                "clears_senco": False,
                "reviewer": str(args.reviewer),
                "approver": str(args.approver),
            }
        )
        detail_rows.append(
            {
                "analyzer_prefix": row.get("analyzer_prefix", ""),
                "analyzer_device_id": device_id,
                "port": analyzer_cfg.get("port", ""),
                "identity_before_json": json.dumps(identity_before, ensure_ascii=False, default=str),
                "identity_after_json": json.dumps(identity_after, ensure_ascii=False, default=str),
                "old_primary_review_json": json.dumps(old_primary_review, ensure_ascii=False),
                "old_primary_live_json": json.dumps(old_primary_live, ensure_ascii=False),
                "rollback_primary_target_json": json.dumps(rollback_primary_values, ensure_ascii=False),
                "candidate_senco1_json": json.dumps(candidate_values, ensure_ascii=False),
                "target_senco1_json": json.dumps(target_values, ensure_ascii=False),
                "old_secondary_review_json": json.dumps(old_secondary_review, ensure_ascii=False),
                "old_secondary_live_json": json.dumps(old_secondary_live, ensure_ascii=False),
                "write_result_json": json.dumps(write_result, ensure_ascii=False, default=str),
                "runtime_restore_json": json.dumps(restore, ensure_ascii=False, default=str),
                "candidate_source_row_json": json.dumps(
                    {key: value for key, value in row.items() if not str(key).startswith("_")},
                    ensure_ascii=False,
                    default=str,
                ),
            }
        )
        if status != "written_readback_verified" and args.stop_on_failure:
            break
        if target_index + 1 < len(targets):
            _sleep_gap(float(args.inter_device_delay_s))

    end_ts = datetime.now().isoformat(timespec="seconds")
    any_failed = any(str(row.get("status") or "") != "written_readback_verified" for row in summary_rows)
    conclusion_rows = [
        {
            "overall_status": "failed" if any_failed else "success",
            "started_at": start_ts,
            "finished_at": end_ts,
            "target_count": len(targets),
            "processed_count": len(summary_rows),
            "success_count": sum(1 for row in summary_rows if row.get("status") == "written_readback_verified"),
            "stop_on_failure": bool(args.stop_on_failure),
            "controls_water_or_gas_routes": False,
            "writes_device_id": False,
            "writes_senco1": True,
            "writes_senco3": False,
            "clears_senco": False,
        }
    ]
    tables = {
        "co2_senco1_write_summary": summary_rows,
        "co2_senco1_write_detail": detail_rows,
        "co2_senco1_write_conclusion": conclusion_rows,
    }
    metadata = ValidationMetadata(
        tool_name="run_v1_5_co2_senco1_controlled_write",
        created_at=end_ts,
        analyzers=[f"{row.get('analyzer_prefix')}:{row.get('analyzer_device_id')}" for row in summary_rows],
        input_paths=[str(cfg_path), str(mapping_path)],
        output_dir=str(destination),
        config_path=str(cfg_path),
        config_summary={
            "write_all_ready": bool(args.write_all_ready),
            "device_ids": [_device_id(item) for item in args.device_id],
            "reviewer": str(args.reviewer),
            "approver": str(args.approver),
            "controls_water_or_gas_routes": False,
            "writes_device_id": False,
            "writes_senco1": True,
            "writes_senco3": False,
            "clears_senco": False,
            "pre_device_cooldown_s": float(args.pre_device_cooldown_s),
            "inter_device_delay_s": float(args.inter_device_delay_s),
            "restore_command_gap_s": float(args.restore_command_gap_s),
            "restore_active_freq": bool(args.restore_active_freq),
            "coefficient_quiet_settle_s": float(args.coefficient_quiet_settle_s),
            "coefficient_read_timeout_s": float(args.coefficient_read_timeout_s),
            "coefficient_read_delay_s": float(args.coefficient_read_delay_s),
            "coefficient_read_retries": int(args.coefficient_read_retries),
            "compare_atol": float(args.compare_atol),
            "preserve_atol": float(args.preserve_atol),
            "write_attempts": int(args.write_attempts),
            "allow_resume_partial_senco1": bool(args.allow_resume_partial_senco1),
        },
        notes=[
            "Controlled real-device CO2 SENCO1 primary coefficient write.",
            "SENCO3 is read before and after as preserved secondary evidence, but is not written.",
            "No PACE, valve, water route, gas route, humidity generator, device-ID writes, or CLEARSENCO commands are performed.",
        ],
    )
    outputs = write_validation_report(
        destination,
        prefix="co2_senco1_controlled_write",
        metadata=metadata,
        tables=tables,
    )
    snapshot_path = destination / "old_getco1_getco3_snapshot.json"
    snapshot_path.write_text(json.dumps(old_snapshot, ensure_ascii=False, indent=2), encoding="utf-8")
    _write_csv(
        destination / "old_getco1_getco3_snapshot.csv",
        [
            {
                "analyzer_device_id": device_id,
                "analyzer_prefix": payload.get("analyzer_prefix", ""),
                "port": payload.get("port", ""),
                "GETCO1_before_review": json.dumps(payload.get("GETCO1_before_review") or [], ensure_ascii=False),
                "GETCO1_before_live": json.dumps(payload.get("GETCO1_before_live") or [], ensure_ascii=False),
                "GETCO1_rollback_target": json.dumps(payload.get("GETCO1_rollback_target") or [], ensure_ascii=False),
                "GETCO3_before_review": json.dumps(payload.get("GETCO3_before_review") or [], ensure_ascii=False),
                "GETCO3_before_live": json.dumps(payload.get("GETCO3_before_live") or [], ensure_ascii=False),
                "candidate_senco1_values": json.dumps(payload.get("candidate_senco1_values") or [], ensure_ascii=False),
                "target_senco1_payload_values": json.dumps(
                    payload.get("target_senco1_payload_values") or [], ensure_ascii=False
                ),
                "senco1_readback": json.dumps(payload.get("senco1_readback") or [], ensure_ascii=False),
                "senco3_readback": json.dumps(payload.get("senco3_readback") or [], ensure_ascii=False),
            }
            for device_id, payload in old_snapshot.items()
        ],
    )
    _log(f"Controlled CO2 SENCO1 write report saved: {outputs['workbook']}")
    _log(f"Old GETCO1/GETCO3 snapshot saved: {snapshot_path}")
    return 1 if any_failed else 0


if __name__ == "__main__":
    sys.exit(main())
