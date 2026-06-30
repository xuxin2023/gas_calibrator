"""Run controlled V1.5 SENCO9 pressure-channel writes.

This is a high-risk real-device tool. It writes only SENCO9, only one analyzer
port at a time, and only from a prior no-write pressure fit artifact. It does
not control PACE, valves, water routes, gas routes, humidity generation, or
device IDs.
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence

from ..config import load_config
from ..devices import GasAnalyzer
from ..validation.common import load_csv_rows
from ..validation.reporting import ValidationMetadata, write_validation_report
from .run_v1_corrected_autodelivery import write_senco_groups_with_full_verification
from .v1_5_serial_safety import require_fragile_serial_timing


CONFIRMATION_TEXT = "WRITE_SENCO9_V1_5_PRESSURE_ONLY"


def _log(message: str) -> None:
    print(message, flush=True)


def _truthy(value: Any) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "y", "on", "pass", "ok", "verified"}


def _safe_float(value: Any) -> Optional[float]:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except Exception:
        return None


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


def _device_id(value: Any) -> str:
    text = str(value or "").strip()
    if text.isdigit():
        return f"{int(text):03d}"
    return text.upper()


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


def _supported_fit_rows(fit_summary_rows: Sequence[Mapping[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for row in fit_summary_rows:
        if (
            str(row.get("status") or "").strip().lower() == "pass"
            and str(row.get("recommendation") or "").strip()
            == "review_senco9_offset_candidate_no_write"
            and not _truthy(row.get("write_allowed"))
        ):
            offset = _safe_float(row.get("offset_only_offset_kpa"))
            if offset is not None:
                out.append(dict(row))
    return out


def _select_targets(
    fit_summary_rows: Sequence[Mapping[str, Any]],
    *,
    selected_device_ids: Sequence[str],
    write_all_supported: bool,
) -> List[Dict[str, Any]]:
    supported = _supported_fit_rows(fit_summary_rows)
    if write_all_supported:
        return supported
    wanted = {_device_id(item) for item in selected_device_ids if str(item or "").strip()}
    return [row for row in supported if _device_id(row.get("analyzer_device_id")) in wanted]


def _sleep_gap(seconds: float) -> None:
    delay = max(0.0, float(seconds or 0.0))
    if delay > 0:
        time.sleep(delay)


def _restore_analyzer_runtime(
    ga: GasAnalyzer,
    analyzer_cfg: Mapping[str, Any],
    *,
    command_gap_s: float = 1.0,
    restore_active_freq: bool = False,
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


def _configure_coefficient_io(ga: GasAnalyzer, args: argparse.Namespace) -> None:
    """Slow down coefficient IO for fragile analyzer serial firmware."""
    ga.COEFFICIENT_COMM_QUIET_DELAY_S = max(0.0, float(args.coefficient_quiet_settle_s))
    ga.COEFFICIENT_READ_TIMEOUT_S = max(0.05, float(args.coefficient_read_timeout_s))
    ga.COEFFICIENT_READ_DELAY_S = max(0.0, float(args.coefficient_read_delay_s))
    ga.COEFFICIENT_READ_RETRY_COUNT = max(0, int(args.coefficient_read_retries))


def _coefficient_values(parsed: Mapping[str, Any], expected_len: int = 4) -> List[float]:
    values: List[float] = []
    for idx in range(max(1, int(expected_len))):
        key = f"C{idx}"
        if key not in parsed:
            raise RuntimeError(f"GETCO9 snapshot missing {key}")
        values.append(float(parsed[key]))
    return values


def _read_getco9_values(ga: GasAnalyzer) -> List[float]:
    parsed = ga.read_coefficient_group(9)
    if not isinstance(parsed, Mapping) or not parsed:
        raise RuntimeError("GETCO9 snapshot empty")
    return _coefficient_values(parsed, expected_len=4)


def _resolve_senco9_target(old_values: Sequence[float], offset_delta_kpa: float) -> List[float]:
    current = [float(value) for value in list(old_values)[:4]]
    while len(current) < 4:
        current.append(0.0)
    if len(current) >= 2 and current[1] == 0.0:
        current[1] = 1.0
    current[0] = float(current[0]) + float(offset_delta_kpa)
    return current


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Controlled V1.5 SENCO9 pressure-channel writer.")
    parser.add_argument("--config", required=True, help="V1.5 hardware config JSON.")
    parser.add_argument("--fit-dir", required=True, help="Directory containing pressure_fit_summary.csv.")
    parser.add_argument("--output-dir", required=True, help="Output directory for write evidence.")
    parser.add_argument(
        "--device-id",
        action="append",
        default=[],
        help="Analyzer MODE2 device ID to write. Repeat for multiple IDs.",
    )
    parser.add_argument(
        "--write-all-supported",
        action="store_true",
        help="Write every supported candidate from the fit artifact, sequentially one port at a time.",
    )
    parser.add_argument("--enable-senco9-write", action="store_true", help="Required to write SENCO9.")
    parser.add_argument(
        "--operator-confirmation",
        default="",
        help=f"Must equal {CONFIRMATION_TEXT!r}.",
    )
    parser.add_argument("--reviewer", required=True)
    parser.add_argument("--approver", required=True)
    parser.add_argument("--stop-on-failure", action="store_true", default=True)
    parser.add_argument("--continue-on-failure", dest="stop_on_failure", action="store_false")
    parser.add_argument("--identity-timeout-s", type=float, default=4.0)
    parser.add_argument("--readback-attempts", type=int, default=3)
    parser.add_argument(
        "--readback-retry-delay-s",
        type=float,
        default=1.0,
        help="Delay between SENCO9 write/readback retry attempts. Keep at least 1s for fragile analyzer serial firmware.",
    )
    parser.add_argument(
        "--pre-device-cooldown-s",
        type=float,
        default=2.0,
        help="Cooling time after opening each analyzer before any coefficient command.",
    )
    parser.add_argument(
        "--inter-device-delay-s",
        type=float,
        default=5.0,
        help="Delay between sequential analyzer write jobs.",
    )
    parser.add_argument(
        "--restore-command-gap-s",
        type=float,
        default=1.0,
        help="Delay between runtime restore commands.",
    )
    parser.add_argument(
        "--restore-active-freq",
        action="store_true",
        help="Also restore FTD active-upload frequency after the write. Default is off for fragile analyzer serial firmware.",
    )
    parser.add_argument(
        "--coefficient-quiet-settle-s",
        type=float,
        default=3.0,
        help="Quiet time after SETCOMWAY=0 before GETCO/SENCO coefficient operations.",
    )
    parser.add_argument(
        "--coefficient-read-timeout-s",
        type=float,
        default=1.2,
        help="GETCO coefficient response scan window.",
    )
    parser.add_argument(
        "--coefficient-read-delay-s",
        type=float,
        default=1.0,
        help="Delay after GETCO command before reading response.",
    )
    parser.add_argument(
        "--coefficient-read-retries",
        type=int,
        default=2,
        help="Additional GETCO retries for each coefficient read.",
    )
    parser.add_argument(
        "--senco9-offset-mode",
        choices=["add-to-current-c0"],
        default="add-to-current-c0",
        help="Interpret no-write fit offset as a delta added to the current GETCO9 C0.",
    )
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    try:
        require_fragile_serial_timing(args, tool_name="run_v1_5_pressure_senco9_controlled_write")
    except ValueError as exc:
        _log(str(exc))
        return 2
    if not args.enable_senco9_write or args.operator_confirmation != CONFIRMATION_TEXT:
        _log("Refusing SENCO9 write: pass --enable-senco9-write and the exact operator confirmation text.")
        return 2
    if not str(args.reviewer or "").strip() or not str(args.approver or "").strip():
        _log("Refusing SENCO9 write: reviewer and approver are required.")
        return 2
    if str(args.reviewer).strip() == str(args.approver).strip():
        _log("Refusing SENCO9 write: reviewer and approver must differ.")
        return 2

    cfg_path = Path(args.config).resolve()
    fit_dir = Path(args.fit_dir).resolve()
    summary_path = fit_dir / "pressure_fit_summary.csv"
    if not summary_path.exists():
        _log(f"Fit summary not found: {summary_path}")
        return 2

    cfg = load_config(cfg_path)
    fit_rows = _read_csv(summary_path)
    targets = _select_targets(
        fit_rows,
        selected_device_ids=args.device_id,
        write_all_supported=bool(args.write_all_supported),
    )
    if not targets:
        _log("No supported SENCO9 targets selected.")
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

        offset = _safe_float(row.get("offset_only_offset_kpa"))
        if offset is None:
            summary_rows.append(
                {
                    "analyzer_device_id": device_id,
                    "analyzer_prefix": row.get("analyzer_prefix", ""),
                    "status": "skipped",
                    "reason": "missing_candidate_offset_kpa",
                    "write_applied": False,
                }
            )
            if args.stop_on_failure:
                break
            continue

        ga = GasAnalyzer(
            str(analyzer_cfg["port"]),
            int(analyzer_cfg.get("baud", analyzer_cfg.get("baudrate", 115200)) or 115200),
            timeout=float(analyzer_cfg.get("timeout", 1.0) or 1.0),
            device_id=device_id,
        )
        identity_before: Dict[str, Any] = {}
        identity_after: Dict[str, Any] = {}
        restore: Dict[str, Any] = {}
        verify_result: Dict[str, Any] = {}
        status = "failed"
        reason = ""
        coeff_before_precheck: List[float] = []
        coeffs: List[float] = []
        try:
            _log(f"SENCO9 controlled write begin: device_id={device_id} port={analyzer_cfg.get('port')}")
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
            coeff_before_precheck = _read_getco9_values(ga)
            coeffs = _resolve_senco9_target(coeff_before_precheck, float(offset))
            verify_result = write_senco_groups_with_full_verification(
                ga,
                expected_groups={9: coeffs},
                restore_mode=int(analyzer_cfg.get("mode", 2) or 2),
                readback_attempts=max(1, int(args.readback_attempts)),
                retry_delay_s=float(args.readback_retry_delay_s),
            )
            restore = _restore_analyzer_runtime(
                ga,
                analyzer_cfg,
                command_gap_s=float(args.restore_command_gap_s),
                restore_active_freq=bool(args.restore_active_freq),
            )
            time.sleep(0.5)
            identity_after = _read_identity_snapshot(
                ga,
                prefer_stream=bool(analyzer_cfg.get("active_send", True)),
                timeout_s=float(args.identity_timeout_s),
            )
            if _device_id(identity_after.get("id")) != device_id:
                raise RuntimeError(
                    f"post_write_identity_mismatch expected={device_id} observed={identity_after.get('id') or '<missing>'}"
                )
            if not bool(verify_result.get("ok")):
                reason = str(verify_result.get("failure_reason") or "write_verification_failed")
                status = "failed"
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

        detail = (list(verify_result.get("detail_rows") or [{}]) or [{}])[0]
        coeff_before = list(detail.get("coeff_before") or [])
        coeff_readback = list(detail.get("coeff_readback") or [])
        coeff_rollback = list(detail.get("coeff_rollback_readback") or [])
        old_values_for_record = coeff_before or coeff_before_precheck
        old_snapshot[device_id] = {
            "analyzer_prefix": row.get("analyzer_prefix", ""),
            "port": analyzer_cfg.get("port", ""),
            "GETCO9_before": old_values_for_record,
            "candidate_offset_kpa": offset,
            "candidate_offset_mode": str(args.senco9_offset_mode),
            "candidate_values": list(coeffs),
            "readback": coeff_readback,
            "rollback_readback": coeff_rollback,
        }
        summary_rows.append(
            {
                "analyzer_prefix": row.get("analyzer_prefix", ""),
                "analyzer_device_id": device_id,
                "port": analyzer_cfg.get("port", ""),
                "candidate_offset_kpa": offset,
                "old_senco9_c0": old_values_for_record[0] if old_values_for_record else "",
                "target_senco9_c0": coeffs[0] if coeffs else "",
                "target_senco9_values": json.dumps(coeffs, ensure_ascii=False),
                "candidate_offset_mode": str(args.senco9_offset_mode),
                "candidate_residual_max_abs_hpa": row.get("offset_only_residual_max_abs_hpa", ""),
                "status": status,
                "reason": reason,
                "write_applied": status == "written_readback_verified",
                "readback_verified": str(detail.get("verify_status") or "") == "success",
                "rollback_attempted": bool(detail.get("rollback_attempted", verify_result.get("rollback_attempted", False))),
                "rollback_confirmed": bool(detail.get("rollback_confirmed", verify_result.get("rollback_confirmed", False))),
                "identity_before": identity_before.get("id", ""),
                "identity_after": identity_after.get("id", ""),
                "runtime_restore_status": restore.get("status", ""),
                "active_freq_restore_status": restore.get("active_freq_restore_status", ""),
                "controls_water_or_gas_routes": False,
                "writes_device_id": False,
                "writes_senco9": status == "written_readback_verified",
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
                "coeff_before_json": json.dumps(coeff_before, ensure_ascii=False),
                "coeff_target_json": json.dumps([float(offset), 1.0, 0.0, 0.0], ensure_ascii=False),
                "coeff_readback_json": json.dumps(coeff_readback, ensure_ascii=False),
                "verify_result_json": json.dumps(verify_result, ensure_ascii=False, default=str),
                "runtime_restore_json": json.dumps(restore, ensure_ascii=False, default=str),
                "candidate_source_row_json": json.dumps(dict(row), ensure_ascii=False, default=str),
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
        }
    ]
    tables = {
        "senco9_write_summary": summary_rows,
        "senco9_write_detail": detail_rows,
        "senco9_write_conclusion": conclusion_rows,
    }
    metadata = ValidationMetadata(
        tool_name="run_v1_5_pressure_senco9_controlled_write",
        created_at=end_ts,
        analyzers=[f"{row.get('analyzer_prefix')}:{row.get('analyzer_device_id')}" for row in summary_rows],
        input_paths=[str(cfg_path), str(summary_path)],
        output_dir=str(destination),
        config_path=str(cfg_path),
        config_summary={
            "write_all_supported": bool(args.write_all_supported),
            "device_ids": [_device_id(item) for item in args.device_id],
            "reviewer": str(args.reviewer),
            "approver": str(args.approver),
            "controls_water_or_gas_routes": False,
            "writes_device_id": False,
            "senco_group": 9,
            "pre_device_cooldown_s": float(args.pre_device_cooldown_s),
            "inter_device_delay_s": float(args.inter_device_delay_s),
            "restore_command_gap_s": float(args.restore_command_gap_s),
            "restore_active_freq": bool(args.restore_active_freq),
            "coefficient_quiet_settle_s": float(args.coefficient_quiet_settle_s),
            "coefficient_read_timeout_s": float(args.coefficient_read_timeout_s),
            "coefficient_read_delay_s": float(args.coefficient_read_delay_s),
            "coefficient_read_retries": int(args.coefficient_read_retries),
            "senco9_offset_mode": str(args.senco9_offset_mode),
        },
        notes=[
            "Controlled real-device SENCO9 write.",
            "Each analyzer is opened by its own configured port and verified by MODE2 frame ID before write.",
            "No PACE, valve, water route, gas route, humidity generator, or device-ID writes are performed.",
        ],
    )
    outputs = write_validation_report(
        destination,
        prefix="pressure_senco9_controlled_write",
        metadata=metadata,
        tables=tables,
    )
    snapshot_path = destination / "old_getco9_snapshot.json"
    snapshot_path.write_text(json.dumps(old_snapshot, ensure_ascii=False, indent=2), encoding="utf-8")
    _write_csv(destination / "old_getco9_snapshot.csv", [
        {
            "analyzer_device_id": device_id,
            "analyzer_prefix": payload.get("analyzer_prefix", ""),
            "port": payload.get("port", ""),
            "GETCO9_before": json.dumps(payload.get("GETCO9_before") or [], ensure_ascii=False),
            "candidate_values": json.dumps(payload.get("candidate_values") or [], ensure_ascii=False),
            "readback": json.dumps(payload.get("readback") or [], ensure_ascii=False),
        }
        for device_id, payload in old_snapshot.items()
    ])
    _log(f"Controlled SENCO9 write report saved: {outputs['workbook']}")
    _log(f"Old GETCO9 snapshot saved: {snapshot_path}")
    return 1 if any_failed else 0


if __name__ == "__main__":
    sys.exit(main())
