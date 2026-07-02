"""Run controlled V1.5 SENCO7/SENCO8 temperature-channel neutralization.

SENCO7 and SENCO8 are analyzer temperature-input correction groups.  V1.5
component calibration uses temperature as a model input, so a stale or extreme
temperature coefficient can make otherwise clean CO2/H2O ratio data appear
wrong.  This tool neutralizes only SENCO7/SENCO8 after the immutable GETCO1-9
epoch-0 snapshot has been captured.

The neutral temperature contract is C0=0, C1=1, C2=0, C3=0.  By default the
tool writes the explicit neutral SENCO7/SENCO8 payload and verifies GETCO
readback.  A CLEARSENCO method is also available for firmware that supports it.

This tool never writes SENCO1/SENCO2/SENCO3/SENCO4/SENCO5/SENCO6/SENCO9, never
changes analyzer IDs, and never controls PACE, valves, gas routes, water routes,
dewpoint instruments, or humidity generation.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence

from ..config import load_config
from ..devices import GasAnalyzer
from ..senco_format import format_senco_values
from ..validation.reporting import ValidationMetadata, write_validation_report
from . import run_v1_5_co2_senco1_controlled_write as base
from .v1_5_serial_safety import require_fragile_serial_timing


CONFIRMATION_TEXT = "WRITE_SENCO78_NEUTRAL_V1_5_TEMPERATURE_INPUTS"
TARGET_SENCO78 = (0.0, 1.0, 0.0, 0.0)
SUPPORTED_CHANNELS = (7, 8)


def _is_neutral(values: Sequence[float], *, atol: float = 0.05) -> bool:
    if len(values) < 4:
        return False
    padded = [float(item) for item in list(values)[:4]]
    return (
        abs(padded[0]) <= float(atol)
        and abs(padded[1] - 1.0) <= float(atol)
        and abs(padded[2]) <= float(atol)
        and abs(padded[3]) <= float(atol)
    )


def _channels(raw: Sequence[int] | None) -> tuple[int, ...]:
    values = tuple(int(item) for item in (raw or ()))
    if not values:
        return SUPPORTED_CHANNELS
    out: list[int] = []
    for value in values:
        if value not in SUPPORTED_CHANNELS:
            raise ValueError(f"SENCO temperature channel must be 7 or 8, got {value}")
        if value not in out:
            out.append(value)
    return tuple(out)


def _send_payload(
    ga: GasAnalyzer,
    payload: str,
    *,
    ack_timeout_s: float,
) -> tuple[bool, List[str]]:
    prepare = getattr(ga, "_prepare_coefficient_io", None)
    if callable(prepare):
        prepare()

    ser = getattr(ga, "ser", None)
    exchange = getattr(ser, "exchange_readlines", None)
    if callable(exchange):
        try:
            ser.flush_input()
        except Exception:
            pass
        raw_lines = exchange(
            payload + "\r\n",
            response_timeout_s=max(0.2, float(ack_timeout_s)),
            read_timeout_s=0.05,
            clear_input=True,
        )
        response_lines: List[str] = []
        for line in raw_lines:
            response_lines.extend(GasAnalyzer._split_stream_lines(line))
        acked = any(GasAnalyzer._is_success_ack(line) for line in response_lines)
        return acked, response_lines

    sender = getattr(ga, "_send_config_with_retries", None)
    if not callable(sender):
        raise RuntimeError("GasAnalyzer does not expose controlled config sender")
    acked = bool(
        sender(
            payload,
            broadcast=True,
            require_ack=True,
            attempts=1 + max(0, int(getattr(ga, "CONFIG_ACK_RETRY_COUNT", 1))),
            retry_delay_s=float(getattr(ga, "CONFIG_ACK_RETRY_DELAY_S", 0.1)),
        )
    )
    return acked, []


def _neutral_payload(
    group: int,
    *,
    method: str,
    target: str,
) -> str:
    if str(method).strip().lower() == "clear":
        return f"CLEARSENCO{int(group)},YGAS,{target}"
    return f"SENCO{int(group)},YGAS,{target}," + ",".join(format_senco_values(TARGET_SENCO78))


def _write_temperature_group(
    ga: GasAnalyzer,
    *,
    group: int,
    device_id: str,
    method: str,
    target_mode: str,
    ack_timeout_s: float,
) -> tuple[bool, str, List[str]]:
    target = device_id if str(target_mode or "").strip().lower() == "device_id" else getattr(ga, "COMMAND_TARGET_ID", "FFF")
    payload = _neutral_payload(group, method=method, target=target)
    acked, response_lines = _send_payload(ga, payload, ack_timeout_s=float(ack_timeout_s))
    return acked, payload, response_lines


def _read_temperature_group(
    ga: GasAnalyzer,
    group: int,
    *,
    attempts: int,
    retry_delay_s: float,
) -> List[float]:
    return base._read_group_values_with_retry(
        ga,
        int(group),
        min_count=4,
        attempts=max(1, int(attempts)),
        retry_delay_s=max(0.0, float(retry_delay_s)),
    )


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Controlled V1.5 SENCO7/SENCO8 temperature neutral writer.")
    parser.add_argument("--config", required=True, help="V1.5 hardware config JSON.")
    parser.add_argument("--output-dir", required=True, help="Output directory for write evidence.")
    parser.add_argument("--device-id", action="append", default=[], help="Analyzer MODE2 device ID to consider.")
    parser.add_argument("--channel", action="append", type=int, choices=SUPPORTED_CHANNELS, default=[])
    parser.add_argument("--write-all-nonneutral", action="store_true", help="Write every selected non-neutral SENCO7/8.")
    parser.add_argument("--enable-senco78-write", action="store_true", help="Unlock the real SENCO7/8 write path.")
    parser.add_argument("--operator-confirmation", default="", help=f"Must equal {CONFIRMATION_TEXT!r}.")
    parser.add_argument("--reviewer", default="")
    parser.add_argument("--approver", default="")
    parser.add_argument("--continue-on-failure", action="store_true")
    parser.add_argument("--identity-timeout-s", type=float, default=6.0)
    parser.add_argument("--readback-attempts", type=int, default=4)
    parser.add_argument("--readback-retry-delay-s", type=float, default=1.0)
    parser.add_argument("--post-write-settle-s", type=float, default=2.0)
    parser.add_argument("--pre-device-cooldown-s", type=float, default=5.0)
    parser.add_argument("--inter-device-delay-s", type=float, default=10.0)
    parser.add_argument("--restore-command-gap-s", type=float, default=1.0)
    parser.add_argument("--restore-active-freq", action="store_true", default=True)
    parser.add_argument("--no-restore-active-freq", dest="restore_active_freq", action="store_false")
    parser.add_argument("--coefficient-quiet-settle-s", type=float, default=3.0)
    parser.add_argument("--coefficient-read-timeout-s", type=float, default=2.0)
    parser.add_argument("--coefficient-read-delay-s", type=float, default=1.0)
    parser.add_argument("--coefficient-read-retries", type=int, default=4)
    parser.add_argument("--compare-atol", type=float, default=0.05)
    parser.add_argument(
        "--method",
        choices=("write-neutral", "clear"),
        default="write-neutral",
        help="Use explicit SENCO7/8 neutral payloads by default, or CLEARSENCO7/8 when firmware is verified.",
    )
    parser.add_argument(
        "--senco78-target-mode",
        choices=("broadcast", "device_id"),
        default="broadcast",
        help="Use YGAS,FFF or YGAS,<device_id> for the neutralization payload.",
    )
    parser.add_argument("--senco78-ack-timeout-s", type=float, default=2.5)
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    output_dir = Path(args.output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    try:
        require_fragile_serial_timing(args, tool_name="run_v1_5_temperature_senco78_neutral_controlled_write")
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        return 2

    if not bool(args.enable_senco78_write) or str(args.operator_confirmation).strip() != CONFIRMATION_TEXT:
        print("SENCO7/8 write locked: explicit unlock and operator confirmation are required.", file=sys.stderr)
        return 2
    if not str(args.reviewer).strip() or not str(args.approver).strip():
        print("SENCO7/8 write locked: reviewer and approver are required.", file=sys.stderr)
        return 2
    if str(args.reviewer).strip() == str(args.approver).strip():
        print("SENCO7/8 write locked: reviewer and approver must differ.", file=sys.stderr)
        return 2

    cfg = load_config(args.config)
    analyzer_map = base._build_analyzer_map(cfg)
    selected = [base._device_id(item) for item in args.device_id if str(item or "").strip()]
    target_ids = selected or sorted(analyzer_map)
    channels = _channels(args.channel)
    if not target_ids:
        print("No enabled gas analyzers found.", file=sys.stderr)
        return 1

    rows: List[Dict[str, Any]] = []
    old_snapshot: Dict[str, Any] = {}
    overall_ok = True

    for idx, device_id in enumerate(target_ids):
        analyzer_cfg = analyzer_map.get(device_id)
        if not analyzer_cfg:
            rows.append(
                {
                    "device_id": device_id,
                    "senco_group": "",
                    "status": "skipped",
                    "reason": "device_id_not_in_config",
                    "write_applied": False,
                }
            )
            overall_ok = False
            if not args.continue_on_failure:
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

        try:
            base._log(
                f"Temperature SENCO7/8 neutral controlled write begin: "
                f"device_id={device_id} port={analyzer_cfg.get('port')}"
            )
            ga.open()
            base._configure_coefficient_io(ga, args)
            base._sleep_gap(float(args.pre_device_cooldown_s))
            identity_before = base._read_identity_snapshot(
                ga,
                prefer_stream=bool(analyzer_cfg.get("active_send", True)),
                timeout_s=float(args.identity_timeout_s),
            )
            if base._device_id(identity_before.get("id")) != device_id:
                raise RuntimeError(
                    f"identity_mismatch expected={device_id} observed={identity_before.get('id') or '<missing>'}"
                )

            old_snapshot.setdefault(
                device_id,
                {
                    "analyzer_prefix": analyzer_cfg.get("name", ""),
                    "port": analyzer_cfg.get("port", ""),
                    "GETCO7_before_live": [],
                    "GETCO8_before_live": [],
                    "GETCO7_after_live": [],
                    "GETCO8_after_live": [],
                },
            )

            for group in channels:
                old_live: List[float] = []
                final_live: List[float] = []
                payload = ""
                write_ack_lines: List[str] = []
                status = "failed"
                reason = ""
                write_applied = False
                try:
                    old_live = _read_temperature_group(
                        ga,
                        group,
                        attempts=max(1, int(args.readback_attempts)),
                        retry_delay_s=float(args.readback_retry_delay_s),
                    )
                    final_live = list(old_live)
                    old_snapshot[device_id][f"GETCO{group}_before_live"] = list(old_live)
                    if _is_neutral(old_live, atol=float(args.compare_atol)):
                        status = "already_neutral"
                    else:
                        if not args.write_all_nonneutral and not selected:
                            status = "blocked"
                            reason = "write_all_nonneutral_or_explicit_device_required"
                        else:
                            if not ga.set_mode_with_ack(2, require_ack=True):
                                raise RuntimeError(f"MODE=2 not acknowledged before SENCO{group} write")
                            base._sleep_gap(float(args.readback_retry_delay_s))
                            acked, payload, write_ack_lines = _write_temperature_group(
                                ga,
                                group=group,
                                device_id=device_id,
                                method=str(args.method),
                                target_mode=str(args.senco78_target_mode),
                                ack_timeout_s=float(args.senco78_ack_timeout_s),
                            )
                            write_applied = True
                            if not acked:
                                raise RuntimeError(f"SENCO{group}_WRITE_ACK_FAILED response={write_ack_lines}")
                            base._sleep_gap(max(0.0, float(args.post_write_settle_s)))
                            final_live = _read_temperature_group(
                                ga,
                                group,
                                attempts=max(1, int(args.readback_attempts)),
                                retry_delay_s=float(args.readback_retry_delay_s),
                            )
                            if not _is_neutral(final_live, atol=float(args.compare_atol)):
                                raise RuntimeError(f"SENCO{group}_READBACK_MISMATCH final={final_live}")
                            status = "written_readback_verified"
                    old_snapshot[device_id][f"GETCO{group}_after_live"] = list(final_live)
                except Exception as exc:
                    reason = str(exc)
                    overall_ok = False
                    if not args.continue_on_failure:
                        raise
                finally:
                    if status not in {"written_readback_verified", "already_neutral"}:
                        overall_ok = False
                    rows.append(
                        {
                            "device_id": device_id,
                            "analyzer_prefix": analyzer_cfg.get("name", ""),
                            "port": analyzer_cfg.get("port", ""),
                            "senco_group": f"SENCO{group}",
                            "old_senco": json.dumps(old_live, ensure_ascii=False),
                            "target_senco": json.dumps(list(TARGET_SENCO78), ensure_ascii=False),
                            "final_senco": json.dumps(final_live, ensure_ascii=False),
                            "method": str(args.method),
                            "payload": payload,
                            "payload_target_mode": str(args.senco78_target_mode),
                            "write_ack_response_json": json.dumps(write_ack_lines, ensure_ascii=False),
                            "status": status,
                            "reason": reason,
                            "write_applied": write_applied,
                            "identity_before": json.dumps(identity_before, ensure_ascii=False, default=str),
                            "identity_after": json.dumps(identity_after, ensure_ascii=False, default=str),
                            "runtime_restore_json": json.dumps(restore, ensure_ascii=False, default=str),
                            "writes_senco1": False,
                            "writes_senco2": False,
                            "writes_senco3": False,
                            "writes_senco4": False,
                            "writes_senco5": False,
                            "writes_senco6": False,
                            "writes_senco7": bool(write_applied and group == 7),
                            "writes_senco8": bool(write_applied and group == 8),
                            "writes_senco9": False,
                            "writes_device_id": False,
                            "clears_senco": bool(write_applied and str(args.method) == "clear"),
                            "clears_senco7": bool(write_applied and group == 7 and str(args.method) == "clear"),
                            "clears_senco8": bool(write_applied and group == 8 and str(args.method) == "clear"),
                            "controls_water_or_gas_routes": False,
                            "controls_pace": False,
                        }
                    )
                    if status not in {"written_readback_verified", "already_neutral"} and not args.continue_on_failure:
                        break

            restore = base._restore_analyzer_runtime(
                ga,
                analyzer_cfg,
                command_gap_s=float(args.restore_command_gap_s),
                restore_active_freq=bool(args.restore_active_freq),
            )
            for row in rows:
                if row.get("device_id") == device_id and not row.get("runtime_restore_json"):
                    row["runtime_restore_json"] = json.dumps(restore, ensure_ascii=False, default=str)
            base._sleep_gap(0.5)
            identity_after = base._read_identity_snapshot(
                ga,
                prefer_stream=bool(analyzer_cfg.get("active_send", True)),
                timeout_s=float(args.identity_timeout_s),
            )
            if base._device_id(identity_after.get("id")) != device_id:
                raise RuntimeError(
                    f"post_write_identity_mismatch expected={device_id} observed={identity_after.get('id') or '<missing>'}"
                )
            for row in rows:
                if row.get("device_id") == device_id:
                    row["identity_after"] = json.dumps(identity_after, ensure_ascii=False, default=str)
        except Exception as exc:
            overall_ok = False
            for row in rows:
                if row.get("device_id") == device_id and row.get("status") in {"failed", "blocked"} and not row.get("reason"):
                    row["reason"] = str(exc)
            try:
                restore = base._restore_analyzer_runtime(
                    ga,
                    analyzer_cfg,
                    command_gap_s=float(args.restore_command_gap_s),
                    restore_active_freq=bool(args.restore_active_freq),
                )
            except Exception:
                pass
            if not args.continue_on_failure:
                break
        finally:
            try:
                ga.close()
            except Exception:
                pass

        if idx + 1 < len(target_ids):
            base._sleep_gap(float(args.inter_device_delay_s))

    conclusion = {
        "overall_status": "success" if overall_ok else "failed",
        "target_count": len(rows),
        "success_count": sum(1 for row in rows if row.get("status") == "written_readback_verified"),
        "already_neutral_count": sum(1 for row in rows if row.get("status") == "already_neutral"),
        "failed_count": sum(1 for row in rows if row.get("status") not in {"written_readback_verified", "already_neutral"}),
    }

    base._write_csv(output_dir / "senco78_neutral_write_events.csv", rows)
    base._write_csv(output_dir / "conclusion.csv", [conclusion])
    (output_dir / "old_senco78_snapshot.json").write_text(
        json.dumps(old_snapshot, ensure_ascii=False, indent=2, default=str),
        encoding="utf-8",
    )
    (output_dir / "senco78_neutral_write_meta.json").write_text(
        json.dumps(
            {
                "created_at": datetime.now().isoformat(timespec="seconds"),
                "tool": "run_v1_5_temperature_senco78_neutral_controlled_write",
                "confirmation_text": CONFIRMATION_TEXT,
                "reviewer": args.reviewer,
                "approver": args.approver,
                "target_senco78": list(TARGET_SENCO78),
                "channels": list(channels),
                "method": str(args.method),
                "senco78_target_mode": str(args.senco78_target_mode),
                "senco78_ack_timeout_s": float(args.senco78_ack_timeout_s),
                "writes_senco7": any(bool(row.get("writes_senco7")) for row in rows),
                "writes_senco8": any(bool(row.get("writes_senco8")) for row in rows),
                "writes_device_id": False,
                "controls_water_or_gas_routes": False,
                "controls_pace": False,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    metadata = ValidationMetadata(
        tool_name="run_v1_5_temperature_senco78_neutral_controlled_write",
        analyzers=[str(row.get("device_id") or "") for row in rows],
        config_path=str(Path(args.config).resolve()),
        output_dir=str(output_dir),
        config_summary={
            "suite_id": "v1_5_temperature_senco78_neutralization",
            "status": str(conclusion["overall_status"]),
            "target_senco78": list(TARGET_SENCO78),
            "channels": list(channels),
            "method": str(args.method),
            "restore_active_freq": bool(args.restore_active_freq),
            "compare_atol": float(args.compare_atol),
        },
        notes=[
            "Controlled real-device SENCO7/SENCO8 neutralization.",
            "This step belongs after immutable GETCO1-9 epoch-0 backup and before pressure/component sampling.",
            "No PACE, valve, water route, gas route, humidity generator, device-ID writes, or CO2/H2O/pressure coefficient writes are performed.",
        ],
    )
    write_validation_report(
        output_dir,
        tables={
            "senco78_neutral_write_events": rows,
            "conclusion": [conclusion],
        },
        metadata=metadata,
        prefix="temperature_senco78_neutral_controlled_write",
    )
    print(json.dumps(conclusion, ensure_ascii=False, indent=2), flush=True)
    return 0 if overall_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
